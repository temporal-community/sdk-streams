# The Redis layout

Every SDK that uses Streams on Redis must use this layout. The scripts in `lua/` are its canonical
form, and the `testdata/` of `temporalio-streams` has its test vectors.

## Keys

```text
<prefix>:{<namespace>:<stream id>}:meta
<prefix>:{<namespace>:<stream id>}:log
<prefix>:{<namespace>:<stream id>}:producers
<prefix>:{<namespace>:<stream id>}:closed
```

- Each part is percent-encoded. The hash tag puts the four keys in one cluster slot.
- The prefix has only letters, digits, `-`, `.`, `_` and `~`, so a key pattern shows it as written.
- A create writes `:meta`. An append writes `:producers` and `:log`, and so does a retry run's
  create.
- A close writes no log entry. It adds one entry to `:closed`, so a read that waits on the log and
  on `:closed` wakes. `:closed` expires with the log.
- The close script gets `:closed` as an argument, not in `KEYS`. While a slot migrates, a script
  with a declared key that doesn't exist yet gets `TRYAGAIN`, so each close would fail. Redis
  doesn't promise a script a key that `KEYS` doesn't name, but it allows one of the script's own
  slot, and the slot migration case of `tests/acl.rs` checks it.

## The meta hash

| Field | Value |
|---|---|
| `v` | The format version, 1 |
| `ret` | The Retention, in ms |
| `own` | `wf` for a Workflow-owned Stream, `client` otherwise |
| `oc` | The owner's run chain, `<workflow id>:<first run id>` with each part encoded |
| `exp` | When the Stream stops taking writes, in ms since 1970 |
| `rid` | The request id of the create that made the Stream, absent when it gave none |
| `closed` | Set by the close |
| `newest` | The entry id of the newest entry |
| `trimmed` | The entry id of the newest entry that Retention or the log's expiry dropped |
| `p\|<kind>\|<producer id>` | The Producer's fence, held epoch and last batch, 129 bytes |
| `oa\|<run>` | The run's Local Activity task keys here that may not have completed |
| `ow\|<run>\|<task key>` | Set when an orphan item withdrew the task's Local Activity Records |
| `tf\|<run>` | The task facts of the run's highest task, as task keys back to back |
| `run\|<run>` | Set when a Producer of the run wrote a Record here |
| `wr\|<run>` | Set when a Workflow retry withdrew the run |

`<kind>` is the `ProducerKind` number, and `<run>` is the percent-encoded run id. The `p|` value
has big-endian numbers in this order.

| Bytes | Field |
|---|---|
| 1 | Flags. Bit 0 is set when the held epoch is present, and the other bits are zero. |
| 40 | The fence, the Producer's newest epoch key. It is also the epoch of the last batch. |
| 40 | The held epoch key, the newest one that still has Records here, or zeros |
| 8 | The last batch's sequence |
| 12 | The last batch's first entry id, the ms in 8 bytes and the seq in 4 |
| 12 | The last batch's last entry id, in the same form |
| 16 | The last batch's digest, which a resend must match |

- The ids are zero when the last batch wrote no entry. A real entry id is never `0-0`.
- A call that finds a value of another length, another flag bit, or only one zero id fails with
  `stream_storage_corrupt`, since a later format may write it.
- A task key is the started event id 8, the attempt 4, and the started seconds 8 and nanos 4.
- An epoch key is the task key, then the attempt's scheduled seconds 8 and nanos 4 and the
  attempt 4. A negative field is zero.
- `oa|` and `tf|` hold 24-byte task keys back to back, and `ow|` ends in the raw task key. A call
  that finds `oa|` or `tf|` ending in a partial key, or `tf|` with fewer than two keys, fails with
  `stream_storage_corrupt`.
- The scripts compare two keys byte by byte. Lua's `<` collates with the server's locale.
- The digest is the first 16 bytes of the batch's SHA-256.

`:producers` scores each `<kind>|<producer id>` with the ms of its last append. An append drops the
`p|` field of up to 64 Producers with no append for one Retention. A resend of such a Producer's
last batch then writes the batch again, and a lower epoch of the Producer is accepted. So the fence
lasts one Retention after the last append.

An append with sequence 0 takes the sequence after the last batch's when the fence is the batch's
epoch key, and 1 otherwise. The script reads `p|` and writes the batch in one step, so two such
batches never get the same sequence. Nothing dedupes such a batch, so its retry writes it again.

## Create

A create of a Stream that exists refuses, in this order, an expired Stream (`stream_expired`), a
create with another owner (`stream_not_owner`) and a closed Stream (`stream_closed`). Then:

- A create with the `rid` of the create that made the Stream is its retry, so it gets `created`
  again. With `WAIT`, it waits for the replicas to have the first create's write.
- Any other create fails with `stream_already_exists`, unless it uses the existing Stream. Then it
  gets `exists`, and the Stream keeps its Retention.

## Log entries

- `r` is the encoded `StreamRecord`, with no offset. A data Record has no Producer in it. An
  append refuses a Record that isn't an encoded `StreamRecord`, since each read decodes it.
- `p` is the encoded `Producer` of the batch, on the batch's first data entry. The Store sets it on
  each data Record of the batch when it reads them.
- `b` is the seq of the batch's first entry, on each later data entry, so a read that starts inside
  a batch gets `p` with one lookup. An entry never has both `p` and `b`.
- A batch's entries share one millisecond, and a trim drops whole milliseconds, so a batch keeps
  its `p` entry for as long as it has any entry.
- `s` is the encoded `StreamSuperseded` with the withdrawal facts. A read builds the Superseded
  item from it.
- A batch that withdraws has `s` on its first entry. The orphan rule adds an entry with only `s`
  before it, so one batch can have two.
- An offset is the entry id `<ms>-<seq>`, packed as `ms << 20 | seq`.

## Orphaned Local Activity Records

A Local Activity batch can give its run's last completed Workflow Task (`previous_completed_task`)
and failed tasks. Then the append withdraws the run's tasks in `oa` that never completed.

- These are the tasks after the completed task and before the batch's task, the failed tasks, and
  the lower attempts of the batch's task and of the completed task.
- One entry with only `s` withdraws them. Its `StreamSuperseded` is of the Workflow kind, with the
  run's Workflow Producer, the batch's task, and the completed and failed tasks.
- The append then drops the tasks at or before the completed task from `oa`.
- Each Local Activity batch with Records adds its task to `oa`, also a batch with no task facts.
- Each `ow|<run>|<task key>` fences a late write of that task.
- A batch with no task facts skips the rule.
- `tf|<run>` keeps the facts of the run's highest task. These are its own key, the completed
  task's key or the zero key before the first, and the key of each failed task's started event id.
- A write of a task that `tf|` shows never completed fails with `stream_fenced`, since a task can
  fail before its call writes.

## Retry runs

- A Workflow retry run gives its previous run in `withdraw_run`, on its create and on the first
  batch of each of its Local Activities.
- The first such call sets `wr|<previous run>`, also when the create makes the Stream.
- When `run|<previous run>` is set, the call also writes `s`. A create writes it on an entry of its
  own, and an append on the first entry of its batch.
- When `run|<previous run>` isn't set, a create that finds the Stream sets only `wr|`, and `exp`
  stays as it is.
- When `wr|<run>` is set, each later append and close of the run's Producers fails with
  `stream_fenced`. A resend still gets its first answer, and a close of a closed Stream gets
  `already`.
- An append with Records sets `run|<run>`.

## TTLs

- Each write sets `exp` to now plus the safety TTL for an open Workflow-owned Stream, and to now
  plus the Retention otherwise.
- `:producers` expires at `exp`. The meta key expires 30 days after `exp`, as the Tombstone.
- The log expires at `exp`, or one Retention after the write if that is earlier.
- Each append drops the Records older than the Retention. A read hides them too.
- A write that finds the log gone sets `trimmed` to `newest`. Before that write, a read at or below
  `newest` that finds no entry fails with `stream_expired`.

## Access

Redis checks the ACL of the script call and of each command in the script. `tests/acl.rs` checks
these lists on a standalone server and on a cluster.

- A Producer creates, appends and closes. It needs the keys `~<prefix>:{<namespace>:*` and the
  commands `+evalsha +script|load +time +hget +hset +hdel +exists +pexpireat +xadd +xtrim
  +xrevrange +zadd +zrangebyscore +zrem`. With `wait_replicas`, it also needs `+wait`.
- A Reader reads and describes. It needs the keys `%R~<prefix>:{<namespace>:*` and the commands
  `+evalsha_ro +script|load +time +hmget +xrange`. A read that waits also needs `+xread`, and a
  read of the last Records needs `+xrevrange`.
- A user that lacks a command of its list can fail a script after its first write, and that write
  stays. `connect` checks `+wait`, since without it each write would fail its `WAIT` after the
  write happened.
- On a cluster, each role also needs `+cluster|slots +asking +ping`, on each node.
- A process with both roles needs both sets.
- `+info` and `+config|get` are optional. Without `+info`, the Store skips the version check.
  Without `+config|get`, it warns that it can't read the eviction policy.
- A user with write access can write as any Producer.
- The ACLs don't limit CPU or memory, since `+script|load` lets a user run any Lua. A Reader with no
  `+script|load` needs an admin to load the two read scripts on each primary, after each restart,
  failover or `SCRIPT FLUSH`. A script's SHA is of its exact bytes, so load them with this command.

  ```sh
  for script in read read_back; do
    { printf '#!lua flags=no-writes\n'; cat lua/batch.lua "lua/$script.lua"; } |
      redis-cli -x SCRIPT LOAD
  done
  ```

## Eviction and replication

- Use `maxmemory-policy noeviction`. Another policy can evict the meta and keep the log, which loses
  the fence, the dedupe and the close. The Store warns about it on connect.
- A failover can lose a write that Redis acknowledged. With `wait_replicas`, each write waits for
  that many replicas with `WAIT`. A write that they don't have after `wait_timeout` fails with
  `stream_storage_unavailable`, and its retry with the same sequence writes it once.
- `WAIT` protects only the writer's answer. A Reader can see a write before its `WAIT` ends, and
  a failover can then lose that write.
- On a cluster, the client sends a write again after it loses the reply. A batch with sequence 0
  then gets two copies, and a resend answer has no flag.
- With `WAIT`, a call that finds its write done, such as a resend, writes `v` again. Then its
  `WAIT` covers the earlier write.
- A failover can still promote a replica that doesn't have an acknowledged write.

## Known limits

- Nothing closes a Workflow-owned Stream whose owner was terminated or timed out, so it stays open
  until its safety TTL passes. The native server closes it when the owner's run ends.
- A Stream with no owner takes a close from any Workflow or client, since the Store can't tell
  which one made it.
- A failed task's Local Activity Records go only when the retry task runs the same call again, or
  a later Local Activity write gives the failed task. Otherwise they stay, as in the memory Store.
- Core sends only the last 100 failed Workflow Tasks of a run. An older failed task before the last
  completed one keeps its Local Activity Records.
- A Local Activity that runs longer than its Workflow Task keeps its Records when the task where it
  resolves fails. Only a later run of the same call withdraws them.
- A retry run withdraws only its previous run. If that run never wrote, older runs keep their
  Records, since the Store can't tell a retry attempt from a continue-as-new run.
- A run that closes its Stream and then fails under a retry policy leaves it closed, so each retry
  run's create fails with `stream_closed`. The close's Readers have ended, so it can't be withdrawn.
- A reset run doesn't withdraw the base run's Records after the reset point, and it doesn't fence
  the base run's Producers.
- The meta keeps the `run|`, `oa|`, `ow|`, `tf|` and `wr|` fields of each run until the Stream
  expires, so a long run chain adds fields for as long as it writes.
- Redis can't tie a run to its run chain. A run of another chain that holds the owner's handle,
  such as a child Workflow with its parent's Stream, can still close it. The native server checks
  the run.
