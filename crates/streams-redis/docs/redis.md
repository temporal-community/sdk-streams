# The Redis layout

Every SDK that uses Streams on Redis must use this layout. The scripts in `lua/` are its canonical
form, and the `testdata/` of `temporalio-streams` has its test vectors.

## Keys

```text
<prefix>:{<namespace>:<stream id>}:meta
<prefix>:{<namespace>:<stream id>}:log
<prefix>:{<namespace>:<stream id>}:producers
```

- Each part is percent-encoded. The hash tag puts the three keys in one cluster slot.
- The prefix has only letters, digits, `-`, `.`, `_` and `~`, so a key pattern names it as written.
- A create writes `:meta`. An append writes `:producers`. An append, a close or a retry run's
  create writes `:log`.

## The meta hash

| Field | Value |
|---|---|
| `v` | The format version, 1 |
| `ret` | The Retention, in ms |
| `own` | `wf` for a Workflow-owned Stream, `client` otherwise |
| `oc` | The owner's run chain, `<workflow id>:<first run id>` with each part encoded |
| `exp` | When the Stream stops taking writes, in ms since 1970 |
| `closed`, `close_id` | Set by the close, with the entry id of the close Record |
| `newest` | The entry id of the newest entry |
| `trimmed` | The entry id of the newest entry that Retention or the log's expiry dropped |
| `e\|<kind>\|<producer id>` | The newest epoch key of the Producer, the fence |
| `d\|<kind>\|<producer id>` | `<epoch key>\|<sequence>\|<first id>\|<last id>\|<digest>` |
| `h\|<kind>\|<producer id>` | The newest epoch key that still has Records here |
| `oa\|<run>` | The run's Local Activity task keys here that may not have completed |
| `ow\|<run>` | The task keys of the run whose Local Activity Records an orphan item withdrew |
| `run\|<run>` | Set when a Producer of the run wrote a Record here |
| `wr\|<run>` | Set when a Workflow retry withdrew the run |

`<kind>` is the `ProducerKind` number, and `<run>` is the percent-encoded run id. `d` is the
Producer's last batch. An epoch key is the epoch's fields as fixed-width hex, so two keys compare as
strings.

`:producers` scores each `<kind>|<producer id>` with the ms of its last append. An append drops the
`e|`, `d|` and `h|` fields of up to 64 Producers with no append for one Retention. A resend of such
a Producer's last batch then writes the batch again, and a lower epoch of the Producer is accepted.
So the fence lasts one Retention after the last append.

## Log entries

- `r` is the encoded `StreamRecord`, with no offset.
- `s` is the encoded `Supersession`, on the first entry of a batch that withdraws. An entry can have
  only `s`.
- An offset is the entry id `<ms>-<seq>`, packed as `ms << 20 | seq`.

## Orphaned Local Activity Records

A Local Activity batch can name its run's last completed Workflow Task (`previous_completed_task`)
and failed tasks. Then the append withdraws the run's tasks in `oa` that never completed.

- These are the tasks after the completed task and before the batch's task, the failed tasks, and
  the lower attempts of the batch's task and of the completed task.
- One entry with only `s` withdraws them. Its `Supersession` is of the Workflow kind, with the
  run's Workflow Producer, the batch's task, and the completed and failed tasks.
- The append then drops the tasks at or before the completed task from `oa`, and adds the batch's
  task when the batch has Records.
- `ow` keeps the withdrawn tasks, and fences a late write of each one.
- A batch with no `previous_completed_task` skips the rule.

## Retry runs

- A Workflow retry run names its previous run in `withdraw_run`, on its create and on its Local
  Activities' appends.
- The first such call sets `wr|<previous run>`, also when the create makes the Stream.
- When `run|<previous run>` is set, the call also writes `s`. A create writes it on an entry of its
  own, and an append on its first entry.
- When `wr|<run>` is set, each later append and close of the run's Producers fails with
  `stream_fenced`.
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
- On a cluster, each role also needs `+cluster|slots +asking +ping`, on each node.
- A process with both roles needs both sets.
- `+info` and `+config|get` are optional. Without `+info`, the Store skips the version check.
  Without `+config|get`, it warns that it can't read the eviction policy.
- A user with write access can write as any Producer.
- The ACLs don't limit CPU or memory, since `+script|load` lets a user run any Lua. A Reader with no
  `+script|load` needs an admin to load the read script on each primary, after each restart,
  failover or `SCRIPT FLUSH`. That script is the line `#!lua flags=no-writes` and then
  `lua/read.lua`.

## Eviction and replication

- Use `maxmemory-policy noeviction`. Another policy can evict the meta and keep the log, which loses
  the fence, the dedupe and the close. The Store warns about it on connect.
- A failover can lose a write that Redis acknowledged. With `wait_replicas`, each write waits for
  that many replicas with `WAIT`. A write that they don't have after `wait_timeout` fails as a
  Store failure, and its retry writes it once.
- With `WAIT`, a call that finds its write done, such as a resend, writes `v` again. Then its
  `WAIT` covers the earlier write.
- A failover can still promote a replica that doesn't have an acknowledged write.

## Known limits

- Nothing closes a Workflow-owned Stream whose owner was terminated or timed out, so it stays open
  until its safety TTL passes. The native server closes it at the owner's end.
- A client's Stream has no owner but the kind, so any client closes it.
- A Local Activity call of a failed Workflow Task is withdrawn only when the retry task runs that
  call again, or when a later Local Activity write names the failed task. A call that no later
  write covers, for example because a Signal changed the path and no Local Activity writes
  again, keeps its Records. The memory Store has the same limit.
- Core sends only the last 100 failed Workflow Tasks of a run. An older failed task before the last
  completed one keeps its Local Activity Records.
- A Local Activity that runs longer than its Workflow Task, which Core then completes as a
  heartbeat, keeps its Records when the task where it resolves fails. Only a later run of the same
  call withdraws them.
- A retry run withdraws only its previous run. When that run never wrote to the Stream, the runs
  before it keep their Records. The Store can't tell an earlier retry attempt from a
  continue-as-new run before it.
- A run that closes its Stream and then fails under a retry policy leaves the Stream closed, and
  each retry run's create fails with `stream_closed`. A close can't be withdrawn, since the
  Readers that read it have ended.
- A reset run doesn't withdraw the base run's Records after the reset point, and it doesn't fence
  the base run's Producers.
- The meta keeps the `run|`, `oa|`, `ow|` and `wr|` fields of each run until the Stream expires. A
  long run chain, such as one that continues-as-new every hour, adds fields to its Stream's meta
  for as long as it writes.
- Redis can't tie a run to its run chain. A run of another chain that holds the owner's ref, such
  as a child Workflow with its parent's Stream, can still close it. The native server checks the
  run.
