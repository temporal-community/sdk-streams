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
- The prefix has only letters, digits, `-`, `.`, `_` and `~`, so a key pattern shows it as written.
- A create writes `:meta`. An append writes `:producers`, and an append or a close writes `:log`.

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
| `p\|<kind>\|<producer id>` | The Producer's fence, held epoch and last batch, 129 bytes |

`<kind>` is the `ProducerKind` number. The `p|` value has big-endian numbers in this order.

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
  `stream_storage`, since a later format may write it.
- An epoch key is the task key (started event id 8, attempt 4, started seconds 8 and nanos 4),
  then the attempt's scheduled seconds 8 and nanos 4 and the attempt 4. A negative field is zero.
- The scripts compare two epoch keys byte by byte. Lua's `<` collates with the server's locale.
- The digest is the first 16 bytes of the batch's SHA-256.

`:producers` scores each `<kind>|<producer id>` with the ms of its last append. An append drops the
`p|` field of up to 64 Producers with no append for one Retention. A resend of such a Producer's
last batch then writes the batch again, and a lower epoch of the Producer is accepted. So the fence
lasts one Retention after the last append.

An append with sequence 0 takes the sequence after the last batch's when the fence is the batch's
epoch key, and 1 otherwise. The script reads `p|` and writes the batch in one step, so two such
batches never get the same sequence. Nothing dedupes such a batch, so its retry writes it again.

## Log entries

- `r` is the encoded `StreamRecord`, with no offset. A data Record has no Producer in it. An
  append refuses a Record that isn't an encoded `StreamRecord`, since each read decodes it.
- `p` is the encoded `Producer` of the batch, on the batch's first data entry. The Store sets it on
  each data Record of the batch when it reads them.
- `b` is the seq of the batch's first entry, on each later data entry, so a read that starts inside
  a batch gets `p` with one lookup. An entry never has both `p` and `b`.
- A batch's entries share one millisecond, and a trim drops whole milliseconds, so a batch keeps
  its `p` entry for as long as it has any entry.
- `s` is the encoded `StreamSuperseded` with the withdrawal facts, on the first entry of a batch
  that withdraws. A read builds the Superseded item from it. An entry can have only `s`.
- An offset is the entry id `<ms>-<seq>`, packed as `ms << 20 | seq`.

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
  `+script|load` needs an admin to load the read script on each primary, after each restart,
  failover or `SCRIPT FLUSH`. The script's SHA is of its exact bytes, so load it with
  `{ printf '#!lua flags=no-writes\n'; cat lua/read.lua; } | redis-cli -x SCRIPT LOAD`.

## Eviction and replication

- Use `maxmemory-policy noeviction`. Another policy can evict the meta and keep the log, which loses
  the fence, the dedupe and the close. The Store warns about it on connect.
- A failover can lose a write that Redis acknowledged. With `wait_replicas`, each write waits for
  that many replicas with `WAIT`. A write that they don't have after `wait_timeout` fails as a
  Store failure, and its retry with the same sequence writes it once.
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
- A client's Stream has no owner but the kind, so any client closes it.
- Redis can't tie a run to its run chain. A run of another chain that holds the owner's handle,
  such as a child Workflow with its parent's Stream, can still close it. The native server checks
  the run.
