# The Redis layout

Every SDK that uses Streams on Redis must use this layout. The scripts in `lua/` are its canonical
form, and the `testdata/` of `temporalio-streams` has its test vectors.

## Keys

```text
<prefix>:{<namespace>:<stream id>}:meta
<prefix>:{<namespace>:<stream id>}:log
<prefix>:{<namespace>:<stream id>}:closed
```

- Each part is percent-encoded. The hash tag puts the three keys in one cluster slot.
- The prefix has only letters, digits, `-`, `.`, `_` and `~`, so a key pattern shows it as written.
- A create writes `:meta`. An append writes `:meta` and `:log`.
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
| `exp` | When the Stream stops taking writes, in ms since 1970 |
| `rid` | The request id of the create that made the Stream, absent when it gave none |
| `closed` | Set by the close |
| `closer` | The encoded Producer that closed the Stream, which a read builds the close item from |
| `cstat` | The close status, `1` for CLOSED, which a Brook Store is the only one it sets |
| `newest` | The entry id of the newest entry |
| `trimmed` | The entry id of `newest` when a write found the log gone |
| `p\|<kind>\|<producer id>` | The Producer's fence and last batch, 88 bytes |

`<kind>` is the `ProducerKind` number. The `p|` value has big-endian numbers in this order.

| Bytes | Field |
|---|---|
| 40 | The fence, the Producer's newest epoch key. It is also the epoch of the last batch. |
| 8 | The last batch's sequence |
| 12 | The last batch's first entry id, the ms in 8 bytes and the seq in 4 |
| 12 | The last batch's last entry id, in the same form |
| 16 | The last batch's digest, which a resend must match |

- The ids are zero when the last batch wrote no entry. A real entry id is never `0-0`.
- A call that finds a value of another length, or only one zero id, fails with
  `stream_storage_corrupt`, since a later format may write it.
- A task key is the started event id 8, the attempt 4, and the started seconds 8 and nanos 4.
- An epoch key is the task key, then the attempt's scheduled seconds 8 and nanos 4 and the
  attempt 4. A negative field is zero.
- The scripts compare two keys byte by byte. Lua's `<` collates with the server's locale.
- The digest is the first 16 bytes of the batch's SHA-256.

No write drops a Producer's `p|` field, however long ago its last append was. So a resend of a
Producer's last batch gets the first answer, and a lower epoch of the Producer is fenced, for as
long as the Stream exists.

An append with sequence 0 takes the sequence after the last batch's when the fence is the batch's
epoch key, and 1 otherwise. The script reads `p|` and writes the batch in one step, so two such
batches never get the same sequence. Nothing dedupes such a batch, so its retry writes it again.

## Create

A create of a Stream that exists refuses, in this order, an expired Stream (`stream_expired`) and a
closed Stream (`stream_closed`). Then:

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
- A batch's entries share one millisecond, and the log loses its entries only all at once, so a
  batch keeps its `p` entry for as long as it has any entry.
- An offset is the entry id `<ms>-<seq>`, packed as `ms << 20 | seq`.

## TTLs

- Each write sets `exp` to now plus the safety TTL for an open Stream, and to now plus the Retention
  for a closed one. So the Retention counts from the close.
- The log expires at `exp`. The meta key expires 30 days after `exp`, as the Tombstone.
- An open Stream keeps all its Records. No append drops a Record by its age, and no read hides one.
- A write that finds the log gone, as after an eviction, sets `trimmed` to `newest`. Before that
  write, a read at or below `newest` that finds no entry fails with `stream_expired`.

## Access

Redis checks the ACL of the script call and of each command in the script. `tests/acl.rs` checks
these lists on a standalone server and on a cluster.

- A Producer creates, appends and closes. It needs the keys `~<prefix>:{<namespace>:*` and the
  commands `+evalsha +script|load +time +hget +hset +exists +pexpireat +xadd +xrevrange`. With
  `wait_replicas`, it also needs `+wait`.
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
  then gets two copies.
- With `WAIT`, a call that finds its write done, such as a resend, writes `v` again. Then its
  `WAIT` covers the earlier write.
- A failover can still promote a replica that doesn't have an acknowledged write.

## Known limits

- An open Stream keeps all its Records until its safety TTL passes after its last write, even with
  a short Retention. A closed Stream expires one Retention after the close.
- The meta hash keeps one 88-byte `p|` field for each Producer that wrote to the Stream, so a
  Stream with many Producers has a large meta hash.
- Any Producer closes a Stream, since the Store doesn't track an owner.
- A cluster with `cluster-preferred-endpoint-type unknown-endpoint` gives no hosts in
  `CLUSTER SLOTS` and in its redirects. A blocking read then finds a slot's node only through the
  seeds and `MOVED`. redis-rs doesn't follow an `ASK` with no host, so a call fails with
  `stream_storage_unavailable` while its slot migrates.
