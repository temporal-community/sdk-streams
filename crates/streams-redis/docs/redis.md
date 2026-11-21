# What the Redis Store keeps

This page is for an operator of the Redis that a Store uses, and for the author of another Store
of this format. The scripts in `lua/` are the canonical form of the format.

On this page, an id is the id of an entry of a Redis stream, `<ms>-<seq>`.

## What to set up

- **Redis 7.0 or later.** The reads use `EVALSHA_RO` and the flags of a script.
- **`maxmemory-policy noeviction`.** With another policy Redis removes keys of a Stream when it
  is full ("A lost log" below). The Store doesn't read the policy.
- **A limit for open Streams.** An open Stream doesn't expire by default, and with `noeviction` a
  Redis that is full refuses every write of every Stream. Set `producer_idle_ttl`, so that an
  open Stream that nobody writes to expires. Close a Stream that is done, so that its Retention
  starts. Watch the memory of a Stream with many writers ("What a Stream costs" below).
- **Persistence.** A Redis with no AOF and no RDB file comes back from a restart with no Stream.
  Every call then fails with `NOT_FOUND`, which no caller tries again. With `appendonly yes` a
  restart loses at most what `appendfsync` allows, which is a second of writes by default.
- **Clocks in sync.** Expiry and the ids of the Records use the `TIME` of the Redis that runs
  the script. After a failover, a new primary whose clock is behind can give a Record an id at
  or below an offset that a reader already has, and one whose clock is ahead by more than a TTL
  expires Streams early.
- **`maxclients`** above the connections of all Stores ("Connections" below).
- **TLS.** A `rediss://` URL connects with TLS and verifies the server against the root
  certificates of the system. A private CA works only through the `SSL_CERT_FILE` variable of
  the process. The Store can't send a client certificate.

## Keys

A Stream has four keys. `<prefix>` is the `key_prefix` of the Store. `<id>` is the Stream id with
`%` written as `%25` and `}` as `%7D`, so the hash tag ends where the id ends.

| Key | Type | Holds |
|---|---|---|
| `<prefix>:{<id>}:meta` | hash | The state of the Stream |
| `<prefix>:{<id>}:log` | stream | The Records, one entry each |
| `<prefix>:{<id>}:producers` | hash | One field for each Producer that wrote to the Stream |
| `<prefix>:{<id>}:closed` | stream | One entry, which the close adds |

- The keys have one hash tag, so a Redis Cluster keeps them in one slot and one script can use
  them all.
- A key has no Temporal namespace, so a Stream id is unique in one Redis and key prefix. Give
  each namespace its own prefix, or its own Redis.

## The meta hash

Each value is text, except `rid`.

| Field | Value |
|---|---|
| `v` | The format version, `1` |
| `rid` | The request key of the create that made the Stream (16 bytes) |
| `ret` | The Retention of the Stream, in ms |
| `idle` | The Producer idle TTL of the Stream, in ms. It's absent when the Stream has none. |
| `exp` | When the Stream expires, in ms since 1970. An open Stream with no `idle` has none. |
| `closed` | `1` once the Stream is closed |
| `newest` | The id of the newest Record. It's absent while the Stream has no Record. |
| `lost` | The id of the newest Record that the log lost |

- A Stream keeps the `ret` and the `idle` of the Store that created it. No later call changes
  them, also not from a Store with other settings.
- A Store refuses every call of a Stream with a higher `v` than it knows, with
  `FORMAT_TOO_NEW`.
- The request key of a call is the first 16 bytes of a SHA-256. For a create it covers the
  `request_id`, and for an append the attempt key and the `request_id`. A call with no
  `request_id` gets 16 random bytes.

## The log

An append is one script call, so Redis writes a batch whole or not at all. Its Records are next
to each other, in the order of the request.

The script picks the id of each entry. The `<ms>` is the time of Redis, or the `<ms>` of `newest`
when the clock is not past it. The `<seq>` starts at 0, or after the `<seq>` of `newest` when the
`<ms>` is the same. So ids never go down, also when the clock steps back, and no Record ever
appears at or below an offset that a read returned.

An entry has two fields, in this order.

- The first entry of a batch has `p`, the encoded `Producer` of the batch, and then `r`.
- Each later entry of a batch has `b`, the `<seq>` of the batch's first entry, and then `r`. The
  entries of a batch have one `<ms>`, so that `<ms>` and `b` are the id of the entry with `p`.
- `r` is the encoded `Payload` of the Record's body.

An offset is the id of its Record in 16 bytes. The `<ms>` comes first and then the `<seq>`, each
as an unsigned 64-bit big-endian number. The offsets of a Stream sort bytewise in the order of
its Records.

The Store deletes no entry of a log and changes none, so a log has every Record of its Stream
or none of them.

## The Producers

A Producer is one writer of a Stream. Each run of a Workflow, each Activity and each client
object that appends is one.

The field of a Producer is its key, as `producer_key` of the `temporalio-streams` crate gives it.
Two Producers have one field exactly when their kind and their ids are equal. The scripts keep
the key as it is and don't parse it. It has these fields.

1. The proto field number of the Producer's kind (1 byte)
2. Each id of the kind, in the order of its proto fields. An id is its length as a big-endian
   number (4 bytes), and then its bytes.

The value of the field starts with the Producer's newest attempt key (40 bytes), which the fence
compares bytewise. Then come the batches of the Producer that the Store remembers, the newest
first. A batch has these big-endian fields.

1. The request key of the batch (16 bytes)
2. The `<ms>` of its Records (8 bytes)
3. The `<seq>` of its first Record (8 bytes)
4. The `<seq>` of its last Record (8 bytes)

- An append whose request key is among them is a resend. It writes nothing and gets the offsets
  of that batch, also after a later attempt wrote and after the close.
- The Store that appends cuts the list at its own `resend_window`, 16 by default. So the value
  of a Producer has 40 bytes and 40 more for each remembered batch.
- An append replaces the attempt key by a later one and never by an earlier one, with the fence
  on or off. A close doesn't change the field.
- The Store keeps the field of a Producer for as long as the Stream exists. No call removes
  one, so nothing bounds the number of fields of a Stream.
- The cluster client can send the bytes of one call a second time, after a dropped connection.
  The second send has the request key of the first, so it's a resend while the Store remembers
  that key. With `resend_window` other batches of the Producer between the two sends, the
  batch is written twice.

## What a Stream costs

Measured on Redis 8, with ids of usual lengths (a namespace, two UUIDs and a short Activity id).

- **A Producer** costs about 230 bytes with one remembered batch and about 900 bytes with the
  default window of 16. So 100,000 Producers of one Stream cost 23 to 90 MB, until the Stream
  expires.
- **A Record** with a body of 100 bytes costs about 220 bytes alone in its batch and about 120
  bytes in a batch of 100, since only the first entry of a batch has the Producer.
- **An append** writes the whole value of its Producer again, with its field. With the default
  window, an append of one Record of 100 bytes adds about 1,260 bytes to the AOF and to the
  replication stream, and an append of 100 such Records about 21,400.

## The close

A close stores no Record. It sets `closed` and adds one entry with the field `c` to `:closed`. A
read that waits blocks on `:log` and on `:closed` in one `XREAD`, so an append and a close both
wake it.

## The order of the checks

An append checks in this order.

1. The Stream doesn't exist or has expired.
2. The batch is a resend.
3. A later attempt of the Producer wrote (with the fence on).
4. The Stream is closed.

So a retry of a batch that was written gets its offsets, whatever happened to the Stream after
the first try.

A close checks in this order.

1. The Stream doesn't exist or has expired.
2. The Stream is closed already.
3. A later attempt of the Producer wrote (with the fence on).

A close of a closed Stream succeeds before the fence, because it changes nothing, and the fence
only stops a write.

## A read

- A read returns at most `max_records` Records, 1000 when the request sets none. It stops
  before the Record that takes the page over `max_bytes`, 4 MiB when the request sets none. A
  Record that alone is over the limit still comes, alone in its page. A Record counts as the
  page encodes it. The script stops near that limit too, so Redis doesn't send the rest of the
  Stream. It counts what the entries store, which is each body and the Producer of each batch.
- A Record has the `Producer` of its append, as the append gave it.
- The `last_offset` of a page is the offset of its last Record, or the `from_offset` of the
  request for an empty page. A read takes every `last_offset` that the Store returned as its
  `from_offset`.
- A `from_offset` above `newest` fails with `OFFSET_TOO_HIGH`, and one that isn't 16 bytes with
  `INVALID_ARGUMENT`.

## Expiry

- An open Stream with `idle` expires that long after its last append, or after its create. Each
  append sets `exp` again. An open Stream with no `idle` doesn't expire, which is the default.
- A closed Stream expires one Retention after its close.
- `:log`, `:producers` and `:closed` expire at `exp`. `:meta` stays 30 days longer. In that time
  every call of the Stream fails with `EXPIRED`, also a create. After it the Store has forgotten
  the Stream, so a call fails with `NOT_FOUND` and a create makes a new Stream. The Store can't
  tell a Stream whose `:meta` Redis removed from one that nobody created.
- A TTL of more than 2^52 ms counts as 2^52 ms, which is 142,000 years.

## A lost log

Redis can lose `:log` and keep `:meta`, for example with an eviction policy. Use
`maxmemory-policy noeviction`. The Store doesn't read the policy.

- A log has lost its Records when the entry `newest` is missing. A read from the start, or from
  an offset below `newest`, then fails with `EXPIRED`. No read skips a lost Record.
- The next append stores the id in `lost` and writes after it. A read from `lost` or above it
  works.
- A lost `:producers` or `:meta` is not detected. The Store then accepts an earlier attempt
  again, and it writes a resend a second time.

## A Stream that the Store can't read

A call fails with `STORE_CORRUPT` when a key of its Stream holds what no script of this Store
wrote. That is a key of another Redis type, a field that a script can't use, or an entry that
doesn't decode. A retry reads the same value, so no caller tries the call again. Each call that
reads the value fails until an operator repairs the keys of the Stream or deletes them. Other
Streams still work.

A wrong setup is `STORE_UNAVAILABLE`, which a caller does try again. That includes a wrong
password, a missing permission, a Redis before 7.0 and a URL of another service. The calls
succeed once the setup is right.

## A Redis Cluster that migrates a slot

Each script declares every key that it uses, so Redis runs it only on a node that has all of
them. While a slot migrates, a script whose keys exist only in part gets `TRYAGAIN`. These calls
fail with `STORE_UNAVAILABLE` until the migration of their slot ends, and they write nothing.

- A close, whose `:closed` key doesn't exist before it
- The first append of a Stream, whose `:log` and `:producers` keys don't exist before it
- A read of a Stream with no Record, whose `:log` key doesn't exist. That is every reader that
  waits for the first Record of a new Stream.

A Stream whose keys all exist is served while its slot migrates.

A read that waits blocks on the node of its Stream's slot, which it finds with `CLUSTER SLOTS`.
While the slot migrates, it polls every 100 ms.

## Connections

One Store has these connections, and `maxclients` of Redis must allow them for all Stores.

- One connection for the calls that don't block. On a cluster it's one to each primary.
- One for each read that waits at the same time, up to `max_blocking_connections` (256 by
  default) to each node.

A connection of a read stays open for the next read. The Store closes none while
it lives, so a Store keeps as many as its busiest moment needed.

A server, or a load balancer in front of it, can close a connection that is idle. The next call
on that connection fails with `STORE_UNAVAILABLE`, and the call after it connects again. The
Store can't know if Redis ran a write whose connection failed, so the Store itself never sends
a write a second time. A retry with the `request_id` of the write writes it once. Only the wait
of a read for a Record runs again by itself, on a new connection. The client of a Redis Cluster
connects again by itself, and it can send a call a second time ("The Producers" above).

## Replication

Redis replicates after it answers, so a failover can lose a write that a call got an answer
for. A read from the offset of a lost write fails with `OFFSET_TOO_HIGH` until the Stream has
Records up to it.
