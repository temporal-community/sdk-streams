# Redis Store operations and format

The scripts in `lua/` define the stored format. An entry id below means Redis `<ms>-<seq>`.

## Setup

- Use Redis 7.0 or later. Reads need `EVALSHA_RO` and script flags.
- Set `maxmemory-policy noeviction`. The Store does not read this setting.
  Eviction can remove Record, resend or fencing state.
- Set retention to fit available memory. It defaults to seven days after create or the last new batch.
  With `noeviction`, memory exhaustion rejects writes across all Streams.
- Configure persistence. Without AOF or RDB, restart removes all Streams.
  AOF recovery can lose writes within the configured `appendfsync` interval.
- Keep server clocks synchronized. Expiry and entry ids use Redis `TIME`.
  A primary with a faster clock can expire Streams early. Failover that loses metadata can reuse offsets.
- Set `maxclients` above the total connection count of all Stores. See [Connections](#connections).
- Use `rediss://` for TLS with server certificate verification.
  Set `SSL_CERT_FILE` for a private CA. Client certificates are not supported.
- Configure the access rules below. Credentials and TLS settings come from the Redis URL.

## Keys

Each Stream uses three keys in one Redis Cluster slot.
`<prefix>` is `key_prefix`. `<id>` is the Stream id with `%` escaped as `%25` and `}` as `%7D`.

| Key | Type | Content |
|---|---|---|
| `<prefix>:{<id>}:meta` | hash | Stream state |
| `<prefix>:{<id>}:log` | stream | One entry per Record |
| `<prefix>:{<id>}:producers` | hash | One field per Producer |

Keys contain no Temporal namespace. Use a separate prefix or Redis deployment for each namespace.

## Metadata

Values in `:meta` are text except `rid`.

| Field | Content |
|---|---|
| `v` | Format version, `1` |
| `rid` | Create request key, 16 bytes |
| `ret` | Retention in milliseconds |
| `exp` | Expiry time in milliseconds since 1970 |
| `newest` | Newest Record id, absent before the first append |
| `lost` | Newest Record id lost with the log |

The Stream retains its original `ret`, even when another Store uses different settings.
A higher `v` returns `FORMAT_TOO_NEW`.

A request key is the first 16 bytes of SHA-256. Create hashes the request id.
Append hashes the attempt key followed by the request id. An empty request id gets a random key.

## Log and offsets

One Lua script appends each batch without interleaving other commands. Records keep request order.
Redis does not roll back script errors. The Store checks permissions and known invalid state before writing.

The script assigns entry ids. It uses the greater of Redis time and the newest entry's `<ms>`.
When `<ms>` is unchanged, `<seq>` starts after the newest entry. Otherwise it starts at zero.
Ids increase while the metadata remains intact, even if the clock goes backward.

Each entry has two fields in this order.

- The first entry has `p`, the encoded `Producer`, followed by `r`, the encoded `Payload`.
- Later entries have `b`, the first entry's `<seq>`, followed by `r`, the encoded `Payload`.
  All entries in a batch share `<ms>`, so `b` identifies the batch's Producer entry.

An offset is `<ms>` followed by `<seq>`, each an unsigned 64-bit big-endian integer.
Unsigned byte order matches Record order. The Store never trims or changes entries.

## Producers and resends

Each Workflow run, Activity or client writer is a separate Producer.
The `producer_key` function in `temporalio-streams` encodes its kind and ids.
The Redis hash field contains that key unchanged.

1. The kind's proto field number, one byte.
2. Each id in proto field order. Each has a four-byte big-endian length followed by its bytes.
   Text uses UTF-8. A nonzero Activity scheduled event id uses eight big-endian bytes.
   Zero scheduled event ids are omitted.

The value starts with the newest attempt key, 28 bytes, compared as unsigned bytes.
Remembered batches follow, newest first. Each uses 40 bytes.

| Field | Bytes |
|---|---|
| Request key | 16 |
| Entry `<ms>` | 8 |
| First `<seq>` | 8 |
| Last `<seq>` | 8 |

An append checks expiry, then resend state, then fencing.
A remembered resend returns the original offsets even after a later attempt writes.
It adds no Records and does not extend retention.

The writing Store keeps its configured `resend_window`, which defaults to 16 batches per Producer.
A forgotten request can write again. This also applies when the Redis cluster client retries a lost response.
The stored attempt only increases, with fencing enabled or disabled.
Producer fields remain until Stream expiry. Their count is not bounded.

A create checks expiry before its request key and conflict policy.
The original request returns `created = true`. Another request with `USE_EXISTING` returns false.
Another request with `FAIL` returns `ALREADY_EXISTS`.

## Memory and write cost

Measurements on Redis 8 used a namespace, two UUIDs and a short Activity id.
Actual costs depend on ids, payloads and Redis configuration.

| Data | Approximate cost |
|---|---|
| Producer with one remembered batch | 230 bytes |
| Producer with the default resend window | 900 bytes |
| 100-byte Record alone in a batch | 220 bytes |
| 100-byte Record in a batch of 100 | 120 bytes |

An append rewrites its Producer value and updates key expiry.
With the default window, one 100-byte Record adds about 1,570 bytes to AOF and replication.
A batch of 100 such Records adds about 21,300 bytes.

## Reads and expiry

A read defaults to 1,000 Records and 4 MiB. Explicit limits stop the page before either is exceeded.
One oversized Record is returned alone so the reader can make progress.
The byte count includes each Record's encoded message envelope.
Lua limits bytes read from Redis. Rust then applies the exact encoded size limit.

Each Record retains its append Producer. A page's `last_offset` is its last Record's offset,
or the request's `from_offset` when empty. A malformed offset returns `INVALID_ARGUMENT`.
An offset above `newest` returns `OFFSET_TOO_HIGH`.

A waiting read ends at its deadline or Stream expiry. Expiry returns `EXPIRED`.
Redis supplies the remaining retention time so the client clock need not match it.

Create starts retention. Only an append that writes a new batch restarts it.
Reads, resends and creates of an existing Stream keep the expiry.
`:log` and `:producers` expire together. `:meta` remains for another 30 days.
During that period all calls, including create, return `EXPIRED`.
After metadata expires, reads return `NOT_FOUND` and create can reuse the id.
Durations above 2^52 milliseconds use that value, about 142,000 years.

## Lost or corrupt state

When `newest` is missing from `:log`, reads below that offset return `EXPIRED`.
The next append records the lost offset in `lost` and writes after it.
Reads at or above `lost` can continue.
This detects a lost log. It does not detect arbitrary entry deletion.

Loss of `:producers` or `:meta` is not detected. Stale attempts and duplicate batches can be accepted.
Use `noeviction` and configure persistence and replication for the required loss tolerance.

Malformed stored data returns `STORE_CORRUPT`. Retrying cannot repair the data.
Bad credentials, missing permissions and unsupported Redis commands return `STORE_UNAVAILABLE`.
Those calls can succeed after the operator corrects the setup.

## Access

Redis checks script access and each command inside the script.
Before create or a new batch, the Store checks write permissions for every affected key.
A rejected preflight leaves Records, resend state and expiry unchanged.

`tests/store/acl.rs` checks these exact command lists.

- A Producer creates and appends. Its user needs the keys `~<prefix>:*` and the commands
  `+evalsha +script|load +time +hmget +hget +hset +pexpireat +xadd +xrange`. With the `wait`
  option it also needs `+wait`.
- A Reader reads and describes. Its user needs the keys `%R~<prefix>:*` and the commands
  `+evalsha_ro +script|load +time +hmget +xrange`. A read that waits also needs `+xread`.
- On a Redis Cluster, each user also needs `+cluster|slots +asking` on every node.

A missing permission returns `STORE_UNAVAILABLE`.
The optional `WAIT` runs after the script. A denied or failed `WAIT` can leave the batch committed.
Retry with the original request id while it remains in the resend window.

Any user with write access can impersonate a Producer. Fencing is not access control.
Use separate credentials and key prefixes for applications that must not share Streams.

## Cluster migration

Every script declares all its keys. During migration, a script with only some keys present returns `TRYAGAIN`.
This includes a first append, and a read before the first Record exists.
The Store reports `STORE_UNAVAILABLE` until migration permits the call. The rejected script writes nothing.
A Stream whose keys all exist on one node remains available.

Waiting reads locate their primary through `CLUSTER SLOTS`. During migration they poll every 100 ms.
Reads use primaries. Replica reads, Sentinel and client TLS certificates are not supported.

## Connections

A Store retains idle connections for reuse. Plan `maxclients` for its highest concurrent load.

- Nonblocking calls share one connection, or one per primary in a cluster.
- Waiting reads use at most `max_blocking_connections` per node, 256 by default.
- Writes with `WAIT` use a separate pool with the same limit.
  Each cluster connection in that pool can connect to every primary.

Extra reads poll once per second. Extra writes wait for a connection until `response_timeout`.
A cancelled blocking call closes its connection so later calls cannot queue behind it.
A stale idle connection is retried only for a waiting read. Writes return an error for the caller to retry.
The Redis cluster client can retry writes itself, using the same request key.

## Replication

Without `wait`, Redis can acknowledge a write before its replicas receive it.
A failover can lose that write. Reads beyond the recovered newest offset return `OFFSET_TOO_HIGH`.
They can continue once new writes reach that offset.

With `wait`, create and append pipeline their script and `WAIT` on a dedicated connection.
Too few acknowledgements return `STORE_UNAVAILABLE` after the write.
A remembered resend writes `v` again so `WAIT` covers the original batch on this connection.

`WAIT` confirms replication, not disk persistence. It does not prevent every failover loss.
A reader can observe a Record before its append finishes `WAIT`.
