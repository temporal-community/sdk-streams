# Changelog

## Unreleased

### Added

- The `temporalio-streams-log` and `temporalio-streams-memory` crates. The first has the `LogStore`
  trait of a Store that keeps each Stream as a log, the code that turns it into a `StreamStore` of
  `temporalio-streams`, and the shared test suites. It also has `dispatch::call`, the byte form of
  the five Stream operations, which was `StreamRuntime::call` in `temporalio-streams`. The second
  is a Store that keeps its Streams in memory, for tests. A read from the start of a closed Stream
  that lost its log fails with `stream_expired`, and an offset that no Store gave is refused.
- The `temporalio-streams-redis` crate, the Redis Store of the Temporal Stream runtime. It answers
  the five operations of the Stream service of `temporalio-streams` on Redis, standalone or a
  cluster. A lang bridge builds `RedisStreamRuntime` from an encoded `RedisStreamConfig` and calls
  `dispatch::call` of `temporalio-streams-log` with the runtime, an operation name and the encoded
  request. The config gives the Store's name in `store_name`, and the runtime sets that name on
  each ref it builds.
- A Stream's Retention counts from its close. An open Stream keeps all its Records until its
  Producer idle TTL passes after the last write of any Producer. With no `producer_idle_ttl`, an
  open Stream stays until its close.
- A Producer id is opaque. The Store takes any non-empty id of at most 4096 bytes, and it keeps the
  dedupe and fence state of each Producer for as long as its Stream exists.
- A read gives only Records, and a close shows as `done`. A read of a Stream that nobody created
  fails at once with `stream_not_found`.
- The batch limits of a Store as options. `RedisStreamConfig` has `max_record_bytes`,
  `max_batch_bytes` and `max_batch_records`, and `MemoryStreamOptions` has `limits`. A Store can
  allow less than the default, and never more. The runtime refuses a larger batch with
  `stream_too_large`.
