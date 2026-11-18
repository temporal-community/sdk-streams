# Changelog

## Unreleased

### Added

- The `temporalio-streams-log` and `temporalio-streams-memory` crates. The first has the `LogStore`
  trait of a Store that keeps each Stream as a log, the code that turns it into a `StreamStore` of
  `temporalio-streams`, and the shared test suites. The second is a Store that keeps its Streams in
  memory, for tests.
- The `temporalio-streams-redis` crate, the Redis Store of the Temporal Stream runtime. It answers
  the five operations of the Stream service of `temporalio-streams` on Redis, standalone or a
  cluster. A lang bridge builds `RedisStreamRuntime` from an encoded `RedisStreamConfig` and calls
  `StreamRuntime::call` with an operation name and the encoded request. The config names the Store
  with `store_name`, and the runtime sets that name on each ref it builds.
- A Stream's Retention counts from its close. An open Stream keeps all its Records until its
  Producer idle TTL passes after the last write of any Producer. With no `producer_idle_ttl`, an
  open Stream stays until its close.
- A Producer id is opaque. The Store takes any non-empty id of at most 4096 bytes, and it keeps the
  dedupe and fence state of each Producer for as long as its Stream exists.
- A read gives only Records, and a close shows as `done`. A read of a Stream that nobody created
  fails at once with `stream_not_found`.
