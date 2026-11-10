# Changelog

## Unreleased

### Added

- The `temporalio-streams-redis` crate, the Redis Store of the Temporal Stream runtime. It serves
  the Stream service of `temporalio-streams` on Redis, standalone or a cluster.

### Changed

- A Stream's Retention counts from its close, as a namespace's Retention does. An open Stream keeps
  all its Records until the safety TTL passes after its last write, and an append no longer drops a
  Record by its age. A Producer no longer needs `+xtrim`.
- A Producer's dedupe and fence state stays for as long as its Stream exists. An append no longer
  drops the state of a Producer with no append for one Retention, and the Store no longer writes the
  `:producers` key. A Producer no longer needs `+hdel`, `+zadd`, `+zrangebyscore` and `+zrem`.
