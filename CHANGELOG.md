# Changelog

## Unreleased

### Added

- The `temporalio-streams-redis` crate, the Redis Store of the Temporal Stream runtime. It serves
  the Stream service of `temporalio-streams` on Redis, standalone or a cluster.

### Changed

- A Stream's Retention counts from its close, as a namespace's Retention does. An open Stream keeps
  all its Records until the safety TTL passes after its last write, and an append no longer drops a
  Record by its age. A Producer no longer needs `+xtrim`.
