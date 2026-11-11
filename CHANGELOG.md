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
  `:producers` key. A Producer no longer needs `+zadd`, `+zrangebyscore` and `+zrem`.
- `safety_ttl` is `last_write_ttl`, with no default. With none, an open Stream stays until its
  close. A Producer needs `+persist`.
- The Store reads no newest-N page, so `read_back` and its script are gone, and a Reader no longer
  needs `+xrevrange`. `Store::read` and `Store::wait_after` take a `StreamRef`.
- The keys hold no Temporal namespace, so a Stream id is unique in one Redis and key prefix, and an
  ACL key pattern gives a user one prefix. The Store calls take no namespace.
