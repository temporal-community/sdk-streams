//! The tests of the Redis Store. Each test starts the Redis servers that it needs and stops them
//! when it ends. `REDIS_SERVER` picks the binary, and `STREAMS_SEED` runs one seed of a generated
//! test.

mod faults;
mod reads;
mod rules;
mod support;
mod time;
