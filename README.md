# sdk-streams

Experimental.

`temporalio-streams-redis` is the Redis Store of the Temporal Stream runtime. The runtime itself,
`temporalio-streams`, is in [sdk-rust](https://github.com/temporalio/sdk-rust).

## Building

- `temporalio-streams` isn't on crates.io yet, so run `scripts/patch-sdk-rust.sh` before you build.
  `SDK_RUST_PATH` points it at a local checkout of sdk-rust.
- A bridge that builds sdk-rust from its `sdk-core` submodule patches `temporalio-common` and
  `temporalio-streams` to those copies in `[patch.crates-io]`, so that each type exists once.
- A FIPS build uses `default-features = false` and the `fips` feature. The default feature links
  `ring`.

## License

MIT. See `LICENSE.txt`.
