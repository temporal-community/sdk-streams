#!/usr/bin/env bash
# Patches `temporalio-streams` and `temporalio-common` to an sdk-rust git commit, for CI until
# `temporalio-streams` is in an sdk-rust release. `SDK_RUST_REV` picks the commit on the sdk-rust
# fork, so a build doesn't move when a branch does.
set -euo pipefail

rev=${SDK_RUST_REV:-b1228145da03e9198d5836f5a47d60a1bc11444e}
mkdir -p .cargo
cat >.cargo/config.toml <<TOML
[patch.crates-io]
temporalio-common = { git = "https://github.com/moetemp/sdk-rust", rev = "$rev" }
temporalio-streams = { git = "https://github.com/moetemp/sdk-rust", rev = "$rev" }
TOML
