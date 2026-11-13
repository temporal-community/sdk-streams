#!/usr/bin/env bash
# Patches `temporalio-streams` and `temporalio-common` to an sdk-rust git commit, for CI until
# `temporalio-streams` is in an sdk-rust release. `SDK_RUST_REV` picks the commit on the sdk-rust
# fork, so a build doesn't move when a branch does.
set -euo pipefail

rev=${SDK_RUST_REV:-5a5f856f4f0641a9322b900e8c650e556f7bdbc0}
mkdir -p .cargo
cat >.cargo/config.toml <<TOML
[patch.crates-io]
temporalio-common = { git = "https://github.com/moetemp/sdk-rust", rev = "$rev" }
temporalio-streams = { git = "https://github.com/moetemp/sdk-rust", rev = "$rev" }
TOML
