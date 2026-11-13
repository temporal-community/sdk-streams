#!/usr/bin/env bash
# Patches `temporalio-streams` and `temporalio-common` to an sdk-rust git commit, for CI until
# `temporalio-streams` is in an sdk-rust release. `SDK_RUST_REV` picks the commit on the sdk-rust
# fork, so a build doesn't move when a branch does.
set -euo pipefail

rev=${SDK_RUST_REV:-42614ce4d9d4dfafe8c25afd34b46e6077efeebc}
mkdir -p .cargo
cat >.cargo/config.toml <<TOML
[patch.crates-io]
temporalio-common = { git = "https://github.com/moetemp/sdk-rust", rev = "$rev" }
temporalio-streams = { git = "https://github.com/moetemp/sdk-rust", rev = "$rev" }
TOML
