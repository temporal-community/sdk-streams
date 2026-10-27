#!/usr/bin/env bash
# Patches `temporalio-streams` and `temporalio-common` to an sdk-rust git ref, for CI until
# `temporalio-streams` is in an sdk-rust release. `SDK_RUST_REF` picks the branch of the review
# stack on the sdk-rust fork.
set -euo pipefail

ref=${SDK_RUST_REF:-moe/brook/review/streams-k10-wit}
mkdir -p .cargo
cat >.cargo/config.toml <<TOML
[patch.crates-io]
temporalio-common = { git = "https://github.com/moetemp/sdk-rust", branch = "$ref" }
temporalio-streams = { git = "https://github.com/moetemp/sdk-rust", branch = "$ref" }
TOML
