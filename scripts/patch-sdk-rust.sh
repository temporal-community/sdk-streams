#!/usr/bin/env bash
# Patches `temporalio-streams` and `temporalio-common` to a copy of sdk-rust, until
# `temporalio-streams` is in an sdk-rust release. `SDK_RUST_PATH` takes a local checkout. Without
# it, `SDK_RUST_GIT` and `SDK_RUST_REF` pick a branch of a git repo.
set -euo pipefail

if [ -n "${SDK_RUST_PATH:-}" ]; then
  common="path = \"$SDK_RUST_PATH/crates/common\""
  streams="path = \"$SDK_RUST_PATH/crates/streams\""
else
  git=${SDK_RUST_GIT:-https://github.com/temporalio/sdk-rust}
  common="git = \"$git\", branch = \"${SDK_RUST_REF:-main}\""
  streams=$common
fi
mkdir -p .cargo
cat >.cargo/config.toml <<TOML
[patch.crates-io]
temporalio-common = { $common }
temporalio-streams = { $streams }
TOML
