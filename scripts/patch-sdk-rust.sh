#!/usr/bin/env bash
# The unreleased Streams crate needs a local `SDK_RUST_PATH` or a Git source.
# `SDK_RUST_GIT` and `SDK_RUST_REF` select that source.
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
