UV ?= uv

.PHONY: format lint-code

format:
	cargo fmt --all
	cd python && $(UV) run --no-sync ruff format .

lint-code:
	cargo fmt --all -- --check
	cargo clippy --workspace --tests -- -D warnings
	cargo clippy --workspace --no-default-features --features tls-aws-lc --tests -- -D warnings
	cd python && $(UV) run --no-sync ruff check .
	cd python && $(UV) run --no-sync ruff format --check .
	cd python && $(UV) run --no-sync pyright
