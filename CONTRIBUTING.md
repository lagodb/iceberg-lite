# Contributing

`iceberg-lite` is maintained as a standalone library. Changes should not add
dependencies on the parent `lagodb` repository or on PostgreSQL-specific
runtime components.

Use the pinned toolchain from `rust-toolchain.toml` and run the complete local
check set before opening a pull request:

```text
cargo fmt --check
cargo check --all-targets --locked
cargo test --all-targets --locked
cargo test --doc --locked
cargo clippy --all-targets --locked -- -D warnings
cargo package --locked --list
```

Keep the synchronous execution model and custom storage abstractions intact.
When porting changes from
[iceberg-rust](https://github.com/apache/iceberg-rust), retain the upstream
context in the pull request description where it helps future maintenance.
