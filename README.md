# Distributed Systems Failure Simulator (DSFS)

Deterministic distributed-systems simulator with fault injection,
Raft consensus and consistency checking.

See `docs/` for architecture decisions.

## Status

Stage 1: project foundation, virtual clock, event scheduler.

## Build & test

    cargo build --workspace
    cargo test --workspace
    cargo clippy --workspace --all-targets -- -D warnings
    cargo fmt --all --check

## License

MIT OR Apache-2.0.