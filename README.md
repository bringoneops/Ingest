# Ingest

A minimal cryptocurrency ingestion engine prototype written in Rust.

## Crates
- `core`: shared types, configs, canonicalization utilities.
- `agents`: venue adapters and adapter SDK. Includes an example Binance adapter.
- `pipeline`: normalizer that validates and canonicalizes raw events.
- `api`: in-process consumer API built on a lock-free queue.
- `ops`: HTTP server providing health, readiness and Prometheus metrics.
- `devtools`: CLI utilities for scaffolding adapters and replaying golden data.

## Example

Run tests to verify the workspace builds:

```bash
cargo test
```

Replay a golden pack:

```bash
cargo run -p devtools -- replay golden/binance_spot_trades.jsonl
```

Scaffold from an adapter spec:

```bash
cargo run -p devtools -- scaffold crates/agents/specs/binance_spot.toml
```

The ops server exposes health and metrics endpoints:

```bash
cargo test -p ops -- --ignored
```

Configuration example enabling BTCUSDT and ETHUSDT ingestion can be found in `config/example.toml`.
