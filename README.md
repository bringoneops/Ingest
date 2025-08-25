# Ingest

A minimal cryptocurrency ingestion engine prototype written in Rust.

## Crates
- `core`: shared types, configs, canonicalization utilities.
- `agents`: venue adapters and adapter SDK. Includes an example Binance adapter.
- `pipeline`: normalizer that validates and canonicalizes raw events.
- `api`: in-process consumer API built on a lock-free queue.
- `ops`: HTTP server providing health, readiness, Prometheus metrics, and an SSE stream of normalized events.
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

The ops server exposes health, metrics and streaming endpoints:

```bash
cargo test -p ops -- --ignored
```

Normalized events are available via Server-Sent Events at `http://127.0.0.1:3000/events`.

Configuration example enabling BTCUSDT and ETHUSDT ingestion can be found in `config/example.toml`.

Symbol discovery relies on exchange REST APIs. If discovery fails (for example, due to blocked network access), the venue is skipped with a warning. To ingest anyway, supply a static `symbols` list in the config to bypass discovery.
