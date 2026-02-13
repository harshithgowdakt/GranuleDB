# GranuleDB

GranuleDB is a learning-focused, ClickHouse-inspired columnar database in Go, built to explore granule-based storage, MergeTree-style parts, vectorized execution, and aggregate state pipelines.

## Important Notice

- This project was developed with significant AI assistance.
- Tools used include **Codex** and **Claude Code**.
- **Do not use this project in production.**
- This project is intended for **learning, experimentation, and educational purposes only**.

## What It Is

GranuleDB is a learning-oriented database engine that explores:

- Columnar storage
- MergeTree-like table parts and background merges
- SQL parsing and execution pipelines
- Aggregate state functions and materialized-view style flows

## What It Is Not

GranuleDB is **not** a production-ready database system.
It does not provide the full correctness, durability, operational safety,
compatibility, and performance guarantees required for real production workloads.

## Quick Start

```bash
go run ./cmd/granuledb
```

By default, GranuleDB starts an HTTP server on `:8123`.

## License

Add your preferred license here.
