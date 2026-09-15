# M18 Scale TODO

This document tracks the remaining work required before M18 can claim bounded-memory 100M–1B generation. The current implementation provides partitioned Parquet, durable chunk checkpoints, checksum validation, disk-backed duplicate/account reconciliation, and explicit streaming adapter APIs. `generate()` and the compatibility sinks remain intentionally in-memory for small-run parity.

## Unsupported or incomplete

- `generate_scale()` currently delegates to the compatibility generator, so entity and behavior populations are still materialized before partitioning.
- Fraud, workflow, graph, campaign, label, and quality stages still depend on complete `EntityDataset`/`BehaviorDataset` objects.
- PIT dataset construction and backtests still materialize feature rows.
- Existing PostgreSQL, Kafka, and Iceberg orchestration paths still use the compatibility objects; their bounded iterator methods must be wired into scale execution.
- The configured DuckDB state backend is not yet the default writer state store; the current implementation uses dependency-free SQLite files for resumable duplicate and ledger checks.
- fsspec publication is available as an adapter, but end-to-end object-store checkpointing and remote chunk discovery are not yet integrated.

## Implementation strategy

1. Add chunk-native entity and profile producers using deterministic ID formulas and shard-local lookup tables; never build population tuples.
2. Generate payments by global ordinal ranges and use DuckDB state tables for account balances, relationship lookups, transfer reconciliation, and idempotent stage progress.
3. Convert fraud/workflow/label/quality stages to two-pass streams: first emit immutable plans/state, then consume payment chunks without global lists.
4. Build graph, PIT, and backtest jobs as partition-aware reducers over Parquet/DuckDB relations, with bounded windows and external sorting where ordering is required.
5. Wire Kafka, PostgreSQL, and Iceberg adapters to consume chunk iterators and publish stage checkpoints after each successful batch.
6. Replace the SQLite compatibility state store with DuckDB tables (retaining a small-run fallback only when explicitly selected).
7. Add remote fsspec storage for atomic chunk/checkpoint publication and resume validation without downloading a complete run.

## Completion gates

- Peak RSS remains bounded as target payments increase from 100k to 1B.
- Payment ordinal ranges are exact, duplicate-free, and gap-free.
- Worker-count changes produce identical chunk and shard fingerprints.
- Interrupted runs process only incomplete chunks and match uninterrupted output fingerprints.
- Per-account and cross-shard ledger reconciliation succeeds.
- Declared feature-matrix stages have out-of-core readers and sinks.
- A benchmark manifest exists for 100M/1B-capable hardware.

Laptop and pull-request tests must use the `dev` profile or synthetic row iterators; they must never create a massive database.
