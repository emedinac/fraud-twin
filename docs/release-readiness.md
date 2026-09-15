# FraudTwin v2.0.0 — Release Readiness & Integration Roadmap

This document is the v2.0.0 release-readiness record. It combines the M18 large-scale work remaining before a bounded-memory claim with the post-core platform integration track. The current package already provides partitioned Parquet output, durable checkpoints, deterministic validation, and the stable in-memory compatibility API for small runs.

## M18 scale boundary

The following capabilities remain outside the v2.0.0 scale claim until their acceptance evidence is available:

- `generate_scale()` still obtains entity and behavior populations through the compatibility generator before partitioning.
- Fraud, workflow, graph, campaign, label, and quality stages still depend on complete in-memory datasets.
- PIT dataset construction and backtests still materialize feature rows.
- PostgreSQL, Kafka, and Iceberg scale orchestration has bounded adapter APIs, but the production path is not yet fully chunk-native.
- The declared DuckDB state design is represented by a dependency-free SQLite compatibility store for resumable duplicate and ledger checks.
- Object-store publication exists through fsspec, while remote checkpoint discovery and end-to-end resume remain release work.

### M18 implementation track

1. Introduce chunk-native entity and profile producers with deterministic ID formulas and shard-local lookup tables.
2. Generate payments by global ordinal ranges and maintain account balances, relationships, transfer reconciliation, and stage progress in DuckDB.
3. Convert fraud, workflow, labels, and quality into two-pass streams over immutable plans and payment chunks.
4. Implement partition-aware graph, PIT, and backtest reducers with bounded windows and external sorting where required.
5. Connect Kafka, PostgreSQL, and Iceberg sinks directly to chunk iterators and checkpoint each successful batch.
6. Add atomic remote chunk/checkpoint publication and resume validation.

### M18 acceptance gates

- Peak RSS remains bounded from 100k through 1B target payments.
- Payment ordinals are exact, unique, and gap-free.
- Worker-count changes produce identical chunk and shard fingerprints.
- Interrupted and uninterrupted runs produce equivalent fingerprints.
- Per-account and cross-shard ledger reconciliation succeeds.
- Enabled feature-matrix stages expose out-of-core readers and sinks.
- A benchmark manifest is archived from hardware capable of 100M/1B runs.

Laptop and pull-request validation use the `dev` profile or synthetic row iterators only; they never create a massive local database.

## Platform integration roadmap

These components are not prerequisites for deterministic simulation, ledger correctness, PIT safety, or benchmark reproducibility. They enter the release only when a concrete deployment need and a small reproducible acceptance test exist.

## Debezium — change-data-capture track

Capture updates made in the PostgreSQL operational mirror as CDC events. This validates update/delete propagation and consumer recovery across a realistic data boundary; it adds no value to the immutable generator path by itself.

## Apache Spark — distributed processing track

Run event-time windows, deduplication, and feature aggregation over Kafka or Parquet/Iceberg partitions. This becomes useful when Polars/DuckDB no longer fits the workload or a Spark consumer contract must be demonstrated.

## Feature store — serving consistency track

Serve the same point-in-time-safe features used during training for online scoring. This validates training/serving parity and feature lineage; the current PIT datasets are sufficient until online inference is a real use case.

## GCP — deployment reference track

Provide a reproducible Cloud Run, object-storage, and managed-database deployment. This demonstrates packaging, security, operations, and cost boundaries without coupling the simulator to a cloud provider.

## Entry criteria

Each track requires a documented use case, a bounded smoke test, deterministic source-artifact preservation, cleanup instructions, and an explicit cost or resource limit. Until then, the integration remains roadmap work rather than a core runtime dependency.
