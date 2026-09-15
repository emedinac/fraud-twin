# Post-validation technical debt

These integrations are intentionally deferred until deterministic generation, ledger correctness, PIT safety, and benchmark evidence are proven. They are useful extensions, not prerequisites for the core simulator.

## Debezium (optional)

Capture changes made to the PostgreSQL operational mirror and publish them as CDC events. This helps test update/delete propagation and downstream CDC consumers; it is not needed for generated payment-event streams.

## Apache Spark (optional)

Run distributed event-time windows, deduplication, and feature aggregation over Kafka or Parquet/Iceberg partitions. This helps demonstrate distributed data processing when local Polars/DuckDB is no longer sufficient; it is not required for correctness or ordinary experiments.

## Feature store (optional)

Materialize and serve the same point-in-time-safe features used by training and online scoring. This helps validate training/serving consistency and feature lineage; the current PIT datasets are sufficient until an online serving use case exists.

## GCP deployment (optional reference)

Provide a reproducible Cloud Run/Cloud Storage/managed database deployment for team or portfolio demonstrations. This helps verify operational packaging, security, and cloud cost boundaries; it should not become a core dependency.

## Implementation gate
 
Only implement an item after a concrete need is demonstrated and the core acceptance evidence is archived. Each integration should remain an adapter or example, preserve deterministic source artifacts, and have one small smoke test plus documented cleanup and cost limits.
