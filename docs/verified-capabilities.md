# Verified capabilities

**Level:** Expert<br><br>
**You will:** check whether a capability has a runnable command, test, artifact,<br><br>
and evidence boundary before relying on it.
**Before you start:** [Architecture](architecture.md).<br><br>
**Services:** None.<br><br>

This page is the release contract. A capability is **verified** only when a
bounded test, runnable command, and inspectable artifact exist together.

| Capability | Runnable surface | Evidence |
| --- | --- | --- |
| Deterministic local generation | `fraudtwin generate configs/minimal.yaml` | Run manifest, Parquet tables, fingerprints, ledger checks |
| Point-in-time ML data | `fraudtwin ml build-dataset` | Dataset manifest and leakage tests |
| Fraud stress benchmarks | `fraudtwin benchmark run FT-B04-CAMOUFLAGE@0.34.0` | Pack manifest, descriptors, model metrics |
| Kafka contracts/publication | `fraudtwin schema validate` and streaming Compose profile | Avro fingerprints, topic publication, chaos report |
| PostgreSQL operational mirror | `fraudtwin db migrate` plus clean generation | Relational reconciliation and idempotent run checks |
| Iceberg materialization | `fraudtwin lakehouse ingest-run` | Bronze/Silver/Gold manifest and snapshots |
| Bounded scale development path | `configs/scale-dev.yaml` and `fraudtwin scale-benchmark` | Checkpoint, partition fingerprints, stage timings, and laptop evidence; core payments use the bounded streaming producer while advanced stages remain compatibility-materialized |
| Spark reference integration | `examples/spark-streaming/spark_streaming.py` | Silver/Gold/late projections and Spark run report |
| Extension SDK | `fraudtwin.extensions` | Protocol tests and manifest provenance |

## Scale claim boundary

The current release verifies deterministic chunking, checkpoint integrity,
resume behavior, worker-count invariance, and bounded development fixtures. The
public `iter_scale_records` and partition reader APIs are the preparation for
the out-of-core producer work, and the core scale feature set now writes
payments, lifecycle events, and ledger entries through bounded batches while
preserving the compatibility row contract. Entity/profile materialization and
advanced fraud, graph, quality, and PIT stages remain bounded development
paths. It does **not** publish a 100M/1B throughput, memory, or production-scale
claim. Those claims require the remaining out-of-core stages and hardware
evidence outside the laptop validation profile.

## Evidence rule

Every benchmark result must include the resolved configuration hash, seed,
generator version, Git revision, hardware summary, package versions, output
fingerprints, and the exact claim scope. Results from different configurations
or hardware must not be compared as if they were one benchmark.

## Next

Use [Release evidence](release-evidence.md) to reproduce the bounded claims, or
choose a route from [Learning paths](learning-paths.md).

## Related

- [Architecture](architecture.md)
- [Compatibility](compatibility.md)
- [Troubleshooting](troubleshooting.md)
