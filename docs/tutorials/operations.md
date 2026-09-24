# Operations and observability

Materialize a generated run into operational storage, verify snapshots, and
interpret lag, duplicates, invalid records, late events, and reconciliation
signals. Docker services are required for the full lakehouse workflow.

| Time | Extras | Output |
| --- | --- | --- |
| 45–75 min | `lakehouse`, `scale` optional | quality audit and checkpoint manifest |
| 45–60 min | `postgres` optional | repair report and reconciliation |
| 30–45 min | `lakehouse`, `observability` optional | snapshot and SLO evidence |

The PostgreSQL and lakehouse notebooks include their client installation cells,
local startup commands, bounded health checks, and cleanup guidance. Offline
Bronze/Silver/Gold projections and reconciliation manifests remain complete
without Docker.

## Tutorials

- [Audit quality faults and build observable projections](operational-lakehouse-observability.ipynb)
- [Materialize a lakehouse snapshot and verify it](lakehouse-observability.ipynb)
- [Resume a scale run from a checkpoint](scale-checkpoint-resume.ipynb)
- [Repair damaged data and replay a bounded interval](data-quality-repair-replay.ipynb)
- [Persist PostgreSQL rows idempotently](postgres-persistence-reconciliation.ipynb)
- [Verify Iceberg time travel and observability signals](iceberg-time-travel-observability.ipynb)

**Related guides:** [Kafka reliability](../kafka-reliability.md), [data-quality
incidents](../data-quality-incidents.md), and [production serving](../production-serving.md).

**Next path:** [Advanced experiments](advanced-experiments.md).
