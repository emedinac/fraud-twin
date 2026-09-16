# Operations and observability

Materialize a generated run into operational storage, verify snapshots, and
interpret lag, duplicates, invalid records, late events, and reconciliation
signals. Docker services are required for the full lakehouse workflow.

| Tutorials | Time | Extras | Output |
| --- | --- | --- | --- |
| 13–15 | 45–75 min | `lakehouse`, `scale` optional | quality audit and checkpoint manifest |
| 22–23 | 45–60 min | `postgres` optional | repair report and reconciliation |
| 24 | 30–45 min | `lakehouse`, `observability` optional | snapshot and SLO evidence |

```{toctree}
:maxdepth: 1

13-operational-lakehouse-observability.ipynb
14-lakehouse-observability.ipynb
15-scale-checkpoint-resume.ipynb
22-data-quality-repair-replay.ipynb
23-postgres-persistence-reconciliation.ipynb
24-iceberg-time-travel-observability.ipynb
```

**Related guides:** [Kafka reliability](../kafka-reliability.md), [data-quality
incidents](../data-quality-incidents.md), and [production serving](../production-serving.md).
