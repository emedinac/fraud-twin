# Streaming and Kafka reliability

Validate contracts, publish observable events, and test delivery semantics with
repeatable logical faults before relying on a broker deployment.

| Tutorials | Time | Extras | Output |
| --- | --- | --- | --- |
| 12 | 30–45 min | `kafka`; Docker optional | topics, encoded records, chaos counts |
| 20 | 30–45 min | base; Kafka optional | outage/recovery audit |
| 21 | 20–30 min | base; Schema Registry optional | compatibility report |

```{toctree}
:maxdepth: 1

12-avro-kafka-stream.ipynb
20-kafka-outage-recovery.ipynb
21-schema-evolution-compatibility.ipynb
```

**Related:** [Kafka reliability](../kafka-reliability.md).

**Next path:** [Operations and incident response](operations.md).
