# Streaming and Kafka reliability

Validate contracts, publish observable events, and test delivery semantics with
repeatable logical faults before relying on a broker deployment.

| IDs | Time | Extras | Output |
| --- | --- | --- | --- |
| 12 | 30–45 min | `kafka`; Docker optional | topics, encoded records, chaos counts |
| 20 | 30–45 min | base; Kafka optional | outage/recovery audit |
| 21 | 20–30 min | base; Schema Registry optional | compatibility report |

The Kafka notebooks include `!pip install confluent-kafka` and optional broker
and Schema Registry checks. They continue with contract-backed local records
and logical chaos when Docker services are unavailable.

```{toctree}
:maxdepth: 1

avro-kafka-stream.ipynb
kafka-outage-recovery.ipynb
schema-evolution-compatibility.ipynb
```

**Related:** [Kafka reliability](../kafka-reliability.md).

**Next path:** [Operations and incident response](operations.md).
