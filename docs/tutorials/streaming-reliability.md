# Streaming and Kafka reliability

Validate contracts, publish observable events, and test delivery semantics with
repeatable logical faults before relying on a broker deployment.

| Time | Extras | Output |
| --- | --- | --- |
| 30–45 min | `kafka`; Docker optional | topics, encoded records, chaos counts |
| 30–45 min | base; Kafka optional | outage/recovery audit |
| 20–30 min | base; Schema Registry optional | compatibility report |

The Kafka notebooks include `!pip install confluent-kafka` and optional broker
and Schema Registry checks. They continue with contract-backed local records
and logical chaos when Docker services are unavailable.

## Tutorials

- [Publish contracts and inspect Kafka delivery semantics](avro-kafka-stream.ipynb)
- [Recover from Kafka outages and duplicate delivery](kafka-outage-recovery.ipynb)
- [Test Avro compatibility and schema evolution](schema-evolution-compatibility.ipynb)

**Related:** [Kafka reliability](../kafka-reliability.md).

**Next path:** [Operations and incident response](operations.md).
