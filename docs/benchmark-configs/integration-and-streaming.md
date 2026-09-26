# Integration and streaming

Use this configuration when publishing to a broker with contract-backed
output. Start the broker and schema registry before running the generation
command.

| Configuration | Capability | Scenario and target | Rails | Output or analysis focus |
| --- | --- | --- | --- | --- |
| `minimal-f01-kafka-only.yaml` | Kafka-only contract-backed output | F01 stream · 50% target | CARD | Broker delivery, schema contracts, and stream verification |

## Configuration details

Configuration: [examples/configuration/minimal-f01-kafka-only.yaml](../../examples/configuration/minimal-f01-kafka-only.yaml). Publishes a small F01 stream so you can verify broker delivery and contract-backed output.

````{dropdown} Kafka-only output

```{literalinclude} ../../examples/configuration/minimal-f01-kafka-only.yaml
:language: yaml
```
````

## Next

Continue with [Integration runbooks](../integrations.md).

## Related

- [Kafka reliability](../kafka-reliability.md)
- [Benchmark catalog](../benchmark-configs.md)
