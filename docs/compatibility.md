# Compatibility and support policy

FraudTwin keeps generated artifacts reproducible, but not every interface has
the same stability promise. Pin the documentation version when a pipeline or
benchmark must be replayed exactly.

| Surface | Compatibility rule | Status |
| --- | --- | --- |
| Supported public API exports | Public names are reviewed for backward compatibility; removals are announced in the changelog. | Stable |
| Configuration YAML | Validate with the matching package version. Unknown fields fail validation instead of being ignored. | Stable |
| Parquet/JSON/Avro contracts | Schema versions and manifest fingerprints identify the producer and contract. Read with the corresponding contract version. | Stable |
| Public benchmark packs | Packs declare generator compatibility ranges and frozen fingerprints. A mismatch is an error, not a warning. | Versioned |
| `fraudtwin.postgres`, `kafka`, `lakehouse`, `observability` | Optional integrations require their matching extra and external service versions. | Optional |
| Calibration, campaign dynamics, learned generation | APIs may evolve between minor releases; persist profile/configuration fingerprints with results. | Experimental |
| `fraudtwin.ml.drift` reports | `M29-drift-1`; persist the report version, window fingerprints, and label policy with every alert. | Experimental |
| `fraudtwin.kafka_chaos` manifests | `M29-kafka-chaos-1`; replay with the same seed and configuration before comparing fault rates. | Experimental |
| Model-service HTTP schemas | Request/response version `1`; keep model ID, feature version, and artifact fingerprint together. | Reference example |

## Upgrade checklist

1. Install the target version in an isolated environment.
2. Run `fraudtwin config validate` against each configuration.
3. Generate a bounded 1,000-payment fixture and compare manifest, schema, and
   logical fingerprints.
4. Re-run representative ML, graph, and integration checks before promoting the
   new version.

See [migration notes](migration.md) and the [release notes](../CHANGELOG.md)
for version-specific changes.
