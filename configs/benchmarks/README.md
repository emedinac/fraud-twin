# Benchmarks and configuration examples

This directory is the canonical index of small, reproducible FraudTwin benchmark configs. Use these as starting points for experiments, scenario comparisons, and integration checks. They are intentionally small enough to inspect and modify quickly while still exercising the relevant feature area.

## Categories

### Minimal and baseline

These are the easiest configurations to validate and adapt when you are learning the simulator or checking a local installation.

- `minimal.yaml` — repository default baseline with a clean source world and local Parquet output.
- `benchmarks/calibration-v1.yaml` — calibration baseline for a small deterministic reference workflow.
- `benchmarks/temporal-v1.yaml` — a compact temporal benchmark profile with train/validation/test/stress windows.

### Fraud scenarios and campaign mixes

Use these when you want to exercise a specific fraud class or a small scenario mix without turning on full benchmark complexity.

- `benchmarks/campaign-dynamics-v1.yaml` — graph campaign dynamic transitions and scenario evolution.
- `benchmarks/counterfactual-v1.yaml` — minimum-change counterfactual generation requests.
- `benchmarks/camouflage-v1.yaml` — feature and relation camouflage stress for realistic-looking fraud.
- `benchmarks/difficulty-v1.yaml` — resolved difficulty benchmark sweep for normal-to-subtle fraud.
- `examples/configuration/card-only-f02-35pct.yaml` — pure-card F02 case at 35% fraud.
- `examples/configuration/card-dispute-burst-f03-45pct.yaml` — high-intensity F03 dispute burst at 45% fraud.
- `examples/configuration/mixed-card-transfer-f01-f03-40pct.yaml` — mixed card and transfer scenario at 40% fraud.
- `examples/configuration/transfer-only-f05-30pct.yaml` — transfer-only F05 example at 30% fraud.
- `examples/configuration/counterfactual-f05-30pct.yaml` — F05 campaign with counterfactual sidecars at 30% fraud.
- `examples/configuration/campaign-dynamics-f03-40pct.yaml` — campaign dynamics plus mule graph behavior at 40% fraud.
- `examples/configuration/label-lifecycle-f02-35pct.yaml` — label lifecycle and investigation workflow at 35% fraud.
- `examples/configuration/temporal-surge-f05-45pct.yaml` — temporal surge regime with elevated fraud in the second half of the run.
- `examples/configuration/mule-ring-graph-f01-f03-50pct.yaml` — mixed F01/F03 ring and mule graph pattern at 50% fraud.

### Graph and network structure

Use these for graph patterns, campaign structure, relationship overlays, and
observable/oracle exports.

- `benchmarks/graph-v2.yaml` — graph-pattern baseline with topology and export checks.
- `examples/configuration/graph-fanout-50pct.yaml` — high-fraud fan-out pattern.
- `examples/configuration/graph-cross-institution-fanout-10k.yaml` — cross-institution transfer fan-out.
- Advanced graph topologies, 10k/50% stress fixtures, and controlled experiment variants are cataloged in [Graph and relationship structures](../../docs/benchmark-configs/graph-and-relationship-structures.md).

### Integration and streaming

These are intended for local service-backed publication and contract validation.

- `examples/configuration/minimal-f01-kafka-only.yaml` — minimal F01 configuration with `outputs.kafka: true` and `dataset.enabled: false`.

## Quick usage

```bash
# Validate a baseline
poetry run fraudtwin config validate configs/minimal.yaml

# Generate a benchmark config
poetry run fraudtwin generate configs/benchmarks/temporal-v1.yaml --output-dir runs/temporal-demo

# Publish a minimal Kafka-only run once Kafka is provisioned
export FRAUDTWIN_KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export FRAUDTWIN_SCHEMA_REGISTRY_URL=http://localhost:8081
poetry run fraudtwin config validate examples/configuration/minimal-f01-kafka-only.yaml
poetry run fraudtwin generate examples/configuration/minimal-f01-kafka-only.yaml --output-dir runs/minimal-f01-kafka-only
```

## Rule of thumb

- Start with `configs/minimal.yaml` for a clean world.
- Move to a category-specific benchmark when you need one feature area in isolation.
- Copy a versioned benchmark into a project-owned file before changing it; treat the packaged files as examples, not working drafts.
- Keep Kafka and service-backed examples separate from offline benchmark files so local validation remains deterministic and easy to reason about.

See also: [docs/configuration.md](../../docs/configuration.md), [docs/graph-and-benchmarks.md](../../docs/graph-and-benchmarks.md), and [docs/configuration-reference.rst](../../docs/configuration-reference.rst).
