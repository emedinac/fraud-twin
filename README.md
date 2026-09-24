<p align="center">
  <img src="docs/_static/fraudtwin-mark.svg" alt="FraudTwin logo" width="120">
</p>

# FraudTwin

[![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-3776AB.svg?logo=python&logoColor=white)](https://docs.python.org/3/)
[![Documentation](https://img.shields.io/badge/docs-latest-0B7285.svg)](https://emedinac.github.io/fraud-twin/latest/)
[![CI](https://img.shields.io/badge/CI-GitHub%20Actions-2088FF?logo=github-actions&logoColor=white)](.github/workflows/ci.yml)
[![Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-green.svg)](LICENSE)
[![Parquet](https://img.shields.io/badge/output-Apache%20Parquet-50ABF1?logo=apacheparquet&logoColor=white)](https://parquet.apache.org/docs/)
[![Pydantic](https://img.shields.io/badge/config-Pydantic%202-E92063?logo=pydantic&logoColor=white)](https://docs.pydantic.dev/)

> A deterministic payment world for building, breaking, and validating fraud systems.

FraudTwin is a Python framework for generating realistic financial behavior and
using it to test fraud detection systems before production data or production
infrastructure is available. It models entities, payment lifecycles, fraud
campaigns, delayed labels, data-quality incidents, graphs, streaming events,
and operational storage as one coherent system.

## Why FraudTwin

Most synthetic-data tools generate independent rows. Most Kafka test fixtures
exercise transport without domain truth. FraudTwin connects the two: every
record has a stable identity, event time, causal context, and an auditable
relationship to the source simulation.

| If you need to… | FraudTwin gives you… |
| --- | --- |
| Test temporal feature engineering | Point-in-time datasets with source-availability cutoffs and label maturity. |
| Investigate realistic fraud | Coordinated F01–F05 scenarios, campaigns, camouflage, hard negatives, graph provenance, and oracle truth. |
| Reproduce a difficult incident | Seeded random streams, configuration hashes, manifests, and stable fingerprints. |
| Validate production assumptions | Logical Kafka loss/duplication/retry/delay, Avro compatibility, replay, quality faults, and late-event repair. |
| Start locally and scale later | The same domain model for in-memory runs, partitioned checkpoints, PostgreSQL, Kafka, Iceberg, Neo4j, and PyTorch Geometric. |

The important boundary is explicit: **observable data** contains what a detector
could know at a chosen time; **oracle data** contains the complete explanation
used for evaluation and audit. This makes leakage and label-delay mistakes
visible instead of silently rewarding them.

## Quick start

Requirements: Python 3.12+ and Poetry 2.x. See the [installation and support
matrix](docs/installation.md) for PyPI installs, optional extras, and resource
expectations.

```bash
git clone https://github.com/emedinac/fraud-twin.git
cd fraud-twin
poetry install

# Validate and generate the minimal local run.
poetry run fraudtwin config validate configs/minimal.yaml
poetry run fraudtwin generate configs/minimal.yaml --output-dir /tmp/fraudtwin-run
```

The command prints an immutable run ID and writes a manifest plus Parquet
tables. The baseline configuration creates a small payment world with fraud
disabled. Copy it, enable the fraud and workflow sections, and keep the same
seed while iterating on a scenario.

### Python API

The public API is typed and has two explicit workflows:

```python
from pathlib import Path

import fraudtwin
from fraudtwin.config import load_config

config = load_config(Path("configs/minimal.yaml"))

# In-memory: precise IDE autocomplete for entities, behavior, and datasets.
data = fraudtwin.generate(config)
dataset = data.require_dataset().frame
print(data.run_id, dataset.shape)

# Persisted: metadata and paths first; load records only when needed.
run = fraudtwin.generate(
    config,
    write=True,
    output_dir=Path("/tmp/fraudtwin-run"),
)
loaded = run.load_data()
print(run.run_id, len(loaded.behavior.payments), run.manifest_path)
```

Use `GeneratedData.require_dataset()` when the configuration enables a
point-in-time dataset. Use `GeneratedRun.load_data()` when a persisted run is
needed by a graph, replay, dataset, or publisher workflow. Both methods keep
the contract explicit and make class members discoverable to Pylance, mypy,
and other Python language servers.

## From simulation to system test

FraudTwin is intended to be used as a progression, not a single generator
call:

```text
configuration
    → entities and behavior
    → payment/lifecycle events
    → fraud campaigns and delayed labels
    → point-in-time datasets and graphs
    → models, backtests, and drift reports
    → Kafka/PostgreSQL/Iceberg/serving integration tests
```

Every stage can run offline. External services add realism but do not change
the source truth or block the core learning path.

## Learning paths

The documentation is organized around engineering tasks rather than a flat
notebook directory:

| Path | What you will build |
| --- | --- |
| [Visualization and exploration](docs/tutorials/visualization.md) | Temporal behavior, fraud scenarios, distributions, correlation, and embeddings. |
| [Getting started](docs/tutorials/getting-started.md) | A first run, configuration changes, payment lifecycles, and delayed labels. |
| [Core workflows](docs/tutorials/core-workflows.md) | Point-in-time data, fraud stress tests, and reproducible benchmarks. |
| [Production ML and reliability](docs/tutorials/production-ml.md) | Model training, serving, promotion, rollback, and segmented drift analysis. |
| [Graph analytics](docs/tutorials/graph-analytics.md) | Temporal graph exports, Neo4j investigations, and PyG features. |
| [Streaming and Kafka reliability](docs/tutorials/streaming-reliability.md) | Avro contracts, delivery faults, outages, duplicates, and event-time correctness. |
| [Operations and incident response](docs/tutorials/operations.md) | Checkpoint/resume, data repair, PostgreSQL reconciliation, and lakehouse observability. |

The [versioned documentation site](https://emedinac.github.io/fraud-twin/latest/)
contains rendered notebooks, guides, troubleshooting, compatibility notes, and
the [Python API reference](https://emedinac.github.io/fraud-twin/latest/api.html).
The API reference lists supported public classes and functions with signatures,
parameters, return types, exceptions, and source links.

For a complete workflow, see the [ML evaluation methodology](docs/ml-evaluation.md)
and the [integration runbooks](docs/integrations.md).

## What is modeled

- Customers, institutions, accounts, cards, merchants, devices, and payment habits.
- CARD, PIX-like, and account-transfer lifecycles with ledger invariants.
- Fraud scenarios, campaigns, camouflage, difficulty, hard negatives, alerts, cases, disputes, and delayed labels.
- Observable and oracle views with label-observation histories.
- Point-in-time datasets, replay, rolling backtests, baseline evaluation, calibration, counterfactuals, and drift reports.
- Graph nodes, edges, campaigns, evidence, Neo4j exports, and PyTorch Geometric conversion.
- Deterministic quality faults, schema evolution, Kafka chaos, partitioning, checkpoints, and reconciliation.
- A versioned extension SDK for custom fraud scenarios, payment rails, behavior models, fault injectors, and output sinks.

## Generated outputs

Each persisted run is self-describing:

```text
runs/<run-id>/
├── manifest.json
├── entities/*.parquet
├── behavior/ behavior_profiles.parquet
├── payments/ payments.parquet payment_events.parquet
├── ledger/ ledger_entries.parquet
├── fraud/ fraud_records.parquet fraud_alerts.parquet fraud_labels.parquet
└── ml/ dataset.parquet dataset_manifest.json
```

The manifest records the seed, validated configuration, schema versions, row
counts, fingerprints, and quality diagnostics. Graph, benchmark, replay,
counterfactual, and campaign workflows add their own manifests without
rewriting the original source records. No real personal data or payment
credentials are generated.

## Optional integrations

Install only what a workflow needs. The base install remains dependency-light.

| Extra | Use |
| --- | --- |
| `ml` | scikit-learn and gradient-boosting baselines. |
| `graph` | PyTorch and PyTorch Geometric conversion/model experiments. |
| `kafka` | Confluent Kafka and Schema Registry publication. |
| `postgres` | Transactional operational persistence and reconciliation. |
| `lakehouse` | Iceberg/PyArrow/Spark materialization and backfill. |
| `observability` | Prometheus metrics for lag, quality, and reconciliation. |
| `mlflow`, `serving` | Artifact tracking and the local FastAPI scoring reference service. |

```bash
poetry install -E ml -E graph
poetry install -E kafka -E postgres -E lakehouse -E observability
```

The optional Spark reference pipeline is documented in
[`docs/spark-streaming.md`](docs/spark-streaming.md) and lives under
`examples/spark-streaming/`. It consumes either the observable Kafka
`payment-event` contract or persisted PaymentEvent Parquet rows. Spark is not
required for local generation.

For laptop validation, use only `configs/scale-dev.yaml` (1,000 target
payments). Larger scale profiles are hardware-dependent benchmark targets and
are not validated by CI or documentation examples.

Docker-backed examples are documented separately and always include an
offline fallback. Logical Kafka chaos simulates message delivery semantics;
it does not claim to reproduce physical packet loss or broker failures.

## Design principles

- **Determinism first.** Named random streams isolate entities, behavior, payments, fraud, quality, graphs, and benchmarks.
- **Truth is not availability.** Oracle truth is never treated as an operational feature.
- **Invariants are executable.** Ledger balance, temporal cutoffs, identity, contracts, and reconciliation are validated in code.
- **Integrations stay at the boundary.** Kafka, databases, lakehouses, and serving adapters consume stable records rather than changing simulation semantics.
- **Evidence over anecdotes.** Every experiment can emit compact metrics, manifests, and fingerprints suitable for review or regression tests.

## Contributing

Bug reports, improvements, documentation updates, and new integration tests
are welcome. See [DEVELOPMENT.md](DEVELOPMENT.md) for the contributor setup,
quality gate, focused tests, and contribution workflow. Pull-request CI runs
the same checks automatically.

## License

FraudTwin is released under the [Apache License 2.0](LICENSE). See
[docs/references.md](docs/references.md) for the standards, repositories, and
research that informed the framework.
