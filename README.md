# FraudTwin

[![Python](https://img.shields.io/badge/python-3.12%2B-3776AB.svg?logo=python&logoColor=white)](https://docs.python.org/3/)
[![Polars](https://img.shields.io/badge/Polars-1.x-CD792C?logo=polars&logoColor=white)](https://docs.pola.rs/)
[![Apache Parquet](https://img.shields.io/badge/data%20format-Apache%20Parquet-50ABF1?logo=apacheparquet&logoColor=white)](https://parquet.apache.org/docs/)
[![Pydantic](https://img.shields.io/badge/Pydantic-2.x-E92063?logo=pydantic&logoColor=white)](https://pydantic.dev/docs/)
[![Pandera](https://img.shields.io/badge/validation-Pandera-150458)](https://pandera.readthedocs.io/en/stable/)
[![Neo4j](https://img.shields.io/badge/graph%20export-Neo4j-4581C3?logo=neo4j&logoColor=white)](https://neo4j.com/docs/)
[![PyTorch Geometric](https://img.shields.io/badge/optional%20graph%20ML-PyTorch%20Geometric-EE4C2C?logo=pyg&logoColor=white)](https://pytorch-geometric.readthedocs.io/)
[![Typer](https://img.shields.io/badge/CLI-Typer-009688?logo=typer&logoColor=white)](https://typer.tiangolo.com/)
[![Poetry](https://img.shields.io/badge/Poetry-2.x-60A5FA?logo=poetry&logoColor=white)](https://python-poetry.org/docs/)
[![CI: GitHub Actions](https://img.shields.io/badge/CI-GitHub%20Actions-2088FF?logo=github-actions&logoColor=white)](.github/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-green.svg)](LICENSE)

> Synthetic financial behavior for testing fraud systems

FraudTwin generates a small, coherent payment world: customers have accounts, cards, merchants, devices, habits, and transaction histories. The same seed and configuration produce the same world, which makes a difficult fraud case reproducible instead of anecdotal.

## What it provides

- Behavior-aware customers and legitimate CARD, PIX-like, and account-transfer payments.
- Card and transfer lifecycles, double-entry ledger entries, and delayed workflow labels.
- F01–F05 fraud scenarios with hard negatives, alerts, cases, disputes, and oracle truth.
- Deterministic quality faults for duplicates, delays, outages, invalid values, and spikes.
- Point-in-time datasets, historical replay, rolling backtests, and versioned benchmark packs.
- Optional graph-fraud campaigns with observable/oracle views, Neo4j artifacts, and PyG export.
- Difficulty and camouflage controls for cases that remain valid but are harder to separate.
- Opt-in counterfactual fraud trajectories with minimum-change budgets and audit lineage.

FraudTwin is designed for fraud engineers, data scientists, ML engineers, and data teams who need realistic relationships and timing before introducing a larger streaming or production stack.

## Quick start

Requirements: Python 3.12 and Poetry 2.x.

```bash
poetry install
poetry run fraudtwin config validate configs/minimal.yaml
poetry run fraudtwin generate configs/minimal.yaml
```

The command prints the run ID and output location. To keep generated files out of the repository, set an output directory:

```bash
poetry run fraudtwin generate configs/minimal.yaml \
  --output-dir /tmp/fraudtwin-run
```

The minimal configuration creates 10 customers, 10 behavior profiles, and 100 target payments. Fraud is disabled in this baseline; enable it in a copied YAML file or start with a fixture under `configs/benchmarks/`.

## Documentation

| Need | Guide |
| --- | --- |
| Browse the documentation set | [Documentation hub](docs/README.md) |
| Install and generate a first run | [Quickstart](docs/quickstart.md) |
| Tune behavior, fraud, quality, and stress | [Configuration](docs/configuration.md) |
| Build datasets, replay runs, and backtest | [Data and evaluation workflows](docs/workflows.md) |
| Export graphs and use benchmark fixtures | [Graph and benchmark workflows](docs/graph-and-benchmarks.md) |
| Contribute and run the quality gate | [Development guide](docs/development.md) |
| Review release history | [CHANGELOG.md](CHANGELOG.md) |
| Understand external references | [References](docs/references.md) |

Tutorials will be added under `docs/` as the workflows settle. Each tutorial should start from a versioned configuration and show the resulting artifacts, not only the command that produced them.

## Generated output

Runs are written under `runs/<run_id>/` (or the directory supplied with `--output-dir`):

```text
runs/<run_id>/
├── manifest.json
├── entities/*.parquet
├── behavior/behavior_profiles.parquet
├── payments/{payments,payment_events}.parquet
├── ledger/ledger_entries.parquet
├── fraud/{fraud_records,fraud_alerts,fraud_cases,fraud_labels}.parquet
└── ml/
    ├── dataset.parquet
    └── dataset_manifest.json
└── counterfactuals/<counterfactual_id>/
    ├── observable/{original,modified}/
    ├── oracle/change_sets.parquet
    └── counterfactual_manifest.json
```

The manifest records the seed, configuration, schemas, counts, fingerprints, and quality diagnostics. Graph and benchmark runs may also contain `oracle/`, graph exports, and backtest folds. No real personal data or payment credentials are generated.

## Design principles

**Determinism first.** Named random streams keep entity, behavior, payment, lifecycle, fraud, quality, graph, and benchmark generation independent and replayable.

**Observed data stays separate from truth.** Operational outputs model what a detector could know at the time. Oracle artifacts preserve the complete causal explanation for evaluation and audit.

**Financial and temporal invariants are explicit.** Payment lifecycles, ledger entries, source availability, labels, graph relationships, and schemas are validated rather than inferred after the fact.

## Project status

| Capability | Status |
| --- | --- |
| Synthetic entities, behavior profiles, and payment lifecycles | Available |
| Fraud scenarios, workflow projections, and hard negatives | Available |
| Data-quality faults and diagnostics | Available |
| Point-in-time datasets, replay, and rolling backtests | Available |
| Graph campaigns and Neo4j/PyG exports | Available |
| Difficulty and camouflage benchmarks | Available |
| Kafka, PostgreSQL, Flink, feature stores, and advanced models | Planned |

## Development

Run the local quality gate with:

```bash
poetry check --strict
poetry run ruff check .
poetry run ruff format --check .
poetry run mypy src
poetry run pytest
poetry build
```

See [`DEVELOPMENT.md`](DEVELOPMENT.md) for the short contributor entry point and [`docs/development.md`](docs/development.md) for focused tests and working conventions.

## License and references

FraudTwin is released under the [Apache License 2.0](LICENSE). See [`docs/references.md`](docs/references.md) for the handbooks, repositories, standards, and research papers that informed the design and how each was used.
