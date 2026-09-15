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

FraudTwin generates evolving adversarial financial environments in which fraud actors coordinate, camouflage their behavior, change strategies over time, and may remain undiscovered for weeks or indefinitely.

Each environment is a small, coherent payment world: customers have accounts, cards, merchants, devices, habits, and transaction histories. The same seed and configuration produce the same world, which makes a difficult fraud case reproducible instead of anecdotal.

## Documentation

Build realistic payment worlds, trace every fraud signal, and create reproducible ML datasets with confidence.

<p align="center">
  <a href="https://emedinac.github.io/fraud-twin/">
    <strong>📚 Explore the FraudTwin documentation →</strong>
  </a>
</p>

<p align="center">
  <a href="https://emedinac.github.io/fraud-twin/api.html">Python API reference</a>
  ·
  <a href="docs/README.md">Documentation sources</a>
</p>

Start with a guided tutorial, then move from configuration to fraud scenarios, graph exports, and point-in-time ML workflows—all backed by deterministic
examples.

## What it provides

- Behavior-aware customers and legitimate CARD, PIX-like, and account-transfer payments.
- Card and transfer lifecycles, double-entry ledger entries, and delayed workflow labels.
- F01–F05 fraud scenarios with hard negatives, alerts, cases, disputes, and oracle truth.
- Deterministic quality faults for duplicates, delays, outages, invalid values, and spikes.
- Point-in-time datasets, historical replay, rolling backtests, and versioned benchmark packs.
- Optional graph-fraud campaigns with observable/oracle views, Neo4j artifacts, and PyG export.
- Difficulty and camouflage controls for cases that remain valid but are harder to separate.
- Opt-in counterfactual fraud trajectories with minimum-change budgets and audit lineage.
- Opt-in dynamic fraud campaigns with phase evolution, rail movement, topology mutations, and oracle sidecars.
- Reference calibration profiles that tune aggregate amounts, timing, balances, activity, and merchant behavior without copying reference rows.
- An opt-in label observation engine for selective, delayed, missing, preliminary, corrected, and reopened labels with point-in-time-safe history.
- Opt-in large-scale generation profiles with stable sharding, bounded chunked Parquet output, parallel scheduling, checkpoint/resume, partition fingerprints, and cross-partition reconciliation.
- Reproducible baseline ML oracles (Logistic Regression, LightGBM, XGBoost, CatBoost), point-in-time external prediction evaluation, fraud metrics, and optional MLflow tracking.
- Generator-quality benchmark reports for native runs and capability-declared external generators, with immutable M22 profiles and `N/A`-aware scoring.

FraudTwin is designed for fraud engineers, data scientists, ML engineers, and data teams who need realistic relationships and timing before introducing a larger streaming or production stack.

## Quick start

Requirements: Python 3.12 and Poetry 2.x.

```bash
poetry install
poetry run fraudtwin config validate configs/minimal.yaml
poetry run fraudtwin generate configs/minimal.yaml
```

To fit and reuse an aggregate-only calibration profile:

```bash
poetry run fraudtwin calibrate reference.parquet --output calibrated-profile.yaml
poetry run fraudtwin generate configs/minimal.yaml \
  --profile calibrated-profile.yaml --seed 42
```

The command prints the run ID and output location. To keep generated files out of the repository, set an output directory:

```bash
poetry run fraudtwin generate configs/minimal.yaml \
  --output-dir /tmp/fraudtwin-run
```

The minimal configuration creates 10 customers, 10 behavior profiles, and 100 target payments. Fraud is disabled in this baseline; enable it in a copied YAML file or start with a fixture under `configs/benchmarks/`.

For deterministic large-run generation, use a scale profile such as `configs/scale-1b.yaml`:

```bash
poetry run fraudtwin generate configs/scale-1b.yaml --workers 16 --checkpoint-dir .fraudtwin/run-1b
poetry run fraudtwin resume .fraudtwin/run-1b
```

Scale execution uses the same canonical simulator. Worker scheduling does not change logical IDs or partition fingerprints, and checkpoints support deterministic retry/resume. The billion profile is a documented benchmark target for suitable hardware, not a test-suite requirement.

For baseline model evaluation, install the optional ML dependencies and train on a previously built PIT dataset:

```bash
poetry install -E ml
poetry run fraudtwin ml train runs/<run-id>/ml/dataset.parquet --output-dir runs/ml-evaluations
poetry run fraudtwin ml evaluate runs/<run-id>/ml/dataset.parquet predictions.parquet
```

Run the generator-quality protocol against all immutable M21 public packs:

```bash
poetry run fraudtwin quality-benchmark --profile standard-v1
poetry run fraudtwin report RUN-<id>
```

The report keeps correctness, fidelity, fraud difficulty, scalability, engineering performance, and reproducibility independent. External generators may provide a normalized bundle or a `module:factory` adapter; unsupported dimensions are reported as `N/A`.

For comparable results across machines and FraudTwin releases, run one of the
bundled immutable public benchmark packs:

```bash
fraudtwin benchmark run FT-B04-CAMOUFLAGE@1.0
fraudtwin benchmark describe FT-B04-CAMOUFLAGE@1.0.0
```

Pack definitions freeze their generation controls, PIT windows, metrics,
calibration identity, descriptors, and logical fingerprints. Historical pack
versions remain runnable; changes publish a new version.

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
└── campaign_dynamics/<m15_id>/
    ├── observable/
    ├── oracle/
    └── campaign_dynamics_manifest.json
```

The manifest records the seed, configuration, schemas, counts, fingerprints, and quality diagnostics. Graph and benchmark runs may also contain `oracle/`, graph exports, and backtest folds. No real personal data or payment credentials are generated.

Calibration profiles contain only deterministic statistical summaries and provenance fingerprints. Reference rows, source identifiers, and fitting data are not written to profiles, generated tables, fidelity reports, or manifests.

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
| PostgreSQL operational mirror | Available (optional `postgres` extra) |
| Kafka, Flink, feature stores, and advanced models | Planned |

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
