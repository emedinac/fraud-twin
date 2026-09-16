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
  <a href="https://emedinac.github.io/fraud-twin/latest/api.html">Python API reference</a>
  ·
  <a href="docs/README.md">Documentation sources</a>
</p>

The documentation is versioned. Use [`latest`](https://emedinac.github.io/fraud-twin/latest/)
for the main branch, or select a release when an API, schema, or manifest must
match a pinned package version. The former unversioned `/api.html` URL is kept as
a redirect for existing bookmarks.

Start with a guided tutorial, then move from configuration to fraud scenarios,
graph exports, point-in-time ML workflows, streaming reliability, and
operational replay—all backed by deterministic examples.

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
- Source-controlled Avro contracts for clean observable operational events, with canonical fingerprints and `FULL_TRANSITIVE` compatibility validation.
- Optional native Kafka streaming for the six Avro subjects, with remote Schema Registry verification and deterministic delivery pacing.

FraudTwin is designed for fraud engineers, data scientists, ML engineers, and data teams who need realistic relationships and timing before introducing a larger streaming or production stack.

## Quick start

Requirements: Python 3.12 and Poetry 2.x.

```bash
poetry install
poetry run fraudtwin config validate configs/minimal.yaml
poetry run fraudtwin generate configs/minimal.yaml
poetry run fraudtwin schema validate
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

### Python API

The high-level API is typed and has two deliberate return modes:

```python
from pathlib import Path

import fraudtwin
from fraudtwin.config import load_config

config = load_config(Path("configs/minimal.yaml"))

# In-memory workflow: entities, behavior, manifest, and an optional PIT dataset.
data = fraudtwin.generate(config)
dataset = data.require_dataset().frame
print(data.run_id, dataset.shape)

# Persisted workflow: metadata and paths first; load domain records explicitly.
written = fraudtwin.generate(config, write=True, output_dir=Path("/tmp/fraudtwin-run"))
loaded = written.load_data()
print(written.run_id, len(loaded.behavior.payments), written.manifest_path)
```

`generate(..., write=False)` returns `GeneratedData`. Use
`data.require_dataset()` when the configuration enables point-in-time data.
`generate(..., write=True)` returns a lightweight `GeneratedRun`; call
`run.load_data()` when a graph, dataset builder, or publisher needs the typed
entities and behavior records. This keeps large persisted runs from being
silently retained in memory and gives IDEs precise autocomplete.

For a laptop-scale smoke run, use the bounded `dev` profile (1,000 payments)
and keep the normal feature path small. The 100k–1B profiles are manual
benchmarks for suitable hardware:

```yaml
scale:
  profile: dev
  target_payments: 1000
  shard_count: 2
  chunk_size: 100
  worker_count: 2
```

For deterministic large-run generation, use a scale profile such as `configs/scale-1b.yaml`:

```bash
poetry run fraudtwin generate configs/scale-1b.yaml --workers 16 --checkpoint-dir .fraudtwin/run-1b
poetry run fraudtwin resume .fraudtwin/run-1b
```

Scale execution uses the same canonical simulator and writes partitioned
Parquet chunks. Worker scheduling does not change logical IDs or partition
fingerprints, and checkpoints support deterministic retry/resume. The billion
profile is a documented benchmark target for suitable hardware, not a
test-suite requirement. Install `-E scale` when using DuckDB/PyArrow
out-of-core tooling. The legacy Python API still materializes the canonical
entity/behavior objects before persistence; use the `dev` profile locally and
run 100M/1B profiles only through a streaming producer deployment.

Producer integrations can use `fraudtwin.iter_scale_records(...)` as a lazy
canonical entity/profile/payment/event/ledger stream and pass it to the scale
partition writer without constructing a `BehaviorDataset`.

Optional sinks expose corresponding bounded interfaces: use
`KafkaPublisher.publish_records`, `persist_scale_records`, and
`IcebergLakehouse.append_stream` for chunk iterators. Partitioned replay can
be consumed with `iter_partition_replay_events`.

See the [v2.0.0 release-readiness roadmap](docs/release-readiness.md) for M18
scale gates and deferred platform integrations.

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

To include measured M18 scalability evidence without running a large workload
as part of the quality command, pass the completed scale manifest:

```bash
poetry run fraudtwin quality-benchmark --profile standard-v1-dev \
  --scale-manifest runs/scale-benchmarks/<run>-benchmark.json
```

For comparable results across machines and FraudTwin releases, run one of the
bundled immutable public benchmark packs:

```bash
fraudtwin benchmark run FT-B04-CAMOUFLAGE@1.0
fraudtwin benchmark describe FT-B04-CAMOUFLAGE@1.0.0
```

Pack definitions freeze their generation controls, PIT windows, metrics,
calibration identity, descriptors, and logical fingerprints. Historical pack
versions remain runnable; changes publish a new version.

## Learning paths

The documentation is organized by the work an engineer needs to complete, not
by an unstructured notebook list:

| Path | Focus |
| --- | --- |
| [Getting started](docs/tutorials/getting-started.md) | Generate a run, configure it, inspect lifecycles, and understand delayed labels. |
| [Core workflows](docs/tutorials/core-workflows.md) | Build point-in-time datasets, stress scenarios, and reproducible benchmarks. |
| [Production ML and reliability](docs/tutorials/production-ml.md) | Train, evaluate, serve, promote, and monitor fraud models. |
| [Graph analytics](docs/tutorials/graph-analytics.md) | Investigate graph fraud with Neo4j and PyTorch Geometric. |
| [Streaming and Kafka reliability](docs/tutorials/streaming-reliability.md) | Validate Avro contracts and recover from logical delivery faults. |
| [Operations and incident response](docs/tutorials/operations.md) | Resume scale runs, repair projections, reconcile storage, and inspect observability. |

The [Python API reference](https://emedinac.github.io/fraud-twin/latest/api.html)
then provides signatures, parameters, return types, exceptions, optional
dependencies, and source links for every supported public export.

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
├── ml/
│   ├── dataset.parquet
│   └── dataset_manifest.json
├── counterfactuals/<counterfactual_id>/
│   ├── observable/{original,modified}/
│   ├── oracle/change_sets.parquet
│   └── counterfactual_manifest.json
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

**Optional infrastructure stays at the boundary.** Core generation and
point-in-time analysis run without Kafka, PostgreSQL, Iceberg, Neo4j, or a
broker. Integrations consume stable records and manifests, so an experiment
can begin offline and move to services without changing the simulated source
truth.

**Evaluation is leakage-aware.** Operational views contain only data available
at the selected cutoff. Oracle views are reserved for evaluation, calibration,
and audit. Temporal splits, label maturity, fingerprints, and manifests make
the distinction inspectable.

## Optional integrations

Install only the capabilities needed for a workflow:

| Extra | Capability | Typical use |
| --- | --- | --- |
| `ml` | scikit-learn and gradient-boosting baselines | Train and evaluate fraud models. |
| `graph` | PyTorch and PyTorch Geometric | Convert temporal graphs and run graph ML experiments. |
| `kafka` | Confluent Kafka and Schema Registry clients | Publish Avro events and validate delivery contracts. |
| `postgres` | PostgreSQL client | Persist an operational mirror and reconcile counts. |
| `lakehouse` | Iceberg/PyArrow/Spark tooling | Materialize Bronze/Silver/Gold snapshots and backfill late events. |
| `observability` | Prometheus client | Expose lag, quality, and reconciliation metrics. |
| `mlflow` / `serving` | MLflow, FastAPI, and Uvicorn | Track artifacts and run the local reference scoring service. |

For example:

```bash
poetry install -E ml -E graph
poetry install -E kafka -E postgres -E lakehouse -E observability
```

Service-backed examples include Docker Compose instructions and an offline
fallback. Docker is not required for the core simulator or documentation
build.

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
| Native Kafka streaming (optional `kafka` extra) | Available |
| Iceberg lakehouse (optional `lakehouse` extra) | Available |
| CLI Prometheus metrics and Grafana dashboard (optional `observability` extra) | Available |
| Spark, feature stores, and advanced models | Planned |

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

Pull-request CI runs three tiny external-service smoke tests for PostgreSQL, Kafka, and Iceberg; they are skipped in normal local test runs.

## License and references

FraudTwin is released under the [Apache License 2.0](LICENSE). See [`docs/references.md`](docs/references.md) for the handbooks, repositories, standards, and research papers that informed the design and how each was used.
