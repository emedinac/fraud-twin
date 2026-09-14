# Quickstart

This walkthrough creates a small payment world locally. It uses the tracked minimal configuration, so the result is quick to generate and easy to inspect.

## Requirements

- Python 3.12
- Poetry 2.x

Install the project from the repository root:

```bash
poetry install
```

## Generate a first run

Validate the configuration before generating data:

```bash
poetry run fraudtwin config validate configs/minimal.yaml
poetry run fraudtwin generate configs/minimal.yaml
```

The command prints a run ID and the output location. To keep generated files outside the repository, choose an output directory explicitly:

```bash
poetry run fraudtwin generate configs/minimal.yaml \
  --output-dir /tmp/fraudtwin-run
```

The minimal configuration creates 10 customers, 10 behavior profiles, 100 target payments, and the lifecycle events those payments require. Fraud is off by default, which makes the first run a clean baseline.

## What a run contains

Each run is stored under `runs/<run_id>/` (or the directory supplied with `--output-dir`):

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
```

The manifest records the seed, configuration, schemas, counts, fingerprints, and quality diagnostics needed to explain or reproduce the run. Benchmark runs may also contain `oracle/`, graph exports, and backtest folds.

## Turn on fraud

Use a benchmark fixture when you want a complete, repeatable example:

```bash
poetry run fraudtwin config validate \
  configs/benchmarks/m12-difficulty-v1.yaml
poetry run fraudtwin generate \
  configs/benchmarks/m12-difficulty-v1.yaml \
  --output-dir /tmp/fraudtwin-m12
```

For a smaller custom run, set `fraud.enabled: true` in a copied YAML file. The generator supports F01 Card Not Present, F02 Card Testing, F03 Account Takeover, F04 Instant-Payment Scam, and F05 Velocity Attack, with configurable hard negatives and workflow projections.

## Next steps

- Adjust behavior and payment settings in [Configuration](configuration.md).
- Build a historical ML table or replay a run with [Workflows](workflows.md).
- Export graph views and compare stress levels with [Graph and benchmark workflows](graph-and-benchmarks.md).
