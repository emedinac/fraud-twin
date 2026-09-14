# Development

FraudTwin is a deterministic Python simulator. Contributions should preserve stable identities, causal timestamps, explicit schemas, and reproducibility.

## Quick entry point

Requirements: Python 3.12 and Poetry 2.x.

```bash
poetry install
poetry run fraudtwin config validate configs/minimal.yaml
poetry run pytest
```

Run the complete local quality gate before opening a pull request:

```bash
poetry check --strict
poetry run ruff check .
poetry run ruff format --check .
poetry run mypy src
poetry run pytest
poetry build
git diff --check
```

Calibration development uses a strict canonical Parquet reference with `amount`,
UTC `event_time`, and `customer_id` columns. Fit profiles in a temporary
directory, compare repeated profile and generated-run fingerprints, and verify
that only aggregate calibration artifacts are emitted.

For focused tests, generated-run smoke checks, architecture notes, and the contribution workflow, see the [development guide](docs/development.md).

## Documentation map

- [Quickstart](docs/quickstart.md) — generate and inspect a first run.
- [Configuration](docs/configuration.md) — tune behavior, fraud, quality, and benchmarks.
- [Data and evaluation workflows](docs/workflows.md) — datasets, replay, and backtests.
- [Graph and benchmark workflows](docs/graph-and-benchmarks.md) — graph export, difficulty, and camouflage.
- [Release notes](CHANGELOG.md) — release history.
