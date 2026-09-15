# Data and evaluation workflows

FraudTwin separates generation from analysis. Generate a source run once, then build datasets, replay windows, or run backtests over those records without regenerating the financial world.

## Build a point-in-time dataset

The dataset builder creates historical features and labels using only data that was available at each row’s `prediction_time`:

```bash
poetry run fraudtwin ml build-dataset configs/minimal.yaml \
  --run-id <run-id> \
  --output-dir runs
```

Source events and ledger entries are filtered by source availability. Labels are filtered by `label_available_at` and the configured label delay. Use `--label-delay-aware` when unresolved labels should be excluded rather than retained for inspection.

The dataset manifest records the source run, feature and label definitions, split boundaries, stable schema, row hash, and output fingerprint.

When M17 is enabled, inspect `label_observations/<id>/observable/observed_labels.parquet` for the operational projection and the corresponding oracle history for audit. Dataset construction filters each version by `label_available_at`; it never exposes future corrections or latent truth.

## Generate and resume large runs

M18 scale profiles use the same simulator and output contracts as ordinary runs while writing deterministic shard/chunk Parquet artifacts and a checkpoint manifest. Worker count changes scheduling only. Resume validates the stored configuration and seed tree, reuses valid completed partitions, and regenerates incomplete work deterministically:

```bash
poetry run fraudtwin generate configs/scale-1b.yaml --workers 16 --checkpoint-dir .fraudtwin/run-1b
poetry run fraudtwin resume .fraudtwin/run-1b
```

The `billion` profile is a hardware-dependent benchmark target and is not part of unit, smoke, or CI validation.

## Replay a historical window

Replay is read-only. It selects records from an existing run over a half-open interval and preserves their source identities and timestamps:

```bash
poetry run fraudtwin replay --run-id <run-id> \
  --from 2026-01-01T00:00:00Z \
  --to 2026-01-02T00:00:00Z \
  --order event_time_order \
  --output-dir runs
```

Use `original_delivery` when the question is about ingestion and processing order rather than business event time. Replay adds sequence metadata without rewriting the source records.

## Run rolling backtests

Backtests use fixed or expanding training windows and explicit validation, test, stress, and label-maturity boundaries:

```bash
poetry run fraudtwin ml backtest configs/minimal.yaml \
  --run-id <run-id> \
  --benchmark-pack configs/benchmarks/m10-minimal-v1.yaml \
  --output-dir runs
```

Windows are chronological and non-overlapping. A benchmark pack freezes its own label-maturity gap, regime policy, seed/configuration identity, and metric definition so later comparisons remain meaningful.

## Train baselines and evaluate external predictions

M19 trains deterministic Logistic Regression, LightGBM, XGBoost, and CatBoost oracles on the same frozen PIT feature allowlist. Install the optional model stack before training:

```bash
poetry install -E ml
poetry run fraudtwin ml train runs/<run-id>/ml/dataset.parquet \
  --config configs/ml-baselines.yaml \
  --output-dir runs/ml-evaluations
```

External models can submit strict Parquet or JSONL records containing one event/payment/customer/account ID, a prediction timestamp, a fraud score, and an optional predicted class:

```bash
poetry run fraudtwin ml evaluate runs/<run-id>/ml/dataset.parquet predictions.jsonl
```

The evaluator reports ranking, threshold, calibration, monetary, detection-delay, and segment metrics. It resolves labels and features at each prediction timestamp and records an immutable evaluation manifest. MLflow is used when a tracking URI is configured; otherwise local artifacts are sufficient.

## A practical evaluation sequence

1. Generate a clean source run and validate its ledger.
2. Build the point-in-time dataset with the desired unresolved-label policy.
3. Replay a historical interval when you need an audit or delivery-order view.
4. Run a benchmark pack and compare fold manifests, not just aggregate scores.
5. Inspect the source, operational, and oracle artifacts together when a case needs an explanation.

This sequence keeps evaluation honest: the model sees only what would have been known at the time, while the oracle remains available for analysis and audit.
