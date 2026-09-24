# Model lifecycle: from simulation to service

**Level:** Intermediate<br><br>
**You will:** move from a generated dataset to a tracked model artifact and a<br><br>
validated local scoring hand-off.
**Before you start:** [ML evaluation](ml-evaluation.md).<br><br>
**Services:** None for local artifacts; MLflow and FastAPI are optional.<br><br>

This guide is the production-shaped path through FraudTwin. It keeps the
generator, feature construction, evaluation, artifact, and serving boundaries
explicit:

```text
simulation run → PIT dataset → train → time-held-out evaluation
             → artifact/MLflow → HTTP or Kafka scoring → monitoring
```

## 1. Generate and inspect the source

Use a bounded 1,000–10,000-payment run for local development. Record the run
manifest, configuration hash, schema versions, payment/event counts, and the
fraud scenario mix before training.

## 2. Build leakage-safe data

```console
fraudtwin ml build-dataset configs/benchmarks/m13-camouflage-v1.yaml \
  --run-id <run-id> --output-dir runs
```

Inspect `prediction_time`, `source_available_at`, `feature_available_at`, and
`label_available_at`. A feature must be available at prediction time; an
unresolved label must not silently become a negative label.

## 3. Train and choose a model

```console
fraudtwin ml train runs/<run-id>/ml/dataset.parquet \
  --config configs/ml-baselines.yaml --output-dir runs/evaluations
```

Compare a deterministic heuristic with Logistic Regression, LightGBM, XGBoost,
or CatBoost on identical temporal splits. Prefer the simplest model that meets
the PR-AUC, recall-at-FPR, calibration, latency, and interpretability goals.

## 4. Promote an artifact

An evaluation directory contains predictions, metrics, an evaluation manifest,
and optional `models/*.joblib` artifacts. The manifest records the feature
allowlist, preprocessing policy, source run, seed, and package versions. When a
tracking URI is configured, `write_evaluation` also logs the run to MLflow.

## 5. Serve and monitor

The [reference FastAPI service](production-serving.md) loads one artifact per
process and validates typed point-in-time requests. Keep offline and online
feature code identical, emit model/version metadata with every score, and
monitor input drift, score drift, latency, errors, and delayed-label quality.

## Next

Follow [Production serving](production-serving.md) for the local HTTP boundary,
then review [Compatibility](compatibility.md) before a package upgrade.

## Related

- [ML evaluation](ml-evaluation.md)
- [Drift and shift](drift-and-shift.md)
- [Integration runbooks](integrations.md)
