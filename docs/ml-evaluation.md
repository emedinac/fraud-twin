# ML evaluation methodology

**Level:** Intermediate<br><br>
**You will:** evaluate predictions with point-in-time-safe features, mature<br><br>
labels, temporal splits, and metrics that match the decision being tested.
**Before you start:** [Data and evaluation workflows](workflows.md).<br><br>
**Services:** None; MLflow is optional.<br><br>

This guide describes how to turn generated events into an evidence-backed
fraud model experiment. It is intended for data scientists, ML engineers, and
researchers. The simulator produces controlled evidence; it does not predict
production performance.

## Leakage-safe evaluation

Build the dataset from a persisted run and use `prediction_time` as the
information boundary:

```console
CONFIG=configs/minimal.yaml
RUN_ID=RUN-...
RUNS_DIR=./runs

poetry run fraudtwin ml build-dataset "$CONFIG" \
  --run-id "$RUN_ID" --output-dir "$RUNS_DIR"
```

For each row, features, labels, corrections, and workflow events must satisfy
their availability timestamps. A label can be fraud in oracle data while still
being unresolved operationally. Exclude immature labels for final evaluation
and report the excluded count.

Use chronological windows rather than random splits:

| Window | Purpose |
| --- | --- |
| Train | Fit parameters and preprocessing |
| Validation | Select threshold and model configuration |
| Test | One-time quality estimate on a later period |
| Stress | Optional scenario, quality, or domain-shift challenge |

Never select a threshold on the test window. Persist split boundaries,
label-maturity policy, feature allowlist, and source fingerprint in the
evaluation manifest.

## Metrics and decisions

The threshold trade-off figure is generated deterministically by the tutorial
workflow and should be read alongside the metric table.

```{image} _static/images/model-threshold-tradeoff.svg
:alt: Precision and recall trade-off across fraud scoring thresholds
:class: evidence-figure
```
*Figure: threshold evidence from a bounded deterministic model exercise; use
the accompanying metrics and manifest for decisions.*

| Metric | Use | Caveat |
| --- | --- | --- |
| PR-AUC | Ranking under class imbalance | Depends on prevalence |
| ROC-AUC | Ranking across thresholds | Can look optimistic for rare fraud |
| Recall at fixed FPR | Capacity-constrained detection | State the chosen FPR and segment |
| Calibration | Whether scores mean probabilities | Recalibrate only on validation data |
| Monetary loss | Business cost trade-off | Document fraud, review, and false-positive costs |
| Detection delay | Time-to-action | Requires mature labels and event timestamps |
| Segment metrics | Fairness and operational stability | Report sample counts with every slice |

Compare a deterministic heuristic and logistic regression before trying larger
models. A model is not promoted because one aggregate metric increased: require
acceptable segment behavior, calibration, latency, artifact compatibility, and
reproducibility.

## 10k-payment experiment

The maintained training tutorial uses approximately 10,000 source payments and
produces a dataset summary, missingness/leakage report, class-balance table,
temporal split, baseline comparison, selected threshold, model artifact, and
evaluation manifest:

```console
poetry install -E ml -E mlflow
RUN_ID=RUN-...
DATASET=./runs/$RUN_ID/ml/dataset.parquet
EVALUATIONS_DIR=./runs/evaluations

poetry run fraudtwin ml train "$DATASET" \
  --config configs/ml-baselines.yaml --output-dir "$EVALUATIONS_DIR"
poetry run fraudtwin ml evaluate "$DATASET" \
  "$EVALUATIONS_DIR/predictions.jsonl"
```

MLflow is optional. Without a tracking URI, local JSON/Parquet manifests remain
the source of truth. With MLflow, record the same model ID, feature version,
dataset fingerprint, configuration fingerprint, and artifact checksum in both
systems.

## Promotion and rollback checklist

Before promotion, verify:

- the artifact loads in a clean environment;
- the request schema matches the feature allowlist;
- test performance uses matured labels;
- threshold and cost policy are recorded;
- warm p50/p95 latency and artifact size are measured;
- online/offline predictions match on a replay sample;
- the previous model remains available for rollback.

Rollback means restoring the previous compatible artifact and recording the
reason, model ID, schema version, and evaluation fingerprint. It does not mean
regenerating the source world.

## Interpretation limits

FraudTwin is useful for testing leakage controls, label delay, scenario
coverage, graph provenance, and operational failure handling. Synthetic metrics
do not establish a production model's expected precision, recall, fairness, or
financial return. Calibrate conclusions against reference data and document
which simulator controls produced the observed result.

## Next

Use [Drift and shift](drift-and-shift.md) after evaluation, or continue to the
[Model lifecycle](model-lifecycle.md) when an artifact should be served.

## Related

- [Data and evaluation workflows](workflows.md)
- [Data contracts](data-contracts.rst)
- [Release evidence](release-evidence.md)
