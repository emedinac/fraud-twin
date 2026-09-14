# Development

Requirements: Python 3.12 and Poetry 2.x.

## Pull request checks

GitHub Actions CI is intentionally limited to pull requests from
`feature/dev` into `main`; pushes to branches do not start CI. In the
repository settings, protect `main` and require the `CI / quality` status
check before merging. This makes a failed check block the merge until the
problem is fixed and a new commit passes CI.

```bash
poetry install
poetry run pytest
poetry run ruff check .
poetry run ruff format --check .
poetry run mypy src
```

The entity generator can be exercised with:

```bash
poetry run fraudtwin config validate configs/minimal.yaml
poetry run fraudtwin generate configs/minimal.yaml
```

The behavior smoke tests intentionally use the minimal 100-payment fixture so they remain fast in local development and CI:

```bash
poetry run pytest tests/test_behavior.py
```

To validate and generate into a temporary directory manually:

```bash
poetry run fraudtwin config validate configs/minimal.yaml
poetry run fraudtwin generate configs/minimal.yaml --output-dir /tmp/fraudtwin-run
```

The behavior section controls `amount_min`, `amount_max`, `active_hours`, `weekday_weights`, `merchant_preference_count`, and `preferred_device_limit`. Generated profiles and payment tables use stable schemas and deterministic IDs. M6 fraud scenarios and the M7 fraud workflow are disabled together when `fraud.enabled` is false. M8 quality injection is controlled independently under `quality`; chargebacks, streaming, and production infrastructure remain deferred.

Card lifecycle tests can be run directly with:

```bash
poetry run pytest tests/test_card_lifecycle.py
```

PIX lifecycle tests can be run directly with:

```bash
poetry run pytest tests/test_pix_lifecycle.py
```

To validate the transfer ledger for a generated run:

```bash
poetry run fraudtwin validate-ledger --run-id <run-id>
```

The `pix_lifecycle` section controls PIX approval, rejection, and return probabilities plus validation, authorization, submission, settlement, receipt, and return delays. All values are validated before generation. PIX lifecycle tests use small datasets and cover ordering, rejection, returns, relationships, schemas, manifests, and ledger reconciliation.

The complete suite, including lightweight lifecycle generation, is:

```bash
poetry run pytest
```

The `card_lifecycle` section controls approval, reversal, and refund probabilities plus authorization, capture, clearing, settlement, reversal, and refund delays in whole seconds. Use `poetry run fraudtwin config validate configs/minimal.yaml` to validate these settings before generation.

Run the focused M6 scenario tests with:

```bash
poetry run pytest tests/test_fraud.py
```

The fraud section controls `enabled`, `target_rate`, `scenario_count`,
`hard_negative_rate`, and per-scenario weights, counts, amounts, durations,
attempt counts, and velocity windows. Validate the configuration and generate
an enabled scenario run into a temporary directory with:

```bash
poetry run fraudtwin config validate configs/minimal.yaml
poetry run fraudtwin generate configs/minimal.yaml --output-dir /tmp/fraudtwin-run
poetry run pytest tests/test_fraud.py tests/test_card_lifecycle.py tests/test_pix_lifecycle.py
```

Follow the milestone order in [`FEATURES.md`](FEATURES.md). Do not add infrastructure before the simulator has correct state and temporal behavior.

Run the focused M7 workflow tests with:

```bash
poetry run pytest tests/test_cases.py
```

For a targeted M7 validation run, use:

```bash
poetry run fraudtwin config validate configs/minimal.yaml
poetry run fraudtwin generate configs/minimal.yaml --output-dir /tmp/fraudtwin-m7
poetry run pytest tests/test_cases.py tests/test_fraud.py tests/test_card_lifecycle.py tests/test_pix_lifecycle.py
poetry run fraudtwin validate-ledger --run-id <run-id> --output-dir /tmp/fraudtwin-m7
```

Run the targeted M8 quality tests with:

```bash
poetry run pytest tests/test_quality.py
poetry run fraudtwin config validate configs/minimal.yaml
poetry run fraudtwin generate configs/minimal.yaml --output-dir /tmp/fraudtwin-m8
poetry run fraudtwin validate-ledger --run-id <run-id> --output-dir /tmp/fraudtwin-m8
```

Use a temporary YAML override to exercise a fault profile without changing the tracked minimal configuration. The M8 tests cover reproducibility, independent fault settings, optional-field handling, invalid values, source timing, delivery ordering, spikes, Parquet schemas, manifests, CLI generation, Pandera dataframe validation, and SDMetrics-aligned uniqueness, structure, and referential-integrity diagnostics. The quality layer reports intentional validation failures instead of rejecting configured chaos output. Hypothesis property tests exercise deterministic fault application across seeds and requested probabilities.

The focused M9 checks are:

```bash
poetry run pytest tests/test_dataset.py
poetry run fraudtwin config validate configs/minimal.yaml
poetry run fraudtwin generate configs/minimal.yaml --output-dir /tmp/fraudtwin-m9
poetry run fraudtwin ml build-dataset configs/minimal.yaml \
  --run-id <run-id> --output-dir /tmp/fraudtwin-m9 --label-delay-aware
```

M9 dataset rows are ordered by stable payment ID. Features are computed from
prior business events at each row's prediction timestamp after filtering on
business time and source availability; labels are filtered by label
availability and configured delay. Temporal split gaps default to the label
delay and are excluded from all partitions. Dataset manifests make the source
run, parameters, split boundaries, schema, row and output fingerprints, and
feature/label policies explicit.

The focused M10 replay and rolling-backtest checks are:

```bash
poetry run pytest tests/test_m10.py
poetry run fraudtwin replay --run-id <run-id> \
  --from 2026-01-01T00:00:00Z --to 2026-01-02T00:00:00Z \
  --order original_delivery --output-dir /tmp/fraudtwin-m10
poetry run fraudtwin ml backtest configs/minimal.yaml --run-id <run-id> \
  --benchmark-pack configs/benchmarks/m10-minimal-v1.yaml \
  --output-dir /tmp/fraudtwin-m10
```

M10 replay selects `[from, to)` and preserves source identities, business and
availability timestamps, causal links, fraud records, workflow history, and
typed schemas. Rolling folds are chronological and non-overlapping; their
manifests record exact windows, source snapshots, PIT checks, resolved regimes,
label policy, deterministic baseline metrics, aggregate dispersion, and
degradation from the first test fold. Benchmark-pack definitions are hashed
and immutable: changing a window, regime, seed/configuration, label policy,
scenario parameter, or metric definition creates a new pack identity.
Rolling test windows are required to be non-overlapping (`step_seconds` must
be at least `test_window_seconds`). Benchmark packs freeze their own positive
label-maturity gap, enforce train/validation/test/stress order, and apply the
gap at the train/validation and validation/test transitions. Regimes may use
`label_observation_policy: unobserved` to retain latent fraud and workflow
history without emitting an observed label.
The focused M11 graph checks are:

```bash
poetry run fraudtwin graph export --run-id <run-id> --output-dir /tmp/fraudtwin-m11
poetry run pytest tests/test_graph.py
```

Graph export is read-only over the source run. It writes separate observable
and oracle views, applies point-in-time/source-availability filtering, and
uses append-only content-addressed output directories.
## M11 expanded graph scenarios

Use the self-contained benchmark fixture and validate both graph views:

```bash
poetry run fraudtwin generate configs/benchmarks/m11-graph-v2.yaml --output-dir /tmp/fraudtwin-m11
poetry run fraudtwin graph export --run-id <run-id> --output-dir /tmp/fraudtwin-m11 --format parquet,neo4j --json
poetry run fraudtwin graph validate --run-id <run-id> --output-dir /tmp/fraudtwin-m11
```

Exports are append-only and content-addressed. Observable graphs contain only
source-supported relationships available at the selected cutoff; oracle graphs
add campaign, pattern, evidence, and optional hyperedge incidence records.
Neo4j artifacts contain deterministic bulk-import CSV/Cypher files. PyTorch
Geometric remains optional via `poetry install -E graph`.

## M12 fraud difficulty

Run the focused M12 tests and the versioned fixture in a temporary directory:

```bash
poetry run pytest tests/test_m12.py
poetry run fraudtwin config validate configs/benchmarks/m12-difficulty-v1.yaml
poetry run fraudtwin generate configs/benchmarks/m12-difficulty-v1.yaml \
  --output-dir /tmp/fraudtwin-m12
poetry run fraudtwin validate-ledger --run-id <run-id> \
  --output-dir /tmp/fraudtwin-m12
```

M12 is disabled when `benchmark.difficulty` is omitted. A level from 1 through
10 resolves measurable transformations for all M6 and M11 scenarios. Advanced
users can replace individual dimensions under `benchmark.controls`; supplied
values do not blend with the level profile. Active M12 runs retain full truth in
oracle artifacts while operational payment events omit direct scenario and
fraud-record linkage fields. The source and point-in-time dataset manifests
record the requested/resolved controls, transformations, effective hash,
fingerprints, and measured summaries.

The M12 regression suite also compares disabled configurations against the
legacy stream and checks deterministic lifecycle, ledger, temporal, graph,
workflow, oracle, manifest, and CLI behavior. The cited research papers and
Fraud Detection Handbook are design references only; no source code, datasets,
models, or trained detectors are copied.
