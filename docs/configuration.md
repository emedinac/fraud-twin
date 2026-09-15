# Configuration

FraudTwin treats configuration as part of the run’s identity. A validated YAML file determines the population, behavior, event timing, fraud campaigns, and output policy. Together with the seed, it gives the run a stable fingerprint.

Start from [`configs/minimal.yaml`](../configs/minimal.yaml) and change only the section that describes the behavior you want to study.

## Configuration at a glance

| Section | Purpose |
| --- | --- |
| `simulation` | Seed, time window, and execution mode |
| `population` | Counts for customers, accounts, cards, merchants, devices, and institutions |
| `payments` | Daily volume and payment-rail weights |
| `behavior` | Customer spending, timing, merchant, and device preferences |
| `card_lifecycle` | Card initiation, authorization, capture, clearing, settlement, refunds, and reversals |
| `pix_lifecycle` | Pix-like validation, authorization, settlement, rejection, timeout, and return timing |
| `fraud` | Scenario selection, prevalence, weights, and hard negatives |
| `fraud_workflow` | Alerts, cases, confirmations, disputes, and label delays |
| `labels` | Opt-in deterministic label observation, missingness, corrections, and reopenings |
| `scale` | Opt-in large-run profiles, stable shards, chunks, workers, and checkpoints |
| `quality` | Deterministic typed faults, delivery defects, outages, schema changes, and spikes |
| `dataset` | Point-in-time feature and label construction |
| `backtest` | Rolling train, validation, test, and stress windows |
| `graph` | Opt-in fraud-network campaigns and graph export inputs |
| `benchmark` / `stress` | Difficulty and camouflage controls |
| `counterfactual` | Opt-in minimum-change fraud trajectories and lineage sidecars |
| `calibration` | Opt-in reference-derived aggregate parameters and fidelity controls |
| `outputs` | Parquet and optional PostgreSQL, Kafka, and Iceberg outputs |
| `lakehouse` | Iceberg namespace and isolated-oracle publication controls; service credentials remain environment-only |

Unknown fields and invalid ranges are rejected during validation. That strict boundary is intentional: a run should fail before it produces ambiguous data.

## Label observation

Label observation is disabled by default and therefore does not change legacy run bytes or identities. Enable it to model selection-dependent discovery and versioned labels:

```yaml
labels:
  enabled: true
  investigation_rate: 0.70
  missing_fraud_rate: 0.05
  preliminary_error_rate: 0.02
  correction_rate: 0.01
  confirmation_delay: lognormal
```

Enabled runs write append-only `label_observations/<id>/observable/observed_labels.parquet` and the oracle-only `label_history.parquet`. PIT rows resolve the greatest label version whose `label_available_at` is no later than `prediction_time`; immature or missing labels follow `dataset.unresolved_labels`. `fraud_workflow` continues to provide alert and case timing, while `labels.investigation_rate` is the sole enabled-run selection control.

## Large-scale generation

Scale generation reuses the canonical simulator and is disabled by default. The
primary target is canonical payment rows; lifecycle, ledger, fraud, and
workflow rows are reported separately. Profiles target 100k, 1M, 10M, 100M,
and 1B payments. The `dev` profile is a bounded 1,000-payment smoke profile
for laptops. Stable shard IDs and hierarchical seed streams make IDs,
timestamps, scenario semantics, financial state, schemas, and partition
fingerprints independent of worker scheduling. Chunked Parquet output is
bounded by `chunk_size` and `output_batch_size`; completed chunks are recorded
in an atomic checkpoint manifest.

```yaml
scale:
  profile: dev
  target_payments: 1000
  shard_count: 4
  chunk_size: 1000
  worker_count: 2
  output_batch_size: 1000
  checkpoint_frequency_chunks: 1
  partition_mapping: stable_hash_v1
```

Run and resume a scale profile with:

```bash
fraudtwin generate configs/scale-1b.yaml --workers 16 --checkpoint-dir .fraudtwin/run-1b
fraudtwin resume .fraudtwin/run-1b
```

The billion profile is a published benchmark target for suitable documented
hardware; it does not require Kafka, Spark, or cloud infrastructure merely to
demonstrate generator scalability. A completed run records target and
realized payment counts, partition checksums, canonical fingerprints, and
cross-partition reconciliation results. Use `poetry install -E scale` for the
optional PyArrow/DuckDB out-of-core tooling.

The compatibility API still materializes canonical entity/behavior objects;
use `dev` on a laptop. Production 100M/1B executions should wire a streaming
canonical-row producer to this chunk writer on SSD or S3/MinIO.

## Reference calibration

Calibration is disabled by default and does not change legacy hashes or output
bytes. Fit a reusable profile from a canonical Parquet transaction table:

```bash
fraudtwin calibrate reference.parquet --output calibrated-profile.yaml
fraudtwin generate configs/minimal.yaml --profile calibrated-profile.yaml --seed 42
```

The reference must contain finite positive `amount`, timezone-aware UTC
`event_time`, and non-empty `customer_id` columns. Optional canonical columns
are `merchant_category`, `account_balance`, `payer_account_id`,
`payee_account_id`, and `campaign_id`. Missing optional columns are recorded as
unavailable; they are never inferred from arbitrary fields.

Profiles contain aggregate distributions, frequencies, dependencies, bounded
graph/campaign summaries, fingerprints, versions, and seed-stream metadata.
They never contain reference rows or source entity identifiers. Generated runs
store append-only `calibration/<profile_id>/fidelity_metrics.parquet` and
`fidelity_report.json` sidecars. Fidelity is report-only unless explicit
thresholds are configured. Existing lifecycle, ledger, graph, fraud, PIT, and
observable/oracle boundaries remain authoritative.

## Customer behavior

Behavior profiles make legitimate activity customer-specific. The controls below shape amounts, active hours, weekday patterns, merchant preferences, and trusted devices:

```yaml
behavior:
  amount_min: 1.00
  amount_max: 5000.00
  active_hours: [8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21]
  weekday_weights: [1.0, 1.0, 1.0, 1.0, 1.0, 0.85, 0.70]
  merchant_preference_count: 3
  preferred_device_limit: 3
```

The same payment can be ordinary for one customer and unusual for another. That distinction is the foundation for hard negatives and behavior-aware fraud cases.

## Counterfactual fraud generation

Counterfactual generation is disabled by default and has no effect on legacy hashes or output files.
Enable it with explicit objective requests:

```yaml
counterfactual:
  enabled: true
  budget: 2.0
  requests:
    - {objective: F01, count: 1}
    - {objective: F04, count: 1}
```

The resolver applies request, objective, family, global, and default controls
in that order. `scenario` is accepted as an alias for `objective`, and
`max_distance` for `budget`; supplying both names is rejected. Dimension costs
cover beneficiary, device, timing, amount, merchant, geography, payment rail,
and graph relationships. The built-in weighted distance can be replaced by a
registered deterministic distance function.

Sources are selected chronologically from the pre-fraud legitimate stream.
Eligibility uses only records available at the source decision timestamp, and
stable IDs make repeated runs identical. The generator searches for a feasible minimum
change, applies the configured amount/timing constraints, and uses isolated
camouflage controls at the generation boundary. Every request records its budget,
costs, changed fields, objective result, constraints, source mapping, and either
a modified trajectory or a deterministic rejection.

Generated originals and modifications live in separate append-only sidecars;
ordinary payment/event schemas remain unchanged. Observable sidecars omit latent
fraud truth, while oracle change sets retain it for evaluation. A standalone
workflow is available for legitimate-only source runs:

```bash
fraudtwin counterfactual generate \
  --config configs/benchmarks/m14-counterfactual-v1.yaml \
  --source-run-id RUN-... \
  --output-dir /tmp/fraudtwin-run
```

Rail conversion is reported as inapplicable when changing rails would require
an unsafe lifecycle-vocabulary conversion. The graph adapter emits a closed
campaign/pattern lineage for each selected trajectory; requests requiring
multi-trajectory campaign evolution are recorded as limited. F02/F05 burst
objectives remain bounded to the selected trajectory and are never silently
expanded into additional payments.

## Fraud scenarios and workflow

Enable fraud explicitly and keep scenario controls visible in the file:

```yaml
fraud:
  enabled: true
  target_rate: 0.20
  scenario_count: 5
  hard_negative_rate: 1.0
  scenarios:
    F01: {weight: 1.0}
    F02: {weight: 1.0, attempt_count: 20}
    F03: {weight: 1.0}
    F04: {weight: 1.0}
    F05: {weight: 1.0, attempt_count: 20, window_seconds: 60}
```

The five built-in scenarios are Card Not Present, Card Testing, Account Takeover, Instant-Payment Scam, and Velocity Attack. A hard negative is a legitimate lookalike generated alongside a selected scenario; it prevents a detector from succeeding on a single obvious feature.

Workflow projections are controlled separately. Their delays are causal: an investigation or label cannot appear before the evidence that makes it possible. Operational records omit latent truth fields; oracle artifacts retain the complete explanation.

## Data quality

Use `quality.profile: clean` for a baseline, `realistic` for bounded faults, or `hostile` for a more demanding stream. Individual probabilities and delays can override a profile. Faults include duplicates, missing optional fields, invalid enums/references, negative or extreme amounts, timestamp/time-zone corruption, schema mismatches, encoding faults, late or out-of-order delivery, source delay, outages, schema changes, fraud spikes, traffic spikes, and deterministic partition skew.

```yaml
quality:
  profile: realistic
  duplicate_event_probability: 0.02
  late_event_probability: 0.05
  source_delay_seconds: 30
  invalid_enum_probability: 0.001
  invalid_reference_probability: 0.001
  negative_amount_probability: 0.001
  corrupted_timestamp_probability: 0.001
  timezone_error_probability: 0.001
  schema_mismatch_probability: 0.001
  extreme_value_probability: 0.001
  encoding_error_probability: 0.001
  partition_skew_probability: 0.01
  schema_changes:
    - at: 2026-01-15T00:00:00Z
      event: payment_events
      version: "6"
      change:
        add_optional_field:
          risk_reason: null

pix_lifecycle:
  timeout_probability: 0.0
  timeout_delay_seconds: 1
```

Quality mutations are applied in a deterministic order, preserve the pristine oracle snapshot, and are audited in the manifest with targets, requested rates, actual mutations, identity effects, boundaries, and expected validation failures. Encoding faults are written to `quality/raw_faults.jsonl`; schema-evolved rows are written under `quality/schema_evolution/`. Version-only `schema_changes` remain supported as a compatibility shorthand.

## Difficulty and camouflage

Difficulty changes how subtle a case is without changing its business objective or required topology:

```yaml
benchmark:
  difficulty: 7
  controls:
    temporal_irregularity: 0.9
    prevalence: 0.6
```

Camouflage is an independent stress layer. It moves fraud features or benign support relationships toward legitimate cohorts while preserving oracle truth:

```yaml
stress:
  camouflage: 0.8
  feature_camouflage: 0.85
  relation_camouflage: 0.75
```

Use either the versioned fixtures under `configs/benchmarks/` or a copied YAML file when comparing runs. The resolved controls, constraints, cohort choices, and effective configuration hash are recorded with the output.

## Dynamic campaign evolution

Campaign dynamics are disabled by default and do not change legacy IDs, hashes, schemas, or files. Enable them with a graph run:

```yaml
campaign_dynamics:
  enabled: true
  bindings:
    - {profile: linear}
    - {profile: rotating_ring}
    - {profile: adaptive_network}
```

The built-ins target `MULE_NETWORK`, `CYCLIC_RING`, and `DENSE_CAMPAIGN`. Bindings validate phase durations and transitions before generation and support `marked_hawkes_v1` (default) or `piecewise_rate_v1`. CARD actions are merchant purchases with the normal card lifecycle; PIX and account-transfer actions preserve account-to-account ledger rules. Rail, template, model, phase, and capacity incompatibilities are rejected deterministically.

Campaign dynamics run after graph construction and before camouflage. Counterfactual generation still selects pristine legitimate sources before fraud, graph, dynamic, and camouflage processing. Dynamic state, reasons, membership history, intensity decisions, topology mutations, and lineage are oracle-only append-only Parquet artifacts under `campaign_dynamics/<campaign_id>/`; observable output uses established payment, event, and ledger schemas. Custom deterministic transition and intensity models can be registered through the public API. `fraudtwin campaign evolve` appends a sidecar to a clean static graph run without modifying its existing artifacts.

## Reproducibility

Named random streams isolate entity, behavior, payment, lifecycle, fraud, quality, graph, and benchmark generation. The same validated configuration and seed produce equivalent identities, records, ordering, schemas, and manifests. Scenario generation never depends on the current clock or uncontrolled global randomness.

Validate before every significant run:

```bash
poetry run fraudtwin config validate path/to/config.yaml
```
