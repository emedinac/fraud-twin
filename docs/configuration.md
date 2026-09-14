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
| `quality` | Deterministic typed faults, delivery defects, outages, schema changes, and spikes |
| `dataset` | Point-in-time feature and label construction |
| `backtest` | Rolling train, validation, test, and stress windows |
| `graph` | Opt-in fraud-network campaigns and graph export inputs |
| `benchmark` / `stress` | Difficulty and camouflage controls |
| `counterfactual` | Opt-in minimum-change fraud trajectories and lineage sidecars |
| `outputs` | Parquet and optional integration outputs |

Unknown fields and invalid ranges are rejected during validation. That strict boundary is intentional: a run should fail before it produces ambiguous data.

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

M14 is disabled by default and has no effect on legacy hashes or output files.
Enable it with explicit M6/M11 objective requests:

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
stable IDs make repeated runs identical. M14 searches for a feasible minimum
change, applies the resolved M12 amount/timing constraints, and uses isolated
M13 camouflage controls at the M14 boundary. Every request records its budget,
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

## Reproducibility

Named random streams isolate entity, behavior, payment, lifecycle, fraud, quality, graph, and benchmark generation. The same validated configuration and seed produce equivalent identities, records, ordering, schemas, and manifests. Scenario generation never depends on the current clock or uncontrolled global randomness.

Validate before every significant run:

```bash
poetry run fraudtwin config validate path/to/config.yaml
```
