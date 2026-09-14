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
| `card_lifecycle` | Card authorization, capture, clearing, settlement, refunds, and reversals |
| `pix_lifecycle` | Pix-like validation, authorization, settlement, rejection, and return timing |
| `fraud` | Scenario selection, prevalence, weights, and hard negatives |
| `fraud_workflow` | Alerts, cases, confirmations, disputes, and label delays |
| `quality` | Deterministic duplicates, missing values, invalid values, delays, and spikes |
| `dataset` | Point-in-time feature and label construction |
| `backtest` | Rolling train, validation, test, and stress windows |
| `graph` | Opt-in fraud-network campaigns and graph export inputs |
| `benchmark` / `stress` | Difficulty and camouflage controls |
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

Use `quality.profile: clean` for a baseline, `realistic` for bounded faults, or `hostile` for a more demanding stream. Individual probabilities and delays can override a profile. Faults include duplicates, missing optional fields, invalid values, late or out-of-order delivery, source delay, outages, schema changes, fraud spikes, and traffic spikes.

```yaml
quality:
  profile: realistic
  duplicate_event_probability: 0.02
  late_event_probability: 0.05
  source_delay_seconds: 30
```

Quality mutations are deterministic and audited in the manifest. They do not silently change the clean lifecycle or ledger path.

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
