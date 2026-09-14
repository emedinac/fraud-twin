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

## Synthetic financial behavior for testing fraud systems

FraudTwin creates a small, coherent financial world that behaves more like a real payment environment than a table of random transactions. It generates customers, accounts, cards, merchants, devices, customer behavior profiles, and legitimate payment events in reproducible batch runs.

The project is designed for fraud engineers, data scientists, ML engineers, and data teams who need realistic relationships and temporal patterns before introducing data-quality faults, streaming, or production infrastructure.

## Current release: 0.13.0 — Fraud Difficulty Engine (Milestone 12)

Milestones 1 through 10 are complete. Every generated customer receives a deterministic
behavior profile, and payments reflect customer-specific preferences for:

- Spending level, income, and monthly budget
- Typical payment hours and weekday activity
- Merchant categories and countries
- Card versus transfer usage
- Online purchases and trusted devices
- Travel frequency

The current release generates legitimate CARD, PIX-like, and account-transfer payments, plus explicit F01 Card Not Present, F02 Card Testing, F03 Account Takeover, F04 Instant-Payment Scam, and F05 Velocity Attack scenarios when fraud generation is enabled. Fraud events retain the existing event envelope and carry scenario ID, type, trigger, reason, and affected entities. The fraud record table contains scenario-linked truth records and legitimate hard negatives. M7 derives fraud alerts, cases, confirmations, customer dispute events, and delayed labels from those records. Confirmed-case losses reflect realized settled payment or net ledger activity; declined, reversed, and refunded attempts contribute no loss. Latent truth remains in the oracle `fraud_records.parquet` table; operational M7 projections write copied truth fields as null, and unresolved cases do not produce labels. M8 adds deterministic duplicates, missing optional fields, invalid values, late and out-of-order events, source delay, source outages, scheduled schema changes, fraud spikes, traffic spikes, and measured quality-fault counts. Card chargebacks are supported as a configurable lifecycle.

M9 adds a local point-in-time dataset builder over those generated records. Historical features use only source records whose `source_available_at` is no later than the row's `prediction_time`; business `event_time` remains separate from feature availability. Labels are included only after their configured `label_available_at` and label delay, or can be retained as unresolved rows for inspection. M10 adds deterministic replay from an existing run, rolling PIT backtests, source-history fraud regimes, and versioned benchmark packs without regenerating the financial world during replay or analysis.

M11 adds an opt-in graph-fraud source mode and a read-only graph exporter. Graphs preserve source IDs and event timestamps, provide separate observable and oracle views, derive shared-device/IP relationships only from supporting events, and write deterministic Parquet and Neo4j artifacts. Enable graph campaigns in a new run with a `graph:` section, then export with `fraudtwin graph export --run-id <run-id> --output-dir runs`.

M12 adds an opt-in difficulty engine above the existing fraud and graph
scenarios. Set `benchmark.difficulty` from 1 (obvious) through 10 (subtle),
then optionally replace individual normalized controls for fraud/legitimate
overlap, behavioral deviation, scenario subtlety, hard-negative noise,
prevalence, temporal irregularity, or graph structural subtlety. The resolved
profile and effective hash are recorded in manifests. Active M12 operational
events omit direct scenario truth fields; complete truth remains available in
the oracle artifacts. Runs without an active benchmark difficulty section keep
the legacy stream and identities unchanged.

## Quick start

Requirements: Python 3.12 and Poetry 2.x.

```bash
poetry install
poetry run fraudtwin config validate configs/minimal.yaml
poetry run fraudtwin generate configs/minimal.yaml
```

The command prints the run ID and output location. The default configuration generates 10 customers, 10 behavior profiles, 100 payments, and a variable number of payment events because each card payment may produce several lifecycle events.

To choose another output directory:

```bash
poetry run fraudtwin generate configs/minimal.yaml --output-dir output
```

## Generated dataset

Each run is written under `runs/<run_id>/`:

```text
runs/<run_id>/
├── manifest.json
├── entities/
│   ├── customers.parquet
│   ├── accounts.parquet
│   ├── cards.parquet
│   ├── merchants.parquet
│   ├── devices.parquet
│   ├── institutions.parquet
│   └── pix_keys.parquet
├── behavior/
│   └── behavior_profiles.parquet
├── payments/
│   ├── payments.parquet
│   └── payment_events.parquet
├── ledger/
│   └── ledger_entries.parquet
├── fraud/
│   ├── fraud_records.parquet
│   ├── fraud_alerts.parquet
│   ├── fraud_cases.parquet
│   ├── case_confirmations.parquet
│   ├── customer_disputes.parquet
│   └── fraud_labels.parquet
├── oracle/                 # active M12 runs only
│   ├── behavior/
│   │   └── behavior_profiles.parquet
│   ├── payments/
│   │   ├── payments.parquet
│   │   └── payment_events.parquet
│   ├── ledger/
│   │   └── ledger_entries.parquet
│   ├── fraud/
│   │   ├── fraud_records.parquet
│   │   └── fraud_labels.parquet
│   └── graph/
│       ├── campaigns.parquet
│       ├── campaign_memberships.parquet
│       ├── patterns.parquet
│       └── graph_evidence.parquet
└── ml/
    ├── dataset.parquet
    ├── dataset_manifest.json
    └── backtests/
        └── <backtest_id>/
            ├── fold_rows.parquet
            ├── fold_metrics.parquet
            └── backtest_manifest.json
```

All Parquet files use explicit, stable schemas and column ordering. Generated payments reference existing accounts, cards, merchants, devices, and customers. Amounts are positive, payment timestamps stay within the configured simulation window, delayed workflow timestamps follow their causal evidence, and no real personal data or payment credentials are used.

The manifest records the seed, configuration hash, schema versions, entity counts, payment/lifecycle/ledger counts, fraud event/record counts, M7 alert/case/confirmation/dispute/label counts, per-scenario fraud rates, M8 quality-fault counts and requested/realized rates, and dataframe-level quality diagnostics. Active M12 manifests additionally record requested and resolved difficulty, per-scenario transformations, effective configuration hash, source snapshot, schema/output fingerprints, and measured overlap, deviation, prevalence, noise, timing, and graph summaries. M8 diagnostics use Pandera schemas for clean and intentionally corrupted tables and report SDMetrics-aligned validity, structure, key-uniqueness, envelope, delivery-order, and relationship measurements. The default configuration uses the clean quality profile and keeps fraud disabled, so it produces the same legitimate CARD, PIX-like, and account-transfer behavior as the previous release.

Build or rebuild the M9 dataset from an existing run with:

```bash
poetry run fraudtwin ml build-dataset configs/minimal.yaml \
  --run-id <run-id> --output-dir runs
```

The dataset manifest records the source run, configuration parameters, feature and label definitions, temporal split boundaries, stable schema columns, row hash, schema fingerprint, output fingerprint, and deterministic ordering. Historical windows are anchored to each row's `prediction_time`; source events and ledger entries must be available by that cutoff, while labels must satisfy `label_available_at <= prediction_time`. Use `--label-delay-aware` to exclude unresolved labels.

M9 settings live under `dataset`:

```yaml
dataset:
  enabled: true
  prediction_delay_seconds: 0
  label_delay_seconds: 3600
  unresolved_labels: exclude  # exclude or include
  splits:
    train_fraction: 0.70
    validation_fraction: 0.15
    test_fraction: 0.15
    label_delay_gap_seconds: 3600  # defaults to the configured label delay
```

Feature windows are explicit positive-second values keyed by the documented
feature names. Unknown fields, naive timestamps, invalid ranges, split
fractions, windows, and label delays are rejected during configuration
validation.

Replay an existing run over a half-open historical period with either business-time or recorded-delivery ordering:

```bash
poetry run fraudtwin replay --run-id <run-id> \
  --from 2026-01-01T00:00:00Z --to 2026-01-02T00:00:00Z \
  --order event_time_order --output-dir runs
```

The replay artifact copies the selected domain records and referenced entities without regeneration. Original event and source timestamps remain unchanged; `replay_sequence` is replay-only metadata. Use `original_delivery` to order by recorded ingestion and processing timestamps.

Rolling backtests use the optional `backtest` section of the simulation YAML. Durations accept seconds or compact values such as `3d`, `12h`, and `30m`:

```yaml
backtest:
  train_mode: expanding
  validation_window_seconds: 1d
  test_window_seconds: 1d
  label_maturity_gap_seconds: 1d
  step_seconds: 1d
  minimum_label_maturity_policy: exclude
```

Rolling test windows must not overlap, so `step_seconds` must be at least the
test-window duration. Versioned benchmark packs carry their own immutable
label-maturity gap; their train, validation, test, and stress windows must be
chronological, and the train/validation and validation/test boundaries must
include that gap. A regime can use `label_observation_policy: unobserved` to
withhold its generated labels while retaining the case and latent truth history.

Run the folds over an existing source run with `poetry run fraudtwin ml backtest config.yaml --run-id <run-id> --output-dir runs`. Versioned fixed-window definitions are available under `configs/benchmarks/` and can be supplied with `--benchmark-pack`.

## Why behavior matters

Fraud detection depends on understanding what is normal for a customer. A payment at 03:00, from a new device, in an unusual merchant category, may be ordinary for one customer and suspicious for another.

FraudTwin makes that context available in the generated data through customer-specific profiles and deterministic temporal behavior. This provides a useful foundation for future anomaly and fraud scenarios without mixing fraud logic into the legitimate baseline.

## Reproducibility

Runs are controlled by the validated YAML configuration and simulation seed. Named, isolated random-number streams keep profile generation independent from entity generation, payment generation, lifecycle generation, each fraud scenario campaign, and each M8 fault. With the same configuration and seed, profiles, payment records, event records, fraud records, quality mutations, IDs, ordering, and schemas are equivalent. Scenario and quality generation never uses current time or global uncontrolled randomness.

Behavior settings can be adjusted under `behavior`:

```yaml
behavior:
  amount_min: 1.00
  amount_max: 5000.00
  active_hours: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23]
  weekday_weights: [1.0, 1.0, 1.0, 1.0, 1.0, 0.85, 0.70]
  merchant_preference_count: 3
  preferred_device_limit: 3
```

Unknown fields, invalid amounts, invalid hours, invalid weights, and invalid counts are rejected during configuration validation.

Card lifecycle settings can be adjusted under `card_lifecycle`:

```yaml
card_lifecycle:
  authorization_approval_probability: 0.90
  reversal_probability: 0.05
  refund_probability: 0.10
  authorization_delay_seconds: 1
  capture_delay_seconds: 5
  clearing_delay_seconds: 30
  settlement_delay_seconds: 60
  reversal_delay_seconds: 10
  refund_delay_seconds: 60
```

Probabilities must be between 0 and 1, and timing values must be non-negative whole seconds. Card lifecycle choices use an isolated deterministic random stream, so adding lifecycle events does not change PIX or account-transfer choices.

PIX lifecycle settings can be adjusted under `pix_lifecycle`:

```yaml
pix_lifecycle:
  authorization_approval_probability: 0.98
  rejection_probability: 0.02
  return_probability: 0.05
  validation_delay_seconds: 1
  authorization_delay_seconds: 1
  submission_delay_seconds: 1
  settlement_delay_seconds: 1
  receipt_delay_seconds: 1
  return_request_delay_seconds: 60
  return_delay_seconds: 60
```

PIX lifecycle choices use their own deterministic random stream. PIX
settlements, returns, and account-transfer completions produce double-sided
entries in `ledger/ledger_entries.parquet`. Validate a generated run with:

```bash
poetry run fraudtwin validate-ledger --run-id <run-id>
```

Fraud generation can be enabled with explicit scenario controls:

```yaml
fraud:
  enabled: true
  target_rate: 1.0
  scenario_count: 5
  hard_negative_rate: 1.0
  scenarios:
    F01:
      weight: 1.0
    F02:
      weight: 1.0
      attempt_count: 20
    F03:
      weight: 1.0
    F04:
      weight: 1.0
    F05:
      weight: 1.0
      attempt_count: 20
      window_seconds: 60
```

`target_rate` limits the number of selected scenario campaigns relative to
the legitimate payment count; scenario constraints can make the realized
record rate lower. Each enabled campaign creates a corresponding legitimate
lookalike when `hard_negative_rate` is greater than zero. Unknown fields,
invalid probabilities, counts, amounts, durations, and windows are rejected
before generation.

M7 workflow settings live under `fraud_workflow`. By default, every M6 fraud
record and hard negative produces an automated alert, case, confirmation, and
delayed label; true fraud records also produce a customer dispute event. The
workflow delays are whole non-negative seconds, and probabilities are bounded
between 0 and 1. A label is available only after the case evidence has been
processed, while `fraud_occurred_at` remains the original scenario occurrence
time.

M8 quality settings live under `quality`:

```yaml
quality:
  profile: clean  # clean, realistic, or hostile
  duplicate_record_probability: 0.0
  duplicate_event_probability: 0.0
  missing_optional_probability: 0.0
  invalid_value_probability: 0.0
  late_event_probability: 0.0
  out_of_order_probability: 0.0
  fraud_spike_probability: 0.0
  traffic_spike_probability: 0.0
  late_event_delay_seconds: 3600
  source_delay_seconds: 0
  fraud_spike_multiplier: 2
  traffic_spike_multiplier: 2
```

The clean profile disables all faults. The realistic and hostile profiles
provide bounded defaults, while explicit probabilities override one fault at a
time. Duplicate rows retain their original IDs and causal metadata. Missing
values are selected only from optional payment/event fields. Invalid values,
late transport timestamps, and delivery reordering are intentional quality
faults; the clean run remains suitable for lifecycle and ledger validation.
The manifest records `quality_fault_counts` plus requested and realized values
in `quality_fault_rates`.

## Difficulty benchmarks (M12)

Use the versioned fixture to generate a boundary-oriented fraud dataset:

```bash
poetry run fraudtwin config validate configs/benchmarks/m12-difficulty-v1.yaml
poetry run fraudtwin generate configs/benchmarks/m12-difficulty-v1.yaml \
  --output-dir /tmp/fraudtwin-m12
```

The simple level form is:

```yaml
benchmark:
  difficulty: 7
```

Difficulty is derived from generator parameters. Higher levels increase the
resolved strengths of legitimate-cohort overlap, behavioral similarity,
scenario subtlety, hard-negative noise, temporal irregularity, and graph
structural subtlety while retaining each scenario objective and topology.
Individual controls replace only their matching level value:

```yaml
benchmark:
  difficulty: 7
  controls:
    temporal_irregularity: 0.9
    prevalence: 0.6
```

All controls are normalized to `[0, 1]`, and overrides require a difficulty
level. `resolve_difficulty` and `apply_difficulty` are available from the
public Python API. M12 uses isolated deterministic streams, preserves full
truth in oracle artifacts, and removes direct scenario/linkage fields from
active-run operational payment events. The oracle records retain the original
IDs, timestamps, campaign membership, graph evidence, and fraud labels.

## Project status

| Capability | Status |
| --- | --- |
| Synthetic customers and financial entities | Available |
| Customer behavior profiles | Available |
| Legitimate payment records and events | Available |
| Typed Parquet batch output | Available |
| Reproducible manifests and seed handling | Available |
| Fraud scenarios and ground truth records | Available |
| Full card lifecycle, including chargebacks | Available |
| PIX-like lifecycle and transfer ledger | Available |
| Delayed labels, alerts, cases, confirmations, and disputes | Available |
| Deterministic M8 data-quality faults, outages, schema changes, and measurements | Available |
| Point-in-time historical ML dataset and delayed labels | Available |
| Deterministic replay, rolling PIT backtesting, regimes, and benchmark packs | Available |
| Opt-in M11 graph-fraud scenarios, oracle truth, Parquet and Neo4j exports | Available |
| Opt-in M12 difficulty levels, controls, oracle separation, and summaries | Available |
| Kafka, PostgreSQL, Flink, feature stores, and advanced models | Planned |

FraudTwin is being built milestone by milestone. The priority is a correct, readable, reproducible simulation core before adding distributed systems or advanced modeling.

## Development

Run the normal quality checks with:

```bash
poetry check --strict
poetry run ruff check .
poetry run ruff format --check .
poetry run mypy src
poetry run pytest
poetry build
```

See [`DEVELOPMENT.md`](DEVELOPMENT.md) for contributor workflows and
[`FEATURES.md`](FEATURES.md) for the complete specification and roadmap.

## License and related work

FraudTwin is released under the [Apache License 2.0](LICENSE).

The project is informed by work such as [SantanderAI/gen-fraud-graph](https://github.com/SantanderAI/gen-fraud-graph) and [synthfin-core](https://github.com/afborda/synthfin-core). FraudTwin's focus is the reproducible system around a payment: entities, behavior, relationships, event timing, and the path toward realistic fraud-data workflows.

M11 is enabled only with a strict `graph.scenarios` list. It provides
deterministic mule, ring, fan-in/out, layered, shared-infrastructure,
dense-community, merchant-community, and control topologies. Graph export is
read-only and writes separate observable and oracle views. Neo4j artifacts are
dependency-free CSV/Cypher; PyTorch Geometric is optional (`poetry install -E graph`).

For release history, see [`CHANGELOG.md`](CHANGELOG.md).
