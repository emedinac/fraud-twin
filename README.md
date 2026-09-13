# FraudTwin

[![CI: GitHub Actions](https://img.shields.io/badge/CI-GitHub%20Actions-2088FF?logo=github-actions&logoColor=white)](.github/workflows/ci.yml)
[![Python](https://img.shields.io/badge/python-3.12%2B-3776AB.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-Apache--2.0-green.svg)](LICENSE)

## Synthetic financial behavior for testing fraud systems

FraudTwin creates a small, coherent financial world that behaves more like a
real payment environment than a table of random transactions. It generates
customers, accounts, cards, merchants, devices, customer behavior profiles,
and legitimate payment events in reproducible batch runs.

The project is designed for fraud engineers, data scientists, ML engineers,
and data teams who need realistic relationships and temporal patterns before
introducing fraud, streaming, or production infrastructure.

## Current release: 0.4.0 — Card Lifecycle

Milestones 3 and 4 are complete. Every generated customer receives a deterministic
behavior profile, and payments reflect customer-specific preferences for:

- Spending level, income, and monthly budget
- Typical payment hours and weekday activity
- Merchant categories and countries
- Card versus transfer usage
- Online purchases and trusted devices
- Travel frequency

The current release generates legitimate CARD, PIX-like, and account-transfer
payments. Card payments produce ordered authorization, approval or decline,
capture, clearing, settlement, reversal, and refund events as permitted by
their configured lifecycle. PIX and account-transfer events retain their M3
behavior. Fraud, chargebacks, and fraud labels are intentionally not included.

## Quick start

Requirements: Python 3.12 and Poetry 2.x.

```bash
poetry install
poetry run fraudtwin config validate configs/minimal.yaml
poetry run fraudtwin generate configs/minimal.yaml
```

The command prints the run ID and output location. The default configuration
generates 10 customers, 10 behavior profiles, 100 payments, and a variable
number of payment events because each card payment may produce several
lifecycle events.

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
└── payments/
    ├── payments.parquet
    └── payment_events.parquet
```

All Parquet files use explicit, stable schemas and column ordering. Generated
payments reference existing accounts, cards, merchants, devices, and
customers. Amounts are positive, timestamps stay within the configured
simulation window, and no real personal data or payment credentials are used.

The manifest records the seed, configuration hash, schema versions, entity
counts, payment counts, and empty fraud counts for this milestone.

## Why behavior matters

Fraud detection depends on understanding what is normal for a customer. A
payment at 03:00, from a new device, in an unusual merchant category, may be
ordinary for one customer and suspicious for another.

FraudTwin makes that context available in the generated data through
customer-specific profiles and deterministic temporal behavior. This provides
a useful foundation for future anomaly and fraud scenarios without mixing
fraud logic into the legitimate baseline.

## Reproducibility

Runs are controlled by the validated YAML configuration and simulation seed.
Named, isolated random-number streams keep profile generation independent from
entity generation and payment generation. With the same configuration and
seed, profiles, payment records, event records, IDs, ordering, and schemas are
equivalent.

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

Unknown fields, invalid amounts, invalid hours, invalid weights, and invalid
counts are rejected during configuration validation.

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

Probabilities must be between 0 and 1, and timing values must be non-negative
whole seconds. Card lifecycle choices use an isolated deterministic random
stream, so adding lifecycle events does not change PIX or account-transfer
choices.

## Project status

| Capability | Status |
| --- | --- |
| Synthetic customers and financial entities | Available |
| Customer behavior profiles | Available |
| Legitimate payment records and events | Available |
| Typed Parquet batch output | Available |
| Reproducible manifests and seed handling | Available |
| Fraud scenarios and labels | Planned |
| Full card lifecycle (without chargebacks) | Available |
| Chargebacks | Planned |
| Kafka, PostgreSQL, Flink, and ML workflows | Planned |

FraudTwin is being built milestone by milestone. The priority is a correct,
readable, reproducible simulation core before adding distributed systems or
advanced modeling.

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

The project is informed by work such as
[SantanderAI/gen-fraud-graph](https://github.com/SantanderAI/gen-fraud-graph)
and [synthfin-core](https://github.com/afborda/synthfin-core). FraudTwin's
focus is the reproducible system around a payment: entities, behavior,
relationships, event timing, and the path toward realistic fraud-data
workflows.

For release history, see [`CHANGELOG.md`](CHANGELOG.md).
