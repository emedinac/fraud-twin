# Release notes

## 0.7.0 — Milestone 7

- Added deterministic fraud alerts, fraud cases, case confirmations, customer dispute events, and delayed fraud labels derived from M6 scenario records and hard negatives.
- Added explicit fraud occurrence, investigation, and label-availability timestamps with causal IDs and relationship validation.
- Added strict fraud workflow probabilities and timing configuration, typed M7 Parquet schemas, and alert/case/confirmation/dispute/label manifest counts.
- Preserved CARD, PIX-like, account-transfer, lifecycle, fraud-scenario, hard-negative, ledger, and reconciliation behavior; deferred M8 data-quality injection and all later infrastructure.

## 0.6.0 — Milestone 6

- Added deterministic F01 Card Not Present, F02 Card Testing, F03 Account Takeover, F04 Instant-Payment Scam, and F05 Velocity Attack scenario generation.
- Added scenario metadata to the existing payment event envelope and a typed `fraud_records.parquet` output with explainable truth records and hard negatives.
- Added isolated fraud RNG streams, strict scenario configuration validation, fraud event/record manifest counts, and realized per-scenario rates.
- Preserved legitimate CARD, PIX-like, account-transfer, lifecycle, ledger, and reconciliation behavior when fraud generation is disabled.
- Deferred delayed labels, alerts, cases, customer disputes, chargebacks, and all later milestones.

## 0.5.0 — Milestone 5

- Added deterministic PIX initiation, validation, authorization, submission, settlement, receipt, rejection, and return lifecycle events.
- Added strict PIX lifecycle probabilities and timing configuration.
- Added double-sided transfer ledger entries, running balances, reconciliation validation, and the `validate-ledger` CLI command.
- Added PIX and ledger counts to manifests while retaining empty fraud counts.

## 0.4.0 — Milestone 4

- Added deterministic card authorization, decline, reversal, capture, clearing, settlement, and refund lifecycle events.
- Added configurable card approval, reversal, and refund probabilities plus lifecycle timing delays with strict validation.
- Extended payment event manifests and schema versioning while keeping payment records separate from their event stream.

## 0.3.0 — Milestone 3

- Added deterministic, customer-specific behavioral profiles covering spending level, payment hours, weekday preferences, countries, merchant categories, devices, income, online purchases, travel, and card-versus-transfer preference.
- Added minimal typed payment, payment-event, and ledger primitives required for legitimate behavior generation.
- Added legitimate CARD, PIX-like, and account-transfer payments with stable IDs, relationship validation, temporal timestamps, and explicit Parquet schemas.
- Added behavior/payment counts to manifests while preserving empty fraud counts.
- Added deterministic profile/payment tests with a lightweight 100-payment behavior smoke test.

## 0.2.0 — Milestone 2

- Added deterministic synthetic entity generation for customers, institutions, accounts, cards, merchants, devices, and PIX-like keys.
- Added typed Polars Parquet output, entity counts in manifests, relationship validation, and an opt-in 100k-customer smoke test.

## 0.1.0 — Milestone 1

Initial repository foundation:

- Python 3.12 Poetry package
- Pydantic configuration validation from YAML
- Typer CLI with `config validate` and `generate`
- Deterministic seed handling and configuration hashing
- Reproducible run manifests
- Pytest, Ruff, mypy, coverage, and GitHub Actions CI

Entity, payment, fraud, streaming, and ML functionality are planned for subsequent milestones.
