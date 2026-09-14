# Release notes

## 0.12.0 — Milestone 11 Graph Fraud

- Added schema-v2 opt-in deterministic graph scenarios: mule, cyclic ring,
  beneficiary, fan-in/out, bipartite, stacked, scatter/gather, shared
  infrastructure, dense and merchant communities, and labelled controls.
- Added temporal observable/oracle graph construction, evidence and hyperedge
  incidence artifacts, structural validation, typed Parquet output, bulk-ready
  Neo4j CSV/Cypher artifacts, and optional PyTorch Geometric export.
- Added graph configuration, source/export manifest metadata, M11 fixture
  configuration, CLI workflow, public graph APIs, and focused regression tests.

## 0.11.0 — Methodology completion for Milestones 1–8

- **Milestone 1:** added optional effective-dated customer/account state history
  with deterministic Parquet output and compatible merchant-acquirer selection.
- **Milestone 2:** enforced active account/card eligibility and card/account
  spend limits during payment generation.
- **Milestone 3:** made spending-level segment weights configurable and added
  beginning/end-of-month, payday, holiday, travel-period, and merchant-hour
  temporal controls.
- **Milestone 4:** added configurable card chargeback creation/resolution and
  card lifecycle participant references.
- **Milestone 8:** added deterministic source-outage and scheduled schema-change
  injectors, per-fault audit metadata, and an exported pristine oracle snapshot
  before corruption.
- **Milestone 10:** replay now carries selected entity state-history records
  through the immutable replay artifact.

## 0.10.0 — Milestone 10

- Added deterministic historical replay from existing generated runs with half-open period selection, event-time ordering, original-delivery ordering, full domain-record preservation, and replay fingerprints.
- Added rolling fixed/expanding point-in-time backtests with label-maturity gaps, deterministic baseline metrics, fold manifests, source snapshots, and PIT validation metadata.
- Added explicit source-history fraud regimes that affect configured source generation; replay and backtesting remain read-only over that history and never create counterfactual records.
- Added versioned immutable benchmark-pack YAML definitions with fixed train, validation, test, and optional stress windows.
- Added `fraudtwin replay` and `fraudtwin ml backtest` commands, stable M10 Parquet schemas, focused tests, and documentation.

## 0.9.0 — Milestone 9

- Added a local, deterministic point-in-time ML dataset builder over existing M1-M8 records.
- Added historical transaction, merchant, device, account, and label-aware fraud features with separate business-event and feature-availability timestamps.
- Added configured label-delay enforcement, unresolved-label handling, reproducible temporal train/validation/test splits, and stable Parquet schemas.
- Added dataset manifests containing source-run lineage, configuration, split, feature, label, schema, and row-reproducibility metadata.
- Added `fraudtwin ml build-dataset` for building a dataset from an existing generated run without regenerating source records; M10 replay and backtesting remain deferred.
- Added Pandera-backed M8 dataframe validation, Hypothesis reproducibility properties, and SDMetrics-aligned validity, structure, uniqueness, and relationship diagnostics for clean and intentionally corrupted outputs.
- Hardened the M7 observation boundary: operational workflow outputs no longer expose copied latent truth, false-positive confirmations are distinct from confirmed fraud, persisted M7 relationships are revalidated before PIT dataset construction, and confirmed-case loss reflects realized payment or ledger activity.

## 0.8.0 — Milestone 8

- Added a separate deterministic chaos/data-quality layer for duplicate records and events, missing optional fields, invalid values, late events, out-of-order delivery, configurable source delay, fraud spikes, and traffic spikes.
- Added strict `clean`, `realistic`, and `hostile` quality profiles with independently configurable probabilities, delays, and spike multipliers.
- Added measured `quality_fault_counts` and requested/realized `quality_fault_rates` to manifests while preserving existing entity, event, fraud, lifecycle, ledger, and schema fields.
- Preserved the clean M1-M7 output path and deferred PostgreSQL, Kafka, Spark, schema registry, ML, point-in-time datasets, feature stores, and later milestones.

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
