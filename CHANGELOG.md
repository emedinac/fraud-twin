# Release notes

## Unreleased — M18 bounded-scale contract

- Made canonical payment rows the scale target, added the bounded `dev` smoke
  profile, derived row-count reporting, target validation, versioned checkpoints,
  chunk markers for interrupted resume, and partition-aware readers.
- Added deterministic payment ordinal helpers, incremental fingerprints,
  cross-account payee reconciliation markers, benchmark evidence manifests, and
  optional PyArrow/DuckDB scale tooling.
- Added table-qualified entity chunks, physical chunk checksum validation on
  resume, a completed-run resume fast path, lazy partition-table readers, and
  optional M22 ingestion of M18 scale evidence manifests.

## 0.32.0 — Milestone 27 Minimal Observability

- Added optional Prometheus metrics for generated runs and ledger validation,
  with run-level generation, fraud, error, throughput, and data-quality
  signals.
- Added a local Prometheus/Grafana Compose profile with provisioned dashboards,
  loopback-only ports, explicit 15-day Prometheus retention, and required
  non-default Grafana credentials.
- Preserved deterministic generator identities, manifests, contracts, Kafka,
  and Iceberg outputs; metrics are disabled unless explicitly requested.

## 0.31.0 — Milestone 26 Lakehouse

- Added optional MinIO/Iceberg lakehouse publication with immutable Bronze,
  deterministic Silver, reproducible Gold, and an isolated opt-in oracle
  namespace.
- Added complete-run Parquet backfill, bounded M25 Kafka ingestion, snapshot
  lineage manifests, local lakehouse Compose services, and lakehouse CLI
  workflows without changing generator identities.

## 0.30.0 — Milestone 25 Native Kafka Streaming

- Added optional native Kafka publication for all six M24 observable Avro subjects with remote Schema Registry reconciliation, idempotent delivery, deterministic ordering, pacing, and run-manifest fingerprints.
- Added clean-contract enforcement, stable topic/key/header conventions, streaming Compose services, and focused producer, registry, ordering, pacing, and identity-regression tests.

## 0.29.0 — Milestone 24 Avro Contracts + Schema Registry

- Added six bundled observable operational Avro contracts with deterministic parsing-canonical fingerprints and a source-controlled `FULL_TRANSITIVE` registry.
- Added `fraudtwin schema validate`, typed timestamp/decimal datum mapping, compatibility checks, and wheel-packaged registry assets.
- Preserved Parquet, PostgreSQL, oracle/observable boundaries, M8 quality mutations, and existing generator identities; Kafka publication and remote registry integration remain deferred to M25.


## Milestone 23 PostgreSQL Operational Mode

- Added an optional transactional PostgreSQL operational mirror with versioned SQL migrations and `fraudtwin db migrate/status` commands.
- Preserved deterministic file outputs and generator identities when PostgreSQL is enabled; added run-scoped relational constraints, provenance, idempotent writes, and observable fraud-case persistence.
- Added the optional `postgres` dependency extra, PostgreSQL workflow documentation, and M23 compatibility/invariant tests.

## Milestone 22 Generator Quality Benchmark

- Added immutable `standard-v1` quality profiles covering all eight M21 public packs, with opt-in medium, large, xlarge, and billion variants.
- Added `fraudtwin quality-benchmark` and `fraudtwin report` commands with separate correctness, fidelity, difficulty, scalability, engineering, and reproducibility sections.
- Added normalized external generator adapter/bundle contracts with capability-aware `N/A` reporting.

## 0.26.0 — Milestone 21 Versioned Public Benchmark Packs

- Added eight immutable, bundled FT-B01–FT-B08 public benchmark packs with semantic versions, compatibility checks, fixed PIT windows, calibration provenance, and logical fingerprints.
- Added `fraudtwin benchmark run` and `fraudtwin benchmark describe` while preserving the generic M20 benchmark command.

## 0.25.0 — Milestone 20 Fraud Stress Benchmark

- Added reproducible baseline, temporal, boundary, camouflage, graph, observability, calibrated, mixed, and all-suite benchmark orchestration.
- Added PIT-safe model-result tables, generator descriptors, latent/observed truth catalogs, fixed split lineage, and framework-neutral external model runners.
- Added the `fraudtwin benchmark` CLI with optional scikit-learn, LightGBM, XGBoost, CatBoost, and external framework adapters.
- Fixed event-level M19 evaluation rejecting unique predictions when customer or account identifiers repeated at the same timestamp.

## 0.24.0 — Milestone 19 Baseline ML + Evaluation Adapter

- Added deterministic Logistic Regression, LightGBM, XGBoost, and CatBoost baseline training over frozen point-in-time datasets.
- Added strict Parquet/JSONL external prediction evaluation, required fraud metrics and segments, model artifacts, and manifest lineage with optional MLflow tracking.
- Preserved M1–M18 generation, label-observation, scale, and legacy heuristic backtest behavior.

## 0.23.0 — Milestone 18 Large-Scale Deterministic Generation

- Added opt-in deterministic scale profiles with stable sharding, hierarchical seed streams, bounded chunked Parquet output, parallel scheduling, checkpoint/resume, partition fingerprints, and cross-partition reconciliation.
- Added the `fraudtwin resume` workflow and scale execution metadata while preserving canonical M1–M17 generation semantics.

## 0.22.0 — Milestone 17  Label Observation Engine

- Added deterministic, opt-in label observation with selective investigation, missing fraud, preliminary errors, corrections, reopenings, immutable history, and PIT-safe version resolution.
- Added typed observation APIs, append-only observable/oracle artifacts, provenance manifests, and strict configuration validation while preserving disabled-run M1–M16 output identity.

## 0.21.0 — Documentation

- Added a Sphinx/Furo documentation site with a hosted Python API reference and GitHub Pages deployment.

## 0.20.0 — Tutorial and Baseline Evaluation Workflows

- Added numbered tutorials covering configuration, payment lifecycles, fraud and delayed labels, point-in-time datasets, stress testing, and simple fraud scoring.
- Added an end-to-end stress-testing tutorial for replay, graph views, difficulty, camouflage, and counterfactual fraud.
- Added a dependency-free, transparent fraud-scoring baseline using point-in-time features with precision and recall evaluation across temporal splits.
- Improved tutorial explanations, references, direct links, and documentation navigation.

## 0.19.0 — Milestone 16 Reference Calibration & Fidelity

- Added strict Parquet reference loading, deterministic aggregate calibration profiles, profile serialization/reuse, custom calibration registries, and named calibration streams.
- Added calibrated generation controls for amount and account-balance distributions, typed fidelity reports, append-only calibration sidecars, provenance fingerprints, and CLI workflows.
- Preserved neutral M1–M15 identities and observable/oracle boundaries when calibration is disabled.

## 0.18.0 — Package convertion release

- Added a `pip install` procedure and first getting-started tutorial in ipynb.
- Added the high-level `fraudtwin.generate()` Python API for in-memory generation or persisted runs while preserving the existing CLI.
- Added a built-in minimal configuration so library users can call `fraudtwin.generate()` without repository-relative paths.

## 0.17.0 — Milestone 15 Advanced Campaign Dynamics

- Added strict opt-in evolving campaign profiles, deterministic phase transitions, bounded Hawkes and piecewise intensity models, actor/device rotation, cross-rail actions, topology mutations, higher-order graph lineage, and reproducible oracle sidecars.
- Added immutable M15 domain records, named streams, stable `M15-` IDs, and the
  append-only `campaign evolve` CLI while preserving M14 source ordering and M13 post-evolution processing.

## 0.16.0 — Methodology completion for Milestones 4, 5 and 8

- Completed strict M4/M5 lifecycle gaps with explicit card initiation, PIX timeout handling, direct settled-return transitions, contract version 5, and legacy v1-v4 read/replay compatibility.
- Expanded M8 with independently configurable deterministic fault types, mutation audit records, diagnostics, raw encoding artifacts, partition-skew assignments, and serialized schema evolution with compatibility metadata.

## 0.15.0 — Milestone 14 Counterfactual Fraud Generation

- Added strict opt-in counterfactual configuration with deterministic source selection, weighted distance budgets, immutable source lineage, and explicit infeasibility records.
- Added M6 and M11 objective requests, isolated M14 streams, derived IDs, and original/modified observable plus oracle sidecar artifacts.
- Added integrated and standalone counterfactual CLI workflows, the M14 benchmark fixture, public resolver/generator APIs, and reproducibility metadata without changing inactive M1–M13 identities.

## 0.14.0 — Milestone 13 Camouflage Engine

- Added strict opt-in feature and relation camouflage controls with global, fraud-family, graph-family, and per-signal overrides.
- Added deterministic legitimate cohort selection, isolated M13 seed streams, measurable similarity summaries, feasibility caps, and observable/oracle truth separation.
- Added lineage-valid benign graph support events while preserving fraud campaign induced topology, lifecycle, ledger, workflow, and oracle truth.
- Added source, dataset, and graph camouflage provenance, the M13 fixture, focused regression tests, and public resolver APIs.

## 0.13.0 — Milestone 12 Fraud Difficulty Engine

- Added strict opt-in difficulty levels 1–10 with per-dimension override controls for overlap, behavioral deviation, scenario subtlety, hard-negative noise, prevalence, temporal irregularity, and graph structure.
- Added deterministic `resolve_difficulty` and `apply_difficulty` APIs with isolated streams, explicit per-scenario transformations, and effective configuration hashes.
- Added difficulty-aware M6/M11 generation while preserving fraud objectives, graph topology, ledger reconciliation, temporal ordering, and oracle truth.
- Added active-run oracle separation, source/dataset/graph difficulty metadata, measured summaries, the M12 fixture, focused tests, and documentation.

## 0.12.0 — Milestone 11 Graph Fraud

- Added schema-v2 opt-in deterministic graph scenarios: mule, cyclic ring, beneficiary, fan-in/out, bipartite, stacked, scatter/gather, shared infrastructure, dense and merchant communities, and labelled controls.
- Added temporal observable/oracle graph construction, evidence and hyperedge incidence artifacts, structural validation, typed Parquet output, bulk-ready Neo4j CSV/Cypher artifacts, and optional PyTorch Geometric export.
- Added graph configuration, source/export manifest metadata, M11 fixture configuration, CLI workflow, public graph APIs, and focused regression tests.

## 0.11.0 — Methodology completion for Milestones 1–8

- **Milestone 1:** added optional effective-dated customer/account state history with deterministic Parquet output and compatible merchant-acquirer selection.
- **Milestone 2:** enforced active account/card eligibility and card/account spend limits during payment generation.
- **Milestone 3:** made spending-level segment weights configurable and added beginning/end-of-month, payday, holiday, travel-period, and merchant-hour temporal controls.
- **Milestone 4:** added configurable card chargeback creation/resolution and card lifecycle participant references.
- **Milestone 8:** added deterministic source-outage and scheduled schema-change injectors, per-fault audit metadata, and an exported pristine oracle snapshot before corruption.
- **Milestone 10:** replay now carries selected entity state-history records through the immutable replay artifact.

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
