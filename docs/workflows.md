# Data and evaluation workflows

**Level:** Intermediate<br><br>
**You will:** generate source data once, then build point-in-time datasets,<br><br>
replay windows, backtests, model evaluations, or operational projections.
**Before you start:** [Quickstart](quickstart.md) and [Concepts](concepts.md).<br><br>
**Services:** None for local workflows; integrations are optional.<br><br>

FraudTwin separates generation from analysis. Generate a source run once, then build datasets, replay windows, or run backtests over those records without regenerating the financial world.

## Build a point-in-time dataset

The dataset builder creates historical features and labels using only data that was available at each row’s `prediction_time`:

```bash
CONFIG=configs/minimal.yaml
RUN_ID=RUN-...
RUNS_DIR=./runs

poetry run fraudtwin ml build-dataset "$CONFIG" \
  --run-id "$RUN_ID" \
  --output-dir "$RUNS_DIR"
```

Source events and ledger entries are filtered by source availability. Labels are filtered by `label_available_at` and the configured label delay. Use `--label-delay-aware` when unresolved labels should be excluded rather than retained for inspection.

The dataset manifest records the source run, feature and label definitions, split boundaries, stable schema, row hash, and output fingerprint.

When label observation is enabled, inspect `label_observations/<id>/observable/observed_labels.parquet` for the operational projection and the corresponding oracle history for audit. Dataset construction filters each version by `label_available_at`; it never exposes future corrections or latent truth.

## Generate and resume large runs

Large-scale profiles use the same simulator and output contracts as ordinary
runs while writing deterministic shard/chunk Parquet artifacts and a checkpoint
manifest. The target is measured in canonical payment rows; lifecycle and
workflow rows are reported separately. Use the `dev` profile for a bounded
laptop smoke run. Worker count changes scheduling only. Resume validates the
stored configuration and seed tree, reuses valid completed chunks, and
regenerates incomplete work deterministically:

```bash
CONFIG=configs/scale-1b.yaml
CHECKPOINT_DIR=./runs/scale-1b-checkpoint

poetry run fraudtwin generate "$CONFIG" --workers 16 --checkpoint-dir "$CHECKPOINT_DIR"
poetry run fraudtwin resume "$CHECKPOINT_DIR"
```

The `billion` profile is a hardware-dependent benchmark target and is not part of unit, smoke, or CI validation.

The compatibility API retains in-memory entity/behavior objects, so use
`dev` for laptop checks. For 100M/1B jobs, connect a streaming canonical-row
producer on self-hosted SSD or S3/MinIO; the writer and checkpoint format are
partition-aware and resume-safe.

## Replay a historical window

Replay is read-only. It selects records from an existing run over a half-open interval and preserves their source identities and timestamps:

```bash
RUN_ID=RUN-...
RUNS_DIR=./runs

poetry run fraudtwin replay --run-id "$RUN_ID" \
  --from 2026-01-01T00:00:00Z \
  --to 2026-01-02T00:00:00Z \
  --order event_time_order \
  --output-dir "$RUNS_DIR"
```

Use `original_delivery` when the question is about ingestion and processing order rather than business event time. Replay adds sequence metadata without rewriting the source records.

## Run rolling backtests

Backtests use fixed or expanding training windows and explicit validation, test, stress, and label-maturity boundaries:

```bash
CONFIG=configs/minimal.yaml
RUN_ID=RUN-...
RUNS_DIR=./runs

poetry run fraudtwin ml backtest "$CONFIG" \
  --run-id "$RUN_ID" \
  --benchmark-pack configs/benchmarks/temporal-v1.yaml \
  --output-dir "$RUNS_DIR"
```

Windows are chronological and non-overlapping. A benchmark pack freezes its own label-maturity gap, regime policy, seed/configuration identity, and metric definition so later comparisons remain meaningful.

For the immutable public benchmark packs, use the separate public-pack
commands:

```bash
fraudtwin benchmark run FT-B04-CAMOUFLAGE@0.34.0
fraudtwin benchmark describe FT-B04-CAMOUFLAGE@0.34.0
fraudtwin benchmark verify runs/benchmarks/BM-<id>
```

These bundled definitions verify generator compatibility and logical output
fingerprints. The existing legacy `--benchmark-pack` option remains for backtests
over a previously generated run.

## Train baselines and evaluate external predictions

The baseline workflow trains Logistic Regression, LightGBM, XGBoost, and CatBoost models on the same frozen point-in-time feature allowlist. Install the optional model stack before training:

```bash
poetry install -E ml
RUN_ID=RUN-...
DATASET=./runs/$RUN_ID/ml/dataset.parquet
EVALUATIONS_DIR=./runs/ml-evaluations

poetry run fraudtwin ml train "$DATASET" \
  --config configs/ml-baselines.yaml \
  --output-dir "$EVALUATIONS_DIR"
```

External models can submit strict Parquet or JSONL records containing one event/payment/customer/account ID, a prediction timestamp, a fraud score, and an optional predicted class:

```bash
poetry run fraudtwin ml evaluate "$DATASET" predictions.jsonl
```

The evaluator reports ranking, threshold, calibration, monetary, detection-delay, and segment metrics. It resolves labels and features at each prediction timestamp and records an immutable evaluation manifest. MLflow is used when a tracking URI is configured; otherwise local artifacts are sufficient.

## Persist an operational run in PostgreSQL

PostgreSQL is an optional mirror; it never changes generated records or their
deterministic run ID. Install the extra, provide the connection string through
the environment, and apply the packaged migrations before enabling the sink:

```bash
poetry install -E postgres
export FRAUDTWIN_POSTGRES_DSN='postgresql://user:password@localhost:5432/fraudtwin'
fraudtwin db migrate
fraudtwin generate configs/minimal.yaml --output-dir runs
```

Set `outputs.postgres: true` in the configuration. Keep `outputs.parquet: true`
when you want the normal file artifacts as well; both sinks receive the same
observable records. PostgreSQL writes currently require `quality.profile:
clean`, are committed transactionally, and are immutable per `run_id` (a
matching repeat is an idempotent no-op). Credentials are never written to the
resolved configuration or manifest. `fraud_truth` is not exposed in the
operational database. Debezium CDC remains a separate platform track; native
Kafka publication is described below.

## Publish a native Kafka stream

Native Kafka publication sends the clean observable contracts directly to Kafka. The
bundled Avro registry remains authoritative; a remote Schema Registry is checked
for matching canonical fingerprints and `FULL_TRANSITIVE` compatibility before
any messages are sent.

Start the local broker and registry with `docker compose --profile streaming up`.
Install the optional client and provide connection settings through the
environment:

```bash
poetry install -E kafka
export FRAUDTWIN_KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export FRAUDTWIN_SCHEMA_REGISTRY_URL=http://localhost:8081
fraudtwin generate configs/minimal.yaml --output-dir runs
```

Set `outputs.kafka: true` and keep `quality.profile: clean`. The six topics use
`payment_id` as their partition key; ordering is guaranteed only within a topic
and key. Producers use idempotence and acknowledged delivery, but retries across
process restarts remain at-least-once, so consumers deduplicate using the stable
record ID header. `simulation.speed` controls publication pacing (`batch`,
real-time, or accelerated); it never changes generated domain content.

### Exercise delivery failures deterministically

The logical-message chaos harness is useful before a broker is available and for
repeatable integration tests. It preserves payloads and event identity while
injecting drops, retries, duplicates, delays, reordering, partition skew, and
outage behavior. It models delivery semantics—not raw TCP packet loss:

```bash
RUN_ID=RUN-...

fraudtwin kafka chaos --run-id "$RUN_ID" --boundary producer \
  --drop-rate 0.02 --duplicate-rate 0.03 --retry-rate 0.05 \
  --delay-seconds 30 --reorder-window 100 --output-dir runs
```

The command writes `kafka-chaos/manifest.json` and `envelopes.jsonl` beside the
run. Review sent, emitted, dropped, retried, duplicated, late, reordered, and
deduplicated counts, partition counts, and input/output fingerprints. Use
`boundary=consumer` to exercise downstream at-least-once handling.

## Validate Avro operational contracts

FraudTwin ships a source-controlled Avro registry for the clean observable
operational event projection. It contains `payment-event`, `customer-dispute`,
`fraud-alert`, `fraud-case`, `fraud-case-confirmation`, and `fraud-label`
subjects. The registry is local and bundled; it does not require a Schema
Registry service or Kafka.

The v1 record names and parsing-canonical fingerprints are recorded in
`contracts/avro/registry.yaml`:

The `1.0.0` values in this table are Avro contract versions, not FraudTwin
package releases. The public benchmark-pack release remains independently
versioned from the FraudTwin package release; contract versions are also
independently versioned for compatibility.

| Subject | Avro record | Version | Canonical SHA-256 |
| --- | --- | --- | --- |
| `payment-event` | `fraudtwin.events.v1.PaymentEvent` | `1.0.0` | `e7f8a63404fb5037bd6e83333550ebd49755d29675564bcdf57a1216c238cd6e` |
| `customer-dispute` | `fraudtwin.events.v1.CustomerDispute` | `1.0.0` | `046eae447f9e49061b5a01238fc613f1622b4a9533e7249fa92b56681c294c0b` |
| `fraud-alert` | `fraudtwin.events.v1.FraudAlert` | `1.0.0` | `694f88da97f952b06729fa0b892a2e6dc783b8518c213b085d9a847a769f82ba` |
| `fraud-case` | `fraudtwin.events.v1.FraudCase` | `1.0.0` | `e079a42c845527312a2d2efeb643fae0a09dbbdd2fc3c916afb5b1c5c1aae656` |
| `fraud-case-confirmation` | `fraudtwin.events.v1.FraudCaseConfirmation` | `1.0.0` | `15c56944c6465f2e6b68c34eacf7bdcc7db467a29f92ae607f629544bcf957e4` |
| `fraud-label` | `fraudtwin.events.v1.FraudLabel` | `1.0.0` | `b062e07420c417ba0c849b1a066b9853484c73e04fae95261319aa433c13b046` |

All subjects use `FULL_TRANSITIVE` compatibility. New optional fields need a
reader default and must remain bidirectionally compatible with every earlier
version. A breaking change is published as a new major subject with
`breaking: true` and `supersedes`, leaving the old subject unchanged.

Validate all schemas, canonical fingerprints, and full transitive reader/writer
compatibility with:

```bash
fraudtwin schema validate
fraudtwin schema validate --registry path/to/contracts/avro
```

Timestamps are timezone-aware UTC `timestamp-micros` values and monetary fields
are two-decimal Avro `decimal` values. Fraud truth and other oracle-only fields
are not part of the operational contracts. A compatible revision must provide
reader defaults for new fields; an incompatible revision requires an explicit
breaking major subject with `supersedes` metadata. Kafka producers and remote
registry registration remains a separate broker integration concern.

## Publish a lakehouse run

The lakehouse path keeps the normal Parquet run as the deterministic source and
adds an optional Iceberg publication.  Start the local MinIO, REST Catalog,
and Spark profile with:

```bash
docker compose --profile lakehouse up -d
export FRAUDTWIN_ICEBERG_CATALOG_URI='http://localhost:8181'
export FRAUDTWIN_ICEBERG_WAREHOUSE='s3://warehouse/'
export FRAUDTWIN_ICEBERG_S3_ENDPOINT='http://localhost:9000'
export FRAUDTWIN_ICEBERG_S3_ACCESS_KEY='minioadmin'
export FRAUDTWIN_ICEBERG_S3_SECRET_KEY='minioadmin'
fraudtwin lakehouse init
fraudtwin lakehouse ingest-run RUN-... --runs-dir runs
```

`ingest-run` creates an immutable materialization manifest under the source
run's `lakehouse/` directory.  Use `--local-only` to build and verify that
manifest without optional Iceberg dependencies or services.  The normal
observable namespaces never contain latent fraud truth; `--include-oracle`
publishes the separate oracle namespace explicitly.

Kafka topics can be consumed incrementally with `fraudtwin lakehouse consume`.
The consumer preserves raw framed payloads and transport metadata in Bronze,
then applies deterministic Silver deduplication.  A complete batch bootstrap
is still required for entity, ledger, graph, and PIT Gold tables because those
records are not Kafka subjects.

The consumer reads `FRAUDTWIN_KAFKA_BOOTSTRAP_SERVERS` and optionally
`FRAUDTWIN_KAFKA_TOPIC_PREFIX` / `FRAUDTWIN_LAKEHOUSE_CONSUMER_GROUP`.
Namespaces are prefixed (`fraudtwin_bronze`, `fraudtwin_silver`,
`fraudtwin_gold`, and `fraudtwin_oracle` by default); the logical tables and
partition specs are recorded in every materialization manifest.  Additive
columns are applied through Iceberg schema metadata.  Breaking changes require
a new versioned contract/table, leaving prior snapshots readable.

Use `fraudtwin lakehouse verify runs/RUN-.../lakehouse/LH-....json` to inspect
source checksums, logical fingerprints, and committed snapshot IDs.  Snapshot
expiration and orphan-file removal are intentionally operator-controlled.
`fraudtwin lakehouse maintenance ...` is dry-run by default; expiration
requires an explicit snapshot ID and retained run snapshots are never expired
implicitly.  Compaction and orphan cleanup are delegated to the Spark profile.

## Observe a generated run

The package provides a small optional Prometheus/Grafana surface for local
batch runs. Install the client and choose a non-default Grafana password:

```bash
poetry install -E observability
export GRAFANA_ADMIN_PASSWORD='choose-a-local-password'
docker compose --profile observability up -d
```

Run the generator with its temporary scrape endpoint. The endpoint is held
after completion so Prometheus can collect the final batch values:

```bash
fraudtwin generate configs/minimal.yaml --output-dir runs \
  --metrics-host 0.0.0.0 --metrics-port 9464 --metrics-hold-seconds 15
```

Open Grafana at `http://localhost:3000` and select a `run_id` in the
**FraudTwin run observability** dashboard. Prometheus retains local samples for
15 days. The target is expected to be down between CLI invocations because the
package does not add a permanent generator daemon, Pushgateway, or remote metrics
storage. The dashboard shows generation rate and fraud volume together with
generator/ledger failures, duplicate rate, invalid-record rate, and late-event
counts. `fraudtwin validate-ledger` accepts the same metrics options and records
the selected run's reconciliation result.

## A practical evaluation sequence

1. Generate a clean source run and validate its ledger.
2. Build the point-in-time dataset with the desired unresolved-label policy.
3. Replay a historical interval when you need an audit or delivery-order view.
4. Run a benchmark pack and compare fold manifests, not just aggregate scores.
5. Inspect the source, operational, and oracle artifacts together when a case needs an explanation.

This sequence keeps evaluation honest: the model sees only what would have been known at the time, while the oracle remains available for analysis and audit.

## Next

Continue with [ML evaluation](ml-evaluation.md), [Drift and shift](drift-and-shift.md),
or [Model lifecycle](model-lifecycle.md) according to the question you need to answer.

## Related

- [Configuration](configuration.md)
- [Data contracts](data-contracts.rst)
- [Graph and benchmark workflows](graph-and-benchmarks.md)
