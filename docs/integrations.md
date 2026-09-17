# Integration runbooks

FraudTwin's core generator is offline and deterministic. These runbooks cover
optional local integrations for MLOps and data-engineering workflows. Every
service-backed path has an offline notebook or manifest-based fallback.

## Integration map

| Integration | Extra/client | Start locally | First health check | Offline fallback |
| --- | --- | --- | --- | --- |
| Kafka + Schema Registry | `kafka` / `confluent-kafka` | `docker compose --profile streaming up -d kafka schema-registry streaming-topics` | broker metadata and `GET /subjects` | `fraudtwin kafka chaos` |
| PostgreSQL | `postgres` / `psycopg[binary]` | `docker compose --profile integration up -d postgres` | `pg_isready` and `SELECT 1` | local Parquet manifest |
| Iceberg + MinIO | `lakehouse` / `pyiceberg[s3fs]` | `docker compose --profile lakehouse up -d minio minio-init iceberg-rest` | catalog namespaces and object-store health | Bronze/Silver/Gold local projections |
| Neo4j | `neo4j` driver (not a project extra) | `docker run --name fraudtwin-neo4j --detach --rm --publish 7474:7474 --publish 7687:7687 --env NEO4J_AUTH=neo4j/password neo4j:5` | Bolt connectivity and `RETURN 1` | Cypher files and local graph frames |
| MLflow | `mlflow` | local file tracking or MLflow server | tracked run metadata | local model metadata |
| FastAPI | `serving` / `fastapi uvicorn httpx` | `uvicorn examples.model_service.app:app` | `GET /health` and `POST /score` | offline prediction replay |
| Prometheus/Grafana | `observability` / `prometheus-client requests` | `docker compose --profile observability up -d prometheus grafana` | Prometheus target health | manifest SLO table |

The tutorial notebooks use the same client commands as this table. Full
installation, authentication, TLS, clustering, backup, and production sizing
remain the responsibility of the service vendor. See the official manuals for
[Neo4j](https://neo4j.com/docs/python-manual/current/),
[Confluent Kafka](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html),
[PostgreSQL](https://www.postgresql.org/docs/),
[PyIceberg](https://py.iceberg.apache.org/),
[MLflow](https://mlflow.org/docs/latest/ml/tracking/),
[FastAPI](https://fastapi.tiangolo.com/), and
[Prometheus](https://prometheus.io/docs/).

## Common operating sequence

The reconciliation figure shows the compact evidence produced by the offline
lakehouse and database paths before a service is available.

```{image} _static/images/reconciliation-summary.svg
:alt: Reconciliation summary for payments, ledger rows, and recovered records
:class: evidence-figure
```
*Figure: compact payment/ledger reconciliation evidence from the offline
integration path, generated with seed 42.*

1. Install the matching extra.
2. Start the documented service profile.
3. Export credentials and endpoints through environment variables.
4. Run the service health check before generation.
5. Keep Parquet enabled as the deterministic source artifact.
6. Generate or publish a bounded run.
7. Verify row counts, fingerprints, and service metrics.
8. Stop services and remove local volumes after the experiment.

Never commit credentials, broker state, database volumes, model binaries, or
large datasets. Keep secrets out of resolved configuration and manifests.

## Kafka and Schema Registry

```console
poetry install -E kafka
docker compose --profile streaming up -d
export FRAUDTWIN_KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export FRAUDTWIN_SCHEMA_REGISTRY_URL=http://localhost:8081
fraudtwin schema validate
curl -fsS http://localhost:8081/subjects
```

The Kafka notebooks also include this optional client cell:

```console
!pip install confluent-kafka
```

They list broker topics, list Schema Registry subjects, and publish a
contract-backed FraudTwin record when the service is reachable. If the broker
is unavailable, the deterministic logical-chaos path remains the offline
result.

The publisher uses stable event identity, explicit topic contracts,
acknowledged/idempotent producer settings, and at-least-once retry semantics.
See [Kafka reliability](kafka-reliability.md) for partitioning, offsets,
watermarks, deduplication, and logical chaos.

## PostgreSQL

```console
poetry install -E postgres
docker compose --profile integration up -d postgres
export FRAUDTWIN_POSTGRES_DSN='postgresql://fraudtwin:fraudtwin@localhost:5432/fraudtwin'
fraudtwin db migrate
fraudtwin db status
```

Persistence is an operational mirror, not a replacement for Parquet source
artifacts. Repeating the same `run_id` is idempotent; credentials are read from
the environment and oracle fraud truth is not written to operational tables.

## Iceberg and object storage

```console
poetry install -E lakehouse
docker compose --profile lakehouse up -d
export FRAUDTWIN_ICEBERG_CATALOG_URI=http://localhost:8181
export FRAUDTWIN_ICEBERG_WAREHOUSE=s3://warehouse/
fraudtwin lakehouse init
fraudtwin lakehouse verify
```

Use snapshots and time travel to correct late events without mutating the
source run. Keep catalog and object-store credentials in environment variables.

## Neo4j graph loading

Graph export is dependency-free: write node/edge CSV and Cypher first, then
install the notebook-only driver:

```console
!pip install neo4j
```

For a local smoke test, start Neo4j with the command in the integration table,
then run `verify_connectivity()`, `RETURN 1`, and a small `MERGE` query over a
stable FraudTwin run ID. Close the driver and remove the container when done.
The offline fallback runs the same investigation predicates over Polars graph
frames. Never load oracle-only relationships into an operational investigation
database without labeling the data boundary.

## MLflow and FastAPI

MLflow stores parameters, metrics, artifact checksums, and fingerprints; local
JSON manifests remain valid when MLflow is unavailable. The MLflow tutorial
installs the client with `!pip install mlflow`, logs a run, and reads it back.
The FastAPI tutorial installs `!pip install fastapi uvicorn httpx`, starts the
reference adapter, and calls `/health` and `/score` with valid and invalid
requests. Add authentication, authorization, rate limiting, timeouts,
structured logs, and deployment isolation before exposing a service outside a
trusted local network.

## Prometheus and Grafana

```console
poetry install -E observability
docker compose --profile observability up -d prometheus grafana
```

Verify target health before interpreting counters. Track generation throughput,
publication lag, invalid records, duplicate rate, reconciliation failures, and
checkpoint progress. The offline fallback records the same checks in a manifest
so a service is never required to learn the operational concepts.
