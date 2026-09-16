# Integration runbooks

FraudTwin's core generator is offline and deterministic. These runbooks cover
optional local integrations for MLOps and data-engineering workflows. Every
service-backed path has an offline notebook or manifest-based fallback.

## Integration map

| Integration | Extra | Start locally | First health check | Offline fallback |
| --- | --- | --- | --- | --- |
| Kafka + Schema Registry | `kafka` | `docker compose --profile streaming up -d` | broker topics and `GET /subjects` | `fraudtwin kafka chaos` |
| PostgreSQL | `postgres` | `docker compose --profile integration up -d postgres` | `pg_isready` | local Parquet manifest |
| Iceberg + MinIO | `lakehouse` | `docker compose --profile lakehouse up -d` | catalog and object-store health | Bronze/Silver/Gold local projections |
| Neo4j | graph export is base; driver optional | external/local Neo4j | Bolt/browser health check | Cypher files and local graph frames |
| MLflow | `mlflow` | local MLflow server | tracking URI request | local model metadata |
| FastAPI | `serving` | `uvicorn examples.model_service.app:app` | `GET /health` | offline prediction replay |
| Prometheus/Grafana | `observability` | `docker compose --profile observability up -d` | Prometheus target health | manifest SLO table |

## Common operating sequence

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
load it into Neo4j using constraints on stable IDs and indexes on event time.
The offline fallback runs the same investigation predicates over Polars graph
frames. Never load oracle-only relationships into an operational investigation
database without labeling the data boundary.

## MLflow and FastAPI

MLflow stores parameters, metrics, artifact checksums, and fingerprints; local
JSON manifests remain valid when MLflow is unavailable. The FastAPI service is
a reference adapter. Add authentication, authorization, rate limiting,
timeouts, structured logs, and deployment isolation before exposing a service
outside a trusted local network.

## Prometheus and Grafana

```console
poetry install -E observability
docker compose --profile observability up -d prometheus grafana
```

Verify target health before interpreting counters. Track generation throughput,
publication lag, invalid records, duplicate rate, reconciliation failures, and
checkpoint progress. The offline fallback records the same checks in a manifest
so a service is never required to learn the operational concepts.
