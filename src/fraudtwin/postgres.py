"""Optional PostgreSQL operational sink for generated FraudTwin runs.

The sink is intentionally kept behind a lazy ``psycopg`` import.  File-mode
generation therefore remains dependency-free, while an enabled PostgreSQL
output receives the same observable records in one transactional write.
"""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from importlib.resources import files
from typing import Any

MIGRATION_PACKAGE = "fraudtwin.migrations"
SCHEMA_NAME = "fraudtwin"
SCHEMA_VERSION = "001_operational"
DSN_ENVIRONMENT = "FRAUDTWIN_POSTGRES_DSN"


@dataclass(frozen=True)
class PostgresPersistenceResult:
    """Non-secret metadata recorded in a generated run manifest."""

    schema_version: str
    row_counts: dict[str, int]
    logical_fingerprint: str
    idempotent: bool = False


def _dsn(value: str | None = None) -> str:
    resolved = value or os.environ.get(DSN_ENVIRONMENT)
    if not resolved:
        raise ValueError(
            f"PostgreSQL output requires {DSN_ENVIRONMENT}; credentials must not be stored in YAML"
        )
    return resolved


def _psycopg() -> Any:
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - exercised without the optional extra
        raise RuntimeError(
            "PostgreSQL output requires the optional 'postgres' dependency (psycopg)"
        ) from exc
    return psycopg


def _migration_sql() -> str:
    resource = files(MIGRATION_PACKAGE).joinpath(f"{SCHEMA_VERSION}.sql")
    if not resource.is_file():
        raise RuntimeError(f"missing packaged PostgreSQL migration: {SCHEMA_VERSION}")
    return resource.read_text(encoding="utf-8")


def migrate_database(dsn: str | None = None) -> str:
    """Apply the packaged, ordered PostgreSQL migrations and return the version."""

    psycopg = _psycopg()
    with psycopg.connect(_dsn(dsn)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "CREATE TABLE IF NOT EXISTS public.schema_migrations "
                "(version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())"
            )
            cursor.execute("SELECT version FROM public.schema_migrations ORDER BY version")
            applied = {row[0] for row in cursor.fetchall()}
            if SCHEMA_VERSION not in applied:
                cursor.execute(_migration_sql())
                cursor.execute(
                    "INSERT INTO public.schema_migrations(version) VALUES (%s)",
                    (SCHEMA_VERSION,),
                )
    return SCHEMA_VERSION


def database_status(dsn: str | None = None) -> tuple[str, ...]:
    """Return applied migration versions without changing the database."""

    psycopg = _psycopg()
    with psycopg.connect(_dsn(dsn)) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT version FROM public.schema_migrations ORDER BY version")
            return tuple(str(row[0]) for row in cursor.fetchall())


def ensure_database_ready(dsn: str | None = None) -> None:
    """Fail before file emission when the operational schema is unavailable."""

    psycopg = _psycopg()
    with psycopg.connect(_dsn(dsn)) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT to_regclass(%s)", (f"{SCHEMA_NAME}.simulation_runs",))
            if cursor.fetchone()[0] is None:
                raise ValueError(
                    "PostgreSQL schema is not initialized; run 'fraudtwin db migrate' first"
                )


def _payload(record: Any, *, hide_truth: bool = False) -> str:
    values = record.model_dump(mode="json")
    if hide_truth and isinstance(values, dict):
        values.pop("fraud_truth", None)
    return json.dumps(values, sort_keys=True, separators=(",", ":"), default=str)


def _records_to_rows(
    records: Any,
    identifier: str,
    references: tuple[str, ...] = (),
    *,
    hide_truth: bool = False,
) -> list[dict[str, Any]]:
    """Project domain records into SQL columns and an exact JSON payload."""

    return [
        {
            "id": getattr(record, identifier),
            **{field: getattr(record, field) for field in references},
            "payload": _payload(record, hide_truth=hide_truth),
        }
        for record in records
    ]


def _rows_for_run(entities: Any, behavior: Any) -> dict[str, list[dict[str, Any]]]:
    """Create typed key columns plus an exact JSON representation for parity checks."""

    specs = (
        ("institutions", entities.institutions, "institution_id", (), False),
        ("customers", entities.customers, "customer_id", (), False),
        ("merchants", entities.merchants, "merchant_id", ("acquirer_id",), False),
        ("devices", entities.devices, "device_id", (), False),
        (
            "accounts",
            entities.accounts,
            "account_id",
            ("customer_id", "institution_id"),
            False,
        ),
        ("cards", entities.cards, "card_id", ("account_id", "customer_id"), False),
        (
            "pix_keys",
            entities.pix_keys,
            "pix_key_id",
            ("account_id", "customer_id", "institution_id"),
            False,
        ),
        (
            "payments",
            behavior.payments,
            "payment_id",
            (
                "payer_account_id",
                "payee_account_id",
                "merchant_id",
                "card_id",
                "payer_institution_id",
                "payee_institution_id",
                "payer_pix_key_id",
                "payee_pix_key_id",
                "amount",
                "initiated_at",
            ),
            False,
        ),
        (
            "payment_events",
            behavior.payment_events,
            "event_id",
            (
                "payment_id",
                "customer_id",
                "account_id",
                "merchant_id",
                "card_id",
                "device_id",
                "event_time",
            ),
            False,
        ),
        (
            "fraud_records",
            behavior.fraud_records,
            "fraud_record_id",
            (
                "customer_id",
                "account_id",
                "card_id",
                "device_id",
                "merchant_id",
                "payment_id",
                "event_id",
                "amount",
                "occurred_at",
            ),
            True,
        ),
        (
            "fraud_alerts",
            behavior.alerts,
            "fraud_alert_id",
            (
                "customer_id",
                "account_id",
                "card_id",
                "device_id",
                "merchant_id",
                "payment_id",
                "event_id",
                "fraud_record_id",
                "amount",
                "alert_created_at",
            ),
            False,
        ),
        (
            "fraud_cases",
            behavior.fraud_cases,
            "fraud_case_id",
            (
                "fraud_alert_id",
                "customer_id",
                "account_id",
                "card_id",
                "device_id",
                "merchant_id",
                "payment_id",
                "event_id",
                "fraud_record_id",
                "amount",
                "case_opened_at",
            ),
            True,
        ),
    )
    tables: dict[str, list[dict[str, Any]]] = {}
    for table, records, identifier, references, hide_truth in specs:
        tables[table] = _records_to_rows(
            records,
            identifier,
            references,
            hide_truth=hide_truth,
        )
    return tables


def _fingerprint(tables: dict[str, list[dict[str, Any]]]) -> str:
    canonical = {table: [row["payload"] for row in rows] for table, rows in sorted(tables.items())}
    return hashlib.sha256(
        json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _insert(cursor: Any, table: str, run_id: str, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    columns = ["run_id", *rows[0].keys()]
    placeholders = ", ".join(["%s"] * len(columns))
    sql = f"INSERT INTO {SCHEMA_NAME}.{table} ({', '.join(columns)}) VALUES ({placeholders})"
    cursor.executemany(sql, [(run_id, *[row[column] for column in columns[1:]]) for row in rows])


def persist_run(
    entities: Any,
    behavior: Any,
    manifest: Any,
    *,
    dsn: str | None = None,
) -> PostgresPersistenceResult:
    """Persist one generated run atomically after migrations have been applied."""

    psycopg = _psycopg()
    resolved_dsn = _dsn(dsn)
    tables = _rows_for_run(entities, behavior)
    fingerprint = _fingerprint(tables)
    row_counts = {name: len(rows) for name, rows in tables.items()}
    run_id = str(manifest.run_id)
    with psycopg.connect(resolved_dsn) as connection:
        with connection.transaction():
            with connection.cursor() as cursor:
                cursor.execute("SELECT to_regclass(%s)", (f"{SCHEMA_NAME}.simulation_runs",))
                if cursor.fetchone()[0] is None:
                    raise ValueError(
                        "PostgreSQL schema is not initialized; run 'fraudtwin db migrate' first"
                    )
                cursor.execute(
                    f"SELECT content_fingerprint FROM {SCHEMA_NAME}.simulation_runs "
                    "WHERE run_id = %s",
                    (run_id,),
                )
                existing = cursor.fetchone()
                if existing is not None:
                    if existing[0] != fingerprint:
                        raise ValueError(
                            f"PostgreSQL run {run_id} already exists with different content"
                        )
                    return PostgresPersistenceResult(
                        SCHEMA_VERSION, row_counts, fingerprint, idempotent=True
                    )
                cursor.execute(
                    f"INSERT INTO {SCHEMA_NAME}.simulation_runs "
                    "(run_id, generator_version, configuration_hash, content_fingerprint, "
                    "status, started_at) "
                    "VALUES (%s, %s, %s, %s, %s, %s)",
                    (
                        run_id,
                        manifest.generator_version,
                        manifest.scenario_config_hash,
                        fingerprint,
                        "COMPLETED",
                        manifest.start_time,
                    ),
                )
                for table, rows in tables.items():
                    _insert(cursor, table, run_id, rows)
    return PostgresPersistenceResult(SCHEMA_VERSION, row_counts, fingerprint)


def persist_scale_records(
    records: Iterable[Mapping[str, Any]],
    run_id: str,
    *,
    dsn: str | None = None,
    batch_size: int = 2_000,
) -> PostgresPersistenceResult:
    """Persist a partition-record stream without whole-run materialization.

    Scale rows are kept in a narrow staging table so chunks can be consumed
    independently by downstream jobs.  The established ``persist_run`` API
    remains unchanged for small, typed in-memory runs.
    """

    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    psycopg = _psycopg()
    digest = hashlib.sha256()
    counts: dict[str, int] = {}
    batch: list[tuple[str, str, str, str | None, str]] = []
    with psycopg.connect(_dsn(dsn)) as connection:
        with connection.transaction():
            with connection.cursor() as cursor:
                cursor.execute(
                    f"CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.scale_records ("
                    "run_id TEXT NOT NULL, logical_type TEXT NOT NULL, "
                    "logical_id TEXT NOT NULL, partition_id TEXT, payload JSONB NOT NULL, "
                    "PRIMARY KEY (run_id, logical_type, logical_id))"
                )

                def flush() -> None:
                    if not batch:
                        return
                    cursor.executemany(
                        f"INSERT INTO {SCHEMA_NAME}.scale_records "
                        "(run_id, logical_type, logical_id, partition_id, payload) "
                        "VALUES (%s, %s, %s, %s, %s::jsonb) "
                        "ON CONFLICT (run_id, logical_type, logical_id) DO NOTHING",
                        batch,
                    )
                    batch.clear()

                for record in records:
                    row = dict(record)
                    logical_type = str(row.get("logical_type", "records"))
                    logical_id = str(row["logical_id"])
                    payload = json.dumps(row, sort_keys=True, separators=(",", ":"), default=str)
                    digest.update(payload.encode())
                    counts[logical_type] = counts.get(logical_type, 0) + 1
                    batch.append(
                        (run_id, logical_type, logical_id, row.get("partition_id"), payload)
                    )
                    if len(batch) >= batch_size:
                        flush()
                flush()
    return PostgresPersistenceResult(
        schema_version=SCHEMA_VERSION,
        row_counts=counts,
        logical_fingerprint=digest.hexdigest(),
    )


__all__ = [
    "DSN_ENVIRONMENT",
    "PostgresPersistenceResult",
    "SCHEMA_VERSION",
    "database_status",
    "ensure_database_ready",
    "migrate_database",
    "persist_scale_records",
    "persist_run",
]
