"""Focused Milestone 23 PostgreSQL operational-mode tests."""

import json
import os
from pathlib import Path

import pytest

from fraudtwin.config import config_hash, load_config
from fraudtwin.generation import generate
from fraudtwin.postgres import (
    DSN_ENVIRONMENT,
    SCHEMA_VERSION,
    _fingerprint,
    _insert,
    _migration_sql,
    _payload,
    _rows_for_run,
    database_status,
    migrate_database,
)


def test_postgres_toggle_does_not_change_generator_identity() -> None:
    base = load_config(Path("configs/minimal.yaml"))
    values = base.model_dump(mode="python")
    values["outputs"]["postgres"] = True
    mirrored = type(base).model_validate(values)
    assert config_hash(base) == config_hash(mirrored)


def test_postgres_requires_external_dsn(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(DSN_ENVIRONMENT, raising=False)
    with pytest.raises((RuntimeError, ValueError)):
        database_status()


def test_postgres_migration_is_packaged_and_versioned() -> None:
    assert migrate_database.__module__ == "fraudtwin.postgres"
    assert "CREATE SCHEMA IF NOT EXISTS fraudtwin" in _migration_sql()
    assert SCHEMA_VERSION == "001_operational"


def test_operational_payload_masks_fraud_truth() -> None:
    class Record:
        def model_dump(self, *, mode: str) -> dict[str, object]:
            return {"fraud_record_id": "FR-1", "fraud_truth": True, "tags": ("a",)}

    value = json.loads(_payload(Record(), hide_truth=True))
    assert value == {"fraud_record_id": "FR-1", "tags": ["a"]}


def test_fingerprint_is_ordered_by_table_and_payload() -> None:
    first = {"payments": [{"payload": '{"payment_id":"P-1"}'}], "customers": []}
    second = {"customers": [], "payments": [{"payload": '{"payment_id":"P-1"}'}]}
    assert _fingerprint(first) == _fingerprint(second)


def test_rows_for_run_contains_required_operational_tables() -> None:
    class Empty:
        institutions = customers = merchants = devices = accounts = cards = pix_keys = ()
        payments = payment_events = fraud_records = alerts = fraud_cases = ()

    tables = _rows_for_run(Empty(), Empty())
    assert {
        "customers",
        "accounts",
        "cards",
        "payments",
        "fraud_cases",
        "payment_events",
        "fraud_records",
        "fraud_alerts",
    }.issubset(tables)


def test_insert_uses_each_row_value() -> None:
    class Cursor:
        def __init__(self) -> None:
            self.calls: list[tuple[str, list[tuple[object, ...]]]] = []

        def executemany(self, sql: str, values: list[tuple[object, ...]]) -> None:
            self.calls.append((sql, values))

    cursor = Cursor()
    _insert(
        cursor,
        "customers",
        "RUN-1",
        [{"id": "C-1", "payload": "one"}, {"id": "C-2", "payload": "two"}],
    )
    assert cursor.calls[0][1] == [("RUN-1", "C-1", "one"), ("RUN-1", "C-2", "two")]


@pytest.mark.skipif(
    not os.environ.get(DSN_ENVIRONMENT), reason="PostgreSQL integration DSN is not configured"
)
def test_postgres_mirror_matches_small_file_run(tmp_path: Path) -> None:
    pytest.importorskip("psycopg")
    base = load_config(Path("configs/minimal.yaml"))
    values = base.model_dump(mode="python")
    values["population"] = {
        "customers": 2,
        "institutions": 1,
        "accounts": 2,
        "cards": 1,
        "merchants": 1,
        "devices": 2,
        "pix_keys": 2,
    }
    values["payments"]["daily_target"] = 2
    values["outputs"]["postgres"] = True
    values["outputs"]["parquet"] = True
    config = type(base).model_validate(values)
    migrate_database()
    result = generate(config, write=True, output_dir=tmp_path / "runs")
    assert result.manifest.postgres is not None
    assert result.manifest.postgres["row_counts"]["customers"] == 2
    assert result.manifest.postgres["row_counts"]["accounts"] == 2
    assert result.manifest.postgres["logical_fingerprint"]
    repeat = generate(config, write=True, output_dir=tmp_path / "repeat")
    assert repeat.manifest.postgres is not None
    assert repeat.manifest.postgres["idempotent"] is True
