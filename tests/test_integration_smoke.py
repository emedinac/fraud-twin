"""Tiny external-service smoke tests, enabled only by the PR integration job."""

import os
from pathlib import Path

import pytest

from fraudtwin.config import SimulationRunConfig, load_config
from fraudtwin.generation import generate

_ENABLED = os.environ.get("FRAUDTWIN_INTEGRATION_SMOKE") == "1"
pytestmark = pytest.mark.skipif(
    not _ENABLED,
    reason="integration smoke tests run only in the pull-request service job",
)


def _tiny_config() -> SimulationRunConfig:
    base = load_config(Path("configs/minimal-v1.yaml"))
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
    values["outputs"]["parquet"] = True
    return type(base).model_validate(values)


def test_postgres_operational_smoke(tmp_path: Path) -> None:
    from fraudtwin.postgres import migrate_database

    base = _tiny_config()
    values = base.model_dump(mode="python")
    values["outputs"]["postgres"] = True
    config = type(base).model_validate(values)
    # Do not use importorskip here: an installed but incomplete psycopg package
    # must fail with the adapter's actionable remediation message.
    migrate_database()
    result = generate(config, write=True, output_dir=tmp_path / "postgres")
    assert result.manifest.postgres is not None
    assert result.manifest.postgres["row_counts"]["payments"] > 0


def test_kafka_publication_smoke() -> None:
    from fraudtwin.kafka import SUBJECTS, publisher_from_environment

    data = generate(_tiny_config(), write=False)
    result = publisher_from_environment().publish(data.behavior, data.run_id)
    assert result.record_counts["payment-event"] > 0
    assert set(result.topics) == set(SUBJECTS)


def test_iceberg_snapshot_smoke(tmp_path: Path) -> None:
    from fraudtwin.lakehouse import LakehouseEnvironment, materialize_run, verify_materialization

    source = generate(_tiny_config(), write=True, output_dir=tmp_path / "runs")
    environment = LakehouseEnvironment.from_environment()
    result = materialize_run(source.run_dir, environment=environment, write_iceberg=True)
    assert result.manifest_path is not None
    payload = verify_materialization(result.manifest_path)
    assert payload["schema_version"] == "m26.v1"
    assert result.snapshots and any(value is not None for value in result.snapshots.values())
