"""Focused Milestone 26 lakehouse boundary and determinism tests."""

import json
from dataclasses import replace
from pathlib import Path

from fraudtwin.config import config_hash, load_config
from fraudtwin.generation import generate
from fraudtwin.lakehouse import (
    LakehouseConfig,
    LakehouseConfigurationError,
    LakehouseEnvironment,
    build_bronze_records,
    deduplicate_records,
    logical_fingerprint,
    materialize_run,
    silver_rows,
    verify_materialization,
)


def test_lakehouse_sink_does_not_change_source_identity() -> None:
    base = load_config(Path("configs/minimal.yaml"))
    values = base.model_dump(mode="python")
    values["outputs"]["iceberg"] = True
    values["lakehouse"] = {"include_oracle": True}
    configured = type(base).model_validate(values)
    assert config_hash(base) == config_hash(configured)


def test_lakehouse_environment_requires_external_catalog(monkeypatch) -> None:
    monkeypatch.delenv("FRAUDTWIN_ICEBERG_CATALOG_URI", raising=False)
    monkeypatch.delenv("FRAUDTWIN_ICEBERG_WAREHOUSE", raising=False)
    try:
        LakehouseEnvironment.from_environment()
    except LakehouseConfigurationError:
        return
    raise AssertionError("missing lakehouse environment should fail before publication")


def test_batch_materialization_is_deterministic_and_local(tmp_path: Path) -> None:
    config = load_config(Path("configs/minimal.yaml"))
    first = generate(config, write=True, output_dir=tmp_path / "first")
    second = generate(config, write=True, output_dir=tmp_path / "second")
    first_result = materialize_run(first.run_dir, write_iceberg=False)
    second_result = materialize_run(second.run_dir, write_iceberg=False)
    assert first_result.logical_fingerprint == second_result.logical_fingerprint
    assert first_result.row_counts == second_result.row_counts
    assert first_result.manifest_path is not None
    assert verify_materialization(first_result.manifest_path)["schema_version"] == "m26.v1"


def test_oracle_records_require_explicit_opt_in(tmp_path: Path) -> None:
    config_values = load_config(Path("configs/minimal.yaml")).model_dump(mode="python")
    config_values["fraud"]["enabled"] = True
    config_values["population"]["customers"] = 2
    config_values["payments"]["daily_target"] = 2
    config = type(load_config(Path("configs/minimal.yaml"))).model_validate(config_values)
    result = generate(config, write=True, output_dir=tmp_path / "runs")
    without_oracle = materialize_run(result.run_dir, write_iceberg=False)
    with_oracle = materialize_run(
        result.run_dir,
        config=LakehouseConfig(include_oracle=True),
        write_iceberg=False,
        include_oracle=True,
    )
    assert "oracle.records" not in json.loads(without_oracle.manifest_path.read_text())["tables"]
    assert "oracle.records" in json.loads(with_oracle.manifest_path.read_text())["tables"]


def test_silver_deduplication_has_stable_winner() -> None:
    data = generate(write=False)
    records = build_bronze_records(data.entities, data.behavior, data.manifest)
    duplicate = records[0]
    assert deduplicate_records((*records, duplicate)).count(duplicate) == 1


def test_batch_and_stream_fingerprints_ignore_transport_metadata() -> None:
    data = generate(write=False)
    batch = build_bronze_records(data.entities, data.behavior, data.manifest)[0]
    streamed = replace(
        batch,
        source="kafka",
        received_at="2099-01-01T00:00:00+00:00",
        kafka_topic="fraudsim.payment.events.v1",
        kafka_partition=2,
        kafka_offset=42,
        raw_payload_b64="AAABAg==",
    )
    assert logical_fingerprint([batch.as_row()]) == logical_fingerprint([streamed.as_row()])
    silver = silver_rows((batch, streamed))
    assert silver[0]["contributing_bronze_ids"] == [batch.record_id, streamed.record_id]
