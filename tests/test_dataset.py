import json
from dataclasses import replace
from datetime import timedelta
from functools import cache
from pathlib import Path

import polars as pl
import pytest
from typer.testing import CliRunner

from fraudtwin.cli import app
from fraudtwin.config import SimulationRunConfig, load_config
from fraudtwin.manifest import create_manifest
from fraudtwin.ml import (
    PIT_DATASET_SCHEMA,
    PointInTimeDatasetBuilder,
    write_point_in_time_dataset,
)
from fraudtwin.simulation import BehaviorGenerator, EntityGenerator

CONFIG_PATH = Path("configs/minimal-v1.yaml")
runner = CliRunner()


@cache
def _source(fraud: bool = False):
    raw = load_config(CONFIG_PATH).model_dump(mode="python")
    raw["dataset"]["unresolved_labels"] = "include"
    if fraud:
        raw["fraud"]["enabled"] = True
        raw["fraud"]["target_rate"] = 1.0
        raw["fraud_workflow"]["alert_delay_seconds"] = 0
        raw["fraud_workflow"]["case_open_delay_seconds"] = 0
        raw["fraud_workflow"]["confirmation_delay_seconds"] = 0
        raw["fraud_workflow"]["customer_dispute_delay_seconds"] = 0
        raw["fraud_workflow"]["label_delay_seconds"] = 60
    config = SimulationRunConfig.model_validate(raw)
    entities = EntityGenerator(config).generate()
    source_manifest = create_manifest(config)
    behavior = BehaviorGenerator(
        config, entities, simulation_run_id=source_manifest.run_id
    ).generate()
    return config, entities, behavior, source_manifest


def test_dataset_is_deterministic_with_stable_rows_and_schema() -> None:
    config, entities, behavior, source_manifest = _source()
    first = PointInTimeDatasetBuilder(config, entities, behavior, source_manifest).build()
    second = PointInTimeDatasetBuilder(config, entities, behavior, source_manifest).build()

    assert first == second
    assert [row["payment_id"] for row in first.rows] == sorted(
        row["payment_id"] for row in first.rows
    )
    assert [row["dataset_row_id"] for row in first.rows] == [
        row["dataset_row_id"] for row in second.rows
    ]
    frame = pl.DataFrame(list(first.rows), schema=PIT_DATASET_SCHEMA, orient="row")
    assert frame.columns == list(PIT_DATASET_SCHEMA)
    assert frame.schema == PIT_DATASET_SCHEMA


def test_features_are_available_at_prediction_time() -> None:
    config, entities, behavior, source_manifest = _source()
    dataset = PointInTimeDatasetBuilder(config, entities, behavior, source_manifest).build()

    assert all(row["business_event_time"] <= row["prediction_time"] for row in dataset.rows)
    assert all(row["source_available_at"] <= row["prediction_time"] for row in dataset.rows)
    assert all(row["feature_available_at"] <= row["prediction_time"] for row in dataset.rows)
    assert all(
        row["label_available_at"] is None or row["label_available_at"] <= row["prediction_time"]
        for row in dataset.rows
    )


def test_unavailable_historical_events_cannot_change_a_prediction_row() -> None:
    config, entities, behavior, source_manifest = _source()
    events = {
        event.payment_id: event for event in behavior.payment_events if event.causation_id is None
    }
    target, candidate = next(
        (target, candidate)
        for target in events.values()
        for candidate in events.values()
        if target.customer_id == candidate.customer_id
        and target.payment_id != candidate.payment_id
        and candidate.event_time < target.source_available_at
    )
    unavailable_at = target.source_available_at + timedelta(seconds=1)
    late_candidate = candidate.model_copy(
        update={
            "source_available_at": unavailable_at,
            "ingested_at": unavailable_at + timedelta(seconds=1),
            "processed_at": unavailable_at + timedelta(seconds=2),
        }
    )
    changed_candidate = late_candidate.model_copy(update={"amount": late_candidate.amount * 100})
    late_behavior = replace(
        behavior,
        payment_events=tuple(
            late_candidate if event.event_id == candidate.event_id else event
            for event in behavior.payment_events
        ),
    )
    changed_behavior = replace(
        behavior,
        payment_events=tuple(
            changed_candidate if event.event_id == candidate.event_id else event
            for event in behavior.payment_events
        ),
    )
    prediction_times = {target.payment_id: target.source_available_at}
    first = PointInTimeDatasetBuilder(config, entities, late_behavior, source_manifest).build(
        prediction_times
    )
    second = PointInTimeDatasetBuilder(config, entities, changed_behavior, source_manifest).build(
        prediction_times
    )

    assert first.rows == second.rows


def test_temporal_splits_have_non_overlapping_label_delay_gaps() -> None:
    config, entities, behavior, source_manifest = _source()
    builder = PointInTimeDatasetBuilder(config, entities, behavior, source_manifest)
    train_end, validation_start, validation_end, test_start, test_end = builder._split_boundaries()

    assert (
        builder._split(
            train_end - timedelta(microseconds=1),
            (train_end, validation_start, validation_end, test_start, test_end),
        )
        == "train"
    )
    assert (
        builder._split(
            train_end, (train_end, validation_start, validation_end, test_start, test_end)
        )
        is None
    )
    assert (
        builder._split(
            validation_start, (train_end, validation_start, validation_end, test_start, test_end)
        )
        == "validation"
    )
    assert (
        builder._split(
            validation_end, (train_end, validation_start, validation_end, test_start, test_end)
        )
        is None
    )
    assert (
        builder._split(
            test_start, (train_end, validation_start, validation_end, test_start, test_end)
        )
        == "test"
    )
    assert train_end < validation_start <= validation_end < test_start < test_end


def test_label_delay_is_enforced_and_mature_labels_are_used() -> None:
    config, entities, behavior, source_manifest = _source(fraud=True)
    end = config.simulation.start + timedelta(days=config.simulation.duration_days)
    dataset = PointInTimeDatasetBuilder(config, entities, behavior, source_manifest).build(
        {payment.payment_id: end - timedelta(microseconds=1) for payment in behavior.payments}
    )

    labelled = [row for row in dataset.rows if row["label"] is not None]
    assert labelled
    assert all(row["label_available_at"] <= row["prediction_time"] for row in labelled)
    assert all(
        row["label_available_at"] >= row["business_event_time"] + timedelta(seconds=60)
        for row in labelled
    )


def test_unavailable_label_is_unresolved_before_its_delay_expires() -> None:
    config, entities, behavior, source_manifest = _source(fraud=True)
    raw = config.model_dump(mode="python")
    raw["dataset"]["splits"]["label_delay_gap_seconds"] = 0
    config = SimulationRunConfig.model_validate(raw)
    events = {
        event.payment_id: event for event in behavior.payment_events if event.causation_id is None
    }
    label, event = next(
        (label, events[label.payment_id])
        for label in behavior.fraud_labels
        if label.payment_id in events
        and events[label.payment_id].source_available_at < label.label_available_at
    )
    prediction_time = label.label_available_at - timedelta(seconds=1)
    dataset = PointInTimeDatasetBuilder(config, entities, behavior, source_manifest).build(
        {label.payment_id: prediction_time}
    )

    row = next(row for row in dataset.rows if row["payment_id"] == label.payment_id)
    assert event.source_available_at <= prediction_time
    assert row["label"] is None
    assert row["fraud_truth"] is None
    assert row["label_available_at"] == label.label_available_at


def test_in_memory_pit_validation_rejects_mismatched_dispute_evidence() -> None:
    config, entities, behavior, source_manifest = _source(fraud=True)
    label = next(label for label in behavior.fraud_labels if label.dispute_event_at is not None)
    changed_label = label.model_copy(
        update={"dispute_event_at": label.dispute_event_at + timedelta(seconds=1)}
    )
    changed_behavior = replace(
        behavior,
        fraud_labels=tuple(
            changed_label if item.label_id == label.label_id else item
            for item in behavior.fraud_labels
        ),
    )

    with pytest.raises(ValueError, match="label dispute timestamp"):
        PointInTimeDatasetBuilder(config, entities, changed_behavior, source_manifest).build()


def test_manifest_contains_split_lineage_and_reproducibility_metadata(tmp_path: Path) -> None:
    config, entities, behavior, source_manifest = _source()
    dataset = PointInTimeDatasetBuilder(config, entities, behavior, source_manifest).build()
    output, manifest_path = write_point_in_time_dataset(dataset, tmp_path / "dataset.parquet")
    saved = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert output.exists()
    assert saved["source_run_id"] == source_manifest.run_id
    assert saved["split_boundaries"] == dataset.manifest.split_boundaries
    assert saved["reproducibility"]["row_hash"]
    assert saved["reproducibility"]["schema_columns"] == list(PIT_DATASET_SCHEMA)
    assert saved["schema_fingerprint"] == dataset.manifest.schema_fingerprint
    assert saved["output_fingerprint"] == dataset.manifest.output_fingerprint
    assert set(saved["feature_definitions"]) == set(PIT_DATASET_SCHEMA) - {
        "dataset_row_id",
        "payment_id",
        "event_id",
        "customer_id",
        "account_id",
        "prediction_time",
        "business_event_time",
        "event_time",
        "source_available_at",
        "feature_available_at",
        "label_available_at",
        "label",
        "fraud_truth",
        "split",
    }
    assert saved["row_counts"]["total"] == dataset.count


def test_invalid_m9_configuration_is_rejected() -> None:
    raw = load_config(CONFIG_PATH).model_dump(mode="python")
    raw["dataset"]["feature_windows"]["unknown_window"] = 10
    with pytest.raises(ValueError):
        SimulationRunConfig.model_validate(raw)


def test_m9_cli_generates_parquet_and_rebuilds_from_a_run(tmp_path: Path) -> None:
    generated = runner.invoke(app, ["generate", str(CONFIG_PATH), "--output-dir", str(tmp_path)])
    assert generated.exit_code == 0
    manifest_path = next(tmp_path.glob("*/manifest.json"))
    run_id = manifest_path.parent.name
    rebuilt = runner.invoke(
        app,
        [
            "ml",
            "build-dataset",
            str(CONFIG_PATH),
            "--run-id",
            run_id,
            "--output-dir",
            str(tmp_path),
            "--output",
            str(tmp_path / "rebuilt.parquet"),
        ],
    )
    assert rebuilt.exit_code == 0
    frame = pl.read_parquet(tmp_path / "rebuilt.parquet")
    original_frame = pl.read_parquet(manifest_path.parent / "ml" / "dataset.parquet")
    assert frame.columns == list(PIT_DATASET_SCHEMA)
    assert frame.schema == PIT_DATASET_SCHEMA
    assert frame.equals(original_frame)
    assert (tmp_path / "dataset_manifest.json").is_file()
    original_manifest = json.loads(
        (manifest_path.parent / "ml" / "dataset_manifest.json").read_text(encoding="utf-8")
    )
    assert (
        json.loads((tmp_path / "dataset_manifest.json").read_text(encoding="utf-8"))
        == original_manifest
    )

    raw = load_config(CONFIG_PATH).model_dump(mode="python")
    raw["dataset"]["splits"]["train_fraction"] = 0.8
    with pytest.raises(ValueError):
        SimulationRunConfig.model_validate(raw)
