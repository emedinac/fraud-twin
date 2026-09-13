import json
from datetime import timedelta
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

CONFIG_PATH = Path("configs/minimal.yaml")
runner = CliRunner()


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

    assert all(
        row["business_event_time"] <= row["prediction_time"]
        or row["source_available_at"] <= row["prediction_time"]
        for row in dataset.rows
    )
    assert all(row["feature_available_at"] <= row["prediction_time"] for row in dataset.rows)
    assert all(
        row["label_available_at"] is None or row["label_available_at"] <= row["prediction_time"]
        for row in dataset.rows
    )


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
    assert frame.columns == list(PIT_DATASET_SCHEMA)
    assert frame.schema == PIT_DATASET_SCHEMA
    assert (tmp_path / "dataset_manifest.json").is_file()

    raw = load_config(CONFIG_PATH).model_dump(mode="python")
    raw["dataset"]["splits"]["train_fraction"] = 0.8
    with pytest.raises(ValueError):
        SimulationRunConfig.model_validate(raw)
