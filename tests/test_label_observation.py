"""Focused Milestone 17 label-observation tests."""

from datetime import timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from fraudtwin.config import SimulationRunConfig, load_config
from fraudtwin.generation import generate
from fraudtwin.label_observation import (
    apply_label_observation,
    reconstruct_label_history,
    validate_label_observation,
    visible_label_at,
)
from fraudtwin.ml import load_generated_run
from fraudtwin.ml.dataset import PointInTimeDatasetBuilder
from fraudtwin.simulation import BehaviorGenerator, EntityGenerator


def _enabled_config():
    base = load_config(Path("configs/minimal.yaml"))
    fraud = base.fraud.model_copy(update={"enabled": True, "target_rate": 1.0, "scenario_count": 1})
    labels = base.labels.model_copy(
        update={
            "enabled": True,
            "investigation_rate": 1.0,
            "missing_fraud_rate": 0.0,
            "preliminary_error_rate": 1.0,
            "correction_rate": 1.0,
            "reopening_rate": 1.0,
        }
    )
    return base.model_copy(update={"fraud": fraud, "labels": labels})


def test_label_observation_strict_configuration_accepts_compact_delay_and_rejects_unknown() -> None:
    base = load_config(Path("configs/minimal.yaml"))
    values = base.model_dump(mode="python")
    values["labels"] = {"enabled": True, "confirmation_delay": "lognormal"}
    config = SimulationRunConfig.model_validate(values)
    assert config.labels.confirmation_delay.distribution == "lognormal"
    values["labels"]["unknown"] = True
    with pytest.raises(ValidationError):
        SimulationRunConfig.model_validate(values)


def test_label_observation_history_is_deterministic_and_truth_is_immutable() -> None:
    config = _enabled_config()
    entities = EntityGenerator(config).generate()
    first_behavior = BehaviorGenerator(config, entities).generate()
    second_behavior = BehaviorGenerator(config, entities).generate()
    assert first_behavior.label_observations == second_behavior.label_observations
    for item in first_behavior.label_observations:
        assert reconstruct_label_history(item) == item.versions
        assert all(version.truth_label == item.truth_label for version in item.versions)
        visible = visible_label_at(
            item, item.versions[-1].label_available_at or config.simulation.start
        )
        assert visible.label_version >= 0


def test_label_observation_pit_uses_only_available_version() -> None:
    config = _enabled_config()
    entities = EntityGenerator(config).generate()
    behavior = BehaviorGenerator(config, entities).generate()
    observation = next(item for item in behavior.label_observations if len(item.versions) > 1)
    first_visible_at = observation.versions[1].label_available_at
    assert first_visible_at is not None
    before = first_visible_at - timedelta(seconds=1)
    version = visible_label_at(observation, before)
    assert version.label_version == 0
    dataset = PointInTimeDatasetBuilder(config, entities, behavior).build(
        {observation.payment_id: before}
    )
    row = next(item for item in dataset.rows if item["payment_id"] == observation.payment_id)
    assert row["label"] is None
    assert row["fraud_truth"] is None


def test_label_observation_standalone_policy_requires_seed() -> None:
    config = _enabled_config()
    entities = EntityGenerator(config).generate()
    behavior = BehaviorGenerator(config, entities).generate()
    with pytest.raises(ValueError, match="seed is required"):
        apply_label_observation(
            config.labels,
            behavior.fraud_records,
            behavior.payments,
        )


def test_label_observation_validation_rejects_inconsistent_and_duplicate_version_ids() -> None:
    config = _enabled_config()
    entities = EntityGenerator(config).generate()
    behavior = BehaviorGenerator(config, entities).generate()
    observation = behavior.label_observations[0]

    mismatched = observation.versions[0].model_copy(update={"payment_id": "PAY-other"})
    with pytest.raises(ValueError, match="another payment"):
        validate_label_observation((observation.model_copy(update={"versions": (mismatched,)}),))

    duplicate = observation.versions[1].model_copy(
        update={"label_version_id": observation.versions[0].label_version_id}
    )
    with pytest.raises(ValueError, match="duplicate label version ID"):
        validate_label_observation(
            (
                observation.model_copy(
                    update={
                        "versions": (observation.versions[0], duplicate, *observation.versions[2:])
                    }
                ),
            )
        )


def test_label_observation_sidecar_round_trips_typed_history_artifacts(tmp_path: Path) -> None:
    config = _enabled_config()
    source = generate(config)
    generated = generate(config, write=True, output_dir=tmp_path)
    _, loaded, _ = load_generated_run(generated.run_dir)

    assert loaded.label_observations == source.behavior.label_observations
    sidecar = next((tmp_path / generated.run_id / "label_observations").iterdir())
    assert (sidecar / "oracle" / "label_corrections.parquet").is_file()
    assert (sidecar / "oracle" / "case_reopenings.parquet").is_file()
    assert (sidecar / "oracle" / "observation_provenance.parquet").is_file()
