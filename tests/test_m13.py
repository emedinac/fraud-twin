"""Focused Milestone 13 camouflage contracts."""

from pathlib import Path

import pytest
from pydantic import ValidationError

from fraudtwin.camouflage import resolve_camouflage
from fraudtwin.config import SimulationRunConfig, StressConfig, load_config
from fraudtwin.manifest import create_manifest
from fraudtwin.simulation import BehaviorGenerator, EntityGenerator


def _fraud_config(strength: float = 0.0) -> SimulationRunConfig:
    base = load_config(Path("configs/minimal.yaml"))
    fraud = base.fraud.model_copy(
        update={"enabled": True, "target_rate": 1.0, "scenario_count": 1, "hard_negative_rate": 0.0}
    )
    return base.model_copy(update={"fraud": fraud, "stress": StressConfig(camouflage=strength)})


def test_m13_configuration_is_strict_and_resolves_precedence() -> None:
    config = _fraud_config()
    raw = config.model_dump(mode="python")
    raw["stress"] = {
        "camouflage": 0.2,
        "feature_camouflage": 0.4,
        "families": {"fraud": {"features": {"amount": 0.9}}},
    }
    resolved = resolve_camouflage(SimulationRunConfig.model_validate(raw))
    assert resolved.plan("fraud", "F01").feature_strengths["amount"] == pytest.approx(0.9)
    assert resolved.plan("fraud", "F01").feature_strengths["timing"] == pytest.approx(0.4)
    assert resolved.plan("graph", "G01").relation_strengths["shares_ip"] == pytest.approx(0.2)

    raw["stress"]["unknown"] = 1.0
    with pytest.raises(ValidationError):
        SimulationRunConfig.model_validate(raw)

    raw = config.model_dump(mode="python")
    raw["stress"] = {"camouflage": 0.2}
    raw["benchmark"]["camouflage"] = 0.2
    with pytest.raises(ValidationError, match="stress or benchmark"):
        SimulationRunConfig.model_validate(raw)


def test_m13_alias_and_inactive_resolution() -> None:
    config = _fraud_config()
    assert resolve_camouflage(config).enabled is False
    raw = config.model_dump(mode="python")
    raw["stress"] = {"camouflage": 0.6}
    raw["benchmark"] = {"camouflage": 0.6}
    del raw["stress"]
    alias = SimulationRunConfig.model_validate(raw)
    assert resolve_camouflage(alias).source_namespace == "benchmark"


def test_m13_generation_is_deterministic_and_reports_similarity() -> None:
    config = _fraud_config(0.8)
    entities = EntityGenerator(config).generate()
    first = BehaviorGenerator(config, entities).generate()
    second = BehaviorGenerator(config, entities).generate()
    assert first == second
    assert first.camouflage_metadata["effective_configuration_hash"]
    summaries = first.camouflage_metadata["measurable_summaries"]
    assert summaries["feature_similarity"]
    assert first.camouflage_metadata["relation_summary"]["added_support_payments"] > 0
    assert all(event.scenario_id is None for event in first.payment_events)
    assert any(event.scenario_id is not None for event in first.oracle_tables["payment_events"])


def test_neutral_m13_keeps_manifest_shape_and_configuration_hash() -> None:
    base = load_config(Path("configs/minimal.yaml"))
    neutral = base.model_copy(update={"stress": StressConfig()})
    assert create_manifest(base).model_dump(mode="json") == create_manifest(neutral).model_dump(
        mode="json"
    )


def test_m13_fixture_validates() -> None:
    config = load_config(Path("configs/benchmarks/m13-camouflage-v1.yaml"))
    assert resolve_camouflage(config).enabled
    assert config.stress.families == {}


def test_m13_fixture_global_strength_changes_generated_summaries() -> None:
    base = load_config(Path("configs/benchmarks/m13-camouflage-v1.yaml"))
    low = base.model_copy(
        update={
            "stress": base.stress.model_copy(
                update={"camouflage": 0.0, "feature_camouflage": 0.0, "relation_camouflage": 0.0}
            )
        }
    )
    high = base.model_copy(
        update={
            "stress": base.stress.model_copy(
                update={"camouflage": 1.0, "feature_camouflage": 1.0, "relation_camouflage": 1.0}
            )
        }
    )
    low_behavior = BehaviorGenerator(low, EntityGenerator(low).generate()).generate()
    high_behavior = BehaviorGenerator(high, EntityGenerator(high).generate()).generate()
    assert low_behavior.camouflage_metadata == {}
    assert high_behavior.camouflage_metadata["measurable_summaries"] != {}
