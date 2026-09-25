"""Focused fraud-difficulty engine contract tests."""

import json
from functools import cache
from pathlib import Path

import pytest
from pydantic import ValidationError
from typer.testing import CliRunner

from fraudtwin import apply_difficulty, resolve_difficulty
from fraudtwin.cli import app
from fraudtwin.config import BenchmarkConfig, SimulationRunConfig, load_config
from fraudtwin.difficulty import difficulty_metadata
from fraudtwin.domain import validate_card_lifecycle, validate_ledger, validate_pix_lifecycle
from fraudtwin.graph import build_graph, validate_graph
from fraudtwin.manifest import create_manifest
from fraudtwin.simulation import BehaviorGenerator, EntityGenerator

runner = CliRunner()


@cache
def _base() -> SimulationRunConfig:
    return load_config(Path("configs/minimal.yaml"))


def _fraud_config(level: int = 1, **controls: float) -> SimulationRunConfig:
    base = _base()
    fraud = base.fraud.model_copy(update={"enabled": True, "target_rate": 1.0, "scenario_count": 5})
    return base.model_copy(
        update={
            "fraud": fraud,
            "benchmark": BenchmarkConfig(difficulty=level, controls=controls),
        }
    )


def test_difficulty_configuration_is_strict_and_overrides_dimensions() -> None:
    raw = _base().model_dump(mode="python")
    raw["benchmark"] = {"controls": {"prevalence": 0.5}}
    with pytest.raises(ValidationError, match="difficulty is required"):
        SimulationRunConfig.model_validate(raw)

    raw = _base().model_dump(mode="python")
    raw["benchmark"] = {"difficulty": 11}
    with pytest.raises(ValidationError):
        SimulationRunConfig.model_validate(raw)

    config = _fraud_config(7, prevalence=0.2)
    resolved = resolve_difficulty(config)
    assert resolved.resolved_controls["prevalence"] == 0.2
    assert resolved.resolved_controls["fraud_legitimate_overlap"] == pytest.approx(6 / 9)
    assert apply_difficulty(resolved, "F01").scenario == "F01"
    assert apply_difficulty(resolved, "MULE_NETWORK").scenario == "MULE_NETWORK"
    assert apply_difficulty(resolved, "G01").scenario == "G01"


def test_inactive_difficulty_preserves_hash_and_manifest_shape() -> None:
    config = _base()
    empty = config.model_copy(update={"benchmark": BenchmarkConfig()})
    assert resolve_difficulty(config).enabled is False
    assert resolve_difficulty(empty).enabled is False
    assert create_manifest(config).model_dump(mode="json") == create_manifest(empty).model_dump(
        mode="json"
    )
    assert "difficulty" not in create_manifest(config).model_dump(mode="json")


def test_difficulty_generation_is_deterministic_and_keeps_oracle_separate() -> None:
    config = _fraud_config(8, noise_hard_negatives=0.6)
    entities = EntityGenerator(config).generate()
    first = BehaviorGenerator(config, entities).generate()
    second = BehaviorGenerator(config, entities).generate()
    assert first == second
    assert first.fraud_records
    assert all(event.scenario_id is None for event in first.payment_events)
    oracle_events = first.oracle_tables["payment_events"]
    assert any(event.scenario_id is not None for event in oracle_events)
    assert len(first.fraud_records) > len(
        [record for record in first.fraud_records if record.fraud_truth]
    )
    validate_ledger(entities.accounts, first.payments, first.payment_events, first.ledger_entries)
    by_payment = {payment.payment_id: [] for payment in first.payments}
    for event in first.payment_events:
        by_payment[event.payment_id].append(event)
    for payment in first.payments:
        events = tuple(by_payment[payment.payment_id])
        if payment.payment_rail == "CARD":
            validate_card_lifecycle(payment, events)
        elif payment.payment_rail == "PIX":
            validate_pix_lifecycle(payment, events)


def test_levels_have_monotonic_resolved_difficulty_dimensions() -> None:
    low = resolve_difficulty(_fraud_config(1))
    high = resolve_difficulty(_fraud_config(10))
    for name in low.resolved_controls:
        assert high.resolved_controls[name] >= low.resolved_controls[name]
    assert low.effective_configuration_hash != high.effective_configuration_hash


def test_observed_amount_overlap_is_reported_for_difficulty_runs() -> None:
    low_config = _fraud_config(1)
    high_config = _fraud_config(10)
    low_entities = EntityGenerator(low_config).generate()
    high_entities = EntityGenerator(high_config).generate()
    low = BehaviorGenerator(low_config, low_entities).generate()
    high = BehaviorGenerator(high_config, high_entities).generate()
    low_summary = difficulty_metadata(low_config, low_entities, low, "low")["measurable_summaries"]
    high_summary = difficulty_metadata(high_config, high_entities, high, "high")[
        "measurable_summaries"
    ]
    assert (
        high_summary["fraud_legitimate_amount_overlap_observed"]
        >= low_summary["fraud_legitimate_amount_overlap_observed"]
    )


def test_difficulty_cli_writes_difficulty_metadata_to_temporary_run(tmp_path: Path) -> None:
    config_path = tmp_path / "m12.yaml"
    config_path.write_text(
        Path("configs/benchmarks/difficulty-v1.yaml").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    result = runner.invoke(app, ["generate", str(config_path), "--output-dir", str(tmp_path)])
    assert result.exit_code == 0, result.stdout
    manifest_path = next(tmp_path.glob("RUN-*/manifest.json"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["difficulty"]["requested_difficulty"] == 7
    assert manifest["difficulty"]["effective_configuration_hash"]
    dataset_manifest = json.loads(
        (manifest_path.parent / "ml" / "dataset_manifest.json").read_text(encoding="utf-8")
    )
    assert (
        dataset_manifest["difficulty"]["effective_configuration_hash"]
        == manifest["difficulty"]["effective_configuration_hash"]
    )


def test_active_graph_difficulty_preserves_oracle_and_observable_views() -> None:
    base = load_config(Path("configs/benchmarks/graph-v2.yaml"))
    config = base.model_copy(update={"benchmark": BenchmarkConfig(difficulty=8)})
    entities = EntityGenerator(config).generate()
    behavior = BehaviorGenerator(config, entities).generate()
    manifest = create_manifest(config)
    oracle = build_graph(config, entities, behavior, manifest, view="oracle")
    observable = build_graph(config, entities, behavior, manifest, view="observable")
    validate_graph(oracle)
    validate_graph(observable)
    assert oracle.patterns
    assert len(oracle.edges) >= len(observable.edges)
    assert behavior.graph_memberships
