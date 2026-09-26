"""Focused counterfactual contract tests."""

import json
from pathlib import Path

import pytest
from pydantic import ValidationError
from typer.testing import CliRunner

from fraudtwin import generate_counterfactuals, resolve_counterfactual
from fraudtwin.cli import app
from fraudtwin.config import CounterfactualConfig, CounterfactualRequestConfig, load_config
from fraudtwin.manifest import create_manifest
from fraudtwin.simulation import BehaviorGenerator, EntityGenerator
from fraudtwin.simulation.payments import PaymentDataset

runner = CliRunner()


def _config() -> object:
    base = load_config(Path("configs/minimal-v1.yaml"))
    return base.model_copy(
        update={
            "counterfactual": CounterfactualConfig(
                enabled=True,
                budget=2.0,
                requests=(
                    CounterfactualRequestConfig(objective="F01"),
                    CounterfactualRequestConfig(objective="F03"),
                ),
            )
        }
    )


def test_counterfactual_configuration_is_strict_and_alias_conflicts_are_rejected() -> None:
    config = _config()
    assert resolve_counterfactual(config).enabled  # type: ignore[arg-type]
    raw = config.model_dump(mode="python")
    raw["counterfactual"]["requests"] = list(raw["counterfactual"]["requests"])
    raw["counterfactual"]["requests"][0] = {"scenario": "F01", "max_distance": 1.0}
    resolved = resolve_counterfactual(type(config).model_validate(raw))
    assert resolved.scopes[0].budget == 1.0
    raw["counterfactual"]["requests"][0]["objective"] = "F01"
    with pytest.raises(ValidationError, match="aliases conflict"):
        type(config).model_validate(raw)


def test_counterfactual_neutral_configuration_preserves_manifest_identity() -> None:
    base = load_config(Path("configs/minimal-v1.yaml"))
    neutral = base.model_copy(update={"counterfactual": CounterfactualConfig()})
    assert create_manifest(base).model_dump(mode="json") == create_manifest(neutral).model_dump(
        mode="json"
    )


def test_counterfactual_generation_is_deterministic_and_keeps_source_immutable() -> None:
    config = _config()
    entities = EntityGenerator(config).generate()  # type: ignore[arg-type]
    first = BehaviorGenerator(config, entities, simulation_run_id="RUN-M14").generate()  # type: ignore[arg-type]
    second = BehaviorGenerator(config, entities, simulation_run_id="RUN-M14").generate()  # type: ignore[arg-type]
    assert first.counterfactual == second.counterfactual
    assert first.counterfactual is not None
    accepted = [item for item in first.counterfactual.change_sets if item.status == "ACCEPTED"]
    assert accepted
    assert all(item.source_payment_id != item.derived_payment_id for item in accepted)
    assert tuple(first.payments) == tuple(second.payments)


def test_counterfactual_public_generator_accepts_pristine_payment_dataset() -> None:
    config = _config()
    entities = EntityGenerator(config).generate()  # type: ignore[arg-type]
    behavior = BehaviorGenerator(
        config.model_copy(update={"counterfactual": CounterfactualConfig()}), entities
    ).generate()  # type: ignore[arg-type]
    dataset = generate_counterfactuals(
        config,
        entities,
        PaymentDataset(behavior.payments, behavior.payment_events, behavior.ledger_entries),
        run_id="RUN-M14",
    )  # type: ignore[arg-type]
    assert dataset.metadata["effective_configuration_hash"]
    assert len(dataset.change_sets) == 2


def test_counterfactual_infeasible_budget_is_recorded_without_partial_output() -> None:
    config = _config().model_copy(
        update={
            "counterfactual": CounterfactualConfig(
                enabled=True,
                budget=0.1,
                requests=(CounterfactualRequestConfig(objective="F01"),),
            )
        }
    )
    entities = EntityGenerator(config).generate()  # type: ignore[arg-type]
    behavior = BehaviorGenerator(config, entities, simulation_run_id="RUN-M14").generate()  # type: ignore[arg-type]
    assert behavior.counterfactual is not None
    assert all(item.status == "REJECTED" for item in behavior.counterfactual.change_sets)
    assert not behavior.counterfactual.modified_payments


def test_counterfactual_standalone_cli_writes_append_only_sidecar(tmp_path: Path) -> None:
    result = runner.invoke(
        app,
        ["generate", "configs/minimal-v1.yaml", "--output-dir", str(tmp_path)],
    )
    assert result.exit_code == 0, result.stdout
    manifest_path = next(tmp_path.glob("RUN-*/manifest.json"))
    run_id = manifest_path.parent.name
    config_path = tmp_path / "m14.yaml"
    config_path.write_text(
        Path("configs/minimal-v1.yaml").read_text(encoding="utf-8")
        + "\n"
        + "counterfactual:\n"
        + "  enabled: true\n"
        + "  budget: 2.0\n"
        + "  requests:\n"
        + "    - {objective: F01, count: 1}\n"
        + "    - {objective: F03, count: 1}\n"
        + "    - {objective: F04, count: 1}\n",
        encoding="utf-8",
    )
    result = runner.invoke(
        app,
        [
            "counterfactual",
            "generate",
            "--config",
            str(config_path),
            "--source-run-id",
            run_id,
            "--output-dir",
            str(tmp_path),
        ],
    )
    assert result.exit_code == 0, result.stdout
    sidecar = next(manifest_path.parent.glob("counterfactuals/*/counterfactual_manifest.json"))
    payload = json.loads(sidecar.read_text(encoding="utf-8"))
    assert payload["counts"]["change_sets"] == 3
    assert payload["counts"]["fraud_records"] >= 1
    assert sidecar.parent.joinpath("oracle", "fraud_records.parquet").exists()
    assert sidecar.parent.joinpath("observable", "original", "fraud_cases.parquet").exists()
    assert sidecar.parent.joinpath("observable", "modified", "fraud_cases.parquet").exists()
