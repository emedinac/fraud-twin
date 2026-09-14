import json
from datetime import UTC, datetime, timedelta
from functools import cache
from pathlib import Path

import polars as pl
import pytest
from pydantic import ValidationError
from typer.testing import CliRunner

from fraudtwin.cli import app
from fraudtwin.config import BacktestConfig, SimulationRunConfig, load_config
from fraudtwin.manifest import create_manifest, write_manifest
from fraudtwin.ml import (
    BACKTEST_ROW_SCHEMA,
    FOLD_METRIC_SCHEMA,
    BenchmarkPack,
    load_benchmark_pack,
    run_backtest,
)
from fraudtwin.replay import REPLAY_EVENT_SCHEMA, replay_run, write_replay
from fraudtwin.simulation import BehaviorGenerator, EntityGenerator
from fraudtwin.simulation.parquet import write_behavior_parquet, write_entity_parquet

CONFIG_PATH = Path("configs/minimal.yaml")
runner = CliRunner()


def _config(days: int = 10, *, fraud: bool = False) -> SimulationRunConfig:
    base = load_config(CONFIG_PATH)
    raw = base.model_dump(mode="python")
    raw["simulation"]["duration_days"] = days
    if fraud:
        raw["fraud"].update({"enabled": True, "target_rate": 1.0, "scenario_count": 5})
        raw["fraud_workflow"].update(
            {
                "alert_delay_seconds": 0,
                "case_open_delay_seconds": 0,
                "confirmation_delay_seconds": 0,
                "customer_dispute_delay_seconds": 0,
                "label_delay_seconds": 60,
            }
        )
    return SimulationRunConfig.model_validate(raw)


@cache
def _generated_run(days: int, fraud: bool):
    config = _config(days, fraud=fraud)
    manifest = create_manifest(config)
    entities = EntityGenerator(config).generate()
    behavior = BehaviorGenerator(config, entities, simulation_run_id=manifest.run_id).generate()
    return entities, behavior, manifest


def _write_run(tmp_path: Path, config: SimulationRunConfig) -> tuple[Path, object, object, object]:
    entities, behavior, manifest = _generated_run(
        config.simulation.duration_days, config.fraud.enabled
    )
    run_dir = tmp_path / manifest.run_id
    write_entity_parquet(entities, run_dir)
    write_behavior_parquet(behavior, run_dir)
    write_manifest(manifest, tmp_path)
    return run_dir, entities, behavior, manifest


def test_replay_is_deterministic_half_open_and_preserves_source_records(tmp_path: Path) -> None:
    run_dir, _, behavior, _ = _write_run(tmp_path, _config(days=1))
    start = datetime(2026, 1, 1, tzinfo=UTC)
    first = replay_run(run_dir, start, start + timedelta(days=1))
    second = replay_run(run_dir, start, start + timedelta(days=1))

    assert first == second
    assert all(start <= row["event_time"] < start + timedelta(days=1) for row in first.envelopes)
    assert {row["event_id"] for row in first.envelopes} == {
        event.event_id for event in behavior.payment_events
    }
    assert first.manifest.output_fingerprint == second.manifest.output_fingerprint

    envelope_path, manifest_path = write_replay(first, tmp_path / "replays")
    assert pl.read_parquet(envelope_path).schema == REPLAY_EVENT_SCHEMA
    assert json.loads(manifest_path.read_text(encoding="utf-8"))["source_run_id"]


def test_replay_order_modes_use_declared_stable_keys(tmp_path: Path) -> None:
    run_dir, _, _, _ = _write_run(tmp_path, _config(days=1))
    start = datetime(2026, 1, 1, tzinfo=UTC)
    event_order = replay_run(run_dir, start, start + timedelta(days=1), order="event_time_order")
    delivery_order = replay_run(
        run_dir, start, start + timedelta(days=1), order="original_delivery"
    )

    assert list(event_order.frame.columns) == list(REPLAY_EVENT_SCHEMA)
    assert list(event_order.frame["replay_sequence"]) == list(range(1, event_order.count + 1))
    delivery_keys = [
        (
            row["ingested_at"],
            row["processed_at"],
            row["source_available_at"],
            row["source_table"],
            row["source_row_number"],
            row["event_id"],
        )
        for row in delivery_order.envelopes
    ]
    assert delivery_keys == sorted(delivery_keys)


def test_replay_marks_deterministic_delivery_fallback_for_legacy_sources(tmp_path: Path) -> None:
    run_dir, _, _, _ = _write_run(tmp_path, _config(days=1))
    payment_events_path = run_dir / "payments" / "payment_events.parquet"
    pl.read_parquet(payment_events_path).drop(["ingested_at", "processed_at"]).write_parquet(
        payment_events_path
    )
    start = datetime(2026, 1, 1, tzinfo=UTC)

    result = replay_run(
        run_dir,
        start,
        start + timedelta(days=1),
        order="original_delivery",
    )

    assert result.manifest.ordering["delivery_fallback"] is True


def test_replay_restores_latent_truth_in_the_immutable_artifact(tmp_path: Path) -> None:
    run_dir, _, behavior, _ = _write_run(tmp_path, _config(days=1, fraud=True))
    start = datetime(2026, 1, 1, tzinfo=UTC)

    result = replay_run(run_dir, start, start + timedelta(days=1))

    assert result.behavior.fraud_cases
    truth_by_record = {
        record.fraud_record_id: record.fraud_truth for record in behavior.fraud_records
    }
    assert all(
        case.fraud_truth == truth_by_record[case.fraud_record_id]
        for case in result.behavior.fraud_cases
    )
    _, replay_manifest = write_replay(result, tmp_path / "truth-replays")
    replay_case_path = (
        tmp_path / "truth-replays" / result.manifest.replay_id / "fraud" / "fraud_cases.parquet"
    )
    assert pl.read_parquet(replay_case_path)["fraud_truth"].null_count() == 0
    assert json.loads(replay_manifest.read_text(encoding="utf-8"))["schema_fingerprint"]


def test_backtest_builds_reproducible_fixed_and_expanding_folds(tmp_path: Path) -> None:
    config = _config(days=10, fraud=True)
    run_dir, entities, behavior, source_manifest = _write_run(tmp_path, config)
    fixed = config.model_copy(
        update={
            "backtest": BacktestConfig(
                train_mode="fixed",
                train_window_seconds="3d",
                validation_window_seconds="1d",
                test_window_seconds="1d",
                label_maturity_gap_seconds="1d",
                step_seconds="1d",
            )
        }
    )
    first = run_backtest(fixed, entities, behavior, source_manifest)
    second = run_backtest(fixed, entities, behavior, source_manifest)

    assert first == second
    assert len(first.manifest.folds) == 4
    assert pl.DataFrame(first.fold_rows, schema=BACKTEST_ROW_SCHEMA).schema == BACKTEST_ROW_SCHEMA
    assert pl.DataFrame(first.fold_metrics, schema=FOLD_METRIC_SCHEMA).schema == FOLD_METRIC_SCHEMA
    for fold in first.manifest.folds:
        assert fold["train_from"] < fold["train_to"] <= fold["test_from"] < fold["test_to"]
    assert first.manifest.pit_validation["future_fold_mutation"] is False
    assert first.manifest.pit_validation["future_fold_isolation_checks"] == 4

    expanding = fixed.model_copy(
        update={
            "backtest": fixed.backtest.model_copy(
                update={"train_mode": "expanding", "train_window_seconds": None}
            )
        }
    )
    expanding_result = run_backtest(expanding, entities, behavior, source_manifest)
    assert expanding_result.manifest.folds[0]["train_from"] == "2026-01-01T00:00:00+00:00"
    assert expanding_result.manifest.folds[1]["train_from"] == "2026-01-01T00:00:00+00:00"


def test_regimes_are_strict_and_recorded_in_source_manifest() -> None:
    raw = load_config(CONFIG_PATH).model_dump(mode="python")
    raw["backtest"]["regimes"] = [
        {
            "id": "stable",
            "from": "2026-01-01T00:00:00Z",
            "to": "2026-01-01T12:00:00Z",
            "camouflage": 0.2,
        },
        {
            "id": "adaptive",
            "from": "2026-01-01T12:00:00Z",
            "to": "2026-01-02T00:00:00Z",
            "prevalence_multiplier": 2.5,
        },
    ]
    config = SimulationRunConfig.model_validate(raw)
    manifest = create_manifest(config)
    assert [item["id"] for item in manifest.regime_definitions] == ["stable", "adaptive"]

    raw["backtest"]["regimes"][1]["from"] = "2026-01-01T11:00:00Z"
    with pytest.raises(ValidationError, match="must not overlap"):
        SimulationRunConfig.model_validate(raw)


def test_regimes_change_source_history_and_source_identity() -> None:
    base = _config(days=10, fraud=True)
    raw = base.model_dump(mode="python")
    raw["backtest"]["regimes"] = [
        {
            "id": "early",
            "from": "2026-01-01T00:00:00Z",
            "to": "2026-01-06T00:00:00Z",
            "prevalence_multiplier": 1.0,
        },
        {
            "id": "adaptive",
            "from": "2026-01-06T00:00:00Z",
            "to": "2026-01-11T00:00:00Z",
            "prevalence_multiplier": 2.0,
            "amount_multiplier": 1.5,
        },
    ]
    regime_config = SimulationRunConfig.model_validate(raw)
    entities = EntityGenerator(regime_config).generate()
    behavior = BehaviorGenerator(regime_config, entities).generate()

    assert behavior.fraud_records
    assert any(
        record.occurred_at >= datetime(2026, 1, 6, tzinfo=UTC) for record in behavior.fraud_records
    )
    assert create_manifest(regime_config).run_id != create_manifest(base).run_id


def test_benchmark_pack_rejects_non_semver_versions() -> None:
    pack = load_benchmark_pack(Path("configs/benchmarks/m10-minimal-v1.yaml"))
    with pytest.raises(ValidationError, match="semantic"):
        BenchmarkPack.model_validate(pack.model_dump(mode="python") | {"version": "v1"})


def test_benchmark_pack_is_versioned_and_cli_generates_results(tmp_path: Path) -> None:
    pack = load_benchmark_pack(Path("configs/benchmarks/m10-minimal-v1.yaml"))
    assert isinstance(pack, BenchmarkPack)
    assert pack.identity.startswith("fraudtwin-m10-minimal@1.0.0-")
    assert pack.windows.stress is not None

    generated = runner.invoke(app, ["generate", str(CONFIG_PATH), "--output-dir", str(tmp_path)])
    assert generated.exit_code == 0, generated.stdout
    run_id = next(tmp_path.glob("*/manifest.json")).parent.name
    result = runner.invoke(
        app,
        [
            "ml",
            "backtest",
            str(CONFIG_PATH),
            "--run-id",
            run_id,
            "--benchmark-pack",
            "configs/benchmarks/m10-minimal-v1.yaml",
            "--output-dir",
            str(tmp_path),
        ],
    )
    assert result.exit_code == 0, result.stdout
    assert list(tmp_path.glob(f"{run_id}/ml/backtests/*/backtest_manifest.json"))
