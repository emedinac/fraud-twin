import json
from datetime import timedelta
from functools import cache
from pathlib import Path

import polars as pl
import pytest
import yaml
from pydantic import ValidationError
from typer.testing import CliRunner

from fraudtwin.cli import app
from fraudtwin.config import SimulationRunConfig, load_config
from fraudtwin.domain import (
    PaymentEvent,
    validate_card_lifecycle,
    validate_ledger,
    validate_pix_lifecycle,
)
from fraudtwin.simulation import BehaviorGenerator, EntityGenerator
from fraudtwin.simulation.behavior import BehaviorDataset
from fraudtwin.simulation.generator import EntityDataset
from fraudtwin.simulation.parquet import (
    FRAUD_RECORD_SCHEMA,
    PAYMENT_EVENT_SCHEMA,
    write_behavior_parquet,
)

CONFIG_PATH = Path("configs/minimal.yaml")
runner = CliRunner()


@cache
def _fraud_config() -> SimulationRunConfig:
    base = load_config(CONFIG_PATH)
    scenario_settings = {
        scenario_id: settings.model_copy(
            update={"attempt_count": 8 if scenario_id == "F02" else 20}
        )
        for scenario_id, settings in base.fraud.scenarios.items()
    }
    fraud = base.fraud.model_copy(
        update={
            "enabled": True,
            "target_rate": 1.0,
            "scenario_count": 5,
            "scenarios": scenario_settings,
        }
    )
    return base.model_copy(update={"fraud": fraud})


def _focused_scenario_config(
    scenario: str,
    *,
    target_rate: float = 1.0,
    scenario_count: int = 2,
    scenario_capacity: int = 2,
    hard_negative_rate: float = 0.0,
    labels_enabled: bool = False,
) -> SimulationRunConfig:
    """Build a small one-rail configuration for campaign semantics tests."""

    base = load_config(CONFIG_PATH)
    scenarios = {
        scenario_id: settings.model_copy(
            update={
                "enabled": scenario_id == scenario,
                "weight": 1.0 if scenario_id == scenario else 0.0,
                "count": scenario_capacity if scenario_id == scenario else 0,
            }
        )
        for scenario_id, settings in base.fraud.scenarios.items()
    }
    fraud = base.fraud.model_copy(
        update={
            "enabled": True,
            "target_rate": target_rate,
            "scenario_count": scenario_count,
            "hard_negative_rate": hard_negative_rate,
            "scenarios": scenarios,
        }
    )
    payments = base.payments.model_copy(
        update={
            "daily_target": 10,
            "rails": {"CARD": 0.0, "PIX": 1.0, "ACCOUNT_TRANSFER": 0.0},
        }
    )
    labels = base.labels.model_copy(
        update={
            "enabled": labels_enabled,
            "investigation_rate": 1.0,
            "missing_fraud_rate": 0.0,
            "preliminary_error_rate": 0.0,
            "correction_rate": 0.0,
        }
    )
    return base.model_copy(
        update={
            "payments": payments,
            "fraud": fraud,
            "labels": labels,
            "population": base.population.model_copy(update={"cards": 0}),
        }
    )


def _baseline_and_fraud(
    config: SimulationRunConfig,
) -> tuple[BehaviorDataset, BehaviorDataset]:
    entities = EntityGenerator(config).generate()
    baseline_config = config.model_copy(
        update={"fraud": config.fraud.model_copy(update={"enabled": False})}
    )
    baseline = BehaviorGenerator(baseline_config, entities).generate()
    fraud = BehaviorGenerator(config, entities).generate()
    return baseline, fraud


@cache
def _dataset() -> tuple[SimulationRunConfig, EntityDataset, BehaviorDataset]:
    config = _fraud_config()
    entities = EntityGenerator(config).generate()
    return config, entities, BehaviorGenerator(config, entities).generate()


def test_all_m6_scenarios_are_deterministic_and_explainable() -> None:
    config, entities, first = _dataset()
    second = BehaviorGenerator(config, entities).generate()

    assert first == second
    assert {record.scenario_type for record in first.fraud_records if record.fraud_truth} == {
        "F01",
        "F02",
        "F03",
        "F04",
        "F05",
    }
    assert all(record.reason and record.trigger for record in first.fraud_records)
    assert all(event.scenario_id and event.scenario_reason for event in first.fraud_events)
    assert len({payment.payment_id for payment in first.payments}) == len(first.payments)
    assert len({event.event_id for event in first.payment_events}) == len(first.payment_events)
    records_by_id = {record.fraud_record_id: record for record in first.fraud_records}
    payments_by_id = {payment.payment_id: payment for payment in first.payments}
    event_ids = {event.event_id for event in first.payment_events}
    entity_ids = {
        entity_id
        for collection in (
            entities.customers,
            entities.accounts,
            entities.cards,
            entities.devices,
            entities.merchants,
            entities.pix_keys,
        )
        for entity in collection
        for entity_id in entity.model_dump().values()
        if isinstance(entity_id, str)
    }
    for event in first.fraud_events:
        assert event.payment_id in payments_by_id
        assert event.fraud_record_id in records_by_id
        assert set(event.affected_entity_ids) <= entity_ids
        assert event.correlation_id == event.payment_id
    for record in first.fraud_records:
        assert record.payment_id in payments_by_id
        assert record.event_id in event_ids
        assert set(record.affected_entity_ids) <= entity_ids
        assert record.amount > 0
    assert all(
        event.event_time <= event.source_available_at <= event.ingested_at <= event.processed_at
        for event in first.payment_events
    )


@pytest.mark.parametrize(
    ("target_rate", "scenario_count", "scenario_capacity", "expected_campaigns"),
    (
        (0.5, 9, 9, 5),
        (1.0, 3, 9, 3),
        (1.0, 9, 2, 2),
    ),
)
def test_campaign_count_uses_all_three_documented_caps(
    target_rate: float,
    scenario_count: int,
    scenario_capacity: int,
    expected_campaigns: int,
) -> None:
    config = _focused_scenario_config(
        "F04",
        target_rate=target_rate,
        scenario_count=scenario_count,
        scenario_capacity=scenario_capacity,
    )
    baseline, fraud = _baseline_and_fraud(config)

    assert len(baseline.payments) == 10
    assert len([record for record in fraud.fraud_records if record.fraud_truth]) == (
        expected_campaigns
    )
    assert len(fraud.payments) - len(baseline.payments) == expected_campaigns


def test_f03_creates_two_true_fraud_payments_per_campaign() -> None:
    config = _focused_scenario_config("F03", scenario_count=2, scenario_capacity=2)
    baseline, fraud = _baseline_and_fraud(config)

    true_records = [record for record in fraud.fraud_records if record.fraud_truth]
    assert len(true_records) == 2 * 2
    assert len(fraud.payments) - len(baseline.payments) == 2 * 2


def test_hard_negatives_do_not_count_as_true_fraud() -> None:
    config = _focused_scenario_config(
        "F04", scenario_count=2, scenario_capacity=2, hard_negative_rate=1.0
    )
    _, fraud = _baseline_and_fraud(config)

    assert sum(record.fraud_truth for record in fraud.fraud_records) == 2
    assert sum(not record.fraud_truth for record in fraud.fraud_records) == 2
    assert all(
        record.record_type == "HARD_NEGATIVE"
        for record in fraud.fraud_records
        if not record.fraud_truth
    )


def test_label_observation_does_not_change_source_fraud() -> None:
    source_config = _focused_scenario_config("F04", labels_enabled=False)
    observed_config = _focused_scenario_config("F04", labels_enabled=True)
    _, source = _baseline_and_fraud(source_config)
    _, observed = _baseline_and_fraud(observed_config)

    assert source.payments == observed.payments
    assert source.fraud_records == observed.fraud_records


def test_scenario_sequences_and_hard_negatives_have_expected_signals() -> None:
    config, entities, dataset = _dataset()
    negatives = [record for record in dataset.fraud_records if not record.fraud_truth]
    assert len(negatives) == 5
    assert all(record.record_type == "HARD_NEGATIVE" for record in negatives)
    assert all(record.payment_id.startswith("PAY-") for record in negatives)

    scenario_events = {
        scenario: [
            event for event in dataset.fraud_events if event.scenario_id.startswith(scenario)
        ]
        for scenario in ("F01", "F02", "F03", "F04", "F05")
    }
    assert any(event.event_type == "CARD_DECLINED" for event in scenario_events["F01"])
    assert sum(event.event_type == "CARD_DECLINED" for event in scenario_events["F02"]) > 1
    assert {event.event_type for event in scenario_events["F03"]} >= {
        "FRAUD_AUTHENTICATION_SUSPICIOUS",
        "FRAUD_PROFILE_CHANGED",
        "FRAUD_BENEFICIARY_ADDED",
    }
    assert any(event.event_type == "PIX_SETTLED" for event in scenario_events["F04"])
    velocity_times = [
        event.event_time
        for event in scenario_events["F05"]
        if event.event_type == "CARD_AUTHORIZATION_REQUESTED"
    ]
    assert len(velocity_times) == 20
    assert max(velocity_times) - min(velocity_times) <= timedelta(seconds=60)

    payments = {payment.payment_id: payment for payment in dataset.payments}
    by_payment: dict[str, list[PaymentEvent]] = {}
    for event in dataset.payment_events:
        by_payment.setdefault(event.payment_id, []).append(event)
    for payment_id, events in by_payment.items():
        if payments[payment_id].payment_rail == "CARD":
            validate_card_lifecycle(payments[payment_id], tuple(events))
        elif payments[payment_id].payment_rail == "PIX":
            validate_pix_lifecycle(payments[payment_id], tuple(events))
    validate_ledger(
        entities.accounts,
        dataset.payments,
        dataset.payment_events,
        dataset.ledger_entries,
    )

    payments_by_id = {payment.payment_id: payment for payment in dataset.payments}
    negatives_by_scenario = {record.scenario_type: record for record in negatives}
    for scenario_type, record in negatives_by_scenario.items():
        prefix = f"PAY-{record.scenario_id}-"
        lookalike_payments = [
            payment for payment in dataset.payments if payment.payment_id.startswith(prefix)
        ]
        lookalike_events = [
            event for event in dataset.payment_events if event.payment_id.startswith(prefix)
        ]
        assert lookalike_payments
        assert lookalike_events
        assert all(event.scenario_id is None for event in lookalike_events)
        assert all(event.scenario_type == scenario_type for event in lookalike_events)
        assert all(event.scenario_trigger and event.scenario_reason for event in lookalike_events)
        assert all(event.affected_entity_ids for event in lookalike_events)
        assert all(event.payment_id in payments_by_id for event in lookalike_events)
        assert all(payment.amount > 0 for payment in lookalike_payments)

        if scenario_type in {"F01", "F02", "F05"}:
            assert all(payment.payment_rail == "CARD" for payment in lookalike_payments)
            assert len({payment.card_id for payment in lookalike_payments}) == 1
            authorization_times = [
                event.event_time
                for event in lookalike_events
                if event.event_type == "CARD_AUTHORIZATION_REQUESTED"
            ]
            expected_count = (
                3 if scenario_type == "F01" else config.fraud.scenarios[scenario_type].attempt_count
            )
            assert len(authorization_times) == expected_count
            if scenario_type in {"F02", "F05"}:
                assert max(authorization_times) - min(authorization_times) <= timedelta(
                    seconds=config.fraud.scenarios[scenario_type].window_seconds
                )
        elif scenario_type == "F03":
            assert len(lookalike_payments) == 2
            assert {event.event_type for event in lookalike_events} >= {
                "FRAUD_AUTHENTICATION_SUSPICIOUS",
                "FRAUD_PROFILE_CHANGED",
                "FRAUD_BENEFICIARY_ADDED",
                "TRANSFER_COMPLETED",
            }
        else:
            assert len(lookalike_payments) == 1
            assert lookalike_payments[0].payment_rail == "PIX"
            validate_pix_lifecycle(lookalike_payments[0], tuple(lookalike_events))

    assert all(not event.payment_id.startswith("PAY-HN-") for event in dataset.fraud_events)


def test_fraud_parquet_schema_and_disabled_behavior_are_stable(tmp_path: Path) -> None:
    base = load_config(CONFIG_PATH)
    entities = EntityGenerator(base).generate()
    baseline = BehaviorGenerator(base, entities).generate()
    disabled_config = base.model_copy(
        update={"fraud": base.fraud.model_copy(update={"enabled": False})}
    )
    disabled = BehaviorGenerator(disabled_config, entities).generate()
    assert baseline == disabled

    _, _, dataset = _dataset()
    paths = write_behavior_parquet(dataset, tmp_path / "run")
    assert pl.read_parquet(paths["payment_events"]).schema == PAYMENT_EVENT_SCHEMA
    assert pl.read_parquet(paths["payment_events"]).columns == list(PAYMENT_EVENT_SCHEMA)
    assert pl.read_parquet(paths["fraud_records"]).schema == FRAUD_RECORD_SCHEMA
    assert pl.read_parquet(paths["fraud_records"]).height == len(dataset.fraud_records)


def test_fraud_config_validation_rejects_unknown_and_invalid_settings() -> None:
    raw = load_config(CONFIG_PATH).model_dump()
    raw["fraud"]["unknown"] = True
    with pytest.raises(ValidationError):
        SimulationRunConfig.model_validate(raw)

    for field, value in (("hard_negative_rate", 1.1), ("scenario_count", -1)):
        raw = load_config(CONFIG_PATH).model_dump()
        raw["fraud"][field] = value
        with pytest.raises(ValidationError):
            SimulationRunConfig.model_validate(raw)

    raw = load_config(CONFIG_PATH).model_dump()
    raw["fraud"]["scenarios"]["F04"]["amount_min"] = 50
    raw["fraud"]["scenarios"]["F04"]["amount_max"] = 10
    with pytest.raises(ValidationError):
        SimulationRunConfig.model_validate(raw)


def test_cli_generates_enabled_fraud_to_temporary_directory(tmp_path: Path) -> None:
    config = _fraud_config()
    config_path = tmp_path / "fraud.yaml"
    config_path.write_text(yaml.safe_dump(config.model_dump(mode="json")), encoding="utf-8")
    result = runner.invoke(
        app, ["generate", str(config_path), "--output-dir", str(tmp_path / "out")]
    )

    assert result.exit_code == 0, result.stdout
    manifest_path = next((tmp_path / "out").glob("*/manifest.json"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["fraud_counts"]["F01"] > 0
    assert manifest["fraud_counts"]["hard_negatives"] == 5
    assert manifest["event_counts"]["fraud_events"] > 0
