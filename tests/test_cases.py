import json
from functools import cache
from pathlib import Path

import polars as pl
import pytest
import yaml
from pydantic import ValidationError
from typer.testing import CliRunner

from fraudtwin.cli import app
from fraudtwin.config import SimulationRunConfig, load_config
from fraudtwin.domain import validate_fraud_workflow, validate_ledger
from fraudtwin.ml import load_generated_run
from fraudtwin.simulation import BehaviorGenerator, EntityGenerator
from fraudtwin.simulation.parquet import (
    CASE_CONFIRMATION_SCHEMA,
    CUSTOMER_DISPUTE_SCHEMA,
    FRAUD_ALERT_SCHEMA,
    FRAUD_CASE_SCHEMA,
    FRAUD_LABEL_SCHEMA,
    write_behavior_parquet,
)

CONFIG_PATH = Path("configs/minimal.yaml")
runner = CliRunner()


@cache
def _dataset():
    base = load_config(CONFIG_PATH)
    scenario_settings = {
        scenario_id: settings.model_copy(
            update={"attempt_count": 8 if scenario_id == "F02" else 20}
        )
        for scenario_id, settings in base.fraud.scenarios.items()
    }
    config = base.model_copy(
        update={
            "fraud": base.fraud.model_copy(
                update={
                    "enabled": True,
                    "target_rate": 1.0,
                    "scenario_count": 5,
                    "scenarios": scenario_settings,
                }
            )
        }
    )
    entities = EntityGenerator(config).generate()
    return config, entities, BehaviorGenerator(config, entities).generate()


def test_m7_is_deterministic_and_causally_covers_fraud_records() -> None:
    config, entities, first = _dataset()
    second = BehaviorGenerator(config, entities).generate()

    assert first == second
    assert len(first.alerts) == len(first.fraud_records)
    assert len(first.fraud_cases) == len(first.fraud_records)
    assert len(first.case_confirmations) == len(first.fraud_records)
    assert len(first.customer_disputes) == sum(record.fraud_truth for record in first.fraud_records)
    assert len(first.fraud_labels) == len(first.fraud_records)
    assert {label.label for label in first.fraud_labels} == {"FRAUD", "LEGITIMATE"}
    assert [alert.fraud_alert_id for alert in first.alerts] == [
        f"ALT-{number:06d}" for number in range(1, len(first.alerts) + 1)
    ]
    assert [case.fraud_case_id for case in first.fraud_cases] == [
        f"CASE-{number:06d}" for number in range(1, len(first.fraud_cases) + 1)
    ]
    validate_ledger(entities.accounts, first.payments, first.payment_events, first.ledger_entries)


def test_m7_references_and_causal_ids_are_valid() -> None:
    _, entities, dataset = _dataset()
    records = {record.fraud_record_id: record for record in dataset.fraud_records}
    cases = {case.fraud_case_id: case for case in dataset.fraud_cases}
    confirmations = {item.fraud_case_id: item for item in dataset.case_confirmations}
    disputes = {item.fraud_case_id: item for item in dataset.customer_disputes}
    payments = {payment.payment_id: payment for payment in dataset.payments}
    events = {event.event_id: event for event in dataset.payment_events}
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

    for alert in dataset.alerts:
        record = records[alert.fraud_record_id]
        assert alert.event_id == record.event_id in events
        assert alert.payment_id == record.payment_id in payments
        assert alert.causation_id == alert.event_id
        assert set(alert.affected_entity_ids) <= entity_ids
        case = next(
            case for case in dataset.fraud_cases if case.fraud_alert_id == alert.fraud_alert_id
        )
        assert case.causation_id == alert.fraud_alert_id
        assert case.label_available_at >= case.fraud_occurred_at
        if case.fraud_confirmed_at is not None:
            assert case.label_available_at >= case.fraud_confirmed_at
        label = next(
            label for label in dataset.fraud_labels if label.fraud_case_id == case.fraud_case_id
        )
        assert label.fraud_occurred_at < label.label_available_at
        assert label.fraud_truth == record.fraud_truth

    for case in dataset.fraud_cases:
        assert case.fraud_case_id in cases
        assert case.fraud_record_id in records
        confirmation = confirmations[case.fraud_case_id]
        assert confirmation.causation_id == case.fraud_case_id
        assert confirmation.investigation_outcome == (
            "CONFIRMED_FRAUD" if case.fraud_truth else "FALSE_POSITIVE"
        )
        if case.fraud_truth:
            assert case.fraud_confirmed_at == confirmation.confirmed_at
        else:
            assert case.fraud_confirmed_at is None
        if case.fraud_truth:
            dispute = disputes[case.fraud_case_id]
            assert dispute.causation_id == case.fraud_case_id
            assert dispute.underlying_event_id == case.event_id in events
            assert dispute.event_time <= dispute.processed_at <= case.label_available_at

    validate_fraud_workflow(
        dataset.alerts,
        dataset.fraud_cases,
        dataset.case_confirmations,
        dataset.customer_disputes,
        dataset.fraud_labels,
        customer_ids=frozenset(entity.customer_id for entity in entities.customers),
        account_ids=frozenset(entity.account_id for entity in entities.accounts),
        card_ids=frozenset(entity.card_id for entity in entities.cards),
        device_ids=frozenset(entity.device_id for entity in entities.devices),
        merchant_ids=frozenset(entity.merchant_id for entity in entities.merchants),
        payment_ids=frozenset(payments),
        event_ids=frozenset(events),
        fraud_record_ids=frozenset(records),
    )


def test_m7_confirmed_loss_uses_realized_payment_outcome() -> None:
    _, _, dataset = _dataset()
    payments = {payment.payment_id: payment for payment in dataset.payments}

    for case in dataset.fraud_cases:
        payment = payments[case.payment_id]
        if case.investigation_outcome != "CONFIRMED_FRAUD":
            assert case.loss_amount == 0.0
            continue
        entries = [
            entry for entry in dataset.ledger_entries if entry.payment_id == payment.payment_id
        ]
        if entries:
            debit = sum(
                entry.amount
                for entry in entries
                if entry.account_id == payment.payer_account_id and entry.entry_type == "DEBIT"
            )
            credit = sum(
                entry.amount
                for entry in entries
                if entry.account_id == payment.payer_account_id and entry.entry_type == "CREDIT"
            )
            expected_loss = max(0.0, round(debit - credit, 2))
        else:
            expected_loss = (
                payment.amount
                if payment.current_status in {"SETTLED", "COMPLETED", "RECEIVED"}
                else 0.0
            )
        assert case.loss_amount == expected_loss


def test_m7_can_leave_cases_unconfirmed_without_exposing_an_early_label() -> None:
    base = load_config(CONFIG_PATH)
    workflow = base.fraud_workflow.model_copy(
        update={
            "confirmation_probability": 0.0,
            "customer_dispute_probability": 0.0,
        }
    )
    fraud = base.fraud.model_copy(update={"enabled": True, "target_rate": 1.0, "scenario_count": 1})
    config = base.model_copy(update={"fraud": fraud, "fraud_workflow": workflow})
    entities = EntityGenerator(config).generate()
    dataset = BehaviorGenerator(config, entities).generate()

    assert dataset.fraud_cases
    assert not dataset.case_confirmations
    assert not dataset.customer_disputes
    assert not dataset.fraud_labels
    assert all(case.fraud_confirmed_at is None for case in dataset.fraud_cases)
    assert all(case.case_closed_at is None for case in dataset.fraud_cases)
    assert all(case.label_available_at is None for case in dataset.fraud_cases)
    assert any(record.fraud_truth for record in dataset.fraud_records)


def test_m7_operational_outputs_do_not_expose_oracle_truth(tmp_path: Path) -> None:
    base = load_config(CONFIG_PATH)
    workflow = base.fraud_workflow.model_copy(
        update={
            "confirmation_probability": 0.0,
            "customer_dispute_probability": 0.0,
        }
    )
    fraud = base.fraud.model_copy(update={"enabled": True, "target_rate": 1.0, "scenario_count": 1})
    config = base.model_copy(update={"fraud": fraud, "fraud_workflow": workflow})
    entities = EntityGenerator(config).generate()
    dataset = BehaviorGenerator(config, entities).generate()
    paths = write_behavior_parquet(dataset, tmp_path / "run")

    for table_name in ("fraud_cases", "case_confirmations", "fraud_labels"):
        frame = pl.read_parquet(paths[table_name])
        assert frame["fraud_truth"].null_count() == frame.height
    assert all(alert.severity == "MEDIUM" for alert in dataset.alerts)
    assert all(case.loss_amount == 0.0 for case in dataset.fraud_cases)


def test_m7_persisted_workflow_is_revalidated_before_pit_loading(tmp_path: Path) -> None:
    config, _, dataset = _dataset()
    config_path = tmp_path / "m7.yaml"
    config_path.write_text(yaml.safe_dump(config.model_dump(mode="json")), encoding="utf-8")
    result = runner.invoke(
        app,
        ["generate", str(config_path), "--output-dir", str(tmp_path / "out")],
    )
    assert result.exit_code == 0, result.stdout
    run_dir = next((tmp_path / "out").glob("*/manifest.json")).parent
    _, loaded, _ = load_generated_run(run_dir)
    assert loaded.fraud_cases
    assert all(case.fraud_truth is None for case in loaded.fraud_cases)
    assert len(loaded.fraud_labels) == len(dataset.fraud_labels)


def test_m7_parquet_schemas_and_manifest_counts_are_stable(tmp_path: Path) -> None:
    config, entities, dataset = _dataset()
    paths = write_behavior_parquet(dataset, tmp_path / "run")
    schemas = {
        "fraud_alerts": FRAUD_ALERT_SCHEMA,
        "fraud_cases": FRAUD_CASE_SCHEMA,
        "case_confirmations": CASE_CONFIRMATION_SCHEMA,
        "customer_disputes": CUSTOMER_DISPUTE_SCHEMA,
        "fraud_labels": FRAUD_LABEL_SCHEMA,
    }
    for table_name, schema in schemas.items():
        frame = pl.read_parquet(paths[table_name])
        assert frame.columns == list(schema)
        assert frame.schema == schema
        assert frame.height == dataset.counts[table_name]

    config_path = tmp_path / "m7.yaml"
    config_path.write_text(yaml.safe_dump(config.model_dump(mode="json")), encoding="utf-8")
    result = runner.invoke(
        app,
        ["generate", str(config_path), "--output-dir", str(tmp_path / "out")],
    )
    assert result.exit_code == 0, result.stdout
    manifest_path = next((tmp_path / "out").glob("*/manifest.json"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["fraud_counts"]["alerts"] == len(dataset.alerts)
    assert manifest["fraud_counts"]["cases"] == len(dataset.fraud_cases)
    assert manifest["fraud_counts"]["confirmations"] == len(dataset.case_confirmations)
    assert manifest["fraud_counts"]["disputes"] == len(dataset.customer_disputes)
    assert manifest["fraud_counts"]["delayed_labels"] == len(dataset.fraud_labels)


@pytest.mark.parametrize(
    "field,value",
    [
        ("alert_probability", 1.1),
        ("case_open_probability", -0.1),
        ("confirmation_probability", 2.0),
        ("customer_dispute_probability", 1.1),
        ("label_delay_seconds", -1),
    ],
)
def test_m7_configuration_rejects_invalid_values(field: str, value: float | int) -> None:
    raw = load_config(CONFIG_PATH).model_dump()
    raw["fraud_workflow"][field] = value
    with pytest.raises(ValidationError):
        SimulationRunConfig.model_validate(raw)


def test_m7_configuration_rejects_unknown_values() -> None:
    raw = load_config(CONFIG_PATH).model_dump()
    raw["fraud_workflow"]["unknown"] = True
    with pytest.raises(ValidationError):
        SimulationRunConfig.model_validate(raw)
