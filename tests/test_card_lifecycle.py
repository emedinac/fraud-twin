from functools import cache
from pathlib import Path

import polars as pl
import pytest
from pydantic import ValidationError

from fraudtwin.config import SimulationRunConfig, load_config
from fraudtwin.domain import validate_card_lifecycle, validate_ledger
from fraudtwin.simulation import BehaviorGenerator, EntityGenerator
from fraudtwin.simulation.parquet import (
    PAYMENT_EVENT_SCHEMA,
    PAYMENT_LIFECYCLE_EVENT_SCHEMA,
    write_behavior_parquet,
)

CONFIG_PATH = Path("configs/minimal.yaml")


@cache
def _dataset(**lifecycle: float | int):
    base = load_config(CONFIG_PATH)
    config = base.model_copy(
        update={
            "payments": base.payments.model_copy(update={"daily_target": 120}),
            "card_lifecycle": base.card_lifecycle.model_copy(update=lifecycle),
        }
    )
    entities = EntityGenerator(config).generate()
    return config, entities, BehaviorGenerator(config, entities).generate()


def _events_by_payment(dataset):
    grouped = {}
    for event in dataset.payment_events:
        grouped.setdefault(event.payment_id, []).append(event)
    return grouped


def test_card_lifecycle_is_deterministic_and_ids_are_stable() -> None:
    first_config, first_entities, first = _dataset()
    second = BehaviorGenerator(first_config, first_entities).generate()

    assert first == second
    assert [payment.payment_id for payment in first.payments] == [
        f"PAY-{number:08d}" for number in range(1, 121)
    ]
    assert len({event.event_id for event in first.payment_events}) == len(first.payment_events)
    assert len({payment.payment_id for payment in first.payments}) == 120


def test_declined_authorizations_have_no_downstream_events() -> None:
    _, _, dataset = _dataset(authorization_approval_probability=0.0)
    payments = {payment.payment_id: payment for payment in dataset.payments}

    for payment_id, events in _events_by_payment(dataset).items():
        if payments[payment_id].payment_rail == "CARD":
            assert [event.event_type for event in events] == [
                "CARD_AUTHORIZATION_REQUESTED",
                "CARD_DECLINED",
            ]
            assert payments[payment_id].current_status == "DECLINED"
            assert not any(
                event.event_type in {"CARD_CAPTURED", "CARD_CLEARED", "CARD_SETTLED"}
                for event in events
            )


def test_approved_authorizations_follow_valid_order_and_can_refund() -> None:
    _, entities, dataset = _dataset(
        authorization_approval_probability=1.0,
        reversal_probability=0.0,
        refund_probability=1.0,
    )
    payments = {payment.payment_id: payment for payment in dataset.payments}

    for payment_id, events in _events_by_payment(dataset).items():
        if payments[payment_id].payment_rail != "CARD":
            continue
        assert [event.event_type for event in events] == [
            "CARD_AUTHORIZATION_REQUESTED",
            "CARD_AUTHORIZED",
            "CARD_CAPTURED",
            "CARD_CLEARED",
            "CARD_SETTLED",
            "CARD_REFUNDED",
        ]
        validate_card_lifecycle(payments[payment_id], tuple(events))
        assert all(
            current.event_time > previous.event_time
            for previous, current in zip(events, events[1:], strict=False)
        )
        assert all(
            current.causation_id == previous.event_id
            for previous, current in zip(events, events[1:], strict=False)
        )


def test_reversals_only_follow_authorized_or_captured_states() -> None:
    _, _, dataset = _dataset(
        authorization_approval_probability=1.0,
        reversal_probability=1.0,
        refund_probability=1.0,
    )
    payments = {payment.payment_id: payment for payment in dataset.payments}

    for payment_id, events in _events_by_payment(dataset).items():
        if payments[payment_id].payment_rail != "CARD":
            continue
        event_types = [event.event_type for event in events]
        assert event_types[-1] == "CARD_REVERSED"
        assert event_types in (
            ["CARD_AUTHORIZATION_REQUESTED", "CARD_AUTHORIZED", "CARD_REVERSED"],
            [
                "CARD_AUTHORIZATION_REQUESTED",
                "CARD_AUTHORIZED",
                "CARD_CAPTURED",
                "CARD_REVERSED",
            ],
        )
        validate_card_lifecycle(payments[payment_id], tuple(events))


def test_chargebacks_follow_settlement_and_resolve() -> None:
    _, entities, dataset = _dataset(
        authorization_approval_probability=1.0,
        reversal_probability=0.0,
        refund_probability=0.0,
        chargeback_probability=1.0,
        chargeback_delay_seconds=1,
        chargeback_resolution_delay_seconds=1,
    )
    payments = {payment.payment_id: payment for payment in dataset.payments}
    for payment_id, events in _events_by_payment(dataset).items():
        if payments[payment_id].payment_rail != "CARD":
            continue
        assert [event.event_type for event in events] == [
            "CARD_AUTHORIZATION_REQUESTED",
            "CARD_AUTHORIZED",
            "CARD_CAPTURED",
            "CARD_CLEARED",
            "CARD_SETTLED",
            "CARD_CHARGEBACK_CREATED",
            "CARD_CHARGEBACK_RESOLVED",
        ]
        assert payments[payment_id].current_status == "CHARGEBACK_RESOLVED"
        validate_card_lifecycle(payments[payment_id], tuple(events))
    validate_ledger(
        entities.accounts,
        dataset.payments,
        dataset.payment_events,
        dataset.ledger_entries,
    )


def test_non_card_records_are_independent_of_lifecycle_settings() -> None:
    _, _, baseline = _dataset(
        authorization_approval_probability=0.0,
        reversal_probability=0.0,
        refund_probability=0.0,
    )
    _, _, changed = _dataset(
        authorization_approval_probability=1.0,
        reversal_probability=1.0,
        refund_probability=1.0,
    )
    baseline_payments = {
        payment.payment_id: payment
        for payment in baseline.payments
        if payment.payment_rail != "CARD"
    }
    changed_payments = {
        payment.payment_id: payment
        for payment in changed.payments
        if payment.payment_rail != "CARD"
    }
    assert baseline_payments == changed_payments
    assert [event for event in baseline.payment_events if event.payment_rail != "CARD"] == [
        event for event in changed.payment_events if event.payment_rail != "CARD"
    ]


def test_lifecycle_parquet_schema_is_explicit_and_stable(tmp_path: Path) -> None:
    _, _, dataset = _dataset()
    paths = write_behavior_parquet(dataset, tmp_path / "run-1")
    frame = pl.read_parquet(paths["payment_events"])

    assert frame.columns == list(PAYMENT_EVENT_SCHEMA)
    assert frame.schema == PAYMENT_LIFECYCLE_EVENT_SCHEMA
    assert frame.height == len(dataset.payment_events)


@pytest.mark.parametrize(
    "field,value",
    [
        ("authorization_approval_probability", 1.1),
        ("reversal_probability", -0.1),
        ("refund_probability", 2.0),
        ("authorization_delay_seconds", -1),
    ],
)
def test_invalid_lifecycle_configuration_is_rejected(field: str, value: float | int) -> None:
    raw = load_config(CONFIG_PATH).model_dump()
    raw["card_lifecycle"][field] = value

    with pytest.raises(ValidationError):
        SimulationRunConfig.model_validate(raw)


def test_unknown_lifecycle_configuration_is_rejected() -> None:
    raw = load_config(CONFIG_PATH).model_dump()
    raw["card_lifecycle"]["unknown"] = 1

    with pytest.raises(ValidationError):
        SimulationRunConfig.model_validate(raw)
