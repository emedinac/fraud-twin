from functools import cache
from pathlib import Path

import polars as pl
import pytest
from pydantic import ValidationError

from fraudtwin.config import SimulationRunConfig, load_config
from fraudtwin.domain import validate_ledger, validate_pix_lifecycle
from fraudtwin.simulation import BehaviorGenerator, EntityGenerator
from fraudtwin.simulation.parquet import (
    LEDGER_ENTRY_SCHEMA,
    PAYMENT_EVENT_SCHEMA,
    PAYMENT_SCHEMA,
    write_behavior_parquet,
)

CONFIG_PATH = Path("configs/minimal-v1.yaml")


@cache
def _dataset(**lifecycle: float | int):
    base = load_config(CONFIG_PATH)
    config = base.model_copy(
        update={
            "payments": base.payments.model_copy(
                update={"daily_target": 40, "rails": {"PIX": 1.0}}
            ),
            "behavior": base.behavior.model_copy(update={"amount_min": 1.0, "amount_max": 10.0}),
            "pix_lifecycle": base.pix_lifecycle.model_copy(update=lifecycle),
        }
    )
    entities = EntityGenerator(config).generate()
    return config, entities, BehaviorGenerator(config, entities).generate()


def _events_by_payment(dataset):
    grouped = {}
    for event in dataset.payment_events:
        grouped.setdefault(event.payment_id, []).append(event)
    return grouped


def test_success_path_is_ordered_deterministic_and_reconciles() -> None:
    config, entities, first = _dataset(
        authorization_approval_probability=1.0,
        rejection_probability=0.0,
        return_probability=0.0,
    )
    second = BehaviorGenerator(config, entities).generate()

    assert first == second
    payments = {payment.payment_id: payment for payment in first.payments}
    accounts = {account.account_id: account for account in entities.accounts}
    pix_keys = {key.pix_key_id: key for key in entities.pix_keys}
    expected = [
        "PIX_INITIATED",
        "PIX_VALIDATED",
        "PIX_AUTHORIZED",
        "PIX_SUBMITTED",
        "PIX_SETTLED",
        "PIX_RECEIVED",
    ]
    for payment_id, events in _events_by_payment(first).items():
        payment = payments[payment_id]
        assert [event.event_type for event in events] == expected
        assert payment.current_status == "RECEIVED"
        validate_pix_lifecycle(payment, tuple(events))
        assert all(
            current.event_time > previous.event_time
            for previous, current in zip(events, events[1:], strict=False)
        )
        assert all(
            current.causation_id == previous.event_id
            for previous, current in zip(events, events[1:], strict=False)
        )
        assert payment.payer_pix_key_id is not None
        assert payment.payee_pix_key_id is not None
        assert pix_keys[payment.payer_pix_key_id].account_id == payment.payer_account_id
        assert pix_keys[payment.payee_pix_key_id].account_id == payment.payee_account_id
        assert payment.payer_institution_id == accounts[payment.payer_account_id].institution_id
        assert payment.payee_institution_id == accounts[payment.payee_account_id].institution_id
    validate_ledger(entities.accounts, first.payments, first.payment_events, first.ledger_entries)


def test_rejected_pix_has_no_settlement_or_receipt() -> None:
    _, _, dataset = _dataset(authorization_approval_probability=0.0, return_probability=1.0)

    for payment, events in zip(dataset.payments, _events_by_payment(dataset).values(), strict=True):
        assert [event.event_type for event in events] == [
            "PIX_INITIATED",
            "PIX_VALIDATED",
            "PIX_REJECTED",
        ]
        assert payment.current_status == "REJECTED"
        assert not any(event.event_type in {"PIX_SETTLED", "PIX_RECEIVED"} for event in events)
        validate_pix_lifecycle(payment, tuple(events))
    assert not dataset.ledger_entries


def test_timeout_path_has_terminal_status_and_no_settlement_effects() -> None:
    _, entities, dataset = _dataset(
        authorization_approval_probability=1.0,
        rejection_probability=0.0,
        timeout_probability=1.0,
        timeout_delay_seconds=7,
    )
    payments = {payment.payment_id: payment for payment in dataset.payments}
    for payment_id, events in _events_by_payment(dataset).items():
        assert [event.event_type for event in events] == [
            "PIX_INITIATED",
            "PIX_VALIDATED",
            "PIX_AUTHORIZED",
            "PIX_SUBMITTED",
            "PIX_TIMEOUT",
        ]
        assert payments[payment_id].current_status == "TIMED_OUT"
        validate_pix_lifecycle(payments[payment_id], tuple(events))
    assert not dataset.ledger_entries
    validate_ledger(entities.accounts, dataset.payments, dataset.payment_events, ())


def test_return_can_start_directly_from_settlement() -> None:
    _, _, dataset = _dataset(
        authorization_approval_probability=1.0,
        rejection_probability=0.0,
        return_probability=0.0,
    )
    payment = dataset.payments[0]
    original_events = list(_events_by_payment(dataset)[payment.payment_id])
    settled = original_events[-2]
    events = original_events[:-2]
    returned_request = settled.model_copy(
        update={
            "event_id": f"{settled.event_id}-RETURN-REQUESTED",
            "event_type": "PIX_RETURN_REQUESTED",
            "event_time": settled.event_time.replace(
                microsecond=settled.event_time.microsecond + 1
            ),
            "causation_id": settled.event_id,
        }
    )
    returned = returned_request.model_copy(
        update={
            "event_id": f"{settled.event_id}-RETURNED",
            "event_type": "PIX_RETURNED",
            "event_time": returned_request.event_time.replace(
                microsecond=returned_request.event_time.microsecond + 1
            ),
            "causation_id": returned_request.event_id,
        }
    )
    events.extend((settled, returned_request, returned))
    updated_payment = payment.model_copy(update={"current_status": "RETURNED"})
    validate_pix_lifecycle(updated_payment, tuple(events))


def test_return_path_is_eligible_and_double_sided() -> None:
    _, entities, dataset = _dataset(
        authorization_approval_probability=1.0,
        rejection_probability=0.0,
        return_probability=1.0,
    )

    for payment, events in zip(dataset.payments, _events_by_payment(dataset).values(), strict=True):
        assert [event.event_type for event in events][-2:] == [
            "PIX_RETURN_REQUESTED",
            "PIX_RETURNED",
        ]
        assert payment.current_status == "RETURNED"
        validate_pix_lifecycle(payment, tuple(events))
    returned_ids = {
        event.event_id for event in dataset.payment_events if event.event_type == "PIX_RETURNED"
    }
    assert len(dataset.ledger_entries) == len(dataset.payments) * 4
    assert all(
        sum(entry.event_id == event_id for entry in dataset.ledger_entries) == 2
        for event_id in returned_ids
    )
    validate_ledger(
        entities.accounts,
        dataset.payments,
        dataset.payment_events,
        dataset.ledger_entries,
    )


def test_invalid_pix_transition_is_rejected() -> None:
    _, _, dataset = _dataset(
        authorization_approval_probability=1.0,
        rejection_probability=0.0,
        return_probability=0.0,
    )
    payment = dataset.payments[0]
    events = list(_events_by_payment(dataset)[payment.payment_id])
    events[2] = events[2].model_copy(update={"event_type": "PIX_SETTLED"})

    with pytest.raises(ValueError, match="invalid PIX lifecycle transition"):
        validate_pix_lifecycle(payment, tuple(events))


def test_pix_lifecycle_settings_do_not_change_card_or_transfer_records() -> None:
    base = load_config(CONFIG_PATH)
    entities = EntityGenerator(base).generate()
    baseline = BehaviorGenerator(base, entities).generate()
    changed_config = base.model_copy(
        update={
            "pix_lifecycle": base.pix_lifecycle.model_copy(
                update={
                    "authorization_approval_probability": 0.0,
                    "rejection_probability": 1.0,
                    "return_probability": 1.0,
                }
            )
        }
    )
    changed = BehaviorGenerator(changed_config, entities).generate()

    assert [payment for payment in baseline.payments if payment.payment_rail != "PIX"] == [
        payment for payment in changed.payments if payment.payment_rail != "PIX"
    ]
    assert [event for event in baseline.payment_events if event.payment_rail != "PIX"] == [
        event for event in changed.payment_events if event.payment_rail != "PIX"
    ]


def test_pix_payment_and_ledger_schemas_are_stable(tmp_path: Path) -> None:
    _, _, dataset = _dataset()
    paths = write_behavior_parquet(dataset, tmp_path / "run-1")

    for name, schema in {
        "payments": PAYMENT_SCHEMA,
        "payment_events": PAYMENT_EVENT_SCHEMA,
        "ledger_entries": LEDGER_ENTRY_SCHEMA,
    }.items():
        frame = pl.read_parquet(paths[name])
        assert frame.columns == list(schema)
        assert frame.schema == schema


@pytest.mark.parametrize(
    "field,value",
    [
        ("authorization_approval_probability", 1.1),
        ("rejection_probability", -0.1),
        ("return_probability", 2.0),
        ("validation_delay_seconds", -1),
    ],
)
def test_invalid_pix_lifecycle_configuration_is_rejected(field: str, value: float | int) -> None:
    raw = load_config(CONFIG_PATH).model_dump()
    raw["pix_lifecycle"][field] = value

    with pytest.raises(ValidationError):
        SimulationRunConfig.model_validate(raw)


def test_unknown_pix_lifecycle_configuration_is_rejected() -> None:
    raw = load_config(CONFIG_PATH).model_dump()
    raw["pix_lifecycle"]["unknown"] = 1

    with pytest.raises(ValidationError):
        SimulationRunConfig.model_validate(raw)
