from datetime import UTC, timedelta
from pathlib import Path

import polars as pl

from fraudtwin.config import load_config
from fraudtwin.simulation import BehaviorGenerator, EntityGenerator
from fraudtwin.simulation.parquet import (
    BEHAVIOR_PROFILE_SCHEMA,
    PAYMENT_EVENT_SCHEMA,
    PAYMENT_SCHEMA,
    write_behavior_parquet,
)

CONFIG_PATH = Path("configs/minimal.yaml")


def _dataset():
    config = load_config(CONFIG_PATH)
    entities = EntityGenerator(config).generate()
    return config, entities, BehaviorGenerator(config, entities).generate()


def test_profiles_vary_between_customers_and_are_deterministic() -> None:
    first_config, first_entities, first = _dataset()
    second = BehaviorGenerator(first_config, first_entities).generate()

    assert first == second
    assert len(first.profiles) == len(first_entities.customers)
    assert len({profile.spending_level for profile in first.profiles}) > 1
    assert len({profile.typical_payment_hours for profile in first.profiles}) > 1
    assert len({profile.monthly_income for profile in first.profiles}) > 1


def test_payments_have_stable_order_valid_relationships_and_profile_preferences() -> None:
    _, entities, dataset = _dataset()
    profiles = {profile.customer_id: profile for profile in dataset.profiles}
    accounts = {account.account_id for account in entities.accounts}
    cards = {card.card_id for card in entities.cards}
    merchants = {merchant.merchant_id: merchant for merchant in entities.merchants}
    devices = {device.device_id for device in entities.devices}

    assert [payment.payment_id for payment in dataset.payments] == [
        f"PAY-{number:08d}" for number in range(1, 101)
    ]
    assert [event.event_id for event in dataset.payment_events] == [
        f"EVT-{number:08d}" for number in range(1, 101)
    ]
    assert len({payment.payment_id for payment in dataset.payments}) == 100
    assert len({event.event_id for event in dataset.payment_events}) == 100
    assert all(1.0 <= payment.amount <= 5_000.0 for payment in dataset.payments)
    assert all(payment.payer_account_id in accounts for payment in dataset.payments)
    assert all(event.account_id in accounts for event in dataset.payment_events)
    assert all(
        event.payment_id == payment.payment_id
        for payment, event in zip(dataset.payments, dataset.payment_events, strict=True)
    )

    for payment, event in zip(dataset.payments, dataset.payment_events, strict=True):
        profile = profiles[event.customer_id]
        assert event.amount == payment.amount
        assert event.currency == payment.currency
        assert event.device_id is None or event.device_id in devices
        if payment.payment_rail == "CARD":
            assert payment.card_id in cards
            assert payment.merchant_id in merchants
            assert (
                merchants[payment.merchant_id].merchant_category_code
                in profile.merchant_category_preferences
            )
        else:
            assert payment.payee_account_id in accounts
        if event.device_id is not None:
            assert event.device_id in profile.preferred_device_ids


def test_time_of_day_weekday_and_duration_constraints_are_applied() -> None:
    config, entities, dataset = _dataset()
    start = config.simulation.start.astimezone(UTC)
    end = start + timedelta(days=config.simulation.duration_days)

    assert all(start <= event.event_time < end for event in dataset.payment_events)
    assert all(
        event.event_time.hour in config.behavior.active_hours for event in dataset.payment_events
    )
    assert {event.event_time.weekday() for event in dataset.payment_events}
    assert {event.event_time.hour for event in dataset.payment_events}
    assert entities.customers


def test_behavior_parquet_files_have_stable_schemas(tmp_path: Path) -> None:
    _, _, dataset = _dataset()
    paths = write_behavior_parquet(dataset, tmp_path / "run-1")

    schemas = {
        "behavior_profiles": BEHAVIOR_PROFILE_SCHEMA,
        "payments": PAYMENT_SCHEMA,
        "payment_events": PAYMENT_EVENT_SCHEMA,
    }
    for table_name, schema in schemas.items():
        frame = pl.read_parquet(paths[table_name])
        assert frame.columns == list(schema)
        assert frame.schema == schema
        assert frame.height == dataset.counts[table_name]
