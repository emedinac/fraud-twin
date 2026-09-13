"""Explicitly typed Parquet output for entities, behavior, and payments."""

from collections.abc import Iterable
from pathlib import Path
from typing import Any

import polars as pl
from pydantic import BaseModel

from fraudtwin.simulation.behavior import BehaviorDataset
from fraudtwin.simulation.generator import EntityDataset

_UTC_TIMESTAMP = pl.Datetime(time_zone="UTC")

# Dict insertion order is part of the output contract: it fixes Parquet column
# order as well as making the schemas easy to inspect in tests and tooling.
ENTITY_SCHEMAS: dict[str, dict[str, Any]] = {
    "customers": {
        "customer_id": pl.Utf8,
        "customer_type": pl.Utf8,
        "customer_status": pl.Utf8,
        "date_of_birth": pl.Date,
        "country": pl.Utf8,
        "city": pl.Utf8,
        "registration_date": _UTC_TIMESTAMP,
        "risk_segment": pl.Utf8,
        "income_band": pl.Utf8,
        "occupation_category": pl.Utf8,
        "preferred_channels": pl.List(pl.Utf8),
        "created_at": _UTC_TIMESTAMP,
        "updated_at": _UTC_TIMESTAMP,
        "valid_from": _UTC_TIMESTAMP,
        "valid_to": _UTC_TIMESTAMP,
        "system_from": _UTC_TIMESTAMP,
        "system_to": _UTC_TIMESTAMP,
    },
    "institutions": {
        "institution_id": pl.Utf8,
        "institution_type": pl.Utf8,
        "country": pl.Utf8,
        "institution_code": pl.Utf8,
        "risk_profile": pl.Utf8,
        "processing_latency_profile": pl.Utf8,
    },
    "accounts": {
        "account_id": pl.Utf8,
        "customer_id": pl.Utf8,
        "institution_id": pl.Utf8,
        "account_type": pl.Utf8,
        "currency": pl.Utf8,
        "opening_date": _UTC_TIMESTAMP,
        "closing_date": _UTC_TIMESTAMP,
        "status": pl.Utf8,
        "credit_limit": pl.Float64,
        "available_balance": pl.Float64,
        "ledger_balance": pl.Float64,
        "overdraft_limit": pl.Float64,
        "created_at": _UTC_TIMESTAMP,
        "updated_at": _UTC_TIMESTAMP,
        "valid_from": _UTC_TIMESTAMP,
        "valid_to": _UTC_TIMESTAMP,
        "system_from": _UTC_TIMESTAMP,
        "system_to": _UTC_TIMESTAMP,
    },
    "cards": {
        "card_id": pl.Utf8,
        "account_id": pl.Utf8,
        "customer_id": pl.Utf8,
        "scheme": pl.Utf8,
        "card_type": pl.Utf8,
        "status": pl.Utf8,
        "issued_at": _UTC_TIMESTAMP,
        "expires_at": _UTC_TIMESTAMP,
        "country": pl.Utf8,
        "network_token_enabled": pl.Boolean,
        "contactless_enabled": pl.Boolean,
        "online_enabled": pl.Boolean,
        "international_enabled": pl.Boolean,
        "daily_limit": pl.Float64,
        "transaction_limit": pl.Float64,
    },
    "merchants": {
        "merchant_id": pl.Utf8,
        "merchant_name": pl.Utf8,
        "merchant_category_code": pl.Utf8,
        "country": pl.Utf8,
        "city": pl.Utf8,
        "risk_segment": pl.Utf8,
        "acquirer_id": pl.Utf8,
        "online_only": pl.Boolean,
        "created_at": _UTC_TIMESTAMP,
    },
    "devices": {
        "device_id": pl.Utf8,
        "device_type": pl.Utf8,
        "os_family": pl.Utf8,
        "browser_family": pl.Utf8,
        "first_seen_at": _UTC_TIMESTAMP,
        "last_seen_at": _UTC_TIMESTAMP,
        "trusted": pl.Boolean,
        "device_fingerprint": pl.Utf8,
        "risk_score": pl.Float64,
    },
    "pix_keys": {
        "pix_key_id": pl.Utf8,
        "account_id": pl.Utf8,
        "customer_id": pl.Utf8,
        "institution_id": pl.Utf8,
        "key_type": pl.Utf8,
        "key_hash_or_synthetic_value": pl.Utf8,
        "created_at": _UTC_TIMESTAMP,
        "status": pl.Utf8,
    },
}

BEHAVIOR_PROFILE_SCHEMA: dict[str, Any] = {
    "behavior_profile_id": pl.Utf8,
    "customer_id": pl.Utf8,
    "spending_level": pl.Utf8,
    "typical_payment_hours": pl.List(pl.Int64),
    "hour_weights": pl.List(pl.Float64),
    "weekday_weights": pl.List(pl.Float64),
    "typical_countries": pl.List(pl.Utf8),
    "merchant_category_preferences": pl.List(pl.Utf8),
    "merchant_category_weights": pl.List(pl.Float64),
    "monthly_income": pl.Float64,
    "monthly_spending_budget": pl.Float64,
    "card_vs_transfer_preference": pl.Float64,
    "online_purchase_rate": pl.Float64,
    "travel_frequency": pl.Float64,
    "preferred_device_ids": pl.List(pl.Utf8),
    "trusted_device_count": pl.Int64,
}

PAYMENT_SCHEMA: dict[str, Any] = {
    "payment_id": pl.Utf8,
    "payment_rail": pl.Utf8,
    "payment_type": pl.Utf8,
    "payer_account_id": pl.Utf8,
    "payee_account_id": pl.Utf8,
    "merchant_id": pl.Utf8,
    "card_id": pl.Utf8,
    "amount": pl.Float64,
    "currency": pl.Utf8,
    "initiated_at": _UTC_TIMESTAMP,
    "current_status": pl.Utf8,
}

PAYMENT_EVENT_SCHEMA: dict[str, Any] = {
    "event_id": pl.Utf8,
    "event_type": pl.Utf8,
    "event_version": pl.Int64,
    "payment_id": pl.Utf8,
    "customer_id": pl.Utf8,
    "account_id": pl.Utf8,
    "event_time": _UTC_TIMESTAMP,
    "source_created_at": _UTC_TIMESTAMP,
    "source_available_at": _UTC_TIMESTAMP,
    "ingested_at": _UTC_TIMESTAMP,
    "processed_at": _UTC_TIMESTAMP,
    "producer": pl.Utf8,
    "source_system": pl.Utf8,
    "schema_version": pl.Utf8,
    "correlation_id": pl.Utf8,
    "causation_id": pl.Utf8,
    "simulation_run_id": pl.Utf8,
    "scenario_id": pl.Utf8,
    "payment_rail": pl.Utf8,
    "payment_type": pl.Utf8,
    "payee_account_id": pl.Utf8,
    "merchant_id": pl.Utf8,
    "card_id": pl.Utf8,
    "device_id": pl.Utf8,
    "online": pl.Boolean,
    "amount": pl.Float64,
    "currency": pl.Utf8,
}


def _write_table(records: Iterable[BaseModel], schema: dict[str, Any], path: Path) -> None:
    rows = [record.model_dump(mode="python") for record in records]
    pl.DataFrame(rows, schema=schema, orient="row").write_parquet(path)


def write_entity_parquet(dataset: EntityDataset, run_dir: Path) -> dict[str, Path]:
    """Write one explicitly typed Parquet file per entity and return its paths."""

    entities_dir = run_dir / "entities"
    entities_dir.mkdir(parents=True, exist_ok=False)
    written: dict[str, Path] = {}
    for entity_name, records in dataset.tables().items():
        schema = ENTITY_SCHEMAS[entity_name]
        path = entities_dir / f"{entity_name}.parquet"
        _write_table(records, schema, path)
        written[entity_name] = path
    return written


def write_behavior_parquet(dataset: BehaviorDataset, run_dir: Path) -> dict[str, Path]:
    """Write behavior profiles, payments, and payment events with stable schemas."""

    behavior_dir = run_dir / "behavior"
    payments_dir = run_dir / "payments"
    behavior_dir.mkdir(parents=True, exist_ok=False)
    payments_dir.mkdir(parents=True, exist_ok=False)
    tables = {
        "behavior_profiles": (dataset.profiles, BEHAVIOR_PROFILE_SCHEMA, behavior_dir),
        "payments": (dataset.payments, PAYMENT_SCHEMA, payments_dir),
        "payment_events": (dataset.payment_events, PAYMENT_EVENT_SCHEMA, payments_dir),
    }
    written: dict[str, Path] = {}
    for table_name, (records, schema, directory) in tables.items():
        path = directory / f"{table_name}.parquet"
        _write_table(records, schema, path)
        written[table_name] = path
    return written
