"""Dataframe validation and diagnostics for the M8 quality layer.

The quality injector deliberately produces invalid rows when configured to do
so.  This module therefore reports validation results instead of rejecting the
output.  Pandera supplies the dataframe-level schema and constraint checks;
the remaining measurements follow the same validity, structure, uniqueness,
and referential-integrity ideas as an SDMetrics diagnostic report.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

import pandera.polars as pa
import polars as pl
from pandera.errors import SchemaError, SchemaErrors

from fraudtwin.domain import PaymentEvent
from fraudtwin.simulation.parquet import PAYMENT_EVENT_SCHEMA, PAYMENT_SCHEMA

if TYPE_CHECKING:
    from fraudtwin.simulation.behavior import BehaviorDataset


def _payment_validation_schema() -> pa.DataFrameSchema:
    optional = {
        "payee_account_id",
        "merchant_id",
        "card_id",
        "payer_institution_id",
        "payee_institution_id",
        "payer_pix_key_id",
        "payee_pix_key_id",
    }
    columns = {}
    for name in PAYMENT_SCHEMA:
        dtype: Any = (
            float
            if name == "amount"
            else pl.Datetime(time_zone="UTC")
            if name == "initiated_at"
            else str
        )
        columns[name] = pa.Column(
            dtype,
            checks=pa.Check.gt(0) if name == "amount" else None,
            nullable=name in optional,
            unique=name == "payment_id",
        )
    return pa.DataFrameSchema(columns, strict=True)


def _event_validation_schema() -> pa.DataFrameSchema:
    optional = {
        "causation_id",
        "scenario_id",
        "payee_account_id",
        "merchant_id",
        "card_id",
        "device_id",
        "scenario_type",
        "scenario_trigger",
        "scenario_reason",
        "fraud_record_id",
    }
    timestamp_columns = {
        "event_time",
        "source_created_at",
        "source_available_at",
        "ingested_at",
        "processed_at",
    }
    columns = {}
    for name in PAYMENT_EVENT_SCHEMA:
        dtype: Any = (
            bool
            if name == "online"
            else float
            if name == "amount"
            else int
            if name == "event_version"
            else pl.Datetime(time_zone="UTC")
            if name in timestamp_columns
            else pl.List(pl.Utf8)
            if name == "affected_entity_ids"
            else str
        )
        columns[name] = pa.Column(
            dtype,
            checks=pa.Check.gt(0) if name == "amount" else None,
            nullable=name in optional,
            unique=name == "event_id",
        )
    return pa.DataFrameSchema(columns, strict=True)


def _frame(records: Iterable[Any], schema: dict[str, Any]) -> pl.DataFrame:
    rows = [record.model_dump(mode="python") for record in records]
    return pl.DataFrame(rows, schema=schema, orient="row")


def _validation_summary(frame: pl.DataFrame, schema: pa.DataFrameSchema) -> dict[str, Any]:
    try:
        schema.validate(frame, lazy=True)
    except (SchemaError, SchemaErrors) as exc:
        failure_cases = getattr(exc, "failure_cases", None)
        failure_count = int(failure_cases.height) if isinstance(failure_cases, pl.DataFrame) else 1
        failed_columns = (
            sorted(failure_cases.get_column("column").drop_nulls().unique().to_list())
            if isinstance(failure_cases, pl.DataFrame) and "column" in failure_cases.columns
            else []
        )
        return {
            "valid": False,
            "failure_count": failure_count,
            "failed_columns": failed_columns,
            "validator": "pandera",
        }
    return {"valid": True, "failure_count": 0, "failed_columns": [], "validator": "pandera"}


def _structure_summary(frame: pl.DataFrame, schema: dict[str, Any]) -> dict[str, Any]:
    expected_columns = list(schema)
    actual_columns = frame.columns
    names_match = actual_columns == expected_columns
    type_matches = sum(
        frame.schema.get(name) == expected_type
        for name, expected_type in schema.items()
        if name in frame.schema
    )
    score = type_matches / max(1, len(schema))
    if set(actual_columns) != set(expected_columns):
        score = 0.0
    return {
        "score": float(score),
        "column_order_matches": names_match,
        "columns": actual_columns,
    }


def _key_summary(values: Iterable[str]) -> dict[str, Any]:
    materialized = tuple(values)
    unique_count = len(set(materialized))
    duplicate_count = len(materialized) - unique_count
    return {
        "rows": len(materialized),
        "unique_values": unique_count,
        "duplicate_rows": duplicate_count,
        "duplicate_rate": duplicate_count / max(1, len(materialized)),
    }


def _relationship_summary(values: Iterable[str | None], targets: set[str]) -> dict[str, Any]:
    materialized = tuple(values)
    checked = tuple(value for value in materialized if value is not None)
    valid_count = sum(value in targets for value in checked)
    return {
        "valid": valid_count,
        "checked": len(checked),
        "null_values": len(materialized) - len(checked),
        "rate": valid_count / len(checked) if checked else 1.0,
    }


def _envelope_summary(events: Iterable[PaymentEvent]) -> dict[str, Any]:
    materialized = tuple(events)
    valid_count = sum(
        event.event_time
        <= event.source_created_at
        <= event.source_available_at
        <= event.ingested_at
        <= event.processed_at
        for event in materialized
    )
    return {
        "valid": valid_count,
        "checked": len(materialized),
        "rate": valid_count / max(1, len(materialized)),
    }


def _out_of_order_summary(events: Iterable[PaymentEvent]) -> dict[str, Any]:
    by_payment: dict[str, list[PaymentEvent]] = {}
    for event in events:
        by_payment.setdefault(event.payment_id, []).append(event)
    comparisons = 0
    inversions = 0
    for payment_events in by_payment.values():
        for previous, current in zip(payment_events, payment_events[1:], strict=False):
            comparisons += 1
            inversions += current.event_time <= previous.event_time
    return {
        "comparisons": comparisons,
        "inversions": inversions,
        "rate": inversions / max(1, comparisons),
    }


def build_quality_diagnostics(clean: BehaviorDataset, output: BehaviorDataset) -> dict[str, Any]:
    """Build deterministic M8 diagnostics for clean and mutated records."""

    clean_payments = _frame(clean.payments, PAYMENT_SCHEMA)
    output_payments = _frame(output.payments, PAYMENT_SCHEMA)
    clean_events = _frame(clean.payment_events, PAYMENT_EVENT_SCHEMA)
    output_events = _frame(output.payment_events, PAYMENT_EVENT_SCHEMA)

    payment_ids = {payment.payment_id for payment in output.payments}
    event_ids = {event.event_id for event in output.payment_events}
    payment_event_refs = _relationship_summary(
        (event.payment_id for event in output.payment_events), payment_ids
    )
    causation_refs = _relationship_summary(
        (event.causation_id for event in output.payment_events), event_ids
    )
    fraud_payment_refs = _relationship_summary(
        (record.payment_id for record in output.fraud_records), payment_ids
    )
    fraud_event_refs = _relationship_summary(
        (record.event_id for record in output.fraud_records), event_ids
    )
    relationship_rates = (
        payment_event_refs["rate"],
        causation_refs["rate"],
        fraud_payment_refs["rate"],
        fraud_event_refs["rate"],
    )

    return {
        "data_validity": {
            "clean": {
                "payments": _validation_summary(clean_payments, _payment_validation_schema()),
                "payment_events": _validation_summary(clean_events, _event_validation_schema()),
            },
            "output": {
                "payments": _validation_summary(output_payments, _payment_validation_schema()),
                "payment_events": _validation_summary(output_events, _event_validation_schema()),
            },
        },
        "data_structure": {
            "payments": _structure_summary(output_payments, PAYMENT_SCHEMA),
            "payment_events": _structure_summary(output_events, PAYMENT_EVENT_SCHEMA),
        },
        "key_uniqueness": {
            "payments": _key_summary(payment.payment_id for payment in output.payments),
            "payment_events": _key_summary(event.event_id for event in output.payment_events),
            "fraud_records": _key_summary(
                record.fraud_record_id for record in output.fraud_records
            ),
        },
        "relationship_validity": {
            "overall_rate": sum(relationship_rates) / len(relationship_rates),
            "payment_event_payment_id": payment_event_refs,
            "payment_event_causation_id": causation_refs,
            "fraud_record_payment_id": fraud_payment_refs,
            "fraud_record_event_id": fraud_event_refs,
        },
        "event_envelope": _envelope_summary(output.payment_events),
        "delivery_order": _out_of_order_summary(output.payment_events),
    }
