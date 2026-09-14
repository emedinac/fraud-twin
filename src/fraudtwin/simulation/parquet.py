"""Explicitly typed Parquet output for entities, behavior, and payments."""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING, Any

import polars as pl
from pydantic import BaseModel

from fraudtwin.domain import (
    GraphCampaign,
    GraphCampaignMembership,
    GraphEvidence,
    GraphHyperedge,
    GraphHyperedgeMembership,
    GraphPattern,
)
from fraudtwin.simulation.generator import EntityDataset

if TYPE_CHECKING:
    from fraudtwin.simulation.behavior import BehaviorDataset

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

NETWORK_ENDPOINT_SCHEMA: dict[str, Any] = {
    "endpoint_id": pl.Utf8,
    "endpoint_type": pl.Utf8,
    "address_hash": pl.Utf8,
    "first_seen_at": _UTC_TIMESTAMP,
    "last_seen_at": _UTC_TIMESTAMP,
    "valid_from": _UTC_TIMESTAMP,
    "valid_to": _UTC_TIMESTAMP,
}

ENTITY_STATE_HISTORY_SCHEMA: dict[str, Any] = {
    "entity_id": pl.Utf8,
    "entity_type": pl.Utf8,
    "from_status": pl.Utf8,
    "to_status": pl.Utf8,
    "effective_at": _UTC_TIMESTAMP,
    "system_from": _UTC_TIMESTAMP,
    "system_to": _UTC_TIMESTAMP,
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
    "payer_institution_id": pl.Utf8,
    "payee_institution_id": pl.Utf8,
    "payer_pix_key_id": pl.Utf8,
    "payee_pix_key_id": pl.Utf8,
}

LEDGER_ENTRY_SCHEMA: dict[str, Any] = {
    "ledger_entry_id": pl.Utf8,
    "account_id": pl.Utf8,
    "payment_id": pl.Utf8,
    "event_id": pl.Utf8,
    "entry_type": pl.Utf8,
    "amount": pl.Float64,
    "currency": pl.Utf8,
    "occurred_at": _UTC_TIMESTAMP,
    "effective_at": _UTC_TIMESTAMP,
    "posted_at": _UTC_TIMESTAMP,
    "balance_after": pl.Float64,
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
    "scenario_type": pl.Utf8,
    "scenario_trigger": pl.Utf8,
    "scenario_reason": pl.Utf8,
    "fraud_record_id": pl.Utf8,
    "affected_entity_ids": pl.List(pl.Utf8),
}

# Lifecycle events use the same stable envelope as all payment events. Keeping
# a named alias makes the contract explicit for consumers and tests.
PAYMENT_LIFECYCLE_EVENT_SCHEMA = PAYMENT_EVENT_SCHEMA
GRAPH_PAYMENT_EVENT_SCHEMA = {
    **PAYMENT_EVENT_SCHEMA,
    "ip_id": pl.Utf8,
}
GRAPH_MEMBERSHIP_SCHEMA = {
    "campaign_id": pl.Utf8,
    "pattern_type": pl.Utf8,
    "member_id": pl.Utf8,
    "member_type": pl.Utf8,
    "role": pl.Utf8,
    "valid_from": _UTC_TIMESTAMP,
    "valid_to": _UTC_TIMESTAMP,
    "source_event_id": pl.Utf8,
    "payment_id": pl.Utf8,
}
GRAPH_PATTERN_SCHEMA = {
    "pattern_id": pl.Utf8,
    "pattern_type": pl.Utf8,
    "campaign_id": pl.Utf8,
    "detected_at": _UTC_TIMESTAMP,
    "window_from": _UTC_TIMESTAMP,
    "window_to": _UTC_TIMESTAMP,
    "member_ids": pl.List(pl.Utf8),
    "source_event_ids": pl.List(pl.Utf8),
    "payment_ids": pl.List(pl.Utf8),
    "threshold": pl.Int64,
    "observed_value": pl.Float64,
    "invariant_status": pl.Utf8,
}

FRAUD_RECORD_SCHEMA: dict[str, Any] = {
    "fraud_record_id": pl.Utf8,
    "record_type": pl.Utf8,
    "scenario_id": pl.Utf8,
    "scenario_type": pl.Utf8,
    "fraud_truth": pl.Boolean,
    "trigger": pl.Utf8,
    "reason": pl.Utf8,
    "customer_id": pl.Utf8,
    "account_id": pl.Utf8,
    "card_id": pl.Utf8,
    "device_id": pl.Utf8,
    "merchant_id": pl.Utf8,
    "payment_id": pl.Utf8,
    "event_id": pl.Utf8,
    "occurred_at": _UTC_TIMESTAMP,
    "amount": pl.Float64,
    "currency": pl.Utf8,
    "correlation_id": pl.Utf8,
    "causation_id": pl.Utf8,
    "affected_entity_ids": pl.List(pl.Utf8),
}

FRAUD_ALERT_SCHEMA: dict[str, Any] = {
    "fraud_alert_id": pl.Utf8,
    "alert_type": pl.Utf8,
    "severity": pl.Utf8,
    "customer_id": pl.Utf8,
    "account_id": pl.Utf8,
    "card_id": pl.Utf8,
    "device_id": pl.Utf8,
    "merchant_id": pl.Utf8,
    "payment_id": pl.Utf8,
    "event_id": pl.Utf8,
    "fraud_record_id": pl.Utf8,
    "scenario_id": pl.Utf8,
    "scenario_type": pl.Utf8,
    "trigger": pl.Utf8,
    "reason": pl.Utf8,
    "alert_created_at": _UTC_TIMESTAMP,
    "amount": pl.Float64,
    "currency": pl.Utf8,
    "correlation_id": pl.Utf8,
    "causation_id": pl.Utf8,
    "simulation_run_id": pl.Utf8,
    "affected_entity_ids": pl.List(pl.Utf8),
}

FRAUD_CASE_SCHEMA: dict[str, Any] = {
    "fraud_case_id": pl.Utf8,
    "fraud_alert_id": pl.Utf8,
    "customer_id": pl.Utf8,
    "account_id": pl.Utf8,
    "card_id": pl.Utf8,
    "device_id": pl.Utf8,
    "merchant_id": pl.Utf8,
    "payment_id": pl.Utf8,
    "event_id": pl.Utf8,
    "fraud_record_id": pl.Utf8,
    "scenario_id": pl.Utf8,
    "scenario_type": pl.Utf8,
    "fraud_truth": pl.Boolean,
    "fraud_occurred_at": _UTC_TIMESTAMP,
    "alert_created_at": _UTC_TIMESTAMP,
    "case_opened_at": _UTC_TIMESTAMP,
    "case_closed_at": _UTC_TIMESTAMP,
    "fraud_confirmed_at": _UTC_TIMESTAMP,
    "label_available_at": _UTC_TIMESTAMP,
    "investigation_outcome": pl.Utf8,
    "loss_amount": pl.Float64,
    "recovered_amount": pl.Float64,
    "amount": pl.Float64,
    "currency": pl.Utf8,
    "correlation_id": pl.Utf8,
    "causation_id": pl.Utf8,
    "simulation_run_id": pl.Utf8,
    "affected_entity_ids": pl.List(pl.Utf8),
}

CASE_CONFIRMATION_SCHEMA: dict[str, Any] = {
    "confirmation_id": pl.Utf8,
    "fraud_case_id": pl.Utf8,
    "fraud_alert_id": pl.Utf8,
    "customer_id": pl.Utf8,
    "payment_id": pl.Utf8,
    "event_id": pl.Utf8,
    "fraud_record_id": pl.Utf8,
    "scenario_id": pl.Utf8,
    "scenario_type": pl.Utf8,
    "fraud_truth": pl.Boolean,
    "confirmed_at": _UTC_TIMESTAMP,
    "investigation_outcome": pl.Utf8,
    "amount": pl.Float64,
    "currency": pl.Utf8,
    "correlation_id": pl.Utf8,
    "causation_id": pl.Utf8,
    "simulation_run_id": pl.Utf8,
    "affected_entity_ids": pl.List(pl.Utf8),
}

CUSTOMER_DISPUTE_SCHEMA: dict[str, Any] = {
    "event_id": pl.Utf8,
    "event_type": pl.Utf8,
    "event_version": pl.Int64,
    "fraud_case_id": pl.Utf8,
    "fraud_alert_id": pl.Utf8,
    "fraud_record_id": pl.Utf8,
    "underlying_event_id": pl.Utf8,
    "payment_id": pl.Utf8,
    "customer_id": pl.Utf8,
    "account_id": pl.Utf8,
    "card_id": pl.Utf8,
    "device_id": pl.Utf8,
    "merchant_id": pl.Utf8,
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
    "scenario_type": pl.Utf8,
    "payment_rail": pl.Utf8,
    "payment_type": pl.Utf8,
    "amount": pl.Float64,
    "currency": pl.Utf8,
    "affected_entity_ids": pl.List(pl.Utf8),
}

FRAUD_LABEL_SCHEMA: dict[str, Any] = {
    "label_id": pl.Utf8,
    "fraud_case_id": pl.Utf8,
    "fraud_alert_id": pl.Utf8,
    "fraud_record_id": pl.Utf8,
    "customer_id": pl.Utf8,
    "payment_id": pl.Utf8,
    "event_id": pl.Utf8,
    "scenario_id": pl.Utf8,
    "scenario_type": pl.Utf8,
    "label": pl.Utf8,
    "fraud_truth": pl.Boolean,
    "fraud_occurred_at": _UTC_TIMESTAMP,
    "fraud_confirmed_at": _UTC_TIMESTAMP,
    "dispute_event_at": _UTC_TIMESTAMP,
    "label_available_at": _UTC_TIMESTAMP,
    "investigation_outcome": pl.Utf8,
    "amount": pl.Float64,
    "currency": pl.Utf8,
    "correlation_id": pl.Utf8,
    "causation_id": pl.Utf8,
    "simulation_run_id": pl.Utf8,
    "affected_entity_ids": pl.List(pl.Utf8),
}

BEHAVIOR_SCHEMAS: dict[str, dict[str, Any]] = {
    "behavior_profiles": BEHAVIOR_PROFILE_SCHEMA,
    "payments": PAYMENT_SCHEMA,
    "payment_events": PAYMENT_EVENT_SCHEMA,
    "ledger_entries": LEDGER_ENTRY_SCHEMA,
    "fraud_records": FRAUD_RECORD_SCHEMA,
    "fraud_alerts": FRAUD_ALERT_SCHEMA,
    "fraud_cases": FRAUD_CASE_SCHEMA,
    "case_confirmations": CASE_CONFIRMATION_SCHEMA,
    "customer_disputes": CUSTOMER_DISPUTE_SCHEMA,
    "fraud_labels": FRAUD_LABEL_SCHEMA,
}

GRAPH_TRUTH_SCHEMAS: dict[str, dict[str, Any]] = {
    "campaigns": {
        "campaign_id": pl.Utf8,
        "scenario_type": pl.Utf8,
        "scenario_code": pl.Utf8,
        "truth_label": pl.Utf8,
        "valid_from": _UTC_TIMESTAMP,
        "valid_to": _UTC_TIMESTAMP,
        "participant_ids": pl.List(pl.Utf8),
        "modifiers": pl.List(pl.Utf8),
    },
    "campaign_memberships": GRAPH_MEMBERSHIP_SCHEMA,
    "patterns": GRAPH_PATTERN_SCHEMA,
    "graph_evidence": {
        "evidence_id": pl.Utf8,
        "edge_id": pl.Utf8,
        "evidence_type": pl.Utf8,
        "resource_id": pl.Utf8,
        "source_event_id": pl.Utf8,
        "payment_id": pl.Utf8,
        "observed_at": _UTC_TIMESTAMP,
        "available_at": _UTC_TIMESTAMP,
    },
    "hyperedges": {
        "hyperedge_id": pl.Utf8,
        "hyperedge_type": pl.Utf8,
        "campaign_id": pl.Utf8,
        "pattern_id": pl.Utf8,
        "valid_from": _UTC_TIMESTAMP,
        "valid_to": _UTC_TIMESTAMP,
        "source_event_ids": pl.List(pl.Utf8),
        "payment_ids": pl.List(pl.Utf8),
    },
    "hyperedge_memberships": {
        "hyperedge_id": pl.Utf8,
        "member_id": pl.Utf8,
        "member_type": pl.Utf8,
        "role": pl.Utf8,
    },
}


def _write_table(
    records: Iterable[BaseModel],
    schema: dict[str, Any],
    path: Path,
    *,
    masked_fields: tuple[str, ...] = (),
) -> None:
    rows = []
    for record in records:
        row = record.model_dump(mode="python")
        for field in masked_fields:
            row[field] = None
        rows.append(row)
    pl.DataFrame(rows, schema=schema, orient="row").write_parquet(path)


def write_entity_parquet(dataset: EntityDataset, run_dir: Path) -> dict[str, Path]:
    """Write one explicitly typed Parquet file per entity and return its paths."""

    entities_dir = run_dir / "entities"
    entities_dir.mkdir(parents=True, exist_ok=False)
    written: dict[str, Path] = {}
    for entity_name, records in dataset.tables().items():
        schema = (
            NETWORK_ENDPOINT_SCHEMA
            if entity_name == "network_endpoints"
            else ENTITY_SCHEMAS[entity_name]
        )
        path = entities_dir / f"{entity_name}.parquet"
        _write_table(records, schema, path)
        written[entity_name] = path
    if dataset.state_history:
        path = entities_dir / "state_history.parquet"
        _write_table(
            dataset.state_history,
            ENTITY_STATE_HISTORY_SCHEMA,
            path,
        )
        written["state_history"] = path
    return written


def _behavior_table_directories(root: Path) -> dict[str, Path]:
    """Map behavior table names to their output directories below ``root``."""

    behavior_dir = root / "behavior"
    payments_dir = root / "payments"
    ledger_dir = root / "ledger"
    fraud_dir = root / "fraud"
    return {
        "behavior_profiles": behavior_dir,
        "payments": payments_dir,
        "payment_events": payments_dir,
        "ledger_entries": ledger_dir,
        "fraud_records": fraud_dir,
        "fraud_alerts": fraud_dir,
        "fraud_cases": fraud_dir,
        "case_confirmations": fraud_dir,
        "customer_disputes": fraud_dir,
        "fraud_labels": fraud_dir,
    }


def write_behavior_parquet(
    dataset: BehaviorDataset,
    run_dir: Path,
    *,
    mask_fraud_truth: bool = True,
) -> dict[str, Path]:
    """Write behavior records with stable schemas.

    Operational M7 exports mask oracle truth by default. Replay exports can
    opt into the complete historical truth stream because replay is an
    immutable research artifact, not an operational label feed.
    """

    table_directories = _behavior_table_directories(run_dir)
    for directory in dict.fromkeys(table_directories.values()):
        directory.mkdir(parents=True, exist_ok=False)
    masked_fields = (
        {
            "fraud_cases": ("fraud_truth",),
            "case_confirmations": ("fraud_truth",),
            "fraud_labels": ("fraud_truth",),
        }
        if mask_fraud_truth
        else {}
    )
    written: dict[str, Path] = {}
    for table_name, records in dataset.tables().items():
        schema = BEHAVIOR_SCHEMAS[table_name]
        if table_name == "payment_events" and any(
            getattr(record, "ip_id", None) is not None for record in records
        ):
            schema = GRAPH_PAYMENT_EVENT_SCHEMA
        directory = table_directories[table_name]
        path = directory / f"{table_name}.parquet"
        _write_table(records, schema, path, masked_fields=masked_fields.get(table_name, ()))
        written[table_name] = path
    if dataset.oracle_tables:
        oracle_dir = run_dir / "oracle"
        oracle_dir.mkdir(parents=True, exist_ok=True)
        oracle_directories = _behavior_table_directories(oracle_dir)
        for directory in set(oracle_directories.values()):
            directory.mkdir(parents=True, exist_ok=True)
        for table_name, records in dataset.oracle_tables.items():
            if table_name in BEHAVIOR_SCHEMAS:
                schema = BEHAVIOR_SCHEMAS[table_name]
                if table_name == "payment_events" and any(
                    getattr(record, "ip_id", None) is not None for record in records
                ):
                    schema = GRAPH_PAYMENT_EVENT_SCHEMA
                _write_table(
                    records,
                    schema,
                    oracle_directories[table_name] / f"{table_name}.parquet",
                )
    return written


def write_graph_truth(
    memberships: tuple[GraphCampaignMembership, ...],
    patterns: tuple[GraphPattern, ...],
    run_dir: Path,
    campaigns: tuple[GraphCampaign, ...] = (),
    evidence: tuple[GraphEvidence, ...] = (),
    hyperedges: tuple[GraphHyperedge, ...] = (),
    hyperedge_memberships: tuple[GraphHyperedgeMembership, ...] = (),
) -> dict[str, Path]:
    """Write opt-in M11 oracle metadata without exposing it operationally."""

    values = {
        "campaigns": campaigns,
        "campaign_memberships": memberships,
        "patterns": patterns,
        "graph_evidence": evidence,
        "hyperedges": hyperedges,
        "hyperedge_memberships": hyperedge_memberships,
    }
    if not any(values.values()):
        return {}
    graph_dir = run_dir / "oracle" / "graph"
    graph_dir.mkdir(parents=True, exist_ok=True)
    written: dict[str, Path] = {}
    for name, records in values.items():
        if records:
            path = graph_dir / f"{name}.parquet"
            _write_table(records, GRAPH_TRUTH_SCHEMAS[name], path)
            written[name] = path
    return written
