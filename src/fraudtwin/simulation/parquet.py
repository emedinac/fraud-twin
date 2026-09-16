"""Explicitly typed Parquet output for entities, behavior, and payments."""

import hashlib
import json
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any

import polars as pl
from pydantic import BaseModel

from fraudtwin.domain import (
    FinalObservedLabel,
    GraphCampaign,
    GraphCampaignMembership,
    GraphEvidence,
    GraphHyperedge,
    GraphHyperedgeMembership,
    GraphPattern,
    LabelObservation,
)
from fraudtwin.reproducibility import sha256_json, write_json
from fraudtwin.simulation.generator import EntityDataset

if TYPE_CHECKING:
    from fraudtwin.campaign_dynamics import DynamicCampaignDataset
    from fraudtwin.simulation.behavior import BehaviorDataset

from fraudtwin.calibration import CalibrationProfile, FidelityReport
from fraudtwin.counterfactual import CounterfactualDataset

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
    "transport_partition": pl.Int64,
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
FRAUD_CASE_SCHEMA_M17 = {**FRAUD_CASE_SCHEMA, "case_reopened_at": _UTC_TIMESTAMP}

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

LABEL_VERSION_SCHEMA: dict[str, Any] = {
    "label_version_id": pl.Utf8,
    "fraud_record_id": pl.Utf8,
    "payment_id": pl.Utf8,
    "case_id": pl.Utf8,
    "truth_label": pl.Boolean,
    "observed_label": pl.Utf8,
    "label_state": pl.Utf8,
    "investigation_selected": pl.Boolean,
    "label_version": pl.Int64,
    "label_available_at": _UTC_TIMESTAMP,
    "label_corrected_at": _UTC_TIMESTAMP,
    "case_reopened_at": _UTC_TIMESTAMP,
    "reason": pl.Utf8,
    "causation_id": pl.Utf8,
    "simulation_run_id": pl.Utf8,
}

FINAL_OBSERVED_LABEL_SCHEMA: dict[str, Any] = {
    "observation_id": pl.Utf8,
    "fraud_record_id": pl.Utf8,
    "payment_id": pl.Utf8,
    "observed_label": pl.Utf8,
    "label_state": pl.Utf8,
    "label_version": pl.Int64,
    "label_available_at": _UTC_TIMESTAMP,
    "label_corrected_at": _UTC_TIMESTAMP,
    "case_reopened_at": _UTC_TIMESTAMP,
    "investigation_selected": pl.Boolean,
    "simulation_run_id": pl.Utf8,
}

LABEL_CORRECTION_SCHEMA: dict[str, Any] = {
    "correction_id": pl.Utf8,
    "observation_id": pl.Utf8,
    "from_version": pl.Int64,
    "to_version": pl.Int64,
    "corrected_at": _UTC_TIMESTAMP,
    "observed_label": pl.Utf8,
    "truth_label": pl.Boolean,
    "causation_id": pl.Utf8,
}

CASE_REOPENING_SCHEMA: dict[str, Any] = {
    "reopening_id": pl.Utf8,
    "observation_id": pl.Utf8,
    "label_version": pl.Int64,
    "reopened_at": _UTC_TIMESTAMP,
    "reason": pl.Utf8,
    "causation_id": pl.Utf8,
}

OBSERVATION_PROVENANCE_SCHEMA: dict[str, Any] = {
    "observation_id": pl.Utf8,
    "policy_hash": pl.Utf8,
    "stream_ids": pl.List(pl.Utf8),
    "source_run_id": pl.Utf8,
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

COUNTERFACTUAL_CHANGE_SCHEMA: dict[str, Any] = {
    "change_set_id": pl.Utf8,
    "request_index": pl.Int64,
    "objective": pl.Utf8,
    "status": pl.Utf8,
    "source_payment_id": pl.Utf8,
    "derived_payment_id": pl.Utf8,
    "requested_budget": pl.Float64,
    "resolved_budget": pl.Float64,
    "effective_distance": pl.Float64,
    "per_dimension_costs": pl.Utf8,
    "changed_fields": pl.Utf8,
    "inapplicable_dimensions": pl.List(pl.Utf8),
    "feasibility_constraints": pl.List(pl.Utf8),
    "objective_satisfaction": pl.Utf8,
    "rejection_reason": pl.Utf8,
    "source_to_counterfactual": pl.Utf8,
}

COUNTERFACTUAL_WORKFLOW_SCHEMAS: dict[str, dict[str, Any]] = {
    "fraud_alerts": FRAUD_ALERT_SCHEMA,
    "fraud_cases": FRAUD_CASE_SCHEMA,
    "case_confirmations": CASE_CONFIRMATION_SCHEMA,
    "customer_disputes": CUSTOMER_DISPUTE_SCHEMA,
    "fraud_labels": FRAUD_LABEL_SCHEMA,
}
COUNTERFACTUAL_MASKED_WORKFLOW_FIELDS = {"fraud_cases", "case_confirmations", "fraud_labels"}

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

CAMPAIGN_DYNAMIC_SCHEMAS: dict[str, dict[str, Any]] = {
    "snapshots": {
        "snapshot_id": pl.Utf8,
        "campaign_id": pl.Utf8,
        "phase": pl.Utf8,
        "intensity": pl.Float64,
        "active_actor_ids": pl.List(pl.Utf8),
        "active_device_ids": pl.List(pl.Utf8),
        "valid_from": _UTC_TIMESTAMP,
        "valid_to": _UTC_TIMESTAMP,
        "transition_id": pl.Utf8,
    },
    "transitions": {
        "transition_id": pl.Utf8,
        "campaign_id": pl.Utf8,
        "from_phase": pl.Utf8,
        "to_phase": pl.Utf8,
        "occurred_at": _UTC_TIMESTAMP,
        "reason": pl.Utf8,
        "model_name": pl.Utf8,
        "source_snapshot_id": pl.Utf8,
        "stream_id": pl.Utf8,
    },
    "phase_changes": {
        "phase_change_id": pl.Utf8,
        "campaign_id": pl.Utf8,
        "transition_id": pl.Utf8,
        "phase": pl.Utf8,
        "valid_from": _UTC_TIMESTAMP,
        "valid_to": _UTC_TIMESTAMP,
    },
    "membership_changes": {
        "membership_change_id": pl.Utf8,
        "campaign_id": pl.Utf8,
        "actor_id": pl.Utf8,
        "actor_type": pl.Utf8,
        "role": pl.Utf8,
        "action": pl.Utf8,
        "occurred_at": _UTC_TIMESTAMP,
        "valid_from": _UTC_TIMESTAMP,
        "valid_to": _UTC_TIMESTAMP,
        "source_entity_id": pl.Utf8,
    },
    "intensity_decisions": {
        "intensity_decision_id": pl.Utf8,
        "campaign_id": pl.Utf8,
        "phase": pl.Utf8,
        "decision_at": _UTC_TIMESTAMP,
        "event_rate": pl.Float64,
        "mark": pl.Float64,
        "model_name": pl.Utf8,
        "stream_id": pl.Utf8,
    },
    "topology_mutations": {
        "topology_mutation_id": pl.Utf8,
        "campaign_id": pl.Utf8,
        "mutation_type": pl.Utf8,
        "occurred_at": _UTC_TIMESTAMP,
        "source_member_ids": pl.List(pl.Utf8),
        "derived_member_ids": pl.List(pl.Utf8),
        "source_payment_ids": pl.List(pl.Utf8),
        "derived_payment_ids": pl.List(pl.Utf8),
        "reason": pl.Utf8,
    },
    "lineage": {
        "lineage_id": pl.Utf8,
        "campaign_id": pl.Utf8,
        "parent_campaign_ids": pl.List(pl.Utf8),
        "source_entity_ids": pl.List(pl.Utf8),
        "source_event_ids": pl.List(pl.Utf8),
        "source_payment_ids": pl.List(pl.Utf8),
        "derived_entity_ids": pl.List(pl.Utf8),
        "derived_event_ids": pl.List(pl.Utf8),
        "derived_payment_ids": pl.List(pl.Utf8),
        "derived_at": _UTC_TIMESTAMP,
        "reason": pl.Utf8,
    },
    "source_snapshots": {
        "source_snapshot_id": pl.Utf8,
        "campaign_id": pl.Utf8,
        "source_run_id": pl.Utf8,
        "captured_at": _UTC_TIMESTAMP,
        "source_campaign_ids": pl.List(pl.Utf8),
        "source_entity_ids": pl.List(pl.Utf8),
        "source_event_ids": pl.List(pl.Utf8),
        "source_payment_ids": pl.List(pl.Utf8),
        "configuration_hash": pl.Utf8,
        "schema_fingerprint": pl.Utf8,
    },
}

CALIBRATION_METRIC_SCHEMA: dict[str, Any] = {
    "name": pl.Utf8,
    "version": pl.Utf8,
    "status": pl.Utf8,
    "score": pl.Float64,
    "weight": pl.Float64,
    "reference_summary_fingerprint": pl.Utf8,
    "generated_summary_fingerprint": pl.Utf8,
    "details": pl.Utf8,
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


def _behavior_schema(table_name: str, records: tuple[BaseModel, ...]) -> dict[str, Any]:
    """Select the stable event schema, including optional graph columns."""

    schema = BEHAVIOR_SCHEMAS[table_name]
    if table_name == "payment_events" and any(
        getattr(record, "ip_id", None) is not None for record in records
    ):
        return GRAPH_PAYMENT_EVENT_SCHEMA
    if table_name == "fraud_cases" and any(
        getattr(record, "case_reopened_at", None) is not None for record in records
    ):
        return FRAUD_CASE_SCHEMA_M17
    return schema


def _write_json_lines(rows: Iterable[Mapping[str, object]], path: Path) -> None:
    """Write deterministic JSONL rows used by quality sidecar artifacts."""

    path.write_text(
        "".join(json.dumps(row, sort_keys=True, default=str) + "\n" for row in rows),
        encoding="utf-8",
    )


def _write_quality_artifacts(dataset: BehaviorDataset, run_dir: Path) -> None:
    """Write optional M8 artifacts without affecting typed Parquet tables."""

    if not (
        dataset.quality_raw_faults or dataset.schema_evolution_rows or dataset.quality_diagnostics
    ):
        return
    quality_dir = run_dir / "quality"
    quality_dir.mkdir(parents=True, exist_ok=True)
    write_json(quality_dir / "fault_audit.json", dataset.quality_diagnostics.get("fault_audit", []))
    if dataset.quality_raw_faults:
        _write_json_lines(dataset.quality_raw_faults, quality_dir / "raw_faults.jsonl")
    if dataset.schema_evolution_rows:
        schema_dir = quality_dir / "schema_evolution"
        schema_dir.mkdir(parents=True, exist_ok=True)
        for name, rows in sorted(dataset.schema_evolution_rows.items()):
            filename = name.replace("/", "_").replace(":", "_") + ".jsonl"
            _write_json_lines(rows, schema_dir / filename)


def _write_counterfactual_workflows(
    records: Mapping[str, Iterable[BaseModel]],
    directory: Path,
    *,
    mask_truth: bool,
) -> None:
    """Write the stable M14 workflow table set for one observable boundary."""

    for table_name, schema in COUNTERFACTUAL_WORKFLOW_SCHEMAS.items():
        masked_fields = (
            ("fraud_truth",)
            if mask_truth and table_name in COUNTERFACTUAL_MASKED_WORKFLOW_FIELDS
            else ()
        )
        _write_table(
            records[table_name],
            schema,
            directory / f"{table_name}.parquet",
            masked_fields=masked_fields,
        )


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
        schema = _behavior_schema(table_name, records)
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
                schema = _behavior_schema(table_name, records)
                _write_table(
                    records,
                    schema,
                    oracle_directories[table_name] / f"{table_name}.parquet",
                )
    _write_quality_artifacts(dataset, run_dir)
    return written


def write_label_observation_sidecar(
    observations: tuple[LabelObservation, ...],
    finals: tuple[FinalObservedLabel, ...],
    run_dir: Path,
    *,
    source_run_id: str,
    policy_hash: str,
    stream_ids: tuple[str, ...],
) -> tuple[Path, Path]:
    """Write immutable M17 history/projection artifacts and a reproducibility manifest."""

    if not observations:
        raise ValueError("cannot write an empty label observation sidecar")
    root = run_dir / "label_observations" / ("M17-" + policy_hash[:16])
    if root.exists():
        raise FileExistsError(f"label observation sidecar already exists: {root}")
    oracle = root / "oracle"
    observable = root / "observable"
    oracle.mkdir(parents=True)
    observable.mkdir()
    history = tuple(version for item in observations for version in item.versions)
    _write_table(history, LABEL_VERSION_SCHEMA, oracle / "label_history.parquet")
    _write_table(finals, FINAL_OBSERVED_LABEL_SCHEMA, observable / "observed_labels.parquet")
    corrections = tuple(correction for item in observations for correction in item.corrections)
    if corrections:
        _write_table(corrections, LABEL_CORRECTION_SCHEMA, oracle / "label_corrections.parquet")
    reopenings = tuple(reopening for item in observations for reopening in item.reopenings)
    if reopenings:
        _write_table(reopenings, CASE_REOPENING_SCHEMA, oracle / "case_reopenings.parquet")
    provenance_rows = [
        {
            "observation_id": item.observation_id,
            **item.provenance.model_dump(mode="python"),
        }
        for item in observations
        if item.provenance is not None
    ]
    if provenance_rows:
        pl.DataFrame(
            provenance_rows,
            schema=OBSERVATION_PROVENANCE_SCHEMA,
            orient="row",
        ).write_parquet(oracle / "observation_provenance.parquet")
    checksums = {
        str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }
    manifest = {
        "version": "1",
        "source_run_id": source_run_id,
        "configuration_hash": policy_hash,
        "seed_stream_ids": list(stream_ids),
        "schema_versions": {
            "label_history": "1",
            "observed_labels": "1",
            "label_corrections": "1",
            "case_reopenings": "1",
            "observation_provenance": "1",
        },
        "schema_fingerprint": sha256_json(
            {
                "label_history": list(LABEL_VERSION_SCHEMA),
                "observed_labels": list(FINAL_OBSERVED_LABEL_SCHEMA),
                "label_corrections": list(LABEL_CORRECTION_SCHEMA),
                "case_reopenings": list(CASE_REOPENING_SCHEMA),
                "observation_provenance": list(OBSERVATION_PROVENANCE_SCHEMA),
            }
        ),
        "output_fingerprint": sha256_json(checksums),
        "counts": {
            "observations": len(observations),
            "versions": len(history),
            "final_labels": len(finals),
        },
        "checksums": checksums,
        "lineage": {"source_run_id": source_run_id, "append_only": True},
    }
    manifest_path = root / "label_observation_manifest.json"
    write_json(manifest_path, manifest)
    return root, manifest_path


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


def write_calibration_artifacts(
    profile: CalibrationProfile,
    report: FidelityReport,
    run_dir: Path,
) -> tuple[Path, Path]:
    """Write aggregate-only M16 fidelity artifacts under an immutable sidecar."""

    root = run_dir / "calibration" / profile.profile_id
    if root.exists():
        raise FileExistsError(f"calibration sidecar already exists: {root}")
    root.mkdir(parents=True)
    rows = [
        {
            **item.model_dump(mode="python"),
            "details": json.dumps(item.details, sort_keys=True),
        }
        for item in report.metrics
    ]
    metrics_path = root / "fidelity_metrics.parquet"
    pl.DataFrame(rows, schema=CALIBRATION_METRIC_SCHEMA).write_parquet(metrics_path)
    report_path = root / "fidelity_report.json"
    report_path.write_text(report.model_dump_json(indent=2) + "\n", encoding="utf-8")
    return metrics_path, report_path


def write_campaign_dynamics_sidecar(
    dataset: DynamicCampaignDataset,
    run_dir: Path,
    *,
    source_run_id: str,
) -> tuple[Path, Path]:
    """Write immutable observable/oracle M15 records and its manifest."""

    if not dataset.active:
        raise ValueError("cannot write an inactive campaign dynamics sidecar")
    root = run_dir / "campaign_dynamics" / _sidecar_id(dataset)
    if root.exists():
        raise FileExistsError(f"campaign dynamics sidecar already exists: {root}")
    observable = root / "observable"
    oracle = root / "oracle"
    observable.mkdir(parents=True)
    oracle.mkdir()
    dynamic_payments = tuple(
        item for item in dataset.graph.payments if item.payment_id.startswith("M15-")
    )
    dynamic_events = tuple(
        item for item in dataset.graph.payment_events if item.payment_id.startswith("M15-")
    )
    dynamic_entries = tuple(
        item for item in dataset.graph.ledger_entries if item.payment_id.startswith("M15-")
    )
    _write_table(dynamic_payments, PAYMENT_SCHEMA, observable / "payments.parquet")
    _write_table(dynamic_events, PAYMENT_EVENT_SCHEMA, observable / "payment_events.parquet")
    _write_table(dynamic_entries, LEDGER_ENTRY_SCHEMA, observable / "ledger_entries.parquet")
    values: dict[str, tuple[BaseModel, ...]] = {
        "snapshots": dataset.snapshots,
        "transitions": dataset.transitions,
        "phase_changes": dataset.phase_changes,
        "membership_changes": dataset.membership_changes,
        "intensity_decisions": dataset.intensity_decisions,
        "topology_mutations": dataset.topology_mutations,
        "lineage": dataset.lineage,
        "source_snapshots": dataset.source_snapshots,
    }
    for name, records in values.items():
        _write_table(records, CAMPAIGN_DYNAMIC_SCHEMAS[name], oracle / f"{name}.parquet")
    graph_dir = oracle / "graph"
    graph_dir.mkdir()
    for name, records in {
        "campaigns": dataset.graph.campaigns,
        "campaign_memberships": dataset.graph.memberships,
        "patterns": dataset.graph.patterns,
        "graph_evidence": dataset.graph.evidence,
        "hyperedges": dataset.graph.hyperedges,
        "hyperedge_memberships": dataset.graph.hyperedge_memberships,
    }.items():
        if records:
            _write_table(records, GRAPH_TRUTH_SCHEMAS[name], graph_dir / f"{name}.parquet")
    files = sorted(path for path in root.rglob("*") if path.is_file())
    checksums = {
        str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest() for path in files
    }
    payload = {
        "version": "1",
        "source_run_id": source_run_id,
        "configuration_hash": dataset.configuration_hash,
        "seed_stream_ids": list(dataset.stream_ids),
        "schema_fingerprint": sha256_json(
            {name: list(schema) for name, schema in CAMPAIGN_DYNAMIC_SCHEMAS.items()}
        ),
        "output_fingerprint": sha256_json(checksums),
        "counts": {
            "payments": len(dynamic_payments),
            "payment_events": len(dynamic_events),
            "ledger_entries": len(dynamic_entries),
            "snapshots": len(dataset.snapshots),
            "transitions": len(dataset.transitions),
            "phase_changes": len(dataset.phase_changes),
            "membership_changes": len(dataset.membership_changes),
            "intensity_decisions": len(dataset.intensity_decisions),
            "topology_mutations": len(dataset.topology_mutations),
            "lineage": len(dataset.lineage),
        },
        "checksums": checksums,
    }
    manifest_path = root / "campaign_dynamics_manifest.json"
    write_json(manifest_path, payload)
    return root, manifest_path


def _sidecar_id(dataset: DynamicCampaignDataset) -> str:
    return (
        "M15-"
        + sha256_json(
            {
                "configuration_hash": dataset.configuration_hash,
                "campaign_ids": [item.campaign_id for item in dataset.graph.campaigns],
            }
        )[:16]
    )


def write_counterfactual_sidecar(
    dataset: CounterfactualDataset,
    run_dir: Path,
) -> tuple[Path, Path]:
    """Write M14 append-only original/modified and oracle sidecars."""

    root = run_dir / "counterfactuals" / dataset.counterfactual_id
    observable_original = root / "observable" / "original"
    observable_modified = root / "observable" / "modified"
    oracle = root / "oracle"
    for directory in (observable_original, observable_modified, oracle):
        directory.mkdir(parents=True, exist_ok=False)
    for directory, payments, events, entries in (
        (
            observable_original,
            dataset.original_payments,
            dataset.original_events,
            dataset.original_ledger_entries,
        ),
        (
            observable_modified,
            dataset.modified_payments,
            dataset.modified_events,
            dataset.modified_ledger_entries,
        ),
    ):
        _write_table(payments, PAYMENT_SCHEMA, directory / "payments.parquet")
        schema = (
            GRAPH_PAYMENT_EVENT_SCHEMA
            if any(item.ip_id is not None for item in events)
            else PAYMENT_EVENT_SCHEMA
        )
        _write_table(events, schema, directory / "payment_events.parquet")
        _write_table(entries, LEDGER_ENTRY_SCHEMA, directory / "ledger_entries.parquet")
        workflow_records = {
            "fraud_alerts": dataset.alerts if directory == observable_modified else (),
            "fraud_cases": dataset.fraud_cases if directory == observable_modified else (),
            "case_confirmations": (
                dataset.case_confirmations if directory == observable_modified else ()
            ),
            "customer_disputes": (
                dataset.customer_disputes if directory == observable_modified else ()
            ),
            "fraud_labels": dataset.fraud_labels if directory == observable_modified else (),
        }
        # Operational copies keep the established observable/oracle boundary:
        # workflow truth fields are masked in observable data.
        _write_counterfactual_workflows(
            workflow_records,
            directory,
            mask_truth=directory == observable_modified,
        )
    _write_table(dataset.fraud_records, FRAUD_RECORD_SCHEMA, oracle / "fraud_records.parquet")
    _write_counterfactual_workflows(
        {
            "fraud_alerts": dataset.alerts,
            "fraud_cases": dataset.fraud_cases,
            "case_confirmations": dataset.case_confirmations,
            "customer_disputes": dataset.customer_disputes,
            "fraud_labels": dataset.fraud_labels,
        },
        oracle,
        mask_truth=False,
    )
    rows = []
    for item in dataset.change_sets:
        row = item.model_dump(mode="json")
        for key in (
            "per_dimension_costs",
            "changed_fields",
            "objective_satisfaction",
            "source_to_counterfactual",
        ):
            row[key] = json.dumps(row[key], sort_keys=True, separators=(",", ":"))
        rows.append(row)
    pl.DataFrame(rows, schema=COUNTERFACTUAL_CHANGE_SCHEMA, orient="row").write_parquet(
        oracle / "change_sets.parquet"
    )
    if (
        dataset.graph_campaigns
        or dataset.graph_memberships
        or dataset.graph_patterns
        or dataset.graph_evidence
        or dataset.graph_hyperedges
        or dataset.graph_hyperedge_memberships
    ):
        graph_oracle = oracle / "graph"
        graph_oracle.mkdir()
        graph_values = {
            "campaigns": dataset.graph_campaigns,
            "campaign_memberships": dataset.graph_memberships,
            "patterns": dataset.graph_patterns,
            "graph_evidence": dataset.graph_evidence,
            "hyperedges": dataset.graph_hyperedges,
            "hyperedge_memberships": dataset.graph_hyperedge_memberships,
        }
        for name, records in graph_values.items():
            if records:
                _write_table(records, GRAPH_TRUTH_SCHEMAS[name], graph_oracle / f"{name}.parquet")
    manifest_path = root / "counterfactual_manifest.json"
    write_json(
        manifest_path,
        {
            "counterfactual_id": dataset.counterfactual_id,
            "metadata": dataset.metadata,
            "counts": {
                "original_payments": len(dataset.original_payments),
                "modified_payments": len(dataset.modified_payments),
                "fraud_records": len(dataset.fraud_records),
                "fraud_alerts": len(dataset.alerts),
                "fraud_cases": len(dataset.fraud_cases),
                "case_confirmations": len(dataset.case_confirmations),
                "customer_disputes": len(dataset.customer_disputes),
                "fraud_labels": len(dataset.fraud_labels),
                "change_sets": len(dataset.change_sets),
                "rejected": len(dataset.rejected),
            },
            "artifacts": {
                "original": str(observable_original),
                "modified": str(observable_modified),
                "oracle": str(oracle),
            },
        },
    )
    return root, manifest_path
