"""Fraud investigations and operationally delayed labels for Milestone 7."""

from collections.abc import Mapping
from datetime import datetime
from typing import Literal

from pydantic import Field

from fraudtwin.domain.entities import _EntityModel

InvestigationOutcome = Literal[
    "CONFIRMED_FRAUD",
    "FALSE_POSITIVE",
    "CUSTOMER_DISPUTE",
    "UNRESOLVED",
    "LEGITIMATE",
]
FraudWorkflowEventType = Literal["CUSTOMER_DISPUTE_SUBMITTED"]

_FULL_REFERENCE_FIELDS = (
    "customer_id",
    "account_id",
    "card_id",
    "device_id",
    "merchant_id",
    "payment_id",
    "event_id",
    "fraud_record_id",
)
_EVENT_REFERENCE_FIELDS = ("customer_id", "payment_id", "event_id", "fraud_record_id")
_DISPUTE_REFERENCE_FIELDS = (
    "customer_id",
    "account_id",
    "card_id",
    "device_id",
    "merchant_id",
    "payment_id",
    "underlying_event_id",
    "fraud_record_id",
)
_CASE_ALERT_FIELDS = (
    "customer_id",
    "account_id",
    "card_id",
    "device_id",
    "merchant_id",
    "payment_id",
    "event_id",
    "scenario_id",
    "scenario_type",
    "amount",
    "currency",
)
_CONFIRMATION_CASE_FIELDS = (
    "customer_id",
    "payment_id",
    "event_id",
    "fraud_record_id",
    "scenario_id",
    "scenario_type",
    "fraud_truth",
    "amount",
    "currency",
)
_DISPUTE_CASE_FIELDS = (
    "customer_id",
    "account_id",
    "card_id",
    "device_id",
    "merchant_id",
    "payment_id",
    "scenario_id",
    "scenario_type",
    "amount",
    "currency",
)
_LABEL_CASE_FIELDS = (
    "customer_id",
    "payment_id",
    "event_id",
    "scenario_id",
    "scenario_type",
    "fraud_truth",
    "amount",
    "currency",
)


class FraudAlert(_EntityModel):
    """An automated alert causally raised from one M6 fraud record."""

    fraud_alert_id: str
    alert_type: Literal["AUTOMATED_SCENARIO_ALERT"]
    severity: Literal["LOW", "MEDIUM", "HIGH"]
    customer_id: str
    account_id: str | None
    card_id: str | None
    device_id: str | None
    merchant_id: str | None
    payment_id: str
    event_id: str
    fraud_record_id: str
    scenario_id: str
    scenario_type: str
    trigger: str
    reason: str
    alert_created_at: datetime
    amount: float = Field(gt=0)
    currency: str
    correlation_id: str
    causation_id: str
    simulation_run_id: str | None
    affected_entity_ids: tuple[str, ...]


class FraudCase(_EntityModel):
    """An investigation opened from an alert and its delayed label state."""

    fraud_case_id: str
    fraud_alert_id: str
    customer_id: str
    account_id: str | None
    card_id: str | None
    device_id: str | None
    merchant_id: str | None
    payment_id: str
    event_id: str
    fraud_record_id: str
    scenario_id: str
    scenario_type: str
    fraud_truth: bool
    fraud_occurred_at: datetime
    alert_created_at: datetime
    case_opened_at: datetime
    case_closed_at: datetime | None
    fraud_confirmed_at: datetime | None
    label_available_at: datetime
    investigation_outcome: InvestigationOutcome
    loss_amount: float = Field(ge=0)
    recovered_amount: float = Field(ge=0)
    amount: float = Field(gt=0)
    currency: str
    correlation_id: str
    causation_id: str
    simulation_run_id: str | None
    affected_entity_ids: tuple[str, ...]


class FraudCaseConfirmation(_EntityModel):
    """A case decision, recorded separately from the fraud ground truth."""

    confirmation_id: str
    fraud_case_id: str
    fraud_alert_id: str
    customer_id: str
    payment_id: str
    event_id: str
    fraud_record_id: str
    scenario_id: str
    scenario_type: str
    fraud_truth: bool
    confirmed_at: datetime
    investigation_outcome: InvestigationOutcome
    amount: float = Field(gt=0)
    currency: str
    correlation_id: str
    causation_id: str
    simulation_run_id: str | None
    affected_entity_ids: tuple[str, ...]


class CustomerDispute(_EntityModel):
    """A customer-submitted dispute event with the common event envelope."""

    event_id: str
    event_type: FraudWorkflowEventType
    event_version: int = Field(ge=1)
    fraud_case_id: str
    fraud_alert_id: str
    fraud_record_id: str
    underlying_event_id: str
    payment_id: str
    customer_id: str
    account_id: str | None
    card_id: str | None
    device_id: str | None
    merchant_id: str | None
    event_time: datetime
    source_created_at: datetime
    source_available_at: datetime
    ingested_at: datetime
    processed_at: datetime
    producer: str
    source_system: str
    schema_version: str
    correlation_id: str
    causation_id: str
    simulation_run_id: str | None
    scenario_id: str
    scenario_type: str
    payment_rail: str
    payment_type: str
    amount: float = Field(gt=0)
    currency: str
    affected_entity_ids: tuple[str, ...]


class DelayedFraudLabel(_EntityModel):
    """A fraud label exposed only after the configured operational evidence."""

    label_id: str
    fraud_case_id: str
    fraud_alert_id: str
    fraud_record_id: str
    customer_id: str
    payment_id: str
    event_id: str
    scenario_id: str
    scenario_type: str
    label: Literal["FRAUD", "LEGITIMATE"]
    fraud_truth: bool
    fraud_occurred_at: datetime
    fraud_confirmed_at: datetime | None
    dispute_event_at: datetime | None
    label_available_at: datetime
    investigation_outcome: InvestigationOutcome
    amount: float = Field(gt=0)
    currency: str
    correlation_id: str
    causation_id: str
    simulation_run_id: str | None
    affected_entity_ids: tuple[str, ...]


def validate_fraud_workflow(
    alerts: tuple[FraudAlert, ...],
    cases: tuple[FraudCase, ...],
    confirmations: tuple[FraudCaseConfirmation, ...],
    disputes: tuple[CustomerDispute, ...],
    labels: tuple[DelayedFraudLabel, ...],
    *,
    customer_ids: frozenset[str] = frozenset(),
    account_ids: frozenset[str] = frozenset(),
    card_ids: frozenset[str] = frozenset(),
    device_ids: frozenset[str] = frozenset(),
    merchant_ids: frozenset[str] = frozenset(),
    payment_ids: frozenset[str] = frozenset(),
    event_ids: frozenset[str] = frozenset(),
    fraud_record_ids: frozenset[str] = frozenset(),
    all_entity_ids: frozenset[str] = frozenset(),
) -> None:
    """Validate M7 references, causal chains, and temporal availability rules."""

    def unique(values: tuple[str, ...], kind: str) -> None:
        if len(set(values)) != len(values):
            raise ValueError(f"duplicate {kind} IDs")

    unique(tuple(alert.fraud_alert_id for alert in alerts), "alert")
    unique(tuple(case.fraud_case_id for case in cases), "case")
    unique(tuple(item.confirmation_id for item in confirmations), "confirmation")
    unique(tuple(item.fraud_case_id for item in confirmations), "confirmation case reference")
    unique(tuple(dispute.event_id for dispute in disputes), "dispute event")
    unique(tuple(dispute.fraud_case_id for dispute in disputes), "dispute case reference")
    unique(tuple(label.label_id for label in labels), "label")

    alerts_by_id = {alert.fraud_alert_id: alert for alert in alerts}
    cases_by_id = {case.fraud_case_id: case for case in cases}
    confirmations_by_case = {item.fraud_case_id: item for item in confirmations}
    disputes_by_case = {item.fraud_case_id: item for item in disputes}
    reference_pools = {
        "customer_id": (customer_ids, "customer"),
        "account_id": (account_ids, "account"),
        "card_id": (card_ids, "card"),
        "device_id": (device_ids, "device"),
        "merchant_id": (merchant_ids, "merchant"),
        "payment_id": (payment_ids, "payment"),
        "event_id": (event_ids, "event"),
        "underlying_event_id": (event_ids, "event"),
        "fraud_record_id": (fraud_record_ids, "fraud record"),
    }

    for alert in alerts:
        _validate_affected_entities(alert.affected_entity_ids, all_entity_ids)
        _validate_references(alert, reference_pools, _FULL_REFERENCE_FIELDS)

    for case in cases:
        _validate_affected_entities(case.affected_entity_ids, all_entity_ids)
        if case.fraud_alert_id not in alerts_by_id:
            raise ValueError("case references an unknown alert")
        alert = alerts_by_id[case.fraud_alert_id]
        if (
            case.fraud_record_id != alert.fraud_record_id
            or case.causation_id != alert.fraud_alert_id
        ):
            raise ValueError("case causal references do not match its alert")
        if not _fields_match(case, alert, _CASE_ALERT_FIELDS):
            raise ValueError("case references do not match its alert")
        _validate_references(case, reference_pools, _FULL_REFERENCE_FIELDS)
        if not case.alert_created_at <= case.case_opened_at:
            raise ValueError("case opened before its alert")
        if case.case_closed_at is not None and case.case_closed_at < case.case_opened_at:
            raise ValueError("case closed before it opened")
        if case.fraud_confirmed_at is not None and case.fraud_confirmed_at < case.case_opened_at:
            raise ValueError("case confirmed before it opened")
        if case.label_available_at < max(
            case.fraud_occurred_at,
            case.case_opened_at,
            case.fraud_confirmed_at or case.case_opened_at,
            case.case_closed_at or case.case_opened_at,
        ):
            raise ValueError("case label became available before required evidence")
        confirmation = confirmations_by_case.get(case.fraud_case_id)
        if case.fraud_confirmed_at != (
            confirmation.confirmed_at if confirmation is not None else None
        ):
            raise ValueError("case confirmation timestamp does not match its confirmation")

    for confirmation in confirmations:
        _validate_affected_entities(confirmation.affected_entity_ids, all_entity_ids)
        fraud_case = cases_by_id.get(confirmation.fraud_case_id)
        if fraud_case is None or confirmation.fraud_alert_id != fraud_case.fraud_alert_id:
            raise ValueError("confirmation references an unknown case")
        if not _fields_match(confirmation, fraud_case, _CONFIRMATION_CASE_FIELDS):
            raise ValueError("confirmation references do not match its case")
        _validate_references(confirmation, reference_pools, _EVENT_REFERENCE_FIELDS)
        if confirmation.causation_id != fraud_case.fraud_case_id:
            raise ValueError("confirmation causation does not reference its case")
        if confirmation.confirmed_at < fraud_case.case_opened_at:
            raise ValueError("confirmation precedes case opening")

    for dispute in disputes:
        _validate_affected_entities(dispute.affected_entity_ids, all_entity_ids)
        fraud_case = cases_by_id.get(dispute.fraud_case_id)
        if fraud_case is None:
            raise ValueError("dispute references an unknown case")
        if dispute.fraud_alert_id != fraud_case.fraud_alert_id:
            raise ValueError("dispute references an unknown case alert")
        if (
            dispute.fraud_record_id != fraud_case.fraud_record_id
            or dispute.causation_id != fraud_case.fraud_case_id
            or dispute.underlying_event_id != fraud_case.event_id
        ):
            raise ValueError("dispute causal references do not match its case")
        if not _fields_match(dispute, fraud_case, _DISPUTE_CASE_FIELDS):
            raise ValueError("dispute references do not match its case")
        _validate_references(dispute, reference_pools, _DISPUTE_REFERENCE_FIELDS)
        if dispute.event_time < fraud_case.case_opened_at:
            raise ValueError("dispute precedes case opening")
        if not (
            dispute.event_time
            <= dispute.source_available_at
            <= dispute.ingested_at
            <= dispute.processed_at
        ):
            raise ValueError("dispute event envelope is not ordered")

    for label in labels:
        _validate_affected_entities(label.affected_entity_ids, all_entity_ids)
        fraud_case = cases_by_id.get(label.fraud_case_id)
        if fraud_case is None:
            raise ValueError("label references an unknown case")
        if label.fraud_alert_id != fraud_case.fraud_alert_id:
            raise ValueError("label references an unknown case alert")
        if label.fraud_record_id != fraud_case.fraud_record_id:
            raise ValueError("label references a different fraud record")
        if not _fields_match(label, fraud_case, _LABEL_CASE_FIELDS):
            raise ValueError("label references do not match its case")
        _validate_references(label, reference_pools, _EVENT_REFERENCE_FIELDS)
        confirmation_item = confirmations_by_case.get(label.fraud_case_id)
        dispute_item = disputes_by_case.get(label.fraud_case_id)
        evidence = [fraud_case.fraud_occurred_at, fraud_case.case_opened_at]
        if confirmation_item is not None:
            evidence.append(confirmation_item.confirmed_at)
        if dispute_item is not None:
            evidence.append(dispute_item.processed_at)
        if label.label_available_at < max(evidence):
            raise ValueError("label became available before its evidence")
        if label.label != ("FRAUD" if label.fraud_truth else "LEGITIMATE"):
            raise ValueError("label value does not match fraud truth")
        if label.label_available_at != fraud_case.label_available_at:
            raise ValueError("label availability does not match its case")
        if label.causation_id not in {
            fraud_case.fraud_case_id,
            confirmation_item.confirmation_id if confirmation_item is not None else "",
            dispute_item.event_id if dispute_item is not None else "",
        }:
            raise ValueError("label causation does not reference its evidence")


def _fields_match(left: object, right: object, fields: tuple[str, ...]) -> bool:
    """Return whether two workflow records share the given context fields."""

    return all(getattr(left, field) == getattr(right, field) for field in fields)


def _validate_references(
    item: object,
    reference_pools: Mapping[str, tuple[frozenset[str], str]],
    fields: tuple[str, ...],
) -> None:
    """Check optional entity references when the caller supplies entity pools."""

    for field in fields:
        reference = getattr(item, field)
        valid_ids, kind = reference_pools[field]
        if reference is not None and valid_ids and reference not in valid_ids:
            raise ValueError(f"workflow references unknown {kind} {reference}")


def _validate_affected_entities(
    affected_entity_ids: tuple[str, ...], all_entity_ids: frozenset[str]
) -> None:
    """Ensure scenario metadata never points at an unknown entity."""

    if all_entity_ids and not set(affected_entity_ids) <= all_entity_ids:
        raise ValueError("workflow affected entities contain an unknown entity")


__all__ = [
    "CustomerDispute",
    "DelayedFraudLabel",
    "FraudAlert",
    "FraudCase",
    "FraudCaseConfirmation",
    "FraudWorkflowEventType",
    "InvestigationOutcome",
    "validate_fraud_workflow",
]
