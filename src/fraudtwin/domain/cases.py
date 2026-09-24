"""Fraud investigations and operationally delayed labels."""

from collections.abc import Mapping
from datetime import datetime
from typing import Literal

from pydantic import Field

from fraudtwin.domain.entities import _EntityModel
from fraudtwin.domain.fraud import FraudRecord

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
_ALERT_RECORD_FIELDS = (
    "customer_id",
    "account_id",
    "card_id",
    "device_id",
    "merchant_id",
    "payment_id",
    "event_id",
    "scenario_id",
    "scenario_type",
    "trigger",
    "reason",
    "amount",
    "currency",
    "correlation_id",
    "affected_entity_ids",
)
_CONFIRMATION_CASE_FIELDS = (
    "customer_id",
    "payment_id",
    "event_id",
    "fraud_record_id",
    "scenario_id",
    "scenario_type",
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
    fraud_truth: bool | None
    fraud_occurred_at: datetime
    alert_created_at: datetime
    case_opened_at: datetime
    case_closed_at: datetime | None
    fraud_confirmed_at: datetime | None
    label_available_at: datetime | None
    investigation_outcome: InvestigationOutcome
    loss_amount: float = Field(ge=0)
    recovered_amount: float = Field(ge=0)
    amount: float = Field(gt=0)
    currency: str
    correlation_id: str
    causation_id: str
    simulation_run_id: str | None
    affected_entity_ids: tuple[str, ...]
    case_reopened_at: datetime | None = None


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
    fraud_truth: bool | None
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
    fraud_truth: bool | None
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
    fraud_truth_by_record: Mapping[str, bool] | None = None,
    fraud_records_by_id: Mapping[str, FraudRecord] | None = None,
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
    truth_by_record = fraud_truth_by_record or {}
    records_by_id = fraud_records_by_id or {}

    for alert in alerts:
        _validate_affected_entities(alert.affected_entity_ids, all_entity_ids)
        _validate_references(alert, reference_pools, _FULL_REFERENCE_FIELDS)
        if truth_by_record and alert.fraud_record_id not in truth_by_record:
            raise ValueError("alert references an unknown fraud record truth")
        record = records_by_id.get(alert.fraud_record_id)
        if record is not None:
            if not _fields_match(alert, record, _ALERT_RECORD_FIELDS):
                raise ValueError("alert fields do not match its fraud record")
            if alert.alert_created_at < record.occurred_at:
                raise ValueError("alert precedes its fraud record")

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
        record = records_by_id.get(case.fraud_record_id)
        if record is not None and case.fraud_occurred_at != record.occurred_at:
            raise ValueError("case occurrence does not match its fraud record")
        truth = truth_by_record.get(case.fraud_record_id, case.fraud_truth)
        if truth_by_record and case.fraud_truth is not None and truth != case.fraud_truth:
            raise ValueError("case fraud truth does not match its fraud record")
        if not case.alert_created_at <= case.case_opened_at:
            raise ValueError("case opened before its alert")
        if case.case_closed_at is not None and case.case_closed_at < case.case_opened_at:
            raise ValueError("case closed before it opened")
        if case.fraud_confirmed_at is not None and case.fraud_confirmed_at < case.case_opened_at:
            raise ValueError("case confirmed before it opened")
        confirmation = confirmations_by_case.get(case.fraud_case_id)
        dispute = disputes_by_case.get(case.fraud_case_id)
        has_label_evidence = confirmation is not None or dispute is not None
        if (case.case_closed_at is None) != (not has_label_evidence):
            raise ValueError("case closure does not match its investigation evidence")
        if (case.label_available_at is None) != (not has_label_evidence):
            raise ValueError("unresolved case cannot expose a label availability time")
        expected_outcome: InvestigationOutcome
        if confirmation is not None:
            if truth is None:
                raise ValueError("confirmed case is missing fraud truth for validation")
            expected_outcome = "CONFIRMED_FRAUD" if truth else "FALSE_POSITIVE"
        elif dispute is not None:
            expected_outcome = "CUSTOMER_DISPUTE"
        else:
            expected_outcome = "UNRESOLVED"
        if case.investigation_outcome != expected_outcome:
            raise ValueError("case outcome does not match its investigation evidence")
        if case.label_available_at is not None and case.label_available_at < max(
            case.fraud_occurred_at,
            case.case_opened_at,
            case.fraud_confirmed_at or case.case_opened_at,
            case.case_closed_at or case.case_opened_at,
        ):
            raise ValueError("case label became available before required evidence")
        if case.fraud_confirmed_at != (
            confirmation.confirmed_at
            if confirmation is not None and expected_outcome == "CONFIRMED_FRAUD"
            else None
        ):
            raise ValueError("case confirmation timestamp does not match its confirmation")
        if case.recovered_amount > case.loss_amount:
            raise ValueError("case recovered amount exceeds realized loss")
        if case.case_closed_at is not None and case.case_closed_at != max(
            case.case_opened_at,
            *(
                timestamp
                for timestamp in (
                    confirmation.confirmed_at if confirmation is not None else None,
                    dispute.processed_at if dispute is not None else None,
                )
                if timestamp is not None
            ),
        ):
            raise ValueError("case closure does not match its latest evidence")

    for confirmation in confirmations:
        _validate_affected_entities(confirmation.affected_entity_ids, all_entity_ids)
        fraud_case = cases_by_id.get(confirmation.fraud_case_id)
        if fraud_case is None or confirmation.fraud_alert_id != fraud_case.fraud_alert_id:
            raise ValueError("confirmation references an unknown case")
        if not _fields_match(confirmation, fraud_case, _CONFIRMATION_CASE_FIELDS):
            raise ValueError("confirmation references do not match its case")
        truth = truth_by_record.get(confirmation.fraud_record_id, confirmation.fraud_truth)
        if (
            truth is not None
            and confirmation.fraud_truth is not None
            and truth != confirmation.fraud_truth
        ):
            raise ValueError("confirmation fraud truth does not match its fraud record")
        expected_confirmation_outcome: InvestigationOutcome | None = (
            "CONFIRMED_FRAUD" if truth else "FALSE_POSITIVE" if truth is False else None
        )
        if (
            expected_confirmation_outcome is not None
            and confirmation.investigation_outcome != expected_confirmation_outcome
        ):
            raise ValueError("confirmation outcome does not match its fraud record")
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
        if confirmation_item is None and dispute_item is None:
            raise ValueError("label has no investigation evidence")
        evidence = [fraud_case.fraud_occurred_at, fraud_case.case_opened_at]
        if confirmation_item is not None:
            evidence.append(confirmation_item.confirmed_at)
        if dispute_item is not None:
            evidence.append(dispute_item.processed_at)
        if label.label_available_at < max(evidence):
            raise ValueError("label became available before its evidence")
        if label.investigation_outcome != fraud_case.investigation_outcome:
            raise ValueError("label outcome does not match its case")
        expected_confirmed_at = (
            confirmation_item.confirmed_at
            if confirmation_item is not None
            and confirmation_item.investigation_outcome == "CONFIRMED_FRAUD"
            else None
        )
        if label.fraud_confirmed_at != expected_confirmed_at:
            raise ValueError("label confirmation timestamp does not match its evidence")
        expected_dispute_at = dispute_item.event_time if dispute_item is not None else None
        if label.dispute_event_at != expected_dispute_at:
            raise ValueError("label dispute timestamp does not match its evidence")
        expected_label = (
            "FRAUD"
            if label.investigation_outcome in {"CONFIRMED_FRAUD", "CUSTOMER_DISPUTE"}
            else "LEGITIMATE"
        )
        if label.label != expected_label:
            raise ValueError("label value does not match fraud truth")
        truth = truth_by_record.get(label.fraud_record_id, label.fraud_truth)
        if truth is not None and label.fraud_truth is not None and truth != label.fraud_truth:
            raise ValueError("label fraud truth does not match its fraud record")
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
