"""Deterministic fraud alert, case, dispute, and delayed-label workflows."""

from dataclasses import dataclass
from datetime import datetime, timedelta

from fraudtwin.config import SimulationRunConfig
from fraudtwin.domain import (
    CustomerDispute,
    DelayedFraudLabel,
    FraudAlert,
    FraudCase,
    FraudCaseConfirmation,
    FraudRecord,
    InvestigationOutcome,
    Payment,
    PaymentEvent,
    validate_fraud_workflow,
)
from fraudtwin.seed import create_stream_rng
from fraudtwin.simulation.fraud import FraudDataset
from fraudtwin.simulation.generator import EntityDataset

_ID_WIDTH = 6


@dataclass(frozen=True)
class FraudWorkflowDataset:
    """Stable M7 workflow records derived from M6 fraud records."""

    alerts: tuple[FraudAlert, ...]
    cases: tuple[FraudCase, ...]
    confirmations: tuple[FraudCaseConfirmation, ...]
    disputes: tuple[CustomerDispute, ...]
    labels: tuple[DelayedFraudLabel, ...]


class FraudWorkflowGenerator:
    """Turn scenario-linked M6 records into a delayed operational truth path."""

    def __init__(
        self,
        config: SimulationRunConfig,
        entities: EntityDataset,
        fraud_dataset: FraudDataset,
    ) -> None:
        self.seed = config.simulation.seed
        self.workflow = config.fraud_workflow
        self.entities = entities
        self.fraud_dataset = fraud_dataset
        self.payments_by_id = {payment.payment_id: payment for payment in fraud_dataset.payments}
        self.events_by_id = {event.event_id: event for event in fraud_dataset.payment_events}

    def generate(self) -> FraudWorkflowDataset:
        """Generate one deterministic workflow per selected alert/case path."""

        empty = FraudWorkflowDataset((), (), (), (), ())
        if not self.workflow.enabled or not self.fraud_dataset.fraud_records:
            return empty

        alerts: list[FraudAlert] = []
        cases: list[FraudCase] = []
        confirmations: list[FraudCaseConfirmation] = []
        disputes: list[CustomerDispute] = []
        labels: list[DelayedFraudLabel] = []
        for number, record in enumerate(self.fraud_dataset.fraud_records, start=1):
            payment = self.payments_by_id.get(record.payment_id)
            event = self.events_by_id.get(record.event_id)
            if payment is None or event is None:
                raise ValueError("fraud workflow input references an unknown payment or event")
            rng = create_stream_rng(self.seed, f"milestone-7:workflow:{record.fraud_record_id}")
            if rng.random() > self.workflow.alert_probability:
                continue
            alert = self._alert(number, record, payment, event)
            alerts.append(alert)
            if rng.random() > self.workflow.case_open_probability:
                continue
            case_number = len(cases) + 1
            case = self._case(case_number, alert, record, payment)

            confirmation: FraudCaseConfirmation | None = None
            if rng.random() <= self.workflow.confirmation_probability:
                confirmation = self._confirmation(len(confirmations) + 1, case, record, payment)
                confirmations.append(confirmation)

            dispute: CustomerDispute | None = None
            if record.fraud_truth and rng.random() <= self.workflow.customer_dispute_probability:
                dispute = self._dispute(len(disputes) + 1, case, record, payment, event)
                disputes.append(dispute)

            outcome = self._outcome(record, confirmation, dispute)
            closed_at = max(
                (
                    confirmation.confirmed_at if confirmation is not None else case.case_opened_at,
                    dispute.processed_at if dispute is not None else case.case_opened_at,
                )
            )
            label_evidence = [case.case_opened_at, closed_at, record.occurred_at]
            if confirmation is not None:
                label_evidence.append(confirmation.confirmed_at)
            if dispute is not None:
                label_evidence.append(dispute.processed_at)
            label_available_at = max(label_evidence) + timedelta(
                seconds=self.workflow.label_delay_seconds
            )
            case = case.model_copy(
                update={
                    "case_closed_at": closed_at,
                    "fraud_confirmed_at": (
                        confirmation.confirmed_at if confirmation is not None else None
                    ),
                    "label_available_at": label_available_at,
                    "investigation_outcome": outcome,
                }
            )
            cases.append(case)
            labels.append(
                self._label(
                    len(labels) + 1,
                    case,
                    record,
                    confirmation,
                    dispute,
                    label_available_at,
                    outcome,
                )
            )

        result = FraudWorkflowDataset(
            tuple(alerts), tuple(cases), tuple(confirmations), tuple(disputes), tuple(labels)
        )
        self._validate(result)
        return result

    def _alert(
        self, number: int, record: FraudRecord, payment: Payment, event: PaymentEvent
    ) -> FraudAlert:
        created_at = max(record.occurred_at, event.processed_at) + timedelta(
            seconds=self.workflow.alert_delay_seconds
        )
        return FraudAlert(
            fraud_alert_id=f"ALT-{number:0{_ID_WIDTH}d}",
            alert_type="AUTOMATED_SCENARIO_ALERT",
            severity="HIGH" if record.fraud_truth else "MEDIUM",
            customer_id=record.customer_id,
            account_id=record.account_id,
            card_id=record.card_id,
            device_id=record.device_id,
            merchant_id=record.merchant_id,
            payment_id=record.payment_id,
            event_id=record.event_id,
            fraud_record_id=record.fraud_record_id,
            scenario_id=record.scenario_id,
            scenario_type=record.scenario_type,
            trigger=record.trigger,
            reason=record.reason,
            alert_created_at=created_at,
            amount=payment.amount,
            currency=payment.currency,
            correlation_id=record.correlation_id,
            causation_id=record.event_id,
            simulation_run_id=event.simulation_run_id,
            affected_entity_ids=record.affected_entity_ids,
        )

    def _case(
        self, number: int, alert: FraudAlert, record: FraudRecord, payment: Payment
    ) -> FraudCase:
        opened_at = alert.alert_created_at + timedelta(
            seconds=self.workflow.case_open_delay_seconds
        )
        return FraudCase(
            fraud_case_id=f"CASE-{number:0{_ID_WIDTH}d}",
            fraud_alert_id=alert.fraud_alert_id,
            customer_id=record.customer_id,
            account_id=record.account_id,
            card_id=record.card_id,
            device_id=record.device_id,
            merchant_id=record.merchant_id,
            payment_id=record.payment_id,
            event_id=record.event_id,
            fraud_record_id=record.fraud_record_id,
            scenario_id=record.scenario_id,
            scenario_type=record.scenario_type,
            fraud_truth=record.fraud_truth,
            fraud_occurred_at=record.occurred_at,
            alert_created_at=alert.alert_created_at,
            case_opened_at=opened_at,
            case_closed_at=None,
            fraud_confirmed_at=None,
            label_available_at=opened_at,
            investigation_outcome="UNRESOLVED",
            loss_amount=payment.amount if record.fraud_truth else 0.0,
            recovered_amount=0.0,
            amount=payment.amount,
            currency=payment.currency,
            correlation_id=record.correlation_id,
            causation_id=alert.fraud_alert_id,
            simulation_run_id=alert.simulation_run_id,
            affected_entity_ids=record.affected_entity_ids,
        )

    def _confirmation(
        self,
        number: int,
        case: FraudCase,
        record: FraudRecord,
        payment: Payment,
    ) -> FraudCaseConfirmation:
        confirmed_at = case.case_opened_at + timedelta(
            seconds=self.workflow.confirmation_delay_seconds
        )
        outcome: InvestigationOutcome = "CONFIRMED_FRAUD" if record.fraud_truth else "LEGITIMATE"
        return FraudCaseConfirmation(
            confirmation_id=f"CNF-{number:0{_ID_WIDTH}d}",
            fraud_case_id=case.fraud_case_id,
            fraud_alert_id=case.fraud_alert_id,
            customer_id=record.customer_id,
            payment_id=record.payment_id,
            event_id=record.event_id,
            fraud_record_id=record.fraud_record_id,
            scenario_id=record.scenario_id,
            scenario_type=record.scenario_type,
            fraud_truth=record.fraud_truth,
            confirmed_at=confirmed_at,
            investigation_outcome=outcome,
            amount=payment.amount,
            currency=payment.currency,
            correlation_id=record.correlation_id,
            causation_id=case.fraud_case_id,
            simulation_run_id=case.simulation_run_id,
            affected_entity_ids=record.affected_entity_ids,
        )

    def _dispute(
        self,
        number: int,
        case: FraudCase,
        record: FraudRecord,
        payment: Payment,
        event: PaymentEvent,
    ) -> CustomerDispute:
        event_time = case.case_opened_at + timedelta(
            seconds=self.workflow.customer_dispute_delay_seconds
        )
        source_available_at = event_time + timedelta(seconds=2)
        return CustomerDispute(
            event_id=f"DSP-EVT-{number:0{_ID_WIDTH}d}",
            event_type="CUSTOMER_DISPUTE_SUBMITTED",
            event_version=1,
            fraud_case_id=case.fraud_case_id,
            fraud_alert_id=case.fraud_alert_id,
            fraud_record_id=record.fraud_record_id,
            underlying_event_id=record.event_id,
            payment_id=payment.payment_id,
            customer_id=record.customer_id,
            account_id=record.account_id,
            card_id=record.card_id,
            device_id=record.device_id,
            merchant_id=record.merchant_id,
            event_time=event_time,
            source_created_at=event_time,
            source_available_at=source_available_at,
            ingested_at=source_available_at + timedelta(seconds=1),
            processed_at=source_available_at + timedelta(seconds=2),
            producer="fraudtwin.customer",
            source_system="synthetic_customer_support_source",
            schema_version="1",
            correlation_id=record.correlation_id,
            causation_id=case.fraud_case_id,
            simulation_run_id=event.simulation_run_id,
            scenario_id=record.scenario_id,
            scenario_type=record.scenario_type,
            payment_rail=payment.payment_rail,
            payment_type=payment.payment_type,
            amount=payment.amount,
            currency=payment.currency,
            affected_entity_ids=record.affected_entity_ids,
        )

    def _label(
        self,
        number: int,
        case: FraudCase,
        record: FraudRecord,
        confirmation: FraudCaseConfirmation | None,
        dispute: CustomerDispute | None,
        label_available_at: datetime,
        outcome: InvestigationOutcome,
    ) -> DelayedFraudLabel:
        causation_id = (
            confirmation.confirmation_id
            if confirmation is not None
            else dispute.event_id
            if dispute is not None
            else case.fraud_case_id
        )
        return DelayedFraudLabel(
            label_id=f"LBL-{number:0{_ID_WIDTH}d}",
            fraud_case_id=case.fraud_case_id,
            fraud_alert_id=case.fraud_alert_id,
            fraud_record_id=record.fraud_record_id,
            customer_id=record.customer_id,
            payment_id=record.payment_id,
            event_id=record.event_id,
            scenario_id=record.scenario_id,
            scenario_type=record.scenario_type,
            label="FRAUD" if record.fraud_truth else "LEGITIMATE",
            fraud_truth=record.fraud_truth,
            fraud_occurred_at=record.occurred_at,
            fraud_confirmed_at=confirmation.confirmed_at if confirmation is not None else None,
            dispute_event_at=dispute.event_time if dispute is not None else None,
            label_available_at=label_available_at,
            investigation_outcome=outcome,
            amount=case.amount,
            currency=case.currency,
            correlation_id=case.correlation_id,
            causation_id=causation_id,
            simulation_run_id=case.simulation_run_id,
            affected_entity_ids=case.affected_entity_ids,
        )

    @staticmethod
    def _outcome(
        record: FraudRecord,
        confirmation: FraudCaseConfirmation | None,
        dispute: CustomerDispute | None,
    ) -> InvestigationOutcome:
        if confirmation is not None:
            return "CONFIRMED_FRAUD" if record.fraud_truth else "LEGITIMATE"
        if dispute is not None:
            return "CUSTOMER_DISPUTE"
        return "UNRESOLVED"

    def _validate(self, dataset: FraudWorkflowDataset) -> None:
        """Validate workflow references against all existing M1-M6 records."""

        entity_ids = {
            "customers": frozenset(entity.customer_id for entity in self.entities.customers),
            "accounts": frozenset(entity.account_id for entity in self.entities.accounts),
            "cards": frozenset(entity.card_id for entity in self.entities.cards),
            "devices": frozenset(entity.device_id for entity in self.entities.devices),
            "merchants": frozenset(entity.merchant_id for entity in self.entities.merchants),
        }
        validate_fraud_workflow(
            dataset.alerts,
            dataset.cases,
            dataset.confirmations,
            dataset.disputes,
            dataset.labels,
            customer_ids=entity_ids["customers"],
            account_ids=entity_ids["accounts"],
            card_ids=entity_ids["cards"],
            device_ids=entity_ids["devices"],
            merchant_ids=entity_ids["merchants"],
            payment_ids=frozenset(self.payments_by_id),
            event_ids=frozenset(self.events_by_id),
            fraud_record_ids=frozenset(
                record.fraud_record_id for record in self.fraud_dataset.fraud_records
            ),
            all_entity_ids=frozenset(
                entity_id
                for collection in (
                    self.entities.customers,
                    self.entities.accounts,
                    self.entities.cards,
                    self.entities.devices,
                    self.entities.merchants,
                    self.entities.pix_keys,
                )
                for entity in collection
                for entity_id in entity.model_dump().values()
                if isinstance(entity_id, str)
            ),
        )


__all__ = ["FraudWorkflowDataset", "FraudWorkflowGenerator"]
