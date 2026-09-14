"""Deterministic M8 data-quality faults applied to generated records.

The quality layer intentionally sits after the M1-M7 domain generators. This
keeps business generation and corruption concerns separate, and lets the
clean profile return the same generated business records and event order as
the earlier milestones.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from datetime import timedelta
from random import Random
from typing import TYPE_CHECKING, TypeVar

from pydantic import BaseModel

from fraudtwin.config import SimulationRunConfig
from fraudtwin.domain import (
    CustomerDispute,
    DelayedFraudLabel,
    FraudAlert,
    FraudCase,
    FraudCaseConfirmation,
    FraudRecord,
    LedgerEntry,
    Payment,
    PaymentEvent,
)
from fraudtwin.seed import create_stream_rng
from fraudtwin.simulation.quality_diagnostics import build_quality_diagnostics

if TYPE_CHECKING:
    from fraudtwin.simulation.behavior import BehaviorDataset

RecordModel = TypeVar("RecordModel", bound=BaseModel)

_FAULT_NAMES = (
    "duplicate_records",
    "duplicate_events",
    "missing_optional_fields",
    "invalid_values",
    "late_events",
    "out_of_order_events",
    "source_delay_events",
    "fraud_spikes",
    "traffic_spikes",
)


def _zero_counts() -> dict[str, int]:
    return dict.fromkeys(_FAULT_NAMES, 0)


def _clear_optional_fields(
    records: tuple[RecordModel, ...],
    *,
    fields: tuple[str, ...],
    rng: Random,
    probability: float,
) -> tuple[tuple[RecordModel, ...], int]:
    """Clear one populated optional field on selected records."""

    result: list[RecordModel] = []
    count = 0
    for record in records:
        if rng.random() >= probability:
            result.append(record)
            continue
        available = tuple(field for field in fields if getattr(record, field) is not None)
        if not available:
            result.append(record)
            continue
        field = rng.choice(available)
        result.append(record.model_copy(update={field: None}))
        count += 1
    return tuple(result), count


def _negative_amounts(
    records: tuple[RecordModel, ...], *, rng: Random, probability: float
) -> tuple[tuple[RecordModel, ...], int]:
    """Inject negative amounts without re-validating the chaos record."""

    result: list[RecordModel] = []
    count = 0
    for record in records:
        if rng.random() < probability:
            # model_copy intentionally bypasses re-validation: the output is
            # a chaos dataset, while the clean domain remains strict.
            amount = record.model_dump(mode="python")["amount"]
            result.append(record.model_copy(update={"amount": -abs(amount)}))
            count += 1
        else:
            result.append(record)
    return tuple(result), count


def _duplicate_rows(
    records: tuple[RecordModel, ...], rng: Random, probability: float
) -> tuple[tuple[RecordModel, ...], int]:
    """Append exact duplicate rows while retaining their original identity."""

    result: list[RecordModel] = []
    count = 0
    for record in records:
        result.append(record)
        if rng.random() < probability:
            result.append(record)
            count += 1
    return tuple(result), count


class QualityFaultInjector:
    """Apply independently configurable, reproducible M8 faults."""

    def __init__(self, config: SimulationRunConfig) -> None:
        self.config = config
        self.quality = config.quality

    def apply(self, dataset: BehaviorDataset) -> BehaviorDataset:
        """Return a quality-mutated dataset and measured fault metadata."""

        counts = _zero_counts()
        payments = dataset.payments
        events = dataset.payment_events
        fraud_records = dataset.fraud_records
        alerts = dataset.alerts
        cases = dataset.fraud_cases
        confirmations = dataset.case_confirmations
        disputes = dataset.customer_disputes
        labels = dataset.fraud_labels

        original_payment_count = len(payments)
        original_event_count = len(events)
        original_fraud_record_count = len(fraud_records)

        payments, payment_missing = self._missing_payment_fields(payments)
        events, event_missing = self._missing_event_fields(events)
        counts["missing_optional_fields"] = payment_missing + event_missing

        payments, payment_invalid = self._invalid_payment_values(payments)
        events, event_invalid = self._invalid_event_values(events)
        counts["invalid_values"] = payment_invalid + event_invalid

        events, source_delay_count = self._apply_source_delay(events)
        counts["source_delay_events"] = source_delay_count
        events, late_count = self._apply_late_events(events)
        counts["late_events"] = late_count

        payments, fraud_records, duplicate_record_count = self._duplicate_records(
            payments, fraud_records
        )
        counts["duplicate_records"] = duplicate_record_count
        events, duplicate_event_count = self._duplicate_events(events)
        counts["duplicate_events"] = duplicate_event_count

        traffic_group_count = len({event.payment_id for event in events})
        fraud_group_count = len(
            {event.scenario_id for event in events if event.scenario_id is not None}
        )
        events, traffic_count = self._apply_traffic_spikes(events)
        counts["traffic_spikes"] = traffic_count
        events, fraud_spike_count = self._apply_fraud_spikes(events)
        counts["fraud_spikes"] = fraud_spike_count
        events, out_of_order_count = self._apply_out_of_order(events)
        counts["out_of_order_events"] = out_of_order_count

        ledger_entries = self._align_ledger_timestamps(dataset.ledger_entries, events)
        alerts, cases, confirmations, disputes, labels = self._align_workflow_timestamps(
            dataset,
            events,
            alerts,
            cases,
            confirmations,
            disputes,
            labels,
        )

        rates = self._rates(
            counts,
            original_payment_count,
            original_event_count,
            original_fraud_record_count,
            traffic_group_count,
            fraud_group_count,
        )
        result = replace(
            dataset,
            payments=payments,
            payment_events=events,
            ledger_entries=ledger_entries,
            fraud_records=fraud_records,
            alerts=alerts,
            fraud_cases=cases,
            case_confirmations=confirmations,
            customer_disputes=disputes,
            fraud_labels=labels,
            quality_fault_counts=counts,
            quality_fault_rates=rates,
        )
        return replace(result, quality_diagnostics=build_quality_diagnostics(dataset, result))

    def _missing_payment_fields(
        self, payments: tuple[Payment, ...]
    ) -> tuple[tuple[Payment, ...], int]:
        return _clear_optional_fields(
            payments,
            fields=(
                "payee_account_id",
                "merchant_id",
                "card_id",
                "payer_institution_id",
                "payee_institution_id",
                "payer_pix_key_id",
                "payee_pix_key_id",
            ),
            rng=create_stream_rng(self.config.simulation.seed, "milestone-8:missing:payments"),
            probability=self.quality.probability("missing_optional"),
        )

    def _missing_event_fields(
        self, events: tuple[PaymentEvent, ...]
    ) -> tuple[tuple[PaymentEvent, ...], int]:
        return _clear_optional_fields(
            events,
            fields=(
                "payee_account_id",
                "merchant_id",
                "card_id",
                "device_id",
                "causation_id",
                "scenario_id",
                "scenario_type",
                "scenario_trigger",
                "scenario_reason",
                "fraud_record_id",
            ),
            rng=create_stream_rng(self.config.simulation.seed, "milestone-8:missing:events"),
            probability=self.quality.probability("missing_optional"),
        )

    def _invalid_payment_values(
        self, payments: tuple[Payment, ...]
    ) -> tuple[tuple[Payment, ...], int]:
        return _negative_amounts(
            payments,
            rng=create_stream_rng(self.config.simulation.seed, "milestone-8:invalid:payments"),
            probability=self.quality.probability("invalid_value"),
        )

    def _invalid_event_values(
        self, events: tuple[PaymentEvent, ...]
    ) -> tuple[tuple[PaymentEvent, ...], int]:
        return _negative_amounts(
            events,
            rng=create_stream_rng(self.config.simulation.seed, "milestone-8:invalid:events"),
            probability=self.quality.probability("invalid_value"),
        )

    def _apply_source_delay(
        self, events: tuple[PaymentEvent, ...]
    ) -> tuple[tuple[PaymentEvent, ...], int]:
        delay = self.quality.source_delay_seconds
        if delay == 0:
            return events, 0
        delta = timedelta(seconds=delay)
        return (
            tuple(self._shift_envelope(event, delta) for event in events),
            len(events),
        )

    def _apply_late_events(
        self, events: tuple[PaymentEvent, ...]
    ) -> tuple[tuple[PaymentEvent, ...], int]:
        probability = self.quality.probability("late_event")
        delay = self.quality.late_event_delay_seconds
        rng = create_stream_rng(self.config.simulation.seed, "milestone-8:late-events")
        result: list[PaymentEvent] = []
        count = 0
        for event in events:
            if delay > 0 and rng.random() < probability:
                result.append(self._shift_envelope(event, timedelta(seconds=delay)))
                count += 1
            else:
                result.append(event)
        return tuple(result), count

    @staticmethod
    def _shift_envelope(event: PaymentEvent, delta: timedelta) -> PaymentEvent:
        return event.model_copy(
            update={
                "source_available_at": event.source_available_at + delta,
                "ingested_at": event.ingested_at + delta,
                "processed_at": event.processed_at + delta,
            }
        )

    def _duplicate_records(
        self,
        payments: tuple[Payment, ...],
        fraud_records: tuple[FraudRecord, ...],
    ) -> tuple[tuple[Payment, ...], tuple[FraudRecord, ...], int]:
        probability = self.quality.probability("duplicate_record")
        rng = create_stream_rng(self.config.simulation.seed, "milestone-8:duplicate-records")
        payment_result, payment_count = _duplicate_rows(payments, rng, probability)
        fraud_result, fraud_count = _duplicate_rows(fraud_records, rng, probability)
        return payment_result, fraud_result, payment_count + fraud_count

    def _duplicate_events(
        self, events: tuple[PaymentEvent, ...]
    ) -> tuple[tuple[PaymentEvent, ...], int]:
        probability = self.quality.probability("duplicate_event")
        rng = create_stream_rng(self.config.simulation.seed, "milestone-8:duplicate-events")
        return _duplicate_rows(events, rng, probability)

    def _apply_traffic_spikes(
        self, events: tuple[PaymentEvent, ...]
    ) -> tuple[tuple[PaymentEvent, ...], int]:
        probability = self.quality.probability("traffic_spike")
        multiplier = self.quality.traffic_spike_multiplier
        if probability == 0 or multiplier < 2:
            return events, 0
        payment_ids = tuple(dict.fromkeys(event.payment_id for event in events))
        rng = create_stream_rng(self.config.simulation.seed, "milestone-8:traffic-spikes")
        selected = {payment_id for payment_id in payment_ids if rng.random() < probability}
        return self._duplicate_groups(events, selected, multiplier), len(selected)

    def _apply_fraud_spikes(
        self, events: tuple[PaymentEvent, ...]
    ) -> tuple[tuple[PaymentEvent, ...], int]:
        probability = self.quality.probability("fraud_spike")
        multiplier = self.quality.fraud_spike_multiplier
        if probability == 0 or multiplier < 2:
            return events, 0
        scenario_ids = tuple(
            dict.fromkeys(event.scenario_id for event in events if event.scenario_id is not None)
        )
        rng = create_stream_rng(self.config.simulation.seed, "milestone-8:fraud-spikes")
        selected = {scenario_id for scenario_id in scenario_ids if rng.random() < probability}
        return self._duplicate_groups(
            events,
            selected,
            multiplier,
            key=lambda event: event.scenario_id,
        ), len(selected)

    @staticmethod
    def _duplicate_groups(
        events: tuple[PaymentEvent, ...],
        selected: set[str],
        multiplier: int,
        *,
        key: Callable[[PaymentEvent], str | None] = lambda event: event.payment_id,
    ) -> tuple[PaymentEvent, ...]:
        groups: dict[str | None, list[PaymentEvent]] = {}
        last_position_by_key: dict[str | None, int] = {}
        for position, event in enumerate(events):
            event_key = key(event)
            groups.setdefault(event_key, []).append(event)
            last_position_by_key[event_key] = position
        result: list[PaymentEvent] = []
        for position, event in enumerate(events):
            result.append(event)
            event_key = key(event)
            if event_key not in selected:
                continue
            if position == last_position_by_key[event_key]:
                result.extend(groups[event_key] * (multiplier - 1))
        return tuple(result)

    def _apply_out_of_order(
        self, events: tuple[PaymentEvent, ...]
    ) -> tuple[tuple[PaymentEvent, ...], int]:
        probability = self.quality.probability("out_of_order")
        rng = create_stream_rng(self.config.simulation.seed, "milestone-8:out-of-order")
        positions_by_payment: dict[str, list[int]] = {}
        for position, event in enumerate(events):
            positions_by_payment.setdefault(event.payment_id, []).append(position)
        result = list(events)
        count = 0
        for positions in positions_by_payment.values():
            if len(positions) < 2 or rng.random() >= probability:
                continue
            values = [result[position] for position in positions]
            for position, value in zip(positions, (values[-1], *values[:-1]), strict=True):
                result[position] = value
            count += len(positions)
        return tuple(result), count

    @staticmethod
    def _align_ledger_timestamps(
        ledger_entries: tuple[LedgerEntry, ...], events: tuple[PaymentEvent, ...]
    ) -> tuple[LedgerEntry, ...]:
        processed_by_id = {event.event_id: event.processed_at for event in events}
        return tuple(
            entry.model_copy(update={"posted_at": processed_by_id[entry.event_id]})
            if entry.event_id in processed_by_id
            else entry
            for entry in ledger_entries
        )

    @staticmethod
    def _align_workflow_timestamps(
        dataset: BehaviorDataset,
        events: tuple[PaymentEvent, ...],
        alerts: tuple[FraudAlert, ...],
        cases: tuple[FraudCase, ...],
        confirmations: tuple[FraudCaseConfirmation, ...],
        disputes: tuple[CustomerDispute, ...],
        labels: tuple[DelayedFraudLabel, ...],
    ) -> tuple[
        tuple[FraudAlert, ...],
        tuple[FraudCase, ...],
        tuple[FraudCaseConfirmation, ...],
        tuple[CustomerDispute, ...],
        tuple[DelayedFraudLabel, ...],
    ]:
        original = {event.event_id: event.processed_at for event in dataset.payment_events}
        updated = {event.event_id: event.processed_at for event in events}
        shifts = {
            event_id: updated[event_id] - processed_at
            for event_id, processed_at in original.items()
            if event_id in updated and updated[event_id] != processed_at
        }
        if not shifts:
            return alerts, cases, confirmations, disputes, labels

        fraud_shifts = {
            record.fraud_record_id: shifts.get(record.event_id, timedelta())
            for record in dataset.fraud_records
        }

        def shift(record_id: str) -> timedelta:
            return fraud_shifts.get(record_id, timedelta())

        shifted_alerts = tuple(
            alert.model_copy(
                update={"alert_created_at": alert.alert_created_at + shift(alert.fraud_record_id)}
            )
            for alert in alerts
        )
        shifted_cases = tuple(
            case.model_copy(
                update={
                    "alert_created_at": case.alert_created_at + shift(case.fraud_record_id),
                    "case_opened_at": case.case_opened_at + shift(case.fraud_record_id),
                    "case_closed_at": (
                        case.case_closed_at + shift(case.fraud_record_id)
                        if case.case_closed_at is not None
                        else None
                    ),
                    "fraud_confirmed_at": (
                        case.fraud_confirmed_at + shift(case.fraud_record_id)
                        if case.fraud_confirmed_at is not None
                        else None
                    ),
                    "label_available_at": case.label_available_at + shift(case.fraud_record_id),
                }
            )
            for case in cases
        )
        shifted_confirmations = tuple(
            confirmation.model_copy(
                update={
                    "confirmed_at": confirmation.confirmed_at + shift(confirmation.fraud_record_id)
                }
            )
            for confirmation in confirmations
        )
        shifted_disputes = tuple(
            dispute.model_copy(
                update={
                    "event_time": dispute.event_time + shift(dispute.fraud_record_id),
                    "source_created_at": dispute.source_created_at + shift(dispute.fraud_record_id),
                    "source_available_at": dispute.source_available_at
                    + shift(dispute.fraud_record_id),
                    "ingested_at": dispute.ingested_at + shift(dispute.fraud_record_id),
                    "processed_at": dispute.processed_at + shift(dispute.fraud_record_id),
                }
            )
            for dispute in disputes
        )
        shifted_labels = tuple(
            label.model_copy(
                update={
                    "fraud_confirmed_at": (
                        label.fraud_confirmed_at + shift(label.fraud_record_id)
                        if label.fraud_confirmed_at is not None
                        else None
                    ),
                    "dispute_event_at": (
                        label.dispute_event_at + shift(label.fraud_record_id)
                        if label.dispute_event_at is not None
                        else None
                    ),
                    "label_available_at": label.label_available_at + shift(label.fraud_record_id),
                }
            )
            for label in labels
        )
        return (
            shifted_alerts,
            shifted_cases,
            shifted_confirmations,
            shifted_disputes,
            shifted_labels,
        )

    def _rates(
        self,
        counts: dict[str, int],
        payment_count: int,
        event_count: int,
        fraud_record_count: int,
        traffic_group_count: int,
        fraud_group_count: int,
    ) -> dict[str, float]:
        denominators = {
            "duplicate_records": payment_count + fraud_record_count,
            "duplicate_events": event_count,
            "missing_optional_fields": payment_count + event_count,
            "invalid_values": payment_count + event_count,
            "late_events": event_count,
            "out_of_order_events": event_count,
            "source_delay_events": event_count,
            "fraud_spikes": fraud_group_count,
            "traffic_spikes": traffic_group_count,
        }
        requested = {
            "duplicate_records": self.quality.probability("duplicate_record"),
            "duplicate_events": self.quality.probability("duplicate_event"),
            "missing_optional_fields": self.quality.probability("missing_optional"),
            "invalid_values": self.quality.probability("invalid_value"),
            "late_events": self.quality.probability("late_event"),
            "out_of_order_events": self.quality.probability("out_of_order"),
            "fraud_spikes": self.quality.probability("fraud_spike"),
            "traffic_spikes": self.quality.probability("traffic_spike"),
        }
        rates: dict[str, float] = {}
        for name, requested_rate in requested.items():
            rates[f"{name}_requested"] = requested_rate
            rates[f"{name}_realized"] = counts[name] / max(1, denominators[name])
        rates["source_delay_seconds"] = float(self.quality.source_delay_seconds)
        rates["late_event_delay_seconds"] = float(self.quality.late_event_delay_seconds)
        return rates
