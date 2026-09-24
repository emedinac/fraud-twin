"""Deterministic M8 data-quality faults applied to generated records.

The quality layer intentionally sits after the M1-M7 domain generators. This
keeps business generation and corruption concerns separate, and lets the
clean profile return the same generated business records and event order as
the earlier milestones.
"""

import base64
import json
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
from fraudtwin.reproducibility import sha256_json
from fraudtwin.schema import apply_schema_change, schema_change_metadata
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
    "invalid_enums",
    "invalid_references",
    "negative_amounts",
    "corrupted_timestamps",
    "timezone_errors",
    "schema_mismatches",
    "extreme_values",
    "encoding_errors",
    "partition_skews",
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
    fault: str,
    audit: list[dict[str, object]],
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
        audit.append(
            {
                "fault": fault,
                "target": getattr(record, "event_id", getattr(record, "payment_id", "unknown")),
                "field": field,
                "requested_probability": probability,
                "mutation": "set_null",
                "logical_identity_preserved": True,
                "affected_boundary": "source",
                "expected_validation_rule": "optional field may be null",
            }
        )
        count += 1
    return tuple(result), count


def _negative_amounts(
    records: tuple[RecordModel, ...],
    *,
    rng: Random,
    probability: float,
    audit: list[dict[str, object]],
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
            audit.append(
                {
                    "fault": "negative_amount",
                    "target": getattr(record, "event_id", getattr(record, "payment_id", "unknown")),
                    "requested_probability": probability,
                    "mutation": {"amount": -abs(amount)},
                    "logical_identity_preserved": True,
                    "affected_boundary": "source",
                    "expected_validation_rule": "amount > 0",
                }
            )
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

    def apply(self, dataset: "BehaviorDataset") -> "BehaviorDataset":
        """Return a quality-mutated dataset and measured fault metadata."""

        # This order is part of the reproducibility contract: field faults,
        # source/contract faults, delivery faults, duplication, and finally
        # group/order stress are applied in stable phases.

        counts = _zero_counts()
        if self.quality.outages:
            counts["outage_events"] = 0
        if self.quality.schema_changes:
            counts["schema_changes"] = 0
        audit: list[dict[str, object]] = []
        raw_faults: list[dict[str, object]] = []
        schema_evolution_rows: dict[str, list[dict[str, object]]] = {}
        oracle_tables = dataset.oracle_tables or dataset.tables()
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

        payments, payment_missing = self._missing_payment_fields(payments, audit)
        events, event_missing = self._missing_event_fields(events, audit)
        counts["missing_optional_fields"] = payment_missing + event_missing

        payments, payment_invalid = self._invalid_payment_values(payments, audit)
        events, event_invalid = self._invalid_event_values(events, audit)
        legacy_invalid_values = (
            self.quality.invalid_value_probability is not None
            and self.quality.negative_amount_probability is None
        )
        counts["negative_amounts"] = 0 if legacy_invalid_values else payment_invalid + event_invalid
        counts["invalid_values"] = (
            payment_invalid + event_invalid
            if self.quality.invalid_value_probability is not None
            else 0
        )

        events, count = self._invalid_enums(events, audit)
        counts["invalid_enums"] = count
        events, count = self._invalid_references(events, audit)
        counts["invalid_references"] = count
        events, count = self._corrupt_timestamps(events, audit)
        counts["corrupted_timestamps"] = count
        events, count = self._timezone_errors(events, audit)
        counts["timezone_errors"] = count
        events, count = self._schema_mismatches(events, audit)
        counts["schema_mismatches"] = count
        events, count = self._extreme_values(events, audit)
        counts["extreme_values"] = count
        events, count = self._encoding_errors(events, audit, raw_faults)
        counts["encoding_errors"] = count
        events, count = self._partition_skew(events, audit)
        counts["partition_skews"] = count

        events, source_delay_count = self._apply_source_delay(events)
        counts["source_delay_events"] = source_delay_count
        events, outage_count = self._apply_outages(events, audit)
        if self.quality.outages:
            counts["outage_events"] = outage_count
        events, schema_count, schema_evolution_rows = self._apply_schema_changes(events, audit)
        if self.quality.schema_changes:
            counts["schema_changes"] = schema_count
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

        for entry in audit:
            entry.setdefault("actual_mutation", entry.get("mutation"))
            entry.setdefault(
                "identity_effect",
                "preserved" if entry.get("logical_identity_preserved") else "changed",
            )
            entry.setdefault("expected_validation_failure", entry.get("expected_validation_rule"))
            entry.setdefault("requested_count", None)

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
            oracle_tables=oracle_tables,
            quality_raw_faults=tuple(raw_faults),
            schema_evolution_rows={key: tuple(rows) for key, rows in schema_evolution_rows.items()},
        )
        diagnostics = build_quality_diagnostics(dataset, result)
        diagnostics["oracle_fingerprint"] = sha256_json(
            {
                name: [record.model_dump(mode="json") for record in records]
                for name, records in oracle_tables.items()
            }
        )
        diagnostics["fault_audit"] = audit
        diagnostics["schema_evolution"] = {
            "changes": [schema_change_metadata(item) for item in self.quality.schema_changes],
            "outputs": {key: len(rows) for key, rows in sorted(schema_evolution_rows.items())},
        }
        return replace(result, quality_diagnostics=diagnostics)

    def _apply_outages(
        self, events: tuple[PaymentEvent, ...], audit: list[dict[str, object]]
    ) -> tuple[tuple[PaymentEvent, ...], int]:
        result: list[PaymentEvent] = []
        count = 0
        rng = create_stream_rng(self.config.simulation.seed, "milestone-8:outages")
        for event in events:
            current = event
            dropped = False
            for outage in self.quality.outages:
                if not outage.from_time <= event.event_time < outage.to_time:
                    continue
                if outage.source not in {
                    "*",
                    "payment_events",
                    event.event_type,
                    event.source_system,
                }:
                    continue
                action = outage.behavior
                if action in {"DROP", "UNAVAILABLE"} or (
                    action == "PARTIAL_REJECT" and rng.random() < outage.reject_probability
                ):
                    dropped = True
                elif action == "BUFFER_AND_FLUSH":
                    current = self._shift_envelope(current, outage.to_time - event.event_time)
                elif action == "DELAY":
                    current = self._shift_envelope(current, timedelta(seconds=outage.delay_seconds))
                count += 1
                audit.append(
                    {
                        "fault": "outage",
                        "event_id": event.event_id,
                        "source": outage.source,
                        "behavior": action,
                        "dropped": dropped,
                    }
                )
            if not dropped:
                result.append(current)
        return tuple(result), count

    def _apply_schema_changes(
        self, events: tuple[PaymentEvent, ...], audit: list[dict[str, object]]
    ) -> tuple[tuple[PaymentEvent, ...], int, dict[str, list[dict[str, object]]]]:
        result: list[PaymentEvent] = []
        count = 0
        serialized_rows: dict[str, list[dict[str, object]]] = {}
        for event in events:
            current = event
            row = event.model_dump(mode="python", warnings=False)
            for change in self.quality.schema_changes:
                if event.event_time < change.at:
                    continue
                if change.event not in {"*", "payment_events", event.event_type}:
                    continue
                current = current.model_copy(update={"schema_version": change.version})
                row = apply_schema_change(row, change)
                row["schema_version"] = change.version
                serialized_rows.setdefault(f"{change.event}:{change.version}", []).append(row)
                count += 1
                audit.append(
                    {
                        "fault": "schema_change",
                        "event_id": event.event_id,
                        "event": change.event,
                        "version": change.version,
                        "change": change.change,
                        "compatibility": schema_change_metadata(change)["compatibility"],
                        "logical_identity_preserved": True,
                        "affected_boundary": "serialized_source",
                    }
                )
            result.append(current)
        return tuple(result), count, serialized_rows

    def _missing_payment_fields(
        self, payments: tuple[Payment, ...], audit: list[dict[str, object]]
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
            fault="missing_field",
            audit=audit,
        )

    def _missing_event_fields(
        self, events: tuple[PaymentEvent, ...], audit: list[dict[str, object]]
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
            fault="missing_field",
            audit=audit,
        )

    def _invalid_payment_values(
        self, payments: tuple[Payment, ...], audit: list[dict[str, object]]
    ) -> tuple[tuple[Payment, ...], int]:
        return _negative_amounts(
            payments,
            rng=create_stream_rng(self.config.simulation.seed, "milestone-8:invalid:payments"),
            probability=self.quality.probability("negative_amount"),
            audit=audit,
        )

    def _invalid_event_values(
        self, events: tuple[PaymentEvent, ...], audit: list[dict[str, object]]
    ) -> tuple[tuple[PaymentEvent, ...], int]:
        return _negative_amounts(
            events,
            rng=create_stream_rng(self.config.simulation.seed, "milestone-8:invalid:events"),
            probability=self.quality.probability("negative_amount"),
            audit=audit,
        )

    def _apply_event_mutation(
        self,
        events: tuple[PaymentEvent, ...],
        audit: list[dict[str, object]],
        *,
        probability_name: str,
        stream_name: str,
        fault: str,
        expected_rule: str,
        mutation: Callable[[PaymentEvent], tuple[dict[str, object], object]],
    ) -> tuple[tuple[PaymentEvent, ...], int]:
        """Apply one deterministic event fault and record its audit entries."""

        probability = self.quality.probability(probability_name)
        rng = create_stream_rng(self.config.simulation.seed, f"milestone-8:{stream_name}")
        result: list[PaymentEvent] = []
        count = 0
        for event in events:
            if rng.random() >= probability:
                result.append(event)
                continue
            updates, mutation_description = mutation(event)
            result.append(event.model_copy(update=updates))
            audit.append(
                {
                    "fault": fault,
                    "target": event.event_id,
                    "requested_probability": probability,
                    "mutation": mutation_description,
                    "logical_identity_preserved": True,
                    "affected_boundary": "typed_output",
                    "expected_validation_rule": expected_rule,
                }
            )
            count += 1
        return tuple(result), count

    def _invalid_enums(
        self, events: tuple[PaymentEvent, ...], audit: list[dict[str, object]]
    ) -> tuple[tuple[PaymentEvent, ...], int]:
        return self._apply_event_mutation(
            events,
            audit,
            probability_name="invalid_enum",
            stream_name="invalid-enums",
            fault="invalid_enum",
            expected_rule="event_type is a registered payment event",
            mutation=lambda event: (
                {"event_type": f"INVALID_ENUM:{event.event_id}"},
                {"event_type": f"INVALID_ENUM:{event.event_id}"},
            ),
        )

    def _invalid_references(
        self, events: tuple[PaymentEvent, ...], audit: list[dict[str, object]]
    ) -> tuple[tuple[PaymentEvent, ...], int]:
        return self._apply_event_mutation(
            events,
            audit,
            probability_name="invalid_reference",
            stream_name="invalid-references",
            fault="invalid_reference",
            expected_rule="payment_id references payments.payment_id",
            mutation=lambda event: (
                {"payment_id": f"PAYMENT-UNKNOWN-{event.event_id}"},
                {"payment_id": f"PAYMENT-UNKNOWN-{event.event_id}"},
            ),
        )

    def _corrupt_timestamps(
        self, events: tuple[PaymentEvent, ...], audit: list[dict[str, object]]
    ) -> tuple[tuple[PaymentEvent, ...], int]:
        return self._apply_event_mutation(
            events,
            audit,
            probability_name="corrupted_timestamp",
            stream_name="corrupted-timestamps",
            fault="corrupted_timestamp",
            expected_rule="event_time <= source_available_at",
            mutation=lambda event: (
                {"source_available_at": event.event_time - timedelta(seconds=1)},
                {"source_available_at": (event.event_time - timedelta(seconds=1)).isoformat()},
            ),
        )

    def _timezone_errors(
        self, events: tuple[PaymentEvent, ...], audit: list[dict[str, object]]
    ) -> tuple[tuple[PaymentEvent, ...], int]:
        return self._apply_event_mutation(
            events,
            audit,
            probability_name="timezone_error",
            stream_name="timezone-errors",
            fault="timezone_error",
            expected_rule="timestamps include a timezone",
            mutation=lambda event: (
                {"event_time": event.event_time.replace(tzinfo=None)},
                {
                    "event_time": event.event_time.replace(tzinfo=None).isoformat(),
                    "timezone": None,
                },
            ),
        )

    def _schema_mismatches(
        self, events: tuple[PaymentEvent, ...], audit: list[dict[str, object]]
    ) -> tuple[tuple[PaymentEvent, ...], int]:
        return self._apply_event_mutation(
            events,
            audit,
            probability_name="schema_mismatch",
            stream_name="schema-mismatches",
            fault="schema_mismatch",
            expected_rule="schema_version is registered",
            mutation=lambda event: (
                {"schema_version": f"unregistered-{event.schema_version}-{event.event_id}"},
                {"schema_version": f"unregistered-{event.schema_version}-{event.event_id}"},
            ),
        )

    def _extreme_values(
        self, events: tuple[PaymentEvent, ...], audit: list[dict[str, object]]
    ) -> tuple[tuple[PaymentEvent, ...], int]:
        return self._apply_event_mutation(
            events,
            audit,
            probability_name="extreme_value",
            stream_name="extreme-values",
            fault="extreme_value",
            expected_rule="amount is within configured business bounds",
            mutation=lambda _event: ({"amount": 1.0e15}, {"amount": 1.0e15}),
        )

    def _encoding_errors(
        self,
        events: tuple[PaymentEvent, ...],
        audit: list[dict[str, object]],
        raw_faults: list[dict[str, object]],
    ) -> tuple[tuple[PaymentEvent, ...], int]:
        probability = self.quality.probability("encoding_error")
        rng = create_stream_rng(self.config.simulation.seed, "milestone-8:encoding-errors")
        result: list[PaymentEvent] = []
        count = 0
        for event in events:
            if rng.random() >= probability:
                result.append(event)
                continue
            payload = json.dumps(
                event.model_dump(mode="python", warnings=False), sort_keys=True, default=str
            ).encode("utf-8")
            raw_faults.append(
                {
                    "fault": "encoding_error",
                    "target": event.event_id,
                    "encoding": "invalid-utf8",
                    "payload_base64": base64.b64encode(payload + b"\xff").decode("ascii"),
                }
            )
            audit.append(
                {
                    "fault": "encoding_error",
                    "target": event.event_id,
                    "requested_probability": probability,
                    "mutation": "raw payload contains invalid UTF-8 byte",
                    "logical_identity_preserved": True,
                    "affected_boundary": "raw_fault_artifact",
                    "expected_validation_rule": "payload decodes as UTF-8",
                }
            )
            result.append(event)
            count += 1
        return tuple(result), count

    def _partition_skew(
        self, events: tuple[PaymentEvent, ...], audit: list[dict[str, object]]
    ) -> tuple[tuple[PaymentEvent, ...], int]:
        probability = self.quality.probability("partition_skew")
        if probability == 0:
            return events, 0
        rng = create_stream_rng(self.config.simulation.seed, "milestone-8:partition-skews")
        selected: set[str] = set()
        for payment_id in dict.fromkeys(event.payment_id for event in events):
            if rng.random() < probability:
                selected.add(payment_id)
        result: list[PaymentEvent] = []
        for event in events:
            partition = int(sha256_json(event.payment_id)[:8], 16) % 16
            if event.payment_id in selected:
                partition = 0
            result.append(event.model_copy(update={"transport_partition": partition}))
        for payment_id in sorted(selected):
            audit.append(
                {
                    "fault": "partition_skew",
                    "target": payment_id,
                    "requested_probability": probability,
                    "mutation": {"transport_partition": 0},
                    "logical_identity_preserved": True,
                    "affected_boundary": "transport_partition",
                    "expected_validation_rule": (
                        "partition distribution remains within configured skew"
                    ),
                }
            )
        return tuple(result), len(selected)

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
        """Align ledger posting order and running balances with delivery order.

        An out-of-order fault changes event processing timestamps.  Reusing the
        old ``balance_after`` values after that change makes an otherwise valid
        ledger fail reconciliation when entries are sorted by ``posted_at``.
        """

        if not ledger_entries:
            return ()
        processed_by_id = {event.event_id: event.processed_at for event in events}
        aligned = tuple(
            entry.model_copy(update={"posted_at": processed_by_id[entry.event_id]})
            if entry.event_id in processed_by_id
            else entry
            for entry in ledger_entries
        )
        opening: dict[str, float] = {}
        for entry in sorted(
            ledger_entries,
            key=lambda item: (item.posted_at, item.event_id, item.account_id, item.entry_type),
        ):
            if entry.account_id not in opening:
                delta = entry.amount if entry.entry_type == "CREDIT" else -entry.amount
                opening[entry.account_id] = round(entry.balance_after - delta, 2)
        balances = dict(opening)
        reconciled: list[LedgerEntry] = []
        for entry in sorted(
            aligned,
            key=lambda item: (item.posted_at, item.event_id, item.account_id, item.entry_type),
        ):
            delta = entry.amount if entry.entry_type == "CREDIT" else -entry.amount
            balance_after = round(balances[entry.account_id] + delta, 2)
            balances[entry.account_id] = balance_after
            reconciled.append(entry.model_copy(update={"balance_after": balance_after}))
        return tuple(reconciled)

    @staticmethod
    def _align_workflow_timestamps(
        dataset: "BehaviorDataset",
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
                    "label_available_at": (
                        case.label_available_at + shift(case.fraud_record_id)
                        if case.label_available_at is not None
                        else None
                    ),
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
            "invalid_enums": event_count,
            "invalid_references": event_count,
            "negative_amounts": payment_count + event_count,
            "corrupted_timestamps": event_count,
            "timezone_errors": event_count,
            "schema_mismatches": event_count,
            "extreme_values": event_count,
            "encoding_errors": event_count,
            "partition_skews": traffic_group_count,
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
            "invalid_enums": self.quality.probability("invalid_enum"),
            "invalid_references": self.quality.probability("invalid_reference"),
            "negative_amounts": self.quality.probability("negative_amount"),
            "corrupted_timestamps": self.quality.probability("corrupted_timestamp"),
            "timezone_errors": self.quality.probability("timezone_error"),
            "schema_mismatches": self.quality.probability("schema_mismatch"),
            "extreme_values": self.quality.probability("extreme_value"),
            "encoding_errors": self.quality.probability("encoding_error"),
            "partition_skews": self.quality.probability("partition_skew"),
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
        if "outage_events" in counts:
            rates["outage_events_realized"] = counts["outage_events"] / max(1, event_count)
        if "schema_changes" in counts:
            rates["schema_changes_realized"] = counts["schema_changes"] / max(1, event_count)
        return rates
