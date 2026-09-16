"""Deterministic historical replay over one already-generated run."""

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Literal

import polars as pl

from fraudtwin import __version__
from fraudtwin.manifest import ReplayManifest
from fraudtwin.ml.dataset import load_generated_run
from fraudtwin.reproducibility import as_utc, sha256_json
from fraudtwin.scale import iter_partition_table
from fraudtwin.simulation.behavior import BehaviorDataset
from fraudtwin.simulation.generator import EntityDataset
from fraudtwin.simulation.parquet import (
    BEHAVIOR_SCHEMAS,
    ENTITY_SCHEMAS,
    write_behavior_parquet,
    write_entity_parquet,
    write_graph_truth,
)

ReplayOrder = Literal["event_time_order", "original_delivery"]

_ENTITY_ID_FIELDS = {
    "customers": "customer_id",
    "institutions": "institution_id",
    "accounts": "account_id",
    "cards": "card_id",
    "merchants": "merchant_id",
    "devices": "device_id",
    "pix_keys": "pix_key_id",
    "network_endpoints": "endpoint_id",
}
_WORKFLOW_ENTITY_ID_FIELDS = {
    "customers": "customer_id",
    "accounts": "account_id",
    "cards": "card_id",
    "merchants": "merchant_id",
    "devices": "device_id",
}

REPLAY_EVENT_SCHEMA: dict[str, Any] = {
    "replay_sequence": pl.Int64,
    "source_table": pl.Utf8,
    "source_row_number": pl.Int64,
    "event_id": pl.Utf8,
    "event_type": pl.Utf8,
    "payment_id": pl.Utf8,
    "customer_id": pl.Utf8,
    "event_time": pl.Datetime(time_zone="UTC"),
    "source_created_at": pl.Datetime(time_zone="UTC"),
    "source_available_at": pl.Datetime(time_zone="UTC"),
    "ingested_at": pl.Datetime(time_zone="UTC"),
    "processed_at": pl.Datetime(time_zone="UTC"),
    "correlation_id": pl.Utf8,
    "causation_id": pl.Utf8,
    "scenario_id": pl.Utf8,
}


def _utc(value: datetime) -> datetime:
    return as_utc(value, error_message="replay timestamps must include a timezone")


def _in_period(value: datetime, start: datetime, end: datetime) -> bool:
    return start <= _utc(value) < end


@dataclass(frozen=True)
class ReplayResult:
    """Selected source records, ordered envelopes, and replay manifest."""

    entities: EntityDataset
    behavior: BehaviorDataset
    envelopes: tuple[dict[str, Any], ...]
    manifest: ReplayManifest

    @property
    def count(self) -> int:
        return len(self.envelopes)

    @property
    def frame(self) -> pl.DataFrame:
        return pl.DataFrame(list(self.envelopes), schema=REPLAY_EVENT_SCHEMA, orient="row")


def _envelope(event: Any, source_table: str, source_row_number: int) -> dict[str, Any]:
    return {
        "source_table": source_table,
        "source_row_number": source_row_number,
        "event_id": event.event_id,
        "event_type": event.event_type,
        "payment_id": event.payment_id,
        "customer_id": event.customer_id,
        "event_time": event.event_time,
        "source_created_at": event.source_created_at,
        "source_available_at": event.source_available_at,
        "ingested_at": event.ingested_at,
        "processed_at": event.processed_at,
        "correlation_id": event.correlation_id,
        "causation_id": event.causation_id,
        "scenario_id": event.scenario_id,
    }


def _restore_latent_truth(
    records: tuple[Any, ...], truth_by_record: dict[str, bool]
) -> tuple[Any, ...]:
    """Restore truth masked in source workflow records before replay export."""

    return tuple(
        record.model_copy(update={"fraud_truth": truth_by_record.get(record.fraud_record_id)})
        for record in records
    )


def _schema_description() -> dict[str, object]:
    """Describe every typed replay table, not only the ordered envelope."""

    return {
        "entities": {
            name: {column: str(dtype) for column, dtype in schema.items()}
            for name, schema in ENTITY_SCHEMAS.items()
        },
        "behavior": {
            name: {column: str(dtype) for column, dtype in schema.items()}
            for name, schema in BEHAVIOR_SCHEMAS.items()
        },
        "replay_events": {name: str(dtype) for name, dtype in REPLAY_EVENT_SCHEMA.items()},
    }


def _delivery_columns_missing(run_dir: Path) -> bool:
    """Detect legacy sources that predate recorded delivery timestamps."""

    required = {"ingested_at", "processed_at"}
    paths = (
        run_dir / "payments" / "payment_events.parquet",
        run_dir / "fraud" / "customer_disputes.parquet",
    )
    return any(required - set(pl.read_parquet(path, n_rows=0).columns) for path in paths)


def _referenced_entities(source: EntityDataset, behavior: BehaviorDataset) -> EntityDataset:
    """Keep only entities referenced by the selected payment/workflow closure."""

    referenced_ids = {kind: set[str]() for kind in _ENTITY_ID_FIELDS}

    def add(value: str | None, kind: str) -> None:
        if value is not None:
            referenced_ids[kind].add(value)

    for payment in behavior.payments:
        add(payment.payer_account_id, "accounts")
        add(payment.payee_account_id, "accounts")
        add(payment.payer_institution_id, "institutions")
        add(payment.payee_institution_id, "institutions")
        add(payment.merchant_id, "merchants")
        add(payment.card_id, "cards")
        add(payment.payer_pix_key_id, "pix_keys")
        add(payment.payee_pix_key_id, "pix_keys")
    for event in behavior.payment_events:
        add(event.customer_id, "customers")
        add(event.account_id, "accounts")
        add(event.payee_account_id, "accounts")
        add(event.merchant_id, "merchants")
        add(event.card_id, "cards")
        add(event.device_id, "devices")
        add(event.ip_id, "network_endpoints")
    for entry in behavior.ledger_entries:
        add(entry.account_id, "accounts")

    workflow_records = (
        *behavior.fraud_records,
        *behavior.alerts,
        *behavior.fraud_cases,
        *behavior.case_confirmations,
        *behavior.customer_disputes,
        *behavior.fraud_labels,
    )
    entity_ids = {
        value: kind
        for kind, records in source.tables().items()
        for record in records
        for value in (getattr(record, _ENTITY_ID_FIELDS[kind], None),)
        if isinstance(value, str)
    }
    for record in workflow_records:
        for kind in ("customers", "accounts", "cards", "merchants", "devices"):
            add(getattr(record, _WORKFLOW_ENTITY_ID_FIELDS[kind], None), kind)
        for value in getattr(record, "affected_entity_ids", ()):
            entity_kind = entity_ids.get(value)
            if entity_kind is not None:
                referenced_ids[entity_kind].add(value)

    referenced_entity_ids = set().union(*referenced_ids.values())
    selected_cards = tuple(card for card in source.cards if card.card_id in referenced_ids["cards"])
    for card in selected_cards:
        add(card.account_id, "accounts")
        add(card.customer_id, "customers")
    selected_pix_keys = tuple(
        key for key in source.pix_keys if key.pix_key_id in referenced_ids["pix_keys"]
    )
    for key in selected_pix_keys:
        add(key.account_id, "accounts")
        add(key.customer_id, "customers")
        add(key.institution_id, "institutions")
    selected_accounts = tuple(
        account for account in source.accounts if account.account_id in referenced_ids["accounts"]
    )
    for account in selected_accounts:
        add(account.customer_id, "customers")
        add(account.institution_id, "institutions")
    for merchant in source.merchants:
        if merchant.merchant_id in referenced_ids["merchants"]:
            add(merchant.acquirer_id, "institutions")

    return EntityDataset(
        customers=tuple(
            item for item in source.customers if item.customer_id in referenced_ids["customers"]
        ),
        institutions=tuple(
            item
            for item in source.institutions
            if item.institution_id in referenced_ids["institutions"]
        ),
        accounts=selected_accounts,
        cards=selected_cards,
        merchants=tuple(
            item for item in source.merchants if item.merchant_id in referenced_ids["merchants"]
        ),
        devices=tuple(
            item for item in source.devices if item.device_id in referenced_ids["devices"]
        ),
        pix_keys=selected_pix_keys,
        network_endpoints=tuple(
            item
            for item in source.network_endpoints
            if item.endpoint_id in referenced_ids["network_endpoints"]
        ),
        state_history=tuple(
            item for item in source.state_history if item.entity_id in referenced_entity_ids
        ),
    )


def replay_run(
    run_dir: Path,
    from_time: datetime,
    to_time: datetime,
    *,
    order: ReplayOrder = "event_time_order",
) -> ReplayResult:
    """Select and order an existing run without regenerating any source data."""

    start = _utc(from_time)
    end = _utc(to_time)
    if end <= start:
        raise ValueError("replay to must be after replay from")
    delivery_fallback = _delivery_columns_missing(run_dir)
    entities, source, source_manifest = load_generated_run(
        run_dir, allow_missing_delivery=delivery_fallback
    )

    payment_roots = {
        event.payment_id
        for event in source.payment_events
        if _in_period(event.event_time, start, end)
    }
    payment_roots.update(
        payment.payment_id
        for payment in source.payments
        if _in_period(payment.initiated_at, start, end)
    )
    payments = tuple(payment for payment in source.payments if payment.payment_id in payment_roots)
    payment_ids = frozenset(payment.payment_id for payment in payments)
    events = tuple(event for event in source.payment_events if event.payment_id in payment_ids)
    fraud_records = tuple(
        record for record in source.fraud_records if record.payment_id in payment_ids
    )
    fraud_record_ids = frozenset(record.fraud_record_id for record in fraud_records)
    alerts = tuple(alert for alert in source.alerts if alert.fraud_record_id in fraud_record_ids)
    alert_ids = frozenset(alert.fraud_alert_id for alert in alerts)
    cases = tuple(case for case in source.fraud_cases if case.fraud_alert_id in alert_ids)
    case_ids = frozenset(case.fraud_case_id for case in cases)
    confirmations = tuple(
        item for item in source.case_confirmations if item.fraud_case_id in case_ids
    )
    disputes = tuple(item for item in source.customer_disputes if item.fraud_case_id in case_ids)
    labels = tuple(item for item in source.fraud_labels if item.fraud_case_id in case_ids)
    ledger_entries = tuple(item for item in source.ledger_entries if item.payment_id in payment_ids)
    truth_by_record = {
        record.fraud_record_id: record.fraud_truth for record in source.fraud_records
    }
    selected_source = BehaviorDataset(
        profiles=source.profiles,
        payments=payments,
        payment_events=events,
        ledger_entries=ledger_entries,
        fraud_records=fraud_records,
        alerts=alerts,
        fraud_cases=cases,
        case_confirmations=confirmations,
        customer_disputes=disputes,
        fraud_labels=labels,
        graph_memberships=tuple(
            item
            for item in source.graph_memberships
            if not item.payment_id or item.payment_id in payment_ids
        ),
        graph_campaigns=source.graph_campaigns,
        graph_patterns=tuple(
            item
            for item in source.graph_patterns
            if not item.payment_ids or set(item.payment_ids) & payment_ids
        ),
        graph_evidence=tuple(
            item
            for item in source.graph_evidence
            if not item.payment_id or item.payment_id in payment_ids
        ),
        graph_hyperedges=source.graph_hyperedges,
        graph_hyperedge_memberships=source.graph_hyperedge_memberships,
    )
    selected_entities = _referenced_entities(entities, selected_source)
    selected_customer_ids = {item.customer_id for item in selected_entities.customers}
    behavior = BehaviorDataset(
        profiles=tuple(
            profile for profile in source.profiles if profile.customer_id in selected_customer_ids
        ),
        payments=payments,
        payment_events=events,
        ledger_entries=ledger_entries,
        fraud_records=fraud_records,
        alerts=alerts,
        fraud_cases=_restore_latent_truth(cases, truth_by_record),
        case_confirmations=_restore_latent_truth(confirmations, truth_by_record),
        customer_disputes=disputes,
        fraud_labels=_restore_latent_truth(labels, truth_by_record),
        graph_memberships=selected_source.graph_memberships,
        graph_campaigns=selected_source.graph_campaigns,
        graph_patterns=selected_source.graph_patterns,
        graph_evidence=selected_source.graph_evidence,
        graph_hyperedges=selected_source.graph_hyperedges,
        graph_hyperedge_memberships=selected_source.graph_hyperedge_memberships,
    )

    candidates = [
        _envelope(event, "payment_events", row_number)
        for row_number, event in enumerate(source.payment_events)
        if event.payment_id in payment_ids and _in_period(event.event_time, start, end)
    ]
    candidates.extend(
        _envelope(event, "customer_disputes", row_number)
        for row_number, event in enumerate(source.customer_disputes)
        if event.payment_id in payment_ids and _in_period(event.event_time, start, end)
    )
    delivery_fallback = delivery_fallback or any(
        row["ingested_at"] is None or row["processed_at"] is None for row in candidates
    )
    if order == "event_time_order":
        candidates.sort(
            key=lambda row: (
                row["event_time"],
                row["source_created_at"],
                row["event_id"],
                row["source_table"],
                row["source_row_number"],
            )
        )
    elif order == "original_delivery":
        if delivery_fallback:
            candidates.sort(
                key=lambda row: (
                    row["source_available_at"],
                    row["event_time"],
                    row["source_table"],
                    row["source_row_number"],
                    row["event_id"],
                )
            )
        else:
            candidates.sort(
                key=lambda row: (
                    row["ingested_at"],
                    row["processed_at"],
                    row["source_available_at"],
                    row["source_table"],
                    row["source_row_number"],
                    row["event_id"],
                )
            )
    else:
        raise ValueError(f"unsupported replay order: {order}")
    envelopes = tuple(
        {**row, "replay_sequence": sequence} for sequence, row in enumerate(candidates, start=1)
    )
    source_hash = sha256_json(source_manifest.model_dump(mode="json"))
    parameters: dict[str, object] = {
        "from": start.isoformat(),
        "to": end.isoformat(),
        "order": order,
        "source_run_id": source_manifest.run_id,
    }
    schema_fingerprint = sha256_json(_schema_description())
    output_fingerprint = sha256_json({
        "schema": schema_fingerprint,
        "envelopes": envelopes,
        "entities": {
            name: [item.model_dump(mode="json") for item in records]
            for name, records in selected_entities.all_tables().items()
        },
        "records": {
            name: [item.model_dump(mode="json") for item in records]
            for name, records in behavior.tables().items()
        },
    })
    replay_id = "RPL-" + sha256_json({"source": source_hash, "parameters": parameters})[:16]
    manifest = ReplayManifest(
        replay_id=replay_id,
        replay_version=__version__,
        source_run_id=source_manifest.run_id,
        source_manifest_hash=source_hash,
        parameters=parameters,
        source_run_information=source_manifest.model_dump(mode="json"),
        row_counts={**behavior.counts, "replay_events": len(envelopes)},
        schema_versions={"replay_events": "1", **source_manifest.schema_versions},
        regime_definitions=source_manifest.regime_definitions,
        ordering={
            "mode": order,
            "tie_breaker": (
                "event_time, source_created_at, event_id, source_table, source_row_number"
            )
            if order == "event_time_order"
            else (
                "ingested_at, processed_at, source_available_at, source_table, "
                "source_row_number, event_id"
            ),
            "delivery_fallback": delivery_fallback,
        },
        closure={
            "entity_tables_copied": True,
            "time_window_primary_events": len(candidates),
            "payment_and_workflow_closure": True,
        },
        schema_fingerprint=schema_fingerprint,
        output_fingerprint=output_fingerprint,
    )
    return ReplayResult(selected_entities, behavior, envelopes, manifest)


def iter_partition_replay_events(
    run_dir: Path,
    from_time: datetime,
    to_time: datetime,
    *,
    order: Literal["original_delivery"] = "original_delivery",
) -> Iterator[dict[str, Any]]:
    """Stream replay envelopes directly from M18 partitioned Parquet."""

    if order != "original_delivery":
        raise ValueError("partition replay supports order='original_delivery' only")
    start, end = _utc(from_time), _utc(to_time)
    if end <= start:
        raise ValueError("replay to must be after replay from")
    sequence = 0
    for table_name in ("payment_events", "customer_disputes"):
        for row_number, row in enumerate(iter_partition_table(run_dir, table_name)):
            event_time = row.get("event_time")
            if event_time is None:
                continue
            try:
                timestamp = _utc(event_time)
            except (TypeError, ValueError):
                continue
            if not (start <= timestamp < end):
                continue
            sequence += 1
            yield {
                "replay_sequence": sequence,
                "source_table": table_name,
                "source_row_number": row_number,
                "event_id": row.get("event_id"),
                "event_type": row.get("event_type"),
                "payment_id": row.get("payment_id"),
                "customer_id": row.get("customer_id"),
                "event_time": timestamp,
                "source_created_at": row.get("source_created_at"),
                "source_available_at": row.get("source_available_at"),
                "ingested_at": row.get("ingested_at"),
                "processed_at": row.get("processed_at"),
                "correlation_id": row.get("correlation_id"),
                "causation_id": row.get("causation_id"),
                "scenario_id": row.get("scenario_id"),
            }


def write_replay(result: ReplayResult, output_dir: Path) -> tuple[Path, Path]:
    """Write a replay artifact below the supplied destination root."""

    replay_dir = output_dir / result.manifest.replay_id
    replay_dir.mkdir(parents=True, exist_ok=False)
    write_entity_parquet(result.entities, replay_dir)
    write_behavior_parquet(result.behavior, replay_dir, mask_fraud_truth=False)
    write_graph_truth(
        result.behavior.graph_memberships,
        result.behavior.graph_patterns,
        replay_dir,
        campaigns=result.behavior.graph_campaigns,
        evidence=result.behavior.graph_evidence,
        hyperedges=result.behavior.graph_hyperedges,
        hyperedge_memberships=result.behavior.graph_hyperedge_memberships,
    )
    envelope_path = replay_dir / "replay_events.parquet"
    result.frame.write_parquet(envelope_path)
    manifest_path = replay_dir / "replay_manifest.json"
    manifest_path.write_text(result.manifest.model_dump_json(indent=2) + "\n", encoding="utf-8")
    return envelope_path, manifest_path


__all__ = [
    "REPLAY_EVENT_SCHEMA",
    "ReplayOrder",
    "ReplayResult",
    "iter_partition_replay_events",
    "replay_run",
    "write_replay",
]
