"""Deterministic point-in-time training data construction for Milestone 9.

The builder works on the existing in-memory M1-M8 records or on one generated
run.  It deliberately does not regenerate simulation state and does not add a
feature-store or model-serving dependency.
"""

import hashlib
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, TypeVar

import polars as pl
from pydantic import BaseModel

from fraudtwin import __version__
from fraudtwin.campaign_dynamics import DynamicCampaignDataset
from fraudtwin.config import SimulationRunConfig, _default_feature_windows, config_hash
from fraudtwin.domain import (
    Account,
    BehaviorProfile,
    CampaignActorMembershipChange,
    CampaignIntensityDecision,
    CampaignLineage,
    CampaignPhaseChange,
    CampaignSourceSnapshot,
    CampaignStateSnapshot,
    CampaignTopologyMutation,
    CampaignTransition,
    Card,
    CaseReopening,
    Customer,
    CustomerDispute,
    DelayedFraudLabel,
    Device,
    EntityStateChange,
    FinalObservedLabel,
    FraudAlert,
    FraudCase,
    FraudCaseConfirmation,
    FraudRecord,
    GraphCampaign,
    GraphCampaignMembership,
    GraphEvidence,
    GraphHyperedge,
    GraphHyperedgeMembership,
    GraphPattern,
    Institution,
    LabelCorrection,
    LabelObservation,
    LabelVersion,
    LedgerEntry,
    Merchant,
    NetworkEndpoint,
    ObservationProvenance,
    Payment,
    PaymentEvent,
    PixKey,
    validate_fraud_workflow,
)
from fraudtwin.label_observation import validate_label_observation, visible_label_at
from fraudtwin.manifest import DatasetManifest, RunManifest
from fraudtwin.reproducibility import as_utc, canonical_json, sha256_json
from fraudtwin.scale import iter_partition_table
from fraudtwin.simulation.behavior import BehaviorDataset
from fraudtwin.simulation.generator import EntityDataset
from fraudtwin.simulation.graph_fraud import GraphFraudDataset

PIT_DATASET_SCHEMA: dict[str, Any] = {
    "dataset_row_id": pl.Utf8,
    "payment_id": pl.Utf8,
    "event_id": pl.Utf8,
    "customer_id": pl.Utf8,
    "account_id": pl.Utf8,
    "prediction_time": pl.Datetime(time_zone="UTC"),
    "business_event_time": pl.Datetime(time_zone="UTC"),
    "event_time": pl.Datetime(time_zone="UTC"),
    "source_available_at": pl.Datetime(time_zone="UTC"),
    "feature_available_at": pl.Datetime(time_zone="UTC"),
    "label_available_at": pl.Datetime(time_zone="UTC"),
    "label": pl.Utf8,
    "fraud_truth": pl.Boolean,
    "amount": pl.Float64,
    "payment_rail": pl.Utf8,
    "payment_type": pl.Utf8,
    "merchant_id": pl.Utf8,
    "card_id": pl.Utf8,
    "device_id": pl.Utf8,
    "online": pl.Boolean,
    "transaction_count_1m": pl.Int64,
    "transaction_count_5m": pl.Int64,
    "transaction_count_1h": pl.Int64,
    "transaction_count_24h": pl.Int64,
    "transaction_count_7d": pl.Int64,
    "transaction_count_30d": pl.Int64,
    "transaction_amount_1h": pl.Float64,
    "transaction_amount_24h": pl.Float64,
    "transaction_amount_7d": pl.Float64,
    "avg_transaction_amount_30d": pl.Float64,
    "max_transaction_amount_7d": pl.Float64,
    "amount_vs_customer_avg": pl.Float64,
    "distinct_merchants_1d": pl.Int64,
    "distinct_merchants_30d": pl.Int64,
    "new_merchant_flag": pl.Boolean,
    "merchant_fraud_rate_historical": pl.Float64,
    "distinct_countries_24h": pl.Int64,
    "new_country_flag": pl.Boolean,
    "device_age_days": pl.Float64,
    "new_device_flag": pl.Boolean,
    "trusted_device_flag": pl.Boolean,
    "device_customer_count_30d": pl.Int64,
    "customers_per_device_24h": pl.Int64,
    "account_age_days": pl.Float64,
    "balance": pl.Float64,
    "available_balance": pl.Float64,
    "credit_limit": pl.Float64,
    "credit_utilization": pl.Float64,
    "days_since_last_payment": pl.Float64,
    "confirmed_fraud_count_90d": pl.Int64,
    "fraud_loss_365d": pl.Float64,
    "days_since_last_confirmed_fraud": pl.Float64,
    "split": pl.Utf8,
}

_INITIAL_EVENT_TYPES = {
    "CARD_PAYMENT_INITIATED",
    "CARD_AUTHORIZATION_REQUESTED",
    "PIX_INITIATED",
    "TRANSFER_COMPLETED",
}

_DATASET_METADATA_COLUMNS = frozenset(
    {
        "dataset_row_id",
        "payment_id",
        "event_id",
        "customer_id",
        "account_id",
        "prediction_time",
        "business_event_time",
        "event_time",
        "source_available_at",
        "feature_available_at",
        "label_available_at",
        "label",
        "fraud_truth",
        "split",
    }
)
_FEATURE_COLUMNS = tuple(
    name for name in PIT_DATASET_SCHEMA if name not in _DATASET_METADATA_COLUMNS
)
_EVENT_WINDOW_FEATURES = (
    ("one_minute", "transaction_count_1m"),
    ("five_minutes", "transaction_count_5m"),
    ("one_hour", "transaction_count_1h"),
    ("day", "transaction_count_24h"),
    ("week", "transaction_count_7d"),
    ("month", "transaction_count_30d"),
    ("amount_hour", "transaction_amount_1h"),
    ("amount_day", "transaction_amount_24h"),
    ("amount_week", "transaction_amount_7d"),
    ("amount_month", "avg_transaction_amount_30d"),
    ("merchant_day", "distinct_merchants_1d"),
    ("merchant_month", "distinct_merchants_30d"),
    ("country_day", "distinct_countries_24h"),
)

T = TypeVar("T")


@dataclass(frozen=True)
class PointInTimeDataset:
    """Stable rows and the manifest that describes their construction."""

    rows: tuple[dict[str, Any], ...]
    manifest: DatasetManifest

    @property
    def count(self) -> int:
        return len(self.rows)

    @property
    def counts(self) -> dict[str, int]:
        return self.manifest.row_counts

    @property
    def frame(self) -> pl.DataFrame:
        """Return the dataset as a frame with the stable output schema."""

        return pl.DataFrame(list(self.rows), schema=PIT_DATASET_SCHEMA, orient="row")


def _utc(value: datetime) -> datetime:
    return as_utc(value, error_message="dataset timestamps must include a timezone")


def _record_key(record: Any) -> str:
    return canonical_json(record.model_dump(mode="json"))


def _schema_fingerprint() -> str:
    schema = {"columns": [[name, str(dtype)] for name, dtype in PIT_DATASET_SCHEMA.items()]}
    return sha256_json(schema)


def _in_window(reference_time: datetime, candidate_time: datetime, window_seconds: int) -> bool:
    """Return whether a prior business event falls in a feature window."""

    delta = reference_time - candidate_time
    return timedelta(0) < delta <= timedelta(seconds=window_seconds)


def _deduplicate(records: Iterable[T], identifier: str) -> tuple[T, ...]:
    """Remove exact M8 duplicate rows and reject conflicting identities."""

    by_id: dict[str, T] = {}
    keys: dict[str, str] = {}
    for record in records:
        record_id = str(getattr(record, identifier))
        key = _record_key(record)
        if record_id in by_id:
            if keys[record_id] != key:
                raise ValueError(f"conflicting duplicate {identifier}: {record_id}")
            continue
        by_id[record_id] = record
        keys[record_id] = key
    return tuple(by_id.values())


def _merge_records(
    existing: tuple[T, ...],
    additions: tuple[T, ...],
    *,
    identifier: str | None = None,
    prefer_additions: bool = False,
) -> tuple[T, ...]:
    """Merge sidecar records in stable order, optionally overlaying updates."""

    if prefer_additions and additions:
        if identifier is None:
            return _merge_records(existing, additions)
        unique_additions = _deduplicate(additions, identifier)
        additions_by_id = {str(getattr(record, identifier)): record for record in unique_additions}
        overlay: list[T] = []
        existing_ids: set[str] = set()
        for record in existing:
            record_id = str(getattr(record, identifier))
            existing_ids.add(record_id)
            overlay.append(additions_by_id.pop(record_id, record))
        overlay.extend(
            record
            for record in unique_additions
            if str(getattr(record, identifier)) not in existing_ids
        )
        return tuple(overlay)
    if identifier is not None:
        return _deduplicate((*existing, *additions), identifier)
    seen: set[str] = set()
    merged: list[T] = []
    for record in (*existing, *additions):
        key = _record_key(record)
        if key not in seen:
            seen.add(key)
            merged.append(record)
    return tuple(merged)


def _read_models(
    path: Path,
    model: type[T],
    *,
    fallback_delivery: bool = False,
) -> tuple[T, ...]:
    try:
        rows = pl.read_parquet(path).to_dicts()
    except (FileNotFoundError, OSError) as exc:
        raise ValueError(f"required generated source is missing: {path}") from exc
    try:
        if fallback_delivery:
            for row in rows:
                if "source_available_at" in row:
                    row.setdefault("ingested_at", row["source_available_at"])
                    row.setdefault("processed_at", row["ingested_at"])
        return tuple(model.model_validate(row) for row in rows)  # type: ignore[attr-defined]
    except ValueError as exc:
        raise ValueError(f"invalid generated source {path}: {exc}") from exc


def _read_run_table(
    run_dir: Path,
    group: str,
    table: str,
    model: type[T],
    *,
    fallback_delivery: bool = False,
) -> tuple[T, ...]:
    path = run_dir / group / f"{table}.parquet"
    if path.is_file():
        return _read_models(path, model, fallback_delivery=fallback_delivery)
    # Scale runs store table-qualified chunks instead of consolidated files.
    # Keep this compatibility loader chunk-aware; callers needing truly
    # out-of-core processing should consume ``iter_partition_table`` directly.
    scale_rows = []
    for row in iter_partition_table(run_dir, table):
        values = {
            key: value
            for key, value in row.items()
            if key
            not in {"logical_id", "logical_type", "source_id", "partition_key", "partition_id"}
        }
        if fallback_delivery and "source_available_at" in values:
            values.setdefault("ingested_at", values["source_available_at"])
            values.setdefault("processed_at", values["ingested_at"])
        scale_rows.append(model.model_validate(values))  # type: ignore[attr-defined]
    if scale_rows:
        return tuple(scale_rows)
    return _read_models(path, model, fallback_delivery=fallback_delivery)


def _read_optional_models(path: Path, model: type[T]) -> tuple[T, ...]:
    """Read an optional sidecar table when it is present."""

    return _read_models(path, model) if path.is_file() else ()


def _validate_behavior_workflow(entities: EntityDataset, behavior: BehaviorDataset) -> None:
    """Validate the complete M7 workflow against its M1-M6 source records."""

    entity_ids = entities.reference_ids
    validate_fraud_workflow(
        behavior.alerts,
        behavior.fraud_cases,
        behavior.case_confirmations,
        behavior.customer_disputes,
        behavior.fraud_labels,
        customer_ids=entity_ids["customers"],
        account_ids=entity_ids["accounts"],
        card_ids=entity_ids["cards"],
        device_ids=entity_ids["devices"],
        merchant_ids=entity_ids["merchants"],
        payment_ids=frozenset(payment.payment_id for payment in behavior.payments),
        event_ids=frozenset(event.event_id for event in behavior.payment_events),
        fraud_record_ids=frozenset(record.fraud_record_id for record in behavior.fraud_records),
        all_entity_ids=entities.all_ids,
        fraud_truth_by_record={
            record.fraud_record_id: record.fraud_truth for record in behavior.fraud_records
        },
        fraud_records_by_id={record.fraud_record_id: record for record in behavior.fraud_records},
    )


def _read_label_observations(run_dir: Path, manifest: RunManifest) -> tuple[LabelObservation, ...]:
    metadata = manifest.label_observation
    if not metadata or not metadata.get("root"):
        return ()
    root = run_dir / str(metadata["root"])
    history_path = root / "oracle" / "label_history.parquet"
    if not history_path.is_file():
        raise ValueError(f"label observation history is missing: {history_path}")
    versions = tuple(_read_models(history_path, LabelVersion))
    grouped: dict[str, list[LabelVersion]] = {}
    for version in versions:
        grouped.setdefault(version.fraud_record_id, []).append(version)
    policy_hash = str(metadata.get("configuration_hash", ""))
    raw_stream_ids = metadata.get("stream_ids", [])
    if not isinstance(raw_stream_ids, list):
        raise ValueError("label observation stream_ids must be a list")
    stream_ids = tuple(str(item) for item in raw_stream_ids)
    corrections_path = root / "oracle" / "label_corrections.parquet"
    corrections = _read_optional_models(corrections_path, LabelCorrection)
    corrections_by_observation: dict[str, list[LabelCorrection]] = {}
    for correction in corrections:
        corrections_by_observation.setdefault(correction.observation_id, []).append(correction)
    reopenings_path = root / "oracle" / "case_reopenings.parquet"
    reopenings = _read_optional_models(reopenings_path, CaseReopening)
    reopenings_by_observation: dict[str, list[CaseReopening]] = {}
    for reopening in reopenings:
        reopenings_by_observation.setdefault(reopening.observation_id, []).append(reopening)
    provenance_path = root / "oracle" / "observation_provenance.parquet"
    provenance_by_observation: dict[str, ObservationProvenance] = {}
    if provenance_path.is_file():
        try:
            provenance_rows = pl.read_parquet(provenance_path).to_dicts()
            for row in provenance_rows:
                observation_id = str(row.pop("observation_id"))
                if observation_id in provenance_by_observation:
                    raise ValueError(f"duplicate observation provenance: {observation_id}")
                provenance_by_observation[observation_id] = ObservationProvenance.model_validate(
                    row
                )
        except (FileNotFoundError, OSError, KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"invalid generated source {provenance_path}: {exc}") from exc
    observation_ids = {f"OBS-{fraud_record_id}" for fraud_record_id in grouped}
    unknown_artifact_ids = (
        set(corrections_by_observation)
        | set(reopenings_by_observation)
        | set(provenance_by_observation)
    ) - observation_ids
    if unknown_artifact_ids:
        raise ValueError(
            "label observation artifact references unknown observations: "
            + ", ".join(sorted(unknown_artifact_ids))
        )
    observations = tuple(
        LabelObservation(
            observation_id=f"OBS-{fraud_record_id}",
            fraud_record_id=fraud_record_id,
            payment_id=items[0].payment_id,
            truth_label=items[0].truth_label,
            investigation_selected=items[0].investigation_selected,
            versions=tuple(sorted(items, key=lambda item: item.label_version)),
            final_label_version=max(item.label_version for item in items),
            policy_hash=policy_hash,
            stream_ids=(
                provenance_by_observation[f"OBS-{fraud_record_id}"].stream_ids
                if f"OBS-{fraud_record_id}" in provenance_by_observation
                else stream_ids
            ),
            simulation_run_id=items[0].simulation_run_id,
            corrections=tuple(corrections_by_observation.get(f"OBS-{fraud_record_id}", ())),
            reopenings=tuple(reopenings_by_observation.get(f"OBS-{fraud_record_id}", ())),
            provenance=provenance_by_observation.get(f"OBS-{fraud_record_id}"),
        )
        for fraud_record_id, items in sorted(grouped.items())
    )
    validate_label_observation(observations)
    return observations


def _read_final_observed_labels(
    run_dir: Path, manifest: RunManifest
) -> tuple[FinalObservedLabel, ...]:
    metadata = manifest.label_observation
    if not metadata or not metadata.get("root"):
        return ()
    path = run_dir / str(metadata["root"]) / "observable" / "observed_labels.parquet"
    return tuple(_read_models(path, FinalObservedLabel)) if path.is_file() else ()


def load_generated_run(
    run_dir: Path,
    *,
    allow_missing_delivery: bool = False,
) -> tuple[EntityDataset, BehaviorDataset, RunManifest]:
    """Load one existing generated run without regenerating unrelated records."""

    try:
        manifest = RunManifest.model_validate_json(
            (run_dir / "manifest.json").read_text(encoding="utf-8")
        )
    except (FileNotFoundError, OSError, ValueError) as exc:
        raise ValueError(f"invalid generated run manifest in {run_dir}") from exc

    entities = EntityDataset(
        customers=_read_run_table(run_dir, "entities", "customers", Customer),
        institutions=_read_run_table(run_dir, "entities", "institutions", Institution),
        accounts=_read_run_table(run_dir, "entities", "accounts", Account),
        cards=_read_run_table(run_dir, "entities", "cards", Card),
        merchants=_read_run_table(run_dir, "entities", "merchants", Merchant),
        devices=_read_run_table(run_dir, "entities", "devices", Device),
        pix_keys=_read_run_table(run_dir, "entities", "pix_keys", PixKey),
        network_endpoints=(
            _read_run_table(run_dir, "entities", "network_endpoints", NetworkEndpoint)
            if (run_dir / "entities" / "network_endpoints.parquet").is_file()
            else ()
        ),
        state_history=(
            tuple(
                EntityStateChange.model_validate(row)
                for row in pl.read_parquet(
                    run_dir / "entities" / "state_history.parquet"
                ).to_dicts()
            )
            if (run_dir / "entities" / "state_history.parquet").is_file()
            else ()
        ),
    )
    behavior = BehaviorDataset(
        profiles=_read_run_table(run_dir, "behavior", "behavior_profiles", BehaviorProfile),
        payments=_read_run_table(run_dir, "payments", "payments", Payment),
        payment_events=_read_run_table(
            run_dir,
            "payments",
            "payment_events",
            PaymentEvent,
            fallback_delivery=allow_missing_delivery,
        ),
        ledger_entries=_read_run_table(run_dir, "ledger", "ledger_entries", LedgerEntry),
        fraud_records=_read_run_table(run_dir, "fraud", "fraud_records", FraudRecord),
        alerts=_read_run_table(run_dir, "fraud", "fraud_alerts", FraudAlert),
        fraud_cases=_read_run_table(run_dir, "fraud", "fraud_cases", FraudCase),
        case_confirmations=_read_run_table(
            run_dir, "fraud", "case_confirmations", FraudCaseConfirmation
        ),
        customer_disputes=_read_run_table(
            run_dir,
            "fraud",
            "customer_disputes",
            CustomerDispute,
            fallback_delivery=allow_missing_delivery,
        ),
        fraud_labels=_read_run_table(run_dir, "fraud", "fraud_labels", DelayedFraudLabel),
        label_observations=_read_label_observations(run_dir, manifest),
        final_observed_labels=_read_final_observed_labels(run_dir, manifest),
        graph_memberships=(
            _read_models(
                run_dir / "oracle" / "graph" / "campaign_memberships.parquet",
                GraphCampaignMembership,
            )
            if (run_dir / "oracle" / "graph" / "campaign_memberships.parquet").is_file()
            else ()
        ),
        graph_campaigns=(
            _read_models(run_dir / "oracle" / "graph" / "campaigns.parquet", GraphCampaign)
            if (run_dir / "oracle" / "graph" / "campaigns.parquet").is_file()
            else ()
        ),
        graph_patterns=(
            _read_models(run_dir / "oracle" / "graph" / "patterns.parquet", GraphPattern)
            if (run_dir / "oracle" / "graph" / "patterns.parquet").is_file()
            else ()
        ),
        graph_evidence=(
            _read_models(run_dir / "oracle" / "graph" / "graph_evidence.parquet", GraphEvidence)
            if (run_dir / "oracle" / "graph" / "graph_evidence.parquet").is_file()
            else ()
        ),
        graph_hyperedges=(
            _read_models(run_dir / "oracle" / "graph" / "hyperedges.parquet", GraphHyperedge)
            if (run_dir / "oracle" / "graph" / "hyperedges.parquet").is_file()
            else ()
        ),
        graph_hyperedge_memberships=(
            _read_models(
                run_dir / "oracle" / "graph" / "hyperedge_memberships.parquet",
                GraphHyperedgeMembership,
            )
            if (run_dir / "oracle" / "graph" / "hyperedge_memberships.parquet").is_file()
            else ()
        ),
    )
    dynamic_roots = (
        sorted((run_dir / "campaign_dynamics").glob("M15-*/oracle"))
        if (run_dir / "campaign_dynamics").is_dir()
        else []
    )
    if dynamic_roots:
        dynamic_oracle = dynamic_roots[-1]
        dynamic_observable = dynamic_oracle.parent / "observable"
        dynamic_payments = _read_optional_models(dynamic_observable / "payments.parquet", Payment)
        dynamic_events = _read_optional_models(
            dynamic_observable / "payment_events.parquet", PaymentEvent
        )
        dynamic_entries = _read_optional_models(
            dynamic_observable / "ledger_entries.parquet", LedgerEntry
        )
        dynamic_graph_dir = dynamic_oracle / "graph"
        dynamic_memberships = _read_optional_models(
            dynamic_graph_dir / "campaign_memberships.parquet", GraphCampaignMembership
        )
        dynamic_campaigns = _read_optional_models(
            dynamic_graph_dir / "campaigns.parquet", GraphCampaign
        )
        dynamic_patterns = _read_optional_models(
            dynamic_graph_dir / "patterns.parquet", GraphPattern
        )
        dynamic_evidence = _read_optional_models(
            dynamic_graph_dir / "graph_evidence.parquet", GraphEvidence
        )
        dynamic_hyperedges = _read_optional_models(
            dynamic_graph_dir / "hyperedges.parquet", GraphHyperedge
        )
        dynamic_hyperedge_memberships = _read_optional_models(
            dynamic_graph_dir / "hyperedge_memberships.parquet", GraphHyperedgeMembership
        )
        behavior = replace(
            behavior,
            payments=_merge_records(behavior.payments, dynamic_payments, identifier="payment_id"),
            payment_events=_merge_records(
                behavior.payment_events, dynamic_events, identifier="event_id"
            ),
            ledger_entries=_merge_records(
                behavior.ledger_entries, dynamic_entries, identifier="ledger_entry_id"
            ),
            graph_memberships=_merge_records(
                behavior.graph_memberships, dynamic_memberships, prefer_additions=True
            ),
            graph_campaigns=_merge_records(
                behavior.graph_campaigns,
                dynamic_campaigns,
                identifier="campaign_id",
                prefer_additions=True,
            ),
            graph_patterns=_merge_records(
                behavior.graph_patterns,
                dynamic_patterns,
                identifier="pattern_id",
                prefer_additions=True,
            ),
            graph_evidence=_merge_records(
                behavior.graph_evidence,
                dynamic_evidence,
                identifier="evidence_id",
                prefer_additions=True,
            ),
            graph_hyperedges=_merge_records(
                behavior.graph_hyperedges,
                dynamic_hyperedges,
                identifier="hyperedge_id",
                prefer_additions=True,
            ),
            graph_hyperedge_memberships=_merge_records(
                behavior.graph_hyperedge_memberships,
                dynamic_hyperedge_memberships,
                prefer_additions=True,
            ),
        )
        dynamic_graph = GraphFraudDataset(
            behavior.payments,
            behavior.payment_events,
            behavior.ledger_entries,
            behavior.fraud_records,
            behavior.graph_memberships,
            behavior.graph_patterns,
            behavior.graph_campaigns,
            behavior.graph_evidence,
            behavior.graph_hyperedges,
            behavior.graph_hyperedge_memberships,
        )
        dynamic = DynamicCampaignDataset(
            graph=dynamic_graph,
            snapshots=_read_models(dynamic_oracle / "snapshots.parquet", CampaignStateSnapshot),
            transitions=_read_models(dynamic_oracle / "transitions.parquet", CampaignTransition),
            phase_changes=_read_models(
                dynamic_oracle / "phase_changes.parquet", CampaignPhaseChange
            ),
            membership_changes=_read_models(
                dynamic_oracle / "membership_changes.parquet", CampaignActorMembershipChange
            ),
            intensity_decisions=_read_models(
                dynamic_oracle / "intensity_decisions.parquet", CampaignIntensityDecision
            ),
            topology_mutations=_read_models(
                dynamic_oracle / "topology_mutations.parquet", CampaignTopologyMutation
            ),
            lineage=_read_models(dynamic_oracle / "lineage.parquet", CampaignLineage),
            source_snapshots=_read_models(
                dynamic_oracle / "source_snapshots.parquet", CampaignSourceSnapshot
            ),
        )
        behavior = replace(behavior, campaign_dynamics=dynamic)
    if manifest.difficulty is not None:
        oracle_models: dict[str, tuple[type[BaseModel], str, str]] = {
            "behavior_profiles": (BehaviorProfile, "oracle/behavior", "behavior_profiles"),
            "payments": (Payment, "oracle/payments", "payments"),
            "payment_events": (PaymentEvent, "oracle/payments", "payment_events"),
            "ledger_entries": (LedgerEntry, "oracle/ledger", "ledger_entries"),
            "fraud_records": (FraudRecord, "oracle/fraud", "fraud_records"),
            "fraud_alerts": (FraudAlert, "oracle/fraud", "fraud_alerts"),
            "fraud_cases": (FraudCase, "oracle/fraud", "fraud_cases"),
            "case_confirmations": (FraudCaseConfirmation, "oracle/fraud", "case_confirmations"),
            "customer_disputes": (CustomerDispute, "oracle/fraud", "customer_disputes"),
            "fraud_labels": (DelayedFraudLabel, "oracle/fraud", "fraud_labels"),
        }
        oracle_tables: dict[str, tuple[BaseModel, ...]] = {}
        for name, (model, group, table) in oracle_models.items():
            path = run_dir / group / f"{table}.parquet"
            if path.is_file():
                oracle_tables[name] = _read_models(path, model)
        behavior = replace(behavior, oracle_tables=oracle_tables)
    _validate_behavior_workflow(entities, behavior)
    return entities, behavior, manifest


class PointInTimeDatasetBuilder:
    """Build historical features and labels using only available source data."""

    def __init__(
        self,
        config: SimulationRunConfig,
        entities: EntityDataset,
        behavior: BehaviorDataset,
        source_manifest: RunManifest | None = None,
    ) -> None:
        self.config = config
        self.settings = config.dataset
        self.entities = entities
        self.behavior = behavior
        self.source_manifest = source_manifest
        if not self.settings.enabled:
            raise ValueError("point-in-time dataset generation is disabled")
        self.start = _utc(self.settings.start or config.simulation.start)
        self.end = _utc(
            self.settings.end
            or config.simulation.start + timedelta(days=config.simulation.duration_days)
        )
        self.feature_windows: dict[str, int] = {
            **{str(name): seconds for name, seconds in _default_feature_windows().items()},
            **{str(name): seconds for name, seconds in self.settings.feature_windows.items()},
        }
        self.label_delay_seconds = config.effective_label_delay_seconds()
        if self.end <= self.start:
            raise ValueError("dataset end must be after dataset start")
        self.fraud_truth_by_record = {
            record.fraud_record_id: record.fraud_truth for record in behavior.fraud_records
        }
        self._validate_sources()
        self.payments = _deduplicate(behavior.payments, "payment_id")
        self.payments_by_id = {payment.payment_id: payment for payment in self.payments}
        self.events = _deduplicate(behavior.payment_events, "event_id")
        self.ledger_entries = _deduplicate(behavior.ledger_entries, "ledger_entry_id")
        self.labels = _deduplicate(behavior.fraud_labels, "label_id")
        self.events_by_payment = self._initial_events_by_payment()
        self.labels_by_payment = self._labels_by_payment()
        self.observations_by_payment = {
            item.payment_id: item for item in behavior.label_observations
        }
        if self.observations_by_payment:
            validate_label_observation(tuple(self.observations_by_payment.values()))
        self.accounts = {account.account_id: account for account in entities.accounts}
        self.cards = {card.card_id: card for card in entities.cards}
        self.merchants = {merchant.merchant_id: merchant for merchant in entities.merchants}
        self.devices = {device.device_id: device for device in entities.devices}

    def _validate_sources(self) -> None:
        _validate_behavior_workflow(self.entities, self.behavior)
        entity_ids = self.entities.reference_ids
        payment_ids = {payment.payment_id for payment in self.behavior.payments}
        for payment in self.behavior.payments:
            _utc(payment.initiated_at)
            if payment.payer_account_id not in entity_ids["accounts"]:
                raise ValueError(f"payment references unknown account {payment.payer_account_id}")
            if (
                payment.merchant_id is not None
                and payment.merchant_id not in entity_ids["merchants"]
            ):
                raise ValueError(f"payment references unknown merchant {payment.merchant_id}")
            if payment.card_id is not None and payment.card_id not in entity_ids["cards"]:
                raise ValueError(f"payment references unknown card {payment.card_id}")
        for account in self.entities.accounts:
            for timestamp in (
                account.opening_date,
                account.created_at,
                account.updated_at,
                account.valid_from,
                account.valid_to,
                account.system_from,
                account.system_to,
            ):
                if timestamp is not None:
                    _utc(timestamp)
            if account.valid_to is not None and account.valid_to <= account.valid_from:
                raise ValueError(f"account {account.account_id} has invalid valid-time bounds")
            if account.system_to is not None and account.system_to <= account.system_from:
                raise ValueError(f"account {account.account_id} has invalid system-time bounds")
        for merchant in self.entities.merchants:
            _utc(merchant.created_at)
        for device in self.entities.devices:
            _utc(device.first_seen_at)
            _utc(device.last_seen_at)
            if device.last_seen_at < device.first_seen_at:
                raise ValueError(f"device {device.device_id} has invalid first-seen bounds")
        for card in self.entities.cards:
            _utc(card.issued_at)
            _utc(card.expires_at)
            if card.expires_at <= card.issued_at:
                raise ValueError(f"card {card.card_id} has invalid issue bounds")
        for event in self.behavior.payment_events:
            for timestamp in (
                event.event_time,
                event.source_created_at,
                event.source_available_at,
                event.ingested_at,
                event.processed_at,
            ):
                _utc(timestamp)
            if event.event_time > event.source_created_at:
                raise ValueError(f"event {event.event_id} has invalid source_created_at")
            if event.source_created_at > event.source_available_at:
                raise ValueError(f"event {event.event_id} has invalid source availability")
            if (
                event.source_available_at > event.ingested_at
                or event.ingested_at > event.processed_at
            ):
                raise ValueError(f"event {event.event_id} has invalid delivery timestamps")
            if event.payment_id not in payment_ids:
                raise ValueError(f"event {event.event_id} references unknown payment")
            if event.customer_id not in entity_ids["customers"]:
                raise ValueError(f"event {event.event_id} references unknown customer")
            if event.account_id not in entity_ids["accounts"]:
                raise ValueError(f"event {event.event_id} references unknown account")
            if event.merchant_id is not None and event.merchant_id not in entity_ids["merchants"]:
                raise ValueError(f"event {event.event_id} references unknown merchant")
            if event.card_id is not None and event.card_id not in entity_ids["cards"]:
                raise ValueError(f"event {event.event_id} references unknown card")
            if event.device_id is not None and event.device_id not in entity_ids["devices"]:
                raise ValueError(f"event {event.event_id} references unknown device")
        for label in self.behavior.fraud_labels:
            _utc(label.fraud_occurred_at)
            _utc(label.label_available_at)
            if label.fraud_confirmed_at is not None:
                _utc(label.fraud_confirmed_at)
            if label.dispute_event_at is not None:
                _utc(label.dispute_event_at)
            if label.label_available_at < label.fraud_occurred_at:
                raise ValueError(f"label {label.label_id} is available before occurrence")
            if (
                label.fraud_confirmed_at is not None
                and label.fraud_confirmed_at > label.label_available_at
            ):
                raise ValueError(f"label {label.label_id} is available before confirmation")
            if label.label_available_at < label.fraud_occurred_at + timedelta(
                seconds=self.label_delay_seconds
            ):
                raise ValueError(f"label {label.label_id} violates configured label delay")
            if (
                label.dispute_event_at is not None
                and label.dispute_event_at > label.label_available_at
            ):
                raise ValueError(f"label {label.label_id} is available before dispute evidence")
            if label.payment_id not in payment_ids:
                raise ValueError(f"label {label.label_id} references unknown payment")
            if label.fraud_record_id not in self.fraud_truth_by_record:
                raise ValueError(f"label {label.label_id} references unknown fraud record")
        event_by_id = {event.event_id: event for event in self.behavior.payment_events}
        for entry in self.behavior.ledger_entries:
            for timestamp in (entry.occurred_at, entry.effective_at, entry.posted_at):
                _utc(timestamp)
            if entry.account_id not in entity_ids["accounts"]:
                raise ValueError(f"ledger entry references unknown account {entry.account_id}")
            if entry.payment_id not in payment_ids or entry.event_id not in event_by_id:
                raise ValueError(f"ledger entry {entry.ledger_entry_id} references unknown source")
            if entry.occurred_at != entry.effective_at or entry.posted_at < entry.effective_at:
                raise ValueError(f"ledger entry {entry.ledger_entry_id} has invalid timestamps")

    def _initial_events_by_payment(self) -> dict[str, PaymentEvent]:
        result: dict[str, PaymentEvent] = {}
        for event in sorted(self.events, key=lambda item: (item.event_time, item.event_id)):
            if event.event_type not in _INITIAL_EVENT_TYPES or event.causation_id is not None:
                continue
            existing = result.get(event.payment_id)
            if existing is not None and existing.event_id != event.event_id:
                raise ValueError(f"payment {event.payment_id} has multiple initial events")
            result[event.payment_id] = event
        return result

    def _labels_by_payment(self) -> dict[str, DelayedFraudLabel]:
        result: dict[str, DelayedFraudLabel] = {}
        for label in sorted(self.labels, key=lambda item: (item.payment_id, item.label_id)):
            existing = result.get(label.payment_id)
            if existing is not None and existing.model_dump(mode="json") != label.model_dump(
                mode="json"
            ):
                raise ValueError(f"payment {label.payment_id} has conflicting labels")
            result[label.payment_id] = label
        return result

    def _prediction_times(self, explicit: Mapping[str, datetime] | None) -> dict[str, datetime]:
        if explicit is not None:
            result = {payment_id: _utc(value) for payment_id, value in explicit.items()}
            unknown = set(result) - {payment.payment_id for payment in self.payments}
            if unknown:
                raise ValueError(f"prediction times reference unknown payments: {sorted(unknown)}")
            return result
        delay = timedelta(seconds=self.settings.prediction_delay_seconds)
        return {
            payment.payment_id: self.events_by_payment[payment.payment_id].source_available_at
            + delay
            for payment in self.payments
            if payment.payment_id in self.events_by_payment
        }

    def _label_delay_gap_seconds(self) -> int:
        configured_gap = self.settings.splits.label_delay_gap_seconds
        return configured_gap if configured_gap is not None else self.label_delay_seconds

    def _split_boundaries(
        self,
    ) -> tuple[datetime, datetime, datetime, datetime, datetime]:
        splits = self.settings.splits
        gap_seconds = self._label_delay_gap_seconds()
        gap = timedelta(seconds=gap_seconds)
        explicit = (splits.train_end, splits.validation_end, splits.test_end)
        if any(value is not None for value in explicit):
            if splits.train_end is None or splits.validation_end is None:
                raise ValueError("explicit temporal splits require train_end and validation_end")
            test_end = splits.test_end or self.end
            train_end = _utc(splits.train_end)
            validation_end = _utc(splits.validation_end)
            test_end = _utc(test_end)
            boundaries = (
                train_end,
                train_end + gap,
                validation_end,
                validation_end + gap,
                test_end,
            )
        else:
            total_seconds = (self.end - self.start).total_seconds() - 2 * gap_seconds
            if total_seconds <= 0:
                raise ValueError("temporal label-delay gaps must fit the dataset range")
            train_end = self.start + timedelta(seconds=total_seconds * splits.train_fraction)
            validation_end = (
                train_end + gap + timedelta(seconds=total_seconds * splits.validation_fraction)
            )
            boundaries = (
                train_end,
                train_end + gap,
                validation_end,
                validation_end + gap,
                self.end,
            )
        if not (
            self.start
            < boundaries[0]
            <= boundaries[1]
            < boundaries[2]
            <= boundaries[3]
            < boundaries[4]
            <= self.end
        ):
            raise ValueError(
                "temporal split boundaries must be strictly ordered in the dataset range"
            )
        return boundaries

    def _split(
        self,
        prediction_time: datetime,
        boundaries: tuple[datetime, datetime, datetime, datetime, datetime],
    ) -> str | None:
        if prediction_time < boundaries[0]:
            return "train"
        if boundaries[1] <= prediction_time < boundaries[2]:
            return "validation"
        if boundaries[3] <= prediction_time < boundaries[4]:
            return "test"
        if prediction_time < boundaries[3]:
            return None
        raise ValueError("prediction timestamp is outside the configured split range")

    def build_rows(
        self, prediction_times: Mapping[str, datetime] | None = None
    ) -> tuple[dict[str, Any], ...]:
        """Build every eligible PIT row without applying M9 split boundaries.

        M10 uses this shared row construction to assign several independent
        rolling folds while retaining the exact M9 availability logic.
        """

        times = self._prediction_times(prediction_times)
        rows: list[dict[str, Any]] = []
        for payment in sorted(self.payments, key=lambda item: item.payment_id):
            event = self.events_by_payment.get(payment.payment_id)
            prediction_time = times.get(payment.payment_id)
            if event is None or prediction_time is None:
                continue
            if prediction_times is not None and not self.start <= prediction_time < self.end:
                raise ValueError(
                    f"prediction timestamp for {payment.payment_id} is outside the dataset range"
                )
            if not self.start <= prediction_time < self.end:
                continue
            if event.source_available_at > prediction_time:
                raise ValueError(
                    f"payment {payment.payment_id} is not available at its prediction timestamp"
                )
            label = self.labels_by_payment.get(payment.payment_id)
            observed_version: LabelVersion | None = None
            observation = self.observations_by_payment.get(payment.payment_id)
            if observation is not None:
                observed_version = visible_label_at(observation, prediction_time)
                mature = observed_version.label_version > 0
            else:
                mature = label is not None and label.label_available_at <= prediction_time
            if not mature and self.settings.unresolved_labels == "exclude":
                continue
            rows.append(
                self._row(
                    payment,
                    event,
                    prediction_time,
                    label,
                    mature,
                    "all",
                    observed_version=observed_version,
                )
            )
        return tuple(rows)

    def build(self, prediction_times: Mapping[str, datetime] | None = None) -> PointInTimeDataset:
        """Build rows in stable payment-ID order.

        By default each payment is scored when its initial source event becomes
        available.  Callers may provide a deterministic payment-to-prediction
        mapping for snapshot or delayed-prediction use cases.
        """

        boundaries = self._split_boundaries()
        rows = [
            row | {"split": split}
            for row in self.build_rows(prediction_times)
            if (split := self._split(row["prediction_time"], boundaries)) is not None
        ]
        manifest = self._manifest(rows, boundaries)
        return PointInTimeDataset(tuple(rows), manifest)

    def _prior_events(
        self,
        event: PaymentEvent,
        prediction_time: datetime,
        *,
        same_customer: bool = True,
    ) -> tuple[PaymentEvent, ...]:
        return tuple(
            candidate
            for candidate in self.events_by_payment.values()
            if (not same_customer or candidate.customer_id == event.customer_id)
            and candidate.payment_id != event.payment_id
            and candidate.event_time < prediction_time
            and candidate.source_available_at <= prediction_time
        )

    def _windowed_events(
        self,
        candidates: tuple[PaymentEvent, ...],
        prediction_time: datetime,
        seconds: int,
    ) -> tuple[PaymentEvent, ...]:
        return tuple(
            candidate
            for candidate in candidates
            if _in_window(prediction_time, candidate.event_time, seconds)
        )

    def _event_windows(
        self, candidates: tuple[PaymentEvent, ...], prediction_time: datetime
    ) -> dict[str, tuple[PaymentEvent, ...]]:
        return {
            name: self._windowed_events(
                candidates, prediction_time, self.feature_windows[feature_name]
            )
            for name, feature_name in _EVENT_WINDOW_FEATURES
        }

    def _device_window(
        self,
        candidates: tuple[PaymentEvent, ...],
        event: PaymentEvent,
        prediction_time: datetime,
        seconds: int,
    ) -> tuple[PaymentEvent, ...]:
        return tuple(
            item
            for item in candidates
            if item.device_id == event.device_id
            and _in_window(prediction_time, item.event_time, seconds)
        )

    def _historical_labels(
        self,
        payment: Payment,
        prediction_time: datetime,
    ) -> tuple[
        tuple[DelayedFraudLabel, ...],
        tuple[DelayedFraudLabel, ...],
        tuple[DelayedFraudLabel, ...],
        tuple[DelayedFraudLabel, ...],
    ]:
        historical_labels = tuple(
            label
            for label in self.labels
            if label.label_available_at <= prediction_time
            and label.payment_id != payment.payment_id
            and label.fraud_occurred_at < prediction_time
        )
        known_labels = tuple(
            label
            for label in historical_labels
            if label.label == "FRAUD" and label.fraud_confirmed_at is not None
        )
        prior_confirmed = tuple(
            label
            for label in known_labels
            if _in_window(
                prediction_time,
                label.fraud_occurred_at,
                self.feature_windows["confirmed_fraud_count_90d"],
            )
        )
        prior_loss = tuple(
            label
            for label in known_labels
            if _in_window(
                prediction_time,
                label.fraud_occurred_at,
                self.feature_windows["fraud_loss_365d"],
            )
        )
        merchant_labels = tuple(
            label
            for label in historical_labels
            if _in_window(
                prediction_time,
                label.fraud_occurred_at,
                self.feature_windows["merchant_fraud_rate_historical"],
            )
            and self._payment_merchant_id(label.payment_id) == payment.merchant_id
        )
        return known_labels, prior_confirmed, prior_loss, merchant_labels

    def _account_balance(
        self, account: Account, prediction_time: datetime
    ) -> tuple[float, datetime]:
        """Return the account balance and latest posted ledger time known at T."""

        balance = account.ledger_balance
        latest_posted_at = account.opening_date
        entries = sorted(
            (
                entry
                for entry in self.ledger_entries
                if entry.account_id == account.account_id
                and entry.effective_at < prediction_time
                and entry.posted_at <= prediction_time
            ),
            key=lambda entry: (entry.posted_at, entry.event_id, entry.ledger_entry_id),
        )
        for entry in entries:
            delta = entry.amount if entry.entry_type == "CREDIT" else -entry.amount
            balance = round(balance + delta, 2)
            latest_posted_at = max(latest_posted_at, entry.posted_at)
        return balance, latest_posted_at

    def _feature_available_at(
        self,
        event: PaymentEvent,
        all_prior_events: tuple[PaymentEvent, ...],
        historical_labels: tuple[DelayedFraudLabel, ...],
        account_balance_available_at: datetime,
        account: Account,
        merchant: Merchant | None,
        device: Device | None,
        card: Card | None,
    ) -> datetime:
        available_timestamps = [
            event.source_available_at,
            *(item.source_available_at for item in all_prior_events),
            *(item.label_available_at for item in historical_labels),
            account.opening_date,
            account.valid_from,
            account.system_from,
            account_balance_available_at,
        ]
        if merchant is not None:
            available_timestamps.append(merchant.created_at)
        if device is not None:
            available_timestamps.append(device.first_seen_at)
        if card is not None:
            available_timestamps.append(card.issued_at)
        return max(available_timestamps)

    def _feature_values(
        self,
        payment: Payment,
        event: PaymentEvent,
        prediction_time: datetime,
        prior: tuple[PaymentEvent, ...],
        windows: dict[str, tuple[PaymentEvent, ...]],
        device_day: tuple[PaymentEvent, ...],
        device_month: tuple[PaymentEvent, ...],
        prior_confirmed: tuple[DelayedFraudLabel, ...],
        customer_labels: tuple[DelayedFraudLabel, ...],
        merchant_labels: tuple[DelayedFraudLabel, ...],
        account: Account,
        merchant: Merchant | None,
        device: Device | None,
        balance: float,
    ) -> dict[str, Any]:
        customer_prior = tuple(
            item for item in windows["amount_month"] if item.customer_id == event.customer_id
        )
        customer_average = (
            sum(item.amount for item in customer_prior) / len(customer_prior)
            if customer_prior
            else None
        )
        prior_merchants = {
            item.merchant_id for item in windows["merchant_month"] if item.merchant_id is not None
        }
        prior_countries = {
            self.merchants[item.merchant_id].country
            for item in windows["merchant_month"]
            if item.merchant_id in self.merchants
        }
        prior_devices = {item.device_id for item in windows["month"] if item.device_id is not None}
        last_payment = max((item.event_time for item in prior), default=None)
        last_fraud = max((item.fraud_occurred_at for item in customer_labels), default=None)
        fraud_rate = (
            sum(item.label == "FRAUD" for item in merchant_labels) / len(merchant_labels)
            if merchant_labels
            else 0.0
        )
        current_country = merchant.country if merchant is not None else None
        account_age = max(0.0, float((prediction_time - account.opening_date).days))
        utilization = (
            (account.credit_limit - balance) / account.credit_limit
            if account.credit_limit > 0
            else 0.0
        )
        return {
            "amount": payment.amount,
            "payment_rail": payment.payment_rail,
            "payment_type": payment.payment_type,
            "merchant_id": payment.merchant_id,
            "card_id": payment.card_id,
            "device_id": event.device_id,
            "online": event.online,
            "transaction_count_1m": len(windows["one_minute"]),
            "transaction_count_5m": len(windows["five_minutes"]),
            "transaction_count_1h": len(windows["one_hour"]),
            "transaction_count_24h": len(windows["day"]),
            "transaction_count_7d": len(windows["week"]),
            "transaction_count_30d": len(windows["month"]),
            "transaction_amount_1h": sum(item.amount for item in windows["amount_hour"]),
            "transaction_amount_24h": sum(item.amount for item in windows["amount_day"]),
            "transaction_amount_7d": sum(item.amount for item in windows["amount_week"]),
            "avg_transaction_amount_30d": customer_average,
            "max_transaction_amount_7d": max(
                (item.amount for item in windows["amount_week"]), default=None
            ),
            "amount_vs_customer_avg": (
                payment.amount / customer_average if customer_average is not None else None
            ),
            "distinct_merchants_1d": len(
                {item.merchant_id for item in windows["merchant_day"] if item.merchant_id}
            ),
            "distinct_merchants_30d": len(prior_merchants),
            "new_merchant_flag": payment.merchant_id is not None
            and payment.merchant_id not in prior_merchants,
            "merchant_fraud_rate_historical": fraud_rate,
            "distinct_countries_24h": len(
                {
                    self.merchants[item.merchant_id].country
                    for item in windows["country_day"]
                    if item.merchant_id in self.merchants
                }
            ),
            "new_country_flag": current_country is not None
            and current_country not in prior_countries,
            "device_age_days": (
                max(0.0, float((prediction_time - device.first_seen_at).days))
                if device is not None
                else None
            ),
            "new_device_flag": event.device_id is not None and event.device_id not in prior_devices,
            "trusted_device_flag": device.trusted if device is not None else False,
            "device_customer_count_30d": len({item.customer_id for item in device_month}),
            "customers_per_device_24h": len({item.customer_id for item in device_day}),
            "account_age_days": account_age,
            "balance": balance,
            "available_balance": balance,
            "credit_limit": account.credit_limit,
            "credit_utilization": utilization,
            "days_since_last_payment": (
                (prediction_time - last_payment).total_seconds() / 86400
                if last_payment is not None
                else -1.0
            ),
            "confirmed_fraud_count_90d": sum(
                item.customer_id == event.customer_id for item in prior_confirmed
            ),
            "fraud_loss_365d": sum(item.amount for item in customer_labels),
            "days_since_last_confirmed_fraud": (
                (prediction_time - last_fraud).total_seconds() / 86400
                if last_fraud is not None
                else -1.0
            ),
        }

    def _row(
        self,
        payment: Payment,
        event: PaymentEvent,
        prediction_time: datetime,
        label: DelayedFraudLabel | None,
        label_is_mature: bool,
        split: str,
        observed_version: LabelVersion | None = None,
    ) -> dict[str, Any]:
        prior = self._prior_events(event, prediction_time)
        all_prior = self._prior_events(event, prediction_time, same_customer=False)
        merchant = self.merchants.get(payment.merchant_id or "")
        device = self.devices.get(event.device_id or "")
        account = self.accounts.get(payment.payer_account_id)
        if account is None:
            raise ValueError(f"payment references unknown account {payment.payer_account_id}")
        if prediction_time < account.valid_from or (
            account.valid_to is not None and prediction_time >= account.valid_to
        ):
            raise ValueError(
                f"account {account.account_id} has no valid snapshot at prediction time"
            )

        windows = self._event_windows(prior, prediction_time)
        device_day = self._device_window(
            all_prior,
            event,
            prediction_time,
            self.feature_windows["customers_per_device_24h"],
        )
        device_month = self._device_window(
            all_prior,
            event,
            prediction_time,
            self.feature_windows["device_customer_count_30d"],
        )
        known_labels, prior_confirmed, prior_loss, merchant_labels = self._historical_labels(
            payment, prediction_time
        )
        customer_labels = tuple(
            item for item in prior_loss if item.customer_id == event.customer_id
        )
        card = self.cards.get(payment.card_id or "")
        balance, balance_available_at = self._account_balance(account, prediction_time)
        historical_labels = known_labels + merchant_labels
        feature_available_at = self._feature_available_at(
            event,
            all_prior,
            historical_labels,
            balance_available_at,
            account,
            merchant,
            device,
            card,
        )
        if feature_available_at > prediction_time:
            raise ValueError(f"features for {payment.payment_id} are not point-in-time correct")
        row_id = (
            "DSR-"
            + hashlib.sha256(
                f"{self._source_run_id()}|{payment.payment_id}|{prediction_time.isoformat()}".encode()
            ).hexdigest()[:24]
        )
        feature_values = self._feature_values(
            payment,
            event,
            prediction_time,
            prior,
            windows,
            device_day,
            device_month,
            prior_confirmed,
            customer_labels,
            merchant_labels,
            account,
            merchant,
            device,
            balance,
        )
        return {
            "dataset_row_id": row_id,
            "payment_id": payment.payment_id,
            "event_id": event.event_id,
            "customer_id": event.customer_id,
            "account_id": payment.payer_account_id,
            "prediction_time": prediction_time,
            "business_event_time": event.event_time,
            "event_time": event.event_time,
            "source_available_at": event.source_available_at,
            "feature_available_at": feature_available_at,
            "label_available_at": (
                observed_version.label_available_at
                if observed_version is not None
                else label.label_available_at
                if label is not None
                else None
            ),
            "label": (
                observed_version.observed_label
                if observed_version is not None and label_is_mature
                else label.label
                if label is not None and label_is_mature
                else None
            ),
            "fraud_truth": (
                None
                if observed_version is not None
                else self.fraud_truth_by_record.get(label.fraud_record_id)
                if label is not None and label_is_mature
                else None
            ),
            **feature_values,
            "split": split,
        }

    def _source_run_id(self) -> str:
        if self.source_manifest is not None:
            return self.source_manifest.run_id
        run_ids = sorted(
            {event.simulation_run_id for event in self.events if event.simulation_run_id}
        )
        return run_ids[0] if run_ids else "in-memory"

    def _payment_merchant_id(self, payment_id: str) -> str | None:
        payment = self.payments_by_id.get(payment_id)
        return payment.merchant_id if payment is not None else None

    def _manifest(
        self,
        rows: list[dict[str, Any]],
        boundaries: tuple[datetime, datetime, datetime, datetime, datetime],
    ) -> DatasetManifest:
        source_run_id = self._source_run_id()
        source_manifest_hash = self._source_manifest_hash()
        configuration_hash = config_hash(self.config, include_dataset=True)
        generator_version = (
            self.source_manifest.generator_version
            if self.source_manifest is not None
            else __version__
        )
        row_counts = {
            "total": len(rows),
            "train": sum(row["split"] == "train" for row in rows),
            "validation": sum(row["split"] == "validation" for row in rows),
            "test": sum(row["split"] == "test" for row in rows),
        }
        features: dict[str, dict[str, object]] = {}
        label_features = {
            "merchant_fraud_rate_historical",
            "confirmed_fraud_count_90d",
            "fraud_loss_365d",
            "days_since_last_confirmed_fraud",
        }
        null_policies = {
            "avg_transaction_amount_30d": "null when no eligible prior customer transactions",
            "max_transaction_amount_7d": "null when no eligible prior transactions",
            "amount_vs_customer_avg": "null when no eligible prior customer transactions",
            "device_age_days": "null when the event has no known device",
            "merchant_fraud_rate_historical": "0.0 when no eligible historical labels exist",
            "days_since_last_payment": "-1.0 when no eligible prior payment exists",
            "days_since_last_confirmed_fraud": "-1.0 when no eligible confirmed fraud exists",
        }
        for name in _FEATURE_COLUMNS:
            uses_labels = name in label_features
            availability_time_field = (
                "label_available_at"
                if uses_labels
                else "source_available_at for payment events; valid_from or posted_at for snapshots"
            )
            definition: dict[str, object] = {
                "version": "1",
                "source": "fraud_labels"
                if uses_labels
                else "payments, entities, and ledger_entries",
                "availability_time_field": availability_time_field,
                "availability_rule": (
                    "label_available_at <= prediction_time"
                    if uses_labels
                    else "every contributing source availability timestamp <= prediction_time"
                ),
                "business_time_rule": "event_time < prediction_time",
                "null_policy": null_policies.get(name, "not nullable for a valid source record"),
            }
            window_seconds = self.feature_windows.get(name)
            if window_seconds is not None:
                definition.update(
                    {
                        "window_seconds": window_seconds,
                        "window_rule": "[prediction_time - window_seconds, prediction_time)",
                    }
                )
            else:
                definition["window_rule"] = "current entity/payment value as-of prediction_time"
            if name == "available_balance":
                definition["notes"] = "derived from posted ledger entries; holds are not modeled"
            features[name] = definition
        split_boundaries = {
            "train_end": boundaries[0].isoformat(),
            "validation_start": boundaries[1].isoformat(),
            "validation_end": boundaries[2].isoformat(),
            "test_start": boundaries[3].isoformat(),
            "test_end": boundaries[4].isoformat(),
        }
        label_definition: dict[str, object] = {
            "field": "label",
            "availability_rule": "label_available_at <= prediction_time",
            "configured_delay_seconds": self.label_delay_seconds,
            "split_gap_seconds": self._label_delay_gap_seconds(),
            "unresolved_policy": self.settings.unresolved_labels,
        }
        row_hash = sha256_json(rows)
        schema_fingerprint = _schema_fingerprint()
        output_fingerprint = sha256_json(
            {
                "schema_fingerprint": schema_fingerprint,
                "rows": rows,
            }
        )
        row_grain = "one row per initial payment event prediction opportunity"
        prediction_entity = "payment_id"
        date_range = {"start": self.start.isoformat(), "end": self.end.isoformat()}
        payload = {
            "dataset_version": "0.9.0",
            "source_run_id": source_run_id,
            "source_manifest_hash": source_manifest_hash,
            "configuration_hash": configuration_hash,
            "parameters": self.settings.model_dump(mode="json"),
            "split_boundaries": split_boundaries,
            "feature_definitions": features,
            "label_definition": label_definition,
            "row_hash": row_hash,
            "row_counts": row_counts,
            "schema_columns": list(PIT_DATASET_SCHEMA),
            "row_grain": row_grain,
            "prediction_entity": prediction_entity,
            "date_range": date_range,
            "schema_fingerprint": schema_fingerprint,
            "output_fingerprint": output_fingerprint,
        }
        dataset_id = "DS-" + sha256_json(payload)[:16]
        source_information: dict[str, object] = {
            "run_id": source_run_id,
            "manifest_hash": source_manifest_hash,
            "generator_version": generator_version,
            "seed": self.config.simulation.seed,
        }
        if self.source_manifest is not None:
            source_information.update(self.source_manifest.model_dump(mode="json"))
        return DatasetManifest(
            dataset_id=dataset_id,
            dataset_version="0.9.0",
            source_run_id=source_run_id,
            source_manifest_hash=source_manifest_hash,
            generator_version=generator_version,
            seed=self.config.simulation.seed,
            configuration_hash=configuration_hash,
            parameters=self.settings.model_dump(mode="json"),
            split_boundaries=split_boundaries,
            feature_definitions=features,
            label_definition=label_definition,
            source_run_information=source_information,
            reproducibility={
                "row_hash": row_hash,
                "schema_fingerprint": schema_fingerprint,
                "output_fingerprint": output_fingerprint,
                "schema_columns": list(PIT_DATASET_SCHEMA),
                "ordering": "payment_id ascending",
                "row_grain": row_grain,
                "prediction_entity": prediction_entity,
                "feature_cutoff": "prediction_time",
                "label_cutoff": "label_available_at <= prediction_time",
                "current_time_used": False,
                "global_randomness_used": False,
            },
            row_counts=row_counts,
            schema_version="1",
            row_grain=row_grain,
            prediction_entity=prediction_entity,
            date_range=date_range,
            schema_fingerprint=schema_fingerprint,
            output_fingerprint=output_fingerprint,
            difficulty=(self.source_manifest.difficulty if self.source_manifest else None),
            camouflage=(self.source_manifest.camouflage if self.source_manifest else None),
            counterfactual=(self.source_manifest.counterfactual if self.source_manifest else None),
        )

    def _source_manifest_hash(self) -> str:
        if self.source_manifest is None:
            return "in-memory"
        return hashlib.sha256(self.source_manifest.model_dump_json().encode("utf-8")).hexdigest()


def build_point_in_time_dataset(
    config: SimulationRunConfig,
    entities: EntityDataset,
    behavior: BehaviorDataset,
    source_manifest: RunManifest | None = None,
    prediction_times: Mapping[str, datetime] | None = None,
) -> PointInTimeDataset:
    """Convenience API for deterministic in-memory M9 construction."""

    return PointInTimeDatasetBuilder(config, entities, behavior, source_manifest).build(
        prediction_times
    )


def write_point_in_time_dataset(
    dataset: PointInTimeDataset,
    output_path: Path,
    manifest_path: Path | None = None,
) -> tuple[Path, Path]:
    """Write the fixed-schema M9 Parquet table and its JSON manifest."""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(list(dataset.rows), schema=PIT_DATASET_SCHEMA, orient="row").write_parquet(
        output_path
    )
    final_manifest_path = manifest_path or output_path.with_name("dataset_manifest.json")
    final_manifest_path.parent.mkdir(parents=True, exist_ok=True)
    final_manifest_path.write_text(
        dataset.manifest.model_dump_json(indent=2) + "\n", encoding="utf-8"
    )
    return output_path, final_manifest_path


DATASET_SCHEMA = PIT_DATASET_SCHEMA
DatasetBuilder = PointInTimeDatasetBuilder
build_dataset = build_point_in_time_dataset


__all__ = [
    "PIT_DATASET_SCHEMA",
    "DATASET_SCHEMA",
    "DatasetBuilder",
    "PointInTimeDataset",
    "PointInTimeDatasetBuilder",
    "build_point_in_time_dataset",
    "build_dataset",
    "load_generated_run",
    "write_point_in_time_dataset",
]
