"""High-level Python API for generating deterministic FraudTwin runs."""

import hashlib
import json
import sqlite3
import tempfile
import time
from collections import Counter
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Literal, cast, overload

from pydantic import BaseModel

from fraudtwin.calibration import (
    CalibrationProfile,
    ResolvedCalibration,
    compute_fidelity_report,
    load_calibration_profile,
    resolve_calibration,
)
from fraudtwin.config import SimulationRunConfig, config_hash, load_config, load_default_config
from fraudtwin.difficulty import difficulty_metadata
from fraudtwin.domain import (
    CARD_LIFECYCLE_EVENT_TYPES,
    PIX_LIFECYCLE_EVENT_TYPES,
    LedgerEntry,
)
from fraudtwin.errors import LedgerCapacityError
from fraudtwin.graph import build_graph
from fraudtwin.kafka import publisher_from_environment
from fraudtwin.manifest import RunManifest, create_manifest, write_manifest
from fraudtwin.ml import (
    PointInTimeDataset,
    PointInTimeDatasetBuilder,
    write_point_in_time_dataset,
)
from fraudtwin.postgres import ensure_database_ready, persist_run
from fraudtwin.reproducibility import sha256_json
from fraudtwin.scale import (
    ScaleCheckpoint,
    load_checkpoint,
    resolve_scale_plan,
    write_scale_partitions,
)
from fraudtwin.seed import create_stream_rng
from fraudtwin.simulation import BehaviorGenerator, EntityGenerator
from fraudtwin.simulation.behavior import BehaviorDataset
from fraudtwin.simulation.generator import EntityDataset
from fraudtwin.simulation.parquet import (
    write_behavior_parquet,
    write_calibration_artifacts,
    write_campaign_dynamics_sidecar,
    write_counterfactual_sidecar,
    write_entity_parquet,
    write_graph_truth,
    write_label_observation_sidecar,
)
from fraudtwin.simulation.payments import PaymentGenerator
from fraudtwin.storage import storage_for
from fraudtwin.timing import StageMetrics, finish_stage, measure_stage


@dataclass(frozen=True)
class GeneratedData:
    """Generated FraudTwin data kept in memory."""

    manifest: RunManifest
    entities: EntityDataset
    behavior: BehaviorDataset
    dataset: PointInTimeDataset | None = None
    stage_timings: StageMetrics = field(default_factory=dict)

    @property
    def run_id(self) -> str:
        """Return the deterministic identifier for this generated run."""

        return self.manifest.run_id

    def require_dataset(self) -> PointInTimeDataset:
        """Return the point-in-time dataset, or explain how to enable it.

        ``generate`` only builds a dataset when ``config.dataset.enabled`` is
        true.  This typed accessor keeps notebook and application code concise
        while preserving the optional dataset for lightweight simulations.

        Raises:
            RuntimeError: If the supplied configuration did not enable the
                point-in-time dataset builder.
        """

        if self.dataset is None:
            raise RuntimeError(
                "this run has no point-in-time dataset; enable dataset.enabled "
                "in the simulation configuration"
            )
        return self.dataset


@dataclass(frozen=True)
class GeneratedRun:
    """Metadata and paths for a generated run written to disk."""

    manifest: RunManifest
    run_dir: Path
    manifest_path: Path
    dataset_path: Path | None = None
    dataset_manifest_path: Path | None = None
    stage_timings: StageMetrics = field(default_factory=dict)

    @property
    def run_id(self) -> str:
        """Return the deterministic identifier for this generated run."""

        return self.manifest.run_id

    def load_data(self) -> GeneratedData:
        """Load the typed entities and behavior behind this written run.

        A written run intentionally returns metadata and paths so large runs
        are not retained in memory.  Call this method when a graph, dataset
        builder, or publisher needs the persisted domain records.

        Raises:
            ValueError: If the run directory is missing or has an invalid
                manifest or artifact.
        """

        from fraudtwin.ml.dataset import load_generated_run

        entities, behavior, manifest = load_generated_run(self.run_dir)
        return GeneratedData(manifest=manifest, entities=entities, behavior=behavior)


def _apply_generation_overrides(
    config: SimulationRunConfig, *, seed: int | None, workers: int | None
) -> SimulationRunConfig:
    """Apply optional API overrides and validate the resulting configuration once."""

    if workers is not None:
        if workers < 1:
            raise ValueError("workers must be positive")
        if not config.scale.enabled:
            raise ValueError("workers requires an enabled scale profile")
    if seed is None and workers is None:
        return config

    values = config.model_dump(mode="python")
    if seed is not None:
        values["simulation"]["seed"] = seed
    if workers is not None:
        values["scale"]["worker_count"] = workers
    return SimulationRunConfig.model_validate(values)


def _resolve_config(
    config: str | Path | SimulationRunConfig | None,
    profile_override: Path | None = None,
) -> SimulationRunConfig:
    if config is None:
        return load_default_config()
    if isinstance(config, SimulationRunConfig):
        return config
    return load_config(Path(config), calibration_profile_override=profile_override)


def _graph_metadata(
    config: SimulationRunConfig,
    entities: EntityDataset,
    behavior: BehaviorDataset,
    manifest: RunManifest,
) -> dict[str, object]:
    """Build optional source-manifest metadata for an enabled graph run."""

    if not config.graph.enabled:
        return {}
    metadata: dict[str, object] = {
        "enabled": True,
        "configuration_hash": config_hash(config, include_scale_execution=False),
        "schema_version": "2",
        "node_count": sum(entities.counts.values()),
        "edge_count": 0,
        "pattern_count": len(behavior.graph_patterns),
        "campaign_membership_count": len(behavior.graph_memberships),
        "campaign_count": len(behavior.graph_campaigns),
        "evidence_count": len(behavior.graph_evidence),
        "hyperedge_count": len(behavior.graph_hyperedges),
        "hyperedge_membership_count": len(behavior.graph_hyperedge_memberships),
        "source_snapshot": {
            "start": config.simulation.start.isoformat(),
            "end": (
                config.simulation.start + timedelta(days=config.simulation.duration_days)
            ).isoformat(),
        },
    }
    source_graph = build_graph(config, entities, behavior, manifest, view="oracle")
    metadata.update(
        {
            "node_count": len(source_graph.nodes),
            "edge_count": len(source_graph.edges),
            "pattern_count": len(source_graph.patterns),
            "output_fingerprint": source_graph.output_fingerprint,
            "schema_fingerprint": sha256_json(
                {
                    "graph_schema_version": "2",
                    "source_tables": entities.counts,
                    "events": behavior.event_counts,
                }
            ),
            "oracle_artifact_fingerprint": sha256_json(
                {
                    name: [item.model_dump(mode="json") for item in records]
                    for name, records in (
                        ("campaigns", behavior.graph_campaigns),
                        ("patterns", behavior.graph_patterns),
                        ("campaign_memberships", behavior.graph_memberships),
                        ("evidence", behavior.graph_evidence),
                        ("hyperedges", behavior.graph_hyperedges),
                        ("hyperedge_memberships", behavior.graph_hyperedge_memberships),
                    )
                }
            ),
        }
    )
    return metadata


def _build_manifest(
    config: SimulationRunConfig,
    base_manifest: RunManifest,
    entities: EntityDataset,
    behavior: BehaviorDataset,
    *,
    campaign_dynamics_metadata: dict[str, object] | None,
    counterfactual_metadata: dict[str, object] | None,
    label_observation_metadata: dict[str, object] | None,
    scale_metadata: dict[str, object] | None,
    calibration: ResolvedCalibration | None = None,
) -> RunManifest:
    entity_counts = {**entities.counts, "behavior_profiles": len(behavior.profiles)}
    if entities.state_history:
        entity_counts["state_history"] = len(entities.state_history)
    event_counts = behavior.event_counts
    return base_manifest.model_copy(
        update={
            "entity_counts": entity_counts,
            "event_counts": event_counts,
            "schema_versions": {
                **{entity_name: "1" for entity_name in entity_counts},
                "payments": "2",
                "payment_events": "5",
                "ledger_entries": "1",
                "fraud_records": "1",
                "fraud_alerts": "1",
                "fraud_cases": "1",
                "case_confirmations": "1",
                "customer_disputes": "1",
                "fraud_labels": "1",
                **({"label_observations": "1"} if config.labels.enabled else {}),
                **(
                    {"counterfactual_change_sets": "1"}
                    if counterfactual_metadata is not None
                    else {}
                ),
            },
            "fraud_counts": behavior.fraud_counts,
            "fraud_rates": behavior.fraud_rates,
            "quality_fault_counts": behavior.quality_fault_counts,
            "quality_fault_rates": behavior.quality_fault_rates,
            "quality_diagnostics": behavior.quality_diagnostics,
            "graph": _graph_metadata(config, entities, behavior, base_manifest),
            "difficulty": difficulty_metadata(
                config,
                entities,
                behavior,
                base_manifest.run_id,
            )
            or None,
            "camouflage": behavior.camouflage_metadata or None,
            "counterfactual": counterfactual_metadata,
            "campaign_dynamics": campaign_dynamics_metadata,
            "calibration": (
                {
                    "profile_id": calibration.profile_id,
                    "profile_version": calibration.profile.profile_version
                    if calibration.profile
                    else None,
                    "effective_configuration_hash": calibration.effective_configuration_hash,
                    "stream_ids": list(
                        dict.fromkeys(
                            [
                                *calibration.stream_ids,
                                *(
                                    calibration.profile.provenance.stream_ids
                                    if calibration.profile
                                    else ()
                                ),
                            ]
                        )
                    ),
                    "source_fingerprint": calibration.profile.provenance.source_fingerprint
                    if calibration.profile
                    else None,
                    "source_schema_fingerprint": (
                        calibration.profile.provenance.source_schema_fingerprint
                    )
                    if calibration.profile
                    else None,
                }
                if calibration is not None and calibration.enabled
                else None
            ),
            "label_observation": label_observation_metadata,
            "scale": scale_metadata,
        }
    )


def _output_fingerprints(run_dir: Path) -> dict[str, object]:
    """Return deterministic checksums for every non-manifest output file."""

    checksums = {
        str(path.relative_to(run_dir)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(run_dir.rglob("*"))
        if path.is_file() and path.name != "manifest.json"
    }
    return {
        "file_checksums": checksums,
        "output_fingerprint": sha256_json(checksums),
    }


def _write_base_outputs(entities: EntityDataset, behavior: BehaviorDataset, run_dir: Path) -> None:
    """Write the stable operational and oracle tables for a generated run."""

    write_entity_parquet(entities, run_dir)
    write_behavior_parquet(behavior, run_dir)
    write_graph_truth(
        behavior.graph_memberships,
        behavior.graph_patterns,
        run_dir,
        campaigns=behavior.graph_campaigns,
        evidence=behavior.graph_evidence,
        hyperedges=behavior.graph_hyperedges,
        hyperedge_memberships=behavior.graph_hyperedge_memberships,
    )


def _campaign_metadata(
    behavior: BehaviorDataset, run_dir: Path, *, write: bool, run_id: str
) -> dict[str, object] | None:
    dataset = behavior.campaign_dynamics
    if dataset is None:
        return None
    metadata: dict[str, object] = {
        "configuration_hash": dataset.configuration_hash,
        "stream_ids": list(dataset.stream_ids),
        "transitions": len(dataset.transitions),
        "snapshots": len(dataset.snapshots),
    }
    if write:
        root, sidecar_manifest = write_campaign_dynamics_sidecar(
            dataset, run_dir, source_run_id=run_id
        )
        metadata.update(
            {
                "root": str(root.relative_to(run_dir)),
                "manifest": str(sidecar_manifest.relative_to(run_dir)),
            }
        )
    return metadata


def _counterfactual_metadata(
    behavior: BehaviorDataset, run_dir: Path, *, write: bool
) -> dict[str, object] | None:
    dataset = behavior.counterfactual
    if dataset is None:
        return None
    metadata: dict[str, object] = {
        **dataset.metadata,
        "counterfactual_id": dataset.counterfactual_id,
        "accepted": len(dataset.modified_payments),
        "rejected": len(dataset.rejected),
    }
    if write:
        root, sidecar_manifest = write_counterfactual_sidecar(dataset, run_dir)
        metadata.update({"root": str(root), "manifest": str(sidecar_manifest)})
    return metadata


def _label_observation_metadata(
    behavior: BehaviorDataset, run_dir: Path, *, write: bool, run_id: str
) -> dict[str, object] | None:
    if not behavior.label_observations:
        return None
    first = behavior.label_observations[0]
    stream_ids = tuple(
        dict.fromkeys(
            stream_id for item in behavior.label_observations for stream_id in item.stream_ids
        )
    )
    metadata: dict[str, object] = {
        "enabled": True,
        "configuration_hash": first.policy_hash,
        "stream_ids": list(stream_ids),
        "observations": len(behavior.label_observations),
        "versions": sum(len(item.versions) for item in behavior.label_observations),
    }
    if write:
        root, sidecar_manifest = write_label_observation_sidecar(
            behavior.label_observations,
            behavior.final_observed_labels,
            run_dir,
            source_run_id=run_id,
            policy_hash=first.policy_hash,
            stream_ids=stream_ids,
        )
        metadata.update(
            {
                "root": str(root.relative_to(run_dir)),
                "manifest": str(sidecar_manifest.relative_to(run_dir)),
            }
        )
    return metadata


def _logical_row(
    table_name: str, row: BaseModel | Mapping[str, object], ordinal: int
) -> dict[str, object]:
    """Add the stable routing fields shared by both scale record iterators."""

    values = row.model_dump(mode="json") if isinstance(row, BaseModel) else dict(row)
    source_id = next(
        (str(value) for key, value in values.items() if key.endswith("_id") and value),
        f"row-{ordinal:08d}",
    )
    owner_id = next(
        (
            str(values[key])
            for key in ("payer_account_id", "account_id", "customer_id")
            if values.get(key)
        ),
        source_id,
    )
    # Keep canonical row fields in the partition artifact.  The additive
    # logical metadata lets readers route and filter rows without a global ID index.
    return {
        **values,
        "logical_id": f"{table_name}:{source_id}",
        "logical_type": table_name,
        "source_id": source_id,
        "partition_key": owner_id,
    }


def _scale_records(
    entities: EntityDataset, behavior: BehaviorDataset
) -> Iterator[dict[str, object]]:
    """Yield stable logical records for the M18 partition writer."""

    ordinal = 0
    for table_name, table_rows in {**entities.all_tables(), **behavior.tables()}.items():
        for row in table_rows:
            logical = _logical_row(table_name, row, ordinal)
            yield logical
            ordinal += 1
            # Cross-account transfers are owned by the payer shard.  Emit a
            # deterministic payee-side reconciliation marker so downstream
            # consumers can close the cross-shard balance without a global
            # account map.
            payer = logical.get("payer_account_id")
            payee = logical.get("payee_account_id")
            if payer and payee and payer != payee and logical.get("payment_id"):
                payment_id = str(logical["payment_id"])
                yield {
                    "logical_id": f"transfer_reconciliation:{payment_id}",
                    "logical_type": "transfer_reconciliation",
                    "source_id": payment_id,
                    "payment_id": payment_id,
                    "payer_account_id": str(payer),
                    "payee_account_id": str(payee),
                    "amount": logical.get("amount"),
                    "partition_key": str(payee),
                    "reconciliation": "PAYEE_SIDE",
                }
                ordinal += 1


def iter_scale_records(
    config: SimulationRunConfig,
    entities: EntityDataset,
    *,
    simulation_run_id: str,
    stage_timings: StageMetrics | None = None,
) -> Iterator[dict[str, object]]:
    """Stream canonical entities, profiles, payments, events, and ledger rows.

    This iterator is the scale writer's producer-facing API.  It deliberately
    keeps only generator state and account balances; records are never
    accumulated into a ``BehaviorDataset``.  Advanced fraud/graph stages still
    use the compatibility generator until their own streaming implementations
    are available.
    """

    behavior_generator = BehaviorGenerator(config, entities, simulation_run_id=simulation_run_id)
    with measure_stage(stage_timings, "profile_generation"):
        profiles = behavior_generator.generate_profiles()

    def logical_rows(table_name: str, rows: Iterable[BaseModel]) -> Iterator[dict[str, object]]:
        for ordinal, row in enumerate(rows):
            yield _logical_row(table_name, row, ordinal)

    for table_name, rows in {**entities.all_tables(), "behavior_profiles": profiles}.items():
        yield from logical_rows(table_name, rows)

    payment_generator = PaymentGenerator(
        config,
        entities.accounts,
        entities.cards,
        entities.merchants,
        entities.devices,
        entities.pix_keys,
        simulation_run_id=simulation_run_id,
        include_lifecycle=(not config.scale.enabled or "lifecycle" in config.scale.features),
        include_ledger=(not config.scale.enabled or "ledger" in config.scale.features),
    )
    lifecycle_rng = create_stream_rng(config.simulation.seed, "milestone-4:card-lifecycle")
    pix_lifecycle_rng = create_stream_rng(config.simulation.seed, "milestone-5:pix-lifecycle")
    include_lifecycle = not config.scale.enabled or "lifecycle" in config.scale.features
    include_ledger = not config.scale.enabled or "ledger" in config.scale.features
    payment_stage_started = time.perf_counter()
    # The compatibility generator exports complete tables in this order:
    # payments, events, then ledger entries.  Stage those rows on disk while
    # payments are generated so the streaming path can retain that contract
    # without retaining all payment objects in memory.
    with tempfile.TemporaryDirectory(prefix="fraudtwin-scale-") as temporary_dir:
        temporary_root = Path(temporary_dir)
        payments_path = temporary_root / "payments.jsonl"
        events_path = temporary_root / "payment-events.jsonl"
        ledger_database = sqlite3.connect(temporary_root / "ledger.sqlite")
        ledger_database.execute(
            "CREATE TABLE ledger_specs ("
            "processed_at TEXT NOT NULL, event_id TEXT NOT NULL, account_id TEXT NOT NULL, "
            "entry_type TEXT NOT NULL, event_json TEXT NOT NULL)"
        )
        try:
            with (
                payments_path.open("w", encoding="utf-8") as payments_stream,
                events_path.open("w", encoding="utf-8") as events_stream,
            ):
                for payment, initial_event in payment_generator.iter_generate(profiles):
                    if include_lifecycle and payment.payment_rail == "CARD":
                        payment, events = payment_generator._card_lifecycle(  # noqa: SLF001
                            payment, initial_event, lifecycle_rng
                        )
                    elif include_lifecycle and payment.payment_rail == "PIX":
                        payment, events = payment_generator._pix_lifecycle(  # noqa: SLF001
                            payment, initial_event, pix_lifecycle_rng
                        )
                    else:
                        events = (initial_event,)
                    payments_stream.write(
                        json.dumps(payment.model_dump(mode="json"), separators=(",", ":")) + "\n"
                    )
                    for event in events:
                        event_values = event.model_dump(mode="json")
                        events_stream.write(json.dumps(event_values, separators=(",", ":")) + "\n")
                    if include_ledger:
                        for event, account_id, raw_entry_type in payment_generator._ledger_specs(  # noqa: SLF001
                            payment, events
                        ):
                            ledger_database.execute(
                                "INSERT INTO ledger_specs "
                                "(processed_at, event_id, account_id, entry_type, event_json) "
                                "VALUES (?, ?, ?, ?, ?)",
                                (
                                    event.processed_at.isoformat(),
                                    event.event_id,
                                    account_id,
                                    raw_entry_type,
                                    json.dumps(
                                        event.model_dump(mode="json"), separators=(",", ":")
                                    ),
                                ),
                            )
                ledger_database.commit()
        finally:
            finish_stage(stage_timings, "payments_lifecycle", payment_stage_started)

        with payments_path.open(encoding="utf-8") as payments_stream:
            for ordinal, line in enumerate(payments_stream):
                values = json.loads(line)
                logical = _logical_row("payments", values, ordinal)
                yield logical
                payer = logical.get("payer_account_id")
                payee = logical.get("payee_account_id")
                if payer and payee and payer != payee:
                    payment_id = str(logical["payment_id"])
                    yield {
                        "logical_id": f"transfer_reconciliation:{payment_id}",
                        "logical_type": "transfer_reconciliation",
                        "source_id": payment_id,
                        "payment_id": payment_id,
                        "payer_account_id": str(payer),
                        "payee_account_id": str(payee),
                        "amount": logical.get("amount"),
                        "partition_key": str(payee),
                        "reconciliation": "PAYEE_SIDE",
                    }

        with events_path.open(encoding="utf-8") as events_stream:
            for ordinal, line in enumerate(events_stream):
                yield _logical_row("payment_events", json.loads(line), ordinal)

        balances = {account.account_id: account.ledger_balance for account in entities.accounts}
        ledger_number = 0
        cursor = ledger_database.execute(
            "SELECT event_json, account_id, entry_type FROM ledger_specs "
            "ORDER BY processed_at, event_id, account_id, entry_type"
        )
        for event_json, account_id, raw_entry_type in cursor:
            event = json.loads(event_json)
            ledger_number += 1
            entry_type = cast(Literal["DEBIT", "CREDIT"], raw_entry_type)
            delta = float(event["amount"]) if entry_type == "CREDIT" else -float(event["amount"])
            balance_before = balances[account_id]
            balance = round(balance_before + delta, 2)
            account = payment_generator.accounts_by_id[account_id]
            if balance < -account.overdraft_limit:
                raise LedgerCapacityError(
                    stage="streaming payment ledger",
                    account_id=account_id,
                    payment_id=str(event["payment_id"]),
                    event_id=str(event["event_id"]),
                    debit_amount=float(event["amount"]),
                    balance_before=balance_before,
                    balance_after=balance,
                    overdraft_limit=account.overdraft_limit,
                )
            balances[account_id] = balance
            yield _logical_row(
                "ledger_entries",
                LedgerEntry(
                    ledger_entry_id=f"LED-{event['event_id']}-{ledger_number:02d}",
                    payment_id=event["payment_id"],
                    account_id=account_id,
                    event_id=event["event_id"],
                    entry_type=entry_type,
                    amount=event["amount"],
                    currency=event["currency"],
                    occurred_at=event["event_time"],
                    effective_at=event["event_time"],
                    posted_at=event["processed_at"],
                    balance_after=balance,
                ),
                ledger_number - 1,
            )
        ledger_database.close()


def iter_scale_run(
    config: SimulationRunConfig | None = None,
    *,
    entities: EntityDataset | None = None,
    run_dir: str | Path | None = None,
    simulation_run_id: str | None = None,
) -> Iterator[dict[str, object]]:
    """Yield scale records from a producer or an existing partitioned run.

    Supplying ``run_dir`` reads one Parquet chunk at a time. Supplying
    ``entities`` exposes the canonical producer stream for integrations that
    already own an entity source. Exactly one source must be provided.
    """

    if (entities is None) == (run_dir is None):
        raise ValueError("provide exactly one of entities or run_dir")
    if run_dir is not None:
        from fraudtwin.scale import iter_partition_rows

        yield from iter_partition_rows(run_dir)
        return
    if config is None or simulation_run_id is None:
        raise ValueError("config and simulation_run_id are required with entities")
    assert entities is not None
    yield from iter_scale_records(config, entities, simulation_run_id=simulation_run_id)


def _streaming_core_scale_supported(config: SimulationRunConfig) -> bool:
    """Return whether the bounded core scale producer can preserve all semantics."""

    return (
        config.scale.enabled
        and not config.fraud.enabled
        and not config.labels.enabled
        and not config.graph.enabled
        and not config.counterfactual.active
        and not config.campaign_dynamics.active
        and not config.stress.active
        and not config.benchmark.enabled
        and not config.benchmark.camouflage_active
        and not config.calibration.enabled
        and not config.extensions.enabled
        and config.quality.profile == "clean"
        and not config.outputs.postgres
        and not config.outputs.kafka
        and not config.outputs.iceberg
        and not (config.dataset.enabled and "pit" in config.scale.features)
    )


def _generate_streaming_core_scale(
    config: SimulationRunConfig,
    base_manifest: RunManifest,
    *,
    output_dir: str | Path,
    checkpoint_dir: str | Path | None,
    stage_timings: StageMetrics,
) -> GeneratedRun:
    """Generate core entities/payments directly into bounded scale partitions."""

    plan = resolve_scale_plan(config, run_id=base_manifest.run_id)
    if plan is None:  # pragma: no cover - guarded by the caller
        raise ValueError("streaming core generation requires an enabled scale profile")
    expected_payments = config.payments.daily_target * config.simulation.duration_days
    if expected_payments < plan.target_payments:
        raise ValueError(
            "scale target not met: expected at least "
            f"{plan.target_payments} payments, realized {expected_payments}"
        )
    with measure_stage(stage_timings, "entity_generation"):
        entities = EntityGenerator(config).generate()
    entity_counts = dict(entities.counts)
    entity_counts["behavior_profiles"] = len(entities.customers)
    event_counts: Counter[str] = Counter()
    entity_names = set(entities.all_tables())
    behavior_started = time.perf_counter()

    def counted_records() -> Iterator[dict[str, object]]:
        try:
            records = iter_scale_records(
                config,
                entities,
                simulation_run_id=base_manifest.run_id,
                stage_timings=stage_timings,
            )
            for row in records:
                logical_type = str(row.get("logical_type", ""))
                if logical_type in entity_names:
                    entity_counts[logical_type] = entity_counts.get(logical_type, 0) + 1
                elif logical_type == "payments":
                    event_counts["payments"] += 1
                elif logical_type == "payment_events":
                    event_counts["payment_events"] += 1
                    event_type = row.get("event_type")
                    if event_type:
                        event_counts[str(event_type)] += 1
                    if event_type in CARD_LIFECYCLE_EVENT_TYPES:
                        event_counts["card_lifecycle_events"] += 1
                    if event_type in PIX_LIFECYCLE_EVENT_TYPES:
                        event_counts["pix_lifecycle_events"] += 1
                elif logical_type == "ledger_entries":
                    event_counts["ledger_entries"] += 1
                yield row
        finally:
            finish_stage(stage_timings, "behavior_generation", behavior_started)

    with measure_stage(stage_timings, "scale_serialization"):
        completions, reconciliation, checkpoint_path = write_scale_partitions(
            Path(output_dir) / base_manifest.run_id,
            plan,
            counted_records(),
            checkpoint_dir=checkpoint_dir,
            resolved_configuration=config.model_dump(mode="json"),
            stage_timings=stage_timings,
        )
    stage_timings.setdefault(
        "fraud_graph_quality",
        {
            "elapsed_seconds": 0.0,
            "peak_rss_mb": stage_timings["scale_serialization"]["peak_rss_mb"],
        },
    )
    run_dir = Path(output_dir) / base_manifest.run_id
    scale_metadata: dict[str, object] = {
        "profile": plan.profile,
        "target_unit": "payments",
        "target_payments": plan.target_payments,
        "payments_realized": event_counts.get("payments", 0),
        "target_met": event_counts.get("payments", 0) >= plan.target_payments,
        "target_logical_events": plan.target_logical_events,
        "logical_events_realized": reconciliation.logical_row_count,
        "shard_count": plan.shard_count,
        "chunk_size": plan.chunk_size,
        "worker_count": plan.worker_count,
        "output_batch_size": plan.output_batch_size,
        "checkpoint_frequency_chunks": plan.checkpoint_frequency_chunks,
        "partition_mapping": plan.partition_mapping,
        "seed_tree_version": plan.seed_tree_version,
        "configuration_hash": plan.configuration_hash,
        "features": list(plan.features),
        "storage_backend": plan.storage_backend,
        "storage_uri": plan.storage_uri,
        "state_backend": plan.state_backend,
        "manifest_version": plan.manifest_version,
        "execution_mode": "streaming-core",
        "partitions": [item.model_dump(mode="json") for item in completions],
        "partition_fingerprints": {item.shard_id: item.fingerprint for item in completions},
        "checkpoint": str(checkpoint_path),
        "reconciliation": reconciliation.model_dump(mode="json"),
        "derived_counts": dict(event_counts),
    }
    schema_versions = {
        **{entity_name: "1" for entity_name in entity_counts},
        "payments": "2",
        "payment_events": "5",
        "ledger_entries": "1",
    }
    manifest = base_manifest.model_copy(
        update={
            "entity_counts": entity_counts,
            "event_counts": dict(event_counts),
            "schema_versions": schema_versions,
            "fraud_counts": {},
            "fraud_rates": {},
            "quality_fault_counts": {},
            "quality_fault_rates": {},
            "quality_diagnostics": {},
            "graph": {},
            "scale": scale_metadata,
        }
    )
    output_metadata = _output_fingerprints(run_dir)
    manifest = manifest.model_copy(
        update={
            "output_artifacts": output_metadata,
            "schema_fingerprint": sha256_json(manifest.schema_versions),
            "output_fingerprint": output_metadata["output_fingerprint"],
            "file_checksums": output_metadata["file_checksums"],
        }
    )
    manifest_path = write_manifest(manifest, Path(output_dir))
    return GeneratedRun(
        manifest=manifest,
        run_dir=run_dir,
        manifest_path=manifest_path,
        stage_timings=stage_timings,
    )


def generate_scale(
    config: str | Path | SimulationRunConfig,
    *,
    output_dir: str | Path = "runs",
    checkpoint_dir: str | Path | None = None,
    profile: str | Path | CalibrationProfile | None = None,
    seed: int | None = None,
    workers: int | None = None,
) -> GeneratedRun:
    """Run an explicitly requested scale job using the scale manifest path.

    This entry point preserves the established generation semantics while
    making scale execution explicit. Large profiles should be run through this
    API/CLI; the compatibility ``generate`` API remains available for small
    in-memory callers.
    """

    resolved = _resolve_config(config, Path(profile) if isinstance(profile, str | Path) else None)
    if not resolved.scale.enabled:
        raise ValueError("generate_scale requires an enabled scale profile")
    result = generate(
        resolved,
        write=True,
        output_dir=output_dir,
        profile=profile,
        seed=seed,
        workers=workers,
        checkpoint_dir=checkpoint_dir,
    )
    if resolved.scale.storage_uri:
        destination = Path(resolved.scale.storage_uri)
        # A local URI may point at the staging run itself; avoid copying files
        # onto themselves while still allowing a separate local publication
        # directory. Remote backends are resolved lazily through fsspec.
        if resolved.scale.storage_backend == "local":
            same_path = destination.resolve() == result.run_dir.resolve()
        else:
            same_path = False
        if not same_path:
            storage_for(resolved.scale.storage_uri, resolved.scale.storage_backend).publish(
                result.run_dir, resolved.scale.storage_uri
            )
    return result


def _scale_metadata(
    config: SimulationRunConfig,
    entities: EntityDataset,
    behavior: BehaviorDataset,
    run_dir: Path,
    *,
    write: bool,
    run_id: str,
    checkpoint_dir: str | Path | None,
    stage_timings: StageMetrics | None = None,
) -> dict[str, object] | None:
    plan = resolve_scale_plan(config, run_id=run_id)
    if plan is None:
        return None
    realized_logical_rows = sum(len(rows) for rows in behavior.tables().values())
    if write and len(behavior.payments) < plan.target_payments:
        raise ValueError(
            "scale target not met: expected at least "
            f"{plan.target_payments} payments, realized {len(behavior.payments)}"
        )
    metadata: dict[str, object] = {
        "profile": plan.profile,
        "target_unit": "payments",
        "target_payments": plan.target_payments,
        "payments_realized": len(behavior.payments),
        "target_met": len(behavior.payments) >= plan.target_payments,
        "target_logical_events": plan.target_logical_events,
        "logical_events_realized": realized_logical_rows,
        "shard_count": plan.shard_count,
        "chunk_size": plan.chunk_size,
        "worker_count": plan.worker_count,
        "output_batch_size": plan.output_batch_size,
        "checkpoint_frequency_chunks": plan.checkpoint_frequency_chunks,
        "partition_mapping": plan.partition_mapping,
        "seed_tree_version": plan.seed_tree_version,
        "configuration_hash": plan.configuration_hash,
        "features": list(plan.features),
        "storage_backend": plan.storage_backend,
        "storage_uri": plan.storage_uri,
        "state_backend": plan.state_backend,
        "manifest_version": plan.manifest_version,
        "execution_mode": "compatibility-materialized",
    }
    if write:
        with measure_stage(stage_timings, "scale_serialization"):
            completions, reconciliation, checkpoint_path = write_scale_partitions(
                run_dir,
                plan,
                _scale_records(entities, behavior),
                checkpoint_dir=checkpoint_dir,
                resolved_configuration=config.model_dump(mode="json"),
                stage_timings=stage_timings,
            )
        metadata.update(
            {
                "partitions": [item.model_dump(mode="json") for item in completions],
                "partition_fingerprints": {item.shard_id: item.fingerprint for item in completions},
                "checkpoint": str(checkpoint_path),
                "reconciliation": reconciliation.model_dump(mode="json"),
                "derived_counts": behavior.event_counts,
                "target_met": len(behavior.payments) >= plan.target_payments,
            }
        )
    return metadata


@overload
def generate(
    config: str | Path | SimulationRunConfig | None = None,
    *,
    write: Literal[False] = False,
    output_dir: str | Path = "runs",
    profile: str | Path | CalibrationProfile | None = None,
    seed: int | None = None,
    workers: int | None = None,
    checkpoint_dir: str | Path | None = None,
) -> GeneratedData: ...


@overload
def generate(
    config: str | Path | SimulationRunConfig | None = None,
    *,
    write: Literal[True],
    output_dir: str | Path = "runs",
    profile: str | Path | CalibrationProfile | None = None,
    seed: int | None = None,
    workers: int | None = None,
    checkpoint_dir: str | Path | None = None,
) -> GeneratedRun: ...


def generate(
    config: str | Path | SimulationRunConfig | None = None,
    *,
    write: bool = False,
    output_dir: str | Path = "runs",
    profile: str | Path | CalibrationProfile | None = None,
    seed: int | None = None,
    workers: int | None = None,
    checkpoint_dir: str | Path | None = None,
) -> GeneratedData | GeneratedRun:
    """Generate a deterministic FraudTwin run.

    If ``config`` is omitted, the built-in minimal configuration is used.
    By default, generated data is returned in memory without writing files.
    Set ``write=True`` to preserve the CLI's Parquet and manifest output layout.
    """

    profile_path = Path(profile) if isinstance(profile, str | Path) else None
    resolved_config = _apply_generation_overrides(
        _resolve_config(config, profile_path), seed=seed, workers=workers
    )
    supplied_profile = (
        load_calibration_profile(profile_path)
        if profile_path is not None
        else cast(CalibrationProfile | None, profile)
    )
    calibration = resolve_calibration(resolved_config, supplied_profile)
    stage_timings: StageMetrics = {}
    base_manifest = create_manifest(resolved_config)
    if calibration.enabled:
        resolved_configuration = dict(base_manifest.resolved_configuration)
        calibration_configuration = resolved_config.calibration.model_dump(mode="json")
        calibration_configuration.pop("profile", None)
        calibration_configuration["profile_id"] = calibration.profile_id
        resolved_configuration["calibration"] = calibration_configuration
        base_manifest = base_manifest.model_copy(
            update={
                "run_id": f"RUN-{calibration.effective_configuration_hash[:16]}",
                "scenario_config_hash": calibration.effective_configuration_hash,
                "resolved_configuration": resolved_configuration,
            }
        )
    if write and _streaming_core_scale_supported(resolved_config):
        return _generate_streaming_core_scale(
            resolved_config,
            base_manifest,
            output_dir=output_dir,
            checkpoint_dir=checkpoint_dir,
            stage_timings=stage_timings,
        )
    with measure_stage(stage_timings, "entity_generation"):
        entities = EntityGenerator(resolved_config, calibration).generate()
    with measure_stage(stage_timings, "behavior_generation"):
        behavior = BehaviorGenerator(
            resolved_config,
            entities,
            simulation_run_id=base_manifest.run_id,
            calibration=calibration,
            stage_timings=stage_timings,
        ).generate()
    scale_plan = resolve_scale_plan(resolved_config, run_id=base_manifest.run_id)
    if write and scale_plan is not None and len(behavior.payments) < scale_plan.target_payments:
        raise ValueError(
            "scale target not met: expected at least "
            f"{scale_plan.target_payments} payments, realized {len(behavior.payments)}"
        )

    output_root = Path(output_dir)
    run_dir = output_root / base_manifest.run_id
    if write and resolved_config.outputs.postgres:
        if resolved_config.quality.profile != "clean":
            raise ValueError("PostgreSQL output currently requires quality.profile: clean")
        ensure_database_ready()
    kafka_publisher = None
    lakehouse_environment = None
    if write and resolved_config.outputs.kafka:
        if resolved_config.quality.profile != "clean":
            raise ValueError("Kafka output currently requires quality.profile: clean")
        # Validate and reconcile contracts before writing files or producing data.
        kafka_publisher = publisher_from_environment(config=resolved_config.kafka)
        kafka_publisher.prepare()
    if write and resolved_config.outputs.iceberg:
        from fraudtwin.lakehouse import LakehouseEnvironment

        if resolved_config.quality.profile != "clean":
            raise ValueError("Iceberg output currently requires quality.profile: clean")
        lakehouse_environment = LakehouseEnvironment.from_environment(resolved_config.lakehouse)
    if write and calibration.enabled and run_dir.exists():
        raise FileExistsError(f"calibrated run artifacts already exist: {run_dir}")
    fresh_output = not run_dir.exists()
    if write and (fresh_output or not resolved_config.scale.enabled):
        with measure_stage(stage_timings, "serialization"):
            _write_base_outputs(entities, behavior, run_dir)

    scale_metadata = _scale_metadata(
        resolved_config,
        entities,
        behavior,
        run_dir,
        write=write,
        run_id=base_manifest.run_id,
        checkpoint_dir=checkpoint_dir,
        stage_timings=stage_timings,
    )

    label_observation_metadata = _label_observation_metadata(
        behavior, run_dir, write=write and fresh_output, run_id=base_manifest.run_id
    )

    campaign_dynamics_metadata = _campaign_metadata(
        behavior, run_dir, write=write and fresh_output, run_id=base_manifest.run_id
    )
    counterfactual_metadata = _counterfactual_metadata(
        behavior, run_dir, write=write and fresh_output
    )

    manifest = _build_manifest(
        resolved_config,
        base_manifest,
        entities,
        behavior,
        campaign_dynamics_metadata=campaign_dynamics_metadata,
        counterfactual_metadata=counterfactual_metadata,
        label_observation_metadata=label_observation_metadata,
        scale_metadata=scale_metadata,
        calibration=calibration,
    )

    postgres_metadata: dict[str, object] | None = None
    if write and resolved_config.outputs.postgres:
        persistence = persist_run(entities, behavior, manifest)
        postgres_metadata = {
            "schema_version": persistence.schema_version,
            "row_counts": persistence.row_counts,
            "logical_fingerprint": persistence.logical_fingerprint,
            "idempotent": persistence.idempotent,
        }
        manifest = manifest.model_copy(update={"postgres": postgres_metadata})

    if write and kafka_publisher is not None:
        publication = kafka_publisher.publish(
            behavior,
            manifest.run_id,
            mode=resolved_config.simulation.speed,
        )
        manifest = manifest.model_copy(
            update={
                "kafka": {
                    "topics": publication.topics,
                    "record_counts": publication.record_counts,
                    "mode": publication.mode,
                    "max_events_per_second": publication.max_events_per_second,
                    "accelerated_time_multiplier": publication.accelerated_time_multiplier,
                    "publication_fingerprint": publication.publication_fingerprint,
                }
            }
        )

    dataset: PointInTimeDataset | None = None
    dataset_path: Path | None = None
    dataset_manifest_path: Path | None = None
    pit_enabled = resolved_config.dataset.enabled and (
        not resolved_config.scale.enabled or "pit" in resolved_config.scale.features
    )
    if pit_enabled:
        with measure_stage(stage_timings, "pit_dataset"):
            dataset = PointInTimeDatasetBuilder(
                resolved_config,
                entities,
                behavior,
                manifest,
            ).build()
        if write:
            dataset_path, dataset_manifest_path = write_point_in_time_dataset(
                dataset,
                run_dir / "ml" / "dataset.parquet",
            )

    if calibration.enabled and calibration.profile:
        report = compute_fidelity_report(
            calibration.profile,
            {summary.name: True for summary in calibration.profile.summaries},
            weights=resolved_config.calibration.weights,
            minimum_scores=resolved_config.calibration.minimum_scores,
        )
        if write:
            metrics_path, report_path = write_calibration_artifacts(
                calibration.profile, report, run_dir
            )
            manifest = manifest.model_copy(
                update={
                    "calibration": {
                        **(manifest.calibration or {}),
                        "fidelity_metrics": str(metrics_path.relative_to(run_dir)),
                        "fidelity_report": str(report_path.relative_to(run_dir)),
                        "report_fingerprint": report.report_fingerprint,
                        "composite_score": report.composite_score,
                    }
                }
            )

    if write and (
        calibration.enabled or resolved_config.labels.enabled or scale_metadata is not None
    ):
        output_metadata = _output_fingerprints(run_dir)
        manifest = manifest.model_copy(
            update={
                "output_artifacts": output_metadata,
                "schema_fingerprint": sha256_json(manifest.schema_versions),
                "output_fingerprint": output_metadata["output_fingerprint"],
                "file_checksums": output_metadata["file_checksums"],
            }
        )
        if calibration.enabled:
            manifest = manifest.model_copy(
                update={
                    "calibration": {
                        **(manifest.calibration or {}),
                        "output_fingerprint": output_metadata["output_fingerprint"],
                    }
                }
            )

    # Lakehouse publication is an optional sink.  Materialize from the already
    # generated in-memory objects so the source manifest can include its
    # immutable snapshot metadata before it is first published.
    if write and resolved_config.outputs.iceberg:
        from fraudtwin.lakehouse import materialize_dataset

        lakehouse_result = materialize_dataset(
            run_dir,
            entities,
            behavior,
            manifest,
            config=resolved_config.lakehouse,
            environment=lakehouse_environment,
            write_iceberg=True,
        )
        manifest = manifest.model_copy(update={"lakehouse": lakehouse_result.as_dict()})

    if not write:
        return GeneratedData(
            manifest=manifest,
            entities=entities,
            behavior=behavior,
            dataset=dataset,
            stage_timings=stage_timings,
        )

    manifest_path = write_manifest(manifest, output_root)
    return GeneratedRun(
        manifest=manifest,
        run_dir=run_dir,
        manifest_path=manifest_path,
        dataset_path=dataset_path,
        dataset_manifest_path=dataset_manifest_path,
        stage_timings=stage_timings,
    )


def resume_generation(checkpoint: str | Path) -> GeneratedRun:
    """Resume a scale run from a validated checkpoint manifest.

    Canonical partition artifacts are idempotently regenerated from the stored
    resolved configuration.  Completed partition fingerprints are checked by
    the caller/consumer through the resulting manifest and checkpoint.
    """

    checkpoint_path = Path(checkpoint)
    state: ScaleCheckpoint = load_checkpoint(checkpoint_path)
    config = SimulationRunConfig.model_validate(state.resolved_configuration)
    if not config.scale.enabled:
        raise ValueError("checkpoint configuration does not enable scale generation")
    if config_hash(config) != state.configuration_hash:
        raise ValueError("checkpoint configuration hash does not match resolved configuration")
    result = generate(
        config,
        write=True,
        output_dir=Path(state.run_dir).parent,
        checkpoint_dir=checkpoint_path.parent,
    )
    if result.run_id != state.run_id:
        raise ValueError("resumed run ID does not match checkpoint")
    return result


__all__ = [
    "GeneratedData",
    "GeneratedRun",
    "generate",
    "generate_scale",
    "iter_scale_run",
    "iter_scale_records",
    "resume_generation",
]
