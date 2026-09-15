"""High-level Python API for generating deterministic FraudTwin runs."""

import hashlib
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import timedelta
from importlib.resources import files
from pathlib import Path
from typing import Literal, cast, overload

import yaml

from fraudtwin.calibration import (
    CalibrationProfile,
    ResolvedCalibration,
    compute_fidelity_report,
    load_calibration_profile,
    resolve_calibration,
)
from fraudtwin.config import SimulationRunConfig, config_hash, load_config
from fraudtwin.difficulty import difficulty_metadata
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


@dataclass(frozen=True)
class GeneratedData:
    """Generated FraudTwin data kept in memory."""

    manifest: RunManifest
    entities: EntityDataset
    behavior: BehaviorDataset
    dataset: PointInTimeDataset | None = None

    @property
    def run_id(self) -> str:
        """Return the deterministic identifier for this generated run."""

        return self.manifest.run_id


@dataclass(frozen=True)
class GeneratedRun:
    """Metadata and paths for a generated run written to disk."""

    manifest: RunManifest
    run_dir: Path
    manifest_path: Path
    dataset_path: Path | None = None
    dataset_manifest_path: Path | None = None

    @property
    def run_id(self) -> str:
        """Return the deterministic identifier for this generated run."""

        return self.manifest.run_id


def _resolve_config(
    config: str | Path | SimulationRunConfig | None,
    profile_override: Path | None = None,
) -> SimulationRunConfig:
    if config is None:
        default_config = files("fraudtwin").joinpath("defaults", "minimal.yaml")
        raw_config = yaml.safe_load(default_config.read_text(encoding="utf-8"))
        if not isinstance(raw_config, dict):
            raise ValueError("the built-in minimal configuration must be a mapping")
        return SimulationRunConfig.model_validate(raw_config)
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
        "configuration_hash": config_hash(config),
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


def _scale_records(
    entities: EntityDataset, behavior: BehaviorDataset
) -> Iterator[dict[str, object]]:
    """Yield stable logical records for the M18 partition writer."""

    ordinal = 0
    for table_name, table_rows in {**entities.all_tables(), **behavior.tables()}.items():
        for row in table_rows:
            values = row.model_dump(mode="json")
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
            # Keep the canonical row fields in the partition artifact.  The
            # logical metadata is additive and lets the mixed scale reader
            # route/filter rows without retaining a global ID index.
            yield {
                **values,
                "logical_id": f"{table_name}:{source_id}",
                "logical_type": table_name,
                "source_id": source_id,
                "partition_key": owner_id,
            }
            ordinal += 1
            # Cross-account transfers are owned by the payer shard.  Emit a
            # deterministic payee-side reconciliation marker so downstream
            # consumers can close the cross-shard balance without a global
            # account map.
            payer = values.get("payer_account_id")
            payee = values.get("payee_account_id")
            if payer and payee and payer != payee and values.get("payment_id"):
                payment_id = str(values["payment_id"])
                yield {
                    "logical_id": f"transfer_reconciliation:{payment_id}",
                    "logical_type": "transfer_reconciliation",
                    "source_id": payment_id,
                    "payment_id": payment_id,
                    "payer_account_id": str(payer),
                    "payee_account_id": str(payee),
                    "amount": values.get("amount"),
                    "partition_key": str(payee),
                    "reconciliation": "PAYEE_SIDE",
                }
                ordinal += 1


def _scale_metadata(
    config: SimulationRunConfig,
    entities: EntityDataset,
    behavior: BehaviorDataset,
    run_dir: Path,
    *,
    write: bool,
    run_id: str,
    checkpoint_dir: str | Path | None,
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
    }
    if write:
        completions, reconciliation, checkpoint_path = write_scale_partitions(
            run_dir,
            plan,
            _scale_records(entities, behavior),
            checkpoint_dir=checkpoint_dir,
            resolved_configuration=config.model_dump(mode="json"),
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
    resolved_config = _resolve_config(config, profile_path)
    if seed is not None:
        values = resolved_config.model_dump(mode="python")
        values["simulation"]["seed"] = seed
        resolved_config = SimulationRunConfig.model_validate(values)
    if workers is not None:
        if workers < 1:
            raise ValueError("workers must be positive")
        if not resolved_config.scale.enabled:
            raise ValueError("workers requires an enabled scale profile")
        values = resolved_config.model_dump(mode="python")
        values["scale"]["worker_count"] = workers
        resolved_config = SimulationRunConfig.model_validate(values)
    supplied_profile = (
        load_calibration_profile(profile_path)
        if profile_path is not None
        else cast(CalibrationProfile | None, profile)
    )
    calibration = resolve_calibration(resolved_config, supplied_profile)
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
    entities = EntityGenerator(resolved_config, calibration).generate()
    behavior = BehaviorGenerator(
        resolved_config,
        entities,
        simulation_run_id=base_manifest.run_id,
        calibration=calibration,
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
        _write_base_outputs(entities, behavior, run_dir)

    scale_metadata = _scale_metadata(
        resolved_config,
        entities,
        behavior,
        run_dir,
        write=write,
        run_id=base_manifest.run_id,
        checkpoint_dir=checkpoint_dir,
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
    if resolved_config.dataset.enabled:
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
        )

    manifest_path = write_manifest(manifest, output_root)
    return GeneratedRun(
        manifest=manifest,
        run_dir=run_dir,
        manifest_path=manifest_path,
        dataset_path=dataset_path,
        dataset_manifest_path=dataset_manifest_path,
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


__all__ = ["GeneratedData", "GeneratedRun", "generate", "resume_generation"]
