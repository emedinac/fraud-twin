"""High-level Python API for generating deterministic FraudTwin runs."""

from dataclasses import dataclass
from datetime import timedelta
from importlib.resources import files
from pathlib import Path
from typing import Literal, overload

import yaml

from fraudtwin.config import SimulationRunConfig, config_hash, load_config
from fraudtwin.difficulty import difficulty_metadata
from fraudtwin.graph import build_graph
from fraudtwin.manifest import RunManifest, create_manifest, write_manifest
from fraudtwin.ml import (
    PointInTimeDataset,
    PointInTimeDatasetBuilder,
    write_point_in_time_dataset,
)
from fraudtwin.reproducibility import sha256_json
from fraudtwin.simulation import BehaviorGenerator, EntityGenerator
from fraudtwin.simulation.behavior import BehaviorDataset
from fraudtwin.simulation.generator import EntityDataset
from fraudtwin.simulation.parquet import (
    write_behavior_parquet,
    write_campaign_dynamics_sidecar,
    write_counterfactual_sidecar,
    write_entity_parquet,
    write_graph_truth,
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


def _resolve_config(config: str | Path | SimulationRunConfig | None) -> SimulationRunConfig:
    if config is None:
        default_config = files("fraudtwin").joinpath("defaults", "minimal.yaml")
        raw_config = yaml.safe_load(default_config.read_text(encoding="utf-8"))
        if not isinstance(raw_config, dict):
            raise ValueError("the built-in minimal configuration must be a mapping")
        return SimulationRunConfig.model_validate(raw_config)
    if isinstance(config, SimulationRunConfig):
        return config
    return load_config(Path(config))


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
                config.simulation.start
                + timedelta(days=config.simulation.duration_days)
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
        }
    )


@overload
def generate(
    config: str | Path | SimulationRunConfig | None = None,
    *,
    write: Literal[False] = False,
    output_dir: str | Path = "runs",
) -> GeneratedData: ...


@overload
def generate(
    config: str | Path | SimulationRunConfig | None = None,
    *,
    write: Literal[True],
    output_dir: str | Path = "runs",
) -> GeneratedRun: ...


def generate(
    config: str | Path | SimulationRunConfig | None = None,
    *,
    write: bool = False,
    output_dir: str | Path = "runs",
) -> GeneratedData | GeneratedRun:
    """Generate a deterministic FraudTwin run.

    If ``config`` is omitted, the built-in minimal configuration is used.
    By default, generated data is returned in memory without writing files.
    Set ``write=True`` to preserve the CLI's Parquet and manifest output layout.
    """

    resolved_config = _resolve_config(config)
    base_manifest = create_manifest(resolved_config)
    entities = EntityGenerator(resolved_config).generate()
    behavior = BehaviorGenerator(
        resolved_config,
        entities,
        simulation_run_id=base_manifest.run_id,
    ).generate()

    output_root = Path(output_dir)
    run_dir = output_root / base_manifest.run_id
    campaign_dynamics_metadata: dict[str, object] | None = None
    counterfactual_metadata: dict[str, object] | None = None

    if write:
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

    if behavior.campaign_dynamics is not None:
        if write:
            dynamic_root, dynamic_manifest_path = write_campaign_dynamics_sidecar(
                behavior.campaign_dynamics,
                run_dir,
                source_run_id=base_manifest.run_id,
            )
            campaign_dynamics_metadata = {
                "root": str(dynamic_root.relative_to(run_dir)),
                "manifest": str(dynamic_manifest_path.relative_to(run_dir)),
            }
        campaign_dynamics_metadata = {
            **(campaign_dynamics_metadata or {}),
            "configuration_hash": behavior.campaign_dynamics.configuration_hash,
            "stream_ids": list(behavior.campaign_dynamics.stream_ids),
            "transitions": len(behavior.campaign_dynamics.transitions),
            "snapshots": len(behavior.campaign_dynamics.snapshots),
        }

    if behavior.counterfactual is not None:
        if write:
            counterfactual_root, counterfactual_manifest_path = write_counterfactual_sidecar(
                behavior.counterfactual, run_dir
            )
            counterfactual_metadata = {
                **behavior.counterfactual.metadata,
                "counterfactual_id": behavior.counterfactual.counterfactual_id,
                "root": str(counterfactual_root),
                "manifest": str(counterfactual_manifest_path),
            }
        else:
            counterfactual_metadata = {
                **behavior.counterfactual.metadata,
                "counterfactual_id": behavior.counterfactual.counterfactual_id,
            }
        counterfactual_metadata.update(
            {
                "accepted": len(behavior.counterfactual.modified_payments),
                "rejected": len(behavior.counterfactual.rejected),
            }
        )

    manifest = _build_manifest(
        resolved_config,
        base_manifest,
        entities,
        behavior,
        campaign_dynamics_metadata=campaign_dynamics_metadata,
        counterfactual_metadata=counterfactual_metadata,
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


__all__ = ["GeneratedData", "GeneratedRun", "generate"]
