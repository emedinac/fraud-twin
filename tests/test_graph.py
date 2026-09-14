"""Focused Milestone 11 graph contracts."""

from pathlib import Path

import pytest
from typer.testing import CliRunner

from fraudtwin.cli import app
from fraudtwin.config import GraphConfig, SimulationRunConfig, load_config
from fraudtwin.graph import (
    GRAPH_EDGE_SCHEMA,
    GRAPH_NODE_SCHEMA,
    build_graph,
    validate_graph,
    write_graph,
)
from fraudtwin.manifest import create_manifest, write_manifest
from fraudtwin.ml import load_generated_run
from fraudtwin.simulation import BehaviorGenerator, EntityGenerator
from fraudtwin.simulation.parquet import (
    write_behavior_parquet,
    write_entity_parquet,
    write_graph_truth,
)

CONFIG_PATH = Path("configs/minimal.yaml")
runner = CliRunner()


def _config(*, graph: bool = False) -> SimulationRunConfig:
    if graph:
        return load_config(Path("configs/benchmarks/m11-graph-v2.yaml"))
    return load_config(CONFIG_PATH)


def _run(tmp_path: Path, *, graph: bool = False) -> tuple[Path, SimulationRunConfig]:
    config = _config(graph=graph)
    manifest = create_manifest(config)
    entities = EntityGenerator(config).generate()
    behavior = BehaviorGenerator(config, entities, simulation_run_id=manifest.run_id).generate()
    run_dir = tmp_path / manifest.run_id
    write_entity_parquet(entities, run_dir)
    write_behavior_parquet(behavior, run_dir)
    write_graph_truth(behavior.graph_memberships, behavior.graph_patterns, run_dir)
    write_manifest(manifest, tmp_path)
    return run_dir, config


def test_graph_config_is_strict_and_legacy_identity_is_unchanged() -> None:
    config = _config()
    assert config.graph.enabled is False
    assert GraphConfig(relationship_window_seconds="2h").relationship_window_seconds == 7_200
    with pytest.raises(ValueError):
        GraphConfig(ring_min_size=6, ring_max_size=3)


def test_graph_build_is_deterministic_and_preserves_source_ids(tmp_path: Path) -> None:
    run_dir, config = _run(tmp_path, graph=True)
    entities, behavior, manifest = load_generated_run(run_dir)
    first = build_graph(config, entities, behavior, manifest, view="oracle")
    second = build_graph(config, entities, behavior, manifest, view="oracle")
    assert first == second
    assert first.output_fingerprint == second.output_fingerprint
    assert {node.node_id for node in first.nodes}.issuperset(
        {item.account_id for item in entities.accounts}
    )
    assert {edge.payment_id for edge in first.edges if edge.payment_id}.issubset(
        {item.payment_id for item in behavior.payments}
    )
    assert first.node_frame.schema == GRAPH_NODE_SCHEMA
    assert first.edge_frame.schema == GRAPH_EDGE_SCHEMA
    validate_graph(first)


def test_observable_graph_respects_availability_and_separates_truth(tmp_path: Path) -> None:
    run_dir, config = _run(tmp_path, graph=True)
    entities, behavior, manifest = load_generated_run(run_dir)
    cutoff = min(event.source_available_at for event in behavior.payment_events)
    observable = build_graph(config, entities, behavior, manifest, view="observable", as_of=cutoff)
    oracle = build_graph(
        config,
        entities,
        behavior,
        manifest,
        view="oracle",
        as_of=cutoff,
        memberships=behavior.graph_memberships,
    )
    assert all(
        edge.available_at is None or edge.available_at <= cutoff for edge in observable.edges
    )
    assert not observable.memberships
    assert oracle.memberships


def test_graph_export_is_append_only_and_cli_supports_temporary_runs(tmp_path: Path) -> None:
    run_dir, config = _run(tmp_path, graph=False)
    entities, behavior, manifest = load_generated_run(run_dir)
    datasets = {
        "observable": build_graph(config, entities, behavior, manifest, view="observable"),
        "oracle": build_graph(config, entities, behavior, manifest, view="oracle"),
    }
    destination, export_manifest = write_graph(
        datasets,
        run_dir / "graph-check",
        source_manifest=manifest,
        config=config,
    )
    assert (destination / "observable" / "nodes.parquet").is_file()
    assert export_manifest.output_fingerprint
    with pytest.raises(FileExistsError):
        write_graph(
            datasets,
            run_dir / "graph-check",
            source_manifest=manifest,
            config=config,
        )
    result = runner.invoke(
        app,
        ["graph", "export", "--run-id", run_dir.name, "--output-dir", str(tmp_path)],
    )
    assert result.exit_code == 0, result.stdout
