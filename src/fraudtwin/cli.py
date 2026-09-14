import json
from datetime import datetime
from pathlib import Path
from typing import Annotated, Literal, cast

import polars as pl
import typer

from fraudtwin.config import SimulationRunConfig, config_hash, load_config
from fraudtwin.domain import Account, LedgerEntry, Payment, PaymentEvent, validate_ledger
from fraudtwin.generation import generate as generate_library
from fraudtwin.graph import GraphDataset, build_graph, validate_graph, write_graph
from fraudtwin.ml import (
    BenchmarkPack,
    PointInTimeDatasetBuilder,
    load_benchmark_pack,
    load_generated_run,
    run_backtest,
    write_backtest,
    write_point_in_time_dataset,
)
from fraudtwin.replay import ReplayOrder, replay_run, write_replay
from fraudtwin.simulation.graph_fraud import GraphFraudDataset
from fraudtwin.simulation.parquet import (
    write_campaign_dynamics_sidecar,
    write_counterfactual_sidecar,
)

app = typer.Typer(help="Synthetic financial-system and fraud digital twin.")
config_app = typer.Typer(help="Validate simulation configuration.")
ml_app = typer.Typer(help="Build local point-in-time ML datasets.")
app.add_typer(config_app, name="config")
app.add_typer(ml_app, name="ml")
graph_app = typer.Typer(help="Build deterministic temporal graph views.")
app.add_typer(graph_app, name="graph")
counterfactual_app = typer.Typer(help="Generate deterministic M14 counterfactual sidecars.")
campaign_app = typer.Typer(help="Evolve deterministic M15 campaign sidecars.")
app.add_typer(counterfactual_app, name="counterfactual")
app.add_typer(campaign_app, name="campaign")


def _load_or_exit(path: Path) -> SimulationRunConfig:
    try:
        return load_config(path)
    except (FileNotFoundError, ValueError) as exc:
        typer.echo(f"Configuration error: {exc}", err=True)
        raise typer.Exit(code=1) from exc


def _parse_optional_timestamp(value: str | None) -> datetime | None:
    """Parse an optional CLI timestamp, preserving ``None`` for omitted filters."""

    return datetime.fromisoformat(value) if value is not None else None


@config_app.command("validate")
def validate_config(
    path: Annotated[Path, typer.Argument(help="YAML configuration file.")],
) -> None:
    """Validate a simulation configuration without running it."""

    config = _load_or_exit(path)
    typer.echo("Configuration is valid.")
    typer.echo(f"Seed: {config.simulation.seed}")
    typer.echo(f"Configuration hash: {config_hash(config)}")


@app.command()
def generate(
    path: Annotated[Path, typer.Argument(help="YAML configuration file.")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="Directory in which to store run manifests."),
    ] = Path("runs"),
) -> None:
    """Validate a configuration and generate a reproducible batch dataset."""

    config = _load_or_exit(path)
    result = generate_library(config, write=True, output_dir=output_dir)
    manifest = result.manifest
    entity_counts = manifest.entity_counts
    event_counts = manifest.event_counts
    manifest_path = result.manifest_path
    typer.echo(f"Run generated: {manifest.run_id}")
    typer.echo(f"Manifest: {manifest_path}")
    if result.dataset_path is not None and result.dataset_manifest_path is not None:
        typer.echo(f"Dataset: {result.dataset_path}")
        typer.echo(f"Dataset manifest: {result.dataset_manifest_path}")
    typer.echo("Generated entity counts:")
    for entity_name, count in entity_counts.items():
        typer.echo(f"  {entity_name}: {count}")
    typer.echo("Generated payment counts:")
    for event_name, count in event_counts.items():
        typer.echo(f"  {event_name}: {count}")


@campaign_app.command("evolve")
def evolve_campaign_command(
    config_path: Annotated[Path, typer.Option("--config", help="M15 configuration YAML file.")],
    source_run_id: Annotated[str, typer.Option("--source-run-id", help="Static source run ID.")],
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="Directory containing the source run.")
    ] = Path("runs"),
) -> None:
    """Append an M15 sidecar to a compatible clean static graph run."""

    config = _load_or_exit(config_path)
    source_dir = output_dir / source_run_id
    try:
        entities, behavior, source_manifest = load_generated_run(source_dir)
        source_config = SimulationRunConfig.model_validate(source_manifest.resolved_configuration)
        if source_config.campaign_dynamics.active or source_config.stress.active:
            raise ValueError("standalone M15 requires a source run without M13/M15")
        if config.stress.active:
            raise ValueError("standalone M15 does not accept active M13 camouflage")
        if source_config.quality.profile != "clean" or config.quality.profile != "clean":
            raise ValueError("standalone M15 requires clean quality output")
        if not config.campaign_dynamics.active or not source_config.graph.enabled:
            raise ValueError("standalone M15 requires enabled campaign dynamics and graph source")
        source_values = source_config.model_dump(mode="json")
        requested_values = config.model_dump(mode="json")
        for section in (
            "simulation",
            "population",
            "payments",
            "behavior",
            "card_lifecycle",
            "pix_lifecycle",
            "fraud",
            "graph",
            "quality",
        ):
            if source_values.get(section) != requested_values.get(section):
                raise ValueError(f"standalone M15 base configuration mismatch in {section}")
        graph_dataset = GraphFraudDataset(
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
        from fraudtwin.campaign_dynamics import evolve_campaigns

        dynamic = evolve_campaigns(config, entities, graph_dataset, source_manifest.run_id)
        root, manifest_path = write_campaign_dynamics_sidecar(
            dynamic, source_dir, source_run_id=source_manifest.run_id
        )
    except (FileNotFoundError, OSError, ValueError) as exc:
        typer.echo(f"Campaign evolution failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    typer.echo(f"Campaign dynamics generated: {root.name}")
    typer.echo(f"Output: {root}")
    typer.echo(f"Manifest: {manifest_path}")


@counterfactual_app.command("generate")
def generate_counterfactual_command(
    config_path: Annotated[Path, typer.Option("--config", help="M14 configuration YAML file.")],
    source_run_id: Annotated[str, typer.Option("--source-run-id", help="Baseline source run ID.")],
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="Directory containing the source run.")
    ] = Path("runs"),
) -> None:
    """Append M14 counterfactuals to a legitimate-only generated run."""

    config = _load_or_exit(config_path)
    source_dir = output_dir / source_run_id
    try:
        entities, behavior, source_manifest = load_generated_run(source_dir)
        source_config = SimulationRunConfig.model_validate(source_manifest.resolved_configuration)
        if source_config.fraud.enabled or source_config.graph.enabled:
            raise ValueError("standalone M14 requires a source run with M6 and M11 disabled")
        if not config.counterfactual.active:
            raise ValueError("standalone M14 requires counterfactual.enabled=true")
        source_values = source_config.model_dump(mode="json")
        requested_values = config.model_dump(mode="json")
        for section in (
            "simulation",
            "population",
            "payments",
            "behavior",
            "card_lifecycle",
            "pix_lifecycle",
        ):
            if source_values.get(section) != requested_values.get(section):
                raise ValueError(f"standalone M14 base configuration mismatch in {section}")
        from fraudtwin.counterfactual import generate_counterfactuals as generate_cf
        from fraudtwin.simulation.payments import PaymentDataset

        dataset = generate_cf(
            config,
            entities,
            PaymentDataset(behavior.payments, behavior.payment_events, behavior.ledger_entries),
            run_id=source_manifest.run_id,
        )
        root, manifest_path = write_counterfactual_sidecar(dataset, source_dir)
    except (OSError, ValueError) as exc:
        typer.echo(f"Counterfactual generation failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    typer.echo(f"Counterfactual generated: {dataset.counterfactual_id}")
    typer.echo(f"Output: {root}")
    typer.echo(f"Manifest: {manifest_path}")


@app.command("validate-ledger")
def validate_ledger_command(
    run_id: Annotated[str, typer.Option("--run-id", help="Generated run identifier.")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="Directory containing generated runs."),
    ] = Path("runs"),
) -> None:
    """Validate transfer ledger entries for a generated run."""

    run_dir = output_dir / run_id
    try:
        accounts = tuple(
            Account.model_validate(row)
            for row in pl.read_parquet(run_dir / "entities" / "accounts.parquet").to_dicts()
        )
        payments = tuple(
            Payment.model_validate(row)
            for row in pl.read_parquet(run_dir / "payments" / "payments.parquet").to_dicts()
        )
        events = tuple(
            PaymentEvent.model_validate(row)
            for row in pl.read_parquet(run_dir / "payments" / "payment_events.parquet").to_dicts()
        )
        entries = tuple(
            LedgerEntry.model_validate(row)
            for row in pl.read_parquet(run_dir / "ledger" / "ledger_entries.parquet").to_dicts()
        )
        validate_ledger(accounts, payments, events, entries)
    except (FileNotFoundError, OSError, ValueError) as exc:
        typer.echo(f"Ledger validation failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    typer.echo(f"Ledger is valid for run {run_id}.")


@graph_app.command("export")
def export_graph_command(
    run_id: Annotated[str, typer.Option("--run-id", help="Existing generated run identifier.")],
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="Directory containing generated runs.")
    ] = Path("runs"),
    as_of: Annotated[
        str | None, typer.Option("--as-of", help="Optional point-in-time ISO-8601 snapshot.")
    ] = None,
    from_time: Annotated[
        str | None, typer.Option("--from", help="Inclusive ISO-8601 event range start.")
    ] = None,
    to_time: Annotated[
        str | None, typer.Option("--to", help="Exclusive ISO-8601 event range end.")
    ] = None,
    view: Annotated[str, typer.Option("--view", help="observable, oracle, or both.")] = "both",
    formats: Annotated[
        str, typer.Option("--format", help="Comma-separated formats: parquet,neo4j,pyg.")
    ] = "parquet,neo4j",
    json_output: Annotated[
        bool, typer.Option("--json", help="Emit machine-readable JSON.")
    ] = False,
) -> None:
    """Export observable and/or oracle graph views from one immutable run."""

    run_dir = output_dir / run_id
    try:
        entities, behavior, source_manifest = load_generated_run(run_dir)
        config = SimulationRunConfig.model_validate(source_manifest.resolved_configuration)
        selected_view = view.lower()
        if selected_view not in {"observable", "oracle", "both"}:
            raise ValueError("--view must be observable, oracle, or both")
        format_values = tuple(
            dict.fromkeys(item.strip().lower() for item in formats.split(",") if item.strip())
        )
        if not format_values or any(
            item not in {"parquet", "neo4j", "pyg"} for item in format_values
        ):
            raise ValueError("--format must contain only parquet, neo4j, or pyg")

        start = _parse_optional_timestamp(from_time)
        end = _parse_optional_timestamp(to_time)
        snapshot = _parse_optional_timestamp(as_of)
        views: tuple[Literal["observable", "oracle"], ...] = (
            ("observable", "oracle")
            if selected_view == "both"
            else (cast(Literal["observable", "oracle"], selected_view),)
        )
        datasets: dict[Literal["observable", "oracle"], GraphDataset] = {
            item: build_graph(
                config,
                entities,
                behavior,
                source_manifest,
                view=item,
                as_of=snapshot,
                from_time=start,
                to_time=end,
                memberships=behavior.graph_memberships,
            )
            for item in views
        }
        destination, manifest = write_graph(
            datasets,
            run_dir / "graphs",
            source_manifest=source_manifest,
            config=config,
            formats=format_values,
        )
    except ValueError as exc:
        typer.echo(f"Graph export failed: {exc}", err=True)
        raise typer.Exit(
            code=3 if "graph" in str(exc).lower() or "pattern" in str(exc).lower() else 2
        ) from exc
    except (FileNotFoundError, OSError, RuntimeError) as exc:
        typer.echo(f"Graph export failed: {exc}", err=True)
        raise typer.Exit(code=4) from exc
    if json_output:
        typer.echo(
            json.dumps(
                {
                    "graph_id": manifest.graph_id,
                    "output": str(destination),
                    "manifest": str(destination / "graph_manifest.json"),
                },
                sort_keys=True,
            )
        )
        return
    typer.echo(f"Graph exported: {manifest.graph_id}")
    typer.echo(f"Output: {destination}")
    typer.echo(f"Manifest: {destination / 'graph_manifest.json'}")


@graph_app.command("validate")
def validate_graph_command(
    run_id: Annotated[str, typer.Option("--run-id")],
    output_dir: Annotated[Path, typer.Option("--output-dir")] = Path("runs"),
) -> None:
    """Validate observable and oracle graph projections for a run."""
    try:
        entities, behavior, manifest = load_generated_run(output_dir / run_id)
        config = SimulationRunConfig.model_validate(manifest.resolved_configuration)
        for view in ("observable", "oracle"):
            validate_graph(build_graph(config, entities, behavior, manifest, view=view))
    except ValueError as exc:
        typer.echo(f"Graph validation failed: {exc}", err=True)
        raise typer.Exit(code=3) from exc
    except (FileNotFoundError, OSError) as exc:
        typer.echo(f"Graph validation failed: {exc}", err=True)
        raise typer.Exit(code=4) from exc
    typer.echo(f"Graph is valid for run {run_id}.")


@ml_app.command("build-dataset")
def build_dataset(
    path: Annotated[Path, typer.Argument(help="YAML configuration file.")],
    run_id: Annotated[str, typer.Option("--run-id", help="Existing generated run identifier.")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="Directory containing the source run."),
    ] = Path("runs"),
    output: Annotated[
        Path | None,
        typer.Option("--output", help="Destination Parquet file."),
    ] = None,
    from_time: Annotated[
        str | None,
        typer.Option("--from", help="Optional ISO-8601 prediction range start."),
    ] = None,
    to_time: Annotated[
        str | None,
        typer.Option("--to", help="Optional ISO-8601 prediction range end."),
    ] = None,
    label_delay_aware: Annotated[
        bool,
        typer.Option("--label-delay-aware", help="Exclude labels unavailable at prediction time."),
    ] = False,
) -> None:
    """Build a deterministic point-in-time dataset from an existing run."""

    config = _load_or_exit(path)
    try:
        start = datetime.fromisoformat(from_time) if from_time is not None else None
        end = datetime.fromisoformat(to_time) if to_time is not None else None
        values = config.model_dump(mode="python")
        dataset_values = values["dataset"]
        if start is not None:
            dataset_values["start"] = start
        if end is not None:
            dataset_values["end"] = end
        if label_delay_aware:
            dataset_values["unresolved_labels"] = "exclude"
        config = SimulationRunConfig.model_validate(values)
        run_dir = output_dir / run_id
        entities, behavior, source_manifest = load_generated_run(run_dir)
        dataset = PointInTimeDatasetBuilder(config, entities, behavior, source_manifest).build()
        destination = output or run_dir / "ml" / "dataset.parquet"
        parquet_path, manifest_path = write_point_in_time_dataset(dataset, destination)
    except (OSError, ValueError) as exc:
        typer.echo(f"Dataset generation failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    typer.echo(f"Dataset generated: {parquet_path}")
    typer.echo(f"Dataset manifest: {manifest_path}")
    typer.echo(f"Rows: {dataset.count}")


@app.command("replay")
def replay_command(
    run_id: Annotated[str, typer.Option("--run-id", help="Existing generated run identifier.")],
    from_time: Annotated[str, typer.Option("--from", help="Inclusive ISO-8601 period start.")],
    to_time: Annotated[str, typer.Option("--to", help="Exclusive ISO-8601 period end.")],
    order: Annotated[
        str,
        typer.Option(
            "--order",
            help="Replay ordering: event_time_order or original_delivery.",
        ),
    ] = "event_time_order",
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="Directory containing generated runs."),
    ] = Path("runs"),
) -> None:
    """Replay a selected period from one immutable generated run."""

    try:
        result = replay_run(
            output_dir / run_id,
            datetime.fromisoformat(from_time),
            datetime.fromisoformat(to_time),
            order=cast(ReplayOrder, order),
        )
        envelope_path, manifest_path = write_replay(result, output_dir / run_id / "replays")
    except (OSError, ValueError) as exc:
        typer.echo(f"Replay generation failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    typer.echo(f"Replay generated: {result.manifest.replay_id}")
    typer.echo(f"Events: {result.count}")
    typer.echo(f"Output: {envelope_path.parent}")
    typer.echo(f"Manifest: {manifest_path}")


@ml_app.command("backtest")
def backtest_command(
    path: Annotated[Path, typer.Argument(help="YAML configuration file.")],
    run_id: Annotated[str, typer.Option("--run-id", help="Existing generated run identifier.")],
    benchmark_pack: Annotated[
        Path | None,
        typer.Option("--benchmark-pack", help="Optional versioned benchmark-pack YAML."),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="Directory containing generated runs."),
    ] = Path("runs"),
) -> None:
    """Build deterministic rolling PIT backtest folds from an existing run."""

    config = _load_or_exit(path)
    try:
        pack: BenchmarkPack | None = (
            load_benchmark_pack(benchmark_pack) if benchmark_pack is not None else None
        )
        run_dir = output_dir / run_id
        entities, behavior, source_manifest = load_generated_run(run_dir)
        result = run_backtest(
            config,
            entities,
            behavior,
            source_manifest,
            benchmark_pack=pack,
        )
        rows_path, metrics_path, manifest_path = write_backtest(
            result, run_dir / "ml" / "backtests"
        )
    except (OSError, ValueError) as exc:
        typer.echo(f"Backtest generation failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    typer.echo(f"Backtest generated: {result.manifest.backtest_id}")
    typer.echo(f"Rows: {len(result.fold_rows)}")
    typer.echo(f"Fold metrics: {metrics_path}")
    typer.echo(f"Rows artifact: {rows_path}")
    typer.echo(f"Manifest: {manifest_path}")


def main() -> None:
    """Run the command-line application."""

    app()


if __name__ == "__main__":
    main()
