from datetime import datetime
from pathlib import Path
from typing import Annotated, cast

import polars as pl
import typer

from fraudtwin.config import SimulationRunConfig, config_hash, load_config
from fraudtwin.domain import Account, LedgerEntry, Payment, PaymentEvent, validate_ledger
from fraudtwin.manifest import create_manifest, write_manifest
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
from fraudtwin.simulation import BehaviorGenerator, EntityGenerator
from fraudtwin.simulation.parquet import write_behavior_parquet, write_entity_parquet

app = typer.Typer(help="Synthetic financial-system and fraud digital twin.")
config_app = typer.Typer(help="Validate simulation configuration.")
ml_app = typer.Typer(help="Build local point-in-time ML datasets.")
app.add_typer(config_app, name="config")
app.add_typer(ml_app, name="ml")


def _load_or_exit(path: Path) -> SimulationRunConfig:
    try:
        return load_config(path)
    except (FileNotFoundError, ValueError) as exc:
        typer.echo(f"Configuration error: {exc}", err=True)
        raise typer.Exit(code=1) from exc


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
    base_manifest = create_manifest(config)
    entity_dataset = EntityGenerator(config).generate()
    behavior_dataset = BehaviorGenerator(
        config, entity_dataset, simulation_run_id=base_manifest.run_id
    ).generate()
    run_dir = output_dir / base_manifest.run_id
    write_entity_parquet(entity_dataset, run_dir)
    write_behavior_parquet(behavior_dataset, run_dir)
    entity_counts = {**entity_dataset.counts, "behavior_profiles": len(behavior_dataset.profiles)}
    event_counts = behavior_dataset.event_counts
    manifest = base_manifest.model_copy(
        update={
            "entity_counts": entity_counts,
            "event_counts": event_counts,
            "schema_versions": {
                **{entity_name: "1" for entity_name in entity_counts},
                "payments": "2",
                "payment_events": "4",
                "ledger_entries": "1",
                "fraud_records": "1",
                "fraud_alerts": "1",
                "fraud_cases": "1",
                "case_confirmations": "1",
                "customer_disputes": "1",
                "fraud_labels": "1",
            },
            "fraud_counts": behavior_dataset.fraud_counts,
            "fraud_rates": behavior_dataset.fraud_rates,
            "quality_fault_counts": behavior_dataset.quality_fault_counts,
            "quality_fault_rates": behavior_dataset.quality_fault_rates,
            "quality_diagnostics": behavior_dataset.quality_diagnostics,
        }
    )
    dataset_path: Path | None = None
    dataset_manifest_path: Path | None = None
    if config.dataset.enabled:
        dataset = PointInTimeDatasetBuilder(
            config, entity_dataset, behavior_dataset, manifest
        ).build()
        dataset_path, dataset_manifest_path = write_point_in_time_dataset(
            dataset, run_dir / "ml" / "dataset.parquet"
        )
    manifest_path = write_manifest(manifest, output_dir)
    typer.echo(f"Run generated: {manifest.run_id}")
    typer.echo(f"Manifest: {manifest_path}")
    if dataset_path is not None and dataset_manifest_path is not None:
        typer.echo(f"Dataset: {dataset_path}")
        typer.echo(f"Dataset manifest: {dataset_manifest_path}")
    typer.echo("Generated entity counts:")
    for entity_name, count in entity_counts.items():
        typer.echo(f"  {entity_name}: {count}")
    typer.echo("Generated payment counts:")
    for event_name, count in event_counts.items():
        typer.echo(f"  {event_name}: {count}")


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
