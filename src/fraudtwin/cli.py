from pathlib import Path
from typing import Annotated

import polars as pl
import typer

from fraudtwin.config import SimulationRunConfig, config_hash, load_config
from fraudtwin.domain import Account, LedgerEntry, Payment, PaymentEvent, validate_ledger
from fraudtwin.manifest import create_manifest, write_manifest
from fraudtwin.simulation import BehaviorGenerator, EntityGenerator
from fraudtwin.simulation.parquet import write_behavior_parquet, write_entity_parquet

app = typer.Typer(help="Synthetic financial-system and fraud digital twin.")
config_app = typer.Typer(help="Validate simulation configuration.")
app.add_typer(config_app, name="config")


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
    """Validate a configuration and generate entities and legitimate behavior."""

    config = _load_or_exit(path)
    manifest = create_manifest(config)
    entity_dataset = EntityGenerator(config).generate()
    behavior_dataset = BehaviorGenerator(config, entity_dataset).generate()
    run_dir = output_dir / manifest.run_id
    write_entity_parquet(entity_dataset, run_dir)
    write_behavior_parquet(behavior_dataset, run_dir)
    entity_counts = {**entity_dataset.counts, "behavior_profiles": len(behavior_dataset.profiles)}
    event_counts = {
        "payments": len(behavior_dataset.payments),
        "payment_events": len(behavior_dataset.payment_events),
        "ledger_entries": len(behavior_dataset.ledger_entries),
        "card_lifecycle_events": sum(behavior_dataset.card_lifecycle_event_counts.values()),
        "pix_lifecycle_events": sum(behavior_dataset.pix_lifecycle_event_counts.values()),
        **behavior_dataset.card_lifecycle_event_counts,
        **behavior_dataset.pix_lifecycle_event_counts,
    }
    manifest = manifest.model_copy(
        update={
            "entity_counts": entity_counts,
            "event_counts": event_counts,
            "schema_versions": {
                **{entity_name: "1" for entity_name in entity_counts},
                "payments": "2",
                "payment_events": "3",
                "ledger_entries": "1",
            },
        }
    )
    manifest_path = write_manifest(manifest, output_dir)
    typer.echo(f"Run generated: {manifest.run_id}")
    typer.echo(f"Manifest: {manifest_path}")
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


def main() -> None:
    """Run the command-line application."""

    app()


if __name__ == "__main__":
    main()
