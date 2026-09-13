from pathlib import Path
from typing import Annotated

import typer

from fraudtwin.config import SimulationRunConfig, config_hash, load_config
from fraudtwin.logging_config import configure_logging
from fraudtwin.manifest import create_manifest, write_manifest
from fraudtwin.simulation import EntityGenerator
from fraudtwin.simulation.parquet import write_entity_parquet

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

    configure_logging()
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
    """Validate a configuration and generate its entity population."""

    configure_logging()
    config = _load_or_exit(path)
    manifest = create_manifest(config)
    dataset = EntityGenerator(config).generate()
    entity_counts = dataset.counts
    write_entity_parquet(dataset, output_dir / manifest.run_id)
    manifest = manifest.model_copy(
        update={
            "entity_counts": entity_counts,
            "schema_versions": {entity_name: "1" for entity_name in entity_counts},
        }
    )
    manifest_path = write_manifest(manifest, output_dir)
    typer.echo(f"Run generated: {manifest.run_id}")
    typer.echo(f"Manifest: {manifest_path}")
    typer.echo("Generated entity counts:")
    for entity_name, count in entity_counts.items():
        typer.echo(f"  {entity_name}: {count}")


def main() -> None:
    """Run the command-line application."""

    app()


if __name__ == "__main__":
    main()
