from pathlib import Path
from typing import Annotated

import typer
from pydantic import ValidationError

from fraudtwin.config import SimulationRunConfig, config_hash, load_config
from fraudtwin.logging_config import configure_logging
from fraudtwin.manifest import create_manifest, write_manifest

app = typer.Typer(help="Synthetic financial-system and fraud digital twin.")
config_app = typer.Typer(help="Validate simulation configuration.")
app.add_typer(config_app, name="config")


def _load_or_exit(path: Path) -> SimulationRunConfig:
    try:
        return load_config(path)
    except (FileNotFoundError, ValueError, ValidationError) as exc:
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
    """Validate a configuration and create its initial run manifest."""

    configure_logging()
    config = _load_or_exit(path)
    manifest = create_manifest(config)
    manifest_path = write_manifest(manifest, output_dir)
    typer.echo(f"Run initialized: {manifest.run_id}")
    typer.echo(f"Manifest: {manifest_path}")
    typer.echo("No entities or events generated yet; this is the Milestone 0 foundation.")


def main() -> None:
    """Run the command-line application."""

    app()


if __name__ == "__main__":
    main()
