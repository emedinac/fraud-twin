import subprocess
import uuid
from datetime import UTC, datetime
from pathlib import Path

from pydantic import BaseModel, ConfigDict

from fraudtwin import __version__
from fraudtwin.config import SimulationRunConfig, config_hash


class RunManifest(BaseModel):
    """Metadata required to reproduce and audit a generated run."""

    model_config = ConfigDict(extra="forbid")

    run_id: str
    generator_version: str
    git_commit: str
    seed: int
    scenario_config_hash: str
    schema_versions: dict[str, str]
    start_time: datetime
    end_time: datetime
    entity_counts: dict[str, int]
    event_counts: dict[str, int]
    fraud_counts: dict[str, int]
    fraud_rates: dict[str, float]
    quality_fault_counts: dict[str, int]


def _git_commit() -> str:
    """Return the current commit when running from a Git checkout."""

    repository_root = Path(__file__).resolve().parents[2]
    try:
        result = subprocess.run(
            ["git", "-C", str(repository_root), "rev-parse", "HEAD"],
            capture_output=True,
            check=False,
            text=True,
            timeout=2,
        )
    except (OSError, subprocess.TimeoutExpired):
        return "unknown"
    return result.stdout.strip() if result.returncode == 0 else "unknown"


def create_manifest(config: SimulationRunConfig) -> RunManifest:
    """Create a manifest for a run before domain generation exists."""

    now = datetime.now(UTC)
    return RunManifest(
        run_id=str(uuid.uuid4()),
        generator_version=__version__,
        git_commit=_git_commit(),
        seed=config.simulation.seed,
        scenario_config_hash=config_hash(config),
        schema_versions={},
        start_time=now,
        end_time=now,
        entity_counts={},
        event_counts={},
        fraud_counts={},
        fraud_rates={},
        quality_fault_counts={},
    )


def write_manifest(manifest: RunManifest, output_dir: Path) -> Path:
    """Persist a manifest and return its path."""

    run_dir = output_dir / manifest.run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    manifest_path = run_dir / "manifest.json"
    manifest_path.write_text(
        manifest.model_dump_json(indent=2) + "\n",
        encoding="utf-8",
    )
    return manifest_path
