import subprocess
from datetime import datetime, timedelta
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field

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
    quality_fault_rates: dict[str, float] = Field(default_factory=dict)
    quality_diagnostics: dict[str, object] = Field(default_factory=dict)


class DatasetManifest(BaseModel):
    """Deterministic lineage and reproducibility metadata for an M9 dataset."""

    model_config = ConfigDict(extra="forbid")

    dataset_id: str
    dataset_version: str
    source_run_id: str
    source_manifest_hash: str
    generator_version: str
    seed: int
    configuration_hash: str
    parameters: dict[str, object]
    split_boundaries: dict[str, str]
    feature_definitions: dict[str, dict[str, object]]
    label_definition: dict[str, object]
    source_run_information: dict[str, object]
    reproducibility: dict[str, object]
    row_counts: dict[str, int]
    schema_version: str
    row_grain: str
    prediction_entity: str
    date_range: dict[str, str]
    schema_fingerprint: str
    output_fingerprint: str


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
    """Create reproducibility metadata for a simulation run."""

    run_hash = config_hash(config)[:16]
    start_time = config.simulation.start
    return RunManifest(
        run_id=f"RUN-{run_hash}",
        generator_version=__version__,
        git_commit=_git_commit(),
        seed=config.simulation.seed,
        scenario_config_hash=config_hash(config),
        schema_versions={},
        start_time=start_time,
        end_time=start_time + timedelta(days=config.simulation.duration_days),
        entity_counts={},
        event_counts={},
        fraud_counts={},
        fraud_rates={},
        quality_fault_counts={},
        quality_fault_rates={},
        quality_diagnostics={},
    )


def write_manifest(manifest: RunManifest, output_dir: Path) -> Path:
    """Persist a manifest and return its path."""

    run_dir = output_dir / manifest.run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = run_dir / "manifest.json"
    manifest_path.write_text(
        manifest.model_dump_json(indent=2) + "\n",
        encoding="utf-8",
    )
    return manifest_path
