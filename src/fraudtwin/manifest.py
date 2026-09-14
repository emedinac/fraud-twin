import subprocess
from datetime import datetime, timedelta
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, model_serializer

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
    config_version: str = "1"
    resolved_configuration: dict[str, object] = Field(default_factory=dict)
    regime_definitions: list[dict[str, object]] = Field(default_factory=list)
    output_artifacts: dict[str, object] = Field(default_factory=dict)
    graph: dict[str, object] = Field(default_factory=dict)
    difficulty: dict[str, object] | None = None

    @model_serializer(mode="wrap")
    def _serialize_without_inactive_difficulty(self, handler):  # type: ignore[no-untyped-def]
        data = handler(self)
        if data.get("difficulty") is None:
            data.pop("difficulty", None)
        return data


class GraphManifest(BaseModel):
    """Lineage and fingerprints for one immutable M11 graph export."""

    model_config = ConfigDict(extra="forbid")

    graph_id: str
    graph_version: str
    source_run_id: str
    source_manifest_hash: str
    configuration_hash: str
    parameters: dict[str, object]
    source_snapshots: dict[str, object]
    schema_versions: dict[str, str]
    counts: dict[str, dict[str, int]]
    schema_fingerprint: str
    output_fingerprint: str
    output_artifacts: dict[str, object]
    ordering: dict[str, object] = Field(default_factory=dict)
    canonical_content_fingerprint: str | None = None
    file_checksums: dict[str, str] = Field(default_factory=dict)
    export_parameters: dict[str, object] = Field(default_factory=dict)
    difficulty: dict[str, object] | None = None

    @model_serializer(mode="wrap")
    def _serialize_without_inactive_difficulty(self, handler):  # type: ignore[no-untyped-def]
        data = handler(self)
        if data.get("difficulty") is None:
            data.pop("difficulty", None)
        return data


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
    difficulty: dict[str, object] | None = None

    @model_serializer(mode="wrap")
    def _serialize_without_inactive_difficulty(self, handler):  # type: ignore[no-untyped-def]
        data = handler(self)
        if data.get("difficulty") is None:
            data.pop("difficulty", None)
        return data


class ReplayManifest(BaseModel):
    """Lineage and fingerprints for one immutable replay selection."""

    model_config = ConfigDict(extra="forbid")

    replay_id: str
    replay_version: str
    source_run_id: str
    source_manifest_hash: str
    parameters: dict[str, object]
    source_run_information: dict[str, object]
    row_counts: dict[str, int]
    schema_versions: dict[str, str]
    regime_definitions: list[dict[str, object]]
    ordering: dict[str, object]
    closure: dict[str, object]
    schema_fingerprint: str
    output_fingerprint: str


class BacktestManifest(BaseModel):
    """Lineage, fold, PIT, regime, and metric metadata for M10."""

    model_config = ConfigDict(extra="forbid")

    backtest_id: str
    backtest_version: str
    source_run_id: str
    source_manifest_hash: str
    configuration_hash: str
    parameters: dict[str, object]
    source_snapshots: dict[str, object]
    feature_version: str
    folds: list[dict[str, object]]
    pit_validation: dict[str, object]
    regime_definitions: list[dict[str, object]]
    resolved_regimes: list[dict[str, object]]
    metric_definitions: list[str]
    per_fold_metrics: list[dict[str, object]]
    aggregate_metrics: dict[str, object]
    benchmark_pack: dict[str, object] | None = None
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
    resolved = config.model_dump(mode="json")
    if not config.graph.enabled:
        resolved.pop("graph", None)
    if not config.benchmark.enabled:
        resolved.pop("benchmark", None)
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
        resolved_configuration=resolved,
        regime_definitions=[regime.model_dump(mode="json") for regime in config.backtest.regimes],
        difficulty=None,
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
