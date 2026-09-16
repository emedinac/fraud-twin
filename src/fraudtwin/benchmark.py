"""Reproducible fraud stress benchmark orchestration.

The benchmark layer owns suite composition and reporting.  It deliberately
delegates generation to the existing deterministic engines and model scoring to
the M19 prediction contract.
"""

import hashlib
import importlib
import json
import platform
import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from importlib.resources import files
from pathlib import Path
from typing import Any, Literal, Protocol, cast

import polars as pl
import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator

from fraudtwin import __version__
from fraudtwin.config import SimulationRunConfig, config_hash
from fraudtwin.domain import validate_ledger, validate_payment_lifecycle
from fraudtwin.generation import GeneratedData, GeneratedRun, generate
from fraudtwin.ml import (
    ALL_MODEL_NAMES,
    BaselineEvaluationConfig,
    EvaluationResult,
    PredictionRecord,
    evaluate_predictions,
    heuristic_predictions,
    load_generated_run,
    train_baselines,
    write_evaluation,
)
from fraudtwin.reproducibility import sha256_json, write_json

SuiteName = Literal[
    "baseline",
    "temporal",
    "boundary",
    "camouflage",
    "graph",
    "observability",
    "calibrated",
    "mixed",
    "all",
]
STANDARD_SUITES: tuple[str, ...] = (
    "baseline",
    "temporal",
    "boundary",
    "camouflage",
    "graph",
    "observability",
    "calibrated",
    "mixed",
)
SUITE_DEFINITION_VERSION = "1"
PUBLIC_PACK_VERSION = "1"
PUBLIC_PACK_RESOURCE_DIR = "public_packs"
PUBLIC_PACK_REFERENCE_RE = re.compile(
    r"^(?P<id>FT-B0[1-8]-(?:STABLE|TEMPORAL|BOUNDARY|CAMOUFLAGE|GRAPH|OBSERVABILITY|CALIBRATED|MIXED))@"
    r"(?P<version>\d+\.\d+(?:\.\d+)?)$"
)


class BenchmarkRequest(BaseModel):
    """Strict command-level benchmark request."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    suite: SuiteName = "mixed"
    difficulty: int = Field(default=7, ge=1, le=10)
    seed: int = Field(default=42, ge=0)
    output_dir: Path = Path("runs/benchmarks")
    models: tuple[str, ...] = ("deterministic_heuristic",)
    runners: tuple[str, ...] = ()
    calibration_profile: Path | None = None

    @field_validator("models")
    @classmethod
    def models_are_unique(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(set(value)) != len(value):
            raise ValueError("benchmark models must be unique")
        return value


class RunnerMetadata(BaseModel):
    """Framework and input lineage declared by an external model runner."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    model_id: str = Field(min_length=1)
    framework: str = Field(min_length=1)
    framework_version: str = "unknown"
    required_inputs: tuple[str, ...] = ()
    parameters: dict[str, Any] = Field(default_factory=dict)
    artifact_checksums: dict[str, str] = Field(default_factory=dict)


@dataclass(frozen=True)
class BenchmarkRunnerInput:
    """Read-only paths and metadata supplied to an external model runner."""

    suite: str
    difficulty: int
    dataset_path: Path
    run_dir: Path
    manifest: dict[str, Any]


class BenchmarkModelRunner(Protocol):
    """Framework-neutral adapter implemented by optional model environments."""

    metadata: RunnerMetadata

    def predict(self, inputs: BenchmarkRunnerInput) -> Sequence[PredictionRecord]: ...


@dataclass(frozen=True)
class BenchmarkResult:
    """Paths and stable metadata emitted by one benchmark invocation."""

    benchmark_id: str
    root: Path
    manifest_path: Path
    results_path: Path
    descriptors_path: Path
    results: tuple[dict[str, Any], ...]


class PublicBenchmarkPack(BaseModel):
    """Immutable, distributable M21 benchmark definition."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    id: str = Field(pattern=r"^FT-B0[1-8]-[A-Z]+$")
    version: str = Field(pattern=r"^(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)$")
    suite: Literal[
        "baseline",
        "temporal",
        "boundary",
        "camouflage",
        "graph",
        "observability",
        "calibrated",
        "mixed",
    ]
    generator_compatibility: str = Field(pattern=r"^>=\d+\.\d+\.\d+,<\d+\.\d+\.\d+$")
    seed: int = Field(ge=0)
    seed_tree_version: str = Field(min_length=1)
    difficulty: int = Field(ge=1, le=10)
    simulation_start: datetime
    simulation_end: datetime
    split_boundaries: dict[str, datetime]
    scenario_definitions: dict[str, Any]
    stress_parameters: dict[str, Any]
    label_observation_policy: dict[str, Any]
    calibration: dict[str, Any] | None = None
    metric_definitions: dict[str, Any]
    resolved_configuration_hash: str = Field(min_length=1)
    expected_descriptors: dict[str, Any]
    expected_fingerprints: dict[str, str]

    @field_validator("simulation_start", "simulation_end", mode="before")
    @classmethod
    def timestamps_are_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("public benchmark pack timestamps must include a timezone")
        return value.astimezone(UTC)

    @field_validator("split_boundaries", mode="before")
    @classmethod
    def split_timestamps_are_aware(cls, value: dict[str, Any]) -> dict[str, datetime]:
        return {key: cls.timestamps_are_aware(item) for key, item in value.items()}

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> PublicBenchmarkPack:
        pack = cls.model_validate(payload)
        if pack.simulation_end <= pack.simulation_start:
            raise ValueError("public benchmark pack simulation_end must follow simulation_start")
        required_splits = {"train_end", "validation_end", "test_end"}
        if set(pack.split_boundaries) != required_splits:
            raise ValueError("public benchmark pack must freeze train, validation, and test ends")
        boundaries = [
            pack.simulation_start,
            *(pack.split_boundaries[name] for name in ("train_end", "validation_end", "test_end")),
        ]
        if any(left >= right for left, right in zip(boundaries, boundaries[1:], strict=False)):
            raise ValueError("public benchmark pack windows must be chronological")
        if pack.split_boundaries["test_end"] != pack.simulation_end:
            raise ValueError("public benchmark pack test_end must equal simulation_end")
        if pack.suite in {"calibrated", "mixed"} and pack.calibration is None:
            raise ValueError(f"public pack suite {pack.suite} requires frozen calibration")
        if not pack.expected_descriptors or not pack.expected_fingerprints:
            raise ValueError(
                "public benchmark pack must freeze expected descriptors and fingerprints"
            )
        return pack

    @property
    def identity(self) -> str:
        return f"{self.id}@{self.version}"

    @property
    def fingerprint(self) -> str:
        return sha256_json(self.model_dump(mode="json"))


# Public packs freeze generator behavior, not integration-only package
# additions. M24 keeps the M21 generator line compatible while adding Avro
# contracts and a registry that do not participate in generation.
_FROZEN_GENERATOR_COMPATIBILITY_VERSION = "0.26.0"


def _base_values(seed: int) -> dict[str, Any]:
    raw = yaml.safe_load(files("fraudtwin").joinpath("defaults/minimal.yaml").read_text())
    if not isinstance(raw, dict):
        raise ValueError("built-in minimal configuration must be a mapping")
    values = cast(dict[str, Any], raw)
    values["simulation"]["seed"] = seed
    values["simulation"]["duration_days"] = 12
    values["population"].update(
        {
            "customers": 80,
            "accounts": 100,
            "cards": 80,
            "merchants": 20,
            "devices": 50,
            "pix_keys": 60,
        }
    )
    values["payments"]["daily_target"] = 40
    values["fraud"].update(
        {
            "enabled": True,
            "target_rate": 0.12,
            "scenario_count": 5,
            "hard_negative_rate": 1.0,
        }
    )
    values["labels"] = {
        "enabled": True,
        "investigation_rate": 0.85,
        "missing_fraud_rate": 0.10,
        "preliminary_error_rate": 0.10,
        "correction_rate": 0.10,
        "reopening_rate": 0.05,
    }
    values["dataset"].update(
        {
            "enabled": True,
            "unresolved_labels": "include",
            "label_delay_seconds": 3_600,
        }
    )
    values["outputs"].update({"parquet": True})
    values["quality"] = {"profile": "clean"}
    return values


def _graph_values() -> dict[str, Any]:
    return {
        "enabled": True,
        "scenarios": [
            {"type": "MULE_NETWORK", "count": 1, "source_count": 3},
            {"type": "CYCLIC_RING", "count": 1, "member_count": 4},
            {"type": "DENSE_CAMPAIGN", "count": 1, "member_count": 4, "edge_count": 6},
        ],
    }


def _dynamic_values() -> dict[str, Any]:
    return {
        "enabled": True,
        "bindings": [
            {"profile": "linear"},
            {"profile": "rotating_ring"},
            {"profile": "adaptive_network"},
        ],
    }


def _regimes(start: datetime) -> list[dict[str, Any]]:
    return [
        {"id": "stable", "from": start, "to": start + timedelta(days=3)},
        {
            "id": "sudden",
            "from": start + timedelta(days=3),
            "to": start + timedelta(days=6),
            "prevalence_multiplier": 2.0,
            "camouflage": 0.35,
        },
        {
            "id": "gradual",
            "from": start + timedelta(days=6),
            "to": start + timedelta(days=9),
            "prevalence_multiplier": 1.5,
            "amount_multiplier": 1.2,
            "timing_multiplier": 1.5,
            "camouflage": 0.55,
        },
        {
            "id": "recurring",
            "from": start + timedelta(days=9),
            "to": start + timedelta(days=12),
            "prevalence_multiplier": 0.8,
            "scenario_mix": {"F01": 0.4, "F02": 0.1, "F03": 0.2, "F04": 0.2, "F05": 0.1},
            "camouflage": 0.25,
        },
    ]


def build_suite_config(
    suite: str,
    *,
    difficulty: int,
    seed: int,
    calibration_profile: Path | None = None,
) -> SimulationRunConfig:
    """Build and validate one standard suite without mutating global config."""

    if suite not in STANDARD_SUITES:
        raise ValueError(f"unsupported benchmark suite: {suite}")
    values = _base_values(seed)
    start = datetime(2026, 1, 1, tzinfo=UTC)
    values["benchmark"] = {"difficulty": difficulty}
    values["dataset"]["splits"] = {
        "train_end": start + timedelta(days=7),
        "validation_end": start + timedelta(days=9),
        "test_end": start + timedelta(days=12),
        "label_delay_gap_seconds": 3_600,
    }
    if suite in {"temporal", "observability", "mixed"}:
        values["backtest"] = {"regimes": _regimes(start)}
    if suite in {"camouflage", "graph", "mixed"}:
        values["stress"] = {
            "camouflage": 0.75,
            "feature_camouflage": 0.80,
            "relation_camouflage": 0.70,
        }
    if suite in {"graph", "mixed"}:
        values["graph"] = _graph_values()
        values["campaign_dynamics"] = _dynamic_values()
    if suite in {"boundary", "mixed"}:
        values["counterfactual"] = {
            "enabled": True,
            "budget": 2.0,
            "requests": [
                {"objective": "F01", "count": 1},
                {"objective": "F03", "count": 1},
                {"objective": "F04", "count": 1},
            ],
        }
    if suite in {"observability", "mixed"}:
        values["quality"] = {
            "profile": "realistic",
            # Preserve one logical row per payment/event for M19 evaluation while
            # retaining realistic late and out-of-order evidence.
            "duplicate_record_probability": 0.0,
            "duplicate_event_probability": 0.0,
            "missing_optional_probability": 0.0,
            "invalid_value_probability": 0.0,
            "negative_amount_probability": 0.0,
        }
    requires_calibration = suite in {"calibrated", "mixed"}
    if requires_calibration and (calibration_profile is None or not calibration_profile.is_file()):
        raise ValueError(f"suite {suite} requires an existing calibration profile")
    if calibration_profile is not None:
        values["calibration"] = {
            "enabled": True,
            "profile": str(calibration_profile),
            # Keep the benchmark's payment/ledger constraints authoritative;
            # these aggregate summaries calibrate legitimate activity without
            # replacing opening balances or transaction amount bounds.
            "summary_names": ["merchant_frequency", "customer_activity", "transaction_count"],
        }
    return SimulationRunConfig.model_validate(values)


def _public_pack_resources() -> tuple[Any, ...]:
    root = files("fraudtwin").joinpath(PUBLIC_PACK_RESOURCE_DIR)
    return tuple(
        sorted(
            (
                item
                for item in root.iterdir()
                if item.name.startswith("FT-B") and item.name.endswith(".yaml")
            ),
            key=lambda item: item.name,
        )
    )


def _pack_payload(resource: Any) -> dict[str, Any]:
    raw = yaml.safe_load(resource.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"public benchmark pack must be a mapping: {resource.name}")
    return cast(dict[str, Any], raw)


def list_public_packs() -> tuple[PublicBenchmarkPack, ...]:
    """Return all bundled public packs in stable identity order."""

    return tuple(
        sorted(
            (
                PublicBenchmarkPack.from_payload(_pack_payload(item))
                for item in _public_pack_resources()
            ),
            key=lambda item: item.identity,
        )
    )


def load_public_pack(reference: str) -> PublicBenchmarkPack:
    """Resolve an exact or unambiguous major/minor public-pack reference."""

    match = PUBLIC_PACK_REFERENCE_RE.fullmatch(reference)
    if match is None:
        raise ValueError("public pack reference must use FT-Bxx-NAME@major.minor[.patch]")
    pack_id = match.group("id")
    requested_version = match.group("version")
    candidates = [item for item in list_public_packs() if item.id == pack_id]
    if requested_version.count(".") == 2:
        candidates = [item for item in candidates if item.version == requested_version]
    else:
        candidates = [
            item for item in candidates if item.version.startswith(requested_version + ".")
        ]
    if not candidates:
        raise ValueError(f"public benchmark pack does not exist: {reference}")
    if len(candidates) > 1:
        raise ValueError(f"public benchmark pack reference is ambiguous: {reference}")
    return candidates[0]


def _calibration_resource(pack: PublicBenchmarkPack) -> Path | None:
    if pack.calibration is None:
        return None
    resource_name = pack.calibration.get("resource")
    if not isinstance(resource_name, str) or not resource_name:
        raise ValueError(f"{pack.identity} calibration must name a bundled resource")
    resource = files("fraudtwin").joinpath(PUBLIC_PACK_RESOURCE_DIR, resource_name)
    if not resource.is_file():
        raise ValueError(f"{pack.identity} calibration resource is missing: {resource_name}")
    path = Path(str(resource))
    if not path.is_file():
        raise ValueError("public benchmark resources must be available as package files")
    return path


def _record_payload(records: Iterable[BaseModel]) -> list[dict[str, Any]]:
    values = [item.model_dump(mode="json") for item in records]
    return sorted(values, key=lambda item: json.dumps(item, sort_keys=True, default=str))


def _logical_fingerprints(
    generated: GeneratedData, dataset_manifest: dict[str, Any]
) -> dict[str, str]:
    """Fingerprint logical content independently of Parquet bytes or machine details."""

    source = {
        "entities": {
            name: _record_payload(records)
            for name, records in sorted(generated.entities.all_tables().items())
        },
        "behavior": {
            name: _record_payload(records)
            for name, records in sorted(generated.behavior.tables().items())
        },
    }
    return {
        "source": sha256_json(source),
        "dataset": str(dataset_manifest.get("output_fingerprint", "")),
        "latent_truth": sha256_json(_record_payload(generated.behavior.fraud_records)),
        "observed_truth": sha256_json(_record_payload(generated.behavior.label_observations)),
    }


def _load_rows(dataset_path: Path) -> list[dict[str, Any]]:
    if not dataset_path.is_file():
        raise ValueError(f"benchmark dataset does not exist: {dataset_path}")
    return pl.read_parquet(dataset_path).to_dicts()


def _benchmark_rows(rows: Sequence[dict[str, Any]], behavior: Any) -> list[dict[str, Any]]:
    """Attach the complete synthetic target used by benchmark model scoring.

    Operational label observations are intentionally incomplete and delayed.
    A frozen synthetic benchmark nevertheless needs both classes in its scoring
    target, so evaluation uses the isolated generator truth while keeping all
    model features observable.
    """

    fraud_payment_ids = {
        record.payment_id for record in behavior.fraud_records if record.fraud_truth
    }
    return [
        row
        | {
            "label": "FRAUD" if row["payment_id"] in fraud_payment_ids else "LEGITIMATE",
        }
        for row in rows
    ]


def _load_runner(reference: str) -> BenchmarkModelRunner:
    if ":" not in reference:
        raise ValueError("runner must use module:factory notation")
    module_name, attribute = reference.split(":", 1)
    try:
        factory = getattr(importlib.import_module(module_name), attribute)
    except (ImportError, AttributeError) as exc:
        raise ValueError(f"cannot load benchmark runner {reference}: {exc}") from exc
    runner = factory() if callable(factory) else factory
    if not hasattr(runner, "metadata") or not hasattr(runner, "predict"):
        raise ValueError(f"benchmark runner {reference} must expose metadata and predict")
    metadata = runner.metadata
    runner.metadata = (
        metadata
        if isinstance(metadata, RunnerMetadata)
        else RunnerMetadata.model_validate(metadata)
    )
    return cast(BenchmarkModelRunner, runner)


def _metric(result: EvaluationResult) -> dict[str, Any]:
    for item in result.metrics:
        if item.get("partition") == "test" and "segment_dimension" not in item:
            return dict(item)
    raise ValueError("evaluation did not emit a test metric")


def _evaluate_model(
    model_id: str,
    rows: Sequence[dict[str, Any]],
    config: BaselineEvaluationConfig,
    *,
    source_run_dir: Path,
    suite: str,
    difficulty: int,
    dataset_path: Path,
    manifest: dict[str, Any],
    root: Path,
    evaluation_role: str,
    runner: BenchmarkModelRunner | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    if runner is not None:
        records = tuple(
            PredictionRecord.model_validate(item)
            for item in runner.predict(
                BenchmarkRunnerInput(suite, difficulty, dataset_path, source_run_dir, manifest)
            )
        )
        metadata = runner.metadata.model_dump(mode="json")
        result = evaluate_predictions(
            rows,
            records,
            config,
            model_id=runner.metadata.model_id,
            source_run_dir=source_run_dir,
        )
    elif model_id == "deterministic_heuristic":
        records = heuristic_predictions(rows)
        metadata = {
            "model_id": model_id,
            "framework": "fraudtwin",
            "framework_version": __version__,
            "required_inputs": ["pit_dataset"],
            "parameters": {"deterministic": True},
        }
        result = evaluate_predictions(
            rows,
            records,
            config,
            model_id=model_id,
            source_run_dir=source_run_dir,
        )
    else:
        if model_id not in ALL_MODEL_NAMES:
            raise ValueError(f"unsupported built-in model: {model_id}")
        result = train_baselines(
            rows,
            config.model_copy(update={"models": (model_id,)}),
            source_run_dir=source_run_dir,
        )
        metadata = dict(result.manifest.get("model_metadata", {}).get(model_id, {}))
        metadata.update({"model_id": model_id, "framework": "optional-ml"})
    evaluation_dir = root / "evaluations" / evaluation_role / model_id
    write_evaluation(result, evaluation_dir)
    metadata["package_versions"] = result.manifest.get("lineage", {}).get("package_versions", {})
    metadata["artifact_checksums"] = result.manifest.get("model_artifact_checksums", {})
    metric = _metric(result)
    row = {
        "model": model_id,
        "suite": suite,
        "difficulty": difficulty,
        "pr_auc": metric.get("pr_auc"),
        "recall_at_fixed_fpr": metric.get("recall_at_fixed_fpr"),
        "f1": metric.get("f1"),
        "detection_delay_seconds": metric.get("detection_delay_seconds_mean"),
        "drop_vs_baseline": 0.0,
        "prediction_count": len(result.predictions),
        "evaluation_fingerprint": result.manifest.get("output_fingerprint"),
    }
    return row, metadata


def _profile_for_suite(request: BenchmarkRequest, suite: str) -> Path | None:
    if request.calibration_profile is None:
        return None
    if request.suite == "all" or suite in {"baseline", "calibrated", "mixed"}:
        return request.calibration_profile
    return None


def _descriptors(
    config: SimulationRunConfig, manifest: dict[str, Any], behavior: Any
) -> dict[str, Any]:
    fraud_records = tuple(item for item in behavior.fraud_records if item.fraud_truth)
    payments = max(1, len(behavior.payments))
    camouflage = manifest.get("camouflage") or {}
    measurable = camouflage.get("measurable_summaries", {}) if isinstance(camouflage, dict) else {}
    similarities = measurable.get("feature_similarity", {}) if isinstance(measurable, dict) else {}
    feature_values = [
        float(v)
        for item in similarities.values()
        if isinstance(item, dict)
        for v in item.values()
        if isinstance(v, int | float)
    ]
    relation = camouflage.get("relation_summary", {}) if isinstance(camouflage, dict) else {}
    relation_score = (
        relation.get("relation_strength_observed", 0.0) if isinstance(relation, dict) else 0.0
    )
    cf = behavior.counterfactual
    distances = (
        [
            item.effective_distance
            for item in cf.change_sets
            if item.status == "ACCEPTED" and item.effective_distance is not None
        ]
        if cf
        else []
    )
    observations = behavior.label_observations
    delays: list[float] = []
    by_payment = {item.payment_id: item.occurred_at for item in behavior.fraud_records}
    for observation in observations:
        occurrence = by_payment.get(observation.payment_id)
        for version in observation.versions:
            if occurrence is not None and version.label_available_at is not None:
                delays.append((version.label_available_at - occurrence).total_seconds())
    regimes = config.backtest.regimes
    drift = 0.0
    for previous, current in zip(regimes, regimes[1:], strict=False):
        drift = max(
            drift,
            abs(previous.prevalence_multiplier - current.prevalence_multiplier)
            + abs(previous.amount_multiplier - current.amount_multiplier)
            + abs(previous.timing_multiplier - current.timing_multiplier)
            + abs(previous.camouflage - current.camouflage),
        )
    realized_scenarios = sorted({item.scenario_type for item in behavior.fraud_records})
    configured_scenarios = sorted(
        name for name, item in config.fraud.scenarios.items() if item.enabled and item.weight > 0
    )
    return {
        "fraud_prevalence": len(fraud_records) / payments,
        "feature_camouflage_score": sum(feature_values) / len(feature_values)
        if feature_values
        else 0.0,
        "relation_camouflage_score": float(relation_score),
        "counterfactual_distance": sum(distances) / len(distances) if distances else None,
        "drift_strength": drift,
        "graph_coordination_score": sum(
            item.scenario_type.startswith(("MULE", "CYCLIC", "DENSE")) for item in fraud_records
        )
        / len(fraud_records)
        if fraud_records
        else 0.0,
        "label_delay_distribution": {
            "count": len(delays),
            "min_seconds": min(delays) if delays else None,
            "median_seconds": sorted(delays)[len(delays) // 2] if delays else None,
            "max_seconds": max(delays) if delays else None,
        },
        "missing_label_rate": sum(
            item.observed_label is None for item in behavior.final_observed_labels
        )
        / max(1, len(behavior.final_observed_labels)),
        "investigation_selection_rate": sum(item.investigation_selected for item in observations)
        / max(1, len(observations)),
        "label_correction_rate": sum(bool(item.corrections) for item in observations)
        / max(1, len(observations)),
        "scenario_coverage": {"configured": configured_scenarios, "realized": realized_scenarios},
        "reference_fidelity_score": (manifest.get("calibration") or {}).get("composite_score")
        if isinstance(manifest.get("calibration"), dict)
        else None,
    }


def _run_one(
    suite: str,
    request: BenchmarkRequest,
    root: Path,
    *,
    role: str,
    calibration_profile: Path | None,
    runners: tuple[BenchmarkModelRunner, ...],
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
    config = build_suite_config(
        suite,
        difficulty=request.difficulty,
        seed=request.seed,
        calibration_profile=calibration_profile,
    )
    in_memory = generate(
        config,
        write=False,
        profile=calibration_profile,
    )
    generated: GeneratedRun = generate(
        config,
        write=True,
        output_dir=root / "runs" / role,
        profile=calibration_profile,
    )
    dataset_path = generated.dataset_path
    if dataset_path is None:
        raise ValueError("benchmark generation did not emit a PIT dataset")
    rows = _benchmark_rows(_load_rows(dataset_path), in_memory.behavior)
    manifest = generated.manifest.model_dump(mode="json")
    dataset_manifest_values: dict[str, Any] = {}
    if generated.dataset_manifest_path is not None:
        dataset_manifest_values = json.loads(generated.dataset_manifest_path.read_text())
    baseline_config = BaselineEvaluationConfig(models=("deterministic_heuristic",))
    all_models = list(
        dict.fromkeys(
            (
                "deterministic_heuristic",
                *request.models,
                *(item.metadata.model_id for item in runners),
            )
        )
    )
    evaluated: list[dict[str, Any]] = []
    lineage: dict[str, Any] = {}
    for model_id in all_models:
        runner = next((item for item in runners if item.metadata.model_id == model_id), None)
        row, metadata = _evaluate_model(
            model_id,
            rows,
            baseline_config,
            source_run_dir=generated.run_dir,
            suite=suite,
            difficulty=request.difficulty,
            dataset_path=dataset_path,
            manifest=manifest,
            root=root,
            evaluation_role=role,
            runner=runner,
        )
        evaluated.append(row)
        lineage[model_id] = metadata
    descriptors = _descriptors(config, manifest, in_memory.behavior)
    catalog = {
        "run_dir": str(generated.run_dir.relative_to(root)),
        "dataset": str(dataset_path.relative_to(root)),
        "dataset_manifest": (
            str(generated.dataset_manifest_path.relative_to(root))
            if generated.dataset_manifest_path is not None
            else None
        ),
        "manifest": str(generated.manifest_path.relative_to(root)),
        "latent_truth": str((generated.run_dir / "oracle").relative_to(root)),
        "observed_truth": str((generated.run_dir / "label_observations").relative_to(root))
        if (generated.run_dir / "label_observations").exists()
        else None,
        "role": role,
    }
    catalog_path = root / "catalog" / f"{role}.json"
    catalog_path.parent.mkdir(parents=True, exist_ok=True)
    write_json(catalog_path, catalog)
    metadata = {
        "manifest": manifest,
        "catalog": catalog,
        "catalog_path": str(catalog_path.relative_to(root)),
        "lineage": lineage,
        "temporal_split_ranges": dataset_manifest_values.get("split_boundaries", {}),
        "fingerprints": {
            "source": manifest.get("output_fingerprint"),
            "dataset": dataset_manifest_values.get("output_fingerprint"),
        },
        "logical_fingerprints": _logical_fingerprints(in_memory, dataset_manifest_values),
        "evaluation_label_policy": "synthetic_oracle_truth_with_observable_features",
    }
    return (
        evaluated,
        metadata,
        descriptors,
    )


def run_benchmark(request: BenchmarkRequest) -> BenchmarkResult:
    """Generate and evaluate one or every standard benchmark suite."""

    if request.calibration_profile is not None and request.suite not in {
        "calibrated",
        "mixed",
        "all",
    }:
        raise ValueError("calibration profile is only valid for calibrated, mixed, or all suites")
    selected = STANDARD_SUITES if request.suite == "all" else (request.suite,)
    if any(item in {"calibrated", "mixed"} for item in selected) and (
        request.calibration_profile is None or not request.calibration_profile.is_file()
    ):
        raise ValueError("calibrated and mixed suites require --calibration-profile")
    profile_fingerprint = None
    profile_version = None
    if request.calibration_profile is not None:
        profile_raw = yaml.safe_load(request.calibration_profile.read_text())
        profile_fingerprint = sha256_json(profile_raw)
        if isinstance(profile_raw, dict):
            profile_version = profile_raw.get("profile_version")
    payload = {
        "benchmark_version": "1",
        "suite_definition_version": SUITE_DEFINITION_VERSION,
        "suite": request.suite,
        "difficulty": request.difficulty,
        "seed": request.seed,
        "models": list(request.models),
        "runners": list(request.runners),
        "calibration_profile_fingerprint": profile_fingerprint,
        "calibration_profile_version": profile_version,
    }
    benchmark_id = "BM-" + sha256_json(payload)[:16]
    root = request.output_dir / benchmark_id
    runners = tuple(_load_runner(item) for item in request.runners)
    runner_ids = [item.metadata.model_id for item in runners]
    if len(runner_ids) != len(set(runner_ids)):
        raise ValueError("runner model IDs must be unique")
    if set(runner_ids) & set(request.models):
        raise ValueError("runner model IDs must not duplicate --models")
    root.mkdir(parents=True, exist_ok=False)
    all_rows: list[dict[str, Any]] = []
    suites_metadata: dict[str, Any] = {}
    descriptors: dict[str, Any] = {}
    baseline_rows: dict[str, float | None] = {}
    for suite in selected:
        rows, metadata, suite_descriptors = _run_one(
            suite,
            request,
            root,
            role="primary-" + suite,
            calibration_profile=_profile_for_suite(request, suite),
            runners=runners,
        )
        for row in rows:
            if suite == "baseline":
                baseline_rows[row["model"]] = cast(float | None, row["pr_auc"])
        if suite != "baseline":
            companion, _, _ = _run_one(
                "baseline",
                request,
                root,
                role="companion-" + suite,
                calibration_profile=_profile_for_suite(request, suite),
                runners=runners,
            )
            baseline_rows.update({item["model"]: item["pr_auc"] for item in companion})
        for row in rows:
            base = baseline_rows.get(row["model"])
            score = row["pr_auc"]
            row["drop_vs_baseline"] = (
                None
                if not isinstance(base, int | float)
                or base == 0
                or not isinstance(score, int | float)
                else (float(score) - float(base)) / float(base)
            )
        all_rows.extend(rows)
        suites_metadata[suite] = metadata
        descriptors[suite] = suite_descriptors
        metadata["logical_fingerprints"]["descriptors"] = sha256_json(suite_descriptors)
    lineage = {
        "generator_versions": {
            suite: metadata["manifest"].get("generator_version")
            for suite, metadata in suites_metadata.items()
        },
        "scenario_mix": {
            suite: metadata["manifest"].get("fraud_counts", {})
            for suite, metadata in suites_metadata.items()
        },
        "calibration_profiles": {
            suite: metadata["manifest"].get("calibration")
            for suite, metadata in suites_metadata.items()
        },
        "stress_parameters": {
            suite: metadata["manifest"].get("resolved_configuration", {})
            for suite, metadata in suites_metadata.items()
        },
        "label_observation_policies": {
            suite: metadata["manifest"].get("label_observation")
            for suite, metadata in suites_metadata.items()
        },
        "temporal_split_ranges": {
            suite: metadata.get("temporal_split_ranges", {})
            for suite, metadata in suites_metadata.items()
        },
        "source_dataset_fingerprints": {
            suite: metadata.get("fingerprints", {}) for suite, metadata in suites_metadata.items()
        },
        "ground_truth_availability_rules": {
            suite: {
                "latent": metadata["catalog"]["latent_truth"],
                "observed": metadata["catalog"]["observed_truth"],
                "prediction_time_cutoff": (
                    "observable source and labels only when available at prediction_time"
                ),
            }
            for suite, metadata in suites_metadata.items()
        },
    }
    results_path = root / "model_results.parquet"
    pl.DataFrame(all_rows).write_parquet(results_path)
    write_json(root / "model_results.jsonl", all_rows)
    descriptors_path = write_json(root / "descriptors.json", descriptors)
    manifest = {
        **payload,
        "benchmark_id": benchmark_id,
        "generator_version": __version__,
        "python": platform.python_version(),
        "selected_suites": list(selected),
        "suites": suites_metadata,
        "lineage": lineage,
        "source_fingerprints": {
            suite: metadata.get("fingerprints", {}).get("source")
            for suite, metadata in suites_metadata.items()
        },
        "dataset_fingerprints": {
            suite: metadata.get("fingerprints", {}).get("dataset")
            for suite, metadata in suites_metadata.items()
        },
        "descriptors": str(descriptors_path.relative_to(root)),
        "results": str(results_path.relative_to(root)),
        "output_fingerprint": sha256_json({"rows": all_rows, "descriptors": descriptors}),
    }
    manifest_path = write_json(root / "benchmark_manifest.json", manifest)
    return BenchmarkResult(
        benchmark_id, root, manifest_path, results_path, descriptors_path, tuple(all_rows)
    )


def _verify_public_pack_calibration(pack: PublicBenchmarkPack, profile_path: Path | None) -> None:
    if pack.calibration is None:
        if profile_path is not None:
            raise ValueError(f"{pack.identity} does not accept a calibration profile")
        return
    if profile_path is None:
        raise ValueError(f"{pack.identity} calibration resource is unavailable")
    raw = yaml.safe_load(profile_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"{pack.identity} calibration profile must be a mapping")
    expected = pack.calibration
    actual = {
        "profile_id": raw.get("profile_id"),
        "profile_version": raw.get("profile_version"),
        "fingerprint": sha256_json(raw),
    }
    for field in ("profile_id", "profile_version", "fingerprint"):
        if expected.get(field) != actual[field]:
            raise ValueError(
                f"{pack.identity} calibration {field} mismatch: "
                f"expected {expected.get(field)!r}, got {actual[field]!r}"
            )


def _version_tuple(value: str) -> tuple[int, int, int]:
    parts = value.split(".")
    if len(parts) != 3 or any(not part.isdigit() for part in parts):
        raise ValueError(f"invalid semantic version: {value}")
    return int(parts[0]), int(parts[1]), int(parts[2])


def _pack_supports_generator(pack: PublicBenchmarkPack) -> bool:
    lower, upper = pack.generator_compatibility.split(",")
    current = _version_tuple(_FROZEN_GENERATOR_COMPATIBILITY_VERSION)
    lower_version = _version_tuple(lower.removeprefix(">="))
    upper_version = _version_tuple(upper.removeprefix("<"))
    return lower_version <= current < upper_version


def run_public_benchmark(
    reference: str,
    *,
    output_dir: Path = Path("runs/benchmarks"),
    models: tuple[str, ...] = ("deterministic_heuristic",),
    runners: tuple[str, ...] = (),
) -> BenchmarkResult:
    """Run one immutable bundled M21 pack and verify its frozen outputs."""

    pack = load_public_pack(reference)
    if not _pack_supports_generator(pack):
        raise ValueError(
            f"{pack.identity} is incompatible with FraudTwin {__version__} "
            f"(requires {pack.generator_compatibility})"
        )
    calibration_profile = _calibration_resource(pack)
    _verify_public_pack_calibration(pack, calibration_profile)
    config = build_suite_config(
        pack.suite,
        difficulty=pack.difficulty,
        seed=pack.seed,
        calibration_profile=calibration_profile,
    )
    if config_hash(config, include_dataset=True) != pack.resolved_configuration_hash:
        raise ValueError(
            f"{pack.identity} resolved configuration differs from the released definition"
        )
    result = run_benchmark(
        BenchmarkRequest(
            suite=pack.suite,
            difficulty=pack.difficulty,
            seed=pack.seed,
            output_dir=output_dir,
            models=models,
            runners=runners,
            calibration_profile=calibration_profile,
        )
    )
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    suite_metadata = manifest["suites"][pack.suite]
    actual_fingerprints = suite_metadata.get("logical_fingerprints", {})
    actual_descriptors = json.loads(result.descriptors_path.read_text(encoding="utf-8"))[pack.suite]
    expected_fingerprints = pack.expected_fingerprints
    if actual_fingerprints != expected_fingerprints:
        raise ValueError(
            f"{pack.identity} logical fingerprint mismatch: "
            f"expected {expected_fingerprints!r}, got {actual_fingerprints!r}"
        )
    if actual_descriptors != pack.expected_descriptors:
        raise ValueError(
            f"{pack.identity} generator descriptors differ from the released definition"
        )
    manifest["public_pack"] = {
        "id": pack.id,
        "version": pack.version,
        "identity": pack.identity,
        "definition_fingerprint": pack.fingerprint,
        "definition_version": PUBLIC_PACK_VERSION,
        "generator_compatibility": pack.generator_compatibility,
        "verification": {
            "configuration_hash": pack.resolved_configuration_hash,
            "logical_fingerprints": actual_fingerprints,
            "descriptors_match": True,
        },
    }
    write_json(result.manifest_path, manifest)
    return result


def verify_public_benchmark(run_dir: Path, *, reference: str | None = None) -> dict[str, Any]:
    """Verify an existing public benchmark artifact without regenerating it."""

    manifest_path = run_dir / "benchmark_manifest.json"
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid benchmark manifest in {run_dir}") from exc
    public_pack = manifest.get("public_pack")
    if not isinstance(public_pack, dict):
        raise ValueError("benchmark artifact has no public-pack verification metadata")
    identity = reference or public_pack.get("identity")
    if not isinstance(identity, str):
        raise ValueError("benchmark artifact does not identify a public pack")
    pack = load_public_pack(identity)
    if public_pack.get("identity") != pack.identity:
        raise ValueError("benchmark artifact public-pack identity differs from its definition")
    if public_pack.get("definition_fingerprint") != pack.fingerprint:
        raise ValueError("benchmark artifact public-pack definition fingerprint mismatch")
    if public_pack.get("generator_compatibility") != pack.generator_compatibility:
        raise ValueError("benchmark artifact generator compatibility differs from its definition")
    if (
        manifest.get("suite") != pack.suite
        or manifest.get("difficulty") != pack.difficulty
        or manifest.get("seed") != pack.seed
    ):
        raise ValueError("benchmark artifact run parameters differ from the released definition")
    if not _pack_supports_generator(pack):
        raise ValueError(
            f"{pack.identity} is incompatible with FraudTwin {__version__} "
            f"(requires {pack.generator_compatibility})"
        )

    suite_metadata = manifest.get("suites", {}).get(pack.suite)
    if not isinstance(suite_metadata, dict):
        raise ValueError(f"benchmark artifact is missing suite metadata: {pack.suite}")

    def artifact_path(relative: object, label: str) -> Path:
        if not isinstance(relative, str) or not relative:
            raise ValueError(f"benchmark artifact is missing {label}")
        root = run_dir.resolve()
        path = (run_dir / relative).resolve()
        try:
            path.relative_to(root)
        except ValueError as exc:
            raise ValueError(f"benchmark artifact {label} must remain inside its root") from exc
        return path

    descriptor_path = artifact_path(manifest.get("descriptors"), "descriptors")
    try:
        descriptors = json.loads(descriptor_path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid benchmark descriptors: {descriptor_path}") from exc
    if descriptors.get(pack.suite) != pack.expected_descriptors:
        raise ValueError(
            f"{pack.identity} generator descriptors differ from the released definition"
        )

    catalog = suite_metadata.get("catalog")
    if not isinstance(catalog, dict):
        raise ValueError("benchmark artifact is missing suite catalog")
    source_run_dir = artifact_path(catalog.get("run_dir"), "source run")
    entities, behavior, source_manifest = load_generated_run(source_run_dir)
    for relative, expected in source_manifest.file_checksums.items():
        path = source_run_dir / relative
        if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
            raise ValueError(f"generated source checksum mismatch: {relative}")
    dataset_path = artifact_path(catalog.get("dataset"), "dataset")
    if not dataset_path.is_file():
        raise ValueError(f"benchmark dataset does not exist: {dataset_path}")
    dataset_manifest_path = artifact_path(catalog.get("dataset_manifest"), "dataset manifest")
    dataset_manifest = json.loads(dataset_manifest_path.read_text(encoding="utf-8"))
    if not dataset_manifest.get("output_fingerprint"):
        raise ValueError("benchmark dataset manifest has no output fingerprint")
    if source_manifest.model_dump(mode="json") != suite_metadata.get("manifest"):
        raise ValueError("generated source manifest differs from the benchmark record")
    validate_ledger(
        entities.accounts,
        behavior.payments,
        behavior.payment_events,
        behavior.ledger_entries,
    )
    for payment in behavior.payments:
        validate_payment_lifecycle(
            payment,
            tuple(
                sorted(
                    (
                        event
                        for event in behavior.payment_events
                        if event.payment_id == payment.payment_id
                    ),
                    key=lambda event: event.event_time,
                )
            ),
        )
    actual_fingerprints = suite_metadata.get("logical_fingerprints", {})
    if actual_fingerprints != pack.expected_fingerprints:
        raise ValueError(
            f"{pack.identity} logical fingerprint mismatch: "
            f"expected {pack.expected_fingerprints!r}, got {actual_fingerprints!r}"
        )

    results_path = artifact_path(manifest.get("results"), "results")
    try:
        results = (
            pl.read_parquet(results_path).to_dicts()
            if results_path.suffix == ".parquet"
            else json.loads(results_path.read_text(encoding="utf-8"))
        )
    except (FileNotFoundError, OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid benchmark results: {results_path}") from exc
    if sha256_json({"rows": results, "descriptors": descriptors}) != manifest.get(
        "output_fingerprint"
    ):
        raise ValueError("benchmark result fingerprint mismatch")
    verification = public_pack.get("verification", {})
    if (
        not isinstance(verification, dict)
        or verification.get("configuration_hash") != pack.resolved_configuration_hash
        or verification.get("logical_fingerprints") != actual_fingerprints
    ):
        raise ValueError("benchmark artifact verification metadata is stale")
    return {
        "benchmark_id": manifest.get("benchmark_id"),
        "pack": pack.identity,
        "suite": pack.suite,
        "logical_fingerprints": actual_fingerprints,
        "descriptors_match": True,
    }


__all__ = [
    "BenchmarkModelRunner",
    "BenchmarkRequest",
    "BenchmarkResult",
    "BenchmarkRunnerInput",
    "PublicBenchmarkPack",
    "RunnerMetadata",
    "STANDARD_SUITES",
    "SUITE_DEFINITION_VERSION",
    "build_suite_config",
    "list_public_packs",
    "load_public_pack",
    "run_public_benchmark",
    "verify_public_benchmark",
    "run_benchmark",
]
