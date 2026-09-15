"""Reproducible fraud stress benchmark orchestration.

The benchmark layer owns suite composition and reporting.  It deliberately
delegates generation to the existing deterministic engines and model scoring to
the M19 prediction contract.
"""

from __future__ import annotations

import importlib
import json
import platform
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from importlib.resources import files
from pathlib import Path
from typing import Any, Literal, Protocol, cast

import polars as pl
import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator

from fraudtwin import __version__
from fraudtwin.config import SimulationRunConfig
from fraudtwin.generation import GeneratedRun, generate
from fraudtwin.ml import (
    ALL_MODEL_NAMES,
    BaselineEvaluationConfig,
    EvaluationResult,
    PredictionRecord,
    evaluate_predictions,
    heuristic_predictions,
    train_baselines,
    write_evaluation,
)
from fraudtwin.reproducibility import sha256_json

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
        {"enabled": True, "target_rate": 0.12, "scenario_count": 5, "hard_negative_rate": 1.0}
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
        {"enabled": True, "unresolved_labels": "include", "label_delay_seconds": 3_600}
    )
    values["dataset"]["splits"] = {"label_delay_gap_seconds": 3_600}
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


def _json_write(path: Path, value: object) -> Path:
    path.write_text(json.dumps(value, indent=2, sort_keys=True, default=str) + "\n")
    return path


def _load_rows(dataset_path: Path) -> list[dict[str, Any]]:
    if not dataset_path.is_file():
        raise ValueError(f"benchmark dataset does not exist: {dataset_path}")
    return pl.read_parquet(dataset_path).to_dicts()


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
    rows = _load_rows(dataset_path)
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
    _json_write(catalog_path, catalog)
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
    if any(item in {"calibrated", "mixed"} for item in selected):
        if request.calibration_profile is None or not request.calibration_profile.is_file():
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
    _json_write(root / "model_results.jsonl", all_rows)
    descriptors_path = _json_write(root / "descriptors.json", descriptors)
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
    manifest_path = _json_write(root / "benchmark_manifest.json", manifest)
    return BenchmarkResult(
        benchmark_id, root, manifest_path, results_path, descriptors_path, tuple(all_rows)
    )


__all__ = [
    "BenchmarkModelRunner",
    "BenchmarkRequest",
    "BenchmarkResult",
    "BenchmarkRunnerInput",
    "RunnerMetadata",
    "STANDARD_SUITES",
    "SUITE_DEFINITION_VERSION",
    "build_suite_config",
    "run_benchmark",
]
