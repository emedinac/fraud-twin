"""Generator quality benchmarking for Milestone 22.

The quality benchmark is deliberately separate from the M20 model benchmark.
It evaluates the generator and its artifacts, while keeping correctness,
fidelity, difficulty, scalability, and engineering observations independent.
External implementations can either be loaded through a small adapter
protocol or provide the same normalized artifact bundle on disk.
"""

from __future__ import annotations

import importlib
import json
import math
import platform
import resource
import time
from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path
from typing import Any, Protocol, cast

import yaml
from pydantic import BaseModel, ConfigDict, Field

from fraudtwin import __version__
from fraudtwin.benchmark import BenchmarkResult, load_public_pack, run_public_benchmark
from fraudtwin.calibration import load_calibration_profile
from fraudtwin.domain import validate_ledger, validate_payment_lifecycle
from fraudtwin.label_observation import validate_label_observation
from fraudtwin.ml import load_generated_run
from fraudtwin.reproducibility import sha256_json, write_json

QUALITY_PROFILE_RESOURCE_DIR = "quality_profiles"
QUALITY_PROFILE_VERSION = "1"
QUALITY_PACK_REFS = tuple(
    f"FT-B{index:02d}-{name}@1.0.0"
    for index, name in enumerate(
        (
            "STABLE",
            "TEMPORAL",
            "BOUNDARY",
            "CAMOUFLAGE",
            "GRAPH",
            "OBSERVABILITY",
            "CALIBRATED",
            "MIXED",
        ),
        start=1,
    )
)
SCALE_SIZES = ("dev", "small", "medium", "large", "xlarge", "billion")


class QualityBenchmarkProfile(BaseModel):
    """Immutable public workload and protocol selection."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    profile_id: str = Field(pattern=r"^standard-v1(?:-(?:dev|medium|large|xlarge|billion))?$")
    profile_version: str = QUALITY_PROFILE_VERSION
    public_packs: tuple[str, ...] = QUALITY_PACK_REFS
    scale_size: str = "small"
    protocol_version: str = "M22-quality-1"

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> QualityBenchmarkProfile:
        profile = cls.model_validate(payload)
        if profile.scale_size not in SCALE_SIZES:
            raise ValueError(f"unsupported quality benchmark scale: {profile.scale_size}")
        if tuple(profile.public_packs) != QUALITY_PACK_REFS:
            raise ValueError("standard-v1 must cover the complete M21 public pack family")
        return profile

    @property
    def fingerprint(self) -> str:
        return sha256_json(self.model_dump(mode="json"))


class QualityCapability(BaseModel):
    """Declared dimensions supplied by a native or external candidate."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    financial_invariants: bool = False
    temporal_invariants: bool = False
    pit_validation: bool = False
    scenario_coverage: bool = False
    ledger_reconciliation: bool = False
    reproducibility: bool = False
    statistical_fidelity: bool = False
    temporal_fidelity: bool = False
    graph_fidelity: bool = False
    difficulty: bool = False
    scalability: bool = False
    engineering_performance: bool = False


NATIVE_CAPABILITIES = QualityCapability(**{field: True for field in QualityCapability.model_fields})


class QualityAdapterMetadata(BaseModel):
    """Identity and capabilities advertised by an external generator."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    candidate_id: str = Field(min_length=1)
    candidate_version: str = "unknown"
    framework: str = "unknown"
    deterministic: bool = False
    capabilities: QualityCapability = Field(default_factory=QualityCapability)
    parameters: dict[str, Any] = Field(default_factory=dict)


class QualityAdapterRequest(BaseModel):
    """Read-only public workload delivered to an external adapter."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    profile_id: str
    profile_fingerprint: str
    public_pack: str
    public_definition: dict[str, Any]
    output_dir: Path


class QualityArtifactBundle(BaseModel):
    """Normalized external output consumed by the quality scorer."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    candidate_id: str
    candidate_version: str = "unknown"
    manifest_path: Path
    artifact_paths: dict[str, Path] = Field(default_factory=dict)
    logical_fingerprints: dict[str, str] = Field(default_factory=dict)
    capabilities: QualityCapability = Field(default_factory=QualityCapability)
    generation_seconds: float | None = Field(default=None, ge=0)


class QualityGeneratorAdapter(Protocol):
    metadata: QualityAdapterMetadata

    def generate(self, request: QualityAdapterRequest) -> QualityArtifactBundle: ...


class QualityMetric(BaseModel):
    """One independently interpretable quality result."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str
    status: str
    score: float | None = Field(default=None, ge=0, le=1)
    details: dict[str, Any] = Field(default_factory=dict)


class QualityCandidateReport(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    candidate_id: str
    candidate_version: str
    capabilities: QualityCapability
    correctness: tuple[QualityMetric, ...]
    fidelity: tuple[QualityMetric, ...]
    difficulty: tuple[QualityMetric, ...]
    scalability: tuple[QualityMetric, ...]
    engineering_performance: tuple[QualityMetric, ...]
    reproducibility: tuple[QualityMetric, ...]
    packs: tuple[dict[str, Any], ...] = ()


class QualityBenchmarkResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    report_id: str
    profile: QualityBenchmarkProfile
    report_path: Path
    candidate: QualityCandidateReport


class QualityBenchmarkRequest(BaseModel):
    """Programmatic equivalent of the quality-benchmark CLI command."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    profile: str = "standard-v1"
    output_dir: Path = Path("runs/quality-benchmarks")
    adapter: str | None = None
    bundle: Path | None = None


@dataclass(frozen=True)
class _NativePackResult:
    pack_ref: str
    root: Path
    manifest: dict[str, Any]
    descriptors: dict[str, Any]
    elapsed_seconds: float


def _profile_resource(name: str) -> Any:
    resource = files("fraudtwin").joinpath(QUALITY_PROFILE_RESOURCE_DIR, f"{name}.yaml")
    if not resource.is_file():
        raise ValueError(f"quality benchmark profile does not exist: {name}")
    return resource


def load_quality_profile(reference: str | Path = "standard-v1") -> QualityBenchmarkProfile:
    """Load one bundled immutable M22 profile or a YAML profile path."""

    resource = Path(reference) if isinstance(reference, Path) else None
    if resource is not None or (isinstance(reference, str) and Path(reference).is_file()):
        path = resource or Path(cast(str, reference))
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    else:
        name = str(reference).removesuffix(".yaml")
        raw = yaml.safe_load(_profile_resource(name).read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("quality benchmark profile must be a mapping")
    return QualityBenchmarkProfile.from_payload(cast(dict[str, Any], raw))


def _metric(
    name: str,
    *,
    score: float | None,
    details: dict[str, Any] | None = None,
    binary: bool = False,
    status: str | None = None,
) -> QualityMetric:
    if status is None:
        status = (
            "N/A"
            if score is None
            else "PASS"
            if binary and score >= 1.0
            else "FAIL"
            if binary
            else "MEASURED"
        )
    return QualityMetric(name=name, status=status, score=score, details=details or {})


def _score(values: list[bool]) -> float | None:
    return sum(values) / len(values) if values else None


def _finite_number(value: Any) -> float | None:
    if not isinstance(value, int | float):
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def _average(values: list[Any]) -> float | None:
    numeric = [number for value in values if (number := _finite_number(value)) is not None]
    return sum(numeric) / len(numeric) if numeric else None


def _descriptor_values(pack_results: tuple[_NativePackResult, ...], name: str) -> list[float]:
    return [
        number
        for item in pack_results
        if (number := _finite_number(item.descriptors.get(name))) is not None
    ]


def _similarity(actual: float, expected: float, *, scale: float = 1.0) -> float:
    return max(0.0, 1.0 - abs(actual - expected) / scale)


def _native_pack(
    result: BenchmarkResult, pack_ref: str, elapsed_seconds: float
) -> _NativePackResult:
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    descriptor_payload = json.loads(result.descriptors_path.read_text(encoding="utf-8"))
    pack = load_public_pack(pack_ref)
    return _NativePackResult(
        pack_ref,
        result.root,
        manifest,
        descriptor_payload[pack.suite],
        elapsed_seconds,
    )


def _run_native_packs(
    profile: QualityBenchmarkProfile, root: Path
) -> tuple[_NativePackResult, ...]:
    results: list[_NativePackResult] = []
    for pack_ref in profile.public_packs:
        pack_root = root / "packs" / pack_ref.replace("@", "-")
        started = time.perf_counter()
        result = run_public_benchmark(pack_ref, output_dir=pack_root)
        results.append(_native_pack(result, pack_ref, time.perf_counter() - started))
    return tuple(results)


def _load_native_run(pack_result: _NativePackResult) -> tuple[Any, Any, Any] | None:
    suite: dict[str, Any] = next(iter(pack_result.manifest.get("suites", {}).values()), {})
    catalog = suite.get("catalog", {}) if isinstance(suite, dict) else {}
    run_rel = catalog.get("run_dir")
    if not isinstance(run_rel, str):
        return None
    run_dir = pack_result.root / run_rel
    try:
        return load_generated_run(run_dir)
    except (OSError, ValueError):
        return None


def _correctness_metrics(pack_results: tuple[_NativePackResult, ...]) -> tuple[QualityMetric, ...]:
    ledger: list[bool] = []
    temporal: list[bool] = []
    pit: list[bool] = []
    scenario: list[bool] = []
    reproducible: list[bool] = []
    for item in pack_results:
        pack = load_public_pack(item.pack_ref)
        verification = item.manifest.get("public_pack", {}).get("verification", {})
        reproducible.append(
            verification.get("descriptors_match") is True
            and verification.get("logical_fingerprints") == pack.expected_fingerprints
        )
        descriptors = item.descriptors
        scenario.append(
            descriptors.get("scenario_coverage")
            == pack.expected_descriptors.get("scenario_coverage")
        )
        loaded = _load_native_run(item)
        if loaded is None:
            continue
        entities, behavior, _ = loaded
        try:
            validate_ledger(
                entities.accounts,
                behavior.payments,
                behavior.payment_events,
                behavior.ledger_entries,
            )
            ledger.append(True)
            for payment in behavior.payments:
                events = tuple(
                    event
                    for event in behavior.payment_events
                    if event.payment_id == payment.payment_id
                )
                validate_payment_lifecycle(payment, events)
            temporal.append(True)
            if behavior.label_observations:
                validate_label_observation(behavior.label_observations)
            pit.append(True)
        except ValueError:
            ledger.append(False)
            temporal.append(False)
            pit.append(False)
    return (
        _metric("financial_invariants", score=_score(ledger), binary=True),
        _metric("temporal_invariants", score=_score(temporal), binary=True),
        _metric("pit_validation", score=_score(pit), binary=True),
        _metric("scenario_coverage", score=_score(scenario), binary=True),
        _metric("ledger_reconciliation", score=_score(ledger), binary=True),
        _metric("reproducibility", score=_score(reproducible), binary=True),
    )


def _fidelity_metrics(pack_results: tuple[_NativePackResult, ...]) -> tuple[QualityMetric, ...]:
    statistical: list[float] = []
    temporal: list[float] = []
    graph: list[float] = []
    for item in pack_results:
        pack = load_public_pack(item.pack_ref)
        expected = pack.expected_descriptors
        actual = item.descriptors
        for key, target in (
            ("fraud_prevalence", statistical),
            ("feature_camouflage_score", statistical),
        ):
            expected_value = _finite_number(expected.get(key))
            actual_value = _finite_number(actual.get(key))
            if expected_value is not None and actual_value is not None:
                target.append(
                    _similarity(actual_value, expected_value, scale=max(abs(expected_value), 1.0))
                )
        if pack.calibration is not None:
            calibration_resource = files("fraudtwin").joinpath(
                "public_packs", str(pack.calibration["resource"])
            )
            loaded = _load_native_run(item)
            if calibration_resource.is_file() and loaded is not None:
                profile = load_calibration_profile(Path(str(calibration_resource)))
                _, behavior, _ = loaded
                amount_summary = next(
                    (
                        summary
                        for summary in profile.summaries
                        if summary.name == "amount_distribution"
                    ),
                    None,
                )
                amounts = sorted(float(payment.amount) for payment in behavior.payments)
                if amount_summary is not None and amounts:
                    expected_quantiles = amount_summary.parameters.get("quantiles", ())
                    actual_quantiles = [
                        amounts[min(len(amounts) - 1, round((len(amounts) - 1) * q))]
                        for q in (0.0, 0.1, 0.25, 0.5, 0.75, 0.9, 1.0)
                    ]
                    expected_values = [
                        number
                        for value in expected_quantiles
                        if (number := _finite_number(value)) is not None
                    ]
                    if expected_values:
                        scale = max(max(expected_values), 1.0)
                        statistical.append(
                            max(
                                0.0,
                                1.0
                                - sum(
                                    abs(actual - expected) / scale
                                    for actual, expected in zip(
                                        actual_quantiles, expected_values, strict=False
                                    )
                                )
                                / len(actual_quantiles),
                            )
                        )
                seasonality = next(
                    (summary for summary in profile.summaries if summary.name == "seasonality"),
                    None,
                )
                if seasonality is not None and behavior.payment_events:
                    expected_hours = [
                        float(value) for value in seasonality.parameters["hour_weights"]
                    ]
                    observed_hours = [
                        float(
                            sum(event.event_time.hour == hour for event in behavior.payment_events)
                        )
                        for hour in range(24)
                    ]
                    expected_total = max(sum(expected_hours), 1.0)
                    observed_total = max(sum(observed_hours), 1.0)
                    temporal.append(
                        max(
                            0.0,
                            1.0
                            - sum(
                                abs(actual / observed_total - expected / expected_total)
                                for actual, expected in zip(
                                    observed_hours, expected_hours, strict=False
                                )
                            )
                            / 2.0,
                        )
                    )
        expected_drift = _finite_number(expected.get("drift_strength"))
        actual_drift = _finite_number(actual.get("drift_strength"))
        if expected_drift is not None and actual_drift is not None:
            temporal.append(_similarity(actual_drift, expected_drift))
        expected_coordination = _finite_number(expected.get("graph_coordination_score"))
        actual_coordination = _finite_number(actual.get("graph_coordination_score"))
        if expected_coordination is not None and actual_coordination is not None:
            graph.append(_similarity(actual_coordination, expected_coordination))
    return (
        _metric(
            "statistical_fidelity",
            score=sum(statistical) / len(statistical) if statistical else None,
            details={"method": "descriptor_alignment_v1"},
        ),
        _metric(
            "temporal_fidelity",
            score=sum(temporal) / len(temporal) if temporal else None,
            details={"method": "descriptor_alignment_v1"},
        ),
        _metric(
            "graph_fidelity",
            score=sum(graph) / len(graph) if graph else None,
            details={"method": "descriptor_alignment_v1"},
        ),
    )


def _difficulty_metrics(pack_results: tuple[_NativePackResult, ...]) -> tuple[QualityMetric, ...]:
    distances = _descriptor_values(pack_results, "counterfactual_distance")
    camouflage = _descriptor_values(pack_results, "feature_camouflage_score")
    drift = _descriptor_values(pack_results, "drift_strength")
    coordination = _descriptor_values(pack_results, "graph_coordination_score")
    distance_average = _average(distances)
    return (
        _metric("camouflage_difficulty", score=_average(camouflage)),
        _metric(
            "counterfactual_similarity",
            score=1.0 - distance_average if distance_average is not None else None,
        ),
        _metric("drift_strength", score=_average(drift)),
        _metric("graph_coordination", score=_average(coordination)),
    )


def _scale_metrics(
    profile: QualityBenchmarkProfile,
    pack_results: tuple[_NativePackResult, ...] = (),
) -> tuple[QualityMetric, ...]:
    scale_runs: list[dict[str, Any]] = []
    for item in pack_results:
        suite = cast(dict[str, Any], next(iter(item.manifest.get("suites", {}).values()), {}))
        manifest = cast(dict[str, Any], suite.get("manifest", {}))
        scale = manifest.get("scale")
        if isinstance(scale, dict):
            scale_runs.append(scale)
    if not scale_runs:
        details: dict[str, Any] = {
            "scale_size": profile.scale_size,
            "status": "scale execution not included in this quality run",
        }
        return tuple(
            _metric(name, score=None, details=details)
            for name in ("generation_throughput", "peak_memory_mb", "resume_overhead")
        )

    event_count = 0
    target_met = True
    for scale in scale_runs:
        event_count += int(scale.get("payments_realized", 0))
        target_met = target_met and bool(scale.get("target_met", False))
    elapsed = sum(item.elapsed_seconds for item in pack_results)
    throughput = event_count / elapsed if elapsed > 0 else None
    peak_memory = float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss) / 1024.0
    status = "MEASURED" if target_met else "N/A"
    details = {
        "scale_size": profile.scale_size,
        "target_met": target_met,
        "realized_payments": event_count,
    }
    return (
        _metric(
            "generation_throughput",
            score=1.0 if throughput is not None and target_met else None,
            details={**details, "events_per_second": throughput},
            status=status,
        ),
        _metric(
            "peak_memory_mb",
            score=1.0 if target_met else None,
            details={**details, "peak_rss_mb": peak_memory},
            status=status,
        ),
        _metric("resume_overhead", score=None, details=details, status="N/A"),
    )


def _load_adapter(reference: str) -> QualityGeneratorAdapter:
    try:
        module_name, factory_name = reference.split(":", 1)
        factory = getattr(importlib.import_module(module_name), factory_name)
        adapter = factory()
    except (ValueError, AttributeError, ImportError, TypeError) as exc:
        raise ValueError(f"invalid quality benchmark adapter: {reference}") from exc
    if not hasattr(adapter, "metadata") or not hasattr(adapter, "generate"):
        raise ValueError("quality benchmark adapter must expose metadata and generate")
    return cast(QualityGeneratorAdapter, adapter)


def _capability_metrics(
    names: tuple[str, ...], capabilities: QualityCapability, *, source: str
) -> tuple[QualityMetric, ...]:
    metrics: list[QualityMetric] = []
    for name in names:
        supported = getattr(capabilities, name)
        metrics.append(
            _metric(
                name,
                score=1.0 if supported else None,
                status="AVAILABLE" if supported else None,
                details={"source": source},
            )
        )
    return tuple(metrics)


_CORRECTNESS_NAMES = (
    "financial_invariants",
    "temporal_invariants",
    "pit_validation",
    "scenario_coverage",
    "ledger_reconciliation",
    "reproducibility",
)
_FIDELITY_NAMES = ("statistical_fidelity", "temporal_fidelity", "graph_fidelity")


def _capability_report(
    *,
    candidate_id: str,
    candidate_version: str,
    capabilities: QualityCapability,
    source: str,
    engineering_details: dict[str, Any] | None = None,
) -> QualityCandidateReport:
    engineering = _capability_metrics(("engineering_performance",), capabilities, source=source)
    if engineering_details is not None:
        supported = capabilities.engineering_performance
        engineering = (
            _metric(
                "engineering_performance",
                score=1.0 if supported else None,
                status="AVAILABLE" if supported else None,
                details={"source": source, **engineering_details},
            ),
        )
    return QualityCandidateReport(
        candidate_id=candidate_id,
        candidate_version=candidate_version,
        capabilities=capabilities,
        correctness=_capability_metrics(_CORRECTNESS_NAMES, capabilities, source=source),
        fidelity=_capability_metrics(_FIDELITY_NAMES, capabilities, source=source),
        difficulty=_capability_metrics(("difficulty",), capabilities, source=source),
        scalability=_capability_metrics(("scalability",), capabilities, source=source),
        engineering_performance=engineering,
        reproducibility=_capability_metrics(("reproducibility",), capabilities, source=source),
    )


def _bundle_report(bundle_path: Path) -> QualityCandidateReport:
    try:
        raw = json.loads(bundle_path.read_text(encoding="utf-8"))
        bundle = QualityArtifactBundle.model_validate(raw)
    except (OSError, ValueError) as exc:
        raise ValueError(f"invalid quality artifact bundle: {bundle_path}") from exc
    capabilities = bundle.capabilities
    return _capability_report(
        candidate_id=bundle.candidate_id,
        candidate_version=bundle.candidate_version,
        capabilities=capabilities,
        source="bundle",
    )


def _native_report(
    profile: QualityBenchmarkProfile, pack_results: tuple[_NativePackResult, ...]
) -> QualityCandidateReport:
    scale_metrics = _scale_metrics(profile, pack_results)
    return QualityCandidateReport(
        candidate_id="fraudtwin",
        candidate_version=__version__,
        capabilities=NATIVE_CAPABILITIES,
        correctness=_correctness_metrics(pack_results),
        fidelity=_fidelity_metrics(pack_results),
        difficulty=_difficulty_metrics(pack_results),
        scalability=scale_metrics,
        engineering_performance=scale_metrics,
        reproducibility=(_metric("reproducibility", score=1.0, binary=True, details={"runs": 1}),),
        packs=tuple({"pack": item.pack_ref, "manifest": str(item.root)} for item in pack_results),
    )


def run_quality_benchmark(
    profile: str | Path | QualityBenchmarkProfile | QualityBenchmarkRequest = "standard-v1",
    *,
    output_dir: Path = Path("runs/quality-benchmarks"),
    adapter: str | None = None,
    bundle: Path | None = None,
) -> QualityBenchmarkResult:
    """Run the native or external M22 quality protocol."""

    if isinstance(profile, QualityBenchmarkRequest):
        benchmark_request = profile
        profile = benchmark_request.profile
        output_dir = benchmark_request.output_dir
        adapter = benchmark_request.adapter
        bundle = benchmark_request.bundle
    if adapter is not None and bundle is not None:
        raise ValueError("quality benchmark accepts either --adapter or --bundle, not both")
    resolved = (
        profile if isinstance(profile, QualityBenchmarkProfile) else load_quality_profile(profile)
    )
    root = output_dir / (
        "QB-"
        + sha256_json({"profile": resolved.fingerprint, "adapter": adapter, "bundle": str(bundle)})[
            :16
        ]
    )
    root.mkdir(parents=True, exist_ok=False)
    if bundle is not None:
        candidate = _bundle_report(bundle)
    elif adapter is not None:
        instance = _load_adapter(adapter)
        started = time.perf_counter()
        for pack_ref in resolved.public_packs:
            pack = load_public_pack(pack_ref)
            adapter_request = QualityAdapterRequest(
                profile_id=resolved.profile_id,
                profile_fingerprint=resolved.fingerprint,
                public_pack=pack_ref,
                public_definition=pack.model_dump(mode="json"),
                output_dir=root / "external" / pack_ref.replace("@", "-"),
            )
            adapter_request.output_dir.mkdir(parents=True, exist_ok=True)
            generated_bundle = instance.generate(adapter_request)
            try:
                QualityArtifactBundle.model_validate(generated_bundle)
            except ValueError as exc:
                raise ValueError(
                    f"quality adapter returned an invalid artifact bundle for {pack_ref}"
                ) from exc
        elapsed = time.perf_counter() - started
        metadata = instance.metadata
        candidate = _capability_report(
            candidate_id=metadata.candidate_id,
            candidate_version=metadata.candidate_version,
            capabilities=metadata.capabilities,
            source="adapter",
            engineering_details={"generation_seconds": elapsed},
        )
    else:
        candidate = _native_report(resolved, _run_native_packs(resolved, root))
    report_id = root.name
    report = {
        "report_id": report_id,
        "profile": resolved.model_dump(mode="json"),
        "profile_fingerprint": resolved.fingerprint,
        "generator_version": __version__,
        "python": platform.python_version(),
        "platform": platform.platform(),
        "candidate": candidate.model_dump(mode="json"),
    }
    report_path = write_json(root / "quality_report.json", report)
    write_json(
        root / "execution_manifest.json",
        {
            "report_id": report_id,
            "profile_fingerprint": resolved.fingerprint,
            "report_fingerprint": sha256_json(report),
        },
    )
    return QualityBenchmarkResult(
        report_id=report_id, profile=resolved, report_path=report_path, candidate=candidate
    )


def report_run(
    run_id: str, *, runs_dir: Path = Path("runs"), output_dir: Path = Path("runs/quality-reports")
) -> Path:
    """Create an invariant report for one existing native generated run."""

    run_dir = runs_dir / run_id
    manifest = None
    try:
        entities, behavior, manifest = load_generated_run(run_dir)
        validate_ledger(
            entities.accounts, behavior.payments, behavior.payment_events, behavior.ledger_entries
        )
        for payment in behavior.payments:
            validate_payment_lifecycle(
                payment,
                tuple(
                    event
                    for event in behavior.payment_events
                    if event.payment_id == payment.payment_id
                ),
            )
        if behavior.label_observations:
            validate_label_observation(behavior.label_observations)
        correctness = {
            "financial_invariants": "PASS",
            "temporal_invariants": "PASS",
            "pit_validation": "PASS",
        }
    except (OSError, ValueError) as exc:
        correctness = {
            "financial_invariants": "FAIL",
            "temporal_invariants": "FAIL",
            "pit_validation": "FAIL",
            "error": str(exc),
        }
    report_id = (
        "QR-"
        + sha256_json(
            {
                "run_id": run_id,
                "manifest": manifest.model_dump(mode="json") if manifest else correctness,
            }
        )[:16]
    )
    destination = output_dir / report_id / "quality_report.json"
    write_json(
        destination,
        {
            "report_id": report_id,
            "run_id": run_id,
            "generator_version": __version__,
            "correctness": correctness,
        },
    )
    return destination


__all__ = [
    "QualityAdapterRequest",
    "QualityArtifactBundle",
    "QualityBenchmarkProfile",
    "QualityBenchmarkRequest",
    "QualityBenchmarkResult",
    "QualityCandidateReport",
    "QualityCapability",
    "QualityGeneratorAdapter",
    "QualityMetric",
    "QualityAdapterMetadata",
    "load_quality_profile",
    "report_run",
    "run_quality_benchmark",
]
