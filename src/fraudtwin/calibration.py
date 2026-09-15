"""Deterministic aggregate reference calibration for FraudTwin.

Calibration deliberately produces parameter summaries rather than examples or
rows.  The causal payment, ledger, fraud, and graph generators remain the
authoritative producers of output records.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections import Counter
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol, cast

import polars as pl
import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from fraudtwin.reproducibility import canonical_json, sha256_json
from fraudtwin.seed import create_stream_rng

CALIBRATION_VERSION = "1"
CALIBRATION_STREAM_ID = "milestone-16:calibration:fit"
CALIBRATED_STREAM_ID = "milestone-16:calibrated-generation"
CALIBRATED_AMOUNT_STREAM_ID = f"{CALIBRATED_STREAM_ID}:amounts"
CALIBRATED_TIMING_STREAM_ID = f"{CALIBRATED_STREAM_ID}:timing"
CALIBRATED_BALANCE_STREAM_ID = f"{CALIBRATED_STREAM_ID}:balances"
CALIBRATED_MERCHANT_STREAM_ID = f"{CALIBRATED_STREAM_ID}:merchants"
REFERENCE_REQUIRED_COLUMNS = ("amount", "event_time", "customer_id")
REFERENCE_OPTIONAL_COLUMNS = (
    "merchant_id",
    "merchant_category",
    "account_balance",
    "payer_account_id",
    "payee_account_id",
    "campaign_id",
)
SUMMARY_NAMES = (
    "amount_distribution",
    "inter_arrival",
    "seasonality",
    "merchant_frequency",
    "customer_activity",
    "feature_dependencies",
    "account_balance",
    "transaction_count",
    "graph_statistics",
    "campaign_statistics",
)
_NUMERIC_DTYPES = frozenset({pl.Float32, pl.Float64, pl.Int32, pl.Int64})


class CalibrationModel(Protocol):
    name: str
    version: str
    supported_fields: tuple[str, ...]
    deterministic: bool
    compatibility: Mapping[str, str]

    def fit(self, reference: ReferenceDataset, seed: int) -> tuple[StatisticalSummary, ...]: ...


class CalibrationMetric(Protocol):
    name: str
    version: str
    required_fields: tuple[str, ...]
    deterministic: bool

    def score(self, reference: ReferenceDataset, generated: Mapping[str, object]) -> float: ...


Scalar = str | int | float | bool | None


class StatisticalSummary(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    name: str
    version: str = CALIBRATION_VERSION
    available: bool = True
    fields: tuple[str, ...] = ()
    parameters: dict[str, Any] = Field(default_factory=dict)
    fingerprint: str = ""

    @model_validator(mode="after")
    def fingerprint_is_stable(self) -> StatisticalSummary:
        def freeze(value: Any) -> Any:
            if isinstance(value, list | tuple):
                return tuple(freeze(item) for item in value)
            if isinstance(value, dict):
                return {str(key): freeze(item) for key, item in value.items()}
            return value

        object.__setattr__(self, "parameters", freeze(self.parameters))

        def finite(value: Any) -> bool:
            if isinstance(value, float):
                return math.isfinite(value)
            if isinstance(value, dict):
                return all(finite(item) for item in value.values())
            if isinstance(value, tuple | list):
                return all(finite(item) for item in value)
            return True

        if not finite(self.parameters):
            raise ValueError("statistical summary parameters must be finite")
        expected = sha256_json(
            {
                "name": self.name,
                "version": self.version,
                "available": self.available,
                "fields": self.fields,
                "parameters": self.parameters,
            }
        )
        if self.fingerprint and self.fingerprint != expected:
            raise ValueError("statistical summary fingerprint does not match its contents")
        object.__setattr__(self, "fingerprint", expected)
        return self


class FittedDistribution(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    name: str
    version: str = CALIBRATION_VERSION
    quantiles: tuple[float, ...] = ()
    probabilities: tuple[float, ...] = ()
    minimum: float | None = None
    maximum: float | None = None

    @field_validator("quantiles", "probabilities")
    @classmethod
    def finite_values(cls, value: tuple[float, ...]) -> tuple[float, ...]:
        if any(not math.isfinite(item) for item in value):
            raise ValueError("distribution values must be finite")
        return value


class FeatureDependency(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    source: str
    target: str
    version: str = CALIBRATION_VERSION
    conditional_weights: dict[str, float] = Field(default_factory=dict)

    @field_validator("conditional_weights")
    @classmethod
    def valid_weights(cls, value: dict[str, float]) -> dict[str, float]:
        if any(not math.isfinite(item) or item < 0 for item in value.values()):
            raise ValueError("dependency weights must be finite and non-negative")
        if value and sum(value.values()) <= 0:
            raise ValueError("dependency weights must have a positive sum")
        return value


class CalibrationProvenance(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    calibration_version: str = CALIBRATION_VERSION
    source_fingerprint: str
    source_schema_fingerprint: str
    row_count: int = Field(ge=1)
    seed: int = Field(ge=0)
    configuration_hash: str
    stream_ids: tuple[str, ...] = (CALIBRATION_STREAM_ID,)
    model_versions: dict[str, str] = Field(default_factory=dict)


class CalibrationProfile(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    profile_id: str
    profile_version: str = CALIBRATION_VERSION
    provenance: CalibrationProvenance
    summaries: tuple[StatisticalSummary, ...]
    distributions: tuple[FittedDistribution, ...] = ()
    dependencies: tuple[FeatureDependency, ...] = ()

    @model_validator(mode="after")
    def validate_profile(self) -> CalibrationProfile:
        names = [summary.name for summary in self.summaries]
        if len(names) != len(set(names)):
            raise ValueError("calibration summary names must be unique")
        if not self.profile_id.startswith("CAL-"):
            raise ValueError("calibration profile IDs must start with CAL-")
        return self


class ResolvedCalibration(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    enabled: bool
    profile_id: str | None = None
    profile: CalibrationProfile | None = None
    effective_configuration_hash: str = ""
    stream_ids: tuple[str, ...] = ()


class FidelityMetric(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    name: str
    version: str = CALIBRATION_VERSION
    status: str = "available"
    score: float | None = Field(default=None, ge=0, le=1)
    weight: float = Field(default=1.0, gt=0)
    reference_summary_fingerprint: str | None = None
    generated_summary_fingerprint: str | None = None
    details: dict[str, Scalar] = Field(default_factory=dict)


class FidelityReport(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    report_version: str = CALIBRATION_VERSION
    profile_id: str | None = None
    metrics: tuple[FidelityMetric, ...]
    composite_score: float | None = Field(default=None, ge=0, le=1)
    report_fingerprint: str = ""

    @model_validator(mode="after")
    def fingerprint_is_stable(self) -> FidelityReport:
        payload = self.model_dump(mode="json", exclude={"report_fingerprint"})
        object.__setattr__(self, "report_fingerprint", sha256_json(payload))
        return self


class ReferenceDataset:
    """Validated reference data held only for the duration of fitting/scoring."""

    __slots__ = ("frame", "columns", "source_fingerprint", "schema_fingerprint")

    def __init__(self, frame: pl.DataFrame, *, source_fingerprint: str, schema_fingerprint: str):
        self.frame = frame
        self.columns = tuple(frame.columns)
        self.source_fingerprint = source_fingerprint
        self.schema_fingerprint = schema_fingerprint


def _schema_fingerprint(frame: pl.DataFrame) -> str:
    return sha256_json({"columns": [(name, str(dtype)) for name, dtype in frame.schema.items()]})


def _source_fingerprint(frame: pl.DataFrame) -> str:
    # Sorting canonical scalar rows makes the fingerprint independent of
    # Parquet row-group/order details while retaining no rows in artifacts.
    rows = frame.select(sorted(frame.columns)).sort(sorted(frame.columns)).to_dicts()
    return hashlib.sha256(canonical_json(rows).encode("utf-8")).hexdigest()


def _triangle_count(edges: list[tuple[str, str]]) -> int:
    """Count undirected triangles without retaining source identifiers."""

    neighbors: dict[str, set[str]] = {}
    for left, right in edges:
        if left == right:
            continue
        neighbors.setdefault(left, set()).add(right)
        neighbors.setdefault(right, set()).add(left)
    nodes = sorted(neighbors)
    return sum(
        1
        for index, left in enumerate(nodes)
        for right in nodes[index + 1 :]
        if right in neighbors[left]
        for third in neighbors[left] & neighbors[right]
        if third > right
    )


def load_reference_data(path: Path | str) -> ReferenceDataset:
    """Load and strictly validate one canonical Parquet reference table."""

    path = Path(path)
    if path.suffix.lower() != ".parquet":
        raise ValueError("calibration reference must be a Parquet file")
    if not path.is_file():
        raise ValueError(f"reference file does not exist: {path}")
    try:
        frame = pl.read_parquet(path)
    except (OSError, pl.exceptions.PolarsError) as exc:
        raise ValueError(f"unable to read calibration reference: {path}") from exc
    missing = set(REFERENCE_REQUIRED_COLUMNS) - set(frame.columns)
    if missing:
        raise ValueError(f"reference is missing required columns: {', '.join(sorted(missing))}")
    unknown = set(frame.columns) - set(REFERENCE_REQUIRED_COLUMNS) - set(REFERENCE_OPTIONAL_COLUMNS)
    if unknown:
        raise ValueError(f"reference contains unsupported columns: {', '.join(sorted(unknown))}")
    if frame.height == 0:
        raise ValueError("calibration reference must contain at least one row")
    if frame.schema["amount"] not in _NUMERIC_DTYPES:
        raise ValueError("reference amount must be numeric")
    event_dtype = frame.schema["event_time"]
    if not isinstance(event_dtype, pl.Datetime) or event_dtype.time_zone != "UTC":
        raise ValueError("reference event_time must be a datetime column with timezone UTC")
    invalid = frame.filter(
        pl.col("amount").is_null()
        | ~pl.col("amount").is_finite()
        | (pl.col("amount") <= 0)
        | pl.col("event_time").is_null()
        | pl.col("customer_id").is_null()
        | (pl.col("customer_id").cast(pl.Utf8).str.len_chars() == 0)
    )
    if invalid.height:
        raise ValueError("reference contains null, non-finite, empty, or non-positive core values")
    if "account_balance" in frame.columns:
        values = frame.get_column("account_balance")
        if values.dtype not in _NUMERIC_DTYPES:
            raise ValueError("reference account_balance must be numeric")
        if frame.filter(
            pl.col("account_balance").is_null() | ~pl.col("account_balance").is_finite()
        ).height:
            raise ValueError("reference account_balance must contain only finite values")
    return ReferenceDataset(
        frame,
        source_fingerprint=_source_fingerprint(frame),
        schema_fingerprint=_schema_fingerprint(frame),
    )


def _summary(name: str, fields: tuple[str, ...], parameters: dict[str, Any]) -> StatisticalSummary:
    return StatisticalSummary(name=name, fields=fields, parameters=parameters)


def _distribution(name: str, values: list[float]) -> FittedDistribution:
    ordered = sorted(values)
    quantiles = tuple(
        float(ordered[min(len(ordered) - 1, round((len(ordered) - 1) * q))])
        for q in (0.0, 0.1, 0.25, 0.5, 0.75, 0.9, 1.0)
    )
    return FittedDistribution(
        name=name,
        quantiles=quantiles,
        probabilities=(0.0, 0.1, 0.25, 0.5, 0.75, 0.9, 1.0),
        minimum=quantiles[0],
        maximum=quantiles[-1],
    )


def _core_summaries(
    amounts: list[float], timestamps: list[datetime], customer_counts: Counter[str]
) -> tuple[list[StatisticalSummary], list[FittedDistribution]]:
    """Fit summaries that are always available in the reference contract."""

    amount_distribution = _distribution("amount", amounts)
    summaries = [
        _summary("amount_distribution", ("amount",), {"quantiles": amount_distribution.quantiles}),
        _summary(
            "seasonality",
            ("event_time",),
            {
                "hour_weights": tuple(
                    float(sum(value.hour == hour for value in timestamps)) for hour in range(24)
                ),
                "weekday_weights": tuple(
                    float(sum(value.weekday() == day for value in timestamps)) for day in range(7)
                ),
            },
        ),
        _summary(
            "customer_activity",
            ("customer_id",),
            {
                "count_quantiles": _distribution(
                    "activity", list(map(float, customer_counts.values()))
                ).quantiles
            },
        ),
        _summary(
            "transaction_count",
            ("customer_id",),
            {
                "total": len(amounts),
                "customer_cardinality": len(customer_counts),
                "count_quantiles": _distribution(
                    "transaction_count", list(map(float, customer_counts.values()))
                ).quantiles,
            },
        ),
    ]
    distributions = [amount_distribution]
    return summaries, distributions


def _optional_summaries(
    frame: pl.DataFrame, timestamps: list[datetime]
) -> tuple[list[StatisticalSummary], list[FittedDistribution]]:
    """Fit summaries whose source columns are optional."""

    summaries: list[StatisticalSummary] = []
    distributions: list[FittedDistribution] = []
    if len(timestamps) > 1:
        ordered_times = sorted(timestamps)
        gaps = [
            (right - left).total_seconds()
            for left, right in zip(ordered_times, ordered_times[1:], strict=False)
        ]
        interarrival = _distribution("inter_arrival_seconds", gaps)
        distributions.append(interarrival)
        summaries.append(
            _summary("inter_arrival", ("event_time",), {"quantiles": interarrival.quantiles})
        )
    else:
        summaries.append(_summary("inter_arrival", ("event_time",), {"quantiles": (0.0,)}))

    if "merchant_category" in frame.columns:
        categories = Counter(
            str(value) for value in frame.get_column("merchant_category").drop_nulls().to_list()
        )
        total = sum(categories.values()) or 1
        summaries.append(
            _summary(
                "merchant_frequency",
                ("merchant_category",),
                {
                    "weights": {
                        key: round(value / total, 12) for key, value in sorted(categories.items())
                    }
                },
            )
        )
    else:
        summaries.append(
            _summary("merchant_frequency", ("merchant_category",), {"unavailable": True})
        )

    if "account_balance" in frame.columns:
        balances = [float(value) for value in frame.get_column("account_balance").to_list()]
        summaries.append(
            _summary(
                "account_balance",
                ("account_balance",),
                {
                    "minimum": min(balances),
                    "maximum": max(balances),
                    "quantiles": _distribution("balance", balances).quantiles,
                },
            )
        )
    else:
        summaries.append(_summary("account_balance", ("account_balance",), {"unavailable": True}))

    summaries.append(_graph_summary(frame))
    summaries.append(_campaign_summary(frame, timestamps))
    return summaries, distributions


def _graph_summary(frame: pl.DataFrame) -> StatisticalSummary:
    if not {"payer_account_id", "payee_account_id"}.issubset(frame.columns):
        return _summary(
            "graph_statistics", ("payer_account_id", "payee_account_id"), {"unavailable": True}
        )
    edges = [
        (str(left), str(right))
        for left, right in zip(frame["payer_account_id"], frame["payee_account_id"], strict=False)
        if left is not None and right is not None
    ]
    degrees = Counter(item for edge in edges for item in edge)
    return _summary(
        "graph_statistics",
        ("payer_account_id", "payee_account_id"),
        {
            "edge_count": len(edges),
            "average_degree": sum(degrees.values()) / len(degrees) if degrees else 0.0,
            "degree_quantiles": _distribution(
                "degree", list(map(float, degrees.values()))
            ).quantiles
            if degrees
            else (0.0,),
            "motif_triangle_count": _triangle_count(edges),
        },
    )


def _campaign_summary(frame: pl.DataFrame, timestamps: list[datetime]) -> StatisticalSummary:
    if "campaign_id" not in frame.columns:
        return _summary("campaign_statistics", ("campaign_id",), {"unavailable": True})
    campaign_rows = [
        (str(campaign), timestamp)
        for campaign, timestamp in zip(
            frame.get_column("campaign_id").to_list(), timestamps, strict=True
        )
        if campaign is not None
    ]
    grouped: dict[str, list[datetime]] = {}
    for campaign, timestamp in campaign_rows:
        grouped.setdefault(campaign, []).append(timestamp)
    durations = [(max(values) - min(values)).total_seconds() for values in grouped.values()]
    intensities = [
        len(values) / max(duration, 1.0)
        for values, duration in zip(grouped.values(), durations, strict=True)
    ]
    return _summary(
        "campaign_statistics",
        ("campaign_id", "event_time"),
        {
            "campaign_count": len(grouped),
            "event_count": len(campaign_rows),
            "duration_seconds_quantiles": _distribution(
                "campaign_duration", durations or [0.0]
            ).quantiles,
            "intensity_quantiles": _distribution(
                "campaign_intensity", intensities or [0.0]
            ).quantiles,
        },
    )


def _dependency_summary(frame: pl.DataFrame) -> tuple[StatisticalSummary, ...]:
    return (
        _summary(
            "feature_dependencies",
            ("amount", "event_time", "merchant_category"),
            {
                "supported": tuple(
                    name
                    for name in ("amount", "event_time", "merchant_category")
                    if name in frame.columns
                )
            },
        ),
    )


def _dependencies(frame: pl.DataFrame) -> tuple[FeatureDependency, ...]:
    if "merchant_category" not in frame.columns:
        return ()
    values = [str(value) for value in frame.get_column("merchant_category").drop_nulls().to_list()]
    counts = Counter(values)
    if not counts:
        return ()
    total = sum(counts.values())
    return (
        FeatureDependency(
            source="merchant_category",
            target="amount",
            conditional_weights={key: value / total for key, value in sorted(counts.items())},
        ),
    )


def fit_calibration_profile(
    reference: ReferenceDataset | Path | str,
    *,
    seed: int = 0,
    configuration: Mapping[str, object] | None = None,
    config: Mapping[str, object] | None = None,
) -> CalibrationProfile:
    """Fit the deterministic built-in aggregate profile."""

    if isinstance(reference, Path | str):
        reference = load_reference_data(reference)
    if seed < 0:
        raise ValueError("calibration seed must be non-negative")
    # Establish the dedicated fit stream even for deterministic built-ins;
    # custom models receive the same seed and must not touch global state.
    create_stream_rng(seed, CALIBRATION_STREAM_ID)
    if configuration is None:
        configuration = config
    if configuration is not None and not isinstance(configuration, Mapping):
        configuration = {
            "model_names": tuple(getattr(configuration, "model_names", ())),
            "summary_names": tuple(getattr(configuration, "summary_names", ())),
        }
    frame = reference.frame
    amounts = [float(value) for value in frame.get_column("amount").to_list()]
    timestamps = [
        cast(datetime, value).astimezone(UTC) for value in frame.get_column("event_time").to_list()
    ]
    customer_counts = Counter(str(value) for value in frame.get_column("customer_id").to_list())
    summaries, distributions = _core_summaries(amounts, timestamps, customer_counts)
    optional_summaries, optional_distributions = _optional_summaries(frame, timestamps)
    summaries.extend(optional_summaries)
    distributions.extend(optional_distributions)
    summaries.extend(_dependency_summary(frame))
    dependencies = _dependencies(frame)
    model_names = tuple(
        cast(list[str] | tuple[str, ...], (configuration or {}).get("model_names", ()))
    )
    selected_names = tuple(
        cast(
            list[str] | tuple[str, ...],
            (configuration or {}).get("summary_names", SUMMARY_NAMES),
        )
    )
    unknown_summaries = set(selected_names) - set(SUMMARY_NAMES)
    if unknown_summaries:
        raise ValueError(f"unsupported calibration summary: {sorted(unknown_summaries)[0]}")
    summaries = [summary for summary in summaries if summary.name in selected_names]
    if "amount_distribution" not in selected_names:
        distributions = [item for item in distributions if item.name != "amount"]
    model_versions = {"builtin": CALIBRATION_VERSION}
    for model_name in model_names:
        model = _CALIBRATION_MODELS.get(model_name)
        if model is None:
            raise ValueError(f"unsupported calibration model: {model_name}")
        if not set(model.supported_fields).issubset(frame.columns):
            raise ValueError(
                f"calibration model {model_name} does not support this reference schema"
            )
        summaries.extend(model.fit(reference, seed))
        model_versions[model_name] = model.version
    config_hash = sha256_json(dict(configuration or {}))
    provenance = CalibrationProvenance(
        source_fingerprint=reference.source_fingerprint,
        source_schema_fingerprint=reference.schema_fingerprint,
        row_count=frame.height,
        seed=seed,
        configuration_hash=config_hash,
        model_versions=model_versions,
    )
    payload = {
        "version": CALIBRATION_VERSION,
        "provenance": provenance.model_dump(mode="json"),
        "summaries": [item.model_dump(mode="json") for item in summaries],
        "distributions": [item.model_dump(mode="json") for item in distributions],
        "dependencies": [item.model_dump(mode="json") for item in dependencies],
    }
    return CalibrationProfile(
        profile_id=f"CAL-{sha256_json(payload)[:20]}",
        provenance=provenance,
        summaries=tuple(summaries),
        distributions=tuple(distributions),
        dependencies=tuple(dependencies),
    )


def write_calibration_profile(profile: CalibrationProfile, path: Path | str) -> Path:
    """Write one immutable inspectable YAML profile and its JSON manifest."""

    path = Path(path)
    if path.suffix.lower() not in {".yaml", ".yml"}:
        raise ValueError("calibration profiles must use YAML output")
    manifest = path.with_suffix(".manifest.json")
    if path.exists() or manifest.exists():
        raise FileExistsError(f"calibration profile artifacts already exist: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(profile.model_dump(mode="json"), sort_keys=True), encoding="utf-8"
    )
    manifest.write_text(
        json.dumps(
            {
                "profile_id": profile.profile_id,
                "profile_version": profile.profile_version,
                "profile_fingerprint": sha256_json(profile.model_dump(mode="json")),
                "source_fingerprint": profile.provenance.source_fingerprint,
                "source_schema_fingerprint": profile.provenance.source_schema_fingerprint,
                "seed": profile.provenance.seed,
                "stream_ids": list(profile.provenance.stream_ids),
            },
            sort_keys=True,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return path


def load_calibration_profile(path: Path | str) -> CalibrationProfile:
    path = Path(path)
    if not path.is_file() or path.suffix.lower() not in {".yaml", ".yml"}:
        raise ValueError(f"calibration profile does not exist or is not YAML: {path}")
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("calibration profile root must be a mapping")
    return CalibrationProfile.model_validate(raw)


_CALIBRATION_MODELS: dict[str, CalibrationModel] = {}
_CALIBRATION_METRICS: dict[str, CalibrationMetric] = {}


def register_calibration_model(name: str, model: CalibrationModel) -> None:
    if not name.strip() or name in _CALIBRATION_MODELS:
        raise ValueError("calibration model name must be non-empty and unique")
    if not model.deterministic or not model.version or not model.supported_fields:
        raise ValueError(
            "calibration models must declare version, fields, and deterministic behavior"
        )
    _CALIBRATION_MODELS[name] = model


def register_calibration_metric(name: str, metric: CalibrationMetric) -> None:
    if not name.strip() or name in _CALIBRATION_METRICS:
        raise ValueError("calibration metric name must be non-empty and unique")
    if not metric.deterministic or not metric.version:
        raise ValueError("calibration metrics must declare version and deterministic behavior")
    _CALIBRATION_METRICS[name] = metric


def resolve_calibration(
    config: Any, profile: CalibrationProfile | None = None
) -> ResolvedCalibration:
    """Resolve an optional profile against a simulation configuration."""

    settings = getattr(config, "calibration", None)
    enabled = bool(getattr(settings, "enabled", False)) if settings is not None else False
    if not enabled and profile is None:
        return ResolvedCalibration(enabled=False)
    if profile is None:
        profile_path = getattr(settings, "profile", None)
        if profile_path is None:
            raise ValueError("calibration.enabled requires calibration.profile")
        profile = load_calibration_profile(Path(profile_path))
    if profile.profile_version != CALIBRATION_VERSION:
        raise ValueError(f"unsupported calibration profile version: {profile.profile_version}")
    configured_path = getattr(settings, "profile", None)
    if configured_path is not None and profile is not None:
        configured_profile = load_calibration_profile(Path(configured_path))
        if configured_profile.profile_id != profile.profile_id:
            raise ValueError("supplied calibration profile does not match configuration.profile")
    model_names = tuple(getattr(settings, "model_names", ("builtin",)))
    unsupported = [
        name for name in model_names if name != "builtin" and name not in _CALIBRATION_MODELS
    ]
    if unsupported:
        raise ValueError(f"unsupported calibration model: {unsupported[0]}")
    behavior = getattr(config, "behavior", None)
    amount_summary = next(
        (item for item in profile.summaries if item.name == "amount_distribution"), None
    )
    if behavior is not None and amount_summary is not None:
        quantiles = amount_summary.parameters.get("quantiles", ())
        if quantiles and (
            max(quantiles) < behavior.amount_min or min(quantiles) > behavior.amount_max
        ):
            raise ValueError("calibration amount distribution is incompatible with amount bounds")
    seasonality = next((item for item in profile.summaries if item.name == "seasonality"), None)
    if behavior is not None and seasonality is not None:
        hours = seasonality.parameters.get("hour_weights", ())
        if hours and not any(
            hour < len(hours) and float(hours[hour]) > 0 for hour in behavior.active_hours
        ):
            raise ValueError("calibration seasonality has no mass in configured active_hours")
    config_payload: Any = getattr(config, "model_dump", lambda **_: {})(mode="json")
    if isinstance(config_payload, dict):
        # Kafka delivery controls are sink concerns and must not perturb the
        # calibrated source-run identity.
        config_payload.pop("kafka", None)
        config_payload.pop("lakehouse", None)
        outputs_payload = config_payload.get("outputs")
        if isinstance(outputs_payload, dict):
            outputs_payload["kafka"] = False
            outputs_payload.pop("iceberg", None)
        calibration_payload = config_payload.get("calibration")
        if isinstance(calibration_payload, dict):
            # A source path is an access detail, not a simulation parameter;
            # profile identity and the validated calibration options are what
            # determine reproducibility.
            calibration_payload = dict(calibration_payload)
            calibration_payload.pop("profile", None)
            calibration_payload["enabled"] = True
            config_payload = dict(config_payload)
            config_payload["calibration"] = calibration_payload
    effective = sha256_json({"config": config_payload, "profile_id": profile.profile_id})
    return ResolvedCalibration(
        enabled=True,
        profile_id=profile.profile_id,
        profile=profile,
        effective_configuration_hash=effective,
        stream_ids=(CALIBRATED_STREAM_ID,),
    )


def apply_calibration_profile(
    profile: CalibrationProfile,
    *,
    seed: int = 0,
    config: Any | None = None,
) -> ResolvedCalibration:
    """Return an immutable generation context for a fitted profile."""

    if seed < 0:
        raise ValueError("calibration seed must be non-negative")
    if config is not None:
        return resolve_calibration(config, profile)
    create_stream_rng(seed, CALIBRATED_STREAM_ID)
    return ResolvedCalibration(
        enabled=True,
        profile_id=profile.profile_id,
        profile=profile,
        effective_configuration_hash=sha256_json({"profile_id": profile.profile_id, "seed": seed}),
        stream_ids=(CALIBRATED_STREAM_ID,),
    )


def resolve_calibration_configuration(config: Any) -> ResolvedCalibration:
    """Explicitly named public alias for resolving calibration configuration."""

    return resolve_calibration(config)


def compute_fidelity_report(
    profile: CalibrationProfile,
    generated: Mapping[str, object],
    *,
    weights: Mapping[Any, float] | None = None,
    minimum_scores: Mapping[Any, float] | None = None,
) -> FidelityReport:
    metrics: list[FidelityMetric] = []
    weights = weights or {}
    minimum_scores = minimum_scores or {}
    for summary in profile.summaries:
        value = generated.get(summary.name)
        score: float | None
        if value is None:
            score = None
        elif isinstance(value, bool):
            score = 1.0 if value else 0.0
        elif isinstance(value, int | float) and math.isfinite(float(value)):
            score = min(1.0, max(0.0, float(value)))
        elif isinstance(value, Mapping) and isinstance(value.get("score"), int | float):
            score = min(1.0, max(0.0, float(cast(float, value["score"]))))
        else:
            score = 1.0
        threshold = minimum_scores.get(summary.name)
        metrics.append(
            FidelityMetric(
                name=summary.name,
                status=(
                    "unavailable"
                    if score is None
                    else "below_threshold"
                    if threshold is not None and score < threshold
                    else "available"
                ),
                score=score,
                weight=float(weights.get(summary.name, 1.0)),
                reference_summary_fingerprint=summary.fingerprint,
                details=({"minimum_score": threshold} if threshold is not None else {}),
            )
        )
    available = [item for item in metrics if item.score is not None]
    composite = (
        sum(cast(float, item.score) * item.weight for item in available)
        / sum(item.weight for item in available)
        if available
        else None
    )
    return FidelityReport(
        profile_id=profile.profile_id, metrics=tuple(metrics), composite_score=composite
    )


def validate_calibration_output(
    profile: CalibrationProfile, generated: Mapping[str, object]
) -> None:
    """Reject output that attempts to carry reference rows or invalid metrics."""

    del profile
    for key, value in generated.items():
        if isinstance(value, pl.DataFrame | pl.LazyFrame):
            raise ValueError(f"calibration output {key} must be aggregate-only")
        if isinstance(value, list | tuple) and len(value) > 10_000:
            raise ValueError("calibration output contains an unbounded row-like collection")


__all__ = [
    "CALIBRATED_AMOUNT_STREAM_ID",
    "CALIBRATED_BALANCE_STREAM_ID",
    "CALIBRATED_MERCHANT_STREAM_ID",
    "CALIBRATED_STREAM_ID",
    "CALIBRATED_TIMING_STREAM_ID",
    "CALIBRATION_STREAM_ID",
    "CalibrationMetric",
    "CalibrationModel",
    "CalibrationProfile",
    "CalibrationProvenance",
    "FeatureDependency",
    "FidelityMetric",
    "FidelityReport",
    "FittedDistribution",
    "ReferenceDataset",
    "ResolvedCalibration",
    "StatisticalSummary",
    "apply_calibration_profile",
    "compute_fidelity_report",
    "fit_calibration_profile",
    "load_calibration_profile",
    "load_reference_data",
    "register_calibration_metric",
    "register_calibration_model",
    "resolve_calibration",
    "resolve_calibration_configuration",
    "validate_calibration_output",
    "write_calibration_profile",
]
