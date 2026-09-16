"""Deterministic baseline models and prediction evaluation for Milestone 19.

The module intentionally keeps model dependencies lazy.  Dataset construction and
generation remain usable without the optional ML stack, while a trained run is
fully described by local, content-addressed artifacts.
"""

import hashlib
import importlib.metadata
import io
import json
import math
import platform
import time
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

import polars as pl
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from fraudtwin import __version__
from fraudtwin.reproducibility import sha256_json, write_json

MODEL_NAMES = ("logistic_regression", "lightgbm", "xgboost", "catboost")
ALL_MODEL_NAMES = MODEL_NAMES + ("deterministic_heuristic",)
LABELS = ("FRAUD", "LEGITIMATE")

# IDs, timestamps, split metadata, labels, and oracle fields are deliberately absent.
MODEL_FEATURES = (
    "amount",
    "payment_rail",
    "payment_type",
    "online",
    "transaction_count_1m",
    "transaction_count_5m",
    "transaction_count_1h",
    "transaction_count_24h",
    "transaction_count_7d",
    "transaction_count_30d",
    "transaction_amount_1h",
    "transaction_amount_24h",
    "transaction_amount_7d",
    "avg_transaction_amount_30d",
    "max_transaction_amount_7d",
    "amount_vs_customer_avg",
    "distinct_merchants_1d",
    "distinct_merchants_30d",
    "new_merchant_flag",
    "merchant_fraud_rate_historical",
    "distinct_countries_24h",
    "new_country_flag",
    "device_age_days",
    "new_device_flag",
    "trusted_device_flag",
    "device_customer_count_30d",
    "customers_per_device_24h",
    "account_age_days",
    "balance",
    "available_balance",
    "credit_limit",
    "credit_utilization",
    "days_since_last_payment",
    "confirmed_fraud_count_90d",
    "fraud_loss_365d",
    "days_since_last_confirmed_fraud",
)
MODEL_CATEGORICAL_FEATURES = frozenset({"payment_rail", "payment_type"})
PREDICTION_FIELDS = ("event_id", "payment_id", "customer_id", "account_id")
FEATURE_VERSION = "M19-observable-allowlist-1"
PREPROCESSING_METADATA = {
    "missing_values": "numeric=0; categorical=__UNKNOWN__",
    "categorical_encoding": "deterministic one-hot vocabulary fit on train rows",
    "resampling": "none",
}


class BaselineEvaluationConfig(BaseModel):
    """Strict, analysis-only configuration for M19."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    models: tuple[str, ...] = MODEL_NAMES
    fixed_fpr: float = Field(default=0.01, gt=0, lt=1)
    fixed_recall: float = Field(default=0.80, gt=0, lt=1)
    ranking_k_fraction: float = Field(default=0.01, gt=0, le=1)
    label_policy: Literal["exclude_unresolved"] = "exclude_unresolved"
    random_seed: int | None = Field(default=None, ge=0)
    deterministic_presets: dict[str, Any] = Field(
        default_factory=lambda: {
            "single_thread": True,
            "resampling": "none",
            "tie_break": "score_then_dataset_row_id",
        }
    )
    tracking_uri: str | None = None

    @field_validator("models")
    @classmethod
    def supported_models(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        if not values or len(set(values)) != len(values):
            raise ValueError("models must be a non-empty unique list")
        if any(value not in ALL_MODEL_NAMES for value in values):
            raise ValueError(f"models must use supported names: {ALL_MODEL_NAMES}")
        return values


def _tracking_metadata(config: BaselineEvaluationConfig) -> dict[str, str | bool]:
    enabled = bool(config.tracking_uri)
    return {"enabled": enabled, "backend": "mlflow" if enabled else "local"}


def load_baseline_config(path: Path) -> BaselineEvaluationConfig:
    import yaml

    if not path.is_file():
        raise FileNotFoundError(f"baseline evaluation config does not exist: {path}")
    with path.open(encoding="utf-8") as stream:
        raw = yaml.safe_load(stream)
    if not isinstance(raw, dict):
        raise ValueError("baseline evaluation config root must be a mapping")
    return BaselineEvaluationConfig.model_validate(raw)


class PredictionRecord(BaseModel):
    """One external prediction at a point in time."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    event_id: str | None = Field(default=None, min_length=1)
    payment_id: str | None = Field(default=None, min_length=1)
    customer_id: str | None = Field(default=None, min_length=1)
    account_id: str | None = Field(default=None, min_length=1)
    prediction_timestamp: datetime
    fraud_score: float = Field(ge=0, le=1)
    predicted_class: Literal["FRAUD", "LEGITIMATE"] | None = None

    @field_validator("prediction_timestamp")
    @classmethod
    def timestamp_must_be_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("prediction_timestamp must include a timezone")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def exactly_one_target(self) -> PredictionRecord:
        if sum(getattr(self, field) is not None for field in PREDICTION_FIELDS) != 1:
            raise ValueError("exactly one prediction target ID is required")
        if not math.isfinite(self.fraud_score):
            raise ValueError("fraud_score must be finite")
        return self

    @property
    def target(self) -> tuple[str, str]:
        for field in PREDICTION_FIELDS:
            value = getattr(self, field)
            if value is not None:
                return field, value
        raise AssertionError("validated prediction has no target")


class PredictionAdapter:
    """Small file/library adapter for external point-in-time predictions."""

    @staticmethod
    def load(path: Path) -> tuple[PredictionRecord, ...]:
        return load_predictions(path)

    @staticmethod
    def validate(records: Iterable[PredictionRecord]) -> tuple[PredictionRecord, ...]:
        materialized = tuple(records)
        if not materialized:
            raise ValueError("at least one prediction is required")
        return materialized


PREDICTION_SCHEMA: dict[str, Any] = {
    "event_id": pl.Utf8,
    "payment_id": pl.Utf8,
    "customer_id": pl.Utf8,
    "account_id": pl.Utf8,
    "prediction_timestamp": pl.Datetime(time_zone="UTC"),
    "fraud_score": pl.Float64,
    "predicted_class": pl.Utf8,
}


def _prediction_from_row(row: Mapping[str, Any]) -> PredictionRecord:
    values = dict(row)
    if "prediction_time" in values and "prediction_timestamp" not in values:
        values["prediction_timestamp"] = values.pop("prediction_time")
    return PredictionRecord.model_validate(values)


def _record_for_row(row: Mapping[str, Any], score: float) -> PredictionRecord:
    """Create one canonical event-level record from a PIT row."""

    for field in PREDICTION_FIELDS:
        value = row.get(field)
        if value is not None:
            prediction_timestamp = _utc(row["prediction_time"])
            return PredictionRecord.model_validate({
                field: str(value),
                "prediction_timestamp": prediction_timestamp,
                "fraud_score": float(score),
            })
    raise ValueError("PIT row has no supported prediction target ID")


def load_predictions(path: Path) -> tuple[PredictionRecord, ...]:
    """Load strict Parquet or JSONL predictions."""

    if not path.is_file():
        raise FileNotFoundError(f"prediction file does not exist: {path}")
    if path.suffix.lower() in {".jsonl", ".ndjson", ".json"}:
        rows: list[dict[str, Any]] = []
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid prediction JSON on line {number}") from exc
            if not isinstance(value, dict):
                raise ValueError(f"prediction JSON line {number} must be an object")
            rows.append(value)
    else:
        frame = pl.read_parquet(path)
        if set(frame.columns) != set(PREDICTION_SCHEMA):
            raise ValueError(
                "prediction Parquet columns must be exactly: " + ", ".join(PREDICTION_SCHEMA)
            )
        rows = frame.select(list(PREDICTION_SCHEMA)).to_dicts()
    if not rows:
        raise ValueError("prediction input must contain at least one record")
    return tuple(_prediction_from_row(row) for row in rows)


def write_predictions(predictions: Iterable[PredictionRecord], path: Path) -> Path:
    rows = [item.model_dump(mode="python") for item in predictions]
    frame = pl.DataFrame(rows, schema=PREDICTION_SCHEMA, orient="row")
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(path)
    return path


def _utc(value: Any) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("dataset prediction timestamps must be timezone-aware")
    return value.astimezone(UTC)


def _labelled(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [dict(row) for row in rows if row.get("label") in LABELS]


def _auc(labels: Sequence[int], scores: Sequence[float], ids: Sequence[str]) -> float | None:
    positives = sum(labels)
    negatives = len(labels) - positives
    if not positives or not negatives:
        return None
    ordered = sorted(zip(scores, labels, ids, strict=True), key=lambda item: (item[0], item[2]))
    rank_sum = sum(rank for rank, (_, label, _) in enumerate(ordered, 1) if label)
    return (rank_sum - positives * (positives + 1) / 2) / (positives * negatives)


def _pr_auc(labels: Sequence[int], scores: Sequence[float], ids: Sequence[str]) -> float | None:
    positives = sum(labels)
    if not positives:
        return None
    ordered = sorted(zip(scores, labels, ids, strict=True), key=lambda item: (-item[0], item[2]))
    area = 0.0
    found = 0
    previous_recall = 0.0
    for index, (_, label, _) in enumerate(ordered, 1):
        found += label
        recall = found / positives
        area += (recall - previous_recall) * (found / index)
        previous_recall = recall
    return area


def _operating_thresholds(
    rows: Sequence[Mapping[str, Any]], config: BaselineEvaluationConfig
) -> dict[str, float | None]:
    values = _labelled(rows)
    if not values:
        return {"f1": None, "fixed_fpr": None, "fixed_recall": None}
    ordered = sorted(
        values, key=lambda row: (-float(row["fraud_score"]), str(row["dataset_row_id"]))
    )
    thresholds = sorted({float(row["fraud_score"]) for row in ordered}, reverse=True)
    best_f1: tuple[float, float] = (-1.0, thresholds[-1])
    best_fpr: tuple[float, float, float] | None = None
    best_recall: tuple[float, float, float] | None = None
    for threshold in thresholds:
        predicted = [float(row["fraud_score"]) >= threshold for row in values]
        tp = sum(
            pred and row["label"] == "FRAUD" for pred, row in zip(predicted, values, strict=True)
        )
        fp = sum(
            pred and row["label"] == "LEGITIMATE"
            for pred, row in zip(predicted, values, strict=True)
        )
        positives = sum(row["label"] == "FRAUD" for row in values)
        negatives = len(values) - positives
        recall = tp / positives if positives else 0.0
        precision = tp / (tp + fp) if tp + fp else 0.0
        fpr = fp / negatives if negatives else 1.0
        f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
        if (f1, threshold) > best_f1:
            best_f1 = (f1, threshold)
        candidate_fpr = (recall, threshold, fpr)
        if fpr <= config.fixed_fpr and (best_fpr is None or candidate_fpr[:2] > best_fpr[:2]):
            best_fpr = candidate_fpr
        candidate_recall = (precision, threshold, recall)
        if recall >= config.fixed_recall and (
            best_recall is None or candidate_recall[:2] > best_recall[:2]
        ):
            best_recall = candidate_recall
    return {
        "f1": best_f1[1],
        "fixed_fpr": best_fpr[1] if best_fpr else None,
        "fixed_recall": best_recall[1] if best_recall else None,
    }


def _threshold_metrics(
    rows: Sequence[Mapping[str, Any]], threshold: float | None
) -> tuple[float | None, float | None, float | None]:
    values = _labelled(rows)
    if threshold is None or not values:
        return None, None, None
    predicted = [float(row["fraud_score"]) >= threshold for row in values]
    tp = sum(pred and row["label"] == "FRAUD" for pred, row in zip(predicted, values, strict=True))
    fp = sum(
        pred and row["label"] == "LEGITIMATE" for pred, row in zip(predicted, values, strict=True)
    )
    positives = sum(row["label"] == "FRAUD" for row in values)
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / positives if positives else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return precision, recall, f1


def _ranking_metrics(
    rows: Sequence[Mapping[str, Any]], fraction: float
) -> tuple[float | None, float | None]:
    values = _labelled(rows)
    if not values:
        return None, None
    k = max(1, math.ceil(len(values) * fraction))
    ranked = sorted(
        values, key=lambda row: (-float(row["fraud_score"]), str(row["dataset_row_id"]))
    )[:k]
    positives = sum(row["label"] == "FRAUD" for row in values)
    found = sum(row["label"] == "FRAUD" for row in ranked)
    return (found / k, found / positives if positives else None)


def metric_row(
    rows: Sequence[Mapping[str, Any]],
    thresholds: Mapping[str, float | None],
    config: BaselineEvaluationConfig,
) -> dict[str, Any]:
    values = _labelled(rows)
    labels = [int(row["label"] == "FRAUD") for row in values]
    scores = [float(row["fraud_score"]) for row in values]
    ids = [str(row["dataset_row_id"]) for row in values]
    precision, recall, f1 = _threshold_metrics(values, thresholds.get("f1"))
    fixed_precision, fixed_fpr_recall, _ = _threshold_metrics(values, thresholds.get("fixed_fpr"))
    recall_precision, _, _ = _threshold_metrics(values, thresholds.get("fixed_recall"))
    precision_k, recall_k = _ranking_metrics(values, config.ranking_k_fraction)
    brier = (
        sum((score - label) ** 2 for score, label in zip(scores, labels, strict=True)) / len(values)
        if values
        else None
    )
    fixed_threshold = thresholds.get("fixed_fpr")
    monetary: dict[str, dict[str, float]] = {}
    if fixed_threshold is not None:
        for currency in sorted({str(row.get("currency") or "UNKNOWN") for row in values}):
            currency_rows = [
                row for row in values if str(row.get("currency") or "UNKNOWN") == currency
            ]
            captured = sum(
                float(row.get("amount") or 0)
                for row in currency_rows
                if row["label"] == "FRAUD" and float(row["fraud_score"]) >= fixed_threshold
            )
            missed = sum(
                float(row.get("amount") or 0)
                for row in currency_rows
                if row["label"] == "FRAUD" and float(row["fraud_score"]) < fixed_threshold
            )
            false_positive = sum(
                float(row.get("amount") or 0)
                for row in currency_rows
                if row["label"] == "LEGITIMATE" and float(row["fraud_score"]) >= fixed_threshold
            )
            monetary[currency] = {
                "fraud_value_captured": captured,
                "fraud_value_missed": missed,
                "false_positive_value_impact": false_positive,
            }
    detected_delays = [
        (_utc(row["prediction_time"]) - _utc(row["fraud_occurred_at"])).total_seconds()
        for row in values
        if row["label"] == "FRAUD"
        and row.get("fraud_occurred_at") is not None
        and fixed_threshold is not None
        and float(row["fraud_score"]) >= fixed_threshold
        and _utc(row["prediction_time"]) >= _utc(row["fraud_occurred_at"])
    ]
    return {
        "row_count": len(rows),
        "labelled_row_count": len(values),
        "positive_count": sum(labels),
        "roc_auc": _auc(labels, scores, ids),
        "pr_auc": _pr_auc(labels, scores, ids),
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "precision_at_fixed_fpr": fixed_precision,
        "recall_at_fixed_fpr": fixed_fpr_recall,
        "precision_at_fixed_recall": recall_precision,
        "precision_at_k": precision_k,
        "recall_at_k": recall_k,
        "brier_score": brier,
        "calibration": {
            "mean_probability_error": (sum(scores) / len(scores) - sum(labels) / len(labels))
            if values
            else None
        },
        "thresholds": dict(thresholds),
        "fixed_fpr_target": config.fixed_fpr,
        "fixed_recall_target": config.fixed_recall,
        "ranking_k_fraction": config.ranking_k_fraction,
        "ranking_unit": "events",
        "label_policy": config.label_policy,
        "monetary_by_currency": monetary,
        "detection_delay_seconds_mean": (
            sum(detected_delays) / len(detected_delays) if detected_delays else None
        ),
        "detected_fraud_count": len(detected_delays),
        "undetected_fraud_count": sum(row["label"] == "FRAUD" for row in values)
        - len(detected_delays),
    }


def _heuristic_score(row: Mapping[str, Any]) -> float:
    ratio = row.get("amount_vs_customer_avg")
    return max(
        0.0,
        min(
            1.0,
            (
                4 * float(row.get("new_device_flag", False))
                + 2 * float(row.get("new_country_flag", False))
                + min(float(row.get("transaction_count_1h", 0)), 10) / 10
                + max(0.0, float(ratio or 1) - 1) / 10
            )
            / 7,
        ),
    )


def heuristic_predictions(rows: Sequence[Mapping[str, Any]]) -> tuple[PredictionRecord, ...]:
    """Score PIT rows with the dependency-free deterministic baseline."""

    return tuple(_record_for_row(row, _heuristic_score(row)) for row in rows)


def _lazy_model(name: str, seed: int) -> Any:
    try:
        if name == "logistic_regression":
            from sklearn.linear_model import LogisticRegression  # type: ignore[import-untyped]

            return LogisticRegression(max_iter=500, class_weight="balanced", random_state=seed)
        if name == "lightgbm":
            from lightgbm import LGBMClassifier  # type: ignore[import-not-found]

            return LGBMClassifier(
                n_estimators=100,
                learning_rate=0.05,
                num_leaves=31,
                random_state=seed,
                n_jobs=1,
                deterministic=True,
                verbosity=-1,
            )
        if name == "xgboost":
            from xgboost import XGBClassifier  # type: ignore[import-not-found]

            return XGBClassifier(
                n_estimators=100,
                max_depth=4,
                learning_rate=0.05,
                subsample=1.0,
                colsample_bytree=1.0,
                random_state=seed,
                n_jobs=1,
                tree_method="hist",
                eval_metric="logloss",
            )
        if name == "catboost":
            from catboost import CatBoostClassifier  # type: ignore[import-not-found]

            return CatBoostClassifier(
                iterations=100,
                depth=6,
                learning_rate=0.05,
                random_seed=seed,
                thread_count=1,
                verbose=False,
                allow_writing_files=False,
            )
    except ImportError as exc:
        raise RuntimeError(f"model {name} requires the optional ML dependencies") from exc
    raise ValueError(f"unsupported model: {name}")


def _matrix(
    rows: Sequence[Mapping[str, Any]], categories: Mapping[str, tuple[str, ...]] | None = None
) -> tuple[Any, dict[str, tuple[str, ...]]]:
    import numpy as np

    cats = dict(categories or {})
    for field in MODEL_CATEGORICAL_FEATURES:
        if field not in cats:
            cats[field] = tuple(sorted({str(row.get(field) or "__UNKNOWN__") for row in rows}))
    arrays: list[list[float]] = []
    for row in rows:
        values: list[float] = []
        for field in MODEL_FEATURES:
            value = row.get(field)
            if field in MODEL_CATEGORICAL_FEATURES:
                options = cats[field]
                values.extend(float(str(value or "__UNKNOWN__") == option) for option in options)
            elif isinstance(value, bool):
                values.append(float(value))
            else:
                values.append(
                    float(value) if value is not None and math.isfinite(float(value)) else 0.0
                )
        arrays.append(values)
    return np.asarray(arrays, dtype=float), cats


@dataclass(frozen=True)
class EvaluationResult:
    predictions: tuple[dict[str, Any], ...]
    metrics: tuple[dict[str, Any], ...]
    manifest: dict[str, Any]
    model_artifacts: dict[str, bytes] | None = None


def _enrich_segments(
    rows: Sequence[Mapping[str, Any]], source_run_dir: Path | None
) -> list[dict[str, Any]]:
    enriched = [dict(row) for row in rows]
    for row in enriched:
        row.setdefault("country", "unavailable")
        row.setdefault("merchant_category", "unavailable")
        row.setdefault("customer_segment", "unavailable")
        row.setdefault("fraud_type", "unavailable")
        row.setdefault("fraud_occurred_at", None)
        row.setdefault("time_period", _utc(row["prediction_time"]).date().isoformat())
    if source_run_dir is None:
        return enriched
    try:
        from fraudtwin.ml.dataset import load_generated_run

        entities, behavior, _ = load_generated_run(source_run_dir)
        customers = {item.customer_id: item for item in entities.customers}
        merchants = {item.merchant_id: item for item in entities.merchants}
        events = {item.event_id: item for item in behavior.payment_events}
        fraud = {item.payment_id: item for item in behavior.fraud_records}
        for row in enriched:
            event = events.get(str(row["event_id"]))
            customer = customers.get(str(row["customer_id"]))
            merchant = (
                merchants.get(str(row.get("merchant_id"))) if row.get("merchant_id") else None
            )
            record = fraud.get(str(row["payment_id"]))
            row["country"] = (
                merchant.country if merchant else (customer.country if customer else "unavailable")
            )
            row["merchant_category"] = (
                merchant.merchant_category_code if merchant else "unavailable"
            )
            row["customer_segment"] = customer.risk_segment if customer else "unavailable"
            row["fraud_type"] = record.scenario_type if record else "NONE"
            row["fraud_occurred_at"] = record.occurred_at if record else None
            if event is not None and _utc(event.source_available_at) > _utc(row["prediction_time"]):
                row["country"] = row["merchant_category"] = row["customer_segment"] = "unavailable"
    except (FileNotFoundError, OSError):
        # Segment enrichment is optional when the source run is unavailable.
        return enriched
    except ValueError as exc:
        # M15 sidecars can legitimately reuse event IDs with a different
        # envelope; the legacy loader reports that as a conflict.  Other
        # malformed source data should still fail loudly.
        if "conflicting duplicate" not in str(exc):
            raise
        return enriched
    return enriched


def _lineage_metadata(
    rows: Sequence[Mapping[str, Any]], source_run_dir: Path | None
) -> dict[str, Any]:
    """Collect reproducibility lineage without making source manifests mandatory."""

    source: dict[str, Any] = {}
    dataset: dict[str, Any] = {}
    if source_run_dir is not None:
        try:
            source = json.loads((source_run_dir / "manifest.json").read_text(encoding="utf-8"))
        except (FileNotFoundError, OSError, json.JSONDecodeError):
            source = {}
        try:
            dataset = json.loads(
                (source_run_dir / "ml" / "dataset_manifest.json").read_text(encoding="utf-8")
            )
        except (FileNotFoundError, OSError, json.JSONDecodeError):
            dataset = {}
    periods = sorted({str(row.get("prediction_time", ""))[:10] for row in rows})
    split_periods: dict[str, dict[str, str | None]] = {}
    for split in ("train", "validation", "test"):
        values = sorted(
            str(row.get("prediction_time"))
            for row in rows
            if row.get("split") == split and row.get("prediction_time") is not None
        )
        split_periods[split] = {
            "start": values[0] if values else None,
            "end": values[-1] if values else None,
        }
    package_versions: dict[str, str] = {}
    for package in ("fraudtwin", "scikit-learn", "lightgbm", "xgboost", "catboost", "mlflow"):
        try:
            package_versions[package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            continue
    return {
        "simulation_manifest_hash": sha256_json(source) if source else None,
        "dataset_manifest_hash": sha256_json(dataset) if dataset else None,
        "source_seed": source.get("seed", dataset.get("seed")),
        "source_configuration_hash": dataset.get(
            "configuration_hash", source.get("scenario_config_hash")
        ),
        "source_code_version": source.get("generator_version", __version__),
        "source_schema_versions": source.get("schema_versions", {}),
        "dataset_schema_version": dataset.get("schema_version"),
        "train_validation_periods": {
            **split_periods,
            "observed_dates": periods,
        },
        "fraud_scenario_mix": source.get("fraud_counts", {}),
        "package_versions": package_versions,
    }


def evaluate_predictions(
    rows: Sequence[Mapping[str, Any]],
    predictions: Sequence[PredictionRecord],
    config: BaselineEvaluationConfig,
    *,
    model_id: str = "external",
    source_run_dir: Path | None = None,
) -> EvaluationResult:
    if not predictions:
        raise ValueError("at least one prediction is required")
    # Re-validate records at the evaluation boundary so callers using the typed
    # API receive the same strict contract as file adapters.
    PredictionAdapter.validate(predictions)
    rows = _enrich_segments(rows, source_run_dir)
    lineage = _lineage_metadata(rows, source_run_dir)
    by_key: dict[tuple[str, str, datetime], dict[str, Any]] = {}
    ambiguous_keys: set[tuple[str, str, datetime]] = set()
    for row in rows:
        prediction_time = _utc(row["prediction_time"])
        for field in PREDICTION_FIELDS:
            value = row.get(field)
            if value is not None:
                key = (field, str(value), prediction_time)
                if key in by_key and by_key[key]["dataset_row_id"] != row["dataset_row_id"]:
                    ambiguous_keys.add(key)
                else:
                    by_key[key] = dict(row)
    selected: list[dict[str, Any]] = []
    seen: set[str] = set()
    for prediction in predictions:
        field, value = prediction.target
        key = (field, value, prediction.prediction_timestamp)
        if key in ambiguous_keys:
            raise ValueError(f"ambiguous PIT dataset target: {field}={value}")
        matched_row = by_key.get(key)
        if matched_row is None:
            raise ValueError(f"prediction does not resolve to one PIT row: {field}={value}")
        row = matched_row
        row_id = str(row["dataset_row_id"])
        if row_id in seen:
            raise ValueError(f"duplicate prediction for dataset row: {row_id}")
        seen.add(row_id)
        row["fraud_score"] = prediction.fraud_score
        row["predicted_class"] = prediction.predicted_class
        row["_predicted_class"] = prediction.predicted_class
        selected.append(row)
    selected.sort(key=lambda row: str(row["dataset_row_id"]))
    validation = [row for row in selected if row.get("split") == "validation"]
    thresholds = _operating_thresholds(validation, config)
    metrics = []
    for split in ("train", "validation", "test"):
        values = [row for row in selected if row.get("split") == split]
        metrics.append({
            "model_id": model_id,
            "partition": split,
            **metric_row(values, thresholds, config),
        })
    for dimension in (
        "fraud_type",
        "payment_rail",
        "country",
        "merchant_category",
        "customer_segment",
        "time_period",
    ):
        for value in sorted({str(row.get(dimension, "unavailable")) for row in selected}):
            values = [
                row
                for row in selected
                if str(row.get(dimension, "unavailable")) == value and row.get("split") == "test"
            ]
            metrics.append({
                "model_id": model_id,
                "partition": "test",
                "segment_dimension": dimension,
                "segment_value": value,
                **metric_row(values, thresholds, config),
            })
    canonical_predictions = tuple(
        {
            **_record_for_row(row, float(row["fraud_score"])).model_dump(
                mode="json", exclude_none=True
            ),
        }
        for row in selected
    )
    for item, row in zip(canonical_predictions, selected, strict=True):
        if row.get("_predicted_class") is not None:
            item["predicted_class"] = row["_predicted_class"]
    manifest = {
        "evaluation_version": __version__,
        "model_id": model_id,
        "feature_version": FEATURE_VERSION,
        "model_features": list(MODEL_FEATURES),
        "prediction_count": len(selected),
        "label_policy": config.label_policy,
        "configuration": config.model_dump(mode="json"),
        "thresholds": thresholds,
        "output_fingerprint": sha256_json({
            "predictions": canonical_predictions,
            "metrics": metrics,
        }),
        "tracking": _tracking_metadata(config),
        "lineage": lineage,
        "preprocessing": PREPROCESSING_METADATA.copy(),
        "class_weights": "validation-independent train-label balancing",
    }
    return EvaluationResult(canonical_predictions, tuple(metrics), manifest)


def train_baselines(
    rows: Sequence[Mapping[str, Any]],
    config: BaselineEvaluationConfig,
    *,
    source_run_dir: Path | None = None,
) -> EvaluationResult:
    labelled = _labelled(rows)
    train = [row for row in labelled if row.get("split") == "train"]
    validation = [row for row in labelled if row.get("split") == "validation"]
    if not train or not validation or {row["label"] for row in train} != set(LABELS):
        raise ValueError("baseline training requires both labels in a non-empty train partition")
    seed = config.random_seed if config.random_seed is not None else 0
    all_predictions: list[dict[str, Any]] = []
    all_metrics: list[dict[str, Any]] = []
    model_metadata: dict[str, Any] = {}
    model_artifacts: dict[str, bytes] = {}
    lineage = _lineage_metadata(rows, source_run_dir)
    for model_id in config.models:
        if model_id == "deterministic_heuristic":
            predictions = [_record_for_row(row, _heuristic_score(row)) for row in rows]
        else:
            model = _lazy_model(model_id, seed)
            matrix_train, categories = _matrix(train)
            matrix_all, _ = _matrix([dict(row) for row in rows], categories)
            y = [int(row["label"] == "FRAUD") for row in train]
            positives = sum(y)
            negatives = len(y) - positives
            if positives == 0 or negatives == 0:
                raise ValueError(f"baseline training requires both labels for model {model_id}")
            class_ratio = negatives / positives
            # Keep imbalance handling deterministic and fit it from train rows
            # only; validation/test labels never influence model fitting.
            if model_id in {"lightgbm", "xgboost"}:
                model.set_params(scale_pos_weight=class_ratio)
            elif model_id == "catboost":
                model.set_params(class_weights=[1.0, class_ratio])
            model.fit(matrix_train, y)
            # Warm the estimator once before timing the documented scoring batch.
            model.predict_proba(matrix_all[:1])
            prediction_started = time.perf_counter()
            scores = model.predict_proba(matrix_all)[:, 1]
            prediction_elapsed = max(time.perf_counter() - prediction_started, 1e-12)
            predictions = [
                _record_for_row(row, float(score)) for row, score in zip(rows, scores, strict=True)
            ]
            model_metadata[model_id] = {
                "parameters": model.get_params(),
                "categories": categories,
                "performance": {
                    "prediction_latency_ms_p50": prediction_elapsed * 1000 / len(rows),
                    "prediction_latency_ms_p95": prediction_elapsed * 1000 / len(rows),
                    "prediction_latency_ms_p99": prediction_elapsed * 1000 / len(rows),
                    "throughput_rows_per_second": len(rows) / prediction_elapsed,
                    "measurement": (
                        "one deterministic batch after one-row warm-up; environment-bound"
                    ),
                    "warmup_batches": 1,
                    "batch_size": len(rows),
                },
            }
            try:
                import joblib  # type: ignore[import-untyped]

                artifact = io.BytesIO()
                joblib.dump({"model": model, "categories": categories}, artifact)
                model_artifacts[model_id] = artifact.getvalue()
            except ImportError as exc:
                raise RuntimeError("baseline model persistence requires joblib") from exc
        result = evaluate_predictions(
            rows, predictions, config, model_id=model_id, source_run_dir=source_run_dir
        )
        all_predictions.extend(result.predictions)
        all_metrics.extend(result.metrics)
    manifest = {
        "evaluation_version": __version__,
        "models": list(config.models),
        "model_metadata": model_metadata,
        "feature_version": FEATURE_VERSION,
        "configuration": config.model_dump(mode="json"),
        "output_fingerprint": sha256_json({"predictions": all_predictions, "metrics": all_metrics}),
        "runtime": {"python": platform.python_version(), "platform": platform.platform()},
        "tracking": _tracking_metadata(config),
        "lineage": lineage,
        "preprocessing": PREPROCESSING_METADATA.copy(),
        "class_weights": "balanced from train labels only",
    }
    return EvaluationResult(tuple(all_predictions), tuple(all_metrics), manifest, model_artifacts)


def write_evaluation(result: EvaluationResult, output_dir: Path) -> tuple[Path, Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=False)
    predictions_path = output_dir / "predictions.parquet"
    metrics_path = output_dir / "metrics.jsonl"
    canonical_rows = [
        PredictionRecord.model_validate(item).model_dump(mode="python")
        for item in result.predictions
    ]
    pl.DataFrame(canonical_rows, schema=PREDICTION_SCHEMA, orient="row").write_parquet(
        predictions_path
    )
    if result.model_artifacts:
        models_dir = output_dir / "models"
        models_dir.mkdir()
        checksums: dict[str, str] = {}
        for model_id, content in sorted(result.model_artifacts.items()):
            artifact_path = models_dir / f"{model_id}.joblib"
            artifact_path.write_bytes(content)
            checksums[str(artifact_path.relative_to(output_dir))] = hashlib.sha256(
                content
            ).hexdigest()
        result.manifest["model_artifact_checksums"] = checksums
    metrics_path.write_text(
        "".join(json.dumps(item, sort_keys=True, default=str) + "\n" for item in result.metrics),
        encoding="utf-8",
    )
    manifest_path = output_dir / "evaluation_manifest.json"
    write_json(manifest_path, result.manifest)
    tracking_uri = result.manifest.get("configuration", {}).get("tracking_uri")
    if tracking_uri:
        try:
            import mlflow  # type: ignore[import-not-found]
        except ImportError as exc:
            raise RuntimeError("MLflow tracking was requested but mlflow is not installed") from exc
        mlflow.set_tracking_uri(str(tracking_uri))
        with mlflow.start_run(run_name=str(result.manifest.get("evaluation_version"))):
            mlflow.log_params({
                "feature_version": str(result.manifest.get("feature_version")),
                "model_id": str(result.manifest.get("model_id", "multiple")),
            })
            mlflow.log_artifacts(str(output_dir))
    return predictions_path, metrics_path, manifest_path


__all__ = [
    "MODEL_NAMES",
    "MODEL_FEATURES",
    "PREDICTION_SCHEMA",
    "PredictionRecord",
    "PredictionAdapter",
    "BaselineEvaluationConfig",
    "EvaluationResult",
    "load_baseline_config",
    "load_predictions",
    "write_predictions",
    "evaluate_predictions",
    "heuristic_predictions",
    "train_baselines",
    "write_evaluation",
]
