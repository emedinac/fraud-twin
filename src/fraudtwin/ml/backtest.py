"""Leakage-safe rolling backtests over one generated FraudTwin history."""

import hashlib
import math
import re
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, cast

import polars as pl
import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from fraudtwin import __version__
from fraudtwin.config import (
    BacktestConfig,
    FraudRegimeConfig,
    MinimumLabelMaturityPolicy,
    SimulationRunConfig,
    _parse_duration_seconds,
)
from fraudtwin.manifest import BacktestManifest, RunManifest
from fraudtwin.ml.dataset import PointInTimeDatasetBuilder
from fraudtwin.reproducibility import as_utc, sha256_json
from fraudtwin.simulation.behavior import BehaviorDataset
from fraudtwin.simulation.generator import EntityDataset

METRIC_NAMES = ("roc_auc", "pr_auc", "precision", "recall", "f1", "brier_score")
BASELINE_FEATURES = (
    "new_device_flag",
    "new_country_flag",
    "transaction_count_1h",
    "amount_vs_customer_avg",
)

BACKTEST_ROW_SCHEMA: dict[str, Any] = {
    "model_id": pl.Utf8,
    "fold_id": pl.Utf8,
    "partition": pl.Utf8,
    "dataset_row_id": pl.Utf8,
    "payment_id": pl.Utf8,
    "prediction_time": pl.Datetime(time_zone="UTC"),
    "source_available_at": pl.Datetime(time_zone="UTC"),
    "feature_available_at": pl.Datetime(time_zone="UTC"),
    "label_available_at": pl.Datetime(time_zone="UTC"),
    "label": pl.Utf8,
    "fraud_score": pl.Float64,
}

FOLD_METRIC_SCHEMA: dict[str, Any] = {
    "model_id": pl.Utf8,
    "fold_id": pl.Utf8,
    "partition": pl.Utf8,
    "row_count": pl.Int64,
    "labelled_row_count": pl.Int64,
    "positive_count": pl.Int64,
    "roc_auc": pl.Float64,
    "pr_auc": pl.Float64,
    "precision": pl.Float64,
    "recall": pl.Float64,
    "f1": pl.Float64,
    "brier_score": pl.Float64,
}


def _utc(value: datetime) -> datetime:
    return as_utc(value, error_message="benchmark timestamps must include a timezone")


def _source_snapshots(entities: EntityDataset, behavior: BehaviorDataset) -> dict[str, object]:
    """Fingerprint every source table used by the historical reconstruction."""

    tables: dict[str, Iterable[Any]] = {**entities.all_tables(), **behavior.tables()}
    snapshots: dict[str, object] = {}
    for name, records in tables.items():
        materialized = tuple(records)
        snapshots[name] = {
            "row_count": len(materialized),
            "sha256": sha256_json([record.model_dump(mode="json") for record in materialized]),
        }
    return snapshots


class BenchmarkWindow(BaseModel):
    """One immutable half-open benchmark interval."""

    model_config = ConfigDict(extra="forbid")

    from_time: datetime = Field(validation_alias="from")
    to_time: datetime = Field(validation_alias="to")

    @field_validator("from_time", "to_time")
    @classmethod
    def timestamps_must_include_timezone(cls, value: datetime) -> datetime:
        return _utc(value)

    @model_validator(mode="after")
    def bounds_must_be_ordered(self) -> BenchmarkWindow:
        if self.to_time <= self.from_time:
            raise ValueError("benchmark window to must be after from")
        return self


class BenchmarkPackWindows(BaseModel):
    """Fixed train, validation, test, and optional stress windows."""

    model_config = ConfigDict(extra="forbid")

    train: BenchmarkWindow
    validation: BenchmarkWindow | None = None
    test: BenchmarkWindow
    stress: BenchmarkWindow | None = None

    @model_validator(mode="after")
    def windows_must_not_overlap(self) -> BenchmarkPackWindows:
        ordered = tuple(
            window for window in (self.train, self.validation, self.test, self.stress) if window
        )
        if any(
            current.from_time < previous.from_time
            for previous, current in zip(ordered, ordered[1:], strict=False)
        ):
            raise ValueError("benchmark windows must be chronological")
        if any(
            previous.to_time > current.from_time
            for previous, current in zip(ordered, ordered[1:], strict=False)
        ):
            raise ValueError("benchmark windows must not overlap")
        return self


class BenchmarkPack(BaseModel):
    """Versioned frozen evaluation definition."""

    model_config = ConfigDict(extra="forbid")

    id: str = Field(min_length=1)
    version: str = Field(min_length=1)
    source_seed: int = Field(ge=0)
    source_config_hash: str = Field(min_length=1)
    windows: BenchmarkPackWindows
    regimes: tuple[FraudRegimeConfig, ...] = ()
    label_policy: MinimumLabelMaturityPolicy = "exclude"
    scenario_parameters: dict[str, object] = Field(default_factory=dict)
    metric_definitions: list[str] = Field(default_factory=lambda: list(METRIC_NAMES))
    label_maturity_gap_seconds: int = Field(default=14 * 86_400, ge=1)

    @field_validator("label_maturity_gap_seconds", mode="before")
    @classmethod
    def maturity_gap_must_be_positive(cls, value: Any) -> int:
        return _parse_duration_seconds(value)

    @field_validator("version")
    @classmethod
    def version_must_be_semver(cls, value: str) -> str:
        if re.fullmatch(r"(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)", value) is None:
            raise ValueError("benchmark pack version must be semantic major.minor.patch")
        return value

    @model_validator(mode="after")
    def metrics_must_be_supported(self) -> BenchmarkPack:
        if not self.metric_definitions or any(
            metric not in METRIC_NAMES for metric in self.metric_definitions
        ):
            raise ValueError(f"metric_definitions must use supported metrics: {METRIC_NAMES}")
        transitions = (
            (
                (self.windows.train, self.windows.validation),
                (self.windows.validation, self.windows.test),
            )
            if self.windows.validation is not None
            else ((self.windows.train, self.windows.test),)
        )
        gap = timedelta(seconds=self.label_maturity_gap_seconds)
        if any(right.from_time < left.to_time + gap for left, right in transitions):
            raise ValueError("benchmark windows must include the label-maturity gap")
        return self

    @property
    def fingerprint(self) -> str:
        return sha256_json(self.model_dump(mode="json"))

    @property
    def identity(self) -> str:
        return f"{self.id}@{self.version}-{self.fingerprint[:12]}"


def load_benchmark_pack(path: Path) -> BenchmarkPack:
    """Load and validate one versioned benchmark-pack YAML file."""

    if not path.is_file():
        raise FileNotFoundError(f"benchmark pack does not exist: {path}")
    with path.open(encoding="utf-8") as benchmark_file:
        raw = yaml.safe_load(benchmark_file)
    if not isinstance(raw, dict):
        raise ValueError("benchmark pack root must be a mapping")
    return BenchmarkPack.model_validate(raw)


def _baseline_raw_score(row: dict[str, Any]) -> float:
    """Return the deterministic feature-only score used by the baseline."""

    amount_ratio = row.get("amount_vs_customer_avg")
    ratio = 1.0 if amount_ratio is None else max(0.0, min(float(amount_ratio), 10.0))
    return round(
        4.0 * float(row["new_device_flag"])
        + 2.0 * float(row["new_country_flag"])
        + min(float(row["transaction_count_1h"]), 10.0) / 10.0
        + max(0.0, ratio - 1.0) / 10.0,
        12,
    )


@dataclass(frozen=True)
class _BaselineScorer:
    """A tiny fold-fitted calibration around the deterministic feature score."""

    center: float = 0.0
    scale: float = 1.0

    def score(self, row: dict[str, Any]) -> float:
        return round((_baseline_raw_score(row) - self.center) / self.scale, 12)

    def as_metadata(self) -> dict[str, object]:
        return {
            "type": "deterministic_center_scale_baseline",
            "features": list(BASELINE_FEATURES),
            "center": self.center,
            "scale": self.scale,
            "tie_breaker": "dataset_row_id",
        }


def _fit_baseline(rows: Iterable[dict[str, Any]]) -> _BaselineScorer:
    """Fit only on mature train labels; unresolved labels are never targets."""

    labelled = [row for row in rows if row["label"] in {"FRAUD", "LEGITIMATE"}]
    positives = [_baseline_raw_score(row) for row in labelled if row["label"] == "FRAUD"]
    negatives = [_baseline_raw_score(row) for row in labelled if row["label"] == "LEGITIMATE"]
    if not positives or not negatives:
        return _BaselineScorer()
    center = (sum(positives) / len(positives) + sum(negatives) / len(negatives)) / 2
    scale = max(abs(sum(positives) / len(positives) - sum(negatives) / len(negatives)), 1.0)
    return _BaselineScorer(round(center, 12), round(scale, 12))


def _auc(labels: list[int], scores: list[float], row_ids: list[str]) -> float | None:
    positives = sum(labels)
    negatives = len(labels) - positives
    if not positives or not negatives:
        return None
    ordered = sorted(zip(scores, labels, row_ids, strict=True), key=lambda item: (item[0], item[2]))
    rank_sum = 0.0
    for rank, (_, label, _) in enumerate(ordered, start=1):
        if label:
            rank_sum += rank
    return (rank_sum - positives * (positives + 1) / 2) / (positives * negatives)


def _pr_auc(labels: list[int], scores: list[float], row_ids: list[str]) -> float | None:
    positives = sum(labels)
    if not positives:
        return None
    ordered = sorted(
        zip(scores, labels, row_ids, strict=True), key=lambda item: (-item[0], item[2])
    )
    area = 0.0
    found = 0
    previous_recall = 0.0
    for index, (_, label, _) in enumerate(ordered, start=1):
        found += label
        recall = found / positives
        precision = found / index
        area += (recall - previous_recall) * precision
        previous_recall = recall
    return area


def _metrics(rows: list[dict[str, Any]]) -> dict[str, object]:
    labelled = sorted(
        (row for row in rows if row["label"] in {"FRAUD", "LEGITIMATE"}),
        key=lambda row: str(row["dataset_row_id"]),
    )
    labels = [int(row["label"] == "FRAUD") for row in labelled]
    scores = [float(row["fraud_score"]) for row in labelled]
    row_ids = [str(row["dataset_row_id"]) for row in labelled]
    predicted = [score >= 1.0 for score in scores]
    tp = sum(prediction and label for prediction, label in zip(predicted, labels, strict=True))
    fp = sum(prediction and not label for prediction, label in zip(predicted, labels, strict=True))
    fn = sum(not prediction and label for prediction, label in zip(predicted, labels, strict=True))
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    probabilities = [1.0 / (1.0 + math.exp(-score)) for score in scores]
    return {
        "row_count": len(rows),
        "labelled_row_count": len(labelled),
        "positive_count": sum(labels),
        "roc_auc": _auc(labels, scores, row_ids),
        "pr_auc": _pr_auc(labels, scores, row_ids),
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "brier_score": (
            sum(
                (probability - label) ** 2
                for probability, label in zip(probabilities, labels, strict=True)
            )
            / len(labels)
            if labels
            else None
        ),
    }


def _mean(values: Iterable[float]) -> float | None:
    values = tuple(values)
    return sum(values) / len(values) if values else None


def _aggregate(fold_metrics: list[dict[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for metric in METRIC_NAMES:
        values = [
            cast(float, item[metric])
            for item in fold_metrics
            if isinstance(item[metric], int | float)
        ]
        mean = _mean(values)
        dispersion = (
            math.sqrt(sum((value - mean) ** 2 for value in values) / len(values))
            if mean is not None
            else None
        )
        baseline = values[0] if values else None
        degradation = (
            [value - baseline for value in values]
            if metric == "brier_score" and baseline is not None
            else [baseline - value for value in values]
            if baseline is not None
            else []
        )
        result[metric] = {
            "mean": mean,
            "dispersion": dispersion,
            "degradation_from_first": degradation,
        }
    return result


def _regime_results(
    rows: Iterable[dict[str, Any]], definitions: list[dict[str, object]]
) -> list[dict[str, object]]:
    parsed = [
        (
            str(definition["id"]),
            _utc(datetime.fromisoformat(str(definition["from_time"]))),
            _utc(datetime.fromisoformat(str(definition["to_time"]))),
            definition,
        )
        for definition in definitions
    ]
    counts: dict[str, dict[str, int]] = {}
    for row in rows:
        match = next(
            (item for item in parsed if item[1] <= _utc(row["prediction_time"]) < item[2]),
            None,
        )
        regime_id = match[0] if match else "unassigned"
        entry = counts.setdefault(regime_id, {"row_count": 0, "fraud_count": 0})
        entry["row_count"] += 1
        entry["fraud_count"] += int(row["label"] == "FRAUD")
    return [
        {
            "id": regime_id,
            "parameters": next((item[3] for item in parsed if item[0] == regime_id), {}),
            **values,
        }
        for regime_id, values in sorted(counts.items())
    ]


@dataclass(frozen=True)
class BacktestResult:
    """Fold rows, metrics, and the immutable result manifest."""

    fold_rows: tuple[dict[str, Any], ...]
    fold_metrics: tuple[dict[str, object], ...]
    manifest: BacktestManifest

    @property
    def rows_frame(self) -> pl.DataFrame:
        return pl.DataFrame(list(self.fold_rows), schema=BACKTEST_ROW_SCHEMA, orient="row")

    @property
    def metrics_frame(self) -> pl.DataFrame:
        return pl.DataFrame(list(self.fold_metrics), schema=FOLD_METRIC_SCHEMA, orient="row")


@dataclass(frozen=True)
class FoldSpec:
    fold_id: str
    train_from: datetime
    train_to: datetime
    validation_from: datetime | None
    validation_to: datetime | None
    test_from: datetime
    test_to: datetime
    stress_from: datetime | None = None
    stress_to: datetime | None = None

    def as_metadata(self) -> dict[str, object]:
        return {
            key: value.isoformat() if isinstance(value, datetime) else value
            for key, value in self.__dict__.items()
        }


def _folds(config: BacktestConfig, start: datetime, end: datetime) -> list[FoldSpec]:
    validation = config.validation_window_seconds or 0
    train = config.train_window_seconds or 0
    initial_train = train if config.train_mode == "fixed" else config.step_seconds
    first_test = start + timedelta(
        seconds=initial_train
        + config.label_maturity_gap_seconds * (2 if validation else 1)
        + validation
    )
    result: list[FoldSpec] = []
    test_start = first_test
    number = 1
    while test_start + timedelta(seconds=config.test_window_seconds) <= end:
        test_end = test_start + timedelta(seconds=config.test_window_seconds)
        validation_end = test_start - timedelta(seconds=config.label_maturity_gap_seconds)
        validation_start = (
            validation_end - timedelta(seconds=validation) if validation else validation_end
        )
        train_end = validation_start - timedelta(
            seconds=config.label_maturity_gap_seconds if validation else 0
        )
        train_start = (
            start if config.train_mode == "expanding" else train_end - timedelta(seconds=train)
        )
        if train_start >= train_end:
            raise ValueError("backtest training window is empty")
        result.append(
            FoldSpec(
                fold_id=f"FOLD-{number:04d}",
                train_from=train_start,
                train_to=train_end,
                validation_from=validation_start if validation else None,
                validation_to=validation_end if validation else None,
                test_from=test_start,
                test_to=test_end,
            )
        )
        test_start += timedelta(seconds=config.step_seconds)
        number += 1
    if not result:
        raise ValueError("backtest settings do not fit the source history")
    return result


def _assign(rows: Iterable[dict[str, Any]], fold: FoldSpec) -> list[tuple[str, dict[str, Any]]]:
    assigned: list[tuple[str, dict[str, Any]]] = []
    for row in rows:
        timestamp = _utc(row["prediction_time"])
        partition: str | None = None
        if fold.train_from <= timestamp < fold.train_to:
            partition = "train"
        elif (
            fold.validation_from is not None
            and fold.validation_to is not None
            and fold.validation_from <= timestamp < fold.validation_to
        ):
            partition = "validation"
        elif fold.test_from <= timestamp < fold.test_to:
            partition = "test"
        elif (
            fold.stress_from is not None
            and fold.stress_to is not None
            and fold.stress_from <= timestamp < fold.stress_to
        ):
            partition = "stress"
        if partition is not None:
            assigned.append((partition, row))
    return assigned


def _source_prefix(behavior: BehaviorDataset, cutoff: datetime) -> BehaviorDataset:
    """Return the source history physically available before ``cutoff``."""

    payments = tuple(
        payment for payment in behavior.payments if _utc(payment.initiated_at) < cutoff
    )
    payment_ids = {payment.payment_id for payment in payments}
    events = tuple(
        event
        for event in behavior.payment_events
        if event.payment_id in payment_ids and _utc(event.event_time) < cutoff
    )
    fraud_records = tuple(
        record
        for record in behavior.fraud_records
        if record.payment_id in payment_ids and _utc(record.occurred_at) < cutoff
    )
    fraud_record_ids = {record.fraud_record_id for record in fraud_records}
    alerts = tuple(
        alert
        for alert in behavior.alerts
        if alert.fraud_record_id in fraud_record_ids and _utc(alert.alert_created_at) < cutoff
    )
    alert_ids = {alert.fraud_alert_id for alert in alerts}
    cases = tuple(
        case
        for case in behavior.fraud_cases
        if case.fraud_alert_id in alert_ids and _utc(case.case_opened_at) < cutoff
    )
    case_ids = {case.fraud_case_id for case in cases}
    return BehaviorDataset(
        profiles=behavior.profiles,
        payments=payments,
        payment_events=events,
        ledger_entries=tuple(
            entry
            for entry in behavior.ledger_entries
            if entry.payment_id in payment_ids and _utc(entry.posted_at) < cutoff
        ),
        fraud_records=fraud_records,
        alerts=alerts,
        fraud_cases=cases,
        case_confirmations=tuple(
            item
            for item in behavior.case_confirmations
            if item.fraud_case_id in case_ids and _utc(item.confirmed_at) < cutoff
        ),
        customer_disputes=tuple(
            item
            for item in behavior.customer_disputes
            if item.fraud_case_id in case_ids and _utc(item.event_time) < cutoff
        ),
        fraud_labels=tuple(
            item
            for item in behavior.fraud_labels
            if item.fraud_case_id in case_ids and _utc(item.label_available_at) < cutoff
        ),
    )


def _assert_future_fold_isolation(
    analysis_config: SimulationRunConfig,
    entities: EntityDataset,
    behavior: BehaviorDataset,
    source_manifest: RunManifest,
    rows: tuple[dict[str, Any], ...],
    folds: list[FoldSpec],
) -> None:
    """Rebuild each historical prefix and prove future records cannot leak back."""

    for fold in folds:
        cutoff = _utc(fold.test_from)
        prefix_rows = PointInTimeDatasetBuilder(
            analysis_config,
            entities,
            _source_prefix(behavior, cutoff),
            source_manifest,
        ).build_rows()
        full_prior = {
            row["dataset_row_id"]: row for row in rows if _utc(row["prediction_time"]) < cutoff
        }
        prefix_prior = {
            row["dataset_row_id"]: row
            for row in prefix_rows
            if _utc(row["prediction_time"]) < cutoff
        }
        if full_prior != prefix_prior:
            raise ValueError(f"future source records changed features before {cutoff.isoformat()}")


def run_backtest(
    config: SimulationRunConfig,
    entities: EntityDataset,
    behavior: BehaviorDataset,
    source_manifest: RunManifest,
    *,
    benchmark_pack: BenchmarkPack | None = None,
) -> BacktestResult:
    """Build reproducible rolling folds from one existing generated history."""

    source_start = _utc(source_manifest.start_time)
    source_end = _utc(source_manifest.end_time)
    label_policy = (
        benchmark_pack.label_policy
        if benchmark_pack is not None
        else config.backtest.minimum_label_maturity_policy
    )
    analysis_dataset = config.dataset.model_copy(
        update={
            "start": source_start,
            "end": source_end,
            "unresolved_labels": ("include" if label_policy == "include_unresolved" else "exclude"),
            "splits": config.dataset.splits.model_copy(
                update={
                    "train_end": None,
                    "validation_end": None,
                    "test_end": None,
                    "label_delay_gap_seconds": 0,
                }
            ),
        }
    )
    analysis_config = config.model_copy(update={"dataset": analysis_dataset})
    rows = PointInTimeDatasetBuilder(
        analysis_config, entities, behavior, source_manifest
    ).build_rows()
    definitions = source_manifest.regime_definitions or [
        regime.model_dump(mode="json") for regime in config.backtest.regimes
    ]
    if benchmark_pack is not None:
        if benchmark_pack.source_seed != source_manifest.seed:
            raise ValueError("benchmark pack seed does not match source run")
        if benchmark_pack.source_config_hash != source_manifest.scenario_config_hash:
            raise ValueError("benchmark pack configuration hash does not match source run")
        benchmark_regimes = [regime.model_dump(mode="json") for regime in benchmark_pack.regimes]
        if benchmark_regimes and benchmark_regimes != definitions:
            raise ValueError("benchmark pack regimes do not match source run")
        windows = (
            benchmark_pack.windows.train,
            benchmark_pack.windows.validation,
            benchmark_pack.windows.test,
            benchmark_pack.windows.stress,
        )
        if any(
            window is not None and (window.from_time < source_start or window.to_time > source_end)
            for window in windows
        ):
            raise ValueError("benchmark windows must fit the source history")
        folds = [
            FoldSpec(
                fold_id="BENCHMARK-0001",
                train_from=benchmark_pack.windows.train.from_time,
                train_to=benchmark_pack.windows.train.to_time,
                validation_from=(
                    benchmark_pack.windows.validation.from_time
                    if benchmark_pack.windows.validation
                    else None
                ),
                validation_to=(
                    benchmark_pack.windows.validation.to_time
                    if benchmark_pack.windows.validation
                    else None
                ),
                test_from=benchmark_pack.windows.test.from_time,
                test_to=benchmark_pack.windows.test.to_time,
                stress_from=(
                    benchmark_pack.windows.stress.from_time
                    if benchmark_pack.windows.stress
                    else None
                ),
                stress_to=(
                    benchmark_pack.windows.stress.to_time if benchmark_pack.windows.stress else None
                ),
            )
        ]
        metric_names = tuple(benchmark_pack.metric_definitions)
    else:
        folds = _folds(config.backtest, source_start, source_end)
        metric_names = METRIC_NAMES

    _assert_future_fold_isolation(analysis_config, entities, behavior, source_manifest, rows, folds)
    fold_rows: list[dict[str, Any]] = []
    fold_metrics: list[dict[str, object]] = []
    fold_metadata: list[dict[str, object]] = []
    all_assigned: list[dict[str, Any]] = []
    for fold in folds:
        assigned = _assign(rows, fold)
        scorer = _fit_baseline(row for partition, row in assigned if partition == "train")
        fold_row_values: dict[str, list[dict[str, Any]]] = {
            "train": [],
            "validation": [],
            "test": [],
            "stress": [],
        }
        for partition, row in assigned:
            if row["source_available_at"] > row["prediction_time"]:
                raise ValueError("backtest source availability is after prediction time")
            if row["feature_available_at"] > row["prediction_time"]:
                raise ValueError("backtest feature availability is after prediction time")
            if (
                row["label"] is not None
                and row["label_available_at"] is not None
                and row["label_available_at"] > row["prediction_time"]
            ):
                raise ValueError("backtest label availability is after prediction time")
            score = scorer.score(row)
            output_row = {
                "model_id": "deterministic_heuristic",
                "fold_id": fold.fold_id,
                "partition": partition,
                "dataset_row_id": row["dataset_row_id"],
                "payment_id": row["payment_id"],
                "prediction_time": row["prediction_time"],
                "source_available_at": row["source_available_at"],
                "feature_available_at": row["feature_available_at"],
                "label_available_at": row["label_available_at"],
                "label": row["label"],
                "fraud_score": score,
            }
            fold_rows.append(output_row)
            fold_row_values[partition].append(output_row)
            all_assigned.append(output_row)
        for partition in ("train", "validation", "test", "stress"):
            if partition == "validation" and fold.validation_from is None:
                continue
            if partition == "stress" and fold.stress_from is None:
                continue
            metrics = _metrics(fold_row_values[partition])
            fold_metrics.append({
                "model_id": "deterministic_heuristic",
                "fold_id": fold.fold_id,
                "partition": partition,
                **metrics,
            })
        metadata = fold.as_metadata()
        metadata["trainable_label_count"] = sum(
            row["label"] in {"FRAUD", "LEGITIMATE"} for row in fold_row_values["train"]
        )
        metadata["unresolved_train_label_count"] = sum(
            row["label"] is None for row in fold_row_values["train"]
        )
        metadata["baseline"] = scorer.as_metadata()
        fold_metadata.append(metadata)

    test_metrics = [item for item in fold_metrics if item["partition"] == "test"]
    aggregate = _aggregate(test_metrics)
    source_hash = hashlib.sha256(source_manifest.model_dump_json().encode()).hexdigest()
    parameters = config.backtest.model_dump(mode="json")
    if benchmark_pack is not None:
        parameters["benchmark_pack"] = benchmark_pack.identity
    output_fingerprint = sha256_json({
        "rows": fold_rows,
        "metrics": fold_metrics,
        "folds": fold_metadata,
    })
    backtest_id = "BT-" + sha256_json({"source": source_hash, "parameters": parameters})[:16]
    manifest = BacktestManifest(
        backtest_id=backtest_id,
        backtest_version=__version__,
        source_run_id=source_manifest.run_id,
        source_manifest_hash=source_hash,
        configuration_hash=source_manifest.scenario_config_hash,
        parameters=parameters,
        source_snapshots={
            "manifest_hash": source_hash,
            "source_run_id": source_manifest.run_id,
            "source_schema_versions": source_manifest.schema_versions,
            "tables": _source_snapshots(entities, behavior),
        },
        feature_version="M9-PIT-schema-1",
        folds=fold_metadata,
        pit_validation={
            "source_available_at_le_prediction_time": True,
            "feature_available_at_le_prediction_time": True,
            "label_policy": label_policy,
            "label_maturity_gap_seconds": (
                benchmark_pack.label_maturity_gap_seconds
                if benchmark_pack is not None
                else config.backtest.label_maturity_gap_seconds
            ),
            "future_fold_mutation": False,
            "future_fold_isolation_checks": len(folds),
        },
        regime_definitions=definitions,
        resolved_regimes=_regime_results(all_assigned, definitions),
        metric_definitions=list(metric_names),
        per_fold_metrics=fold_metrics,
        aggregate_metrics=aggregate,
        benchmark_pack=(
            {
                "id": benchmark_pack.id,
                "version": benchmark_pack.version,
                "identity": benchmark_pack.identity,
                "fingerprint": benchmark_pack.fingerprint,
                "definition": benchmark_pack.model_dump(mode="json"),
            }
            if benchmark_pack is not None
            else None
        ),
        output_fingerprint=output_fingerprint,
    )
    return BacktestResult(tuple(fold_rows), tuple(fold_metrics), manifest)


def run_model_backtest(
    config: SimulationRunConfig,
    entities: EntityDataset,
    behavior: BehaviorDataset,
    source_manifest: RunManifest,
    *,
    models: tuple[str, ...],
    benchmark_pack: BenchmarkPack | None = None,
) -> BacktestResult:
    """Run selected M19 model adapters over the existing M10 temporal folds."""

    from fraudtwin.ml.baseline import (
        MODEL_NAMES,
        BaselineEvaluationConfig,
        train_baselines,
    )

    if not models or any(model not in MODEL_NAMES for model in models):
        raise ValueError(f"models must use M19 names: {MODEL_NAMES}")
    legacy = run_backtest(
        config, entities, behavior, source_manifest, benchmark_pack=benchmark_pack
    )
    analysis_dataset = config.dataset.model_copy(
        update={
            "start": _utc(source_manifest.start_time),
            "end": _utc(source_manifest.end_time),
            "unresolved_labels": "exclude",
            "splits": config.dataset.splits.model_copy(
                update={
                    "train_end": None,
                    "validation_end": None,
                    "test_end": None,
                    "label_delay_gap_seconds": 0,
                }
            ),
        }
    )
    rows = PointInTimeDatasetBuilder(
        config.model_copy(update={"dataset": analysis_dataset}),
        entities,
        behavior,
        source_manifest,
    ).build_rows()
    rows_by_id = {str(row["dataset_row_id"]): row for row in rows}
    fold_rows: list[dict[str, Any]] = []
    fold_metrics: list[dict[str, object]] = []
    for model_id in models:
        for fold in legacy.manifest.folds:
            fold_id = str(fold["fold_id"])
            assigned_ids = [
                row["dataset_row_id"] for row in legacy.fold_rows if row["fold_id"] == fold_id
            ]
            source_rows = [dict(rows_by_id[str(row_id)]) for row_id in assigned_ids]
            partitions = {
                str(row["dataset_row_id"]): str(row["partition"])
                for row in legacy.fold_rows
                if row["fold_id"] == fold_id
            }
            source_rows = [
                dict(row, split=partitions[str(row["dataset_row_id"])]) for row in source_rows
            ]
            result = train_baselines(
                source_rows,
                BaselineEvaluationConfig(
                    models=(model_id,),
                    random_seed=source_manifest.seed,
                ),
            )
            for prediction in result.predictions:
                target_field = next(
                    field
                    for field in ("event_id", "payment_id", "customer_id", "account_id")
                    if prediction.get(field) is not None
                )
                source_row = next(
                    row
                    for row in source_rows
                    if str(row.get(target_field)) == str(prediction[target_field])
                    and _utc(row["prediction_time"])
                    == datetime.fromisoformat(
                        str(prediction["prediction_timestamp"]).replace("Z", "+00:00")
                    )
                )
                fold_rows.append({
                    "model_id": model_id,
                    "fold_id": fold_id,
                    "partition": partitions[str(source_row["dataset_row_id"])],
                    "dataset_row_id": source_row["dataset_row_id"],
                    "payment_id": source_row["payment_id"],
                    "prediction_time": source_row["prediction_time"],
                    "source_available_at": source_row["source_available_at"],
                    "feature_available_at": source_row["feature_available_at"],
                    "label_available_at": source_row["label_available_at"],
                    "label": source_row["label"],
                    "fraud_score": prediction["fraud_score"],
                })
            for metric in result.metrics:
                if "segment_dimension" not in metric:
                    fold_metrics.append({
                        "model_id": model_id,
                        "fold_id": fold_id,
                        "partition": metric["partition"],
                        "row_count": metric["row_count"],
                        "labelled_row_count": metric["labelled_row_count"],
                        "positive_count": metric["positive_count"],
                        "roc_auc": metric["roc_auc"],
                        "pr_auc": metric["pr_auc"],
                        "precision": metric["precision"],
                        "recall": metric["recall"],
                        "f1": metric["f1"],
                        "brier_score": metric["brier_score"],
                    })
    aggregate = {
        model: _aggregate([
            item
            for item in fold_metrics
            if item["model_id"] == model and item["partition"] == "test"
        ])
        for model in models
    }
    manifest = legacy.manifest.model_copy(
        update={
            "parameters": {**legacy.manifest.parameters, "models": list(models)},
            "per_fold_metrics": fold_metrics,
            "aggregate_metrics": aggregate,
            "output_fingerprint": sha256_json({
                "rows": fold_rows,
                "metrics": fold_metrics,
                "models": models,
            }),
        }
    )
    return BacktestResult(tuple(fold_rows), tuple(fold_metrics), manifest)


def write_backtest(result: BacktestResult, output_dir: Path) -> tuple[Path, Path, Path]:
    """Persist fold rows, metrics, and the append-only backtest manifest."""

    result_dir = output_dir / result.manifest.backtest_id
    result_dir.mkdir(parents=True, exist_ok=False)
    rows_path = result_dir / "fold_rows.parquet"
    metrics_path = result_dir / "fold_metrics.parquet"
    result.rows_frame.write_parquet(rows_path)
    result.metrics_frame.write_parquet(metrics_path)
    manifest_path = result_dir / "backtest_manifest.json"
    manifest_path.write_text(result.manifest.model_dump_json(indent=2) + "\n", encoding="utf-8")
    return rows_path, metrics_path, manifest_path


__all__ = [
    "BACKTEST_ROW_SCHEMA",
    "FOLD_METRIC_SCHEMA",
    "METRIC_NAMES",
    "BenchmarkPack",
    "BenchmarkPackWindows",
    "BenchmarkWindow",
    "BacktestResult",
    "FoldSpec",
    "load_benchmark_pack",
    "run_backtest",
    "run_model_backtest",
    "write_backtest",
]
