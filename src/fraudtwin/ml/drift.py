"""Deterministic data, domain, concept, and performance drift reports.

The drift helpers operate on ordinary row mappings so they can compare PIT
datasets, replay windows, or production extracts without requiring SciPy or a
monitoring vendor.  Reports contain the comparison policy and input
fingerprints, making an alert reproducible instead of an opaque dashboard
number.
"""

from __future__ import annotations

import math
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from fraudtwin.reproducibility import sha256_json

DriftFieldType = Literal["numeric", "categorical", "prevalence", "quality", "performance"]


class DriftConfig(BaseModel):
    """Immutable policy controlling a drift comparison.

    ``reference_name`` and ``comparison_name`` identify the windows in the
    report.  Numeric PSI bins are fitted from the reference values only.
    Thresholds are absolute metric values; an alert is raised when a metric is
    greater than or equal to its threshold.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    reference_name: str = Field(default="reference", min_length=1)
    comparison_name: str = Field(default="comparison", min_length=1)
    numeric_bins: int = Field(default=10, ge=2, le=100)
    psi_threshold: float = Field(default=0.20, ge=0)
    wasserstein_threshold: float = Field(default=0.10, ge=0)
    js_threshold: float = Field(default=0.10, ge=0)
    prevalence_threshold: float = Field(default=0.05, ge=0)
    performance_threshold: float = Field(default=0.05, ge=0)
    minimum_samples: int = Field(default=30, ge=1)
    smoothing: float = Field(default=1e-6, gt=0, lt=0.5)
    fields: tuple[str, ...] | None = None
    label_policy: str = Field(default="exclude_unresolved", min_length=1)

    @model_validator(mode="after")
    def names_must_differ(self) -> DriftConfig:
        if self.reference_name == self.comparison_name:
            raise ValueError("reference_name and comparison_name must differ")
        if self.fields is not None and len(set(self.fields)) != len(self.fields):
            raise ValueError("fields must not contain duplicates")
        return self


class DriftMetric(BaseModel):
    """One measured drift value and its decision threshold."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    field: str = Field(min_length=1)
    field_type: DriftFieldType
    method: str = Field(min_length=1)
    reference_value: float | None = None
    comparison_value: float | None = None
    threshold: float | None = Field(default=None, ge=0)
    alerted: bool = False
    reference_count: int = Field(ge=0)
    comparison_count: int = Field(ge=0)
    metadata: dict[str, Any] = Field(default_factory=dict)


class DriftReport(BaseModel):
    """Reproducible comparison of two row windows."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    report_version: str = "M29-drift-1"
    reference_window: str
    comparison_window: str
    reference_count: int = Field(ge=0)
    comparison_count: int = Field(ge=0)
    reference_fingerprint: str
    comparison_fingerprint: str
    label_policy: str
    metrics: tuple[DriftMetric, ...] = ()
    performance_metrics: tuple[DriftMetric, ...] = ()
    manifest: dict[str, Any] = Field(default_factory=dict)

    @property
    def alerts(self) -> tuple[DriftMetric, ...]:
        """Return metrics that exceeded their configured thresholds."""

        return tuple(
            metric for metric in (*self.metrics, *self.performance_metrics) if metric.alerted
        )

    @property
    def fingerprint(self) -> str:
        """Return a stable fingerprint for policy, inputs, and measurements."""

        return sha256_json(self.model_dump(mode="json", exclude={"manifest"}))


def _rows(value: Iterable[Mapping[str, Any]]) -> tuple[dict[str, Any], ...]:
    to_dicts = getattr(value, "to_dicts", None)
    if callable(to_dicts):
        return tuple(dict(row) for row in to_dicts())
    return tuple(dict(row) for row in value)


def _fingerprint(rows: Sequence[Mapping[str, Any]]) -> str:
    canonical = sorted(
        (dict(row) for row in rows),
        key=lambda row: sha256_json(row),
    )
    return sha256_json(canonical)


def _values(rows: Sequence[Mapping[str, Any]], field: str) -> tuple[Any, ...]:
    return tuple(row[field] for row in rows if field in row and row[field] is not None)


def _numeric(values: Sequence[Any]) -> tuple[float, ...] | None:
    if not values or any(isinstance(value, bool) for value in values):
        return None
    try:
        result = tuple(float(value) for value in values)
    except (TypeError, ValueError):
        return None
    return result if all(math.isfinite(value) for value in result) else None


def _quantiles(values: Sequence[float], count: int) -> tuple[float, ...]:
    ordered = sorted(values)
    if len(ordered) == 1:
        return (ordered[0],) * count
    result: list[float] = []
    for index in range(count):
        position = index * (len(ordered) - 1) / (count - 1)
        lower = math.floor(position)
        upper = math.ceil(position)
        fraction = position - lower
        result.append(ordered[lower] + (ordered[upper] - ordered[lower]) * fraction)
    return tuple(result)


def _bin_edges(values: Sequence[float], bins: int) -> tuple[float, ...]:
    quantiles = _quantiles(values, bins + 1)
    edges: list[float] = [quantiles[0]]
    for value in quantiles[1:]:
        if value > edges[-1]:
            edges.append(value)
    if len(edges) == 1:
        edges.append(edges[0] + 1.0)
    return tuple(edges)


def _histogram(values: Sequence[float], edges: Sequence[float]) -> tuple[int, ...]:
    counts = [0] * (len(edges) - 1)
    for value in values:
        index = len(counts) - 1
        for candidate in range(len(edges) - 1):
            if value < edges[candidate + 1]:
                index = candidate
                break
        counts[index] += 1
    return tuple(counts)


def _psi(reference: Sequence[int], comparison: Sequence[int], smoothing: float) -> float:
    ref_total = sum(reference) + smoothing * len(reference)
    cmp_total = sum(comparison) + smoothing * len(comparison)
    return sum(
        (((left + smoothing) / ref_total) - ((right + smoothing) / cmp_total))
        * math.log(((left + smoothing) / ref_total) / ((right + smoothing) / cmp_total))
        for left, right in zip(reference, comparison, strict=True)
    )


def _wasserstein(reference: Sequence[float], comparison: Sequence[float]) -> float:
    if not reference or not comparison:
        return 0.0
    sample_count = max(len(reference), len(comparison), 2)
    ref_quantiles = _quantiles(reference, sample_count)
    cmp_quantiles = _quantiles(comparison, sample_count)
    return (
        sum(abs(left - right) for left, right in zip(ref_quantiles, cmp_quantiles, strict=True))
        / sample_count
    )


def _js(reference: Sequence[Any], comparison: Sequence[Any], smoothing: float) -> float:
    ref_counts = Counter(reference)
    cmp_counts = Counter(comparison)
    categories = sorted(set(ref_counts) | set(cmp_counts), key=str)
    ref_total = sum(ref_counts.values()) + smoothing * len(categories)
    cmp_total = sum(cmp_counts.values()) + smoothing * len(categories)
    divergence = 0.0
    for category in categories:
        left = (ref_counts[category] + smoothing) / ref_total
        right = (cmp_counts[category] + smoothing) / cmp_total
        midpoint = (left + right) / 2
        divergence += 0.5 * left * math.log(left / midpoint) + 0.5 * right * math.log(
            right / midpoint
        )
    return divergence


def _metric(
    *,
    field: str,
    field_type: DriftFieldType,
    method: str,
    reference_value: float | None,
    comparison_value: float | None,
    threshold: float | None,
    reference_count: int,
    comparison_count: int,
    metadata: dict[str, Any] | None = None,
    alerted: bool | None = None,
) -> DriftMetric:
    if alerted is None:
        alerted = (
            threshold is not None
            and comparison_value is not None
            and comparison_value >= threshold
            and reference_count > 0
            and comparison_count > 0
        )
    return DriftMetric(
        field=field,
        field_type=field_type,
        method=method,
        reference_value=reference_value,
        comparison_value=comparison_value,
        threshold=threshold,
        alerted=alerted,
        reference_count=reference_count,
        comparison_count=comparison_count,
        metadata=metadata or {},
    )


def compare_windows(
    reference: Iterable[Mapping[str, Any]],
    comparison: Iterable[Mapping[str, Any]],
    config: DriftConfig | None = None,
) -> DriftReport:
    """Compare two row windows using deterministic distribution metrics.

    Args:
        reference: Baseline rows. Numeric bins are fitted from this window.
        comparison: New rows to evaluate against the baseline.
        config: Thresholds, window names, and fields. Defaults to
            :class:`DriftConfig`.

    Returns:
        A report containing feature, missingness, prevalence, and duplicate
        metrics plus input fingerprints.

    Raises:
        ValueError: If either input is empty or configured fields are absent.
    """

    policy = config or DriftConfig()
    reference_rows = _rows(reference)
    comparison_rows = _rows(comparison)
    if not reference_rows or not comparison_rows:
        raise ValueError("reference and comparison windows must contain at least one row")
    fields = policy.fields or tuple(
        sorted(set().union(*(row.keys() for row in reference_rows + comparison_rows)))
    )
    missing = [
        field
        for field in fields
        if not any(field in row for row in reference_rows + comparison_rows)
    ]
    if missing:
        raise ValueError("configured drift fields are absent: " + ", ".join(missing))

    metrics: list[DriftMetric] = []
    for field in fields:
        ref = _values(reference_rows, field)
        cmp = _values(comparison_rows, field)
        ref_numeric = _numeric(ref)
        cmp_numeric = _numeric(cmp)
        if ref_numeric is not None and cmp_numeric is not None:
            edges = _bin_edges(ref_numeric, policy.numeric_bins)
            ref_hist = _histogram(ref_numeric, edges)
            cmp_hist = _histogram(cmp_numeric, edges)
            metrics.append(
                _metric(
                    field=field,
                    field_type="numeric",
                    method="psi",
                    reference_value=0.0,
                    comparison_value=_psi(ref_hist, cmp_hist, policy.smoothing),
                    threshold=policy.psi_threshold,
                    reference_count=len(ref_numeric),
                    comparison_count=len(cmp_numeric),
                    metadata={"bins": len(edges) - 1, "edges": edges},
                )
            )
            metrics.append(
                _metric(
                    field=field,
                    field_type="numeric",
                    method="wasserstein",
                    reference_value=0.0,
                    comparison_value=_wasserstein(ref_numeric, cmp_numeric),
                    threshold=policy.wasserstein_threshold,
                    reference_count=len(ref_numeric),
                    comparison_count=len(cmp_numeric),
                )
            )
        else:
            metrics.append(
                _metric(
                    field=field,
                    field_type="categorical",
                    method="jensen_shannon",
                    reference_value=0.0,
                    comparison_value=_js(ref, cmp, policy.smoothing) if ref and cmp else None,
                    threshold=policy.js_threshold,
                    reference_count=len(ref),
                    comparison_count=len(cmp),
                )
            )
        ref_missing = 1.0 - len(ref) / len(reference_rows)
        cmp_missing = 1.0 - len(cmp) / len(comparison_rows)
        metrics.append(
            _metric(
                field=field,
                field_type="quality",
                method="missing_rate_delta",
                reference_value=ref_missing,
                comparison_value=abs(cmp_missing - ref_missing),
                threshold=policy.prevalence_threshold,
                reference_count=len(reference_rows),
                comparison_count=len(comparison_rows),
            )
        )

    if "label" in fields:
        ref_labels = [value for value in _values(reference_rows, "label") if value is not None]
        cmp_labels = [value for value in _values(comparison_rows, "label") if value is not None]

        def is_fraud(value: Any) -> bool:
            return value is True or value == 1 or str(value).upper() == "FRAUD"

        ref_rate = (
            sum(is_fraud(value) for value in ref_labels) / len(ref_labels) if ref_labels else None
        )
        cmp_rate = (
            sum(is_fraud(value) for value in cmp_labels) / len(cmp_labels) if cmp_labels else None
        )
        if ref_rate is not None and cmp_rate is not None:
            metrics.append(
                _metric(
                    field="label",
                    field_type="prevalence",
                    method="fraud_rate_delta",
                    reference_value=ref_rate,
                    comparison_value=abs(cmp_rate - ref_rate),
                    threshold=policy.prevalence_threshold,
                    reference_count=len(ref_labels),
                    comparison_count=len(cmp_labels),
                    metadata={"comparison_rate": cmp_rate, "label_policy": policy.label_policy},
                )
            )
    for identity in ("event_id", "payment_id"):
        ref_ids = _values(reference_rows, identity)
        cmp_ids = _values(comparison_rows, identity)
        if ref_ids or cmp_ids:
            ref_rate = 1.0 - len(set(ref_ids)) / len(ref_ids) if ref_ids else 0.0
            cmp_rate = 1.0 - len(set(cmp_ids)) / len(cmp_ids) if cmp_ids else 0.0
            metrics.append(
                _metric(
                    field=identity,
                    field_type="quality",
                    method="duplicate_rate_delta",
                    reference_value=ref_rate,
                    comparison_value=abs(cmp_rate - ref_rate),
                    threshold=policy.prevalence_threshold,
                    reference_count=len(ref_ids),
                    comparison_count=len(cmp_ids),
                    metadata={"comparison_rate": cmp_rate},
                )
            )
    if (
        len(reference_rows) < policy.minimum_samples
        or len(comparison_rows) < policy.minimum_samples
    ):
        metrics.append(
            _metric(
                field="__window__",
                field_type="quality",
                method="minimum_sample_size",
                reference_value=float(len(reference_rows)),
                comparison_value=float(len(comparison_rows)),
                threshold=float(policy.minimum_samples),
                alerted=True,
                reference_count=len(reference_rows),
                comparison_count=len(comparison_rows),
                metadata={"insufficient": True},
            )
        )
    report = DriftReport(
        reference_window=policy.reference_name,
        comparison_window=policy.comparison_name,
        reference_count=len(reference_rows),
        comparison_count=len(comparison_rows),
        reference_fingerprint=_fingerprint(reference_rows),
        comparison_fingerprint=_fingerprint(comparison_rows),
        label_policy=policy.label_policy,
        metrics=tuple(metrics),
    )
    return report.model_copy(
        update={
            "manifest": {
                "fingerprint": report.fingerprint,
                "config": policy.model_dump(mode="json"),
            }
        }
    )


def compare_performance(
    reference: Mapping[str, float],
    comparison: Mapping[str, float],
    *,
    config: DriftConfig | None = None,
    label_policy: str | None = None,
) -> tuple[DriftMetric, ...]:
    """Compare matching evaluation metrics under one threshold policy.

    Args:
        reference: Metric name to value mapping for the baseline window.
        comparison: Metric name to value mapping for the new window.
        config: Provides ``performance_threshold`` and sample policy.
        label_policy: Optional explicit policy; it must match the config when
            both are supplied.

    Returns:
        One relative absolute-change metric for every common metric.

    Raises:
        ValueError: If metric keys differ or label policies conflict.
    """

    policy = config or DriftConfig()
    if label_policy is not None and label_policy != policy.label_policy:
        raise ValueError("performance comparisons must use the same label policy")
    if set(reference) != set(comparison):
        raise ValueError("reference and comparison performance metrics must have identical keys")
    result = []
    for name in sorted(reference):
        left = float(reference[name])
        right = float(comparison[name])
        if not math.isfinite(left) or not math.isfinite(right):
            raise ValueError(f"performance metric {name!r} must be finite")
        delta = abs(right - left)
        result.append(
            _metric(
                field=name,
                field_type="performance",
                method="absolute_metric_delta",
                reference_value=left,
                comparison_value=delta,
                threshold=policy.performance_threshold,
                reference_count=1,
                comparison_count=1,
                metadata={"comparison_value": right, "label_policy": policy.label_policy},
            )
        )
    return tuple(result)


__all__ = ["DriftConfig", "DriftMetric", "DriftReport", "compare_performance", "compare_windows"]
