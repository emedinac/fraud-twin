import pytest

from fraudtwin.ml.drift import DriftConfig, compare_performance, compare_windows


def test_compare_windows_reports_numeric_categorical_and_quality_drift() -> None:
    reference = tuple(
        {"amount": float(index), "merchant": "A", "label": "LEGITIMATE", "event_id": f"e-{index}"}
        for index in range(40)
    )
    comparison = tuple(
        {"amount": float(index + 100), "merchant": "B", "label": "FRAUD", "event_id": f"e-{index}"}
        for index in range(40)
    )
    report = compare_windows(
        reference,
        comparison,
        DriftConfig(
            reference_name="train",
            comparison_name="production",
            fields=("amount", "merchant", "label"),
        ),
    )
    assert report.reference_window == "train"
    assert report.comparison_window == "production"
    assert report.fingerprint == report.manifest["fingerprint"]
    assert any(metric.method == "wasserstein" and metric.alerted for metric in report.metrics)
    assert any(metric.method == "jensen_shannon" and metric.alerted for metric in report.metrics)
    assert any(metric.method == "fraud_rate_delta" and metric.alerted for metric in report.metrics)


def test_performance_comparison_requires_matching_keys_and_policy() -> None:
    metrics = compare_performance(
        {"pr_auc": 0.80, "recall_at_fixed_fpr": 0.50},
        {"pr_auc": 0.60, "recall_at_fixed_fpr": 0.45},
        config=DriftConfig(performance_threshold=0.04),
    )
    assert {metric.field for metric in metrics} == {"pr_auc", "recall_at_fixed_fpr"}
    assert all(metric.alerted for metric in metrics)


def test_compare_windows_rejects_empty_and_flags_small_windows() -> None:
    with pytest.raises(ValueError, match="at least one row"):
        compare_windows([], [{"amount": 1.0}])
    report = compare_windows(
        [{"amount": 1.0}, {"amount": 2.0}],
        [{"amount": 1.0}, {"amount": 3.0}],
        DriftConfig(minimum_samples=3),
    )
    sample_metric = next(
        metric for metric in report.metrics if metric.method == "minimum_sample_size"
    )
    assert sample_metric.alerted
