"""Focused Milestone 19 baseline and prediction-adapter tests."""

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from fraudtwin.ml import (
    BaselineEvaluationConfig,
    PredictionAdapter,
    PredictionRecord,
    evaluate_predictions,
    load_baseline_config,
    write_evaluation,
    write_predictions,
)


def _rows() -> list[dict[str, object]]:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    labels = ("LEGITIMATE", "FRAUD", "LEGITIMATE", "FRAUD", "LEGITIMATE", "FRAUD")
    return [
        {
            "dataset_row_id": f"row-{index}",
            "event_id": f"event-{index}",
            "payment_id": f"payment-{index}",
            "customer_id": f"customer-{index}",
            "account_id": f"account-{index}",
            "prediction_time": start + timedelta(days=index),
            "label": label,
            "split": "train" if index < 2 else "validation" if index < 4 else "test",
            "amount": 10.0,
            "currency": "USD",
            "payment_rail": "CARD",
            "payment_type": "PURCHASE",
            "online": True,
            "new_device_flag": index == 3,
            "new_country_flag": False,
            "transaction_count_1h": index,
        }
        for index, label in enumerate(labels)
    ]


def test_prediction_contract_is_strict_and_timezone_safe() -> None:
    prediction = PredictionRecord(
        event_id="event-1",
        prediction_timestamp=datetime(2026, 1, 1, tzinfo=UTC),
        fraud_score=0.5,
    )
    assert prediction.score == prediction.fraud_score
    with pytest.raises(ValidationError):
        PredictionRecord(
            event_id="event-1",
            payment_id="payment-1",
            prediction_timestamp=datetime(2026, 1, 1),
            fraud_score=0.5,
        )
    with pytest.raises(ValidationError):
        PredictionRecord(
            event_id="event-1",
            prediction_timestamp=datetime(2026, 1, 1, tzinfo=UTC),
            fraud_score=0.5,
            predicted_class="UNKNOWN",  # type: ignore[arg-type]
        )


def test_external_predictions_are_evaluated_deterministically(tmp_path: Path) -> None:
    rows = _rows()
    records = tuple(
        PredictionRecord(
            event_id=str(row["event_id"]),
            prediction_timestamp=row["prediction_time"],  # type: ignore[arg-type]
            fraud_score=score,
        )
        for row, score in zip(rows, (0.1, 0.9, 0.2, 0.8, 0.1, 0.9), strict=True)
    )
    config = BaselineEvaluationConfig(models=("deterministic_heuristic",))
    result = evaluate_predictions(rows, records, config)
    assert result.manifest["feature_version"] == "M19-observable-allowlist-1"
    test_metrics = next(
        item
        for item in result.metrics
        if item["partition"] == "test" and "segment_dimension" not in item
    )
    assert test_metrics["roc_auc"] == 1.0
    assert test_metrics["ranking_unit"] == "events"
    assert "monetary_by_currency" in test_metrics
    output = write_evaluation(result, tmp_path / "evaluation")
    assert all(path.is_file() for path in output)


def test_prediction_adapter_round_trip(tmp_path: Path) -> None:
    rows = _rows()
    records = tuple(
        PredictionRecord(
            payment_id=str(row["payment_id"]),
            prediction_timestamp=row["prediction_time"],  # type: ignore[arg-type]
            fraud_score=0.25,
        )
        for row in rows
    )
    path = write_predictions(records, tmp_path / "predictions.parquet")
    assert PredictionAdapter.validate(PredictionAdapter.load(path)) == records


def test_baseline_config_is_strict() -> None:
    config = load_baseline_config(Path("configs/ml-baselines.yaml"))
    assert config.models == ("logistic_regression", "lightgbm", "xgboost", "catboost")
    with pytest.raises(ValidationError):
        BaselineEvaluationConfig.model_validate({"models": ["unknown"]})
