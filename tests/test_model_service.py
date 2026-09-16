# ruff: noqa
from datetime import UTC, datetime
from pathlib import Path

import pytest

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient

from examples.model_service.app import MODEL_FEATURES, create_app


def test_service_rejects_unknown_features_and_reports_missing_model() -> None:
    client = TestClient(create_app(Path("/tmp/does-not-exist.joblib")))
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "starting"
    response = client.post(
        "/score",
        json={
            "event_id": "EVT-1",
            "prediction_timestamp": datetime.now(UTC).isoformat(),
            "features": {"not_a_feature": 1},
        },
    )
    assert response.status_code == 422


def test_service_request_model_uses_documented_feature_allowlist() -> None:
    assert "amount" in MODEL_FEATURES
