"""Small reference FastAPI service for a FraudTwin baseline artifact.

This example deliberately keeps transport concerns outside ``fraudtwin``. It
loads one immutable joblib artifact per process and accepts Kafka-shaped
point-in-time feature envelopes.
"""

import os
from datetime import datetime
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from fraudtwin.ml.baseline import (
    MODEL_FEATURES,
    load_model_artifact,
    score_loaded_model,
)

try:
    from fastapi import FastAPI, HTTPException
except ImportError as exc:  # pragma: no cover - exercised in the optional example extra
    raise RuntimeError("install fraudtwin[serving] to run the model service") from exc


class ScoreRequest(BaseModel):
    """Typed point-in-time scoring request."""

    model_config = ConfigDict(extra="forbid")

    event_id: str = Field(min_length=1)
    prediction_timestamp: datetime
    features: dict[str, Any]

    @field_validator("prediction_timestamp")
    @classmethod
    def timestamp_must_be_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("prediction_timestamp must include a timezone")
        return value

    @field_validator("features")
    @classmethod
    def features_must_be_known(cls, value: dict[str, Any]) -> dict[str, Any]:
        unknown = sorted(set(value) - set(MODEL_FEATURES))
        if unknown:
            raise ValueError("unknown model features: " + ", ".join(unknown))
        return value


class ScoreResponse(BaseModel):
    """Typed scoring response returned to a stream or HTTP caller."""

    event_id: str
    fraud_score: float = Field(ge=0, le=1)
    decision: str
    model_id: str
    feature_version: str


class ModelRuntime:
    """Lazy, process-local model artifact loader and scorer."""

    def __init__(self, artifact: Path, threshold: float = 0.5) -> None:
        self.artifact = artifact
        self.threshold = threshold
        self._loaded = False
        self._model_artifact: dict[str, Any] | None = None

    def score(self, request: ScoreRequest) -> ScoreResponse:
        if self._model_artifact is None:
            self._model_artifact = load_model_artifact(self.artifact)
        scores = score_loaded_model(self._model_artifact, [request.features])
        score = scores[0]
        self._loaded = True
        return ScoreResponse(
            event_id=request.event_id,
            fraud_score=score,
            decision="REVIEW" if score >= self.threshold else "ALLOW",
            model_id=self.artifact.stem,
            feature_version="M19-observable-allowlist-1",
        )

    @property
    def metadata(self) -> dict[str, object]:
        """Return non-sensitive model metadata for startup logs and health checks."""

        return {
            "model_id": self.artifact.stem,
            "feature_version": "M19-observable-allowlist-1",
            "artifact": self.artifact.name,
            "loaded": self._loaded,
        }

    @property
    def healthy(self) -> bool:
        return self.artifact.is_file() and self._loaded


def create_app(artifact: Path | None = None) -> FastAPI:
    """Create the service app without loading the model during import."""

    model_path = artifact or Path(os.environ.get("FRAUDTWIN_MODEL_ARTIFACT", "model.joblib"))
    threshold = float(os.environ.get("FRAUDTWIN_REVIEW_THRESHOLD", "0.5"))
    runtime = ModelRuntime(model_path, threshold)
    app = FastAPI(title="FraudTwin model service", version="1")

    @app.get("/health")
    def health() -> dict[str, object]:
        return {"status": "ok" if runtime.healthy else "starting", "model": runtime.artifact.name}

    @app.post("/score", response_model=ScoreResponse)
    def score(request: ScoreRequest) -> ScoreResponse:
        try:
            return runtime.score(request)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=503, detail="model artifact is unavailable") from exc
        except (RuntimeError, ValueError) as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    return app


app = create_app()
