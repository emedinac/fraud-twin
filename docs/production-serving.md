# Production serving reference

The repository includes a small local FastAPI reference service in
`examples/model_service`. It is designed to teach the boundary between a
trained FraudTwin artifact and an online scoring system, not to replace a
production platform.

```console
poetry install -E ml -E serving
FRAUDTWIN_MODEL_ARTIFACT=runs/evaluations/models/logistic_regression.joblib \
  poetry run uvicorn examples.model_service.app:app --host 0.0.0.0 --port 8000
```

Endpoints:

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/health` | reports whether the configured artifact is available |
| `POST` | `/score` | validates a point-in-time request and returns a score/decision |

Requests require an event ID, timezone-aware `prediction_timestamp`, and a
feature mapping restricted to the M19 model allowlist. Responses include the
model ID and feature version so predictions can be joined to deployment
metadata.

Before promotion, measure artifact size, cold-load time, warm p50/p95 latency,
validation errors, low-confidence behavior, and offline/online prediction
parity. Keep MLflow metadata, evaluation fingerprints, and serving schema
versions together in the deployment record.
