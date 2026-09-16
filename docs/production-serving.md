# Production serving reference

The repository includes a small local FastAPI reference service in
`examples/model_service`. It is designed to teach the boundary between a
trained FraudTwin artifact and an online scoring system, not to replace a
production platform.

Install the optional client packages in a notebook or local environment with:

```console
!pip install fastapi uvicorn httpx
```

```console
poetry install -E ml -E serving
FRAUDTWIN_MODEL_ARTIFACT=runs/evaluations/models/logistic_regression.joblib \
  poetry run uvicorn examples.model_service.app:app --host 0.0.0.0 --port 8000
```

Verify the running adapter with:

```console
curl -fsS http://127.0.0.1:8000/health
curl -fsS -X POST http://127.0.0.1:8000/score \
  -H 'content-type: application/json' \
  -d '{"event_id":"tutorial-10","prediction_timestamp":"2026-01-01T00:00:00Z","features":{}}'
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

## Serving contract

The reference adapter uses request/response schema version ``1``. A request
contains an event identifier, a timezone-aware ``prediction_timestamp``, and
features from the model's allowlist. The response contains the model ID,
feature version, score, decision, confidence state, and schema version. Reject
unknown feature names and timestamps without timezone information rather than
silently coercing them.

Treat the following as deployment compatibility keys:

| Key | Why it matters |
| --- | --- |
| Model ID/version | Identifies the loaded artifact and supports rollback |
| Feature version | Prevents online/offline feature mismatch |
| Dataset fingerprint | Connects the model to its evaluation data |
| Artifact checksum | Detects accidental replacement |
| Request/response schema version | Allows clients to migrate deliberately |

## MLOps deployment checklist

Before exposing a service beyond a trusted local network:

- load the artifact once per process and fail readiness if it cannot load;
- add authentication, authorization, and secret management outside this example;
- set request, model-inference, and upstream timeouts;
- limit concurrency and request body size;
- add rate limiting and a bounded human-review fallback;
- emit structured logs without payment or customer secrets;
- export request count, validation failures, latency, score distribution, and
  model version metrics;
- retain the previous compatible artifact for rollback;
- replay a fixed historical sample and compare online/offline predictions;
- record deployment metadata, fingerprints, and approval evidence.

The example intentionally does not provide authentication, durable queues,
autoscaling, multi-process model coordination, or a production secrets policy.
Those are platform responsibilities, not guarantees of the reference service.

## Local researcher checklist

For an offline experiment, the following is sufficient:

1. train and evaluate on a mature-label test window;
2. write a model artifact and metadata manifest;
3. start the service with ``FRAUDTWIN_MODEL_ARTIFACT``;
4. call ``GET /health`` and a valid/invalid ``POST /score`` request;
5. replay historical rows and compare predictions;
6. remove the temporary service and artifact directory.
