# FraudTwin model service

This is a local reference service, not a hardened deployment. Train an
evaluation with the `ml` extra, point `FRAUDTWIN_MODEL_ARTIFACT` at a persisted
`logistic_regression.joblib`, and start it with:

```console
poetry install -E ml -E serving
FRAUDTWIN_MODEL_ARTIFACT=runs/ml-evaluations/models/logistic_regression.joblib \
  poetry run uvicorn examples.model_service.app:app --reload
```

The service exposes `GET /health` and typed `POST /score`. It validates
timezone-aware prediction timestamps and rejects unknown model features. Load
the model once per process and keep the artifact fingerprint in the deployment
manifest.
