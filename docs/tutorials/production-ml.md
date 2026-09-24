# Production ML and reliability

This path follows a model and its data into operational conditions: temporal
evaluation, typed serving, model promotion, and segmented drift detection. All
core exercises run offline; MLflow and serving integrations are optional.

| Time | Extras | Output |
| --- | --- | --- |
| 45–90 min | `ml` | PIT dataset, metrics, model artifact |
| 30–60 min | `serving`, `mlflow` (optional) | service checks, promotion manifest |
| 30–60 min | base + `ml` (optional) | drift report and segment alerts |

The model lifecycle notebooks include optional `mlflow` and FastAPI/Uvicorn
client cells. They log or score a bounded artifact when available and retain a
local manifest fallback when no tracking server or HTTP process is running.

## Tutorials

- [Build a Simple Fraud Scoring Model](train-a-simple-fraud-model.ipynb)
- [Train, evaluate, and track a fraud model](train-and-track-fraud-model.ipynb)
- [Promote and serve a model locally](stress-drift-and-camouflage.ipynb)
- [Resume a scale run from a checkpoint](checkpoint-resume-scale.ipynb)
- [Promote, reject, and roll back model versions](mlflow-model-promotion.ipynb)
- [Measure drift by operational segment](segmented-drift-analysis.ipynb)

**Next path:** [Graph analytics](graph-analytics.md).
