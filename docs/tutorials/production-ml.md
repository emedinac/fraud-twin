# Production ML and reliability

This path follows a model and its data into operational conditions: temporal
evaluation, typed serving, model promotion, and segmented drift detection. All
core exercises run offline; MLflow and serving integrations are optional.

| IDs | Time | Extras | Output |
| --- | --- | --- | --- |
| 7, 9 | 45–90 min | `ml` | PIT dataset, metrics, model artifact |
| 10, 18 | 30–60 min | `serving`, `mlflow` (optional) | service checks, promotion manifest |
| 11, 19 | 30–60 min | base + `ml` (optional) | drift report and segment alerts |

The model lifecycle notebooks include optional `mlflow` and FastAPI/Uvicorn
client cells. They log or score a bounded artifact when available and retain a
local manifest fallback when no tracking server or HTTP process is running.

```{toctree}
:maxdepth: 1

train-a-simple-fraud-model.ipynb
train-and-track-fraud-model.ipynb
stress-drift-and-camouflage.ipynb
checkpoint-resume-scale.ipynb
mlflow-model-promotion.ipynb
segmented-drift-analysis.ipynb
```

**Next path:** [Graph analytics](graph-analytics.md).
