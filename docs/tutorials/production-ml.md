# Production ML and reliability

This path follows a model and its data into operational conditions: temporal
evaluation, typed serving, model promotion, and segmented drift detection. All
core exercises run offline; MLflow and serving integrations are optional.

| Tutorials | Time | Extras | Output |
| --- | --- | --- | --- |
| 07, 09 | 45–90 min | `ml` | PIT dataset, metrics, model artifact |
| 10, 18 | 30–60 min | `serving`, `mlflow` (optional) | service checks, promotion manifest |
| 11, 19 | 30–60 min | base + `ml` (optional) | drift report and segment alerts |

```{toctree}
:maxdepth: 1

07-train-a-simple-fraud-model.ipynb
09-10k-payments-to-fraud-model.ipynb
10-stress-drift-and-camouflage.ipynb
11-checkpoint-resume-scale.ipynb
18-mlflow-model-promotion.ipynb
19-segmented-drift-analysis.ipynb
```

**Next path:** [Graph analytics](graph-analytics.md).
