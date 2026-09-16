# Visualization and exploration

Start here when you want to understand the generated payment world before
building a model or connecting a service. The notebooks use 1,000 deterministic
payments and show temporal behavior, distributions, fraud populations, and
point-in-time feature structure.

The core path uses the existing Polars and FraudTwin APIs. A clearly marked
optional notebook cell can install `matplotlib` and `scikit-learn` for richer
plots and t-SNE; no package-level dependency is added.

| ID | Focus | Time | Extras | Output |
| --- | --- | --- | --- | --- |
| 25 | Time, space, amounts, and lifecycle events | 20–30 min | base; plotting optional | tables, SVG plot, fingerprint |
| 26 | Scenarios, difficulty, camouflage, and benchmarks | 20–30 min | base | comparison tables and manifest |
| 27 | Distributions, correlation, leakage, and embeddings | 20–30 min | `ml`/plotting optional | feature report and split checks |

```{toctree}
:maxdepth: 1

temporal-payment-lifecycle-visualization.ipynb
fraud-scenarios-difficulty-benchmarks.ipynb
ml-feature-distributions-embeddings.ipynb
```

**Next path:** [Getting started](getting-started.md).
