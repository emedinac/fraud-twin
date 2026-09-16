# Tutorials

The tutorials are rendered from the checked-in Jupyter notebooks. Their last
saved outputs are shown in the documentation, and every page includes a
download link so you can run it locally.

Every tutorial follows the same contract: start with a concrete goal, state
the prerequisites, produce a named run or artifact, and finish with a quick
verification step. The notebook is the executable companion; the surrounding
description tells you what to look for before you open it.

## Get started

| Tutorial | Goal and prerequisites | Produces / verify |
| --- | --- | --- |
| [Getting Started](tutorials/01-getting-started.ipynb) | Goal: generate and inspect a first deterministic run. Prerequisite: a base install. | Produces a run manifest and tables. Verify the seed and row counts; next read [determinism](concepts.md#determinism-and-seed-streams). |
| [Configure a Simulation](tutorials/02-configure-a-simulation.ipynb) | Goal: edit YAML and reproduce a run. Prerequisite: the first tutorial. | Produces a validated configuration. Verify the logical fingerprint; next read [configuration](configuration.md). |
| [Explore Payments and Lifecycle Events](tutorials/03-explore-payments-and-lifecycles.ipynb) | Goal: trace entities, payments, events, and ledger effects. Prerequisite: a generated run. | Produces lifecycle and ledger views. Verify balanced postings; next read [payment lifecycle](concepts.md#payment-and-ledger-lifecycle). |
| [Explore Fraud and Delayed Labels](tutorials/04-explore-fraud-and-delayed-labels.ipynb) | Goal: compare fraud truth, workflow artifacts, and label availability. Prerequisite: fraud-enabled configuration. | Produces observable and oracle comparisons. Verify mature labels only appear after their availability time; next read [point-in-time safety](concepts.md#observable-and-oracle-data). |

## Core tutorials

| Tutorial | Goal and prerequisites | Produces / verify |
| --- | --- | --- |
| [From Events to an ML Dataset](tutorials/05-from-events-to-ml-dataset.ipynb) | Goal: build point-in-time features and labels. Prerequisite: a generated run. | Produces an ML dataset. Verify no feature timestamp exceeds the cutoff; next read [dataset workflows](workflows.md#build-a-point-in-time-dataset). |
| [Stress-Test Fraud Scenarios](tutorials/06-stress-test-fraud-scenarios.ipynb) | Goal: replay runs, inspect graphs, and vary difficulty. Prerequisite: graph and fraud extras when enabled. | Produces comparable scenario outputs. Verify fingerprints differ only where expected; next read [graph workflows](graph-and-benchmarks.md). |
| [Build a Simple Fraud Scoring Model](tutorials/07-train-a-simple-fraud-model.ipynb) | Goal: train a transparent baseline. Prerequisite: the `ml` extra and a point-in-time dataset. | Produces scored predictions and metrics. Verify evaluation uses a held-out time window; next read [backtesting](workflows.md#run-rolling-backtests). |

## Advanced tutorial

| Tutorial | Goal and prerequisites | Produces / verify |
| --- | --- | --- |
| [Build a Reproducible Fraud Benchmark](tutorials/08-build-a-reproducible-fraud-benchmark.ipynb) | Goal: combine calibration, campaigns, labels, scale, and evaluation. Prerequisite: the core tutorials and optional benchmark extras. | Produces a benchmark manifest and report. Verify the manifest captures config, seed, and versions; next read [benchmark workflows](graph-and-benchmarks.md#run-the-fraud-stress-benchmark). |

The notebooks use the `python3` kernel. Install the project and the optional
ML extra before running the model and benchmark tutorials:

```console
poetry install -E ml
```

The published site does not execute notebooks during its build. This keeps the
Pages artifact deterministic; notebook JSON validity is checked separately in
CI.

```{toctree}
:hidden:

tutorials/01-getting-started.ipynb
tutorials/02-configure-a-simulation.ipynb
tutorials/03-explore-payments-and-lifecycles.ipynb
tutorials/04-explore-fraud-and-delayed-labels.ipynb
tutorials/05-from-events-to-ml-dataset.ipynb
tutorials/06-stress-test-fraud-scenarios.ipynb
tutorials/07-train-a-simple-fraud-model.ipynb
tutorials/08-build-a-reproducible-fraud-benchmark.ipynb
```
