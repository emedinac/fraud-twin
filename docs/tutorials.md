# Tutorials

The tutorials are rendered from the checked-in Jupyter notebooks. Their last
saved outputs are shown in the documentation, and every page includes a
download link so you can run it locally.

## Get started

| Tutorial | What you will learn |
| --- | --- |
| [Getting Started](tutorials/01-getting-started.ipynb) | Generate and inspect a first deterministic run. |
| [Configure a Simulation](tutorials/02-configure-a-simulation.ipynb) | Edit YAML configuration and reproduce a run. |
| [Explore Payments and Lifecycle Events](tutorials/03-explore-payments-and-lifecycles.ipynb) | Trace entities, payments, events, and ledger effects. |
| [Explore Fraud and Delayed Labels](tutorials/04-explore-fraud-and-delayed-labels.ipynb) | Compare fraud truth, workflow artifacts, and label availability. |

## Core tutorials

| Tutorial | What you will learn |
| --- | --- |
| [From Events to an ML Dataset](tutorials/05-from-events-to-ml-dataset.ipynb) | Build point-in-time features and labels without leakage. |
| [Stress-Test Fraud Scenarios](tutorials/06-stress-test-fraud-scenarios.ipynb) | Replay runs, inspect graphs, and vary scenario difficulty. |
| [Build a Simple Fraud Scoring Model](tutorials/07-train-a-simple-fraud-model.ipynb) | Train a transparent baseline on historical features. |

## Advanced tutorial

| Tutorial | What you will learn |
| --- | --- |
| [Build a Reproducible Fraud Benchmark](tutorials/08-build-a-reproducible-fraud-benchmark.ipynb) | Combine calibration, campaigns, labels, scale, and evaluation. |

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
