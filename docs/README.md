# FraudTwin documentation

FraudTwin is a deterministic synthetic payment world for fraud detection, graph analysis, data engineering, and machine-learning experiments. Follow the learning path below to move from a first generated run to reproducible fraud benchmarks. The published manual is versioned; use the version selector when an example must match a specific release.

## Installation

- [Quickstart](quickstart.md) - install FraudTwin and generate your first run.
- [Python package and CLI](../README.md#quick-start) - choose between the Python API and command-line workflows.
- Build the documentation with `poetry run sphinx-build -E -W --keep-going -b html docs docs/_build/html`.

## Get Started

- [Getting Started](tutorials/01-getting-started.ipynb) - generate and inspect a first run in a notebook.
- [Configure a Simulation](tutorials/02-configure-a-simulation.ipynb) - use a YAML configuration and reproduce a run.
- [Explore Payments and Lifecycle Events](tutorials/03-explore-payments-and-lifecycles.ipynb) - follow entities, payments, events, and ledger effects.
- [Explore Fraud and Delayed Labels](tutorials/04-explore-fraud-and-delayed-labels.ipynb) - compare fraud truth, hard negatives, workflow artifacts, and label availability.

## Tutorials

- [From Events to an ML Dataset](tutorials/05-from-events-to-ml-dataset.ipynb) - handle imperfect events and build a point-in-time dataset.
- [Stress-Test Fraud Scenarios](tutorials/06-stress-test-fraud-scenarios.ipynb) - replay runs, inspect graphs, increase difficulty, and test counterfactuals.
- [Build a Simple Fraud Scoring Model](tutorials/07-train-a-simple-fraud-model.ipynb) - score point-in-time features with a transparent formula.
- [Build a Reproducible Fraud Benchmark](tutorials/08-build-a-reproducible-fraud-benchmark.ipynb) - connect counterfactuals, campaigns, calibration, label observation, scale planning, and evaluation.

## Guides and concepts

- [Configuration](configuration.md) - control populations, behavior, payment rails, fraud, labels, calibration, scale, and campaign dynamics.
- [Data and evaluation workflows](workflows.md) - build point-in-time datasets, replay runs, backtest models, and evaluate predictions.
- [Graph and benchmark workflows](graph-and-benchmarks.md) - export observable and oracle graph views and compare stress levels.
- [Concepts](concepts.md) - understand determinism, lifecycle events, labels, graph provenance, and quality faults.
- [Troubleshooting](troubleshooting.md) - resolve installation, configuration, optional dependency, graph, and scale issues.
- [Migration guides](migration.md) - check compatibility boundaries when moving between releases.
- [v2.0.0 release-readiness roadmap](release-readiness.md) - M18 scale gates and deferred platform integrations.

## Package Reference

- [Python API reference](api.rst) - curated, generated documentation for the public `fraudtwin` API.
- [Configuration parameter reference](configuration-reference.rst) - generated tables for every major Pydantic configuration section.
- [CLI reference](cli.rst) - command groups, validation commands, and workflow entry points.
- [Data contracts](data-contracts.rst) - generated-run artifacts, grains, and oracle boundaries.
- [Source code on GitHub](https://github.com/emedinac/fraud-twin) - implementation, tests, and issue tracker.

## Cheatsheets

- [Configuration cheatsheet](configuration.md#configuration-at-a-glance) - the main configuration sections at a glance.
- [Workflow cheatsheet](workflows.md#a-practical-evaluation-sequence) - the recommended generation and evaluation sequence.
- [Graph and benchmark cheatsheet](graph-and-benchmarks.md#choosing-a-fixture) - choose the fixture that matches your experiment.

## Resources

- [Development guide](development.md) - run checks, target tests, and contribute safely.
- [Glossary](glossary.md) - definitions for observable views, label maturity, campaigns, fingerprints, and checkpoints.
- [Release notes](../CHANGELOG.md) - follow release history and notable changes.
- [Contributor entry point](../DEVELOPMENT.md) - the short repository-level workflow.

## External Resources

- [FraudTwin on GitHub](https://github.com/emedinac/fraud-twin) - source code, tests, issues, and project history.
- [References and related work](references.md) - standards, handbooks, papers, and projects that informed the design.

## A useful mental model

Every run starts with entities and customer behavior, produces legitimate payments and lifecycle events, and can then layer on fraud, workflow, quality faults, graph structure, or benchmark stress. The simulator keeps those layers deterministic and connected, so a generated case can be traced back to the payment, actors, timestamps, and source records that explain it.
