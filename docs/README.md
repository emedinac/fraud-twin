# FraudTwin documentation

FraudTwin is a deterministic synthetic payment world for fraud detection, graph analysis, data engineering, and machine-learning experiments. Follow the learning path below to move from a first generated run to reproducible fraud benchmarks. The published manual is versioned; use the version selector when an example must match a specific release.

## Installation

- [Quickstart](quickstart.md) - install FraudTwin and generate your first run.
- [Python package and CLI](../README.md#quick-start) - choose between the Python API and command-line workflows.
- Build the documentation with `poetry run sphinx-build -E -W --keep-going -b html docs docs/_build/html`.

## Tutorials

The published sidebar groups these notebooks into seven learning paths:
[Visualization and exploration](tutorials/visualization.md),
[Getting started](tutorials/getting-started.md),
[Core workflows](tutorials/core-workflows.md),
[Production ML and reliability](tutorials/production-ml.md),
[Graph analytics](tutorials/graph-analytics.md),
[Streaming and Kafka reliability](tutorials/streaming-reliability.md), and
[Operations and incident response](tutorials/operations.md).

Start with a category page instead of a flat notebook list. Each page explains
the expected input size, optional extras, service requirements, generated
artifacts, and the next recommended step.

The numeric IDs are stable compatibility identifiers, not a required sequence.
Follow the category order for the recommended learning path; descriptive
filenames and rendered titles explain what each notebook teaches.

- [Visualization and exploration](tutorials/visualization.md) - IDs 25–27 for temporal, spatial, distribution, scenario, and feature analysis.
- [Getting started](tutorials/getting-started.md) - IDs 1–4, from installation to delayed labels.
- [Core workflows](tutorials/core-workflows.md) - IDs 5, 6, and 8 for PIT datasets and benchmarks.
- [Production ML and reliability](tutorials/production-ml.md) - IDs 7, 9–11, 18, and 19 for model lifecycle and drift.
- [Graph analytics](tutorials/graph-analytics.md) - IDs 16 and 17 for Neo4j and PyTorch Geometric.
- [Streaming and Kafka reliability](tutorials/streaming-reliability.md) - IDs 12, 20, and 21 for delivery faults and Avro evolution.
- [Operations and incident response](tutorials/operations.md) - IDs 13–15 and 22–24 for repair, scale, persistence, and observability.

## Guides and concepts

- [Configuration](configuration.md) - control populations, behavior, payment rails, fraud, labels, calibration, scale, and campaign dynamics.
- [Data and evaluation workflows](workflows.md) - build point-in-time datasets, replay runs, backtest models, and evaluate predictions.
- [Model lifecycle](model-lifecycle.md) - move from a generated run to tracked artifacts and online scoring.
- [Production serving](production-serving.md) - run and validate the optional FastAPI reference service.
- [Drift and shift](drift-and-shift.md) - distinguish data, domain, concept, and performance drift.
- [Kafka reliability](kafka-reliability.md) - exercise logical-message loss, retries, duplicates, delays, and reordering.
- [Data-quality incidents](data-quality-incidents.md) - inject faults, replay windows, and repair projections.
- [Graph and benchmark workflows](graph-and-benchmarks.md) - export observable and oracle graph views and compare stress levels.
- [Concepts](concepts.md) - understand determinism, lifecycle events, labels, graph provenance, and quality faults.
- [Troubleshooting](troubleshooting.md) - resolve installation, configuration, optional dependency, graph, and scale issues.
- [Migration guides](migration.md) - check compatibility boundaries when moving between releases.
- [Compatibility and support policy](compatibility.md) - understand stable, optional, and experimental surfaces before upgrading.
- [Release-readiness roadmap](release-readiness.md) - scale gates and deferred platform integrations.

## Package Reference

- [Python API reference](api.rst) - workflow guides plus generated indexes for every supported `fraudtwin` export and importable module.
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
