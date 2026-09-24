# FraudTwin documentation

**Level:** All levels<br>
**You will:** find the right learning route, canonical guide, and reference<br>
surface for your work.
**Before you start:** none.<br>
**Services:** None.<br>

FraudTwin is a deterministic synthetic payment world for fraud detection,
graph analysis, data engineering, and machine-learning experiments. The manual
is organized by experience level, not by internal project history. Follow one
route from a first generated run to reproducible evaluation or production-style
integration work.

## Installation

- [Installation and support](installation.md) - install the base package, choose extras, and verify an environment.
- [Quickstart](quickstart.md) - install FraudTwin and generate your first run.
- [Learning paths](learning-paths.md) - choose the beginner, intermediate, or expert route.
- [Python package and CLI](../README.md#quick-start) - choose between the Python API and command-line workflows.
- Build the documentation with `poetry run sphinx-build -E -W --keep-going -b html docs docs/_build/html`.

## Tutorials

The published sidebar groups these notebooks into eight tutorial categories:
[Visualization and exploration](tutorials/visualization.md),
[Getting started](tutorials/getting-started.md),
[Core workflows](tutorials/core-workflows.md),
[Production ML and reliability](tutorials/production-ml.md),
[Graph analytics](tutorials/graph-analytics.md),
[Streaming and Kafka reliability](tutorials/streaming-reliability.md),
[Operations and incident response](tutorials/operations.md), and
[Advanced experiments](tutorials/advanced-experiments.md).

Start with a category page instead of a flat notebook list. Each page explains
the expected input size, optional extras, service requirements, generated
artifacts, and the next recommended step.

Tutorial filenames are descriptive. Historical numeric routes remain redirects
for readers with old bookmarks; use the category pages as the recommended path.
Follow the category order for the recommended learning path; descriptive
filenames and rendered titles explain what each notebook teaches.

- [Visualization and exploration](tutorials/visualization.md) - temporal, spatial, distribution, scenario, and feature analysis.
- [Getting started](tutorials/getting-started.md) - installation through delayed labels.
- [Core workflows](tutorials/core-workflows.md) - point-in-time datasets and benchmarks.
- [Production ML and reliability](tutorials/production-ml.md) - model lifecycle and drift.
- [Graph analytics](tutorials/graph-analytics.md) - Neo4j and PyTorch Geometric.
- [Streaming and Kafka reliability](tutorials/streaming-reliability.md) - delivery faults and Avro evolution.
- [Operations and incident response](tutorials/operations.md) - repair, scale, persistence, and observability.
- [Advanced experiments](tutorials/advanced-experiments.md) - four combined workflows for calibration/interventions, graph investigations, ML shift/backtesting, and scale/reproducibility.

Tutorials remain a separate, notebook-first learning surface. They are linked
from the level routes but are not rewritten as part of the guide refactor.

## Choose your level

- [Beginner](levels/beginner.md) - install, generate, inspect, and understand the core model.
- [Intermediate](levels/intermediate.md) - configure evaluation workflows, quality, ML, and graphs.
- [Expert](levels/expert.md) - operate integrations, extensions, scale workflows, and release evidence.

## Find your path by role

- [New users](audiences/new-users.md)
- [Data and ML practitioners](audiences/data-ml.md)
- [Fraud and risk practitioners](audiences/fraud-risk.md)
- [Engineering and MLOps](audiences/engineering-mlops.md)
- [Researchers and governance reviewers](audiences/researchers-governance.md)

The role pages are shortcuts into the level routes. They do not duplicate the
canonical guides.

## Guides and concepts

- [Configuration](configuration.md) - control populations, behavior, payment rails, fraud, labels, calibration, scale, and campaign dynamics.
- [Data and evaluation workflows](workflows.md) - build point-in-time datasets, replay runs, backtest models, and evaluate predictions.
- [ML evaluation methodology](ml-evaluation.md) - use leakage-safe temporal splits, metrics, promotion criteria, and rollback checks.
- [Model lifecycle](model-lifecycle.md) - move from a generated run to tracked artifacts and online scoring.
- [Production serving](production-serving.md) - run and validate the optional FastAPI reference service.
- [Integration runbooks](integrations.md) - operate Kafka, PostgreSQL, Iceberg, Neo4j, MLflow, serving, and observability.
- [Drift and shift](drift-and-shift.md) - distinguish data, domain, concept, and performance drift.
- [Kafka reliability](kafka-reliability.md) - exercise logical-message loss, retries, duplicates, delays, and reordering.
- [Data-quality incidents](data-quality-incidents.md) - inject faults, replay windows, and repair projections.
- [Graph and benchmark workflows](graph-and-benchmarks.md) - export observable and oracle graph views and compare stress levels.
- [Concepts](concepts.md) - understand determinism, lifecycle events, labels, graph provenance, and quality faults.
- [Troubleshooting](troubleshooting.md) - resolve installation, configuration, optional dependency, graph, and scale issues.
- [Migration guides](migration.md) - check compatibility boundaries when moving between releases.
- [Compatibility and support policy](compatibility.md) - understand stable, optional, and experimental surfaces before upgrading.
- [Release-readiness roadmap](release-readiness.md) - scale gates and deferred platform integrations.
- [Architecture and trust boundaries](architecture.md) - source truth, chunked execution, and integration boundaries.
- [Verified capabilities](verified-capabilities.md) - runnable commands and evidence behind supported workflows.
- [Spark Structured Streaming](spark-streaming.md) - bounded Kafka/Parquet event-time processing.
- [Extension SDK](extensions.md) - stable ports, discovery, provenance, and compatibility rules.
- [Scale operations](scale-operations.md) - checkpointing, resume, storage, and claim boundaries.
- [Release and benchmark evidence](release-evidence.md) - reproducible laptop evidence and release verification.

## Documentation style

Every maintained guide states its level, expected outcome, prerequisites, and
service requirements near the beginning. Commands are written for copy/paste,
with the expected artifact or next decision explained immediately afterward.
Feature names are used in public prose; compatibility identifiers remain only
where a file format, release history, or existing command requires them.

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

## Next

Choose a route from [Learning paths](learning-paths.md), starting with
[Beginner](levels/beginner.md) if you are new to FraudTwin.

## Related

- [Installation](installation.md)
- [Concepts](concepts.md)
- [API reference](api.rst)
