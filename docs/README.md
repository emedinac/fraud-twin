# FraudTwin documentation

**Level:** All levels<br>
**You will:** find the right learning route, canonical guide, and reference<br>
surface for your work.
**Before you start:** none.<br>
**Services:** None.<br>

FraudTwin is a deterministic synthetic payment world for fraud detection,
graph analysis, data engineering, and machine-learning experiments. The manual
is organized by reader intent: Tutorials, How-to guides, Explanation, and
Reference. Use the onboarding page when you want help choosing where to begin.

## Installation

- [Installation and support](installation.md) - install the base package, choose extras, and verify an environment.
- [Quickstart](quickstart.md) - install FraudTwin and generate your first run.
- [Choose your path](learning-paths.md) - select the canonical documentation section for your goal.
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

## How-to guides

- [Build and evaluate](how-to/build-and-evaluate.md) - configure runs, build datasets, evaluate models, and measure drift.
- [Integrate and serve](how-to/integrate-and-serve.md) - connect optional services and validate serving or streaming paths.
- [Operate and repair](how-to/operate-and-repair.md) - handle quality faults, graphs, reconciliation, and bounded scale.
- [Extend and release](how-to/extend-and-release.md) - publish extensions and prepare release or benchmark evidence.

## Explanation

- [Core concepts](concepts.md) - understand determinism, lifecycles, labels, graph provenance, and quality faults.
- [Determinism and reproducibility](concepts/determinism-and-reproducibility.md) - understand seeds, streams, and fingerprints.
- [Architecture and trust boundaries](architecture.md) - understand source truth and integration boundaries.

## Reference

- [Python API reference](api.rst) - supported objects, workflow APIs, and generated module indexes.
- [Configuration parameter reference](configuration-reference.rst) - generated tables for configuration sections.
- [CLI reference](cli.rst) - command groups and workflow entry points.
- [Data contracts](data-contracts.rst) - generated-run artifacts, grains, and oracle boundaries.
- [Verified capabilities](verified-capabilities.md) - runnable commands and evidence behind supported workflows.

## Resources

- [Troubleshooting](troubleshooting.md) - resolve installation and optional dependency issues.
- [Migration guides](migration.md) - check compatibility boundaries when moving between releases.
- [Compatibility and support policy](compatibility.md) - understand stable, optional, and experimental surfaces.
- [Development guide](development.md) - run checks and contribute safely.
- [References and related work](references.md) - standards, papers, and projects that informed the design.

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
- [Source code on GitHub](https://github.com/emedinac/fraudtwin) - implementation, tests, and issue tracker.

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

- [FraudTwin on GitHub](https://github.com/emedinac/fraudtwin) - source code, tests, issues, and project history.
- [References and related work](references.md) - standards, handbooks, papers, and projects that informed the design.

## A useful mental model

Every run starts with entities and customer behavior, produces legitimate payments and lifecycle events, and can then layer on fraud, workflow, quality faults, graph structure, or benchmark stress. The simulator keeps those layers deterministic and connected, so a generated case can be traced back to the payment, actors, timestamps, and source records that explain it.

## Next

Choose a route from [Choose your path](learning-paths.md), starting with the
[Quickstart](quickstart.md) if you are new to FraudTwin.

## Related

- [Installation](installation.md)
- [Concepts](concepts.md)
- [API reference](api.rst)
