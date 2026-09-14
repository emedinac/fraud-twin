# FraudTwin documentation

FraudTwin is easiest to understand as a small, deterministic payment world. Start with the shortest path to a generated run, then move into configuration and the workflows you need.

## Start here

- [Getting Started tutorial](tutorials/01-getting-started.ipynb) - generate and inspect a first run in a notebook.
- [Configure a Simulation tutorial](tutorials/02-configure-a-simulation.ipynb) - edit a YAML configuration and generate a reproducible run.
- [Explore Payments and Lifecycle Events tutorial](tutorials/03-explore-payments-and-lifecycles.ipynb) - follow entities, payments, events, and ledger effects.
- [Explore Fraud and Delayed Labels tutorial](tutorials/04-explore-fraud-and-delayed-labels.ipynb) - compare fraud truth, hard negatives, workflow artifacts, and label availability.
- [From Events to an ML Dataset tutorial](tutorials/05-from-events-to-ml-dataset.ipynb) - handle imperfect events and build a point-in-time dataset.
- [Stress-Test Fraud Scenarios tutorial](tutorials/06-stress-test-fraud-scenarios.ipynb) - replay runs, inspect graphs, increase difficulty, and test counterfactuals.
- [Build a Simple Fraud Scoring Model tutorial](tutorials/07-train-a-simple-fraud-model.ipynb) - score point-in-time features with a transparent formula.
- [Quickstart](quickstart.md) - install FraudTwin and generate your first run.
- [Configuration](configuration.md) - shape populations, behavior, payment rails, fraud, and quality.
- [Workflows](workflows.md) - build point-in-time datasets, replay runs, and backtest models.
- [Graph and benchmark workflows](graph-and-benchmarks.md) - export graph views and create harder cases.
- [Development guide](development.md) - run checks, target tests, and contribute safely.

## Reference material

- [Release notes](../CHANGELOG.md) - milestone history and notable changes.
- [Contributor entry point](../DEVELOPMENT.md) - the short version of the development workflow.

## A useful mental model

Every run starts with entities and customer behavior, produces legitimate payments and lifecycle events, and then optionally layers on fraud, workflow, quality faults, graph structure, or benchmark stress. The simulator keeps those layers deterministic and connected, so a generated case can be traced back to the payment, actors, timestamps, and source records that explain it.

If you are preparing a tutorial, use the minimal configuration first. The versioned files under `configs/benchmarks/` are better for demonstrations of graph structure, difficulty, and camouflage because they make those choices explicit and reproducible.
