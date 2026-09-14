# FraudTwin documentation

FraudTwin is easiest to understand as a small, deterministic payment world. Start with the shortest path to a generated run, then move into configuration and the workflows you need.

## Start here

- [Quickstart](quickstart.md) — install FraudTwin and generate your first run.
- [Configuration](configuration.md) — shape populations, behavior, payment rails, fraud, and quality.
- [Workflows](workflows.md) — build point-in-time datasets, replay runs, and backtest models.
- [Graph and benchmark workflows](graph-and-benchmarks.md) — export graph views and create harder cases.
- [Development guide](development.md) — run checks, target tests, and contribute safely.

## Reference material

- [Release notes](../CHANGELOG.md) — milestone history and notable changes.
- [Contributor entry point](../DEVELOPMENT.md) — the short version of the development workflow.

## A useful mental model

Every run starts with entities and customer behavior, produces legitimate payments and lifecycle events, and then optionally layers on fraud, workflow, quality faults, graph structure, or benchmark stress. The simulator keeps those layers deterministic and connected, so a generated case can be traced back to the payment, actors, timestamps, and source records that explain it.

If you are preparing a tutorial, use the minimal configuration first. The versioned files under `configs/benchmarks/` are better for demonstrations of graph structure, difficulty, and camouflage because they make those choices explicit and reproducible.
