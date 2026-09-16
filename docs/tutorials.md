# Tutorials

The tutorials are rendered from the checked-in Jupyter notebooks. Saved
outputs are shown when included, and every page includes a download link so
you can run it locally.

Every tutorial follows the same contract: start with a concrete goal, state
the prerequisites, produce a named run or artifact, and finish with a quick
verification step. The notebook is the executable companion; the surrounding
description tells you what to look for before you open it.

## Learning paths

Choose a path first; each category page contains the same notebooks with their
original names and stable URLs.

```{toctree}
:caption: Tutorial categories
:maxdepth: 1

tutorials/getting-started
tutorials/core-workflows
tutorials/production-ml
tutorials/graph-analytics
tutorials/streaming-reliability
tutorials/operations
```

## Categories at a glance

The cards summarize the learning path. Open a category to see its explicit
notebook toctree and the complete workflow context; no notebook is intentionally
listed twice on this page.

````{grid} 2
:gutter: 3

```{grid-item-card} Getting started
:link: tutorials/getting-started
:link-type: doc

**Tutorials 01–04 · 60–90 min · base install · offline**

Generate a world, configure it, inspect lifecycle events, and understand fraud
truth versus delayed labels. Produces manifests, tables, and ledger checks.
```

```{grid-item-card} Core workflows
:link: tutorials/core-workflows
:link-type: doc

**Tutorials 05, 06, 08 · 90–120 min · base + optional ML/graph · offline**

Build point-in-time datasets, stress scenarios, and reproducible benchmarks at
1k–10k source payments.
```

```{grid-item-card} Production ML and reliability
:link: tutorials/production-ml
:link-type: doc

**Tutorials 07, 09–11, 18–19 · 2–3 h · ML/serving/MLflow optional**

Train and evaluate models, validate serving hand-offs, promote artifacts, and
measure segmented drift. Docker services enhance but do not block offline work.
```

```{grid-item-card} Graph analytics
:link: tutorials/graph-analytics
:link-type: doc

**Tutorials 16–17 · 60–90 min · graph + ML extras · Neo4j optional**

Export provenance-rich graphs, investigate fraud with Cypher, and build temporal
PyTorch Geometric features with an evaluation split.
```

```{grid-item-card} Streaming and Kafka reliability
:link: tutorials/streaming-reliability
:link-type: doc

**Tutorials 12, 20–21 · 60–90 min · Kafka extra; Docker optional**

Exercise logical loss, retries, duplicates, delays, reordering, outages, and
Avro compatibility without simulating physical network packets.
```

```{grid-item-card} Operations and incident response
:link: tutorials/operations
:link-type: doc

**Tutorials 13–15, 22–24 · 2–3 h · service extras + Docker optional**

Repair damaged projections, resume scale runs, reconcile PostgreSQL, and verify
Iceberg snapshots and observability SLOs.
```
````

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
```
