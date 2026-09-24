# Graph analytics

Build explainable graph views and use the same temporal provenance in a graph
database or a PyTorch Geometric experiment.

| IDs | Time | Extras | Output |
| --- | --- | --- | --- |
| 16 | 30–45 min | base; Neo4j optional | graph tables, Cypher, fingerprint |
| 17 | 30–45 min | `graph` optional | temporal features and evaluation |

Tutorial 16 includes the optional `!pip install neo4j` cell, a local Neo4j
startup command, connectivity check, and a small Cypher query. The offline
graph export and Polars investigation remain the default path. Tutorial 17
uses the existing `graph` extra; no external service is required.

## Tutorials

- [Investigate fraud with temporal graph exports](neo4j-graph-fraud.ipynb)
- [Build and evaluate temporal graph features](pyg-graph-model.ipynb)

**Related:** [graph and benchmark workflows](../graph-and-benchmarks.md).

**Next path:** [Streaming and Kafka reliability](streaming-reliability.md).
