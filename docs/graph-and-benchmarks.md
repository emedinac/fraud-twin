# Graph and benchmark workflows

Graph campaigns add relationships to the same payment and event identities already present in a run. They do not create a separate synthetic universe.

## Export graph views

Enable a `graph.scenarios` list in a new run, or use the versioned graph fixture:

```bash
poetry run fraudtwin generate \
  configs/benchmarks/m11-graph-v2.yaml \
  --output-dir /tmp/fraudtwin-graph
poetry run fraudtwin graph export \
  --run-id <run-id> \
  --output-dir /tmp/fraudtwin-graph \
  --format parquet,neo4j \
  --json
```

The exporter is read-only. Observable graphs contain only relationships supported by source events and available at the selected cutoff. Oracle graphs also contain campaign membership, patterns, evidence, and optional hyperedge incidence records. Neo4j output is dependency-free CSV/Cypher; PyTorch Geometric output is optional (`poetry install -E graph`).

Validate both graph structure and provenance when working with a fixture:

```bash
poetry run fraudtwin graph validate \
  --run-id <run-id> \
  --output-dir /tmp/fraudtwin-graph
```

## Difficulty benchmarks

Difficulty levels run from obvious to subtle. They adjust measurable controls such as fraud/legitimate overlap, behavioral deviation, hard-negative noise, prevalence, temporal irregularity, and graph structural subtlety while keeping the scenario objective and topology intact.

```bash
poetry run fraudtwin generate \
  configs/benchmarks/m12-difficulty-v1.yaml \
  --output-dir /tmp/fraudtwin-difficulty
```

The resolved profile, transformations, effective hash, and measured summaries are recorded in the source, dataset, and graph manifests. Active operational events omit direct scenario linkage; oracle artifacts retain it.

## Camouflage stress

Camouflage makes fraud look more like legitimate activity without changing the underlying truth. Feature camouflage affects amount, timing, merchant, device, geography, and frequency. Relation camouflage adds ordinary support payments and graph relationships while leaving campaign membership and induced fraud topology unchanged.

```bash
poetry run fraudtwin generate \
  configs/benchmarks/m13-camouflage-v1.yaml \
  --output-dir /tmp/fraudtwin-camouflage
```

Requested and effective strengths, cohort snapshots, constraints, and measured observable/oracle summaries are recorded in manifests. Geography is represented through valid customer and merchant choices because the payment schema has no standalone geography field. Requests that exceed available capacity are capped or redirected deterministically and recorded as constraints.

## Advanced campaign dynamics

M15 evolves matching M11 campaigns in stable campaign order. Phase snapshots and transitions use isolated per-campaign streams, and derived payments receive reserved `M15-` IDs with source lineage. Actor joins/leaves, mule and device rotation, cross-rail movement, split/merge mutations, and structural hyperedges remain closed over the oracle graph. Observable graph views use only source-available events; dynamic truth and transition reasons remain oracle-only.

## Choosing a fixture

| Fixture | Best for |
| --- | --- |
| `m10-minimal-v1.yaml` | Temporal splits, replay, and rolling backtests |
| `m11-graph-v2.yaml` | Graph structure, observable/oracle views, and exports |
| `m12-difficulty-v1.yaml` | Boundary-oriented fraud difficulty |
| `m13-camouflage-v1.yaml` | Feature and relation camouflage |

Reference calibration can be combined with graph fixtures when the reference
contains transfer endpoints. Graph calibration contributes aggregate degree and
motif summaries only; configured M11 topology and graph closure remain
authoritative. Campaign summaries are available when the reference supplies a
campaign identifier and M15 is enabled.

Treat these files as immutable examples. Copy one into a working configuration when you need to explore a variation; do not edit the versioned fixture in place.
