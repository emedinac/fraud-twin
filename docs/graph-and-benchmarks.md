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

Campaign dynamics evolve matching graph campaigns in stable campaign order. Phase snapshots and transitions use isolated per-campaign streams, and derived payments receive reserved IDs with source lineage. Actor joins/leaves, mule and device rotation, cross-rail movement, split/merge mutations, and structural hyperedges remain closed over the oracle graph. Observable graph views use only source-available events; dynamic truth and transition reasons remain oracle-only.

## Choosing a fixture

| Fixture | Best for |
| --- | --- |
| `m10-minimal-v1.yaml` | Temporal splits, replay, and rolling backtests |
| `m11-graph-v2.yaml` | Graph structure, observable/oracle views, and exports |
| `m12-difficulty-v1.yaml` | Boundary-oriented fraud difficulty |
| `m13-camouflage-v1.yaml` | Feature and relation camouflage |

Reference calibration can be combined with graph fixtures when the reference
contains transfer endpoints. Graph calibration contributes aggregate degree and
motif summaries only; configured graph topology and graph closure remain
authoritative. Campaign summaries are available when the reference supplies a
campaign identifier and campaign dynamics are enabled.

Treat these files as immutable examples. Copy one into a working configuration when you need to explore a variation; do not edit the versioned fixture in place.

## Run the fraud stress benchmark

The benchmark command packages the deterministic stress dimensions into standard suites
and writes a PIT-safe dataset, latent/observed truth catalog, descriptors,
fixed split lineage, and comparable model results:

```bash
poetry run fraudtwin benchmark --suite mixed --difficulty 7 --seed 42 \
  --calibration-profile profiles/reference.yaml
poetry run fraudtwin benchmark --suite all --difficulty 7 --seed 42
```

The built-in dependency-free heuristic always runs. Optional scikit-learn,
LightGBM, XGBoost, and CatBoost models are selected with `--models` after
installing the optional ML extra:

```bash
poetry install -E ml
poetry run fraudtwin benchmark --suite graph \
  --models deterministic_heuristic --models xgboost
```

External runners use a `module:factory` adapter and may use any framework,
including PyTorch, TensorFlow/Keras, JAX/Flax, PyTorch Geometric, or another
scikit-learn-compatible environment. The runner receives only observable PIT
artifacts and must return the canonical prediction records; framework
dependencies remain outside FraudTwin's core installation.

Each benchmark manifest records the generator and suite versions, seed,
scenario mix, resolved stress controls, calibration identity, label-observation
policy, temporal ranges, ground-truth availability rules, model lineage, and
output fingerprints. Model scoring uses the isolated synthetic truth as its
complete target while keeping the PIT feature set observable; operational label
observations remain delayed and incomplete in the generated dataset.

## Run an immutable public benchmark pack

Milestone 21 bundles eight named packs whose generation controls, PIT windows,
metrics, calibration identity, descriptors, and logical content fingerprints
are frozen in the installed package:

```bash
fraudtwin benchmark run FT-B04-CAMOUFLAGE@1.0
fraudtwin benchmark describe FT-B04-CAMOUFLAGE@1.0.0
fraudtwin benchmark verify runs/benchmarks/BM-<id>
```

The shorthand `@1.0` is accepted only when it resolves to one patch version.
Pack runs reject incompatible FraudTwin versions, altered definitions, and
logical output drift. `benchmark verify` checks an existing artifact without
rerunning it. Released definitions are never edited or removed; a
change to seeds, scenarios, splits, metrics, label policy, or calibration
creates a new pack version, while prior versions remain runnable.

The M10 `fraudtwin.ml.BenchmarkPack` fixtures remain supported for backtests
over an existing generated run and are separate from these M21 public packs.

## Generator-quality benchmark

Milestone 22 evaluates the generator itself rather than a fraud model. The
standard profile runs every immutable M21 pack and reports correctness,
statistical/temporal/graph fidelity, fraud difficulty, scalability,
engineering performance, and reproducibility independently:

```bash
poetry run fraudtwin quality-benchmark --profile standard-v1
poetry run fraudtwin report RUN-<id>
```

Scalability metrics are populated from an explicitly executed M18 job; this
keeps large workloads out of normal quality runs.  Pass its evidence manifest
to the matching profile:

```bash
poetry run fraudtwin quality-benchmark \
  --profile standard-v1-dev \
  --scale-manifest runs/scale-benchmarks/<run>-benchmark.json
```

The default profile uses the small workload. The immutable
`standard-v1-medium`, `standard-v1-large`, `standard-v1-xlarge`, and
`standard-v1-billion` profiles are opt-in and require documented hardware.
External generators can be supplied as a `module:factory` adapter or a
normalized artifact bundle. A capability that is not supplied is reported as
`N/A`, never as a zero score.
