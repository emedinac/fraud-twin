# Graph and relationship structures

Use this category for graph patterns, relationship structures, stress
fixtures, and controlled graph experiments. Compare transfer topology and
observable/oracle views using the examples below.

| Configuration | Capability | Scenario and target | Rails | Output or analysis focus |
| --- | --- | --- | --- | --- |
| `graph-v2.yaml` | Graph patterns and observable/oracle exports | Graph benchmark · 0.2% configured target | CARD, PIX, ACCOUNT_TRANSFER | Compare observable and oracle exports across topology patterns and provenance evidence |
| `graph-fanout-50pct.yaml` | Fan-out relationship pattern | F05 fan-out graph · 50% target | CARD, ACCOUNT_TRANSFER | Analyze fan-out destinations, relationship heuristics, and resulting graph alert patterns |

## Configuration details

Configuration: [configs/benchmarks/graph-v2.yaml](../../configs/benchmarks/graph-v2.yaml). Covers multiple graph patterns plus observable/oracle exports for topology and provenance checks.

````{dropdown} Graph patterns and exports

```{literalinclude} ../../configs/benchmarks/graph-v2.yaml
:language: yaml
```
````

Configuration: [examples/configuration/graph-fanout-50pct.yaml](../../examples/configuration/graph-fanout-50pct.yaml). Creates activity that converges on selected destinations for graph-alert and relationship heuristics.

````{dropdown} Fan-out relationship pattern

```{literalinclude} ../../examples/configuration/graph-fanout-50pct.yaml
:language: yaml
```
````

## Advanced graph topologies

| Configuration | Capability | Scenario and target | Rails | Output or analysis focus |
| --- | --- | --- | --- | --- |
| `bipartite-network-25pct-10k.yaml` | Originator/beneficiary bipartite graph | F03 · 25% | CARD, ACCOUNT_TRANSFER | Compare observable and oracle graph views across originator and beneficiary relationships |
| `scatter-gather-network-25pct-10k.yaml` | Intermediary scatter/gather flow | F05 · 25% | CARD, ACCOUNT_TRANSFER | Study how short dwell affects paths through shared intermediary accounts |
| `gather-scatter-network-25pct-10k.yaml` | Source/destination gather/scatter flow | F01 · 25% | CARD, ACCOUNT_TRANSFER | Trace source and destination flows across institution boundaries and transfer stages |
| `shared-device-infrastructure-25pct-10k.yaml` | Shared-device infrastructure | F05 · 25% | CARD, ACCOUNT_TRANSFER | Inspect observable shared-device links and the evidence available to investigators |
| `shared-ip-infrastructure-25pct-10k.yaml` | Shared-IP infrastructure | F03 · 25% | CARD, ACCOUNT_TRANSFER | Trace oracle shared-IP relationships and their supporting source-event provenance |
| `dense-campaign-25pct-10k.yaml` | Dense campaign and graph camouflage | F05 · 25% | CARD, ACCOUNT_TRANSFER | Measure dense campaign edges alongside feature and relation camouflage effects |
| `merchant-customer-community-25pct-10k.yaml` | Repeated merchant/customer community | F01 · 25% | CARD, ACCOUNT_TRANSFER | Analyze repeated customer activity and the resulting merchant community structure |
| `random-alert-control-25pct-10k.yaml` | Random-alert control graph | F03 · 25% | CARD, ACCOUNT_TRANSFER | Compare observable control edges against topology signals in fraud campaigns |

### Configuration details

Configuration: [examples/configuration/bipartite-network-25pct-10k.yaml](../../examples/configuration/bipartite-network-25pct-10k.yaml). Connects a set of originators to beneficiaries so you can inspect two-sided campaign structure.

````{dropdown} Bipartite originator/beneficiary graph

```{literalinclude} ../../examples/configuration/bipartite-network-25pct-10k.yaml
:language: yaml
```
````

Configuration: [examples/configuration/scatter-gather-network-25pct-10k.yaml](../../examples/configuration/scatter-gather-network-25pct-10k.yaml). Routes activity through shared intermediaries to study short-dwell network behavior.

````{dropdown} Intermediary scatter/gather flow

```{literalinclude} ../../examples/configuration/scatter-gather-network-25pct-10k.yaml
:language: yaml
```
````

Configuration: [examples/configuration/gather-scatter-network-25pct-10k.yaml](../../examples/configuration/gather-scatter-network-25pct-10k.yaml). Concentrates activity from sources into destinations for cross-institution flow analysis.

````{dropdown} Source/destination gather/scatter flow

```{literalinclude} ../../examples/configuration/gather-scatter-network-25pct-10k.yaml
:language: yaml
```
````

Configuration: [examples/configuration/shared-device-infrastructure-25pct-10k.yaml](../../examples/configuration/shared-device-infrastructure-25pct-10k.yaml). Shows how a shared device can connect otherwise separate actors in an observable graph.

````{dropdown} Shared-device infrastructure

```{literalinclude} ../../examples/configuration/shared-device-infrastructure-25pct-10k.yaml
:language: yaml
```
````

Configuration: [examples/configuration/shared-ip-infrastructure-25pct-10k.yaml](../../examples/configuration/shared-ip-infrastructure-25pct-10k.yaml). Adds shared network infrastructure for comparing observable evidence with oracle relationships.

````{dropdown} Shared-IP infrastructure

```{literalinclude} ../../examples/configuration/shared-ip-infrastructure-25pct-10k.yaml
:language: yaml
```
````

Configuration: [examples/configuration/dense-campaign-25pct-10k.yaml](../../examples/configuration/dense-campaign-25pct-10k.yaml). Creates a dense campaign and makes its features and relationships harder to distinguish.

````{dropdown} Dense campaign with camouflage

```{literalinclude} ../../examples/configuration/dense-campaign-25pct-10k.yaml
:language: yaml
```
````

Configuration: [examples/configuration/merchant-customer-community-25pct-10k.yaml](../../examples/configuration/merchant-customer-community-25pct-10k.yaml). Repeats interactions between selected customers and merchants to expose community structure.

````{dropdown} Merchant/customer community

```{literalinclude} ../../examples/configuration/merchant-customer-community-25pct-10k.yaml
:language: yaml
```
````

Configuration: [examples/configuration/random-alert-control-25pct-10k.yaml](../../examples/configuration/random-alert-control-25pct-10k.yaml). Provides a control topology for separating graph structure signals from fraud-specific structure.

````{dropdown} Random-alert control graph

```{literalinclude} ../../examples/configuration/random-alert-control-25pct-10k.yaml
:language: yaml
```
````

## Graph stress examples — 50% fraud target, 10k baseline

These fixtures use `payments.daily_target: 1000` over ten days, giving 10,000
baseline payments before graph and fraud scenarios add their generated
activity. Each sets `fraud.target_rate: 0.5`; this is a campaign target, not a
guarantee that half of all final payment rows are fraudulent. The static
relationship fixtures exercise graph scenario generation (Milestone 11). The
campaign fixtures enable graph dynamics (Milestone 15), whose default lifecycle
phases are compromise, setup, transfer, cash-out, dormant, and closed.

### Static graph relationships

| Configuration | Relation or topology | Focus |
| --- | --- | --- |
| `beneficiary-convergence-f03-50pct-10k.yaml` | Many sources converge on a shared beneficiary | Study how many source accounts concentrate transfers at one beneficiary |
| `shared-device-hyperedge-50pct-10k.yaml` | Shared-device structural hyperedge | Inspect account relationships formed through shared infrastructure identities |
| `shared-ip-hyperedge-50pct-10k.yaml` | Shared-IP semantic hyperedge | Inspect account relationships formed through shared infrastructure identities |
| `graph-10-hop-cross-institution-50pct-10k.yaml` | 10-edge closed ring across institutions | Analyze transfer-cycle length, timing, and cross-account movement patterns |
| `graph-25-hop-short-dwell-50pct-10k.yaml` | 25-edge closed ring with short dwell | Analyze transfer-cycle length, timing, and cross-account movement patterns |
| `graph-large-bipartite-50pct-10k.yaml` | Larger originator-to-beneficiary mesh | Compare originator and beneficiary connectivity across a larger transfer network |
| `graph-large-scatter-gather-50pct-10k.yaml` | Multiple intermediaries gather and forward flows | Trace transfers as they spread through and reconverge across intermediaries |
| `graph-camouflaged-community-50pct-10k.yaml` | Dense graph with relation camouflage | Measure whether relation camouflage obscures dense campaign links from graph analysis |

The hop count is the number of directed transfer edges in a closed ring.
`STACKED_NETWORK` remains a two-layer topology even with many intermediaries;
it is not a long sequential chain.

#### Configuration files

Configuration: [examples/configuration/beneficiary-convergence-f03-50pct-10k.yaml](../../examples/configuration/beneficiary-convergence-f03-50pct-10k.yaml). Connects multiple source accounts to one shared beneficiary for inbound-flow analysis.

````{dropdown} Shared-beneficiary convergence

```{literalinclude} ../../examples/configuration/beneficiary-convergence-f03-50pct-10k.yaml
:language: yaml
```
````

Configuration: [examples/configuration/shared-device-hyperedge-50pct-10k.yaml](../../examples/configuration/shared-device-hyperedge-50pct-10k.yaml). Links accounts through a shared device and emits the structural hyperedge relation.

````{dropdown} Shared-device structural hyperedge

```{literalinclude} ../../examples/configuration/shared-device-hyperedge-50pct-10k.yaml
:language: yaml
```
````

Configuration: [examples/configuration/shared-ip-hyperedge-50pct-10k.yaml](../../examples/configuration/shared-ip-hyperedge-50pct-10k.yaml). Links accounts through shared IP context and emits the semantic hyperedge relation.

````{dropdown} Shared-IP semantic hyperedge

```{literalinclude} ../../examples/configuration/shared-ip-hyperedge-50pct-10k.yaml
:language: yaml
```
````

Configuration: [examples/configuration/graph-10-hop-cross-institution-50pct-10k.yaml](../../examples/configuration/graph-10-hop-cross-institution-50pct-10k.yaml). Creates closed transfer cycles with ten directed edges across institutions.

````{dropdown} Ten-edge cross-institution ring

```{literalinclude} ../../examples/configuration/graph-10-hop-cross-institution-50pct-10k.yaml
:language: yaml
```
````

Configuration: [examples/configuration/graph-25-hop-short-dwell-50pct-10k.yaml](../../examples/configuration/graph-25-hop-short-dwell-50pct-10k.yaml). Creates closed transfer cycles with twenty-five directed edges and short dwell.

````{dropdown} Twenty-five-edge short-dwell ring

```{literalinclude} ../../examples/configuration/graph-25-hop-short-dwell-50pct-10k.yaml
:language: yaml
```
````

Configuration: [examples/configuration/graph-large-bipartite-50pct-10k.yaml](../../examples/configuration/graph-large-bipartite-50pct-10k.yaml). Connects originator and beneficiary groups for two-sided campaign analysis.

````{dropdown} Larger bipartite network

```{literalinclude} ../../examples/configuration/graph-large-bipartite-50pct-10k.yaml
:language: yaml
```
````

Configuration: [examples/configuration/graph-large-scatter-gather-50pct-10k.yaml](../../examples/configuration/graph-large-scatter-gather-50pct-10k.yaml). Routes transfers through multiple intermediaries between source and destination accounts.

````{dropdown} Large scatter/gather network

```{literalinclude} ../../examples/configuration/graph-large-scatter-gather-50pct-10k.yaml
:language: yaml
```
````

Configuration: [examples/configuration/graph-camouflaged-community-50pct-10k.yaml](../../examples/configuration/graph-camouflaged-community-50pct-10k.yaml). Builds a dense account graph and applies relation camouflage to its campaign.

````{dropdown} Dense camouflaged graph

```{literalinclude} ../../examples/configuration/graph-camouflaged-community-50pct-10k.yaml
:language: yaml
```
````

### Evolving campaign relationships

| Configuration | Dynamic graph behavior | Lifecycle focus |
| --- | --- | --- |
| `campaign-actor-churn-f03-50pct-10k.yaml` | Actors join and leave; mule and device relationships rotate | Track actor membership and device or mule rotations from setup through transfer |
| `campaign-split-merge-f03-50pct-10k.yaml` | Dense campaign topology receives split and merge mutations | Inspect split and merge mutations recorded when the campaign reaches its end |

#### Configuration files

Configuration: [examples/configuration/campaign-actor-churn-f03-50pct-10k.yaml](../../examples/configuration/campaign-actor-churn-f03-50pct-10k.yaml). Evolves campaign membership through actor joins and leaves, with mule and device rotations.

````{dropdown} Actor churn in a mule campaign

```{literalinclude} ../../examples/configuration/campaign-actor-churn-f03-50pct-10k.yaml
:language: yaml
```
````

Configuration: [examples/configuration/campaign-split-merge-f03-50pct-10k.yaml](../../examples/configuration/campaign-split-merge-f03-50pct-10k.yaml). Applies dense-campaign split and merge mutations; both are recorded at campaign end.

````{dropdown} Campaign split and merge

```{literalinclude} ../../examples/configuration/campaign-split-merge-f03-50pct-10k.yaml
:language: yaml
```
````
Campaign phase snapshots and topology mutations are retained in the oracle
graph. Observable graph exports contain only source events available at the
selected cutoff.

### Controlled graph experiment configs

These variants hold the 10k baseline and 50% F03 campaign target constant.
Paired configs reuse seeds and shared settings where possible, changing the
listed graph or campaign control to make comparisons easier to interpret.

#### Hop depth and institution scope

| Configuration | Comparison focus |
| --- | --- |
| `graph-5-hop-short-dwell-50pct-10k.yaml` | Compare five-edge short-dwell cycles with longer rings to study how path depth changes graph signals. |
| `graph-15-hop-cross-institution-50pct-10k.yaml` | Measure how a fifteen-edge cycle across institutions affects transfer-path detection. |
| `graph-20-hop-cross-institution-50pct-10k.yaml` | Measure how a twenty-edge cycle across institutions affects transfer-path detection. |
| `graph-10-hop-single-institution-50pct-10k.yaml` | Compare single-institution and cross-institution ten-edge rings while holding cycle length constant. |

#### Hyperedge ablations

| Configuration | Comparison focus |
| --- | --- |
| `shared-device-hyperedge-off-50pct-10k.yaml` | Compare shared-device edges with and without their structural hyperedge to isolate higher-order relation value. |
| `shared-ip-hyperedge-off-50pct-10k.yaml` | Compare shared-IP edges with and without their semantic hyperedge to isolate higher-order relation value. |

#### Topology controls

| Configuration | Comparison focus |
| --- | --- |
| `graph-bipartite-2x50-50pct-10k.yaml` | Study degree imbalance when two originators connect to fifty beneficiary accounts. |
| `graph-matched-random-control-50pct-10k.yaml` | Compare a random-alert control with the dense graph using matched member and edge counts. |

#### Campaign lifecycle controls

| Configuration | Comparison focus |
| --- | --- |
| `campaign-static-control-f03-50pct-10k.yaml` | Use a fixed mule graph as the baseline for comparisons with evolving campaigns. |
| `campaign-phase-control-f03-50pct-10k.yaml` | Measure phase transitions and generated activity while actor changes, rotations, splits, and merges remain disabled. |
| `campaign-split-only-f03-50pct-10k.yaml` | Isolate end-of-campaign split mutations while merge mutations remain disabled. |
| `campaign-merge-only-f03-50pct-10k.yaml` | Isolate end-of-campaign merge mutations while split mutations remain disabled. |

All twelve controlled experiment configs are in
[`examples/configuration`](../../examples/configuration/). Together with the
ten graph stress configs above, this section now covers 22 runnable examples.

#### Configuration files

Configuration: [examples/configuration/graph-5-hop-short-dwell-50pct-10k.yaml](../../examples/configuration/graph-5-hop-short-dwell-50pct-10k.yaml). Adds a five-edge short-dwell point to the ring-depth comparison.

````{dropdown} Five-edge short-dwell ring

```{literalinclude} ../../examples/configuration/graph-5-hop-short-dwell-50pct-10k.yaml
:language: yaml
```
````

Configuration: [examples/configuration/graph-15-hop-cross-institution-50pct-10k.yaml](../../examples/configuration/graph-15-hop-cross-institution-50pct-10k.yaml). Adds a fifteen-edge cycle across institutions.

````{dropdown} Fifteen-edge cross-institution ring

```{literalinclude} ../../examples/configuration/graph-15-hop-cross-institution-50pct-10k.yaml
:language: yaml
```
````

Configuration: [examples/configuration/graph-20-hop-cross-institution-50pct-10k.yaml](../../examples/configuration/graph-20-hop-cross-institution-50pct-10k.yaml). Adds a twenty-edge cycle across institutions.

````{dropdown} Twenty-edge cross-institution ring

```{literalinclude} ../../examples/configuration/graph-20-hop-cross-institution-50pct-10k.yaml
:language: yaml
```
````

Configuration: [examples/configuration/graph-10-hop-single-institution-50pct-10k.yaml](../../examples/configuration/graph-10-hop-single-institution-50pct-10k.yaml). Pairs with the cross-institution ten-edge ring to compare institution scope.

````{dropdown} Ten-edge single-institution ring

```{literalinclude} ../../examples/configuration/graph-10-hop-single-institution-50pct-10k.yaml
:language: yaml
```
````

Configuration: [examples/configuration/shared-device-hyperedge-off-50pct-10k.yaml](../../examples/configuration/shared-device-hyperedge-off-50pct-10k.yaml). Provides a shared-device control with the structural hyperedge modifier disabled.

````{dropdown} Shared device without hyperedge

```{literalinclude} ../../examples/configuration/shared-device-hyperedge-off-50pct-10k.yaml
:language: yaml
```
````

Configuration: [examples/configuration/shared-ip-hyperedge-off-50pct-10k.yaml](../../examples/configuration/shared-ip-hyperedge-off-50pct-10k.yaml). Provides a shared-IP control with the semantic hyperedge modifier disabled.

````{dropdown} Shared IP without hyperedge

```{literalinclude} ../../examples/configuration/shared-ip-hyperedge-off-50pct-10k.yaml
:language: yaml
```
````

Configuration: [examples/configuration/graph-bipartite-2x50-50pct-10k.yaml](../../examples/configuration/graph-bipartite-2x50-50pct-10k.yaml). Connects two originators to fifty beneficiaries to stress degree imbalance.

````{dropdown} Asymmetric 2 by 50 bipartite graph

```{literalinclude} ../../examples/configuration/graph-bipartite-2x50-50pct-10k.yaml
:language: yaml
```
````

Configuration: [examples/configuration/graph-matched-random-control-50pct-10k.yaml](../../examples/configuration/graph-matched-random-control-50pct-10k.yaml). Uses the same member and edge counts as the dense graph fixture for a topology control.

````{dropdown} Matched random-alert control

```{literalinclude} ../../examples/configuration/graph-matched-random-control-50pct-10k.yaml
:language: yaml
```
````

Configuration: [examples/configuration/campaign-static-control-f03-50pct-10k.yaml](../../examples/configuration/campaign-static-control-f03-50pct-10k.yaml). Keeps the mule graph fixed and disables campaign dynamics.

````{dropdown} Static mule campaign control

```{literalinclude} ../../examples/configuration/campaign-static-control-f03-50pct-10k.yaml
:language: yaml
```
````

Configuration: [examples/configuration/campaign-phase-control-f03-50pct-10k.yaml](../../examples/configuration/campaign-phase-control-f03-50pct-10k.yaml). Generates phase snapshots, transitions, and transition-driven activity while disabling membership and topology mutation limits.

````{dropdown} Campaign phase control

```{literalinclude} ../../examples/configuration/campaign-phase-control-f03-50pct-10k.yaml
:language: yaml
```
````

Configuration: [examples/configuration/campaign-split-only-f03-50pct-10k.yaml](../../examples/configuration/campaign-split-only-f03-50pct-10k.yaml). Enables split mutations and disables merges; mutations are recorded at campaign end.

````{dropdown} Split-only campaign dynamics

```{literalinclude} ../../examples/configuration/campaign-split-only-f03-50pct-10k.yaml
:language: yaml
```
````

Configuration: [examples/configuration/campaign-merge-only-f03-50pct-10k.yaml](../../examples/configuration/campaign-merge-only-f03-50pct-10k.yaml). Enables merge mutations and disables splits; mutations are recorded at campaign end.

````{dropdown} Merge-only campaign dynamics

```{literalinclude} ../../examples/configuration/campaign-merge-only-f03-50pct-10k.yaml
:language: yaml
```
````

All 22 configurations are available under
[`examples/configuration`](../../examples/configuration/). Validate a fixture
before generating it; for example:

```bash
CONFIG=examples/configuration/graph-25-hop-short-dwell-50pct-10k.yaml
poetry run fraudtwin config validate "$CONFIG"
poetry run fraudtwin generate "$CONFIG" --output-dir runs/graph-25-hop-short-dwell-50pct-10k
```

## Next

Continue with [Advanced examples](advanced-examples.md) for data quality,
labels, point-in-time datasets, backtesting, and scale.

## Related

- [Graph and benchmark workflows](../graph-and-benchmarks.md)
- [Benchmark catalog](../benchmark-configs.md)
