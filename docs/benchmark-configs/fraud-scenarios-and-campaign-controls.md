# Fraud scenarios and campaign controls

Use these configurations to compare fraud mechanisms, campaign evolution,
counterfactuals, difficulty, camouflage, and operational label behavior.

| Configuration | Capability | Scenario and target | Rails | Output or analysis focus |
| --- | --- | --- | --- | --- |
| `campaign-dynamics-v1.yaml` | Dynamic campaign evolution | Campaign phases · 0.2% configured target | CARD, PIX, ACCOUNT_TRANSFER | Campaign phases, actor changes, and payment behavior |
| `counterfactual-v1.yaml` | Controlled counterfactual requests | F01/F03/F04 alternatives · 0% baseline | CARD, PIX, ACCOUNT_TRANSFER | Controlled alternatives and resulting outcome comparison |
| `camouflage-v1.yaml` | Feature and relationship camouflage | Camouflaged fraud · 20% configured target | CARD, PIX, ACCOUNT_TRANSFER | Feature similarity and relationship camouflage evaluation |
| `difficulty-v1.yaml` | Detection difficulty progression | Mixed difficult fraud · 100% configured target | CARD, PIX, ACCOUNT_TRANSFER | Detection difficulty, hard negatives, and overlap analysis |
| `card-only-f02-35pct.yaml` | Card-testing attempts | F02 card testing · 35% target | CARD | Card-testing bursts and authorization-control behavior |
| `card-dispute-burst-f03-45pct.yaml` | Dispute-focused takeover activity | F03 dispute burst · 45% target | CARD, ACCOUNT_TRANSFER | Dispute timing, takeover signals, and case workflows |
| `mixed-card-transfer-f01-f03-40pct.yaml` | Mixed card and transfer fraud | F01/F03 mixed campaign · 40% target | CARD, ACCOUNT_TRANSFER | Cross-scenario signals across card and transfer fraud |
| `transfer-only-f05-30pct.yaml` | High-velocity transfer attempts | F05 transfer velocity · 30% target | ACCOUNT_TRANSFER | High-velocity transfer bursts and hard-negative detection |
| `counterfactual-f05-30pct.yaml` | Velocity attack counterfactuals | F05 with alternatives · 30% target | CARD, ACCOUNT_TRANSFER | Velocity attacks, counterfactual sidecars, and comparison |
| `campaign-dynamics-f03-40pct.yaml` | Dynamic mule campaign | F03 evolving campaign · 40% target | CARD, ACCOUNT_TRANSFER | Mule campaign phases and evolving relationship behavior |
| `label-lifecycle-f02-35pct.yaml` | Delayed operational labels | F02 label lifecycle · 35% target | CARD, ACCOUNT_TRANSFER | Delayed alerts, investigations, confirmations, and corrections |
| `temporal-surge-f05-45pct.yaml` | Temporal prevalence shift | F05 regime surge · 45% target | CARD, ACCOUNT_TRANSFER | Regime shifts, prevalence surge, and backtesting behavior |
| `mule-ring-graph-f01-f03-50pct.yaml` | Mule-ring graph structures | F01/F03 mule rings · 50% target | CARD, ACCOUNT_TRANSFER | Closed mule rings and cross-institution provenance analysis |

## Configuration details

Configuration: [configs/benchmarks/campaign-dynamics-v1.yaml](../../configs/benchmarks/campaign-dynamics-v1.yaml). Evolves a coordinated campaign through phases so you can inspect changing actors, relationships, and payment behavior over time.

````{dropdown} Campaign evolution

```{literalinclude} ../../configs/benchmarks/campaign-dynamics-v1.yaml
:language: yaml
```
````

Configuration: [configs/benchmarks/counterfactual-v1.yaml](../../configs/benchmarks/counterfactual-v1.yaml). Requests controlled alternatives to a fraud run so you can compare minimum changes and resulting outcomes.

````{dropdown} Counterfactual requests

```{literalinclude} ../../configs/benchmarks/counterfactual-v1.yaml
:language: yaml
```
````

Configuration: [configs/benchmarks/camouflage-v1.yaml](../../configs/benchmarks/camouflage-v1.yaml). Makes fraudulent activity resemble legitimate behavior while preserving the underlying oracle truth.

````{dropdown} Feature and relation camouflage

```{literalinclude} ../../configs/benchmarks/camouflage-v1.yaml
:language: yaml
```
````

Configuration: [configs/benchmarks/difficulty-v1.yaml](../../configs/benchmarks/difficulty-v1.yaml). Raises measurable detection difficulty while keeping the fraud objective and topology stable.

````{dropdown} Difficulty progression

```{literalinclude} ../../configs/benchmarks/difficulty-v1.yaml
:language: yaml
```
````

Configuration: [examples/configuration/card-only-f02-35pct.yaml](../../examples/configuration/card-only-f02-35pct.yaml). Produces repeated card-testing attempts for studying bursty authorization behavior and card controls.

````{dropdown} Card-testing scenario (F02)

```{literalinclude} ../../examples/configuration/card-only-f02-35pct.yaml
:language: yaml
```
````

Configuration: [examples/configuration/card-dispute-burst-f03-45pct.yaml](../../examples/configuration/card-dispute-burst-f03-45pct.yaml). Combines account-takeover activity with concentrated disputes for workflow and label-timing analysis.

````{dropdown} Card disputes after takeover (F03)

```{literalinclude} ../../examples/configuration/card-dispute-burst-f03-45pct.yaml
:language: yaml
```
````

Configuration: [examples/configuration/mixed-card-transfer-f01-f03-40pct.yaml](../../examples/configuration/mixed-card-transfer-f01-f03-40pct.yaml). Places card-not-present and account-takeover fraud in one world to compare cross-scenario signals.

````{dropdown} Mixed card and transfer fraud (F01/F03)

```{literalinclude} ../../examples/configuration/mixed-card-transfer-f01-f03-40pct.yaml
:language: yaml
```
````

Configuration: [examples/configuration/transfer-only-f05-30pct.yaml](../../examples/configuration/transfer-only-f05-30pct.yaml). Focuses on high-velocity account-transfer attempts for burst detection and hard-negative testing.

````{dropdown} High-velocity transfers (F05)

```{literalinclude} ../../examples/configuration/transfer-only-f05-30pct.yaml
:language: yaml
```
````

Configuration: [examples/configuration/counterfactual-f05-30pct.yaml](../../examples/configuration/counterfactual-f05-30pct.yaml). Generates a velocity-attack baseline together with controlled counterfactual sidecars.

````{dropdown} Velocity attack with counterfactuals (F05)

```{literalinclude} ../../examples/configuration/counterfactual-f05-30pct.yaml
:language: yaml
```
````

Configuration: [examples/configuration/campaign-dynamics-f03-40pct.yaml](../../examples/configuration/campaign-dynamics-f03-40pct.yaml). Evolves a mule campaign across setup, transfer, and cash-out phases.

````{dropdown} Dynamic mule campaign (F03)

```{literalinclude} ../../examples/configuration/campaign-dynamics-f03-40pct.yaml
:language: yaml
```
````

Configuration: [examples/configuration/label-lifecycle-f02-35pct.yaml](../../examples/configuration/label-lifecycle-f02-35pct.yaml). Delays alerts, investigations, confirmations, and corrections to model incomplete labels.

````{dropdown} Delayed label lifecycle (F02)

```{literalinclude} ../../examples/configuration/label-lifecycle-f02-35pct.yaml
:language: yaml
```
````

Configuration: [examples/configuration/temporal-surge-f05-45pct.yaml](../../examples/configuration/temporal-surge-f05-45pct.yaml). Creates a quiet period followed by a prevalence surge for temporal-shift and backtesting experiments.

````{dropdown} Temporal fraud surge (F05)

```{literalinclude} ../../examples/configuration/temporal-surge-f05-45pct.yaml
:language: yaml
```
````

Configuration: [examples/configuration/mule-ring-graph-f01-f03-50pct.yaml](../../examples/configuration/mule-ring-graph-f01-f03-50pct.yaml). Builds closed mule rings and cross-institution paths for relationship and provenance analysis.

````{dropdown} Mule rings and networks (F01/F03)

```{literalinclude} ../../examples/configuration/mule-ring-graph-f01-f03-50pct.yaml
:language: yaml
```
````

## Next

Continue with [Graph and relationship structures](graph-and-relationship-structures.md).

## Related

- [Configuration](../configuration.md)
- [Benchmark catalog](../benchmark-configs.md)
