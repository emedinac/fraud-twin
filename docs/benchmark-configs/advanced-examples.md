# Advanced examples

Use this category for bounded examples that exercise data quality, labels,
point-in-time datasets, backtesting, and scale. The examples on this page set
`fraud.target_rate: 0.25` and use card and account-transfer traffic. The target
rate controls the campaign target; it does not guarantee the same share of
final payment rows.

Graph topologies, graph stress configs, and controlled graph experiments are
collected in [Graph and relationship structures](graph-and-relationship-structures.md).

## Data quality and schema evolution

| Configuration | Capability | Scenario and target | Rails | Output or analysis focus |
| --- | --- | --- | --- | --- |
| `quality-outage-schema-25pct-10k.yaml` | Outages, late events, duplicates, and schema changes | F05 · 25% | CARD, ACCOUNT_TRANSFER | Practice outage repair, event replay, duplicate handling, and schema compatibility workflows |

### Configuration details

Configuration: [examples/configuration/quality-outage-schema-25pct-10k.yaml](../../examples/configuration/quality-outage-schema-25pct-10k.yaml). Combines outages, late events, duplicates, and a scheduled schema change for repair workflows.

````{dropdown} Quality faults and schema evolution

```{literalinclude} ../../examples/configuration/quality-outage-schema-25pct-10k.yaml
:language: yaml
```
````

## Labels, PIT datasets, backtesting, and scale

| Configuration | Capability | Scenario and target | Rails | Output or analysis focus |
| --- | --- | --- | --- | --- |
| `labels-scale-backtest-25pct-10k.yaml` | Conditional labels, PIT splits, regime shifts, and bounded scale | F05 · 25% | CARD, ACCOUNT_TRANSFER | Study label maturity, point-in-time splits, rolling backtests, and checkpoint behavior |

### Configuration details

Configuration: [examples/configuration/labels-scale-backtest-25pct-10k.yaml](../../examples/configuration/labels-scale-backtest-25pct-10k.yaml). Combines conditional labels, point-in-time splits, regime shifts, and checkpoint controls.

````{dropdown} Labels, PIT backtesting, and bounded scale

```{literalinclude} ../../examples/configuration/labels-scale-backtest-25pct-10k.yaml
:language: yaml
```
````

To run one of these examples manually, validate it first and then generate it
into a run-specific directory:

```bash
CONFIG=examples/configuration/quality-outage-schema-25pct-10k.yaml
poetry run fraudtwin config validate "$CONFIG"
poetry run fraudtwin generate "$CONFIG" --output-dir runs/quality-outage-schema-25pct-10k
```

Replace `CONFIG` and the output directory with the matching filename for any
other advanced example. Graph fixtures are documented in the graph category.
These commands are intentionally manual; the documentation build does not
generate experiment data.

## Next

Continue with [Integration and streaming](integration-and-streaming.md).

## Related

- [Graph and benchmark workflows](../graph-and-benchmarks.md)
- [Benchmark catalog](../benchmark-configs.md)
