# Minimal and baseline

Use these configurations to start with a small valid world or explore
time-split and reference-driven baselines.

| Configuration | Capability | Scenario and target | Rails | Output or analysis focus |
| --- | --- | --- | --- | --- |
| `minimal.yaml` | Small valid baseline world | Clean baseline · 0% configured fraud | CARD | Installation, manifests, and basic Parquet output |
| `temporal-v1.yaml` | Chronological evaluation windows | Backtesting definition · no fraud target | Source configuration rails | Time-split metrics and label-maturity evaluation |
| `calibration-v1.yaml` | Reference calibration profile | Calibration baseline · 0.2% configured target | CARD, PIX, ACCOUNT_TRANSFER | Amount, timing, and population calibration checks |

## Configuration details

Configuration: [configs/minimal.yaml](../../configs/minimal.yaml). Creates the smallest clean payment world for checking installation, manifests, and basic Parquet output.

````{dropdown} Clean baseline

```{literalinclude} ../../configs/minimal.yaml
:language: yaml
```
````

Configuration: [configs/benchmarks/temporal-v1.yaml](../../configs/benchmarks/temporal-v1.yaml). Adds explicit time splits and bounded history for replay and rolling-window evaluation.

````{dropdown} Temporal windows

```{literalinclude} ../../configs/benchmarks/temporal-v1.yaml
:language: yaml
```
````

Configuration: [configs/benchmarks/calibration-v1.yaml](../../configs/benchmarks/calibration-v1.yaml). Shows how a deterministic reference profile can shape amounts, timing, and population behavior.

````{dropdown} Reference calibration

```{literalinclude} ../../configs/benchmarks/calibration-v1.yaml
:language: yaml
```
````

## Next

Continue with [Fraud scenarios and campaign controls](fraud-scenarios-and-campaign-controls.md).

## Related

- [Configuration](../configuration.md)
- [Benchmark catalog](../benchmark-configs.md)
