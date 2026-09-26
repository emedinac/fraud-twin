# Benchmarks and configuration examples

**Level:** Beginner to Intermediate<br><br>
**Before you start:** [Quickstart](quickstart.md) and the [Configuration](configuration.md) guide.<br><br>
**You will:** choose a benchmark or example configuration by capability,
inspect its complete YAML, and run it manually when you are ready.<br><br>
**Services:** None for local benchmark files; service-backed examples require
Docker or external integrations.

FraudTwin ships with a versioned library of benchmark and example
configurations under `configs/benchmarks/` and `examples/configuration/`.
The category pages below follow the same expandable navigation pattern as the
[Python API reference](api.rst): each category has its own page and appears as
a child entry in the documentation sidebar.

## Category index

Use the category that matches the behavior you want to inspect:

| Category | Best for |
| --- | --- |
| [Minimal and baseline](benchmark-configs/minimal-and-baseline.md) | First valid runs, temporal windows, and calibration baselines |
| [Fraud scenarios and campaign controls](benchmark-configs/fraud-scenarios-and-campaign-controls.md) | Fraud mechanisms, campaigns, counterfactuals, camouflage, and labels |
| [Graph and relationship structures](benchmark-configs/graph-and-relationship-structures.md) | Graph patterns, stress configs, controlled experiments, and observable/oracle exports |
| [Advanced examples](benchmark-configs/advanced-examples.md) | Data quality, labels, PIT, backtesting, and scale configurations |
| [Integration and streaming](benchmark-configs/integration-and-streaming.md) | Kafka and contract-backed output |

```{toctree}
:maxdepth: 1

benchmark-configs/minimal-and-baseline
benchmark-configs/fraud-scenarios-and-campaign-controls
benchmark-configs/graph-and-relationship-structures
benchmark-configs/advanced-examples
benchmark-configs/integration-and-streaming
```

The category pages include the source YAML with `literalinclude`, so the
documentation stays synchronized with the repository configuration files.

## How to choose a fixture

1. Start with [Minimal and baseline](benchmark-configs/minimal-and-baseline.md)
   when you want the cleanest example.
2. Choose a category-specific configuration when the investigation is about
   one concern, such as graph structure, quality faults, or temporal evolution.
3. Copy the file into a project-owned configuration before editing it.
4. Keep benchmark fixtures immutable; treat the versioned files as reference
   examples, not editing targets.

## Quick commands

```bash
# Validate a configuration
poetry run fraudtwin config validate path/to/config.yaml

# Generate it manually after validation
poetry run fraudtwin generate path/to/config.yaml --output-dir runs/example
```

No experiment is run by the documentation build; generation is always an
explicit command that you execute yourself.

## Next

Continue with [Graph and benchmark workflows](graph-and-benchmarks.md).

## Related documents

- [Configuration](configuration.md)
- [Graph and benchmark workflows](graph-and-benchmarks.md)
- [Configuration reference](configuration-reference.rst)
- [Kafka reliability](kafka-reliability.md)

The canonical benchmark index is also kept in `configs/benchmarks/README.md` for
quick browsing from source checkouts.
