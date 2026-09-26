# Quickstart

**Level:** Beginner<br><br>
**You will:** install or enter a FraudTwin environment, generate a small run,<br><br>
and locate its main artifacts.
**Before you start:** [Installation](installation.md).<br><br>
**Services:** None.<br><br>

This walkthrough creates a small payment world locally. You can use the
packaged template in an installed project or the tracked fixture in a source
checkout. The result is quick to generate and easy to inspect.

## Requirements

- Python 3.12
- Poetry 2.x

Install the project from the repository root:

```bash
poetry install
```

## Create a project-owned configuration

From an installed package, create a visible configuration file before changing
simulation settings:

```bash
fraudtwin config init config.yaml
fraudtwin config validate config.yaml
```

The command refuses to overwrite an existing file unless `--force` is supplied.
From a repository checkout, `configs/minimal.yaml` is the equivalent tracked
fixture.

## Generate a first run

Validate the configuration before generating data:

```bash
poetry run fraudtwin config validate configs/minimal.yaml
poetry run fraudtwin generate configs/minimal.yaml
```

The command prints a run ID and the output location. To keep generated files in
the ignored local run area, choose an output directory explicitly:

```bash
RUNS_DIR=./runs

poetry run fraudtwin generate configs/minimal.yaml --output-dir "$RUNS_DIR"
```

The minimal configuration creates 10 customers, 10 behavior profiles, 100 target payments, and the lifecycle events those payments require. Fraud is off by default, which makes the first run a clean baseline.

## Use the Python API

The same run can be generated in memory from Python:

```python
from pathlib import Path

import fraudtwin
from fraudtwin.config import load_config

config = load_config(Path("configs/minimal.yaml"))
data = fraudtwin.generate(config)

print(data.run_id, len(data.behavior.payments))
```

Use the CLI when you want the standard Parquet and manifest layout. Use the
Python API when you want typed records directly in a notebook or application.

When settings depend on runtime values, start from the packaged defaults instead
of reading an internal YAML resource:

```python
from datetime import datetime, timezone

import fraudtwin
from fraudtwin.config import SimulationRunConfig

config = fraudtwin.load_default_config()
values = config.model_dump(mode="python")
values["simulation"].update(
    start=datetime(2026, 1, 1, tzinfo=timezone.utc),
    duration_days=30,
)
config = SimulationRunConfig.model_validate(values)
data = fraudtwin.generate(config)
```

## What a run contains

Each run is stored under `runs/<run_id>/` (or the directory supplied with `--output-dir`):

```text
runs/<run_id>/
├── manifest.json
├── entities/*.parquet
├── behavior/behavior_profiles.parquet
├── payments/{payments,payment_events}.parquet
├── ledger/ledger_entries.parquet
├── fraud/{fraud_records,fraud_alerts,fraud_cases,fraud_labels}.parquet
└── ml/
    ├── dataset.parquet
    └── dataset_manifest.json
```

The manifest records the seed, configuration, schemas, counts, fingerprints, and quality diagnostics needed to explain or reproduce the run. Benchmark runs may also contain `oracle/`, graph exports, and backtest folds.

## Turn on fraud

Use a benchmark fixture when you want a complete, repeatable example:

```bash
CONFIG=configs/benchmarks/difficulty-v1.yaml
RUNS_DIR=./runs

poetry run fraudtwin config validate "$CONFIG"
poetry run fraudtwin generate "$CONFIG" --output-dir "$RUNS_DIR"
```

For a smaller custom run, set `fraud.enabled: true` in a copied YAML file. The
generator supports five built-in stories:

- `F01` Card Not Present: three card payments per campaign.
- `F02` Card Testing: repeated low-value card attempts, controlled by
  `attempt_count`.
- `F03` Account Takeover: two account-transfer payments per campaign.
- `F04` Instant-Payment Scam: one PIX payment per campaign.
- `F05` Velocity Attack: repeated card attempts, controlled by
  `attempt_count` and `window_seconds`.

Card scenarios require active cards. F03 requires account relationships, and
F04 requires PIX-capable accounts and PIX keys. Capacity identifiers such as
`C04` (ledger debit capacity) explain generation failures after configuration
validation; see the [vocabulary reference](vocabulary.md). `fraud.target_rate` is a
campaign budget, not a guarantee that the same percentage of final payment
rows will be fraudulent; see the worked
[fraud prevalence examples](configuration.md#choosing-a-target-fraud-prevalence).
When customizing `fraud.scenarios`, remember that a partial mapping inherits
the other built-in scenarios. Disable unwanted scenario IDs explicitly when
you want an F04-only or otherwise restricted run.

## Next

- Adjust behavior and payment settings in [Configuration](configuration.md).
- Build a historical ML table or replay a run with [Workflows](workflows.md).
- Export graph views and compare stress levels with [Graph and benchmark workflows](graph-and-benchmarks.md).

## Related

- [Choose your path](learning-paths.md)
- [Data contracts](data-contracts.rst)
- [Troubleshooting](troubleshooting.md)
