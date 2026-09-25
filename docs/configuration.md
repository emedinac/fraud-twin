# Configuration

**Level:** Beginner to Intermediate<br><br>
**You will:** change the simulator safely, starting with a minimal YAML file<br><br>
and progressing to evaluation, quality, scale, and integrations.
**Before you start:** the [Quickstart](quickstart.md).<br><br>
**Services:** None for configuration; enabled integrations may require Docker<br><br>
or external services.

FraudTwin treats configuration as part of the run’s identity. A validated YAML file determines the population, behavior, event timing, fraud campaigns, and output policy. Together with the seed, it gives the run a stable fingerprint.

## Choose your configuration workflow

There are several useful ways to work with configuration. Choose the one that
matches how much control and repeatability you need.

### Use the built-in defaults

For a quick installation check or a first experiment, let the Python API use
the packaged minimal configuration:

```python
import fraudtwin

run = fraudtwin.generate(write=True, output_dir="runs")
print(run.run_id, run.manifest_path)
```

This is convenient, but the configuration is implicit. Use an explicit file
when you want another person—or your future self—to see the choices directly.

### Create a project-owned YAML file

This is the recommended workflow for experiments, notebooks, CI jobs, and
team-owned simulation scenarios:

```console
fraudtwin config init config.yaml
fraudtwin config validate config.yaml
fraudtwin generate config.yaml --output-dir runs
```

`config init` copies the supported minimal template into your project. It does
not overwrite an existing file unless you explicitly pass `--force`.

### Load an existing YAML file from Python

Use `load_config()` when the configuration already belongs to your project:

```python
from pathlib import Path

import fraudtwin
from fraudtwin.config import load_config

config = load_config(Path("config.yaml"))
data = fraudtwin.generate(config)
print(data.run_id, len(data.behavior.payments))
```

The loader parses YAML, resolves supported relative paths, and validates the
complete typed configuration before generation.

### Start from built-in defaults and customize in Python

Use `load_default_config()` when values are calculated dynamically—for example,
when a run should cover the five years before today:

```python
from calendar import monthrange
from datetime import datetime, timezone

import fraudtwin
from fraudtwin.config import SimulationRunConfig


def years_before(moment: datetime, years: int) -> datetime:
    year = moment.year - years
    day = min(moment.day, monthrange(year, moment.month)[1])
    return moment.replace(year=year, day=day)


config = fraudtwin.load_default_config()
values = config.model_dump(mode="python")
end = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
start = years_before(end, 5)
values["simulation"].update(start=start, duration_days=(end - start).days)
values["payments"]["daily_target"] = 500
config = SimulationRunConfig.model_validate(values)

data = fraudtwin.generate(config)
```

Use fixed dates when you need the same run identity over time. A rolling
`datetime.now()` window changes the validated configuration and therefore the
run hash whenever the date changes.

### Use repository fixtures

The repository contains `configs/minimal.yaml` and versioned files under
`configs/benchmarks/`. These are useful when working from a source checkout or
running the project’s tests. They are not the recommended discovery mechanism
for an installed package; use `fraudtwin config init` instead.

Do not import `fraudtwin/defaults/minimal.yaml` directly. That is a packaged
implementation resource. The supported Python entry point is
`fraudtwin.load_default_config()`.

## A complete annotated configuration

The following is a complete, valid clean-baseline file. Comments explain the
intent of each setting; YAML comments do not affect validation, configuration
hashes, or generated records.

```yaml
# Controls the simulation clock and reproducibility.
simulation:
  seed: 42
  start: 2026-01-01T00:00:00Z
  duration_days: 30
  speed: batch

# Entity pools used by the generated payment world.
population:
  customers: 100
  institutions: 5
  accounts: 150
  cards: 100
  merchants: 25
  devices: 100
  pix_keys: 80

# Target payment volume and the relative mix of payment rails.
payments:
  daily_target: 250
  rails:
    CARD: 0.50
    PIX: 0.30
    ACCOUNT_TRANSFER: 0.20

# Legitimate customer payment behavior.
behavior:
  amount_min: 1.00
  amount_max: 5000.00

# Fraud is disabled for a clean baseline.
fraud:
  enabled: false
  target_rate: 0.002

# Keep the source clean unless you are intentionally testing data defects.
quality:
  profile: clean

# Parquet is the local audit output. Other sinks are opt-in.
outputs:
  parquet: true
  postgres: false
  kafka: false
  iceberg: false
```

The file includes the required top-level sections while relying on validated
defaults for lifecycle timing, fraud workflow, labels, scale, graph, and other
advanced settings. A YAML fragment such as `payments: ...` is useful when
describing an override, but it is not necessarily a complete configuration
file. YAML is the recommended format because it supports comments and readable
multi-line sections. JSON-shaped content is accepted by the YAML parser, but it
does not provide the same explanation and editing experience.

## Configuration at a glance

| Section | Purpose |
| --- | --- |
| `simulation` | Seed, time window, and execution mode |
| `population` | Counts for customers, accounts, cards, merchants, devices, and institutions |
| `payments` | Daily volume and payment-rail weights |
| `behavior` | Customer spending, timing, merchant, and device preferences |
| `card_lifecycle` | Card initiation, authorization, capture, clearing, settlement, refunds, and reversals |
| `pix_lifecycle` | Pix-like validation, authorization, settlement, rejection, timeout, and return timing |
| `fraud` | Campaign selection, scenario weights, and hard negatives |
| `fraud_workflow` | Alerts, cases, confirmations, disputes, and label delays |
| `labels` | Opt-in deterministic label observation, missingness, corrections, and reopenings |
| `scale` | Opt-in large-run profiles, stable shards, chunks, workers, and checkpoints |
| `quality` | Deterministic typed faults, delivery defects, outages, schema changes, and spikes |
| `dataset` | Point-in-time feature and label construction |
| `backtest` | Rolling train, validation, test, and stress windows |
| `graph` | Opt-in fraud-network campaigns and graph export inputs |
| `benchmark` / `stress` | Difficulty and camouflage controls |
| `counterfactual` | Opt-in minimum-change fraud trajectories and lineage sidecars |
| `calibration` | Opt-in reference-derived aggregate parameters and fidelity controls |
| `outputs` | Parquet and optional PostgreSQL, Kafka, and Iceberg outputs |
| `lakehouse` | Iceberg namespace and isolated-oracle publication controls; service credentials remain environment-only |

Unknown fields and invalid ranges are rejected during validation. That strict boundary is intentional: a run should fail before it produces ambiguous data.

## Configuration fields by question

### “What time period and random sequence should this run use?”

Use `simulation.seed` for deterministic random streams, `simulation.start` for
the timezone-aware beginning of the source window, `simulation.duration_days`
for its length, and `simulation.speed` for batch or accelerated execution.
Fixed dates and a fixed seed are the easiest way to make an experiment
reproducible.

### “How large is the payment world?”

`population` controls customers, institutions, accounts, cards, merchants,
devices, and PIX keys. The relationships must be possible: accounts need
customers and institutions, cards need accounts, merchants need institutions,
and PIX keys need accounts, customers, and institutions. Set `cards: 0` when
the scenario is intentionally limited to PIX and account transfers.

### “How many payments should be generated each day?”

`payments.daily_target` sets the target daily volume. `payments.rails` assigns
the payment-rail mix. Every weight must be non-negative and the weights must
sum to exactly `1.0`:

```yaml
payments:
  daily_target: 600
  rails:
    CARD: 0.00
    PIX: 0.75
    ACCOUNT_TRANSFER: 0.25
```

### “What does normal customer behavior look like?”

`behavior.amount_min` and `behavior.amount_max` bound legitimate amounts.
`active_hours`, `weekday_weights`, `merchant_preference_count`, and
`preferred_device_limit` shape when and where customers normally pay. These
controls are useful for creating realistic hard negatives instead of making
all customers behave identically.

### “Should this run contain fraud?”

`fraud.enabled` turns fraud generation on or off. Fraud is generated as a
bounded set of campaigns, not by directly changing a percentage of payment
rows. For a standard run (with the advanced difficulty controls disabled), the
campaign count is:

```text
campaign_count = min(
    scenario_count,
    floor(baseline_payment_count * target_rate),
    enabled_scenario_capacity,
)
```

Here, `enabled_scenario_capacity` is the sum of `scenarios.<id>.count` for
enabled scenarios with a positive weight. `target_rate` therefore limits the
number of campaigns relative to the baseline; it does not guarantee that the
same percentage of final payment rows are fraudulent. `scenario_count` is a
maximum campaign count, not the number of scenario types. Each scenario can
create a different number of payments, and `scenarios.<id>.count` limits how
many campaigns may use that scenario.

`hard_negative_rate` adds legitimate lookalike records alongside campaigns.
Those records are useful for testing false positives, but they do not increase
true fraud prevalence. Difficulty and camouflage settings can further modify
the effective campaign plan, so inspect the manifest when exact realized
counts matter.

The source fraud records, observed labels, and dataset rows are different
measurements. A source record has `fraud_truth`; labels may be delayed, missing,
or corrected; and a point-in-time dataset may include only labels available at
the feature timestamp.

### “When does fraud become observable?”

`fraud_workflow` controls alerts, cases, confirmations, disputes, and delays.
`labels` models selection-dependent investigation and corrections. Source fraud
generation and observed labels are separate: a fraud event may exist in oracle
truth before it is available to a detector or dataset.

### “How long should payment lifecycles take?”

`card_lifecycle` and `pix_lifecycle` control authorization, rejection,
settlement, reversal, refund, timeout, and return timing. Lifecycle delays must
fit inside the simulation window; a one-day simulation cannot contain a
multi-day lifecycle.

### “Do I need a point-in-time ML dataset?”

Set `dataset.enabled: true` when you want features and labels built as of their
historical availability time. Set it to `false` for an event-and-ledger run
where dataset materialization is unnecessary. `dataset.unresolved_labels`
controls how immature labels are handled.

### “Do I want clean data or deliberate defects?”

`quality.profile: clean` is the baseline. `realistic` and `hostile` apply
deterministic duplicates, delays, schema faults, malformed values, outages, or
other defects. Keep a clean source run when you need a reconciliation baseline.

### “Do I need external outputs?”

`outputs.parquet` is the local audit output. PostgreSQL, Kafka, and Iceberg are
optional projections that require their corresponding extras, credentials, and
healthy services. Keep Parquet enabled when using an external sink so the
source records remain inspectable.

### “What are the advanced controls for?”

`scale`, `graph`, `benchmark`, `stress`, `counterfactual`, `campaign_dynamics`,
and `calibration` are opt-in controls. Start with the clean baseline and add
one advanced section at a time. The generated [configuration parameter
reference](configuration-reference.rst) remains the exhaustive field-level
reference.

## Label observation

Label observation is disabled by default and therefore does not change legacy run bytes or identities. Enable it to model selection-dependent discovery and versioned labels:

```yaml
labels:
  enabled: true
  investigation_rate: 0.70
  missing_fraud_rate: 0.05
  preliminary_error_rate: 0.02
  correction_rate: 0.01
  confirmation_delay: lognormal
```

Enabled runs write append-only `label_observations/<id>/observable/observed_labels.parquet` and the oracle-only `label_history.parquet`. PIT rows resolve the greatest label version whose `label_available_at` is no later than `prediction_time`; immature or missing labels follow `dataset.unresolved_labels`. `fraud_workflow` continues to provide alert and case timing, while `labels.investigation_rate` is the sole enabled-run selection control.

## Large-scale generation

Scale generation reuses the canonical simulator and is disabled by default. The
primary target is canonical payment rows; lifecycle, ledger, fraud, and
workflow rows are reported separately. Profiles target 100k, 1M, 10M, 100M,
and 1B payments. The `dev` profile is a bounded 1,000-payment smoke profile
for laptops. Stable shard IDs and hierarchical seed streams make IDs,
timestamps, scenario semantics, financial state, schemas, and partition
fingerprints independent of worker scheduling. Chunked Parquet output is
bounded by `chunk_size` and `output_batch_size`; completed chunks are recorded
in an atomic checkpoint manifest.

```yaml
scale:
  profile: dev
  target_payments: 1000
  shard_count: 4
  chunk_size: 1000
  worker_count: 2
  output_batch_size: 1000
  checkpoint_frequency_chunks: 1
  partition_mapping: stable_hash_v1
```

Run and resume a scale profile with:

```bash
CONFIG=configs/scale-1b.yaml
CHECKPOINT_DIR=./runs/scale-1b-checkpoint

fraudtwin generate "$CONFIG" --workers 16 --checkpoint-dir "$CHECKPOINT_DIR"
fraudtwin resume "$CHECKPOINT_DIR"
```

The billion profile is a published benchmark target for suitable documented
hardware; it does not require Kafka, Spark, or cloud infrastructure merely to
demonstrate generator scalability. A completed run records target and
realized payment counts, partition checksums, canonical fingerprints, and
cross-partition reconciliation results. Use `poetry install -E scale` for the
optional PyArrow/DuckDB out-of-core tooling.

The compatibility API still materializes canonical entity/behavior objects;
use `dev` on a laptop. The core scale feature set streams payment, lifecycle,
and ledger rows through bounded batches, while advanced fraud/graph/quality/PIT
features retain the compatibility materialization boundary. Production
100M/1B executions still require the remaining out-of-core stages and hardware
evidence.

The quality benchmark provides `standard-v1-dev` for running the quality protocol against
the 1,000-payment smoke workload without launching a large benchmark.

## Reference calibration

Calibration is disabled by default and does not change legacy hashes or output
bytes. Fit a reusable profile from a canonical Parquet transaction table:

```bash
fraudtwin calibrate reference.parquet --output calibrated-profile.yaml
fraudtwin generate configs/minimal.yaml --profile calibrated-profile.yaml --seed 42
```

The reference must contain finite positive `amount`, timezone-aware UTC
`event_time`, and non-empty `customer_id` columns. Optional canonical columns
are `merchant_category`, `account_balance`, `payer_account_id`,
`payee_account_id`, and `campaign_id`. Missing optional columns are recorded as
unavailable; they are never inferred from arbitrary fields.

Profiles contain aggregate distributions, frequencies, dependencies, bounded
graph/campaign summaries, fingerprints, versions, and seed-stream metadata.
They never contain reference rows or source entity identifiers. Generated runs
store append-only `calibration/<profile_id>/fidelity_metrics.parquet` and
`fidelity_report.json` sidecars. Fidelity is report-only unless explicit
thresholds are configured. Existing lifecycle, ledger, graph, fraud, PIT, and
observable/oracle boundaries remain authoritative.

## Customer behavior

Behavior profiles make legitimate activity customer-specific. The controls below shape amounts, active hours, weekday patterns, merchant preferences, and trusted devices:

```yaml
behavior:
  amount_min: 1.00
  amount_max: 5000.00
  active_hours: [8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21]
  weekday_weights: [1.0, 1.0, 1.0, 1.0, 1.0, 0.85, 0.70]
  merchant_preference_count: 3
  preferred_device_limit: 3
```

The same payment can be ordinary for one customer and unusual for another. That distinction is the foundation for hard negatives and behavior-aware fraud cases.

## Counterfactual fraud generation

Counterfactual generation is disabled by default and has no effect on legacy hashes or output files.
Enable it with explicit objective requests:

```yaml
counterfactual:
  enabled: true
  budget: 2.0
  requests:
    - {objective: F01, count: 1}
    - {objective: F04, count: 1}
```

The resolver applies request, objective, family, global, and default controls
in that order. `scenario` is accepted as an alias for `objective`, and
`max_distance` for `budget`; supplying both names is rejected. Dimension costs
cover beneficiary, device, timing, amount, merchant, geography, payment rail,
and graph relationships. The built-in weighted distance can be replaced by a
registered deterministic distance function.

Sources are selected chronologically from the pre-fraud legitimate stream.
Eligibility uses only records available at the source decision timestamp, and
stable IDs make repeated runs identical. The generator searches for a feasible minimum
change, applies the configured amount/timing constraints, and uses isolated
camouflage controls at the generation boundary. Every request records its budget,
costs, changed fields, objective result, constraints, source mapping, and either
a modified trajectory or a deterministic rejection.

Generated originals and modifications live in separate append-only sidecars;
ordinary payment/event schemas remain unchanged. Observable sidecars omit latent
fraud truth, while oracle change sets retain it for evaluation. A standalone
workflow is available for legitimate-only source runs:

```bash
fraudtwin counterfactual generate \
  --config configs/benchmarks/counterfactual-v1.yaml \
  --source-run-id RUN-... \
  --output-dir ./runs
```

Rail conversion is reported as inapplicable when changing rails would require
an unsafe lifecycle-vocabulary conversion. The graph adapter emits a closed
campaign/pattern lineage for each selected trajectory; requests requiring
multi-trajectory campaign evolution are recorded as limited. F02/F05 burst
objectives remain bounded to the selected trajectory and are never silently
expanded into additional payments.

## Fraud scenarios and workflow

FraudTwin's built-in fraud scenarios are small, named stories about how a
payment can become suspicious. You select those stories under
`fraud.scenarios`; you do not invent a new ID such as `F06` in a YAML file.
The accepted IDs are `F01` through `F05`.

Enable fraud explicitly and keep scenario controls visible in the file:

```yaml
fraud:
  enabled: true
  # This is a campaign budget relative to baseline payments, not a final
  # percentage of payment rows.
  target_rate: 0.20
  # Maximum number of campaigns across the enabled scenario types.
  scenario_count: 5
  # Add legitimate lookalikes for false-positive testing.
  hard_negative_rate: 1.0
  scenarios:
    F01: {enabled: true, weight: 1.0, count: 1}
    F02: {enabled: true, weight: 1.0, count: 1, attempt_count: 20}
    F03: {enabled: true, weight: 1.0, count: 1}
    F04: {enabled: true, weight: 1.0, count: 1}
    F05: {enabled: true, weight: 1.0, count: 1, attempt_count: 20, window_seconds: 60}
```

### What each F scenario means

The table below is the practical guide: start with the business story you want
to test, then check the entities and payment shape that story needs.

| ID | Human meaning | What it uses | Nominal payments per campaign |
| --- | --- | --- | ---: |
| `F01` | Card-not-present activity on a new device | An active card, device, merchant, and card lifecycle | 3 |
| `F02` | Card testing: a low-value authorization burst | An active card, merchant, and card lifecycle | `attempt_count` (20 by default) |
| `F03` | Account takeover followed by beneficiary activity | Customers, accounts, institutions, devices, and account transfers | 2 transfers |
| `F04` | Instant-payment scam using a new beneficiary | PIX-capable accounts, customers, institutions, and PIX keys | 1 PIX payment |
| `F05` | Card payment velocity attack | An active card, merchant, and card lifecycle | `attempt_count` (20 by default) |

These are campaign shapes, not guaranteed row counts. A card scenario cannot
create a payment when there are no usable cards. F04 cannot create a payment
when there are no eligible PIX keys or accounts. F03 and F04 can also be
limited by account relationships or ledger capacity. In those cases the run
reports the failure or the campaign produces no payment; it does not silently
turn into another scenario.

The controls have simple meanings:

- `enabled` decides whether a scenario is eligible at all.
- `weight` decides how often an eligible scenario is selected relative to the
  other eligible scenarios. It is not a fraud percentage.
- `count` is the maximum number of campaigns for that scenario.
- `attempt_count` controls the repeated attempts in F02 and F05. It is not
  used to turn F03 into more transfers or F04 into more PIX payments.
- `window_seconds` keeps burst-style attempts within a time window, especially
  for F05.
- `amount_min` and `amount_max` override the normal behavior bounds for that
  scenario. If omitted, the scenario inherits `behavior.amount_min` and
  `behavior.amount_max`.

For example, this is a small account-transfer and PIX experiment. It is a
mergeable section, not a complete configuration file:

```yaml
fraud:
  enabled: true
  target_rate: 0.05       # Campaign budget, not a final fraud-row percentage.
  scenario_count: 20      # At most 20 campaigns in this run.
  hard_negative_rate: 0.0 # Keep the first run free of extra lookalikes.
  scenarios:
    F01: {enabled: false, count: 0} # Requires cards; unused here.
    F02: {enabled: false, count: 0} # Requires cards; unused here.
    F03:
      enabled: true
      weight: 1.0
      count: 10
      amount_min: 1.00
      amount_max: 100.00
    F04:
      enabled: true
      weight: 1.0
      count: 10
      amount_min: 1.00
      amount_max: 100.00
    F05: {enabled: false, count: 0} # Requires cards; unused here.
```

For a card-testing experiment, use a configuration with cards and make the
burst explicit:

```yaml
# Merge into the fraud section of a configuration that has active cards.
fraud:
  enabled: true
  target_rate: 0.05
  scenario_count: 2
  hard_negative_rate: 0.0
  scenarios:
    F01: {enabled: false, count: 0}
    F02:
      enabled: true
      count: 2          # Two F02 campaigns.
      attempt_count: 8  # Eight authorization attempts per campaign.
      window_seconds: 60
      amount_min: 1.00
      amount_max: 10.00
    F03: {enabled: false, count: 0}
    F04: {enabled: false, count: 0}
    F05: {enabled: false, count: 0}
```

### Where the F names come from

The F identifiers are part of the typed configuration schema and the built-in
fraud generator. The authoritative references are the
[`FraudScenarioId` and `FraudScenarioSettings` definitions](https://github.com/emedinac/fraudtwin/blob/main/src/fraudtwin/config.py)
and the [scenario dispatch and payment-shape implementation](https://github.com/emedinac/fraudtwin/blob/main/src/fraudtwin/simulation/fraud.py).
The generated [configuration reference](configuration-reference.rst) documents
the field types and allowed values.

### How partial scenario maps are interpreted

The `scenarios` mapping is currently an overlay on FraudTwin's built-in
scenario settings. This means that omitting the mapping and providing a partial
mapping have different consequences:

- If `scenarios` is omitted, the five built-in scenarios receive their default
  settings.
- If `scenarios` contains only one scenario, the other scenarios still receive
  their defaults. They are not automatically disabled.
- If a scenario is present but one of its fields is omitted, that field receives
  its normal default. For example, `weight` defaults to `1.0`.

For example, this does **not** mean “F04 only”:

```yaml
# The other built-in scenarios are still present with their defaults.
scenarios:
  F04:
    enabled: true
    count: 5478
    amount_min: 1.0
    amount_max: 100.0
```

To select only F04, explicitly disable every other scenario:

```yaml
# Merge into the fraud section of an existing configuration.
scenarios:
  F01: {enabled: false, count: 0}
  F02: {enabled: false, count: 0}
  F03: {enabled: false, count: 0}
  F04:
    enabled: true
    count: 5478
    amount_min: 1.0
    amount_max: 100.0
  F05: {enabled: false, count: 0}
```

An unwanted scenario is excluded when it is disabled or has either `count: 0`
or `weight: 0.0`. This explicit form makes the intended scenario population
visible and avoids accidentally consuming campaign capacity with inherited
defaults.

### Choosing a target fraud prevalence

If the goal is a particular percentage of true-fraud payment rows, work
backward from the generated payments:

1. Calculate the baseline payment volume from the date window and
   `payments.daily_target`.
2. Identify the selected scenario's nominal payments per campaign.
3. Calculate the desired number of true-fraud payments.
4. Set `scenario_count` and each scenario's `count` high enough to permit that
   many campaigns, then set `target_rate` high enough to avoid becoming the
   limiting cap.
5. Set `hard_negative_rate` separately, because hard negatives add legitimate
   lookalikes rather than true fraud.

For example, the complete
[`examples/configuration/f04-half-fraud.yaml`](../examples/configuration/f04-half-fraud.yaml)
uses 5,478 baseline payments, 5,478 F04 campaigns, and no hard negatives. F04
creates one payment per campaign, so the result is approximately 5,478 baseline
payments plus 5,478 true-fraud payments: roughly 50% true fraud by payment row.
This outcome comes from matching the campaign count to the baseline count; it
does not follow from `target_rate: 1.0` by itself.

By contrast, a five-year run with six baseline payments per day has about
10,956 baseline payments. If only 40 campaigns are permitted, as in the
following fragment, the run cannot produce 5% fraud merely because
`target_rate` is `0.05`:

```yaml
# Merge into an existing configuration; this is not a complete file.
fraud:
  enabled: true
  target_rate: 0.05
  scenario_count: 40
  hard_negative_rate: 1.0
  scenarios:
    F03: {enabled: true, count: 20, weight: 1.0}
    F04: {enabled: true, count: 20, weight: 1.0}
```

The 40-campaign capacity wins over the 547-campaign target cap. F03 and F04
also create different numbers of payments, while hard negatives add additional
non-fraud records. The realized fraud percentage must therefore be measured
from the generated records, not inferred from `target_rate`.

A valid YAML file can consequently be semantically unsuitable for the intended
prevalence. Validate the file first, then inspect the manifest's fraud counts,
the `fraud_truth` field in source records, and observed-label tables separately.

Workflow projections are controlled separately. Their delays are causal: an investigation or label cannot appear before the evidence that makes it possible. Operational records omit latent truth fields; oracle artifacts retain the complete explanation.

## Data quality

Use `quality.profile: clean` for a baseline, `realistic` for bounded faults, or `hostile` for a more demanding stream. Individual probabilities and delays can override a profile. Faults include duplicates, missing optional fields, invalid enums/references, negative or extreme amounts, timestamp/time-zone corruption, schema mismatches, encoding faults, late or out-of-order delivery, source delay, outages, schema changes, fraud spikes, traffic spikes, and deterministic partition skew.

```yaml
quality:
  profile: realistic
  duplicate_event_probability: 0.02
  late_event_probability: 0.05
  source_delay_seconds: 30
  invalid_enum_probability: 0.001
  invalid_reference_probability: 0.001
  negative_amount_probability: 0.001
  corrupted_timestamp_probability: 0.001
  timezone_error_probability: 0.001
  schema_mismatch_probability: 0.001
  extreme_value_probability: 0.001
  encoding_error_probability: 0.001
  partition_skew_probability: 0.01
  schema_changes:
    - at: 2026-01-15T00:00:00Z
      event: payment_events
      version: "6"
      change:
        add_optional_field:
          risk_reason: null

pix_lifecycle:
  timeout_probability: 0.0
  timeout_delay_seconds: 1
```

Quality mutations are applied in a deterministic order, preserve the pristine oracle snapshot, and are audited in the manifest with targets, requested rates, actual mutations, identity effects, boundaries, and expected validation failures. Encoding faults are written to `quality/raw_faults.jsonl`; schema-evolved rows are written under `quality/schema_evolution/`. Version-only `schema_changes` remain supported as a compatibility shorthand.

## Difficulty and camouflage

Difficulty changes how subtle a case is without changing its business objective or required topology:

```yaml
benchmark:
  difficulty: 7
  controls:
    temporal_irregularity: 0.9
    prevalence: 0.6
```

Camouflage is an independent stress layer. It moves fraud features or benign support relationships toward legitimate cohorts while preserving oracle truth:

```yaml
stress:
  camouflage: 0.8
  feature_camouflage: 0.85
  relation_camouflage: 0.75
```

Use either the versioned fixtures under `configs/benchmarks/` or a copied YAML file when comparing runs. The resolved controls, constraints, cohort choices, and effective configuration hash are recorded with the output.

## Dynamic campaign evolution

Campaign dynamics are disabled by default and do not change legacy IDs, hashes, schemas, or files. Enable them with a graph run:

```yaml
campaign_dynamics:
  enabled: true
  bindings:
    - {profile: linear}
    - {profile: rotating_ring}
    - {profile: adaptive_network}
```

The built-ins target `MULE_NETWORK`, `CYCLIC_RING`, and `DENSE_CAMPAIGN`. Bindings validate phase durations and transitions before generation and support `marked_hawkes_v1` (default) or `piecewise_rate_v1`. CARD actions are merchant purchases with the normal card lifecycle; PIX and account-transfer actions preserve account-to-account ledger rules. Rail, template, model, phase, and capacity incompatibilities are rejected deterministically.

Campaign dynamics run after graph construction and before camouflage. Counterfactual generation still selects pristine legitimate sources before fraud, graph, dynamic, and camouflage processing. Dynamic state, reasons, membership history, intensity decisions, topology mutations, and lineage are oracle-only append-only Parquet artifacts under `campaign_dynamics/<campaign_id>/`; observable output uses established payment, event, and ledger schemas. Custom deterministic transition and intensity models can be registered through the public API. `fraudtwin campaign evolve` appends a sidecar to a clean static graph run without modifying its existing artifacts.

## Reproducibility

Named random streams isolate entity, behavior, payment, lifecycle, fraud, quality, graph, and benchmark generation. The same validated configuration and seed produce equivalent identities, records, ordering, schemas, and manifests. Scenario generation never depends on the current clock or uncontrolled global randomness.

Validate before every significant run:

```bash
poetry run fraudtwin config validate path/to/config.yaml
```

Validation checks the configuration schema; generation also checks the
realized state, including account ledger capacity. A configuration can pass
the first check and still fail while payments are being materialized. See
[Generation failures after validation](troubleshooting.md#generation-fails-after-configuration-validation)
for the stage, record, and remediation details printed by the CLI.

## Task-oriented recipes

The generated [configuration parameter reference](configuration-reference.rst)
lists every field. These recipes show the smallest useful combinations for
common users.

### Minimal first run

Use `config init` for an installed package. The command creates a complete
file; then validate before generating:

```console
fraudtwin config init config.yaml
fraudtwin config validate config.yaml
fraudtwin generate config.yaml --output-dir runs/minimal
```

When working from a repository checkout, `configs/minimal.yaml` is the
equivalent tracked fixture. The following is an override fragment, not a
complete file by itself:

```yaml
# Merge these sections into a project-owned configuration.
simulation: {seed: 42, start: 2026-01-01T00:00:00Z, duration_days: 1, speed: batch}
population: {customers: 10, institutions: 3, accounts: 15, cards: 12, merchants: 3, devices: 12, pix_keys: 8}
payments: {daily_target: 100, rails: {CARD: 0.55, PIX: 0.30, ACCOUNT_TRANSFER: 0.15}}
```

### Five-year PIX and account-transfer history

Keep the business choices in YAML and calculate only the rolling dates in
Python. The complete example is available at
[`examples/configuration/five-year-pix-transfer.yaml`](../examples/configuration/five-year-pix-transfer.yaml).
Its important settings are:

```yaml
# These sections belong in config.yaml.
simulation:
  seed: 42
  duration_days: 1826 # Five years in this fixed example.

payments:
  daily_target: 6
  rails:
    CARD: 0.0
    PIX: 0.75
    ACCOUNT_TRANSFER: 0.25

behavior:
  amount_max: 5.0

population:
  customers: 1000
  accounts: 1000
  cards: 0 # Not needed when CARD traffic and card scenarios are disabled.
  merchants: 25
  devices: 1000
  pix_keys: 800

dataset:
  enabled: false
```

For a rolling five-year window, load the file and replace only the simulation
dates. The calendar-safe helper avoids failing when the end date is February
29:

```python
from calendar import monthrange
from datetime import datetime, timezone
from pathlib import Path

import fraudtwin
from fraudtwin.config import SimulationRunConfig, load_config


def years_before(moment: datetime, years: int) -> datetime:
    year = moment.year - years
    day = min(moment.day, monthrange(year, moment.month)[1])
    return moment.replace(year=year, day=day)


base = load_config(Path("config.yaml"))
values = base.model_dump(mode="python")
end = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
start = years_before(end, 5)
values["simulation"].update(start=start, duration_days=(end - start).days)
config = SimulationRunConfig.model_validate(values)

run = fraudtwin.generate(config=config, write=True, output_dir="runs")
print(run.run_id, run.run_dir)
```

Fixed dates produce stable run identities. A `datetime.now()` window produces a
different configuration—and therefore a different run hash—when the calendar
window changes.

### Fraud and delayed labels

This is a mergeable section, not a complete configuration file. Enable fraud
and observation separately when studying detection and label maturity. Keep
`fraud.enabled` true while varying `labels` or `fraud_workflow`; changing the
label policy should not silently change source payment identity.

```yaml
# Merge these sections into config.yaml; this is not a complete file.
fraud:
  enabled: true
  target_rate: 0.05 # Campaign cap; not a promised row-level fraud rate.
  scenario_count: 3
  # In a standard run, a positive value requests one lookalike per campaign.
  hard_negative_rate: 1.0
  scenarios:
    F04: {enabled: true, weight: 1.0, count: 3}

labels:
  enabled: true
  investigation_rate: 0.70
  missing_fraud_rate: 0.05
  preliminary_error_rate: 0.02
  correction_rate: 0.01
  confirmation_delay: lognormal

dataset:
  enabled: true
  unresolved_labels: include
```

Changing `labels` or `fraud_workflow` changes what becomes observable, not the
source fraud campaigns. Keep the source configuration fixed when comparing
label policies so differences in observed data can be attributed to the
workflow rather than to a new fraud population.

### Graph, benchmark, and calibration runs

Enable `graph` when the experiment needs relationship provenance; use
`benchmark`/`stress` to compare difficulty or camouflage; use `calibration`
only when a reference table and its fingerprint are available. Record the
resolved configuration alongside every comparison.

```yaml
graph: {enabled: true, view: observable, as_of: 2026-01-02T00:00:00Z}
benchmark: {difficulty: 5}
stress: {camouflage: 0.4, feature_camouflage: 0.4, relation_camouflage: 0.2}
```

### Scale and quality runs

Use `scale.profile: dev` for a laptop and increase `target_payments` only after
checking disk and memory. Apply `quality.profile: realistic` or `hostile` to a
copy of a clean configuration so the pristine source remains available for
reconciliation.

```yaml
scale: {profile: dev, target_payments: 1000, shard_count: 4, chunk_size: 1000}
quality: {profile: realistic, late_event_probability: 0.05, duplicate_event_probability: 0.02}
```

### PIX-heavy payment traffic

This is a mergeable fragment. Set `cards: 0` in `population` if card entities
should not be created:

```yaml
payments:
  daily_target: 600
  rails:
    CARD: 0.00
    PIX: 0.75
    ACCOUNT_TRANSFER: 0.25

population:
  cards: 0
```

### Dataset-disabled event generation

This is a mergeable fragment for runs that need payment, lifecycle, and ledger
records but do not need point-in-time ML tables:

```yaml
dataset:
  enabled: false
```

### PostgreSQL or Kafka output

This is a mergeable fragment. The corresponding optional extra and external
service must also be available:

```yaml
outputs:
  parquet: true   # Keep the deterministic local audit source.
  postgres: true  # Requires the PostgreSQL extra and a configured DSN.
  kafka: false
  iceberg: false
```

```yaml
outputs:
  parquet: true
  postgres: false
  kafka: true     # Requires Kafka and Schema Registry settings.
  iceberg: false
```

### Integration outputs

Keep `outputs.parquet: true` when enabling PostgreSQL, Kafka, or Iceberg. The
Parquet run is the deterministic audit source; external sinks are projections
that can be retried or rebuilt.

## Cross-field constraints

Generated field tables cannot express every relationship. Validate these rules
before running a large job:

| Relationship | Rule |
| --- | --- |
| Population | Accounts, cards, devices, and payment volume must have enough parent entities |
| Payment rails | Rail weights must be non-negative and sum to `1.0` |
| Simulation time | Start and duration define the source window; timestamps must be timezone-aware |
| Fraud | `fraud.enabled` is required before scenario or hard-negative controls have an effect |
| Labels | Label observation requires a workflow/delay policy; unresolved labels are not mature labels |
| Dataset | A PIT dataset needs a source run and feature/label availability policy |
| Graph | Graph controls require graph-enabled source data and a valid temporal cutoff |
| Scale | Checkpoint, shard, and chunk settings must be positive and use a stable mapping |
| Quality | Fault profiles should be applied to a copy when source reconciliation is required |
| Integrations | An enabled sink requires its extra, credentials, and a healthy external service |

When validation succeeds, save the resolved YAML and manifest together. When it
fails, fix the first reported field rather than disabling strict validation.

For example, this file fails because the rail weights total `0.9`:

```yaml
payments:
  daily_target: 100
  rails: {CARD: 0.5, PIX: 0.4}
```

Validation reports the rail-distribution error. Add the missing weight or
correct the existing values so the total is `1.0`, then run validation again.
Unknown keys fail for the same reason: they usually indicate a spelling error
or a setting copied from a different version of the schema.

## Next

Use [Data and evaluation workflows](workflows.md) for datasets and backtests,
or choose an optional path from [Integration runbooks](integrations.md).

## Related

- [Configuration reference](configuration-reference.rst)
- [CLI reference](cli.rst)
- [Troubleshooting](troubleshooting.md)
