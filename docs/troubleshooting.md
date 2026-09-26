# Troubleshooting

**Level:** Beginner to Expert<br><br>
**You will:** identify installation, configuration, service, data, and resume<br><br>
failures from the first useful error message.
**Before you start:** the guide that led to the failure.<br><br>
**Services:** Follow the service requirements of the failing workflow.<br><br>

## Installation and optional extras

Install the base package first, then add only the integration you need:

```console
poetry install
poetry install -E ml -E graph
poetry install -E postgres -E kafka -E lakehouse
```

Optional dependency failures have three distinct causes:

- A missing module means the extra is not installed. Add the matching Poetry
  extra.
- A package that imports but lacks `Producer`, `Schema`,
  `SchemaRegistryClient`, or callable `psycopg.connect` is incomplete. Repair
  it with `poetry install -E kafka -E postgres`; FraudTwin reports this
  separately from a missing service.
- A connection-refused or health-check failure means the client is installed
  but Kafka, Schema Registry, or PostgreSQL is unavailable.

The base installation remains valid for offline generation and tutorials.
Verify both optional clients with:

```console
poetry run python -c \
  "from confluent_kafka import Producer; from confluent_kafka.schema_registry import Schema, SchemaRegistryClient; print('Kafka extra OK')"

poetry run python -c \
  "import psycopg; assert callable(psycopg.connect); print('PostgreSQL extra OK')"
```

## Configuration validation fails

Run validation before generation and inspect the first reported field:

```console
poetry run fraudtwin config validate configs/minimal.yaml
```

Unknown fields are rejected intentionally. Check required relationships such as
accounts needing customers and institutions, rail weights summing to `1.0`,
and timezone-aware simulation timestamps.

## Generation fails after configuration validation

Configuration validation answers whether the YAML has the right fields and
values. Generation performs a second kind of validation: it realizes entities,
payments, lifecycle events, fraud, and ledger entries. A file can therefore be
valid YAML and a valid `SimulationRunConfig`, but still request a generated
state that cannot be reconciled.

Known generation failures are reported with a code, stage, affected record,
and suggested fixes. For example:

```text
Generation failed [LEDGER_OVERDRAFT_EXCEEDED]

Stage: baseline payment ledger
Protocol: P01
Capacity: C04
Account: ACC-000228
Payment: PAY-...
Event: EVT-...
Debit: 537.34
Balance before: 345.11
Balance after: -192.23
Allowed overdraft: 100.00
```

The stage tells you whether the failure happened while creating baseline,
fraud, graph, camouflage, campaign-dynamics, or streaming records. `P##`
identifies the active generation protocol and `C##` identifies the failed
capacity or invariant. Currently, structured capacity errors report `C04`
(ledger debit capacity); see the [vocabulary reference](vocabulary.md). The
account, payment, and event identify the first failing ledger operation. No
manifest is available when generation stops before run materialization.

### Ledger capacity errors

FraudTwin preserves every requested payment amount. It does not silently clip
an amount to make an invalid ledger pass. For an account, the permitted debit
boundary is:

```text
available debit capacity = running ledger balance + overdraft limit
```

The running balance starts at the opening ledger balance and includes prior
credits and debits in posting order.

`behavior.amount_max` limits one payment; it does not limit the total amount
spent by an account across a long simulation. Many individually valid payments
can exhaust the same account's capacity. Reduce `payments.daily_target`,
`behavior.amount_max`, or `simulation.duration_days`, increase the account
capacity, or choose a capacity-aware scenario. Labels and dataset settings are
not involved when the failure stage is `baseline payment ledger`.

### Other generated-state failures

- A lifecycle-window failure means an authorization, settlement, refund, or
  return delay no longer fits inside the simulation window. Shorten the delay
  or extend `simulation.duration_days`.
- A missing-entity-capacity failure means the requested payment or scenario
  cannot find a valid account, customer, institution, card, PIX key, merchant,
  or device. Increase the relevant population or disable the dependent rail or
  scenario.
- Fraud and hard-negative capacity failures mean the selected campaigns or
  lookalikes cannot be placed with the requested relationships or ledger
  capacity. Reduce campaign counts, hard-negative rate, or scenario scope.
- Optional sink failures usually mean a missing extra, unavailable service, or
  invalid connection settings. Install the matching extra and check the
  service before retrying.

Retry only after correcting the field or capacity named by the report. Python
callers can catch `fraudtwin.errors.GenerationError` and inspect `error.code`,
`error.stage`, `error.protocol_id`, `error.capacity_id`, `error.scenario_id`,
and the typed diagnostic attributes. Unexpected programming
errors remain tracebacks so they can be reported to developers.

## Labels are missing or unresolved

Check `label_available_at` against `prediction_time`. A label may be true in
oracle data but unavailable operationally. Review `dataset.unresolved_labels`
and the configured workflow/observation delay before changing model code.

## Graph export is empty or incomplete

Confirm that graph scenarios were enabled and that the selected `view`,
`as_of`, `from_time`, and `to_time` lie inside the source run window. Observable
exports intentionally omit future and latent oracle-only relationships.

## Scale runs stop or cannot resume

Use a bounded profile locally, keep the checkpoint directory intact, and resume
from its manifest:

```console
CONFIG=configs/scale-1b.yaml
CHECKPOINT_DIR=./runs/scale-checkpoint

fraudtwin generate "$CONFIG" --workers 16 --checkpoint-dir "$CHECKPOINT_DIR"
fraudtwin resume "$CHECKPOINT_DIR"
```

Resume validates the resolved configuration and seed tree. Do not edit a
checkpoint or versioned benchmark fixture in place.

## Next

Return to the relevant [learning path](learning-paths.md) after the environment
is healthy.

## Related

- [Installation](installation.md)
- [Configuration](configuration.md)
- [Verified capabilities](verified-capabilities.md)
