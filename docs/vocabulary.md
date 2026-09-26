# F, P, and C identifiers

**Level:** Beginner to Intermediate<br><br>
**You will:** understand the stable IDs used in configuration, diagnostics, and
protocol descriptions.<br><br>
**Before you start:** [Configuration](configuration.md).<br><br>
**Services:** None.

FraudTwin uses short, stable identifiers for the things users configure and the
constraints that explain generation. The identifier is for software; the name
next to it is for people.

- `F##` identifies a fraud scenario.
- `P##` identifies a generation protocol or capability.
- `C##` identifies a capacity or generation constraint.

The P and C identifiers are metadata, not additional YAML keys. F identifiers
remain the keys under `fraud.scenarios`. Adding
the vocabulary does not change configuration hashes, random streams, payment
amounts, or record ordering.

## Fraud scenarios

| ID | Name | Meaning | Protocol | Main capacities |
| --- | --- | --- | --- | --- |
| `F01` | card-not-present | Card activity where the physical card is not present | `P05` | `C01`, `C04`, `C09` |
| `F02` | card-testing | Repeated low-value card authorization attempts | `P06` | `C01`, `C04`, `C09` |
| `F03` | account-takeover | Fraud following compromised account access | `P07` | `C01`, `C04`, `C08` |
| `F04` | instant-payment-scam | Fraudulent PIX payment, often to a new beneficiary | `P08` | `C01`, `C04`, `C12` |
| `F05` | velocity-attack | A rapid burst of repeated payment attempts | `P09` | `C01`, `C04`, `C09` |

The YAML key remains the `F##` value:

```yaml
fraud:
  scenarios:
    F04:
      enabled: true
      count: 100
```

`F04` is the scenario identity. `P08` is the protocol that creates the F04
pattern. `C01`, `C02`, and `C03` limit how many campaigns can be attempted;
`C04` limits whether the resulting payments can be posted to the ledger.

## Protocols

| ID | Name | Purpose | Default or activation |
| --- | --- | --- | --- |
| `P01` | fraud-generation | Creates fraud campaigns and scenario-linked payment records | Used when fraud generation is enabled |
| `P02` | difficulty | Makes generated fraud harder to distinguish from legitimate activity | Opt in through `benchmark.difficulty` |
| `P03` | counterfactual | Creates alternative trajectories or outcomes | Opt in through `counterfactual.enabled` |
| `P04` | campaign-dynamics | Evolves campaigns through deterministic timing and topology changes | Opt in through `campaign_dynamics.enabled` |
| `P05` | card-not-present-protocol | Implements `F01` | Active when `F01` is selected |
| `P06` | card-testing-protocol | Implements `F02` | Active when `F02` is selected |
| `P07` | account-takeover-protocol | Implements `F03` | Active when `F03` is selected |
| `P08` | instant-payment-scam-protocol | Implements `F04` | Active when `F04` is selected |
| `P09` | velocity-attack-protocol | Implements `F05` | Active when `F05` is selected |

`get_protocol()` also accepts the legacy aliases `M6` → `P01`, `M12` → `P02`,
`M14` → `P03`, and `M15` → `P04`. These lookup aliases do not rename existing
configuration fields, manifest fields, or deterministic random streams.

## Capacities and constraints

| ID | Name | What it limits or validates | Configuration source |
| --- | --- | --- | --- |
| `C01` | baseline-payment-capacity | Legitimate payment volume | `payments.daily_target * simulation.duration_days` |
| `C02` | fraud-campaign-capacity | Overall campaign count | `fraud.scenario_count` and `fraud.target_rate` |
| `C03` | scenario-campaign-capacity | Campaigns for one scenario | `fraud.scenarios.<F##>.count` |
| `C04` | ledger-debit-capacity | Running balance plus overdraft | Generated account ledger fields |
| `C05` | lifecycle-window-capacity | Whether lifecycle delays fit the simulation window | Simulation and lifecycle settings |
| `C06` | customer-capacity | Generated customers | `population.customers` |
| `C07` | institution-capacity | Generated institutions | `population.institutions` |
| `C08` | account-capacity | Generated accounts | `population.accounts` |
| `C09` | card-capacity | Generated active cards | `population.cards` |
| `C10` | merchant-capacity | Generated merchants | `population.merchants` |
| `C11` | device-capacity | Generated devices | `population.devices` |
| `C12` | pix-key-capacity | Generated PIX keys and PIX-capable accounts | `population.pix_keys` |
| `C13` | account-ownership-capacity | Valid customer-to-account relationships | Customers and accounts |
| `C14` | institution-account-capacity | Valid institution-to-account relationships | Institutions and accounts |
| `C15` | pix-key-assignment-capacity | Valid PIX-key-to-account relationships | Accounts and PIX keys |
| `C16` | hard-negative-capacity | Legitimate fraud lookalikes | `fraud.hard_negative_rate` |
| `C17` | label-observation-capacity | Records that can receive observed labels | Fraud workflow and dataset settings |

Capacity IDs describe existing limits and relationships; they do not add YAML
settings or new validation rules. Currently, structured ledger errors report
`C04`. The other IDs provide reference terminology and are not separate error
codes emitted by generation.

## Defaults and omitted scenarios

`fraud.scenarios` is an overlay on the built-in scenario map. Omitting the map,
or specifying only one scenario, does not turn the other scenarios off. Missing
scenario settings receive the schema defaults:

```yaml
enabled: true
weight: 1.0
count: 1
attempt_count: 20
window_seconds: 60
```

`amount_min` and `amount_max` default to `None`, so each scenario chooses its
own amount bounds; `duration_seconds` defaults to `0`. Settings such as
`attempt_count` only affect the scenarios that use them.

The following is a scenario-selection fragment to merge into a complete
configuration. To generate F04 campaigns, also set `fraud.enabled: true`, a
positive `fraud.target_rate`, and a sufficient `fraud.scenario_count`. Disable
every other scenario:

```yaml
fraud:
  scenarios:
    F01: {enabled: false, count: 0}
    F02: {enabled: false, count: 0}
    F03: {enabled: false, count: 0}
    F04:
      enabled: true
      count: 100
      amount_min: 1.00
      amount_max: 100.00
    F05: {enabled: false, count: 0}
```

## Campaigns, payments, and capacities

With advanced difficulty controls disabled, campaign selection is bounded by
the baseline volume and campaign capacities:

```text
campaign_count = min(
    scenario_count,
    floor(baseline_payment_count * target_rate),
    enabled_scenario_capacity,
)
```

`enabled_scenario_capacity` sums `count` only for scenarios with `enabled: true`
and positive `weight`. Difficulty controls can change the effective target rate
and overall campaign budget.

A campaign is not necessarily one payment. F03 creates two transfer payments per
campaign; F04 creates one PIX payment; F02 and F05 create repeated attempts.
Hard negatives consume additional generation work but are legitimate records,
not true fraud. Observed labels and point-in-time dataset rows are separate from
source fraud records.

## Capacity planning and failure guide

Only `C04` currently appears as a structured capacity failure. Other rows help
explain configuration limits or unexpected output counts.

| Capacity | Typical cause | Corrective action |
| --- | --- | --- |
| `C01` | Too many requested baseline payments for the available entities or ledger | Reduce daily volume or shorten the time window |
| `C02` / `C03` | Campaign budget or scenario count is too small or too large for the intended prevalence | Work backward from payments per campaign and adjust the relevant campaign limit |
| `C04` | A debit exceeds the running balance plus overdraft | Reduce volume or amounts, shorten the run, increase account capacity, or choose a capacity-aware scenario |
| `C05` | Lifecycle events fall outside the simulation window | Extend the simulation or reduce lifecycle delays |
| `C08`–`C15` | A scenario cannot find the required entities or relationships | Increase the required population or disable the dependent scenario/rail |
| `C16` | Hard-negative lookalikes cannot be placed safely | Reduce `hard_negative_rate` or scenario scope |
| `C17` | Labels are not observable within the requested dataset window | Adjust workflow delays, dataset windows, or unresolved-label policy |

A valid configuration can still fail during generation because schema validation
checks fields while capacity validation checks the realized generated state.
Ledger failures expose the stage, protocol, `C04` capacity, and affected record.
For built-in fraud scenarios, `scenario_id` identifies the `F##` scenario and
`campaign_id` identifies the individual campaign in `error.as_dict()`. These
values can be `None` for baseline payments; graph scenarios use their own
scenario names. An explicit transformation protocol takes precedence over the
scenario protocol, so use `stage` and `scenario_id` together to interpret it.

## Python lookup API

The registry is available to applications that need stable metadata:

```python
import fraudtwin

scenario = fraudtwin.get_scenario("F04")
protocol = fraudtwin.get_protocol("P08")
capacity = fraudtwin.get_capacity("C04")

print(scenario.name, protocol.name, capacity.name)
```

Lookups are case-sensitive and raise `KeyError` for unknown identifiers.

The authoritative implementation is
[`src/fraudtwin/vocabulary.py`](https://github.com/emedinac/fraudtwin/blob/main/src/fraudtwin/vocabulary.py).
Configuration defaults remain authoritative in
[`src/fraudtwin/config.py`](https://github.com/emedinac/fraudtwin/blob/main/src/fraudtwin/config.py)
and the packaged default YAML.

## Next

Return to the [configuration guide](configuration.md) to build a complete run.

## Related

- [Quickstart](quickstart.md)
- [Troubleshooting](troubleshooting.md)
- [API configuration reference](api/configuration.rst)
