# Core concepts

**Level:** Beginner<br><br>
**You will:** build a simple mental model of identities, time, lifecycles,
labels, graphs, and data quality in a generated run.<br><br>
**Before you start:** the [Quickstart](quickstart.md).<br><br>
**Services:** None.

FraudTwin becomes easier to use once a few ideas are clear. A run is more
than a collection of rows: it has stable identities, a timeline, operational
views, and evidence that explains what happened. This section explains the
model behind the workflows; use the how-to guides when you are ready to act.

## Four ideas to keep in mind

### Determinism is part of the model

A configuration and seed produce the same logical identities, events, labels,
and output fingerprints. FraudTwin uses named random streams for different
parts of the simulation, so changing one part of a configuration does not
silently replace every other identity. Worker scheduling can change execution
order, but it should not change logical IDs or recorded fingerprints.

### A payment is a timeline, not a row

Payments are business identities. Lifecycle events describe authorization,
capture, clearing, settlement, reversal, refund, return, and chargeback
timing. The ledger records balanced financial postings; lifecycle events and
ledger entries describe related but different facts.

### Operational and oracle data answer different questions

The operational view contains only records and relationships a detector could
know at a selected cutoff. Oracle artifacts retain latent fraud scenarios,
campaign membership, evidence, and future labels for evaluation and audit.
Oracle data explains a result, but must not become a model feature by accident.

### Quality faults are evidence, not noise

Duplicates, delays, invalid values, outages, schema changes, and spikes are
intentional test conditions. They are reported in manifests so a workflow can
detect, quarantine, repair, or replay a known fault without losing provenance.

## Choose a concept

- [Data model and lifecycle](concepts/data-model-and-lifecycle.md) — how
  entities, payments, events, and ledger entries fit together.
- [Point-in-time data and labels](concepts/point-in-time-data-and-labels.md) —
  what a detector could know at a given time and how delayed labels work.
- [Fraud, graphs, and data quality](concepts/fraud-graphs-and-data-quality.md) —
  how scenarios, graph relationships, and deliberate quality faults are
  represented for testing.

You do not need to memorize every term before using FraudTwin. Start with the
first page, then return to the others when a workflow calls for them.

## Next

Try the [Quickstart](quickstart.md), then read [Data model and lifecycle](concepts/data-model-and-lifecycle.md).

## Related

- [Glossary](glossary.md)
- [Data contracts](data-contracts.rst)
- [Verified capabilities](verified-capabilities.md)
