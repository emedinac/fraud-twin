# Concepts

## Determinism and seed streams

FraudTwin derives named random streams from the configured seed. Entity IDs,
payment identities, timestamps, fraud campaigns, quality faults, graph edges,
and benchmark transformations remain reproducible without relying on global
random state. Worker scheduling may change execution order, but it must not
change logical IDs or recorded fingerprints.

## Payment and ledger lifecycle

Payments are business identities. Lifecycle events describe authorization,
capture, clearing, settlement, reversal, refund, return, and chargeback timing.
The ledger records balanced postings for financial movements; lifecycle events
are not interchangeable with ledger entries.

## Observable and oracle data

The observable view contains only records and relationships a detector could
know at the selected cutoff. Oracle artifacts retain latent fraud scenario,
campaign membership, evidence, and future labels for evaluation and audit. Keep
oracle artifacts out of model features and operational exports.

## Labels and point-in-time safety

Labels have an availability time, not just a truth value. Dataset construction
uses the greatest label version available at `prediction_time`; immature or
missing labels follow the configured unresolved-label policy. This prevents
future investigations and corrections from becoming training leakage.

## Fraud scenarios and stress controls

Fraud scenarios describe the objective of an attack. Hard negatives, difficulty,
camouflage, dynamic campaigns, and counterfactuals alter how detectable or
evolvable that objective is without changing the underlying provenance rules.

## Graph provenance and temporal cutoffs

Graph edges derive from payment and event identities. Observable graphs include
source-supported relationships available at a cutoff; oracle graphs may include
campaign evidence, hyperedges, and latent membership. Every export should be
read with its view and time window recorded.

## Data quality and schema evolution

Quality profiles inject explicit faults such as duplicates, delays, invalid
values, outages, schema changes, and spikes. The fault is intentional and
reported in manifests; it is not a silent relaxation of the source contract.
