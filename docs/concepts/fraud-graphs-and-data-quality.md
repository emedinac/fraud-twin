# Fraud, graphs, and data quality

**Level:** Intermediate<br><br>
**You will:** understand how FraudTwin represents attack objectives, temporal
graph evidence, and deliberate data faults for robust evaluation.<br><br>
**Before you start:** [Point-in-time data and labels](point-in-time-data-and-labels.md).<br><br>
**Services:** None for the core concepts; optional integrations are documented
separately.

FraudTwin separates the objective of an attack from the controls used to make
that attack easy or difficult to detect. This gives experiments a stable
provenance while allowing the observed behavior to change.

## Fraud scenarios and stress controls

A fraud scenario describes what an attacker is trying to achieve. Hard
negatives, difficulty, camouflage, dynamic campaigns, and counterfactuals
control how detectable or evolvable that scenario is.

These controls do not replace the underlying provenance. A run can therefore
record both the generated behavior and the reason that behavior was produced.
That distinction is important when comparing models or explaining a benchmark.

## Temporal graph provenance

Graph nodes and edges derive from payment and event identities. An observable
graph contains relationships supported by information available at its cutoff.
An oracle graph may also include campaign evidence, hyperedges, and latent
membership for evaluation.

Every graph export should be read together with its view and time window. The
same entities can form different operational relationships as the timeline
moves forward.

## Data quality and schema evolution

Quality profiles inject explicit faults such as duplicates, delays, invalid
values, outages, schema changes, and spikes. The fault is intentional and is
reported in the manifest; it is not a silent relaxation of the source
contract.

This lets a data-quality test ask a precise question: can the workflow detect,
quarantine, repair, or replay the known fault without losing provenance?

## Next

Move to the [Data and evaluation workflows](../workflows.md) or the [Graph and
benchmark workflows](../graph-and-benchmarks.md) when you are ready to run
these concepts in practice.

## Related

- [Architecture and trust boundaries](../architecture.md)
- [Data contracts](../data-contracts.rst)
- [Compatibility and support policy](../compatibility.md)
