# Determinism and reproducibility

**Level:** Beginner<br><br>
**You will:** understand how configuration, seeds, named streams, and
fingerprints make a run repeatable.<br><br>
**Before you start:** the [Quickstart](../quickstart.md).<br><br>
**Services:** None.

Reproducibility is part of FraudTwin's data model, not only a property of the
test suite. A run records enough identity and configuration information to
explain how its outputs were produced and to compare them with another run.

## Configuration and seed identity

A validated configuration and seed determine the logical world: entities,
payment identities, event timestamps, fraud scenarios, labels, and output
artifacts. The run manifest records the resolved configuration, schema
versions, counts, and output fingerprints.

Changing a relevant configuration value should produce a new run identity.
Keeping the same configuration and seed should reproduce the same logical
identities and values.

## Named random streams

FraudTwin derives named random streams for independent parts of generation
instead of relying on one mutable global random state. This limits unrelated
changes: adding or changing one controlled behavior should not silently
replace every entity ID or payment identity.

Parallel workers may finish in a different order. That can change execution
order, but it must not change logical IDs, event meaning, or recorded
fingerprints.

## Fingerprints are evidence

Use the manifest and stage fingerprints to compare runs. A matching
fingerprint supports the claim that the relevant logical output is the same;
it does not mean that every incidental file timestamp or physical partition
layout is identical.

Record the configuration, seed, package version, schema versions, and selected
view or cutoff with any benchmark or evaluation result. Without those
boundaries, a result may be repeatable but not explainable.

## Related concepts

- [Data model and lifecycle](data-model-and-lifecycle.md)
- [Point-in-time data and labels](point-in-time-data-and-labels.md)
- [Fraud, graphs, and data quality](fraud-graphs-and-data-quality.md)
- [Verified capabilities](../verified-capabilities.md)
