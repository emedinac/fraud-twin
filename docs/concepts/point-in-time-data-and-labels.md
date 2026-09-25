# Point-in-time data and labels

**Level:** Beginner<br><br>
**You will:** understand what information is available at a prediction time
and why labels may arrive later.<br><br>
**Before you start:** [Data model and lifecycle](data-model-and-lifecycle.md).<br><br>
**Services:** None.

Fraud detection is a problem that unfolds over time. A model must use the
information that was available when a decision was made, not information that
appeared later during an investigation.

## Observable and oracle views

The observable view contains records and relationships that an operational
detector could know at a selected cutoff time. It is the view to use when you
build features or simulate a live workflow.

Oracle artifacts contain the complete explanation: latent fraud scenarios,
campaign membership, evidence, and future labels. They are useful for
measuring performance and explaining a result, but they must not become model
features by accident.

## Labels have an availability time

A label is more than `fraud` or `legitimate`. It also has an availability time:
when the workflow could reasonably have known it. A chargeback or investigation
may confirm the truth days or weeks after the original payment.

When FraudTwin builds a point-in-time dataset, it selects the latest label
version available at `prediction_time`. If no suitable label is available, the
configured unresolved-label policy decides what happens.

## Avoiding future information

A safe feature pipeline follows three rules:

1. Choose a prediction time.
2. Use only observable records available by that time.
3. Treat later labels and oracle evidence as evaluation data.

This prevents future investigations, corrections, and campaign knowledge from
leaking into training data.

## Next

Read [Fraud, graphs, and data quality](fraud-graphs-and-data-quality.md) to
learn how FraudTwin creates difficult but explainable test cases.

## Related

- [Data contracts](../data-contracts.rst)
- [Data and evaluation workflows](../workflows.md)
- [ML evaluation methodology](../ml-evaluation.md)
