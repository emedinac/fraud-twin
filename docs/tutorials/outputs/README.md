# Compact tutorial outputs

These fixtures are small, deterministic examples used by the drift and Kafka
tutorials. They are intentionally not representative production metrics; run
the notebooks at 1,000–10,000 payments to produce workload-specific reports.

* `drift-report.json` demonstrates numeric PSI/Wasserstein, categorical
  Jensen–Shannon, and fraud-prevalence comparisons.
* `kafka-chaos-report.json` demonstrates seeded drops, retries, duplicates,
  delays, reordering, partitions, and consumer-visible rates.
