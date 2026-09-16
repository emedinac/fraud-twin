# Data, domain, and concept shift

FraudTwin can compare a stable reference window with a later production or
stress window using `fraudtwin.ml.drift`.

```python
from fraudtwin.ml.drift import DriftConfig, compare_performance, compare_windows

report = compare_windows(
    reference_rows,
    production_rows,
    DriftConfig(
        reference_name="training",
        comparison_name="production-week-4",
        fields=("amount", "payment_rail", "merchant_category", "label"),
    ),
)
print(report.fingerprint)
for metric in report.alerts:
    print(metric.field, metric.method, metric.comparison_value)
```

Interpret the metrics by asking which distribution changed:

| Question | Meaning | Typical response |
| --- | --- | --- |
| Did feature values change? | Data drift, `P(X)` | inspect source, missingness, or feature logic |
| Did customer/merchant/channel mix change? | Domain shift | evaluate slices and recalibrate sampling |
| Did fraud prevalence or conditional outcomes change? | Concept drift, `P(Y\|X)` | retrain, recalibrate, or redesign features |
| Did the same model score worse? | Performance drift | investigate threshold, labels, and attack mix |

Numeric reports include reference-fitted PSI bins and empirical Wasserstein
distance. Categorical reports include Jensen–Shannon divergence. Reports also
include missingness, duplicate-rate, sample-size, prevalence, input
fingerprints, thresholds, and label policy. Do not treat an alert as proof of
model failure until mature labels and the relevant business slice are present.

Use the temporal and public benchmark packs to create controlled shifts, then
compare the exact same metric definitions and thresholds across windows.

## Window and sample policy

Choose a reference window that represents the data used to train or approve the
model. The comparison window must use the same feature definitions and the same
unit of analysis. Persist both fingerprints and the time boundaries. Do not
compare a matured-label training window with an immature production window and
call the result performance drift.

Minimum sample sizes and smoothing are part of ``DriftConfig``. Small samples
should produce an informational result or an explicit insufficient-sample
status, not a high-severity alert based on unstable bins. Segment reports must
include segment counts; a high PSI on a segment with very few rows is a review
signal, not proof of a population change.

## Alert interpretation and action

| Evidence | Likely interpretation | Recommended action |
| --- | --- | --- |
| PSI/Wasserstein alert with stable labels | Data drift | Inspect ingestion, feature logic, and missingness |
| Merchant/customer/channel mix changes | Domain shift | Re-evaluate slices and sampling assumptions |
| Prevalence or conditional outcomes change | Concept drift | Recalibrate, retrain, or redesign features |
| Same policy performs worse on mature labels | Performance drift | Check attack mix, threshold, latency, and label policy |
| Unresolved-label rate increases | Label delay | Defer promotion/retraining decisions |

Use severity based on magnitude, sample size, business impact, and persistence
across windows. A single alert should open an investigation; it should not
automatically retrain a model without a promotion policy and an approved
evaluation window.

## Segment and multiple-alert policy

Compare customer cohort, merchant, channel, device, geography, and scenario
segments only when the segment key is available at prediction time. Record the
number of segments tested and the minimum segment size. For many simultaneous
segments, expect false positives: prioritize alerts that are large, persistent,
business-critical, or corroborated by performance drift.

## Reproducible response

Store the drift report, configuration, reference/comparison fingerprints,
thresholds, sample counts, label policy, and model/evaluation fingerprint. A
reproducible response is:

1. freeze the affected windows;
2. rerun the report with the same policy;
3. inspect data-quality and domain-mix evidence;
4. evaluate mature labels with the unchanged threshold policy;
5. choose investigation, recalibration, retraining, or no action;
6. record the decision and next review window.
