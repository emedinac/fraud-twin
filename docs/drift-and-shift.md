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
