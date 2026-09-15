# References and related work

FraudTwin is released under the [Apache License 2.0](../LICENSE).

These references informed FraudTwin’s design. The repository does not bundle copied proprietary source code or external datasets; generated records are synthetic and produced by FraudTwin. Third-party dependencies and referenced works remain subject to their own licenses and terms.

- Mothilal, Sharma & Tan, [*Explaining Machine Learning Classifiers through Diverse Counterfactual Explanations*](https://doi.org/10.1145/3351095.3372850) *(design reference only)* - informed proximity, feasibility, and constrained counterfactual search; no source code, data, or models are copied.
- [DiCE reference implementation](https://github.com/interpretml/DiCE) *(design reference only)* - informed pluggable distance and feasibility concepts; FraudTwin does not import or depend on the repository.

## Synthetic data and fraud workflows

- [Fraud Detection Handbook simulator](https://fraud-detection-handbook.github.io/fraud-detection-handbook/Chapter_3_GettingStarted/SimulatedDataset.html) *(handbook)* - informed deterministic customer profiles, temporal payment behavior, rule-based fraud scenarios, and the legitimate baseline used for hard negatives.
- [Fraud Detection Handbook validation strategies](https://fraud-detection-handbook.github.io/fraud-detection-handbook/Chapter_5_ModelValidationAndSelection/ValidationStrategies.html) *(handbook)* - informed point-in-time features, delayed-label gaps, future-only evaluation windows, and rolling backtests.
- Vasquez et al., [The Hidden Cost of Fraud](https://proceedings.mlr.press/v183/vasquez22a.html) *(paper)* - motivated positive-unlabeled handling for fraud that remains undetected.
- He et al., [Identifying Labeling Mechanism in Positive-Unlabeled Learning under Unknown Class Prior](https://proceedings.mlr.press/v337/he26a.html) *(paper)* - informed selection-dependent labeling controls.
- [PaySim](https://github.com/EdgarLopezPhD/PaySim) *(repository and simulator)* - provided comparison points for synthetic mobile-money behavior and aggregate calibration without importing its schemas or data.
- [SynthFin Core](https://github.com/afborda/synthfin-core) *(repository)* - provided practical comparison points for behavioral enrichment, fraud scenarios, and fraud-pattern generation.

## Payment and financial references

- [TigerBeetle financial accounting](https://docs.tigerbeetle.com/coding/financial-accounting/) and [two-phase transfers](https://docs.tigerbeetle.com/coding/two-phase-transfers/) *(technical references)* - informed double-entry ledger invariants and authorization-hold, post, and void semantics.
- [Stripe manual capture](https://docs.stripe.com/payments/place-a-hold-on-a-payment-method), [PaymentIntent lifecycle](https://docs.stripe.com/payments/paymentintents/lifecycle), and [disputes](https://docs.stripe.com/disputes/how-disputes-work) *(public API documentation)* - informed the card authorization, capture, reversal, refund, and chargeback lifecycle abstractions.
- [Banco Central do Brasil Pix initiation standards](https://www.bcb.gov.br/estabilidadefinanceira/exibenormativo?numero=769&tipo=Instru%C3%A7%C3%A3o+Normativa+BCB), [Pix/SPI technical documents](https://www.bcb.gov.br/estabilidadefinanceira/comunicacaodados), and the [Pix timing manual](https://www.bcb.gov.br/estabilidadefinanceira/exibenormativo?numero=654&tipo=Instru%C3%A7%C3%A3o+Normativa+BCB) *(official standards)* - informed the Pix-like lifecycle and timing model without claiming to reproduce proprietary SPI internals.

## Graph fraud and research papers

- [Santander Gen-Fraud-Graph](https://github.com/SantanderAI/gen-fraud-graph) *(repository)* - informed reproducible financial graph generation, fraud-ring patterns, graph exports, and benchmark-oriented scale considerations.
- [NumPy parallel random generation](https://numpy.org/doc/stable/reference/random/parallel.html) and [`SeedSequence`](https://numpy.org/doc/stable/reference/random/seed_sequence.html) *(technical references)* - informed hierarchical deterministic streams for independent scale workers.
- [IBM AMLSim](https://github.com/IBM/AMLSim) *(repository)* - provided a comparison point for multi-agent synthetic banking graphs and known AML/fraud patterns.
- Haghighi et al., [*Beyond pairwise relationships: a transformer-based hypergraph learning approach for fraud detection*](https://doi.org/10.1007/s10115-025-02476-5) *(paper)* - motivated higher-order graph relations and the separation of observable structure from latent fraud truth.
- Prasetya et al., [*A multi-rounded adversarial scenario for graph-based promo fraud detection*](https://doi.org/10.1007/s13278-025-01566-0) *(paper)* - informed evolving, multi-round difficulty and adversarial graph stress controls.
- Fan et al., [*Fraud learns too*](https://doi.org/10.1038/s41598-026-60997-7) *(paper)* - informed strategic drift and changing fraud-network structure as future-facing stress dimensions.
- [GRAD: Guided Relation Diffusion Generation for Graph Augmentation in Graph Fraud Detection](https://doi.org/10.1145/3696410.3714520) *(paper)* - informed relation-level augmentation and benign-looking graph support events.
- [Fraud detection based on GNNs with local augmentation and adaptive relation aggregation](https://doi.org/10.1016/j.eswa.2025.130110) *(paper)* - informed the distinction between feature camouflage and relation camouflage controls.
- [FRAUDAR: Graph-based fraud detection in the face of camouflage](https://doi.org/10.1145/2939672.2939747) *(paper)* - provided the foundational relation-camouflage threat model for making fraudulent structure resemble legitimate activity.

## Engineering references

## Reference calibration

- [SDV data quality reports](https://docs.sdv.dev/sdv/multi-table-data/evaluation/data-quality) and [SDMetrics quality reports](https://docs.sdv.dev/sdmetrics/data-metrics/quality/quality-report) - informed aggregate distribution, pair-trend, cardinality, and fidelity-report concepts. FraudTwin stores deterministic summaries rather than source rows.
- [PaySim](https://github.com/EdgarLopezPhD/PaySim) - informed aggregate calibration of simulation parameters while retaining domain-specific causal payment rules.

## Advanced Campaign Dynamics references

- Prasetya et al., [*A multi-rounded adversarial scenario for graph-based promo fraud detection*](https://doi.org/10.1007/s13278-025-01566-0) - evolving campaign structure.
- Fan et al., [*Fraud learns too*](https://doi.org/10.1038/s41598-026-60997-7) - structural drift and perturbation concepts.
- Haghighi et al., [*Beyond pairwise relationships*](https://doi.org/10.1007/s10115-025-02476-5) - higher-order fraud relationships.

- [Python Packaging User Guide](https://packaging.python.org/en/latest/), [Pydantic documentation](https://docs.pydantic.dev/latest/), and [pytest documentation](https://docs.pytest.org/en/stable/) - informed package layout, typed configuration validation, and deterministic regression testing.
