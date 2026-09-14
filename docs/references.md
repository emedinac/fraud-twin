# References and related work

FraudTwin is released under the [Apache License 2.0](../LICENSE).

These references informed FraudTwin’s design. The repository does not bundle copied proprietary source code or external datasets; generated records are synthetic and produced by FraudTwin. Third-party dependencies and referenced works remain subject to their own licenses and terms.

## Synthetic data and fraud workflows

- [Fraud Detection Handbook simulator](https://fraud-detection-handbook.github.io/fraud-detection-handbook/Chapter_3_GettingStarted/SimulatedDataset.html) *(handbook)* — informed deterministic customer profiles, temporal payment behavior, rule-based fraud scenarios, and the legitimate baseline used for hard negatives.
- [Fraud Detection Handbook validation strategies](https://fraud-detection-handbook.github.io/fraud-detection-handbook/Chapter_5_ModelValidationAndSelection/ValidationStrategies.html) *(handbook)* — informed point-in-time features, delayed-label gaps, future-only evaluation windows, and rolling backtests.
- [PaySim](https://github.com/EdgarLopezPhD/PaySim) *(repository and simulator)* — provided comparison points for synthetic mobile-money behavior and aggregate calibration without importing its schemas or data.
- [SynthFin Core](https://github.com/afborda/synthfin-core) *(repository)* — provided practical comparison points for behavioral enrichment, fraud scenarios, and fraud-pattern generation.

## Payment and financial references

- [TigerBeetle financial accounting](https://docs.tigerbeetle.com/coding/financial-accounting/) and [two-phase transfers](https://docs.tigerbeetle.com/coding/two-phase-transfers/) *(technical references)* — informed double-entry ledger invariants and authorization-hold, post, and void semantics.
- [Stripe manual capture](https://docs.stripe.com/payments/place-a-hold-on-a-payment-method), [PaymentIntent lifecycle](https://docs.stripe.com/payments/paymentintents/lifecycle), and [disputes](https://docs.stripe.com/disputes/how-disputes-work) *(public API documentation)* — informed the card authorization, capture, reversal, refund, and chargeback lifecycle abstractions.
- [Banco Central do Brasil Pix initiation standards](https://www.bcb.gov.br/estabilidadefinanceira/exibenormativo?numero=769&tipo=Instru%C3%A7%C3%A3o+Normativa+BCB), [Pix/SPI technical documents](https://www.bcb.gov.br/estabilidadefinanceira/comunicacaodados), and the [Pix timing manual](https://www.bcb.gov.br/estabilidadefinanceira/exibenormativo?numero=654&tipo=Instru%C3%A7%C3%A3o+Normativa+BCB) *(official standards)* — informed the Pix-like lifecycle and timing model without claiming to reproduce proprietary SPI internals.

## Graph fraud and research papers

- [Santander Gen-Fraud-Graph](https://github.com/SantanderAI/gen-fraud-graph) *(repository)* — informed reproducible financial graph generation, fraud-ring patterns, graph exports, and benchmark-oriented scale considerations.
- [IBM AMLSim](https://github.com/IBM/AMLSim) *(repository)* — provided a comparison point for multi-agent synthetic banking graphs and known AML/fraud patterns.
- Haghighi et al., [*Beyond pairwise relationships: a transformer-based hypergraph learning approach for fraud detection*](https://doi.org/10.1007/s10115-025-02476-5) *(paper)* — motivated higher-order graph relations and the separation of observable structure from latent fraud truth.
- Prasetya et al., [*A multi-rounded adversarial scenario for graph-based promo fraud detection*](https://doi.org/10.1007/s13278-025-01566-0) *(paper)* — informed evolving, multi-round difficulty and adversarial graph stress controls.
- Fan et al., [*Fraud learns too*](https://doi.org/10.1038/s41598-026-60997-7) *(paper)* — informed strategic drift and changing fraud-network structure as future-facing stress dimensions.
- [GRAD: Guided Relation Diffusion Generation for Graph Augmentation in Graph Fraud Detection](https://doi.org/10.1145/3696410.3714520) *(paper)* — informed relation-level augmentation and benign-looking graph support events.
- [Fraud detection based on GNNs with local augmentation and adaptive relation aggregation](https://doi.org/10.1016/j.eswa.2025.130110) *(paper)* — informed the distinction between feature camouflage and relation camouflage controls.
- [FRAUDAR: Graph-based fraud detection in the face of camouflage](https://doi.org/10.1145/2939672.2939747) *(paper)* — provided the foundational relation-camouflage threat model for making fraudulent structure resemble legitimate activity.

## Engineering references

- [Python Packaging User Guide](https://packaging.python.org/en/latest/), [Pydantic documentation](https://docs.pydantic.dev/latest/), and [pytest documentation](https://docs.pytest.org/en/stable/) — informed package layout, typed configuration validation, and deterministic regression testing.
