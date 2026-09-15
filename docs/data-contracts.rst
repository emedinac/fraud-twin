Data contracts
==============

Generated runs are portable Parquet artifacts with a JSON manifest. The
manifest records the effective configuration, seed, schema versions, counts,
and fingerprints needed to reproduce or audit a run.

.. list-table:: Core artifacts
   :header-rows: 1

   * - Artifact
     - Grain
     - Purpose
   * - ``entities/*.parquet``
     - One row per entity
     - Customers, accounts, cards, merchants, devices, and institutions.
   * - ``payments/payments.parquet``
     - One row per payment
     - Business payment identity, parties, rail, amount, and timestamps.
   * - ``payments/payment_events.parquet``
     - One row per lifecycle event
     - Authorization, settlement, reversal, refund, and return events.
   * - ``ledger/ledger_entries.parquet``
     - One row per ledger posting
     - Balanced financial postings for every settled movement.
   * - ``fraud/fraud_records.parquet``
     - One row per fraud attempt
     - Scenario and campaign truth retained in the oracle boundary.
   * - ``ml/dataset.parquet``
     - One row per prediction entity/time
     - Point-in-time-safe features, labels, and split metadata.

The operational view never exposes future labels or latent campaign links.
Oracle sidecars are explicitly named and should be used only for evaluation or
audit. The Avro payment-event contracts are versioned under
``contracts/avro/payment-event`` and are validated by the schema registry
commands described in :doc:`cli`.
