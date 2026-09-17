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

Output directory layout
-----------------------

Persisted runs use this stable layout. Optional directories are omitted when
their configuration section is disabled.

.. code-block:: text

   RUN-.../
   ├── manifest.json
   ├── entities/{customers,accounts,cards,merchants,devices,institutions}.parquet
   ├── payments/{payments,payment_events}.parquet
   ├── ledger/ledger_entries.parquet
   ├── fraud/{fraud_records,alerts,cases}.parquet
   ├── labels/{observed_labels,label_history}.parquet
   ├── ml/dataset.parquet
   ├── graph/{nodes,edges,evidence}.parquet
   ├── scale/checkpoint.json
   ├── kafka-chaos/{manifest.json,envelopes.jsonl}
   └── schemas/...

The manifest is the join point for every artifact. It records the effective
configuration, package/schema versions, seed, row counts, and fingerprints.
Do not compare files from different runs without first comparing their source
run and configuration fingerprints.

Core column contracts
---------------------

The following tables document the stable columns used by the most common
workflows. Parquet types are represented using Polars terminology; nullable
columns are marked ``yes``. The schema files and generated model reference are
authoritative if a table and a release differ.

Payments
~~~~~~~~

.. list-table:: ``payments/payments.parquet``
   :header-rows: 1

   * - Column
     - Type
     - Nullable
     - Meaning and timing
   * - ``payment_id``
     - ``Utf8``
     - no
     - Stable business identity for the payment.
   * - ``customer_id`` / ``account_id``
     - ``Utf8``
     - no
     - Payer ownership at payment creation.
   * - ``payee_account_id`` / ``merchant_id``
     - ``Utf8``
     - yes
     - Destination identity when the rail provides it.
   * - ``payment_rail``
     - ``Utf8``
     - no
     - Configured rail such as ``CARD`` or ``PIX``.
   * - ``amount``
     - ``Float64``
     - no
     - Positive currency amount in the generated currency.
   * - ``currency``
     - ``Utf8``
     - no
     - Currency code associated with ``amount``.
   * - ``created_at`` / ``event_time``
     - ``Datetime``
     - no
     - Business time; it is not an ingestion timestamp.
   * - ``source_available_at``
     - ``Datetime``
     - no
     - Earliest operational availability boundary.

Payment lifecycle events
~~~~~~~~~~~~~~~~~~~~~~~~

.. list-table:: ``payments/payment_events.parquet``
   :header-rows: 1

   * - Column
     - Type
     - Nullable
     - Meaning
   * - ``event_id``
     - ``Utf8``
     - no
     - Stable event identity used for deduplication.
   * - ``payment_id``
     - ``Utf8``
     - no
     - Parent payment identity.
   * - ``event_type``
     - ``Utf8``
     - no
     - Lifecycle state such as authorization, settlement, refund, or reversal.
   * - ``event_time``
     - ``Datetime``
     - no
     - Domain occurrence time.
   * - ``source_created_at``
     - ``Datetime``
     - no
     - Source-system creation time.
   * - ``source_available_at``
     - ``Datetime``
     - no
     - Time at which a point-in-time consumer may observe the event.
   * - ``ingested_at`` / ``processed_at``
     - ``Datetime``
     - no
     - Transport and processing timestamps; these may differ from event time.

Ledger and labels
~~~~~~~~~~~~~~~~~

Ledger rows are one posting per financial movement. ``payment_id`` and
``entry_id`` identify the business movement and posting; ``account_id``,
``direction``, ``amount``, ``currency``, and ``posted_at`` describe the
double-entry leg. A valid settled movement balances debits and credits.

Observed label rows are versioned by ``label_id``/``label_version`` and carry
``label_available_at``. ``label`` describes the observed classification,
whereas oracle fraud records can contain latent scenario and campaign truth.
Point-in-time datasets may use only versions whose availability is no later
than ``prediction_time``.

.. list-table:: Entity tables (common columns)
   :header-rows: 1

   * - Artifact
     - Key columns
     - Timing/visibility
   * - ``entities/customers.parquet``
     - ``customer_id`` (required), profile attributes
     - Entity snapshot; observable unless marked oracle-only
   * - ``entities/accounts.parquet``
     - ``account_id`` (required), ``customer_id``, ``institution_id``
     - Parent references must resolve in the same run
   * - ``entities/cards.parquet``
     - ``card_id`` (required), ``account_id``, lifecycle state
     - Card lifecycle timestamps define availability
   * - ``entities/merchants.parquet``
     - ``merchant_id`` (required), category, geography
     - Merchant attributes available from source creation
   * - ``entities/devices.parquet``
     - ``device_id`` (required), customer/account association
     - Device history is observable only up to the selected cutoff
   * - ``entities/institutions.parquet``
     - ``institution_id`` (required), institution attributes
     - Stable reference data for account and transfer relationships

.. list-table:: Fraud, workflow, and label artifacts
   :header-rows: 1

   * - Artifact
     - Required identity/grain
     - Oracle boundary
   * - ``fraud/fraud_records.parquet``
     - ``fraud_record_id``, ``payment_id``, scenario, attempt time
     - Scenario truth and campaign membership are oracle-only
   * - ``fraud/fraud_alerts.parquet``
     - ``alert_id``, payment/entity ID, created time, status
     - Operational workflow; no latent truth required
   * - ``fraud/fraud_cases.parquet``
     - ``case_id``, alert ID, opened/closed timestamps
     - Operational case lifecycle
   * - ``labels/observed_labels.parquet``
     - label identity/version and ``label_available_at``
     - Observable label projection; may be unresolved or corrected
   * - ``labels/label_history.parquet``
     - all label versions and corrections
     - Oracle/audit sidecar; never use as an online feature

Scale checkpoints contain resolved configuration and seed-tree fingerprints,
shard/chunk descriptors, completed ranges, row counts, and checksums. A
checkpoint is valid only with the same package/schema compatibility policy; do
not edit it manually or mix it with a different configuration.

ML and graph contracts
~~~~~~~~~~~~~~~~~~~~~~

``ml/dataset.parquet`` has one row per prediction entity/time and includes the
entity key, ``prediction_time``, feature columns, label state, split name, and
source/run fingerprints. Feature values must be available at prediction time;
future labels and oracle-only campaign fields are excluded from the observable
view.

Graph tables preserve provenance. Nodes have a stable ``node_id`` and entity
type; edges have source/target IDs, relation type, ``event_time``, and source
event/payment IDs. ``as_of`` and interval filters define temporal visibility.
Oracle evidence and hyperedge sidecars are evaluation artifacts, not
operational features.

Avro and transport contracts
----------------------------

Avro schemas live under ``contracts/avro/<subject>/<version>.avsc`` and the
registry metadata is in ``contracts/avro/registry.yaml``. The observable
``payment-event`` contract includes required identity, timing, producer, rail,
and amount fields plus nullable scenario/entity references. Timestamps are
UTC ``timestamp-micros`` values and amounts use decimal precision 18/scale 2.

New optional fields require reader defaults and must remain ``FULL_TRANSITIVE``
compatible. A breaking change uses a new major subject and leaves the previous
subject immutable. Validate before publication:

.. code-block:: console

   $ fraudtwin schema validate

Drift and chaos reports
-----------------------

Drift reports record reference/comparison window names, input fingerprints,
sample counts, metric method, threshold, alert status, and label policy. Chaos
``kafka-chaos/manifest.json`` records seed/configuration, input/output fingerprints, envelope
identity, topic/partition, attempt, scheduled/delivered timestamps, and counts
for dropped, retried, duplicated, delayed, reordered, late, and deduplicated
records. These report schemas are experimental and must be version-pinned when
used in an automated alert pipeline.

Contract verification checklist
-------------------------------

Before sharing an artifact, verify:

1. the manifest references the intended package and contract versions;
2. row counts and fingerprints match the generated files;
3. business IDs are unique at their documented grain;
4. timestamps include timezone and availability semantics;
5. observable outputs do not contain oracle-only truth;
6. Avro fingerprints and compatibility checks pass;
7. the output directory contains no credentials or service state.
