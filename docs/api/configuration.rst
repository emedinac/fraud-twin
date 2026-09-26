Configuration
=============

Configuration is strict, typed, and part of a run's identity. Unknown fields
and invalid ranges fail validation before generation.

.. config-model:: fraudtwin.config.SimulationRunConfig

Loading and hashing
-------------------

.. autosummary::
   :nosignatures:

   fraudtwin.config.load_config
   fraudtwin.config.load_default_config
   fraudtwin.config.config_hash

Configuration model classes
---------------------------

.. autosummary::
   :nosignatures:

   fraudtwin.config.SimulationRunConfig
   fraudtwin.config.SimulationConfig
   fraudtwin.config.PopulationConfig
   fraudtwin.config.PaymentsConfig
   fraudtwin.config.FraudConfig
   fraudtwin.config.PointInTimeDatasetConfig
   fraudtwin.config.GraphConfig
   fraudtwin.config.BenchmarkConfig

Fraud prevalence semantics
--------------------------

Fraud generation is campaign-based. In a standard run, the selected campaign
count is bounded by ``scenario_count``, ``floor(baseline_payment_count *
target_rate)``, and the sum of the configured per-scenario ``count`` values for
enabled scenarios. ``target_rate`` is therefore a campaign budget rather than
a direct percentage of final payment rows. F03 produces two transfer payments
per campaign, while F04 produces one PIX payment per campaign; other scenarios
can have different attempt counts. The five built-in stories are:

* ``F01`` Card Not Present: three card payments per campaign.
* ``F02`` Card Testing: repeated card attempts using ``attempt_count``.
* ``F03`` Account Takeover: two account-transfer payments per campaign.
* ``F04`` Instant-Payment Scam: one PIX payment per campaign.
* ``F05`` Velocity Attack: repeated card attempts within ``window_seconds``.

Card scenarios require active cards; F03 requires account relationships; and
F04 requires eligible PIX accounts and keys. See the detailed
:doc:`/configuration` scenario guide for the complete field meanings and
mergeable examples.

F/P/C identifiers
-----------------

FraudTwin uses ``F##`` for fraud scenarios, ``P##`` for generation protocols,
and ``C##`` for capacities and generation constraints. ``F04`` identifies the
instant-payment-scam scenario; ``P08`` identifies the protocol that implements
it; and ``C04`` identifies the ledger debit capacity that can stop materialization.
These identifiers are metadata and do not replace YAML keys or change run
hashes. See :doc:`/vocabulary` for the complete tables, defaults, relationships,
and examples.

The protocol lookup accepts legacy milestone aliases: M6 maps to P01, M12 to
P02, M14 to P03, and M15 to P04. Existing configuration and manifest fields
keep their names.

``hard_negative_rate`` adds legitimate lookalike records and does not increase
true fraud. Source records, observed labels, and point-in-time dataset rows
must be measured separately. See the complete
``examples/configuration/f04-half-fraud.yaml`` example and the
:doc:`/configuration` guide for worked calculations.

Vocabulary API
--------------

The stable metadata vocabulary is available without loading a YAML file. Use
``F##`` identifiers for scenarios, ``P##`` identifiers for protocols, and
``C##`` identifiers for capacities:

.. autosummary::
   :nosignatures:

   fraudtwin.vocabulary.get_scenario
   fraudtwin.vocabulary.get_protocol
   fraudtwin.vocabulary.get_capacity

The public registries are ``fraudtwin.FRAUD_SCENARIOS``,
``fraudtwin.PROTOCOLS``, and ``fraudtwin.CAPACITIES``. They describe names,
relationships, default settings, and compatibility aliases.

Generation errors
------------------

Known failures raised while materializing a valid configuration are exposed as
typed ``ValueError`` subclasses. Catch ``fraudtwin.errors.GenerationError`` in
applications and inspect ``error.code``, ``error.stage``, ``error.protocol_id``,
``error.capacity_id``, and ``error.scenario_id`` (metadata can be ``None``). Ledger failures
are represented by ``fraudtwin.errors.LedgerCapacityError`` and include the
affected account, payment, event, debit amount, running balances, and
overdraft limit. The CLI prints the same information in an actionable report;
see :doc:`/troubleshooting`.

.. autosummary::
   :nosignatures:

   fraudtwin.errors.GenerationError
   fraudtwin.errors.LedgerCapacityError
