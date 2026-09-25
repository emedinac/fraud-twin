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

Feature labels
--------------

Advanced documentation and manifests may also use labels such as ``M6``,
``M12``, ``M14``, and ``M15``. These are release or feature milestones, not
values to place under ``fraud.scenarios``:

* ``M6`` is the core fraud campaign workflow.
* ``M12`` is fraud difficulty and scenario similarity.
* ``M14`` is counterfactual generation.
* ``M15`` is campaign dynamics and evolving campaigns.

Configure those capabilities through their own top-level sections, such as
``difficulty``, ``counterfactual``, and ``campaign_dynamics``. Use ``F01`` to
``F05`` when selecting one of the five built-in payment-fraud stories.

``hard_negative_rate`` adds legitimate lookalike records and does not increase
true fraud. Source records, observed labels, and point-in-time dataset rows
must be measured separately. See the complete
``examples/configuration/f04-half-fraud.yaml`` example and the
:doc:`/configuration` guide for worked calculations.

Generation errors
------------------

Known failures raised while materializing a valid configuration are exposed as
typed ``ValueError`` subclasses. Catch ``fraudtwin.errors.GenerationError`` in
applications and inspect ``error.code`` and ``error.stage``. Ledger failures
are represented by ``fraudtwin.errors.LedgerCapacityError`` and include the
affected account, payment, event, debit amount, running balances, and
overdraft limit. The CLI prints the same information in an actionable report;
see :doc:`/troubleshooting`.

.. autosummary::
   :nosignatures:

   fraudtwin.errors.GenerationError
   fraudtwin.errors.LedgerCapacityError
