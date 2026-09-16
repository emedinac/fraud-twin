Configuration parameter reference
=================================

This page is the precise parameter manual for the YAML configuration. Tables
are generated from the installed Pydantic models, so defaults, constraints,
required fields, and nested model links stay aligned with validation. Read
:doc:`configuration` first for trade-offs and recipes.

Minimal configuration
---------------------

Use this when learning the lifecycle or checking an installation:

.. tab-set::

   .. tab-item:: YAML

      .. code-block:: yaml

         simulation:
           seed: 42
           start: 2026-01-01T00:00:00Z
           duration_days: 1
           speed: batch
         population:
           customers: 10
           institutions: 3
           accounts: 15
           cards: 12
           merchants: 3
           devices: 12
           pix_keys: 8
         payments:
           daily_target: 100
           rails: {CARD: 0.55, PIX: 0.30, ACCOUNT_TRANSFER: 0.15}

   .. tab-item:: Validate

      .. code-block:: console

         $ fraudtwin config validate configs/minimal.yaml

Top-level model
---------------

.. config-model:: fraudtwin.config.SimulationRunConfig

Major sections
--------------

Each section has a stable anchor and can be linked directly from a guide or a
configuration review:

Simulation and population
~~~~~~~~~~~~~~~~~~~~~~~~~

.. config-model:: fraudtwin.config.SimulationConfig

.. config-model:: fraudtwin.config.PopulationConfig

Payments and fraud
~~~~~~~~~~~~~~~~~~

.. config-model:: fraudtwin.config.PaymentsConfig

.. config-model:: fraudtwin.config.FraudConfig

Labels, datasets, and graphs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. config-model:: fraudtwin.config.FraudWorkflowConfig

.. config-model:: fraudtwin.config.PointInTimeDatasetConfig

.. config-model:: fraudtwin.config.GraphConfig

Scale and benchmarks
~~~~~~~~~~~~~~~~~~~~

.. config-model:: fraudtwin.config.ScaleConfig

.. config-model:: fraudtwin.config.BenchmarkConfig

Outputs and integrations
~~~~~~~~~~~~~~~~~~~~~~~~

.. config-model:: fraudtwin.config.OutputsConfig

.. config-model:: fraudtwin.config.KafkaConfig

.. config-model:: fraudtwin.config.LakehouseConfig

.. note::

   Optional integrations are only validated when their corresponding output
   is enabled. See :doc:`troubleshooting` for missing-extra and connection
   errors.
