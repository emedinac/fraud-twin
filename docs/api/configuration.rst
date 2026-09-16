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
