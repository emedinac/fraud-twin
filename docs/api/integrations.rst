Integrations
============

Integration modules are optional adapters. Core generation remains dependency
free; install the corresponding extra before using a sink or publisher.

.. note::

   These adapters are versioned with the core package, but their third-party
   services are not. Pin the client extra and validate connectivity in CI
   before publishing a run.

PostgreSQL
----------

.. autosummary::
   :nosignatures:

   fraudtwin.postgres.persist_run
   fraudtwin.postgres.persist_scale_records
   fraudtwin.postgres.migrate_database
   fraudtwin.postgres.database_status
   fraudtwin.postgres.PostgresPersistenceResult

Kafka and lakehouse
-------------------

.. autosummary::
   :nosignatures:

   fraudtwin.kafka.KafkaPublisher
   fraudtwin.kafka.publication_records
   fraudtwin.kafka.topic_for
   fraudtwin.lakehouse.IcebergLakehouse
   fraudtwin.lakehouse.materialize_run
   fraudtwin.lakehouse.materialize_dataset
   fraudtwin.lakehouse.verify_materialization

Kafka reliability
~~~~~~~~~~~~~~~~~

The chaos harness models logical delivery semantics without changing event
identity or payload data. It is useful for testing retry, loss, duplication,
delay, reordering, and partition-skew handling before connecting a broker.

.. autosummary::
   :nosignatures:

   fraudtwin.kafka_chaos.KafkaChaosConfig
   fraudtwin.kafka_chaos.ChaosEnvelope
   fraudtwin.kafka_chaos.KafkaChaosResult
   fraudtwin.kafka_chaos.simulate_delivery

.. code-block:: python

   from fraudtwin.kafka_chaos import KafkaChaosConfig, simulate_delivery

   result = simulate_delivery(records, KafkaChaosConfig(seed=7, duplicate_probability=0.1))
   print(result.manifest["counts"])

Replay and scale
----------------

.. autosummary::
   :nosignatures:

   fraudtwin.replay.replay_run
   fraudtwin.replay.write_replay
   fraudtwin.scale.resolve_scale_plan
   fraudtwin.scale.require_scale_plan
   fraudtwin.scale.iter_chunks
   fraudtwin.scale.write_checkpoint
   fraudtwin.scale.load_checkpoint
   fraudtwin.scale.reconcile_logical_ids
   fraudtwin.scale.aggregate_fingerprint
   fraudtwin.scale.chunk_payment_ranges
   fraudtwin.scale.iter_partition_rows
   fraudtwin.scale.iter_partition_query
   fraudtwin.scale.iter_payment_ranges
   fraudtwin.scale.run_scale_benchmark
   fraudtwin.scale.write_scale_benchmark_manifest
   fraudtwin.storage.FsspecScaleStorage
   fraudtwin.storage.LocalScaleStorage
   fraudtwin.storage.storage_for
