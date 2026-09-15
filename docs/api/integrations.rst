Integrations
============

Integration modules are optional adapters. Core generation remains dependency
free; install the corresponding extra before using a sink or publisher.

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

Replay and scale
----------------

.. autosummary::
   :nosignatures:

   fraudtwin.replay.replay_run
   fraudtwin.replay.write_replay
   fraudtwin.scale.resolve_scale_plan
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
