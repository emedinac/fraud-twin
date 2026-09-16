CLI reference
=============

The ``fraudtwin`` command exposes the same deterministic workflows as the
Python API. Run ``fraudtwin --help`` or a command's ``--help`` option for the
complete version-specific option list.

.. code-block:: console

   $ fraudtwin --help
   $ fraudtwin generate --help
   $ fraudtwin ml --help
   $ fraudtwin graph --help
   $ fraudtwin benchmark --help
   $ fraudtwin kafka chaos --help

Generation and validation
--------------------------

.. list-table::
   :header-rows: 1

   * - Command
     - Purpose
   * - ``fraudtwin config validate``
     - Validate a YAML configuration before generating data.
   * - ``fraudtwin generate``
     - Generate an in-memory or persisted deterministic run.
   * - ``fraudtwin resume``
     - Resume a scale run from a validated checkpoint.
   * - ``fraudtwin validate-ledger``
     - Check the persisted double-entry ledger invariants.
   * - ``fraudtwin replay``
     - Replay a historical window with source or delivery ordering.

Analysis and integrations
-------------------------

See :doc:`workflows` for complete examples of ``ml build-dataset``,
``ml backtest``, ``ml train``, ``ml evaluate``, graph export/validation,
benchmark runs, and optional PostgreSQL, Kafka, and Iceberg sinks.

Kafka chaos
-----------

``kafka chaos`` transforms a persisted run into deterministic transport
envelopes. It models logical delivery behavior, not physical packet loss.

.. list-table::
   :header-rows: 1

   * - Option
     - Default
     - Meaning
   * - ``--run-id``
     - required
     - Generated run to publish.
   * - ``--boundary``
     - ``producer``
     - Inject before producer delivery or after consumer receipt.
   * - ``--drop-rate`` / ``--duplicate-rate`` / ``--retry-rate``
     - ``0``
     - Independent seeded fault probabilities.
   * - ``--delay-seconds``
     - ``0``
     - Maximum deterministic delivery delay.
   * - ``--reorder-window``
     - ``0``
     - Shuffle envelopes in bounded windows to model out-of-order delivery.
   * - ``--partition-count`` / ``--partition-skew``
     - ``3`` / ``0``
     - Partition assignment and probability of concentrating keys on partition 0.
   * - ``--output-dir``
     - ``runs``
     - Parent directory containing the generated run.

The command writes ``kafka-chaos/manifest.json`` and a hex-encoded
``envelopes.jsonl`` audit stream. See :doc:`kafka-reliability` for topic,
acknowledgement, watermark, and deduplication guidance.
