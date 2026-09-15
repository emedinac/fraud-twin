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
