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
   * - ``fraudtwin config init``
     - Create a project-owned copy of the packaged minimal YAML template.
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

Command map
-----------

The CLI is grouped by the workflow it performs. Every command supports
``--help``; the installed package is authoritative for the exact option list
and defaults.

.. list-table:: Supported command groups
   :header-rows: 1

   * - Group
     - Commands
     - Typical output
   * - Configuration
     - ``config init``, ``config validate``
     - Project template or validation diagnostics; non-zero exit on invalid YAML
   * - Generation
     - ``generate``, ``resume``, ``replay``, ``validate-ledger``, ``report``
     - Run directory, checkpoint, replay, or manifest report
   * - ML
     - ``ml build-dataset``, ``ml train``, ``ml evaluate``, ``ml backtest``
     - PIT dataset, predictions, metrics, and evaluation manifest
   * - Graph
     - ``graph export``, ``graph validate``
     - Nodes/edges, Cypher, and validation report
   * - Benchmarks
     - ``quality-benchmark``, ``scale-benchmark``, ``benchmark run``, ``benchmark describe``, ``benchmark verify``
     - Benchmark artifacts and fingerprints
   * - Domain extensions
     - ``calibrate``, ``counterfactual generate``, ``campaign evolve``
     - Versioned sidecars and profile manifests
   * - Services
     - ``db migrate``, ``db status``, ``lakehouse init``, ``lakehouse ingest-run``, ``lakehouse consume``, ``lakehouse verify``, ``lakehouse maintenance``
     - External projections and health/reconciliation reports
   * - Contracts and transport
     - ``schema validate``, ``kafka chaos``
     - Avro registry report or transport-fault manifest

Common option policy
--------------------

| Option family | Meaning | Safe retry behavior |
| --- | --- | --- |
| ``--config`` | Validated YAML source | Reuse only with the same configuration fingerprint |
| ``--run-id`` | Existing persisted source run | Read-only commands can be repeated |
| ``--output-dir`` | Destination for artifacts | Use a new temporary directory or an idempotent run directory |
| ``--from``/``--to``/``--as-of`` | Half-open event-time or cutoff window | Repeatable when source run is unchanged |
| ``--seed`` | Explicit deterministic seed | Persist it in the manifest |
| ``--workers``/``--checkpoint-dir`` | Scale scheduling and resume state | Never edit a checkpoint by hand |
| integration environment variables | Service endpoint/credentials | Keep secrets out of YAML and manifests |

Exit behavior
-------------

Exit code ``0`` means the requested operation completed and its verification
checks passed. Invalid configuration, missing files, incompatible schemas, or
unhealthy optional services, and known generated-state failures return a
non-zero exit code and an actionable message. Generation failures use the
following format:

.. code-block:: text

   Generation failed [LEDGER_OVERDRAFT_EXCEEDED]

   Stage: baseline payment ledger
   Protocol: P01
   Capacity: C04
   Account: ACC-000228
   Payment: PAY-...
   Event: EVT-...
   Debit: 537.34
   Balance before: 345.11
   Balance after: -192.23
   Allowed overdraft: 100.00

The report also includes the active ``P##`` protocol, failed ``C##`` capacity,
what the error means, possible fixes, and a link to the :doc:`troubleshooting`
guide. Commands do not silently replace an existing
run with a different configuration. Inspect the manifest and fingerprint
before retrying. Unexpected programming errors retain their traceback for
developers.

Examples by audience
--------------------

.. code-block:: console

   # New user: validate and generate a bounded run
   $ fraudtwin config init config.yaml
   $ fraudtwin config validate config.yaml
   $ fraudtwin generate config.yaml --output-dir runs/quickstart

   # Repository checkout: use a tracked fixture
   $ fraudtwin config validate configs/minimal.yaml
   $ fraudtwin generate configs/minimal.yaml --output-dir runs/quickstart

   # Data scientist: create a point-in-time dataset and backtest
   $ fraudtwin ml build-dataset configs/minimal.yaml --run-id RUN-... --output-dir runs
   $ fraudtwin ml backtest configs/minimal.yaml --run-id RUN-... --output-dir runs

   # MLOps engineer: inspect an integration before publishing
   $ fraudtwin db status
   $ fraudtwin schema validate
   $ fraudtwin kafka chaos --run-id RUN-... --boundary consumer --output-dir runs

   # Researcher: verify a public benchmark identity
   $ fraudtwin benchmark describe FT-B04-CAMOUFLAGE@0.34.0
   $ fraudtwin benchmark verify runs/benchmarks/BM-...

The documentation tests compare the registered Typer command names with this
command map. Add a command to this page and its workflow guide in the same
change as the implementation.
