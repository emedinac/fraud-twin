Typed API cookbook
==================

The generated API inventory is the authoritative signature reference. This
page shows the safe object-narrowing patterns that make the public API pleasant
to use from an IDE and from type checkers.

Generate and load a persisted run
----------------------------------

.. code-block:: python

   from pathlib import Path

   from fraudtwin import generate
   from fraudtwin.config import load_config

   config = load_config(Path("configs/minimal-v1.yaml"))
   persisted = generate(config, write=True, output_dir=Path("runs/example"))
   data = persisted.load_data()
   print(len(data.entities.customers), len(data.behavior.payments))

``write=True`` returns :class:`fraudtwin.GeneratedRun`. Call
``load_data()`` before accessing entities or behavior. In-memory generation
returns :class:`fraudtwin.GeneratedData` directly.

Build a point-in-time dataset
-----------------------------

.. code-block:: python

   from fraudtwin import generate
   from fraudtwin.config import SimulationRunConfig

   config = SimulationRunConfig()
   data = generate(config)
   dataset = data.require_dataset()
   frame = dataset.frame
   print(dataset.count, frame.columns)

``require_dataset()`` narrows the optional dataset and gives Pylance/mypy a
non-optional ``frame``. Use the same pattern for optional scale plans with
``require_scale_plan(config)``.

Graph, drift, and Kafka workflows
---------------------------------

.. code-block:: python

   from fraudtwin import build_graph
   from fraudtwin.kafka_chaos import KafkaChaosConfig, simulate_delivery
   from fraudtwin.ml.drift import DriftConfig, compare_windows

   data = generate(config)
   graph = build_graph(config, data.entities, data.behavior, data.manifest, view="observable")
   drift = compare_windows(reference_rows, comparison_rows, DriftConfig(
       reference_name="train", comparison_name="production",
   ))
   chaos = simulate_delivery(records, KafkaChaosConfig(seed=17, drop_probability=0.02))
   print(graph.nodes.height, drift.fingerprint, chaos.manifest["output_fingerprint"])

These APIs preserve stable event identity. Graph exports use an explicit
observable/oracle view; drift reports require a declared label policy; Kafka
chaos changes transport behavior without rewriting domain payload identity.

Which API should I use?
---------------------------

.. list-table::
   :header-rows: 1

   * - Goal
     - Primary API
     - Next reference
   * - Generate a reproducible world
     - ``fraudtwin.generate``
     - :doc:`generation`
   * - Build historical ML rows
     - ``build_point_in_time_dataset``
     - :doc:`data-ml`
   * - Replay or backtest
     - ``replay`` and ``run_backtest``
     - :doc:`data-ml`
   * - Export fraud relationships
     - ``build_graph`` and ``write_graph``
     - :doc:`graph`
   * - Compare windows
     - ``compare_windows`` and ``compare_performance``
     - :doc:`data-ml`
   * - Exercise transport faults
     - ``simulate_delivery``
     - :doc:`integrations`
   * - Scale and resume
     - ``generate_scale`` and ``resume_generation``
     - :doc:`generation`

Optional integrations raise a clear dependency error when their extra is not
installed. Keep the offline path in tests and use the integration pages for
service-backed execution.
