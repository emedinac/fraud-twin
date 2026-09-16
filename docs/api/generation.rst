Generation
==========

The generation API returns typed in-memory objects by default and can persist
the same deterministic run when ``write=True``.

``generate(..., write=False)`` returns :class:`fraudtwin.GeneratedData`, whose
``entities`` and ``behavior`` attributes are available for graph and dataset
workflows. ``generate(..., write=True)`` returns
:class:`fraudtwin.GeneratedRun`, a lightweight manifest/path object suitable for
large runs. Call ``run.load_data()`` when a persisted run must be loaded back
into typed domain records. If a point-in-time dataset is enabled, use
``data.require_dataset()`` to narrow the optional dataset and enable IDE
completion for ``rows``, ``count``, and ``frame``.

.. autosummary::
   :nosignatures:

   fraudtwin.generate
   fraudtwin.generate_scale
   fraudtwin.iter_scale_run
   fraudtwin.iter_scale_records
   fraudtwin.resume_generation
   fraudtwin.GeneratedData
   fraudtwin.GeneratedRun

Large-scale iteration
---------------------

.. autosummary::
   :nosignatures:

   fraudtwin.generate_scale
   fraudtwin.iter_scale_run
   fraudtwin.iter_scale_records
   fraudtwin.run_scale_benchmark
   fraudtwin.write_scale_benchmark_manifest
