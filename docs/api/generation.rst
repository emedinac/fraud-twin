Generation
==========

The generation API returns typed in-memory objects by default and can persist
the same deterministic run when ``write=True``.

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
