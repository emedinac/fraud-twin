Benchmarks, quality, and observability
======================================

These APIs describe reproducible benchmark suites, generator-quality reports,
and optional production metrics.

Benchmarks
----------

.. autosummary::
   :nosignatures:

   fraudtwin.benchmark.run_benchmark
   fraudtwin.benchmark.run_public_benchmark
   fraudtwin.benchmark.build_suite_config
   fraudtwin.list_public_packs
   fraudtwin.load_public_pack
   fraudtwin.PublicBenchmarkPack

Generator quality
-----------------

.. autosummary::
   :nosignatures:

   fraudtwin.run_quality_benchmark
   fraudtwin.report_run
   fraudtwin.load_quality_profile
   fraudtwin.QualityAdapterMetadata
   fraudtwin.QualityAdapterRequest
   fraudtwin.QualityArtifactBundle
   fraudtwin.QualityBenchmarkProfile
   fraudtwin.QualityBenchmarkRequest
   fraudtwin.QualityBenchmarkResult
   fraudtwin.QualityCandidateReport
   fraudtwin.QualityCapability
   fraudtwin.QualityGeneratorAdapter
   fraudtwin.QualityMetric

Observability
-------------

.. autosummary::
   :nosignatures:

   fraudtwin.observability.MetricsSession
   fraudtwin.observability.ObservabilityDependencyError
