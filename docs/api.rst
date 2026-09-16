Python API reference
====================

This is the curated reference for supported FraudTwin Python APIs. Signatures
and member lists are generated from the installed source; implementation
helpers and private names are intentionally omitted.

API status at a glance
----------------------

.. list-table::
   :header-rows: 1
   :widths: 24 18 58

   * - Area
     - Status
     - Use it for
   * - Generation and configuration
     - Stable
     - Deterministic runs, YAML loading, validation, and run identity.
   * - Data, ML, and graph
     - Stable
     - Point-in-time datasets, replay, evaluation, graph export, and PyG conversion.
   * - Calibration and benchmarks
     - Experimental
     - Reference-data fitting and stress suites whose interfaces may evolve between releases.
   * - PostgreSQL, Kafka, and Iceberg
     - Optional
     - Install the matching extra before importing an integration adapter.

Every detailed page starts with a symbol summary and then expands the public
objects with signatures, typed parameters, return values, exceptions, and
source links. A symbol marked experimental or optional is still supported, but
should be pinned to a documentation version in production integrations.

.. toctree::
   :maxdepth: 2

   api/generation
   api/configuration
   api/data-ml
   api/graph
   api/advanced
   api/integrations
   api/quality

Import the stable high-level API from :mod:`fraudtwin` whenever possible. The
module pages identify optional dependencies and the output objects returned by
each operation.
