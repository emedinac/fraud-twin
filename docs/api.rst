Python API reference
====================

This is the reference for supported FraudTwin Python APIs. The task-oriented
pages below group the most common workflows, while the exhaustive inventory
lists every public export from ``fraudtwin.__all__``. Signatures and member
lists are generated from the installed source; implementation helpers and
private names are intentionally omitted.

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

Module index
------------

The workflow pages below are the module-level entry points. The complete
alphabetized class, exception, function, and constant indexes follow them.

.. list-table::
   :header-rows: 1

   * - Module area
     - Documentation page
   * - ``fraudtwin.generation`` and ``fraudtwin.scale``
     - :doc:`api/generation`
   * - ``fraudtwin.config``
     - :doc:`api/configuration` and :doc:`configuration-reference`
   * - ``fraudtwin.ml``
     - :doc:`api/data-ml`
   * - ``fraudtwin.graph``
     - :doc:`api/graph`
   * - Calibration, difficulty, camouflage, and campaigns
     - :doc:`api/advanced`
   * - PostgreSQL, Kafka, lakehouse, replay, and storage
     - :doc:`api/integrations`
   * - Benchmarks, quality, and observability
     - :doc:`api/quality`
   * - All importable public modules
     - :doc:`api/modules`

.. toctree::
   :maxdepth: 2

   api/generation
   api/configuration
   api/data-ml
   api/graph
   api/advanced
   api/integrations
   api/quality
   api/modules

Import the stable high-level API from :mod:`fraudtwin` whenever possible. The
module pages identify optional dependencies and the output objects returned by
each operation.

Complete public API
-------------------

.. api-inventory:: fraudtwin
