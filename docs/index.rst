FraudTwin
=========

.. container:: fraudtwin-hero

   FraudTwin is a deterministic payment-world simulator for fraud detection,
   graph analysis, data engineering, and machine-learning experiments.

   The same configuration and seed produce the same entities, events, labels,
   and output fingerprints, so difficult fraud cases become explainable,
   testable, and reproducible.

   .. button-ref:: quickstart
      :color: primary
      :shadow:

      Generate your first run

Install and run
---------------

.. code-block:: console

   $ poetry install
   $ poetry run fraudtwin config validate configs/minimal.yaml
   $ poetry run fraudtwin generate configs/minimal.yaml --output-dir /tmp/fraudtwin-run

Choose your path
----------------

.. grid:: 2
   :gutter: 3

   .. grid-item-card:: Generate and inspect
      :link: quickstart
      :link-type: doc
      :class-card: sd-border-0

      Start with a small deterministic payment world and inspect the generated
      entities, payments, lifecycle events, ledger, and manifest.

   .. grid-item-card:: Build an ML dataset
      :link: workflows
      :link-type: doc
      :class-card: sd-border-0

      Build point-in-time-safe features, labels, replay windows, and rolling
      backtests without leaking future information.

   .. grid-item-card:: Explore graph fraud
      :link: graph-and-benchmarks
      :link-type: doc
      :class-card: sd-border-0

      Export observable and oracle graph views to Neo4j or PyTorch Geometric
      while preserving event provenance.

   .. grid-item-card:: Use the Python API
      :link: api
      :link-type: doc
      :class-card: sd-border-0

      Find signatures, parameters, return objects, source links, optional
      dependencies, and configuration schemas in the reference manual.

How a run becomes an experiment
-------------------------------

.. container:: fraudtwin-flow

   configuration → entities → behavior → payments → fraud/workflow → datasets / graphs / benchmarks

Each layer keeps stable identities and causal timestamps. Operational views
contain only information available at the selected time; oracle artifacts keep
the complete explanation for evaluation and audit.

Documentation
-------------

.. toctree::
   :maxdepth: 2
   :caption: Getting started

   quickstart
   tutorials

.. toctree::
   :maxdepth: 2
   :caption: Guides

   configuration
   workflows
   model-lifecycle
   production-serving
   drift-and-shift
   kafka-reliability
   data-quality-incidents
   graph-and-benchmarks
   release-readiness

.. toctree::
   :maxdepth: 2
   :caption: Concepts

   concepts

.. toctree::
   :maxdepth: 2
   :caption: Reference

   api
   configuration-reference
   cli
   data-contracts

.. toctree::
   :maxdepth: 1
   :caption: Resources

   glossary
   troubleshooting
   migration
   compatibility
   development
   references

.. toctree::
   :hidden:

   README
