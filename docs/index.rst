FraudTwin documentation
=======================

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
   $ poetry run fraudtwin generate configs/minimal.yaml --output-dir ./runs

Start here
----------

.. grid:: 3
   :gutter: 3

   .. grid-item-card:: Quickstart
      :link: quickstart
      :link-type: doc
      :class-card: sd-border-0

      Install FraudTwin, create a small run, and learn what the generated
      manifest and Parquet files mean.

   .. grid-item-card:: Tutorials
      :link: tutorials
      :link-type: doc
      :class-card: sd-border-0

      Learn FraudTwin through short, executable paths grouped by workflow.

   .. grid-item-card:: How-to guides
      :link: how-to
      :link-type: doc
      :class-card: sd-border-0

      Configure runs, build datasets, operate integrations, and solve focused
      implementation tasks.

   .. grid-item-card:: Explanation
      :link: concepts
      :link-type: doc
      :class-card: sd-border-0

      Understand determinism, lifecycles, point-in-time data, fraud graphs,
      and data quality.

   .. grid-item-card:: Reference
      :link: api
      :link-type: doc
      :class-card: sd-border-0

      Look up the Python API, configuration, CLI, and data contracts.

Need a guided route?
--------------------

Use :doc:`Choose your path <learning-paths>` if you want a recommended
starting point based on your goal or experience. It is an onboarding aid, not
a second documentation hierarchy.

How a run becomes an experiment
-------------------------------

.. container:: fraudtwin-flow

   configuration → entities → behavior → payments → fraud/workflow → datasets / graphs / benchmarks

Each layer keeps stable identities and causal timestamps. Operational views
contain only information available at the selected time; oracle artifacts keep
the complete explanation for evaluation and audit.

.. toctree::
   :maxdepth: 1
   :caption: Getting started
   :hidden:

   quickstart
   installation
   learning-paths

.. toctree::
   :maxdepth: 1
   :caption: Tutorials
   :hidden:

   tutorials/getting-started
   tutorials/visualization
   tutorials/core-workflows
   tutorials/production-ml
   tutorials/graph-analytics
   tutorials/streaming-reliability
   tutorials/operations
   tutorials/advanced-experiments

.. toctree::
   :maxdepth: 2
   :caption: How-to guides
   :hidden:

   how-to/build-and-evaluate
   how-to/integrate-and-serve
   how-to/operate-and-repair
   how-to/extend-and-release

.. toctree::
   :maxdepth: 1
   :caption: Explanation
   :hidden:

   concepts
   concepts/determinism-and-reproducibility
   concepts/data-model-and-lifecycle
   concepts/point-in-time-data-and-labels
   concepts/fraud-graphs-and-data-quality
   architecture

.. toctree::
   :maxdepth: 2
   :caption: Reference
   :hidden:

   api
   configuration-reference
   cli
   data-contracts
   verified-capabilities

.. toctree::
   :maxdepth: 1
   :caption: Resources
   :hidden:

   glossary
   troubleshooting
   migration
   compatibility
   development
   references

.. toctree::
   :hidden:

   README
