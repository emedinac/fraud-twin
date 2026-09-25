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

   .. grid-item-card:: Start Here
      :link: quickstart
      :link-type: doc
      :class-card: sd-border-0

      Install FraudTwin, create a small run, and learn what the generated
      manifest and Parquet files mean.

   .. grid-item-card:: Beginner
      :link: levels/beginner
      :link-type: doc
      :class-card: sd-border-0

      Follow a guided path for Python users who are new to data systems and
      optional services.

   .. grid-item-card:: Intermediate
      :link: levels/intermediate
      :link-type: doc
      :class-card: sd-border-0

      Build reproducible datasets, evaluate models, inspect quality, and use
      graph workflows.

   .. grid-item-card:: Expert
      :link: levels/expert
      :link-type: doc
      :class-card: sd-border-0

      Work with contracts, integrations, checkpoints, extensions, operations,
      and release evidence.

   .. grid-item-card:: Reference
      :link: api
      :link-type: doc
      :class-card: sd-border-0

      Find the complete Python API, CLI, configuration, and data-contract
      reference.

Find your path by role
----------------------

.. grid:: 3
   :gutter: 2

   .. grid-item-card:: New users and analysts
      :link: audiences/new-users
      :link-type: doc

      Use the beginner route for a local, offline start.

   .. grid-item-card:: Data and ML
      :link: audiences/data-ml
      :link-type: doc

      Use the intermediate route for datasets, evaluation, and drift.

   .. grid-item-card:: Engineering and MLOps
      :link: audiences/engineering-mlops
      :link-type: doc

      Use the expert route for services, operations, and release boundaries.

How a run becomes an experiment
-------------------------------

.. container:: fraudtwin-flow

   configuration → entities → behavior → payments → fraud/workflow → datasets / graphs / benchmarks

Each layer keeps stable identities and causal timestamps. Operational views
contain only information available at the selected time; oracle artifacts keep
the complete explanation for evaluation and audit.

.. toctree::
   :maxdepth: 2
   :caption: Getting started
   :hidden:

   quickstart
   installation
   learning-paths
   tutorials

.. toctree::
   :hidden:

   tutorials/first-generated-run.ipynb
   tutorials/configure-a-simulation.ipynb
   tutorials/explore-payments-and-lifecycles.ipynb
   tutorials/explore-fraud-and-delayed-labels.ipynb
   tutorials/temporal-payment-lifecycle-visualization.ipynb
   tutorials/fraud-scenarios-difficulty-benchmarks.ipynb
   tutorials/ml-feature-distributions-embeddings.ipynb
   tutorials/from-events-to-ml-dataset.ipynb
   tutorials/stress-test-fraud-scenarios.ipynb
   tutorials/build-a-reproducible-fraud-benchmark.ipynb
   tutorials/train-a-simple-fraud-model.ipynb
   tutorials/train-and-track-fraud-model.ipynb
   tutorials/stress-drift-and-camouflage.ipynb
   tutorials/checkpoint-resume-scale.ipynb
   tutorials/mlflow-model-promotion.ipynb
   tutorials/segmented-drift-analysis.ipynb
   tutorials/neo4j-graph-fraud.ipynb
   tutorials/pyg-graph-model.ipynb
   tutorials/avro-kafka-stream.ipynb
   tutorials/kafka-outage-recovery.ipynb
   tutorials/schema-evolution-compatibility.ipynb
   tutorials/operational-lakehouse-observability.ipynb
   tutorials/lakehouse-observability.ipynb
   tutorials/scale-checkpoint-resume.ipynb
   tutorials/data-quality-repair-replay.ipynb
   tutorials/postgres-persistence-reconciliation.ipynb
   tutorials/iceberg-time-travel-observability.ipynb
   tutorials/calibration-and-counterfactuals.ipynb
   tutorials/campaign-graph-investigation.ipynb
   tutorials/ml-shift-and-backtesting.ipynb
   tutorials/scale-reconciliation-and-reproducibility.ipynb

.. toctree::
   :maxdepth: 1
   :caption: Learn by level
   :hidden:

   levels/beginner
   levels/intermediate
   levels/expert

.. toctree::
   :maxdepth: 1
   :caption: Find by role
   :hidden:

   audiences

.. toctree::
   :maxdepth: 2
   :caption: Guides
   :hidden:

   configuration
   workflows
   ml-evaluation
   model-lifecycle
   production-serving
   integrations
   drift-and-shift
   kafka-reliability
   data-quality-incidents
   graph-and-benchmarks
   release-readiness
   architecture
   verified-capabilities
   spark-streaming
   extensions
   scale-operations
   release-evidence

.. toctree::
   :maxdepth: 2
   :caption: Concepts
   :hidden:

   concepts

.. toctree::
   :maxdepth: 2
   :caption: Reference
   :hidden:

   api
   configuration-reference
   cli
   data-contracts

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
