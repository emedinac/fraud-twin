FraudTwin
=========

FraudTwin is a deterministic payment-world simulator for fraud detection,
graph analysis, data engineering, and machine-learning experiments. The same
configuration and seed produce the same entities, events, labels, and output
fingerprints, so experiments can be explained and reproduced.

Start here
----------

Install FraudTwin with Poetry and generate a first run in a few minutes:

.. code-block:: console

   $ poetry install
   $ poetry run fraudtwin config validate configs/minimal.yaml
   $ poetry run fraudtwin generate configs/minimal.yaml --output-dir /tmp/fraudtwin-run

Then choose a path:

* **Learn by doing:** follow the :doc:`tutorials` notebooks.
* **Build an experiment:** read :doc:`configuration`, :doc:`workflows`, and
  :doc:`graph-and-benchmarks`.
* **Integrate the library:** use the :doc:`api` and :doc:`cli` references.

Documentation
-------------

.. toctree::
   :maxdepth: 2
   :caption: Get started

   quickstart
   tutorials

.. toctree::
   :maxdepth: 2
   :caption: Guides

   configuration
   workflows
   graph-and-benchmarks
   release-readiness

.. toctree::
   :maxdepth: 2
   :caption: Reference

   api
   cli
   data-contracts

.. toctree::
   :maxdepth: 1
   :caption: Project

   development
   references

.. toctree::
   :hidden:

   README

Deterministic by design
-----------------------

Every run starts with entities and customer behavior, produces legitimate
payments and lifecycle events, and can layer on fraud, workflow, quality
faults, graph structure, or benchmark stress. Operational views intentionally
exclude future and oracle-only information; manifests retain the lineage and
fingerprints required to audit a result.
