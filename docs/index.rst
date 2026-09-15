FraudTwin
=========

FraudTwin is a deterministic payment-world simulator for fraud and ML
experiments.

Hello, FraudTwin
----------------

Install the documentation dependencies and build this site with::

   poetry install --with docs
   poetry run sphinx-build -b html docs docs/_build/html

Then open ``docs/_build/html/index.html`` in a browser. The smallest Python
example generates a run in memory::

   from fraudtwin import generate

   run = generate()
   print(f"Hello from FraudTwin: {run.run_id}")
   print(run.entities.counts)

The generated data is deterministic for the same configuration and seed.

Documentation
-------------

.. toctree::
   :maxdepth: 2

   api
   README
   quickstart
   configuration
   workflows
   graph-and-benchmarks
   release-readiness
   development
   references
