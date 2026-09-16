Data and machine learning
=========================

These APIs preserve event-time and label-availability boundaries when building
historical datasets, replaying runs, backtesting, or evaluating predictions.

Point-in-time datasets
----------------------

.. autosummary::
   :nosignatures:

   fraudtwin.ml.dataset.PointInTimeDataset
   fraudtwin.ml.dataset.PointInTimeDatasetBuilder
   fraudtwin.ml.dataset.build_point_in_time_dataset
   fraudtwin.ml.dataset.write_point_in_time_dataset
   fraudtwin.ml.dataset.load_generated_run

Backtesting and prediction evaluation
--------------------------------------

.. autosummary::
   :nosignatures:

   fraudtwin.ml.backtest.BacktestResult
   fraudtwin.ml.backtest.run_backtest
   fraudtwin.ml.backtest.run_model_backtest
   fraudtwin.ml.backtest.write_backtest
   fraudtwin.ml.baseline.PredictionRecord
   fraudtwin.ml.baseline.EvaluationResult
   fraudtwin.ml.baseline.evaluate_predictions
   fraudtwin.ml.baseline.train_baselines
