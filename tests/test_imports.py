"""Import smoke checks for modules using type-only and forward references."""

from importlib import import_module

import pytest


@pytest.mark.parametrize(
    "module_name",
    (
        "fraudtwin.benchmark",
        "fraudtwin.counterfactual",
        "fraudtwin.kafka",
        "fraudtwin.kafka_chaos",
        "fraudtwin.lakehouse",
        "fraudtwin.manifest",
        "fraudtwin.ml.backtest",
        "fraudtwin.ml.baseline",
        "fraudtwin.ml.drift",
        "fraudtwin.simulation.behavior",
        "fraudtwin.simulation.graph_fraud",
        "fraudtwin.simulation.parquet",
        "fraudtwin.simulation.quality",
        "fraudtwin.simulation.quality_diagnostics",
    ),
)
def test_modules_import_without_postponed_annotations(module_name: str) -> None:
    import_module(module_name)
