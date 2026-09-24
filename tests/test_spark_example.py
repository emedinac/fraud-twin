"""Bounded contract tests for the optional Spark reference application."""

import importlib.util
from pathlib import Path


def _module():
    path = Path("examples/spark-streaming/spark_streaming.py")
    spec = importlib.util.spec_from_file_location("fraudtwin_spark_example", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_spark_example_is_bounded_and_contract_backed() -> None:
    module = _module()
    assert module.WATERMARK == "10 minutes"
    assert module.WINDOW == "5 minutes"
    assert len(module._contract_fingerprint()) == 64
    args = module._parser().parse_args(
        [
            "--source",
            "parquet",
            "--input",
            "events",
            "--output",
            "out",
            "--checkpoint",
            "checkpoint",
        ]
    )
    assert args.trigger == "available-now"
