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


class _FakeColumn:
    def __getitem__(self, key: str) -> "_FakeColumn":
        return self

    def __eq__(self, other: object) -> "_FakeColumn":
        return self

    def isNotNull(self) -> "_FakeColumn":
        return self


class _FakeFunctions:
    def map_from_entries(self, value: object) -> object:
        return value

    def expr(self, value: str) -> str:
        return value

    def col(self, value: str) -> _FakeColumn:
        return _FakeColumn()

    def lit(self, value: str) -> str:
        return value


class _FakeFrame:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def withColumn(self, name: str, value: object) -> "_FakeFrame":
        self.calls.append(f"withColumn:{name}")
        return self

    def filter(self, value: object) -> "_FakeFrame":
        self.calls.append("filter")
        return self

    def select(self, value: str) -> "_FakeFrame":
        self.calls.append(f"select:{value}")
        return self


def test_kafka_decode_filters_before_permissive_deserialization(monkeypatch) -> None:
    module = _module()
    frame = _FakeFrame()
    options: dict[str, str] = {}

    def fake_from_avro(data: object, schema: str, supplied_options: dict[str, str]) -> object:
        options.update(supplied_options)
        return (data, schema)

    decoded = module._decode_kafka_events(frame, _FakeFunctions(), fake_from_avro)

    assert decoded is frame
    assert frame.calls == [
        "withColumn:header_map",
        "filter",
        "withColumn:event",
        "filter",
        "select:event.*",
    ]
    assert options == {"mode": "PERMISSIVE"}
