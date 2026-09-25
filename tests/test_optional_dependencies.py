"""Verification for optional clients and their actionable diagnostics."""

import importlib
import importlib.util
import sys
import types

import pytest

import fraudtwin.kafka as kafka
import fraudtwin.postgres as postgres


def test_kafka_extra_is_complete_when_installed() -> None:
    if importlib.util.find_spec("confluent_kafka") is None:
        pytest.skip("Kafka extra is not installed")

    confluent_kafka = importlib.import_module("confluent_kafka")
    schema_registry = importlib.import_module("confluent_kafka.schema_registry")
    assert callable(getattr(confluent_kafka, "Producer", None))
    assert callable(getattr(schema_registry, "Schema", None))
    assert callable(getattr(schema_registry, "SchemaRegistryClient", None))
    producer, registry_client = kafka._dependencies()
    assert producer is confluent_kafka.Producer
    assert registry_client is schema_registry.SchemaRegistryClient


def test_postgres_extra_is_complete_when_installed() -> None:
    if importlib.util.find_spec("psycopg") is None:
        pytest.skip("PostgreSQL extra is not installed")

    psycopg = importlib.import_module("psycopg")
    assert callable(getattr(psycopg, "connect", None))
    assert postgres._psycopg() is psycopg


def test_kafka_incomplete_installation_has_remediation(monkeypatch: pytest.MonkeyPatch) -> None:
    confluent_kafka = types.ModuleType("confluent_kafka")
    schema_registry = types.ModuleType("confluent_kafka.schema_registry")
    confluent_kafka.Producer = object  # type: ignore[attr-defined]
    schema_registry.SchemaRegistryClient = object  # type: ignore[attr-defined]

    real_import = kafka.importlib.import_module

    def fake_import(name: str) -> types.ModuleType:
        if name == "confluent_kafka":
            return confluent_kafka
        if name == "confluent_kafka.schema_registry":
            return schema_registry
        return real_import(name)

    monkeypatch.setattr(kafka.importlib, "import_module", fake_import)
    with pytest.raises(kafka.KafkaConfigurationError, match="Schema.*poetry install -E kafka"):
        kafka._dependencies()


def test_postgres_incomplete_installation_has_remediation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    incomplete = types.ModuleType("psycopg")
    monkeypatch.setitem(sys.modules, "psycopg", incomplete)
    with pytest.raises(RuntimeError, match="psycopg.connect.*poetry install -E postgres"):
        postgres._psycopg()


def test_postgres_missing_dsn_is_reported_before_client_validation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(postgres.DSN_ENVIRONMENT, raising=False)
    incomplete = types.ModuleType("psycopg")
    monkeypatch.setitem(sys.modules, "psycopg", incomplete)
    with pytest.raises(ValueError, match=postgres.DSN_ENVIRONMENT):
        postgres.database_status()

