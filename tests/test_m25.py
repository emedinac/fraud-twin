"""Focused Milestone 25 native Kafka publication contracts."""

from __future__ import annotations

from pathlib import Path

import pytest

from fraudtwin.config import config_hash, load_config
from fraudtwin.contracts import contract_registry
from fraudtwin.generation import generate
from fraudtwin.kafka import (
    KafkaConfigurationError,
    KafkaPublisher,
    publication_records,
    topic_for,
)


class _Remote:
    def __init__(self) -> None:
        self.registered: list[str] = []

    def get_versions(self, subject: str) -> tuple[int, ...]:
        raise RuntimeError("404 not found")

    def register_schema(self, subject: str, schema: object) -> int:
        self.registered.append(subject)
        return len(self.registered)

    def set_compatibility(self, *_args: object) -> None:
        return None


class _Producer:
    def __init__(self) -> None:
        self.messages: list[dict[str, object]] = []

    def produce(self, **kwargs: object) -> None:
        self.messages.append(kwargs)
        callback = kwargs["on_delivery"]
        assert callable(callback)
        callback(None, None)

    def poll(self, _timeout: float) -> None:
        return None

    def flush(self, _timeout: float) -> int:
        return 0


def test_kafka_settings_do_not_change_source_identity() -> None:
    base = load_config(Path("configs/minimal.yaml"))
    values = base.model_dump(mode="python")
    values["outputs"]["kafka"] = True
    values["kafka"] = {"topic_prefix": "other", "max_events_per_second": 2.0}
    configured = type(base).model_validate(values)
    assert config_hash(base) == config_hash(configured)


def test_topics_are_stable_and_records_are_deterministically_ordered() -> None:
    data = generate(write=False)
    records = publication_records(data.behavior, data.run_id)
    assert topic_for("payment-event") == "fraudsim.payment.events.v1"
    assert [item.observable_time for item in records] == sorted(
        item.observable_time for item in records
    )
    assert records == publication_records(data.behavior, data.run_id)


def test_publisher_frames_avro_and_records_manifest_metadata() -> None:
    data = generate(write=False)
    producer = _Producer()
    publisher = KafkaPublisher(
        producer=producer,
        registry_client=_Remote(),
        registry=contract_registry(),
    )
    result = publisher.publish(data.behavior, data.run_id)
    assert result.record_counts["payment-event"] == len(data.behavior.payment_events)
    assert len(producer.messages) == len(publication_records(data.behavior, data.run_id))
    assert producer.messages[0]["value"][:1] == b"\x00"
    assert dict(producer.messages[0]["headers"])["fraudtwin-run-id"] == data.run_id.encode()


def test_remote_mismatch_is_rejected() -> None:
    class Existing(_Remote):
        def get_versions(self, _subject: str) -> tuple[int, ...]:
            return (1,)

        def get_version(self, _subject: str, _version: int) -> object:
            class Registered:
                schema_id = 1
                schema = '{"type":"string"}'

            return Registered()

        def get_compatibility(self, _subject: str) -> str:
            return "FULL_TRANSITIVE"

    with pytest.raises(KafkaConfigurationError, match="fingerprint mismatch"):
        KafkaPublisher(
            producer=_Producer(),
            registry_client=Existing(),
            registry=contract_registry(),
        ).prepare()
