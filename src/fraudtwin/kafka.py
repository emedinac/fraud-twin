"""Native Kafka publication for the clean observable M24 event contracts."""

import importlib
import json
import os
import struct
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from hashlib import sha256
from typing import Any

from fraudtwin.config import KafkaConfig
from fraudtwin.contracts import AvroContractRegistry, contract_registry
from fraudtwin.contracts.registry import ContractSubject
from fraudtwin.reproducibility import canonical_json

SUBJECTS: tuple[str, ...] = (
    "payment-event",
    "customer-dispute",
    "fraud-alert",
    "fraud-case",
    "fraud-case-confirmation",
    "fraud-label",
)
_TOPIC_SUFFIXES = {
    "payment-event": "payment.events.v1",
    "customer-dispute": "customer.disputes.v1",
    "fraud-alert": "fraud.alerts.v1",
    "fraud-case": "fraud.cases.v1",
    "fraud-case-confirmation": "fraud.case-confirmations.v1",
    "fraud-label": "fraud.labels.v1",
}
_RECORD_FIELDS = {
    "payment-event": ("event_id", "payment_id", "event_time"),
    "customer-dispute": ("event_id", "payment_id", "event_time"),
    "fraud-alert": ("fraud_alert_id", "payment_id", "alert_created_at"),
    "fraud-case": ("fraud_case_id", "payment_id", "case_opened_at"),
    "fraud-case-confirmation": ("confirmation_id", "payment_id", "confirmed_at"),
    "fraud-label": ("label_id", "payment_id", "label_available_at"),
}
_BEHAVIOR_FIELDS = {
    "payment-event": "payment_events",
    "customer-dispute": "customer_disputes",
    "fraud-alert": "alerts",
    "fraud-case": "fraud_cases",
    "fraud-case-confirmation": "case_confirmations",
    "fraud-label": "fraud_labels",
}


class KafkaConfigurationError(ValueError):
    """Raised when Kafka or Schema Registry configuration is incomplete."""


class KafkaPublicationError(RuntimeError):
    """Raised when one or more Kafka records fail delivery."""


@dataclass(frozen=True)
class PublicationRecord:
    """A deterministic, contract-encoded Kafka message."""

    subject: str
    topic: str
    version: str
    fingerprint: str
    key: str
    record_id: str
    observable_time: datetime
    datum: dict[str, Any]
    value: bytes
    headers: tuple[tuple[str, bytes], ...]


@dataclass(frozen=True)
class KafkaPublicationResult:
    """Non-secret publication metadata suitable for a run manifest."""

    topics: dict[str, dict[str, object]]
    record_counts: dict[str, int]
    mode: str
    max_events_per_second: float | None
    accelerated_time_multiplier: float
    publication_fingerprint: str


def topic_for(subject: str, prefix: str = "fraudsim") -> str:
    """Return the stable versioned topic for an M24 subject."""

    try:
        return f"{prefix}.{_TOPIC_SUFFIXES[subject]}"
    except KeyError as exc:
        raise KeyError(f"unknown Kafka contract subject: {subject}") from exc


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Kafka observable timestamps must be timezone-aware")
    return value.astimezone(UTC)


def publication_records(
    behavior: Any,
    run_id: str,
    *,
    registry: AvroContractRegistry | None = None,
    topic_prefix: str = "fraudsim",
) -> tuple[PublicationRecord, ...]:
    """Map all observable M24 records into one deterministic publication order."""

    loaded = registry or contract_registry()
    mapper = loaded.mapper()
    records: list[PublicationRecord] = []
    for subject in SUBJECTS:
        record_id_field, key_field, time_field = _RECORD_FIELDS[subject]
        for record in getattr(behavior, _BEHAVIOR_FIELDS[subject]):
            datum = mapper.to_datum(subject, record)
            contract = loaded.contract(subject)
            raw = mapper.encode(subject, record)
            record_id = str(getattr(record, record_id_field))
            key = str(getattr(record, key_field))
            observable_time = _as_utc(getattr(record, time_field))
            headers = (
                ("fraudtwin-subject", subject.encode()),
                ("fraudtwin-contract-version", contract.version.encode()),
                ("fraudtwin-contract-fingerprint", contract.canonical_sha256.encode()),
                ("fraudtwin-run-id", run_id.encode()),
                ("fraudtwin-record-id", record_id.encode()),
            )
            records.append(
                PublicationRecord(
                    subject=subject,
                    topic=topic_for(subject, topic_prefix),
                    version=contract.version,
                    fingerprint=contract.canonical_sha256,
                    key=key,
                    record_id=record_id,
                    observable_time=observable_time,
                    datum=datum,
                    value=raw,
                    headers=headers,
                )
            )
    records.sort(key=lambda item: (item.observable_time, item.subject, item.key, item.record_id))
    return tuple(records)


def publication_fingerprint(records: Iterable[PublicationRecord]) -> str:
    """Fingerprint logical content, excluding broker-assigned transport values."""

    payload = [
        {
            "subject": item.subject,
            "version": item.version,
            "fingerprint": item.fingerprint,
            "key": item.key,
            "record_id": item.record_id,
            "datum": item.datum,
        }
        for item in records
    ]
    return sha256(canonical_json(payload).encode()).hexdigest()


def _schema_text(schema: Any) -> str:
    value = getattr(schema, "schema_str", schema)
    return str(value)


def reconcile_remote_registry(
    client: Any,
    registry: AvroContractRegistry,
) -> dict[str, int]:
    """Verify/register all local subjects in a Confluent-compatible registry."""

    result: dict[str, int] = {}
    for subject in registry.subjects:
        try:
            versions = tuple(client.get_versions(subject.name))
        except Exception as exc:
            if not _is_missing_subject(exc):
                raise KafkaConfigurationError(
                    f"cannot inspect remote schema subject {subject.name!r}: {exc}"
                ) from exc
            result[subject.name] = _register_subject_versions(client, subject)
            continue
        if not versions:
            result[subject.name] = _register_subject_versions(client, subject)
            continue
        if len(versions) != len(subject.versions):
            raise KafkaConfigurationError(
                f"remote subject {subject.name!r} versions diverge from the bundled registry"
            )
        remote = None
        for index, local in enumerate(subject.versions, start=1):
            remote = client.get_version(subject.name, index)
            actual_schema = _schema_text(getattr(remote, "schema", remote))
            try:
                import avro.schema

                actual = avro.schema.parse(actual_schema)
                actual_fingerprint = sha256(actual.canonical_form.encode()).hexdigest()
            except Exception as exc:
                raise KafkaConfigurationError(
                    f"remote subject {subject.name!r} contains invalid Avro: {exc}"
                ) from exc
            if actual_fingerprint != local.canonical_sha256:
                raise KafkaConfigurationError(
                    f"remote subject {subject.name!r} fingerprint mismatch: "
                    f"expected {local.canonical_sha256}, got {actual_fingerprint}"
                )
        compatibility = str(client.get_compatibility(subject.name)).upper().split(".")[-1]
        if compatibility != "FULL_TRANSITIVE":
            raise KafkaConfigurationError(
                f"remote subject {subject.name!r} must use FULL_TRANSITIVE, got {compatibility}"
            )
        result[subject.name] = int(getattr(remote, "schema_id", getattr(remote, "id", 0)))
    return result


def _is_missing_subject(exc: Exception) -> bool:
    return "404" in str(exc) or "not found" in str(exc).lower()


def _register_subject_versions(client: Any, subject: ContractSubject) -> int:
    """Register every local version and return the latest remote schema ID."""

    schema_id = 0
    for local in subject.versions:
        schema_id = _register(client, subject.name, local.schema)
    return int(schema_id)


def _register(client: Any, subject: str, schema: Any) -> int:
    try:
        try:
            schema_registry = importlib.import_module("confluent_kafka.schema_registry")
            schema_class = vars(schema_registry)["Schema"]
            schema_payload = (
                schema.to_json() if hasattr(schema, "to_json") else schema.canonical_form
            )
            remote_schema = schema_class(json.dumps(schema_payload, separators=(",", ":")))
        except ImportError:
            remote_schema = schema.canonical_form
        registered = client.register_schema(subject, remote_schema)
    except Exception as exc:
        raise KafkaConfigurationError(
            f"failed to register schema subject {subject!r}: {exc}"
        ) from exc
    try:
        client.set_compatibility(subject, "FULL_TRANSITIVE")
    except Exception as exc:
        raise KafkaConfigurationError(
            f"remote subject {subject!r} could not be set to FULL_TRANSITIVE: {exc}"
        ) from exc
    return int(getattr(registered, "schema_id", getattr(registered, "id", registered)))


def _dependencies() -> tuple[Any, Any]:
    try:
        confluent_kafka = importlib.import_module("confluent_kafka")
        schema_registry = importlib.import_module("confluent_kafka.schema_registry")
    except ImportError as exc:  # pragma: no cover - depends on optional extra
        raise RuntimeError("Kafka output requires the optional 'kafka' dependency") from exc
    return vars(confluent_kafka)["Producer"], vars(schema_registry)["SchemaRegistryClient"]


def publisher_from_environment(
    *, registry: AvroContractRegistry | None = None, config: KafkaConfig | None = None
) -> "KafkaPublisher":
    """Build a publisher from environment-only connection settings."""

    settings = config or KafkaConfig()
    brokers = os.environ.get("FRAUDTWIN_KAFKA_BOOTSTRAP_SERVERS")
    registry_url = os.environ.get("FRAUDTWIN_SCHEMA_REGISTRY_URL")
    if not brokers or not registry_url:
        raise KafkaConfigurationError(
            "Kafka output requires FRAUDTWIN_KAFKA_BOOTSTRAP_SERVERS and "
            "FRAUDTWIN_SCHEMA_REGISTRY_URL"
        )
    Producer, SchemaRegistryClient = _dependencies()
    producer_options: dict[str, Any] = {
        "bootstrap.servers": brokers,
        "enable.idempotence": True,
        "acks": "all",
        "retries": 10,
        "max.in.flight.requests.per.connection": 5,
        "delivery.timeout.ms": int(settings.delivery_timeout_seconds * 1000),
        "client.id": "fraudtwin",
    }
    for key in (
        "security.protocol",
        "sasl.mechanisms",
        "sasl.username",
        "sasl.password",
        "ssl.ca.location",
    ):
        env_key = "FRAUDTWIN_KAFKA_" + key.upper().replace(".", "_")
        if env_key in os.environ:
            producer_options[key] = os.environ[env_key]
    registry_options: dict[str, Any] = {"url": registry_url}
    user_info = os.environ.get("FRAUDTWIN_SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO")
    if user_info:
        registry_options["basic.auth.user.info"] = user_info
    return KafkaPublisher(
        producer=Producer(producer_options),
        registry_client=SchemaRegistryClient(registry_options),
        registry=registry or contract_registry(),
        config=settings,
    )


class KafkaPublisher:
    """Publish mapped M24 records with deterministic pacing and acknowledgements."""

    def __init__(
        self,
        *,
        producer: Any,
        registry_client: Any,
        registry: AvroContractRegistry,
        config: KafkaConfig | None = None,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.producer = producer
        self.registry_client = registry_client
        self.registry = registry
        self.config = config or KafkaConfig()
        self.clock = clock
        self.sleep = sleep
        self._schema_ids: dict[str, int] = {}

    def prepare(self) -> None:
        self.registry.validate()
        list_topics = getattr(self.producer, "list_topics", None)
        if callable(list_topics):
            try:
                metadata = list_topics(timeout=self.config.delivery_timeout_seconds)
                topics = (
                    metadata.get("topics", {})
                    if isinstance(metadata, Mapping)
                    else getattr(metadata, "topics", {})
                )
                expected_topics = {
                    topic_for(subject, self.config.topic_prefix) for subject in SUBJECTS
                }
                missing = expected_topics - set(topics)
            except Exception as exc:
                raise KafkaConfigurationError(f"cannot inspect Kafka topics: {exc}") from exc
            if missing:
                raise KafkaConfigurationError(
                    "Kafka topics are not provisioned (auto-creation is disabled): "
                    + ", ".join(sorted(missing))
                )
        self._schema_ids = reconcile_remote_registry(self.registry_client, self.registry)

    def publish(
        self,
        behavior: Any,
        run_id: str,
        *,
        mode: str = "batch",
    ) -> KafkaPublicationResult:
        records = publication_records(
            behavior, run_id, registry=self.registry, topic_prefix=self.config.topic_prefix
        )
        result = self.publish_records(records, run_id, mode=mode)
        # Preserve the historical list-based fingerprint for the compatibility
        # API; the streaming method uses an incremental equivalent.
        return replace(result, publication_fingerprint=publication_fingerprint(records))

    def publish_records(
        self,
        records: Iterable[PublicationRecord],
        run_id: str,
        *,
        mode: str = "batch",
    ) -> KafkaPublicationResult:
        """Publish a pre-encoded stream without materializing all records."""

        if mode not in {"batch", "real_time", "accelerated"}:
            raise ValueError(f"unsupported Kafka publication mode: {mode}")
        if not self._schema_ids:
            self.prepare()
        errors: list[str] = []
        started = self.clock()
        first_time: datetime | None = None
        counts = {subject: 0 for subject in SUBJECTS}
        digest = sha256()
        for index, item in enumerate(records):
            if first_time is None:
                first_time = item.observable_time
            self._pace(item, index, first_time, started, mode)
            counts[item.subject] = counts.get(item.subject, 0) + 1
            digest.update(
                canonical_json(
                    {
                        "subject": item.subject,
                        "version": item.version,
                        "fingerprint": item.fingerprint,
                        "key": item.key,
                        "record_id": item.record_id,
                        "datum": item.datum,
                    }
                ).encode()
            )
            schema_id = self._schema_ids.get(item.subject)
            if schema_id is None:
                errors.append(f"{item.subject}: no remote schema ID")
                continue
            value = struct.pack(">bI", 0, schema_id) + item.value
            try:
                self.producer.produce(
                    topic=item.topic,
                    key=item.key.encode(),
                    value=value,
                    headers=list(item.headers),
                    on_delivery=(
                        lambda error, _message: errors.append(str(error)) if error else None
                    ),
                )
                self.producer.poll(0)
            except Exception as exc:
                errors.append(f"{item.subject}/{item.record_id}: {exc}")
        try:
            self.producer.flush(self.config.delivery_timeout_seconds)
        except Exception as exc:
            errors.append(f"flush: {exc}")
        if errors:
            raise KafkaPublicationError("Kafka delivery failed: " + "; ".join(errors))
        topics: dict[str, dict[str, object]] = {
            subject: {
                "topic": topic_for(subject, self.config.topic_prefix),
                "version": self.registry.contract(subject).version,
                "fingerprint": self.registry.contract(subject).canonical_sha256,
                "key": "payment_id",
            }
            for subject in SUBJECTS
        }
        return KafkaPublicationResult(
            topics=topics,
            record_counts=counts,
            mode=mode,
            max_events_per_second=self.config.max_events_per_second,
            accelerated_time_multiplier=self.config.accelerated_time_multiplier,
            publication_fingerprint=digest.hexdigest(),
        )

    def _pace(
        self,
        item: PublicationRecord,
        index: int,
        first_time: datetime | None,
        started: float,
        mode: str,
    ) -> None:
        if mode == "batch" and self.config.max_events_per_second is None:
            return
        multiplier = 1.0 if mode == "real_time" else self.config.accelerated_time_multiplier
        target = 0.0
        if mode != "batch" and first_time is not None:
            target = max(0.0, (item.observable_time - first_time).total_seconds()) / multiplier
        if self.config.max_events_per_second is not None:
            target = max(target, index / self.config.max_events_per_second)
        remaining = target - (self.clock() - started)
        if remaining > 0:
            self.sleep(remaining)


__all__ = [
    "KafkaConfigurationError",
    "KafkaPublicationError",
    "KafkaPublicationResult",
    "KafkaPublisher",
    "PublicationRecord",
    "SUBJECTS",
    "publication_fingerprint",
    "publication_records",
    "publisher_from_environment",
    "reconcile_remote_registry",
    "topic_for",
]
