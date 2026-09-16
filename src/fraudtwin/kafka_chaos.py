"""Deterministic logical-message chaos for Kafka integration exercises.

This module does not manipulate sockets or broker state.  It transforms encoded
publication records into transport envelopes so producer and consumer logic
can be tested with repeatable drops, retries, duplicates, delays, reordering,
outages, and partition skew.
"""

import hashlib
from datetime import UTC, datetime, timedelta
from random import Random
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from fraudtwin.kafka import PublicationRecord
from fraudtwin.reproducibility import sha256_json


class KafkaChaosOutage(BaseModel):
    """A source-time interval with a deterministic delivery behavior."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    from_time: datetime
    to_time: datetime
    behavior: Literal["DROP", "BUFFER_AND_FLUSH", "DELAY", "PARTIAL_REJECT"]
    reject_probability: float = Field(default=0.5, ge=0, le=1)
    delay_seconds: int = Field(default=0, ge=0)

    @model_validator(mode="after")
    def interval_must_be_ordered(self) -> KafkaChaosOutage:
        if self.from_time.tzinfo is None or self.to_time.tzinfo is None:
            raise ValueError("Kafka chaos outage timestamps must be timezone-aware")
        if self.to_time <= self.from_time:
            raise ValueError("Kafka chaos outage to_time must be after from_time")
        if self.behavior == "DELAY" and self.delay_seconds < 1:
            raise ValueError("DELAY outages require delay_seconds >= 1")
        return self


class KafkaChaosConfig(BaseModel):
    """Immutable, seeded policy for logical Kafka delivery faults."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    seed: int = Field(default=42, ge=0)
    boundary: Literal["producer", "consumer"] = "producer"
    drop_probability: float = Field(default=0.0, ge=0, le=1)
    duplicate_probability: float = Field(default=0.0, ge=0, le=1)
    retry_probability: float = Field(default=0.0, ge=0, le=1)
    max_delay_seconds: int = Field(default=0, ge=0)
    reorder_window: int = Field(default=0, ge=0)
    partition_count: int = Field(default=3, ge=1)
    partition_skew_probability: float = Field(default=0.0, ge=0, le=1)
    outages: tuple[KafkaChaosOutage, ...] = ()


class ChaosEnvelope(BaseModel):
    """Transport metadata wrapped around one immutable publication payload."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    subject: str
    topic: str
    key: str
    record_id: str
    event_id: str
    payload: bytes
    scheduled_at: datetime
    delivered_at: datetime
    transport_message_id: str
    attempt: int = Field(ge=1)
    partition: int = Field(ge=0)
    retried: bool = False
    duplicated: bool = False
    late: bool = False


class KafkaChaosResult(BaseModel):
    """Auditable output of one seeded logical-message chaos run."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    manifest_version: str = "M29-kafka-chaos-1"
    seed: int = Field(ge=0)
    boundary: Literal["producer", "consumer"]
    input_count: int = Field(ge=0)
    emitted_count: int = Field(ge=0)
    dropped_count: int = Field(ge=0)
    retried_count: int = Field(ge=0)
    duplicated_count: int = Field(ge=0)
    late_count: int = Field(ge=0)
    out_of_order_count: int = Field(ge=0)
    deduplicated_count: int = Field(ge=0)
    partition_counts: dict[str, int] = Field(default_factory=dict)
    input_fingerprint: str
    output_fingerprint: str
    envelopes: tuple[ChaosEnvelope, ...] = ()
    audit: tuple[dict[str, Any], ...] = ()

    @property
    def manifest(self) -> dict[str, Any]:
        """Return JSON-ready counts, fingerprints, and audit metadata."""

        sent = self.input_count
        unique_delivered = self.emitted_count - self.deduplicated_count
        return self.model_dump(mode="json", exclude={"envelopes", "audit"}) | {
            "audit": list(self.audit),
            "configuration_boundary": self.boundary,
            "counts": {
                "sent": sent,
                "acknowledged": self.emitted_count,
                "dropped": self.dropped_count,
                "retried": self.retried_count,
                "duplicated": self.duplicated_count,
                "late": self.late_count,
                "reordered": self.out_of_order_count,
                "deduplicated": self.deduplicated_count,
            },
            "consumer_visible_loss_rate": (sent - unique_delivered) / sent if sent else 0.0,
            "consumer_visible_duplicate_rate": (
                self.deduplicated_count / self.emitted_count if self.emitted_count else 0.0
            ),
        }


def _source_values(
    record: PublicationRecord | ChaosEnvelope,
) -> tuple[str, str, str, str, bytes, datetime, datetime]:
    if isinstance(record, ChaosEnvelope):
        return (
            record.subject,
            record.topic,
            record.key,
            record.record_id,
            record.payload,
            record.scheduled_at,
            record.delivered_at,
        )
    return (
        record.subject,
        record.topic,
        record.key,
        record.record_id,
        record.value,
        record.observable_time,
        record.observable_time,
    )


def _partition(key: str, config: KafkaChaosConfig, rng: Random) -> int:
    if config.partition_count == 1:
        return 0
    if config.partition_skew_probability and rng.random() < config.partition_skew_probability:
        return 0
    digest = hashlib.sha256(key.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") % config.partition_count


def _outage_for(
    timestamp: datetime, config: KafkaChaosConfig, rng: Random
) -> KafkaChaosOutage | None:
    for outage in config.outages:
        if not outage.from_time <= timestamp < outage.to_time:
            continue
        if outage.behavior == "PARTIAL_REJECT" and rng.random() >= outage.reject_probability:
            continue
        return outage
    return None


def simulate_delivery(
    records: list[PublicationRecord | ChaosEnvelope]
    | tuple[PublicationRecord | ChaosEnvelope, ...],
    config: KafkaChaosConfig | None = None,
) -> KafkaChaosResult:
    """Apply seeded logical-message faults at a producer or consumer boundary.

    Args:
        records: Encoded publication records, or envelopes from a prior
            boundary. Payload bytes and stable record IDs are never rewritten.
        config: Chaos policy. Defaults to a clean producer-boundary pass.

    Returns:
        Envelopes, counts, fingerprints, and an audit trail suitable for a
        reliability tutorial or test fixture.

    Raises:
        ValueError: If ``records`` contain naive timestamps or incompatible
            envelopes.
    """

    policy = config or KafkaChaosConfig()
    rng = Random(policy.seed)
    source = tuple(records)
    input_payload = [
        {"record_id": _source_values(item)[3], "payload": _source_values(item)[4].hex()}
        for item in source
    ]
    output: list[ChaosEnvelope] = []
    audit: list[dict[str, Any]] = []
    dropped = retried = duplicated = late = 0
    for index, record in enumerate(source):
        subject, topic, key, record_id, payload, scheduled, prior_delivery = _source_values(record)
        if scheduled.tzinfo is None or prior_delivery.tzinfo is None:
            raise ValueError("Kafka chaos timestamps must be timezone-aware")
        outage = _outage_for(scheduled, policy, rng)
        if outage is not None and outage.behavior == "DROP":
            dropped += 1
            audit.append({"record_id": record_id, "fault": "outage", "behavior": "DROP"})
            continue
        if rng.random() < policy.drop_probability:
            dropped += 1
            audit.append({"record_id": record_id, "fault": "drop"})
            continue
        copies = 2 if rng.random() < policy.duplicate_probability else 1
        if copies == 2:
            duplicated += 1
        attempts = 2 if rng.random() < policy.retry_probability else 1
        if attempts == 2:
            retried += 1
        delay = rng.randint(0, policy.max_delay_seconds) if policy.max_delay_seconds else 0
        if outage is not None:
            if outage.behavior == "BUFFER_AND_FLUSH":
                delay += max(0, int((outage.to_time - scheduled).total_seconds()))
            elif outage.behavior == "DELAY":
                delay += outage.delay_seconds
        for attempt in range(1, attempts + 1):
            for copy_index in range(copies):
                delivered = prior_delivery.astimezone(UTC) + timedelta(seconds=delay + attempt - 1)
                is_late = delivered > scheduled.astimezone(UTC)
                late += int(is_late)
                output.append(
                    ChaosEnvelope(
                        subject=subject,
                        topic=topic,
                        key=key,
                        record_id=record_id,
                        event_id=record_id,
                        payload=payload,
                        scheduled_at=scheduled.astimezone(UTC),
                        delivered_at=delivered,
                        transport_message_id=f"chaos-{policy.seed}-{index:08d}-{attempt}-{copy_index}",
                        attempt=attempt,
                        partition=_partition(key, policy, rng),
                        retried=attempt > 1,
                        duplicated=copy_index > 0,
                        late=is_late,
                    )
                )
    if policy.reorder_window > 1:
        reordered: list[ChaosEnvelope] = []
        for offset in range(0, len(output), policy.reorder_window):
            group = output[offset : offset + policy.reorder_window]
            rng.shuffle(group)
            reordered.extend(group)
        out_of_order = sum(
            left.delivered_at > right.delivered_at
            for left, right in zip(reordered, reordered[1:], strict=False)
        )
        output = reordered
    else:
        out_of_order = 0
    unique_ids = {item.record_id for item in output}
    deduplicated = len(output) - len(unique_ids)
    partition_counts: dict[str, int] = {}
    for item in output:
        key = str(item.partition)
        partition_counts[key] = partition_counts.get(key, 0) + 1
    output_payload = [
        {"record_id": item.record_id, "transport_message_id": item.transport_message_id}
        for item in output
    ]
    return KafkaChaosResult(
        seed=policy.seed,
        boundary=policy.boundary,
        input_count=len(source),
        emitted_count=len(output),
        dropped_count=dropped,
        retried_count=retried,
        duplicated_count=duplicated,
        late_count=late,
        out_of_order_count=out_of_order,
        deduplicated_count=deduplicated,
        partition_counts=partition_counts,
        input_fingerprint=sha256_json(input_payload),
        output_fingerprint=sha256_json(output_payload),
        envelopes=tuple(output),
        audit=tuple(audit),
    )


__all__ = [
    "ChaosEnvelope",
    "KafkaChaosConfig",
    "KafkaChaosOutage",
    "KafkaChaosResult",
    "simulate_delivery",
]
