from datetime import UTC, datetime

import pytest

from fraudtwin.kafka import PublicationRecord
from fraudtwin.kafka_chaos import KafkaChaosConfig, KafkaChaosOutage, simulate_delivery


def _records() -> tuple[PublicationRecord, ...]:
    moment = datetime(2026, 1, 1, tzinfo=UTC)
    return tuple(
        PublicationRecord(
            subject="payment-event",
            topic="fraudsim.payment.events.v1",
            version="1.0.0",
            fingerprint="schema",
            key=f"PAY-{index}",
            record_id=f"EVT-{index}",
            observable_time=moment,
            datum={"event_id": f"EVT-{index}"},
            value=f"payload-{index}".encode(),
            headers=(),
        )
        for index in range(20)
    )


def test_chaos_is_deterministic_and_preserves_payload_identity() -> None:
    config = KafkaChaosConfig(
        seed=7,
        duplicate_probability=0.5,
        retry_probability=0.25,
        max_delay_seconds=10,
        reorder_window=5,
        partition_count=3,
    )
    first = simulate_delivery(_records(), config)
    second = simulate_delivery(_records(), config)
    assert first == second
    assert first.emitted_count >= first.input_count
    assert {item.record_id for item in first.envelopes} <= {item.record_id for item in _records()}
    assert all(item.payload.startswith(b"payload-") for item in first.envelopes)
    assert first.deduplicated_count == first.emitted_count - len(
        {item.record_id for item in first.envelopes}
    )
    assert first.manifest["counts"]["sent"] == first.input_count
    assert first.manifest["counts"]["acknowledged"] == first.emitted_count


def test_chaos_applies_drop_outage_and_rejects_naive_timestamps() -> None:
    records = _records()
    outage = KafkaChaosOutage(
        from_time=datetime(2025, 12, 31, tzinfo=UTC),
        to_time=datetime(2026, 1, 2, tzinfo=UTC),
        behavior="DROP",
    )
    result = simulate_delivery(records, KafkaChaosConfig(outages=(outage,)))
    assert result.emitted_count == 0
    assert result.dropped_count == len(records)
    with pytest.raises(ValueError, match="timezone-aware"):
        bad = records[0]
        simulate_delivery(
            (bad.__class__(**{**bad.__dict__, "observable_time": datetime(2026, 1, 1)}),)
        )
