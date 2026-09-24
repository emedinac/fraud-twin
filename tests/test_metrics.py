"""Focused Prometheus metric contract tests."""

import pytest

from fraudtwin.generation import generate
from fraudtwin.observability import MetricsSession

prometheus_client = pytest.importorskip("prometheus_client")
generate_latest = prometheus_client.generate_latest


class _FakeServer:
    def shutdown(self) -> None:
        pass

    def server_close(self) -> None:
        pass


def test_metric_surface_is_stable_and_run_labelled(monkeypatch) -> None:
    monkeypatch.setattr(
        prometheus_client,
        "start_http_server",
        lambda _port, addr, registry: (_FakeServer(), None),
    )
    result = generate(write=False)
    session = MetricsSession("127.0.0.1", 9464, hold_seconds=0)
    session.start()
    try:
        session.observe_manifest(result.manifest, elapsed_seconds=2.0)
        assert session.registry is not None
        output = generate_latest(session.registry).decode("utf-8")
    finally:
        session.finish()

    expected = {
        "events_generated_total",
        "events_generated_per_second",
        "fraud_events_total",
        "generator_errors_total",
        "duplicate_rate",
        "late_event_count",
        "invalid_record_rate",
        "ledger_reconciliation_failures",
    }
    names = {
        line.split("{", 1)[0].split(" ", 1)[0]
        for line in output.splitlines()
        if line and not line.startswith("#")
    }
    assert expected <= names
    assert f'run_id="{result.run_id}"' in output
    event_count = result.manifest.event_counts["payment_events"]
    assert f'events_generated_total{{run_id="{result.run_id}"}} {event_count}.0' in output
    assert 'fraud_events_total{run_id="' in output


def test_generator_error_is_counted_without_swallowing(monkeypatch) -> None:
    monkeypatch.setattr(
        prometheus_client,
        "start_http_server",
        lambda _port, addr, registry: (_FakeServer(), None),
    )
    session = MetricsSession("127.0.0.1", 9464, hold_seconds=0)
    session.start()
    try:
        session.record_generator_error()
        assert session.registry is not None
        output = generate_latest(session.registry).decode("utf-8")
    finally:
        session.finish()
    assert 'generator_errors_total{run_id="unknown"} 1.0' in output
