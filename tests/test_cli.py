import json
from datetime import UTC, datetime
from pathlib import Path

from typer.testing import CliRunner

from fraudtwin.cli import app

runner = CliRunner()


def test_config_validate_command() -> None:
    result = runner.invoke(app, ["config", "validate", "configs/minimal.yaml"])

    assert result.exit_code == 0
    assert "Configuration is valid." in result.stdout


def test_generate_command_writes_manifest(tmp_path: Path) -> None:
    result = runner.invoke(
        app,
        ["generate", "configs/minimal.yaml", "--output-dir", str(tmp_path)],
    )

    assert result.exit_code == 0
    manifests = list(tmp_path.glob("*/manifest.json"))
    assert len(manifests) == 1
    assert "Generated entity counts:" in result.stdout
    run_dir = manifests[0].parent
    manifest = json.loads(manifests[0].read_text(encoding="utf-8"))
    assert manifest["entity_counts"] == {
        "customers": 10,
        "institutions": 3,
        "accounts": 15,
        "cards": 12,
        "merchants": 3,
        "devices": 12,
        "pix_keys": 8,
        "behavior_profiles": 10,
    }
    assert manifest["event_counts"]["payments"] == 100
    assert manifest["event_counts"]["payment_events"] >= 100
    assert manifest["event_counts"]["card_lifecycle_events"] > 0
    assert manifest["event_counts"]["CARD_AUTHORIZATION_REQUESTED"] > 0
    assert manifest["event_counts"]["PIX_INITIATED"] > 0
    assert manifest["event_counts"]["PIX_SETTLED"] > 0
    assert manifest["event_counts"]["ledger_entries"] > 0
    assert manifest["schema_versions"]["payment_events"] == "5"
    assert manifest["fraud_counts"] == {}
    assert sorted(path.name for path in (run_dir / "entities").glob("*.parquet")) == [
        "accounts.parquet",
        "cards.parquet",
        "customers.parquet",
        "devices.parquet",
        "institutions.parquet",
        "merchants.parquet",
        "pix_keys.parquet",
    ]
    assert sorted(path.name for path in (run_dir / "behavior").glob("*.parquet")) == [
        "behavior_profiles.parquet",
    ]
    assert sorted(path.name for path in (run_dir / "payments").glob("*.parquet")) == [
        "payment_events.parquet",
        "payments.parquet",
    ]
    ledger_result = runner.invoke(
        app,
        ["validate-ledger", "--run-id", run_dir.name, "--output-dir", str(tmp_path)],
    )
    assert ledger_result.exit_code == 0
    assert "Ledger is valid" in ledger_result.stdout


def test_kafka_chaos_command_writes_audit_files(tmp_path: Path, monkeypatch) -> None:
    from fraudtwin import cli
    from fraudtwin.kafka import PublicationRecord

    moment = datetime(2026, 1, 1, tzinfo=UTC)
    records = tuple(
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

    monkeypatch.setattr(cli, "load_generated_run", lambda _: (None, object(), None))
    monkeypatch.setattr(cli, "publication_records", lambda *_args: records)
    result = runner.invoke(
        app,
        [
            "kafka",
            "chaos",
            "--run-id",
            "RUN-TEST",
            "--drop-rate",
            "0.1",
            "--output-dir",
            str(tmp_path),
        ],
    )
    assert result.exit_code == 0, result.stdout
    destination = tmp_path / "RUN-TEST" / "kafka-chaos"
    manifest = json.loads((destination / "manifest.json").read_text())
    assert manifest["input_count"] == 20
    assert (destination / "envelopes.jsonl").is_file()
