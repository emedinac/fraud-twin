import json
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
    assert manifest["schema_versions"]["payment_events"] == "2"
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
