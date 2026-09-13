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
    }
    assert manifest["event_counts"] == {}
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
