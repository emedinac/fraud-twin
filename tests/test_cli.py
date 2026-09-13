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
