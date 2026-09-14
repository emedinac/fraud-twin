import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

import fraudtwin
from fraudtwin.cli import app
from fraudtwin.generation import GeneratedData, GeneratedRun

runner = CliRunner()
CONFIG_PATH = Path("configs/minimal.yaml")


def test_generate_returns_deterministic_in_memory_data() -> None:
    first = fraudtwin.generate(CONFIG_PATH)
    second = fraudtwin.generate(CONFIG_PATH)

    assert isinstance(first, GeneratedData)
    assert isinstance(second, GeneratedData)
    assert first.run_id == second.run_id
    assert first.manifest.model_dump(mode="json") == second.manifest.model_dump(mode="json")
    assert first.entities.counts == second.entities.counts
    assert first.behavior.event_counts == second.behavior.event_counts
    assert first.dataset is not None
    assert first.dataset.count == second.dataset.count


def test_generate_uses_built_in_config_outside_repository(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)

    result = fraudtwin.generate()

    assert isinstance(result, GeneratedData)
    assert result.run_id == "RUN-19652188a1efbe6c"
    assert len(result.behavior.payments) == 100


def test_generate_writes_run_and_returns_paths(tmp_path: Path) -> None:
    result = fraudtwin.generate(CONFIG_PATH, write=True, output_dir=tmp_path)

    assert isinstance(result, GeneratedRun)
    assert result.run_dir.is_dir()
    assert result.manifest_path.is_file()
    assert result.manifest_path == result.run_dir / "manifest.json"
    assert result.dataset_path is not None and result.dataset_path.is_file()
    assert result.dataset_manifest_path is not None and result.dataset_manifest_path.is_file()
    assert json.loads(result.manifest_path.read_text(encoding="utf-8"))["run_id"] == result.run_id


def test_library_and_cli_generation_have_equivalent_manifests(tmp_path: Path) -> None:
    library_result = fraudtwin.generate(
        CONFIG_PATH,
        write=True,
        output_dir=tmp_path / "library",
    )
    cli_result = runner.invoke(
        app,
        [
            "generate",
            str(CONFIG_PATH),
            "--output-dir",
            str(tmp_path / "cli"),
        ],
    )

    assert isinstance(library_result, GeneratedRun)
    assert cli_result.exit_code == 0
    cli_manifest_path = next((tmp_path / "cli").glob("*/manifest.json"))
    library_manifest = json.loads(library_result.manifest_path.read_text(encoding="utf-8"))
    cli_manifest = json.loads(cli_manifest_path.read_text(encoding="utf-8"))
    assert library_manifest == cli_manifest
    assert "Run generated:" in cli_result.stdout


def test_generate_rejects_missing_configuration(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="configuration file does not exist"):
        fraudtwin.generate(tmp_path / "missing.yaml")
