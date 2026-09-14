import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl
import pytest
from pydantic import ValidationError
from typer.testing import CliRunner

from fraudtwin.calibration import (
    compute_fidelity_report,
    fit_calibration_profile,
    load_calibration_profile,
    load_reference_data,
    validate_calibration_output,
    write_calibration_profile,
)
from fraudtwin.cli import app
from fraudtwin.config import SimulationRunConfig, load_config

runner = CliRunner()


def _reference(path: Path) -> Path:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    pl.DataFrame(
        {
            "amount": [10.0, 20.0, 30.0, 40.0],
            "event_time": [start + timedelta(hours=index) for index in range(4)],
            "customer_id": ["CUS-A", "CUS-A", "CUS-B", "CUS-B"],
            "merchant_category": ["5411", "5812", "5411", "5411"],
            "account_balance": [100.0, 200.0, 300.0, 400.0],
            "payer_account_id": ["A", "A", "B", "B"],
            "payee_account_id": ["B", "B", "A", "A"],
        }
    ).write_parquet(path)
    return path


def test_reference_and_profile_are_deterministic_and_reusable(tmp_path: Path) -> None:
    reference = load_reference_data(_reference(tmp_path / "reference.parquet"))
    first = fit_calibration_profile(reference, seed=7)
    second = fit_calibration_profile(reference, seed=7)
    assert first == second
    profile_path = tmp_path / "profile.yaml"
    write_calibration_profile(first, profile_path)
    assert load_calibration_profile(profile_path) == first
    assert (
        json.loads((tmp_path / "profile.manifest.json").read_text())["profile_id"]
        == first.profile_id
    )


def test_invalid_reference_values_and_formats_fail_before_generation(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="Parquet"):
        load_reference_data(tmp_path / "missing.csv")
    invalid = pl.DataFrame(
        {
            "amount": [float("nan")],
            "event_time": [datetime(2026, 1, 1, tzinfo=UTC)],
            "customer_id": ["CUS-A"],
        }
    )
    path = tmp_path / "invalid.parquet"
    invalid.write_parquet(path)
    with pytest.raises(ValueError, match="non-finite"):
        load_reference_data(path)


def test_calibration_config_is_strict_and_neutral_hash_is_stable() -> None:
    config = load_config(Path("configs/minimal.yaml"))
    assert not config.calibration.enabled
    raw = config.model_dump(mode="python")
    raw["calibration"]["unknown"] = 1
    with pytest.raises(ValidationError):
        SimulationRunConfig.model_validate(raw)


def test_cli_calibrate_and_generate_profile(tmp_path: Path) -> None:
    reference = _reference(tmp_path / "reference.parquet")
    profile = tmp_path / "profile.yaml"
    result = runner.invoke(app, ["calibrate", str(reference), "--output", str(profile)])
    assert result.exit_code == 0, result.stdout
    generated = runner.invoke(
        app,
        [
            "generate",
            "configs/minimal.yaml",
            "--profile",
            str(profile),
            "--output-dir",
            str(tmp_path / "runs"),
        ],
    )
    assert generated.exit_code == 0, generated.stdout
    manifest_path = next((tmp_path / "runs").glob("RUN-*/manifest.json"))
    manifest = json.loads(manifest_path.read_text())
    assert manifest["calibration"]["profile_id"].startswith("CAL-")
    assert manifest_path.parent.joinpath(
        "calibration", manifest["calibration"]["profile_id"], "fidelity_report.json"
    ).is_file()


def test_fidelity_report_and_output_validation_are_typed(tmp_path: Path) -> None:
    profile = fit_calibration_profile(
        load_reference_data(_reference(tmp_path / "reference.parquet"))
    )
    report = compute_fidelity_report(profile, {item.name: True for item in profile.summaries})
    assert report.composite_score == 1.0
    validate_calibration_output(profile, {"amount_distribution": {"median": 20.0}})
    with pytest.raises(ValueError, match="aggregate-only"):
        validate_calibration_output(profile, {"rows": pl.DataFrame({"amount": [1.0]})})
