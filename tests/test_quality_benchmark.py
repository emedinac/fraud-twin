"""Focused generator-quality benchmark tests."""

import json
from pathlib import Path

from typer.testing import CliRunner

from fraudtwin.cli import app
from fraudtwin.quality_benchmark import (
    QUALITY_PACK_REFS,
    load_quality_profile,
    run_quality_benchmark,
)


def test_quality_benchmark_standard_profile_is_complete_and_versioned() -> None:
    profile = load_quality_profile("standard-v1")
    assert profile.profile_id == "standard-v1"
    assert profile.protocol_version == "M22-quality-1"
    assert profile.scale_size == "small"
    assert profile.public_packs == QUALITY_PACK_REFS
    assert len(profile.fingerprint) == 64


def test_quality_benchmark_dev_profile_matches_laptop_scale_contract() -> None:
    profile = load_quality_profile("standard-v1-dev")
    assert profile.profile_id == "standard-v1-dev"
    assert profile.scale_size == "dev"


def test_quality_benchmark_external_bundle_reports_unsupported_dimensions_as_na(
    tmp_path: Path,
) -> None:
    bundle = tmp_path / "bundle.json"
    bundle.write_text(
        json.dumps(
            {
                "candidate_id": "fixture-generator",
                "candidate_version": "1.0.0",
                "manifest_path": "manifest.json",
                "capabilities": {
                    "financial_invariants": False,
                    "temporal_invariants": False,
                    "pit_validation": False,
                    "scenario_coverage": True,
                    "ledger_reconciliation": False,
                    "reproducibility": False,
                    "statistical_fidelity": False,
                    "temporal_fidelity": True,
                    "graph_fidelity": False,
                    "difficulty": False,
                    "scalability": False,
                    "engineering_performance": False,
                },
            }
        ),
        encoding="utf-8",
    )
    result = run_quality_benchmark("standard-v1", output_dir=tmp_path / "reports", bundle=bundle)
    report = json.loads(result.report_path.read_text(encoding="utf-8"))
    metrics = {item["name"]: item for item in report["candidate"]["correctness"]}
    assert metrics["financial_invariants"]["status"] == "N/A"
    assert metrics["scenario_coverage"]["status"] == "AVAILABLE"


def test_quality_benchmark_cli_accepts_bundle(tmp_path: Path) -> None:
    bundle = tmp_path / "bundle.json"
    bundle.write_text(
        json.dumps(
            {
                "candidate_id": "cli-fixture",
                "manifest_path": "manifest.json",
                "capabilities": {},
            }
        ),
        encoding="utf-8",
    )
    result = CliRunner().invoke(
        app,
        [
            "quality-benchmark",
            "--profile",
            "standard-v1",
            "--bundle",
            str(bundle),
            "--output-dir",
            str(tmp_path / "cli-reports"),
        ],
    )
    assert result.exit_code == 0, result.stdout
    assert "Quality benchmark generated:" in result.stdout
