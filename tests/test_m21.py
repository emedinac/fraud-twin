"""Milestone 21 immutable public benchmark-pack tests."""

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from fraudtwin.benchmark import list_public_packs, load_public_pack, run_public_benchmark
from fraudtwin.cli import app

PACK_IDS = (
    "FT-B01-STABLE",
    "FT-B02-TEMPORAL",
    "FT-B03-BOUNDARY",
    "FT-B04-CAMOUFLAGE",
    "FT-B05-GRAPH",
    "FT-B06-OBSERVABILITY",
    "FT-B07-CALIBRATED",
    "FT-B08-MIXED",
)


def test_m21_registry_contains_the_complete_initial_pack_family() -> None:
    packs = list_public_packs()
    assert tuple(pack.id for pack in packs) == PACK_IDS
    assert all(pack.version == "1.0.0" for pack in packs)
    assert all(pack.expected_descriptors for pack in packs)
    assert all(pack.expected_fingerprints for pack in packs)
    assert load_public_pack("FT-B04-CAMOUFLAGE@1.0").identity == "FT-B04-CAMOUFLAGE@1.0.0"


@pytest.mark.parametrize("pack_id", PACK_IDS)
def test_m21_pack_runs_match_frozen_goldens(pack_id: str, tmp_path: Path) -> None:
    result = run_public_benchmark(f"{pack_id}@1.0.0", output_dir=tmp_path)
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["public_pack"]["identity"] == f"{pack_id}@1.0.0"
    assert manifest["public_pack"]["verification"]["descriptors_match"] is True


def test_m21_repeated_executions_are_identical(tmp_path: Path) -> None:
    first = run_public_benchmark("FT-B08-MIXED@1.0.0", output_dir=tmp_path / "first")
    second = run_public_benchmark("FT-B08-MIXED@1.0.0", output_dir=tmp_path / "second")
    first_manifest = json.loads(first.manifest_path.read_text(encoding="utf-8"))
    second_manifest = json.loads(second.manifest_path.read_text(encoding="utf-8"))
    assert (
        first_manifest["public_pack"]["definition_fingerprint"]
        == second_manifest["public_pack"]["definition_fingerprint"]
    )
    assert first_manifest["output_fingerprint"] == second_manifest["output_fingerprint"]


def test_m21_cli_describe_and_legacy_generic_command(tmp_path: Path) -> None:
    runner = CliRunner()
    described = runner.invoke(app, ["benchmark", "describe", "FT-B01-STABLE@1.0"])
    assert described.exit_code == 0, described.stdout
    assert "FT-B01-STABLE" in described.stdout

    generic = runner.invoke(
        app,
        [
            "benchmark",
            "--suite",
            "baseline",
            "--difficulty",
            "1",
            "--output-dir",
            str(tmp_path / "generic"),
        ],
    )
    assert generic.exit_code == 0, generic.stdout
    assert "Benchmark generated:" in generic.stdout


def test_m21_rejects_unknown_pack_reference() -> None:
    with pytest.raises(ValueError, match="public pack reference"):
        load_public_pack("FT-B99-UNKNOWN@1.0")
