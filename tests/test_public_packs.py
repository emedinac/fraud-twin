"""Immutable public benchmark-pack tests."""

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from fraudtwin.benchmark import (
    list_public_packs,
    load_public_pack,
    run_public_benchmark,
    verify_public_benchmark,
)
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


def _pack_identity(pack_id: str) -> str:
    return next(pack.identity for pack in list_public_packs() if pack.id == pack_id)


def _pack_minor_reference(pack_id: str) -> str:
    pack = next(pack for pack in list_public_packs() if pack.id == pack_id)
    major, minor, _ = pack.version.split(".")
    return f"{pack.id}@{major}.{minor}"


def test_public_pack_registry_contains_the_complete_initial_pack_family() -> None:
    packs = list_public_packs()
    assert tuple(pack.id for pack in packs) == PACK_IDS
    assert len({pack.version for pack in packs}) == 1
    assert all(pack.expected_descriptors for pack in packs)
    assert all(pack.expected_fingerprints for pack in packs)
    assert load_public_pack(_pack_minor_reference("FT-B04-CAMOUFLAGE")).identity == _pack_identity(
        "FT-B04-CAMOUFLAGE"
    )


@pytest.mark.parametrize("pack_id", PACK_IDS)
def test_public_pack_runs_match_frozen_goldens(pack_id: str, tmp_path: Path) -> None:
    identity = _pack_identity(pack_id)
    result = run_public_benchmark(identity, output_dir=tmp_path)
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["public_pack"]["identity"] == identity
    assert manifest["public_pack"]["verification"]["descriptors_match"] is True
    if pack_id == "FT-B04-CAMOUFLAGE":
        row = result.results[0]
        assert row["pr_auc"] is not None
        assert row["f1"] is not None


def test_public_pack_repeated_executions_are_identical(tmp_path: Path) -> None:
    identity = _pack_identity("FT-B08-MIXED")
    first = run_public_benchmark(identity, output_dir=tmp_path / "first")
    second = run_public_benchmark(identity, output_dir=tmp_path / "second")
    first_manifest = json.loads(first.manifest_path.read_text(encoding="utf-8"))
    second_manifest = json.loads(second.manifest_path.read_text(encoding="utf-8"))
    assert (
        first_manifest["public_pack"]["definition_fingerprint"]
        == second_manifest["public_pack"]["definition_fingerprint"]
    )
    assert first_manifest["output_fingerprint"] == second_manifest["output_fingerprint"]


def test_public_pack_cli_describe_and_legacy_generic_command(tmp_path: Path) -> None:
    runner = CliRunner()
    described = runner.invoke(app, ["benchmark", "describe", "FT-B01-STABLE@0.34"])
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


def test_public_pack_verifies_existing_artifact_and_rejects_tampered_descriptors(
    tmp_path: Path,
) -> None:
    result = run_public_benchmark(_pack_identity("FT-B01-STABLE"), output_dir=tmp_path / "run")
    verified = verify_public_benchmark(result.root)
    assert verified["descriptors_match"] is True

    descriptors = json.loads(result.descriptors_path.read_text(encoding="utf-8"))
    descriptors["baseline"]["fraud_prevalence"] = -1.0
    result.descriptors_path.write_text(json.dumps(descriptors), encoding="utf-8")
    with pytest.raises(ValueError, match="descriptors differ"):
        verify_public_benchmark(result.root)

    runner = CliRunner()
    checked = runner.invoke(app, ["benchmark", "verify", str(result.root)])
    assert checked.exit_code == 1


def test_public_pack_rejects_unknown_pack_reference() -> None:
    with pytest.raises(ValueError, match="public pack reference"):
        load_public_pack("FT-B99-UNKNOWN@0.34")

    with pytest.raises(ValueError, match="does not exist"):
        load_public_pack("FT-B01-STABLE@1.0.0")
