"""Focused Milestone 20 fraud stress benchmark tests."""

import json
import sys
from pathlib import Path
from types import ModuleType

import polars as pl
import pytest

from fraudtwin.benchmark import (
    BenchmarkRequest,
    RunnerMetadata,
    _profile_for_suite,
    build_suite_config,
    run_benchmark,
)
from fraudtwin.ml import PredictionRecord, heuristic_predictions


def test_m20_suite_composition_is_strict_and_deterministic() -> None:
    for suite in ("baseline", "temporal", "boundary", "camouflage", "observability"):
        first = build_suite_config(suite, difficulty=7, seed=42)
        second = build_suite_config(suite, difficulty=7, seed=42)
        assert first.model_dump(mode="json") == second.model_dump(mode="json")
        assert first.simulation.seed == 42
        assert first.benchmark.difficulty == 7

    graph = build_suite_config("graph", difficulty=7, seed=42)
    assert graph.graph.enabled
    assert graph.campaign_dynamics.enabled
    assert graph.stress.active


def test_m20_calibrated_suites_require_a_profile(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="requires an existing calibration profile"):
        build_suite_config("calibrated", difficulty=7, seed=42)
    with pytest.raises(ValueError, match="require --calibration-profile"):
        run_benchmark(BenchmarkRequest(suite="mixed", output_dir=tmp_path / "missing-profile"))


def test_m20_companion_reuses_calibration_profile(tmp_path: Path) -> None:
    profile = tmp_path / "profile.yaml"
    profile.write_text("profile_version: test\n")
    request = BenchmarkRequest(suite="calibrated", calibration_profile=profile)
    assert _profile_for_suite(request, "baseline") == profile


def test_m20_baseline_emits_reproducible_artifacts(tmp_path: Path) -> None:
    first = run_benchmark(
        BenchmarkRequest(suite="baseline", difficulty=1, seed=42, output_dir=tmp_path / "first")
    )
    second = run_benchmark(
        BenchmarkRequest(suite="baseline", difficulty=1, seed=42, output_dir=tmp_path / "second")
    )
    assert first.benchmark_id == second.benchmark_id
    assert first.results == second.results
    first_manifest = json.loads(first.manifest_path.read_text())
    second_manifest = json.loads(second.manifest_path.read_text())
    assert first_manifest["output_fingerprint"] == second_manifest["output_fingerprint"]
    assert first.descriptors_path.is_file()
    assert first.results_path.is_file()
    results = pl.read_parquet(first.results_path)
    assert results.select("suite").unique().to_series().to_list() == ["baseline"]
    assert {"pr_auc", "recall_at_fixed_fpr", "f1", "drop_vs_baseline"}.issubset(results.columns)


class _FrameworkRunner:
    metadata = RunnerMetadata(
        model_id="fixture_runner",
        framework="fixture-framework",
        framework_version="1.0",
        required_inputs=("pit_dataset",),
    )

    def predict(self, inputs: object) -> tuple[PredictionRecord, ...]:
        dataset_path = inputs.dataset_path  # type: ignore[attr-defined]
        rows = pl.read_parquet(dataset_path).to_dicts()
        return heuristic_predictions(rows)


def test_m20_framework_neutral_runner_is_evaluated(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = ModuleType("m20_runner_fixture")
    module.make_runner = _FrameworkRunner
    monkeypatch.setitem(sys.modules, "m20_runner_fixture", module)
    result = run_benchmark(
        BenchmarkRequest(
            suite="baseline",
            difficulty=1,
            seed=42,
            output_dir=tmp_path,
            runners=("m20_runner_fixture:make_runner",),
        )
    )
    assert {row["model"] for row in result.results} == {
        "deterministic_heuristic",
        "fixture_runner",
    }
