"""Focused Milestone 18 deterministic scale tests (small fixtures only)."""

from pathlib import Path

import pytest
from pydantic import ValidationError

from fraudtwin.config import ScaleConfig, SimulationRunConfig, load_config
from fraudtwin.generation import generate, resume_generation
from fraudtwin.scale import (
    ScalePlan,
    checkpoint_fingerprint,
    iter_chunks,
    load_checkpoint,
    partition_id,
    reconcile_logical_ids,
    resolve_scale_plan,
)


def _scale_config():
    base = load_config(Path("configs/minimal.yaml"))
    return base.model_copy(
        update={
            "scale": ScaleConfig(
                profile="small",
                shard_count=3,
                chunk_size=7,
                worker_count=2,
                output_batch_size=7,
                checkpoint_frequency_chunks=1,
                partition_mapping="stable_hash_v1",
            )
        }
    )


def test_scale_profiles_and_strict_controls() -> None:
    config = _scale_config()
    plan = resolve_scale_plan(config)
    assert plan is not None
    assert plan.target_logical_events == 100_000
    assert plan.seed_tree_version == "M18-seed-tree-1"
    values = config.model_dump(mode="python")
    values["scale"]["unknown"] = True
    with pytest.raises(ValidationError):
        SimulationRunConfig.model_validate(values)


def test_disabled_scale_preserves_legacy_config_hash() -> None:
    base = load_config(Path("configs/minimal.yaml"))
    values = base.model_dump(mode="python")
    values["scale"] = {"profile": None}
    assert SimulationRunConfig.model_validate(values).scale.enabled is False


def test_partition_mapping_and_chunks_are_stable() -> None:
    assert partition_id("payments:P-0001", 4) == partition_id("payments:P-0001", 4)
    chunks = list(iter_chunks("SHARD-000000", 0, 17, 7))
    assert [(item.start_ordinal, item.end_ordinal) for item in chunks] == [
        (0, 7),
        (7, 14),
        (14, 17),
    ]


def test_reconciliation_detects_duplicate_and_missing_ids() -> None:
    result = reconcile_logical_ids(["a", "b"], ["a", "a"], expected_ids=["a", "b"])
    assert result.valid is False
    assert result.duplicate_logical_ids == ("a",)
    assert result.missing_logical_ids == ("b",)


def test_scale_generation_and_resume_are_reproducible(tmp_path: Path) -> None:
    config = _scale_config()
    first = generate(
        config, write=True, output_dir=tmp_path / "runs", checkpoint_dir=tmp_path / "checkpoint"
    )
    checkpoint = load_checkpoint(tmp_path / "checkpoint" / "checkpoint.json")
    assert first.manifest.scale is not None
    assert checkpoint.reconciliation is not None and checkpoint.reconciliation.valid
    resumed = resume_generation(tmp_path / "checkpoint")
    assert resumed.run_id == first.run_id
    assert resumed.manifest.scale is not None
    assert checkpoint_fingerprint(checkpoint) == checkpoint_fingerprint(
        load_checkpoint(tmp_path / "checkpoint" / "checkpoint.json")
    )


def test_scale_plan_is_typed() -> None:
    plan = ScalePlan(
        profile="small",
        target_logical_events=100_000,
        shard_count=1,
        chunk_size=1,
        worker_count=1,
        output_batch_size=1,
        checkpoint_frequency_chunks=1,
        partition_mapping="stable_hash_v1",
        seed=0,
        run_id="RUN-test",
        configuration_hash="a" * 64,
    )
    assert plan.profile == "small"
