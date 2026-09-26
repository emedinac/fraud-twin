"""Focused deterministic scale tests (small fixtures only)."""

import json
from pathlib import Path
from random import Random

import pytest
from pydantic import ValidationError

from fraudtwin.config import ScaleConfig, SimulationRunConfig, load_config
from fraudtwin.generation import (
    _scale_records,
    generate,
    generate_scale,
    iter_scale_records,
    iter_scale_run,
    resume_generation,
)
from fraudtwin.scale import (
    ScalePlan,
    aggregate_fingerprint,
    checkpoint_fingerprint,
    iter_chunks,
    iter_partition_rows,
    iter_partition_table,
    iter_payment_ranges,
    load_checkpoint,
    partition_id,
    reconcile_logical_ids,
    require_scale_plan,
    resolve_scale_plan,
    write_scale_benchmark_manifest,
    write_scale_partitions,
)
from fraudtwin.simulation import BehaviorGenerator
from fraudtwin.simulation.generator import EntityGenerator
from fraudtwin.simulation.payments import PaymentGenerator


def _scale_config():
    base = load_config(Path("configs/minimal-v1.yaml"))
    return base.model_copy(
        update={
            "scale": ScaleConfig(
                profile="small",
                target_payments=100,
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


def test_dev_scale_profile_is_bounded() -> None:
    config = load_config(Path("configs/scale-dev.yaml"))
    plan = resolve_scale_plan(config)
    assert plan is not None
    assert plan.target_payments == 1_000


def test_require_scale_plan_narrows_enabled_and_rejects_disabled() -> None:
    assert require_scale_plan(_scale_config()).target_payments == 100
    with pytest.raises(ValueError, match="scale generation is not enabled"):
        require_scale_plan(load_config(Path("configs/minimal-v1.yaml")))


def test_disabled_scale_preserves_legacy_config_hash() -> None:
    base = load_config(Path("configs/minimal-v1.yaml"))
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


def test_payment_ranges_cover_target_without_overlap() -> None:
    ranges = list(iter_payment_ranges(10, 3))
    assert [(item[2], item[3]) for item in ranges] == [(0, 3), (3, 6), (6, 10)]
    assert sum(end - start for _, _, start, end in ranges) == 10


def test_aggregate_fingerprint_is_order_sensitive_and_streamable() -> None:
    first = aggregate_fingerprint(({"logical_id": "a"}, {"logical_id": "b"}))
    second = aggregate_fingerprint(({"logical_id": "b"}, {"logical_id": "a"}))
    assert first != second


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
    rows = tuple(iter_partition_rows(first.run_dir))
    assert len(rows) == checkpoint.reconciliation.partition_row_count
    assert checkpoint.completed_chunks
    assert {
        "entity_generation",
        "behavior_generation",
        "profile_generation",
        "payments_lifecycle",
        "fraud_graph_quality",
        "scale_serialization",
        "checkpoint_reconciliation",
    } <= set(first.stage_timings)
    assert tuple(iter_partition_table(first.run_dir, "payments"))
    projected = next(iter_partition_table(first.run_dir, "payments", columns=["logical_id"]))
    assert set(projected) == {"logical_id"}


def test_scale_benchmark_manifest_records_target_and_host(tmp_path: Path) -> None:
    destination = write_scale_benchmark_manifest(
        tmp_path / "benchmark.json",
        target_payments=10,
        realized_counts={"payments": 10, "payment_events": 12},
        configuration_hash="a" * 64,
        seed=7,
        shard_count=2,
        worker_count=1,
        elapsed_seconds=2.0,
        stage_timings={"entity_generation": {"elapsed_seconds": 0.5, "peak_rss_mb": 10.0}},
    )
    payload = json.loads(destination.read_text(encoding="utf-8"))
    assert payload["target_met"] is True
    assert payload["throughput_payments_per_second"] == 5.0
    assert payload["claim_scope"] == "laptop-dev-only"
    assert payload["git_revision"]
    assert payload["stage_timings"]["entity_generation"]["peak_rss_mb"] == 10.0


def test_scale_target_mismatch_is_rejected_before_output(tmp_path: Path) -> None:
    config = _scale_config().model_copy(
        update={"scale": _scale_config().scale.model_copy(update={"target_payments": 101})}
    )
    with pytest.raises(ValueError, match="scale target not met"):
        generate(config, write=True, output_dir=tmp_path / "runs")


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


def test_streaming_scale_records_preserve_canonical_tables() -> None:
    config = _scale_config()
    entities = EntityGenerator(config).generate()
    reference = generate(config)
    expected = list(_scale_records(reference.entities, reference.behavior))
    actual = list(iter_scale_records(config, entities, simulation_run_id=reference.run_id))
    assert actual == expected


def test_payment_lookup_caches_preserve_ordered_candidates() -> None:
    config = _scale_config()
    entities = EntityGenerator(config).generate()
    generator = PaymentGenerator(
        config,
        entities.accounts,
        entities.cards,
        entities.merchants,
        entities.devices,
        entities.pix_keys,
        simulation_run_id="RUN-cache",
    )
    profiles = BehaviorGenerator(config, entities).generate_profiles()
    eligible = generator._eligible_profiles(profiles)  # noqa: SLF001
    assert eligible
    profile = eligible[0]
    assert (
        generator._available_rails(profile)
        == generator._eligible_rails_by_customer[  # noqa: SLF001
            profile.customer_id
        ]
    )
    assert profile.behavior_profile_id in generator._profile_time_cache  # noqa: SLF001
    payer = generator.accounts_by_customer[profile.customer_id][0].account_id
    assert generator._payee_account(payer, Random(1)).account_id in {  # noqa: SLF001
        account.account_id for account in entities.accounts
    }


def test_explicit_scale_api_and_partition_reader(tmp_path: Path) -> None:
    result = generate_scale(_scale_config(), output_dir=tmp_path / "runs")
    rows = iter_scale_run(run_dir=result.run_dir)
    assert next(rows)["logical_type"] in {
        *result.manifest.entity_counts.keys(),
        "behavior_profiles",
    }


def test_worker_counts_preserve_canonical_scale_output(tmp_path: Path) -> None:
    fingerprints = []
    for workers in (1, 2, 16):
        config = _scale_config().model_copy(
            update={"scale": _scale_config().scale.model_copy(update={"worker_count": workers})}
        )
        result = generate(
            config,
            write=True,
            output_dir=tmp_path / f"runs-{workers}",
            checkpoint_dir=tmp_path / f"checkpoint-{workers}",
        )
        fingerprints.append(
            (
                result.run_id,
                result.manifest.scenario_config_hash,
                result.manifest.output_fingerprint,
                result.manifest.scale["partition_fingerprints"],
            )
        )
    assert len({item[0] for item in fingerprints}) == 1
    assert len({item[1] for item in fingerprints}) == 1
    assert len({item[2] for item in fingerprints}) == 1
    assert len({str(item[3]) for item in fingerprints}) == 1


def test_scale_feature_selection_skips_unrequested_expensive_stages() -> None:
    config = _scale_config().model_copy(
        update={
            "scale": _scale_config().scale.model_copy(
                update={"features": ("entities", "behavior", "payments")}
            )
        }
    )
    result = generate(config)
    assert result.dataset is None
    assert result.behavior.ledger_entries == ()
    assert len(result.behavior.payment_events) == len(result.behavior.payments)


def test_scale_feature_matrix_rejects_enabled_stage_without_feature() -> None:
    config = _scale_config().model_copy(
        update={
            "fraud": _scale_config().fraud.model_copy(update={"enabled": True}),
            "scale": _scale_config().scale.model_copy(
                update={"features": ("entities", "behavior", "payments")}
            ),
        }
    )
    with pytest.raises(ValueError, match="fraud.enabled requires scale feature 'fraud'"):
        resolve_scale_plan(config)


def test_scale_writer_detects_duplicate_ids_without_global_memory(tmp_path: Path) -> None:
    config = _scale_config()
    plan = resolve_scale_plan(config, run_id="RUN-duplicate")
    assert plan is not None
    _, reconciliation, _ = write_scale_partitions(
        tmp_path / "run",
        plan,
        (
            {"logical_id": "PAY-1", "logical_type": "payments"},
            {"logical_id": "PAY-1", "logical_type": "payments"},
        ),
    )
    assert reconciliation.valid is False
    assert reconciliation.duplicate_logical_ids == ("PAY-1",)
