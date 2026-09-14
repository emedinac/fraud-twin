import json
from datetime import UTC, datetime, timedelta
from functools import cache
from pathlib import Path

import polars as pl
import pytest
import yaml
from hypothesis import given, settings
from hypothesis import strategies as st
from pydantic import ValidationError
from typer.testing import CliRunner

from fraudtwin.cli import app
from fraudtwin.config import OutageConfig, SchemaChangeConfig, SimulationRunConfig, load_config
from fraudtwin.domain import validate_ledger
from fraudtwin.simulation import BehaviorGenerator, EntityGenerator, QualityFaultInjector
from fraudtwin.simulation.parquet import (
    PAYMENT_EVENT_SCHEMA,
    PAYMENT_SCHEMA,
    write_behavior_parquet,
)

CONFIG_PATH = Path("configs/minimal.yaml")
runner = CliRunner()


@cache
def _config(**quality_updates):
    base = load_config(CONFIG_PATH)
    quality = base.quality.model_copy(update={"profile": "clean", **quality_updates})
    return base.model_copy(update={"quality": quality})


def _dataset(config=None):
    if config is None:
        return _clean_dataset()
    config = config or _config()
    entities = EntityGenerator(config).generate()
    return config, entities, BehaviorGenerator(config, entities).generate()


@cache
def _clean_dataset():
    config = _config()
    entities = EntityGenerator(config).generate()
    return config, entities, BehaviorGenerator(config, entities).generate()


@given(
    seed=st.integers(min_value=0, max_value=10_000),
    probability=st.floats(min_value=0.0, max_value=1.0, allow_nan=False, allow_infinity=False),
)
@settings(max_examples=8, deadline=None)
def test_quality_reproducibility_property(seed: int, probability: float) -> None:
    base = load_config(CONFIG_PATH)
    simulation = base.simulation.model_copy(update={"seed": seed})
    clean_quality = base.quality.model_copy(update={"profile": "clean"})
    _, _, clean = _dataset()
    configured = base.model_copy(
        update={
            "simulation": simulation,
            "quality": clean_quality.model_copy(
                update={"duplicate_event_probability": probability}
            ),
        }
    )

    first = QualityFaultInjector(configured).apply(clean)
    second = QualityFaultInjector(configured).apply(clean)

    assert first == second
    assert first.quality_fault_rates["duplicate_events_requested"] == probability
    assert first.quality_diagnostics == second.quality_diagnostics


def test_clean_quality_is_a_m1_to_m7_regression_baseline() -> None:
    config, entities, first = _dataset()
    second = BehaviorGenerator(config, entities).generate()

    assert first == second
    assert first.quality_fault_counts == {
        "duplicate_records": 0,
        "duplicate_events": 0,
        "missing_optional_fields": 0,
        "invalid_values": 0,
        "invalid_enums": 0,
        "invalid_references": 0,
        "negative_amounts": 0,
        "corrupted_timestamps": 0,
        "timezone_errors": 0,
        "schema_mismatches": 0,
        "extreme_values": 0,
        "encoding_errors": 0,
        "partition_skews": 0,
        "late_events": 0,
        "out_of_order_events": 0,
        "source_delay_events": 0,
        "fraud_spikes": 0,
        "traffic_spikes": 0,
    }
    validate_ledger(entities.accounts, first.payments, first.payment_events, first.ledger_entries)


def test_outages_schema_changes_and_oracle_are_recorded(tmp_path: Path) -> None:
    base = load_config(CONFIG_PATH)
    start = datetime(2026, 1, 1, tzinfo=UTC)
    quality = base.quality.model_copy(
        update={
            "outages": (
                OutageConfig(
                    source="payment_events",
                    **{"from": start, "to": start + timedelta(days=1)},
                    behavior="DELAY",
                    delay_seconds=3,
                ),
            ),
            "schema_changes": (SchemaChangeConfig(at=start, event="payment_events", version="9"),),
        }
    )
    config = base.model_copy(update={"quality": quality})
    entities = EntityGenerator(config).generate()
    dataset = BehaviorGenerator(config, entities).generate()
    assert dataset.quality_fault_counts["outage_events"] == len(dataset.payment_events)
    assert dataset.quality_fault_counts["schema_changes"] == len(dataset.payment_events)
    assert all(event.schema_version == "9" for event in dataset.payment_events)
    assert dataset.oracle_tables["payment_events"]
    write_behavior_parquet(dataset, tmp_path / "run")
    assert (tmp_path / "run" / "oracle" / "payments" / "payment_events.parquet").is_file()


def test_schema_changes_mutate_rows_at_boundary_and_encoding_has_raw_artifact(
    tmp_path: Path,
) -> None:
    base = load_config(CONFIG_PATH)
    boundary = datetime(2026, 1, 1, tzinfo=UTC)
    quality = base.quality.model_copy(
        update={
            "encoding_error_probability": 1.0,
            "schema_changes": (
                SchemaChangeConfig(
                    at=boundary,
                    event="payment_events",
                    version="6",
                    change={"rename": {"amount": "gross_amount"}},
                ),
            ),
        }
    )
    config = base.model_copy(update={"quality": quality})
    entities = EntityGenerator(config).generate()
    dataset = BehaviorGenerator(config, entities).generate()
    assert dataset.schema_evolution_rows["payment_events:6"]
    assert all(
        "gross_amount" in row and "amount" not in row
        for row in dataset.schema_evolution_rows["payment_events:6"]
    )
    assert dataset.quality_raw_faults
    write_behavior_parquet(dataset, tmp_path / "run")
    assert (tmp_path / "run" / "quality" / "raw_faults.jsonl").is_file()
    assert (tmp_path / "run" / "quality" / "fault_audit.json").is_file()


@pytest.mark.parametrize(
    ("field", "count_key"),
    [
        ("duplicate_record_probability", "duplicate_records"),
        ("duplicate_event_probability", "duplicate_events"),
        ("missing_optional_probability", "missing_optional_fields"),
        ("invalid_value_probability", "invalid_values"),
        ("invalid_enum_probability", "invalid_enums"),
        ("invalid_reference_probability", "invalid_references"),
        ("negative_amount_probability", "negative_amounts"),
        ("corrupted_timestamp_probability", "corrupted_timestamps"),
        ("timezone_error_probability", "timezone_errors"),
        ("schema_mismatch_probability", "schema_mismatches"),
        ("extreme_value_probability", "extreme_values"),
        ("encoding_error_probability", "encoding_errors"),
        ("partition_skew_probability", "partition_skews"),
        ("late_event_probability", "late_events"),
        ("out_of_order_probability", "out_of_order_events"),
    ],
)
def test_each_quality_fault_can_be_enabled_independently(field: str, count_key: str) -> None:
    _, _, clean = _dataset()
    configured = QualityFaultInjector(_config(**{field: 1.0})).apply(clean)

    assert configured.quality_fault_counts[count_key] > 0
    for other_key, count in configured.quality_fault_counts.items():
        if other_key != count_key:
            assert count == 0


def test_duplicate_records_and_events_keep_identity_and_causal_metadata() -> None:
    _, _, clean = _dataset()
    duplicated = QualityFaultInjector(
        _config(duplicate_record_probability=1.0, duplicate_event_probability=1.0)
    ).apply(clean)

    assert len(duplicated.payments) == len(clean.payments) * 2
    assert len(duplicated.payment_events) == len(clean.payment_events) * 2
    assert len({payment.payment_id for payment in duplicated.payments}) < len(duplicated.payments)
    assert len({event.event_id for event in duplicated.payment_events}) < len(
        duplicated.payment_events
    )
    assert duplicated.payment_events[0] == duplicated.payment_events[1]
    assert duplicated.payments[0] == duplicated.payments[1]
    assert duplicated.quality_diagnostics["key_uniqueness"]["payments"]["duplicate_rows"] > 0


def test_missing_optional_and_invalid_values_are_measured() -> None:
    _, _, clean = _dataset()
    corrupted = QualityFaultInjector(
        _config(missing_optional_probability=1.0, invalid_value_probability=1.0)
    ).apply(clean)

    assert corrupted.quality_fault_counts["missing_optional_fields"] > 0
    assert corrupted.quality_fault_counts["invalid_values"] == len(clean.payments) + len(
        clean.payment_events
    )
    assert any(payment.amount < 0 for payment in corrupted.payments)
    assert any(event.amount < 0 for event in corrupted.payment_events)
    assert any(
        payment_before != payment_after
        for payment_before, payment_after in zip(clean.payments, corrupted.payments, strict=True)
    )


def test_pandera_and_sdmetrics_aligned_diagnostics_distinguish_clean_and_corrupt() -> None:
    _, _, clean = _dataset()
    assert clean.quality_diagnostics["data_validity"]["clean"]["payments"]["valid"] is True
    assert clean.quality_diagnostics["data_validity"]["output"]["payment_events"]["valid"] is True
    assert clean.quality_diagnostics["data_structure"]["payments"]["score"] == 1.0
    assert clean.quality_diagnostics["relationship_validity"]["overall_rate"] == 1.0

    corrupted = QualityFaultInjector(
        _config(invalid_value_probability=1.0, duplicate_event_probability=1.0)
    ).apply(clean)
    output_validity = corrupted.quality_diagnostics["data_validity"]["output"]
    assert output_validity["payments"]["valid"] is False
    assert output_validity["payment_events"]["valid"] is False
    assert corrupted.quality_diagnostics["key_uniqueness"]["payment_events"]["duplicate_rows"] > 0

    import pandas as pd
    from sdmetrics.column_pairs import ReferentialIntegrity
    from sdmetrics.single_column import KeyUniqueness
    from sdmetrics.single_table import TableStructure

    payment_frame = pd.DataFrame([payment.model_dump(mode="python") for payment in clean.payments])
    event_frame = pd.DataFrame([event.model_dump(mode="python") for event in clean.payment_events])
    duplicate_event_frame = pd.DataFrame(
        [event.model_dump(mode="python") for event in corrupted.payment_events]
    )
    assert (
        KeyUniqueness.compute(
            real_data=event_frame["event_id"], synthetic_data=duplicate_event_frame["event_id"]
        )
        < 1.0
    )
    assert (
        TableStructure.compute(real_data=event_frame, synthetic_data=duplicate_event_frame) == 1.0
    )
    assert (
        ReferentialIntegrity.compute(
            real_data=(payment_frame["payment_id"], event_frame["payment_id"]),
            synthetic_data=(payment_frame["payment_id"], duplicate_event_frame["payment_id"]),
        )
        == 1.0
    )


def test_late_events_and_source_delay_preserve_envelope_order() -> None:
    config, _, clean = _dataset()
    delayed_config = config.model_copy(
        update={
            "quality": config.quality.model_copy(
                update={
                    "late_event_probability": 1.0,
                    "late_event_delay_seconds": 13,
                    "source_delay_seconds": 11,
                }
            )
        }
    )
    delayed = QualityFaultInjector(delayed_config).apply(clean)
    clean_by_id = {event.event_id: event for event in clean.payment_events}

    assert delayed.quality_fault_counts["late_events"] == len(clean.payment_events)
    assert delayed.quality_fault_counts["source_delay_events"] == len(clean.payment_events)
    for event in delayed.payment_events:
        original = clean_by_id[event.event_id]
        assert event.event_time <= event.source_created_at
        assert event.source_created_at <= event.source_available_at
        assert event.source_available_at <= event.ingested_at <= event.processed_at
        assert event.processed_at - original.processed_at == timedelta(seconds=24)
    assert delayed.quality_fault_rates["late_events_requested"] == 1.0


def test_out_of_order_changes_delivery_order_not_business_time_or_identity() -> None:
    _, _, clean = _dataset()
    reordered = QualityFaultInjector(_config(out_of_order_probability=1.0)).apply(clean)
    clean_ids = sorted(event.event_id for event in clean.payment_events)
    reordered_ids = sorted(event.event_id for event in reordered.payment_events)

    assert reordered_ids == clean_ids
    groups = {}
    for event in reordered.payment_events:
        groups.setdefault(event.payment_id, []).append(event)
    assert any(
        any(
            current.event_time <= previous.event_time
            for previous, current in zip(events, events[1:], strict=False)
        )
        for events in groups.values()
        if len(events) > 1
    )


def test_spikes_are_reproducible_and_apply_to_existing_events() -> None:
    fraud = load_config(CONFIG_PATH).fraud.model_copy(
        update={"enabled": True, "target_rate": 1.0, "scenario_count": 5}
    )
    config = _config(
        traffic_spike_probability=1.0,
        traffic_spike_multiplier=2,
        fraud_spike_probability=1.0,
        fraud_spike_multiplier=2,
    ).model_copy(update={"fraud": fraud})
    _, _, clean = _dataset(
        config.model_copy(
            update={
                "quality": config.quality.model_copy(
                    update={
                        "traffic_spike_probability": 0.0,
                        "fraud_spike_probability": 0.0,
                    }
                )
            }
        )
    )
    first = QualityFaultInjector(config).apply(clean)
    second = QualityFaultInjector(config).apply(clean)

    assert first == second
    assert first.quality_fault_counts["traffic_spikes"] == len(clean.payments)
    assert first.quality_fault_counts["fraud_spikes"] == 5
    assert len(first.payment_events) > len(clean.payment_events)
    assert {event.event_id for event in clean.payment_events}.issubset(
        {event.event_id for event in first.payment_events}
    )


def test_quality_parquet_schemas_and_manifest_counts_are_stable(tmp_path: Path) -> None:
    config = _config(source_delay_seconds=7)
    entities = EntityGenerator(config).generate()
    dataset = BehaviorGenerator(config, entities).generate()
    paths = write_behavior_parquet(dataset, tmp_path / "run")

    assert pl.read_parquet(paths["payments"]).columns == list(PAYMENT_SCHEMA)
    assert pl.read_parquet(paths["payment_events"]).columns == list(PAYMENT_EVENT_SCHEMA)
    raw = config.model_dump(mode="json")
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.safe_dump(raw), encoding="utf-8")
    result = runner.invoke(app, ["generate", str(config_path), "--output-dir", str(tmp_path)])
    assert result.exit_code == 0, result.stdout
    manifest_path = next(tmp_path.glob("*/manifest.json"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["quality_fault_counts"]["source_delay_events"] > 0
    assert manifest["quality_fault_rates"]["source_delay_seconds"] == 7.0
    assert manifest["quality_diagnostics"]["data_validity"]["output"]["payments"]["valid"] is True
    assert manifest["quality_diagnostics"]["data_structure"]["payment_events"]["score"] == 1.0


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("quality", "profile"), "unknown"),
        (("quality", "duplicate_event_probability"), 1.1),
        (("quality", "late_event_delay_seconds"), -1),
        (("quality", "source_delay_seconds"), -1),
        (("quality", "traffic_spike_multiplier"), 0),
    ],
)
def test_quality_configuration_is_strictly_validated(path: tuple[str, str], value) -> None:
    raw = load_config(CONFIG_PATH).model_dump()
    raw[path[0]][path[1]] = value

    with pytest.raises(ValidationError):
        SimulationRunConfig.model_validate(raw)


def test_unknown_quality_field_is_rejected() -> None:
    raw = load_config(CONFIG_PATH).model_dump()
    raw["quality"]["unknown"] = 1

    with pytest.raises(ValidationError):
        SimulationRunConfig.model_validate(raw)


def test_specification_quality_aliases_are_supported() -> None:
    raw = load_config(CONFIG_PATH).model_dump()
    raw["quality"] = {
        "profile": "realistic",
        "duplicate_records": 0.1,
        "duplicate_events": 0.2,
        "missing_optional": 0.3,
        "invalid_records": 0.4,
        "late_events": 0.5,
        "out_of_order_events": 0.6,
        "source_delay": 7,
        "late_event_delay": 11,
    }

    config = SimulationRunConfig.model_validate(raw)

    assert config.quality.duplicate_record_probability == 0.1
    assert config.quality.invalid_value_probability == 0.4
    assert config.quality.source_delay_seconds == 7
