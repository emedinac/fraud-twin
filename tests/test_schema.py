from datetime import UTC, datetime

from fraudtwin.config import SchemaChangeConfig
from fraudtwin.schema import (
    SchemaRegistry,
    apply_schema_change,
    infer_compatibility,
    schema_change_metadata,
)


def _change(operation: str, specification: object, compatibility=None) -> SchemaChangeConfig:
    return SchemaChangeConfig(
        at=datetime(2026, 1, 1, tzinfo=UTC),
        event="payment_events",
        version="6",
        change={operation: specification},
        compatibility=compatibility,
    )


def test_schema_mutations_are_applied_to_serialized_rows_only() -> None:
    row = {"event_id": "EVT-1", "amount": 12.0, "schema_version": "5"}
    assert (
        apply_schema_change(row, _change("add_optional_field", {"risk_reason": None}))[
            "risk_reason"
        ]
        is None
    )
    assert apply_schema_change(row, _change("rename", {"amount": "gross_amount"})) == {
        "event_id": "EVT-1",
        "gross_amount": 12.0,
        "schema_version": "5",
    }
    assert "amount" not in apply_schema_change(row, _change("remove_field", "amount"))
    assert apply_schema_change(row, _change("type", {"amount": "string"}))["amount"] == "12.0"
    assert (
        apply_schema_change(row, _change("nullability", {"risk_reason": True}))["risk_reason"]
        is None
    )
    assert row == {"event_id": "EVT-1", "amount": 12.0, "schema_version": "5"}


def test_schema_compatibility_covers_declared_policies() -> None:
    assert infer_compatibility(_change("add_optional_field", {"x": None})) == "FULLY_COMPATIBLE"
    assert infer_compatibility(_change("enum", {"status": ["NEW", "OLD"]})) == "FORWARD_COMPATIBLE"
    assert infer_compatibility(_change("type", {"amount": "decimal"})) == "BACKWARD_COMPATIBLE"
    assert infer_compatibility(_change("remove_field", "amount")) == "BREAKING"
    assert schema_change_metadata(_change("rename", {"amount": "gross_amount"}))[
        "schema_fingerprint"
    ]


def test_schema_registry_fingerprints_and_rejects_breaking_shape_changes() -> None:
    registry = SchemaRegistry()
    registry.register("5", {"event_id": "string", "amount": "float"})
    registry.register("6", {"event_id": "string", "amount": "float", "risk": "string?"})
    assert registry.compatibility("5", "6") == "FULLY_COMPATIBLE"
    registry.register("7", {"event_id": "string"})
    assert registry.compatibility("6", "7") == "BREAKING"
