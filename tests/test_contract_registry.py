import hashlib
import json
import shutil
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import avro.schema
import pytest
import yaml
from typer.testing import CliRunner

from fraudtwin.cli import app
from fraudtwin.config import load_config
from fraudtwin.contracts import ContractValidationError, contract_registry, load_contract_registry
from fraudtwin.domain import (
    CustomerDispute,
    DelayedFraudLabel,
    FraudAlert,
    FraudCase,
    FraudCaseConfirmation,
)
from fraudtwin.generation import generate


def _workflow_records() -> (
    tuple[
        FraudAlert,
        FraudCase,
        FraudCaseConfirmation,
        CustomerDispute,
        DelayedFraudLabel,
    ]
):
    start = datetime(2026, 1, 1, 12, tzinfo=UTC)
    common = {
        "customer_id": "C-1",
        "account_id": "A-1",
        "card_id": None,
        "device_id": "D-1",
        "merchant_id": "M-1",
        "payment_id": "PAY-1",
        "event_id": "EVT-1",
        "fraud_record_id": "FR-1",
        "scenario_id": "SCN-1",
        "scenario_type": "F01",
        "amount": 12.34,
        "currency": "EUR",
        "correlation_id": "PAY-1",
        "simulation_run_id": "RUN-1",
        "affected_entity_ids": ("C-1", "A-1", "D-1"),
    }
    alert = FraudAlert(
        fraud_alert_id="ALT-1",
        alert_type="AUTOMATED_SCENARIO_ALERT",
        severity="MEDIUM",
        trigger="velocity",
        reason="repeated attempts",
        alert_created_at=start + timedelta(minutes=1),
        causation_id="EVT-1",
        **common,
    )
    case = FraudCase(
        fraud_case_id="CASE-1",
        fraud_alert_id="ALT-1",
        fraud_truth=True,
        fraud_occurred_at=start,
        alert_created_at=start + timedelta(minutes=1),
        case_opened_at=start + timedelta(minutes=6),
        case_closed_at=start + timedelta(days=1),
        fraud_confirmed_at=start + timedelta(days=1),
        label_available_at=start + timedelta(days=1, hours=1),
        investigation_outcome="CONFIRMED_FRAUD",
        loss_amount=12.34,
        recovered_amount=0.0,
        causation_id="ALT-1",
        **{key: value for key, value in common.items() if key != "event_id"},
        event_id="EVT-1",
    )
    confirmation = FraudCaseConfirmation(
        confirmation_id="CNF-1",
        fraud_case_id="CASE-1",
        fraud_alert_id="ALT-1",
        confirmed_at=start + timedelta(days=1),
        investigation_outcome="CONFIRMED_FRAUD",
        causation_id="CASE-1",
        fraud_truth=True,
        **{
            key: common[key]
            for key in common
            if key
            in {
                "customer_id",
                "payment_id",
                "event_id",
                "fraud_record_id",
                "scenario_id",
                "scenario_type",
                "amount",
                "currency",
                "correlation_id",
                "simulation_run_id",
                "affected_entity_ids",
            }
        },
    )
    dispute = CustomerDispute(
        event_id="DSP-1",
        event_type="CUSTOMER_DISPUTE_SUBMITTED",
        event_version=1,
        fraud_case_id="CASE-1",
        fraud_alert_id="ALT-1",
        underlying_event_id="EVT-1",
        event_time=start + timedelta(days=2),
        source_created_at=start + timedelta(days=2),
        source_available_at=start + timedelta(days=2, seconds=2),
        ingested_at=start + timedelta(days=2, seconds=2),
        processed_at=start + timedelta(days=2, seconds=3),
        producer="fraudtwin",
        source_system="simulated-bank",
        schema_version="1",
        causation_id="CASE-1",
        payment_rail="CARD",
        payment_type="PURCHASE",
        **{key: value for key, value in common.items() if key != "event_id"},
    )
    label = DelayedFraudLabel(
        label_id="LBL-1",
        fraud_case_id="CASE-1",
        fraud_alert_id="ALT-1",
        event_id="EVT-1",
        label="FRAUD",
        fraud_truth=True,
        fraud_occurred_at=start,
        fraud_confirmed_at=start + timedelta(days=1),
        dispute_event_at=start + timedelta(days=2),
        label_available_at=start + timedelta(days=2, hours=1),
        investigation_outcome="CONFIRMED_FRAUD",
        causation_id="DSP-1",
        **{
            key: value
            for key, value in common.items()
            if key
            in {
                "customer_id",
                "payment_id",
                "fraud_record_id",
                "scenario_id",
                "scenario_type",
                "amount",
                "currency",
                "correlation_id",
                "simulation_run_id",
                "affected_entity_ids",
            }
        },
    )
    return alert, case, confirmation, dispute, label


def test_bundled_registry_validates_and_cli_reports_subjects() -> None:
    registry = contract_registry()
    assert len(registry.subjects) == 6
    result = CliRunner().invoke(app, ["schema", "validate"])
    assert result.exit_code == 0, result.stdout
    assert "Subjects: 6; versions: 6" in result.stdout


def test_clean_generated_payment_event_round_trips_without_mutating_source() -> None:
    config = load_config(Path("configs/minimal.yaml"))
    generated = generate(config, write=False)
    event = generated.behavior.payment_events[0]
    original_amount = event.amount
    registry = contract_registry()
    datum = registry.mapper().to_datum("payment-event", event)
    assert datum["amount"] == Decimal(str(original_amount)).quantize(Decimal("0.01"))
    assert datum["event_time"].tzinfo is not None
    decoded = registry.decode("payment-event", registry.encode("payment-event", event))
    assert decoded["event_id"] == event.event_id
    assert decoded["amount"] == datum["amount"]
    assert event.amount == original_amount


def test_observable_workflow_contracts_mask_latent_truth() -> None:
    alert, case, confirmation, dispute, label = _workflow_records()
    registry = contract_registry()
    records = {
        "fraud-alert": alert,
        "fraud-case": case,
        "fraud-case-confirmation": confirmation,
        "customer-dispute": dispute,
        "fraud-label": label,
    }
    for subject, record in records.items():
        datum = registry.datum(subject, record)
        assert "fraud_truth" not in datum
        assert "truth_label" not in datum
        assert registry.decode(subject, registry.encode(subject, record))


def _copy_registry(tmp_path: Path) -> tuple[Path, dict]:
    source = Path("contracts/avro")
    destination = tmp_path / "avro"
    shutil.copytree(source, destination)
    metadata = yaml.safe_load((destination / "registry.yaml").read_text(encoding="utf-8"))
    return destination, metadata


def _add_version(
    root: Path,
    metadata: dict,
    subject_name: str,
    version: str,
    schema_text: str,
    **extra: object,
) -> None:
    subject = next(item for item in metadata["subjects"] if item["name"] == subject_name)
    relative = f"{subject_name}/{version}.avsc"
    (root / relative).write_text(schema_text, encoding="utf-8")
    parsed = avro.schema.parse(schema_text)
    entry = {
        "version": version,
        "path": relative,
        "canonical_sha256": hashlib.sha256(parsed.canonical_form.encode("utf-8")).hexdigest(),
        "breaking": False,
        **extra,
    }
    subject["versions"].append(entry)
    subject["latest"] = version
    (root / "registry.yaml").write_text(yaml.safe_dump(metadata, sort_keys=False), encoding="utf-8")


def test_full_transitive_compatibility_accepts_defaulted_addition_and_rejects_breaking_changes(
    tmp_path: Path,
) -> None:
    root, metadata = _copy_registry(tmp_path)
    original = (root / "payment-event/1.0.0.avsc").read_text(encoding="utf-8")
    parsed = json.loads(original)
    parsed["fields"].append(
        {
            "name": "producer_region",
            "type": ["null", "string"],
            "default": None,
        }
    )
    _add_version(root, metadata, "payment-event", "1.1.0", json.dumps(parsed))
    assert load_contract_registry(root).validate().versions == 7

    root, metadata = _copy_registry(tmp_path / "required")
    parsed = json.loads((root / "payment-event/1.0.0.avsc").read_text(encoding="utf-8"))
    parsed["fields"].append({"name": "required_region", "type": "string"})
    _add_version(root, metadata, "payment-event", "1.1.0", json.dumps(parsed))
    with pytest.raises(ContractValidationError, match="FULL_TRANSITIVE"):
        load_contract_registry(root).validate()


def test_explicit_breaking_major_subject_is_allowed_but_unmarked_breaking_is_rejected(
    tmp_path: Path,
) -> None:
    root, metadata = _copy_registry(tmp_path)
    original = json.loads((root / "payment-event/1.0.0.avsc").read_text(encoding="utf-8"))
    original["name"] = "PaymentEventV2"
    original["namespace"] = "fraudtwin.events.v2"
    original["fields"][0]["type"] = "long"
    subject = {
        "name": "payment-event-v2",
        "record_name": "fraudtwin.events.v2.PaymentEventV2",
        "latest": "2.0.0",
        "versions": [],
    }
    metadata["subjects"].append(subject)
    (root / "payment-event-v2").mkdir()
    relative = "payment-event-v2/2.0.0.avsc"
    (root / relative).write_text(json.dumps(original), encoding="utf-8")
    parsed = avro.schema.parse(json.dumps(original))
    subject["versions"].append(
        {
            "version": "2.0.0",
            "path": relative,
            "canonical_sha256": hashlib.sha256(parsed.canonical_form.encode("utf-8")).hexdigest(),
            "breaking": True,
            "supersedes": "payment-event",
        }
    )
    (root / "registry.yaml").write_text(yaml.safe_dump(metadata, sort_keys=False), encoding="utf-8")
    assert load_contract_registry(root).validate().subjects == 7

    root, metadata = _copy_registry(tmp_path / "unmarked")
    parsed = json.loads((root / "payment-event/1.0.0.avsc").read_text(encoding="utf-8"))
    parsed["fields"][0]["type"] = "long"
    _add_version(root, metadata, "payment-event", "1.1.0", json.dumps(parsed))
    with pytest.raises(ContractValidationError):
        load_contract_registry(root).validate()


def test_full_transitive_rejects_removal_enum_changes_and_bad_metadata(tmp_path: Path) -> None:
    root, metadata = _copy_registry(tmp_path / "removed")
    parsed = json.loads((root / "payment-event/1.0.0.avsc").read_text(encoding="utf-8"))
    parsed["fields"] = [field for field in parsed["fields"] if field["name"] != "event_id"]
    _add_version(root, metadata, "payment-event", "1.1.0", json.dumps(parsed))
    with pytest.raises(ContractValidationError, match="FULL_TRANSITIVE"):
        load_contract_registry(root).validate()

    root, metadata = _copy_registry(tmp_path / "enum")
    parsed = json.loads((root / "payment-event/1.0.0.avsc").read_text(encoding="utf-8"))
    next(field for field in parsed["fields"] if field["name"] == "event_type")["type"] = {
        "type": "enum",
        "name": "PaymentEventType",
        "symbols": ["ONLY_ONE"],
    }
    _add_version(root, metadata, "payment-event", "1.1.0", json.dumps(parsed))
    with pytest.raises(ContractValidationError, match="FULL_TRANSITIVE"):
        load_contract_registry(root).validate()

    root, metadata = _copy_registry(tmp_path / "metadata")
    metadata["compatibility"] = "BACKWARD"
    (root / "registry.yaml").write_text(yaml.safe_dump(metadata, sort_keys=False), encoding="utf-8")
    with pytest.raises(ContractValidationError, match="FULL_TRANSITIVE"):
        load_contract_registry(root).validate()


def test_breaking_revision_cannot_be_added_to_the_original_subject(tmp_path: Path) -> None:
    root, metadata = _copy_registry(tmp_path)
    original = json.loads((root / "payment-event/1.0.0.avsc").read_text(encoding="utf-8"))
    original["fields"][0]["type"] = "long"
    _add_version(
        root,
        metadata,
        "payment-event",
        "2.0.0",
        json.dumps(original),
        breaking=True,
        supersedes="payment-event",
    )
    with pytest.raises(ContractValidationError, match="new subject"):
        load_contract_registry(root).validate()
