import re

import pytest

import fraudtwin
from fraudtwin.vocabulary import CAPACITIES, FRAUD_SCENARIOS, PROTOCOLS


def test_public_vocabulary_uses_unique_letter_and_two_digit_ids() -> None:
    for prefix, entries in (("F", FRAUD_SCENARIOS), ("P", PROTOCOLS), ("C", CAPACITIES)):
        identifiers = [entry.id for entry in entries]
        assert len(identifiers) == len(set(identifiers))
        assert all(re.fullmatch(rf"{prefix}[0-9]{{2}}", identifier) for identifier in identifiers)


def test_scenario_protocol_and_capacity_relationships_are_documented() -> None:
    expected = {
        "F01": ("P05", "C01", "C04", "C09"),
        "F02": ("P06", "C01", "C04", "C09"),
        "F03": ("P07", "C01", "C04", "C08"),
        "F04": ("P08", "C01", "C04", "C12"),
        "F05": ("P09", "C01", "C04", "C09"),
    }

    for scenario_id, related_ids in expected.items():
        assert fraudtwin.get_scenario(scenario_id).related_ids == related_ids


def test_protocol_legacy_aliases_resolve_without_becoming_canonical_ids() -> None:
    assert fraudtwin.get_protocol("M6").id == "P01"
    assert fraudtwin.get_protocol("M12").id == "P02"
    assert fraudtwin.get_protocol("M14").id == "P03"
    assert fraudtwin.get_protocol("M15").id == "P04"
    assert all(not entry.id.startswith("M") for entry in PROTOCOLS)


def test_capacity_lookup_explains_ledger_boundary() -> None:
    capacity = fraudtwin.get_capacity("C04")

    assert capacity.name == "ledger-debit-capacity"
    assert "overdraft" in capacity.description


def test_fraud_scenario_defaults_match_configuration_model() -> None:
    config = fraudtwin.load_default_config()
    defaults = dict(fraudtwin.get_scenario("F04").defaults)
    settings = config.fraud.scenarios["F04"]

    assert defaults["enabled"] == str(settings.enabled).lower()
    assert defaults["weight"] == str(settings.weight)
    assert defaults["count"] == str(settings.count)
    assert defaults["attempt_count"] == str(settings.attempt_count)
    assert defaults["window_seconds"] == str(settings.window_seconds)


def test_ledger_error_contains_protocol_and_capacity_metadata() -> None:
    from fraudtwin.errors import LedgerCapacityError

    error = LedgerCapacityError(
        stage="fraud payment ledger",
        account_id="ACC-1",
        payment_id="PAY-1",
        event_id="EVT-1",
        debit_amount=10.0,
        balance_before=0.0,
        balance_after=-10.0,
        overdraft_limit=0.0,
        protocol_id="P08",
        capacity_id="C04",
        scenario_id="F04",
        campaign_id="F04-0001",
    )

    report = error.format_report()
    assert "Scenario: F04" in report
    assert "Protocol: P08" in report
    assert "Capacity: C04" in report
    assert error.as_dict()["campaign_id"] == "F04-0001"


def test_unknown_vocabulary_ids_raise_key_error() -> None:
    with pytest.raises(KeyError):
        fraudtwin.get_capacity("C99")
