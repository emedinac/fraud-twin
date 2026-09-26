from pathlib import Path

import pytest

from fraudtwin.config import load_config
from fraudtwin.errors import GenerationError, LedgerCapacityError
from fraudtwin.simulation import BehaviorGenerator, EntityGenerator
from fraudtwin.simulation.payments import PaymentGenerator


def test_ledger_capacity_error_exposes_structured_diagnostics() -> None:
    error = LedgerCapacityError(
        stage="baseline payment ledger",
        account_id="ACC-000228",
        payment_id="PAY-000001",
        event_id="EVT-000001",
        debit_amount=537.34,
        balance_before=345.11,
        balance_after=-192.23,
        overdraft_limit=100.0,
    )

    assert isinstance(error, ValueError)
    assert isinstance(error, GenerationError)
    assert error.code == "LEDGER_OVERDRAFT_EXCEEDED"
    assert error.stage == "baseline payment ledger"
    assert error.account_id == "ACC-000228"
    assert error.payment_id == "PAY-000001"
    assert error.event_id == "EVT-000001"
    assert error.debit_amount == 537.34
    assert error.balance_before == 345.11
    assert error.balance_after == -192.23
    assert error.overdraft_limit == 100.0
    assert error.available_debit_capacity == 445.11
    assert error.invariant == "balance_after >= -overdraft_limit"
    assert "reduce payments.daily_target" in error.suggested_actions
    assert "permitted overdraft boundary" in str(error)
    report = error.format_report()
    assert "Generation failed [LEDGER_OVERDRAFT_EXCEEDED]" in report
    assert "Account: ACC-000228" in report
    assert "reduce payments.daily_target" in report
    assert error.as_dict()["account_id"] == "ACC-000228"


def test_low_balance_failure_reports_the_first_payment_context() -> None:
    config = load_config(Path("configs/minimal-v1.yaml"))
    config = config.model_copy(
        update={
            "payments": config.payments.model_copy(
                update={
                    "daily_target": 1,
                    "rails": {"CARD": 0.0, "PIX": 0.0, "ACCOUNT_TRANSFER": 1.0},
                }
            )
        }
    )
    entities = EntityGenerator(config).generate()
    profiles = BehaviorGenerator(config, entities).generate_profiles()
    generator = PaymentGenerator(
        config,
        entities.accounts,
        entities.cards,
        entities.merchants,
        entities.devices,
        entities.pix_keys,
        include_lifecycle=False,
        include_ledger=False,
    )
    payment, event = next(generator.iter_generate(profiles))
    low_accounts = tuple(
        account.model_copy(update={"ledger_balance": 0.0, "overdraft_limit": 0.0})
        if account.account_id == payment.payer_account_id
        else account
        for account in entities.accounts
    )
    low_generator = PaymentGenerator(
        config,
        low_accounts,
        entities.cards,
        entities.merchants,
        entities.devices,
        entities.pix_keys,
        include_lifecycle=False,
        include_ledger=False,
    )

    with pytest.raises(LedgerCapacityError) as raised:
        low_generator.materialize_ledger((payment,), (event,), stage="baseline payment ledger")

    error = raised.value
    assert error.protocol_id == "P01"
    assert error.account_id == payment.payer_account_id
    assert error.payment_id == payment.payment_id
    assert error.event_id == event.event_id
    assert error.debit_amount == payment.amount
    assert error.balance_before == 0.0
    assert error.balance_after == -payment.amount
    assert error.overdraft_limit == 0.0
    assert error.stage == "baseline payment ledger"


@pytest.mark.parametrize(
    ("scenario_id", "protocol_id", "expected_protocol"),
    [
        ("F04", "P01", "P08"),
        ("F04", "P02", "P02"),
        ("F04", "P04", "P04"),
        ("MULE_NETWORK", "P01", "P01"),
    ],
)
def test_scenario_ledger_failure_reports_scenario_protocol(
    scenario_id: str, protocol_id: str, expected_protocol: str
) -> None:
    config = load_config(Path("configs/minimal-v1.yaml"))
    config = config.model_copy(
        update={
            "payments": config.payments.model_copy(
                update={
                    "daily_target": 1,
                    "rails": {"CARD": 0.0, "PIX": 0.0, "ACCOUNT_TRANSFER": 1.0},
                }
            )
        }
    )
    entities = EntityGenerator(config).generate()
    profiles = BehaviorGenerator(config, entities).generate_profiles()
    generator = PaymentGenerator(
        config,
        entities.accounts,
        entities.cards,
        entities.merchants,
        entities.devices,
        entities.pix_keys,
        include_lifecycle=False,
        include_ledger=False,
    )
    payment, event = next(generator.iter_generate(profiles))
    scenario_event = event.model_copy(
        update={"scenario_type": scenario_id, "scenario_id": "campaign-1"}
    )
    low_accounts = tuple(
        account.model_copy(update={"ledger_balance": 0.0, "overdraft_limit": 0.0})
        if account.account_id == payment.payer_account_id
        else account
        for account in entities.accounts
    )
    low_generator = PaymentGenerator(
        config,
        low_accounts,
        entities.cards,
        entities.merchants,
        entities.devices,
        entities.pix_keys,
        include_lifecycle=False,
        include_ledger=False,
    )

    with pytest.raises(LedgerCapacityError) as raised:
        low_generator.materialize_ledger(
            (payment,), (scenario_event,), stage="fraud payment ledger", protocol_id=protocol_id
        )

    error = raised.value
    assert error.scenario_id == scenario_id
    assert error.protocol_id == expected_protocol
    assert error.as_dict()["campaign_id"] == "campaign-1"
    assert error.capacity_id == "C04"
