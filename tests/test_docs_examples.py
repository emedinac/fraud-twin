"""Small executable checks for the examples shown in the documentation."""

from pathlib import Path

from fraudtwin.config import load_config
from fraudtwin.generation import generate


def test_quickstart_configuration_generates_a_reproducible_run() -> None:
    config = load_config(Path("configs/minimal-v1.yaml"))

    first = generate(config, write=False)
    second = generate(config, write=False)

    assert first.run_id == second.run_id
    assert first.behavior.payments == second.behavior.payments
    assert first.manifest.event_counts["payments"] > 0


def test_configuration_scenario_example_is_valid() -> None:
    config = load_config(Path("examples/configuration/five-year-pix-transfer.yaml"))

    assert config.simulation.duration_days == 1826
    assert config.payments.daily_target == 6
    assert config.payments.rails == {
        "CARD": 0.0,
        "PIX": 0.75,
        "ACCOUNT_TRANSFER": 0.25,
    }
    assert config.population.cards == 0
    assert config.dataset.enabled is False


def test_f04_prevalence_example_is_valid() -> None:
    config = load_config(Path("examples/configuration/f04-half-fraud.yaml"))

    assert config.payments.daily_target == 3
    assert config.simulation.duration_days == 1826
    assert config.fraud.enabled is True
    assert config.fraud.target_rate == 1.0
    assert config.fraud.scenario_count == 5478
    assert config.fraud.hard_negative_rate == 0.0
    assert config.fraud.scenarios["F04"].count == 5478
    assert config.fraud.scenarios["F04"].amount_min == 1.0
    assert config.fraud.scenarios["F04"].amount_max == 1.0
