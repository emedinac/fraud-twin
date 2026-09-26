from pathlib import Path

import pytest
from pydantic import ValidationError

from fraudtwin.config import SimulationRunConfig, config_hash, load_config, load_default_config
from fraudtwin.seed import create_rng

CONFIG_PATH = Path("configs/minimal-v1.yaml")


def test_minimal_config_is_valid_and_hash_is_stable() -> None:
    config = load_config(CONFIG_PATH)

    assert config.simulation.seed == 42
    assert config_hash(config) == config_hash(load_config(CONFIG_PATH))


def test_labels_and_quality_sections_default_when_omitted() -> None:
    config = load_config(CONFIG_PATH).model_dump(mode="python")
    config.pop("labels")
    config.pop("quality")

    validated = SimulationRunConfig.model_validate(config)

    assert validated.labels.enabled is False
    assert validated.quality.profile == "clean"


def test_public_default_config_matches_packaged_minimal_fixture() -> None:
    packaged = load_default_config()
    fixture = load_config(Path("src/fraudtwin/defaults/minimal-v1.yaml"))

    assert config_hash(packaged) == config_hash(fixture)


def test_invalid_rail_distribution_is_rejected() -> None:
    config = load_config(CONFIG_PATH).model_dump()
    config["payments"]["rails"]["CARD"] = 0.9

    with pytest.raises(ValidationError):
        SimulationRunConfig.model_validate(config)


def test_negative_entity_count_is_rejected() -> None:
    config = load_config(CONFIG_PATH).model_dump()
    config["population"]["cards"] = -1

    with pytest.raises(ValidationError):
        SimulationRunConfig.model_validate(config)


def test_pix_keys_default_to_zero_when_omitted() -> None:
    config = load_config(CONFIG_PATH).model_dump()
    config["population"].pop("pix_keys")

    validated = SimulationRunConfig.model_validate(config)

    assert validated.population.pix_keys == 0


def test_unknown_population_field_is_rejected() -> None:
    config = load_config(CONFIG_PATH).model_dump()
    config["population"]["unknown"] = 1

    with pytest.raises(ValidationError):
        SimulationRunConfig.model_validate(config)


def test_naive_simulation_start_is_rejected() -> None:
    config = load_config(CONFIG_PATH).model_dump()
    config["simulation"]["start"] = "2026-01-01T00:00:00"

    with pytest.raises(ValidationError):
        SimulationRunConfig.model_validate(config)


def test_invalid_behavior_amount_bounds_are_rejected() -> None:
    config = load_config(CONFIG_PATH).model_dump()
    config["behavior"]["amount_min"] = 0

    with pytest.raises(ValidationError):
        SimulationRunConfig.model_validate(config)


def test_invalid_behavior_time_settings_are_rejected() -> None:
    config = load_config(CONFIG_PATH).model_dump()
    config["behavior"]["active_hours"] = [24]

    with pytest.raises(ValidationError):
        SimulationRunConfig.model_validate(config)

    config["behavior"]["active_hours"] = [8, 8]
    with pytest.raises(ValidationError):
        SimulationRunConfig.model_validate(config)


def test_partial_fraud_scenario_map_disables_omitted_scenarios() -> None:
    config = load_config(CONFIG_PATH).model_dump()
    config["fraud"] = {
        "enabled": True,
        "target_rate": 0.1,
        "scenario_count": 10,
        "scenario_selection": "explicit",
        "scenarios": {
            "F04": {
                "enabled": True,
                "count": 1,
            }
        },
    }

    validated = SimulationRunConfig.model_validate(config)

    assert validated.fraud.scenarios["F01"].enabled is False
    assert validated.fraud.scenarios["F02"].enabled is False
    assert validated.fraud.scenarios["F03"].enabled is False
    assert validated.fraud.scenarios["F05"].enabled is False
    assert validated.fraud.scenarios["F04"].enabled is True

    for unsupported_name in ("scenario_mode", "scenario_strategy"):
        invalid_config = load_config(CONFIG_PATH).model_dump()
        invalid_config["fraud"] = {
            "enabled": True,
            "target_rate": 0.1,
            "scenario_count": 10,
            unsupported_name: "explicit",
            "scenarios": {"F04": {"enabled": True, "count": 1}},
        }
        with pytest.raises(ValidationError):
            SimulationRunConfig.model_validate(invalid_config)


def test_invalid_weekday_distribution_is_rejected() -> None:
    config = load_config(CONFIG_PATH).model_dump()
    config["behavior"]["weekday_weights"] = [1.0, 1.0]

    with pytest.raises(ValidationError):
        SimulationRunConfig.model_validate(config)


def test_seeded_generators_start_with_the_same_sequence() -> None:
    first = create_rng(42)
    second = create_rng(42)

    assert [first.random() for _ in range(3)] == [second.random() for _ in range(3)]


def test_extensions_are_opt_in_and_must_be_unique() -> None:
    config = load_config(CONFIG_PATH).model_dump(mode="python")
    config["extensions"] = {"enabled": True, "selected": ["example.extension"]}
    enabled = SimulationRunConfig.model_validate(config)
    assert enabled.extensions.selected == ("example.extension",)
    config["extensions"]["selected"] = ["duplicate", "duplicate"]
    with pytest.raises(ValidationError, match="unique"):
        SimulationRunConfig.model_validate(config)
