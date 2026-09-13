from pathlib import Path

import pytest
from pydantic import ValidationError

from fraudtwin.config import SimulationRunConfig, config_hash, load_config
from fraudtwin.seed import create_rng

CONFIG_PATH = Path("configs/minimal.yaml")


def test_minimal_config_is_valid_and_hash_is_stable() -> None:
    config = load_config(CONFIG_PATH)

    assert config.simulation.seed == 42
    assert config_hash(config) == config_hash(load_config(CONFIG_PATH))


def test_invalid_rail_distribution_is_rejected() -> None:
    config = load_config(CONFIG_PATH).model_dump()
    config["payments"]["rails"]["CARD"] = 0.9

    with pytest.raises(ValidationError):
        SimulationRunConfig.model_validate(config)


def test_seeded_generators_start_with_the_same_sequence() -> None:
    first = create_rng(42)
    second = create_rng(42)

    assert [first.random() for _ in range(3)] == [second.random() for _ in range(3)]
