from pathlib import Path

import pytest

from fraudtwin.config import load_config
from fraudtwin.simulation import EntityGenerator


@pytest.mark.large
def test_100k_customer_generation_smoke() -> None:
    config = load_config(Path("configs/minimal.yaml"))
    config = config.model_copy(
        update={
            "population": config.population.model_copy(update={"customers": 100_000}),
        }
    )

    dataset = EntityGenerator(config).generate()

    assert len(dataset.customers) == 100_000
