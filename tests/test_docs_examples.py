"""Small executable checks for the examples shown in the documentation."""

from pathlib import Path

from fraudtwin.config import load_config
from fraudtwin.generation import generate


def test_quickstart_configuration_generates_a_reproducible_run() -> None:
    config = load_config(Path("configs/minimal.yaml"))

    first = generate(config, write=False)
    second = generate(config, write=False)

    assert first.run_id == second.run_id
    assert first.behavior.payments == second.behavior.payments
    assert first.manifest.event_counts["payments"] > 0
