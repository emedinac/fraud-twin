"""Focused Milestone 15 campaign-dynamics contracts."""

from pathlib import Path

import pytest
from pydantic import ValidationError

from fraudtwin import (
    __version__,
    register_transition_model,
    resolve_campaign_dynamics,
)
from fraudtwin.config import CampaignDynamicsConfig, SimulationRunConfig, config_hash, load_config
from fraudtwin.domain import validate_payment_lifecycle
from fraudtwin.simulation import BehaviorGenerator, EntityGenerator

FIXTURE = Path("configs/benchmarks/m15-campaign-dynamics-v1.yaml")


def test_m15_is_strict_and_neutral_identity_is_unchanged() -> None:
    assert __version__ == "0.18.0"
    base = load_config(Path("configs/minimal.yaml"))
    neutral = base.model_copy(update={"campaign_dynamics": CampaignDynamicsConfig()})
    assert config_hash(base) == config_hash(neutral)
    raw = base.model_dump(mode="python")
    raw["campaign_dynamics"] = {"enabled": True, "unknown": 1}
    with pytest.raises(ValidationError):
        SimulationRunConfig.model_validate(raw)


def test_m15_rejects_invalid_transition_graph_and_template() -> None:
    base = load_config(FIXTURE).model_dump(mode="python")
    base["campaign_dynamics"]["bindings"][0]["template"] = "CYCLIC_RING"
    with pytest.raises(ValidationError, match="requires template"):
        SimulationRunConfig.model_validate(base)
    base = load_config(FIXTURE).model_dump(mode="python")
    base["campaign_dynamics"]["bindings"][0]["transition_probabilities"]["closed"] = {"setup": 1.0}
    with pytest.raises(ValidationError, match="closed phase"):
        SimulationRunConfig.model_validate(base)


def _dynamic_run() -> tuple[SimulationRunConfig, object, object]:
    config = load_config(FIXTURE)
    entities = EntityGenerator(config).generate()
    behavior = BehaviorGenerator(config, entities, simulation_run_id="RUN-M15").generate()
    return config, entities, behavior


def test_m15_is_deterministic_and_validates_all_dynamic_lifecycles() -> None:
    config, entities, first = _dynamic_run()
    second = BehaviorGenerator(config, entities, simulation_run_id="RUN-M15").generate()
    assert first.campaign_dynamics == second.campaign_dynamics
    assert first.payments == second.payments
    assert first.campaign_dynamics is not None
    dynamic_ids = {item.payment_id for item in first.payments if item.payment_id.startswith("M15-")}
    assert dynamic_ids
    events = {}
    for event in first.payment_events:
        events.setdefault(event.payment_id, []).append(event)
    for payment_id in dynamic_ids:
        payment = next(item for item in first.payments if item.payment_id == payment_id)
        validate_payment_lifecycle(
            payment, tuple(sorted(events[payment_id], key=lambda item: item.event_time))
        )
    assert first.campaign_dynamics.stream_ids
    assert any(
        item.mutation_type == "CROSS_RAIL" for item in first.campaign_dynamics.topology_mutations
    )


def test_m15_dynamic_graph_truth_has_membership_and_hyperedge_closure() -> None:
    config, entities, behavior = _dynamic_run()
    assert behavior.campaign_dynamics is not None
    assert any(item.campaign_id for item in behavior.graph_memberships)
    assert any(item.hyperedge_id for item in behavior.graph_hyperedge_memberships)
    assert {item.hyperedge_id for item in behavior.graph_hyperedge_memberships}.issubset(
        {item.hyperedge_id for item in behavior.graph_hyperedges}
    )


def test_m15_custom_transition_registration_is_public() -> None:
    class CloseImmediately:
        name = "test-close-immediately"

        def choose(self, binding, phase, decision_at, rng):  # type: ignore[no-untyped-def]
            return "closed", "test model"

    register_transition_model("test-close-immediately", CloseImmediately())
    config = load_config(FIXTURE).model_copy(
        update={
            "campaign_dynamics": load_config(FIXTURE).campaign_dynamics.model_copy(
                update={
                    "bindings": tuple(
                        item.model_copy(update={"transition_model": "test-close-immediately"})
                        for item in load_config(FIXTURE).campaign_dynamics.bindings
                    )
                }
            )
        }
    )
    assert resolve_campaign_dynamics(config)
