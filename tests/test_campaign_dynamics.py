"""Focused campaign-dynamics contract tests."""

from importlib.metadata import version as installed_version
from pathlib import Path

import pytest
from pydantic import ValidationError

from fraudtwin import (
    __version__,
    register_transition_model,
    resolve_campaign_dynamics,
)
from fraudtwin.campaign_dynamics import evolve_campaigns
from fraudtwin.config import CampaignDynamicsConfig, SimulationRunConfig, config_hash, load_config
from fraudtwin.domain import validate_payment_lifecycle
from fraudtwin.generation import GeneratedData, GeneratedRun, generate
from fraudtwin.ml import load_generated_run
from fraudtwin.simulation import BehaviorGenerator, EntityGenerator
from fraudtwin.simulation.graph_fraud import GraphFraudDataset
from fraudtwin.simulation.parquet import write_campaign_dynamics_sidecar

FIXTURE = Path("configs/benchmarks/campaign-dynamics-v1.yaml")


def test_campaign_dynamics_is_strict_and_neutral_identity_is_unchanged() -> None:
    assert __version__ == installed_version("fraudtwin")
    base = load_config(Path("configs/minimal-v1.yaml"))
    neutral = base.model_copy(update={"campaign_dynamics": CampaignDynamicsConfig()})
    assert config_hash(base) == config_hash(neutral)
    raw = base.model_dump(mode="python")
    raw["campaign_dynamics"] = {"enabled": True, "unknown": 1}
    with pytest.raises(ValidationError):
        SimulationRunConfig.model_validate(raw)


def test_campaign_dynamics_rejects_invalid_transition_graph_and_template() -> None:
    base = load_config(FIXTURE).model_dump(mode="python")
    base["campaign_dynamics"]["bindings"][0]["template"] = "CYCLIC_RING"
    with pytest.raises(ValidationError, match="requires template"):
        SimulationRunConfig.model_validate(base)
    base = load_config(FIXTURE).model_dump(mode="python")
    base["campaign_dynamics"]["bindings"][0]["transition_probabilities"]["closed"] = {"setup": 1.0}
    with pytest.raises(ValidationError, match="closed phase"):
        SimulationRunConfig.model_validate(base)


def test_campaign_dynamics_rejects_zero_actions_and_caps_initial_membership() -> None:
    base = load_config(FIXTURE).model_dump(mode="python")
    base["campaign_dynamics"]["bindings"][0]["max_actions"] = 0
    with pytest.raises(ValidationError, match="greater than or equal to 1"):
        SimulationRunConfig.model_validate(base)
    base = load_config(FIXTURE).model_dump(mode="python")
    base["campaign_dynamics"]["bindings"][0]["max_active_members"] = 2
    with pytest.raises(ValidationError, match="initial membership exceeds"):
        SimulationRunConfig.model_validate(base)


def _dynamic_run() -> tuple[SimulationRunConfig, object, object]:
    config = load_config(FIXTURE)
    entities = EntityGenerator(config).generate()
    behavior = BehaviorGenerator(config, entities, simulation_run_id="RUN-M15").generate()
    return config, entities, behavior


def test_campaign_dynamics_is_deterministic_and_validates_all_dynamic_lifecycles() -> None:
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
    assert any(
        item.mutation_type == "RING_SPLIT" for item in first.campaign_dynamics.topology_mutations
    )
    assert any(
        item.mutation_type == "RING_MERGE" for item in first.campaign_dynamics.topology_mutations
    )
    snapshots = first.campaign_dynamics.snapshots
    assert snapshots and all(snapshot.active_device_ids for snapshot in snapshots)
    rotations = [
        item
        for item in first.campaign_dynamics.topology_mutations
        if item.mutation_type == "DEVICE_ROTATION"
    ]
    assert rotations
    rotated_device = rotations[0].derived_member_ids[0]
    assert any(
        rotated_device in snapshot.active_device_ids
        and snapshot.valid_from >= rotations[0].occurred_at
        for snapshot in snapshots
    )


def test_campaign_dynamics_joins_respect_max_active_members() -> None:
    config = load_config(FIXTURE)
    bindings = list(config.campaign_dynamics.bindings)
    bindings[0] = bindings[0].model_copy(update={"max_active_members": 6})
    limited = config.model_copy(
        update={
            "campaign_dynamics": config.campaign_dynamics.model_copy(
                update={"bindings": tuple(bindings)}
            )
        }
    )
    behavior = BehaviorGenerator(limited, EntityGenerator(limited).generate()).generate()
    assert behavior.campaign_dynamics is not None
    capped_campaigns = {
        campaign.campaign_id
        for campaign in behavior.graph_campaigns
        if campaign.scenario_type == "MULE_NETWORK"
    }
    capped_snapshots = [
        snapshot
        for snapshot in behavior.campaign_dynamics.snapshots
        if snapshot.campaign_id in capped_campaigns
    ]
    assert capped_snapshots
    assert all(len(snapshot.active_actor_ids) <= 6 for snapshot in capped_snapshots)


def test_campaign_dynamics_dynamic_graph_truth_has_membership_and_hyperedge_closure() -> None:
    config, entities, behavior = _dynamic_run()
    assert behavior.campaign_dynamics is not None
    assert any(item.campaign_id for item in behavior.graph_memberships)
    assert any(item.hyperedge_id for item in behavior.graph_hyperedge_memberships)
    assert {item.hyperedge_id for item in behavior.graph_hyperedge_memberships}.issubset(
        {item.hyperedge_id for item in behavior.graph_hyperedges}
    )


def test_campaign_dynamics_sidecar_records_are_loaded_and_merged(tmp_path: Path) -> None:
    config = load_config(FIXTURE)
    static_config = config.model_copy(update={"campaign_dynamics": CampaignDynamicsConfig()})
    source = generate(static_config)
    generated = generate(static_config, write=True, output_dir=tmp_path)
    assert isinstance(source, GeneratedData)
    assert isinstance(generated, GeneratedRun)
    source_graph = GraphFraudDataset(
        source.behavior.payments,
        source.behavior.payment_events,
        source.behavior.ledger_entries,
        source.behavior.fraud_records,
        source.behavior.graph_memberships,
        source.behavior.graph_patterns,
        source.behavior.graph_campaigns,
        source.behavior.graph_evidence,
        source.behavior.graph_hyperedges,
        source.behavior.graph_hyperedge_memberships,
    )
    dynamic = evolve_campaigns(config, source.entities, source_graph, generated.run_id)
    write_campaign_dynamics_sidecar(dynamic, generated.run_dir, source_run_id=generated.run_id)

    _, loaded, _ = load_generated_run(generated.run_dir)
    expected_payments = {
        item.payment_id for item in dynamic.graph.payments if item.payment_id.startswith("M15-")
    }
    assert expected_payments.issubset({item.payment_id for item in loaded.payments})
    assert any(item.payment_id.startswith("M15-") for item in loaded.payment_events)
    assert loaded.campaign_dynamics is not None
    assert {item.campaign_id for item in dynamic.graph.campaigns}.issubset(
        {item.campaign_id for item in loaded.graph_campaigns}
    )


def test_campaign_dynamics_custom_transition_registration_is_public() -> None:
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
