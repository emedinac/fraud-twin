"""Deterministic M12 fraud-difficulty resolution and scenario plans.

The difficulty engine intentionally contains no business or ledger logic.  It
turns a requested level and optional normalized overrides into explicit,
auditable parameters consumed by the existing M6 and M11 generators.
"""

from collections.abc import Mapping
from datetime import timedelta
from typing import TYPE_CHECKING, Any, cast

from pydantic import BaseModel, ConfigDict

from fraudtwin.config import (
    DifficultyControlName,
    SimulationRunConfig,
)
from fraudtwin.reproducibility import sha256_json

if TYPE_CHECKING:
    from fraudtwin.simulation.behavior import BehaviorDataset
    from fraudtwin.simulation.generator import EntityDataset

DIFFICULTY_RESOLVER_VERSION = "1"
DIFFICULTY_DIMENSIONS: tuple[DifficultyControlName, ...] = (
    "fraud_legitimate_overlap",
    "behavioral_deviation",
    "scenario_subtlety",
    "noise_hard_negatives",
    "prevalence",
    "temporal_irregularity",
    "graph_structural_subtlety",
)
FRAUD_SCENARIOS = ("F01", "F02", "F03", "F04", "F05")
GRAPH_SCENARIOS = (
    "MULE_NETWORK",
    "CYCLIC_RING",
    "BENEFICIARY_NETWORK",
    "FAN_OUT",
    "BIPARTITE_NETWORK",
    "STACKED_NETWORK",
    "SCATTER_GATHER",
    "GATHER_SCATTER",
    "SHARED_DEVICE_INFRASTRUCTURE",
    "SHARED_IP_INFRASTRUCTURE",
    "DENSE_CAMPAIGN",
    "MERCHANT_CUSTOMER_COMMUNITY",
    "RANDOM_ALERT_CONTROL",
)
GRAPH_SCENARIO_IDS = {
    f"G{index:02d}": scenario for index, scenario in enumerate(GRAPH_SCENARIOS, 1)
}
GRAPH_SCENARIO_CODES = {scenario: code for code, scenario in GRAPH_SCENARIO_IDS.items()}


class ScenarioDifficultyPlan(BaseModel):
    """Resolved transformations for one fraud or graph scenario."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    scenario: str
    amount_similarity: float
    behavior_similarity: float
    merchant_similarity: float
    device_similarity: float
    channel_similarity: float
    geography_similarity: float
    scenario_subtlety: float
    noise_hard_negatives: float
    prevalence: float
    temporal_irregularity: float
    graph_structural_subtlety: float
    amount_multiplier: float
    timing_multiplier: float
    hard_negative_multiplier: float
    parameter_transformations: dict[str, object]


class ResolvedDifficulty(BaseModel):
    """Complete, hashable M12 difficulty configuration."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    enabled: bool
    resolver_version: str
    requested_difficulty: int | None
    requested_controls: dict[str, float | None]
    resolved_controls: dict[str, float]
    scenario_transformations: dict[str, dict[str, object]]
    effective_configuration_hash: str

    @property
    def level(self) -> int | None:
        """Return the requested level for convenient API consumers."""

        return self.requested_difficulty

    def plan(self, scenario: str) -> ScenarioDifficultyPlan:
        """Return the immutable plan for one scenario."""

        return apply_difficulty(self, scenario)


def _scenario_values(resolved: dict[str, float], scenario: str) -> dict[str, object]:
    scenario = GRAPH_SCENARIO_IDS.get(scenario, scenario)
    overlap = resolved["fraud_legitimate_overlap"]
    behavior = resolved["behavioral_deviation"]
    subtlety = resolved["scenario_subtlety"]
    noise = resolved["noise_hard_negatives"]
    prevalence = resolved["prevalence"]
    temporal = resolved["temporal_irregularity"]
    structural = resolved["graph_structural_subtlety"]
    # These are explicit measurable transformations, not arbitrary labels.
    values: dict[str, object] = {
        "amount_similarity": round(overlap, 6),
        "behavior_similarity": round(behavior, 6),
        "merchant_similarity": round(behavior, 6),
        "device_similarity": round(behavior, 6),
        "channel_similarity": round(behavior, 6),
        "geography_similarity": round(behavior, 6),
        "scenario_subtlety": round(subtlety, 6),
        "noise_hard_negatives": round(noise, 6),
        "prevalence": round(prevalence, 6),
        "temporal_irregularity": round(temporal, 6),
        "graph_structural_subtlety": round(structural, 6),
        "amount_multiplier": round(1.0 - 0.35 * overlap, 6),
        "timing_multiplier": round(1.0 + 2.0 * temporal, 6),
        "hard_negative_multiplier": round(1.0 + 3.0 * noise, 6),
    }
    if scenario in FRAUD_SCENARIOS:
        objective = {
            "F01": "card-not-present activity remains card-not-present",
            "F02": "low-value authorization burst remains a burst",
            "F03": "account takeover retains authentication, profile, and beneficiary steps",
            "F04": "instant-payment scam retains a new-beneficiary transfer",
            "F05": "velocity attack retains a bounded authorization burst",
        }[scenario]
    else:
        objective = {
            "MULE_NETWORK": "fan-in and fan-out through the mule remain",
            "CYCLIC_RING": "the directed cycle remains closed",
            "BENEFICIARY_NETWORK": "the shared beneficiary remains",
            "FAN_OUT": "the source-to-destination fan remains",
            "BIPARTITE_NETWORK": "the originator-beneficiary mesh remains",
            "STACKED_NETWORK": "the layered path remains",
            "SCATTER_GATHER": "scatter and gather paths remain",
            "GATHER_SCATTER": "gather and scatter paths remain",
            "SHARED_DEVICE_INFRASTRUCTURE": "shared-device incidence remains",
            "SHARED_IP_INFRASTRUCTURE": "shared-IP incidence remains",
            "DENSE_CAMPAIGN": "the configured dense edge set remains",
            "MERCHANT_CUSTOMER_COMMUNITY": "the merchant-customer community remains",
            "RANDOM_ALERT_CONTROL": "the control campaign remains a control",
        }[scenario]
    values["objective_invariant"] = objective
    values["scenario_family"] = "fraud" if scenario in FRAUD_SCENARIOS else "graph"
    return values


def resolve_difficulty(config: SimulationRunConfig) -> ResolvedDifficulty:
    """Resolve a level and its optional per-dimension replacements."""

    benchmark = config.benchmark
    if not benchmark.enabled:
        return ResolvedDifficulty(
            enabled=False,
            resolver_version=DIFFICULTY_RESOLVER_VERSION,
            requested_difficulty=None,
            requested_controls={name: None for name in DIFFICULTY_DIMENSIONS},
            resolved_controls={},
            scenario_transformations={},
            effective_configuration_hash="",
        )
    assert benchmark.difficulty is not None
    base = (benchmark.difficulty - 1) / 9
    requested = benchmark.controls.values()
    resolved: dict[str, float] = {}
    for name in DIFFICULTY_DIMENSIONS:
        override = requested[name]
        resolved[name] = float(override) if override is not None else base
    # Publish the stable F01–F05/G01–G13 benchmark identifiers. M11 callers
    # continue to use native scenario type names, mapped in apply_difficulty.
    scenarios = FRAUD_SCENARIOS + tuple(GRAPH_SCENARIO_IDS)
    transformations = {scenario: _scenario_values(resolved, scenario) for scenario in scenarios}
    hash_payload = {
        "resolver_version": DIFFICULTY_RESOLVER_VERSION,
        "requested_difficulty": benchmark.difficulty,
        "requested_controls": requested,
        "resolved_controls": resolved,
        "scenario_transformations": transformations,
    }
    return ResolvedDifficulty(
        enabled=True,
        resolver_version=DIFFICULTY_RESOLVER_VERSION,
        requested_difficulty=benchmark.difficulty,
        requested_controls=requested,
        resolved_controls=resolved,
        scenario_transformations=transformations,
        effective_configuration_hash=sha256_json(hash_payload),
    )


def apply_difficulty(
    resolved: ResolvedDifficulty,
    scenario: str,
    context: dict[str, Any] | None = None,
) -> ScenarioDifficultyPlan:
    """Apply resolved controls to one scenario without changing its objective.

    ``context`` is accepted for extension compatibility and is intentionally
    excluded from the plan hash; all stochastic choices use generator-owned
    M12 stream names.
    """

    del context
    if not resolved.enabled:
        values = {name: 0.0 for name in DIFFICULTY_DIMENSIONS} | {
            "amount_multiplier": 1.0,
            "timing_multiplier": 1.0,
            "hard_negative_multiplier": 1.0,
            "objective_invariant": "legacy behavior",
            "scenario_family": "legacy",
        }
        return ScenarioDifficultyPlan(
            scenario=scenario,
            amount_similarity=0.0,
            behavior_similarity=0.0,
            merchant_similarity=0.0,
            device_similarity=0.0,
            channel_similarity=0.0,
            geography_similarity=0.0,
            scenario_subtlety=0.0,
            noise_hard_negatives=0.0,
            prevalence=0.0,
            temporal_irregularity=0.0,
            graph_structural_subtlety=0.0,
            amount_multiplier=1.0,
            timing_multiplier=1.0,
            hard_negative_multiplier=1.0,
            parameter_transformations=values,
        )
    canonical_scenario = GRAPH_SCENARIO_CODES.get(scenario, scenario)
    if canonical_scenario not in resolved.scenario_transformations:
        raise ValueError(f"unsupported difficulty scenario: {scenario}")
    values = resolved.scenario_transformations[canonical_scenario]

    def number(name: str) -> float:
        return float(cast(float, values[name]))

    return ScenarioDifficultyPlan(
        scenario=scenario,
        amount_similarity=number("amount_similarity"),
        behavior_similarity=number("behavior_similarity"),
        merchant_similarity=number("merchant_similarity"),
        device_similarity=number("device_similarity"),
        channel_similarity=number("channel_similarity"),
        geography_similarity=number("geography_similarity"),
        scenario_subtlety=number("scenario_subtlety"),
        noise_hard_negatives=number("noise_hard_negatives"),
        prevalence=number("prevalence"),
        temporal_irregularity=number("temporal_irregularity"),
        graph_structural_subtlety=number("graph_structural_subtlety"),
        amount_multiplier=number("amount_multiplier"),
        timing_multiplier=number("timing_multiplier"),
        hard_negative_multiplier=number("hard_negative_multiplier"),
        parameter_transformations=dict(values),
    )


def _json_tables(tables: Mapping[str, tuple[BaseModel, ...]]) -> dict[str, list[dict[str, object]]]:
    """Serialize generated tables in their existing deterministic order."""

    return {
        name: [record.model_dump(mode="json") for record in records]
        for name, records in tables.items()
    }


def _schema_tables(tables: Mapping[str, tuple[BaseModel, ...]]) -> dict[str, list[str]]:
    """Return ordered columns for each generated table."""

    return {
        name: list(records[0].model_dump()) if records else [] for name, records in tables.items()
    }


def difficulty_metadata(
    config: SimulationRunConfig,
    entities: "EntityDataset",
    behavior: "BehaviorDataset",
    run_id: str,
) -> dict[str, object]:
    """Build reproducibility and measured-output metadata for an active run."""

    resolved = resolve_difficulty(config)
    if not resolved.enabled:
        return {}
    behavior_tables = behavior.tables()
    entity_tables = entities.tables()
    tables = _json_tables(behavior_tables)
    entity_rows = _json_tables(entity_tables)
    true_records = sum(record.fraud_truth for record in behavior.fraud_records)
    hard_negative_count = len(behavior.fraud_records) - true_records
    fraud_event_count = len(behavior.fraud_events)
    total_payments = len(behavior.payments)
    true_payment_ids = {
        record.payment_id for record in behavior.fraud_records if record.fraud_truth
    }
    legitimate_amounts: dict[str, list[float]] = {}
    for payment in behavior.payments:
        if payment.payment_id not in true_payment_ids:
            legitimate_amounts.setdefault(payment.payment_rail, []).append(payment.amount)
    similarities: list[float] = []
    for payment in behavior.payments:
        if payment.payment_id not in true_payment_ids:
            continue
        cohort = legitimate_amounts.get(payment.payment_rail, [])
        if not cohort:
            continue
        reference = sorted(cohort)[len(cohort) // 2]
        similarities.append(max(0.0, 1.0 - abs(payment.amount - reference) / max(reference, 1.0)))
    observed_overlap = sum(similarities) / len(similarities) if similarities else 0.0
    summary = {
        "fraud_legitimate_overlap": resolved.resolved_controls["fraud_legitimate_overlap"],
        "fraud_legitimate_amount_overlap_observed": round(observed_overlap, 6),
        "behavioral_deviation": 1.0 - resolved.resolved_controls["behavioral_deviation"],
        "scenario_subtlety": resolved.resolved_controls["scenario_subtlety"],
        "noise_hard_negative_count": hard_negative_count,
        "noise_hard_negative_rate": hard_negative_count / max(1, len(behavior.fraud_records)),
        "prevalence": true_records / max(1, total_payments),
        "temporal_irregularity": resolved.resolved_controls["temporal_irregularity"],
        "graph_structural_subtlety": resolved.resolved_controls["graph_structural_subtlety"],
        "fraud_event_count": fraud_event_count,
        "payment_count": total_payments,
        "graph_pattern_count": len(behavior.graph_patterns),
        "graph_campaign_count": len(behavior.graph_campaigns),
        "graph_membership_count": len(behavior.graph_memberships),
        "graph_evidence_count": len(behavior.graph_evidence),
        "graph_hyperedge_count": len(behavior.graph_hyperedges),
    }
    schema_fingerprint = sha256_json(
        {
            "entities": _schema_tables(entity_tables),
            "behavior": _schema_tables(behavior_tables),
        }
    )
    return {
        "requested_difficulty": resolved.requested_difficulty,
        "requested_controls": resolved.requested_controls,
        "resolved_controls": resolved.resolved_controls,
        "parameter_transformations": resolved.scenario_transformations,
        "effective_configuration_hash": resolved.effective_configuration_hash,
        "resolver_version": resolved.resolver_version,
        "source_snapshot": {
            "run_id": run_id,
            "start": config.simulation.start.isoformat(),
            "end": (
                config.simulation.start + timedelta(days=config.simulation.duration_days)
            ).isoformat(),
        },
        "measurable_summaries": summary,
        "schema_fingerprint": schema_fingerprint,
        "output_fingerprint": sha256_json({"entities": entity_rows, "behavior": tables}),
    }


__all__ = [
    "DIFFICULTY_DIMENSIONS",
    "DIFFICULTY_RESOLVER_VERSION",
    "GRAPH_SCENARIO_CODES",
    "GRAPH_SCENARIO_IDS",
    "ResolvedDifficulty",
    "ScenarioDifficultyPlan",
    "apply_difficulty",
    "resolve_difficulty",
]
