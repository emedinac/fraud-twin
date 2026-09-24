"""Deterministic counterfactual generation.

The engine deliberately operates on a pristine legitimate payment stream.
It produces append-only sidecar data and never mutates the source models or the
ordinary output tables.
"""

import hashlib
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Protocol, cast

from pydantic import BaseModel, ConfigDict, Field

from fraudtwin.camouflage import resolve_camouflage
from fraudtwin.config import (
    CounterfactualConfig,
    CounterfactualDimension,
    CounterfactualDimensionConfig,
    CounterfactualObjective,
    CounterfactualRequestConfig,
    SimulationRunConfig,
)
from fraudtwin.difficulty import resolve_difficulty
from fraudtwin.domain import (
    CustomerDispute,
    DelayedFraudLabel,
    FraudAlert,
    FraudCase,
    FraudCaseConfirmation,
    FraudRecord,
    GraphCampaign,
    GraphCampaignMembership,
    GraphEvidence,
    GraphHyperedge,
    GraphHyperedgeMembership,
    GraphPattern,
    LedgerEntry,
    Payment,
    PaymentEvent,
    validate_card_lifecycle,
    validate_ledger,
    validate_payment_lifecycle,
    validate_pix_lifecycle,
)
from fraudtwin.reproducibility import sha256_json
from fraudtwin.seed import create_stream_rng

if TYPE_CHECKING:
    from fraudtwin.simulation.generator import EntityDataset
    from fraudtwin.simulation.payments import PaymentDataset

FRAUD_OBJECTIVES: tuple[str, ...] = ("F01", "F02", "F03", "F04", "F05")
GRAPH_OBJECTIVES: tuple[str, ...] = (
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
ALL_OBJECTIVES = FRAUD_OBJECTIVES + GRAPH_OBJECTIVES
DIMENSIONS: tuple[CounterfactualDimension, ...] = (
    "beneficiary",
    "device",
    "timing",
    "amount",
    "merchant",
    "geography",
    "payment_rail",
    "graph_relationships",
)


class DistanceFunction(Protocol):
    """Protocol implemented by pluggable counterfactual distances."""

    def __call__(
        self, changes: Mapping[str, Mapping[str, object]], costs: Mapping[str, float]
    ) -> float: ...


DistanceCallable = Callable[[Mapping[str, Mapping[str, object]], Mapping[str, float]], float]
_DISTANCES: dict[str, DistanceCallable] = {}


def register_distance_function(name: str, function: DistanceCallable) -> None:
    """Register a deterministic named distance function."""

    if not name.strip():
        raise ValueError("distance function name must not be empty")
    _DISTANCES[name] = function


def _weighted_changed_dimensions(
    changes: Mapping[str, Mapping[str, object]], costs: Mapping[str, float]
) -> float:
    return round(sum(costs.get(dimension, 1.0) for dimension in changes), 9)


register_distance_function("weighted_changed_dimensions_v1", _weighted_changed_dimensions)


class CounterfactualScope(BaseModel):
    """Resolved controls for one objective request."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    objective: CounterfactualObjective
    family: str
    enabled: bool
    budget: float
    costs: dict[str, float]
    enabled_dimensions: tuple[str, ...]
    distance_function: str


class ResolvedCounterfactual(BaseModel):
    """Complete immutable M14 configuration resolution."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    enabled: bool
    resolver_version: str
    requested: dict[str, object]
    scopes: tuple[CounterfactualScope, ...]
    effective_configuration_hash: str

    def scope(self, objective: str) -> CounterfactualScope:
        for scope in self.scopes:
            if scope.objective == objective:
                return scope
        raise ValueError(f"counterfactual objective is not configured: {objective}")


class SourceTrajectory(BaseModel):
    """One legitimate source payment and its point-in-time snapshot."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    source_payment: Payment
    source_events: tuple[PaymentEvent, ...]
    source_ledger_entries: tuple[LedgerEntry, ...]
    decision_at: datetime
    available_event_ids: tuple[str, ...]


class CounterfactualChangeSet(BaseModel):
    """Machine-readable explanation of one accepted or rejected request."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    change_set_id: str
    request_index: int
    objective: str
    status: str
    source_payment_id: str | None
    derived_payment_id: str | None
    requested_budget: float
    resolved_budget: float
    effective_distance: float | None
    per_dimension_costs: dict[str, float]
    changed_fields: dict[str, dict[str, object]]
    inapplicable_dimensions: tuple[str, ...]
    feasibility_constraints: tuple[str, ...]
    objective_satisfaction: dict[str, object]
    rejection_reason: str | None = None
    source_to_counterfactual: dict[str, str] = Field(default_factory=dict)


class CounterfactualDataset(BaseModel):
    """Original/modified sidecar records and oracle metadata."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    counterfactual_id: str
    original_payments: tuple[Payment, ...] = ()
    original_events: tuple[PaymentEvent, ...] = ()
    original_ledger_entries: tuple[LedgerEntry, ...] = ()
    modified_payments: tuple[Payment, ...] = ()
    modified_events: tuple[PaymentEvent, ...] = ()
    modified_ledger_entries: tuple[LedgerEntry, ...] = ()
    fraud_records: tuple[FraudRecord, ...] = ()
    alerts: tuple[FraudAlert, ...] = ()
    fraud_cases: tuple[FraudCase, ...] = ()
    case_confirmations: tuple[FraudCaseConfirmation, ...] = ()
    customer_disputes: tuple[CustomerDispute, ...] = ()
    fraud_labels: tuple[DelayedFraudLabel, ...] = ()
    change_sets: tuple[CounterfactualChangeSet, ...] = ()
    graph_campaigns: tuple[GraphCampaign, ...] = ()
    graph_memberships: tuple[GraphCampaignMembership, ...] = ()
    graph_patterns: tuple[GraphPattern, ...] = ()
    graph_evidence: tuple[GraphEvidence, ...] = ()
    graph_hyperedges: tuple[GraphHyperedge, ...] = ()
    graph_hyperedge_memberships: tuple[GraphHyperedgeMembership, ...] = ()
    metadata: dict[str, object] = Field(default_factory=dict)

    @property
    def rejected(self) -> tuple[CounterfactualChangeSet, ...]:
        return tuple(item for item in self.change_sets if item.status == "REJECTED")


@dataclass(frozen=True)
class _Candidate:
    trajectory: SourceTrajectory
    request_index: int


def _family(objective: str) -> str:
    return "fraud" if objective in FRAUD_OBJECTIVES else "graph"


def _scope_values(
    global_config: CounterfactualConfig,
    objective: str,
    request: CounterfactualRequestConfig,
) -> CounterfactualScope:
    family = _family(objective)
    family_scope = global_config.families.get(cast(Any, family))
    objective_scope = global_config.scenarios.get(cast(Any, objective))
    costs: dict[str, float] = {}
    enabled: dict[str, bool] = {}
    for dimension in DIMENSIONS:
        setting = global_config.dimensions.get(dimension, CounterfactualDimensionConfig())
        costs[dimension] = setting.cost
        enabled[dimension] = setting.enabled
        for override in (
            family_scope.dimensions.get(dimension) if family_scope else None,
            objective_scope.dimensions.get(dimension) if objective_scope else None,
            request.dimensions.get(dimension),
        ):
            if override is not None:
                costs[dimension] = override.cost
                enabled[dimension] = override.enabled
    budget = global_config.budget
    for budget_override in (
        family_scope.budget if family_scope else None,
        objective_scope.budget if objective_scope else None,
        request.budget,
    ):
        if budget_override is not None:
            budget = budget_override
    active = True
    for enabled_override in (
        family_scope.enabled if family_scope else None,
        objective_scope.enabled if objective_scope else None,
        request.enabled,
    ):
        if enabled_override is not None:
            active = enabled_override
    return CounterfactualScope(
        objective=cast(Any, objective),
        family=family,
        enabled=active,
        budget=budget,
        costs=costs,
        enabled_dimensions=tuple(dimension for dimension in DIMENSIONS if enabled[dimension]),
        distance_function=global_config.distance_function,
    )


def resolve_counterfactual(config: SimulationRunConfig) -> ResolvedCounterfactual:
    """Resolve M14 precedence without consuming a random stream."""

    settings = config.counterfactual
    if not settings.active:
        return ResolvedCounterfactual(
            enabled=False,
            resolver_version="1",
            requested={},
            scopes=(),
            effective_configuration_hash="",
        )
    requests = list(settings.requests)
    if settings.include_enabled_objectives:
        requests.extend(
            CounterfactualRequestConfig(objective=objective)
            for objective in (
                tuple(
                    scenario
                    for scenario, item in config.fraud.scenarios.items()
                    if item.enabled and item.count > 0
                )
                if config.fraud.enabled
                else ()
            )
        )
        requests.extend(
            CounterfactualRequestConfig(
                objective=cast(Any, scenario.type), graph_template=scenario.type
            )
            for scenario in config.graph.scenarios
            if scenario.count > 0
        )
    if not requests:
        raise ValueError("counterfactual configuration expands to no objectives")
    objectives = [request.objective for request in requests]
    if any(objective is None for objective in objectives):
        raise ValueError("counterfactual requests must have objectives")
    scopes = tuple(
        _scope_values(settings, cast(str, request.objective), request) for request in requests
    )
    if len(set(objectives)) != len(objectives):
        raise ValueError("counterfactual request expansion produced duplicate objectives")
    payload = {
        "resolver_version": "1",
        "requested": settings.model_dump(mode="json"),
        "scopes": [scope.model_dump(mode="json") for scope in scopes],
    }
    return ResolvedCounterfactual(
        enabled=True,
        resolver_version="1",
        requested=cast(dict[str, object], payload["requested"]),
        scopes=scopes,
        effective_configuration_hash=sha256_json(payload),
    )


def _initial_event(events: Iterable[PaymentEvent]) -> PaymentEvent:
    ordered = tuple(sorted(events, key=lambda event: (event.event_time, event.event_id)))
    if not ordered:
        raise ValueError("source payment has no events")
    return ordered[0]


def select_source_trajectories(
    config: SimulationRunConfig,
    payments: "tuple[Payment, ...] | PaymentDataset",
    *,
    events: tuple[PaymentEvent, ...] | None = None,
    ledger_entries: tuple[LedgerEntry, ...] = (),
    request_index: int = 0,
) -> tuple[SourceTrajectory, ...]:
    """Select deterministic legitimate-only source trajectories."""

    if not isinstance(payments, tuple):
        payment_records = payments.payments
        event_records = payments.payment_events
        ledger_records = payments.ledger_entries
    else:
        payment_records = payments
        event_records = events or ()
        ledger_records = ledger_entries
    del request_index
    events_by_payment: dict[str, list[PaymentEvent]] = {}
    for event in event_records:
        events_by_payment.setdefault(event.payment_id, []).append(event)
    ledger_by_payment: dict[str, list[LedgerEntry]] = {}
    for entry in ledger_records:
        ledger_by_payment.setdefault(entry.payment_id, []).append(entry)
    candidates: list[SourceTrajectory] = []
    for payment in payment_records:
        payment_events = tuple(events_by_payment.get(payment.payment_id, ()))
        if not payment_events:
            continue
        initial = _initial_event(payment_events)
        available = tuple(
            event.event_id
            for event in payment_events
            if event.source_available_at <= initial.source_available_at
        )
        candidates.append(
            SourceTrajectory(
                source_payment=payment,
                source_events=payment_events,
                source_ledger_entries=tuple(ledger_by_payment.get(payment.payment_id, ())),
                decision_at=initial.source_available_at,
                available_event_ids=available,
            )
        )
    if config.counterfactual.source_strategy == "stable_hash":
        candidates.sort(
            key=lambda item: hashlib.sha256(
                f"{config.simulation.seed}:{item.source_payment.payment_id}".encode()
            ).hexdigest()
        )
    else:
        candidates.sort(
            key=lambda item: (
                item.decision_at,
                item.source_payment.payment_id,
                _initial_event(item.source_events).event_id,
            )
        )
    return tuple(candidates)


def _derived_id(prefix: str, material: str) -> str:
    return f"CF-{prefix}-{hashlib.sha256(material.encode()).hexdigest()[:20]}"


def _candidate_values(
    dimension: str,
    source: Payment,
    entities: "EntityDataset",
) -> tuple[object, ...]:
    if dimension == "amount":
        return tuple(
            value
            for value in (round(source.amount * 0.9, 2), round(source.amount * 0.75, 2))
            if value > 0
        )
    if dimension == "beneficiary":
        return tuple(
            account.account_id
            for account in entities.accounts
            if account.account_id != source.payee_account_id and account.status == "ACTIVE"
        )
    if dimension == "merchant":
        if source.merchant_id is None:
            return ()
        return tuple(
            merchant.merchant_id
            for merchant in entities.merchants
            if merchant.merchant_id != source.merchant_id
        )
    if dimension == "geography":
        if source.merchant_id is None:
            return ()
        source_country = next(
            (
                merchant.country
                for merchant in entities.merchants
                if merchant.merchant_id == source.merchant_id
            ),
            None,
        )
        return tuple(
            merchant.merchant_id
            for merchant in entities.merchants
            if merchant.country != source_country
        )
    if dimension == "device":
        return tuple(device.device_id for device in entities.devices)
    if dimension == "timing":
        return (30, 300, 3600)
    if dimension == "payment_rail":
        # Rail conversion requires constructing a different lifecycle
        # vocabulary; report it as inapplicable until a complete conversion
        # plan is available rather than emitting a mislabeled trajectory.
        return ()
    if dimension == "graph_relationships":
        return ("add_campaign_edge",)
    return ()


def _objective_satisfied(
    objective: str, payment: Payment, changes: Mapping[str, Mapping[str, object]]
) -> tuple[bool, str]:
    if objective == "F01":
        return payment.payment_rail == "CARD", "card-not-present activity remains card based"
    if objective in {"F02", "F05"}:
        return payment.payment_rail == "CARD", "card authorization activity remains bounded"
    if objective == "F03":
        return bool(
            changes.get("device") or changes.get("beneficiary")
        ), "account takeover retains identity or beneficiary change"
    if objective == "F04":
        return payment.payment_rail in {"PIX", "ACCOUNT_TRANSFER"} and bool(
            changes.get("beneficiary")
        ), "instant-payment scam retains a new beneficiary"
    return bool(
        changes.get("graph_relationships")
    ), "graph relationship objective remains represented"


def _objective_rails(objective: str) -> set[str]:
    if objective in {"F01", "F02", "F05"}:
        return {"CARD"}
    if objective == "F04":
        return {"PIX", "ACCOUNT_TRANSFER"}
    return {"CARD", "PIX", "ACCOUNT_TRANSFER"}


def _find_feasible_mutation(
    scope: CounterfactualScope,
    source: SourceTrajectory,
    entities: "EntityDataset",
    *,
    amount_multiplier: float,
    timing_multiplier: float,
    timing_strength: float,
) -> tuple[dict[str, dict[str, object]], float, bool, str, tuple[str, ...]]:
    """Return the stable minimum-cost one-dimension mutation for a source."""

    source_payment = source.source_payment
    values_by_dimension: dict[str, tuple[object, ...]] = {
        dimension: _candidate_values(dimension, source_payment, entities)
        for dimension in DIMENSIONS
    }
    available_dimensions = [
        dimension for dimension in scope.enabled_dimensions if values_by_dimension[dimension]
    ]
    inapplicable = tuple(
        dimension for dimension in DIMENSIONS if dimension not in available_dimensions
    )
    distance = _DISTANCES.get(scope.distance_function)
    if distance is None:
        raise ValueError(
            f"counterfactual distance function is not registered: {scope.distance_function}"
        )

    options: list[tuple[float, str, dict[str, dict[str, object]], str]] = []
    for dimension in available_dimensions:
        if scope.costs[dimension] > scope.budget:
            continue
        values = values_by_dimension[dimension]
        if dimension == "amount":
            values = tuple(
                round(float(cast(float, value)) * amount_multiplier, 2)
                for value in values
                if float(cast(float, value)) * amount_multiplier > 0
            )
        elif dimension == "timing":
            values = tuple(
                round(
                    float(cast(int, value)) * timing_multiplier * (1.0 - timing_strength),
                    0,
                )
                for value in values
            )
        if dimension == "device":
            source_devices = {event.device_id for event in source.source_events}
            values = tuple(value for value in values if value not in source_devices)
        for value in sorted(values, key=str):
            candidate_changes = {dimension: {"value": value, "cost": scope.costs[dimension]}}
            satisfied, reason = _objective_satisfied(
                scope.objective, source_payment, candidate_changes
            )
            candidate_distance = distance(candidate_changes, scope.costs)
            if satisfied and candidate_distance <= scope.budget:
                options.append((candidate_distance, dimension, candidate_changes, reason))

    options.sort(key=lambda item: (item[0], len(item[2]), item[1], repr(item[2])))
    if options:
        total, _, changes, reason = options[0]
        return changes, total, True, reason, inapplicable
    changes = {}
    satisfied, reason = _objective_satisfied(scope.objective, source_payment, changes)
    return changes, 0.0, satisfied, reason, inapplicable


def _clone_trajectory(
    trajectory: SourceTrajectory,
    changes: Mapping[str, Mapping[str, object]],
    *,
    objective: str,
    config: SimulationRunConfig,
    entities: "EntityDataset",
    request_index: int,
) -> tuple[Payment, tuple[PaymentEvent, ...], tuple[LedgerEntry, ...], dict[str, str]]:
    source = trajectory.source_payment
    material = f"{config.simulation.seed}:{request_index}:{source.payment_id}:{objective}"
    payment_id = _derived_id("PAY", material)
    update: dict[str, object] = {"payment_id": payment_id}
    if "amount" in changes:
        update["amount"] = float(cast(float, changes["amount"]["value"]))
    if "beneficiary" in changes:
        target = str(changes["beneficiary"]["value"])
        account = next(item for item in entities.accounts if item.account_id == target)
        update.update(
            {
                "payee_account_id": account.account_id,
                "payee_institution_id": account.institution_id,
            }
        )
        if source.payer_account_id in {item.account_id for item in entities.accounts}:
            pix = next(
                (
                    item
                    for item in entities.pix_keys
                    if item.account_id == account.account_id and item.status == "ACTIVE"
                ),
                None,
            )
            update["payee_pix_key_id"] = pix.pix_key_id if pix else source.payee_pix_key_id
    if "merchant" in changes or "geography" in changes:
        merchant_change = changes.get("merchant") or changes.get("geography")
        if merchant_change is not None:
            update["merchant_id"] = str(merchant_change["value"])
    payment = source.model_copy(update=update)
    event_map: dict[str, str] = {}
    event_updates: list[PaymentEvent] = []
    timing_shift = int(cast(int, changes.get("timing", {}).get("value", 0)))
    device_value = changes.get("device", {}).get("value")
    for ordinal, event in enumerate(trajectory.source_events, 1):
        event_id = _derived_id("EVT", f"{material}:{event.event_id}")
        event_map[event.event_id] = event_id
        event_update: dict[str, object] = {
            "event_id": event_id,
            "payment_id": payment_id,
            "correlation_id": payment_id,
            "causation_id": None
            if ordinal == 1
            else event_map[trajectory.source_events[ordinal - 2].event_id],
            "source_created_at": event.source_created_at + timedelta(seconds=timing_shift),
            "source_available_at": event.source_available_at + timedelta(seconds=timing_shift),
            "ingested_at": event.ingested_at + timedelta(seconds=timing_shift),
            "processed_at": event.processed_at + timedelta(seconds=timing_shift),
            "event_time": event.event_time + timedelta(seconds=timing_shift),
            "amount": payment.amount,
            "payee_account_id": payment.payee_account_id,
            "merchant_id": payment.merchant_id,
            "card_id": payment.card_id,
            "scenario_id": None,
            "scenario_type": None,
            "scenario_trigger": None,
            "scenario_reason": None,
            "fraud_record_id": None,
            "affected_entity_ids": (),
        }
        if device_value is not None:
            event_update["device_id"] = str(device_value)
        event_updates.append(event.model_copy(update=event_update))
    modified_events = tuple(event_updates)
    modified_entries: list[LedgerEntry] = []
    account_balances = {account.account_id: account.ledger_balance for account in entities.accounts}
    for entry in trajectory.source_ledger_entries:
        derived_event_id = event_map.get(entry.event_id)
        if derived_event_id is None:
            continue
        modified_event = next(
            (item for item in modified_events if item.event_id == derived_event_id), None
        )
        if modified_event is None:
            continue
        account_id = entry.account_id
        if source.payee_account_id is not None and account_id == source.payee_account_id:
            account_id = payment.payee_account_id or account_id
        balance = account_balances.get(account_id, 0.0)
        delta = payment.amount if entry.entry_type == "CREDIT" else -payment.amount
        balance_after = round(balance + delta, 2)
        account_balances[account_id] = balance_after
        modified_entries.append(
            entry.model_copy(
                update={
                    "ledger_entry_id": _derived_id("LED", f"{material}:{entry.ledger_entry_id}"),
                    "account_id": account_id,
                    "payment_id": payment_id,
                    "event_id": modified_event.event_id,
                    "amount": payment.amount,
                    "occurred_at": modified_event.event_time,
                    "effective_at": modified_event.event_time,
                    "posted_at": modified_event.processed_at,
                    "balance_after": balance_after,
                }
            )
        )
    return (
        payment,
        modified_events,
        tuple(modified_entries),
        event_map | {source.payment_id: payment_id},
    )


def generate_counterfactuals(
    config: SimulationRunConfig,
    entities: "EntityDataset",
    baseline: "PaymentDataset",
    *,
    counterfactual_id: str | None = None,
    run_id: str | None = None,
) -> CounterfactualDataset:
    """Generate deterministic sidecar counterfactuals from a pristine stream."""

    resolved = resolve_counterfactual(config)
    if not resolved.enabled:
        return CounterfactualDataset(counterfactual_id=counterfactual_id or "CF-INACTIVE")
    identity_material = {"run_id": run_id, "config": resolved.effective_configuration_hash}
    cf_id = counterfactual_id or f"CF-{sha256_json(identity_material)[:16]}"
    candidates = select_source_trajectories(config, baseline)
    resolved_difficulty = resolve_difficulty(config)
    resolved_camouflage = resolve_camouflage(config)
    used_sources: set[str] = set()
    original_payments: list[Payment] = []
    original_events: list[PaymentEvent] = []
    original_entries: list[LedgerEntry] = []
    modified_payments: list[Payment] = []
    modified_events: list[PaymentEvent] = []
    modified_entries: list[LedgerEntry] = []
    changesets: list[CounterfactualChangeSet] = []
    fraud_records: list[FraudRecord] = []
    graph_campaigns: list[GraphCampaign] = []
    graph_memberships: list[GraphCampaignMembership] = []
    graph_patterns: list[GraphPattern] = []
    # Keep the named stream namespace stable even though source ordering is
    # canonical and therefore does not consume selection randomness.
    create_stream_rng(config.simulation.seed, "milestone-14:selection")
    mutation_rng = create_stream_rng(config.simulation.seed, "milestone-14:mutation")
    for request_index, scope in enumerate(resolved.scopes):
        if not scope.enabled:
            for instance in range(_request_count(config.counterfactual, request_index)):
                changesets.append(
                    CounterfactualChangeSet(
                        change_set_id=_derived_id("SET", f"{cf_id}:{request_index}:{instance}"),
                        request_index=request_index,
                        objective=scope.objective,
                        status="REJECTED",
                        source_payment_id=None,
                        derived_payment_id=None,
                        requested_budget=scope.budget,
                        resolved_budget=scope.budget,
                        effective_distance=None,
                        per_dimension_costs=scope.costs,
                        changed_fields={},
                        inapplicable_dimensions=(),
                        feasibility_constraints=("objective enabled",),
                        objective_satisfaction={"satisfied": False},
                        rejection_reason="counterfactual objective is disabled",
                    )
                )
            continue
        difficulty_plan = resolved_difficulty.plan(scope.objective)
        camouflage_plan = (
            resolved_camouflage.plan(scope.family, scope.objective)
            if resolved_camouflage.enabled
            else None
        )
        request_candidates = [
            candidate
            for candidate in candidates
            if candidate.source_payment.payment_id not in used_sources
        ]
        if config.counterfactual.allow_source_reuse:
            request_candidates = list(candidates)
        if scope.objective in FRAUD_OBJECTIVES:
            request_candidates = [
                item
                for item in request_candidates
                if item.source_payment.payment_rail in _objective_rails(scope.objective)
            ]
        for instance in range(_request_count(config.counterfactual, request_index)):
            candidate = (
                request_candidates[instance % len(request_candidates)]
                if request_candidates
                else None
            )
            requested_budget = scope.budget
            if candidate is None:
                changesets.append(
                    CounterfactualChangeSet(
                        change_set_id=_derived_id("SET", f"{cf_id}:{request_index}:{instance}"),
                        request_index=request_index,
                        objective=scope.objective,
                        status="REJECTED",
                        source_payment_id=None,
                        derived_payment_id=None,
                        requested_budget=requested_budget,
                        resolved_budget=scope.budget,
                        effective_distance=None,
                        per_dimension_costs=scope.costs,
                        changed_fields={},
                        inapplicable_dimensions=(),
                        feasibility_constraints=("legitimate source capacity",),
                        objective_satisfaction={"satisfied": False},
                        rejection_reason="insufficient legitimate trajectory capacity",
                    )
                )
                continue
            source = candidate
            source_payment = source.source_payment
            timing_strength = (
                camouflage_plan.feature_strengths["timing"] if camouflage_plan is not None else 0.0
            )
            changes, total, satisfied, reason, inapplicable = _find_feasible_mutation(
                scope,
                source,
                entities,
                amount_multiplier=difficulty_plan.amount_multiplier,
                timing_multiplier=difficulty_plan.timing_multiplier,
                timing_strength=timing_strength,
            )
            if not changes or total > scope.budget or not satisfied:
                changesets.append(
                    CounterfactualChangeSet(
                        change_set_id=_derived_id("SET", f"{cf_id}:{request_index}:{instance}"),
                        request_index=request_index,
                        objective=scope.objective,
                        status="REJECTED",
                        source_payment_id=source_payment.payment_id,
                        derived_payment_id=None,
                        requested_budget=requested_budget,
                        resolved_budget=scope.budget,
                        effective_distance=total,
                        per_dimension_costs=scope.costs,
                        changed_fields=changes,
                        inapplicable_dimensions=inapplicable,
                        feasibility_constraints=("objective satisfaction", "distance budget"),
                        objective_satisfaction={"satisfied": satisfied, "reason": reason},
                        rejection_reason="no feasible mutation within distance budget",
                    )
                )
                continue
            payment, events, entries, mapping = _clone_trajectory(
                source,
                changes,
                objective=scope.objective,
                config=config,
                entities=entities,
                request_index=request_index * 1000 + instance,
            )
            try:
                grouped = tuple(event for event in events if event.payment_id == payment.payment_id)
                validate_payment_lifecycle(payment, grouped)
                if payment.payment_rail == "CARD":
                    validate_card_lifecycle(payment, grouped)
                elif payment.payment_rail == "PIX":
                    validate_pix_lifecycle(payment, grouped)
                if entries:
                    validate_ledger(entities.accounts, (payment,), grouped, entries)
            except ValueError as exc:
                changesets.append(
                    CounterfactualChangeSet(
                        change_set_id=_derived_id("SET", f"{cf_id}:{request_index}:{instance}"),
                        request_index=request_index,
                        objective=scope.objective,
                        status="REJECTED",
                        source_payment_id=source_payment.payment_id,
                        derived_payment_id=payment.payment_id,
                        requested_budget=requested_budget,
                        resolved_budget=scope.budget,
                        effective_distance=total,
                        per_dimension_costs=scope.costs,
                        changed_fields=changes,
                        inapplicable_dimensions=inapplicable,
                        feasibility_constraints=("lifecycle", "ledger"),
                        objective_satisfaction={"satisfied": False},
                        rejection_reason=str(exc),
                        source_to_counterfactual=mapping,
                    )
                )
                continue
            used_sources.add(source_payment.payment_id)
            original_payments.append(source_payment)
            original_events.extend(source.source_events)
            original_entries.extend(source.source_ledger_entries)
            modified_payments.append(payment)
            modified_events.extend(events)
            modified_entries.extend(entries)
            initial_event = _initial_event(events)
            payer_account = next(
                account
                for account in entities.accounts
                if account.account_id == payment.payer_account_id
            )
            fraud_records.append(
                FraudRecord(
                    fraud_record_id=_derived_id(
                        "FRAUD", f"{cf_id}:{request_index}:{instance}:{payment.payment_id}"
                    ),
                    record_type="FRAUD",
                    scenario_id=f"CF-{scope.objective}-{request_index}-{instance}",
                    scenario_type=scope.objective,
                    fraud_truth=True,
                    trigger="COUNTERFACTUAL_OBJECTIVE",
                    reason=reason,
                    customer_id=payer_account.customer_id,
                    account_id=payment.payer_account_id,
                    card_id=payment.card_id,
                    device_id=initial_event.device_id,
                    merchant_id=payment.merchant_id,
                    payment_id=payment.payment_id,
                    event_id=initial_event.event_id,
                    occurred_at=initial_event.event_time,
                    amount=payment.amount,
                    currency=payment.currency,
                    correlation_id=initial_event.correlation_id,
                    causation_id=initial_event.causation_id,
                    affected_entity_ids=tuple(
                        item
                        for item in (
                            payer_account.customer_id,
                            payment.payer_account_id,
                            payment.payee_account_id,
                            payment.merchant_id,
                            payment.card_id,
                            initial_event.device_id,
                        )
                        if item is not None
                    ),
                )
            )
            if scope.objective in GRAPH_OBJECTIVES:
                campaign_id = _derived_id("CMP", f"{cf_id}:{request_index}:{instance}")
                valid_from = min(item.event_time for item in events)
                valid_to = max(item.event_time for item in events)
                participants = tuple(
                    item
                    for item in (payment.payer_account_id, payment.payee_account_id)
                    if item is not None
                )
                graph_campaigns.append(
                    GraphCampaign(
                        campaign_id=campaign_id,
                        scenario_type=cast(Any, scope.objective),
                        scenario_code=scope.objective,
                        truth_label="FRAUD",
                        valid_from=valid_from,
                        valid_to=valid_to,
                        participant_ids=participants,
                    )
                )
                for role, member_id in zip(("SOURCE", "BENEFICIARY"), participants, strict=False):
                    graph_memberships.append(
                        GraphCampaignMembership(
                            campaign_id=campaign_id,
                            pattern_type=cast(Any, scope.objective),
                            member_id=member_id,
                            member_type="ACCOUNT",
                            role=role,
                            valid_from=valid_from,
                            valid_to=valid_to,
                            source_event_id=events[0].event_id,
                            payment_id=payment.payment_id,
                        )
                    )
                graph_patterns.append(
                    GraphPattern(
                        pattern_id=_derived_id("PAT", campaign_id),
                        pattern_type=cast(Any, scope.objective),
                        campaign_id=campaign_id,
                        detected_at=valid_to,
                        window_from=valid_from,
                        window_to=valid_to,
                        member_ids=participants,
                        source_event_ids=tuple(item.event_id for item in events),
                        payment_ids=(payment.payment_id,),
                        truth_label="FRAUD",
                        scenario_code=scope.objective,
                    )
                )
            changesets.append(
                CounterfactualChangeSet(
                    change_set_id=_derived_id("SET", f"{cf_id}:{request_index}:{instance}"),
                    request_index=request_index,
                    objective=scope.objective,
                    status="ACCEPTED",
                    source_payment_id=source_payment.payment_id,
                    derived_payment_id=payment.payment_id,
                    requested_budget=requested_budget,
                    resolved_budget=scope.budget,
                    effective_distance=total,
                    per_dimension_costs=scope.costs,
                    changed_fields=changes,
                    inapplicable_dimensions=inapplicable,
                    feasibility_constraints=("lifecycle", "ledger", "point-in-time source"),
                    objective_satisfaction={"satisfied": True, "reason": reason},
                    source_to_counterfactual=mapping,
                )
            )
            mutation_rng.random()
    alerts: tuple[FraudAlert, ...] = ()
    fraud_cases: tuple[FraudCase, ...] = ()
    case_confirmations: tuple[FraudCaseConfirmation, ...] = ()
    customer_disputes: tuple[CustomerDispute, ...] = ()
    fraud_labels: tuple[DelayedFraudLabel, ...] = ()
    if fraud_records and config.fraud_workflow.enabled:
        from fraudtwin.simulation.cases import FraudWorkflowGenerator
        from fraudtwin.simulation.fraud import FraudDataset

        workflow = FraudWorkflowGenerator(
            config,
            entities,
            FraudDataset(
                tuple(modified_payments),
                tuple(modified_events),
                tuple(modified_entries),
                tuple(fraud_records),
            ),
        ).generate()
        alerts = workflow.alerts
        fraud_cases = workflow.cases
        case_confirmations = workflow.confirmations
        customer_disputes = workflow.disputes
        fraud_labels = workflow.labels
    metadata: dict[str, object] = {
        "resolver_version": resolved.resolver_version,
        "effective_configuration_hash": resolved.effective_configuration_hash,
        "requested_configuration": resolved.requested,
        "resolved_scopes": [scope.model_dump(mode="json") for scope in resolved.scopes],
        "source_run_id": run_id,
        "source_snapshot": {
            "run_id": run_id,
            "decision_rule": "source_available_at <= initial source_available_at",
            "candidate_count": len(candidates),
            "candidate_payment_ids": [item.source_payment.payment_id for item in candidates],
            "selected_source_payment_ids": [item.payment_id for item in original_payments],
            "available_event_ids": {
                item.source_payment.payment_id: list(item.available_event_ids)
                for item in candidates
            },
        },
        "seed_streams": ["milestone-14:selection", "milestone-14:mutation"],
        "source_fingerprint": sha256_json(
            [item.model_dump(mode="json") for item in original_payments]
        ),
        "output_fingerprint": sha256_json(
            [item.model_dump(mode="json") for item in modified_payments]
        ),
        "schema_fingerprint": sha256_json(
            {
                "payments": list(Payment.model_fields),
                "payment_events": list(PaymentEvent.model_fields),
                "ledger_entries": list(LedgerEntry.model_fields),
                "change_sets": list(CounterfactualChangeSet.model_fields),
            }
        ),
        "transformation_order": ["M14", "M12", "M13"],
        "difficulty": resolved_difficulty.model_dump(mode="json"),
        "camouflage": resolved_camouflage.model_dump(mode="json"),
        "m12_m13_application": {
            "m12_resolved": True,
            "m13_resolved": True,
            "m12_applied_to_search_constraints": True,
            "m13_applied_to_search_constraints": bool(resolved_camouflage.enabled),
            "m13_workflow_generated": bool(config.fraud_workflow.enabled),
            "note": "M12/M13 streams are isolated to M14; legacy streams are untouched.",
        },
    }
    return CounterfactualDataset(
        counterfactual_id=cf_id,
        original_payments=tuple(original_payments),
        original_events=tuple(original_events),
        original_ledger_entries=tuple(original_entries),
        modified_payments=tuple(modified_payments),
        modified_events=tuple(modified_events),
        modified_ledger_entries=tuple(modified_entries),
        fraud_records=tuple(fraud_records),
        alerts=alerts,
        fraud_cases=fraud_cases,
        case_confirmations=case_confirmations,
        customer_disputes=customer_disputes,
        fraud_labels=fraud_labels,
        change_sets=tuple(changesets),
        graph_campaigns=tuple(graph_campaigns),
        graph_memberships=tuple(graph_memberships),
        graph_patterns=tuple(graph_patterns),
        metadata=metadata,
    )


def _request_count(config: CounterfactualConfig, index: int) -> int:
    if index < len(config.requests):
        return config.requests[index].count
    return 1


__all__ = [
    "ALL_OBJECTIVES",
    "CounterfactualChangeSet",
    "CounterfactualDataset",
    "CounterfactualScope",
    "DistanceFunction",
    "ResolvedCounterfactual",
    "SourceTrajectory",
    "generate_counterfactuals",
    "register_distance_function",
    "resolve_counterfactual",
    "select_source_trajectories",
]
