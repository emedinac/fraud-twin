"""Deterministic camouflage resolution and transformations."""

import statistics
from collections import defaultdict
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, cast

from pydantic import BaseModel, ConfigDict

from fraudtwin.config import (
    CamouflageCohortConfig,
    CamouflageFamilyConfig,
    SimulationRunConfig,
    StressConfig,
)
from fraudtwin.domain import (
    PAYMENT_EVENT_CONTRACT_VERSION,
    BehaviorProfile,
    FraudRecord,
    GraphCampaign,
    GraphCampaignMembership,
    GraphEvidence,
    GraphPattern,
    LedgerEntry,
    Payment,
    PaymentEvent,
)
from fraudtwin.reproducibility import sha256_json
from fraudtwin.seed import create_stream_rng

if TYPE_CHECKING:
    from fraudtwin.simulation.generator import EntityDataset

CAMOUFLAGE_VERSION = "1"
FEATURES: tuple[str, ...] = (
    "amount",
    "timing",
    "merchant",
    "device",
    "geography",
    "frequency",
)
RELATIONS: tuple[str, ...] = (
    "transferred_to",
    "transacted_with",
    "shares_device",
    "shares_ip",
)


class CamouflagePlan(BaseModel):
    """Resolved strengths for one generator family and scenario."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    family: str
    scenario: str
    feature_strengths: dict[str, float]
    relation_strengths: dict[str, float]
    cohort: dict[str, object]


class ResolvedCamouflage(BaseModel):
    """Complete, deterministic and hashable M13 configuration."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    enabled: bool
    resolver_version: str
    source_namespace: str | None
    requested: dict[str, object]
    resolved_global: dict[str, float]
    resolved_families: dict[str, dict[str, object]]
    cohort: dict[str, object]
    effective_configuration_hash: str

    def plan(self, family: str, scenario: str) -> CamouflagePlan:
        return apply_camouflage(self, family, scenario)


def _default_cohort() -> CamouflageCohortConfig:
    return CamouflageCohortConfig()


def _settings(config: SimulationRunConfig) -> tuple[str | None, Any]:
    if config.stress.active:
        return "stress", config.stress
    if config.benchmark.camouflage_active:
        return "benchmark", config.benchmark
    return None, None


def _family_map(settings: Any) -> dict[str, CamouflageFamilyConfig]:
    if isinstance(settings, StressConfig):
        return cast(dict[str, CamouflageFamilyConfig], settings.families)
    return cast(dict[str, CamouflageFamilyConfig], settings.camouflage_families)


def _cohort(settings: Any) -> CamouflageCohortConfig:
    return settings.cohort if isinstance(settings, StressConfig) else settings.camouflage_cohort


def resolve_camouflage(config: SimulationRunConfig) -> ResolvedCamouflage:
    """Resolve global, family, and leaf M13 controls without consuming RNG."""

    namespace, settings = _settings(config)
    if settings is None:
        return ResolvedCamouflage(
            enabled=False,
            resolver_version=CAMOUFLAGE_VERSION,
            source_namespace=None,
            requested={},
            resolved_global={name: 0.0 for name in ("camouflage", "feature", "relation")},
            resolved_families={},
            cohort=_default_cohort().model_dump(mode="json"),
            effective_configuration_hash="",
        )
    global_strength = settings.camouflage
    feature = (
        settings.feature_camouflage if settings.feature_camouflage is not None else global_strength
    )
    relation = (
        settings.relation_camouflage
        if settings.relation_camouflage is not None
        else global_strength
    )
    families = _family_map(settings)
    resolved_families: dict[str, dict[str, object]] = {}
    for family_name in ("fraud", "graph"):
        family = families.get(family_name, CamouflageFamilyConfig())
        family_feature = family.feature_camouflage
        if family_feature is None:
            family_feature = family.camouflage if family.camouflage is not None else feature
        family_relation = family.relation_camouflage
        if family_relation is None:
            family_relation = family.camouflage if family.camouflage is not None else relation
        resolved_families[family_name] = {
            "feature_camouflage": float(family_feature or 0.0),
            "relation_camouflage": float(family_relation or 0.0),
            "features": {
                name: float(family_feature if value is None else value)
                for name, value in family.features.items()
            },
            "relations": {
                name: float(family_relation if value is None else value)
                for name, value in family.relations.items()
            },
        }
    requested = settings.model_dump(mode="json")
    payload = {
        "resolver_version": CAMOUFLAGE_VERSION,
        "source_namespace": namespace,
        "requested": requested,
        "resolved_global": {
            "camouflage": float(global_strength or 0.0),
            "feature": float(feature or 0.0),
            "relation": float(relation or 0.0),
        },
        "resolved_families": resolved_families,
        "cohort": _cohort(settings).model_dump(mode="json"),
    }
    return ResolvedCamouflage(
        enabled=True,
        resolver_version=CAMOUFLAGE_VERSION,
        source_namespace=namespace,
        requested=requested,
        resolved_global=payload["resolved_global"],
        resolved_families=resolved_families,
        cohort=payload["cohort"],
        effective_configuration_hash=sha256_json(payload),
    )


def apply_camouflage(resolved: ResolvedCamouflage, family: str, scenario: str) -> CamouflagePlan:
    """Return a stable per-scenario plan; no data or RNG is mutated."""

    if family not in {"fraud", "graph"}:
        raise ValueError(f"unsupported camouflage family: {family}")
    values = resolved.resolved_families.get(family)
    if values is None:
        raise ValueError(f"unsupported camouflage family: {family}")
    family_feature = cast(float, values["feature_camouflage"])
    family_relation = cast(float, values["relation_camouflage"])
    features = {name: family_feature for name in FEATURES}
    features.update(
        {name: float(value) for name, value in cast(dict[str, float], values["features"]).items()}
    )
    relations = {name: family_relation for name in RELATIONS}
    relations.update(
        {name: float(value) for name, value in cast(dict[str, float], values["relations"]).items()}
    )
    return CamouflagePlan(
        family=family,
        scenario=scenario,
        feature_strengths=features,
        relation_strengths=relations,
        cohort=dict(resolved.cohort),
    )


def _truth_ids(records: tuple[FraudRecord, ...]) -> set[str]:
    return {record.payment_id for record in records if record.fraud_truth}


def _family_for_scenario(scenario: str) -> str:
    return "fraud" if scenario.startswith("F") else "graph"


def _blend(value: float, reference: float, strength: float) -> float:
    return round((1.0 - strength) * value + strength * reference, 2)


def _shift_event(event: PaymentEvent, delta: timedelta) -> PaymentEvent:
    return event.model_copy(
        update={
            "event_time": event.event_time + delta,
            "source_created_at": event.source_created_at + delta,
            "source_available_at": event.source_available_at + delta,
            "ingested_at": event.ingested_at + delta,
            "processed_at": event.processed_at + delta,
        }
    )


def _matches_profile(
    candidate: Payment,
    *,
    events_by_payment: dict[str, tuple[PaymentEvent, ...]],
    accounts_by_id: dict[str, Any],
    customers_by_id: dict[str, Any],
    profiles_by_customer: dict[str, BehaviorProfile],
    merchants_by_id: dict[str, Any],
    devices: tuple[Any, ...],
    target_profile: BehaviorProfile | None,
    target_customer: Any,
    dimensions: tuple[str, ...],
) -> bool:
    account = accounts_by_id.get(candidate.payer_account_id)
    profile = profiles_by_customer.get(account.customer_id) if account else None
    customer = customers_by_id.get(account.customer_id) if account else None
    candidate_events = events_by_payment.get(candidate.payment_id, ())
    candidate_initial = (
        min(candidate_events, key=lambda event: event.event_time) if candidate_events else None
    )
    if target_profile is None or profile is None or candidate_initial is None:
        return False
    if "spending_level" in dimensions and profile.spending_level != target_profile.spending_level:
        return False
    if (
        "country" in dimensions
        and target_customer
        and customer
        and customer.country != target_customer.country
    ):
        return False
    if "merchant_category" in dimensions and candidate.merchant_id:
        merchant = merchants_by_id.get(candidate.merchant_id)
        if (
            merchant
            and merchant.merchant_category_code not in target_profile.merchant_category_preferences
        ):
            return False
    if (
        "typical_payment_hour" in dimensions
        and candidate_initial.event_time.hour not in target_profile.typical_payment_hours
    ):
        return False
    if "trusted_device" in dimensions:
        trusted = any(
            device.device_id == candidate_initial.device_id and device.trusted for device in devices
        )
        if trusted != bool(target_profile.preferred_device_ids):
            return False
    return True


def _build_relation_support(
    config: SimulationRunConfig,
    entities: "EntityDataset",
    records: tuple[FraudRecord, ...],
    legitimate: list[Payment],
    plans_by_record_id: dict[str, CamouflagePlan],
    accounts_by_id: dict[str, Any],
    constraints: dict[str, dict[str, object]],
    events: tuple[PaymentEvent, ...],
) -> tuple[list[Payment], list[PaymentEvent]]:
    """Create benign sourced events for the strongest available relation."""

    relation_payments: list[Payment] = []
    relation_events: list[PaymentEvent] = []
    if not legitimate:
        return relation_payments, relation_events

    participant_ids = {
        record.account_id for record in records if record.fraud_truth and record.account_id
    }
    eligible_targets = tuple(
        account for account in entities.accounts if account.account_id not in participant_ids
    )
    if not eligible_targets:
        return relation_payments, relation_events

    from fraudtwin.simulation.payments import payment_event_times

    simulation_run_id = events[0].simulation_run_id if events else "in-memory"
    legitimate_reference_amount = statistics.median(item.amount for item in legitimate)
    merchant_values = tuple(sorted({item.merchant_id for item in legitimate if item.merchant_id}))
    device_values = tuple(sorted(device.device_id for device in entities.devices))
    ip_values = tuple(sorted(endpoint.endpoint_id for endpoint in entities.network_endpoints))

    relation_ordinal = 0
    for record in records:
        if not record.fraud_truth or not record.account_id:
            continue
        family = _family_for_scenario(record.scenario_type)
        plan = plans_by_record_id[record.fraud_record_id]
        if max(plan.relation_strengths.values()) <= 0:
            continue
        relation_ordinal += 1
        source = accounts_by_id.get(record.account_id)
        if source is None:
            continue
        relation_rng = create_stream_rng(
            config.simulation.seed,
            f"milestone-13:{family}:{record.scenario_id}:{relation_ordinal}:relation",
        )
        target = eligible_targets[relation_rng.randrange(len(eligible_targets))]
        when = min(
            record.occurred_at + timedelta(seconds=relation_ordinal),
            config.simulation.start
            + timedelta(days=config.simulation.duration_days, microseconds=-1),
        )
        available_relations = tuple(
            name
            for name in RELATIONS
            if name != "transacted_with" or merchant_values
            if name != "shares_device" or device_values
            if name != "shares_ip" or ip_values
        )
        relation_name = max(
            available_relations,
            key=lambda name: (plan.relation_strengths[name], -RELATIONS.index(name)),
        )
        requested_relation = max(
            RELATIONS,
            key=lambda name: (plan.relation_strengths[name], -RELATIONS.index(name)),
        )
        if relation_name != requested_relation:
            constraint_entry = constraints.setdefault(record.scenario_id, {})
            constrained = cast(dict[str, object], constraint_entry.setdefault("constrained", {}))
            constrained[requested_relation] = True

        support_accounts: tuple[Any, ...] = (
            (source, target) if relation_name in {"shares_device", "shares_ip"} else (source,)
        )
        for support_index, payer in enumerate(support_accounts, start=1):
            is_transfer = relation_name == "transferred_to"
            merchant_id = (
                merchant_values[(relation_ordinal - 1) % len(merchant_values)]
                if relation_name == "transacted_with" and merchant_values
                else None
            )
            device_id = (
                device_values[(relation_ordinal - 1) % len(device_values)]
                if relation_name == "shares_device" and device_values
                else None
            )
            ip_id = (
                ip_values[(relation_ordinal - 1) % len(ip_values)]
                if relation_name == "shares_ip" and ip_values
                else None
            )
            available = payer.available_balance + payer.overdraft_limit
            amount = min(max(0.01, legitimate_reference_amount), max(0.01, available))
            payment_id = f"PAY-CAMO-{family.upper()}-{relation_ordinal:06d}-{support_index:02d}"
            event_id = f"EVT-CAMO-{family.upper()}-{relation_ordinal:06d}-{support_index:02d}"
            event_time = when + timedelta(microseconds=support_index - 1)
            available_at, ingested_at, processed_at = payment_event_times(
                event_time, "ACCOUNT_TRANSFER"
            )
            payment = Payment(
                payment_id=payment_id,
                payment_rail="ACCOUNT_TRANSFER",
                payment_type="TRANSFER" if is_transfer else "PURCHASE",
                payer_account_id=payer.account_id,
                payee_account_id=target.account_id if is_transfer else None,
                merchant_id=merchant_id,
                card_id=None,
                amount=round(amount, 2),
                currency=payer.currency,
                initiated_at=event_time,
                current_status="COMPLETED",
                payer_institution_id=payer.institution_id,
                payee_institution_id=target.institution_id if is_transfer else None,
            )
            relation_payments.append(payment)
            relation_events.append(
                PaymentEvent(
                    event_id=event_id,
                    event_type="TRANSFER_COMPLETED",
                    event_version=1,
                    payment_id=payment_id,
                    customer_id=payer.customer_id,
                    account_id=payer.account_id,
                    event_time=event_time,
                    source_created_at=event_time,
                    source_available_at=available_at,
                    ingested_at=ingested_at,
                    processed_at=processed_at,
                    producer="fraudtwin.camouflage",
                    source_system="synthetic_payment_source",
                    schema_version=PAYMENT_EVENT_CONTRACT_VERSION,
                    correlation_id=payment_id,
                    causation_id=None,
                    simulation_run_id=simulation_run_id,
                    scenario_id=None,
                    payment_rail="ACCOUNT_TRANSFER",
                    payment_type="TRANSFER" if is_transfer else "PURCHASE",
                    payee_account_id=target.account_id if is_transfer else None,
                    merchant_id=merchant_id,
                    card_id=None,
                    device_id=device_id,
                    ip_id=ip_id,
                    online=False,
                    amount=payment.amount,
                    currency=payment.currency,
                )
            )
    return relation_payments, relation_events


def transform_generated_data(
    config: SimulationRunConfig,
    entities: "EntityDataset",
    profiles: tuple[BehaviorProfile, ...],
    payments: tuple[Payment, ...],
    events: tuple[PaymentEvent, ...],
    ledger_entries: tuple[LedgerEntry, ...],
    records: tuple[FraudRecord, ...],
    memberships: tuple[GraphCampaignMembership, ...],
    campaigns: tuple[GraphCampaign, ...],
    patterns: tuple[GraphPattern, ...],
    evidence: tuple[GraphEvidence, ...],
) -> tuple[
    tuple[Payment, ...],
    tuple[PaymentEvent, ...],
    tuple[LedgerEntry, ...],
    tuple[FraudRecord, ...],
    tuple[GraphCampaignMembership, ...],
    tuple[GraphCampaign, ...],
    tuple[GraphPattern, ...],
    tuple[GraphEvidence, ...],
    dict[str, object],
]:
    """Apply M13 as a post-generation, invariant-preserving transformation."""

    # Keep the import local: ``simulation.generator`` imports behavior, which
    # imports this module.  The runtime check fixes the old TYPE_CHECKING-only
    # dependency without introducing an import cycle during package startup.
    from fraudtwin.simulation.generator import EntityDataset as RuntimeEntityDataset

    if not isinstance(entities, RuntimeEntityDataset):
        raise TypeError("entities must be an EntityDataset")

    resolved = resolve_camouflage(config)
    if not resolved.enabled or not any(
        value > 0
        for family in resolved.resolved_families.values()
        for value in (
            cast(float, family["feature_camouflage"]),
            cast(float, family["relation_camouflage"]),
        )
    ):
        return (
            payments,
            events,
            ledger_entries,
            records,
            memberships,
            campaigns,
            patterns,
            evidence,
            {},
        )

    true_ids = _truth_ids(records)
    events_by_payment_mutable: defaultdict[str, list[PaymentEvent]] = defaultdict(list)
    for event in events:
        events_by_payment_mutable[event.payment_id].append(event)
    events_by_payment = {
        payment_id: tuple(payment_events)
        for payment_id, payment_events in events_by_payment_mutable.items()
    }
    legitimate = [payment for payment in payments if payment.payment_id not in true_ids]
    payments_by_id = {payment.payment_id: payment for payment in payments}
    accounts_by_id = {account.account_id: account for account in entities.accounts}
    customers_by_id = {customer.customer_id: customer for customer in entities.customers}
    profiles_by_customer = {profile.customer_id: profile for profile in profiles}
    merchants_by_id = {merchant.merchant_id: merchant for merchant in entities.merchants}
    payment_updates: dict[str, Payment] = {}
    event_updates: dict[str, PaymentEvent] = {}
    record_updates: dict[str, FraudRecord] = {}
    feature_summary: dict[str, dict[str, float]] = defaultdict(dict)
    similarity_summary: dict[str, dict[str, float]] = defaultdict(dict)
    cohort_resolution: dict[str, dict[str, object]] = {}
    constraints: dict[str, dict[str, object]] = {}
    scenario_ordinals: dict[str, int] = defaultdict(int)
    scenario_last_time: dict[str, datetime] = {}
    plans_by_record_id = {
        record.fraud_record_id: apply_camouflage(
            resolved, _family_for_scenario(record.scenario_type), record.scenario_type
        )
        for record in records
        if record.fraud_truth
    }
    for record in records:
        if not record.fraud_truth or record.payment_id not in events_by_payment:
            continue
        family = _family_for_scenario(record.scenario_type)
        plan = plans_by_record_id[record.fraud_record_id]
        payment = payments_by_id[record.payment_id]
        strategy = str(resolved.cohort.get("strategy", "same_rail_and_profile"))
        if strategy == "global_legitimate":
            cohort = list(legitimate)
        else:
            cohort = [item for item in legitimate if item.payment_rail == payment.payment_rail]
        if strategy == "same_rail_and_profile":
            target_account = accounts_by_id.get(payment.payer_account_id)
            target_profile = (
                profiles_by_customer.get(target_account.customer_id) if target_account else None
            )
            target_customer = (
                customers_by_id.get(target_account.customer_id) if target_account else None
            )
            dimensions = tuple(cast(list[str], resolved.cohort.get("profile_dimensions", [])))

            cohort = [
                item
                for item in cohort
                if _matches_profile(
                    item,
                    events_by_payment=events_by_payment,
                    accounts_by_id=accounts_by_id,
                    customers_by_id=customers_by_id,
                    profiles_by_customer=profiles_by_customer,
                    merchants_by_id=merchants_by_id,
                    devices=entities.devices,
                    target_profile=target_profile,
                    target_customer=target_customer,
                    dimensions=dimensions,
                )
            ]
        requested_strategy = str(resolved.cohort.get("strategy", "same_rail_and_profile"))
        resolved_strategy = requested_strategy
        fallback_used = False
        if len(cohort) < int(cast(int, resolved.cohort.get("minimum_size", 3))):
            cohort = legitimate
            resolved_strategy = str(resolved.cohort.get("fallback", "same_rail"))
            fallback_used = True
        cohort_resolution[record.scenario_id] = {
            "requested_strategy": requested_strategy,
            "resolved_strategy": resolved_strategy,
            "fallback_used": fallback_used,
            "cohort_size": len(cohort),
        }
        if not cohort:
            if resolved.cohort.get("fallback") == "reject":
                raise ValueError(
                    f"camouflage cohort capacity is insufficient for {record.scenario_id}"
                )
            constraints[record.scenario_id] = {
                "requested": dict(plan.feature_strengths),
                "resolved": dict(plan.feature_strengths),
                "effective": {name: 0.0 for name in FEATURES},
                "constrained": {"cohort": True},
            }
            continue
        reference_amount = statistics.median(item.amount for item in cohort)
        amount_strength = plan.feature_strengths["amount"]
        new_amount = _blend(payment.amount, reference_amount, amount_strength)
        current_events = events_by_payment[payment.payment_id]
        initial = min(current_events, key=lambda item: item.event_time)
        cohort_initial_times = sorted(
            min(
                events_by_payment[item.payment_id], key=lambda event: event.event_time
            ).event_time.timestamp()
            for item in cohort
            if events_by_payment.get(item.payment_id)
        )
        scenario_ordinals[record.scenario_id] += 1
        target_time = cohort_initial_times[
            (scenario_ordinals[record.scenario_id] - 1) % len(cohort_initial_times)
        ]
        frequency_strength = plan.feature_strengths["frequency"]
        cohort_intervals = [
            right - left
            for left, right in zip(cohort_initial_times, cohort_initial_times[1:], strict=False)
        ]
        if (
            frequency_strength > 0
            and cohort_intervals
            and scenario_ordinals[record.scenario_id] > 1
        ):
            reference_interval = statistics.median(cohort_intervals)
            frequency_target = (
                cohort_initial_times[0]
                + (scenario_ordinals[record.scenario_id] - 1) * reference_interval
            )
            target_time = (
                1.0 - frequency_strength
            ) * target_time + frequency_strength * frequency_target
        current_time = initial.event_time.timestamp()
        timing_strength = plan.feature_strengths["timing"]
        raw_delta_seconds = (target_time - current_time) * timing_strength
        lower_delta = (
            config.simulation.start - min(item.event_time for item in current_events)
        ).total_seconds()
        upper_delta = (
            config.simulation.start
            + timedelta(days=config.simulation.duration_days, microseconds=-1)
            - max(item.event_time for item in current_events)
        ).total_seconds()
        delta = timedelta(seconds=max(lower_delta, min(upper_delta, raw_delta_seconds)))
        transformed_initial = initial.event_time + delta
        previous_time = scenario_last_time.get(record.scenario_id)
        if previous_time is not None and transformed_initial <= previous_time:
            delta += previous_time + timedelta(microseconds=1) - transformed_initial
        scenario_last_time[record.scenario_id] = initial.event_time + delta
        shifted = tuple(_shift_event(event, delta) for event in current_events)
        merchant_id = payment.merchant_id
        device_id = initial.device_id
        if plan.feature_strengths["merchant"] > 0:
            merchant_values = [item.merchant_id for item in cohort if item.merchant_id is not None]
            if merchant_values:
                merchant_rng = create_stream_rng(
                    config.simulation.seed,
                    f"milestone-13:{family}:{record.scenario_id}:{scenario_ordinals[record.scenario_id]}:merchant",
                )
                merchant_id = merchant_rng.choice(sorted(merchant_values))
        if plan.feature_strengths["device"] > 0:
            device_values = [
                event.device_id
                for item in cohort
                for event in events_by_payment.get(item.payment_id, ())
                if event.device_id
            ]
            if device_values:
                device_rng = create_stream_rng(
                    config.simulation.seed,
                    f"milestone-13:{family}:{record.scenario_id}:{scenario_ordinals[record.scenario_id]}:device",
                )
                device_id = device_rng.choice(sorted(device_values))
        updated_payment = payment.model_copy(
            update={
                "amount": new_amount,
                "merchant_id": merchant_id,
                "initiated_at": payment.initiated_at + delta,
            }
        )
        payment_updates[payment.payment_id] = updated_payment
        for event in shifted:
            event_updates[event.event_id] = event.model_copy(
                update={"amount": new_amount, "merchant_id": merchant_id, "device_id": device_id}
            )
        record_updates[record.fraud_record_id] = record.model_copy(
            update={
                "amount": new_amount,
                "occurred_at": record.occurred_at + delta,
                "merchant_id": merchant_id,
                "device_id": device_id,
            }
        )
        timing_constrained = abs(delta.total_seconds() - raw_delta_seconds) > 1e-6
        constraints[record.scenario_id] = {
            "requested": {
                "amount": plan.feature_strengths["amount"],
                "timing": plan.feature_strengths["timing"],
                "merchant": plan.feature_strengths["merchant"],
                "device": plan.feature_strengths["device"],
                "geography": plan.feature_strengths["geography"],
                "frequency": plan.feature_strengths["frequency"],
            },
            "resolved": dict(plan.feature_strengths),
            "effective": {
                "amount": plan.feature_strengths["amount"],
                "timing": plan.feature_strengths["timing"]
                if not timing_constrained
                else round(
                    max(
                        0.0,
                        min(
                            1.0,
                            abs(delta.total_seconds()) / max(abs(target_time - current_time), 1.0),
                        ),
                    ),
                    6,
                ),
                "merchant": plan.feature_strengths["merchant"] if merchant_id else 0.0,
                "device": plan.feature_strengths["device"] if device_id else 0.0,
                "geography": plan.feature_strengths["geography"] if merchant_id else 0.0,
                "frequency": plan.feature_strengths["frequency"],
            },
            "constrained": {"timing": timing_constrained},
        }
        feature_summary[record.scenario_type] = {
            "amount_strength": amount_strength,
            "timing_strength": timing_strength,
            "merchant_strength": plan.feature_strengths["merchant"],
            "device_strength": plan.feature_strengths["device"],
            "geography_strength": plan.feature_strengths["geography"],
            "frequency_strength": frequency_strength,
        }
        cohort_merchant_ids = {item.merchant_id for item in cohort if item.merchant_id}
        cohort_device_ids = {
            event.device_id
            for item in cohort
            for event in events_by_payment.get(item.payment_id, ())
            if event.device_id
        }
        amount_score = max(
            0.0, 1.0 - abs(new_amount - reference_amount) / max(reference_amount, 1.0)
        )
        timing_reference = target_time
        timing_score = max(
            0.0,
            1.0 - abs((initial.event_time + delta).timestamp() - timing_reference) / (7 * 86_400),
        )
        frequency_score = 1.0
        if previous_time is not None and cohort_intervals:
            observed_interval = abs((initial.event_time + delta - previous_time).total_seconds())
            reference_interval = float(statistics.median(cohort_intervals))
            frequency_score = max(
                0.0,
                1.0 - abs(observed_interval - reference_interval) / max(reference_interval, 1.0),
            )
        similarity_summary[record.scenario_type] = {
            "amount": round(amount_score, 6),
            "timing": round(timing_score, 6),
            "merchant": 1.0 if merchant_id in cohort_merchant_ids else 0.0,
            "device": 1.0 if device_id in cohort_device_ids else 0.0,
            "geography": 1.0 if merchant_id in cohort_merchant_ids else 0.0,
            "frequency": round(frequency_score, 6),
        }

    updated_payments = tuple(payment_updates.get(item.payment_id, item) for item in payments)
    updated_events = tuple(event_updates.get(item.event_id, item) for item in events)
    updated_records = tuple(record_updates.get(item.fraud_record_id, item) for item in records)

    relation_payments, relation_events = _build_relation_support(
        config,
        entities,
        updated_records,
        legitimate,
        plans_by_record_id,
        accounts_by_id,
        constraints,
        events,
    )

    updated_payments = (*updated_payments, *relation_payments)
    updated_events = (*updated_events, *relation_events)
    updated_memberships = tuple(
        item.model_copy(
            update={
                "valid_from": next(
                    (
                        event.event_time
                        for event in updated_events
                        if event.event_id == item.source_event_id
                    ),
                    item.valid_from,
                )
            }
        )
        for item in memberships
    )
    updated_evidence = tuple(
        item.model_copy(
            update={
                "observed_at": next(
                    (
                        event.event_time
                        for event in updated_events
                        if event.event_id == item.source_event_id
                    ),
                    item.observed_at,
                )
            }
        )
        for item in evidence
    )
    from fraudtwin.simulation.payments import PaymentGenerator

    payment_generator = PaymentGenerator(
        config,
        entities.accounts,
        entities.cards,
        entities.merchants,
        entities.devices,
        entities.pix_keys,
    )
    updated_ledger = payment_generator.materialize_ledger(
        updated_payments,
        updated_events,
        stage="camouflage ledger",
        protocol_id="P02",
        capacity_id="C04",
    )
    true_records = tuple(record for record in updated_records if record.fraud_truth)
    metadata = {
        "requested": resolved.requested,
        "resolved_global": resolved.resolved_global,
        "resolved_families": resolved.resolved_families,
        "override_provenance": {
            "namespace": resolved.source_namespace,
            "precedence": ["leaf", "family_modality", "global_modality", "global"],
            "resolver_version": resolved.resolver_version,
        },
        "cohort": resolved.cohort,
        "cohort_resolution": cohort_resolution,
        "effective_strengths": resolved.resolved_families,
        "constraints": constraints,
        "feasibility_caps": constraints,
        "schema_fingerprint": sha256_json(
            {
                "payments": list(Payment.model_fields),
                "payment_events": list(PaymentEvent.model_fields),
                "fraud_records": list(FraudRecord.model_fields),
            }
        ),
        "output_fingerprint": sha256_json(
            {
                "payments": [item.model_dump(mode="json") for item in updated_payments],
                "payment_events": [item.model_dump(mode="json") for item in updated_events],
                "fraud_records": [item.model_dump(mode="json") for item in updated_records],
            }
        ),
        "effective_configuration_hash": resolved.effective_configuration_hash,
        "resolver_version": resolved.resolver_version,
        "seed_stream_prefix": "milestone-13",
        "seed_stream_identifiers": [
            "milestone-13:{family}:{scenario}:{ordinal}:merchant",
            "milestone-13:{family}:{scenario}:{ordinal}:device",
            "milestone-13:{family}:{scenario}:{ordinal}:relation",
        ],
        "feature_summary": dict(feature_summary),
        "transformations": {
            "features": dict(feature_summary),
            "relations": {"support_events": len(relation_events)},
        },
        "measurable_summaries": {
            "feature_similarity": dict(similarity_summary),
            "feature_coverage": {
                name: sum(1 for item in similarity_summary.values() if name in item)
                for name in FEATURES
            },
        },
        "relation_summary": {
            "added_support_payments": len(relation_payments),
            "added_support_events": len(relation_events),
            "fraud_induced_topology_preserved": True,
            "relation_strengths": {
                relation: round(
                    max(
                        (
                            plans_by_record_id[record.fraud_record_id].relation_strengths[relation]
                            for record in true_records
                        ),
                        default=0.0,
                    ),
                    6,
                )
                for relation in RELATIONS
            },
            "relation_strength_observed": round(
                sum(
                    max(plans_by_record_id[record.fraud_record_id].relation_strengths.values())
                    for record in true_records
                )
                / max(1, len(true_records)),
                6,
            ),
        },
        "source_snapshot": {
            "legitimate_payment_count": len(legitimate),
            "fraud_payment_count": len(true_ids),
            "profile_count": len(profiles),
        },
        "source_snapshots": {
            "legitimate_payment_count": len(legitimate),
            "fraud_payment_count": len(true_ids),
            "profile_count": len(profiles),
            "event_count": len(updated_events),
        },
        "observable_oracle_summaries": {
            "observable": {
                "feature_similarity": dict(similarity_summary),
                "relation_summary": "benign_support_events_only",
            },
            "oracle": {
                "fraud_records": len(true_records),
                "campaign_memberships": len(memberships),
                "induced_topology_preserved": True,
            },
        },
    }
    return (
        updated_payments,
        updated_events,
        updated_ledger,
        updated_records,
        updated_memberships,
        campaigns,
        patterns,
        updated_evidence,
        metadata,
    )


__all__ = [
    "CamouflagePlan",
    "ResolvedCamouflage",
    "apply_camouflage",
    "resolve_camouflage",
    "transform_generated_data",
]
