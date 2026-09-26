"""Deterministic, opt-in campaign evolution."""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from random import Random
from typing import TYPE_CHECKING, Any, Protocol, cast

from fraudtwin.config import CampaignDynamicsBinding, SimulationRunConfig
from fraudtwin.domain import (
    Account,
    CampaignActorMembershipChange,
    CampaignIntensityDecision,
    CampaignLineage,
    CampaignPhaseChange,
    CampaignSourceSnapshot,
    CampaignStateSnapshot,
    CampaignTopologyMutation,
    CampaignTransition,
    Card,
    Device,
    FraudRecord,
    GraphCampaign,
    GraphCampaignMembership,
    GraphHyperedge,
    GraphHyperedgeMembership,
    Merchant,
    Payment,
    PaymentEvent,
    PixKey,
)
from fraudtwin.domain.campaign_dynamics import CampaignPhaseName
from fraudtwin.reproducibility import sha256_json
from fraudtwin.seed import create_stream_rng
from fraudtwin.simulation.graph_fraud import SCENARIO_CODES, GraphFraudDataset
from fraudtwin.simulation.payments import PaymentGenerator, payment_event_times

if TYPE_CHECKING:
    from fraudtwin.simulation.generator import EntityDataset


@dataclass(frozen=True)
class DynamicCampaignDataset:
    """Dynamic records and the complete post-evolution graph dataset."""

    graph: GraphFraudDataset
    snapshots: tuple[CampaignStateSnapshot, ...] = ()
    transitions: tuple[CampaignTransition, ...] = ()
    phase_changes: tuple[CampaignPhaseChange, ...] = ()
    membership_changes: tuple[CampaignActorMembershipChange, ...] = ()
    intensity_decisions: tuple[CampaignIntensityDecision, ...] = ()
    topology_mutations: tuple[CampaignTopologyMutation, ...] = ()
    lineage: tuple[CampaignLineage, ...] = ()
    source_snapshots: tuple[CampaignSourceSnapshot, ...] = ()
    stream_ids: tuple[str, ...] = ()
    configuration_hash: str = ""

    @property
    def active(self) -> bool:
        return bool(self.transitions or self.snapshots)


class TransitionModel(Protocol):
    name: str

    def choose(
        self,
        binding: CampaignDynamicsBinding,
        phase: str,
        decision_at: datetime,
        rng: Random,
    ) -> tuple[str, str]: ...


class IntensityModel(Protocol):
    name: str

    def decide(
        self,
        binding: CampaignDynamicsBinding,
        phase: str,
        decision_at: datetime,
        previous_events: int,
        rng: Random,
    ) -> tuple[float, float]: ...


class _PhaseTransition:
    name = "phase_transition_v1"

    def choose(
        self, binding: CampaignDynamicsBinding, phase: str, decision_at: datetime, rng: Random
    ) -> tuple[str, str]:
        options = binding.transition_probabilities.get(cast(Any, phase), {})
        if not options:
            return "closed", "phase has no configured transition"
        destinations = tuple(sorted(options))
        destination = rng.choices(
            destinations, weights=tuple(options[item] for item in destinations), k=1
        )[0]
        return destination, f"{phase}->{destination} selected by {self.name}"


class _MarkedHawkes:
    name = "marked_hawkes_v1"

    def decide(
        self,
        binding: CampaignDynamicsBinding,
        phase: str,
        decision_at: datetime,
        previous_events: int,
        rng: Random,
    ) -> tuple[float, float]:
        mark = binding.phase_marks.get(cast(Any, phase), 1.0)
        rate = binding.hawkes_baseline + binding.hawkes_excitation * (
            1.0 - 2.718281828 ** (-binding.hawkes_decay * previous_events)
        )
        return max(0.0, rate * mark), mark


class _PiecewiseRate:
    name = "piecewise_rate_v1"

    def decide(
        self,
        binding: CampaignDynamicsBinding,
        phase: str,
        decision_at: datetime,
        previous_events: int,
        rng: Random,
    ) -> tuple[float, float]:
        mark = binding.phase_marks.get(cast(Any, phase), 1.0)
        return max(0.0, binding.hawkes_baseline * mark), mark


_TRANSITION_MODELS: dict[str, TransitionModel] = {_PhaseTransition.name: _PhaseTransition()}
_INTENSITY_MODELS: dict[str, IntensityModel] = {
    _MarkedHawkes.name: _MarkedHawkes(),
    _PiecewiseRate.name: _PiecewiseRate(),
}


def register_transition_model(name: str, model: TransitionModel) -> None:
    """Register a campaign phase-transition model by stable name.

    Args:
        name: Non-empty name referenced by campaign-dynamics configuration.
        model: Callable protocol implementation that chooses the next phase.

    Raises:
        ValueError: If ``name`` is empty.
    """

    if not name.strip():
        raise ValueError("transition model name must not be empty")
    _TRANSITION_MODELS[name] = model


def register_intensity_model(name: str, model: IntensityModel) -> None:
    """Register a campaign intensity model by stable name.

    Args:
        name: Non-empty name referenced by campaign-dynamics configuration.
        model: Callable protocol implementation that computes event intensity.

    Raises:
        ValueError: If ``name`` is empty.
    """

    if not name.strip():
        raise ValueError("intensity model name must not be empty")
    _INTENSITY_MODELS[name] = model


def resolve_campaign_dynamics(config: SimulationRunConfig) -> tuple[CampaignDynamicsBinding, ...]:
    """Validate and resolve registered models without consuming randomness."""

    settings = config.campaign_dynamics
    if not settings.active:
        return ()
    for binding in settings.bindings:
        if binding.transition_model not in _TRANSITION_MODELS:
            raise ValueError(f"unsupported transition model: {binding.transition_model}")
        if binding.intensity_model not in _INTENSITY_MODELS:
            raise ValueError(f"unsupported intensity model: {binding.intensity_model}")
    return settings.bindings


def _binding_for_campaign(
    bindings: tuple[CampaignDynamicsBinding, ...], campaign: GraphCampaign
) -> CampaignDynamicsBinding | None:
    for binding in bindings:
        template = binding.template or {
            "linear": "MULE_NETWORK",
            "rotating_ring": "CYCLIC_RING",
            "adaptive_network": "DENSE_CAMPAIGN",
        }.get(binding.profile)
        if template == campaign.scenario_type:
            return binding
    return None


def _derived(prefix: str, material: object) -> str:
    return f"M15-{prefix}-{sha256_json(material)[:20]}"


def _initial_event(
    payment: Payment,
    customer_id: str,
    device_id: str | None,
    event_type: str,
    run_id: str,
    scenario_id: str,
    when: datetime,
) -> PaymentEvent:
    available, ingested, processed = payment_event_times(when, payment.payment_rail)
    return PaymentEvent(
        event_id=_derived("EVT", (payment.payment_id, event_type)),
        event_type=cast(Any, event_type),
        event_version=1,
        payment_id=payment.payment_id,
        customer_id=customer_id,
        account_id=payment.payer_account_id,
        event_time=when,
        source_created_at=when,
        source_available_at=available,
        ingested_at=ingested,
        processed_at=processed,
        producer="fraudtwin.campaign_dynamics",
        source_system="synthetic_payment_source",
        schema_version="5",
        correlation_id=payment.payment_id,
        causation_id=None,
        simulation_run_id=run_id,
        scenario_id=scenario_id,
        payment_rail=payment.payment_rail,
        payment_type=payment.payment_type,
        payee_account_id=payment.payee_account_id,
        merchant_id=payment.merchant_id,
        card_id=payment.card_id,
        device_id=device_id,
        online=payment.payment_rail == "CARD",
        amount=payment.amount,
        currency=payment.currency,
        scenario_type="M15_DYNAMIC",
        scenario_trigger="CAMPAIGN_DYNAMICS",
        scenario_reason="deterministic M15 campaign action",
        fraud_record_id=_derived("FRD", payment.payment_id),
        affected_entity_ids=(
            payment.payer_account_id,
            *(x for x in (payment.payee_account_id,) if x),
        ),
    )


def _campaign_actions(
    config: SimulationRunConfig,
    accounts: tuple[Account, ...],
    cards: tuple[Card, ...],
    merchants: tuple[Merchant, ...],
    devices: tuple[Device, ...],
    pix_keys: tuple[PixKey, ...],
    campaign: GraphCampaign,
    binding: CampaignDynamicsBinding,
    run_id: str,
    source: GraphFraudDataset,
) -> tuple[GraphFraudDataset, DynamicCampaignDataset]:
    start = config.simulation.start.astimezone(UTC)
    end = start + timedelta(days=config.simulation.duration_days)
    transition_rng = create_stream_rng(
        config.simulation.seed, f"milestone-15:campaign:{campaign.campaign_id}:transition"
    )
    intensity_rng = create_stream_rng(
        config.simulation.seed, f"milestone-15:campaign:{campaign.campaign_id}:intensity"
    )
    actor_rng = create_stream_rng(
        config.simulation.seed, f"milestone-15:campaign:{campaign.campaign_id}:actor-selection"
    )
    stream_ids = (
        f"milestone-15:campaign:{campaign.campaign_id}:transition",
        f"milestone-15:campaign:{campaign.campaign_id}:intensity",
        f"milestone-15:campaign:{campaign.campaign_id}:actor-selection",
    )
    model = _TRANSITION_MODELS[binding.transition_model]
    intensity = _INTENSITY_MODELS[binding.intensity_model]
    members = sorted(
        {item.member_id for item in source.memberships if item.campaign_id == campaign.campaign_id}
    )
    accounts_by_id = {item.account_id: item for item in accounts}
    member_accounts = [accounts_by_id[item] for item in members if item in accounts_by_id]
    if len(member_accounts) < 2:
        raise ValueError(f"campaign {campaign.campaign_id} has insufficient account members")
    snapshots: list[CampaignStateSnapshot] = []
    transitions: list[CampaignTransition] = []
    phases: list[CampaignPhaseChange] = []
    changes: list[CampaignActorMembershipChange] = []
    decisions: list[CampaignIntensityDecision] = []
    mutations: list[CampaignTopologyMutation] = []
    lineage: list[CampaignLineage] = []
    source_snapshots = [
        CampaignSourceSnapshot(
            source_snapshot_id=_derived("SRC", campaign.campaign_id),
            campaign_id=campaign.campaign_id,
            source_run_id=run_id,
            captured_at=campaign.valid_from,
            source_campaign_ids=(campaign.campaign_id,),
            source_entity_ids=tuple(members),
            source_event_ids=tuple(
                sorted(
                    {
                        item.source_event_id
                        for item in source.memberships
                        if item.campaign_id == campaign.campaign_id and item.source_event_id
                    }
                )
            ),
            source_payment_ids=tuple(
                sorted(
                    {
                        item.payment_id
                        for item in source.memberships
                        if item.campaign_id == campaign.campaign_id and item.payment_id
                    }
                )
            ),
            configuration_hash=sha256_json(config.model_dump(mode="json")),
            schema_fingerprint=sha256_json(
                {
                    "version": "m15-1",
                    "models": ["CampaignStateSnapshot", "CampaignTransition"],
                }
            ),
        )
    ]
    current = "compromise"
    current_time = max(start, campaign.valid_from)
    current_device = devices[0].device_id if devices else None
    for ordinal in range(binding.max_actions):
        if current == "closed" or current_time >= end:
            break
        rate, mark = intensity.decide(binding, current, current_time, ordinal, intensity_rng)
        decisions.append(
            CampaignIntensityDecision(
                intensity_decision_id=_derived("INT", (campaign.campaign_id, ordinal)),
                campaign_id=campaign.campaign_id,
                phase=cast(CampaignPhaseName, current),
                decision_at=current_time,
                event_rate=rate,
                mark=mark,
                model_name=intensity.name,
                stream_id=stream_ids[1],
            )
        )
        snapshot_id = _derived("SNP", (campaign.campaign_id, ordinal, current))
        snapshots.append(
            CampaignStateSnapshot(
                snapshot_id=snapshot_id,
                campaign_id=campaign.campaign_id,
                phase=cast(CampaignPhaseName, current),
                intensity=rate,
                active_actor_ids=tuple(members),
                active_device_ids=(current_device,) if current_device else (),
                valid_from=current_time,
            )
        )
        next_phase, reason = model.choose(binding, current, current_time, transition_rng)
        transition_id = _derived("TRN", (campaign.campaign_id, ordinal, current, next_phase))
        transitions.append(
            CampaignTransition(
                transition_id=transition_id,
                campaign_id=campaign.campaign_id,
                from_phase=cast(CampaignPhaseName, current),
                to_phase=cast(CampaignPhaseName, next_phase),
                occurred_at=current_time,
                reason=reason,
                model_name=model.name,
                source_snapshot_id=snapshot_id,
                stream_id=stream_ids[0],
            )
        )
        phases.append(
            CampaignPhaseChange(
                phase_change_id=_derived("PHS", transition_id),
                campaign_id=campaign.campaign_id,
                transition_id=transition_id,
                phase=cast(CampaignPhaseName, next_phase),
                valid_from=current_time,
            )
        )
        current = next_phase
        duration = binding.phase_durations.get(cast(CampaignPhaseName, current), 1)
        current_time = min(end, current_time + timedelta(seconds=duration))
        if (
            current != "closed"
            and ordinal == 0
            and binding.max_actor_joins
            and len(members) < binding.max_active_members
        ):
            available = [item for item in accounts if item.account_id not in members]
            if available:
                actor = available[actor_rng.randrange(len(available))]
                members.append(actor.account_id)
                changes.append(
                    CampaignActorMembershipChange(
                        membership_change_id=_derived(
                            "ACT", (campaign.campaign_id, actor.account_id, "JOIN")
                        ),
                        campaign_id=campaign.campaign_id,
                        actor_id=actor.account_id,
                        actor_type="ACCOUNT",
                        role="ROTATED_MEMBER",
                        action="JOIN",
                        occurred_at=current_time,
                        valid_from=current_time,
                    )
                )
        if current != "closed" and ordinal == 1 and binding.max_actor_leaves and len(members) > 2:
            actor_id = sorted(members)[-1]
            members.remove(actor_id)
            changes.append(
                CampaignActorMembershipChange(
                    membership_change_id=_derived("ACT", (campaign.campaign_id, actor_id, "LEAVE")),
                    campaign_id=campaign.campaign_id,
                    actor_id=actor_id,
                    actor_type="ACCOUNT",
                    role="ROTATED_MEMBER",
                    action="LEAVE",
                    occurred_at=current_time,
                    valid_from=current_time,
                    valid_to=current_time,
                )
            )
        if current != "closed" and ordinal == 2 and binding.max_mule_rotations:
            available = [item for item in accounts if item.account_id not in members]
            if available:
                outgoing = sorted(members)[0]
                incoming = available[actor_rng.randrange(len(available))]
                members.remove(outgoing)
                members.append(incoming.account_id)
                mutations.append(
                    CampaignTopologyMutation(
                        topology_mutation_id=_derived(
                            "TOP", (campaign.campaign_id, "MULE_ROTATION")
                        ),
                        campaign_id=campaign.campaign_id,
                        mutation_type="MULE_ROTATION",
                        occurred_at=current_time,
                        source_member_ids=(outgoing,),
                        derived_member_ids=(incoming.account_id,),
                        reason="deterministic mule rotation",
                    )
                )
        if current != "closed" and ordinal == 3 and binding.max_device_rotations and devices:
            current_device = devices[(ordinal // 3) % len(devices)].device_id
            mutations.append(
                CampaignTopologyMutation(
                    topology_mutation_id=_derived("TOP", (campaign.campaign_id, "DEVICE_ROTATION")),
                    campaign_id=campaign.campaign_id,
                    mutation_type="DEVICE_ROTATION",
                    occurred_at=current_time,
                    derived_member_ids=(current_device,),
                    reason="deterministic device rotation",
                )
            )
    # Materialize a small, valid set of campaign actions after the state machine.
    payments = list(source.payments)
    events = list(source.payment_events)
    records = list(source.fraud_records)
    memberships = list(source.memberships)
    card_by_account = {item.account_id: item for item in cards}
    for index, when in enumerate(sorted({item.occurred_at for item in transitions}), 1):
        payer = accounts_by_id[members[(index - 1) % len(members)]]
        payee = accounts_by_id[members[index % len(members)]]
        rail = binding.allowed_rails[(index - 1) % len(binding.allowed_rails)]
        card = card_by_account.get(payer.account_id) if rail == "CARD" else None
        merchant = merchants[(index - 1) % len(merchants)] if rail == "CARD" and merchants else None
        key = next((item for item in pix_keys if item.account_id == payee.account_id), None)
        if rail == "CARD" and (card is None or merchant is None):
            continue
        if rail == "PIX" and key is None:
            continue
        pid = _derived("PAY", (campaign.campaign_id, index, rail))
        payment = Payment(
            payment_id=pid,
            payment_rail=rail,
            payment_type="PURCHASE" if rail == "CARD" else "TRANSFER",
            payer_account_id=payer.account_id,
            payee_account_id=None if rail == "CARD" else payee.account_id,
            merchant_id=merchant.merchant_id if merchant is not None else None,
            card_id=card.card_id if card is not None else None,
            amount=1.0,
            currency=payer.currency,
            initiated_at=when,
            current_status="COMPLETED"
            if rail == "CARD"
            else ("RECEIVED" if rail == "PIX" else "COMPLETED"),
            payer_institution_id=payer.institution_id,
            payee_institution_id=payee.institution_id,
            payee_pix_key_id=key.pix_key_id if rail == "PIX" and key else None,
        )
        initial_type = (
            "CARD_PAYMENT_INITIATED"
            if rail == "CARD"
            else ("PIX_INITIATED" if rail == "PIX" else "TRANSFER_COMPLETED")
        )
        event = _initial_event(
            payment,
            payer.customer_id,
            current_device,
            initial_type,
            run_id,
            campaign.campaign_id,
            when,
        )
        generator = PaymentGenerator(
            config, accounts, cards, merchants, (), pix_keys, simulation_run_id=run_id
        )
        if rail == "CARD":
            payment, lifecycle = generator._card_lifecycle(payment, event, intensity_rng)
        elif rail == "PIX":
            payment, lifecycle = generator._pix_lifecycle(
                payment, event, intensity_rng, always_approve=True
            )
        else:
            lifecycle = (event,)
        payments.append(payment)
        events.extend(lifecycle)
        records.append(
            FraudRecord(
                fraud_record_id=event.fraud_record_id or _derived("FRD", pid),
                record_type="FRAUD",
                scenario_id=campaign.campaign_id,
                scenario_type=SCENARIO_CODES.get(campaign.scenario_type, "G11"),
                fraud_truth=True,
                trigger="CAMPAIGN_DYNAMICS",
                reason="dynamic campaign action",
                customer_id=payer.customer_id,
                account_id=payer.account_id,
                card_id=payment.card_id,
                device_id=None,
                merchant_id=payment.merchant_id,
                payment_id=pid,
                event_id=event.event_id,
                occurred_at=when,
                amount=payment.amount,
                currency=payment.currency,
                correlation_id=pid,
                causation_id=None,
                affected_entity_ids=event.affected_entity_ids,
            )
        )
        for account, role in ((payer, "SOURCE"), (payee, "DESTINATION")):
            memberships.append(
                GraphCampaignMembership(
                    campaign_id=campaign.campaign_id,
                    pattern_type=campaign.scenario_type,
                    member_id=account.account_id,
                    member_type="ACCOUNT",
                    role=role,
                    valid_from=when,
                    source_event_id=event.event_id,
                    payment_id=pid,
                )
            )
        mutations.append(
            CampaignTopologyMutation(
                topology_mutation_id=_derived("TOP", (campaign.campaign_id, "CROSS_RAIL", index)),
                campaign_id=campaign.campaign_id,
                mutation_type="CROSS_RAIL",
                occurred_at=when,
                source_member_ids=(payer.account_id, payee.account_id),
                derived_payment_ids=(pid,),
                reason=f"campaign action used {rail}",
            )
        )
    if binding.max_splits and campaign.scenario_type == "DENSE_CAMPAIGN" and len(members) >= 4:
        mutations.append(
            CampaignTopologyMutation(
                topology_mutation_id=_derived("TOP", (campaign.campaign_id, "RING_SPLIT")),
                campaign_id=campaign.campaign_id,
                mutation_type="RING_SPLIT",
                occurred_at=end,
                source_member_ids=tuple(sorted(members)),
                derived_member_ids=tuple(sorted(members[:2])),
                reason="deterministic campaign split",
            )
        )
    if binding.max_merges and campaign.scenario_type == "DENSE_CAMPAIGN":
        mutations.append(
            CampaignTopologyMutation(
                topology_mutation_id=_derived("TOP", (campaign.campaign_id, "RING_MERGE")),
                campaign_id=campaign.campaign_id,
                mutation_type="RING_MERGE",
                occurred_at=end,
                source_member_ids=tuple(sorted(members)),
                derived_member_ids=tuple(sorted(members)),
                reason="deterministic campaign merge",
            )
        )
    all_events = tuple(sorted(events, key=lambda item: (item.event_time, item.event_id)))
    ledger = PaymentGenerator(
        config,
        accounts,
        cards,
        merchants,
        (),
        pix_keys,
        simulation_run_id=run_id,
    ).materialize_ledger(
        tuple(payments),
        all_events,
        stage="campaign dynamics ledger",
        protocol_id="P04",
        capacity_id="C04",
    )
    campaign_update = campaign.model_copy(
        update={"valid_to": end, "participant_ids": tuple(sorted(set(members)))}
    )
    campaigns = tuple(
        campaign_update if item.campaign_id == campaign.campaign_id else item
        for item in source.campaigns
    )
    pattern_update = tuple(
        item.model_copy(
            update={
                "payment_ids": tuple(
                    sorted(
                        {
                            *item.payment_ids,
                            *[p.payment_id for p in payments if p.payment_id.startswith("M15-")],
                        }
                    )
                ),
                "window_to": end,
            }
        )
        if item.campaign_id == campaign.campaign_id
        else item
        for item in source.patterns
    )
    hyperedges = list(source.hyperedges)
    hyperedge_memberships = list(source.hyperedge_memberships)
    if campaign.scenario_type == "DENSE_CAMPAIGN":
        hyperedge_id = _derived("HYP", campaign.campaign_id)
        hyperedges.append(
            GraphHyperedge(
                hyperedge_id=hyperedge_id,
                hyperedge_type="STRUCTURAL",
                campaign_id=campaign.campaign_id,
                pattern_id=None,
                valid_from=start,
                valid_to=end,
                source_event_ids=tuple(
                    item.event_id for item in events if item.event_id.startswith("M15-")
                ),
                payment_ids=tuple(
                    item.payment_id for item in payments if item.payment_id.startswith("M15-")
                ),
            )
        )
        hyperedge_memberships.extend(
            GraphHyperedgeMembership(
                hyperedge_id=hyperedge_id,
                member_id=item,
                member_type="ACCOUNT",
                role="MEMBER",
            )
            for item in sorted(members)
        )
    result_graph = GraphFraudDataset(
        tuple(payments),
        all_events,
        ledger,
        tuple(records),
        tuple(memberships),
        pattern_update,
        campaigns,
        source.evidence,
        tuple(hyperedges),
        tuple(hyperedge_memberships),
    )
    lineage.append(
        CampaignLineage(
            lineage_id=_derived("LIN", campaign.campaign_id),
            campaign_id=campaign.campaign_id,
            source_entity_ids=tuple(sorted(set(members))),
            source_event_ids=source_snapshots[0].source_event_ids,
            source_payment_ids=source_snapshots[0].source_payment_ids,
            derived_event_ids=tuple(
                item.event_id for item in all_events if item.event_id.startswith("M15-")
            ),
            derived_payment_ids=tuple(
                item.payment_id for item in payments if item.payment_id.startswith("M15-")
            ),
            derived_at=start,
            reason="deterministic campaign evolution",
        )
    )
    dynamic = DynamicCampaignDataset(
        result_graph,
        tuple(snapshots),
        tuple(transitions),
        tuple(phases),
        tuple(changes),
        tuple(decisions),
        tuple(mutations),
        tuple(lineage),
        tuple(source_snapshots),
        stream_ids,
        sha256_json(config.model_dump(mode="json")),
    )
    return result_graph, dynamic


def evolve_campaigns(
    config: SimulationRunConfig,
    entities: "EntityDataset",
    graph_dataset: GraphFraudDataset,
    run_id: str,
) -> DynamicCampaignDataset:
    """Evolve all configured M11 campaigns in stable order."""

    bindings = resolve_campaign_dynamics(config)
    if not bindings:
        return DynamicCampaignDataset(graph_dataset)
    entity_accounts = entities.accounts
    entity_cards = entities.cards
    entity_merchants = entities.merchants
    entity_pix_keys = entities.pix_keys
    current = graph_dataset
    combined = DynamicCampaignDataset(current)
    for campaign in sorted(current.campaigns, key=lambda item: item.campaign_id):
        binding = _binding_for_campaign(bindings, campaign)
        if binding is None:
            continue
        current, item = _campaign_actions(
            config,
            entity_accounts,
            entity_cards,
            entity_merchants,
            entities.devices,
            entity_pix_keys,
            campaign,
            binding,
            run_id,
            current,
        )
        combined = DynamicCampaignDataset(
            current,
            combined.snapshots + item.snapshots,
            combined.transitions + item.transitions,
            combined.phase_changes + item.phase_changes,
            combined.membership_changes + item.membership_changes,
            combined.intensity_decisions + item.intensity_decisions,
            combined.topology_mutations + item.topology_mutations,
            combined.lineage + item.lineage,
            combined.source_snapshots + item.source_snapshots,
            combined.stream_ids + item.stream_ids,
            item.configuration_hash,
        )
    validate_campaign_dynamics(config, entities, combined)
    return combined


def validate_campaign_dynamics(
    config: SimulationRunConfig, entities: "EntityDataset", dataset: DynamicCampaignDataset
) -> None:
    """Validate dynamic IDs, lifecycle records, graph membership, and ledger closure."""

    if not dataset.active:
        return
    if len({item.snapshot_id for item in dataset.snapshots}) != len(dataset.snapshots):
        raise ValueError("dynamic campaign snapshots contain duplicate IDs")
    payment_by_id = {item.payment_id: item for item in dataset.graph.payments}
    events_by_payment: dict[str, list[PaymentEvent]] = {}
    for event in dataset.graph.payment_events:
        events_by_payment.setdefault(event.payment_id, []).append(event)
    for payment in dataset.graph.payments:
        if payment.payment_id.startswith("M15-"):
            from fraudtwin.domain import validate_payment_lifecycle

            validate_payment_lifecycle(
                payment,
                tuple(
                    sorted(
                        events_by_payment.get(payment.payment_id, ()),
                        key=lambda item: item.event_time,
                    )
                ),
            )
    account_ids = {item.account_id for item in entities.accounts}
    if any(
        item.member_id not in account_ids
        for item in dataset.graph.memberships
        if item.member_type == "ACCOUNT"
    ):
        raise ValueError("dynamic membership references an unknown account")
    if any(
        item.derived_payment_ids
        and any(pid not in payment_by_id for pid in item.derived_payment_ids)
        for item in dataset.lineage
    ):
        raise ValueError("dynamic lineage references an unknown derived payment")


__all__ = [
    "DynamicCampaignDataset",
    "evolve_campaigns",
    "register_intensity_model",
    "register_transition_model",
    "resolve_campaign_dynamics",
    "validate_campaign_dynamics",
]
