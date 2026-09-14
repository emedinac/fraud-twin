"""Deterministic, opt-in M11 scenario generation over existing entities."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, timedelta
from typing import TYPE_CHECKING

from fraudtwin.config import GraphScenarioConfig, SimulationRunConfig
from fraudtwin.difficulty import apply_difficulty, resolve_difficulty
from fraudtwin.domain import (
    PAYMENT_EVENT_CONTRACT_VERSION,
    Account,
    Device,
    FraudRecord,
    GraphCampaign,
    GraphCampaignMembership,
    GraphEvidence,
    GraphHyperedge,
    GraphHyperedgeMembership,
    GraphPattern,
    LedgerEntry,
    Merchant,
    NetworkEndpoint,
    Payment,
    PaymentEvent,
    PixKey,
)
from fraudtwin.simulation.fraud import FraudDataset
from fraudtwin.simulation.graph_planner import GraphScenarioPlanner, PlannedScenario
from fraudtwin.simulation.payments import PaymentDataset, PaymentGenerator

if TYPE_CHECKING:
    from fraudtwin.campaign_dynamics import DynamicCampaignDataset

SCENARIO_CODES = {
    "MULE_NETWORK": "G01",
    "CYCLIC_RING": "G02",
    "BENEFICIARY_NETWORK": "G03",
    "FAN_OUT": "G04",
    "BIPARTITE_NETWORK": "G05",
    "STACKED_NETWORK": "G06",
    "SCATTER_GATHER": "G07",
    "GATHER_SCATTER": "G08",
    "SHARED_DEVICE_INFRASTRUCTURE": "G09",
    "SHARED_IP_INFRASTRUCTURE": "G10",
    "DENSE_CAMPAIGN": "G11",
    "MERCHANT_CUSTOMER_COMMUNITY": "G12",
    "RANDOM_ALERT_CONTROL": "G13",
}


@dataclass(frozen=True)
class GraphFraudDataset:
    payments: tuple[Payment, ...]
    payment_events: tuple[PaymentEvent, ...]
    ledger_entries: tuple[LedgerEntry, ...]
    fraud_records: tuple[FraudRecord, ...]
    memberships: tuple[GraphCampaignMembership, ...]
    patterns: tuple[GraphPattern, ...]
    campaigns: tuple[GraphCampaign, ...] = ()
    evidence: tuple[GraphEvidence, ...] = ()
    hyperedges: tuple[GraphHyperedge, ...] = ()
    hyperedge_memberships: tuple[GraphHyperedgeMembership, ...] = ()
    dynamic: DynamicCampaignDataset | None = None


class GraphFraudGenerator:
    """Generate graph scenarios using canonical participant allocation."""

    def __init__(
        self,
        config: SimulationRunConfig,
        accounts: tuple[Account, ...],
        devices: tuple[Device, ...],
        endpoints: tuple[NetworkEndpoint, ...],
        baseline: PaymentDataset | FraudDataset,
        simulation_run_id: str | None = None,
        merchants: tuple[Merchant, ...] = (),
        pix_keys: tuple[PixKey, ...] = (),
    ) -> None:
        self.config = config
        self.accounts = tuple(sorted(accounts, key=lambda x: x.account_id))
        self.devices = tuple(sorted(devices, key=lambda x: x.device_id))
        self.endpoints = tuple(sorted(endpoints, key=lambda x: x.endpoint_id))
        self.merchants = tuple(sorted(merchants, key=lambda x: x.merchant_id))
        self.pix_keys = tuple(sorted(pix_keys, key=lambda x: x.pix_key_id))
        self.baseline = baseline
        self.run_id = simulation_run_id or "in-memory"
        self.start = config.simulation.start.astimezone(UTC)
        self.end = self.start + timedelta(days=config.simulation.duration_days)
        self._difficulty = resolve_difficulty(config)
        self._active_plan = apply_difficulty(self._difficulty, "MULE_NETWORK")

    def generate(self) -> GraphFraudDataset:
        if not self.config.graph.enabled:
            return GraphFraudDataset(
                self.baseline.payments,
                self.baseline.payment_events,
                self.baseline.ledger_entries,
                (),
                (),
                (),
            )
        payments = list(self.baseline.payments)
        events = list(self.baseline.payment_events)
        records: list[FraudRecord] = []
        memberships: list[GraphCampaignMembership] = []
        patterns: list[GraphPattern] = []
        campaigns: list[GraphCampaign] = []
        evidence: list[GraphEvidence] = []
        spent: dict[str, float] = {}
        hyperedges: list[GraphHyperedge] = []
        hyper_members: list[GraphHyperedgeMembership] = []
        plans = GraphScenarioPlanner(
            self.config, self.accounts, self.merchants, self.devices, self.endpoints, self.pix_keys
        ).plan()
        for plan in plans:
            scenario = plan.scenario
            self._active_plan = apply_difficulty(self._difficulty, scenario.type)
            campaign = f"G-{scenario.type}-{plan.ordinal:04d}"
            pairs = self._pairs(scenario, plan.accounts)
            generated = self._materialize(scenario, campaign, pairs, plan, spent)
            payments.extend(generated[0])
            events.extend(generated[1])
            records.extend(generated[2])
            memberships.extend(generated[3])
            evidence.extend(generated[4])
            member_ids = tuple(sorted({m.member_id for m in generated[3]}))
            event_ids = tuple(sorted(e.event_id for e in generated[1]))
            payment_ids = tuple(sorted(p.payment_id for p in generated[0]))
            pattern = GraphPattern(
                pattern_id=f"PAT-{campaign}",
                pattern_type=scenario.type,
                campaign_id=campaign,
                detected_at=max((e.event_time for e in generated[1]), default=self.start),
                window_from=min((e.event_time for e in generated[1]), default=self.start),
                window_to=max((e.event_time for e in generated[1]), default=self.start)
                + timedelta(microseconds=1),
                member_ids=member_ids,
                source_event_ids=event_ids,
                payment_ids=payment_ids,
                threshold=scenario.member_count or scenario.source_count or 1,
                observed_value=float(len(generated[0])),
                truth_label="CONTROL" if scenario.type == "RANDOM_ALERT_CONTROL" else "FRAUD",
                scenario_code=SCENARIO_CODES[scenario.type],
            )
            patterns.append(pattern)
            campaigns.append(
                GraphCampaign(
                    campaign_id=campaign,
                    scenario_type=scenario.type,
                    scenario_code=SCENARIO_CODES[scenario.type],
                    truth_label="CONTROL" if scenario.type == "RANDOM_ALERT_CONTROL" else "FRAUD",
                    valid_from=pattern.window_from,
                    valid_to=pattern.window_to,
                    participant_ids=member_ids,
                    modifiers=tuple(scenario.modifiers),
                )
            )
            for modifier in scenario.modifiers:
                if modifier in {"STRUCTURAL_HYPEREDGE", "SEMANTIC_HYPEREDGE"}:
                    hid = f"HYP-{campaign}-{modifier}"
                    hyperedges.append(
                        GraphHyperedge(
                            hyperedge_id=hid,
                            hyperedge_type="STRUCTURAL"
                            if modifier == "STRUCTURAL_HYPEREDGE"
                            else "SEMANTIC",
                            campaign_id=campaign,
                            pattern_id=pattern.pattern_id,
                            valid_from=pattern.window_from,
                            valid_to=pattern.window_to,
                            source_event_ids=event_ids,
                            payment_ids=payment_ids,
                        )
                    )
                    hyper_members.extend(
                        GraphHyperedgeMembership(
                            hyperedge_id=hid, member_id=m, member_type="ACCOUNT", role="MEMBER"
                        )
                        for m in member_ids
                    )
        ledger = PaymentGenerator(
            self.config, self.accounts, (), (), self.devices, (), simulation_run_id=self.run_id
        ).materialize_ledger(tuple(payments), tuple(events))
        return GraphFraudDataset(
            tuple(payments),
            tuple(events),
            ledger,
            tuple(records),
            tuple(memberships),
            tuple(patterns),
            tuple(campaigns),
            tuple(evidence),
            tuple(hyperedges),
            tuple(hyper_members),
        )

    def _accounts(self, accounts: tuple[Account, ...], count: int) -> list[Account]:
        selected = list(accounts[:count])
        if len(selected) < count:
            raise ValueError(f"graph scenario requires {count} disjoint accounts")
        return selected

    def _pairs(
        self, s: GraphScenarioConfig, accounts: tuple[Account, ...]
    ) -> list[tuple[Account, Account]]:
        typ = s.type
        if typ == "CYCLIC_RING":
            a = self._accounts(accounts, s.member_count or 3)
            return list(zip(a, a[1:] + a[:1], strict=True))
        if typ == "MULE_NETWORK":
            a = self._accounts(accounts, (s.source_count or 1) + 2)
            return [(x, a[-2]) for x in a[:-2]] + [(a[-2], a[-1])]
        if typ == "BENEFICIARY_NETWORK":
            a = self._accounts(accounts, (s.source_count or 1) + 1)
            return [(x, a[-1]) for x in a[:-1]]
        if typ == "FAN_OUT":
            a = self._accounts(accounts, (s.destination_count or 1) + 1)
            return [(a[0], x) for x in a[1:]]
        if typ == "BIPARTITE_NETWORK":
            n, m = s.originator_count or 1, s.beneficiary_count or 1
            a = self._accounts(accounts, n + m)
            return [(x, y) for x in a[:n] for y in a[n:]]
        if typ == "STACKED_NETWORK":
            n, i, b = s.originator_count or 1, s.intermediary_count or 1, s.beneficiary_count or 1
            a = self._accounts(accounts, n + i + b)
            return [(x, y) for x in a[:n] for y in a[n : n + i]] + [
                (x, y) for x in a[n : n + i] for y in a[n + i :]
            ]
        if typ == "SCATTER_GATHER":
            a = self._accounts(accounts, (s.intermediary_count or 1) + 2)
            return [(a[0], x) for x in a[1:-1]] + [(x, a[-1]) for x in a[1:-1]]
        if typ == "GATHER_SCATTER":
            n, d = s.source_count or 1, s.destination_count or 1
            a = self._accounts(accounts, n + d + 1)
            return [(x, a[n]) for x in a[:n]] + [(a[n], x) for x in a[n + 1 :]]
        if typ == "DENSE_CAMPAIGN":
            a = self._accounts(accounts, s.member_count or 2)
            return [(a[i], a[j]) for i in range(len(a)) for j in range(len(a)) if i != j][
                : s.edge_count or max(1, len(a))
            ]
        if typ == "RANDOM_ALERT_CONTROL":
            a = self._accounts(accounts, s.member_count or 2)
            return [(a[i % len(a)], a[(i + 1) % len(a)]) for i in range(s.edge_count or 1)]
        if typ == "MERCHANT_CUSTOMER_COMMUNITY":
            a = self._accounts(accounts, s.customer_count or 1)
            merchants = max(1, s.merchant_count or 1)
            repeats = max(1, s.repeat_count or 1)
            return [
                (a[i % len(a)], a[(i + 1) % len(a)]) for i in range(len(a) * merchants * repeats)
            ]
        if typ in {"SHARED_DEVICE_INFRASTRUCTURE", "SHARED_IP_INFRASTRUCTURE"}:
            a = self._accounts(accounts, s.member_count or 2)
            return [(x, a[(i + 1) % len(a)]) for i, x in enumerate(a)]
        raise ValueError(f"unsupported graph scenario {typ}")

    def _materialize(
        self,
        s: GraphScenarioConfig,
        campaign: str,
        pairs: list[tuple[Account, Account]],
        plan: PlannedScenario,
        spent: dict[str, float],
    ) -> tuple[
        list[Payment],
        list[PaymentEvent],
        list[FraudRecord],
        list[GraphCampaignMembership],
        list[GraphEvidence],
    ]:
        payments: list[Payment] = []
        events: list[PaymentEvent] = []
        records: list[FraudRecord] = []
        memberships: list[GraphCampaignMembership] = []
        evidence: list[GraphEvidence] = []
        base_amount = s.min_amount or self.config.behavior.amount_min
        dwell = self.config.graph.dwell_threshold_seconds
        if self._difficulty.enabled:
            dwell = max(
                1,
                round(
                    dwell
                    * self._active_plan.timing_multiplier
                    * (1.0 + self._active_plan.graph_structural_subtlety)
                ),
            )
        step = max(1, dwell // max(1, len(pairs)))
        for index, (payer, payee) in enumerate(pairs, 1):
            jitter = 0
            if self._difficulty.enabled:
                jitter = (index % 3) * round(
                    step * 0.2 * self._active_plan.graph_structural_subtlety
                )
            when = min(
                self.start + timedelta(seconds=index * step + jitter),
                self.end - timedelta(microseconds=1),
            )
            amount = round(
                base_amount * (1 - (0.05 * (index - 1) if s.type == "CYCLIC_RING" else 0)),
                2,
            )
            if (
                self._active_plan.amount_similarity > 0 or "CAMOUFLAGE_FEATURE" in s.modifiers
            ) and self.baseline.payments:
                cohort_amounts = sorted(item.amount for item in self.baseline.payments)
                reference = cohort_amounts[len(cohort_amounts) // 2]
                amount = round(
                    (1.0 - self._active_plan.amount_similarity) * amount
                    + self._active_plan.amount_similarity * reference,
                    2,
                )
            if s.max_amount is not None:
                amount = min(amount, s.max_amount)
            if payer.available_balance < amount:
                raise ValueError(f"graph scenario source account {payer.account_id} lacks balance")
            spent[payer.account_id] = spent.get(payer.account_id, 0.0) + amount
            if spent[payer.account_id] > payer.available_balance + payer.overdraft_limit:
                raise ValueError(
                    f"graph scenario account {payer.account_id} exceeds available balance"
                )
            pid, eid = f"PAY-{campaign}-{index:05d}", f"EVT-{campaign}-{index:05d}"
            is_merchant = s.type == "MERCHANT_CUSTOMER_COMMUNITY" and bool(plan.merchants)
            merchant_id = (
                plan.merchants[(index - 1) % len(plan.merchants)].merchant_id
                if is_merchant
                else None
            )
            beneficiary_key = next(
                (key for key in plan.pix_keys if key.account_id == payee.account_id), None
            )
            payment = Payment(
                payment_id=pid,
                payment_rail="ACCOUNT_TRANSFER",
                payment_type="PURCHASE" if is_merchant else "TRANSFER",
                payer_account_id=payer.account_id,
                payee_account_id=None if is_merchant else payee.account_id,
                merchant_id=merchant_id,
                card_id=None,
                amount=amount,
                currency=payer.currency,
                initiated_at=when,
                current_status="COMPLETED",
                payer_institution_id=payer.institution_id,
                payee_institution_id=payee.institution_id,
                payee_pix_key_id=(
                    beneficiary_key.pix_key_id
                    if s.type == "BENEFICIARY_NETWORK" and beneficiary_key
                    else None
                ),
            )
            shared_device = (
                plan.devices[0].device_id
                if s.type == "SHARED_DEVICE_INFRASTRUCTURE" and plan.devices
                else None
            )
            shared_ip = (
                plan.endpoints[0].endpoint_id
                if s.type == "SHARED_IP_INFRASTRUCTURE" and plan.endpoints
                else None
            )
            event = PaymentEvent(
                event_id=eid,
                event_type="TRANSFER_COMPLETED",
                event_version=1,
                payment_id=pid,
                customer_id=payer.customer_id,
                account_id=payer.account_id,
                event_time=when,
                source_created_at=when,
                source_available_at=when + timedelta(seconds=5),
                ingested_at=when + timedelta(seconds=6),
                processed_at=when + timedelta(seconds=7),
                producer="fraudtwin.graph_fraud",
                source_system="synthetic_payment_source",
                schema_version=PAYMENT_EVENT_CONTRACT_VERSION,
                correlation_id=pid,
                causation_id=None,
                simulation_run_id=self.run_id,
                scenario_id=campaign,
                payment_rail="ACCOUNT_TRANSFER",
                payment_type="PURCHASE" if is_merchant else "TRANSFER",
                payee_account_id=None if is_merchant else payee.account_id,
                merchant_id=merchant_id,
                card_id=None,
                device_id=shared_device,
                ip_id=shared_ip,
                online=False,
                amount=amount,
                currency=payer.currency,
                scenario_type=SCENARIO_CODES[s.type],
                scenario_trigger=s.type,
                scenario_reason=f"deterministic M11 {s.type}",
                fraud_record_id=f"FRD-{campaign}-{index:05d}",
                affected_entity_ids=(payer.customer_id, payer.account_id, payee.account_id),
            )
            payments.append(payment)
            events.append(event)
            truth = s.type != "RANDOM_ALERT_CONTROL"
            records.append(
                FraudRecord(
                    fraud_record_id=event.fraud_record_id or eid,
                    record_type="FRAUD" if truth else "HARD_NEGATIVE",
                    scenario_id=campaign,
                    scenario_type=SCENARIO_CODES[s.type],
                    fraud_truth=truth,
                    trigger=s.type,
                    reason=event.scenario_reason or s.type,
                    customer_id=payer.customer_id,
                    account_id=payer.account_id,
                    card_id=None,
                    device_id=shared_device,
                    merchant_id=None,
                    payment_id=pid,
                    event_id=eid,
                    occurred_at=when,
                    amount=amount,
                    currency=payer.currency,
                    correlation_id=pid,
                    causation_id=None,
                    affected_entity_ids=event.affected_entity_ids,
                )
            )
            for account, role in ((payer, "SOURCE"), (payee, "DESTINATION")):
                memberships.append(
                    GraphCampaignMembership(
                        campaign_id=campaign,
                        pattern_type=s.type,
                        member_id=account.account_id,
                        member_type="ACCOUNT",
                        role=role,
                        valid_from=when,
                        source_event_id=eid,
                        payment_id=pid,
                    )
                )
            if shared_device or shared_ip:
                resource = shared_device or shared_ip
                evidence.append(
                    GraphEvidence(
                        evidence_id=f"EVD-{campaign}-{index:05d}",
                        evidence_type="SHARED_INFRASTRUCTURE",
                        resource_id=resource,
                        source_event_id=eid,
                        payment_id=pid,
                        observed_at=when,
                        available_at=event.source_available_at,
                    )
                )
            if "CAMOUFLAGE_RELATION" in s.modifiers:
                supporting = next(
                    (
                        item
                        for item in self.baseline.payment_events
                        if item.customer_id == payer.customer_id
                    ),
                    None,
                )
                if supporting is not None:
                    memberships.append(
                        GraphCampaignMembership(
                            campaign_id=campaign,
                            pattern_type=s.type,
                            member_id=payer.account_id,
                            member_type="ACCOUNT",
                            role="CAMOUFLAGE",
                            valid_from=supporting.event_time,
                            source_event_id=supporting.event_id,
                            payment_id=supporting.payment_id,
                        )
                    )
        return payments, events, records, memberships, evidence


__all__ = ["GraphFraudDataset", "GraphFraudGenerator", "SCENARIO_CODES"]
