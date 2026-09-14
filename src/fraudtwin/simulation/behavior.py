# ruff: noqa: E501
"""Customer behavior profiles and their generated payment dataset."""

from dataclasses import dataclass, field, replace
from random import Random
from typing import Literal

from pydantic import BaseModel

from fraudtwin.camouflage import resolve_camouflage, transform_generated_data
from fraudtwin.config import SimulationRunConfig
from fraudtwin.counterfactual import CounterfactualDataset, generate_counterfactuals
from fraudtwin.difficulty import resolve_difficulty
from fraudtwin.domain import (
    BehaviorProfile,
    Customer,
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
)
from fraudtwin.domain.payments import LedgerEntry, Payment, PaymentEvent
from fraudtwin.seed import create_stream_rng
from fraudtwin.simulation.cases import FraudWorkflowGenerator
from fraudtwin.simulation.fraud import (
    FraudDataset,
    FraudScenarioGenerator,
    count_true_fraud_records,
    scenario_events,
)
from fraudtwin.simulation.generator import EntityDataset
from fraudtwin.simulation.graph_fraud import GraphFraudGenerator
from fraudtwin.simulation.payments import (
    PaymentGenerator,
    count_card_lifecycle_events,
    count_pix_lifecycle_events,
)
from fraudtwin.simulation.quality import QualityFaultInjector

_PROFILE_ID_WIDTH = 6
_COUNTRIES = ("BR", "US", "GB", "DE")
SpendingLevel = Literal["LOW", "MEDIUM", "HIGH"]


@dataclass(frozen=True)
class BehaviorDataset:
    """Stable, ordered behavior profiles and their generated payment stream."""

    profiles: tuple[BehaviorProfile, ...]
    payments: tuple[Payment, ...]
    payment_events: tuple[PaymentEvent, ...]
    ledger_entries: tuple[LedgerEntry, ...] = ()
    fraud_records: tuple[FraudRecord, ...] = ()
    alerts: tuple[FraudAlert, ...] = ()
    fraud_cases: tuple[FraudCase, ...] = ()
    case_confirmations: tuple[FraudCaseConfirmation, ...] = ()
    customer_disputes: tuple[CustomerDispute, ...] = ()
    fraud_labels: tuple[DelayedFraudLabel, ...] = ()
    quality_fault_counts: dict[str, int] = field(default_factory=dict)
    quality_fault_rates: dict[str, float] = field(default_factory=dict)
    quality_diagnostics: dict[str, object] = field(default_factory=dict)
    graph_memberships: tuple[GraphCampaignMembership, ...] = ()
    graph_campaigns: tuple[GraphCampaign, ...] = ()
    graph_patterns: tuple[GraphPattern, ...] = ()
    graph_evidence: tuple[GraphEvidence, ...] = ()
    graph_hyperedges: tuple[GraphHyperedge, ...] = ()
    graph_hyperedge_memberships: tuple[GraphHyperedgeMembership, ...] = ()
    camouflage_metadata: dict[str, object] = field(default_factory=dict)
    # M8 keeps an immutable in-memory oracle before intentional corruption.
    oracle_tables: dict[str, tuple[BaseModel, ...]] = field(default_factory=dict, repr=False)
    quality_raw_faults: tuple[dict[str, object], ...] = field(default_factory=tuple, repr=False)
    schema_evolution_rows: dict[str, tuple[dict[str, object], ...]] = field(
        default_factory=dict, repr=False
    )
    counterfactual: CounterfactualDataset | None = field(default=None, repr=False)

    def tables(self) -> dict[str, tuple[BaseModel, ...]]:
        """Return all behavior tables in their stable export order."""

        return {
            "behavior_profiles": self.profiles,
            "payments": self.payments,
            "payment_events": self.payment_events,
            "ledger_entries": self.ledger_entries,
            "fraud_records": self.fraud_records,
            "fraud_alerts": self.alerts,
            "fraud_cases": self.fraud_cases,
            "case_confirmations": self.case_confirmations,
            "customer_disputes": self.customer_disputes,
            "fraud_labels": self.fraud_labels,
        }

    @property
    def counts(self) -> dict[str, int]:
        return {name: len(records) for name, records in self.tables().items()}

    @property
    def fraud_events(self) -> tuple[PaymentEvent, ...]:
        """Return scenario-linked payment events, excluding hard negatives."""

        source_events = self.oracle_tables.get("payment_events", self.payment_events)
        typed_events = tuple(event for event in source_events if isinstance(event, PaymentEvent))
        return scenario_events(typed_events)

    @property
    def fraud_record_counts(self) -> dict[str, int]:
        """Count true scenario records for manifest reporting."""

        return count_true_fraud_records(self.fraud_records)

    @property
    def fraud_counts(self) -> dict[str, int]:
        """Return manifest fraud counters, or empty counters when disabled."""

        if not self.fraud_records:
            return {}
        record_counts = self.fraud_record_counts
        fraud_count = sum(record_counts.values())
        return {
            **record_counts,
            "fraud_events": len(self.fraud_events),
            "fraud_records": fraud_count,
            "hard_negatives": len(self.fraud_records) - fraud_count,
            "alerts": len(self.alerts),
            "cases": len(self.fraud_cases),
            "confirmations": len(self.case_confirmations),
            "disputes": len(self.customer_disputes),
            "delayed_labels": len(self.fraud_labels),
        }

    @property
    def fraud_rates(self) -> dict[str, float]:
        """Return realized true-record rates by scenario."""

        if not self.payments:
            return {}
        return {
            scenario: count / len(self.payments)
            for scenario, count in self.fraud_record_counts.items()
        }

    @property
    def card_lifecycle_event_counts(self) -> dict[str, int]:
        """Return counts for the explicit card event vocabulary."""

        return count_card_lifecycle_events(self.payment_events)

    @property
    def pix_lifecycle_event_counts(self) -> dict[str, int]:
        """Return counts for the explicit PIX event vocabulary."""

        return count_pix_lifecycle_events(self.payment_events)

    @property
    def event_counts(self) -> dict[str, int]:
        """Return payment, lifecycle, ledger, and fraud counts for the manifest."""

        card_counts = self.card_lifecycle_event_counts
        pix_counts = self.pix_lifecycle_event_counts
        return {
            "payments": len(self.payments),
            "payment_events": len(self.payment_events),
            "ledger_entries": len(self.ledger_entries),
            "card_lifecycle_events": sum(card_counts.values()),
            "pix_lifecycle_events": sum(pix_counts.values()),
            "fraud_events": len(self.fraud_events),
            "fraud_records": len(self.fraud_records),
            "fraud_alerts": len(self.alerts),
            "fraud_cases": len(self.fraud_cases),
            "case_confirmations": len(self.case_confirmations),
            "customer_disputes": len(self.customer_disputes),
            "fraud_labels": len(self.fraud_labels),
            **card_counts,
            **pix_counts,
        }


def _mask_difficulty_event_truth(dataset: BehaviorDataset) -> BehaviorDataset:
    """Keep direct M6 scenario annotations in oracle tables only."""

    masked_events = tuple(
        event.model_copy(
            update={
                "scenario_id": None,
                "scenario_type": None,
                "scenario_trigger": None,
                "scenario_reason": None,
                "fraud_record_id": None,
                "affected_entity_ids": (),
            }
        )
        for event in dataset.payment_events
    )
    return replace(dataset, payment_events=masked_events)


class BehaviorGenerator:
    """Generate customer profiles and their payment stream from M1 entities."""

    def __init__(
        self,
        config: SimulationRunConfig,
        entities: EntityDataset,
        simulation_run_id: str | None = None,
    ) -> None:
        self.config = config
        self.entities = entities
        self.simulation_run_id = simulation_run_id
        self.merchant_categories = tuple(
            sorted({merchant.merchant_category_code for merchant in entities.merchants})
        )

    def _profile_devices(self, rng: Random) -> tuple[str, ...]:
        limit = self.config.behavior.preferred_device_limit
        if limit == 0 or not self.entities.devices:
            return ()
        trusted = tuple(device.device_id for device in self.entities.devices if device.trusted)
        if not trusted:
            return ()
        count = min(len(trusted), rng.randint(1, limit))
        return tuple(sorted(rng.sample(trusted, count)))

    def _profile_hours(self, rng: Random) -> tuple[tuple[int, ...], tuple[float, ...]]:
        active = tuple(sorted(self.config.behavior.active_hours))
        patterns = (
            (7, 8, 12, 13, 18, 19),
            (9, 10, 12, 18, 20, 21),
            (6, 7, 11, 17, 18, 22),
        )
        typical = tuple(hour for hour in rng.choice(patterns) if hour in active)
        if not typical:
            typical = active[: min(6, len(active))]
        weights = tuple(
            0.0
            if hour not in active
            else (2.5 if hour in typical else 0.35) * rng.uniform(0.8, 1.2)
            for hour in range(24)
        )
        return typical, weights

    def _profile_weekdays(self, rng: Random) -> tuple[float, ...]:
        weekend_preference = rng.uniform(0.55, 1.8)
        return tuple(
            weight * (weekend_preference if weekday >= 5 else rng.uniform(0.85, 1.15))
            for weekday, weight in enumerate(self.config.behavior.weekday_weights)
        )

    def _profile(self, customer: Customer, number: int, rng: Random) -> BehaviorProfile:
        spending_level: SpendingLevel = rng.choices(
            ("LOW", "MEDIUM", "HIGH"),
            weights=self.config.behavior.spending_level_weights,
            k=1,
        )[0]
        income_ranges = {
            "LOW": (1_500.0, 3_500.0),
            "MEDIUM": (3_500.0, 8_000.0),
            "HIGH": (8_000.0, 30_000.0),
        }
        monthly_income = round(rng.uniform(*income_ranges[spending_level]), 2)
        budget_rate = rng.uniform(0.15, 0.35)
        monthly_budget = round(monthly_income * budget_rate, 2)
        typical_hours, hour_weights = self._profile_hours(rng)
        weekday_weights = self._profile_weekdays(rng)
        travel_frequency = round(rng.uniform(0.01, 0.30), 4)
        countries = [customer.country]
        if rng.random() < travel_frequency:
            travel_countries = tuple(
                country for country in _COUNTRIES if country != customer.country
            )
            countries.append(rng.choice(travel_countries))

        preference_count = min(
            self.config.behavior.merchant_preference_count, len(self.merchant_categories)
        )
        category_preferences = tuple(sorted(rng.sample(self.merchant_categories, preference_count)))
        category_weights = tuple(round(rng.uniform(0.5, 2.0), 4) for _ in category_preferences)
        preferred_devices = self._profile_devices(rng)
        return BehaviorProfile(
            behavior_profile_id=f"BEH-{number:0{_PROFILE_ID_WIDTH}d}",
            customer_id=customer.customer_id,
            spending_level=spending_level,
            typical_payment_hours=typical_hours,
            hour_weights=hour_weights,
            weekday_weights=weekday_weights,
            typical_countries=tuple(countries),
            merchant_category_preferences=category_preferences,
            merchant_category_weights=category_weights,
            monthly_income=monthly_income,
            monthly_spending_budget=monthly_budget,
            card_vs_transfer_preference=round(rng.uniform(0.15, 0.95), 4),
            online_purchase_rate=round(rng.uniform(0.1, 0.9), 4),
            travel_frequency=travel_frequency,
            preferred_device_ids=preferred_devices,
            trusted_device_count=len(preferred_devices),
        )

    def generate_profiles(self) -> tuple[BehaviorProfile, ...]:
        """Generate one deterministic profile per existing customer."""

        rng = create_stream_rng(self.config.simulation.seed, "milestone-3:profiles")
        return tuple(
            self._profile(customer, number, rng)
            for number, customer in enumerate(self.entities.customers, start=1)
        )

    def generate(self) -> BehaviorDataset:
        """Generate profiles, base payments, fraud scenarios, and events."""

        profiles = self.generate_profiles()
        payment_dataset = PaymentGenerator(
            self.config,
            self.entities.accounts,
            self.entities.cards,
            self.entities.merchants,
            self.entities.devices,
            self.entities.pix_keys,
            simulation_run_id=self.simulation_run_id,
        ).generate(profiles)
        counterfactual_dataset = (
            generate_counterfactuals(
                self.config,
                self.entities,
                payment_dataset,
                counterfactual_id=(
                    f"CF-{self.simulation_run_id}" if self.simulation_run_id else None
                ),
                run_id=self.simulation_run_id,
            )
            if self.config.counterfactual.active
            else None
        )
        fraud_dataset = FraudScenarioGenerator(
            self.config,
            self.entities.accounts,
            self.entities.cards,
            self.entities.merchants,
            self.entities.devices,
            self.entities.pix_keys,
            payment_dataset,
            simulation_run_id=self.simulation_run_id,
        ).generate()
        graph_dataset = GraphFraudGenerator(
            self.config,
            self.entities.accounts,
            self.entities.devices,
            self.entities.network_endpoints,
            fraud_dataset,
            simulation_run_id=self.simulation_run_id,
            merchants=self.entities.merchants,
            pix_keys=self.entities.pix_keys,
        ).generate()
        (
            camo_payments,
            camo_events,
            camo_ledger,
            camo_records,
            camo_memberships,
            camo_campaigns,
            camo_patterns,
            camo_evidence,
            camouflage_metadata,
        ) = transform_generated_data(
            self.config,
            self.entities,
            profiles,
            graph_dataset.payments,
            graph_dataset.payment_events,
            graph_dataset.ledger_entries,
            fraud_dataset.fraud_records + graph_dataset.fraud_records,
            graph_dataset.memberships,
            graph_dataset.campaigns,
            graph_dataset.patterns,
            graph_dataset.evidence,
        )
        workflow_source = FraudDataset(
            payments=camo_payments,
            payment_events=camo_events,
            ledger_entries=camo_ledger,
            fraud_records=camo_records,
        )
        workflow_dataset = FraudWorkflowGenerator(
            self.config,
            self.entities,
            workflow_source,
        ).generate()
        dataset = BehaviorDataset(
            profiles=profiles,
            payments=camo_payments,
            payment_events=camo_events,
            ledger_entries=camo_ledger,
            fraud_records=camo_records,
            alerts=workflow_dataset.alerts,
            fraud_cases=workflow_dataset.cases,
            case_confirmations=workflow_dataset.confirmations,
            customer_disputes=workflow_dataset.disputes,
            fraud_labels=workflow_dataset.labels,
            graph_memberships=camo_memberships,
            graph_campaigns=camo_campaigns,
            graph_patterns=camo_patterns,
            graph_evidence=camo_evidence,
            graph_hyperedges=graph_dataset.hyperedges,
            graph_hyperedge_memberships=graph_dataset.hyperedge_memberships,
            camouflage_metadata=camouflage_metadata,
            counterfactual=counterfactual_dataset,
        )
        dataset = QualityFaultInjector(self.config).apply(dataset)
        if resolve_difficulty(self.config).enabled or resolve_camouflage(self.config).enabled:
            dataset = _mask_difficulty_event_truth(dataset)
        return dataset


def generate_behavior(config: SimulationRunConfig, entities: EntityDataset) -> BehaviorDataset:
    """Convenience function for deterministic behavior generation."""

    return BehaviorGenerator(config, entities).generate()
