"""Deterministic generation of legitimate payment records and events."""

import math
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from random import Random
from typing import Literal, TypeVar

from fraudtwin.config import SimulationRunConfig, config_hash
from fraudtwin.domain import Account, BehaviorProfile, Card, Device, Merchant, Payment, PaymentEvent
from fraudtwin.domain.payments import PaymentRail
from fraudtwin.seed import create_stream_rng

_ID_WIDTH = 8
T = TypeVar("T")
PaymentType = Literal["PURCHASE", "TRANSFER"]
EventType = Literal["CARD_PAYMENT_COMPLETED", "PIX_SETTLED", "TRANSFER_COMPLETED"]


def _weighted_choice(rng: Random, values: tuple[T, ...], weights: tuple[float, ...]) -> T:
    return rng.choices(values, weights=weights, k=1)[0]


@dataclass(frozen=True)
class PaymentDataset:
    """Stable, ordered payment business objects and their event envelopes."""

    payments: tuple[Payment, ...]
    payment_events: tuple[PaymentEvent, ...]

    @property
    def counts(self) -> dict[str, int]:
        return {
            "payments": len(self.payments),
            "payment_events": len(self.payment_events),
        }


class PaymentGenerator:
    """Generate positive, relationship-valid, legitimate payment events."""

    def __init__(
        self,
        config: SimulationRunConfig,
        accounts: tuple[Account, ...],
        cards: tuple[Card, ...],
        merchants: tuple[Merchant, ...],
        devices: tuple[Device, ...],
        simulation_run_id: str | None = None,
    ) -> None:
        self.config = config
        self.start = config.simulation.start.astimezone(UTC)
        self.end = self.start + timedelta(days=config.simulation.duration_days)
        self.accounts = accounts
        self.merchants = merchants
        self.simulation_run_id = simulation_run_id or self._stable_run_id()
        self.accounts_by_customer = self._group_accounts_by_customer(accounts)
        self.cards_by_customer = self._group_cards_by_customer(cards)
        self.accounts_by_id = {account.account_id: account for account in accounts}
        self.devices_by_id = {device.device_id: device for device in devices}
        self._days = self._simulation_days()
        self._valid_hours_by_day = self._build_valid_hours_by_day()

    @staticmethod
    def _group_accounts_by_customer(
        records: tuple[Account, ...],
    ) -> dict[str, tuple[Account, ...]]:
        grouped: dict[str, list[Account]] = {}
        for record in records:
            grouped.setdefault(record.customer_id, []).append(record)
        return {customer_id: tuple(values) for customer_id, values in grouped.items()}

    @staticmethod
    def _group_cards_by_customer(records: tuple[Card, ...]) -> dict[str, tuple[Card, ...]]:
        grouped: dict[str, list[Card]] = {}
        for record in records:
            grouped.setdefault(record.customer_id, []).append(record)
        return {customer_id: tuple(values) for customer_id, values in grouped.items()}

    def _stable_run_id(self) -> str:
        return f"SIM-{config_hash(self.config)[:16]}"

    def _simulation_days(self) -> tuple[date, ...]:
        first = self.start.date()
        last = self.end.date()
        return tuple(first + timedelta(days=offset) for offset in range((last - first).days + 1))

    def _build_valid_hours_by_day(self) -> dict[date, tuple[int, ...]]:
        active_hours = self.config.behavior.active_hours
        return {
            current_day: tuple(
                hour
                for hour in active_hours
                if self.start <= datetime.combine(current_day, time(hour), tzinfo=UTC) < self.end
            )
            for current_day in self._days
        }

    def _eligible_profiles(
        self, profiles: tuple[BehaviorProfile, ...]
    ) -> tuple[BehaviorProfile, ...]:
        eligible = tuple(
            profile
            for profile in profiles
            if profile.customer_id in self.accounts_by_customer and self._available_rails(profile)
        )
        if self.config.payments.daily_target and not eligible:
            raise ValueError("payment generation requires at least one customer with an account")
        return eligible

    def _available_rails(self, profile: BehaviorProfile) -> tuple[PaymentRail, ...]:
        configured = tuple(
            rail for rail, weight in self.config.payments.rails.items() if weight > 0
        )
        available: list[PaymentRail] = []
        for rail in configured:
            if rail == "CARD" and profile.customer_id in self.cards_by_customer and self.merchants:
                available.append(rail)
            elif (
                rail in ("PIX", "ACCOUNT_TRANSFER")
                and profile.customer_id in self.accounts_by_customer
            ):
                available.append(rail)
        return tuple(available)

    def _choose_rail(self, profile: BehaviorProfile, rng: Random) -> PaymentRail:
        available = self._available_rails(profile)
        if not available:
            raise ValueError("payment generation has no configured rail with required entities")
        weights = tuple(
            self.config.payments.rails[rail]
            * (
                0.5 + profile.card_vs_transfer_preference
                if rail == "CARD"
                else 1.5 - profile.card_vs_transfer_preference
            )
            for rail in available
        )
        return _weighted_choice(rng, available, weights)

    def _sample_time(self, profile: BehaviorProfile, rng: Random) -> datetime:
        active_hours = tuple(hour for hour, weight in enumerate(profile.hour_weights) if weight > 0)
        day_candidates = tuple(
            current_day for current_day in self._days if self._valid_hours_by_day[current_day]
        )
        day_weights = [
            profile.weekday_weights[current_day.weekday()]
            * sum(profile.hour_weights[hour] for hour in self._valid_hours_by_day[current_day])
            for current_day in day_candidates
        ]
        if not day_candidates:
            seconds = rng.randrange(max(1, int((self.end - self.start).total_seconds())))
            return self.start + timedelta(seconds=seconds)
        if sum(day_weights) <= 0:
            day_weights = [1.0] * len(day_candidates)

        current_day = _weighted_choice(rng, tuple(day_candidates), tuple(day_weights))
        valid_hours = tuple(
            hour for hour in self._valid_hours_by_day[current_day] if hour in active_hours
        )
        hour_weights = tuple(profile.hour_weights[hour] for hour in valid_hours)
        hour = _weighted_choice(rng, valid_hours, hour_weights)
        event_time = datetime.combine(current_day, time(hour), tzinfo=UTC) + timedelta(
            minutes=rng.randrange(60)
        )
        if event_time < self.start:
            return self.start
        if event_time >= self.end:
            return self.end - timedelta(microseconds=1)
        return event_time

    def _sample_amount(self, profile: BehaviorProfile, rng: Random) -> float:
        daily_budget = profile.monthly_spending_budget / 30.0
        median_by_level = {
            "LOW": daily_budget * 0.35,
            "MEDIUM": daily_budget * 0.75,
            "HIGH": daily_budget * 1.2,
        }
        median = max(self.config.behavior.amount_min, median_by_level[profile.spending_level])
        amount = math.exp(rng.gauss(math.log(median), 0.65))
        amount = min(self.config.behavior.amount_max, max(self.config.behavior.amount_min, amount))
        rounded = round(amount, 2)
        return rounded if rounded > 0 else self.config.behavior.amount_min

    def _merchant(self, profile: BehaviorProfile, rng: Random) -> Merchant:
        merchants = self.merchants
        if profile.merchant_category_preferences:
            preferred = set(profile.merchant_category_preferences)
            matching = tuple(
                merchant for merchant in merchants if merchant.merchant_category_code in preferred
            )
            if matching:
                weights_by_code = dict(
                    zip(
                        profile.merchant_category_preferences,
                        profile.merchant_category_weights,
                        strict=True,
                    )
                )
                weights = tuple(
                    weights_by_code[merchant.merchant_category_code] for merchant in matching
                )
                if sum(weights) > 0:
                    return _weighted_choice(rng, matching, weights)
        return rng.choice(merchants)

    def _device_id(self, profile: BehaviorProfile, rng: Random) -> str | None:
        available = tuple(
            device_id
            for device_id in profile.preferred_device_ids
            if device_id in self.devices_by_id
        )
        return rng.choice(available) if available else None

    def _payee_account(self, payer_account_id: str, rng: Random) -> Account:
        alternatives = tuple(
            account for account in self.accounts if account.account_id != payer_account_id
        )
        return rng.choice(alternatives or self.accounts)

    def _generate_one(
        self, number: int, profile: BehaviorProfile, rng: Random
    ) -> tuple[Payment, PaymentEvent]:
        rail = self._choose_rail(profile, rng)
        initiated_at = self._sample_time(profile, rng)
        amount = self._sample_amount(profile, rng)
        device_id = self._device_id(profile, rng)
        merchant_id: str | None = None
        card_id: str | None = None
        payee_account_id: str | None = None
        payment_type: PaymentType
        event_type: EventType

        if rail == "CARD":
            card = rng.choice(self.cards_by_customer[profile.customer_id])
            card_id = card.card_id
            payer_account_id = card.account_id
            merchant = self._merchant(profile, rng)
            merchant_id = merchant.merchant_id
            payment_type = "PURCHASE"
            online = merchant.online_only or rng.random() < profile.online_purchase_rate
            event_type = "CARD_PAYMENT_COMPLETED"
        else:
            payer_account = rng.choice(self.accounts_by_customer[profile.customer_id])
            payer_account_id = payer_account.account_id
            payee_account_id = self._payee_account(payer_account_id, rng).account_id
            payment_type = "TRANSFER"
            online = False
            event_type = "PIX_SETTLED" if rail == "PIX" else "TRANSFER_COMPLETED"

        payment_id = f"PAY-{number:0{_ID_WIDTH}d}"
        event_id = f"EVT-{number:0{_ID_WIDTH}d}"
        payment = Payment(
            payment_id=payment_id,
            payment_rail=rail,
            payment_type=payment_type,
            payer_account_id=payer_account_id,
            payee_account_id=payee_account_id,
            merchant_id=merchant_id,
            card_id=card_id,
            amount=amount,
            currency=self.accounts_by_id[payer_account_id].currency,
            initiated_at=initiated_at,
            current_status="SETTLED" if rail == "PIX" else "COMPLETED",
        )
        source_available_at = initiated_at + timedelta(seconds=2 if rail == "PIX" else 5)
        ingested_at = source_available_at + timedelta(seconds=1)
        processed_at = ingested_at + timedelta(seconds=1)
        event = PaymentEvent(
            event_id=event_id,
            event_type=event_type,
            event_version=1,
            payment_id=payment_id,
            customer_id=profile.customer_id,
            account_id=payer_account_id,
            event_time=initiated_at,
            source_created_at=initiated_at,
            source_available_at=source_available_at,
            ingested_at=ingested_at,
            processed_at=processed_at,
            producer="fraudtwin.behavior",
            source_system="synthetic_payment_source",
            schema_version="1",
            correlation_id=payment_id,
            causation_id=None,
            simulation_run_id=self.simulation_run_id,
            scenario_id=None,
            payment_rail=rail,
            payment_type=payment_type,
            payee_account_id=payee_account_id,
            merchant_id=merchant_id,
            card_id=card_id,
            device_id=device_id,
            online=online,
            amount=amount,
            currency=payment.currency,
        )
        return payment, event

    def iter_generate(
        self, profiles: tuple[BehaviorProfile, ...]
    ) -> Iterator[tuple[Payment, PaymentEvent]]:
        """Stream exactly daily_target * duration_days legitimate payment pairs."""

        count = self.config.payments.daily_target * self.config.simulation.duration_days
        if count == 0:
            return
        eligible_profiles = self._eligible_profiles(profiles)
        if not eligible_profiles:
            raise ValueError("payment generation requires at least one eligible behavior profile")

        rng = create_stream_rng(self.config.simulation.seed, "milestone-3:payments")
        for number in range(1, count + 1):
            profile = rng.choice(eligible_profiles)
            yield self._generate_one(number, profile, rng)

    def generate(self, profiles: tuple[BehaviorProfile, ...]) -> PaymentDataset:
        """Materialize the deterministic payment stream as an in-memory dataset."""

        payments: list[Payment] = []
        events: list[PaymentEvent] = []
        for payment, event in self.iter_generate(profiles):
            payments.append(payment)
            events.append(event)
        return PaymentDataset(tuple(payments), tuple(events))
