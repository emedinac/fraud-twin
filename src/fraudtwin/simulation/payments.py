"""Deterministic generation of legitimate payment records and events."""

import math
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from random import Random
from typing import Literal, TypeVar, cast

from fraudtwin.config import (
    CARD_EVENT_ENVELOPE_DELAY_SECONDS,
    PIX_EVENT_ENVELOPE_DELAY_SECONDS,
    SimulationRunConfig,
    config_hash,
)
from fraudtwin.domain import (
    CARD_LIFECYCLE_EVENT_TYPES,
    PIX_LIFECYCLE_EVENT_TYPES,
    Account,
    BehaviorProfile,
    Card,
    CardLifecycleEventType,
    Device,
    LedgerEntry,
    Merchant,
    Payment,
    PaymentEvent,
    PaymentEventType,
    PaymentRail,
    PaymentType,
    PixKey,
    PixLifecycleEventType,
    validate_payment_lifecycle,
)
from fraudtwin.seed import create_stream_rng

_ID_WIDTH = 8
_CARD_SOURCE_DELAY_SECONDS = 5
_PIX_SOURCE_DELAY_SECONDS = 2
T = TypeVar("T")
Record = TypeVar("Record")


def _weighted_choice(rng: Random, values: tuple[T, ...], weights: tuple[float, ...]) -> T:
    return rng.choices(values, weights=weights, k=1)[0]


def _group_by(
    records: tuple[Record, ...], key: Callable[[Record], str]
) -> dict[str, tuple[Record, ...]]:
    """Group records by a stable relationship key."""

    grouped: dict[str, list[Record]] = {}
    for record in records:
        grouped.setdefault(key(record), []).append(record)
    return {group_key: tuple(values) for group_key, values in grouped.items()}


def count_lifecycle_events(
    events: Iterable[PaymentEvent], event_types: tuple[str, ...]
) -> dict[str, int]:
    """Count a lifecycle vocabulary in one pass."""

    counts = dict.fromkeys(event_types, 0)
    for event in events:
        if event.event_type in counts:
            counts[event.event_type] += 1
    return counts


def count_card_lifecycle_events(events: Iterable[PaymentEvent]) -> dict[str, int]:
    """Count the explicit card lifecycle event types."""

    return count_lifecycle_events(events, CARD_LIFECYCLE_EVENT_TYPES)


def count_pix_lifecycle_events(events: Iterable[PaymentEvent]) -> dict[str, int]:
    """Count the explicit PIX lifecycle event types."""

    return count_lifecycle_events(events, PIX_LIFECYCLE_EVENT_TYPES)


def _event_times(event_time: datetime, source_delay_seconds: int) -> tuple[datetime, ...]:
    """Return source, ingestion, and processing times for one event."""

    source_available_at = event_time + timedelta(seconds=source_delay_seconds)
    ingested_at = source_available_at + timedelta(seconds=1)
    processed_at = ingested_at + timedelta(seconds=1)
    return source_available_at, ingested_at, processed_at


@dataclass(frozen=True)
class PaymentDataset:
    """Stable, ordered payment business objects and their event envelopes."""

    payments: tuple[Payment, ...]
    payment_events: tuple[PaymentEvent, ...]
    ledger_entries: tuple[LedgerEntry, ...] = ()

    @property
    def counts(self) -> dict[str, int]:
        return {
            "payments": len(self.payments),
            "payment_events": len(self.payment_events),
            "ledger_entries": len(self.ledger_entries),
        }

    @property
    def card_lifecycle_event_counts(self) -> dict[str, int]:
        return count_card_lifecycle_events(self.payment_events)

    @property
    def pix_lifecycle_event_counts(self) -> dict[str, int]:
        return count_pix_lifecycle_events(self.payment_events)


class PaymentGenerator:
    """Generate positive, relationship-valid, legitimate payment events."""

    def __init__(
        self,
        config: SimulationRunConfig,
        accounts: tuple[Account, ...],
        cards: tuple[Card, ...],
        merchants: tuple[Merchant, ...],
        devices: tuple[Device, ...],
        pix_keys: tuple[PixKey, ...] = (),
        simulation_run_id: str | None = None,
    ) -> None:
        self.config = config
        self.start = config.simulation.start.astimezone(UTC)
        self.end = self.start + timedelta(days=config.simulation.duration_days)
        self.accounts = accounts
        self.merchants = merchants
        self.simulation_run_id = simulation_run_id or self._stable_run_id()
        self.accounts_by_customer = _group_by(accounts, lambda account: account.customer_id)
        self.cards_by_customer = _group_by(cards, lambda card: card.customer_id)
        self.accounts_by_id = {account.account_id: account for account in accounts}
        self.devices_by_id = {device.device_id: device for device in devices}
        self.pix_keys_by_account = _group_by(pix_keys, lambda key: key.account_id)
        self._days = self._simulation_days()
        self._valid_hours_by_day = self._build_valid_hours_by_day()
        self._card_max_delay_seconds = config.card_lifecycle.maximum_delay_seconds
        self._pix_max_delay_seconds = config.pix_lifecycle.maximum_delay_seconds

    def _stable_run_id(self) -> str:
        # Lifecycle settings must not change the base payment stream ID.
        stable_hash = config_hash(
            self.config,
            include_card_lifecycle=False,
            include_pix_lifecycle=False,
        )
        return f"SIM-{stable_hash[:16]}"

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
                if rail == "ACCOUNT_TRANSFER" or any(
                    account.account_id in self.pix_keys_by_account
                    for account in self.accounts_by_customer[profile.customer_id]
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
        sampled_amount = math.exp(rng.gauss(math.log(median), 0.65))
        bounded_amount = min(
            self.config.behavior.amount_max,
            max(self.config.behavior.amount_min, sampled_amount),
        )
        rounded = round(bounded_amount, 2)
        return rounded if rounded > 0 else self.config.behavior.amount_min

    def _sample_lifecycle_time(
        self,
        profile: BehaviorProfile,
        rng: Random,
        maximum_delay_seconds: int,
        envelope_delay_seconds: int,
        rail_name: str,
    ) -> datetime:
        """Sample a payment time while leaving room for its lifecycle."""

        latest = self.end - timedelta(
            seconds=maximum_delay_seconds + envelope_delay_seconds,
            microseconds=1,
        )
        if latest <= self.start:
            raise ValueError(f"{rail_name} lifecycle timing settings exceed the simulation window")
        return min(self._sample_time(profile, rng), latest)

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

    def _payee_account(
        self, payer_account_id: str, rng: Random, *, require_pix_key: bool = False
    ) -> Account:
        alternatives = tuple(
            account for account in self.accounts if account.account_id != payer_account_id
        )
        if require_pix_key:
            keyed = tuple(
                account
                for account in alternatives
                if account.account_id in self.pix_keys_by_account
            )
            alternatives = keyed or tuple(
                account
                for account in self.accounts
                if account.account_id in self.pix_keys_by_account
            )
        return rng.choice(alternatives or self.accounts)

    def _generate_one(
        self, number: int, profile: BehaviorProfile, rng: Random
    ) -> tuple[Payment, PaymentEvent]:
        rail = self._choose_rail(profile, rng)
        initiated_at = (
            self._sample_lifecycle_time(
                profile,
                rng,
                self._card_max_delay_seconds,
                CARD_EVENT_ENVELOPE_DELAY_SECONDS,
                "card",
            )
            if rail == "CARD"
            else (
                self._sample_lifecycle_time(
                    profile,
                    rng,
                    self._pix_max_delay_seconds,
                    PIX_EVENT_ENVELOPE_DELAY_SECONDS,
                    "PIX",
                )
                if rail == "PIX"
                else self._sample_time(profile, rng)
            )
        )
        amount = self._sample_amount(profile, rng)
        device_id = self._device_id(profile, rng)
        merchant_id: str | None = None
        card_id: str | None = None
        payee_account_id: str | None = None
        payer_institution_id: str | None = None
        payee_institution_id: str | None = None
        payer_pix_key_id: str | None = None
        payee_pix_key_id: str | None = None
        payment_type: PaymentType
        event_type: PaymentEventType

        if rail == "CARD":
            card = rng.choice(self.cards_by_customer[profile.customer_id])
            card_id = card.card_id
            payer_account_id = card.account_id
            merchant = self._merchant(profile, rng)
            merchant_id = merchant.merchant_id
            payment_type = "PURCHASE"
            online = merchant.online_only or rng.random() < profile.online_purchase_rate
            event_type = "CARD_AUTHORIZATION_REQUESTED"
        else:
            payer_candidates = self.accounts_by_customer[profile.customer_id]
            if rail == "PIX":
                keyed_payers = tuple(
                    account
                    for account in payer_candidates
                    if account.account_id in self.pix_keys_by_account
                )
                payer_candidates = keyed_payers or payer_candidates
            payer_account = rng.choice(payer_candidates)
            payer_account_id = payer_account.account_id
            payee_account = self._payee_account(
                payer_account_id, rng, require_pix_key=rail == "PIX"
            )
            payee_account_id = payee_account.account_id
            payer_institution_id = payer_account.institution_id
            payee_institution_id = payee_account.institution_id
            if rail == "PIX":
                payer_keys = self.pix_keys_by_account.get(payer_account_id, ())
                payee_keys = self.pix_keys_by_account.get(payee_account_id, ())
                payer_pix_key_id = payer_keys[0].pix_key_id if payer_keys else None
                payee_pix_key_id = payee_keys[0].pix_key_id if payee_keys else None
            payment_type = "TRANSFER"
            online = False
            event_type = "PIX_INITIATED" if rail == "PIX" else "TRANSFER_COMPLETED"

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
            payer_institution_id=payer_institution_id,
            payee_institution_id=payee_institution_id,
            payer_pix_key_id=payer_pix_key_id,
            payee_pix_key_id=payee_pix_key_id,
        )
        source_delay_seconds = (
            _PIX_SOURCE_DELAY_SECONDS if rail == "PIX" else _CARD_SOURCE_DELAY_SECONDS
        )
        source_available_at, ingested_at, processed_at = _event_times(
            initiated_at, source_delay_seconds
        )
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
            schema_version="2" if rail == "CARD" else "3" if rail == "PIX" else "1",
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

    @staticmethod
    def _advance(previous: datetime, delay_seconds: int) -> datetime:
        """Advance an event clock while keeping zero-delay events ordered."""

        candidate = previous + timedelta(seconds=delay_seconds)
        return candidate if candidate > previous else previous + timedelta(microseconds=1)

    def _lifecycle_event(
        self,
        initial: PaymentEvent,
        event_type: CardLifecycleEventType | PixLifecycleEventType,
        event_time: datetime,
        sequence: int,
        causation_id: str,
        source_delay_seconds: int,
    ) -> PaymentEvent:
        """Create a lifecycle event while preserving the common envelope."""

        source_available_at, ingested_at, processed_at = _event_times(
            event_time, source_delay_seconds
        )
        return initial.model_copy(
            update={
                "event_id": f"{initial.event_id}-{sequence:02d}",
                "event_type": event_type,
                "event_time": event_time,
                "source_created_at": event_time,
                "source_available_at": source_available_at,
                "ingested_at": ingested_at,
                "processed_at": processed_at,
                "causation_id": causation_id,
            }
        )

    def _append_lifecycle_event(
        self,
        events: list[PaymentEvent],
        initial: PaymentEvent,
        previous_time: datetime,
        previous_id: str,
        event_type: CardLifecycleEventType | PixLifecycleEventType,
        delay_seconds: int,
        source_delay_seconds: int,
    ) -> tuple[datetime, str]:
        """Append one ordered lifecycle event and return its new cursor."""

        event_time = self._advance(previous_time, delay_seconds)
        if event_time >= self.end:
            raise ValueError("lifecycle events exceed the simulation window")
        event = self._lifecycle_event(
            initial,
            event_type,
            event_time,
            len(events) + 1,
            previous_id,
            source_delay_seconds,
        )
        events.append(event)
        return event_time, event.event_id

    def _card_lifecycle(
        self,
        payment: Payment,
        initial: PaymentEvent,
        rng: Random,
    ) -> tuple[Payment, tuple[PaymentEvent, ...]]:
        """Generate and validate one deterministic card lifecycle."""

        settings = self.config.card_lifecycle
        events = [initial]
        previous_time = initial.event_time
        previous_id = initial.event_id

        def append(event_type: CardLifecycleEventType, delay_seconds: int) -> None:
            nonlocal previous_time, previous_id
            previous_time, previous_id = self._append_lifecycle_event(
                events,
                initial,
                previous_time,
                previous_id,
                event_type,
                delay_seconds,
                _CARD_SOURCE_DELAY_SECONDS,
            )

        if rng.random() >= settings.authorization_approval_probability:
            append("CARD_DECLINED", settings.authorization_delay_seconds)
            final_status = "DECLINED"
        else:
            append("CARD_AUTHORIZED", settings.authorization_delay_seconds)
            if rng.random() < settings.reversal_probability:
                if rng.random() < 0.5:
                    append("CARD_REVERSED", settings.reversal_delay_seconds)
                else:
                    append("CARD_CAPTURED", settings.capture_delay_seconds)
                    append("CARD_REVERSED", settings.reversal_delay_seconds)
                final_status = "REVERSED"
            else:
                append("CARD_CAPTURED", settings.capture_delay_seconds)
                append("CARD_CLEARED", settings.clearing_delay_seconds)
                append("CARD_SETTLED", settings.settlement_delay_seconds)
                if rng.random() < settings.refund_probability:
                    append("CARD_REFUNDED", settings.refund_delay_seconds)
                    final_status = "REFUNDED"
                else:
                    final_status = "SETTLED"

        result = payment.model_copy(update={"current_status": final_status})
        event_tuple = tuple(events)
        validate_payment_lifecycle(result, event_tuple)
        return result, event_tuple

    def _pix_lifecycle(
        self,
        payment: Payment,
        initial: PaymentEvent,
        rng: Random,
        *,
        always_approve: bool = False,
    ) -> tuple[Payment, tuple[PaymentEvent, ...]]:
        """Generate and validate one deterministic PIX lifecycle."""

        settings = self.config.pix_lifecycle
        events = [initial]
        previous_time = initial.event_time
        previous_id = initial.event_id

        def append(event_type: PixLifecycleEventType, delay_seconds: int) -> None:
            nonlocal previous_time, previous_id
            previous_time, previous_id = self._append_lifecycle_event(
                events,
                initial,
                previous_time,
                previous_id,
                event_type,
                delay_seconds,
                _PIX_SOURCE_DELAY_SECONDS,
            )

        append("PIX_VALIDATED", settings.validation_delay_seconds)
        rejected = not always_approve and (
            rng.random() >= settings.authorization_approval_probability
            or rng.random() < settings.rejection_probability
        )
        if rejected:
            append("PIX_REJECTED", settings.authorization_delay_seconds)
            final_status = "REJECTED"
        else:
            append("PIX_AUTHORIZED", settings.authorization_delay_seconds)
            append("PIX_SUBMITTED", settings.submission_delay_seconds)
            append("PIX_SETTLED", settings.settlement_delay_seconds)
            append("PIX_RECEIVED", settings.receipt_delay_seconds)
            if rng.random() < settings.return_probability:
                append("PIX_RETURN_REQUESTED", settings.return_request_delay_seconds)
                append("PIX_RETURNED", settings.return_delay_seconds)
                final_status = "RETURNED"
            else:
                final_status = "RECEIVED"

        result = payment.model_copy(update={"current_status": final_status})
        event_tuple = tuple(events)
        validate_payment_lifecycle(result, event_tuple)
        return result, event_tuple

    @staticmethod
    def _ledger_specs(
        payment: Payment, events: tuple[PaymentEvent, ...]
    ) -> list[tuple[PaymentEvent, str, str]]:
        """Return event, account, and direction tuples for posted transfers."""

        payer = payment.payer_account_id
        payee = payment.payee_account_id
        if payee is None:
            return []
        specs: list[tuple[PaymentEvent, str, str]] = []
        for event in events:
            if event.event_type in {"PIX_SETTLED", "TRANSFER_COMPLETED"}:
                specs.extend(((event, payer, "DEBIT"), (event, payee, "CREDIT")))
            elif event.event_type == "PIX_RETURNED":
                specs.extend(((event, payer, "CREDIT"), (event, payee, "DEBIT")))
        return specs

    def _materialize_ledger(
        self, specs: list[tuple[PaymentEvent, str, str]]
    ) -> tuple[LedgerEntry, ...]:
        """Create stable ledger rows and running balances from account openings."""

        balances = {account.account_id: account.ledger_balance for account in self.accounts}
        ordered = sorted(
            specs,
            key=lambda item: (
                item[0].processed_at,
                item[0].event_id,
                item[1],
                item[2],
            ),
        )
        entries: list[LedgerEntry] = []
        for event, account_id, raw_entry_type in ordered:
            entry_type = cast(Literal["DEBIT", "CREDIT"], raw_entry_type)
            delta = event.amount if entry_type == "CREDIT" else -event.amount
            balance = round(balances[account_id] + delta, 2)
            if balance < -self.accounts_by_id[account_id].overdraft_limit:
                raise ValueError(f"ledger debit exceeds overdraft limit for {account_id}")
            balances[account_id] = balance
            entries.append(
                LedgerEntry(
                    ledger_entry_id=f"LED-{event.event_id}-{len(entries) + 1:02d}",
                    payment_id=event.payment_id,
                    account_id=account_id,
                    event_id=event.event_id,
                    entry_type=entry_type,
                    amount=event.amount,
                    currency=event.currency,
                    occurred_at=event.event_time,
                    effective_at=event.event_time,
                    posted_at=event.processed_at,
                    balance_after=balance,
                )
            )
        return tuple(entries)

    def materialize_ledger(
        self, payments: tuple[Payment, ...], events: tuple[PaymentEvent, ...]
    ) -> tuple[LedgerEntry, ...]:
        """Reconcile a complete payment stream, including scenario payments."""

        payments_by_id = {payment.payment_id: payment for payment in payments}
        specs: list[tuple[PaymentEvent, str, str]] = []
        for event in events:
            payment = payments_by_id.get(event.payment_id)
            if payment is not None:
                specs.extend(self._ledger_specs(payment, (event,)))
        return self._materialize_ledger(specs)

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
        ledger_specs: list[tuple[PaymentEvent, str, str]] = []
        lifecycle_rng = create_stream_rng(self.config.simulation.seed, "milestone-4:card-lifecycle")
        pix_lifecycle_rng = create_stream_rng(
            self.config.simulation.seed, "milestone-5:pix-lifecycle"
        )
        for payment, event in self.iter_generate(profiles):
            if payment.payment_rail == "CARD":
                payment, payment_events = self._card_lifecycle(payment, event, lifecycle_rng)
                events.extend(payment_events)
            elif payment.payment_rail == "PIX":
                payment, payment_events = self._pix_lifecycle(payment, event, pix_lifecycle_rng)
                events.extend(payment_events)
                ledger_specs.extend(self._ledger_specs(payment, payment_events))
            else:
                payment_events = (event,)
                events.append(event)
                ledger_specs.extend(self._ledger_specs(payment, payment_events))
            payments.append(payment)
        return PaymentDataset(
            tuple(payments), tuple(events), self._materialize_ledger(ledger_specs)
        )
