"""Deterministic generation of legitimate payment records and events."""

import math
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from random import Random
from typing import Literal, TypeVar, cast

from fraudtwin.calibration import (
    CALIBRATED_AMOUNT_STREAM_ID,
    CALIBRATED_TIMING_STREAM_ID,
    ResolvedCalibration,
)
from fraudtwin.config import (
    ACCOUNT_TRANSFER_SOURCE_DELAY_SECONDS,
    CARD_EVENT_ENVELOPE_DELAY_SECONDS,
    CARD_SOURCE_DELAY_SECONDS,
    PIX_EVENT_ENVELOPE_DELAY_SECONDS,
    PIX_SOURCE_DELAY_SECONDS,
    SimulationRunConfig,
    config_hash,
)
from fraudtwin.domain import (
    CARD_LIFECYCLE_EVENT_TYPES,
    PAYMENT_EVENT_CONTRACT_VERSION,
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


def _event_times(
    event_time: datetime, source_delay_seconds: int
) -> tuple[datetime, datetime, datetime]:
    """Return source, ingestion, and processing times for one event."""

    source_available_at = event_time + timedelta(seconds=source_delay_seconds)
    ingested_at = source_available_at + timedelta(seconds=1)
    processed_at = ingested_at + timedelta(seconds=1)
    return source_available_at, ingested_at, processed_at


def payment_event_times(
    event_time: datetime, rail: PaymentRail
) -> tuple[datetime, datetime, datetime]:
    """Return source, ingestion, and processing times for a payment rail."""

    source_delay_seconds = {
        "CARD": CARD_SOURCE_DELAY_SECONDS,
        "PIX": PIX_SOURCE_DELAY_SECONDS,
        "ACCOUNT_TRANSFER": ACCOUNT_TRANSFER_SOURCE_DELAY_SECONDS,
    }[rail]
    return _event_times(event_time, source_delay_seconds)


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


@dataclass(frozen=True)
class _PaymentDetails:
    """Rail-specific fields assembled before creating shared payment records."""

    amount: float
    merchant_id: str | None
    card_id: str | None
    payer_account_id: str
    payee_account_id: str | None
    payer_institution_id: str | None
    payee_institution_id: str | None
    payer_pix_key_id: str | None
    payee_pix_key_id: str | None
    payment_type: PaymentType
    event_type: PaymentEventType
    online: bool


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
        calibration: ResolvedCalibration | None = None,
    ) -> None:
        self.config = config
        self.start = config.simulation.start.astimezone(UTC)
        self.end = self.start + timedelta(days=config.simulation.duration_days)
        self.accounts = accounts
        self.merchants = merchants
        self.simulation_run_id = simulation_run_id or self._stable_run_id()
        self.calibration = calibration
        self.accounts_by_customer = _group_by(accounts, lambda account: account.customer_id)
        self.accounts_by_institution = _group_by(accounts, lambda account: account.institution_id)
        self.cards_by_customer = _group_by(cards, lambda card: card.customer_id)
        self.accounts_by_id = {account.account_id: account for account in accounts}
        self.devices_by_id = {device.device_id: device for device in devices}
        self.pix_keys_by_account = _group_by(pix_keys, lambda key: key.account_id)
        self._days = self._simulation_days()
        self._valid_hours_by_day = self._build_valid_hours_by_day()
        self._card_max_delay_seconds = config.card_lifecycle.maximum_delay_seconds
        self._pix_max_delay_seconds = config.pix_lifecycle.maximum_delay_seconds
        self._card_daily_spend: dict[tuple[str, date], float] = {}
        self._account_spend: dict[str, float] = {}
        self._calibrated_amount_rng = (
            create_stream_rng(self.config.simulation.seed, CALIBRATED_AMOUNT_STREAM_ID)
            if calibration is not None and calibration.enabled
            else None
        )
        self._calibrated_time_rng = (
            create_stream_rng(self.config.simulation.seed, CALIBRATED_TIMING_STREAM_ID)
            if calibration is not None and calibration.enabled
            else None
        )

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
            if (
                rail == "CARD"
                and any(
                    card.status == "ACTIVE" and card.expires_at > self.start
                    for card in self.cards_by_customer.get(profile.customer_id, ())
                )
                and self.merchants
            ):
                available.append(rail)
            elif (
                rail in ("PIX", "ACCOUNT_TRANSFER")
                and any(
                    account.status == "ACTIVE"
                    for account in self.accounts_by_customer.get(profile.customer_id, ())
                )
                and (
                    rail == "ACCOUNT_TRANSFER"
                    or any(
                        account.account_id in self.pix_keys_by_account
                        for account in self.accounts_by_customer[profile.customer_id]
                    )
                )
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
        behavior = self.config.behavior
        allowed_months = set(behavior.travel_period_months)
        holiday_dates = set(behavior.holiday_dates)
        day_candidates = tuple(
            current_day
            for current_day in self._days
            if self._valid_hours_by_day[current_day] and current_day.month in allowed_months
        )
        day_weights = []
        for current_day in day_candidates:
            weight = profile.weekday_weights[current_day.weekday()]
            if current_day.day <= 3:
                weight *= behavior.beginning_of_month_weight
            if current_day.day >= 28:
                weight *= behavior.end_of_month_weight
            if current_day.day in behavior.payday_days:
                weight *= behavior.payday_weight
            if current_day.isoformat() in holiday_dates:
                weight *= behavior.holiday_weight
            weight *= sum(
                profile.hour_weights[hour]
                for hour in self._valid_hours_by_day[current_day]
                if hour in behavior.merchant_active_hours
            )
            day_weights.append(weight)
        if not day_candidates:
            seconds = rng.randrange(max(1, int((self.end - self.start).total_seconds())))
            return self.start + timedelta(seconds=seconds)
        if sum(day_weights) <= 0:
            day_weights = [1.0] * len(day_candidates)

        current_day = _weighted_choice(rng, tuple(day_candidates), tuple(day_weights))
        valid_hours = tuple(
            hour
            for hour in self._valid_hours_by_day[current_day]
            if hour in active_hours and hour in behavior.merchant_active_hours
        )
        if not valid_hours:
            valid_hours = tuple(self._valid_hours_by_day[current_day])
        hour_weights = tuple(profile.hour_weights[hour] for hour in valid_hours)
        if (
            self.calibration is not None
            and self.calibration.enabled
            and self.calibration.profile
            and "seasonality" in self.config.calibration.summary_names
        ):
            seasonality = next(
                (item for item in self.calibration.profile.summaries if item.name == "seasonality"),
                None,
            )
            calibrated_hours = (
                cast(tuple[float, ...], seasonality.parameters["hour_weights"])
                if seasonality and "hour_weights" in seasonality.parameters
                else ()
            )
            if calibrated_hours and sum(calibrated_hours[hour] for hour in valid_hours) > 0:
                hour_weights = tuple(
                    calibrated_hours[hour] * max(weight, 0.01)
                    for hour, weight in zip(valid_hours, hour_weights, strict=True)
                )
        timing_rng = self._calibrated_time_rng or rng
        hour = _weighted_choice(timing_rng, valid_hours, hour_weights)
        event_time = datetime.combine(current_day, time(hour), tzinfo=UTC) + timedelta(
            minutes=rng.randrange(60)
        )
        if event_time < self.start:
            return self.start
        if event_time >= self.end:
            return self.end - timedelta(microseconds=1)
        return event_time

    def _sample_amount(self, profile: BehaviorProfile, rng: Random) -> float:
        if (
            self.calibration is not None
            and self.calibration.enabled
            and self.calibration.profile
            and "amount_distribution" in self.config.calibration.summary_names
        ):
            distribution = next(
                (item for item in self.calibration.profile.distributions if item.name == "amount"),
                None,
            )
            if distribution and distribution.quantiles:
                assert self._calibrated_amount_rng is not None
                sampled = self._calibrated_amount_rng.choice(distribution.quantiles)
                return round(
                    min(
                        self.config.behavior.amount_max,
                        max(self.config.behavior.amount_min, sampled),
                    ),
                    2,
                )
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
            account
            for account in self.accounts
            if account.account_id != payer_account_id and account.status == "ACTIVE"
        )
        if require_pix_key:
            keyed = tuple(
                account
                for account in alternatives
                if account.account_id in self.pix_keys_by_account
                and any(
                    key.status == "ACTIVE" for key in self.pix_keys_by_account[account.account_id]
                )
            )
            alternatives = keyed or tuple(
                account
                for account in self.accounts
                if account.status == "ACTIVE" and account.account_id in self.pix_keys_by_account
            )
        return rng.choice(alternatives or self.accounts)

    def _initiated_at(self, profile: BehaviorProfile, rail: PaymentRail, rng: Random) -> datetime:
        if rail == "CARD":
            return self._sample_lifecycle_time(
                profile,
                rng,
                self._card_max_delay_seconds,
                CARD_EVENT_ENVELOPE_DELAY_SECONDS,
                "card",
            )
        if rail == "PIX":
            return self._sample_lifecycle_time(
                profile,
                rng,
                self._pix_max_delay_seconds,
                PIX_EVENT_ENVELOPE_DELAY_SECONDS,
                "PIX",
            )
        return self._sample_time(profile, rng)

    def _card_details(
        self,
        profile: BehaviorProfile,
        initiated_at: datetime,
        amount: float,
        rng: Random,
    ) -> _PaymentDetails:
        cards = tuple(
            card
            for card in self.cards_by_customer[profile.customer_id]
            if card.status == "ACTIVE" and card.expires_at > initiated_at
        )
        if not cards:
            raise ValueError("card payment requires an active, unexpired card")
        card = rng.choice(cards)
        payer_account_id = card.account_id
        payer_institution_id = self.accounts_by_id[payer_account_id].institution_id
        merchant = self._merchant(profile, rng)
        settlement_accounts = tuple(
            account
            for account in self.accounts_by_institution.get(merchant.acquirer_id, ())
            if account.status == "ACTIVE" and account.account_id != payer_account_id
        )
        payee_account_id = (
            settlement_accounts[0].account_id if settlement_accounts else payer_account_id
        )
        amount = min(amount, card.transaction_limit)
        spent = self._card_daily_spend.get((card.card_id, initiated_at.date()), 0.0)
        remaining = max(0.0, card.daily_limit - spent)
        if remaining < self.config.behavior.amount_min:
            raise ValueError("card daily limit exhausted")
        amount = min(amount, remaining)
        account = self.accounts_by_id[payer_account_id]
        account_spend = self._account_spend.get(account.account_id, 0.0)
        available = max(
            account.available_balance - account_spend,
            account.ledger_balance + account.credit_limit + account.overdraft_limit - account_spend,
        )
        if available < self.config.behavior.amount_min:
            raise ValueError("account spendable balance exhausted")
        amount = min(amount, available)
        self._card_daily_spend[(card.card_id, initiated_at.date())] = round(spent + amount, 2)
        self._account_spend[account.account_id] = round(account_spend + amount, 2)
        return _PaymentDetails(
            amount=amount,
            merchant_id=merchant.merchant_id,
            card_id=card.card_id,
            payer_account_id=payer_account_id,
            payee_account_id=payee_account_id,
            payer_institution_id=payer_institution_id,
            payee_institution_id=merchant.acquirer_id,
            payer_pix_key_id=None,
            payee_pix_key_id=None,
            payment_type="PURCHASE",
            event_type="CARD_PAYMENT_INITIATED",
            online=merchant.online_only or rng.random() < profile.online_purchase_rate,
        )

    def _transfer_details(
        self,
        profile: BehaviorProfile,
        rail: PaymentRail,
        amount: float,
        rng: Random,
    ) -> _PaymentDetails:
        payer_candidates = tuple(
            account
            for account in self.accounts_by_customer[profile.customer_id]
            if account.status == "ACTIVE"
        )
        if rail == "PIX":
            keyed_payers = tuple(
                account
                for account in payer_candidates
                if any(
                    key.status == "ACTIVE"
                    for key in self.pix_keys_by_account.get(account.account_id, ())
                )
            )
            payer_candidates = keyed_payers or payer_candidates
        payer_account = rng.choice(payer_candidates)
        payee_account = self._payee_account(
            payer_account.account_id, rng, require_pix_key=rail == "PIX"
        )
        payer_keys = self.pix_keys_by_account.get(payer_account.account_id, ())
        payee_keys = self.pix_keys_by_account.get(payee_account.account_id, ())
        return _PaymentDetails(
            amount=amount,
            merchant_id=None,
            card_id=None,
            payer_account_id=payer_account.account_id,
            payee_account_id=payee_account.account_id,
            payer_institution_id=payer_account.institution_id,
            payee_institution_id=payee_account.institution_id,
            payer_pix_key_id=(payer_keys[0].pix_key_id if rail == "PIX" and payer_keys else None),
            payee_pix_key_id=(payee_keys[0].pix_key_id if rail == "PIX" and payee_keys else None),
            payment_type="TRANSFER",
            event_type="PIX_INITIATED" if rail == "PIX" else "TRANSFER_COMPLETED",
            online=False,
        )

    def _generate_one(
        self, number: int, profile: BehaviorProfile, rng: Random
    ) -> tuple[Payment, PaymentEvent]:
        rail = self._choose_rail(profile, rng)
        initiated_at = self._initiated_at(profile, rail, rng)
        amount = self._sample_amount(profile, rng)
        device_id = self._device_id(profile, rng)
        details = (
            self._card_details(profile, initiated_at, amount, rng)
            if rail == "CARD"
            else self._transfer_details(profile, rail, amount, rng)
        )

        payment_id = f"PAY-{number:0{_ID_WIDTH}d}"
        event_id = f"EVT-{number:0{_ID_WIDTH}d}"
        payment = Payment(
            payment_id=payment_id,
            payment_rail=rail,
            payment_type=details.payment_type,
            payer_account_id=details.payer_account_id,
            payee_account_id=details.payee_account_id,
            merchant_id=details.merchant_id,
            card_id=details.card_id,
            amount=details.amount,
            currency=self.accounts_by_id[details.payer_account_id].currency,
            initiated_at=initiated_at,
            current_status="SETTLED" if rail == "PIX" else "COMPLETED",
            payer_institution_id=details.payer_institution_id,
            payee_institution_id=details.payee_institution_id,
            payer_pix_key_id=details.payer_pix_key_id,
            payee_pix_key_id=details.payee_pix_key_id,
        )
        source_available_at, ingested_at, processed_at = payment_event_times(initiated_at, rail)
        event = PaymentEvent(
            event_id=event_id,
            event_type=details.event_type,
            event_version=1,
            payment_id=payment_id,
            customer_id=profile.customer_id,
            account_id=details.payer_account_id,
            event_time=initiated_at,
            source_created_at=initiated_at,
            source_available_at=source_available_at,
            ingested_at=ingested_at,
            processed_at=processed_at,
            producer="fraudtwin.behavior",
            source_system="synthetic_payment_source",
            schema_version=PAYMENT_EVENT_CONTRACT_VERSION,
            correlation_id=payment_id,
            causation_id=None,
            simulation_run_id=self.simulation_run_id,
            scenario_id=None,
            payment_rail=rail,
            payment_type=details.payment_type,
            payee_account_id=details.payee_account_id,
            merchant_id=details.merchant_id,
            card_id=details.card_id,
            device_id=device_id,
            online=details.online,
            amount=details.amount,
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
                CARD_SOURCE_DELAY_SECONDS,
            )

        append("CARD_AUTHORIZATION_REQUESTED", settings.authorization_delay_seconds)
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
                if rng.random() < settings.chargeback_probability:
                    append("CARD_CHARGEBACK_CREATED", settings.chargeback_delay_seconds)
                    append(
                        "CARD_CHARGEBACK_RESOLVED",
                        settings.chargeback_resolution_delay_seconds,
                    )
                    final_status = "CHARGEBACK_RESOLVED"

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
                PIX_SOURCE_DELAY_SECONDS,
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
            if rng.random() < settings.timeout_probability:
                append("PIX_TIMEOUT", settings.timeout_delay_seconds)
                final_status = "TIMED_OUT"
            else:
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
            if event.event_type in {"PIX_SETTLED", "TRANSFER_COMPLETED", "CARD_SETTLED"}:
                specs.extend(((event, payer, "DEBIT"), (event, payee, "CREDIT")))
            elif event.event_type in {
                "PIX_RETURNED",
                "CARD_REFUNDED",
                "CARD_CHARGEBACK_RESOLVED",
            }:
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
                ledger_specs.extend(self._ledger_specs(payment, payment_events))
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
