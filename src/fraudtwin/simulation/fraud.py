"""Deterministic, scenario-driven fraud generation for Milestone 6."""

from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from random import Random

from fraudtwin.config import (
    ACCOUNT_TRANSFER_EVENT_ENVELOPE_DELAY_SECONDS,
    ACCOUNT_TRANSFER_SOURCE_DELAY_SECONDS,
    CARD_EVENT_ENVELOPE_DELAY_SECONDS,
    CARD_SOURCE_DELAY_SECONDS,
    FRAUD_SCENARIO_IDS,
    PIX_EVENT_ENVELOPE_DELAY_SECONDS,
    PIX_SOURCE_DELAY_SECONDS,
    FraudScenarioId,
    FraudScenarioSettings,
    SimulationRunConfig,
)
from fraudtwin.domain import (
    Account,
    Card,
    Device,
    FraudRecord,
    FraudScenarioType,
    LedgerEntry,
    Merchant,
    Payment,
    PaymentEvent,
    PaymentEventType,
    PaymentRail,
    PaymentType,
    PixKey,
    validate_card_lifecycle,
)
from fraudtwin.seed import create_stream_rng
from fraudtwin.simulation.payments import PaymentDataset, PaymentGenerator

_ID_WIDTH = 6


def scenario_events(events: tuple[PaymentEvent, ...]) -> tuple[PaymentEvent, ...]:
    """Return payment events produced by a fraud scenario."""

    return tuple(event for event in events if event.scenario_id is not None)


def count_true_fraud_records(records: tuple[FraudRecord, ...]) -> dict[str, int]:
    """Count records with scenario-established fraud truth."""

    counts: dict[str, int] = defaultdict(int)
    for record in records:
        if record.fraud_truth:
            counts[record.scenario_type] += 1
    return dict(counts)


@dataclass(frozen=True)
class FraudDataset:
    """Complete payment stream plus explainable M6 truth records."""

    payments: tuple[Payment, ...]
    payment_events: tuple[PaymentEvent, ...]
    ledger_entries: tuple[LedgerEntry, ...]
    fraud_records: tuple[FraudRecord, ...]

    @property
    def fraud_events(self) -> tuple[PaymentEvent, ...]:
        return scenario_events(self.payment_events)

    @property
    def fraud_record_counts(self) -> dict[str, int]:
        return count_true_fraud_records(self.fraud_records)


class FraudScenarioGenerator:
    """Add only explicit F01-F05 campaigns to an existing legitimate stream."""

    def __init__(
        self,
        config: SimulationRunConfig,
        accounts: tuple[Account, ...],
        cards: tuple[Card, ...],
        merchants: tuple[Merchant, ...],
        devices: tuple[Device, ...],
        pix_keys: tuple[PixKey, ...],
        baseline: PaymentDataset,
        simulation_run_id: str | None = None,
    ) -> None:
        self.config = config
        self.start = config.simulation.start.astimezone(UTC)
        self.end = self.start + timedelta(days=config.simulation.duration_days)
        self.accounts = accounts
        self.cards = cards
        self.merchants = merchants
        self.devices = devices
        self.pix_keys = pix_keys
        self.baseline = baseline
        self.payment_generator = PaymentGenerator(
            config,
            accounts,
            cards,
            merchants,
            devices,
            pix_keys,
            simulation_run_id=simulation_run_id,
        )
        self.accounts_by_id = {account.account_id: account for account in accounts}
        self.pix_keys_by_account: dict[str, tuple[PixKey, ...]] = defaultdict(tuple)
        for key in pix_keys:
            self.pix_keys_by_account[key.account_id] += (key,)
        self.outgoing_by_account: dict[str, float] = defaultdict(float)
        payments_by_id = {payment.payment_id: payment for payment in baseline.payments}
        for event in baseline.payment_events:
            if event.event_type in {"PIX_SETTLED", "TRANSFER_COMPLETED"}:
                self.outgoing_by_account[payments_by_id[event.payment_id].payer_account_id] += (
                    event.amount
                )
        self.untrusted_devices = tuple(device for device in devices if not device.trusted)

    def generate(self) -> FraudDataset:
        """Generate deterministic campaigns and preserve baseline output when disabled."""

        if not self.config.fraud.enabled or self.config.fraud.target_rate <= 0:
            return FraudDataset(
                self.baseline.payments,
                self.baseline.payment_events,
                self.baseline.ledger_entries,
                (),
            )

        settings_by_id = self.config.fraud.scenarios
        candidates = tuple(
            scenario_id
            for scenario_id in FRAUD_SCENARIO_IDS
            if (
                scenario_id in settings_by_id
                and settings_by_id[scenario_id].enabled
                and settings_by_id[scenario_id].weight > 0
                and settings_by_id[scenario_id].count > 0
            )
        )
        campaign_count = min(
            self.config.fraud.scenario_count,
            int(len(self.baseline.payments) * self.config.fraud.target_rate),
            sum(settings_by_id[scenario_id].count for scenario_id in candidates),
        )
        if not candidates or campaign_count == 0:
            return FraudDataset(
                self.baseline.payments,
                self.baseline.payment_events,
                self.baseline.ledger_entries,
                (),
            )

        selected = self._select_scenarios(campaign_count, candidates, settings_by_id)

        payments = list(self.baseline.payments)
        events = list(self.baseline.payment_events)
        records: list[FraudRecord] = []
        for campaign_number, scenario_type in enumerate(selected, start=1):
            scenario_id = f"{scenario_type}-{campaign_number:0{_ID_WIDTH}d}"
            settings = settings_by_id[scenario_type]
            rng = create_stream_rng(
                self.config.simulation.seed, f"milestone-6:{scenario_type}:{campaign_number}"
            )
            new_payments, new_events, new_records = self._generate_campaign(
                scenario_type, scenario_id, settings, rng
            )
            payments.extend(new_payments)
            events.extend(new_events)
            records.extend(new_records)
            if self.config.fraud.hard_negative_rate > 0 and self.baseline.payments:
                hard_negative = self._hard_negative_campaign(
                    scenario_type, campaign_number, settings
                )
                if hard_negative is not None:
                    negative_payments, negative_events, negative_record = hard_negative
                    payments.extend(negative_payments)
                    events.extend(negative_events)
                    records.append(negative_record)

        payment_tuple = tuple(payments)
        event_tuple = tuple(events)
        ledger = self.payment_generator.materialize_ledger(payment_tuple, event_tuple)
        return FraudDataset(payment_tuple, event_tuple, ledger, tuple(records))

    def _select_scenarios(
        self,
        campaign_count: int,
        candidates: tuple[FraudScenarioType, ...],
        settings_by_id: Mapping[FraudScenarioId, FraudScenarioSettings],
    ) -> tuple[FraudScenarioType, ...]:
        """Select bounded campaigns with one deterministic stream."""

        selection_rng = create_stream_rng(self.config.simulation.seed, "milestone-6:selection")
        selected: list[FraudScenarioType] = list(candidates[:campaign_count])
        selected_counts = {scenario_id: selected.count(scenario_id) for scenario_id in candidates}
        while len(selected) < campaign_count:
            available = tuple(
                scenario_id
                for scenario_id in candidates
                if selected_counts[scenario_id] < settings_by_id[scenario_id].count
            )
            weights = tuple(settings_by_id[scenario_id].weight for scenario_id in available)
            selected_scenario = selection_rng.choices(available, weights=weights, k=1)[0]
            selected.append(selected_scenario)
            selected_counts[selected_scenario] += 1
        return tuple(selected)

    def _generate_campaign(
        self,
        scenario_type: FraudScenarioType,
        scenario_id: str,
        settings: FraudScenarioSettings,
        rng: Random,
    ) -> tuple[list[Payment], list[PaymentEvent], list[FraudRecord]]:
        if scenario_type == "F01":
            return self._card_campaign(
                scenario_type, scenario_id, settings, rng, 3, "CNP_NEW_DEVICE"
            )
        if scenario_type == "F02":
            return self._card_campaign(
                scenario_type,
                scenario_id,
                settings,
                rng,
                settings.attempt_count,
                "LOW_VALUE_AUTHORIZATION_BURST",
            )
        if scenario_type == "F03":
            return self._account_takeover(scenario_type, scenario_id, settings, rng)
        if scenario_type == "F04":
            return self._pix_scam(scenario_type, scenario_id, settings, rng)
        return self._card_campaign(
            scenario_type,
            scenario_id,
            settings,
            rng,
            settings.attempt_count,
            "PAYMENT_VELOCITY_BURST",
        )

    def _card_campaign(
        self,
        scenario_type: FraudScenarioType,
        scenario_id: str,
        settings: FraudScenarioSettings,
        rng: Random,
        count: int,
        trigger: str,
    ) -> tuple[list[Payment], list[PaymentEvent], list[FraudRecord]]:
        target = self._card_target(rng)
        if target is None:
            return [], [], []
        card, device = target
        payments: list[Payment] = []
        events: list[PaymentEvent] = []
        records: list[FraudRecord] = []
        for attempt in range(1, count + 1):
            merchant = self._merchant_for_attempt(attempt, rng)
            generated = self._card_attempt(
                scenario_type,
                scenario_id,
                settings,
                rng,
                count,
                trigger,
                attempt,
                card,
                device,
                merchant,
            )
            if generated is None:
                continue
            payment, payment_events, record = generated
            payments.append(payment)
            events.extend(payment_events)
            records.append(record)
        return payments, events, records

    def _card_attempt(
        self,
        scenario_type: FraudScenarioType,
        scenario_id: str,
        settings: FraudScenarioSettings,
        rng: Random,
        count: int,
        trigger: str,
        attempt: int,
        card: Card,
        device: Device | None,
        merchant: Merchant,
    ) -> tuple[Payment, tuple[PaymentEvent, ...], FraudRecord] | None:
        """Generate one card scenario attempt and its explainable record."""

        generated = self._card_payment(
            scenario_type,
            scenario_id,
            settings,
            rng,
            count,
            trigger,
            attempt,
            card,
            device,
            merchant,
            force_scenario_decline=True,
        )
        if generated is None:
            return None
        payment, payment_events = generated
        record_id = f"FRD-{scenario_id}-{attempt:0{_ID_WIDTH}d}"
        affected = (card.customer_id, card.account_id, card.card_id, merchant.merchant_id)
        reason = self._reason(scenario_type, trigger, device.device_id if device else None)
        payment_events = self._annotate_events(
            payment_events,
            scenario_id,
            scenario_type,
            trigger,
            reason,
            record_id,
            affected,
        )
        return (
            payment,
            payment_events,
            self._record(
                record_id,
                scenario_id,
                scenario_type,
                trigger,
                reason,
                payment,
                payment_events[0],
                affected,
                device.device_id if device else None,
            ),
        )

    def _card_payment(
        self,
        scenario_type: FraudScenarioType,
        scenario_id: str,
        settings: FraudScenarioSettings,
        rng: Random,
        count: int,
        trigger: str,
        attempt: int,
        card: Card,
        device: Device | None,
        merchant: Merchant,
        *,
        force_scenario_decline: bool,
    ) -> tuple[Payment, tuple[PaymentEvent, ...]] | None:
        """Generate one card payment with optional scenario-forced decline."""

        amount = self._amount(settings, rng, low_value=scenario_type == "F02")
        if amount is None:
            return None
        offset = attempt - 1
        if trigger in {"LOW_VALUE_AUTHORIZATION_BURST", "PAYMENT_VELOCITY_BURST"}:
            offset = ((attempt - 1) * settings.window_seconds) // count
        payment, initial = self._initial_payment(
            f"PAY-{scenario_id}-{attempt:0{_ID_WIDTH}d}",
            f"EVT-{scenario_id}-{attempt:0{_ID_WIDTH}d}",
            "CARD",
            "PURCHASE",
            card.account_id,
            card.customer_id,
            amount,
            self._start_time(settings, rail="CARD", offset=offset),
            card_id=card.card_id,
            merchant_id=merchant.merchant_id,
            device_id=device.device_id if device else None,
            online=True,
            event_type="CARD_AUTHORIZATION_REQUESTED",
        )
        payment, payment_events = self.payment_generator._card_lifecycle(payment, initial, rng)
        should_decline = force_scenario_decline and (
            (scenario_type == "F01" and attempt < count)
            or (scenario_type == "F02" and attempt % 4 != 0)
        )
        if should_decline:
            declined = payment_events[1].model_copy(update={"event_type": "CARD_DECLINED"})
            payment = payment.model_copy(update={"current_status": "DECLINED"})
            payment_events = (payment_events[0], declined)
            validate_card_lifecycle(payment, payment_events)
        return payment, payment_events

    def _account_takeover(
        self,
        scenario_type: FraudScenarioType,
        scenario_id: str,
        settings: FraudScenarioSettings,
        rng: Random,
    ) -> tuple[list[Payment], list[PaymentEvent], list[FraudRecord]]:
        pair = self._account_pair(require_pix=False)
        if pair is None:
            return [], [], []
        payer, payee = pair
        customer_id = payer.customer_id
        device = self._choose_device(rng)
        reason = self._reason(
            scenario_type, "NEW_UNTRUSTED_DEVICE", device.device_id if device else None
        )
        amount = self._amount(settings, rng, capacity=self._capacity(payer) / 2)
        if amount is None:
            return [], [], []
        first, first_event = self._transfer_payment(
            scenario_id, 1, payer, payee, amount, device, None, settings
        )
        second, second_event = self._transfer_payment(
            scenario_id,
            2,
            payer,
            payee,
            amount,
            device,
            first_event.event_id,
            settings,
        )
        record_ids = (f"FRD-{scenario_id}-000001", f"FRD-{scenario_id}-000002")
        signal_types = (
            "FRAUD_AUTHENTICATION_SUSPICIOUS",
            "FRAUD_PROFILE_CHANGED",
            "FRAUD_BENEFICIARY_ADDED",
        )
        signal_events: list[PaymentEvent] = []
        previous_id: str | None = None
        for index, event_type in enumerate(signal_types, start=1):
            event_time = self._start_time(settings, rail="ACCOUNT_TRANSFER", offset=index - 1)
            signal = first_event.model_copy(
                update={
                    "event_id": f"EVT-{scenario_id}-SIGNAL-{index:02d}",
                    "event_type": event_type,
                    "event_time": event_time,
                    "source_created_at": event_time,
                    "source_available_at": event_time
                    + timedelta(seconds=ACCOUNT_TRANSFER_SOURCE_DELAY_SECONDS),
                    "ingested_at": event_time
                    + timedelta(seconds=ACCOUNT_TRANSFER_SOURCE_DELAY_SECONDS + 1),
                    "processed_at": event_time
                    + timedelta(seconds=ACCOUNT_TRANSFER_SOURCE_DELAY_SECONDS + 2),
                    "causation_id": previous_id,
                    "scenario_id": scenario_id,
                    "scenario_type": scenario_type,
                    "scenario_trigger": "NEW_UNTRUSTED_DEVICE",
                    "scenario_reason": reason,
                    "fraud_record_id": record_ids[0],
                    "affected_entity_ids": (customer_id, payer.account_id, payee.account_id),
                }
            )
            signal_events.append(signal)
            previous_id = signal.event_id
        first_event = first_event.model_copy(
            update={"causation_id": signal_events[-1].event_id, "fraud_record_id": record_ids[0]}
        )
        second_event = second_event.model_copy(update={"fraud_record_id": record_ids[1]})
        first_event = self._annotate_events(
            (first_event,),
            scenario_id,
            scenario_type,
            "NEW_BENEFICIARY",
            reason,
            record_ids[0],
            (customer_id, payer.account_id, payee.account_id),
        )[0]
        second_event = self._annotate_events(
            (second_event,),
            scenario_id,
            scenario_type,
            "REPEAT_TRANSFER",
            reason,
            record_ids[1],
            (customer_id, payer.account_id, payee.account_id),
        )[0]
        payments = [first, second]
        events = [*signal_events, first_event, second_event]
        records = [
            self._record(
                record_ids[0],
                scenario_id,
                scenario_type,
                "NEW_BENEFICIARY",
                reason,
                first,
                first_event,
                (customer_id, payer.account_id, payee.account_id),
                device.device_id if device else None,
            ),
            self._record(
                record_ids[1],
                scenario_id,
                scenario_type,
                "REPEAT_TRANSFER",
                reason,
                second,
                second_event,
                (customer_id, payer.account_id, payee.account_id),
                device.device_id if device else None,
            ),
        ]
        return payments, events, records

    def _pix_scam(
        self,
        scenario_type: FraudScenarioType,
        scenario_id: str,
        settings: FraudScenarioSettings,
        rng: Random,
    ) -> tuple[list[Payment], list[PaymentEvent], list[FraudRecord]]:
        pair = self._account_pair(require_pix=True)
        if pair is None:
            return [], [], []
        payer, payee = pair
        key = self.pix_keys_by_account[payee.account_id][0]
        device = self._choose_device(rng)
        amount = self._amount(settings, rng, capacity=self._capacity(payer), high_value=True)
        if amount is None:
            return [], [], []
        reason = self._reason(
            scenario_type, "NEW_BENEFICIARY_HIGH_VALUE", device.device_id if device else None
        )
        payment, initial = self._initial_payment(
            f"PAY-{scenario_id}-000001",
            f"EVT-{scenario_id}-000001",
            "PIX",
            "TRANSFER",
            payer.account_id,
            payer.customer_id,
            amount,
            self._start_time(settings, rail="PIX"),
            payee_account_id=payee.account_id,
            payer_institution_id=payer.institution_id,
            payee_institution_id=payee.institution_id,
            payer_pix_key_id=self.pix_keys_by_account[payer.account_id][0].pix_key_id,
            payee_pix_key_id=key.pix_key_id,
            device_id=device.device_id if device else None,
            event_type="PIX_INITIATED",
        )
        payment, payment_events = self.payment_generator._pix_lifecycle(
            payment, initial, rng, always_approve=True
        )
        record_id = f"FRD-{scenario_id}-000001"
        payment_events = self._annotate_events(
            payment_events,
            scenario_id,
            scenario_type,
            "NEW_BENEFICIARY_HIGH_VALUE",
            reason,
            record_id,
            (payer.customer_id, payer.account_id, payee.account_id, key.pix_key_id),
        )
        record = self._record(
            record_id,
            scenario_id,
            scenario_type,
            "NEW_BENEFICIARY_HIGH_VALUE",
            reason,
            payment,
            payment_events[0],
            (payer.customer_id, payer.account_id, payee.account_id, key.pix_key_id),
            device.device_id if device else None,
        )
        return [payment], list(payment_events), [record]

    def _transfer_payment(
        self,
        scenario_id: str,
        number: int,
        payer: Account,
        payee: Account,
        amount: float,
        device: Device | None,
        causation_id: str | None,
        settings: FraudScenarioSettings,
    ) -> tuple[Payment, PaymentEvent]:
        return self._initial_payment(
            f"PAY-{scenario_id}-{number:0{_ID_WIDTH}d}",
            f"EVT-{scenario_id}-{number:0{_ID_WIDTH}d}",
            "ACCOUNT_TRANSFER",
            "TRANSFER",
            payer.account_id,
            payer.customer_id,
            amount,
            self._start_time(settings, rail="ACCOUNT_TRANSFER", offset=number + 2),
            payee_account_id=payee.account_id,
            payer_institution_id=payer.institution_id,
            payee_institution_id=payee.institution_id,
            device_id=device.device_id if device else None,
            event_type="TRANSFER_COMPLETED",
            causation_id=causation_id,
        )

    def _initial_payment(
        self,
        payment_id: str,
        event_id: str,
        rail: PaymentRail,
        payment_type: PaymentType,
        account_id: str,
        customer_id: str,
        amount: float,
        initiated_at: datetime,
        *,
        card_id: str | None = None,
        merchant_id: str | None = None,
        device_id: str | None = None,
        online: bool = False,
        event_type: PaymentEventType,
        payee_account_id: str | None = None,
        payer_institution_id: str | None = None,
        payee_institution_id: str | None = None,
        payer_pix_key_id: str | None = None,
        payee_pix_key_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Payment, PaymentEvent]:
        currency = self.accounts_by_id[account_id].currency
        payment = Payment(
            payment_id=payment_id,
            payment_rail=rail,
            payment_type=payment_type,
            payer_account_id=account_id,
            payee_account_id=payee_account_id,
            merchant_id=merchant_id,
            card_id=card_id,
            amount=amount,
            currency=currency,
            initiated_at=initiated_at,
            current_status="AUTHORIZED"
            if rail == "CARD"
            else "RECEIVED"
            if rail == "PIX"
            else "COMPLETED",
            payer_institution_id=payer_institution_id,
            payee_institution_id=payee_institution_id,
            payer_pix_key_id=payer_pix_key_id,
            payee_pix_key_id=payee_pix_key_id,
        )
        source_delay = {
            "CARD": CARD_SOURCE_DELAY_SECONDS,
            "PIX": PIX_SOURCE_DELAY_SECONDS,
            "ACCOUNT_TRANSFER": ACCOUNT_TRANSFER_SOURCE_DELAY_SECONDS,
        }[rail]
        event = PaymentEvent(
            event_id=event_id,
            event_type=event_type,
            event_version=1,
            payment_id=payment_id,
            customer_id=customer_id,
            account_id=account_id,
            event_time=initiated_at,
            source_created_at=initiated_at,
            source_available_at=initiated_at + timedelta(seconds=source_delay),
            ingested_at=initiated_at + timedelta(seconds=source_delay + 1),
            processed_at=initiated_at + timedelta(seconds=source_delay + 2),
            producer="fraudtwin.fraud",
            source_system="synthetic_fraud_source",
            schema_version="2" if rail == "CARD" else "3" if rail == "PIX" else "1",
            correlation_id=payment_id,
            causation_id=causation_id,
            simulation_run_id=self.payment_generator.simulation_run_id,
            scenario_id=None,
            payment_rail=rail,
            payment_type=payment_type,
            payee_account_id=payee_account_id,
            merchant_id=merchant_id,
            card_id=card_id,
            device_id=device_id,
            online=online,
            amount=amount,
            currency=currency,
        )
        return payment, event

    def _account_pair(self, *, require_pix: bool) -> tuple[Account, Account] | None:
        payers = sorted(self.accounts, key=self._capacity, reverse=True)
        for payer in payers:
            payees = tuple(
                account
                for account in self.accounts
                if account.account_id != payer.account_id
                and (not require_pix or account.account_id in self.pix_keys_by_account)
            )
            if (
                payees
                and (not require_pix or payer.account_id in self.pix_keys_by_account)
                and self._capacity(payer) > 0
            ):
                return payer, payees[0]
        return None

    def _capacity(self, account: Account) -> float:
        return max(
            0.0,
            account.ledger_balance
            + account.overdraft_limit
            - self.outgoing_by_account[account.account_id],
        )

    def _card_target(self, rng: Random) -> tuple[Card, Device | None] | None:
        """Choose a card and device for a card campaign."""

        if not self.cards or not self.merchants or not self.accounts:
            return None
        return self.cards[rng.randrange(len(self.cards))], self._choose_device(rng)

    def _choose_device(self, rng: Random) -> Device | None:
        device_pool = self.untrusted_devices or self.devices
        return device_pool[rng.randrange(len(device_pool))] if device_pool else None

    def _merchant_for_attempt(self, attempt: int, rng: Random) -> Merchant:
        return self.merchants[(attempt + rng.randrange(len(self.merchants))) % len(self.merchants)]

    def _hard_negative_campaign(
        self,
        scenario_type: FraudScenarioType,
        number: int,
        settings: FraudScenarioSettings,
    ) -> tuple[list[Payment], list[PaymentEvent], FraudRecord] | None:
        """Generate one legitimate sequence with the selected fraud signals."""

        rng = create_stream_rng(
            self.config.simulation.seed, f"milestone-6:hard-negative:{scenario_type}:{number}"
        )
        trigger, reason = self._lookalike_metadata(scenario_type)
        if scenario_type in {"F01", "F02", "F05"}:
            return self._card_hard_negative(scenario_type, number, settings, rng, trigger, reason)
        if scenario_type == "F03":
            return self._account_hard_negative(
                scenario_type, number, settings, rng, trigger, reason
            )
        return self._pix_hard_negative(number, settings, rng, trigger, reason)

    def _card_hard_negative(
        self,
        scenario_type: FraudScenarioType,
        number: int,
        settings: FraudScenarioSettings,
        rng: Random,
        trigger: str,
        reason: str,
    ) -> tuple[list[Payment], list[PaymentEvent], FraudRecord] | None:
        target = self._card_target(rng)
        if target is None:
            return None
        card, device = target
        count = 3 if scenario_type == "F01" else settings.attempt_count
        payments: list[Payment] = []
        events: list[PaymentEvent] = []
        for attempt in range(1, count + 1):
            merchant = self._merchant_for_attempt(attempt, rng)
            generated = self._card_payment(
                scenario_type,
                f"HN-{scenario_type}-{number:0{_ID_WIDTH}d}",
                settings,
                rng,
                count,
                trigger,
                attempt,
                card,
                device,
                merchant,
                force_scenario_decline=False,
            )
            if generated is None:
                continue
            payment, payment_events = generated
            affected = (card.customer_id, card.account_id, card.card_id, merchant.merchant_id)
            payment_events = self._annotate_lookalike_events(
                payment_events, scenario_type, trigger, reason, affected
            )
            payments.append(payment)
            events.extend(payment_events)
        if not payments or not events:
            return None
        return payments, events, self._hard_negative(scenario_type, number, payments[0], events[0])

    def _account_hard_negative(
        self,
        scenario_type: FraudScenarioType,
        number: int,
        settings: FraudScenarioSettings,
        rng: Random,
        trigger: str,
        reason: str,
    ) -> tuple[list[Payment], list[PaymentEvent], FraudRecord] | None:
        pair = self._account_pair(require_pix=False)
        if pair is None:
            return None
        payer, payee = pair
        device = self._choose_device(rng)
        amount = self._amount(settings, rng, capacity=self._capacity(payer) / 4)
        if amount is None:
            return None
        scenario_id = f"HN-F03-{number:0{_ID_WIDTH}d}"
        first, first_event = self._transfer_payment(
            scenario_id, 1, payer, payee, amount, device, None, settings
        )
        second, second_event = self._transfer_payment(
            scenario_id, 2, payer, payee, amount, device, first_event.event_id, settings
        )
        affected = (payer.customer_id, payer.account_id, payee.account_id)
        signals: list[PaymentEvent] = []
        previous_id: str | None = None
        for index, event_type in enumerate(
            (
                "FRAUD_AUTHENTICATION_SUSPICIOUS",
                "FRAUD_PROFILE_CHANGED",
                "FRAUD_BENEFICIARY_ADDED",
            ),
            start=1,
        ):
            event_time = self._start_time(settings, rail="ACCOUNT_TRANSFER", offset=index - 1)
            signals.append(
                first_event.model_copy(
                    update={
                        "event_id": f"EVT-{scenario_id}-SIGNAL-{index:02d}",
                        "event_type": event_type,
                        "event_time": event_time,
                        "source_created_at": event_time,
                        "source_available_at": event_time
                        + timedelta(seconds=ACCOUNT_TRANSFER_SOURCE_DELAY_SECONDS),
                        "ingested_at": event_time
                        + timedelta(seconds=ACCOUNT_TRANSFER_SOURCE_DELAY_SECONDS + 1),
                        "processed_at": event_time
                        + timedelta(seconds=ACCOUNT_TRANSFER_SOURCE_DELAY_SECONDS + 2),
                        "causation_id": previous_id,
                        "scenario_id": None,
                        "scenario_type": scenario_type,
                        "scenario_trigger": trigger,
                        "scenario_reason": reason,
                        "fraud_record_id": None,
                        "affected_entity_ids": affected,
                    }
                )
            )
            previous_id = signals[-1].event_id
        first_event = self._annotate_lookalike_events(
            (first_event.model_copy(update={"causation_id": signals[-1].event_id}),),
            scenario_type,
            trigger,
            reason,
            affected,
        )[0]
        second_event = self._annotate_lookalike_events(
            (second_event,), scenario_type, trigger, reason, affected
        )[0]
        return (
            [first, second],
            [*signals, first_event, second_event],
            self._hard_negative(scenario_type, number, first, first_event),
        )

    def _pix_hard_negative(
        self,
        number: int,
        settings: FraudScenarioSettings,
        rng: Random,
        trigger: str,
        reason: str,
    ) -> tuple[list[Payment], list[PaymentEvent], FraudRecord] | None:
        pair = self._account_pair(require_pix=True)
        if pair is None:
            return None
        payer, payee = pair
        payer_key = self.pix_keys_by_account[payer.account_id][0]
        payee_key = self.pix_keys_by_account[payee.account_id][0]
        device = self._choose_device(rng)
        amount = self._amount(settings, rng, capacity=self._capacity(payer), high_value=True)
        if amount is None:
            return None
        scenario_id = f"HN-F04-{number:0{_ID_WIDTH}d}"
        payment, initial = self._initial_payment(
            f"PAY-{scenario_id}-000001",
            f"EVT-{scenario_id}-000001",
            "PIX",
            "TRANSFER",
            payer.account_id,
            payer.customer_id,
            amount,
            self._start_time(settings, rail="PIX"),
            payee_account_id=payee.account_id,
            payer_institution_id=payer.institution_id,
            payee_institution_id=payee.institution_id,
            payer_pix_key_id=payer_key.pix_key_id,
            payee_pix_key_id=payee_key.pix_key_id,
            device_id=device.device_id if device else None,
            event_type="PIX_INITIATED",
        )
        payment, payment_events = self.payment_generator._pix_lifecycle(payment, initial, rng)
        affected = (payer.customer_id, payer.account_id, payee.account_id, payee_key.pix_key_id)
        events = self._annotate_lookalike_events(payment_events, "F04", trigger, reason, affected)
        return [payment], list(events), self._hard_negative("F04", number, payment, events[0])

    @staticmethod
    def _lookalike_metadata(scenario_type: FraudScenarioType) -> tuple[str, str]:
        return {
            "F01": (
                "TRAVEL_NEW_DEVICE",
                "legitimate travel and a new device resemble card-not-present signals",
            ),
            "F02": (
                "LEGITIMATE_LOW_VALUE_BURST",
                "legitimate repeated low-value authorizations resemble card testing",
            ),
            "F03": (
                "NEW_PHONE_TRAVEL_LEGITIMATE_BENEFICIARY",
                "legitimate phone replacement and beneficiary transfer resemble takeover behavior",
            ),
            "F04": (
                "LEGITIMATE_NEW_BENEFICIARY",
                "legitimate new beneficiary transfer resembles an instant-payment scam",
            ),
            "F05": (
                "LEGITIMATE_ACTIVITY_BURST",
                "legitimate activity burst resembles a transaction velocity attack",
            ),
        }[scenario_type]

    def _amount(
        self,
        settings: FraudScenarioSettings,
        rng: Random,
        *,
        low_value: bool = False,
        high_value: bool = False,
        capacity: float | None = None,
    ) -> float | None:
        lower = settings.amount_min or self.config.behavior.amount_min
        upper = settings.amount_max or self.config.behavior.amount_max
        if low_value:
            upper = min(upper, max(lower, self.config.behavior.amount_min * 2.0))
        if high_value:
            lower = max(lower, upper * 0.75)
        if capacity is not None:
            upper = min(upper, capacity)
        if upper < lower or upper <= 0:
            return None
        return round(max(lower, rng.uniform(lower, upper)), 2)

    def _start_time(
        self, settings: FraudScenarioSettings, *, rail: str, offset: int = 0
    ) -> datetime:
        lifecycle = (
            self.config.card_lifecycle.maximum_delay_seconds + CARD_EVENT_ENVELOPE_DELAY_SECONDS
            if rail == "CARD"
            else self.config.pix_lifecycle.maximum_delay_seconds + PIX_EVENT_ENVELOPE_DELAY_SECONDS
            if rail == "PIX"
            else ACCOUNT_TRANSFER_EVENT_ENVELOPE_DELAY_SECONDS
        )
        latest = self.end - timedelta(seconds=lifecycle + settings.duration_seconds, microseconds=1)
        base = self.start + timedelta(seconds=offset)
        if latest <= self.start:
            return self.start
        return min(base, latest)

    @staticmethod
    def _reason(scenario_type: FraudScenarioType, trigger: str, device_id: str | None) -> str:
        device_part = f" using device {device_id}" if device_id else ""
        return f"{scenario_type} trigger {trigger}{device_part}; coordinated scenario behavior"

    @staticmethod
    def _annotate_events(
        events: tuple[PaymentEvent, ...],
        scenario_id: str,
        scenario_type: FraudScenarioType,
        trigger: str,
        reason: str,
        record_id: str,
        affected: tuple[str, ...],
    ) -> tuple[PaymentEvent, ...]:
        return tuple(
            event.model_copy(
                update={
                    "scenario_id": scenario_id,
                    "scenario_type": scenario_type,
                    "scenario_trigger": trigger,
                    "scenario_reason": reason,
                    "fraud_record_id": record_id,
                    "affected_entity_ids": affected,
                }
            )
            for event in events
        )

    @staticmethod
    def _annotate_lookalike_events(
        events: tuple[PaymentEvent, ...],
        scenario_type: FraudScenarioType,
        trigger: str,
        reason: str,
        affected: tuple[str, ...],
    ) -> tuple[PaymentEvent, ...]:
        """Add lookalike context without marking an event as fraud."""

        return tuple(
            event.model_copy(
                update={
                    "scenario_id": None,
                    "scenario_type": scenario_type,
                    "scenario_trigger": trigger,
                    "scenario_reason": reason,
                    "fraud_record_id": None,
                    "affected_entity_ids": affected,
                }
            )
            for event in events
        )

    @staticmethod
    def _record(
        record_id: str,
        scenario_id: str,
        scenario_type: FraudScenarioType,
        trigger: str,
        reason: str,
        payment: Payment,
        event: PaymentEvent,
        affected: tuple[str, ...],
        device_id: str | None,
    ) -> FraudRecord:
        return FraudRecord(
            fraud_record_id=record_id,
            record_type="FRAUD",
            scenario_id=scenario_id,
            scenario_type=scenario_type,
            fraud_truth=True,
            trigger=trigger,
            reason=reason,
            customer_id=event.customer_id,
            account_id=payment.payer_account_id,
            card_id=payment.card_id,
            device_id=device_id,
            merchant_id=payment.merchant_id,
            payment_id=payment.payment_id,
            event_id=event.event_id,
            occurred_at=event.event_time,
            amount=payment.amount,
            currency=payment.currency,
            correlation_id=event.correlation_id,
            causation_id=event.causation_id,
            affected_entity_ids=affected,
        )

    @staticmethod
    def _hard_negative(
        scenario_type: FraudScenarioType, number: int, payment: Payment, event: PaymentEvent
    ) -> FraudRecord:
        record_id = f"HN-{scenario_type}-{number:0{_ID_WIDTH}d}"
        return FraudRecord(
            fraud_record_id=record_id,
            record_type="HARD_NEGATIVE",
            scenario_id=record_id,
            scenario_type=scenario_type,
            fraud_truth=False,
            trigger=event.scenario_trigger or "LEGITIMATE_LOOKALIKE",
            reason=event.scenario_reason
            or f"legitimate {scenario_type} lookalike retained as non-fraud",
            customer_id=event.customer_id,
            account_id=payment.payer_account_id,
            card_id=payment.card_id,
            device_id=event.device_id,
            merchant_id=payment.merchant_id,
            payment_id=payment.payment_id,
            event_id=event.event_id,
            occurred_at=event.event_time,
            amount=payment.amount,
            currency=payment.currency,
            correlation_id=event.correlation_id,
            causation_id=event.causation_id,
            affected_entity_ids=event.affected_entity_ids or (payment.payer_account_id,),
        )
