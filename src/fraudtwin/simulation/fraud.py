"""Deterministic, scenario-driven fraud generation."""

from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from random import Random

from fraudtwin.config import (
    ACCOUNT_TRANSFER_EVENT_ENVELOPE_DELAY_SECONDS,
    CARD_EVENT_ENVELOPE_DELAY_SECONDS,
    FRAUD_SCENARIO_IDS,
    PIX_EVENT_ENVELOPE_DELAY_SECONDS,
    FraudRegimeConfig,
    FraudScenarioId,
    FraudScenarioSettings,
    SimulationRunConfig,
)
from fraudtwin.difficulty import apply_difficulty, resolve_difficulty
from fraudtwin.domain import (
    PAYMENT_EVENT_CONTRACT_VERSION,
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
from fraudtwin.simulation.payments import PaymentDataset, PaymentGenerator, payment_event_times

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
        self._campaign_anchor: datetime | None = None
        self._active_camouflage = 0.0
        self._difficulty = resolve_difficulty(config)
        self._active_plan = apply_difficulty(self._difficulty, "F01")
        self._difficulty_choice_counter = 0

    def generate(self) -> FraudDataset:
        """Generate deterministic campaigns and preserve baseline output when disabled."""

        self._difficulty_choice_counter = 0

        baseline_result = FraudDataset(
            self.baseline.payments,
            self.baseline.payment_events,
            self.baseline.ledger_entries,
            (),
        )
        if not self.config.fraud.enabled or self.config.fraud.target_rate <= 0:
            return baseline_result

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
        prevalence_strength = (
            self._difficulty.resolved_controls.get("prevalence", 0.0)
            if self._difficulty.enabled
            else 0.0
        )
        effective_target_rate = self.config.fraud.target_rate * (1.0 - 0.5 * prevalence_strength)
        effective_scenario_count = self.config.fraud.scenario_count
        if self._difficulty.enabled:
            effective_scenario_count = max(
                1,
                round(self.config.fraud.scenario_count * (1.0 - 0.75 * prevalence_strength)),
            )
        campaign_count = min(
            effective_scenario_count,
            int(len(self.baseline.payments) * effective_target_rate),
            sum(settings_by_id[scenario_id].count for scenario_id in candidates),
        )
        if not candidates or campaign_count == 0:
            return baseline_result

        selected = self._select_scenarios(campaign_count, candidates, settings_by_id)
        campaign_plan = self._campaign_plan(campaign_count, candidates, settings_by_id, selected)

        payments = list(self.baseline.payments)
        events = list(self.baseline.payment_events)
        records: list[FraudRecord] = []
        for campaign_number, (scenario_type, regime) in enumerate(campaign_plan, start=1):
            scenario_id = f"{scenario_type}-{campaign_number:0{_ID_WIDTH}d}"
            settings = self._settings_for_regime(settings_by_id[scenario_type], regime)
            self._active_plan = apply_difficulty(self._difficulty, scenario_type)
            if self._difficulty.enabled:
                settings = settings.model_copy(
                    update={
                        "window_seconds": max(
                            1,
                            round(settings.window_seconds * self._active_plan.timing_multiplier),
                        ),
                        "duration_seconds": max(
                            0,
                            round(settings.duration_seconds * self._active_plan.timing_multiplier),
                        ),
                    }
                )
            self._campaign_anchor = self._campaign_anchor_time(regime, campaign_number)
            self._active_camouflage = regime.camouflage if regime is not None else 0.0
            rng = create_stream_rng(
                self.config.simulation.seed, f"milestone-6:{scenario_type}:{campaign_number}"
            )
            new_payments, new_events, new_records = self._generate_campaign(
                scenario_type, scenario_id, settings, rng
            )
            payments.extend(new_payments)
            events.extend(new_events)
            records.extend(new_records)
            negative_count = 1
            if self._difficulty.enabled:
                negative_count = max(
                    1,
                    round(
                        self._active_plan.hard_negative_multiplier
                        * self.config.fraud.hard_negative_rate
                    ),
                )
            if self.config.fraud.hard_negative_rate > 0 and self.baseline.payments:
                for negative_number in range(negative_count):
                    hard_negative = self._hard_negative_campaign(
                        scenario_type,
                        campaign_number + negative_number * max(1, campaign_count),
                        settings,
                    )
                    if hard_negative is None:
                        continue
                    negative_payments, negative_events, negative_record = hard_negative
                    payments.extend(negative_payments)
                    events.extend(negative_events)
                    records.append(negative_record)

        payment_tuple = tuple(payments)
        event_tuple = tuple(events)
        ledger = self.payment_generator.materialize_ledger(
            payment_tuple,
            event_tuple,
            stage="fraud payment ledger",
            protocol_id="P01",
            capacity_id="C04",
        )
        return FraudDataset(payment_tuple, event_tuple, ledger, tuple(records))

    def _campaign_plan(
        self,
        campaign_count: int,
        candidates: tuple[FraudScenarioId, ...],
        settings_by_id: Mapping[FraudScenarioId, FraudScenarioSettings],
        selected: tuple[FraudScenarioType, ...],
    ) -> tuple[tuple[FraudScenarioType, FraudRegimeConfig | None], ...]:
        """Assign campaigns to declared regimes without changing legacy output."""

        regimes = tuple(sorted(self.config.backtest.regimes, key=lambda item: item.from_time))
        if not regimes:
            return tuple((scenario, None) for scenario in selected)

        total_seconds = sum(
            max(1.0, (regime.to_time - regime.from_time).total_seconds())
            * regime.prevalence_multiplier
            for regime in regimes
        )
        history_seconds = max(1.0, (self.end - self.start).total_seconds())
        average_prevalence = total_seconds / history_seconds
        maximum_campaigns = sum(settings_by_id[scenario].count for scenario in candidates)
        regime_count = min(
            maximum_campaigns,
            max(0, round(campaign_count * average_prevalence)),
        )
        if regime_count == 0:
            return ()

        rng = create_stream_rng(self.config.simulation.seed, "milestone-10:regime-selection")
        selected_counts: dict[FraudScenarioId, int] = dict.fromkeys(candidates, 0)
        plan: list[tuple[FraudScenarioType, FraudRegimeConfig]] = []
        for _ in range(regime_count):
            available_regimes = tuple(
                regime for regime in regimes if regime.prevalence_multiplier > 0
            )
            if not available_regimes:
                break
            regime_weights = tuple(
                max(1.0, (regime.to_time - regime.from_time).total_seconds())
                * regime.prevalence_multiplier
                for regime in available_regimes
            )
            regime = rng.choices(available_regimes, weights=regime_weights, k=1)[0]
            available = tuple(
                scenario
                for scenario in candidates
                if selected_counts[scenario] < settings_by_id[scenario].count
            )
            if not available:
                break
            scenario_weights = tuple(
                settings_by_id[scenario].weight
                * (regime.scenario_mix.get(scenario, 0.0) if regime.scenario_mix else 1.0)
                * self._rail_weight(scenario, regime)
                for scenario in available
            )
            if not any(scenario_weights):
                scenario_weights = tuple(settings_by_id[scenario].weight for scenario in available)
            scenario = rng.choices(available, weights=scenario_weights, k=1)[0]
            selected_counts[scenario] += 1
            plan.append((scenario, regime))
        return tuple(plan)

    @staticmethod
    def _rail_weight(scenario: FraudScenarioId, regime: FraudRegimeConfig) -> float:
        if not regime.payment_rail_mix:
            return 1.0
        rail_by_scenario: dict[FraudScenarioId, PaymentRail] = {
            "F01": "CARD",
            "F02": "CARD",
            "F03": "ACCOUNT_TRANSFER",
            "F04": "PIX",
            "F05": "CARD",
        }
        rail = rail_by_scenario[scenario]
        return regime.payment_rail_mix.get(rail, 0.0)

    def _settings_for_regime(
        self,
        settings: FraudScenarioSettings,
        regime: FraudRegimeConfig | None,
    ) -> FraudScenarioSettings:
        if regime is None:
            return settings
        amount_multiplier = regime.amount_multiplier
        minimum = settings.amount_min or self.config.behavior.amount_min
        maximum = settings.amount_max or self.config.behavior.amount_max
        return settings.model_copy(
            update={
                "amount_min": round(minimum * amount_multiplier, 2),
                "amount_max": round(maximum * amount_multiplier, 2),
                "duration_seconds": max(
                    0, round(settings.duration_seconds * regime.timing_multiplier)
                ),
                "window_seconds": max(1, round(settings.window_seconds * regime.timing_multiplier)),
                "attempt_count": max(1, round(settings.attempt_count * regime.campaign_intensity)),
            }
        )

    def _campaign_anchor_time(
        self, regime: FraudRegimeConfig | None, campaign_number: int
    ) -> datetime:
        if regime is None:
            return self.start
        span = max(0.0, (regime.to_time - regime.from_time).total_seconds())
        rng = create_stream_rng(
            self.config.simulation.seed, f"milestone-10:regime-anchor:{campaign_number}"
        )
        return regime.from_time + timedelta(seconds=rng.random() * span)

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
            event_type="CARD_PAYMENT_INITIATED",
        )
        payment, payment_events = self.payment_generator._card_lifecycle(payment, initial, rng)
        subtlety = self._active_plan.scenario_subtlety if self._difficulty.enabled else 0.0
        should_decline = force_scenario_decline and (
            (
                scenario_type == "F01"
                and attempt < count
                and not (subtlety >= 0.67 and attempt == count - 1)
            )
            or (
                scenario_type == "F02"
                and attempt % 4 != 0
                and not (subtlety >= 0.67 and attempt % 4 == 2)
            )
        )
        if should_decline:
            authorization_index = next(
                (
                    index
                    for index, event in enumerate(payment_events)
                    if event.event_type == "CARD_AUTHORIZED"
                ),
                None,
            )
            if authorization_index is not None:
                declined = payment_events[authorization_index].model_copy(
                    update={"event_type": "CARD_DECLINED"}
                )
                payment = payment.model_copy(update={"current_status": "DECLINED"})
                payment_events = (*payment_events[:authorization_index], declined)
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
            source_available_at, ingested_at, processed_at = payment_event_times(
                event_time, "ACCOUNT_TRANSFER"
            )
            signal = first_event.model_copy(
                update={
                    "event_id": f"EVT-{scenario_id}-SIGNAL-{index:02d}",
                    "event_type": event_type,
                    "event_time": event_time,
                    "source_created_at": event_time,
                    "source_available_at": source_available_at,
                    "ingested_at": ingested_at,
                    "processed_at": processed_at,
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
        source_available_at, ingested_at, processed_at = payment_event_times(initiated_at, rail)
        event = PaymentEvent(
            event_id=event_id,
            event_type=event_type,
            event_version=1,
            payment_id=payment_id,
            customer_id=customer_id,
            account_id=account_id,
            event_time=initiated_at,
            source_created_at=initiated_at,
            source_available_at=source_available_at,
            ingested_at=ingested_at,
            processed_at=processed_at,
            producer="fraudtwin.fraud",
            source_system="synthetic_fraud_source",
            schema_version=PAYMENT_EVENT_CONTRACT_VERSION,
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
        if self._difficulty.enabled:
            self._difficulty_choice_counter += 1
            rng = create_stream_rng(
                self.config.simulation.seed,
                f"milestone-12:device:{self._active_plan.scenario}:"
                f"{self._difficulty_choice_counter}",
            )
        # Higher behavioral similarity deliberately permits trusted devices
        # while retaining a deterministic device stream.
        if (
            self._difficulty.enabled
            and self.devices
            and rng.random() < self._active_plan.behavior_similarity
        ):
            return self.devices[rng.randrange(len(self.devices))]
        device_pool = self.untrusted_devices or self.devices
        return device_pool[rng.randrange(len(device_pool))] if device_pool else None

    def _merchant_for_attempt(self, attempt: int, rng: Random) -> Merchant:
        if self._difficulty.enabled and self.merchants:
            baseline_ids = {
                payment.merchant_id
                for payment in self.baseline.payments
                if payment.merchant_id is not None
            }
            cohort = tuple(
                merchant for merchant in self.merchants if merchant.merchant_id in baseline_ids
            )
            if cohort:
                cohort_rng = create_stream_rng(
                    self.config.simulation.seed,
                    f"milestone-12:merchant:{self._active_plan.scenario}:{attempt}",
                )
                if cohort_rng.random() < self._active_plan.behavior_similarity:
                    return cohort[cohort_rng.randrange(len(cohort))]
        return self.merchants[(attempt + rng.randrange(len(self.merchants))) % len(self.merchants)]

    def _hard_negative_campaign(
        self,
        scenario_type: FraudScenarioType,
        number: int,
        settings: FraudScenarioSettings,
    ) -> tuple[list[Payment], list[PaymentEvent], FraudRecord] | None:
        """Generate one legitimate sequence with the selected fraud signals."""

        stream_prefix = "milestone-12" if self._difficulty.enabled else "milestone-6"
        rng = create_stream_rng(
            self.config.simulation.seed, f"{stream_prefix}:hard-negative:{scenario_type}:{number}"
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
            source_available_at, ingested_at, processed_at = payment_event_times(
                event_time, "ACCOUNT_TRANSFER"
            )
            signals.append(
                first_event.model_copy(
                    update={
                        "event_id": f"EVT-{scenario_id}-SIGNAL-{index:02d}",
                        "event_type": event_type,
                        "event_time": event_time,
                        "source_created_at": event_time,
                        "source_available_at": source_available_at,
                        "ingested_at": ingested_at,
                        "processed_at": processed_at,
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
        amount = max(lower, rng.uniform(lower, upper))
        if (
            self._difficulty.enabled
            and self._active_plan.amount_similarity
            and self.baseline.payments
        ):
            rail = "CARD" if low_value else "PIX" if high_value else None
            cohort = [
                item.amount
                for item in self.baseline.payments
                if rail is None or item.payment_rail == rail
            ]
            if cohort:
                reference = sorted(cohort)[len(cohort) // 2]
                amount = (1.0 - self._active_plan.amount_similarity) * amount + (
                    self._active_plan.amount_similarity * reference
                )
        amount = min(max(amount, lower), upper)
        return round(amount, 2)

    def _start_time(
        self, settings: FraudScenarioSettings, *, rail: str, offset: int = 0
    ) -> datetime:
        if self._difficulty.enabled and self._active_plan.temporal_irregularity:
            offset = round(offset * (1.0 + self._active_plan.temporal_irregularity))
        lifecycle = (
            self.config.card_lifecycle.maximum_delay_seconds + CARD_EVENT_ENVELOPE_DELAY_SECONDS
            if rail == "CARD"
            else self.config.pix_lifecycle.maximum_delay_seconds + PIX_EVENT_ENVELOPE_DELAY_SECONDS
            if rail == "PIX"
            else ACCOUNT_TRANSFER_EVENT_ENVELOPE_DELAY_SECONDS
        )
        latest = self.end - timedelta(seconds=lifecycle + settings.duration_seconds, microseconds=1)
        base = (self._campaign_anchor or self.start) + timedelta(seconds=offset)
        if latest <= self.start:
            return self.start
        return min(base, latest)

    def _reason(self, scenario_type: FraudScenarioType, trigger: str, device_id: str | None) -> str:
        device_part = f" using device {device_id}" if device_id else ""
        camouflage_part = (
            f"; camouflage={self._active_camouflage:.4f}" if self._active_camouflage else ""
        )
        return (
            f"{scenario_type} trigger {trigger}{device_part}; "
            f"coordinated scenario behavior{camouflage_part}"
        )

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
