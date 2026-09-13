"""Deterministic point-in-time training data construction for Milestone 9.

The builder works on the existing in-memory M1-M8 records or on one generated
run.  It deliberately does not regenerate simulation state and does not add a
feature-store or model-serving dependency.
"""

import hashlib
import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, TypeVar

import polars as pl

from fraudtwin.config import SimulationRunConfig, _default_feature_windows, config_hash
from fraudtwin.domain import (
    Account,
    BehaviorProfile,
    Card,
    Customer,
    CustomerDispute,
    DelayedFraudLabel,
    Device,
    FraudAlert,
    FraudCase,
    FraudCaseConfirmation,
    FraudRecord,
    Institution,
    LedgerEntry,
    Merchant,
    Payment,
    PaymentEvent,
    PixKey,
)
from fraudtwin.manifest import DatasetManifest, RunManifest
from fraudtwin.simulation.behavior import BehaviorDataset
from fraudtwin.simulation.generator import EntityDataset

PIT_DATASET_SCHEMA: dict[str, Any] = {
    "dataset_row_id": pl.Utf8,
    "payment_id": pl.Utf8,
    "event_id": pl.Utf8,
    "customer_id": pl.Utf8,
    "account_id": pl.Utf8,
    "prediction_time": pl.Datetime(time_zone="UTC"),
    "business_event_time": pl.Datetime(time_zone="UTC"),
    "event_time": pl.Datetime(time_zone="UTC"),
    "source_available_at": pl.Datetime(time_zone="UTC"),
    "feature_available_at": pl.Datetime(time_zone="UTC"),
    "label_available_at": pl.Datetime(time_zone="UTC"),
    "label": pl.Utf8,
    "fraud_truth": pl.Boolean,
    "amount": pl.Float64,
    "payment_rail": pl.Utf8,
    "payment_type": pl.Utf8,
    "merchant_id": pl.Utf8,
    "card_id": pl.Utf8,
    "device_id": pl.Utf8,
    "online": pl.Boolean,
    "transaction_count_1m": pl.Int64,
    "transaction_count_5m": pl.Int64,
    "transaction_count_1h": pl.Int64,
    "transaction_count_24h": pl.Int64,
    "transaction_count_7d": pl.Int64,
    "transaction_count_30d": pl.Int64,
    "transaction_amount_1h": pl.Float64,
    "transaction_amount_24h": pl.Float64,
    "transaction_amount_7d": pl.Float64,
    "avg_transaction_amount_30d": pl.Float64,
    "max_transaction_amount_7d": pl.Float64,
    "amount_vs_customer_avg": pl.Float64,
    "distinct_merchants_1d": pl.Int64,
    "distinct_merchants_30d": pl.Int64,
    "new_merchant_flag": pl.Boolean,
    "merchant_fraud_rate_historical": pl.Float64,
    "distinct_countries_24h": pl.Int64,
    "new_country_flag": pl.Boolean,
    "new_device_flag": pl.Boolean,
    "trusted_device_flag": pl.Boolean,
    "device_customer_count_30d": pl.Int64,
    "customers_per_device_24h": pl.Int64,
    "account_age_days": pl.Float64,
    "balance": pl.Float64,
    "available_balance": pl.Float64,
    "credit_limit": pl.Float64,
    "credit_utilization": pl.Float64,
    "days_since_last_payment": pl.Float64,
    "confirmed_fraud_count_90d": pl.Int64,
    "fraud_loss_365d": pl.Float64,
    "days_since_last_confirmed_fraud": pl.Float64,
    "split": pl.Utf8,
}

_INITIAL_EVENT_TYPES = {
    "CARD_AUTHORIZATION_REQUESTED",
    "PIX_INITIATED",
    "TRANSFER_COMPLETED",
}

T = TypeVar("T")


@dataclass(frozen=True)
class PointInTimeDataset:
    """Stable rows and the manifest that describes their construction."""

    rows: tuple[dict[str, Any], ...]
    manifest: DatasetManifest

    @property
    def count(self) -> int:
        return len(self.rows)

    @property
    def counts(self) -> dict[str, int]:
        return self.manifest.row_counts

    @property
    def frame(self) -> pl.DataFrame:
        """Return the dataset as a frame with the stable output schema."""

        return pl.DataFrame(list(self.rows), schema=PIT_DATASET_SCHEMA, orient="row")


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("dataset timestamps must include a timezone")
    return value.astimezone(UTC)


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _record_key(record: Any) -> str:
    return _canonical(record.model_dump(mode="json"))


def _in_window(event_time: datetime, candidate_time: datetime, seconds: int) -> bool:
    """Return whether a prior business event falls in a feature window."""

    delta = event_time - candidate_time
    return timedelta(0) < delta <= timedelta(seconds=seconds)


def _deduplicate(records: Iterable[T], identifier: str) -> tuple[T, ...]:
    """Remove exact M8 duplicate rows and reject conflicting identities."""

    by_id: dict[str, T] = {}
    keys: dict[str, str] = {}
    for record in records:
        record_id = str(getattr(record, identifier))
        key = _record_key(record)
        if record_id in by_id:
            if keys[record_id] != key:
                raise ValueError(f"conflicting duplicate {identifier}: {record_id}")
            continue
        by_id[record_id] = record
        keys[record_id] = key
    return tuple(by_id.values())


def _read_models(path: Path, model: type[T]) -> tuple[T, ...]:
    try:
        rows = pl.read_parquet(path).to_dicts()
    except (FileNotFoundError, OSError) as exc:
        raise ValueError(f"required generated source is missing: {path}") from exc
    try:
        return tuple(model.model_validate(row) for row in rows)  # type: ignore[attr-defined]
    except ValueError as exc:
        raise ValueError(f"invalid generated source {path}: {exc}") from exc


def _read_run_table(run_dir: Path, group: str, table: str, model: type[T]) -> tuple[T, ...]:
    return _read_models(run_dir / group / f"{table}.parquet", model)


def load_generated_run(run_dir: Path) -> tuple[EntityDataset, BehaviorDataset, RunManifest]:
    """Load one existing generated run without regenerating unrelated records."""

    try:
        manifest = RunManifest.model_validate_json(
            (run_dir / "manifest.json").read_text(encoding="utf-8")
        )
    except (FileNotFoundError, OSError, ValueError) as exc:
        raise ValueError(f"invalid generated run manifest in {run_dir}") from exc

    entities = EntityDataset(
        customers=_read_run_table(run_dir, "entities", "customers", Customer),
        institutions=_read_run_table(run_dir, "entities", "institutions", Institution),
        accounts=_read_run_table(run_dir, "entities", "accounts", Account),
        cards=_read_run_table(run_dir, "entities", "cards", Card),
        merchants=_read_run_table(run_dir, "entities", "merchants", Merchant),
        devices=_read_run_table(run_dir, "entities", "devices", Device),
        pix_keys=_read_run_table(run_dir, "entities", "pix_keys", PixKey),
    )
    behavior = BehaviorDataset(
        profiles=_read_run_table(run_dir, "behavior", "behavior_profiles", BehaviorProfile),
        payments=_read_run_table(run_dir, "payments", "payments", Payment),
        payment_events=_read_run_table(run_dir, "payments", "payment_events", PaymentEvent),
        ledger_entries=_read_run_table(run_dir, "ledger", "ledger_entries", LedgerEntry),
        fraud_records=_read_run_table(run_dir, "fraud", "fraud_records", FraudRecord),
        alerts=_read_run_table(run_dir, "fraud", "fraud_alerts", FraudAlert),
        fraud_cases=_read_run_table(run_dir, "fraud", "fraud_cases", FraudCase),
        case_confirmations=_read_run_table(
            run_dir, "fraud", "case_confirmations", FraudCaseConfirmation
        ),
        customer_disputes=_read_run_table(run_dir, "fraud", "customer_disputes", CustomerDispute),
        fraud_labels=_read_run_table(run_dir, "fraud", "fraud_labels", DelayedFraudLabel),
    )
    return entities, behavior, manifest


class PointInTimeDatasetBuilder:
    """Build historical features and labels using only available source data."""

    def __init__(
        self,
        config: SimulationRunConfig,
        entities: EntityDataset,
        behavior: BehaviorDataset,
        source_manifest: RunManifest | None = None,
    ) -> None:
        self.config = config
        self.settings = config.dataset
        self.entities = entities
        self.behavior = behavior
        self.source_manifest = source_manifest
        if not self.settings.enabled:
            raise ValueError("point-in-time dataset generation is disabled")
        self.start = _utc(self.settings.start or config.simulation.start)
        self.end = _utc(
            self.settings.end
            or config.simulation.start + timedelta(days=config.simulation.duration_days)
        )
        self.feature_windows: dict[str, int] = {
            **{str(name): seconds for name, seconds in _default_feature_windows().items()},
            **{str(name): seconds for name, seconds in self.settings.feature_windows.items()},
        }
        if self.end <= self.start:
            raise ValueError("dataset end must be after dataset start")
        self._validate_sources()
        self.payments = _deduplicate(behavior.payments, "payment_id")
        self.payments_by_id = {payment.payment_id: payment for payment in self.payments}
        self.events = _deduplicate(behavior.payment_events, "event_id")
        self.labels = _deduplicate(behavior.fraud_labels, "label_id")
        self.events_by_payment = self._initial_events_by_payment()
        self.labels_by_payment = self._labels_by_payment()
        self.accounts = {account.account_id: account for account in entities.accounts}
        self.cards = {card.card_id: card for card in entities.cards}
        self.merchants = {merchant.merchant_id: merchant for merchant in entities.merchants}
        self.devices = {device.device_id: device for device in entities.devices}

    def _validate_sources(self) -> None:
        entity_ids = {
            "customers": {item.customer_id for item in self.entities.customers},
            "accounts": {item.account_id for item in self.entities.accounts},
            "cards": {item.card_id for item in self.entities.cards},
            "devices": {item.device_id for item in self.entities.devices},
            "merchants": {item.merchant_id for item in self.entities.merchants},
        }
        payment_ids = {payment.payment_id for payment in self.behavior.payments}
        for payment in self.behavior.payments:
            _utc(payment.initiated_at)
            if payment.payer_account_id not in entity_ids["accounts"]:
                raise ValueError(f"payment references unknown account {payment.payer_account_id}")
        event_ids: set[str] = set()
        for event in self.behavior.payment_events:
            for timestamp in (
                event.event_time,
                event.source_created_at,
                event.source_available_at,
                event.ingested_at,
                event.processed_at,
            ):
                _utc(timestamp)
            if event.event_time > event.source_created_at:
                raise ValueError(f"event {event.event_id} has invalid source_created_at")
            if event.source_created_at > event.source_available_at:
                raise ValueError(f"event {event.event_id} has invalid source availability")
            if (
                event.source_available_at > event.ingested_at
                or event.ingested_at > event.processed_at
            ):
                raise ValueError(f"event {event.event_id} has invalid delivery timestamps")
            if event.payment_id not in payment_ids:
                raise ValueError(f"event {event.event_id} references unknown payment")
            if event.event_id in event_ids:
                continue
            event_ids.add(event.event_id)
        for label in self.behavior.fraud_labels:
            _utc(label.fraud_occurred_at)
            _utc(label.label_available_at)
            if label.label_available_at < label.fraud_occurred_at:
                raise ValueError(f"label {label.label_id} is available before occurrence")
            required_delay = self.settings.label_delay_seconds
            if required_delay is None:
                required_delay = self.config.fraud_workflow.label_delay_seconds
            if label.label_available_at < label.fraud_occurred_at + timedelta(
                seconds=required_delay
            ):
                raise ValueError(f"label {label.label_id} violates configured label delay")
            if label.payment_id not in payment_ids:
                raise ValueError(f"label {label.label_id} references unknown payment")

    def _initial_events_by_payment(self) -> dict[str, PaymentEvent]:
        result: dict[str, PaymentEvent] = {}
        for event in sorted(self.events, key=lambda item: (item.event_time, item.event_id)):
            if event.event_type not in _INITIAL_EVENT_TYPES or event.causation_id is not None:
                continue
            existing = result.get(event.payment_id)
            if existing is not None and existing.event_id != event.event_id:
                raise ValueError(f"payment {event.payment_id} has multiple initial events")
            result[event.payment_id] = event
        return result

    def _labels_by_payment(self) -> dict[str, DelayedFraudLabel]:
        result: dict[str, DelayedFraudLabel] = {}
        for label in sorted(self.labels, key=lambda item: (item.payment_id, item.label_id)):
            existing = result.get(label.payment_id)
            if existing is not None and existing.model_dump(mode="json") != label.model_dump(
                mode="json"
            ):
                raise ValueError(f"payment {label.payment_id} has conflicting labels")
            result[label.payment_id] = label
        return result

    def _prediction_times(self, explicit: Mapping[str, datetime] | None) -> dict[str, datetime]:
        if explicit is not None:
            result = {payment_id: _utc(value) for payment_id, value in explicit.items()}
            unknown = set(result) - {payment.payment_id for payment in self.payments}
            if unknown:
                raise ValueError(f"prediction times reference unknown payments: {sorted(unknown)}")
            return result
        delay = timedelta(seconds=self.settings.prediction_delay_seconds)
        return {
            payment.payment_id: self.events_by_payment[payment.payment_id].source_available_at
            + delay
            for payment in self.payments
            if payment.payment_id in self.events_by_payment
        }

    def _split_boundaries(self) -> tuple[datetime, datetime, datetime]:
        splits = self.settings.splits
        explicit = (splits.train_end, splits.validation_end, splits.test_end)
        if any(value is not None for value in explicit):
            if splits.train_end is None or splits.validation_end is None:
                raise ValueError("explicit temporal splits require train_end and validation_end")
            test_end = splits.test_end or self.end
            boundaries = (_utc(splits.train_end), _utc(splits.validation_end), _utc(test_end))
        else:
            total_seconds = (self.end - self.start).total_seconds()
            train_end = self.start + timedelta(seconds=total_seconds * splits.train_fraction)
            validation_end = train_end + timedelta(
                seconds=total_seconds * splits.validation_fraction
            )
            boundaries = (train_end, validation_end, self.end)
        if not self.start < boundaries[0] < boundaries[1] < boundaries[2] <= self.end:
            raise ValueError(
                "temporal split boundaries must be strictly ordered in the dataset range"
            )
        return boundaries

    def _split(
        self, prediction_time: datetime, boundaries: tuple[datetime, datetime, datetime]
    ) -> str:
        if prediction_time < boundaries[0]:
            return "train"
        if prediction_time < boundaries[1]:
            return "validation"
        if prediction_time <= boundaries[2]:
            return "test"
        raise ValueError("prediction timestamp is outside the configured split range")

    def build(self, prediction_times: Mapping[str, datetime] | None = None) -> PointInTimeDataset:
        """Build rows in stable payment-ID order.

        By default each payment is scored when its initial source event becomes
        available.  Callers may provide a deterministic payment-to-prediction
        mapping for snapshot or delayed-prediction use cases.
        """

        boundaries = self._split_boundaries()
        times = self._prediction_times(prediction_times)
        rows: list[dict[str, Any]] = []
        for payment in sorted(self.payments, key=lambda item: item.payment_id):
            event = self.events_by_payment.get(payment.payment_id)
            prediction_time = times.get(payment.payment_id)
            if event is None or prediction_time is None:
                continue
            if not self.start <= prediction_time <= self.end:
                continue
            if event.source_available_at > prediction_time:
                raise ValueError(
                    f"payment {payment.payment_id} is not available at its prediction timestamp"
                )
            label = self.labels_by_payment.get(payment.payment_id)
            mature = label is not None and label.label_available_at <= prediction_time
            if not mature and self.settings.unresolved_labels == "exclude":
                continue
            rows.append(self._row(payment, event, prediction_time, label, mature, boundaries))
        manifest = self._manifest(rows, boundaries)
        return PointInTimeDataset(tuple(rows), manifest)

    def _prior_events(
        self, event: PaymentEvent, prediction_time: datetime
    ) -> tuple[PaymentEvent, ...]:
        return tuple(
            candidate
            for candidate in self.events_by_payment.values()
            if candidate.customer_id == event.customer_id
            and candidate.event_time < event.event_time
            and candidate.source_available_at <= prediction_time
        )

    def _all_prior_events(
        self, event: PaymentEvent, prediction_time: datetime
    ) -> tuple[PaymentEvent, ...]:
        return tuple(
            candidate
            for candidate in self.events_by_payment.values()
            if candidate.event_time < event.event_time
            and candidate.source_available_at <= prediction_time
        )

    def _windowed_events(
        self,
        event: PaymentEvent,
        candidates: tuple[PaymentEvent, ...],
        seconds: int,
    ) -> tuple[PaymentEvent, ...]:
        return tuple(
            candidate
            for candidate in candidates
            if _in_window(event.event_time, candidate.event_time, seconds)
        )

    def _historical_labels(
        self,
        payment: Payment,
        event: PaymentEvent,
        prediction_time: datetime,
    ) -> tuple[
        tuple[DelayedFraudLabel, ...],
        tuple[DelayedFraudLabel, ...],
        tuple[DelayedFraudLabel, ...],
        tuple[DelayedFraudLabel, ...],
    ]:
        known_labels = tuple(
            label
            for label in self.labels
            if label.label_available_at <= prediction_time
            and label.label == "FRAUD"
            and label.fraud_confirmed_at is not None
            and label.fraud_occurred_at < event.event_time
        )
        prior_confirmed = tuple(
            label
            for label in known_labels
            if _in_window(
                event.event_time,
                label.fraud_occurred_at,
                self.feature_windows["confirmed_fraud_count_90d"],
            )
        )
        prior_loss = tuple(
            label
            for label in known_labels
            if _in_window(
                event.event_time,
                label.fraud_occurred_at,
                self.feature_windows["fraud_loss_365d"],
            )
        )
        merchant_labels = tuple(
            label
            for label in self.labels
            if label.label_available_at <= prediction_time
            and label.fraud_occurred_at < event.event_time
            and _in_window(
                event.event_time,
                label.fraud_occurred_at,
                self.feature_windows["merchant_fraud_rate_historical"],
            )
            and self._payment_merchant_id(label.payment_id) == payment.merchant_id
        )
        return known_labels, prior_confirmed, prior_loss, merchant_labels

    def _feature_available_at(
        self,
        event: PaymentEvent,
        prior_events: tuple[PaymentEvent, ...],
        all_prior_events: tuple[PaymentEvent, ...],
        historical_labels: tuple[DelayedFraudLabel, ...],
        account: Account,
        merchant: Merchant | None,
        device: Device | None,
        card: Card | None,
    ) -> datetime:
        available_timestamps = [
            event.source_available_at,
            *(item.source_available_at for item in prior_events),
            *(item.source_available_at for item in all_prior_events),
            *(item.label_available_at for item in historical_labels),
            account.opening_date,
        ]
        if merchant is not None:
            available_timestamps.append(merchant.created_at)
        if device is not None:
            available_timestamps.append(device.first_seen_at)
        if card is not None:
            available_timestamps.append(card.issued_at)
        return max(available_timestamps)

    def _row(
        self,
        payment: Payment,
        event: PaymentEvent,
        prediction_time: datetime,
        label: DelayedFraudLabel | None,
        label_is_mature: bool,
        boundaries: tuple[datetime, datetime, datetime],
    ) -> dict[str, Any]:
        prior = self._prior_events(event, prediction_time)
        all_prior = self._all_prior_events(event, prediction_time)
        merchant = self.merchants.get(payment.merchant_id or "")
        device = self.devices.get(event.device_id or "")
        account = self.accounts.get(payment.payer_account_id)
        if account is None:
            raise ValueError(f"payment references unknown account {payment.payer_account_id}")

        windows = {
            "one_minute": self._windowed_events(
                event, prior, self.feature_windows["transaction_count_1m"]
            ),
            "five_minutes": self._windowed_events(
                event, prior, self.feature_windows["transaction_count_5m"]
            ),
            "one_hour": self._windowed_events(
                event, prior, self.feature_windows["transaction_count_1h"]
            ),
            "day": self._windowed_events(
                event, prior, self.feature_windows["transaction_count_24h"]
            ),
            "week": self._windowed_events(
                event, prior, self.feature_windows["transaction_count_7d"]
            ),
            "month": self._windowed_events(
                event, prior, self.feature_windows["transaction_count_30d"]
            ),
            "amount_hour": self._windowed_events(
                event, prior, self.feature_windows["transaction_amount_1h"]
            ),
            "amount_day": self._windowed_events(
                event, prior, self.feature_windows["transaction_amount_24h"]
            ),
            "amount_week": self._windowed_events(
                event, prior, self.feature_windows["transaction_amount_7d"]
            ),
            "amount_month": self._windowed_events(
                event, prior, self.feature_windows["avg_transaction_amount_30d"]
            ),
            "merchant_day": self._windowed_events(
                event, prior, self.feature_windows["distinct_merchants_1d"]
            ),
            "merchant_month": self._windowed_events(
                event, prior, self.feature_windows["distinct_merchants_30d"]
            ),
            "country_day": self._windowed_events(
                event, prior, self.feature_windows["distinct_countries_24h"]
            ),
        }
        device_day = tuple(
            item
            for item in all_prior
            if item.device_id == event.device_id
            and _in_window(
                event.event_time,
                item.event_time,
                self.feature_windows["customers_per_device_24h"],
            )
        )
        device_month = tuple(
            item
            for item in all_prior
            if item.device_id == event.device_id
            and _in_window(
                event.event_time,
                item.event_time,
                self.feature_windows["device_customer_count_30d"],
            )
        )
        known_labels, prior_confirmed, prior_loss, merchant_labels = self._historical_labels(
            payment, event, prediction_time
        )
        customer_labels = tuple(
            item for item in prior_loss if item.customer_id == event.customer_id
        )
        customer_prior = tuple(
            item for item in windows["amount_month"] if item.customer_id == event.customer_id
        )
        customer_average = (
            sum(item.amount for item in customer_prior) / len(customer_prior)
            if customer_prior
            else payment.amount
        )
        prior_merchants = {
            item.merchant_id for item in windows["merchant_month"] if item.merchant_id is not None
        }
        prior_countries = set(
            self.merchants[item.merchant_id].country
            for item in windows["merchant_month"]
            if item.merchant_id in self.merchants
        )
        prior_devices = {item.device_id for item in windows["month"] if item.device_id is not None}
        last_payment = max((item.event_time for item in prior), default=None)
        last_fraud = max((item.fraud_occurred_at for item in customer_labels), default=None)
        card = self.cards.get(payment.card_id or "")
        historical_labels = known_labels + merchant_labels
        feature_available_at = self._feature_available_at(
            event,
            prior,
            all_prior,
            historical_labels,
            account,
            merchant,
            device,
            card,
        )
        if feature_available_at > prediction_time:
            raise ValueError(f"features for {payment.payment_id} are not point-in-time correct")
        row_id = (
            "DSR-"
            + hashlib.sha256(
                f"{self._source_run_id()}|{payment.payment_id}|{prediction_time.isoformat()}".encode()
            ).hexdigest()[:24]
        )
        fraud_rate = (
            sum(item.label == "FRAUD" for item in merchant_labels) / len(merchant_labels)
            if merchant_labels
            else 0.0
        )
        current_country = merchant.country if merchant is not None else None
        account_age = max(0.0, (event.event_time - account.opening_date).total_seconds() / 86400)
        utilization = (
            (account.credit_limit - account.available_balance) / account.credit_limit
            if account.credit_limit > 0
            else 0.0
        )
        return {
            "dataset_row_id": row_id,
            "payment_id": payment.payment_id,
            "event_id": event.event_id,
            "customer_id": event.customer_id,
            "account_id": payment.payer_account_id,
            "prediction_time": prediction_time,
            "business_event_time": event.event_time,
            "event_time": event.event_time,
            "source_available_at": event.source_available_at,
            "feature_available_at": feature_available_at,
            "label_available_at": label.label_available_at if label is not None else None,
            "label": label.label if label is not None and label_is_mature else None,
            "fraud_truth": label.fraud_truth if label is not None and label_is_mature else None,
            "amount": payment.amount,
            "payment_rail": payment.payment_rail,
            "payment_type": payment.payment_type,
            "merchant_id": payment.merchant_id,
            "card_id": payment.card_id,
            "device_id": event.device_id,
            "online": event.online,
            "transaction_count_1m": len(windows["one_minute"]),
            "transaction_count_5m": len(windows["five_minutes"]),
            "transaction_count_1h": len(windows["one_hour"]),
            "transaction_count_24h": len(windows["day"]),
            "transaction_count_7d": len(windows["week"]),
            "transaction_count_30d": len(windows["month"]),
            "transaction_amount_1h": sum(item.amount for item in windows["amount_hour"]),
            "transaction_amount_24h": sum(item.amount for item in windows["amount_day"]),
            "transaction_amount_7d": sum(item.amount for item in windows["amount_week"]),
            "avg_transaction_amount_30d": customer_average,
            "max_transaction_amount_7d": max(
                (item.amount for item in windows["amount_week"]), default=0.0
            ),
            "amount_vs_customer_avg": payment.amount / customer_average
            if customer_average
            else 1.0,
            "distinct_merchants_1d": len(
                {item.merchant_id for item in windows["merchant_day"] if item.merchant_id}
            ),
            "distinct_merchants_30d": len(prior_merchants),
            "new_merchant_flag": payment.merchant_id is not None
            and payment.merchant_id not in prior_merchants,
            "merchant_fraud_rate_historical": fraud_rate,
            "distinct_countries_24h": len(
                {
                    self.merchants[item.merchant_id].country
                    for item in windows["country_day"]
                    if item.merchant_id in self.merchants
                }
            ),
            "new_country_flag": current_country is not None
            and current_country not in prior_countries,
            "new_device_flag": event.device_id is not None and event.device_id not in prior_devices,
            "trusted_device_flag": device.trusted if device is not None else False,
            "device_customer_count_30d": len({item.customer_id for item in device_month}),
            "customers_per_device_24h": len({item.customer_id for item in device_day}),
            "account_age_days": account_age,
            "balance": account.ledger_balance,
            "available_balance": account.available_balance,
            "credit_limit": account.credit_limit,
            "credit_utilization": utilization,
            "days_since_last_payment": (
                (event.event_time - last_payment).total_seconds() / 86400
                if last_payment is not None
                else -1.0
            ),
            "confirmed_fraud_count_90d": sum(
                item.customer_id == event.customer_id for item in prior_confirmed
            ),
            "fraud_loss_365d": sum(item.amount for item in customer_labels),
            "days_since_last_confirmed_fraud": (
                (event.event_time - last_fraud).total_seconds() / 86400
                if last_fraud is not None
                else -1.0
            ),
            "split": self._split(prediction_time, boundaries),
        }

    def _source_run_id(self) -> str:
        if self.source_manifest is not None:
            return self.source_manifest.run_id
        run_ids = sorted(
            {event.simulation_run_id for event in self.events if event.simulation_run_id}
        )
        return run_ids[0] if run_ids else "in-memory"

    def _payment_merchant_id(self, payment_id: str) -> str | None:
        payment = self.payments_by_id.get(payment_id)
        return payment.merchant_id if payment is not None else None

    def _manifest(
        self, rows: list[dict[str, Any]], boundaries: tuple[datetime, datetime, datetime]
    ) -> DatasetManifest:
        source_run_id = self._source_run_id()
        source_manifest_hash = self._source_manifest_hash()
        configuration_hash = config_hash(self.config, include_dataset=True)
        generator_version = (
            self.source_manifest.generator_version if self.source_manifest is not None else "0.9.0"
        )
        row_counts = {
            "total": len(rows),
            "train": sum(row["split"] == "train" for row in rows),
            "validation": sum(row["split"] == "validation" for row in rows),
            "test": sum(row["split"] == "test" for row in rows),
        }
        features: dict[str, dict[str, object]] = {
            name: {
                "window_seconds": self.feature_windows[name],
                "availability_rule": "source_available_at <= prediction_time",
            }
            for name in self.feature_windows
        }
        split_boundaries = {
            "train_end": boundaries[0].isoformat(),
            "validation_end": boundaries[1].isoformat(),
            "test_end": boundaries[2].isoformat(),
        }
        label_definition: dict[str, object] = {
            "field": "label",
            "availability_rule": "label_available_at <= prediction_time",
            "configured_delay_seconds": self.settings.label_delay_seconds
            if self.settings.label_delay_seconds is not None
            else self.config.fraud_workflow.label_delay_seconds,
            "unresolved_policy": self.settings.unresolved_labels,
        }
        payload = {
            "dataset_version": "0.9.0",
            "source_run_id": source_run_id,
            "source_manifest_hash": source_manifest_hash,
            "configuration_hash": configuration_hash,
            "parameters": self.settings.model_dump(mode="json"),
            "split_boundaries": split_boundaries,
            "feature_definitions": features,
            "label_definition": label_definition,
            "row_hash": hashlib.sha256(_canonical(rows).encode()).hexdigest(),
            "row_counts": row_counts,
            "schema_columns": list(PIT_DATASET_SCHEMA),
        }
        dataset_id = "DS-" + hashlib.sha256(_canonical(payload).encode()).hexdigest()[:16]
        source_information: dict[str, object] = {
            "run_id": source_run_id,
            "manifest_hash": source_manifest_hash,
            "generator_version": generator_version,
            "seed": self.config.simulation.seed,
        }
        if self.source_manifest is not None:
            source_information.update(self.source_manifest.model_dump(mode="json"))
        return DatasetManifest(
            dataset_id=dataset_id,
            dataset_version="0.9.0",
            source_run_id=source_run_id,
            source_manifest_hash=source_manifest_hash,
            generator_version=generator_version,
            seed=self.config.simulation.seed,
            configuration_hash=configuration_hash,
            parameters=self.settings.model_dump(mode="json"),
            split_boundaries=split_boundaries,
            feature_definitions=features,
            label_definition=label_definition,
            source_run_information=source_information,
            reproducibility={
                "row_hash": payload["row_hash"],
                "schema_columns": list(PIT_DATASET_SCHEMA),
                "ordering": "payment_id ascending",
                "current_time_used": False,
                "global_randomness_used": False,
            },
            row_counts=row_counts,
            schema_version="1",
        )

    def _source_manifest_hash(self) -> str:
        if self.source_manifest is None:
            return "in-memory"
        return hashlib.sha256(self.source_manifest.model_dump_json().encode("utf-8")).hexdigest()


def build_point_in_time_dataset(
    config: SimulationRunConfig,
    entities: EntityDataset,
    behavior: BehaviorDataset,
    source_manifest: RunManifest | None = None,
    prediction_times: Mapping[str, datetime] | None = None,
) -> PointInTimeDataset:
    """Convenience API for deterministic in-memory M9 construction."""

    return PointInTimeDatasetBuilder(config, entities, behavior, source_manifest).build(
        prediction_times
    )


def write_point_in_time_dataset(
    dataset: PointInTimeDataset,
    output_path: Path,
    manifest_path: Path | None = None,
) -> tuple[Path, Path]:
    """Write the fixed-schema M9 Parquet table and its JSON manifest."""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(list(dataset.rows), schema=PIT_DATASET_SCHEMA, orient="row").write_parquet(
        output_path
    )
    final_manifest_path = manifest_path or output_path.with_name("dataset_manifest.json")
    final_manifest_path.parent.mkdir(parents=True, exist_ok=True)
    final_manifest_path.write_text(
        dataset.manifest.model_dump_json(indent=2) + "\n", encoding="utf-8"
    )
    return output_path, final_manifest_path


DATASET_SCHEMA = PIT_DATASET_SCHEMA
DatasetBuilder = PointInTimeDatasetBuilder
build_dataset = build_point_in_time_dataset


__all__ = [
    "PIT_DATASET_SCHEMA",
    "DATASET_SCHEMA",
    "DatasetBuilder",
    "PointInTimeDataset",
    "PointInTimeDatasetBuilder",
    "build_point_in_time_dataset",
    "build_dataset",
    "load_generated_run",
    "write_point_in_time_dataset",
]
