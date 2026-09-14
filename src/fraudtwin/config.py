import hashlib
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Annotated, Any, Literal, cast

import yaml
from pydantic import AliasChoices, BaseModel, ConfigDict, Field, field_validator, model_validator

Rail = Literal["CARD", "PIX", "ACCOUNT_TRANSFER"]
FraudScenarioId = Literal["F01", "F02", "F03", "F04", "F05"]
FRAUD_SCENARIO_IDS: tuple[FraudScenarioId, ...] = ("F01", "F02", "F03", "F04", "F05")
Speed = Literal["batch", "real_time", "accelerated"]
QualityProfile = Literal["clean", "realistic", "hostile"]
UnresolvedLabelPolicy = Literal["exclude", "include"]
MinimumLabelMaturityPolicy = Literal["exclude", "include_unresolved"]
TrainWindowMode = Literal["fixed", "expanding"]
RegimeLabelPolicy = Literal["original", "unobserved"]
FeatureWindowName = Literal[
    "transaction_count_1m",
    "transaction_count_5m",
    "transaction_count_1h",
    "transaction_count_24h",
    "transaction_count_7d",
    "transaction_count_30d",
    "transaction_amount_1h",
    "transaction_amount_24h",
    "transaction_amount_7d",
    "avg_transaction_amount_30d",
    "max_transaction_amount_7d",
    "distinct_merchants_1d",
    "distinct_merchants_30d",
    "merchant_fraud_rate_historical",
    "distinct_countries_24h",
    "device_customer_count_30d",
    "customers_per_device_24h",
    "confirmed_fraud_count_90d",
    "fraud_loss_365d",
    "days_since_last_confirmed_fraud",
]
# Source delay plus one second each for ingestion and processing.
CARD_SOURCE_DELAY_SECONDS = 5
PIX_SOURCE_DELAY_SECONDS = 2
ACCOUNT_TRANSFER_SOURCE_DELAY_SECONDS = 5
CARD_EVENT_ENVELOPE_DELAY_SECONDS = CARD_SOURCE_DELAY_SECONDS + 2
PIX_EVENT_ENVELOPE_DELAY_SECONDS = PIX_SOURCE_DELAY_SECONDS + 2
ACCOUNT_TRANSFER_EVENT_ENVELOPE_DELAY_SECONDS = ACCOUNT_TRANSFER_SOURCE_DELAY_SECONDS + 2


def _require_timezone(value: datetime, message: str) -> datetime:
    """Validate a timezone-aware datetime while preserving caller-specific errors."""

    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(message)
    return value


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)


class SimulationConfig(_StrictModel):
    """Clock and execution settings for a simulation."""

    seed: Annotated[int, Field(ge=0)]
    start: datetime
    duration_days: Annotated[int, Field(gt=0)]
    speed: Speed = "batch"

    @field_validator("start")
    @classmethod
    def start_must_include_timezone(cls, value: datetime) -> datetime:
        return _require_timezone(value, "start must include a timezone")


class PopulationConfig(_StrictModel):
    """Requested population sizes."""

    customers: Annotated[int, Field(ge=0)]
    institutions: Annotated[int, Field(ge=0)]
    accounts: Annotated[int, Field(ge=0)]
    cards: Annotated[int, Field(ge=0)]
    merchants: Annotated[int, Field(ge=0)]
    devices: Annotated[int, Field(ge=0)]
    pix_keys: Annotated[int, Field(ge=0)]

    @model_validator(mode="after")
    def relationships_have_required_pools(self) -> "PopulationConfig":
        """Reject populations that cannot satisfy the entity relationships."""

        if self.accounts and (not self.customers or not self.institutions):
            raise ValueError("accounts require at least one customer and institution")
        if self.cards and not self.accounts:
            raise ValueError("cards require at least one account")
        if self.merchants and not self.institutions:
            raise ValueError("merchants require at least one institution")
        if self.pix_keys and (not self.accounts or not self.customers or not self.institutions):
            raise ValueError("pix_keys require at least one account, customer, and institution")
        return self


class PaymentsConfig(_StrictModel):
    """Payment-volume and rail-mix settings."""

    daily_target: Annotated[int, Field(ge=0)]
    rails: dict[Rail, Annotated[float, Field(ge=0)]]

    @field_validator("rails")
    @classmethod
    def rail_weights_must_form_distribution(cls, value: dict[Rail, float]) -> dict[Rail, float]:
        if not value:
            raise ValueError("rails must contain at least one payment rail")
        if abs(sum(value.values()) - 1.0) > 1e-9:
            raise ValueError("rail weights must sum to 1.0")
        return value


class BehaviorConfig(_StrictModel):
    """Settings used by the customer behavior and legitimate payment engine."""

    amount_min: float = Field(default=1.0, gt=0)
    amount_max: float = Field(default=5_000.0, gt=0)
    active_hours: tuple[int, ...] = Field(default=tuple(range(24)), min_length=1)
    weekday_weights: tuple[float, ...] = (
        1.0,
        1.0,
        1.0,
        1.0,
        1.0,
        0.85,
        0.7,
    )
    merchant_preference_count: Annotated[int, Field(ge=1)] = 3
    preferred_device_limit: Annotated[int, Field(ge=0)] = 3
    spending_level_weights: tuple[float, float, float] = (0.3, 0.5, 0.2)
    payday_days: tuple[int, ...] = (1, 15)
    payday_weight: float = Field(default=1.25, ge=0)
    beginning_of_month_weight: float = Field(default=1.0, ge=0)
    end_of_month_weight: float = Field(default=1.0, ge=0)
    holiday_dates: tuple[str, ...] = ()
    holiday_weight: float = Field(default=1.0, ge=0)
    travel_period_months: tuple[int, ...] = tuple(range(1, 13))
    merchant_active_hours: tuple[int, ...] = tuple(range(24))
    state_change_probability: float = Field(default=0.0, ge=0, le=1)

    @field_validator("active_hours")
    @classmethod
    def active_hours_must_be_unique_valid_hours(cls, value: tuple[int, ...]) -> tuple[int, ...]:
        if len(set(value)) != len(value):
            raise ValueError("active_hours must not contain duplicates")
        if any(hour < 0 or hour > 23 for hour in value):
            raise ValueError("active_hours must contain hours from 0 through 23")
        return value

    @field_validator("weekday_weights")
    @classmethod
    def weekday_weights_must_be_valid_distribution(
        cls, value: tuple[float, ...]
    ) -> tuple[float, ...]:
        if len(value) != 7:
            raise ValueError("weekday_weights must contain seven non-negative values")
        if any(weight < 0 for weight in value) or sum(value) <= 0:
            raise ValueError("weekday_weights must contain non-negative values with a positive sum")
        return value

    @field_validator("spending_level_weights")
    @classmethod
    def spending_weights_must_be_valid(
        cls, value: tuple[float, float, float]
    ) -> tuple[float, float, float]:
        if any(weight < 0 for weight in value) or sum(value) <= 0:
            raise ValueError(
                "spending_level_weights must contain non-negative values with a positive sum"
            )
        return value

    @field_validator("payday_days")
    @classmethod
    def payday_days_must_be_valid(cls, value: tuple[int, ...]) -> tuple[int, ...]:
        if any(day < 1 or day > 31 for day in value):
            raise ValueError("payday_days must contain calendar days from 1 through 31")
        return value

    @field_validator("merchant_active_hours")
    @classmethod
    def merchant_hours_must_be_valid(cls, value: tuple[int, ...]) -> tuple[int, ...]:
        if len(set(value)) != len(value) or any(hour < 0 or hour > 23 for hour in value):
            raise ValueError("merchant_active_hours must contain unique hours from 0 through 23")
        return value

    @field_validator("travel_period_months")
    @classmethod
    def travel_months_must_be_valid(cls, value: tuple[int, ...]) -> tuple[int, ...]:
        if len(set(value)) != len(value) or any(month < 1 or month > 12 for month in value):
            raise ValueError("travel_period_months must contain unique months from 1 through 12")
        return value

    @model_validator(mode="after")
    def amount_bounds_must_be_ordered(self) -> "BehaviorConfig":
        if self.amount_max < self.amount_min:
            raise ValueError("amount_max must be greater than or equal to amount_min")
        return self


class CardLifecycleConfig(_StrictModel):
    """Deterministic probabilities and delays for card payment lifecycles."""

    authorization_approval_probability: float = Field(default=0.9, ge=0, le=1)
    reversal_probability: float = Field(default=0.05, ge=0, le=1)
    refund_probability: float = Field(default=0.1, ge=0, le=1)
    chargeback_probability: float = Field(default=0.0, ge=0, le=1)
    authorization_delay_seconds: Annotated[int, Field(ge=0)] = 1
    capture_delay_seconds: Annotated[int, Field(ge=0)] = 5
    clearing_delay_seconds: Annotated[int, Field(ge=0)] = 30
    settlement_delay_seconds: Annotated[int, Field(ge=0)] = 60
    reversal_delay_seconds: Annotated[int, Field(ge=0)] = 10
    refund_delay_seconds: Annotated[int, Field(ge=0)] = 60
    chargeback_delay_seconds: Annotated[int, Field(ge=0)] = 86_400
    chargeback_resolution_delay_seconds: Annotated[int, Field(ge=0)] = 86_400

    @property
    def maximum_delay_seconds(self) -> int:
        """Return the largest possible lifecycle delay before envelope timing."""

        return sum(
            (
                self.authorization_delay_seconds,
                self.authorization_delay_seconds,
                self.capture_delay_seconds,
                self.clearing_delay_seconds,
                self.settlement_delay_seconds,
                self.reversal_delay_seconds,
                self.refund_delay_seconds,
                self.chargeback_delay_seconds + self.chargeback_resolution_delay_seconds
                if self.chargeback_probability > 0
                else 0,
            )
        )


class PixLifecycleConfig(_StrictModel):
    """Deterministic probabilities and delays for PIX payment lifecycles."""

    authorization_approval_probability: float = Field(default=0.98, ge=0, le=1)
    rejection_probability: float = Field(default=0.02, ge=0, le=1)
    timeout_probability: float = Field(default=0.0, ge=0, le=1)
    return_probability: float = Field(default=0.05, ge=0, le=1)
    validation_delay_seconds: Annotated[int, Field(ge=0)] = 1
    authorization_delay_seconds: Annotated[int, Field(ge=0)] = 1
    submission_delay_seconds: Annotated[int, Field(ge=0)] = 1
    timeout_delay_seconds: Annotated[int, Field(ge=0)] = 1
    settlement_delay_seconds: Annotated[int, Field(ge=0)] = 1
    receipt_delay_seconds: Annotated[int, Field(ge=0)] = 1
    return_request_delay_seconds: Annotated[int, Field(ge=0)] = 60
    return_delay_seconds: Annotated[int, Field(ge=0)] = 60

    @property
    def maximum_delay_seconds(self) -> int:
        """Return the longest possible PIX path, including a return."""

        return sum(
            (
                self.validation_delay_seconds,
                self.authorization_delay_seconds,
                self.submission_delay_seconds,
                max(self.timeout_delay_seconds, self.settlement_delay_seconds),
                self.receipt_delay_seconds,
                self.return_request_delay_seconds,
                self.return_delay_seconds,
            )
        )


class FraudScenarioSettings(_StrictModel):
    """Bounds and prevalence controls shared by one M6 scenario."""

    enabled: bool = True
    weight: Annotated[float, Field(ge=0)] = 1.0
    count: Annotated[int, Field(ge=0)] = 1
    amount_min: float | None = Field(default=None, gt=0)
    amount_max: float | None = Field(default=None, gt=0)
    duration_seconds: Annotated[int, Field(ge=0)] = 0
    attempt_count: Annotated[int, Field(ge=1)] = 20
    window_seconds: Annotated[int, Field(gt=0)] = 60

    @model_validator(mode="after")
    def amount_bounds_must_be_ordered(self) -> "FraudScenarioSettings":
        if (
            self.amount_min is not None
            and self.amount_max is not None
            and self.amount_max < self.amount_min
        ):
            raise ValueError(
                "fraud scenario amount_max must be greater than or equal to amount_min"
            )
        return self


def _default_fraud_scenarios() -> dict[FraudScenarioId, FraudScenarioSettings]:
    return {scenario_id: FraudScenarioSettings() for scenario_id in FRAUD_SCENARIO_IDS}


class FraudConfig(_StrictModel):
    """Explicit scenario settings for the first fraud release."""

    enabled: bool = False
    target_rate: Annotated[float, Field(ge=0, le=1)]
    scenario_count: Annotated[int, Field(ge=0)] = 5
    hard_negative_rate: Annotated[float, Field(ge=0, le=1)] = 1.0
    scenarios: dict[FraudScenarioId, FraudScenarioSettings] = Field(
        default_factory=_default_fraud_scenarios
    )

    @model_validator(mode="before")
    @classmethod
    def fill_missing_scenario_settings(cls, value: Any) -> Any:
        if isinstance(value, dict) and isinstance(value.get("scenarios", {}), dict):
            value = dict(value)
            value["scenarios"] = {
                **_default_fraud_scenarios(),
                **value.get("scenarios", {}),
            }
        return value

    @model_validator(mode="after")
    def enabled_scenarios_must_be_selectable(self) -> "FraudConfig":
        if self.enabled and self.target_rate > 0 and self.scenario_count > 0:
            if not any(
                settings.enabled and settings.weight > 0 for settings in self.scenarios.values()
            ):
                raise ValueError(
                    "enabled fraud generation requires a scenario with positive weight"
                )
        return self


class FraudWorkflowConfig(_StrictModel):
    """Operational fraud alert, case, dispute, and label timing controls."""

    enabled: bool = True
    alert_probability: float = Field(default=1.0, ge=0, le=1)
    case_open_probability: float = Field(default=1.0, ge=0, le=1)
    confirmation_probability: float = Field(default=1.0, ge=0, le=1)
    customer_dispute_probability: float = Field(default=1.0, ge=0, le=1)
    alert_delay_seconds: Annotated[int, Field(ge=0)] = 60
    case_open_delay_seconds: Annotated[int, Field(ge=0)] = 300
    confirmation_delay_seconds: Annotated[int, Field(ge=0)] = 86_400
    customer_dispute_delay_seconds: Annotated[int, Field(ge=0)] = 172_800
    label_delay_seconds: Annotated[int, Field(ge=0)] = 3_600


class OutageConfig(_StrictModel):
    """A deterministic source-boundary outage used by M8."""

    source: str = Field(min_length=1)
    from_time: datetime = Field(validation_alias=AliasChoices("from", "from_time"))
    to_time: datetime = Field(validation_alias=AliasChoices("to", "to_time"))
    behavior: Literal["DROP", "BUFFER_AND_FLUSH", "DELAY", "PARTIAL_REJECT", "UNAVAILABLE"]
    delay_seconds: Annotated[int, Field(ge=0)] = 0
    reject_probability: float = Field(default=0.5, ge=0, le=1)

    @field_validator("from_time", "to_time")
    @classmethod
    def outage_timestamps_must_include_timezone(cls, value: datetime) -> datetime:
        return _require_timezone(value, "outage timestamps must include a timezone")

    @model_validator(mode="after")
    def outage_bounds_must_be_ordered(self) -> "OutageConfig":
        if self.to_time <= self.from_time:
            raise ValueError("outage to must be after outage from")
        return self


class SchemaChangeConfig(_StrictModel):
    """A scheduled event-schema change with optional compatibility metadata."""

    at: datetime
    event: str = Field(min_length=1)
    version: str = Field(min_length=1)
    change: dict[str, object] = Field(default_factory=dict)
    compatibility: (
        Literal[
            "BACKWARD_COMPATIBLE",
            "FORWARD_COMPATIBLE",
            "FULLY_COMPATIBLE",
            "BREAKING",
        ]
        | None
    ) = None

    @field_validator("at")
    @classmethod
    def schema_change_timestamp_must_include_timezone(cls, value: datetime) -> datetime:
        return _require_timezone(value, "schema change timestamp must include a timezone")

    @model_validator(mode="after")
    def schema_change_definition_must_be_supported(self) -> "SchemaChangeConfig":
        supported = {
            "add_optional_field",
            "rename",
            "remove_field",
            "nullability",
            "enum",
            "type",
        }
        unknown = set(self.change) - supported
        if unknown:
            raise ValueError(f"unsupported schema change operation(s): {sorted(unknown)}")
        if len(self.change) > 1:
            raise ValueError("schema change must contain exactly one operation")
        return self


class QualityConfig(_StrictModel):
    """Deterministic M8 data-quality fault controls.

    ``None`` values use the selected profile's defaults. Explicit values are
    useful for testing one fault in isolation without changing the other
    quality dimensions.
    """

    profile: QualityProfile = "clean"
    duplicate_record_probability: float | None = Field(
        default=None,
        ge=0,
        le=1,
        validation_alias=AliasChoices("duplicate_record_probability", "duplicate_records"),
    )
    duplicate_event_probability: float | None = Field(
        default=None,
        ge=0,
        le=1,
        validation_alias=AliasChoices("duplicate_event_probability", "duplicate_events"),
    )
    missing_optional_probability: float | None = Field(
        default=None,
        ge=0,
        le=1,
        validation_alias=AliasChoices("missing_optional_probability", "missing_optional"),
    )
    invalid_value_probability: float | None = Field(
        default=None,
        ge=0,
        le=1,
        validation_alias=AliasChoices("invalid_value_probability", "invalid_records"),
    )
    invalid_enum_probability: float | None = Field(default=None, ge=0, le=1)
    invalid_reference_probability: float | None = Field(default=None, ge=0, le=1)
    negative_amount_probability: float | None = Field(default=None, ge=0, le=1)
    corrupted_timestamp_probability: float | None = Field(default=None, ge=0, le=1)
    timezone_error_probability: float | None = Field(default=None, ge=0, le=1)
    schema_mismatch_probability: float | None = Field(default=None, ge=0, le=1)
    extreme_value_probability: float | None = Field(default=None, ge=0, le=1)
    encoding_error_probability: float | None = Field(default=None, ge=0, le=1)
    partition_skew_probability: float | None = Field(default=None, ge=0, le=1)
    late_event_probability: float | None = Field(
        default=None,
        ge=0,
        le=1,
        validation_alias=AliasChoices("late_event_probability", "late_events"),
    )
    out_of_order_probability: float | None = Field(
        default=None,
        ge=0,
        le=1,
        validation_alias=AliasChoices("out_of_order_probability", "out_of_order_events"),
    )
    fraud_spike_probability: float | None = Field(
        default=None,
        ge=0,
        le=1,
        validation_alias=AliasChoices("fraud_spike_probability", "fraud_spikes"),
    )
    traffic_spike_probability: float | None = Field(
        default=None,
        ge=0,
        le=1,
        validation_alias=AliasChoices("traffic_spike_probability", "traffic_spikes"),
    )
    late_event_delay_seconds: int = Field(
        default=3_600,
        ge=0,
        validation_alias=AliasChoices("late_event_delay_seconds", "late_event_delay"),
    )
    source_delay_seconds: int = Field(
        default=0,
        ge=0,
        validation_alias=AliasChoices("source_delay_seconds", "source_delay"),
    )
    fraud_spike_multiplier: int = Field(default=2, ge=1)
    traffic_spike_multiplier: int = Field(default=2, ge=1)
    outages: tuple[OutageConfig, ...] = ()
    schema_changes: tuple[SchemaChangeConfig, ...] = ()

    @model_validator(mode="after")
    def spike_multipliers_must_be_meaningful(self) -> "QualityConfig":
        if self.fraud_spike_probability and self.fraud_spike_multiplier < 2:
            raise ValueError(
                "fraud_spike_multiplier must be at least 2 when fraud spikes are enabled"
            )
        if self.traffic_spike_probability and self.traffic_spike_multiplier < 2:
            raise ValueError(
                "traffic_spike_multiplier must be at least 2 when traffic spikes are enabled"
            )
        return self

    def probability(self, fault_name: str) -> float:
        """Return an explicit probability or the selected profile default."""

        explicit = getattr(self, f"{fault_name}_probability")
        if explicit is not None:
            return cast(float, explicit)
        if fault_name == "negative_amount":
            legacy = self.invalid_value_probability
            if legacy is not None:
                return legacy
        return _QUALITY_PROFILE_DEFAULTS[self.profile][fault_name]


_QUALITY_PROFILE_DEFAULTS: dict[QualityProfile, dict[str, float]] = {
    "clean": {
        "duplicate_record": 0.0,
        "duplicate_event": 0.0,
        "missing_optional": 0.0,
        "invalid_value": 0.0,
        "invalid_enum": 0.0,
        "invalid_reference": 0.0,
        "negative_amount": 0.0,
        "corrupted_timestamp": 0.0,
        "timezone_error": 0.0,
        "schema_mismatch": 0.0,
        "extreme_value": 0.0,
        "encoding_error": 0.0,
        "partition_skew": 0.0,
        "late_event": 0.0,
        "out_of_order": 0.0,
        "fraud_spike": 0.0,
        "traffic_spike": 0.0,
    },
    "realistic": {
        "duplicate_record": 0.001,
        "duplicate_event": 0.002,
        "missing_optional": 0.01,
        "invalid_value": 0.0005,
        "invalid_enum": 0.0,
        "invalid_reference": 0.0,
        "negative_amount": 0.0005,
        "corrupted_timestamp": 0.0,
        "timezone_error": 0.0,
        "schema_mismatch": 0.0,
        "extreme_value": 0.0,
        "encoding_error": 0.0,
        "partition_skew": 0.0,
        "late_event": 0.03,
        "out_of_order": 0.02,
        "fraud_spike": 0.0,
        "traffic_spike": 0.0,
    },
    "hostile": {
        "duplicate_record": 0.03,
        "duplicate_event": 0.03,
        "missing_optional": 0.08,
        "invalid_value": 0.02,
        "invalid_enum": 0.0,
        "invalid_reference": 0.0,
        "negative_amount": 0.02,
        "corrupted_timestamp": 0.0,
        "timezone_error": 0.0,
        "schema_mismatch": 0.0,
        "extreme_value": 0.0,
        "encoding_error": 0.0,
        "partition_skew": 0.0,
        "late_event": 0.20,
        "out_of_order": 0.20,
        "fraud_spike": 0.10,
        "traffic_spike": 0.10,
    },
}


class OutputsConfig(_StrictModel):
    """Output sinks enabled for later milestones."""

    parquet: bool = False
    postgres: bool = False
    kafka: bool = False


class TemporalSplitConfig(_StrictModel):
    """Deterministic, chronological train/validation/test split settings."""

    train_fraction: float = Field(default=0.7, gt=0, lt=1)
    validation_fraction: float = Field(default=0.15, gt=0, lt=1)
    test_fraction: float = Field(default=0.15, gt=0, lt=1)
    label_delay_gap_seconds: Annotated[int, Field(ge=0)] | None = None
    train_end: datetime | None = None
    validation_end: datetime | None = None
    test_end: datetime | None = None

    @field_validator("train_end", "validation_end", "test_end")
    @classmethod
    def split_timestamps_must_include_timezone(cls, value: datetime | None) -> datetime | None:
        return (
            _require_timezone(value, "temporal split timestamps must include a timezone")
            if value is not None
            else None
        )

    @model_validator(mode="after")
    def split_fractions_must_form_distribution(self) -> "TemporalSplitConfig":
        if abs(self.train_fraction + self.validation_fraction + self.test_fraction - 1.0) > 1e-9:
            raise ValueError("temporal split fractions must sum to 1.0")
        if self.train_end is not None and self.validation_end is not None:
            if self.validation_end <= self.train_end:
                raise ValueError("validation_end must be after train_end")
        if self.validation_end is not None and self.test_end is not None:
            if self.test_end <= self.validation_end:
                raise ValueError("test_end must be after validation_end")
        return self


def _default_feature_windows() -> dict[FeatureWindowName, int]:
    return {
        "transaction_count_1m": 60,
        "transaction_count_5m": 5 * 60,
        "transaction_count_1h": 60 * 60,
        "transaction_count_24h": 24 * 60 * 60,
        "transaction_count_7d": 7 * 24 * 60 * 60,
        "transaction_count_30d": 30 * 24 * 60 * 60,
        "transaction_amount_1h": 60 * 60,
        "transaction_amount_24h": 24 * 60 * 60,
        "transaction_amount_7d": 7 * 24 * 60 * 60,
        "avg_transaction_amount_30d": 30 * 24 * 60 * 60,
        "max_transaction_amount_7d": 7 * 24 * 60 * 60,
        "distinct_merchants_1d": 24 * 60 * 60,
        "distinct_merchants_30d": 30 * 24 * 60 * 60,
        "merchant_fraud_rate_historical": 365 * 24 * 60 * 60,
        "distinct_countries_24h": 24 * 60 * 60,
        "device_customer_count_30d": 30 * 24 * 60 * 60,
        "customers_per_device_24h": 24 * 60 * 60,
        "confirmed_fraud_count_90d": 90 * 24 * 60 * 60,
        "fraud_loss_365d": 365 * 24 * 60 * 60,
        "days_since_last_confirmed_fraud": 365 * 24 * 60 * 60,
    }


class PointInTimeDatasetConfig(_StrictModel):
    """Configuration for the local M9 historical dataset builder."""

    enabled: bool = True
    start: datetime | None = None
    end: datetime | None = None
    prediction_delay_seconds: Annotated[int, Field(ge=0)] = 0
    feature_windows: dict[FeatureWindowName, Annotated[int, Field(gt=0)]] = Field(
        default_factory=_default_feature_windows
    )
    label_delay_seconds: Annotated[int, Field(ge=0)] | None = None
    unresolved_labels: UnresolvedLabelPolicy = "exclude"
    splits: TemporalSplitConfig = Field(default_factory=TemporalSplitConfig)

    @field_validator("start", "end")
    @classmethod
    def dataset_timestamps_must_include_timezone(cls, value: datetime | None) -> datetime | None:
        return (
            _require_timezone(value, "dataset timestamps must include a timezone")
            if value is not None
            else None
        )

    @model_validator(mode="after")
    def dataset_range_must_be_ordered(self) -> "PointInTimeDatasetConfig":
        if self.start is not None and self.end is not None and self.end <= self.start:
            raise ValueError("dataset end must be after dataset start")
        return self


def _parse_duration_seconds(value: Any) -> int:
    """Parse a positive duration expressed as seconds or a compact unit string."""

    if isinstance(value, bool):
        raise ValueError("duration must be a positive number of seconds")
    if isinstance(value, int):
        seconds = value
    elif isinstance(value, float) and value.is_integer():
        seconds = int(value)
    elif isinstance(value, str):
        text = value.strip().lower()
        units = (("d", 86_400), ("h", 3_600), ("m", 60), ("s", 1))
        seconds = 0
        for suffix, multiplier in units:
            if text.endswith(suffix):
                try:
                    amount = float(text[: -len(suffix)])
                except ValueError as exc:
                    raise ValueError(f"invalid duration: {value}") from exc
                if amount <= 0 or not amount.is_integer():
                    raise ValueError("duration must be a positive whole-unit value")
                seconds = int(amount * multiplier)
                break
        else:
            try:
                seconds = int(text)
            except ValueError as exc:
                raise ValueError(f"invalid duration: {value}") from exc
    else:
        raise ValueError("duration must be a positive number of seconds")
    if seconds <= 0:
        raise ValueError("duration must be positive")
    return seconds


class FraudRegimeConfig(_StrictModel):
    """A declared fraud regime applied only while generating source history."""

    id: str = Field(min_length=1)
    from_time: datetime = Field(validation_alias=AliasChoices("from", "from_time"))
    to_time: datetime = Field(validation_alias=AliasChoices("to", "to_time"))
    prevalence_multiplier: float = Field(default=1.0, ge=0)
    scenario_mix: dict[FraudScenarioId, Annotated[float, Field(ge=0)]] = Field(default_factory=dict)
    amount_multiplier: float = Field(default=1.0, gt=0)
    timing_multiplier: float = Field(default=1.0, gt=0)
    camouflage: float = Field(default=0.0, ge=0, le=1)
    campaign_intensity: float = Field(default=1.0, ge=0)
    payment_rail_mix: dict[Rail, Annotated[float, Field(ge=0)]] | None = None
    label_observation_policy: RegimeLabelPolicy = "original"

    @field_validator("from_time", "to_time")
    @classmethod
    def regime_timestamps_must_include_timezone(cls, value: datetime) -> datetime:
        return _require_timezone(value, "regime timestamps must include a timezone")

    @model_validator(mode="after")
    def regime_bounds_and_mixes_must_be_valid(self) -> "FraudRegimeConfig":
        if self.to_time <= self.from_time:
            raise ValueError("regime to must be after regime from")
        if self.scenario_mix and abs(sum(self.scenario_mix.values()) - 1.0) > 1e-9:
            raise ValueError("regime scenario_mix must sum to 1.0")
        if self.payment_rail_mix and abs(sum(self.payment_rail_mix.values()) - 1.0) > 1e-9:
            raise ValueError("regime payment_rail_mix must sum to 1.0")
        return self


class BacktestConfig(_StrictModel):
    """Rolling PIT backtest and source-history regime settings."""

    train_mode: TrainWindowMode = "expanding"
    train_window_seconds: int | None = None
    validation_window_seconds: int | None = None
    test_window_seconds: int = 30 * 86_400
    label_maturity_gap_seconds: int = 14 * 86_400
    step_seconds: int = 30 * 86_400
    minimum_label_maturity_policy: MinimumLabelMaturityPolicy = "exclude"
    regimes: tuple[FraudRegimeConfig, ...] = ()

    @field_validator(
        "train_window_seconds",
        "validation_window_seconds",
        "test_window_seconds",
        "label_maturity_gap_seconds",
        "step_seconds",
        mode="before",
    )
    @classmethod
    def durations_must_be_positive_seconds(cls, value: Any) -> int | None:
        return None if value is None else _parse_duration_seconds(value)

    @model_validator(mode="after")
    def train_window_must_match_mode(self) -> "BacktestConfig":
        if self.train_mode == "fixed" and self.train_window_seconds is None:
            raise ValueError("fixed backtests require train_window_seconds")
        if self.train_mode == "expanding" and self.train_window_seconds is not None:
            raise ValueError("expanding backtests must not set train_window_seconds")
        if self.step_seconds < self.test_window_seconds:
            raise ValueError("backtest step must be at least the test window")
        return self


GraphViewPolicy = Literal["observable", "oracle", "both"]
GraphScenarioType = Literal[
    "MULE_NETWORK",
    "CYCLIC_RING",
    "BENEFICIARY_NETWORK",
    "FAN_OUT",
    "BIPARTITE_NETWORK",
    "STACKED_NETWORK",
    "SCATTER_GATHER",
    "GATHER_SCATTER",
    "SHARED_DEVICE_INFRASTRUCTURE",
    "SHARED_IP_INFRASTRUCTURE",
    "DENSE_CAMPAIGN",
    "MERCHANT_CUSTOMER_COMMUNITY",
    "RANDOM_ALERT_CONTROL",
]
GraphModifierType = Literal[
    "SHORT_DWELL",
    "CROSS_INSTITUTION",
    "CAMOUFLAGE_RELATION",
    "CAMOUFLAGE_FEATURE",
    "STRUCTURAL_HYPEREDGE",
    "SEMANTIC_HYPEREDGE",
]

CounterfactualObjective = Literal[
    "F01",
    "F02",
    "F03",
    "F04",
    "F05",
    "MULE_NETWORK",
    "CYCLIC_RING",
    "BENEFICIARY_NETWORK",
    "FAN_OUT",
    "BIPARTITE_NETWORK",
    "STACKED_NETWORK",
    "SCATTER_GATHER",
    "GATHER_SCATTER",
    "SHARED_DEVICE_INFRASTRUCTURE",
    "SHARED_IP_INFRASTRUCTURE",
    "DENSE_CAMPAIGN",
    "MERCHANT_CUSTOMER_COMMUNITY",
    "RANDOM_ALERT_CONTROL",
]
CounterfactualDimension = Literal[
    "beneficiary",
    "device",
    "timing",
    "amount",
    "merchant",
    "geography",
    "payment_rail",
    "graph_relationships",
]


class CounterfactualDimensionConfig(_StrictModel):
    """Cost and mutability controls for one counterfactual dimension."""

    enabled: bool = True
    cost: float = Field(default=1.0, gt=0)


class CounterfactualScopeConfig(_StrictModel):
    """Global, family, or objective-level M14 overrides."""

    enabled: bool | None = None
    budget: float | None = Field(default=None, ge=0)
    dimensions: dict[CounterfactualDimension, CounterfactualDimensionConfig] = Field(
        default_factory=dict
    )


class CounterfactualRequestConfig(_StrictModel):
    """One deterministic counterfactual objective request."""

    objective: CounterfactualObjective | None = None
    scenario: CounterfactualObjective | None = Field(default=None, exclude=True)
    count: int = Field(default=1, ge=1)
    enabled: bool | None = None
    budget: float | None = Field(default=None, ge=0)
    max_distance: float | None = Field(default=None, exclude=True, ge=0)
    dimensions: dict[CounterfactualDimension, CounterfactualDimensionConfig] = Field(
        default_factory=dict
    )
    graph_template: GraphScenarioType | None = None

    @model_validator(mode="before")
    @classmethod
    def aliases_must_not_conflict(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        raw = dict(value)
        if "objective" in raw and "scenario" in raw:
            raise ValueError("counterfactual request objective and scenario aliases conflict")
        if "budget" in raw and "max_distance" in raw:
            raise ValueError("counterfactual request budget and max_distance aliases conflict")
        if "objective" not in raw and "scenario" in raw:
            raw["objective"] = raw.pop("scenario")
        if "budget" not in raw and "max_distance" in raw:
            raw["budget"] = raw.pop("max_distance")
        return raw

    @model_validator(mode="after")
    def objective_must_be_present(self) -> "CounterfactualRequestConfig":
        if self.objective is None:
            raise ValueError("counterfactual request requires objective")
        return self


def _default_counterfactual_dimensions() -> (
    dict[CounterfactualDimension, CounterfactualDimensionConfig]
):
    names: tuple[CounterfactualDimension, ...] = (
        "beneficiary",
        "device",
        "timing",
        "amount",
        "merchant",
        "geography",
        "payment_rail",
        "graph_relationships",
    )
    return {name: CounterfactualDimensionConfig() for name in names}


class CounterfactualConfig(_StrictModel):
    """Strict, opt-in Milestone 14 counterfactual controls."""

    enabled: bool = False
    budget: float = Field(default=4.0, ge=0)
    distance_function: str = "weighted_changed_dimensions_v1"
    source_strategy: Literal["chronological_first", "stable_hash"] = "chronological_first"
    allow_source_reuse: bool = False
    include_enabled_objectives: bool = False
    dimensions: dict[CounterfactualDimension, CounterfactualDimensionConfig] = Field(
        default_factory=_default_counterfactual_dimensions
    )
    families: dict[Literal["fraud", "graph"], CounterfactualScopeConfig] = Field(
        default_factory=dict
    )
    scenarios: dict[CounterfactualObjective, CounterfactualScopeConfig] = Field(
        default_factory=dict
    )
    requests: tuple[CounterfactualRequestConfig, ...] = ()

    @model_validator(mode="after")
    def requests_must_be_feasible_in_shape(self) -> "CounterfactualConfig":
        if not self.enabled:
            return self
        if not self.requests and not self.include_enabled_objectives:
            raise ValueError(
                "enabled counterfactual generation requires requests or include_enabled_objectives"
            )
        if self.distance_function != "weighted_changed_dimensions_v1":
            # Registered custom functions are resolved at runtime; names are
            # intentionally accepted here for extension compatibility.
            if not self.distance_function.strip():
                raise ValueError("counterfactual distance_function must not be empty")
        seen_objectives: set[str] = set()
        required_dimensions: dict[str, set[CounterfactualDimension]] = {
            "F03": {"device", "beneficiary"},
            "F04": {"beneficiary"},
        }
        for request in self.requests:
            assert request.objective is not None
            if request.objective in seen_objectives:
                raise ValueError(
                    "counterfactual requests must use count for repeated objectives, "
                    "not duplicate request entries"
                )
            seen_objectives.add(request.objective)
            if request.objective in {"F01", "F02", "F03", "F04", "F05"}:
                if request.graph_template is not None:
                    raise ValueError("fraud counterfactual requests cannot use graph_template")
            else:
                if request.graph_template is None:
                    raise ValueError("graph counterfactual requests require graph_template")
                if request.graph_template != request.objective:
                    raise ValueError("graph_template must match the graph counterfactual objective")
            family_name = "fraud" if request.objective.startswith("F") else "graph"
            family = self.families.get(cast(Literal["fraud", "graph"], family_name))
            objective_scope = self.scenarios.get(request.objective)
            disabled: set[CounterfactualDimension] = set()
            for dimension in _default_counterfactual_dimensions():
                setting = self.dimensions.get(dimension, CounterfactualDimensionConfig())
                for override in (
                    family.dimensions.get(dimension) if family else None,
                    objective_scope.dimensions.get(dimension) if objective_scope else None,
                    request.dimensions.get(dimension),
                ):
                    if override is not None:
                        setting = override
                if not setting.enabled:
                    disabled.add(dimension)
            required = required_dimensions.get(
                request.objective,
                {"graph_relationships"} if not request.objective.startswith("F") else set(),
            )
            missing = required & disabled
            if missing:
                names = ", ".join(sorted(missing))
                raise ValueError(
                    f"counterfactual objective {request.objective} requires disabled "
                    f"dimensions: {names}"
                )
        return self

    @property
    def active(self) -> bool:
        """Whether M14 changes generation or artifacts."""

        return self.enabled


DifficultyControlName = Literal[
    "fraud_legitimate_overlap",
    "behavioral_deviation",
    "scenario_subtlety",
    "noise_hard_negatives",
    "prevalence",
    "temporal_irregularity",
    "graph_structural_subtlety",
]

CamouflageFeatureName = Literal[
    "amount",
    "timing",
    "merchant",
    "device",
    "geography",
    "frequency",
]
CamouflageRelationName = Literal[
    "transferred_to",
    "transacted_with",
    "shares_device",
    "shares_ip",
]
CamouflageFamilyName = Literal["fraud", "graph"]


class DifficultyControls(_StrictModel):
    """Optional per-dimension M12 difficulty overrides.

    Values are normalized strengths: zero is the level profile's easiest
    setting and one is its most difficult setting.  A supplied value replaces
    only that dimension of the requested benchmark level.
    """

    fraud_legitimate_overlap: float | None = Field(default=None, ge=0, le=1)
    behavioral_deviation: float | None = Field(default=None, ge=0, le=1)
    scenario_subtlety: float | None = Field(default=None, ge=0, le=1)
    noise_hard_negatives: float | None = Field(default=None, ge=0, le=1)
    prevalence: float | None = Field(default=None, ge=0, le=1)
    temporal_irregularity: float | None = Field(default=None, ge=0, le=1)
    graph_structural_subtlety: float | None = Field(default=None, ge=0, le=1)

    @property
    def active(self) -> bool:
        """Whether at least one dimension was explicitly overridden."""

        return any(getattr(self, field) is not None for field in type(self).model_fields)

    def values(self) -> dict[str, float | None]:
        """Return all normalized override values in stable field order."""

        return {field: getattr(self, field) for field in type(self).model_fields}


class BenchmarkConfig(_StrictModel):
    """Opt-in M12 fraud-difficulty controls."""

    difficulty: Annotated[int, Field(ge=1, le=10)] | None = None
    controls: DifficultyControls = Field(default_factory=DifficultyControls)
    # M13 compatibility alias.  The canonical surface is ``stress``.
    camouflage: float | None = Field(default=None, ge=0, le=1)
    feature_camouflage: float | None = Field(default=None, ge=0, le=1)
    relation_camouflage: float | None = Field(default=None, ge=0, le=1)
    camouflage_cohort: "CamouflageCohortConfig" = Field(
        default_factory=lambda: CamouflageCohortConfig()
    )
    camouflage_families: dict[CamouflageFamilyName, "CamouflageFamilyConfig"] = Field(
        default_factory=dict
    )

    @model_validator(mode="after")
    def level_required_for_overrides(self) -> "BenchmarkConfig":
        if self.controls.active and self.difficulty is None:
            raise ValueError("benchmark.difficulty is required when controls are supplied")
        return self

    @property
    def enabled(self) -> bool:
        """Whether M12 should alter generation or artifacts."""

        return self.difficulty is not None

    @property
    def camouflage_active(self) -> bool:
        return (
            any(
                value is not None and value > 0
                for value in (
                    self.camouflage,
                    self.feature_camouflage,
                    self.relation_camouflage,
                )
            )
            or any(family.active for family in self.camouflage_families.values())
            or self.camouflage_cohort != CamouflageCohortConfig()
        )


class CamouflageCohortConfig(_StrictModel):
    """Deterministic legitimate peer selection for M13."""

    strategy: Literal["same_rail_and_profile", "same_rail", "global_legitimate"] = (
        "same_rail_and_profile"
    )
    profile_dimensions: tuple[
        Literal[
            "spending_level",
            "country",
            "merchant_category",
            "typical_payment_hour",
            "trusted_device",
        ],
        ...,
    ] = (
        "spending_level",
        "country",
        "merchant_category",
        "typical_payment_hour",
        "trusted_device",
    )
    minimum_size: Annotated[int, Field(ge=1)] = 3
    fallback: Literal["same_rail", "global_legitimate", "reject"] = "same_rail"


class CamouflageFamilyConfig(_StrictModel):
    """Optional M13 controls for the fraud or graph generator family."""

    camouflage: float | None = Field(default=None, ge=0, le=1)
    feature_camouflage: float | None = Field(default=None, ge=0, le=1)
    relation_camouflage: float | None = Field(default=None, ge=0, le=1)
    features: dict[CamouflageFeatureName, Annotated[float, Field(ge=0, le=1)]] = Field(
        default_factory=dict
    )
    relations: dict[CamouflageRelationName, Annotated[float, Field(ge=0, le=1)]] = Field(
        default_factory=dict
    )

    @property
    def active(self) -> bool:
        return (
            any(
                value is not None and value > 0
                for value in (
                    self.camouflage,
                    self.feature_camouflage,
                    self.relation_camouflage,
                )
            )
            or any(value > 0 for value in self.features.values())
            or any(value > 0 for value in self.relations.values())
        )


class StressConfig(_StrictModel):
    """Opt-in M13 camouflage controls."""

    camouflage: float | None = Field(default=None, ge=0, le=1)
    feature_camouflage: float | None = Field(default=None, ge=0, le=1)
    relation_camouflage: float | None = Field(default=None, ge=0, le=1)
    cohort: CamouflageCohortConfig = Field(default_factory=CamouflageCohortConfig)
    families: dict[CamouflageFamilyName, CamouflageFamilyConfig] = Field(default_factory=dict)

    @property
    def active(self) -> bool:
        return (
            any(
                value is not None and value > 0
                for value in (
                    self.camouflage,
                    self.feature_camouflage,
                    self.relation_camouflage,
                )
            )
            or any(family.active for family in self.families.values())
            or self.cohort != CamouflageCohortConfig()
        )


class GraphScenarioConfig(_StrictModel):
    """One discriminated, deterministic M11 scenario specification."""

    type: GraphScenarioType
    count: Annotated[int, Field(ge=0)] = 1
    member_count: int | None = Field(default=None, ge=2)
    source_count: int | None = Field(default=None, ge=1)
    destination_count: int | None = Field(default=None, ge=1)
    originator_count: int | None = Field(default=None, ge=1)
    intermediary_count: int | None = Field(default=None, ge=1)
    beneficiary_count: int | None = Field(default=None, ge=1)
    customer_count: int | None = Field(default=None, ge=1)
    merchant_count: int | None = Field(default=None, ge=1)
    edge_count: int | None = Field(default=None, ge=1)
    repeat_count: int | None = Field(default=None, ge=1)
    min_amount: float | None = Field(default=None, gt=0)
    max_amount: float | None = Field(default=None, gt=0)
    window_seconds: int | None = Field(default=None, gt=0)
    institution_scope: Literal["ANY", "SINGLE_INSTITUTION", "CROSS_INSTITUTION"] = "ANY"
    density_threshold: float = Field(default=0.5, ge=0, le=1)
    modifiers: tuple[GraphModifierType, ...] = ()

    @model_validator(mode="after")
    def validate_shape(self) -> "GraphScenarioConfig":
        if (
            self.max_amount is not None
            and self.min_amount is not None
            and self.max_amount < self.min_amount
        ):
            raise ValueError("graph scenario max_amount must be >= min_amount")
        if len(set(self.modifiers)) != len(self.modifiers):
            raise ValueError("graph scenario modifiers must be unique")
        required: dict[str, tuple[str, ...]] = {
            "MULE_NETWORK": ("source_count",),
            "CYCLIC_RING": ("member_count",),
            "BENEFICIARY_NETWORK": ("source_count",),
            "FAN_OUT": ("destination_count",),
            "BIPARTITE_NETWORK": ("originator_count", "beneficiary_count"),
            "STACKED_NETWORK": ("originator_count", "intermediary_count", "beneficiary_count"),
            "SCATTER_GATHER": ("intermediary_count",),
            "GATHER_SCATTER": ("source_count", "destination_count"),
            "SHARED_DEVICE_INFRASTRUCTURE": ("member_count",),
            "SHARED_IP_INFRASTRUCTURE": ("member_count",),
            "DENSE_CAMPAIGN": ("member_count",),
            "MERCHANT_CUSTOMER_COMMUNITY": ("customer_count", "merchant_count", "repeat_count"),
            "RANDOM_ALERT_CONTROL": ("member_count", "edge_count"),
        }
        if self.count and any(getattr(self, field) is None for field in required[self.type]):
            missing = [field for field in required[self.type] if getattr(self, field) is None]
            raise ValueError(f"graph scenario {self.type} requires {', '.join(missing)}")
        common = {
            "type",
            "count",
            "min_amount",
            "max_amount",
            "window_seconds",
            "institution_scope",
            "modifiers",
        }
        allowed_by_type: dict[str, set[str]] = {
            "MULE_NETWORK": {"source_count"},
            "CYCLIC_RING": {"member_count"},
            "BENEFICIARY_NETWORK": {"source_count"},
            "FAN_OUT": {"destination_count"},
            "BIPARTITE_NETWORK": {"originator_count", "beneficiary_count"},
            "STACKED_NETWORK": {"originator_count", "intermediary_count", "beneficiary_count"},
            "SCATTER_GATHER": {"intermediary_count"},
            "GATHER_SCATTER": {"source_count", "destination_count"},
            "SHARED_DEVICE_INFRASTRUCTURE": {"member_count"},
            "SHARED_IP_INFRASTRUCTURE": {"member_count"},
            "DENSE_CAMPAIGN": {"member_count", "edge_count", "density_threshold"},
            "MERCHANT_CUSTOMER_COMMUNITY": {"customer_count", "merchant_count", "repeat_count"},
            "RANDOM_ALERT_CONTROL": {"member_count", "edge_count"},
        }
        unsupported = sorted(
            key
            for key in self.model_fields_set
            if key not in common | allowed_by_type[self.type]
            and getattr(self, key) is not None
            and getattr(self, key) != self.model_fields[key].default
        )
        if unsupported:
            raise ValueError(
                f"graph scenario {self.type} does not support field(s): {', '.join(unsupported)}"
            )
        if "CROSS_INSTITUTION" in self.modifiers and self.institution_scope == "SINGLE_INSTITUTION":
            raise ValueError("CROSS_INSTITUTION conflicts with SINGLE_INSTITUTION")
        if "SHORT_DWELL" in self.modifiers and self.type not in {
            "MULE_NETWORK",
            "CYCLIC_RING",
            "STACKED_NETWORK",
            "SCATTER_GATHER",
            "GATHER_SCATTER",
        }:
            raise ValueError(f"SHORT_DWELL is not supported for {self.type}")
        if "CROSS_INSTITUTION" in self.modifiers and self.type in {
            "SHARED_DEVICE_INFRASTRUCTURE",
            "SHARED_IP_INFRASTRUCTURE",
        }:
            raise ValueError(f"CROSS_INSTITUTION is not supported for {self.type}")
        if self.type == "DENSE_CAMPAIGN" and self.member_count and self.edge_count is not None:
            possible = self.member_count * (self.member_count - 1)
            if self.edge_count < int(self.density_threshold * possible):
                raise ValueError("dense campaign edge_count does not meet density_threshold")
        return self


class MuleNetworkScenario(GraphScenarioConfig):
    type: Literal["MULE_NETWORK"] = "MULE_NETWORK"


class CyclicRingScenario(GraphScenarioConfig):
    type: Literal["CYCLIC_RING"] = "CYCLIC_RING"


class BeneficiaryNetworkScenario(GraphScenarioConfig):
    type: Literal["BENEFICIARY_NETWORK"] = "BENEFICIARY_NETWORK"


class FanOutScenario(GraphScenarioConfig):
    type: Literal["FAN_OUT"] = "FAN_OUT"


class BipartiteNetworkScenario(GraphScenarioConfig):
    type: Literal["BIPARTITE_NETWORK"] = "BIPARTITE_NETWORK"


class StackedNetworkScenario(GraphScenarioConfig):
    type: Literal["STACKED_NETWORK"] = "STACKED_NETWORK"


class ScatterGatherScenario(GraphScenarioConfig):
    type: Literal["SCATTER_GATHER"] = "SCATTER_GATHER"


class GatherScatterScenario(GraphScenarioConfig):
    type: Literal["GATHER_SCATTER"] = "GATHER_SCATTER"


class SharedDeviceScenario(GraphScenarioConfig):
    type: Literal["SHARED_DEVICE_INFRASTRUCTURE"] = "SHARED_DEVICE_INFRASTRUCTURE"


class SharedIPScenario(GraphScenarioConfig):
    type: Literal["SHARED_IP_INFRASTRUCTURE"] = "SHARED_IP_INFRASTRUCTURE"


class DenseCampaignScenario(GraphScenarioConfig):
    type: Literal["DENSE_CAMPAIGN"] = "DENSE_CAMPAIGN"


class MerchantCustomerCommunityScenario(GraphScenarioConfig):
    type: Literal["MERCHANT_CUSTOMER_COMMUNITY"] = "MERCHANT_CUSTOMER_COMMUNITY"


class RandomAlertControlScenario(GraphScenarioConfig):
    type: Literal["RANDOM_ALERT_CONTROL"] = "RANDOM_ALERT_CONTROL"


type GraphScenario = Annotated[
    MuleNetworkScenario
    | CyclicRingScenario
    | BeneficiaryNetworkScenario
    | FanOutScenario
    | BipartiteNetworkScenario
    | StackedNetworkScenario
    | ScatterGatherScenario
    | GatherScatterScenario
    | SharedDeviceScenario
    | SharedIPScenario
    | DenseCampaignScenario
    | MerchantCustomerCommunityScenario
    | RandomAlertControlScenario,
    Field(discriminator="type"),
]


def graph_account_capacity(scenario: GraphScenarioConfig) -> int:
    """Return the number of distinct accounts required by one scenario instance."""

    special_cases = {
        "MULE_NETWORK": (scenario.source_count or 1) + 2,
        "BENEFICIARY_NETWORK": (scenario.source_count or 1) + 1,
        "FAN_OUT": (scenario.destination_count or 1) + 1,
        "SCATTER_GATHER": (scenario.intermediary_count or 1) + 2,
        "GATHER_SCATTER": (scenario.source_count or 1) + (scenario.destination_count or 1) + 1,
    }
    if scenario.type in special_cases:
        return special_cases[scenario.type]
    return max(
        scenario.member_count or 0,
        scenario.source_count or 0,
        scenario.destination_count or 0,
        (scenario.originator_count or 0)
        + (scenario.intermediary_count or 0)
        + (scenario.beneficiary_count or 0),
        scenario.customer_count or 0,
        2,
    )


class GraphConfig(_StrictModel):
    """Strict controls for M11 graph construction and export (schema v2)."""

    schema_version: Literal["2"] = "2"
    enabled: bool = False
    scenarios: tuple[GraphScenario, ...] = ()
    relationship_window_seconds: int = Field(default=30 * 86_400, gt=0)
    pattern_window_seconds: int = Field(default=24 * 3_600, gt=0)
    ring_min_size: int = Field(default=3, ge=3, le=7)
    ring_max_size: int = Field(default=7, ge=3, le=7)
    fan_threshold: int = Field(default=3, ge=2)
    shared_threshold: int = Field(default=2, ge=2)
    dwell_threshold_seconds: int = Field(default=3_600, gt=0)
    amount_retention_tolerance: float = Field(default=0.20, ge=0, lt=1)
    export_policy: GraphViewPolicy = "both"

    @field_validator(
        "relationship_window_seconds",
        "pattern_window_seconds",
        "dwell_threshold_seconds",
        mode="before",
    )
    @classmethod
    def graph_durations_must_be_positive(cls, value: Any) -> int:
        return _parse_duration_seconds(value)

    @model_validator(mode="after")
    def graph_bounds_must_be_ordered(self) -> "GraphConfig":
        if self.ring_max_size < self.ring_min_size:
            raise ValueError("graph ring_max_size must be at least ring_min_size")
        if self.enabled and not any(item.count for item in self.scenarios):
            raise ValueError("enabled graph configuration requires at least one scenario")
        for scenario in self.scenarios:
            if (
                scenario.window_seconds is not None
                and scenario.window_seconds > self.pattern_window_seconds
            ):
                raise ValueError("scenario window_seconds cannot exceed pattern_window_seconds")
        return self


class SimulationRunConfig(_StrictModel):
    """Top-level configuration accepted by the CLI."""

    simulation: SimulationConfig
    population: PopulationConfig
    payments: PaymentsConfig
    behavior: BehaviorConfig = Field(default_factory=BehaviorConfig)
    card_lifecycle: CardLifecycleConfig = Field(default_factory=CardLifecycleConfig)
    pix_lifecycle: PixLifecycleConfig = Field(default_factory=PixLifecycleConfig)
    fraud: FraudConfig
    fraud_workflow: FraudWorkflowConfig = Field(default_factory=FraudWorkflowConfig)
    quality: QualityConfig
    outputs: OutputsConfig
    dataset: PointInTimeDatasetConfig = Field(default_factory=PointInTimeDatasetConfig)
    backtest: BacktestConfig = Field(default_factory=BacktestConfig)
    graph: GraphConfig = Field(default_factory=GraphConfig)
    benchmark: BenchmarkConfig = Field(default_factory=BenchmarkConfig)
    stress: StressConfig = Field(default_factory=StressConfig)
    counterfactual: CounterfactualConfig = Field(default_factory=CounterfactualConfig)

    def effective_label_delay_seconds(self) -> int:
        """Return the dataset label delay, falling back to workflow settings."""

        return (
            self.dataset.label_delay_seconds
            if self.dataset.label_delay_seconds is not None
            else self.fraud_workflow.label_delay_seconds
        )

    @model_validator(mode="after")
    def card_lifecycle_must_fit_simulation_window(self) -> "SimulationRunConfig":
        window_seconds = self.simulation.duration_days * 24 * 60 * 60
        if (
            self.card_lifecycle.maximum_delay_seconds + CARD_EVENT_ENVELOPE_DELAY_SECONDS
            >= window_seconds
        ):
            raise ValueError("card lifecycle timing settings must fit the simulation window")
        if (
            self.pix_lifecycle.maximum_delay_seconds + PIX_EVENT_ENVELOPE_DELAY_SECONDS
            >= window_seconds
        ):
            raise ValueError("PIX lifecycle timing settings must fit the simulation window")
        for scenario_id, settings in self.fraud.scenarios.items():
            if settings.duration_seconds >= window_seconds:
                raise ValueError(
                    f"fraud scenario {scenario_id} duration must fit the simulation window"
                )
            if settings.window_seconds >= window_seconds:
                raise ValueError(
                    f"fraud scenario {scenario_id} window must fit the simulation window"
                )
        dataset_start = self.dataset.start or self.simulation.start
        dataset_end = self.dataset.end or (
            self.simulation.start + timedelta(days=self.simulation.duration_days)
        )
        simulation_end = self.simulation.start + timedelta(days=self.simulation.duration_days)
        if dataset_start < self.simulation.start or dataset_end > simulation_end:
            raise ValueError("dataset range must fit the simulation window")
        for boundary_name, boundary in (
            ("train_end", self.dataset.splits.train_end),
            ("validation_end", self.dataset.splits.validation_end),
            ("test_end", self.dataset.splits.test_end),
        ):
            if boundary is not None and not dataset_start < boundary <= dataset_end:
                raise ValueError(f"dataset split {boundary_name} must fit the dataset range")
        previous_regime_end: datetime | None = None
        for regime in sorted(self.backtest.regimes, key=lambda item: item.from_time):
            if regime.from_time < self.simulation.start or regime.to_time > simulation_end:
                raise ValueError("fraud regime must fit the simulation window")
            if previous_regime_end is not None and regime.from_time < previous_regime_end:
                raise ValueError("fraud regimes must not overlap")
            previous_regime_end = regime.to_time
        if self.dataset.splits.test_end is not None and self.dataset.splits.test_end != dataset_end:
            raise ValueError("dataset test_end must equal the dataset end")
        label_delay_gap = self.dataset.splits.label_delay_gap_seconds
        if label_delay_gap is None:
            label_delay_gap = self.effective_label_delay_seconds()
        dataset_duration = (dataset_end - dataset_start).total_seconds()
        if 2 * label_delay_gap >= dataset_duration:
            raise ValueError("temporal label-delay gaps must fit the dataset range")
        if self.dataset.splits.train_end is not None:
            validation_end = self.dataset.splits.validation_end
            test_end = self.dataset.splits.test_end or dataset_end
            if validation_end is None:
                raise ValueError("explicit temporal splits require validation_end")
            if self.dataset.splits.train_end + timedelta(seconds=label_delay_gap) >= validation_end:
                raise ValueError("train-to-validation label-delay gap leaves no validation range")
            if validation_end + timedelta(seconds=label_delay_gap) >= test_end:
                raise ValueError("validation-to-test label-delay gap leaves no test range")
        if self.graph.enabled:
            required_accounts = sum(
                scenario.count * graph_account_capacity(scenario)
                for scenario in self.graph.scenarios
            )
            if self.population.accounts < required_accounts:
                raise ValueError(
                    "graph scenarios require more disjoint accounts than population.accounts"
                )
            required_devices = sum(
                scenario.count
                for scenario in self.graph.scenarios
                if scenario.type == "SHARED_DEVICE_INFRASTRUCTURE"
            )
            if self.population.devices < required_devices:
                raise ValueError("shared-device graph scenarios require enough devices")
            if self.population.pix_keys < sum(
                scenario.count
                for scenario in self.graph.scenarios
                if scenario.type == "BENEFICIARY_NETWORK"
            ):
                raise ValueError("beneficiary graph scenarios require at least one PIX key")
        stress_active = self.stress.active
        benchmark_camouflage_active = self.benchmark.camouflage_active
        if stress_active and benchmark_camouflage_active:
            raise ValueError(
                "M13 camouflage must be configured under stress or benchmark, not both"
            )
        if stress_active or benchmark_camouflage_active:
            if not (self.fraud.enabled or self.graph.enabled or self.counterfactual.active):
                raise ValueError("camouflage requires fraud or graph generation to be enabled")
            camouflage_settings = self.stress if stress_active else self.benchmark
            relation_strength = (
                camouflage_settings.relation_camouflage
                if camouflage_settings.relation_camouflage is not None
                else camouflage_settings.camouflage
            )
            feature_strength = (
                camouflage_settings.feature_camouflage
                if camouflage_settings.feature_camouflage is not None
                else camouflage_settings.camouflage
            )
            family_items = (
                camouflage_settings.families.items()
                if isinstance(camouflage_settings, StressConfig)
                else camouflage_settings.camouflage_families.items()
            )
            family_active = any(family.active for _, family in family_items)
            if (relation_strength or feature_strength or family_active) and (
                self.payments.daily_target == 0 or self.population.customers < 2
            ):
                raise ValueError("camouflage requires legitimate payment and customer capacity")
            if (relation_strength or family_active) and self.population.accounts < 2:
                raise ValueError("relation camouflage requires at least two accounts")
            for family_name, family in family_items:
                if (
                    family_name == "fraud"
                    and not (self.fraud.enabled or self.counterfactual.active)
                    and family.active
                ):
                    raise ValueError("fraud camouflage controls require fraud generation")
                if (
                    family_name == "graph"
                    and not (self.graph.enabled or self.counterfactual.active)
                    and family.active
                ):
                    raise ValueError("graph camouflage controls require graph generation")
        if self.benchmark.enabled and not (
            self.fraud.enabled or self.graph.enabled or self.counterfactual.active
        ):
            raise ValueError(
                "benchmark difficulty requires fraud or graph generation to be enabled"
            )
        if self.benchmark.enabled:
            if self.fraud.enabled and self.fraud.target_rate > 0:
                if self.payments.daily_target == 0 or self.population.customers == 0:
                    raise ValueError(
                        "difficulty-enabled fraud generation requires payment capacity"
                    )
                if self.population.cards == 0 or self.population.merchants == 0:
                    raise ValueError(
                        "difficulty-enabled fraud generation requires cards and merchants"
                    )
                if self.population.accounts < 2:
                    raise ValueError("difficulty-enabled fraud generation requires two accounts")
                if self.fraud.scenario_count > min(
                    self.population.cards, self.population.merchants, self.population.accounts
                ):
                    raise ValueError(
                        "difficulty-enabled fraud scenario count exceeds entity capacity"
                    )
        if self.counterfactual.active:
            if self.payments.daily_target == 0:
                raise ValueError("counterfactual generation requires legitimate payment capacity")
            if not self.counterfactual.allow_source_reuse:
                requested = sum(item.count for item in self.counterfactual.requests)
                if requested > self.payments.daily_target * self.simulation.duration_days:
                    raise ValueError("counterfactual request count exceeds trajectory capacity")
            for request in self.counterfactual.requests:
                if request.objective in {"F01", "F02", "F05"} and self.population.cards == 0:
                    raise ValueError(
                        f"counterfactual {request.objective} requires at least one card"
                    )
                if request.objective == "F04" and self.population.accounts < 2:
                    raise ValueError("counterfactual F04 requires at least two accounts")
                if (
                    request.objective
                    in {
                        "MULE_NETWORK",
                        "CYCLIC_RING",
                        "BENEFICIARY_NETWORK",
                        "FAN_OUT",
                        "BIPARTITE_NETWORK",
                        "STACKED_NETWORK",
                        "SCATTER_GATHER",
                        "GATHER_SCATTER",
                        "DENSE_CAMPAIGN",
                        "RANDOM_ALERT_CONTROL",
                    }
                    and self.population.accounts < 2
                ):
                    raise ValueError(
                        f"counterfactual {request.objective} requires at least two accounts"
                    )
            if self.counterfactual.include_enabled_objectives and not self.counterfactual.requests:
                has_fraud_objective = self.fraud.enabled and any(
                    item.enabled and item.count > 0 for item in self.fraud.scenarios.values()
                )
                has_graph_objective = self.graph.enabled and any(
                    item.count > 0 for item in self.graph.scenarios
                )
                if not has_fraud_objective and not has_graph_objective:
                    raise ValueError(
                        "counterfactual include_enabled_objectives expands to no objectives"
                    )
        return self


def load_config(path: Path) -> SimulationRunConfig:
    """Load and validate a YAML configuration file."""

    if not path.is_file():
        raise FileNotFoundError(f"configuration file does not exist: {path}")

    with path.open(encoding="utf-8") as config_file:
        raw_config = yaml.safe_load(config_file)

    if not isinstance(raw_config, dict):
        raise ValueError("configuration root must be a mapping")
    return SimulationRunConfig.model_validate(raw_config)


def config_hash(
    config: SimulationRunConfig,
    *,
    include_card_lifecycle: bool = True,
    include_pix_lifecycle: bool = True,
    include_dataset: bool = False,
) -> str:
    """Return a stable SHA-256 hash of the validated configuration.

    The optional compatibility modes keep the base payment stream identity
    unchanged when only lifecycle or M9 dataset settings differ.
    """

    return hashlib.sha256(
        _canonical_config(
            config,
            include_card_lifecycle=include_card_lifecycle,
            include_pix_lifecycle=include_pix_lifecycle,
            include_dataset=include_dataset,
        ).encode("utf-8")
    ).hexdigest()


def _canonical_config(
    config: SimulationRunConfig,
    *,
    include_card_lifecycle: bool = True,
    include_pix_lifecycle: bool = True,
    include_dataset: bool = True,
) -> str:
    """Serialize configuration once for hashes and deterministic stream IDs."""

    payload = config.model_dump(mode="json")
    if not include_card_lifecycle:
        payload.pop("card_lifecycle", None)
    if not include_pix_lifecycle:
        payload.pop("pix_lifecycle", None)
    if not include_dataset:
        payload.pop("dataset", None)
    # M11 is opt-in; a disabled graph section must not change legacy run IDs.
    if not config.graph.enabled:
        payload.pop("graph", None)
    # M12 is opt-in; a benchmark section with no level must not change legacy
    # run IDs, manifests, or fingerprints.
    if not config.benchmark.enabled and not config.benchmark.camouflage_active:
        payload.pop("benchmark", None)
    # M13 is opt-in; a disabled stress section must not alter legacy identities.
    if not config.stress.active:
        payload.pop("stress", None)
    # M14 is opt-in; its neutral section must not alter legacy identities.
    if not config.counterfactual.active:
        payload.pop("counterfactual", None)
    # Keep run identities backward-compatible when newly optional methodology
    # controls remain at their neutral defaults.
    neutral_defaults: dict[str, dict[str, object]] = {
        "behavior": {
            "spending_level_weights": [0.3, 0.5, 0.2],
            "payday_days": [1, 15],
            "payday_weight": 1.25,
            "beginning_of_month_weight": 1.0,
            "end_of_month_weight": 1.0,
            "holiday_dates": [],
            "holiday_weight": 1.0,
            "travel_period_months": list(range(1, 13)),
            "merchant_active_hours": list(range(24)),
            "state_change_probability": 0.0,
        },
        "card_lifecycle": {
            "chargeback_probability": 0.0,
            "chargeback_delay_seconds": 86_400,
            "chargeback_resolution_delay_seconds": 86_400,
        },
        "quality": {"outages": [], "schema_changes": []},
    }
    for section, defaults in neutral_defaults.items():
        section_payload = payload.get(section)
        if isinstance(section_payload, dict):
            for key, default in defaults.items():
                if section_payload.get(key) == default:
                    section_payload.pop(key, None)
    # Rolling-window settings describe read-only analysis and must not alter
    # the logical stream.  Declared regimes are different: they are generator
    # inputs, so they must participate in the source-run identity.
    if config.backtest.regimes:
        payload["backtest"] = {"regimes": payload["backtest"]["regimes"]}
    else:
        payload.pop("backtest", None)
    canonical = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
    )
    return canonical
