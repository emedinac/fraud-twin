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
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("start must include a timezone")
        return value


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
    authorization_delay_seconds: Annotated[int, Field(ge=0)] = 1
    capture_delay_seconds: Annotated[int, Field(ge=0)] = 5
    clearing_delay_seconds: Annotated[int, Field(ge=0)] = 30
    settlement_delay_seconds: Annotated[int, Field(ge=0)] = 60
    reversal_delay_seconds: Annotated[int, Field(ge=0)] = 10
    refund_delay_seconds: Annotated[int, Field(ge=0)] = 60

    @property
    def maximum_delay_seconds(self) -> int:
        """Return the largest possible lifecycle delay before envelope timing."""

        return sum(
            (
                self.authorization_delay_seconds,
                self.capture_delay_seconds,
                self.clearing_delay_seconds,
                self.settlement_delay_seconds,
                self.reversal_delay_seconds,
                self.refund_delay_seconds,
            )
        )


class PixLifecycleConfig(_StrictModel):
    """Deterministic probabilities and delays for PIX payment lifecycles."""

    authorization_approval_probability: float = Field(default=0.98, ge=0, le=1)
    rejection_probability: float = Field(default=0.02, ge=0, le=1)
    return_probability: float = Field(default=0.05, ge=0, le=1)
    validation_delay_seconds: Annotated[int, Field(ge=0)] = 1
    authorization_delay_seconds: Annotated[int, Field(ge=0)] = 1
    submission_delay_seconds: Annotated[int, Field(ge=0)] = 1
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
                self.settlement_delay_seconds,
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
        return _QUALITY_PROFILE_DEFAULTS[self.profile][fault_name]


_QUALITY_PROFILE_DEFAULTS: dict[QualityProfile, dict[str, float]] = {
    "clean": {
        "duplicate_record": 0.0,
        "duplicate_event": 0.0,
        "missing_optional": 0.0,
        "invalid_value": 0.0,
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
        if value is not None and (value.tzinfo is None or value.utcoffset() is None):
            raise ValueError("temporal split timestamps must include a timezone")
        return value

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
        if value is not None and (value.tzinfo is None or value.utcoffset() is None):
            raise ValueError("dataset timestamps must include a timezone")
        return value

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
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("regime timestamps must include a timezone")
        return value

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
