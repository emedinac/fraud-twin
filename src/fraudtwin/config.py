import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Annotated, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

Rail = Literal["CARD", "PIX", "ACCOUNT_TRANSFER"]
Speed = Literal["batch", "real_time", "accelerated"]


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


class FraudConfig(_StrictModel):
    """Ground-truth fraud-rate settings."""

    target_rate: Annotated[float, Field(ge=0, le=1)]


class QualityConfig(_StrictModel):
    """Data-quality profile to apply in later milestones."""

    profile: str = Field(min_length=1)


class OutputsConfig(_StrictModel):
    """Output sinks enabled for later milestones."""

    parquet: bool = False
    postgres: bool = False
    kafka: bool = False


class SimulationRunConfig(_StrictModel):
    """Top-level configuration accepted by the CLI."""

    simulation: SimulationConfig
    population: PopulationConfig
    payments: PaymentsConfig
    behavior: BehaviorConfig = Field(default_factory=BehaviorConfig)
    fraud: FraudConfig
    quality: QualityConfig
    outputs: OutputsConfig


def load_config(path: Path) -> SimulationRunConfig:
    """Load and validate a YAML configuration file."""

    if not path.is_file():
        raise FileNotFoundError(f"configuration file does not exist: {path}")

    with path.open(encoding="utf-8") as config_file:
        raw_config = yaml.safe_load(config_file)

    if not isinstance(raw_config, dict):
        raise ValueError("configuration root must be a mapping")
    return SimulationRunConfig.model_validate(raw_config)


def config_hash(config: SimulationRunConfig) -> str:
    """Return a stable SHA-256 hash of the validated configuration."""

    canonical = json.dumps(
        config.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()
