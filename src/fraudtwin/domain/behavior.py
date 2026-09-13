"""Customer behavior profiles used by the legitimate payment generator."""

from typing import Literal

from pydantic import Field, model_validator

from fraudtwin.domain.entities import _EntityModel


class BehaviorProfile(_EntityModel):
    """A deterministic, synthetic latent profile for one customer."""

    behavior_profile_id: str
    customer_id: str
    spending_level: Literal["LOW", "MEDIUM", "HIGH"]
    typical_payment_hours: tuple[int, ...]
    hour_weights: tuple[float, ...] = Field(min_length=24, max_length=24)
    weekday_weights: tuple[float, ...] = Field(min_length=7, max_length=7)
    typical_countries: tuple[str, ...]
    merchant_category_preferences: tuple[str, ...]
    merchant_category_weights: tuple[float, ...]
    monthly_income: float = Field(gt=0)
    monthly_spending_budget: float = Field(gt=0)
    card_vs_transfer_preference: float = Field(ge=0, le=1)
    online_purchase_rate: float = Field(ge=0, le=1)
    travel_frequency: float = Field(ge=0, le=1)
    preferred_device_ids: tuple[str, ...]
    trusted_device_count: int = Field(ge=0)

    @model_validator(mode="after")
    def profile_distributions_are_valid(self) -> "BehaviorProfile":
        if any(weight < 0 for weight in self.hour_weights) or sum(self.hour_weights) <= 0:
            raise ValueError("hour_weights must contain non-negative values with a positive sum")
        if any(weight < 0 for weight in self.weekday_weights) or sum(self.weekday_weights) <= 0:
            raise ValueError("weekday_weights must contain non-negative values with a positive sum")
        if len(self.merchant_category_preferences) != len(self.merchant_category_weights):
            raise ValueError("merchant category preferences and weights must have equal lengths")
        if any(weight < 0 for weight in self.merchant_category_weights):
            raise ValueError("merchant category weights must be non-negative")
        if self.merchant_category_preferences and sum(self.merchant_category_weights) <= 0:
            raise ValueError("merchant category weights must have a positive sum")
        if self.monthly_spending_budget > self.monthly_income:
            raise ValueError("monthly_spending_budget cannot exceed monthly_income")
        if self.trusted_device_count != len(self.preferred_device_ids):
            raise ValueError("trusted_device_count must match preferred_device_ids")
        if any(hour < 0 or hour > 23 for hour in self.typical_payment_hours):
            raise ValueError("typical_payment_hours must contain hours from 0 through 23")
        return self
