"""Core synthetic entities.

These models deliberately contain no generated real-world personal data. They
are immutable after validation so relationship-bearing records cannot be
changed accidentally while being written to an output sink.
"""

from datetime import date, datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class _EntityModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    @field_validator("*", mode="after")
    @classmethod
    def datetimes_must_be_timezone_aware(cls, value: Any) -> Any:
        if isinstance(value, datetime) and (value.tzinfo is None or value.utcoffset() is None):
            raise ValueError("entity datetimes must include a timezone")
        return value


class Customer(_EntityModel):
    """A synthetic customer and its valid-time metadata."""

    customer_id: str
    customer_type: Literal["PERSONAL", "BUSINESS"]
    customer_status: Literal["PENDING", "ACTIVE", "RESTRICTED", "BLOCKED", "INACTIVE", "CLOSED"]
    date_of_birth: date
    country: str
    city: str
    registration_date: datetime
    risk_segment: str
    income_band: str
    occupation_category: str
    preferred_channels: tuple[str, ...]
    created_at: datetime
    updated_at: datetime
    valid_from: datetime
    valid_to: datetime | None
    system_from: datetime
    system_to: datetime | None


class Institution(_EntityModel):
    """A synthetic bank, PSP, issuer, or acquirer."""

    institution_id: str
    institution_type: Literal[
        "BANK",
        "PSP",
        "ISSUER",
        "ACQUIRER",
        "DIGITAL_BANK",
        "PAYMENT_INSTITUTION",
    ]
    country: str
    institution_code: str
    risk_profile: str
    processing_latency_profile: str


class Account(_EntityModel):
    """A synthetic customer account held at an institution."""

    account_id: str
    customer_id: str
    institution_id: str
    account_type: Literal[
        "CHECKING",
        "PAYMENT_ACCOUNT",
        "CREDIT_CARD_ACCOUNT",
        "SAVINGS",
        "PERSONAL_LOAN",
        "BUSINESS_ACCOUNT",
    ]
    currency: str
    opening_date: datetime
    closing_date: datetime | None
    status: Literal["PENDING", "ACTIVE", "RESTRICTED", "BLOCKED", "CLOSED"]
    credit_limit: float = Field(ge=0)
    available_balance: float
    ledger_balance: float
    overdraft_limit: float = Field(ge=0)
    created_at: datetime
    updated_at: datetime
    valid_from: datetime
    valid_to: datetime | None
    system_from: datetime
    system_to: datetime | None


class Card(_EntityModel):
    """A synthetic card associated with an account and its customer."""

    card_id: str
    account_id: str
    customer_id: str
    scheme: Literal["VISA", "MASTERCARD", "OTHER"]
    card_type: Literal["DEBIT", "CREDIT", "PREPAID"]
    status: Literal["PENDING", "ACTIVE", "BLOCKED", "EXPIRED", "CLOSED"]
    issued_at: datetime
    expires_at: datetime
    country: str
    network_token_enabled: bool
    contactless_enabled: bool
    online_enabled: bool
    international_enabled: bool
    daily_limit: float = Field(ge=0)
    transaction_limit: float = Field(ge=0)


class PixKey(_EntityModel):
    """A synthetic PIX-like payment key."""

    pix_key_id: str
    account_id: str
    customer_id: str
    institution_id: str
    key_type: Literal["CPF_LIKE", "PHONE", "EMAIL", "RANDOM", "BUSINESS_ID_LIKE"]
    key_hash_or_synthetic_value: str
    created_at: datetime
    status: Literal["ACTIVE", "INACTIVE", "BLOCKED"]


class Merchant(_EntityModel):
    """A synthetic merchant acquired by an institution."""

    merchant_id: str
    merchant_name: str
    merchant_category_code: str
    country: str
    city: str
    risk_segment: str
    acquirer_id: str
    online_only: bool
    created_at: datetime


class Device(_EntityModel):
    """A synthetic device fingerprint associated with payment activity."""

    device_id: str
    device_type: Literal["MOBILE", "DESKTOP", "TABLET", "POS_TERMINAL", "ATM"]
    os_family: str
    browser_family: str
    first_seen_at: datetime
    last_seen_at: datetime
    trusted: bool
    device_fingerprint: str
    risk_score: float = Field(ge=0, le=1)


class EntityStateChange(_EntityModel):
    """Effective-dated state history for reconstructing entity status."""

    entity_id: str
    entity_type: Literal["CUSTOMER", "ACCOUNT"]
    from_status: str
    to_status: str
    effective_at: datetime
    system_from: datetime
    system_to: datetime | None
