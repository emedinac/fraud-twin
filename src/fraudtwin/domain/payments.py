"""Minimal payment and ledger primitives required by Milestone 3."""

from datetime import datetime
from typing import Literal

from pydantic import Field

from fraudtwin.domain.entities import _EntityModel

PaymentRail = Literal["CARD", "PIX", "ACCOUNT_TRANSFER"]


class Payment(_EntityModel):
    """The business object represented by one legitimate payment."""

    payment_id: str
    payment_rail: PaymentRail
    payment_type: Literal["PURCHASE", "TRANSFER"]
    payer_account_id: str
    payee_account_id: str | None
    merchant_id: str | None
    card_id: str | None
    amount: float = Field(gt=0)
    currency: str
    initiated_at: datetime
    current_status: Literal["COMPLETED", "SETTLED"]


class PaymentEvent(_EntityModel):
    """A common event envelope for a generated payment event."""

    event_id: str
    event_type: Literal["CARD_PAYMENT_COMPLETED", "PIX_SETTLED", "TRANSFER_COMPLETED"]
    event_version: int = Field(ge=1)
    payment_id: str
    customer_id: str
    account_id: str
    event_time: datetime
    source_created_at: datetime
    source_available_at: datetime
    ingested_at: datetime
    processed_at: datetime
    producer: str
    source_system: str
    schema_version: str
    correlation_id: str
    causation_id: str | None
    simulation_run_id: str
    scenario_id: str | None
    payment_rail: PaymentRail
    payment_type: Literal["PURCHASE", "TRANSFER"]
    payee_account_id: str | None
    merchant_id: str | None
    card_id: str | None
    device_id: str | None
    online: bool
    amount: float = Field(gt=0)
    currency: str


class LedgerEntry(_EntityModel):
    """A minimal double-entry-compatible ledger record for future milestones."""

    ledger_entry_id: str
    payment_id: str
    account_id: str
    entry_type: Literal["DEBIT", "CREDIT"]
    amount: float = Field(gt=0)
    currency: str
    occurred_at: datetime
