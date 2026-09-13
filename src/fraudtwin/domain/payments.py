"""Payment primitives and card-lifecycle transition rules."""

from datetime import datetime
from typing import Literal

from pydantic import Field

from fraudtwin.domain.entities import _EntityModel

PaymentRail = Literal["CARD", "PIX", "ACCOUNT_TRANSFER"]
PaymentType = Literal["PURCHASE", "TRANSFER"]
CardLifecycleEventType = Literal[
    "CARD_AUTHORIZATION_REQUESTED",
    "CARD_AUTHORIZED",
    "CARD_DECLINED",
    "CARD_REVERSED",
    "CARD_CAPTURED",
    "CARD_CLEARED",
    "CARD_SETTLED",
    "CARD_REFUNDED",
]
PaymentEventType = CardLifecycleEventType | Literal["PIX_SETTLED", "TRANSFER_COMPLETED"]
CARD_LIFECYCLE_EVENT_TYPES = (
    "CARD_AUTHORIZATION_REQUESTED",
    "CARD_AUTHORIZED",
    "CARD_DECLINED",
    "CARD_REVERSED",
    "CARD_CAPTURED",
    "CARD_CLEARED",
    "CARD_SETTLED",
    "CARD_REFUNDED",
)
_CARD_LIFECYCLE_TRANSITIONS: dict[str, tuple[str, ...]] = {
    "CARD_AUTHORIZATION_REQUESTED": ("CARD_AUTHORIZED", "CARD_DECLINED"),
    "CARD_AUTHORIZED": ("CARD_REVERSED", "CARD_CAPTURED"),
    "CARD_CAPTURED": ("CARD_REVERSED", "CARD_CLEARED"),
    "CARD_CLEARED": ("CARD_SETTLED",),
    "CARD_SETTLED": ("CARD_REFUNDED",),
    "CARD_DECLINED": (),
    "CARD_REVERSED": (),
    "CARD_REFUNDED": (),
}


class Payment(_EntityModel):
    """The business object represented by one legitimate payment."""

    payment_id: str
    payment_rail: PaymentRail
    payment_type: PaymentType
    payer_account_id: str
    payee_account_id: str | None
    merchant_id: str | None
    card_id: str | None
    amount: float = Field(gt=0)
    currency: str
    initiated_at: datetime
    current_status: Literal[
        "AUTHORIZED",
        "DECLINED",
        "CAPTURED",
        "CLEARED",
        "SETTLED",
        "REVERSED",
        "REFUNDED",
        "COMPLETED",
    ]


class PaymentEvent(_EntityModel):
    """A common event envelope for a generated payment event."""

    event_id: str
    event_type: PaymentEventType
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


def validate_card_lifecycle(payment: Payment, events: tuple[PaymentEvent, ...]) -> None:
    """Reject card event sequences that cannot occur in the card rail."""

    if payment.payment_rail != "CARD":
        return
    if not events:
        raise ValueError(f"card payment {payment.payment_id} has no lifecycle events")

    expected_payment_id = payment.payment_id
    if any(event.payment_id != expected_payment_id for event in events):
        raise ValueError("card lifecycle event references a different payment")
    if any(event.payment_rail != "CARD" for event in events):
        raise ValueError("card lifecycle contains a non-card event")
    if any(event.event_type not in CARD_LIFECYCLE_EVENT_TYPES for event in events):
        raise ValueError("card lifecycle contains an unsupported event type")
    if len({event.event_id for event in events}) != len(events):
        raise ValueError("card lifecycle contains duplicate event IDs")
    if any(event.correlation_id != expected_payment_id for event in events):
        raise ValueError("card lifecycle events must share the payment correlation ID")
    if events[0].causation_id is not None:
        raise ValueError("card lifecycle must begin without a causation ID")
    if any(
        current.causation_id != previous.event_id
        for previous, current in zip(events, events[1:], strict=False)
    ):
        raise ValueError("card lifecycle causation IDs must form an event chain")
    if any(
        current.event_time <= previous.event_time
        for previous, current in zip(events, events[1:], strict=False)
    ):
        raise ValueError("card lifecycle event times must be strictly increasing")
    if events[0].event_type != "CARD_AUTHORIZATION_REQUESTED":
        raise ValueError("card lifecycle must begin with authorization requested")

    for previous, current in zip(events, events[1:], strict=False):
        allowed = _CARD_LIFECYCLE_TRANSITIONS[previous.event_type]
        if current.event_type not in allowed:
            raise ValueError(
                f"invalid card lifecycle transition: {previous.event_type} -> "
                f"{current.event_type}"
            )
    if events[-1].event_type == "CARD_DECLINED" and any(
        event.event_type in {"CARD_CAPTURED", "CARD_CLEARED", "CARD_SETTLED"} for event in events
    ):
        raise ValueError("declined card payment cannot be captured, cleared, or settled")
    expected_status = {
        "CARD_DECLINED": "DECLINED",
        "CARD_REVERSED": "REVERSED",
        "CARD_SETTLED": "SETTLED",
        "CARD_REFUNDED": "REFUNDED",
    }.get(events[-1].event_type)
    if expected_status is None or payment.current_status != expected_status:
        raise ValueError("card payment status does not match its lifecycle terminal event")


class LedgerEntry(_EntityModel):
    """A minimal double-entry-compatible ledger record for future milestones."""

    ledger_entry_id: str
    payment_id: str
    account_id: str
    entry_type: Literal["DEBIT", "CREDIT"]
    amount: float = Field(gt=0)
    currency: str
    occurred_at: datetime
