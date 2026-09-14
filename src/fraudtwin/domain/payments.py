"""Payment primitives and payment-rail lifecycle transition rules."""

from datetime import datetime
from typing import Literal

from pydantic import Field

from fraudtwin.domain.entities import Account, _EntityModel

PAYMENT_EVENT_CONTRACT_VERSION = "5"

PaymentRail = Literal["CARD", "PIX", "ACCOUNT_TRANSFER"]
PaymentType = Literal["PURCHASE", "TRANSFER"]
CardLifecycleEventType = Literal[
    "CARD_PAYMENT_INITIATED",
    "CARD_AUTHORIZATION_REQUESTED",
    "CARD_AUTHORIZED",
    "CARD_DECLINED",
    "CARD_REVERSED",
    "CARD_CAPTURED",
    "CARD_CLEARED",
    "CARD_SETTLED",
    "CARD_REFUNDED",
    "CARD_CHARGEBACK_CREATED",
    "CARD_CHARGEBACK_RESOLVED",
]
PixLifecycleEventType = Literal[
    "PIX_INITIATED",
    "PIX_VALIDATED",
    "PIX_AUTHORIZED",
    "PIX_SUBMITTED",
    "PIX_TIMEOUT",
    "PIX_SETTLED",
    "PIX_RECEIVED",
    "PIX_REJECTED",
    "PIX_RETURN_REQUESTED",
    "PIX_RETURNED",
]
FraudSignalEventType = Literal[
    "FRAUD_AUTHENTICATION_SUSPICIOUS",
    "FRAUD_PROFILE_CHANGED",
    "FRAUD_BENEFICIARY_ADDED",
]
PaymentEventType = (
    CardLifecycleEventType
    | PixLifecycleEventType
    | FraudSignalEventType
    | Literal["TRANSFER_COMPLETED"]
)
CARD_LIFECYCLE_EVENT_TYPES = (
    "CARD_PAYMENT_INITIATED",
    "CARD_AUTHORIZATION_REQUESTED",
    "CARD_AUTHORIZED",
    "CARD_DECLINED",
    "CARD_REVERSED",
    "CARD_CAPTURED",
    "CARD_CLEARED",
    "CARD_SETTLED",
    "CARD_REFUNDED",
    "CARD_CHARGEBACK_CREATED",
    "CARD_CHARGEBACK_RESOLVED",
)
PIX_LIFECYCLE_EVENT_TYPES = (
    "PIX_INITIATED",
    "PIX_VALIDATED",
    "PIX_AUTHORIZED",
    "PIX_SUBMITTED",
    "PIX_TIMEOUT",
    "PIX_SETTLED",
    "PIX_RECEIVED",
    "PIX_REJECTED",
    "PIX_RETURN_REQUESTED",
    "PIX_RETURNED",
)
_CARD_LIFECYCLE_TRANSITIONS: dict[str, tuple[str, ...]] = {
    "CARD_PAYMENT_INITIATED": ("CARD_AUTHORIZATION_REQUESTED",),
    "CARD_AUTHORIZATION_REQUESTED": ("CARD_AUTHORIZED", "CARD_DECLINED"),
    "CARD_AUTHORIZED": ("CARD_REVERSED", "CARD_CAPTURED"),
    "CARD_CAPTURED": ("CARD_REVERSED", "CARD_CLEARED"),
    "CARD_CLEARED": ("CARD_SETTLED",),
    "CARD_SETTLED": ("CARD_REFUNDED", "CARD_CHARGEBACK_CREATED"),
    "CARD_REFUNDED": ("CARD_CHARGEBACK_CREATED",),
    "CARD_CHARGEBACK_CREATED": ("CARD_CHARGEBACK_RESOLVED",),
    "CARD_DECLINED": (),
    "CARD_REVERSED": (),
    "CARD_CHARGEBACK_RESOLVED": (),
}


class Payment(_EntityModel):
    """The business object represented by one payment."""

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
        "REJECTED",
        "TIMED_OUT",
        "RECEIVED",
        "RETURNED",
        "CHARGEBACK_RESOLVED",
    ]
    payer_institution_id: str | None = None
    payee_institution_id: str | None = None
    payer_pix_key_id: str | None = None
    payee_pix_key_id: str | None = None


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
    ip_id: str | None = None
    transport_partition: int | None = Field(default=None, ge=0)
    online: bool
    amount: float = Field(gt=0)
    currency: str
    scenario_type: str | None = None
    scenario_trigger: str | None = None
    scenario_reason: str | None = None
    fraud_record_id: str | None = None
    affected_entity_ids: tuple[str, ...] = ()


def _validate_lifecycle_envelope(
    payment: Payment,
    events: tuple[PaymentEvent, ...],
    *,
    rail: PaymentRail,
    event_types: tuple[str, ...],
    initial_event_type: str | tuple[str, ...],
) -> None:
    """Validate the fields shared by all payment lifecycle event chains."""

    if not events:
        raise ValueError(f"{rail} payment {payment.payment_id} has no lifecycle events")

    if any(event.payment_id != payment.payment_id for event in events):
        raise ValueError(f"{rail} lifecycle event references a different payment")
    if any(event.payment_rail != rail for event in events):
        raise ValueError(f"{rail} lifecycle contains an event from another rail")
    if any(event.event_type not in event_types for event in events):
        raise ValueError(f"{rail} lifecycle contains an unsupported event type")
    if len({event.event_id for event in events}) != len(events):
        raise ValueError(f"{rail} lifecycle contains duplicate event IDs")
    if any(event.correlation_id != payment.payment_id for event in events):
        raise ValueError(f"{rail} lifecycle events must share the payment correlation ID")
    if events[0].causation_id is not None:
        raise ValueError(f"{rail} lifecycle must begin without a causation ID")
    if any(
        current.causation_id != previous.event_id
        for previous, current in zip(events, events[1:], strict=False)
    ):
        raise ValueError(f"{rail} lifecycle causation IDs must form an event chain")
    if any(
        current.event_time <= previous.event_time
        for previous, current in zip(events, events[1:], strict=False)
    ):
        raise ValueError(f"{rail} lifecycle event times must be strictly increasing")
    allowed_initial_types = (
        (initial_event_type,) if isinstance(initial_event_type, str) else initial_event_type
    )
    if events[0].event_type not in allowed_initial_types:
        raise ValueError(f"{rail} lifecycle has an invalid initial event")


def validate_card_lifecycle(payment: Payment, events: tuple[PaymentEvent, ...]) -> None:
    """Reject card event sequences that cannot occur in the card rail."""

    if payment.payment_rail != "CARD":
        return
    initial_event_type: str | tuple[str, ...] = "CARD_PAYMENT_INITIATED"
    if events and events[0].event_type == "CARD_AUTHORIZATION_REQUESTED":
        if events[0].schema_version not in {"1", "2", "3", "4"}:
            raise ValueError("card authorization-first lifecycle requires a legacy contract")
        initial_event_type = "CARD_AUTHORIZATION_REQUESTED"
    _validate_lifecycle_envelope(
        payment,
        events,
        rail="CARD",
        event_types=CARD_LIFECYCLE_EVENT_TYPES,
        initial_event_type=initial_event_type,
    )

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
        "CARD_CHARGEBACK_RESOLVED": "CHARGEBACK_RESOLVED",
    }.get(events[-1].event_type)
    if expected_status is None or payment.current_status != expected_status:
        raise ValueError("card payment status does not match its lifecycle terminal event")


_PIX_LIFECYCLE_TRANSITIONS: dict[str, tuple[str, ...]] = {
    "PIX_INITIATED": ("PIX_VALIDATED",),
    "PIX_VALIDATED": ("PIX_AUTHORIZED", "PIX_REJECTED"),
    "PIX_AUTHORIZED": ("PIX_SUBMITTED",),
    "PIX_SUBMITTED": ("PIX_SETTLED", "PIX_TIMEOUT"),
    "PIX_SETTLED": ("PIX_RECEIVED", "PIX_RETURN_REQUESTED"),
    "PIX_RECEIVED": ("PIX_RETURN_REQUESTED",),
    "PIX_RETURN_REQUESTED": ("PIX_RETURNED",),
    "PIX_REJECTED": (),
    "PIX_TIMEOUT": (),
    "PIX_RETURNED": (),
}


def validate_pix_lifecycle(payment: Payment, events: tuple[PaymentEvent, ...]) -> None:
    """Reject PIX event sequences that cannot occur on the PIX rail."""

    if payment.payment_rail != "PIX":
        return
    _validate_lifecycle_envelope(
        payment,
        events,
        rail="PIX",
        event_types=PIX_LIFECYCLE_EVENT_TYPES,
        initial_event_type="PIX_INITIATED",
    )

    for previous, current in zip(events, events[1:], strict=False):
        allowed = _PIX_LIFECYCLE_TRANSITIONS[previous.event_type]
        if current.event_type not in allowed:
            raise ValueError(
                f"invalid PIX lifecycle transition: {previous.event_type} -> {current.event_type}"
            )
    if events[-1].event_type == "PIX_REJECTED" and any(
        event.event_type in {"PIX_AUTHORIZED", "PIX_SUBMITTED", "PIX_SETTLED", "PIX_RECEIVED"}
        for event in events
    ):
        raise ValueError("rejected PIX payment cannot reach authorization or settlement")
    expected_status = {
        "PIX_REJECTED": "REJECTED",
        "PIX_TIMEOUT": "TIMED_OUT",
        "PIX_RECEIVED": "RECEIVED",
        "PIX_RETURNED": "RETURNED",
    }.get(events[-1].event_type)
    if expected_status is None or payment.current_status != expected_status:
        raise ValueError("PIX payment status does not match its lifecycle terminal event")


def validate_payment_lifecycle(payment: Payment, events: tuple[PaymentEvent, ...]) -> None:
    """Validate the lifecycle for whichever supported rail owns a payment."""

    if payment.payment_rail == "CARD":
        validate_card_lifecycle(payment, events)
    elif payment.payment_rail == "PIX":
        validate_pix_lifecycle(payment, events)


class LedgerEntry(_EntityModel):
    """A posted, double-entry-compatible ledger record."""

    ledger_entry_id: str
    account_id: str
    payment_id: str
    entry_type: Literal["DEBIT", "CREDIT"]
    amount: float = Field(gt=0)
    currency: str
    occurred_at: datetime
    event_id: str
    effective_at: datetime
    posted_at: datetime
    balance_after: float


def validate_ledger(
    accounts: tuple[Account, ...],
    payments: tuple[Payment, ...],
    events: tuple[PaymentEvent, ...],
    entries: tuple[LedgerEntry, ...],
) -> None:
    """Validate posted transfer entries against payments and account balances.

    The ledger uses each account's opening ledger balance as its deterministic
    starting point and checks every subsequent running balance.
    """

    account_by_id = {account.account_id: account for account in accounts}
    payment_by_id = {payment.payment_id: payment for payment in payments}
    event_by_id = {event.event_id: event for event in events}
    entries_by_event: dict[str, list[LedgerEntry]] = {}

    for entry in entries:
        if entry.account_id not in account_by_id:
            raise ValueError(f"ledger entry references unknown account {entry.account_id}")
        payment = payment_by_id.get(entry.payment_id)
        event = event_by_id.get(entry.event_id)
        if payment is None or event is None:
            raise ValueError("ledger entry references an unknown payment or event")
        if event.payment_id != payment.payment_id:
            raise ValueError("ledger entry payment and event references do not match")
        if entry.currency != payment.currency or entry.amount != payment.amount:
            raise ValueError("ledger entry amount or currency does not match its payment")
        if entry.effective_at != event.event_time or entry.occurred_at != event.event_time:
            raise ValueError("ledger entry effective time does not match its event")
        if entry.posted_at != event.processed_at or entry.posted_at < entry.effective_at:
            raise ValueError("ledger entry posting time is invalid")
        entries_by_event.setdefault(entry.event_id, []).append(entry)

    expected_events = {
        event.event_id
        for event in events
        if event.event_type
        in {
            "PIX_SETTLED",
            "PIX_RETURNED",
            "TRANSFER_COMPLETED",
            "CARD_SETTLED",
            "CARD_REFUNDED",
            "CARD_CHARGEBACK_RESOLVED",
        }
        and payment_by_id[event.payment_id].payee_account_id is not None
    }
    if set(entries_by_event) != expected_events:
        raise ValueError("posted transfer events and ledger events do not reconcile")

    for event_id, event_entries in entries_by_event.items():
        if len(event_entries) != 2:
            raise ValueError(f"ledger event {event_id} is not double-sided")
        event = event_by_id[event_id]
        payment = payment_by_id[event.payment_id]
        payer = payment.payer_account_id
        payee = payment.payee_account_id
        expected = (
            {(payer, "DEBIT"), (payee, "CREDIT")}
            if event.event_type in {"PIX_SETTLED", "TRANSFER_COMPLETED", "CARD_SETTLED"}
            else {(payer, "CREDIT"), (payee, "DEBIT")}
        )
        if {(entry.account_id, entry.entry_type) for entry in event_entries} != expected:
            raise ValueError(f"ledger event {event_id} has incorrect debit and credit sides")

    balances = {account_id: account.ledger_balance for account_id, account in account_by_id.items()}
    ordered_entries = sorted(
        entries,
        key=lambda entry: (entry.posted_at, entry.event_id, entry.account_id, entry.entry_type),
    )
    for entry in ordered_entries:
        delta = entry.amount if entry.entry_type == "CREDIT" else -entry.amount
        expected_balance = round(balances[entry.account_id] + delta, 2)
        if entry.balance_after != expected_balance:
            raise ValueError(f"ledger balance does not reconcile for {entry.account_id}")
        if expected_balance < -account_by_id[entry.account_id].overdraft_limit:
            raise ValueError(f"ledger balance exceeds overdraft limit for {entry.account_id}")
        balances[entry.account_id] = expected_balance
