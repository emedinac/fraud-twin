"""Small, explainable ground-truth records for Milestone 6 scenarios."""

from datetime import datetime
from typing import Literal

from pydantic import Field

from fraudtwin.domain.entities import _EntityModel

FraudScenarioType = Literal["F01", "F02", "F03", "F04", "F05"]
FraudRecordType = Literal["FRAUD", "HARD_NEGATIVE"]


class FraudRecord(_EntityModel):
    """One scenario-linked truth record, including legitimate lookalikes."""

    fraud_record_id: str
    record_type: FraudRecordType
    scenario_id: str
    scenario_type: FraudScenarioType
    fraud_truth: bool
    trigger: str
    reason: str
    customer_id: str
    account_id: str | None
    card_id: str | None
    device_id: str | None
    merchant_id: str | None
    payment_id: str
    event_id: str
    occurred_at: datetime
    amount: float = Field(gt=0)
    currency: str
    correlation_id: str
    causation_id: str | None
    affected_entity_ids: tuple[str, ...]
