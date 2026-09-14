"""Domain records used by the optional Milestone 11 graph layer."""

from datetime import datetime
from typing import Literal

from pydantic import Field

from fraudtwin.domain.entities import _EntityModel

GraphPatternType = Literal[
    "MULE_NETWORK",
    "CYCLIC_RING",
    "BENEFICIARY_NETWORK",
    "SHARED_DEVICE_INFRASTRUCTURE",
    "SHARED_IP_INFRASTRUCTURE",
    "FAN_IN",
    "FAN_OUT",
    "SHORT_MONEY_DWELL",
    "DENSE_CAMPAIGN",
    "MERCHANT_CUSTOMER_COMMUNITY",
    "BIPARTITE_NETWORK",
    "STACKED_NETWORK",
    "SCATTER_GATHER",
    "GATHER_SCATTER",
    "RANDOM_ALERT_CONTROL",
]
GraphTruthLabel = Literal["FRAUD", "CONTROL"]


class NetworkEndpoint(_EntityModel):
    """An opt-in synthetic network endpoint; absent from legacy runs."""

    endpoint_id: str
    endpoint_type: Literal["IP", "NETWORK"] = "IP"
    address_hash: str
    first_seen_at: datetime
    last_seen_at: datetime
    valid_from: datetime
    valid_to: datetime | None = None


class GraphCampaignMembership(_EntityModel):
    """Oracle membership for one generated graph-fraud campaign."""

    campaign_id: str
    pattern_type: GraphPatternType
    member_id: str
    member_type: str
    role: str
    valid_from: datetime
    valid_to: datetime | None = None
    source_event_id: str | None = None
    payment_id: str | None = None


class GraphCampaign(_EntityModel):
    """Oracle descriptor for one deterministic scenario instance."""

    campaign_id: str
    scenario_type: GraphPatternType
    scenario_code: str
    truth_label: GraphTruthLabel
    valid_from: datetime
    valid_to: datetime
    participant_ids: tuple[str, ...] = ()
    modifiers: tuple[str, ...] = ()


class GraphPattern(_EntityModel):
    """A deterministic structural pattern descriptor."""

    pattern_id: str
    pattern_type: GraphPatternType
    campaign_id: str | None = None
    detected_at: datetime
    window_from: datetime
    window_to: datetime
    member_ids: tuple[str, ...] = ()
    source_event_ids: tuple[str, ...] = ()
    payment_ids: tuple[str, ...] = ()
    threshold: int | None = Field(default=None, ge=1)
    observed_value: float | None = None
    invariant_status: Literal["PASS", "UNAVAILABLE", "FAIL"] = "PASS"
    truth_label: GraphTruthLabel = "FRAUD"
    scenario_code: str | None = None
    reason: str | None = None


class GraphEvidence(_EntityModel):
    """Normalized lineage for a derived relationship."""

    evidence_id: str
    edge_id: str | None = None
    evidence_type: str
    resource_id: str | None = None
    source_event_id: str | None = None
    payment_id: str | None = None
    observed_at: datetime
    available_at: datetime | None = None


class GraphHyperedge(_EntityModel):
    """Optional higher-order campaign incidence record."""

    hyperedge_id: str
    hyperedge_type: Literal["STRUCTURAL", "SEMANTIC"]
    campaign_id: str | None = None
    pattern_id: str | None = None
    valid_from: datetime
    valid_to: datetime | None = None
    source_event_ids: tuple[str, ...] = ()
    payment_ids: tuple[str, ...] = ()


class GraphHyperedgeMembership(_EntityModel):
    hyperedge_id: str
    member_id: str
    member_type: str
    role: str | None = None


__all__ = [
    "GraphCampaignMembership",
    "GraphCampaign",
    "GraphEvidence",
    "GraphHyperedge",
    "GraphHyperedgeMembership",
    "GraphPattern",
    "GraphPatternType",
    "NetworkEndpoint",
]
