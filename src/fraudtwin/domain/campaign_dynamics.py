"""Immutable domain records for Milestone 15 campaign evolution."""

from datetime import datetime
from typing import Literal

from pydantic import Field

from fraudtwin.domain.entities import _EntityModel

CampaignPhaseName = Literal["compromise", "setup", "transfer", "cash_out", "dormant", "closed"]


class CampaignStateSnapshot(_EntityModel):
    snapshot_id: str
    campaign_id: str
    phase: CampaignPhaseName
    intensity: float = Field(ge=0)
    active_actor_ids: tuple[str, ...] = ()
    active_device_ids: tuple[str, ...] = ()
    valid_from: datetime
    valid_to: datetime | None = None
    transition_id: str | None = None


class CampaignTransition(_EntityModel):
    transition_id: str
    campaign_id: str
    from_phase: CampaignPhaseName
    to_phase: CampaignPhaseName
    occurred_at: datetime
    reason: str
    model_name: str
    source_snapshot_id: str | None = None
    stream_id: str


class CampaignPhaseChange(_EntityModel):
    phase_change_id: str
    campaign_id: str
    transition_id: str
    phase: CampaignPhaseName
    valid_from: datetime
    valid_to: datetime | None = None


class CampaignActorMembershipChange(_EntityModel):
    membership_change_id: str
    campaign_id: str
    actor_id: str
    actor_type: Literal["ACCOUNT", "CUSTOMER", "CARD", "DEVICE", "MERCHANT"]
    role: str
    action: Literal["JOIN", "LEAVE", "ROTATE_IN", "ROTATE_OUT"]
    occurred_at: datetime
    valid_from: datetime
    valid_to: datetime | None = None
    source_entity_id: str | None = None


class CampaignIntensityDecision(_EntityModel):
    intensity_decision_id: str
    campaign_id: str
    phase: CampaignPhaseName
    decision_at: datetime
    event_rate: float = Field(ge=0)
    mark: float = Field(ge=0)
    model_name: str
    stream_id: str


class CampaignTopologyMutation(_EntityModel):
    topology_mutation_id: str
    campaign_id: str
    mutation_type: Literal[
        "MULE_ROTATION",
        "DEVICE_ROTATION",
        "RING_SPLIT",
        "RING_MERGE",
        "CROSS_RAIL",
        "HYPEREDGE_ADD",
    ]
    occurred_at: datetime
    source_member_ids: tuple[str, ...] = ()
    derived_member_ids: tuple[str, ...] = ()
    source_payment_ids: tuple[str, ...] = ()
    derived_payment_ids: tuple[str, ...] = ()
    reason: str


class CampaignLineage(_EntityModel):
    lineage_id: str
    campaign_id: str
    parent_campaign_ids: tuple[str, ...] = ()
    source_entity_ids: tuple[str, ...] = ()
    source_event_ids: tuple[str, ...] = ()
    source_payment_ids: tuple[str, ...] = ()
    derived_entity_ids: tuple[str, ...] = ()
    derived_event_ids: tuple[str, ...] = ()
    derived_payment_ids: tuple[str, ...] = ()
    derived_at: datetime
    reason: str


class CampaignSourceSnapshot(_EntityModel):
    source_snapshot_id: str
    campaign_id: str
    source_run_id: str
    captured_at: datetime
    source_campaign_ids: tuple[str, ...] = ()
    source_entity_ids: tuple[str, ...] = ()
    source_event_ids: tuple[str, ...] = ()
    source_payment_ids: tuple[str, ...] = ()
    configuration_hash: str
    schema_fingerprint: str


__all__ = [
    "CampaignActorMembershipChange",
    "CampaignIntensityDecision",
    "CampaignLineage",
    "CampaignPhaseChange",
    "CampaignSourceSnapshot",
    "CampaignStateSnapshot",
    "CampaignTopologyMutation",
    "CampaignTransition",
]
