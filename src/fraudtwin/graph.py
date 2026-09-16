"""Deterministic graph views over one generated FraudTwin run."""

import csv
import hashlib
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from itertools import combinations
from pathlib import Path
from typing import Any, Literal

import polars as pl
from pydantic import BaseModel, ConfigDict

from fraudtwin.config import GraphConfig, SimulationRunConfig, config_hash
from fraudtwin.domain import (
    GraphCampaign,
    GraphCampaignMembership,
    GraphEvidence,
    GraphHyperedge,
    GraphHyperedgeMembership,
    GraphPattern,
    Payment,
    PaymentEvent,
)
from fraudtwin.manifest import GraphManifest, RunManifest
from fraudtwin.reproducibility import sha256_json
from fraudtwin.simulation.behavior import BehaviorDataset
from fraudtwin.simulation.generator import EntityDataset

GraphView = Literal["observable", "oracle"]
GRAPH_SCHEMA_VERSION = "2"
EdgeOrigin = Literal["DOMAIN_EVENT", "DERIVED_RELATION", "SCENARIO_GROUND_TRUTH"]

_UTC = pl.Datetime(time_zone="UTC")
GRAPH_NODE_SCHEMA: dict[str, Any] = {
    "node_id": pl.Utf8,
    "node_type": pl.Utf8,
    "valid_from": _UTC,
    "valid_to": _UTC,
    "available_at": _UTC,
    "attributes_version": pl.Utf8,
}
GRAPH_EDGE_SCHEMA: dict[str, Any] = {
    "edge_id": pl.Utf8,
    "src_id": pl.Utf8,
    "dst_id": pl.Utf8,
    "edge_type": pl.Utf8,
    "event_time": _UTC,
    "valid_from": _UTC,
    "valid_to": _UTC,
    "source_event_id": pl.Utf8,
    "payment_id": pl.Utf8,
    "scenario_id": pl.Utf8,
    "campaign_id": pl.Utf8,
    "edge_origin": pl.Utf8,
    "weight": pl.Float64,
    "support_count": pl.Int64,
    "available_at": _UTC,
}
GRAPH_MEMBERSHIP_SCHEMA: dict[str, Any] = {
    "campaign_id": pl.Utf8,
    "pattern_type": pl.Utf8,
    "member_id": pl.Utf8,
    "member_type": pl.Utf8,
    "role": pl.Utf8,
    "valid_from": _UTC,
    "valid_to": _UTC,
    "source_event_id": pl.Utf8,
    "payment_id": pl.Utf8,
}
GRAPH_CAMPAIGN_SCHEMA: dict[str, Any] = {
    "campaign_id": pl.Utf8,
    "scenario_type": pl.Utf8,
    "scenario_code": pl.Utf8,
    "truth_label": pl.Utf8,
    "valid_from": _UTC,
    "valid_to": _UTC,
    "participant_ids": pl.List(pl.Utf8),
    "modifiers": pl.List(pl.Utf8),
}
GRAPH_PATTERN_SCHEMA: dict[str, Any] = {
    "pattern_id": pl.Utf8,
    "pattern_type": pl.Utf8,
    "campaign_id": pl.Utf8,
    "detected_at": _UTC,
    "window_from": _UTC,
    "window_to": _UTC,
    "member_ids": pl.List(pl.Utf8),
    "source_event_ids": pl.List(pl.Utf8),
    "payment_ids": pl.List(pl.Utf8),
    "threshold": pl.Int64,
    "observed_value": pl.Float64,
    "invariant_status": pl.Utf8,
    "truth_label": pl.Utf8,
    "scenario_code": pl.Utf8,
    "reason": pl.Utf8,
}
GRAPH_EVIDENCE_SCHEMA: dict[str, Any] = {
    "evidence_id": pl.Utf8,
    "edge_id": pl.Utf8,
    "evidence_type": pl.Utf8,
    "resource_id": pl.Utf8,
    "source_event_id": pl.Utf8,
    "payment_id": pl.Utf8,
    "observed_at": _UTC,
    "available_at": _UTC,
}
GRAPH_HYPEREDGE_SCHEMA: dict[str, Any] = {
    "hyperedge_id": pl.Utf8,
    "hyperedge_type": pl.Utf8,
    "campaign_id": pl.Utf8,
    "pattern_id": pl.Utf8,
    "valid_from": _UTC,
    "valid_to": _UTC,
    "source_event_ids": pl.List(pl.Utf8),
    "payment_ids": pl.List(pl.Utf8),
}
GRAPH_HYPEREDGE_MEMBERSHIP_SCHEMA: dict[str, Any] = {
    "hyperedge_id": pl.Utf8,
    "member_id": pl.Utf8,
    "member_type": pl.Utf8,
    "role": pl.Utf8,
}


class GraphNode(BaseModel):
    """Versioned entity node in an observable or oracle graph view."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    node_id: str
    node_type: str
    valid_from: datetime
    valid_to: datetime | None = None
    available_at: datetime | None = None
    attributes_version: str = "1"


class GraphEdge(BaseModel):
    """Time-bounded relationship with provenance back to source events."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    edge_id: str
    src_id: str
    dst_id: str
    edge_type: str
    event_time: datetime | None = None
    valid_from: datetime
    valid_to: datetime | None = None
    source_event_id: str | None = None
    payment_id: str | None = None
    scenario_id: str | None = None
    campaign_id: str | None = None
    edge_origin: EdgeOrigin
    weight: float | None = None
    support_count: int | None = None
    available_at: datetime | None = None


@dataclass(frozen=True)
class GraphDataset:
    """One immutable graph view and its structural descriptors."""

    view: GraphView
    source_run_id: str
    nodes: tuple[GraphNode, ...]
    edges: tuple[GraphEdge, ...]
    memberships: tuple[GraphCampaignMembership, ...]
    patterns: tuple[GraphPattern, ...]
    from_time: datetime
    to_time: datetime
    as_of: datetime
    campaigns: tuple[GraphCampaign, ...] = ()
    evidence: tuple[GraphEvidence, ...] = ()
    hyperedges: tuple[GraphHyperedge, ...] = ()
    hyperedge_memberships: tuple[GraphHyperedgeMembership, ...] = ()

    @property
    def node_frame(self) -> pl.DataFrame:
        return _frame(self.nodes, GRAPH_NODE_SCHEMA)

    @property
    def edge_frame(self) -> pl.DataFrame:
        return _frame(self.edges, GRAPH_EDGE_SCHEMA)

    @property
    def membership_frame(self) -> pl.DataFrame:
        return _frame(self.memberships, GRAPH_MEMBERSHIP_SCHEMA)

    @property
    def campaign_frame(self) -> pl.DataFrame:
        return _frame(self.campaigns, GRAPH_CAMPAIGN_SCHEMA)

    @property
    def pattern_frame(self) -> pl.DataFrame:
        return _frame(self.patterns, GRAPH_PATTERN_SCHEMA)

    @property
    def evidence_frame(self) -> pl.DataFrame:
        return _frame(self.evidence, GRAPH_EVIDENCE_SCHEMA)

    @property
    def hyperedge_frame(self) -> pl.DataFrame:
        return _frame(self.hyperedges, GRAPH_HYPEREDGE_SCHEMA)

    @property
    def hyperedge_membership_frame(self) -> pl.DataFrame:
        return _frame(self.hyperedge_memberships, GRAPH_HYPEREDGE_MEMBERSHIP_SCHEMA)

    @property
    def output_fingerprint(self) -> str:
        return sha256_json(
            {
                "view": self.view,
                "nodes": [item.model_dump(mode="json") for item in self.nodes],
                "edges": [item.model_dump(mode="json") for item in self.edges],
                "memberships": [item.model_dump(mode="json") for item in self.memberships],
                "campaigns": [item.model_dump(mode="json") for item in self.campaigns],
                "patterns": [item.model_dump(mode="json") for item in self.patterns],
                "evidence": [item.model_dump(mode="json") for item in self.evidence],
                "hyperedges": [item.model_dump(mode="json") for item in self.hyperedges],
                "hyperedge_memberships": [
                    item.model_dump(mode="json") for item in self.hyperedge_memberships
                ],
            }
        )


def _frame(records: tuple[BaseModel, ...], schema: dict[str, Any]) -> pl.DataFrame:
    rows = [record.model_dump(mode="python") for record in records]
    return pl.DataFrame(rows, schema=schema, orient="row")


def _as_utc(value: datetime) -> datetime:
    return value.astimezone(UTC)


def _edge_id(
    src_id: str,
    dst_id: str,
    edge_type: str,
    source_event_id: str | None,
    payment_id: str | None,
    valid_from: datetime,
) -> str:
    payload = (src_id, dst_id, edge_type, source_event_id, payment_id, valid_from.isoformat())
    return "GED-" + hashlib.sha256("|".join(map(str, payload)).encode()).hexdigest()[:20]


def _node(
    node_id: str,
    node_type: str,
    valid_from: datetime,
    valid_to: datetime | None = None,
    available_at: datetime | None = None,
    support_count: int | None = None,
) -> GraphNode:
    return GraphNode(
        node_id=node_id,
        node_type=node_type,
        valid_from=_as_utc(valid_from),
        valid_to=_as_utc(valid_to) if valid_to else None,
        available_at=_as_utc(available_at) if available_at else None,
    )


def _static_nodes(entities: EntityDataset, start: datetime) -> tuple[GraphNode, ...]:
    nodes: list[GraphNode] = []
    for customer in entities.customers:
        nodes.append(
            _node(
                customer.customer_id,
                "CUSTOMER",
                customer.valid_from,
                customer.valid_to,
                customer.created_at,
            )
        )
    for institution in entities.institutions:
        nodes.append(_node(institution.institution_id, "INSTITUTION", start, None, start))
    for account in entities.accounts:
        nodes.append(
            _node(
                account.account_id,
                "ACCOUNT",
                account.valid_from,
                account.valid_to,
                account.created_at,
            )
        )
    for card in entities.cards:
        nodes.append(_node(card.card_id, "CARD", card.issued_at, card.expires_at, card.issued_at))
    for device in entities.devices:
        nodes.append(
            _node(
                device.device_id,
                "DEVICE",
                device.first_seen_at,
                device.last_seen_at,
                device.first_seen_at,
            )
        )
    for merchant in entities.merchants:
        nodes.append(
            _node(merchant.merchant_id, "MERCHANT", merchant.created_at, None, merchant.created_at)
        )
    for key in entities.pix_keys:
        nodes.append(_node(key.pix_key_id, "BENEFICIARY", key.created_at, None, key.created_at))
    for endpoint in entities.network_endpoints:
        nodes.append(
            _node(
                endpoint.endpoint_id,
                "IP",
                endpoint.valid_from,
                endpoint.valid_to,
                endpoint.first_seen_at,
            )
        )
    return tuple(sorted(nodes, key=lambda item: (item.node_type, item.node_id)))


def _edge(
    src: str,
    dst: str,
    edge_type: str,
    *,
    valid_from: datetime,
    valid_to: datetime | None = None,
    event_time: datetime | None = None,
    event: PaymentEvent | None = None,
    source_event_id: str | None = None,
    payment_id: str | None = None,
    scenario_id: str | None = None,
    campaign_id: str | None = None,
    origin: EdgeOrigin = "DOMAIN_EVENT",
    weight: float | None = None,
    support_count: int | None = None,
    available_at: datetime | None = None,
) -> GraphEdge:
    return GraphEdge(
        edge_id=_edge_id(
            src,
            dst,
            edge_type,
            event.event_id if event else source_event_id,
            payment_id,
            valid_from,
        ),
        src_id=src,
        dst_id=dst,
        edge_type=edge_type,
        event_time=_as_utc(event_time) if event_time else None,
        valid_from=_as_utc(valid_from),
        valid_to=_as_utc(valid_to) if valid_to else None,
        source_event_id=event.event_id if event else source_event_id,
        payment_id=payment_id,
        scenario_id=scenario_id,
        campaign_id=campaign_id,
        edge_origin=origin,
        weight=weight,
        support_count=support_count,
        available_at=_as_utc(available_at) if available_at else None,
    )


def _event_edges(
    entities: EntityDataset,
    behavior: BehaviorDataset,
    payments: dict[str, Payment],
    start: datetime,
    end: datetime,
    events: tuple[PaymentEvent, ...] | None = None,
) -> list[GraphEdge]:
    edges: list[GraphEdge] = []
    key_by_id = {item.pix_key_id: item for item in entities.pix_keys}
    for account in entities.accounts:
        edges.append(
            _edge(account.customer_id, account.account_id, "OWNS", valid_from=account.opening_date)
        )
        edges.append(
            _edge(
                account.account_id,
                account.institution_id,
                "HELD_AT",
                valid_from=account.opening_date,
            )
        )
    for card in entities.cards:
        edges.append(
            _edge(
                card.customer_id,
                card.card_id,
                "OWNS",
                valid_from=card.issued_at,
                valid_to=card.expires_at,
            )
        )
    for key in entities.pix_keys:
        edges.append(_edge(key.customer_id, key.pix_key_id, "OWNS", valid_from=key.created_at))
    for merchant in entities.merchants:
        edges.append(
            _edge(
                merchant.merchant_id,
                merchant.acquirer_id,
                "ACQUIRED_BY",
                valid_from=merchant.created_at,
            )
        )
    event_records = events if events is not None else behavior.payment_events
    for event in sorted(event_records, key=lambda item: (item.event_time, item.event_id)):
        if not start <= event.event_time < end:
            continue
        if event.event_type not in {
            "CARD_PAYMENT_INITIATED",
            "CARD_AUTHORIZATION_REQUESTED",
            "PIX_INITIATED",
            "TRANSFER_COMPLETED",
        }:
            continue
        payment = payments.get(event.payment_id)
        if payment is None:
            continue
        common: dict[str, Any] = {
            "valid_from": event.event_time,
            "event_time": event.event_time,
            "event": event,
            "payment_id": payment.payment_id,
            "scenario_id": event.scenario_id,
            "available_at": event.source_available_at,
        }
        if event.device_id:
            edges.append(_edge(event.customer_id, event.device_id, "USES", **common))
        if event.ip_id:
            edges.append(_edge(event.customer_id, event.ip_id, "LOGGED_IN_FROM", **common))
        if payment.payment_type == "PURCHASE" and payment.merchant_id:
            edges.append(_edge(payment.payer_account_id, payment.merchant_id, "PAID", **common))
            edges.append(_edge(event.customer_id, payment.merchant_id, "PURCHASED_FROM", **common))
        if payment.payment_type == "TRANSFER" and payment.payee_account_id:
            edges.append(
                _edge(
                    payment.payer_account_id, payment.payee_account_id, "TRANSFERRED_TO", **common
                )
            )
            edges.append(
                _edge(payment.payee_account_id, payment.payer_account_id, "RECEIVED_FROM", **common)
            )
            if payment.payee_pix_key_id and payment.payee_pix_key_id in key_by_id:
                edges.append(
                    _edge(
                        payment.payer_account_id,
                        payment.payee_pix_key_id,
                        "TRANSFERRED_TO",
                        **common,
                    )
                )
    return edges


def _derived_shared(
    edges: list[GraphEdge], config: GraphConfig, edge_type: Literal["SHARES_DEVICE", "SHARES_IP"]
) -> list[GraphEdge]:
    use_type = "USES" if edge_type == "SHARES_DEVICE" else "LOGGED_IN_FROM"
    grouped: dict[str, list[GraphEdge]] = defaultdict(list)
    for edge in edges:
        if edge.edge_type == use_type and edge.dst_id:
            grouped[edge.dst_id].append(edge)
    result: list[GraphEdge] = []
    emitted: set[tuple[str, str, str]] = set()
    window = timedelta(seconds=config.relationship_window_seconds)
    for _shared_id, uses in sorted(grouped.items()):
        if len({u.src_id for u in uses}) < config.shared_threshold:
            continue
        uses.sort(key=lambda item: (item.event_time or item.valid_from, item.src_id, item.edge_id))
        for left, right in combinations(uses, 2):
            if left.src_id == right.src_id:
                continue
            left_time = left.event_time or left.valid_from
            right_time = right.event_time or right.valid_from
            if abs(right_time - left_time) > window:
                continue
            src, dst = sorted((left.src_id, right.src_id))
            pair_key = (edge_type, src, dst)
            if pair_key in emitted:
                continue
            emitted.add(pair_key)
            result.append(
                _edge(
                    src,
                    dst,
                    edge_type,
                    valid_from=min(left_time, right_time),
                    valid_to=max(left_time, right_time) + window,
                    event_time=max(left_time, right_time),
                    source_event_id=right.source_event_id,
                    payment_id=right.payment_id or left.payment_id,
                    scenario_id=right.scenario_id or left.scenario_id,
                    origin="DERIVED_RELATION",
                    weight=1.0,
                    support_count=2,
                    available_at=max(
                        left.available_at or left_time, right.available_at or right_time
                    ),
                )
            )
    return result


def _patterns(
    edges: list[GraphEdge],
    config: GraphConfig,
    memberships: tuple[GraphCampaignMembership, ...],
    node_types: dict[str, str],
) -> tuple[GraphPattern, ...]:
    transfers = [edge for edge in edges if edge.edge_type == "TRANSFERRED_TO" and edge.event_time]
    patterns: list[GraphPattern] = []
    by_dst: dict[str, list[GraphEdge]] = defaultdict(list)
    by_src: dict[str, list[GraphEdge]] = defaultdict(list)
    for edge in transfers:
        by_dst[edge.dst_id].append(edge)
        by_src[edge.src_id].append(edge)
    for hub, incoming in sorted(by_dst.items()):
        incoming_times = [item.event_time for item in incoming if item.event_time is not None]
        if not incoming_times:
            continue
        incoming = [
            item
            for item in incoming
            if item.event_time
            and max(incoming_times) - min(incoming_times)
            <= timedelta(seconds=config.pattern_window_seconds)
        ]
        if not incoming:
            continue
        distinct = tuple(sorted({item.src_id for item in incoming}))
        if len(distinct) >= config.fan_threshold:
            times = [item.event_time for item in incoming if item.event_time]
            patterns.append(
                _pattern(
                    "FAN_IN", hub, incoming, config.fan_threshold, distinct, min(times), max(times)
                )
            )
            if node_types.get(hub) == "BENEFICIARY":
                patterns.append(
                    _pattern(
                        "BENEFICIARY_NETWORK",
                        hub,
                        incoming,
                        config.fan_threshold,
                        distinct + (hub,),
                        min(times),
                        max(times),
                    )
                )
    for hub, outgoing in sorted(by_src.items()):
        outgoing_times = [item.event_time for item in outgoing if item.event_time is not None]
        if not outgoing_times:
            continue
        outgoing = [
            item
            for item in outgoing
            if item.event_time
            and max(outgoing_times) - min(outgoing_times)
            <= timedelta(seconds=config.pattern_window_seconds)
        ]
        if not outgoing:
            continue
        distinct = tuple(sorted({item.dst_id for item in outgoing}))
        if len(distinct) >= config.fan_threshold:
            times = [item.event_time for item in outgoing if item.event_time]
            patterns.append(
                _pattern(
                    "FAN_OUT", hub, outgoing, config.fan_threshold, distinct, min(times), max(times)
                )
            )
    for hub in sorted(set(by_dst) & set(by_src)):
        incoming = by_dst[hub]
        outgoing = by_src[hub]
        if (
            len({item.src_id for item in incoming}) >= config.fan_threshold
            and len({item.dst_id for item in outgoing}) >= config.fan_threshold
        ):
            combined = incoming + outgoing
            times = [item.event_time for item in combined if item.event_time]
            patterns.append(
                _pattern(
                    "MULE_NETWORK",
                    hub,
                    combined,
                    config.fan_threshold,
                    tuple(
                        sorted(
                            {item.src_id for item in combined} | {item.dst_id for item in combined}
                        )
                    ),
                    min(times),
                    max(times),
                )
            )
    adjacency: dict[str, list[GraphEdge]] = defaultdict(list)
    for edge in transfers:
        adjacency[edge.src_id].append(edge)
    for start_node in sorted(adjacency):

        def visit(
            current: str,
            path: tuple[str, ...],
            used: tuple[GraphEdge, ...],
            root: str = start_node,
        ) -> None:
            if len(path) > config.ring_max_size:
                return
            for edge in sorted(adjacency.get(current, ()), key=lambda item: item.edge_id):
                edge_time = edge.event_time or edge.valid_from
                if (
                    edge.dst_id == root
                    and config.ring_min_size <= len(path) <= config.ring_max_size
                ):
                    if used and (used[-1].event_time or used[-1].valid_from) < edge_time:
                        patterns.append(
                            _pattern(
                                "CYCLIC_RING",
                                root,
                                [*used, edge],
                                len(path),
                                tuple(sorted(path)),
                                min(item.event_time or item.valid_from for item in (*used, edge)),
                                max(item.event_time or item.valid_from for item in (*used, edge)),
                            )
                        )
                    continue
                if edge.dst_id in path:
                    continue
                if used and edge_time <= (used[-1].event_time or used[-1].valid_from):
                    continue
                visit(edge.dst_id, (*path, edge.dst_id), (*used, edge))

        visit(start_node, (start_node,), ())
    for edge in edges:
        if edge.edge_type in {"SHARES_DEVICE", "SHARES_IP"}:
            kind = (
                "SHARED_DEVICE_INFRASTRUCTURE"
                if edge.edge_type == "SHARES_DEVICE"
                else "SHARED_IP_INFRASTRUCTURE"
            )
            patterns.append(
                _pattern(
                    kind,
                    edge.src_id,
                    [edge],
                    config.shared_threshold,
                    (edge.src_id, edge.dst_id),
                    edge.valid_from,
                    edge.event_time or edge.valid_from,
                )
            )
    membership_by_campaign: dict[str, list[GraphCampaignMembership]] = defaultdict(list)
    for membership in memberships:
        membership_by_campaign[membership.campaign_id].append(membership)
    for campaign_id, members in sorted(membership_by_campaign.items()):
        member_ids = tuple(sorted({item.member_id for item in members}))
        pattern_type = members[0].pattern_type
        if len(member_ids) >= config.ring_min_size or pattern_type in {
            "MULE_NETWORK",
            "BENEFICIARY_NETWORK",
        }:
            patterns.append(
                GraphPattern(
                    pattern_id="PAT-" + sha256_json((campaign_id, pattern_type, member_ids))[:20],
                    pattern_type=pattern_type,
                    campaign_id=campaign_id,
                    detected_at=max(item.valid_from for item in members),
                    window_from=min(item.valid_from for item in members),
                    window_to=max(item.valid_to or item.valid_from for item in members),
                    member_ids=member_ids,
                    source_event_ids=tuple(
                        sorted({item.source_event_id for item in members if item.source_event_id})
                    ),
                    payment_ids=tuple(
                        sorted({item.payment_id for item in members if item.payment_id})
                    ),
                    threshold=config.ring_min_size,
                )
            )
    return tuple(sorted(patterns, key=lambda item: (item.detected_at, item.pattern_id)))


def _pattern(
    kind: Any,
    hub: str,
    edges: list[GraphEdge],
    threshold: int,
    members: tuple[str, ...],
    start: datetime,
    end: datetime,
) -> GraphPattern:
    return GraphPattern(
        pattern_id="PAT-"
        + sha256_json((kind, hub, members, start.isoformat(), end.isoformat()))[:20],
        pattern_type=kind,
        detected_at=end,
        window_from=start,
        window_to=end,
        member_ids=members,
        source_event_ids=tuple(
            sorted({edge.source_event_id for edge in edges if edge.source_event_id})
        ),
        payment_ids=tuple(sorted({edge.payment_id for edge in edges if edge.payment_id})),
        threshold=threshold,
        observed_value=float(len(members)),
    )


def build_graph(
    config: SimulationRunConfig,
    entities: EntityDataset,
    behavior: BehaviorDataset,
    source_manifest: RunManifest | None = None,
    *,
    view: GraphView = "observable",
    as_of: datetime | None = None,
    from_time: datetime | None = None,
    to_time: datetime | None = None,
    memberships: tuple[GraphCampaignMembership, ...] = (),
    evidence: tuple[GraphEvidence, ...] = (),
    hyperedges: tuple[GraphHyperedge, ...] = (),
    hyperedge_memberships: tuple[GraphHyperedgeMembership, ...] = (),
    campaigns: tuple[GraphCampaign, ...] = (),
) -> GraphDataset:
    """Build a deterministic temporal graph without generating source data."""

    start = _as_utc(
        from_time or (source_manifest.start_time if source_manifest else config.simulation.start)
    )
    end = _as_utc(
        to_time
        or (
            source_manifest.end_time
            if source_manifest
            else config.simulation.start + timedelta(days=config.simulation.duration_days)
        )
    )
    source_start = _as_utc(
        source_manifest.start_time if source_manifest else config.simulation.start
    )
    source_end = _as_utc(
        source_manifest.end_time
        if source_manifest
        else config.simulation.start + timedelta(days=config.simulation.duration_days)
    )
    if start < source_start or end > source_end:
        raise ValueError("graph temporal selection must lie within the source run window")
    if end <= start:
        raise ValueError("graph to_time must be after from_time")
    cutoff = _as_utc(as_of or end)
    if cutoff < start or cutoff > end:
        raise ValueError("graph as_of must lie within the selected temporal window")
    payments = {item.payment_id: item for item in behavior.payments}
    if not memberships:
        memberships = behavior.graph_memberships
    if not campaigns:
        campaigns = behavior.graph_campaigns
    if not evidence:
        evidence = behavior.graph_evidence
    if not hyperedges:
        hyperedges = behavior.graph_hyperedges
    if not hyperedge_memberships:
        hyperedge_memberships = behavior.graph_hyperedge_memberships
    nodes = _static_nodes(entities, start)
    oracle_events = behavior.oracle_tables.get("payment_events") if view == "oracle" else None
    event_records = (
        tuple(item for item in oracle_events if isinstance(item, PaymentEvent))
        if oracle_events is not None
        else None
    )
    edges = _event_edges(entities, behavior, payments, start, end, event_records)
    edges.extend(_derived_shared(edges, config.graph, "SHARES_DEVICE"))
    edges.extend(_derived_shared(edges, config.graph, "SHARES_IP"))
    derived_by_event = {
        edge.source_event_id: edge.edge_id
        for edge in edges
        if edge.edge_origin == "DERIVED_RELATION" and edge.source_event_id
    }
    evidence = tuple(
        item.model_copy(update={"edge_id": derived_by_event.get(item.source_event_id)})
        if item.edge_id is None
        and item.source_event_id is not None
        and item.source_event_id in derived_by_event
        else item
        for item in evidence
    )
    if view == "oracle":
        campaign_ids = sorted(
            {item.campaign_id for item in campaigns} | {m.campaign_id for m in memberships}
        )
        pattern_ids = sorted({p.pattern_id for p in behavior.graph_patterns})
        nodes = tuple(
            sorted(
                (
                    *nodes,
                    *(_node(cid, "CAMPAIGN", start, end, start) for cid in campaign_ids),
                    *(_node(pid, "PATTERN", start, end, start) for pid in pattern_ids),
                ),
                key=lambda item: (item.node_type, item.node_id),
            )
        )
        for membership in memberships:
            if membership.member_id in {n.node_id for n in nodes}:
                edges.append(
                    _edge(
                        membership.member_id,
                        membership.campaign_id,
                        "MEMBER_OF",
                        valid_from=membership.valid_from,
                        valid_to=membership.valid_to,
                        origin="SCENARIO_GROUND_TRUTH",
                        scenario_id=membership.campaign_id,
                        campaign_id=membership.campaign_id,
                        available_at=membership.valid_from,
                    )
                )
    if view == "observable":
        nodes = tuple(
            item for item in nodes if (item.available_at is None or item.available_at <= cutoff)
        )
        node_ids = {item.node_id for item in nodes}
        edges = [
            item
            for item in edges
            if (item.available_at is None or item.available_at <= cutoff)
            and item.src_id in node_ids
            and item.dst_id in node_ids
        ]
        memberships = ()
        campaigns = ()
        evidence = ()
        hyperedges = ()
        hyperedge_memberships = ()
    else:
        campaigns = tuple(
            item for item in campaigns if item.valid_from < end and item.valid_to > start
        )
        memberships = tuple(
            item
            for item in memberships
            if item.valid_from < end and (item.valid_to is None or item.valid_to > start)
        )
        evidence = tuple(
            item for item in evidence if item.observed_at < end and item.observed_at >= start
        )
        hyperedges = tuple(
            item
            for item in hyperedges
            if item.valid_from < end and (item.valid_to is None or item.valid_to > start)
        )
        hyperedge_memberships = tuple(
            item
            for item in hyperedge_memberships
            if item.hyperedge_id in {h.hyperedge_id for h in hyperedges}
        )
    # Static relationships are retained only when their validity intersects the selection.
    edges = [
        item
        for item in edges
        if item.event_time is not None
        or (item.valid_from < end and (item.valid_to is None or item.valid_to > start))
    ]
    edges = sorted(
        edges,
        key=lambda item: (item.valid_from, item.edge_type, item.src_id, item.dst_id, item.edge_id),
    )
    seen_edge_ids: dict[str, int] = defaultdict(int)
    unique_edges: list[GraphEdge] = []
    for edge in edges:
        seen_edge_ids[edge.edge_id] += 1
        occurrence = seen_edge_ids[edge.edge_id]
        unique_edges.append(
            edge
            if occurrence == 1
            else edge.model_copy(update={"edge_id": f"{edge.edge_id}-{occurrence:02d}"})
        )
    edges = unique_edges
    patterns = _patterns(
        edges,
        config.graph,
        memberships,
        {node.node_id: node.node_type for node in nodes},
    )
    return GraphDataset(
        view,
        source_manifest.run_id if source_manifest else "in-memory",
        nodes,
        tuple(edges),
        memberships,
        patterns,
        start,
        end,
        cutoff,
        campaigns,
        evidence,
        hyperedges,
        hyperedge_memberships,
    )


def validate_graph(dataset: GraphDataset) -> None:
    """Validate IDs, temporal closure, and edge references for one graph view."""

    node_ids = {node.node_id for node in dataset.nodes}
    if len(node_ids) != len(dataset.nodes):
        raise ValueError("graph contains duplicate node IDs")
    edge_ids = [edge.edge_id for edge in dataset.edges]
    if len(set(edge_ids)) != len(edge_ids):
        raise ValueError("graph contains duplicate edge IDs")
    for edge in dataset.edges:
        if edge.src_id not in node_ids or edge.dst_id not in node_ids:
            raise ValueError("graph edge references an unavailable node")
        if edge.valid_to is not None and edge.valid_to <= edge.valid_from:
            raise ValueError("graph edge validity interval is not ordered")
        if (
            edge.event_time is not None
            and not dataset.from_time <= edge.event_time < dataset.to_time
        ):
            raise ValueError("graph event edge is outside the selected range")
    for pattern in dataset.patterns:
        if pattern.window_to < pattern.window_from:
            raise ValueError("graph pattern window is not ordered")
    validate_graph_scenarios(dataset)


def validate_graph_scenarios(dataset: GraphDataset) -> None:
    """Validate campaign closure and higher-order incidence records."""
    campaign_members = defaultdict(set)
    available_nodes = {n.node_id for n in dataset.nodes}
    available_events = {e.source_event_id for e in dataset.edges if e.source_event_id}
    available_payments = {e.payment_id for e in dataset.edges if e.payment_id}
    for membership in dataset.memberships:
        if membership.member_id not in available_nodes:
            raise ValueError("campaign membership references unavailable node")
        if membership.source_event_id and membership.source_event_id not in available_events:
            raise ValueError("campaign membership references an unavailable source event")
        if membership.payment_id and membership.payment_id not in available_payments:
            raise ValueError("campaign membership references an unavailable payment")
        campaign_members[membership.campaign_id].add(membership.member_id)
    for pattern in dataset.patterns:
        if (
            pattern.campaign_id
            and pattern.campaign_id in campaign_members
            and not set(pattern.member_ids).issubset(campaign_members[pattern.campaign_id])
        ):
            raise ValueError("pattern membership is not closed over campaign members")
        if pattern.truth_label == "CONTROL" and pattern.pattern_type != "RANDOM_ALERT_CONTROL":
            raise ValueError("only random-alert patterns may carry CONTROL truth")
    for edge in dataset.edges:
        if edge.src_id == edge.dst_id:
            raise ValueError("graph contains an unsupported self-loop")
        if edge.edge_type in {"SHARES_DEVICE", "SHARES_IP"} and (edge.support_count or 0) < 2:
            raise ValueError("shared infrastructure edge lacks supporting actors")
    hyper_ids = {h.hyperedge_id for h in dataset.hyperedges}
    if any(item.hyperedge_id not in hyper_ids for item in dataset.hyperedge_memberships):
        raise ValueError("hyperedge membership references an unknown hyperedge")


def _schema_fingerprint() -> str:
    return sha256_json(
        {
            "version": GRAPH_SCHEMA_VERSION,
            "nodes": list(GRAPH_NODE_SCHEMA),
            "edges": list(GRAPH_EDGE_SCHEMA),
            "memberships": list(GRAPH_MEMBERSHIP_SCHEMA),
            "campaigns": list(GRAPH_CAMPAIGN_SCHEMA),
            "patterns": list(GRAPH_PATTERN_SCHEMA),
            "evidence": list(GRAPH_EVIDENCE_SCHEMA),
            "hyperedges": list(GRAPH_HYPEREDGE_SCHEMA),
            "hyperedge_memberships": list(GRAPH_HYPEREDGE_MEMBERSHIP_SCHEMA),
        }
    )


def _write_parquet_artifacts(dataset: GraphDataset, view_dir: Path) -> dict[str, str]:
    """Write all canonical graph tables and return their relative paths."""

    paths: dict[str, str] = {}
    for name, frame in (
        ("campaigns", dataset.campaign_frame),
        ("nodes", dataset.node_frame),
        ("edges", dataset.edge_frame),
        ("campaign_memberships", dataset.membership_frame),
        ("patterns", dataset.pattern_frame),
        ("graph_evidence", dataset.evidence_frame),
        ("hyperedges", dataset.hyperedge_frame),
        ("hyperedge_memberships", dataset.hyperedge_membership_frame),
    ):
        path = view_dir / f"{name}.parquet"
        frame.write_parquet(path)
        paths[name] = str(path.relative_to(view_dir.parent.parent))
    return paths


def _file_checksums(directory: Path, relative_to: Path) -> dict[str, str]:
    """Return deterministic SHA-256 checksums for files below ``directory``."""

    return {
        str(path.relative_to(relative_to)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(directory.rglob("*"))
        if path.is_file()
    }


def write_graph(
    datasets: dict[GraphView, GraphDataset],
    output_dir: Path,
    *,
    source_manifest: RunManifest,
    config: SimulationRunConfig,
    formats: tuple[str, ...] = ("parquet", "neo4j"),
) -> tuple[Path, GraphManifest]:
    """Write one append-only graph artifact containing the requested views."""

    if not datasets:
        raise ValueError("at least one graph view is required")
    for dataset in datasets.values():
        validate_graph(dataset)
    if "pyg" in formats:
        for dataset in datasets.values():
            to_pyg(dataset)
    payload = {
        "source": source_manifest.run_id,
        "config": config_hash(config),
        "views": {name: item.output_fingerprint for name, item in sorted(datasets.items())},
        "formats": formats,
    }
    graph_id = "GRF-" + sha256_json(payload)[:16]
    graph_dir = output_dir / graph_id
    output_dir.mkdir(parents=True, exist_ok=True)
    graph_dir.mkdir(parents=False, exist_ok=False)
    artifacts: dict[str, object] = {}
    for view, dataset in sorted(datasets.items()):
        view_dir = graph_dir / view
        view_dir.mkdir()
        view_artifacts: dict[str, object] = {
            "node_count": len(dataset.nodes),
            "edge_count": len(dataset.edges),
            "pattern_count": len(dataset.patterns),
            "campaign_count": len(dataset.campaigns),
            "output_fingerprint": dataset.output_fingerprint,
        }
        if "parquet" in formats:
            view_artifacts.update(_write_parquet_artifacts(dataset, view_dir))
        if "neo4j" in formats:
            neo_dir = view_dir / "neo4j"
            neo_dir.mkdir()
            _write_neo4j(dataset, neo_dir)
            view_artifacts["neo4j"] = str(neo_dir.relative_to(graph_dir))
        if "pyg" in formats:
            try:
                import torch  # type: ignore[import-not-found]
            except ImportError as exc:
                raise RuntimeError(
                    "PyTorch Geometric export requires `poetry install -E graph`"
                ) from exc
            pyg_path = view_dir / "graph.pt"
            torch.save(to_pyg(dataset), pyg_path)
            view_artifacts["pyg"] = str(pyg_path.relative_to(graph_dir))
        checksums = _file_checksums(view_dir, graph_dir)
        view_artifacts["checksums"] = checksums
        artifacts[view] = view_artifacts
    manifest = GraphManifest(
        graph_id=graph_id,
        graph_version=GRAPH_SCHEMA_VERSION,
        source_run_id=source_manifest.run_id,
        source_manifest_hash=sha256_json(source_manifest.model_dump(mode="json")),
        configuration_hash=config_hash(config),
        parameters={"graph": config.graph.model_dump(mode="json"), "formats": list(formats)},
        source_snapshots={
            "run_id": source_manifest.run_id,
            "manifest_hash": sha256_json(source_manifest.model_dump(mode="json")),
            "from": next(iter(datasets.values())).from_time.isoformat(),
            "to": next(iter(datasets.values())).to_time.isoformat(),
        },
        schema_versions={
            "nodes": GRAPH_SCHEMA_VERSION,
            "edges": GRAPH_SCHEMA_VERSION,
            "campaign_memberships": GRAPH_SCHEMA_VERSION,
            "patterns": GRAPH_SCHEMA_VERSION,
            "campaigns": GRAPH_SCHEMA_VERSION,
            "graph_evidence": GRAPH_SCHEMA_VERSION,
            "hyperedges": GRAPH_SCHEMA_VERSION,
        },
        counts={
            view: {
                "nodes": len(item.nodes),
                "edges": len(item.edges),
                "patterns": len(item.patterns),
                "campaigns": len(item.campaigns),
                "memberships": len(item.memberships),
                "evidence": len(item.evidence),
                "hyperedges": len(item.hyperedges),
                "hyperedge_memberships": len(item.hyperedge_memberships),
            }
            for view, item in sorted(datasets.items())
        },
        schema_fingerprint=_schema_fingerprint(),
        output_fingerprint=sha256_json(
            {view: item.output_fingerprint for view, item in sorted(datasets.items())}
        ),
        output_artifacts=artifacts,
        ordering={
            "nodes": ["node_type", "node_id"],
            "edges": ["valid_from", "edge_type", "src_id", "dst_id", "edge_id"],
        },
        canonical_content_fingerprint=sha256_json(
            {view: item.output_fingerprint for view, item in sorted(datasets.items())}
        ),
        file_checksums={
            path: checksum
            for value in artifacts.values()
            if isinstance(value, dict)
            for path, checksum in (
                value.get("checksums", {}) if isinstance(value.get("checksums", {}), dict) else {}
            ).items()
        },
        export_parameters={"views": sorted(datasets), "formats": list(formats)},
        difficulty=source_manifest.difficulty,
        camouflage=source_manifest.camouflage,
        counterfactual=source_manifest.counterfactual,
    )
    manifest_path = graph_dir / "graph_manifest.json"
    manifest_path.write_text(manifest.model_dump_json(indent=2) + "\n", encoding="utf-8")
    return graph_dir, manifest


def _write_neo4j(dataset: GraphDataset, directory: Path) -> None:
    with (directory / "nodes.csv").open("w", encoding="utf-8", newline="") as handle:
        fields = ["node_id:ID", ":LABEL", *[f for f in GRAPH_NODE_SCHEMA if f != "node_id"]]
        writer = csv.DictWriter(
            handle, fieldnames=fields, lineterminator="\n", extrasaction="ignore"
        )
        writer.writeheader()
        writer.writerows(
            {**row, "node_id": row["node_id"], ":LABEL": row["node_type"]}
            for row in dataset.node_frame.to_dicts()
            if dataset.view != "oracle" or row["node_type"] not in {"CAMPAIGN", "PATTERN"}
        )
    with (directory / "relationships.csv").open("w", encoding="utf-8", newline="") as handle:
        fields = [
            "edge_id:ID",
            ":START_ID",
            ":END_ID",
            ":TYPE",
            *[
                f
                for f in GRAPH_EDGE_SCHEMA
                if f not in {"edge_id", "src_id", "dst_id", "edge_type"}
            ],
        ]
        writer = csv.DictWriter(
            handle, fieldnames=fields, lineterminator="\n", extrasaction="ignore"
        )
        writer.writeheader()
        writer.writerows(
            {
                **row,
                "edge_id": row["edge_id"],
                ":START_ID": row["src_id"],
                ":END_ID": row["dst_id"],
                ":TYPE": row["edge_type"],
            }
            for row in dataset.edge_frame.to_dicts()
        )
    labels = sorted({node.node_type for node in dataset.nodes})
    constraints = "".join(
        f"CREATE CONSTRAINT graph_{label.lower()}_id IF NOT EXISTS FOR (n:{label}) "
        "REQUIRE n.node_id IS UNIQUE;\n"
        for label in labels
    )
    (directory / "constraints.cypher").write_text(constraints, encoding="utf-8")
    (directory / "neo4j-admin-import.args").write_text(
        "--nodes=nodes.csv\n--relationships=relationships.csv\n"
        + (
            "--nodes=oracle_nodes.csv\n--relationships=oracle_memberships.csv\n"
            if dataset.view == "oracle"
            else ""
        ),
        encoding="utf-8",
    )
    if dataset.view == "oracle":
        with (directory / "oracle_nodes.csv").open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(
                handle, fieldnames=["node_id:ID", ":LABEL"], lineterminator="\n"
            )
            writer.writeheader()
            for node in dataset.nodes:
                if node.node_type in {"CAMPAIGN", "PATTERN"}:
                    writer.writerow({"node_id:ID": node.node_id, ":LABEL": node.node_type})
        with (directory / "oracle_memberships.csv").open(
            "w", encoding="utf-8", newline=""
        ) as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=[":START_ID", ":END_ID", ":TYPE", "campaign_id", "role"],
                lineterminator="\n",
            )
            writer.writeheader()
            for membership in dataset.memberships:
                writer.writerow(
                    {
                        ":START_ID": membership.member_id,
                        ":END_ID": membership.campaign_id,
                        ":TYPE": "MEMBER_OF",
                        "campaign_id": membership.campaign_id,
                        "role": membership.role,
                    }
                )
    (directory / "IMPORT.md").write_text(
        "# FraudTwin Neo4j import\n\n"
        "The CSV files are dependency-free, UTF-8, newline-delimited, and sorted by "
        "canonical IDs. Import the base graph with:\n\n"
        "```text\n"
        "neo4j-admin database import full --nodes=nodes.csv "
        "--relationships=relationships.csv fraudtwin\n"
        "```\n\n"
        "`nodes.csv` uses `node_id:ID` and `:LABEL`; `relationships.csv` uses "
        "`:START_ID`, `:END_ID`, and `:TYPE`. Oracle exports additionally provide "
        "`oracle_nodes.csv` and `oracle_memberships.csv` and include their files in "
        "`neo4j-admin-import.args`. Apply `constraints.cypher` after import. No "
        "Neo4j server or Python driver is required to produce these artifacts.\n",
        encoding="utf-8",
    )


def to_pyg(dataset: GraphDataset) -> Any:
    """Convert a graph to ``torch_geometric.data.HeteroData`` when installed."""

    try:
        import torch
        from torch_geometric.data import HeteroData  # type: ignore[import-not-found]
    except ImportError as exc:
        raise RuntimeError("PyTorch Geometric export requires `poetry install -E graph`") from exc
    node_types = sorted({node.node_type for node in dataset.nodes})
    indices = {
        kind: {
            node.node_id: index
            for index, node in enumerate(
                sorted(
                    (item for item in dataset.nodes if item.node_type == kind),
                    key=lambda item: item.node_id,
                )
            )
        }
        for kind in node_types
    }
    data = HeteroData()
    node_type_by_id = {node.node_id: node.node_type for node in dataset.nodes}
    for kind, values in indices.items():
        data[kind].node_id = sorted(values, key=lambda value: values[value])
    for edge_type in sorted({edge.edge_type for edge in dataset.edges}):
        selected = [
            edge
            for edge in dataset.edges
            if edge.edge_type == edge_type
            and edge.src_id in node_type_by_id
            and edge.dst_id in node_type_by_id
        ]
        if not selected:
            continue
        src_kind = node_type_by_id[selected[0].src_id]
        dst_kind = node_type_by_id[selected[0].dst_id]
        data[(src_kind, edge_type, dst_kind)].edge_index = torch.tensor(
            [
                [indices[src_kind][item.src_id] for item in selected],
                [indices[dst_kind][item.dst_id] for item in selected],
            ],
            dtype=torch.long,
        )
        relation = data[(src_kind, edge_type, dst_kind)]
        relation.weight = torch.tensor(
            [item.weight if item.weight is not None else 1.0 for item in selected],
            dtype=torch.float,
        )
        relation.event_time = torch.tensor(
            [int((item.event_time or item.valid_from).timestamp()) for item in selected],
            dtype=torch.long,
        )
        relation.availability_time = torch.tensor(
            [int((item.available_at or item.valid_from).timestamp()) for item in selected],
            dtype=torch.long,
        )
        relation.support_count = torch.tensor(
            [item.support_count or 0 for item in selected], dtype=torch.long
        )
        origin_codes = {
            "DOMAIN_EVENT": 0,
            "DERIVED_RELATION": 1,
            "SCENARIO_GROUND_TRUTH": 2,
        }
        relation.provenance = torch.tensor(
            [origin_codes[item.edge_origin] for item in selected], dtype=torch.long
        )
    return data


__all__ = [
    "GRAPH_EDGE_SCHEMA",
    "GRAPH_MEMBERSHIP_SCHEMA",
    "GRAPH_CAMPAIGN_SCHEMA",
    "GRAPH_NODE_SCHEMA",
    "GRAPH_PATTERN_SCHEMA",
    "GRAPH_EVIDENCE_SCHEMA",
    "GRAPH_HYPEREDGE_SCHEMA",
    "GRAPH_HYPEREDGE_MEMBERSHIP_SCHEMA",
    "GRAPH_SCHEMA_VERSION",
    "GraphDataset",
    "GraphEdge",
    "GraphCampaign",
    "GraphNode",
    "GraphEvidence",
    "GraphHyperedge",
    "GraphHyperedgeMembership",
    "build_graph",
    "to_pyg",
    "validate_graph",
    "validate_graph_scenarios",
    "write_graph",
]
