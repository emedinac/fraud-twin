"""Deterministic entity generation and batch output adapters."""

from fraudtwin.domain.graph import (
    GraphCampaign,
    GraphCampaignMembership,
    GraphEvidence,
    GraphHyperedge,
    GraphHyperedgeMembership,
    GraphPattern,
    NetworkEndpoint,
)
from fraudtwin.simulation.behavior import BehaviorDataset, BehaviorGenerator, generate_behavior
from fraudtwin.simulation.cases import FraudWorkflowDataset, FraudWorkflowGenerator
from fraudtwin.simulation.fraud import FraudDataset, FraudScenarioGenerator
from fraudtwin.simulation.generator import EntityDataset, EntityGenerator, generate_entities
from fraudtwin.simulation.graph_fraud import GraphFraudDataset, GraphFraudGenerator
from fraudtwin.simulation.payments import (
    PaymentDataset,
    PaymentGenerator,
    count_pix_lifecycle_events,
)
from fraudtwin.simulation.quality import QualityFaultInjector

__all__ = [
    "BehaviorDataset",
    "BehaviorGenerator",
    "EntityDataset",
    "EntityGenerator",
    "FraudDataset",
    "FraudScenarioGenerator",
    "FraudWorkflowDataset",
    "FraudWorkflowGenerator",
    "PaymentDataset",
    "PaymentGenerator",
    "count_pix_lifecycle_events",
    "QualityFaultInjector",
    "generate_behavior",
    "generate_entities",
    "GraphCampaignMembership",
    "GraphCampaign",
    "GraphPattern",
    "GraphEvidence",
    "GraphHyperedge",
    "GraphHyperedgeMembership",
    "NetworkEndpoint",
    "GraphFraudDataset",
    "GraphFraudGenerator",
]
