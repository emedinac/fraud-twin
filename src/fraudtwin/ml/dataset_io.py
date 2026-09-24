"""Loading and validating persisted point-in-time dataset sources."""

from dataclasses import replace
from pathlib import Path

import polars as pl
from pydantic import BaseModel

from fraudtwin.campaign_dynamics import DynamicCampaignDataset
from fraudtwin.domain import (
    Account,
    BehaviorProfile,
    CampaignActorMembershipChange,
    CampaignIntensityDecision,
    CampaignLineage,
    CampaignPhaseChange,
    CampaignSourceSnapshot,
    CampaignStateSnapshot,
    CampaignTopologyMutation,
    CampaignTransition,
    Card,
    Customer,
    CustomerDispute,
    DelayedFraudLabel,
    Device,
    EntityStateChange,
    FraudAlert,
    FraudCase,
    FraudCaseConfirmation,
    FraudRecord,
    GraphCampaign,
    GraphCampaignMembership,
    GraphEvidence,
    GraphHyperedge,
    GraphHyperedgeMembership,
    GraphPattern,
    Institution,
    LedgerEntry,
    Merchant,
    NetworkEndpoint,
    Payment,
    PaymentEvent,
    PixKey,
)
from fraudtwin.manifest import RunManifest
from fraudtwin.ml.dataset import (
    _merge_records,
    _read_final_observed_labels,
    _read_label_observations,
    _read_models,
    _read_optional_models,
    _read_run_table,
    _validate_behavior_workflow,
)
from fraudtwin.simulation.behavior import BehaviorDataset
from fraudtwin.simulation.generator import EntityDataset
from fraudtwin.simulation.graph_fraud import GraphFraudDataset


def load_generated_run(
    run_dir: Path,
    *,
    allow_missing_delivery: bool = False,
) -> tuple[EntityDataset, BehaviorDataset, RunManifest]:
    """Load one existing generated run without regenerating unrelated records."""

    try:
        manifest = RunManifest.model_validate_json(
            (run_dir / "manifest.json").read_text(encoding="utf-8")
        )
    except (FileNotFoundError, OSError, ValueError) as exc:
        raise ValueError(f"invalid generated run manifest in {run_dir}") from exc

    entities = EntityDataset(
        customers=_read_run_table(run_dir, "entities", "customers", Customer),
        institutions=_read_run_table(run_dir, "entities", "institutions", Institution),
        accounts=_read_run_table(run_dir, "entities", "accounts", Account),
        cards=_read_run_table(run_dir, "entities", "cards", Card),
        merchants=_read_run_table(run_dir, "entities", "merchants", Merchant),
        devices=_read_run_table(run_dir, "entities", "devices", Device),
        pix_keys=_read_run_table(run_dir, "entities", "pix_keys", PixKey),
        network_endpoints=(
            _read_run_table(run_dir, "entities", "network_endpoints", NetworkEndpoint)
            if (run_dir / "entities" / "network_endpoints.parquet").is_file()
            else ()
        ),
        state_history=(
            tuple(
                EntityStateChange.model_validate(row)
                for row in pl.read_parquet(
                    run_dir / "entities" / "state_history.parquet"
                ).to_dicts()
            )
            if (run_dir / "entities" / "state_history.parquet").is_file()
            else ()
        ),
    )
    behavior = BehaviorDataset(
        profiles=_read_run_table(run_dir, "behavior", "behavior_profiles", BehaviorProfile),
        payments=_read_run_table(run_dir, "payments", "payments", Payment),
        payment_events=_read_run_table(
            run_dir,
            "payments",
            "payment_events",
            PaymentEvent,
            fallback_delivery=allow_missing_delivery,
        ),
        ledger_entries=_read_run_table(run_dir, "ledger", "ledger_entries", LedgerEntry),
        fraud_records=_read_run_table(run_dir, "fraud", "fraud_records", FraudRecord),
        alerts=_read_run_table(run_dir, "fraud", "fraud_alerts", FraudAlert),
        fraud_cases=_read_run_table(run_dir, "fraud", "fraud_cases", FraudCase),
        case_confirmations=_read_run_table(
            run_dir, "fraud", "case_confirmations", FraudCaseConfirmation
        ),
        customer_disputes=_read_run_table(
            run_dir,
            "fraud",
            "customer_disputes",
            CustomerDispute,
            fallback_delivery=allow_missing_delivery,
        ),
        fraud_labels=_read_run_table(run_dir, "fraud", "fraud_labels", DelayedFraudLabel),
        label_observations=_read_label_observations(run_dir, manifest),
        final_observed_labels=_read_final_observed_labels(run_dir, manifest),
        graph_memberships=(
            _read_models(
                run_dir / "oracle" / "graph" / "campaign_memberships.parquet",
                GraphCampaignMembership,
            )
            if (run_dir / "oracle" / "graph" / "campaign_memberships.parquet").is_file()
            else ()
        ),
        graph_campaigns=(
            _read_models(run_dir / "oracle" / "graph" / "campaigns.parquet", GraphCampaign)
            if (run_dir / "oracle" / "graph" / "campaigns.parquet").is_file()
            else ()
        ),
        graph_patterns=(
            _read_models(run_dir / "oracle" / "graph" / "patterns.parquet", GraphPattern)
            if (run_dir / "oracle" / "graph" / "patterns.parquet").is_file()
            else ()
        ),
        graph_evidence=(
            _read_models(run_dir / "oracle" / "graph" / "graph_evidence.parquet", GraphEvidence)
            if (run_dir / "oracle" / "graph" / "graph_evidence.parquet").is_file()
            else ()
        ),
        graph_hyperedges=(
            _read_models(run_dir / "oracle" / "graph" / "hyperedges.parquet", GraphHyperedge)
            if (run_dir / "oracle" / "graph" / "hyperedges.parquet").is_file()
            else ()
        ),
        graph_hyperedge_memberships=(
            _read_models(
                run_dir / "oracle" / "graph" / "hyperedge_memberships.parquet",
                GraphHyperedgeMembership,
            )
            if (run_dir / "oracle" / "graph" / "hyperedge_memberships.parquet").is_file()
            else ()
        ),
    )
    dynamic_roots = (
        sorted((run_dir / "campaign_dynamics").glob("M15-*/oracle"))
        if (run_dir / "campaign_dynamics").is_dir()
        else []
    )
    if dynamic_roots:
        dynamic_oracle = dynamic_roots[-1]
        dynamic_observable = dynamic_oracle.parent / "observable"
        dynamic_payments = _read_optional_models(dynamic_observable / "payments.parquet", Payment)
        dynamic_events = _read_optional_models(
            dynamic_observable / "payment_events.parquet", PaymentEvent
        )
        dynamic_entries = _read_optional_models(
            dynamic_observable / "ledger_entries.parquet", LedgerEntry
        )
        dynamic_graph_dir = dynamic_oracle / "graph"
        dynamic_memberships = _read_optional_models(
            dynamic_graph_dir / "campaign_memberships.parquet", GraphCampaignMembership
        )
        dynamic_campaigns = _read_optional_models(
            dynamic_graph_dir / "campaigns.parquet", GraphCampaign
        )
        dynamic_patterns = _read_optional_models(
            dynamic_graph_dir / "patterns.parquet", GraphPattern
        )
        dynamic_evidence = _read_optional_models(
            dynamic_graph_dir / "graph_evidence.parquet", GraphEvidence
        )
        dynamic_hyperedges = _read_optional_models(
            dynamic_graph_dir / "hyperedges.parquet", GraphHyperedge
        )
        dynamic_hyperedge_memberships = _read_optional_models(
            dynamic_graph_dir / "hyperedge_memberships.parquet", GraphHyperedgeMembership
        )
        behavior = replace(
            behavior,
            payments=_merge_records(
                behavior.payments,
                dynamic_payments,
                identifier="payment_id",
                prefer_existing=True,
            ),
            payment_events=_merge_records(
                behavior.payment_events, dynamic_events, identifier="event_id", prefer_existing=True
            ),
            ledger_entries=_merge_records(
                behavior.ledger_entries,
                dynamic_entries,
                identifier="ledger_entry_id",
                prefer_existing=True,
            ),
            graph_memberships=_merge_records(
                behavior.graph_memberships, dynamic_memberships, prefer_additions=True
            ),
            graph_campaigns=_merge_records(
                behavior.graph_campaigns,
                dynamic_campaigns,
                identifier="campaign_id",
                prefer_additions=True,
            ),
            graph_patterns=_merge_records(
                behavior.graph_patterns,
                dynamic_patterns,
                identifier="pattern_id",
                prefer_additions=True,
            ),
            graph_evidence=_merge_records(
                behavior.graph_evidence,
                dynamic_evidence,
                identifier="evidence_id",
                prefer_additions=True,
            ),
            graph_hyperedges=_merge_records(
                behavior.graph_hyperedges,
                dynamic_hyperedges,
                identifier="hyperedge_id",
                prefer_additions=True,
            ),
            graph_hyperedge_memberships=_merge_records(
                behavior.graph_hyperedge_memberships,
                dynamic_hyperedge_memberships,
                prefer_additions=True,
            ),
        )
        dynamic_graph = GraphFraudDataset(
            behavior.payments,
            behavior.payment_events,
            behavior.ledger_entries,
            behavior.fraud_records,
            behavior.graph_memberships,
            behavior.graph_patterns,
            behavior.graph_campaigns,
            behavior.graph_evidence,
            behavior.graph_hyperedges,
            behavior.graph_hyperedge_memberships,
        )
        dynamic = DynamicCampaignDataset(
            graph=dynamic_graph,
            snapshots=_read_models(dynamic_oracle / "snapshots.parquet", CampaignStateSnapshot),
            transitions=_read_models(dynamic_oracle / "transitions.parquet", CampaignTransition),
            phase_changes=_read_models(
                dynamic_oracle / "phase_changes.parquet", CampaignPhaseChange
            ),
            membership_changes=_read_models(
                dynamic_oracle / "membership_changes.parquet", CampaignActorMembershipChange
            ),
            intensity_decisions=_read_models(
                dynamic_oracle / "intensity_decisions.parquet", CampaignIntensityDecision
            ),
            topology_mutations=_read_models(
                dynamic_oracle / "topology_mutations.parquet", CampaignTopologyMutation
            ),
            lineage=_read_models(dynamic_oracle / "lineage.parquet", CampaignLineage),
            source_snapshots=_read_models(
                dynamic_oracle / "source_snapshots.parquet", CampaignSourceSnapshot
            ),
        )
        behavior = replace(behavior, campaign_dynamics=dynamic)
    if manifest.difficulty is not None:
        oracle_models: dict[str, tuple[type[BaseModel], str, str]] = {
            "behavior_profiles": (BehaviorProfile, "oracle/behavior", "behavior_profiles"),
            "payments": (Payment, "oracle/payments", "payments"),
            "payment_events": (PaymentEvent, "oracle/payments", "payment_events"),
            "ledger_entries": (LedgerEntry, "oracle/ledger", "ledger_entries"),
            "fraud_records": (FraudRecord, "oracle/fraud", "fraud_records"),
            "fraud_alerts": (FraudAlert, "oracle/fraud", "fraud_alerts"),
            "fraud_cases": (FraudCase, "oracle/fraud", "fraud_cases"),
            "case_confirmations": (FraudCaseConfirmation, "oracle/fraud", "case_confirmations"),
            "customer_disputes": (CustomerDispute, "oracle/fraud", "customer_disputes"),
            "fraud_labels": (DelayedFraudLabel, "oracle/fraud", "fraud_labels"),
        }
        oracle_tables: dict[str, tuple[BaseModel, ...]] = {}
        for name, (model, group, table) in oracle_models.items():
            path = run_dir / group / f"{table}.parquet"
            if path.is_file():
                oracle_tables[name] = _read_models(path, model)
        behavior = replace(behavior, oracle_tables=oracle_tables)
    _validate_behavior_workflow(entities, behavior)
    return entities, behavior, manifest
