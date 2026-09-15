"""Deterministic, append-only observation of latent fraud labels."""

from __future__ import annotations

import math
from collections.abc import Iterable
from datetime import datetime, timedelta
from random import Random
from typing import Literal

from fraudtwin.config import (
    LabelDelayConfig,
    LabelObservationCondition,
    LabelObservationConfig,
    SimulationRunConfig,
)
from fraudtwin.domain import (
    CaseReopening,
    Customer,
    FinalObservedLabel,
    FraudAlert,
    FraudRecord,
    LabelCorrection,
    LabelObservation,
    LabelVersion,
    ObservationProvenance,
    Payment,
)
from fraudtwin.reproducibility import sha256_json
from fraudtwin.seed import create_stream_rng

STREAM_NAMES = (
    "investigation-selection",
    "missing",
    "confirmation-delay",
    "preliminary-error",
    "correction",
    "correction-delay",
    "reopening",
    "reopening-delay",
)


def resolve_label_observation(
    config: SimulationRunConfig | LabelObservationConfig,
) -> LabelObservationConfig:
    """Return the validated M17 policy from either a run or policy config."""

    return config.labels if isinstance(config, SimulationRunConfig) else config


def _rng(root_seed: int, stage: str, record_id: str) -> Random:
    """Return the deterministic stream used for one observation stage."""

    return create_stream_rng(root_seed, f"m17:{stage}:{record_id}")


def _delay(rng: Random, settings: LabelDelayConfig) -> timedelta:
    if settings.distribution == "fixed":
        seconds = settings.median_seconds
    else:
        seconds = rng.lognormvariate(math.log(settings.median_seconds), settings.sigma)
    seconds = max(settings.minimum_seconds, seconds)
    if settings.maximum_seconds is not None:
        seconds = min(settings.maximum_seconds, seconds)
    if not math.isfinite(seconds) or seconds < 0:
        raise ValueError("resolved label delay must be finite and non-negative")
    return timedelta(seconds=seconds)


def _matches(
    condition: LabelObservationCondition,
    record: FraudRecord,
    payment: Payment,
    customer: Customer | None,
    alert: FraudAlert | None,
) -> bool:
    return (
        (condition.amount_min is None or payment.amount >= condition.amount_min)
        and (condition.amount_max is None or payment.amount <= condition.amount_max)
        and (condition.payment_rail is None or payment.payment_rail == condition.payment_rail)
        and (condition.fraud_type is None or record.scenario_type == condition.fraud_type)
        and (
            condition.customer_segment is None
            or (customer is not None and customer.risk_segment == condition.customer_segment)
        )
        and (
            condition.alert_severity is None
            or (alert is not None and alert.severity == condition.alert_severity)
        )
        and (condition.campaign_id is None or condition.campaign_id in record.affected_entity_ids)
    )


def _condition_for(
    policy: LabelObservationConfig,
    record: FraudRecord,
    payment: Payment,
    customer: Customer | None,
    alert: FraudAlert | None,
) -> LabelObservationCondition | None:
    matches = [
        item for item in policy.conditions if _matches(item, record, payment, customer, alert)
    ]
    if len(matches) > 1:
        raise ValueError(f"overlapping label observation conditions for {record.fraud_record_id}")
    return matches[0] if matches else None


def apply_label_observation(
    config: SimulationRunConfig | LabelObservationConfig,
    fraud_records: Iterable[FraudRecord],
    payments: Iterable[Payment],
    *,
    customers: Iterable[Customer] = (),
    alerts: Iterable[FraudAlert] = (),
    seed: int | None = None,
    simulation_run_id: str | None = None,
) -> tuple[tuple[LabelObservation, ...], tuple[FinalObservedLabel, ...]]:
    """Apply a deterministic observation policy without mutating source records."""

    policy = resolve_label_observation(config)
    if not policy.enabled:
        return (), ()
    if isinstance(config, SimulationRunConfig):
        root_seed = config.simulation.seed if seed is None else seed
    else:
        if seed is None:
            raise ValueError("seed is required when applying a standalone label policy")
        root_seed = seed
    policy_hash = sha256_json(policy.model_dump(mode="json"))
    payment_by_id = {item.payment_id: item for item in payments}
    customer_by_id = {item.customer_id: item for item in customers}
    alert_by_record = {item.fraud_record_id: item for item in alerts}
    observations: list[LabelObservation] = []
    finals: list[FinalObservedLabel] = []
    for record in sorted(fraud_records, key=lambda item: item.fraud_record_id):
        payment = payment_by_id.get(record.payment_id)
        if payment is None:
            raise ValueError(f"fraud record references unknown payment: {record.payment_id}")
        customer = customer_by_id.get(record.customer_id)
        condition = _condition_for(
            policy, record, payment, customer, alert_by_record.get(record.fraud_record_id)
        )
        rate = (
            condition.investigation_rate
            if condition and condition.investigation_rate is not None
            else policy.investigation_rate
        )
        selected = _rng(root_seed, STREAM_NAMES[0], record.fraud_record_id).random() < rate
        versions: list[LabelVersion] = [
            LabelVersion(
                label_version_id=f"OBS-{record.fraud_record_id}-V0",
                fraud_record_id=record.fraud_record_id,
                payment_id=record.payment_id,
                case_id=None,
                truth_label=record.fraud_truth,
                observed_label=None,
                label_state="UNOBSERVED",
                investigation_selected=selected,
                label_version=0,
                label_available_at=None,
                label_corrected_at=None,
                case_reopened_at=None,
                reason="NOT_INVESTIGATED" if not selected else "PENDING_OBSERVATION",
                causation_id=record.event_id,
                simulation_run_id=simulation_run_id,
            )
        ]
        corrections: list[LabelCorrection] = []
        reopenings: list[CaseReopening] = []
        if selected:
            missing = (
                record.fraud_truth
                and _rng(root_seed, STREAM_NAMES[1], record.fraud_record_id).random()
                < policy.missing_fraud_rate
            )
            if not missing:
                delay_settings = (
                    condition.delay
                    if condition and condition.delay is not None
                    else policy.confirmation_delay
                )
                available_at = record.occurred_at + _delay(
                    _rng(root_seed, STREAM_NAMES[2], record.fraud_record_id),
                    delay_settings,
                )
                observed: Literal["FRAUD", "LEGITIMATE"] = (
                    "FRAUD" if record.fraud_truth else "LEGITIMATE"
                )
                errored = (
                    _rng(root_seed, STREAM_NAMES[3], record.fraud_record_id).random()
                    < policy.preliminary_error_rate
                )
                if errored:
                    observed = "LEGITIMATE" if observed == "FRAUD" else "FRAUD"
                versions.append(
                    versions[0].model_copy(
                        update={
                            "label_version_id": f"OBS-{record.fraud_record_id}-V1",
                            "observed_label": observed,
                            "label_state": "PRELIMINARY" if errored else "CONFIRMED",
                            "label_version": 1,
                            "label_available_at": available_at,
                            "reason": "PRELIMINARY_ERROR" if errored else "CONFIRMED",
                        }
                    )
                )
                if (
                    _rng(root_seed, STREAM_NAMES[4], record.fraud_record_id).random()
                    < policy.correction_rate
                ):
                    corrected_at = available_at + _delay(
                        _rng(root_seed, STREAM_NAMES[5], record.fraud_record_id),
                        policy.correction_delay,
                    )
                    reopened = (
                        _rng(root_seed, STREAM_NAMES[6], record.fraud_record_id).random()
                        < policy.reopening_rate
                    )
                    if reopened:
                        reopened_at = corrected_at + _delay(
                            _rng(root_seed, STREAM_NAMES[7], record.fraud_record_id),
                            policy.reopening_delay,
                        )
                        versions.append(
                            versions[-1].model_copy(
                                update={
                                    "label_version_id": f"OBS-{record.fraud_record_id}-V2",
                                    "label_state": "REOPENED",
                                    "label_version": 2,
                                    "label_available_at": reopened_at,
                                    "case_reopened_at": reopened_at,
                                    "reason": "CASE_REOPENED",
                                }
                            )
                        )
                        reopenings.append(
                            CaseReopening(
                                reopening_id=f"REOPEN-{record.fraud_record_id}-V2",
                                observation_id=f"OBS-{record.fraud_record_id}",
                                label_version=2,
                                reopened_at=reopened_at,
                                reason="CASE_REOPENED",
                                causation_id=record.event_id,
                            )
                        )
                        corrected_at = reopened_at
                    final_version = versions[-1].label_version + 1
                    versions.append(
                        versions[-1].model_copy(
                            update={
                                "label_version_id": (
                                    f"OBS-{record.fraud_record_id}-V{final_version}"
                                ),
                                "observed_label": "FRAUD" if record.fraud_truth else "LEGITIMATE",
                                "label_state": "CORRECTED",
                                "label_version": final_version,
                                "label_available_at": corrected_at,
                                "label_corrected_at": corrected_at,
                                "case_reopened_at": corrected_at if reopened else None,
                                "reason": "LABEL_CORRECTED",
                            }
                        )
                    )
                    corrections.append(
                        LabelCorrection(
                            correction_id=f"CORR-{record.fraud_record_id}-V{final_version}",
                            observation_id=f"OBS-{record.fraud_record_id}",
                            from_version=2 if reopened else 1,
                            to_version=final_version,
                            corrected_at=corrected_at,
                            observed_label="FRAUD" if record.fraud_truth else "LEGITIMATE",
                            truth_label=record.fraud_truth,
                            causation_id=record.event_id,
                        )
                    )
        stream_ids = tuple(f"m17:{name}:{record.fraud_record_id}" for name in STREAM_NAMES)
        observation = LabelObservation(
            observation_id=f"OBS-{record.fraud_record_id}",
            fraud_record_id=record.fraud_record_id,
            payment_id=record.payment_id,
            truth_label=record.fraud_truth,
            investigation_selected=selected,
            versions=tuple(versions),
            final_label_version=versions[-1].label_version,
            policy_hash=policy_hash,
            stream_ids=stream_ids,
            simulation_run_id=simulation_run_id,
            corrections=tuple(corrections),
            reopenings=tuple(reopenings),
            provenance=ObservationProvenance(
                policy_hash=policy_hash,
                stream_ids=stream_ids,
                source_run_id=simulation_run_id,
            ),
        )
        observations.append(observation)
        final = versions[-1]
        finals.append(
            FinalObservedLabel(
                observation_id=observation.observation_id,
                fraud_record_id=record.fraud_record_id,
                payment_id=record.payment_id,
                observed_label=final.observed_label,
                label_state=final.label_state,
                label_version=final.label_version,
                label_available_at=final.label_available_at,
                label_corrected_at=final.label_corrected_at,
                case_reopened_at=final.case_reopened_at,
                investigation_selected=selected,
                simulation_run_id=simulation_run_id,
            )
        )
    validate_label_observation(observations)
    return tuple(observations), tuple(finals)


def reconstruct_label_history(observation: LabelObservation) -> tuple[LabelVersion, ...]:
    """Return the immutable versions in validated chronological order."""

    validate_label_observation((observation,))
    return observation.versions


def visible_label_at(observation: LabelObservation, prediction_time: datetime) -> LabelVersion:
    """Resolve the latest label version available at a prediction timestamp."""

    visible = tuple(
        item
        for item in observation.versions
        if item.label_available_at is not None and item.label_available_at <= prediction_time
    )
    return (
        max(visible, key=lambda item: (item.label_available_at, item.label_version))
        if visible
        else observation.versions[0]
    )


def validate_label_observation(observations: Iterable[LabelObservation]) -> None:
    """Validate references, ordering, and immutable truth invariants."""

    seen: set[str] = set()
    seen_version_ids: set[str] = set()
    for observation in observations:
        if observation.observation_id in seen:
            raise ValueError(f"duplicate observation ID: {observation.observation_id}")
        seen.add(observation.observation_id)
        if not observation.versions or observation.versions[0].label_version != 0:
            raise ValueError("observation history must begin at version zero")
        previous_at: datetime | None = None
        versions_by_number: dict[int, LabelVersion] = {}
        for expected, version in enumerate(observation.versions):
            if version.label_version != expected:
                raise ValueError(f"non-monotonic label versions for {observation.observation_id}")
            if version.label_version_id in seen_version_ids:
                raise ValueError(f"duplicate label version ID: {version.label_version_id}")
            expected_version_id = f"{observation.observation_id}-V{version.label_version}"
            if version.label_version_id != expected_version_id:
                raise ValueError(
                    f"label version {version.label_version_id} references another observation"
                )
            seen_version_ids.add(version.label_version_id)
            versions_by_number[version.label_version] = version
            if version.fraud_record_id != observation.fraud_record_id:
                raise ValueError("label version references another fraud record")
            if version.payment_id != observation.payment_id:
                raise ValueError("label version references another payment")
            if version.simulation_run_id != observation.simulation_run_id:
                raise ValueError("label version references another simulation run")
            if version.truth_label != observation.truth_label:
                raise ValueError("latent truth changed in label history")
            if version.label_available_at is not None:
                if previous_at is not None and version.label_available_at < previous_at:
                    raise ValueError("label availability timestamps are not ordered")
                previous_at = version.label_available_at
        if observation.final_label_version != observation.versions[-1].label_version:
            raise ValueError("final label version does not match history")
        if observation.provenance is not None:
            if observation.provenance.policy_hash != observation.policy_hash:
                raise ValueError("observation provenance has another policy hash")
            if observation.provenance.stream_ids != observation.stream_ids:
                raise ValueError("observation provenance has different stream IDs")
            if observation.provenance.source_run_id != observation.simulation_run_id:
                raise ValueError("observation provenance references another simulation run")
        correction_ids: set[str] = set()
        for correction in observation.corrections:
            if correction.correction_id in correction_ids:
                raise ValueError(f"duplicate correction ID: {correction.correction_id}")
            correction_ids.add(correction.correction_id)
            if correction.observation_id != observation.observation_id:
                raise ValueError("correction references another observation")
            if not (
                0
                <= correction.from_version
                < correction.to_version
                <= observation.final_label_version
            ):
                raise ValueError("correction references an invalid label version range")
            if correction.to_version != observation.final_label_version:
                raise ValueError("correction does not target the final label version")
            if correction.truth_label != observation.truth_label:
                raise ValueError("correction changes latent truth")
            target = versions_by_number[correction.to_version]
            if target.observed_label != correction.observed_label:
                raise ValueError("correction label does not match its target version")
        reopening_ids: set[str] = set()
        for reopening in observation.reopenings:
            if reopening.reopening_id in reopening_ids:
                raise ValueError(f"duplicate reopening ID: {reopening.reopening_id}")
            reopening_ids.add(reopening.reopening_id)
            if reopening.observation_id != observation.observation_id:
                raise ValueError("reopening references another observation")
            if not 1 <= reopening.label_version <= observation.final_label_version:
                raise ValueError("reopening references an invalid label version")
            version = versions_by_number[reopening.label_version]
            if version.label_state != "REOPENED":
                raise ValueError("reopening does not reference a reopened label version")


__all__ = [
    "apply_label_observation",
    "reconstruct_label_history",
    "resolve_label_observation",
    "validate_label_observation",
    "visible_label_at",
]
