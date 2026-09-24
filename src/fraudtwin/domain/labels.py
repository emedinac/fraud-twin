"""Immutable label-observation records."""

from datetime import datetime
from typing import Literal

from pydantic import Field

from fraudtwin.domain.entities import _EntityModel

LabelState = Literal["UNOBSERVED", "PRELIMINARY", "CONFIRMED", "CORRECTED", "REOPENED"]
ObservedLabelValue = Literal["FRAUD", "LEGITIMATE"]


class LabelVersion(_EntityModel):
    """One immutable version of the operational label visible at a time."""

    label_version_id: str
    fraud_record_id: str
    payment_id: str
    case_id: str | None
    truth_label: bool
    observed_label: ObservedLabelValue | None
    label_state: LabelState
    investigation_selected: bool
    label_version: int = Field(ge=0)
    label_available_at: datetime | None
    label_corrected_at: datetime | None
    case_reopened_at: datetime | None
    reason: str
    causation_id: str
    simulation_run_id: str | None


class ObservationProvenance(_EntityModel):
    """Reproducibility metadata for one observation policy application."""

    policy_hash: str
    stream_ids: tuple[str, ...]
    source_run_id: str | None


class LabelCorrection(_EntityModel):
    """An immutable correction from one observed label version to another."""

    correction_id: str
    observation_id: str
    from_version: int = Field(ge=0)
    to_version: int = Field(ge=1)
    corrected_at: datetime
    observed_label: ObservedLabelValue
    truth_label: bool
    causation_id: str


class CaseReopening(_EntityModel):
    """An immutable reopening event in an observation history."""

    reopening_id: str
    observation_id: str
    label_version: int = Field(ge=1)
    reopened_at: datetime
    reason: str
    causation_id: str


class LabelObservation(_EntityModel):
    """A complete append-only observation history for one fraud record."""

    observation_id: str
    fraud_record_id: str
    payment_id: str
    truth_label: bool
    investigation_selected: bool
    versions: tuple[LabelVersion, ...]
    final_label_version: int = Field(ge=0)
    policy_hash: str
    stream_ids: tuple[str, ...]
    simulation_run_id: str | None
    corrections: tuple[LabelCorrection, ...] = ()
    reopenings: tuple[CaseReopening, ...] = ()
    provenance: ObservationProvenance | None = None


class FinalObservedLabel(_EntityModel):
    """The final observed projection, without oracle-only history fields."""

    observation_id: str
    fraud_record_id: str
    payment_id: str
    observed_label: ObservedLabelValue | None
    label_state: LabelState
    label_version: int = Field(ge=0)
    label_available_at: datetime | None
    label_corrected_at: datetime | None
    case_reopened_at: datetime | None
    investigation_selected: bool
    simulation_run_id: str | None


__all__ = [
    "FinalObservedLabel",
    "CaseReopening",
    "LabelCorrection",
    "LabelObservation",
    "ObservationProvenance",
    "LabelState",
    "LabelVersion",
    "ObservedLabelValue",
]
