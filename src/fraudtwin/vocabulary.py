"""Stable identifiers for FraudTwin scenarios, protocols, and capacities.

The identifiers in this module are descriptive metadata.  They are deliberately
separate from configuration keys so that adding vocabulary does not change
configuration hashes or generated record ordering.
"""

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class VocabularyDefinition:
    """One stable identifier and the human meaning attached to it."""

    id: str
    name: str
    description: str
    related_ids: tuple[str, ...] = ()
    defaults: tuple[tuple[str, str], ...] = ()
    aliases: tuple[str, ...] = ()


FRAUD_SCENARIOS: tuple[VocabularyDefinition, ...] = (
    VocabularyDefinition(
        "F01",
        "card-not-present",
        "Card activity where the physical card is not present.",
        related_ids=("P05", "C01", "C04", "C09"),
        defaults=(
            ("enabled", "true"),
            ("weight", "1.0"),
            ("count", "1"),
            ("attempt_count", "20"),
            ("window_seconds", "60"),
        ),
    ),
    VocabularyDefinition(
        "F02",
        "card-testing",
        "Repeated low-value card authorization attempts.",
        related_ids=("P06", "C01", "C04", "C09"),
        defaults=(
            ("enabled", "true"),
            ("weight", "1.0"),
            ("count", "1"),
            ("attempt_count", "20"),
            ("window_seconds", "60"),
        ),
    ),
    VocabularyDefinition(
        "F03",
        "account-takeover",
        "Fraud following compromised account access.",
        related_ids=("P07", "C01", "C04", "C08"),
        defaults=(
            ("enabled", "true"),
            ("weight", "1.0"),
            ("count", "1"),
            ("attempt_count", "20"),
            ("window_seconds", "60"),
        ),
    ),
    VocabularyDefinition(
        "F04",
        "instant-payment-scam",
        "A fraudulent PIX payment, often to a new beneficiary.",
        related_ids=("P08", "C01", "C04", "C12"),
        defaults=(
            ("enabled", "true"),
            ("weight", "1.0"),
            ("count", "1"),
            ("attempt_count", "20"),
            ("window_seconds", "60"),
        ),
    ),
    VocabularyDefinition(
        "F05",
        "velocity-attack",
        "A rapid burst of repeated payment attempts.",
        related_ids=("P09", "C01", "C04", "C09"),
        defaults=(
            ("enabled", "true"),
            ("weight", "1.0"),
            ("count", "1"),
            ("attempt_count", "20"),
            ("window_seconds", "60"),
        ),
    ),
)

PROTOCOLS: tuple[VocabularyDefinition, ...] = (
    VocabularyDefinition(
        "P01",
        "fraud-generation",
        "Creates fraud campaigns and their scenario-linked payment records.",
        related_ids=("F01", "F02", "F03", "F04", "F05", "C01", "C02", "C03"),
        aliases=("M6",),
    ),
    VocabularyDefinition(
        "P02",
        "difficulty",
        "Controls how difficult generated fraud is to distinguish from legitimate activity.",
        related_ids=("C01", "C16"),
        aliases=("M12",),
    ),
    VocabularyDefinition(
        "P03",
        "counterfactual",
        "Creates alternative trajectories or outcomes for a source run.",
        related_ids=("C01", "C05"),
        aliases=("M14",),
    ),
    VocabularyDefinition(
        "P04",
        "campaign-dynamics",
        "Evolves campaigns through deterministic timing and topology changes.",
        related_ids=("C03", "C04", "C05"),
        aliases=("M15",),
    ),
    VocabularyDefinition(
        "P05",
        "card-not-present-protocol",
        "Implements the F01 card-not-present payment pattern.",
        related_ids=("F01", "C09"),
    ),
    VocabularyDefinition(
        "P06",
        "card-testing-protocol",
        "Implements the F02 repeated authorization pattern.",
        related_ids=("F02", "C09"),
    ),
    VocabularyDefinition(
        "P07",
        "account-takeover-protocol",
        "Implements the F03 compromised-account payment pattern.",
        related_ids=("F03", "C08"),
    ),
    VocabularyDefinition(
        "P08",
        "instant-payment-scam-protocol",
        "Implements the F04 instant-payment scam pattern.",
        related_ids=("F04", "C12"),
    ),
    VocabularyDefinition(
        "P09",
        "velocity-attack-protocol",
        "Implements the F05 rapid payment-attempt pattern.",
        related_ids=("F05", "C09"),
    ),
)

CAPACITIES: tuple[VocabularyDefinition, ...] = (
    VocabularyDefinition(
        "C01",
        "baseline-payment-capacity",
        "Legitimate payment volume requested by the simulation.",
        defaults=(("source", "payments.daily_target * simulation.duration_days"),),
    ),
    VocabularyDefinition(
        "C02",
        "fraud-campaign-capacity",
        "Overall fraud campaign limit from scenario_count and target_rate.",
        related_ids=("P01",),
    ),
    VocabularyDefinition(
        "C03",
        "scenario-campaign-capacity",
        "Per-scenario campaign limit from fraud.scenarios.<F##>.count.",
        related_ids=("P01",),
    ),
    VocabularyDefinition(
        "C04",
        "ledger-debit-capacity",
        "Running ledger balance plus the account overdraft limit.",
        related_ids=("P01", "P04"),
    ),
    VocabularyDefinition(
        "C05",
        "lifecycle-window-capacity",
        "Time available for authorization, settlement, refund, reversal, and return events.",
        related_ids=("P03", "P04"),
    ),
    VocabularyDefinition(
        "C06",
        "customer-capacity",
        "Number of generated customers.",
        defaults=(("source", "population.customers"),),
    ),
    VocabularyDefinition(
        "C07",
        "institution-capacity",
        "Number of generated institutions.",
        defaults=(("source", "population.institutions"),),
    ),
    VocabularyDefinition(
        "C08",
        "account-capacity",
        "Number of generated accounts.",
        related_ids=("F03",),
        defaults=(("source", "population.accounts"),),
    ),
    VocabularyDefinition(
        "C09",
        "card-capacity",
        "Number of generated active cards.",
        related_ids=("F01", "F02", "F05"),
        defaults=(("source", "population.cards"),),
    ),
    VocabularyDefinition(
        "C10",
        "merchant-capacity",
        "Number of generated merchants.",
        defaults=(("source", "population.merchants"),),
    ),
    VocabularyDefinition(
        "C11",
        "device-capacity",
        "Number of generated devices.",
        defaults=(("source", "population.devices"),),
    ),
    VocabularyDefinition(
        "C12",
        "pix-key-capacity",
        "Number of generated PIX keys and PIX-capable accounts.",
        related_ids=("F04",),
        defaults=(("source", "population.pix_keys"),),
    ),
    VocabularyDefinition(
        "C13",
        "account-ownership-capacity",
        "Valid customer-to-account relationships.",
        related_ids=("C06", "C08"),
    ),
    VocabularyDefinition(
        "C14",
        "institution-account-capacity",
        "Valid institution-to-account relationships.",
        related_ids=("C07", "C08"),
    ),
    VocabularyDefinition(
        "C15",
        "pix-key-assignment-capacity",
        "Valid PIX-key-to-account relationships.",
        related_ids=("C08", "C12"),
    ),
    VocabularyDefinition(
        "C16",
        "hard-negative-capacity",
        "Legitimate lookalike records created for fraud training.",
        related_ids=("P02",),
    ),
    VocabularyDefinition(
        "C17",
        "label-observation-capacity",
        "Records that can receive observed investigation and fraud labels.",
        related_ids=("P03",),
    ),
)


def _index(entries: tuple[VocabularyDefinition, ...]) -> dict[str, VocabularyDefinition]:
    return {key: entry for entry in entries for key in (entry.id, *entry.aliases)}


_SCENARIO_INDEX = _index(FRAUD_SCENARIOS)
_PROTOCOL_INDEX = _index(PROTOCOLS)
_CAPACITY_INDEX = _index(CAPACITIES)


def _validate_registry() -> None:
    for prefix, entries in (("F", FRAUD_SCENARIOS), ("P", PROTOCOLS), ("C", CAPACITIES)):
        ids = [entry.id for entry in entries]
        if len(ids) != len(set(ids)):
            raise RuntimeError(f"duplicate {prefix} vocabulary identifier")
        if any(not re.fullmatch(rf"{prefix}[0-9]{{2}}", identifier) for identifier in ids):
            raise RuntimeError(f"invalid {prefix} vocabulary identifier")


_validate_registry()


def get_scenario(identifier: str) -> VocabularyDefinition:
    """Return the fraud scenario definition for an ``F##`` identifier."""

    return _SCENARIO_INDEX[identifier]


def get_protocol(identifier: str) -> VocabularyDefinition:
    """Return the protocol definition, including legacy aliases."""

    return _PROTOCOL_INDEX[identifier]


def get_capacity(identifier: str) -> VocabularyDefinition:
    """Return the capacity definition for a ``C##`` identifier."""

    return _CAPACITY_INDEX[identifier]


__all__ = [
    "FRAUD_SCENARIOS",
    "CAPACITIES",
    "PROTOCOLS",
    "VocabularyDefinition",
    "get_capacity",
    "get_protocol",
    "get_scenario",
]
