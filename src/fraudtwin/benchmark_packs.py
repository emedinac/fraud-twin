"""Immutable public benchmark-pack definitions and resource loading."""

import re
from datetime import UTC, datetime
from importlib.resources import files
from pathlib import Path
from typing import Any, Literal, cast

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator

from fraudtwin.reproducibility import sha256_json

PUBLIC_PACK_VERSION = "0.34.0"
PUBLIC_PACK_RESOURCE_DIR = "public_packs"
PUBLIC_PACK_REFERENCE_RE = re.compile(
    r"^(?P<id>FT-B0[1-8]-(?:STABLE|TEMPORAL|BOUNDARY|CAMOUFLAGE|GRAPH|OBSERVABILITY|CALIBRATED|MIXED))@"
    r"(?P<version>\d+\.\d+(?:\.\d+)?)$"
)


class PublicBenchmarkPack(BaseModel):
    """Immutable, distributable M21 benchmark definition."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    id: str = Field(pattern=r"^FT-B0[1-8]-[A-Z]+$")
    version: str = Field(pattern=r"^(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)$")
    suite: Literal[
        "baseline",
        "temporal",
        "boundary",
        "camouflage",
        "graph",
        "observability",
        "calibrated",
        "mixed",
    ]
    generator_compatibility: str = Field(pattern=r"^>=\d+\.\d+\.\d+,<\d+\.\d+\.\d+$")
    seed: int = Field(ge=0)
    seed_tree_version: str = Field(min_length=1)
    difficulty: int = Field(ge=1, le=10)
    simulation_start: datetime
    simulation_end: datetime
    split_boundaries: dict[str, datetime]
    scenario_definitions: dict[str, Any]
    stress_parameters: dict[str, Any]
    label_observation_policy: dict[str, Any]
    calibration: dict[str, Any] | None = None
    metric_definitions: dict[str, Any]
    resolved_configuration_hash: str = Field(min_length=1)
    expected_descriptors: dict[str, Any]
    expected_fingerprints: dict[str, str]

    @field_validator("simulation_start", "simulation_end", mode="before")
    @classmethod
    def timestamps_are_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("public benchmark pack timestamps must include a timezone")
        return value.astimezone(UTC)

    @field_validator("split_boundaries", mode="before")
    @classmethod
    def split_timestamps_are_aware(cls, value: dict[str, Any]) -> dict[str, datetime]:
        return {key: cls.timestamps_are_aware(item) for key, item in value.items()}

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "PublicBenchmarkPack":
        pack = cls.model_validate(payload)
        if pack.simulation_end <= pack.simulation_start:
            raise ValueError("public benchmark pack simulation_end must follow simulation_start")
        required_splits = {"train_end", "validation_end", "test_end"}
        if set(pack.split_boundaries) != required_splits:
            raise ValueError("public benchmark pack must freeze train, validation, and test ends")
        boundaries = [
            pack.simulation_start,
            *(pack.split_boundaries[name] for name in ("train_end", "validation_end", "test_end")),
        ]
        if any(left >= right for left, right in zip(boundaries, boundaries[1:], strict=False)):
            raise ValueError("public benchmark pack windows must be chronological")
        if pack.split_boundaries["test_end"] != pack.simulation_end:
            raise ValueError("public benchmark pack test_end must equal simulation_end")
        if pack.suite in {"calibrated", "mixed"} and pack.calibration is None:
            raise ValueError(f"public pack suite {pack.suite} requires frozen calibration")
        if not pack.expected_descriptors or not pack.expected_fingerprints:
            raise ValueError(
                "public benchmark pack must freeze expected descriptors and fingerprints"
            )
        return pack

    @property
    def identity(self) -> str:
        return f"{self.id}@{self.version}"

    @property
    def fingerprint(self) -> str:
        return sha256_json(self.model_dump(mode="json"))


def _public_pack_resources() -> tuple[Any, ...]:
    root = files("fraudtwin").joinpath(PUBLIC_PACK_RESOURCE_DIR)
    return tuple(
        sorted(
            (
                item
                for item in root.iterdir()
                if item.name.startswith("FT-B") and item.name.endswith(".yaml")
            ),
            key=lambda item: item.name,
        )
    )


def _pack_payload(resource: Any) -> dict[str, Any]:
    raw = yaml.safe_load(resource.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"public benchmark pack must be a mapping: {resource.name}")
    return cast(dict[str, Any], raw)


def list_public_packs() -> tuple[PublicBenchmarkPack, ...]:
    """Return all bundled public packs in stable identity order."""

    return tuple(
        sorted(
            (
                PublicBenchmarkPack.from_payload(_pack_payload(item))
                for item in _public_pack_resources()
            ),
            key=lambda item: item.identity,
        )
    )


def load_public_pack(reference: str) -> PublicBenchmarkPack:
    """Resolve an exact or unambiguous major/minor public-pack reference."""

    match = PUBLIC_PACK_REFERENCE_RE.fullmatch(reference)
    if match is None:
        raise ValueError("public pack reference must use FT-Bxx-NAME@major.minor[.patch]")
    pack_id = match.group("id")
    requested_version = match.group("version")
    candidates = [item for item in list_public_packs() if item.id == pack_id]
    if requested_version.count(".") == 2:
        candidates = [item for item in candidates if item.version == requested_version]
    else:
        candidates = [
            item for item in candidates if item.version.startswith(requested_version + ".")
        ]
    if not candidates:
        raise ValueError(f"public benchmark pack does not exist: {reference}")
    if len(candidates) > 1:
        raise ValueError(f"public benchmark pack reference is ambiguous: {reference}")
    return candidates[0]


def _calibration_resource(pack: PublicBenchmarkPack) -> Path | None:
    if pack.calibration is None:
        return None
    resource_name = pack.calibration.get("resource")
    if not isinstance(resource_name, str) or not resource_name:
        raise ValueError(f"{pack.identity} calibration must name a bundled resource")
    resource = files("fraudtwin").joinpath(PUBLIC_PACK_RESOURCE_DIR, resource_name)
    if not resource.is_file():
        raise ValueError(f"{pack.identity} calibration resource is missing: {resource_name}")
    path = Path(str(resource))
    if not path.is_file():
        raise ValueError("public benchmark resources must be available as package files")
    return path
