"""Avro contract loading, compatibility checking, and observable mapping.

The M8 ``fraudtwin.schema`` helpers intentionally remain separate: those
helpers model data-quality schema faults, while this module validates the
clean observable contracts that a future M25 producer will publish.
"""

import hashlib
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from importlib.resources import files
from pathlib import Path
from typing import Any, cast

import avro.compatibility
import avro.io
import avro.schema
import yaml
from pydantic import BaseModel

_SEMVER = re.compile(r"^(?P<major>0|[1-9]\d*)\.(?P<minor>0|[1-9]\d*)\.(?P<patch>0|[1-9]\d*)$")
_DECIMAL_QUANTUM = Decimal("0.01")
_DECIMAL_PRECISION = 18
_COMPATIBLE = avro.compatibility.SchemaCompatibilityType.compatible


class ContractValidationError(ValueError):
    """Raised when registry metadata or an Avro contract is invalid."""


@dataclass(frozen=True)
class ContractVersion:
    """One immutable schema version registered for a subject."""

    subject: str
    version: str
    path: Path
    schema: avro.schema.Schema
    canonical_sha256: str
    breaking: bool
    supersedes: str | None = None

    @property
    def record_name(self) -> str:
        return cast(str, cast(Any, self.schema).fullname)


@dataclass(frozen=True)
class ContractSubject:
    """A subject and all of its versions in ascending semantic order."""

    name: str
    record_name: str
    latest: str
    versions: tuple[ContractVersion, ...]


@dataclass(frozen=True)
class AvroDatumMapper:
    """Map domain records to and from one loaded observable contract registry."""

    registry: "AvroContractRegistry"

    def to_datum(self, subject: str, record: BaseModel | Mapping[str, Any]) -> dict[str, Any]:
        """Return a validated observable Avro datum for ``subject``."""

        return self.registry.datum(subject, record)

    def encode(self, subject: str, record: BaseModel | Mapping[str, Any]) -> bytes:
        """Encode a domain record using the latest subject schema."""

        return self.registry.encode(subject, record)

    def decode(self, subject: str, payload: bytes) -> dict[str, Any]:
        """Decode one payload using the latest subject schema."""

        return self.registry.decode(subject, payload)


@dataclass(frozen=True)
class RegistryReport:
    """Summary returned after validating a complete registry."""

    path: Path
    subjects: int
    versions: int
    fingerprints: dict[str, str]


@dataclass(frozen=True)
class AvroContractRegistry:
    """Parsed source-controlled registry and its Avro schemas."""

    root: Path
    compatibility: str
    subjects: tuple[ContractSubject, ...]

    def subject(self, name: str) -> ContractSubject:
        for item in self.subjects:
            if item.name == name:
                return item
        raise KeyError(f"unknown Avro contract subject: {name}")

    def contract(self, subject: str, version: str | None = None) -> ContractVersion:
        item = self.subject(subject)
        requested = version or item.latest
        for schema_version in item.versions:
            if schema_version.version == requested:
                return schema_version
        raise KeyError(f"unknown Avro contract version: {subject} {requested}")

    def mapper(self) -> AvroDatumMapper:
        """Return the explicit datum mapper for this registry."""

        return AvroDatumMapper(self)

    def validate(self) -> RegistryReport:
        """Validate metadata, canonical fingerprints, and full compatibility."""

        if self.compatibility != "FULL_TRANSITIVE":
            raise ContractValidationError(
                f"registry compatibility must be FULL_TRANSITIVE, got {self.compatibility!r}"
            )
        fingerprints: dict[str, str] = {}
        total_versions = 0
        subject_names = {subject.name for subject in self.subjects}
        for subject in self.subjects:
            versions = sorted(subject.versions, key=lambda item: _version_key(item.version))
            if not versions:
                raise ContractValidationError(f"subject {subject.name!r} has no versions")
            if subject.latest != versions[-1].version:
                raise ContractValidationError(
                    f"subject {subject.name!r} latest must be {versions[-1].version}, "
                    f"got {subject.latest}"
                )
            for index, current in enumerate(versions):
                total_versions += 1
                key = f"{subject.name}:{current.version}"
                actual = hashlib.sha256(current.schema.canonical_form.encode("utf-8")).hexdigest()
                if current.canonical_sha256 != actual:
                    raise ContractValidationError(
                        f"{key} canonical_sha256 does not match Avro parsing canonical form"
                    )
                if current.record_name != subject.record_name:
                    raise ContractValidationError(
                        f"{key} record_name does not match the registered subject"
                    )
                fingerprints[key] = actual
                if current.breaking:
                    if not current.supersedes or current.supersedes not in subject_names:
                        raise ContractValidationError(
                            f"{key} breaking revisions require supersedes metadata naming "
                            "an existing subject"
                        )
                    if index > 0:
                        raise ContractValidationError(
                            f"{key} breaking revisions must be published as a new subject"
                        )
                    if _version_key(current.version)[0] == 0:
                        raise ContractValidationError(
                            f"{key} breaking revisions must increment the major version"
                        )
                    continue
                for previous in versions[:index]:
                    _require_compatible(current, previous, subject.name)
        return RegistryReport(
            path=self.root,
            subjects=len(self.subjects),
            versions=total_versions,
            fingerprints=fingerprints,
        )

    def datum(self, subject: str, record: BaseModel | Mapping[str, Any]) -> dict[str, Any]:
        """Convert a domain record to a clean observable Avro datum.

        Latent truth fields are never copied, even when present on the source
        model. Values are converted to Avro logical Python values (``datetime``
        and ``Decimal``) without mutating the source model.
        """

        contract = self.contract(subject)
        source = record.model_dump(mode="python") if isinstance(record, BaseModel) else dict(record)
        fields = _record_fields(contract.schema)
        result: dict[str, Any] = {}
        for field in fields:
            name = field.name
            if name not in source:
                if field.has_default:
                    continue
                raise ValueError(f"{subject} record is missing required field {name!r}")
            result[name] = _normalize_value(source[name], field.type)
        validate_avro_datum(contract.schema, result)
        return result

    def encode(self, subject: str, record: BaseModel | Mapping[str, Any]) -> bytes:
        """Encode one observable datum using the latest subject schema."""

        import io

        datum = self.datum(subject, record)
        buffer = io.BytesIO()
        avro.io.DatumWriter(self.contract(subject).schema).write(
            datum, avro.io.BinaryEncoder(buffer)
        )
        return buffer.getvalue()

    def decode(self, subject: str, payload: bytes) -> dict[str, Any]:
        """Decode one payload using the latest subject schema."""

        import io

        decoded = avro.io.DatumReader(self.contract(subject).schema).read(
            avro.io.BinaryDecoder(io.BytesIO(payload))
        )
        return cast(dict[str, Any], decoded)


def _version_key(version: str) -> tuple[int, int, int]:
    match = _SEMVER.fullmatch(version)
    if match is None:
        raise ContractValidationError(f"invalid semantic schema version: {version!r}")
    return tuple(int(match.group(name)) for name in ("major", "minor", "patch"))  # type: ignore[return-value]


def _record_fields(schema: avro.schema.Schema) -> tuple[avro.schema.Field, ...]:
    if not isinstance(schema, avro.schema.RecordSchema):
        raise ContractValidationError(f"contract root must be an Avro record, got {schema.type}")
    return tuple(schema.fields)


def _require_compatible(
    current: ContractVersion,
    previous: ContractVersion,
    subject: str,
) -> None:
    checker = avro.compatibility.ReaderWriterCompatibilityChecker()  # type: ignore[no-untyped-call]
    # Both directions are required for FULL compatibility. The current schema
    # is the reader in the first check and the writer in the second.
    checks = (
        (current.schema, previous.schema, "backward"),
        (previous.schema, current.schema, "forward"),
    )
    for reader, writer, direction in checks:
        result = checker.get_compatibility(reader, writer)
        if result.compatibility != _COMPATIBLE:
            raise ContractValidationError(
                f"{subject} {current.version} is not FULL_TRANSITIVE compatible "
                f"with {previous.version} ({direction}): "
                f"{'; '.join(sorted(result.messages)) or 'schema resolution failed'}"
            )


def _normalize_value(value: Any, avro_schema: avro.schema.Schema) -> Any:
    if value is None:
        return None
    if isinstance(avro_schema, avro.schema.UnionSchema):
        for branch in avro_schema.schemas:
            if branch.type == "null":
                continue
            try:
                return _normalize_value(value, branch)
            except (TypeError, ValueError, InvalidOperation):
                continue
        raise ValueError(f"value {value!r} does not match Avro union")
    logical_type = getattr(avro_schema, "logical_type", None)
    if logical_type == "timestamp-micros":
        if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamp-micros values must be timezone-aware datetimes")
        return value.astimezone(UTC)
    if logical_type == "decimal":
        try:
            decimal = value if isinstance(value, Decimal) else Decimal(str(value))
        except (InvalidOperation, ValueError) as exc:
            raise ValueError(f"invalid decimal value: {value!r}") from exc
        if not decimal.is_finite():
            raise ValueError("decimal values must be finite")
        quantized = decimal.quantize(_DECIMAL_QUANTUM)
        if quantized != decimal:
            raise ValueError(f"monetary value has more than two decimal places: {value!r}")
        if len(quantized.as_tuple().digits) > _DECIMAL_PRECISION:
            raise ValueError(f"monetary value exceeds precision {_DECIMAL_PRECISION}")
        return quantized
    if avro_schema.type == "array":
        if not isinstance(value, Sequence) or isinstance(value, str | bytes):
            raise ValueError("Avro array values must be sequences")
        items_schema = cast(Any, avro_schema).items
        return [_normalize_value(item, items_schema) for item in value]
    if avro_schema.type == "map":
        if not isinstance(value, Mapping):
            raise ValueError("Avro map values must be mappings")
        values_schema = cast(Any, avro_schema).values
        return {str(key): _normalize_value(item, values_schema) for key, item in value.items()}
    return value


def validate_avro_datum(schema: avro.schema.Schema, datum: Mapping[str, Any]) -> None:
    """Validate one mapped datum with Apache Avro's implementation."""

    if not avro.io.validate(schema, datum):
        raise ValueError(
            f"datum does not conform to Avro schema {getattr(schema, 'fullname', schema)}"
        )


def _registry_root(path: str | Path | None) -> Path:
    if path is not None:
        candidate = Path(path)
    else:
        candidate = default_registry_path()
    if candidate.is_file():
        candidate = candidate.parent
    return candidate.resolve()


def default_registry_path() -> Path:
    """Return the bundled registry path in a checkout or installed wheel."""

    packaged = Path(str(files("fraudtwin").joinpath("contracts/avro")))
    if (packaged / "registry.yaml").is_file():
        return packaged
    module_path = Path(__file__).resolve()
    # Wheel builds preserve the project-level ``contracts/`` directory next
    # to the installed ``fraudtwin/`` package.  Source checkouts place it at
    # the repository root one level further up.
    for parent in (module_path.parents[2], module_path.parents[3]):
        candidate = parent / "contracts" / "avro"
        if (candidate / "registry.yaml").is_file():
            return candidate
    return module_path.parents[3] / "contracts" / "avro"


def load_contract_registry(path: str | Path | None = None) -> AvroContractRegistry:
    """Load registry metadata and parse every declared Avro schema."""

    root = _registry_root(path)
    registry_file = root / "registry.yaml"
    if not registry_file.is_file():
        raise ContractValidationError(f"registry metadata not found: {registry_file}")
    try:
        raw = yaml.safe_load(registry_file.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ContractValidationError(f"invalid registry YAML: {exc}") from exc
    if not isinstance(raw, Mapping):
        raise ContractValidationError("registry metadata must be a mapping")
    subjects_raw = raw.get("subjects")
    if raw.get("format") != "avro" or raw.get("registry_version") != "1":
        raise ContractValidationError("registry format must be avro version 1")
    if not isinstance(subjects_raw, list):
        raise ContractValidationError("registry subjects must be a list")
    subjects: list[ContractSubject] = []
    seen_subjects: set[str] = set()
    for raw_subject in subjects_raw:
        if not isinstance(raw_subject, Mapping):
            raise ContractValidationError("each registry subject must be a mapping")
        name = _required_text(raw_subject, "name")
        if name in seen_subjects:
            raise ContractValidationError(f"duplicate registry subject: {name}")
        seen_subjects.add(name)
        record_name = _required_text(raw_subject, "record_name")
        latest = _required_text(raw_subject, "latest")
        versions_raw = raw_subject.get("versions")
        if not isinstance(versions_raw, list):
            raise ContractValidationError(f"subject {name} versions must be a list")
        versions: list[ContractVersion] = []
        seen_versions: set[str] = set()
        for raw_version in versions_raw:
            if not isinstance(raw_version, Mapping):
                raise ContractValidationError(f"subject {name} version must be a mapping")
            version = _required_text(raw_version, "version")
            _version_key(version)
            if version in seen_versions:
                raise ContractValidationError(f"duplicate version {name}:{version}")
            seen_versions.add(version)
            relative_path = _required_text(raw_version, "path")
            schema_path = (root / relative_path).resolve()
            if root not in schema_path.parents or not schema_path.is_file():
                raise ContractValidationError(f"schema path is outside registry: {relative_path}")
            try:
                parsed = avro.schema.parse(schema_path.read_text(encoding="utf-8"))
            except Exception as exc:
                raise ContractValidationError(
                    f"invalid Avro schema {relative_path}: {exc}"
                ) from exc
            breaking = raw_version.get("breaking", False)
            if not isinstance(breaking, bool):
                raise ContractValidationError(f"{name}:{version} breaking must be boolean")
            supersedes = raw_version.get("supersedes")
            if supersedes is not None and (not isinstance(supersedes, str) or not supersedes):
                raise ContractValidationError(
                    f"{name}:{version} supersedes must be a non-empty string"
                )
            versions.append(
                ContractVersion(
                    subject=name,
                    version=version,
                    path=schema_path,
                    schema=parsed,
                    canonical_sha256=_required_text(raw_version, "canonical_sha256"),
                    breaking=breaking,
                    supersedes=supersedes,
                )
            )
        subjects.append(
            ContractSubject(
                name=name,
                record_name=record_name,
                latest=latest,
                versions=tuple(versions),
            )
        )
    return AvroContractRegistry(
        root=root,
        compatibility=str(raw.get("compatibility", "")),
        subjects=tuple(subjects),
    )


def _required_text(mapping: Mapping[str, Any], key: str) -> str:
    value = mapping.get(key)
    if not isinstance(value, str) or not value:
        raise ContractValidationError(f"registry field {key!r} must be a non-empty string")
    return value


def contract_registry(path: str | Path | None = None) -> AvroContractRegistry:
    """Load and validate a registry in one call."""

    registry = load_contract_registry(path)
    registry.validate()
    return registry


__all__ = [
    "AvroContractRegistry",
    "AvroDatumMapper",
    "ContractValidationError",
    "RegistryReport",
    "contract_registry",
    "default_registry_path",
    "load_contract_registry",
    "validate_avro_datum",
]
