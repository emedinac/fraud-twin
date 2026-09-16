"""Small, dependency-free schema evolution helpers for the M8 serializer."""

from collections.abc import Mapping
from typing import Any

from fraudtwin.config import SchemaChangeConfig
from fraudtwin.reproducibility import sha256_json

SCHEMA_CHANGE_OPERATIONS = frozenset(
    {
        "add_optional_field",
        "rename",
        "remove_field",
        "nullability",
        "enum",
        "type",
    }
)


class SchemaRegistry:
    """Minimal in-memory registry for serialized event schemas.

    The registry intentionally stores field declarations rather than domain
    models. This keeps schema migration concerns at the serialization boundary
    and makes compatibility checks deterministic and easy to use in CI.
    """

    def __init__(self) -> None:
        self._schemas: dict[str, dict[str, object]] = {}

    def register(self, version: str, fields: Mapping[str, object]) -> str:
        """Register a version and return its deterministic fingerprint."""

        schema = {str(name): value for name, value in fields.items()}
        self._schemas[version] = schema
        return self.fingerprint(version)

    def fingerprint(self, version: str) -> str:
        """Return a stable fingerprint for one registered schema."""

        return sha256_json({"version": version, "fields": self._schemas[version]})

    def compatibility(self, old_version: str, new_version: str) -> str:
        """Classify changes between two registered field declarations."""

        old = self._schemas[old_version]
        new = self._schemas[new_version]
        removed = set(old) - set(new)
        added = set(new) - set(old)
        changed = {name for name in set(old) & set(new) if old[name] != new[name]}
        if removed or changed:
            return "BREAKING"
        if added:
            return "FULLY_COMPATIBLE"
        return "FULLY_COMPATIBLE"


def schema_fingerprint(fields: Mapping[str, object]) -> str:
    """Fingerprint a serialized schema declaration without registering it."""

    return sha256_json({"fields": dict(fields)})


def _operation(change: SchemaChangeConfig) -> tuple[str, Any] | None:
    return next(iter(change.change.items()), None)


def apply_schema_change(row: Mapping[str, Any], change: SchemaChangeConfig) -> dict[str, Any]:
    """Apply one serialized-row schema change without mutating the source row."""

    result = dict(row)
    operation = _operation(change)
    if operation is None:
        return result
    name, specification = operation
    if name == "add_optional_field":
        fields = specification if isinstance(specification, Mapping) else {str(specification): None}
        for field in fields:
            result.setdefault(str(field), None)
    elif name == "rename":
        if not isinstance(specification, Mapping):
            raise ValueError("schema rename must map an old field name to a new field name")
        for old, new in specification.items():
            if old in result:
                result[str(new)] = result.pop(old)
    elif name == "remove_field":
        remove_fields: Any = (
            specification if isinstance(specification, list | tuple | set) else (specification,)
        )
        for field in remove_fields:
            result.pop(str(field), None)
    elif name == "type":
        if not isinstance(specification, Mapping):
            raise ValueError("schema type must map field names to declarations")
        for field, target_type in specification.items():
            field_name = str(field)
            if field_name not in result:
                continue
            normalized = str(target_type).lower()
            if normalized in {"string", "str", "utf8"}:
                result[field_name] = str(result[field_name])
            elif normalized in {"float", "double", "decimal", "number"}:
                result[field_name] = float(result[field_name])
            elif normalized in {"integer", "int", "int64"}:
                result[field_name] = int(result[field_name])
            elif normalized in {"boolean", "bool"}:
                result[field_name] = bool(result[field_name])
    elif name in {"nullability", "enum"}:
        # These operations primarily change the declared contract. Existing
        # values stay stable, while newly nullable fields are materialized as
        # explicit nulls so the serialized mutation is inspectable.
        if not isinstance(specification, Mapping):
            raise ValueError(f"schema {name} must map field names to declarations")
        if name == "nullability":
            for field, nullable in specification.items():
                field_name = str(field)
                if bool(nullable) and field_name not in result:
                    result[field_name] = None
    else:  # Defensive guard if a caller bypasses Pydantic validation.
        raise ValueError(f"unsupported schema change operation: {name}")
    return result


def infer_compatibility(change: SchemaChangeConfig) -> str:
    """Infer compatibility for a change unless the configuration overrides it."""

    if change.compatibility is not None:
        return change.compatibility
    operation = _operation(change)
    if operation is None:
        return "FULLY_COMPATIBLE"
    name, specification = operation
    if name == "add_optional_field":
        return "FULLY_COMPATIBLE"
    if name in {"rename", "remove_field"}:
        return "BREAKING"
    if name == "nullability":
        values = specification.values() if isinstance(specification, Mapping) else ()
        return "FULLY_COMPATIBLE" if all(bool(value) for value in values) else "BREAKING"
    if name == "enum":
        return "FORWARD_COMPATIBLE"
    if name == "type":
        return "BACKWARD_COMPATIBLE"
    return "BREAKING"


def schema_change_metadata(change: SchemaChangeConfig) -> dict[str, object]:
    """Return deterministic registry metadata for one scheduled change."""

    operation = _operation(change)
    return {
        "at": change.at.isoformat(),
        "event": change.event,
        "version": change.version,
        "change": change.change,
        "compatibility": infer_compatibility(change),
        "effective_version": change.version,
        "schema_fingerprint": schema_fingerprint(
            {
                "version": change.version,
                "operation": operation,
            }
        ),
        "migration_metadata": {
            "operation": operation[0] if operation else "version_only",
            "specification": operation[1] if operation else None,
        },
    }
