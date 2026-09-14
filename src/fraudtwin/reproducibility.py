"""Small helpers for stable, content-addressed research artifacts."""

import hashlib
import json
from datetime import UTC, datetime
from typing import Any


def canonical_json(value: Any) -> str:
    """Serialize JSON-compatible values with one stable representation."""

    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def sha256_json(value: Any) -> str:
    """Return the SHA-256 fingerprint of a canonical JSON value."""

    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def as_utc(value: datetime, *, error_message: str) -> datetime:
    """Normalize a timezone-aware timestamp for deterministic comparisons."""

    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(error_message)
    return value.astimezone(UTC)
