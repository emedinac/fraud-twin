"""Stable extension contracts for FraudTwin.

Extensions are deliberately small ports.  They describe the boundary between
the deterministic simulator and user-owned behavior; they do not provide a
second configuration system or a plugin runtime.  Applications may register
implementations explicitly, while package distributions can expose the same
implementations through the ``fraudtwin.extensions`` entry-point group.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from importlib.metadata import EntryPoint, entry_points
from typing import Any, Protocol, cast, runtime_checkable


@dataclass(frozen=True)
class ExtensionMetadata:
    """Identity recorded in manifests for one installed extension."""

    extension_id: str
    extension_version: str
    distribution: str = "local"
    distribution_version: str = "local"


@runtime_checkable
class FraudScenario(Protocol):
    """Port for a deterministic fraud-scenario implementation."""

    metadata: ExtensionMetadata

    def generate(self, context: Mapping[str, Any], *, seed: int) -> Iterable[Mapping[str, Any]]:
        """Yield oracle records or mutations for a bounded generation context."""


@runtime_checkable
class PaymentRail(Protocol):
    """Port for a payment rail that preserves FraudTwin lifecycle semantics."""

    metadata: ExtensionMetadata

    def lifecycle(self, payment: Mapping[str, Any], *, seed: int) -> Iterable[Mapping[str, Any]]:
        """Yield lifecycle events for one payment."""


@runtime_checkable
class BehaviorModel(Protocol):
    """Port for a deterministic customer-behavior model."""

    metadata: ExtensionMetadata

    def profile(self, entity: Mapping[str, Any], *, seed: int) -> Mapping[str, Any]:
        """Return one behavior profile without mutating the source entity."""


@runtime_checkable
class DataFaultInjector(Protocol):
    """Port for deterministic observable-data mutations."""

    metadata: ExtensionMetadata

    def apply(self, record: Mapping[str, Any], *, seed: int) -> Mapping[str, Any]:
        """Return the mutated record and preserve an audit-friendly identity."""


@runtime_checkable
class OutputSink(Protocol):
    """Port for a chunk-aware output adapter."""

    metadata: ExtensionMetadata

    def write(self, records: Iterable[Mapping[str, Any]], *, run_id: str) -> Mapping[str, Any]:
        """Persist a bounded batch and return checkpoint metadata."""


Extension = FraudScenario | PaymentRail | BehaviorModel | DataFaultInjector | OutputSink


class ExtensionRegistry:
    """Explicit, deterministic registry of extension implementations."""

    def __init__(self, extensions: Iterable[Extension] = ()) -> None:
        self._extensions: dict[str, Extension] = {}
        for extension in extensions:
            self.register(extension)

    def register(self, extension: Extension) -> None:
        metadata = getattr(extension, "metadata", None)
        if not isinstance(metadata, ExtensionMetadata):
            raise TypeError("extensions must expose ExtensionMetadata as metadata")
        if not metadata.extension_id or metadata.extension_id in self._extensions:
            raise ValueError(f"duplicate or empty extension id: {metadata.extension_id!r}")
        self._extensions[metadata.extension_id] = extension

    def get(self, extension_id: str) -> Extension:
        try:
            return self._extensions[extension_id]
        except KeyError as exc:
            raise KeyError(f"unknown FraudTwin extension: {extension_id}") from exc

    def metadata(self) -> tuple[ExtensionMetadata, ...]:
        return tuple(
            sorted(
                (extension.metadata for extension in self._extensions.values()),
                key=lambda item: item.extension_id,
            )
        )

    def manifest(self) -> list[dict[str, str]]:
        return [
            {
                "extension_id": item.extension_id,
                "extension_version": item.extension_version,
                "distribution": item.distribution,
                "distribution_version": item.distribution_version,
            }
            for item in self.metadata()
        ]


def discover_extensions(*, group: str = "fraudtwin.extensions") -> ExtensionRegistry:
    """Load installed entry points in stable name order.

    Entry points are loaded from the local environment only.  A package must
    therefore be installed explicitly; configuration never executes an
    arbitrary import path or downloads code.
    """

    discovered = entry_points()
    selected = discovered.select(group=group)
    registry = ExtensionRegistry()
    for entry_point in sorted(selected, key=lambda item: item.name):
        extension = _load_entry_point(entry_point)
        registry.register(extension)
    return registry


def _load_entry_point(entry_point: EntryPoint) -> Extension:
    extension = entry_point.load()
    if callable(extension) and not hasattr(extension, "metadata"):
        extension = extension()
    metadata = getattr(extension, "metadata", None)
    if not isinstance(metadata, ExtensionMetadata):
        raise TypeError(f"entry point {entry_point.name!r} did not provide ExtensionMetadata")
    return cast(Extension, extension)


__all__ = [
    "BehaviorModel",
    "DataFaultInjector",
    "Extension",
    "ExtensionMetadata",
    "ExtensionRegistry",
    "FraudScenario",
    "OutputSink",
    "PaymentRail",
    "discover_extensions",
]
