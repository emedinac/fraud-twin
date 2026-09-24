"""Public extension SDK contracts and deterministic registry behavior."""

from dataclasses import dataclass
from pathlib import Path

import pytest

from fraudtwin.config import load_config
from fraudtwin.extensions import ExtensionMetadata, ExtensionRegistry
from fraudtwin.manifest import create_manifest


@dataclass
class _Extension:
    metadata: ExtensionMetadata


def test_registry_orders_manifest_metadata() -> None:
    registry = ExtensionRegistry(
        (
            _Extension(ExtensionMetadata("z-last", "1.0.0")),
            _Extension(ExtensionMetadata("a-first", "2.0.0")),
        )
    )
    assert [item["extension_id"] for item in registry.manifest()] == ["a-first", "z-last"]


def test_registry_rejects_duplicate_ids() -> None:
    extension = _Extension(ExtensionMetadata("same", "1.0.0"))
    registry = ExtensionRegistry((extension,))
    with pytest.raises(ValueError, match="duplicate"):
        registry.register(_Extension(ExtensionMetadata("same", "1.0.0")))


def test_registry_rejects_missing_metadata() -> None:
    with pytest.raises(TypeError, match="ExtensionMetadata"):
        ExtensionRegistry((object(),))  # type: ignore[arg-type]


def test_selected_extension_provenance_is_manifested(monkeypatch: pytest.MonkeyPatch) -> None:
    import fraudtwin.extensions as extension_module

    extension = _Extension(ExtensionMetadata("example.fraud", "1.2.3"))
    monkeypatch.setattr(
        extension_module, "discover_extensions", lambda: ExtensionRegistry((extension,))
    )
    values = load_config(Path("configs/minimal.yaml")).model_dump(mode="python")
    values["extensions"] = {"enabled": True, "selected": ("example.fraud",)}
    from fraudtwin.config import SimulationRunConfig

    config = SimulationRunConfig.model_validate(values)
    manifest = create_manifest(config)
    assert manifest.extensions[0]["extension_id"] == "example.fraud"
    assert manifest.extensions[0]["extension_version"] == "1.2.3"
    assert manifest.extensions[0]["order"] == "1"
    assert len(manifest.extensions[0]["configuration_hash"]) == 64


def test_disabled_extensions_do_not_discover_entry_points(monkeypatch: pytest.MonkeyPatch) -> None:
    import fraudtwin.extensions as extension_module

    def fail_discovery():
        raise AssertionError("extension discovery should be disabled")

    monkeypatch.setattr(extension_module, "discover_extensions", fail_discovery)
    config = load_config(Path("configs/minimal.yaml"))
    manifest = create_manifest(config)
    assert manifest.extensions == []


def test_enabled_extension_discovery_errors_are_not_suppressed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import fraudtwin.extensions as extension_module
    from fraudtwin.config import SimulationRunConfig

    def fail_discovery() -> object:
        raise TypeError("invalid extension")

    monkeypatch.setattr(
        extension_module,
        "discover_extensions",
        fail_discovery,
    )
    values = load_config(Path("configs/minimal.yaml")).model_dump(mode="python")
    values["extensions"] = {"enabled": True, "selected": ("example.fraud",)}
    config = SimulationRunConfig.model_validate(values)
    with pytest.raises(TypeError, match="invalid extension"):
        create_manifest(config)


def test_enabled_extension_import_errors_are_not_suppressed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import fraudtwin.extensions as extension_module
    from fraudtwin.config import SimulationRunConfig

    def fail_discovery() -> object:
        raise ImportError("broken extension")

    monkeypatch.setattr(extension_module, "discover_extensions", fail_discovery)
    values = load_config(Path("configs/minimal.yaml")).model_dump(mode="python")
    values["extensions"] = {"enabled": True, "selected": ("example.fraud",)}
    config = SimulationRunConfig.model_validate(values)
    with pytest.raises(ImportError, match="broken extension"):
        create_manifest(config)
