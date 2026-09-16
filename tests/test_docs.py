import importlib
import json
import pkgutil
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest
from docutils.parsers.rst import DirectiveError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
ConfigModelDirective = importlib.import_module("docs._ext.config_schema").ConfigModelDirective
api_inventory = importlib.import_module("docs._ext.api_inventory")
prepare_site = importlib.import_module("docs.prepare_site").prepare_site


def _directive(model_path: str) -> ConfigModelDirective:
    return ConfigModelDirective(
        "config-model",
        [model_path],
        {},
        [],
        0,
        0,
        "",
        Mock(),
        Mock(),
    )


def test_configuration_directive_renders_root_and_nested_tables() -> None:
    result = _directive("fraudtwin.config:SimulationRunConfig").run()

    assert result[0].astext() == "SimulationRunConfig"
    tables = [node for node in result if node.tagname == "table"]
    assert len(tables) > 1
    assert "simulation" in tables[0].astext()
    assert "Clock, seed, duration, and execution speed." in tables[0].astext()
    assert "SimulationConfig" in tables[0].astext()


def test_configuration_directive_reports_invalid_model_path() -> None:
    with pytest.raises(DirectiveError):
        _directive("fraudtwin.config:DoesNotExist").run()


def test_public_api_inventory_covers_every_export(tmp_path: Path) -> None:
    import fraudtwin

    exports = api_inventory._package_exports("fraudtwin")
    names = [name for category in exports.values() for name in category]
    assert set(names) == {f"fraudtwin.{name}" for name in fraudtwin.__all__}
    assert all(hasattr(fraudtwin, name) for name in fraudtwin.__all__)

    output_dir = tmp_path / "generated"
    api_inventory.write_api_stubs(output_dir, "fraudtwin")
    assert {path.stem for path in output_dir.glob("*.rst")} == {
        f"fraudtwin.{name}" for name in fraudtwin.__all__
    }


def test_public_module_inventory_writes_every_module_page(tmp_path: Path) -> None:
    import fraudtwin

    discovered = set()
    for module_info in pkgutil.walk_packages(fraudtwin.__path__, "fraudtwin."):
        module = importlib.import_module(module_info.name)
        if hasattr(module, "__all__"):
            discovered.add(module_info.name)
    assert discovered <= set(api_inventory.PUBLIC_MODULES)

    for module_name in api_inventory.PUBLIC_MODULES:
        module = importlib.import_module(module_name)
        assert getattr(module, "__all__", None) or module_name == "fraudtwin.config"

    output_dir = tmp_path / "modules"
    api_inventory.write_module_stubs(output_dir)
    assert {path.stem for path in output_dir.glob("*.rst")} == set(api_inventory.PUBLIC_MODULES)


def test_all_tutorials_are_valid_notebook_json() -> None:
    notebooks = sorted(Path("docs/tutorials").glob("*.ipynb"))

    assert len(notebooks) == 8
    for notebook in notebooks:
        document = json.loads(notebook.read_text(encoding="utf-8"))
        assert document["nbformat"] >= 4
        assert document["cells"]
        assert document["metadata"]["kernelspec"]["name"] == "python3"


def test_versioned_site_preparation_creates_latest_and_switcher(tmp_path: Path) -> None:
    (tmp_path / "main").mkdir()
    (tmp_path / "main" / "index.html").write_text("latest", encoding="utf-8")
    (tmp_path / "v0.32.0").mkdir()
    (tmp_path / "v0.32.0" / "index.html").write_text("release", encoding="utf-8")

    prepare_site(tmp_path)

    assert (tmp_path / "latest" / "index.html").read_text(encoding="utf-8") == "latest"
    switcher = json.loads((tmp_path / "version-switcher.json").read_text(encoding="utf-8"))
    assert [item["version"] for item in switcher] == ["latest", "v0.32.0"]
    assert "url=latest/" in (tmp_path / "index.html").read_text(encoding="utf-8")
