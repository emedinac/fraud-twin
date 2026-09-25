import importlib
import inspect
import json
import pkgutil
import re
import sys
import tomllib
from pathlib import Path
from unittest.mock import Mock

import pytest
from docutils.parsers.rst import DirectiveError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
PROJECT_VERSION = str(tomllib.loads(Path("pyproject.toml").read_text())["project"]["version"])
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


def test_public_api_inventory_reports_invalid_package(tmp_path: Path) -> None:
    with pytest.raises(ModuleNotFoundError, match="fraudtwin.not_a_module"):
        api_inventory.write_api_stubs(tmp_path / "generated", "fraudtwin.not_a_module")


def test_top_level_function_exports_are_typed_for_ide_completion() -> None:
    import fraudtwin

    for name in fraudtwin.__all__:
        value = getattr(fraudtwin, name)
        if not inspect.isfunction(value):
            continue
        signature = inspect.signature(value)
        assert signature.return_annotation is not inspect.Signature.empty, name
        assert all(
            parameter.annotation is not inspect.Signature.empty
            for parameter in signature.parameters.values()
            if parameter.name != "self"
        ), name


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

    module_page = (output_dir / "fraudtwin.ml.rst").read_text(encoding="utf-8")
    assert ".. _api-module-fraudtwin-ml:" in module_page
    assert "**Status:** Stable" in module_page
    assert ".. autosummary::" in module_page
    assert "fraudtwin.ml.build_point_in_time_dataset" in module_page
    assert "Detailed API" in module_page


def test_user_manual_covers_the_first_ten_documentation_gaps() -> None:
    required_pages = (
        "installation.md",
        "integrations.md",
        "ml-evaluation.md",
        "data-contracts.rst",
        "configuration.md",
        "configuration-reference.rst",
        "api/cookbook.rst",
        "cli.rst",
        "production-serving.md",
        "drift-and-shift.md",
        "kafka-reliability.md",
    )
    for relative in required_pages:
        assert (Path("docs") / relative).is_file(), relative

    installation = Path("docs/installation.md").read_text(encoding="utf-8")
    for extra in tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))["project"][
        "optional-dependencies"
    ]:
        assert f"`{extra}`" in installation or f"-E {extra}" in installation, extra

    contracts = Path("docs/data-contracts.rst").read_text(encoding="utf-8")
    for artifact in (
        "payments/payments.parquet",
        "payments/payment_events.parquet",
        "ledger/ledger_entries.parquet",
        "ml/dataset.parquet",
        "contracts/avro/registry.yaml",
        "kafka-chaos/manifest.json",
    ):
        assert artifact in contracts, artifact

    for guide, marker in (
        ("docs/api/cookbook.rst", "load_data()"),
        ("docs/cli.rst", "Command map"),
        ("docs/ml-evaluation.md", "PR-AUC"),
        ("docs/drift-and-shift.md", "Jensen–Shannon"),
        ("docs/kafka-reliability.md", "at-least-once"),
        ("docs/production-serving.md", "authentication"),
    ):
        assert marker in Path(guide).read_text(encoding="utf-8"), marker


def test_non_tutorial_guides_have_reader_contracts_and_resolved_navigation() -> None:
    """Keep the public guide layer level-aware and connected."""

    guides = [
        *Path("docs").glob("*.md"),
        *Path("docs/audiences").glob("*.md"),
        *Path("docs/levels").glob("*.md"),
    ]
    excluded = {Path("docs/tutorials.md")}
    required = (
        "**Level:**",
        "**You will:**",
        "**Before you start:**",
        "**Services:**",
        "## Next",
        "## Related",
    )
    markdown_link = re.compile(r"\]\(([^)]+)\)")
    for guide in sorted(path for path in guides if path not in excluded):
        text = guide.read_text(encoding="utf-8")
        for marker in required:
            assert marker in text, f"{guide} is missing {marker}"
        if guide.name != "compatibility.md":
            public_prose = re.sub(r"```.*?```", "", text, flags=re.DOTALL)
            public_prose = re.sub(r"`[^`]+`", "", public_prose)
            assert not re.search(r"\b(?:M\d+|milestone)\b", public_prose, re.IGNORECASE), guide
        for raw_target in markdown_link.findall(text):
            target = raw_target.split("#", 1)[0].strip()
            if not target or target.startswith(("http://", "https://", "mailto:")):
                continue
            assert (guide.parent / target).resolve().exists(), (guide, raw_target)


def test_cli_command_map_covers_registered_commands() -> None:
    from fraudtwin import cli

    page = Path("docs/cli.rst").read_text(encoding="utf-8")
    root_commands = {
        command.name or command.callback.__name__.replace("_command", "")
        for command in cli.app.registered_commands
        if command.callback is not None
    }
    group_commands = {
        f"{group.name} {command.name}"
        for group in cli.app.registered_groups
        for command in group.typer_instance.registered_commands
        if group.name and command.name
    }
    for command in (*root_commands, *group_commands):
        assert command in page, command


def test_python_api_cookbook_examples_compile() -> None:
    source = Path("docs/api/cookbook.rst").read_text(encoding="utf-8").splitlines()
    blocks: list[str] = []
    collecting = False
    current: list[str] = []
    for line in source + [""]:
        if line.strip() == ".. code-block:: python":
            collecting = True
            current = []
            continue
        if collecting and (line.startswith("   ") or not line.strip()):
            current.append(line[3:] if line.startswith("   ") else "")
            continue
        if collecting:
            blocks.append("\n".join(current).rstrip())
            collecting = False
    assert blocks
    for index, block in enumerate(blocks):
        compile(block, f"docs/api/cookbook.rst:{index}", "exec")


def test_all_tutorials_are_valid_notebook_json() -> None:
    notebooks = sorted(Path("docs/tutorials").glob("*.ipynb"))

    assert len(notebooks) == 31
    assert not [path.name for path in notebooks if re.match(r"^\d", path.name)]
    for notebook in notebooks:
        document = json.loads(notebook.read_text(encoding="utf-8"))
        assert document["nbformat"] >= 4
        assert document["cells"]
        assert document["metadata"]["kernelspec"]["name"] == "python3"


def test_tutorials_have_marked_offline_code_cells() -> None:
    """Prevent instructional notebooks from regressing to empty scaffolds."""

    advanced_slugs = {
        "calibration-counterfactuals",
        "campaign-graph-investigation",
        "ml-shift-backtesting",
        "scale-reconciliation-reproducibility",
    }
    for notebook in sorted(Path("docs/tutorials").glob("*.ipynb")):
        document = json.loads(notebook.read_text(encoding="utf-8"))
        tutorial_meta = document["metadata"].get("fraudtwin", {})
        tutorial_slug = tutorial_meta.get("tutorial_slug")
        if tutorial_slug is not None:
            assert tutorial_slug in advanced_slugs
        minimum_code_cells = tutorial_meta.get("minimum_code_cells", 10)
        minimum_offline_code_cells = tutorial_meta.get("minimum_offline_code_cells", 10)
        assert isinstance(minimum_code_cells, int) and minimum_code_cells > 0
        assert isinstance(minimum_offline_code_cells, int) and minimum_offline_code_cells > 0
        if tutorial_slug is not None:
            assert tutorial_meta.get("expected_source_size")
            assert isinstance(tutorial_meta.get("required_extras"), list)
            assert tutorial_meta.get("artifacts")
            assert tutorial_meta.get("offline_supported") is True
        code_cells = [cell for cell in document["cells"] if cell.get("cell_type") == "code"]
        assert code_cells, notebook.name
        for cell in code_cells:
            marker = cell.get("metadata", {}).get("fraudtwin", {}).get("offline")
            assert isinstance(marker, bool), f"missing offline marker: {notebook.name}"
            if marker is True:
                compile("".join(cell.get("source", [])), f"{notebook.name}:cell", "exec")
        offline_cells = [
            cell for cell in code_cells if cell["metadata"]["fraudtwin"]["offline"] is True
        ]
        assert len(code_cells) >= minimum_code_cells, notebook.name
        assert len(offline_cells) >= minimum_offline_code_cells, notebook.name


def test_integration_tutorials_have_guarded_client_smoke_cells() -> None:
    requirements = {
        "neo4j-graph-fraud.ipynb": ("neo4j", ("verify_connectivity", "RETURN 1", "MERGE")),
        "avro-kafka-stream.ipynb": (
            "confluent-kafka",
            ("list_topics", "get_subjects", "publish"),
        ),
        "kafka-outage-recovery.ipynb": ("confluent-kafka", ("list_topics", "publish")),
        "schema-evolution-compatibility.ipynb": (
            "confluent-kafka",
            ("get_subjects", "get_latest_version"),
        ),
        "postgres-persistence-reconciliation.ipynb": (
            "psycopg",
            ("migrate_database", "persist_run", "database_status"),
        ),
        "iceberg-time-travel-observability.ipynb": (
            "pyiceberg",
            ("load_catalog", "list_namespaces", "prometheus", "api/v1/query", "api/health"),
        ),
        "lakehouse-observability.ipynb": (
            "pyiceberg",
            ("load_catalog", "list_namespaces", "prometheus"),
        ),
        "operational-lakehouse-observability.ipynb": (
            "pyiceberg",
            ("load_catalog", "list_namespaces", "prometheus"),
        ),
        "mlflow-model-promotion.ipynb": ("mlflow", ("start_run", "log_metric", "get_run")),
        "train-and-track-fraud-model.ipynb": ("fastapi", ("TestClient", "/health", "/score")),
        "pyg-graph-model.ipynb": ("torch-geometric", ("to_pyg", "num_nodes")),
    }
    startup_markers = {
        "neo4j-graph-fraud.ipynb": "docker run --name fraudtwin-neo4j",
        "avro-kafka-stream.ipynb": "docker compose --profile streaming up -d",
        "kafka-outage-recovery.ipynb": '"docker", "compose", "--profile", "streaming", "up"',
        "schema-evolution-compatibility.ipynb": "docker compose --profile streaming up -d",
        "postgres-persistence-reconciliation.ipynb": "docker compose --profile integration up -d",
        "iceberg-time-travel-observability.ipynb": "docker compose --profile lakehouse up -d",
        "lakehouse-observability.ipynb": "docker compose --profile lakehouse up -d",
        "operational-lakehouse-observability.ipynb": "docker compose --profile lakehouse up -d",
        "mlflow-model-promotion.ipynb": "mlflow server --host 127.0.0.1",
        "train-and-track-fraud-model.ipynb": "uvicorn examples.model_service.app:app",
    }
    for filename, (package, operations) in requirements.items():
        document = json.loads((Path("docs/tutorials") / filename).read_text(encoding="utf-8"))
        service_cells = [
            cell
            for cell in document["cells"]
            if cell.get("cell_type") == "code"
            and cell.get("metadata", {}).get("fraudtwin", {}).get("offline") is False
        ]
        assert service_cells, filename
        source = "\n".join("".join(cell.get("source", [])) for cell in service_cells)
        assert re.search(
            rf"^[!%]pip install .*{re.escape(package)}.*$", source, re.MULTILINE
        ), filename
        assert "--disable-pip-version-check" not in source
        assert " -q" not in source
        for operation in operations:
            assert operation in source, (filename, operation)
        assert "except Exception" in source, filename
        assert "offline_fallback" in source, filename
        if filename in startup_markers:
            notebook_source = "\n".join(
                "".join(cell.get("source", [])) for cell in document["cells"]
            )
            marker = startup_markers[filename]
            accepted = (
                marker,
                marker.replace(
                    " --profile lakehouse up",
                    " --profile lakehouse --profile observability up",
                ),
            )
            normalized_source = re.sub(r"\s+", " ", notebook_source)
            assert any(
                item in notebook_source or re.sub(r"\s+", " ", item) in normalized_source
                for item in accepted
            ), filename
        for cell in service_cells:
            metadata = cell["metadata"]["fraudtwin"]
            assert metadata.get("integration"), filename
            assert metadata.get("requires_service") in {True, False}, filename


def test_every_tutorial_belongs_to_exactly_one_category() -> None:
    notebooks = {path.name for path in Path("docs/tutorials").glob("*.ipynb")}
    category_pages = (
        "visualization.md",
        "getting-started.md",
        "core-workflows.md",
        "production-ml.md",
        "graph-analytics.md",
        "streaming-reliability.md",
        "operations.md",
        "advanced-experiments.md",
    )
    memberships = [
        notebook
        for page in category_pages
        for line in (Path("docs/tutorials") / page).read_text().splitlines()
        for notebook in re.findall(r"(?:^|\]\()([A-Za-z0-9][^\s)]+\.ipynb)", line)
    ]
    assert set(memberships) == notebooks
    assert len(memberships) == len(notebooks)


def test_audience_pages_and_documentation_images_are_discoverable() -> None:
    audience_pages = sorted((Path("docs") / "audiences").glob("*.md"))
    assert {page.stem for page in audience_pages} == {
        "new-users",
        "data-ml",
        "fraud-risk",
        "engineering-mlops",
        "researchers-governance",
    }
    index = Path("docs/audiences.md").read_text(encoding="utf-8")
    for page in audience_pages:
        assert page.stem in index
        content = page.read_text(encoding="utf-8")
        assert "Start with" in content
        assert "offline" in content.lower()

    images = sorted((Path("docs/_static/images")).glob("*.svg"))
    assert len(images) >= 10
    for image in images:
        assert image.stat().st_size < 100_000
        content = image.read_text(encoding="utf-8")
        assert "<title>" in content
        assert 'font-size="13"' in content or 'font-size="19"' in content

    for page in (
        Path("docs/tutorials/visualization.md"),
        Path("docs/drift-and-shift.md"),
        Path("docs/kafka-reliability.md"),
        Path("docs/ml-evaluation.md"),
        Path("docs/integrations.md"),
        Path("docs/graph-and-benchmarks.md"),
    ):
        for reference in re.findall(r"_static/images/([^\s)`]+)", page.read_text(encoding="utf-8")):
            assert (Path("docs/_static/images") / reference).is_file(), reference


def test_representative_offline_tutorial_cells_execute() -> None:
    """Keep the service-optional tutorials runnable without Docker services."""
    tutorial_names = tuple(path.name for path in sorted(Path("docs/tutorials").glob("*.ipynb")))
    for name in tutorial_names:
        document = json.loads((Path("docs/tutorials") / name).read_text(encoding="utf-8"))
        namespace = {"__name__": "__tutorial__", "display": lambda *args, **kwargs: None}
        for index, cell in enumerate(document["cells"]):
            if (
                cell.get("cell_type") == "code"
                and cell.get("metadata", {}).get("fraudtwin", {}).get("offline") is True
            ):
                code = "".join(cell.get("source", []))
                compile(code, f"{name}:{index}", "exec")
                exec(code, namespace)


def test_versioned_site_preparation_creates_latest_and_switcher(tmp_path: Path) -> None:
    (tmp_path / "main").mkdir()
    (tmp_path / "main" / "index.html").write_text("latest", encoding="utf-8")
    (tmp_path / f"v{PROJECT_VERSION}").mkdir()
    (tmp_path / f"v{PROJECT_VERSION}" / "index.html").write_text("release", encoding="utf-8")

    prepare_site(tmp_path)

    assert (tmp_path / "latest" / "index.html").read_text(encoding="utf-8") == "latest"
    switcher = json.loads((tmp_path / "version-switcher.json").read_text(encoding="utf-8"))
    assert [item["version"] for item in switcher] == ["latest", f"v{PROJECT_VERSION}"]
    assert "url=latest/" in (tmp_path / "index.html").read_text(encoding="utf-8")
    api_redirect = (tmp_path / "api.html").read_text(encoding="utf-8")
    assert "url=latest/api.html" in api_redirect
    assert "latest/api.html" in api_redirect
    alias = (tmp_path / "latest/tutorials/28-calibration-reference-data.html").read_text(
        encoding="utf-8"
    )
    assert "calibration-and-counterfactuals.html" in alias
