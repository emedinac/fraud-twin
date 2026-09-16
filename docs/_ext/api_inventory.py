"""Generate an exhaustive public API inventory from a package ``__all__``."""

from __future__ import annotations

import importlib
import inspect
from pathlib import Path
from typing import Any

from docutils.parsers.rst import Directive

PUBLIC_MODULES = tuple(
    sorted(
        (
            "fraudtwin.benchmark",
            "fraudtwin.calibration",
            "fraudtwin.camouflage",
            "fraudtwin.campaign_dynamics",
            "fraudtwin.config",
            "fraudtwin.contracts",
            "fraudtwin.contracts.registry",
            "fraudtwin.counterfactual",
            "fraudtwin.difficulty",
            "fraudtwin.domain.campaign_dynamics",
            "fraudtwin.domain.cases",
            "fraudtwin.domain.graph",
            "fraudtwin.domain.labels",
            "fraudtwin.domain",
            "fraudtwin.generation",
            "fraudtwin.graph",
            "fraudtwin.kafka",
            "fraudtwin.label_observation",
            "fraudtwin.lakehouse",
            "fraudtwin.ml",
            "fraudtwin.ml.backtest",
            "fraudtwin.ml.baseline",
            "fraudtwin.ml.dataset",
            "fraudtwin.observability",
            "fraudtwin.postgres",
            "fraudtwin.quality_benchmark",
            "fraudtwin.replay",
            "fraudtwin.scale",
            "fraudtwin.simulation.cases",
            "fraudtwin.simulation.graph_fraud",
            "fraudtwin.simulation.graph_planner",
            "fraudtwin.simulation",
            "fraudtwin.storage",
        )
    )
)

MODULE_PURPOSES = {
    "fraudtwin.benchmark": "Benchmark packs and reproducible suite execution.",
    "fraudtwin.calibration": "Reference-data calibration and fidelity reports.",
    "fraudtwin.camouflage": "Fraud camouflage transformations.",
    "fraudtwin.campaign_dynamics": "Dynamic campaign evolution and registration hooks.",
    "fraudtwin.config": "Typed configuration models, loading, and validation.",
    "fraudtwin.contracts": "Versioned data contracts and schema access.",
    "fraudtwin.contracts.registry": "Versioned Avro contract validation and mapping.",
    "fraudtwin.counterfactual": "Counterfactual scenario generation and resolution.",
    "fraudtwin.difficulty": "Scenario difficulty plans and resolvers.",
    "fraudtwin.domain": "Stable domain records for entities, payments, fraud, and labels.",
    "fraudtwin.domain.campaign_dynamics": "Domain types for evolving fraud campaigns.",
    "fraudtwin.domain.cases": "Reusable domain case and scenario records.",
    "fraudtwin.domain.graph": "Domain records for graph nodes, edges, and evidence.",
    "fraudtwin.domain.labels": "Domain records for label maturity and observation.",
    "fraudtwin.generation": "Deterministic standard and scale run generation.",
    "fraudtwin.graph": "Temporal graph construction, validation, and export.",
    "fraudtwin.kafka": "Optional Kafka publication adapters.",
    "fraudtwin.label_observation": "Label availability and observation histories.",
    "fraudtwin.lakehouse": "Optional Iceberg materialization and verification.",
    "fraudtwin.ml.backtest": "Replay, folds, backtesting, and metrics.",
    "fraudtwin.ml.baseline": "Baseline models and prediction evaluation.",
    "fraudtwin.ml.dataset": "Point-in-time dataset construction.",
    "fraudtwin.ml": "Point-in-time datasets, replay, and machine-learning evaluation.",
    "fraudtwin.observability": "Optional run metrics and observability sessions.",
    "fraudtwin.postgres": "Optional PostgreSQL persistence adapters.",
    "fraudtwin.quality_benchmark": "Generator-quality benchmark contracts.",
    "fraudtwin.replay": "Historical event replay and ordering.",
    "fraudtwin.scale": "Partition planning, checkpoints, and scale iteration.",
    "fraudtwin.simulation": "Simulation scenario composition and generation controls.",
    "fraudtwin.storage": "Local and fsspec-backed scale storage.",
}


def _package_exports(package_path: str) -> dict[str, list[str]]:
    package = importlib.import_module(package_path)
    classes: list[str] = []
    exceptions: list[str] = []
    functions: list[str] = []
    constants: list[str] = []
    for name in package.__all__:
        qualified_name = f"{package_path}.{name}"
        value = getattr(package, name)
        if inspect.isclass(value) and issubclass(value, Exception):
            exceptions.append(qualified_name)
        elif inspect.isclass(value):
            classes.append(qualified_name)
        elif inspect.isfunction(value):
            functions.append(qualified_name)
        else:
            constants.append(qualified_name)
    return {
        "classes": sorted(classes),
        "exceptions": sorted(exceptions),
        "functions": sorted(functions),
        "constants": sorted(constants),
    }


def _autosummary(title: str, names: list[str], *, template: str | None = None) -> list[str]:
    lines = [title, "-" * len(title), "", ".. autosummary::", "   :nosignatures:"]
    if template:
        lines.append(f"   :template: {template}")
    lines.extend(["", *[f"   {name}" for name in names], ""])
    return lines


def _stub_name(qualified_name: str) -> str:
    return qualified_name


def write_api_stubs(output_dir: Path, package_path: str) -> None:
    """Write ignored, build-time autodoc pages for the package public exports."""

    output_dir.mkdir(parents=True, exist_ok=True)
    package = importlib.import_module(package_path)
    exports = _package_exports(package_path)
    expected: set[str] = set()
    for names in exports.values():
        for qualified_name in names:
            filename = f"{_stub_name(qualified_name)}.rst"
            expected.add(filename)
            value = getattr(package, qualified_name.rsplit(".", 1)[-1])
            if inspect.isclass(value) and issubclass(value, Exception):
                directive = "autoexception"
            elif inspect.isclass(value):
                directive = "autoclass"
            elif inspect.isfunction(value):
                directive = "autofunction"
            else:
                directive = "autodata"
            options = (
                "   :show-inheritance:\n   :members:\n"
                if directive
                in {
                    "autoclass",
                    "autoexception",
                }
                else ""
            )
            content = (
                ":orphan:\n\n"
                f"{qualified_name}\n{'=' * len(qualified_name)}\n\n"
                f".. {directive}:: {qualified_name}\n"
                f"{options}"
            )
            (output_dir / filename).write_text(content, encoding="utf-8")
    for stale in output_dir.glob("*.rst"):
        if stale.name not in expected:
            stale.unlink()


def write_module_stubs(output_dir: Path) -> None:
    """Write build-time module pages for the documented public modules."""

    output_dir.mkdir(parents=True, exist_ok=True)
    expected: set[str] = set()
    for module in PUBLIC_MODULES:
        filename = f"{module}.rst"
        expected.add(filename)
        package = importlib.import_module(module)
        imported_members = "   :imported-members:\n" if hasattr(package, "__path__") else ""
        content = (
            ":orphan:\n\n"
            f"{module}\n{'=' * len(module)}\n\n"
            f".. automodule:: {module}\n"
            "   :members:\n"
            "   :show-inheritance:\n"
            f"{imported_members}"
            "   :noindex:\n"
        )
        (output_dir / filename).write_text(content, encoding="utf-8")
    for stale in output_dir.glob("*.rst"):
        if stale.name not in expected:
            stale.unlink()


class ApiInventoryDirective(Directive):
    """Insert API summary sections for every export in a package ``__all__``."""

    required_arguments = 1
    optional_arguments = 0
    has_content = False

    def run(self) -> list[Any]:
        package_path = self.arguments[0].strip()
        exports = _package_exports(package_path)
        lines: list[str] = [
            "The following indexes are generated from ``fraudtwin.__all__``.",
            "Every listed symbol has a detail page with its signature, docstring,",
            "members, and source link.",
            "",
        ]
        labels = (
            ("Classes", exports["classes"], "class.rst"),
            ("Exceptions", exports["exceptions"], "class.rst"),
            ("Functions", exports["functions"], None),
            ("Constants and protocols", exports["constants"], None),
        )
        for title, names, template in labels:
            if names:
                lines.extend(_autosummary(title, names, template=template))
        self.state_machine.insert_input(lines, self.state_machine.document["source"])
        return []


class ApiModuleIndexDirective(Directive):
    """Insert an index of generated module pages."""

    required_arguments = 0
    optional_arguments = 0
    has_content = False

    def run(self) -> list[Any]:
        lines = [
            "Every module page is generated with ``automodule`` and lists its public members.",
            "Use the global class/function index when you already know a symbol name.",
            "",
            ".. list-table:: Public modules",
            "   :header-rows: 1",
            "",
            "   * - Module",
            "     - Purpose",
        ]
        for module in PUBLIC_MODULES:
            purpose = MODULE_PURPOSES.get(module, f"Public API exported by ``{module}``.")
            lines.extend(
                [
                    f"   * - `{module} <modules/{module}.html>`_",
                    f"     - {purpose}",
                ]
            )
        self.state_machine.insert_input(lines, self.state_machine.document["source"])
        return []


def setup(app: Any) -> dict[str, Any]:
    app.add_directive("api-inventory", ApiInventoryDirective)
    app.add_directive("api-module-index", ApiModuleIndexDirective)
    return {"version": "1", "parallel_read_safe": True}
