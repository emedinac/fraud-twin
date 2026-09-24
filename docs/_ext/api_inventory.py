"""Generate an exhaustive inventory of a package's supported public API."""

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
            "fraudtwin.extensions",
            "fraudtwin.domain.campaign_dynamics",
            "fraudtwin.domain.cases",
            "fraudtwin.domain.graph",
            "fraudtwin.domain.labels",
            "fraudtwin.domain",
            "fraudtwin.generation",
            "fraudtwin.graph",
            "fraudtwin.kafka",
            "fraudtwin.kafka_chaos",
            "fraudtwin.label_observation",
            "fraudtwin.lakehouse",
            "fraudtwin.ml",
            "fraudtwin.ml.backtest",
            "fraudtwin.ml.baseline",
            "fraudtwin.ml.dataset",
            "fraudtwin.ml.drift",
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
    "fraudtwin.extensions": "Stable extension protocols and local entry-point discovery.",
    "fraudtwin.domain": "Stable domain records for entities, payments, fraud, and labels.",
    "fraudtwin.domain.campaign_dynamics": "Domain types for evolving fraud campaigns.",
    "fraudtwin.domain.cases": "Reusable domain case and scenario records.",
    "fraudtwin.domain.graph": "Domain records for graph nodes, edges, and evidence.",
    "fraudtwin.domain.labels": "Domain records for label maturity and observation.",
    "fraudtwin.generation": "Deterministic standard and scale run generation.",
    "fraudtwin.graph": "Temporal graph construction, validation, and export.",
    "fraudtwin.kafka": "Optional Kafka publication adapters.",
    "fraudtwin.kafka_chaos": "Deterministic logical-message delivery fault simulation.",
    "fraudtwin.label_observation": "Label availability and observation histories.",
    "fraudtwin.lakehouse": "Optional Iceberg materialization and verification.",
    "fraudtwin.ml.backtest": "Replay, folds, backtesting, and metrics.",
    "fraudtwin.ml.baseline": "Baseline models and prediction evaluation.",
    "fraudtwin.ml.dataset": "Point-in-time dataset construction.",
    "fraudtwin.ml.drift": "Deterministic data, domain, and concept drift reports.",
    "fraudtwin.ml": "Point-in-time datasets, replay, and machine-learning evaluation.",
    "fraudtwin.observability": "Optional run metrics and observability sessions.",
    "fraudtwin.postgres": "Optional PostgreSQL persistence adapters.",
    "fraudtwin.quality_benchmark": "Generator-quality benchmark contracts.",
    "fraudtwin.replay": "Historical event replay and ordering.",
    "fraudtwin.scale": "Partition planning, checkpoints, and scale iteration.",
    "fraudtwin.simulation": "Simulation scenario composition and generation controls.",
    "fraudtwin.storage": "Local and fsspec-backed scale storage.",
}

MODULE_STATUSES = {
    "fraudtwin.benchmark": "Experimental",
    "fraudtwin.calibration": "Experimental",
    "fraudtwin.campaign_dynamics": "Experimental",
    "fraudtwin.counterfactual": "Experimental",
    "fraudtwin.difficulty": "Experimental",
    "fraudtwin.extensions": "Stable",
    "fraudtwin.kafka": "Optional",
    "fraudtwin.kafka_chaos": "Experimental",
    "fraudtwin.lakehouse": "Optional",
    "fraudtwin.observability": "Optional",
    "fraudtwin.postgres": "Optional",
    "fraudtwin.quality_benchmark": "Experimental",
    "fraudtwin.scale": "Experimental",
    "fraudtwin.ml.drift": "Experimental",
}


def _classify_exports(module_path: str, names: list[str]) -> dict[str, list[str]]:
    module = importlib.import_module(module_path)
    classes: list[str] = []
    exceptions: list[str] = []
    functions: list[str] = []
    constants: list[str] = []
    for name in names:
        qualified_name = f"{module_path}.{name}"
        value = getattr(module, name)
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


def _module_exports(module_path: str) -> dict[str, list[str]]:
    """Return documented public names for a module, grouped by kind."""

    module = importlib.import_module(module_path)
    names = getattr(module, "__all__", None)
    if names is None:
        names = [
            name
            for name, value in inspect.getmembers(module)
            if not name.startswith("_")
            and (inspect.isclass(value) or inspect.isfunction(value))
            and getattr(value, "__module__", None) == module_path
        ]
    return _classify_exports(module_path, list(names))


def _package_exports(package_path: str) -> dict[str, list[str]]:
    package = importlib.import_module(package_path)
    return _classify_exports(package_path, list(package.__all__))


def _autosummary(title: str, names: list[str], *, template: str | None = None) -> list[str]:
    lines = [title, "-" * len(title), "", ".. autosummary::", "   :nosignatures:"]
    if template:
        lines.append(f"   :template: {template}")
    lines.extend(["", *[f"   {name}" for name in names], ""])
    return lines


def _constant_summary(names: list[str]) -> list[str]:
    """Render data names without parsing builtin ``dict`` docstrings."""

    lines = [
        "Constants and protocols",
        "-" * len("Constants and protocols"),
        "",
        ".. list-table::",
        "   :header-rows: 1",
        "",
        "   * - Name",
        "     - Reference",
    ]
    for name in names:
        lines.extend(
            [
                f"   * - ``{name.rsplit('.', 1)[-1]}``",
                f"     - :py:data:`{name}`",
            ]
        )
    lines.append("")
    return lines


def _stub_name(qualified_name: str) -> str:
    return qualified_name


def _label_name(prefix: str, qualified_name: str) -> str:
    return f"{prefix}-{qualified_name.replace('.', '-')}"


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
                f".. _{_label_name('api', qualified_name)}:\n\n"
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
        exports = _module_exports(module)
        status = MODULE_STATUSES.get(module, "Stable")
        summary: list[str] = [
            MODULE_PURPOSES.get(module, f"Public API exported by ``{module}``."),
            "",
            f"**Status:** {status}",
            "",
        ]
        for title, names in (
            ("Classes", exports["classes"]),
            ("Exceptions", exports["exceptions"]),
            ("Functions", exports["functions"]),
        ):
            if names:
                summary.extend(_autosummary(title, names))
        if exports["constants"]:
            summary.extend(_constant_summary(exports["constants"]))
        summary.extend(["Detailed API", "------------", ""])
        imported_members = "   :imported-members:\n" if hasattr(package, "__path__") else ""
        content = (
            ":orphan:\n\n"
            f".. _{_label_name('api-module', module)}:\n\n"
            f"{module}\n{'=' * len(module)}\n\n"
            + "\n".join(summary)
            + "\n"
            + f".. automodule:: {module}\n"
            + "   :members:\n"
            + "   :show-inheritance:\n"
            + imported_members
            + "   :noindex:\n"
        )
        (output_dir / filename).write_text(content, encoding="utf-8")
    for stale in output_dir.glob("*.rst"):
        if stale.name not in expected:
            stale.unlink()


class ApiInventoryDirective(Directive):
    """Insert API summary sections for every supported package export."""

    required_arguments = 1
    optional_arguments = 0
    has_content = False

    def run(self) -> list[Any]:
        package_path = self.arguments[0].strip()
        exports = _package_exports(package_path)
        lines: list[str] = [
            "The following indexes are generated from the package's supported public API.",
            "Every listed symbol has a detail page with its signature, docstring,",
            "members, and source link.",
            "",
        ]
        labels = (
            ("Classes", exports["classes"], "class.rst"),
            ("Exceptions", exports["exceptions"], "class.rst"),
            ("Functions", exports["functions"], None),
        )
        for title, names, template in labels:
            if names:
                lines.extend(_autosummary(title, names, template=template))
        if exports["constants"]:
            lines.extend(_constant_summary(exports["constants"]))
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
            "     - Status",
        ]
        for module in PUBLIC_MODULES:
            purpose = MODULE_PURPOSES.get(module, f"Public API exported by ``{module}``.")
            lines.extend(
                [
                    f"   * - :doc:`{module} <modules/{module}>`",
                    f"     - {purpose}",
                    f"     - {MODULE_STATUSES.get(module, 'Stable')}",
                ]
            )
        self.state_machine.insert_input(lines, self.state_machine.document["source"])
        return []


def setup(app: Any) -> dict[str, Any]:
    app.add_directive("api-inventory", ApiInventoryDirective)
    app.add_directive("api-module-index", ApiModuleIndexDirective)
    return {"version": "1", "parallel_read_safe": True}
