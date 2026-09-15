"""Sphinx configuration for the FraudTwin documentation."""

import sys
import tomllib
from pathlib import Path

DOCS_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = DOCS_ROOT.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))
sys.path.insert(0, str(DOCS_ROOT))

with (PROJECT_ROOT / "pyproject.toml").open("rb") as project_file:
    project_metadata = tomllib.load(project_file)["project"]

project = "FraudTwin"
copyright = "2026, FraudTwin contributors"
author = "FraudTwin contributors"
release = str(project_metadata["version"])
version = release

extensions = [
    "myst_nb",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "_ext.config_schema",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]
autosummary_generate = True
autodoc_member_order = "bysource"
autodoc_typehints = "description"
autodoc_default_options = {
    "show-inheritance": True,
}
# Tutorial outputs are committed notebook artifacts.  Rendering them keeps
# Pages builds fast and deterministic; execution belongs in an explicit test.
nb_execution_mode = "off"
nb_render_markdown_format = "myst"

# The Markdown hub links to repository-level files that are intentionally not
# Sphinx source documents.
suppress_warnings = ["myst.xref_missing"]
html_theme = "furo"
html_title = f"{project} {version} documentation"
html_theme_options = {
    "source_repository": "https://github.com/emedinac/fraud-twin",
    "source_branch": "main",
    "source_directory": "docs/",
    "navigation_with_keys": True,
}
