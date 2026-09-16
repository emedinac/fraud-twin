"""Sphinx configuration for the FraudTwin documentation."""

import os
import sys
import tomllib
from importlib import import_module
from inspect import getsourcefile, getsourcelines
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
    "sphinx.ext.linkcode",
    "sphinx_autodoc_typehints",
    "sphinx_copybutton",
    "sphinx_design",
    "sphinx_multiversion",
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
autodoc_preserve_defaults = True
python_use_unqualified_type_names = True
if os.environ.get("FRAUDTWIN_DOCS_EXTERNAL_XREFS"):
    extensions.append("sphinx.ext.intersphinx")
    intersphinx_mapping = {
        "python": ("https://docs.python.org/3", None),
        "pydantic": ("https://docs.pydantic.dev/latest/", None),
        "polars": ("https://docs.pola.rs/api/python/stable/", None),
    }
# Tutorial outputs are committed notebook artifacts.  Rendering them keeps
# Pages builds fast and deterministic; execution belongs in an explicit test.
nb_execution_mode = "off"
nb_render_markdown_format = "myst"

# The Markdown hub links to repository-level files that are intentionally not
# Sphinx source documents.
suppress_warnings = [
    "myst.xref_missing",
    "intersphinx.external",
    "intersphinx.load_failure",
]
html_theme = "pydata_sphinx_theme"
html_title = f"{project} {version} documentation"
html_logo = "_static/fraudtwin-mark.svg"
html_favicon = "_static/fraudtwin-mark.svg"
html_static_path = ["_static"]
html_css_files = ["custom.css"]
_site_version = globals().get("smv_current_version", "latest")
if _site_version in {"main", "dev"}:
    _site_version = "latest"
html_baseurl = f"https://emedinac.github.io/fraud-twin/{_site_version}/"
html_theme_options = {
    "logo": {"text": "FraudTwin"},
    "navbar_align": "content",
    "navbar_center": ["navbar-nav"],
    "navbar_end": ["navbar-icon-links", "theme-switcher"],
    "header_links_before_dropdown": 5,
    "show_nav_level": 2,
    "navigation_with_keys": True,
    "show_version_warning_banner": True,
    "github_url": "https://github.com/emedinac/fraud-twin",
    "use_edit_page_button": True,
    "announcement": (
        "FraudTwin documentation is versioned. Check the version selector before "
        "copying an API example."
    ),
    "icon_links": [
        {
            "name": "GitHub",
            "url": "https://github.com/emedinac/fraud-twin",
            "icon": "fa-brands fa-github",
        },
    ],
}
if os.environ.get("FRAUDTWIN_VERSIONED_BUILD") and not os.environ.get(
    "FRAUDTWIN_SKIP_VERSION_SWITCHER"
):
    html_theme_options["switcher"] = {
        "json_url": "https://emedinac.github.io/fraud-twin/version-switcher.json",
        "version_match": version,
    }
html_context = {
    "github_user": "emedinac",
    "github_repo": "fraud-twin",
    "github_version": "main",
    "doc_path": "docs",
}

smv_branch_whitelist = (
    r"^(main|dev)$" if os.environ.get("FRAUDTWIN_DOCS_INCLUDE_DEV") else r"^main$"
)
smv_tag_whitelist = r"^v\d+\.\d+\.\d+$"
smv_remote_whitelist = None


def linkcode_resolve(domain: str, info: dict[str, str]) -> str | None:
    """Point Python API source links at the matching GitHub release/source."""

    if domain != "py" or not info.get("module") or not info.get("fullname"):
        return None
    try:
        module = import_module(info["module"])
        target = module
        for part in info["fullname"].split("."):
            target = getattr(target, part)
        source = getsourcefile(target)
        line_number = getsourcelines(target)[1]
    except (AttributeError, ImportError, OSError, TypeError):
        return None
    if source is None:
        return None
    try:
        relative = Path(source).resolve().relative_to(PROJECT_ROOT.resolve())
    except ValueError:
        return None
    end_line = line_number + max(len(getsourcelines(target)[0]) - 1, 0)
    source_ref = globals().get("smv_current_version", f"v{release}")
    if source_ref in {"main", "dev"}:
        source_ref = "main"
    return (
        "https://github.com/emedinac/fraud-twin/blob/"
        f"{source_ref}/{relative.as_posix()}#L{line_number}-L{end_line}"
    )
