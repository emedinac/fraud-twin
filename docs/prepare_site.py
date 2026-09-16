"""Prepare a sphinx-multiversion output tree for GitHub Pages."""

import json
import shutil
import sys
from pathlib import Path


def prepare_site(root: Path) -> None:
    root = root.resolve()
    candidates = sorted(
        path
        for path in root.iterdir()
        if path.is_dir() and (path.name in {"main", "dev"} or path.name.startswith("v"))
    )
    if not candidates:
        raise SystemExit(f"no sphinx-multiversion outputs found in {root}")
    main_output = next((path for path in candidates if path.name in {"main", "dev"}), candidates[0])
    latest = root / "latest"
    if latest.exists():
        shutil.rmtree(latest)
    shutil.copytree(main_output, latest)
    versions = [
        {
            "name": "latest",
            "version": "latest",
            "url": "https://emedinac.github.io/fraud-twin/latest/",
        }
    ]
    for path in candidates:
        if path.name in {"main", "dev"}:
            continue
        versions.append(
            {
                "name": path.name,
                "version": path.name,
                "url": f"https://emedinac.github.io/fraud-twin/{path.name}/",
            }
        )
    (root / "version-switcher.json").write_text(
        json.dumps(versions, indent=2) + "\n", encoding="utf-8"
    )
    (root / "index.html").write_text(
        '<!doctype html><meta http-equiv="refresh" content="0; url=latest/">'
        '<link rel="canonical" href="latest/">\n',
        encoding="utf-8",
    )


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: prepare_site.py BUILD_DIRECTORY")
    prepare_site(Path(sys.argv[1]))


if __name__ == "__main__":
    main()
