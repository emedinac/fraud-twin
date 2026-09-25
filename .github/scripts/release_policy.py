"""Validate and update FraudTwin release metadata for CI."""

import argparse
import re
import sys
import tomllib
from pathlib import Path

VERSION_RE = re.compile(r"^(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)$")
VERSION_HEADING_RE = re.compile(
    r"^##\s+(?P<version>(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*))(?:\s|$)"
)
VERSION_ASSIGNMENT_RE = re.compile(
    r'(?m)^(?P<prefix>__version__\s*=\s*["\'])(?P<version>[^"\']+)(?P<suffix>["\'])'
)
PROJECT_VERSION_RE = re.compile(
    r'(?m)^(?P<prefix>version\s*=\s*["\'])(?P<version>[^"\']+)(?P<suffix>["\'])'
)
COMMIT_SUBJECT_RE = re.compile(
    r"^(?:(?:feat|fix|refactor|perf|docs|test|build|ci|chore|style|revert)"
    r'(?:\([^()]+\))?!?:\s+\S.*|Revert "\S.*")$'
)
MERGE_COMMIT_SUBJECT_RE = re.compile(
    r"^Merge (?:branches? '[^']+'(?:, '[^']+')*|remote-tracking branch '[^']+'|pull request #[0-9]+ from \S+)"
    r"(?: of \S+)?(?: into \S+)?$"
)
PACKAGE_PIN_RE = re.compile(
    r"\bfraudtwin(?:==|>=|<=|~=)\s*" r"(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\b"
)


def _read_project_version(root: Path) -> str:
    with (root / "pyproject.toml").open("rb") as stream:
        version = tomllib.load(stream)["project"]["version"]
    if not isinstance(version, str) or VERSION_RE.fullmatch(version) is None:
        raise ValueError(f"pyproject.toml has an invalid project version: {version!r}")
    return version


def _read_init_version(root: Path) -> str:
    path = root / "src" / "fraudtwin" / "__init__.py"
    match = VERSION_ASSIGNMENT_RE.search(path.read_text(encoding="utf-8"))
    if match is None:
        raise ValueError(f"{path} must define __version__")
    version = match.group("version")
    if VERSION_RE.fullmatch(version) is None:
        raise ValueError(f"{path} has an invalid __version__: {version!r}")
    return version


def _read_changelog_version(root: Path) -> str:
    path = root / "CHANGELOG.md"
    lines = path.read_text(encoding="utf-8").splitlines()
    for index, line in enumerate(lines):
        match = VERSION_HEADING_RE.match(line)
        if match is not None:
            next_heading = next(
                (
                    position
                    for position, candidate in enumerate(lines[index + 1 :], index + 1)
                    if candidate.startswith("## ")
                ),
                len(lines),
            )
            if not any(
                candidate.lstrip().startswith("- ") for candidate in lines[index + 1 : next_heading]
            ):
                raise ValueError("the current CHANGELOG.md release must contain a bullet entry")
            return match.group("version")
    raise ValueError("CHANGELOG.md must start with a versioned release heading")


def _package_reference_errors(root: Path) -> list[str]:
    errors: list[str] = []
    init_path = Path("src/fraudtwin/__init__.py")
    for path in root.joinpath("src", "fraudtwin").rglob("*.py"):
        if path.relative_to(root) == init_path:
            continue
        if VERSION_ASSIGNMENT_RE.search(path.read_text(encoding="utf-8")) is not None:
            errors.append(f"{path.relative_to(root)} defines a duplicate __version__")

    for path in (
        *root.joinpath("src").rglob("*.py"),
        *root.joinpath("tests").rglob("*.py"),
        *root.joinpath("docs").rglob("*.md"),
        *root.joinpath("docs").rglob("*.rst"),
        root / "README.md",
    ):
        if not path.is_file():
            continue
        relative = path.relative_to(root)
        if relative == Path("CHANGELOG.md") or "public_packs" in relative.parts:
            continue
        for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if PACKAGE_PIN_RE.search(line):
                errors.append(
                    f"{relative}:{line_number} hardcodes a FraudTwin package version; "
                    "use the current package metadata instead"
                )
    return errors


def check_release_identity(root: Path, tag: str | None = None) -> None:
    errors: list[str] = []
    try:
        project_version = _read_project_version(root)
        init_version = _read_init_version(root)
        changelog_version = _read_changelog_version(root)
    except (OSError, KeyError, TypeError, tomllib.TOMLDecodeError, ValueError) as exc:
        errors.append(str(exc))
    else:
        if init_version != project_version:
            errors.append(
                f"src/fraudtwin/__init__.py has {init_version}, "
                f"but pyproject.toml has {project_version}"
            )
        if changelog_version != project_version:
            errors.append(
                f"CHANGELOG.md starts with {changelog_version}, "
                f"but pyproject.toml has {project_version}"
            )
        if tag is not None and tag.removeprefix("v") != project_version:
            errors.append(f"release tag {tag} does not match project version {project_version}")

    errors.extend(_package_reference_errors(root))
    if errors:
        raise ValueError("\n".join(f"- {error}" for error in errors))


def _version_tuple(version: str) -> tuple[int, int, int]:
    return tuple(int(part) for part in version.split("."))  # type: ignore[return-value]


def check_version_advanced(root: Path, base_root: Path) -> None:
    """Require a PR version to advance beyond the target branch version."""
    current_version = _read_project_version(root)
    base_version = _read_project_version(base_root)
    if _version_tuple(current_version) <= _version_tuple(base_version):
        raise ValueError(
            f"PR version {current_version} must be greater than base version {base_version}; "
            "update pyproject.toml, src/fraudtwin/__init__.py, and CHANGELOG.md"
        )


def _next_patch(version: str) -> str:
    major, minor, patch = _version_tuple(version)
    return f"{major}.{minor}.{patch + 1}"


def bump_release_identity(root: Path, base_root: Path) -> str | None:
    base_version = _read_project_version(base_root)
    current_version = _read_project_version(root)
    if _version_tuple(current_version) > _version_tuple(base_version):
        if _read_init_version(root) != current_version:
            raise ValueError("pyproject.toml and __version__ must match before the PR can continue")
        return None

    target_version = _next_patch(base_version)
    pyproject = root / "pyproject.toml"
    pyproject_text = pyproject.read_text(encoding="utf-8")
    pyproject_text, count = PROJECT_VERSION_RE.subn(
        lambda match: f"{match.group('prefix')}{target_version}{match.group('suffix')}",
        pyproject_text,
        count=1,
    )
    if count != 1:
        raise ValueError("could not update project version in pyproject.toml")
    pyproject.write_text(pyproject_text, encoding="utf-8")

    init_path = root / "src" / "fraudtwin" / "__init__.py"
    init_text = init_path.read_text(encoding="utf-8")
    init_text, count = VERSION_ASSIGNMENT_RE.subn(
        lambda match: f"{match.group('prefix')}{target_version}{match.group('suffix')}",
        init_text,
        count=1,
    )
    if count != 1:
        raise ValueError("could not update __version__ in src/fraudtwin/__init__.py")
    init_path.write_text(init_text, encoding="utf-8")

    changelog_path = root / "CHANGELOG.md"
    changelog_text = changelog_path.read_text(encoding="utf-8")
    changelog_text, count = re.subn(
        VERSION_HEADING_RE.pattern,
        lambda match: match.group(0).replace(match.group("version"), target_version, 1),
        changelog_text,
        count=1,
        flags=re.MULTILINE,
    )
    if count != 1:
        raise ValueError("could not update current CHANGELOG.md release heading")
    changelog_path.write_text(changelog_text, encoding="utf-8")
    return target_version


def check_commit_subjects(path: Path) -> None:
    errors = []
    for line_number, subject in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not subject.strip():
            continue
        if (
            COMMIT_SUBJECT_RE.fullmatch(subject) is None
            and MERGE_COMMIT_SUBJECT_RE.fullmatch(subject) is None
        ):
            errors.append(f"{line_number}: {subject}")
    if errors:
        raise ValueError(
            "invalid commit subjects (use feat|fix|refactor|perf|docs|test|build|ci|chore|"
            "style|revert, or a generated merge subject):\n"
            + "\n".join(f"- {error}" for error in errors)
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    check_parser = subparsers.add_parser("check")
    check_parser.add_argument("--root", type=Path, default=Path.cwd())
    check_parser.add_argument("--base-root", type=Path)
    check_parser.add_argument("--tag")
    check_parser.add_argument("--commit-subjects", type=Path)

    bump_parser = subparsers.add_parser("bump")
    bump_parser.add_argument("--root", type=Path, required=True)
    bump_parser.add_argument("--base-root", type=Path, required=True)

    args = parser.parse_args()
    try:
        if args.command == "check":
            check_release_identity(args.root.resolve(), args.tag)
            if args.base_root is not None:
                check_version_advanced(args.root.resolve(), args.base_root.resolve())
            if args.commit_subjects is not None:
                check_commit_subjects(args.commit_subjects)
        else:
            target = bump_release_identity(args.root.resolve(), args.base_root.resolve())
            if target is not None:
                print(target)
    except (OSError, ValueError, tomllib.TOMLDecodeError) as exc:
        print(exc, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
