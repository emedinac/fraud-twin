import importlib.util
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

_POLICY_PATH = Path(__file__).resolve().parents[1] / ".github" / "scripts" / "release_policy.py"
_POLICY_SPEC = importlib.util.spec_from_file_location("fraud_twin_release_policy", _POLICY_PATH)
assert _POLICY_SPEC is not None and _POLICY_SPEC.loader is not None
_POLICY = importlib.util.module_from_spec(_POLICY_SPEC)
_POLICY_SPEC.loader.exec_module(_POLICY)
bump_release_identity = _POLICY.bump_release_identity
check_commit_subjects = _POLICY.check_commit_subjects
check_release_identity = _POLICY.check_release_identity


def _write_release_tree(root: Path, *, version: str, changelog_version: str | None = None) -> None:
    (root / "src" / "fraudtwin").mkdir(parents=True)
    (root / "pyproject.toml").write_text(
        f'[project]\nname = "fraudtwin"\nversion = "{version}"\n',
        encoding="utf-8",
    )
    (root / "src" / "fraudtwin" / "__init__.py").write_text(
        f'__version__ = "{version}"\n',
        encoding="utf-8",
    )
    (root / "CHANGELOG.md").write_text(
        f"# Release notes\n\n## {changelog_version or version} — Test release\n\n- Test entry.\n",
        encoding="utf-8",
    )


def test_release_identity_requires_three_matching_sources(tmp_path: Path) -> None:
    _write_release_tree(tmp_path, version="0.34.0")
    check_release_identity(tmp_path)

    (tmp_path / "CHANGELOG.md").write_text(
        "# Release notes\n\n## 0.34.1 — Test release\n\n- Test entry.\n", encoding="utf-8"
    )
    with pytest.raises(ValueError, match="CHANGELOG.md"):
        check_release_identity(tmp_path)


def test_bump_updates_pyproject_and_runtime_version(tmp_path: Path) -> None:
    base = tmp_path / "base"
    work = tmp_path / "work"
    _write_release_tree(base, version="0.34.0")
    _write_release_tree(work, version="0.34.0")

    assert bump_release_identity(work, base) == "0.34.1"
    assert 'version = "0.34.1"' in (work / "pyproject.toml").read_text(encoding="utf-8")
    assert '__version__ = "0.34.1"' in (work / "src" / "fraudtwin" / "__init__.py").read_text(
        encoding="utf-8"
    )


def test_bump_does_not_repeat_for_an_already_higher_version(tmp_path: Path) -> None:
    base = tmp_path / "base"
    work = tmp_path / "work"
    _write_release_tree(base, version="0.35.1")
    _write_release_tree(work, version="0.35.2")

    assert bump_release_identity(work, base) is None
    check_release_identity(work)


def test_commit_subjects_follow_the_conventional_commit_allowlist(tmp_path: Path) -> None:
    subjects = tmp_path / "subjects.txt"
    subjects.write_text("feat(cli): add command\nfix: correct output\n", encoding="utf-8")
    check_commit_subjects(subjects)

    subjects.write_text("Update CI\n", encoding="utf-8")
    with pytest.raises(ValueError, match="invalid commit subjects"):
        check_commit_subjects(subjects)
