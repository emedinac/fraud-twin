import json
from pathlib import Path

from fraudtwin.config import load_config
from fraudtwin.manifest import create_manifest, write_manifest


def test_manifest_contains_reproducibility_metadata(tmp_path: Path) -> None:
    manifest = create_manifest(load_config(Path("configs/minimal-v1.yaml")))
    manifest_path = write_manifest(manifest, tmp_path)
    saved = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert saved["seed"] == 42
    assert saved["scenario_config_hash"] == manifest.scenario_config_hash
    assert saved["entity_counts"] == {}
    assert manifest_path.name == "manifest.json"
