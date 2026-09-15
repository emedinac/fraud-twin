"""Storage ports for disk-first M18 runs.

The generator writes to a local staging directory and publishes completed
files through this small adapter.  Local storage is dependency-free; URI-based
storage imports fsspec lazily and is therefore optional.
"""

from __future__ import annotations

import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol


class ScaleStorage(Protocol):
    """Port used to publish a completed scale run."""

    def publish(self, local_root: Path, destination: str | Path) -> None:
        """Publish local files below ``destination`` atomically where possible."""


@dataclass(frozen=True)
class LocalScaleStorage:
    """Filesystem implementation used by default and in CI."""

    def publish(self, local_root: Path, destination: str | Path) -> None:
        target = Path(destination)
        if target.resolve() == local_root.resolve():
            return
        target.mkdir(parents=True, exist_ok=True)
        for source in sorted(local_root.rglob("*")):
            if not source.is_file():
                continue
            relative = source.relative_to(local_root)
            final = target / relative
            final.parent.mkdir(parents=True, exist_ok=True)
            temporary = final.with_name(final.name + ".tmp")
            shutil.copyfile(source, temporary)
            os.replace(temporary, final)


@dataclass(frozen=True)
class FsspecScaleStorage:
    """S3/MinIO-compatible publisher backed by optional fsspec."""

    uri: str

    def publish(self, local_root: Path, destination: str | Path | None = None) -> None:
        try:
            import fsspec  # type: ignore[import-not-found]
        except ImportError as exc:  # pragma: no cover - optional extra
            raise RuntimeError(
                "fsspec scale storage requires the optional 'scale-storage' dependency"
            ) from exc
        target = str(destination or self.uri).rstrip("/")
        filesystem, base = fsspec.core.url_to_fs(target)
        for source in sorted(local_root.rglob("*")):
            if not source.is_file():
                continue
            relative = source.relative_to(local_root).as_posix()
            destination_path = f"{base.rstrip('/')}/{relative}"
            temporary_path = destination_path + ".tmp"
            with source.open("rb") as input_stream, filesystem.open(temporary_path, "wb") as output:
                shutil.copyfileobj(input_stream, output)
            filesystem.mv(temporary_path, destination_path)


def storage_for(uri: str | None, backend: str = "local") -> ScaleStorage:
    """Resolve a configured storage backend without importing optional packages."""

    if backend == "local":
        return LocalScaleStorage()
    if backend == "fsspec":
        if not uri:
            raise ValueError("fsspec scale storage requires storage_uri")
        return FsspecScaleStorage(uri)
    raise ValueError(f"unsupported scale storage backend: {backend}")


__all__ = ["FsspecScaleStorage", "LocalScaleStorage", "ScaleStorage", "storage_for"]
