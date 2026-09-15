"""Deterministic sharding, checkpointing, and reconciliation for M18.

The scale layer deliberately operates on the canonical generated records.  It
only controls partitioning and persistence; it does not contain a second
simulation model.
"""

from __future__ import annotations

import hashlib
import os
from collections.abc import Iterable, Iterator, Mapping
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from random import Random
from typing import Any

import polars as pl
from pydantic import BaseModel, ConfigDict, Field

from fraudtwin.config import (
    PartitionMapping,
    ScaleProfile,
    SimulationRunConfig,
    config_hash,
)
from fraudtwin.reproducibility import sha256_json
from fraudtwin.seed import create_stream_rng

SEED_TREE_VERSION = "M18-seed-tree-1"


class ScalePlan(BaseModel):
    """Resolved, immutable execution parameters for one scale run."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    profile: ScaleProfile
    target_logical_events: int
    shard_count: int = Field(ge=1)
    chunk_size: int = Field(ge=1)
    worker_count: int = Field(ge=1)
    output_batch_size: int = Field(ge=1)
    checkpoint_frequency_chunks: int = Field(ge=1)
    partition_mapping: PartitionMapping
    seed: int = Field(ge=0)
    run_id: str
    configuration_hash: str
    seed_tree_version: str = SEED_TREE_VERSION


class ShardDescriptor(BaseModel):
    """Stable shard identity; it is independent of the worker executing it."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    shard_index: int = Field(ge=0)
    shard_id: str
    mapping: str


class ChunkDescriptor(BaseModel):
    """Half-open logical ordinal range belonging to a stable shard."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    shard_id: str
    chunk_index: int = Field(ge=0)
    start_ordinal: int = Field(ge=0)
    end_ordinal: int = Field(gt=0)


class PartitionCompletion(BaseModel):
    """Fingerprint and counts for one atomically completed partition."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    shard_id: str
    row_count: int = Field(ge=0)
    chunk_count: int = Field(ge=0)
    fingerprint: str
    checksum: str
    path: str


class ReconciliationResult(BaseModel):
    """Cross-partition invariant results recorded in manifests/checkpoints."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    logical_row_count: int = Field(ge=0)
    partition_row_count: int = Field(ge=0)
    duplicate_logical_ids: tuple[str, ...] = ()
    missing_logical_ids: tuple[str, ...] = ()
    valid: bool
    checks: dict[str, bool] = Field(default_factory=dict)


class ScaleCheckpoint(BaseModel):
    """Serializable checkpoint manifest for deterministic resume."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    checkpoint_version: str = "M18-checkpoint-1"
    run_id: str
    run_dir: str
    configuration_hash: str
    resolved_configuration: dict[str, Any]
    seed: int = Field(ge=0)
    seed_tree_version: str = SEED_TREE_VERSION
    plan: ScalePlan
    completed_partitions: tuple[PartitionCompletion, ...] = ()
    reconciliation: ReconciliationResult | None = None
    created_at: datetime
    integrity_hash: str | None = None


def resolve_scale_plan(
    config: SimulationRunConfig, *, run_id: str | None = None
) -> ScalePlan | None:
    """Resolve the opt-in scale configuration, returning ``None`` when disabled."""

    profile = config.scale.profile
    if profile is None:
        return None
    resolved_hash = config_hash(config)
    return ScalePlan(
        profile=profile,
        target_logical_events=config.scale.target_logical_events or 0,
        shard_count=config.scale.shard_count,
        chunk_size=config.scale.chunk_size,
        worker_count=config.scale.worker_count,
        output_batch_size=config.scale.output_batch_size,
        checkpoint_frequency_chunks=config.scale.checkpoint_frequency_chunks,
        partition_mapping=config.scale.partition_mapping,
        seed=config.simulation.seed,
        run_id=run_id or f"RUN-{resolved_hash[:16]}",
        configuration_hash=resolved_hash,
    )


def shard_descriptors(plan: ScalePlan) -> tuple[ShardDescriptor, ...]:
    """Return shard descriptors in canonical index order."""

    return tuple(
        ShardDescriptor(
            shard_index=index,
            shard_id=f"SHARD-{index:06d}",
            mapping=plan.partition_mapping,
        )
        for index in range(plan.shard_count)
    )


def partition_index(logical_id: str, shard_count: int) -> int:
    """Map a logical ID to a stable shard without using Python hash randomization."""

    if shard_count < 1:
        raise ValueError("shard_count must be positive")
    digest = hashlib.sha256(logical_id.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") % shard_count


def partition_id(logical_id: str, shard_count: int) -> str:
    """Return the canonical shard ID for a logical record."""

    return f"SHARD-{partition_index(logical_id, shard_count):06d}"


def create_scale_stream_rng(seed: int, *components: str) -> Random:
    """Create a deterministic hierarchical stream for one scale task."""

    if not components:
        raise ValueError("scale RNG requires at least one stable component")
    return create_stream_rng(seed, "m18:" + ":".join(components))


def iter_chunks(shard_id: str, start: int, end: int, chunk_size: int) -> Iterator[ChunkDescriptor]:
    """Yield deterministic half-open chunk descriptors."""

    if start < 0 or end < start or chunk_size < 1:
        raise ValueError("invalid chunk range or chunk_size")
    index = 0
    for offset in range(start, end, chunk_size):
        yield ChunkDescriptor(
            shard_id=shard_id,
            chunk_index=index,
            start_ordinal=offset,
            end_ordinal=min(offset + chunk_size, end),
        )
        index += 1


def fingerprint_rows(rows: Iterable[Mapping[str, Any]]) -> str:
    """Hash canonical JSON rows, independent of process or file ordering."""

    return sha256_json([dict(row) for row in rows])


def reconcile_logical_ids(
    logical_ids: Iterable[str],
    partition_ids: Iterable[str],
    *,
    expected_ids: Iterable[str] | None = None,
) -> ReconciliationResult:
    """Detect duplicate/lost IDs and return a serializable reconciliation result."""

    emitted = list(partition_ids)
    counts: dict[str, int] = {}
    for item in emitted:
        counts[item] = counts.get(item, 0) + 1
    duplicates = tuple(sorted(item for item, count in counts.items() if count > 1))
    expected = set(expected_ids) if expected_ids is not None else set(logical_ids)
    missing = tuple(sorted(expected - set(emitted)))
    valid = not duplicates and not missing and len(emitted) == len(expected)
    return ReconciliationResult(
        logical_row_count=len(expected),
        partition_row_count=len(emitted),
        duplicate_logical_ids=duplicates,
        missing_logical_ids=missing,
        valid=valid,
        checks={"unique_logical_ids": not duplicates, "complete_logical_ids": not missing},
    )


def write_checkpoint(path: str | Path, checkpoint: ScaleCheckpoint) -> Path:
    """Atomically publish a checkpoint manifest."""

    if checkpoint.integrity_hash is None:
        checkpoint = checkpoint.model_copy(
            update={"integrity_hash": checkpoint_fingerprint(checkpoint)}
        )
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(destination.name + ".tmp")
    temporary.write_text(checkpoint.model_dump_json(indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, destination)
    return destination


def load_checkpoint(path: str | Path) -> ScaleCheckpoint:
    """Load and validate a checkpoint manifest."""

    source = Path(path)
    if source.is_dir():
        source = source / "checkpoint.json"
    try:
        checkpoint = ScaleCheckpoint.model_validate_json(source.read_text(encoding="utf-8"))
        if checkpoint.integrity_hash is not None:
            if checkpoint.integrity_hash != checkpoint_fingerprint(checkpoint):
                raise ValueError("checkpoint integrity hash mismatch")
        return checkpoint
    except (OSError, ValueError) as exc:
        raise ValueError(f"invalid scale checkpoint: {source}") from exc


def write_scale_partitions(
    run_dir: str | Path,
    plan: ScalePlan,
    records: Iterable[Mapping[str, Any]],
    *,
    checkpoint_dir: str | Path | None = None,
    resolved_configuration: dict[str, Any] | None = None,
) -> tuple[tuple[PartitionCompletion, ...], ReconciliationResult, Path]:
    """Write deterministic partition/chunk artifacts and a checkpoint.

    Records are expected to contain a unique ``logical_id`` and ``logical_type``.
    The writer sorts by those stable fields before chunking; worker execution is
    intentionally not represented in the output contract.
    """

    root = Path(run_dir) / "scale" / "partitions"
    root.mkdir(parents=True, exist_ok=True)
    grouped: dict[str, list[dict[str, Any]]] = {
        item.shard_id: [] for item in shard_descriptors(plan)
    }
    logical_ids: list[str] = []
    for record in records:
        row = dict(record)
        logical_id = str(row["logical_id"])
        row["partition_id"] = partition_id(logical_id, plan.shard_count)
        grouped[row["partition_id"]].append(row)
        logical_ids.append(logical_id)

    def write_partition(
        descriptor: ShardDescriptor, rows: list[dict[str, Any]]
    ) -> tuple[PartitionCompletion, tuple[str, ...]]:
        ordered = sorted(
            rows,
            key=lambda item: (str(item["logical_id"]), str(item["logical_type"])),
        )
        shard_root = root / descriptor.shard_id
        shard_root.mkdir(parents=True, exist_ok=True)
        chunks = tuple(iter_chunks(descriptor.shard_id, 0, len(ordered), plan.chunk_size))
        for chunk in chunks:
            chunk_rows = ordered[chunk.start_ordinal : chunk.end_ordinal]
            frame = pl.DataFrame(chunk_rows)
            temporary = shard_root / f"chunk-{chunk.chunk_index:06d}.parquet.tmp"
            destination = shard_root / f"chunk-{chunk.chunk_index:06d}.parquet"
            frame.write_parquet(temporary)
            os.replace(temporary, destination)
        checksum = sha256_json({"rows": ordered, "chunks": len(chunks)})
        completion = PartitionCompletion(
            shard_id=descriptor.shard_id,
            row_count=len(ordered),
            chunk_count=len(chunks),
            fingerprint=fingerprint_rows(ordered),
            checksum=checksum,
            path=str(shard_root.relative_to(Path(run_dir))),
        )
        return completion, tuple(str(item["logical_id"]) for item in ordered)

    descriptors = shard_descriptors(plan)
    with ThreadPoolExecutor(max_workers=min(plan.worker_count, plan.shard_count)) as executor:
        futures = [
            executor.submit(write_partition, item, grouped[item.shard_id]) for item in descriptors
        ]
        results = [future.result() for future in futures]
    completions = [item[0] for item in results]
    emitted_ids = [logical_id for _, ids in results for logical_id in ids]

    reconciliation = reconcile_logical_ids(logical_ids, emitted_ids, expected_ids=logical_ids)
    checkpoint_root = (
        Path(checkpoint_dir) if checkpoint_dir is not None else Path(run_dir) / "scale"
    )
    checkpoint = ScaleCheckpoint(
        run_id=plan.run_id,
        run_dir=str(Path(run_dir).resolve()),
        configuration_hash=plan.configuration_hash,
        resolved_configuration=resolved_configuration or {},
        seed=plan.seed,
        plan=plan,
        completed_partitions=tuple(completions),
        reconciliation=reconciliation,
        created_at=datetime.now(UTC),
    )
    checkpoint_path = write_checkpoint(checkpoint_root / "checkpoint.json", checkpoint)
    return tuple(completions), reconciliation, checkpoint_path


def checkpoint_fingerprint(checkpoint: ScaleCheckpoint) -> str:
    """Return a stable fingerprint excluding the wall-clock creation time."""

    return sha256_json(_checkpoint_integrity_payload(checkpoint))


def _checkpoint_integrity_payload(checkpoint: ScaleCheckpoint) -> dict[str, Any]:
    payload = checkpoint.model_dump(mode="json")
    payload.pop("integrity_hash", None)
    payload.pop("created_at", None)
    return payload


__all__ = [
    "SEED_TREE_VERSION",
    "ScalePlan",
    "ShardDescriptor",
    "ChunkDescriptor",
    "PartitionCompletion",
    "ReconciliationResult",
    "ScaleCheckpoint",
    "resolve_scale_plan",
    "shard_descriptors",
    "partition_index",
    "partition_id",
    "create_scale_stream_rng",
    "iter_chunks",
    "fingerprint_rows",
    "reconcile_logical_ids",
    "write_checkpoint",
    "load_checkpoint",
    "write_scale_partitions",
    "checkpoint_fingerprint",
]
