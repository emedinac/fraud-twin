"""Deterministic chunk execution, sharding, and reconciliation for M18.

The scale layer consumes canonical row iterators and owns partitioning,
bounded-memory spooling, persistence, and checkpoint metadata. It does not
contain a second simulation model.
"""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
import os
import platform
import resource
import sqlite3
import sys
import time
from collections.abc import Iterable, Iterator, Mapping
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from random import Random
from typing import Any, cast

import polars as pl
from pydantic import BaseModel, ConfigDict, Field

from fraudtwin.config import (
    DEFAULT_SCALE_FEATURES,
    PartitionMapping,
    ScaleFeature,
    ScaleProfile,
    ScaleStateBackend,
    ScaleStorageBackend,
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
    target_payments: int = Field(default=0, ge=0)
    seed_tree_version: str = SEED_TREE_VERSION
    features: tuple[ScaleFeature, ...] = DEFAULT_SCALE_FEATURES
    storage_backend: ScaleStorageBackend = "local"
    storage_uri: str | None = None
    state_backend: ScaleStateBackend = "duckdb"
    manifest_version: str = "M18-scale-1"


def validate_scale_feature_matrix(config: SimulationRunConfig) -> tuple[ScaleFeature, ...]:
    """Validate optional scale stages before any generation work starts."""

    features = config.scale.features
    if "graph" in features and not config.graph.enabled:
        raise ValueError("scale feature 'graph' requires graph.enabled")
    if "pit" in features and not config.dataset.enabled:
        raise ValueError("scale feature 'pit' requires dataset.enabled")
    if "backtest" in features and not config.backtest.regimes:
        raise ValueError("scale feature 'backtest' requires backtest configuration")
    return features


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


class ChunkCompletion(BaseModel):
    """Fingerprint and location for one atomically completed chunk."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    shard_id: str
    logical_type: str = "records"
    chunk_index: int = Field(ge=0)
    start_ordinal: int = Field(ge=0)
    end_ordinal: int = Field(ge=0)
    row_count: int = Field(ge=0)
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
    ledger_debit_total: float = 0.0
    ledger_credit_total: float = 0.0
    ledger_balanced: bool = True
    shard_ledger_totals: dict[str, dict[str, float]] = Field(default_factory=dict)
    transfer_reconciliation_records: int = Field(default=0, ge=0)
    account_balance_violations: int = Field(default=0, ge=0)
    accounts_reconciled: int = Field(default=0, ge=0)
    valid: bool
    checks: dict[str, bool] = Field(default_factory=dict)


class ScaleCheckpoint(BaseModel):
    """Serializable checkpoint manifest for deterministic resume."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    checkpoint_version: str = "M18-checkpoint-4"
    run_id: str
    run_dir: str
    configuration_hash: str
    resolved_configuration: dict[str, Any]
    seed: int = Field(ge=0)
    seed_tree_version: str = SEED_TREE_VERSION
    plan: ScalePlan
    completed_partitions: tuple[PartitionCompletion, ...] = ()
    completed_chunks: tuple[ChunkCompletion, ...] = ()
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
    validate_scale_feature_matrix(config)
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
        target_payments=config.scale.resolved_target_payments or 0,
        features=config.scale.features,
        storage_backend=config.scale.storage_backend,
        storage_uri=config.scale.storage_uri,
        state_backend=config.scale.state_backend,
        manifest_version=config.scale.manifest_version,
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


def aggregate_fingerprint(rows: Iterable[Mapping[str, Any]]) -> str:
    """Hash rows incrementally without retaining the complete input."""

    digest = hashlib.sha256()
    for row in rows:
        encoded = json.dumps(
            dict(row), sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
    return digest.hexdigest()


def iter_payment_ranges(
    target_payments: int, shard_count: int
) -> Iterator[tuple[str, int, int, int]]:
    """Yield deterministic contiguous payment ordinal ranges per shard."""

    if target_payments < 0 or shard_count < 1:
        raise ValueError("target_payments must be non-negative and shard_count positive")
    for index in range(shard_count):
        start = (target_payments * index) // shard_count
        end = (target_payments * (index + 1)) // shard_count
        yield f"SHARD-{index:06d}", index, start, end


def chunk_payment_ranges(
    target_payments: int, shard_count: int, chunk_size: int
) -> Iterator[tuple[str, int, int, int]]:
    """Yield stable shard-local payment chunks without materializing IDs."""

    if chunk_size < 1:
        raise ValueError("chunk_size must be positive")
    for shard_id, _, start, end in iter_payment_ranges(target_payments, shard_count):
        for chunk_index, offset in enumerate(range(start, end, chunk_size)):
            yield shard_id, chunk_index, offset, min(offset + chunk_size, end)


def iter_partition_rows(
    run_dir: str | Path,
    *,
    shard_id: str | None = None,
    columns: list[str] | None = None,
) -> Iterator[dict[str, Any]]:
    """Read scale chunks one file at a time, keeping reader memory bounded."""

    pattern = f"{shard_id}/**/chunk-*.parquet" if shard_id else "SHARD-*/**/chunk-*.parquet"
    scale_root = Path(run_dir) / "scale"
    # Prefer the current shard layout.  Fall back to the legacy partition
    # layout only when no current output exists, avoiding duplicate reads when
    # a run directory contains artifacts from both formats.
    roots = (scale_root / "shards", scale_root / "partitions")
    selected_root = next((root for root in roots if root.exists()), None)
    if selected_root is None:
        return
    for path in sorted(selected_root.glob(pattern)):
        yield from pl.read_parquet(path, columns=columns).iter_rows(named=True)


def iter_partition_table(
    run_dir: str | Path,
    logical_type: str,
    *,
    shard_id: str | None = None,
    columns: list[str] | None = None,
) -> Iterator[dict[str, Any]]:
    """Yield rows for one logical table from partitioned scale output.

    This is intentionally lazy and keeps the existing in-memory readers
    untouched.  Consumers that can operate out-of-core should use this helper
    instead of loading a complete generated run.
    """

    if not logical_type:
        raise ValueError("logical_type must not be empty")
    # Keep the discriminator available for filtering even when callers ask
    # for a projected column set.  Return only the requested columns after
    # filtering so projection remains useful for out-of-core consumers.
    read_columns = columns
    if columns is not None and "logical_type" not in columns:
        read_columns = [*columns, "logical_type"]
    for row in iter_partition_rows(run_dir, shard_id=shard_id, columns=read_columns):
        if row.get("logical_type") != logical_type:
            continue
        if columns is None:
            yield row
        else:
            yield {name: row[name] for name in columns if name in row}


def iter_partition_query(
    run_dir: str | Path,
    query: str,
    *,
    shard_id: str | None = None,
    batch_size: int = 10_000,
) -> Iterator[dict[str, Any]]:
    """Execute an out-of-core DuckDB query over partitioned Parquet.

    ``query`` must reference ``{source}``, which is replaced by a
    ``read_parquet`` relation.  Results are yielded in bounded record batches;
    DuckDB is imported lazily so the core generator has no dependency on it.
    """

    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    try:
        import duckdb  # type: ignore[import-not-found]
    except ImportError as exc:  # pragma: no cover - depends on optional extra
        raise RuntimeError("DuckDB support requires the optional 'scale' dependency extra") from exc
    scale_root = Path(run_dir) / "scale"
    selected_root = next(
        (root for root in (scale_root / "shards", scale_root / "partitions") if root.exists()),
        None,
    )
    if selected_root is None:
        return
    pattern = f"{shard_id}/**/chunk-*.parquet" if shard_id else "SHARD-*/**/chunk-*.parquet"
    source = (selected_root / pattern).as_posix()
    if "{source}" not in query:
        raise ValueError("query must contain the {source} relation placeholder")
    relation = f"read_parquet('{source}')"
    connection = duckdb.connect()
    try:
        reader = connection.execute(query.replace("{source}", relation)).fetch_record_batch(
            rows_per_batch=batch_size
        )
        for batch in reader:
            yield from batch.to_pylist()
    finally:
        connection.close()


def write_scale_benchmark_manifest(
    path: str | Path,
    *,
    target_payments: int,
    realized_counts: Mapping[str, int],
    configuration_hash: str,
    seed: int,
    shard_count: int,
    worker_count: int,
    elapsed_seconds: float,
    resume: Mapping[str, Any] | None = None,
    output_dir: str | Path | None = None,
) -> Path:
    """Write non-deterministic machine evidence separately from run identity."""

    output_bytes = None
    if output_dir is not None:
        output_bytes = sum(
            item.stat().st_size for item in Path(output_dir).rglob("*") if item.is_file()
        )
    payload = {
        "version": "M18-benchmark-1",
        "target_unit": "payments",
        "target_payments": target_payments,
        "realized_counts": dict(realized_counts),
        "target_met": realized_counts.get("payments", 0) >= target_payments,
        "configuration_hash": configuration_hash,
        "seed": seed,
        "shard_count": shard_count,
        "worker_count": worker_count,
        "elapsed_seconds": elapsed_seconds,
        "throughput_payments_per_second": (
            realized_counts.get("payments", 0) / elapsed_seconds if elapsed_seconds > 0 else None
        ),
        "peak_rss_mb": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0,
        "output_bytes": output_bytes,
        "host": {
            "platform": platform.platform(),
            "python": sys.version,
            "cpu_count": os.cpu_count(),
            "memory_bytes": _host_memory_bytes(),
            "packages": _package_versions(),
        },
        "resume": dict(resume or {}),
        "recorded_at_epoch": time.time(),
    }
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, destination)
    return destination


def _host_memory_bytes() -> int | None:
    """Return host memory when available without adding a runtime dependency."""

    try:
        pages = os.sysconf("SC_PHYS_PAGES")
        page_size = os.sysconf("SC_PAGE_SIZE")
    except (AttributeError, OSError, ValueError):
        return None
    return int(pages * page_size) if pages > 0 and page_size > 0 else None


def _package_versions() -> dict[str, str]:
    versions: dict[str, str] = {}
    for name in ("fraudtwin", "polars", "pyarrow", "duckdb"):
        try:
            versions[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            continue
    return versions


def _file_checksum(path: Path) -> str:
    """Return a streaming SHA-256 checksum for one output chunk."""

    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _checkpoint_files_valid(run_root: Path, checkpoint: ScaleCheckpoint) -> bool:
    """Validate completed chunk files before reusing checkpoint state."""

    for completion in checkpoint.completed_chunks:
        path = run_root / completion.path
        if not path.is_file():
            return False
        # M18-checkpoint-2/3 stored the logical fingerprint as ``checksum``;
        # those artifacts remain readable, while current checkpoints verify
        # the physical file bytes.
        if (
            completion.checksum != completion.fingerprint
            and _file_checksum(path) != completion.checksum
        ):
            return False
    return True


def run_scale_benchmark(
    config: SimulationRunConfig,
    *,
    output_dir: str | Path,
    checkpoint_dir: str | Path | None = None,
    evidence_dir: str | Path | None = None,
) -> Path:
    """Run one explicitly requested scale job and publish machine evidence.

    This is intentionally a manual-job API.  It does not change the normal
    generation path or make large profiles part of unit/CI tests.
    """

    if not config.scale.enabled:
        raise ValueError("scale benchmark requires an enabled scale profile")
    from fraudtwin.generation import generate_scale

    started = time.perf_counter()
    result = generate_scale(
        config,
        output_dir=output_dir,
        checkpoint_dir=checkpoint_dir,
    )
    elapsed = time.perf_counter() - started
    scale = result.manifest.scale or {}
    target = int(
        cast(int, scale.get("target_payments", config.scale.resolved_target_payments or 0))
    )
    destination_root = Path(evidence_dir) if evidence_dir is not None else Path(output_dir)
    return write_scale_benchmark_manifest(
        destination_root / f"{result.run_id}-benchmark.json",
        target_payments=target,
        realized_counts=result.manifest.event_counts,
        configuration_hash=result.manifest.scenario_config_hash,
        seed=config.simulation.seed,
        shard_count=config.scale.shard_count,
        worker_count=config.scale.worker_count,
        elapsed_seconds=elapsed,
        resume={"checkpoint": scale.get("checkpoint"), "completed": True},
        output_dir=result.run_dir,
    )


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
        if (
            checkpoint.integrity_hash is not None
            and checkpoint.integrity_hash != checkpoint_fingerprint(checkpoint)
        ):
            raise ValueError("checkpoint integrity hash mismatch")
        return checkpoint
    except (OSError, ValueError) as exc:
        raise ValueError(f"invalid scale checkpoint: {source}") from exc


def _group_chunks(chunks: Iterable[ChunkCompletion]) -> tuple[tuple[ChunkCompletion, ...], ...]:
    """Group chunks by shard/table in canonical ordinal order."""

    grouped: dict[tuple[str, str], list[ChunkCompletion]] = {}
    for chunk in chunks:
        grouped.setdefault((chunk.shard_id, chunk.logical_type), []).append(chunk)
    return tuple(
        tuple(sorted(values, key=lambda item: item.chunk_index))
        for _, values in sorted(grouped.items())
    )


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
    An optional ``partition_key`` keeps account-local records together; worker
    execution is intentionally not represented in the output contract.
    """

    run_root = Path(run_dir)
    root = run_root / "scale" / "shards"
    spool_root = run_root / "scale" / ".spool"
    root.mkdir(parents=True, exist_ok=True)
    spool_root.mkdir(parents=True, exist_ok=True)
    descriptors = shard_descriptors(plan)
    checkpoint_root = Path(checkpoint_dir) if checkpoint_dir is not None else run_root / "scale"
    prior_chunks: dict[tuple[str, str, int], ChunkCompletion] = {}
    prior_checkpoint_model: ScaleCheckpoint | None = None
    prior_checkpoint = checkpoint_root / "checkpoint.json"
    if prior_checkpoint.is_file():
        try:
            checkpoint = load_checkpoint(prior_checkpoint)
            if (
                checkpoint.configuration_hash == plan.configuration_hash
                and checkpoint.seed == plan.seed
                and checkpoint.seed_tree_version == plan.seed_tree_version
                and checkpoint.plan == plan
            ):
                prior_checkpoint_model = checkpoint
                prior_chunks = {
                    (item.shard_id, item.logical_type, item.chunk_index): item
                    for item in checkpoint.completed_chunks
                }
        except ValueError:
            # A corrupt or legacy checkpoint is never trusted for reuse; the
            # current run will rewrite the affected chunks deterministically.
            prior_chunks = {}

    # A completed, integrity-checked run is already resumable and must not be
    # regenerated.  This fast path is especially important for billion-row
    # jobs where a resume invocation should do no producer work at all.
    if (
        prior_checkpoint_model is not None
        and prior_checkpoint_model.reconciliation is not None
        and prior_checkpoint_model.reconciliation.valid
        and prior_checkpoint_model.completed_partitions
        and _checkpoint_files_valid(run_root, prior_checkpoint_model)
    ):
        return (
            prior_checkpoint_model.completed_partitions,
            prior_checkpoint_model.reconciliation,
            prior_checkpoint,
        )

    # Chunk markers are written immediately after each atomic Parquet rename.
    # They make an interrupted run resumable even when the final checkpoint
    # has not been published yet.
    marker_root = checkpoint_root / "chunks"
    if marker_root.exists():
        for marker in marker_root.glob("SHARD-*/**/chunk-*.json"):
            try:
                completion = ChunkCompletion.model_validate_json(marker.read_text(encoding="utf-8"))
                chunk_path = run_root / completion.path
                if chunk_path.is_file():
                    prior_chunks[
                        (completion.shard_id, completion.logical_type, completion.chunk_index)
                    ] = completion
            except (OSError, ValueError):
                continue

    # Route input rows to disk first.  This keeps the scale writer's resident
    # memory bounded even when the producer is a billion-row iterator.  The
    # completed spool marker is deliberately retained until all chunks are
    # committed, so a process interrupted during chunk writing can resume
    # without regenerating or re-spooling the producer input.
    spool_manifest = spool_root / "manifest.json"
    spool_identity = {
        "configuration_hash": plan.configuration_hash,
        "seed": plan.seed,
        "seed_tree_version": plan.seed_tree_version,
        "run_id": plan.run_id,
        "shard_count": plan.shard_count,
    }
    reused_spool = False
    total_rows = 0
    ledger_debit_total = Decimal("0")
    ledger_credit_total = Decimal("0")
    shard_ledger_totals: dict[str, dict[str, Decimal]] = {}
    transfer_reconciliation_records = 0
    account_balance_violations = 0
    accounts_reconciled = 0
    duplicate_logical_ids: list[str] = []
    duplicate_count = 0
    if spool_manifest.is_file():
        try:
            payload = json.loads(spool_manifest.read_text(encoding="utf-8"))
            reused_spool = payload.get("identity") == spool_identity and all(
                (spool_root / f"{item.shard_id}.jsonl").is_file() for item in descriptors
            )
            if reused_spool:
                total_rows = int(payload["total_rows"])
                ledger_debit_total = Decimal(str(payload["ledger_debit_total"]))
                ledger_credit_total = Decimal(str(payload["ledger_credit_total"]))
                shard_ledger_totals = {
                    str(shard): {
                        "debit": Decimal(str(values.get("debit", 0))),
                        "credit": Decimal(str(values.get("credit", 0))),
                    }
                    for shard, values in payload.get("shard_ledger_totals", {}).items()
                }
                transfer_reconciliation_records = int(
                    payload.get("transfer_reconciliation_records", 0)
                )
                account_balance_violations = int(payload.get("account_balance_violations", 0))
                accounts_reconciled = int(payload.get("accounts_reconciled", 0))
                duplicate_count = int(payload.get("duplicate_count", 0))
                duplicate_logical_ids = [
                    str(item) for item in payload.get("duplicate_logical_ids", [])
                ]
        except (OSError, TypeError, ValueError, json.JSONDecodeError):
            reused_spool = False
    if not reused_spool:
        (spool_root / "logical-ids.sqlite").unlink(missing_ok=True)
        (spool_root / "ledger-state.sqlite").unlink(missing_ok=True)
        id_database = sqlite3.connect(spool_root / "logical-ids.sqlite")
        id_database.execute("CREATE TABLE IF NOT EXISTS logical_ids (logical_id TEXT PRIMARY KEY)")
        id_database.execute("PRAGMA synchronous=OFF")
        ledger_database = sqlite3.connect(spool_root / "ledger-state.sqlite")
        ledger_database.execute(
            "CREATE TABLE IF NOT EXISTS ledger_state "
            "(account_id TEXT PRIMARY KEY, balance REAL NOT NULL, entries INTEGER NOT NULL)"
        )
        ledger_database.execute("PRAGMA synchronous=OFF")
        handles = {
            item.shard_id: (spool_root / f"{item.shard_id}.jsonl").open("w", encoding="utf-8")
            for item in descriptors
        }
        try:
            for record in records:
                row = dict(record)
                logical_id = str(row["logical_id"])
                inserted = id_database.execute(
                    "INSERT OR IGNORE INTO logical_ids(logical_id) VALUES (?)", (logical_id,)
                ).rowcount
                if inserted == 0:
                    duplicate_count += 1
                    if len(duplicate_logical_ids) < 100:
                        duplicate_logical_ids.append(logical_id)
                # Account-local rows carry a stable partition key.  Legacy callers
                # may omit it, in which case the logical ID remains the fallback.
                owner_key = str(row.get("partition_key", logical_id))
                row["partition_id"] = partition_id(owner_key, plan.shard_count)
                if row.get("logical_type") == "ledger_entries":
                    amount = Decimal(str(row.get("amount", 0)))
                    account_id = str(row.get("account_id", ""))
                    previous = ledger_database.execute(
                        "SELECT balance FROM ledger_state WHERE account_id = ?", (account_id,)
                    ).fetchone()
                    if previous is not None:
                        delta = amount if row.get("entry_type") == "CREDIT" else -amount
                        expected = round(float(previous[0]) + float(delta), 2)
                        actual = round(float(row.get("balance_after", 0)), 2)
                        if expected != actual:
                            account_balance_violations += 1
                        ledger_database.execute(
                            "UPDATE ledger_state SET balance = ?, entries = entries + 1 "
                            "WHERE account_id = ?",
                            (actual, account_id),
                        )
                    else:
                        accounts_reconciled += 1
                        ledger_database.execute(
                            "INSERT INTO ledger_state(account_id, balance, entries) "
                            "VALUES (?, ?, 1)",
                            (account_id, float(row.get("balance_after", 0))),
                        )
                    shard_totals = shard_ledger_totals.setdefault(
                        row["partition_id"], {"debit": Decimal("0"), "credit": Decimal("0")}
                    )
                    if row.get("entry_type") == "DEBIT":
                        ledger_debit_total += amount
                        shard_totals["debit"] += amount
                    elif row.get("entry_type") == "CREDIT":
                        ledger_credit_total += amount
                        shard_totals["credit"] += amount
                if row.get("logical_type") == "transfer_reconciliation":
                    transfer_reconciliation_records += 1
                handles[row["partition_id"]].write(
                    json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n"
                )
                total_rows += 1
                if total_rows % 10_000 == 0:
                    id_database.commit()
                    ledger_database.commit()
        finally:
            for handle in handles.values():
                handle.close()
            id_database.commit()
            id_database.close()
            ledger_database.commit()
            ledger_database.close()
        spool_payload = {
            "identity": spool_identity,
            "total_rows": total_rows,
            "ledger_debit_total": str(ledger_debit_total),
            "ledger_credit_total": str(ledger_credit_total),
            "shard_ledger_totals": {
                shard: {name: str(amount) for name, amount in totals.items()}
                for shard, totals in shard_ledger_totals.items()
            },
            "transfer_reconciliation_records": transfer_reconciliation_records,
            "duplicate_count": duplicate_count,
            "duplicate_logical_ids": duplicate_logical_ids,
            "account_balance_violations": account_balance_violations,
            "accounts_reconciled": accounts_reconciled,
        }
        temporary = spool_manifest.with_suffix(".json.tmp")
        temporary.write_text(json.dumps(spool_payload, sort_keys=True) + "\n", encoding="utf-8")
        os.replace(temporary, spool_manifest)

    def write_partition(
        descriptor: ShardDescriptor,
    ) -> tuple[PartitionCompletion, tuple[ChunkCompletion, ...]]:
        shard_root = root / descriptor.shard_id
        shard_root.mkdir(parents=True, exist_ok=True)
        source = spool_root / f"{descriptor.shard_id}.jsonl"
        row_count = 0
        chunk_completions: list[ChunkCompletion] = []
        buffers: dict[str, list[dict[str, Any]]] = {}
        type_row_counts: dict[str, int] = {}
        type_chunk_indices: dict[str, int] = {}

        def flush(logical_type: str) -> None:
            nonlocal row_count
            partition_rows = buffers.get(logical_type, [])
            if not partition_rows:
                return
            start = type_row_counts.get(logical_type, 0)
            end = start + len(partition_rows)
            chunk_index = type_chunk_indices.get(logical_type, 0)
            table_root = shard_root / logical_type
            table_root.mkdir(parents=True, exist_ok=True)
            path = table_root / f"chunk-{chunk_index:06d}.parquet"
            temporary = path.with_suffix(path.suffix + ".tmp")
            prior = prior_chunks.get((descriptor.shard_id, logical_type, chunk_index))
            if (
                prior is not None
                and path.is_file()
                and prior.start_ordinal == start
                and prior.end_ordinal == end
                and prior.row_count == len(partition_rows)
                and prior.path == str(path.relative_to(run_root))
                # New checkpoints carry a file checksum.  Older M18
                # checkpoints used the logical fingerprint in this field;
                # accept those for compatibility but never trust a mismatched
                # current file.
                and (prior.checksum == prior.fingerprint or prior.checksum == _file_checksum(path))
            ):
                chunk_completions.append(prior)
            else:
                frame = pl.DataFrame(partition_rows)
                frame.write_parquet(temporary)
                os.replace(temporary, path)
                fingerprint = aggregate_fingerprint(partition_rows)
                chunk_completions.append(
                    ChunkCompletion(
                        shard_id=descriptor.shard_id,
                        logical_type=logical_type,
                        chunk_index=chunk_index,
                        start_ordinal=start,
                        end_ordinal=end,
                        row_count=len(partition_rows),
                        fingerprint=fingerprint,
                        checksum=_file_checksum(path),
                        path=str(path.relative_to(run_root)),
                    )
                )
                marker = (
                    marker_root
                    / descriptor.shard_id
                    / logical_type
                    / f"chunk-{chunk_index:06d}.json"
                )
                marker.parent.mkdir(parents=True, exist_ok=True)
                marker_tmp = marker.with_suffix(marker.suffix + ".tmp")
                marker_tmp.write_text(
                    chunk_completions[-1].model_dump_json() + "\n", encoding="utf-8"
                )
                os.replace(marker_tmp, marker)
            row_count += len(partition_rows)
            type_row_counts[logical_type] = end
            type_chunk_indices[logical_type] = chunk_index + 1
            buffers[logical_type] = []

        if source.exists():
            with source.open(encoding="utf-8") as stream:
                for line in stream:
                    row = json.loads(line)
                    logical_type = "".join(
                        char if char.isalnum() or char in "_-" else "_"
                        for char in str(row.get("logical_type") or "records")
                    )
                    buffer = buffers.setdefault(logical_type, [])
                    buffer.append(row)
                    if len(buffer) >= plan.chunk_size:
                        flush(logical_type)
        for logical_type in sorted(buffers):
            flush(logical_type)
        partition_fingerprint = aggregate_fingerprint(
            item for chunk in chunk_completions for item in (chunk.model_dump(mode="json"),)
        )
        completion = PartitionCompletion(
            shard_id=descriptor.shard_id,
            row_count=row_count,
            chunk_count=len(chunk_completions),
            fingerprint=partition_fingerprint,
            checksum=partition_fingerprint,
            path=str(shard_root.relative_to(run_root)),
        )
        return completion, tuple(chunk_completions)

    with ThreadPoolExecutor(max_workers=min(plan.worker_count, plan.shard_count)) as executor:
        futures = [executor.submit(write_partition, item) for item in descriptors]
        results = [future.result() for future in futures]
    completions = [item[0] for item in results]
    chunks = tuple(chunk for _, values in results for chunk in values)
    emitted_rows = sum(item.row_count for item in completions)
    for spool_file in spool_root.glob("*.jsonl"):
        spool_file.unlink()
    spool_manifest.unlink(missing_ok=True)
    (spool_root / "logical-ids.sqlite").unlink(missing_ok=True)
    (spool_root / "ledger-state.sqlite").unlink(missing_ok=True)
    spool_root.rmdir()

    # Exact ID reconciliation remains available as a small-run API.  The
    # streaming writer validates completeness by row count and chunk ranges;
    # retaining every ID here would defeat bounded-memory execution.
    reconciliation = ReconciliationResult(
        logical_row_count=total_rows,
        partition_row_count=emitted_rows,
        duplicate_logical_ids=tuple(sorted(set(duplicate_logical_ids))),
        ledger_debit_total=float(ledger_debit_total),
        ledger_credit_total=float(ledger_credit_total),
        ledger_balanced=ledger_debit_total == ledger_credit_total,
        shard_ledger_totals={
            shard_id: {name: float(amount) for name, amount in totals.items()}
            for shard_id, totals in shard_ledger_totals.items()
        },
        transfer_reconciliation_records=transfer_reconciliation_records,
        account_balance_violations=account_balance_violations,
        accounts_reconciled=accounts_reconciled,
        valid=(
            duplicate_count == 0
            and total_rows == emitted_rows
            and account_balance_violations == 0
            and ledger_debit_total == ledger_credit_total
            and all(chunk.end_ordinal - chunk.start_ordinal == chunk.row_count for chunk in chunks)
            and all(
                grouped[0].start_ordinal == 0
                and all(
                    chunk.end_ordinal - chunk.start_ordinal == chunk.row_count for chunk in grouped
                )
                and all(
                    chunk.start_ordinal == previous.end_ordinal
                    for previous, chunk in zip(grouped, grouped[1:], strict=False)
                )
                for grouped in _group_chunks(chunks)
            )
            and all(
                chunk.start_ordinal == previous.end_ordinal
                for grouped in _group_chunks(chunks)
                for previous, chunk in zip(grouped, grouped[1:], strict=False)
            )
        ),
        checks={
            "row_counts": total_rows == emitted_rows,
            "unique_logical_ids": duplicate_count == 0,
            "account_balance_continuity": account_balance_violations == 0,
            "chunk_ranges": all(
                grouped[0].start_ordinal == 0
                and all(
                    chunk.end_ordinal - chunk.start_ordinal == chunk.row_count for chunk in grouped
                )
                and all(
                    chunk.start_ordinal == previous.end_ordinal
                    for previous, chunk in zip(grouped, grouped[1:], strict=False)
                )
                for grouped in _group_chunks(chunks)
            )
            and all(chunk.end_ordinal - chunk.start_ordinal == chunk.row_count for chunk in chunks)
            and all(
                chunk.start_ordinal == previous.end_ordinal
                for grouped in _group_chunks(chunks)
                for previous, chunk in zip(grouped, grouped[1:], strict=False)
            ),
            "ledger_double_entry": ledger_debit_total == ledger_credit_total,
        },
    )
    checkpoint = ScaleCheckpoint(
        run_id=plan.run_id,
        run_dir=str(Path(run_dir).resolve()),
        configuration_hash=plan.configuration_hash,
        resolved_configuration=resolved_configuration or {},
        seed=plan.seed,
        plan=plan,
        completed_partitions=tuple(completions),
        completed_chunks=chunks,
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
    # M18-checkpoint-2 predates table-qualified chunks and ledger aggregate
    # fields.  Preserve its original integrity calculation while accepting
    # those artifacts as readable inputs.
    if checkpoint.checkpoint_version in {"M18-checkpoint-2", "M18-checkpoint-3"}:
        for item in payload.get("completed_chunks", []):
            if (
                checkpoint.checkpoint_version == "M18-checkpoint-2"
                and item.get("logical_type") == "records"
            ):
                item.pop("logical_type", None)
        reconciliation = payload.get("reconciliation")
        if isinstance(reconciliation, dict):
            reconciliation.pop("shard_ledger_totals", None)
            reconciliation.pop("transfer_reconciliation_records", None)
            reconciliation.pop("account_balance_violations", None)
            reconciliation.pop("accounts_reconciled", None)
            if checkpoint.checkpoint_version == "M18-checkpoint-2":
                reconciliation.pop("ledger_debit_total", None)
                reconciliation.pop("ledger_credit_total", None)
                reconciliation.pop("ledger_balanced", None)
    return payload


__all__ = [
    "SEED_TREE_VERSION",
    "ScalePlan",
    "ShardDescriptor",
    "ChunkDescriptor",
    "PartitionCompletion",
    "ChunkCompletion",
    "ReconciliationResult",
    "ScaleCheckpoint",
    "resolve_scale_plan",
    "shard_descriptors",
    "partition_index",
    "partition_id",
    "create_scale_stream_rng",
    "iter_chunks",
    "fingerprint_rows",
    "aggregate_fingerprint",
    "iter_payment_ranges",
    "chunk_payment_ranges",
    "iter_partition_rows",
    "iter_partition_table",
    "iter_partition_query",
    "write_scale_benchmark_manifest",
    "run_scale_benchmark",
    "reconcile_logical_ids",
    "write_checkpoint",
    "load_checkpoint",
    "write_scale_partitions",
    "checkpoint_fingerprint",
]
