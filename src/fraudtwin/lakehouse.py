"""Optional Iceberg lakehouse adapters for generated FraudTwin runs.

The generator remains file-first.  This module provides deterministic batch
backfill and a small Kafka consumer adapter at the integration boundary.  All
Iceberg and Kafka imports are lazy so normal installs remain dependency-free.
"""

import base64
import hashlib
import importlib
import json
import os
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import polars as pl

from fraudtwin.config import LakehouseConfig
from fraudtwin.contracts import AvroContractRegistry, contract_registry
from fraudtwin.kafka import SUBJECTS, publication_records
from fraudtwin.manifest import RunManifest
from fraudtwin.ml import load_generated_run
from fraudtwin.reproducibility import canonical_json, sha256_json


class LakehouseConfigurationError(ValueError):
    """Raised for missing or unsafe lakehouse configuration."""


class LakehouseDependencyError(RuntimeError):
    """Raised when an Iceberg/Kafka integration extra is not installed."""


BRONZE_NAMESPACE = "bronze"
SILVER_NAMESPACE = "silver"
GOLD_NAMESPACE = "gold"
ORACLE_NAMESPACE = "oracle"
CONTRACT_TABLES = {
    "payment-event": "payment_events",
    "customer-dispute": "customer_disputes",
    "fraud-alert": "fraud_alerts",
    "fraud-case": "fraud_cases",
    "fraud-case-confirmation": "case_confirmations",
    "fraud-label": "fraud_labels",
}
TRANSPORT_FIELDS = frozenset(
    {
        "received_at",
        "source",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "raw_payload_b64",
    }
)
ENTITY_TABLES = (
    "institutions",
    "customers",
    "merchants",
    "devices",
    "accounts",
    "cards",
    "pix_keys",
    "network_endpoints",
    "state_history",
)
BEHAVIOR_TABLES = (
    "profiles",
    "payments",
    "ledger_entries",
    "fraud_records",
    "label_observations",
    "final_observed_labels",
)
ORACLE_TABLES = frozenset({"fraud_records", "label_observations"})
CONFLUENT_HEADER_LENGTH = 5


@dataclass(frozen=True)
class LakehouseEnvironment:
    """Non-secret connection settings resolved from environment variables."""

    catalog_uri: str
    warehouse: str
    s3_endpoint: str | None = None
    s3_region: str = "us-east-1"
    s3_access_key: str | None = None
    s3_secret_key: str | None = None
    catalog_name: str = "fraudtwin"

    @classmethod
    def from_environment(cls, config: LakehouseConfig | None = None) -> LakehouseEnvironment:
        settings = config or LakehouseConfig()
        catalog_uri = os.environ.get("FRAUDTWIN_ICEBERG_CATALOG_URI")
        warehouse = os.environ.get("FRAUDTWIN_ICEBERG_WAREHOUSE")
        if not catalog_uri or not warehouse:
            raise LakehouseConfigurationError(
                "lakehouse output requires FRAUDTWIN_ICEBERG_CATALOG_URI and "
                "FRAUDTWIN_ICEBERG_WAREHOUSE"
            )
        return cls(
            catalog_uri=catalog_uri,
            warehouse=warehouse,
            s3_endpoint=os.environ.get("FRAUDTWIN_ICEBERG_S3_ENDPOINT"),
            s3_region=os.environ.get("FRAUDTWIN_ICEBERG_S3_REGION", "us-east-1"),
            s3_access_key=os.environ.get("FRAUDTWIN_ICEBERG_S3_ACCESS_KEY"),
            s3_secret_key=os.environ.get("FRAUDTWIN_ICEBERG_S3_SECRET_KEY"),
            catalog_name=settings.catalog_name,
        )

    def catalog_properties(self) -> dict[str, str]:
        """Return PyIceberg REST/S3 properties resolved from the environment."""

        properties = {
            "type": "rest",
            "uri": self.catalog_uri,
            "warehouse": self.warehouse,
            "s3.region": self.s3_region,
        }
        if self.s3_endpoint:
            properties.update(
                {
                    "s3.endpoint": self.s3_endpoint,
                    "s3.path-style-access": "true",
                }
            )
        if self.s3_access_key:
            properties["s3.access-key-id"] = self.s3_access_key
        if self.s3_secret_key:
            properties["s3.secret-access-key"] = self.s3_secret_key
        return properties


@dataclass(frozen=True)
class BronzeRecord:
    """Canonical raw-record envelope used by both ingestion paths."""

    run_id: str
    source: str
    subject: str
    table_name: str
    record_id: str
    record_key: str | None
    contract_fingerprint: str | None
    payload: dict[str, Any]
    raw_payload_b64: str | None
    headers: dict[str, str]
    event_time: str | None
    received_at: str
    kafka_topic: str | None = None
    kafka_partition: int | None = None
    kafka_offset: int | None = None

    def as_row(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "source": self.source,
            "subject": self.subject,
            "table_name": self.table_name,
            "event_family": self.subject,
            "record_id": self.record_id,
            "record_key": self.record_key,
            "contract_fingerprint": self.contract_fingerprint,
            "payload_json": canonical_json(self.payload),
            "raw_payload_b64": self.raw_payload_b64,
            "headers_json": canonical_json(self.headers),
            "event_time": self.event_time,
            "received_at": self.received_at,
            "kafka_topic": self.kafka_topic,
            "kafka_partition": self.kafka_partition,
            "kafka_offset": self.kafka_offset,
        }


@dataclass(frozen=True)
class LakehouseMaterializationResult:
    """Immutable metadata produced by a batch or streaming materialization."""

    materialization_id: str
    run_id: str
    source_mode: str
    row_counts: dict[str, int]
    logical_fingerprint: str
    snapshots: dict[str, int | None]
    manifest_path: Path | None

    def as_dict(self) -> dict[str, object]:
        return {
            "materialization_id": self.materialization_id,
            "run_id": self.run_id,
            "source_mode": self.source_mode,
            "row_counts": self.row_counts,
            "logical_fingerprint": self.logical_fingerprint,
            "snapshots": self.snapshots,
            "manifest_path": str(self.manifest_path) if self.manifest_path else None,
        }


def _json_value(value: Any) -> Any:
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_json_value(item) for item in value]
    return value


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    return str(value)


def _record_id(payload: Mapping[str, Any], fallback: str) -> str:
    for key, value in payload.items():
        if key.endswith("_id") and value is not None:
            return str(value)
    return fallback


def _event_time(payload: Mapping[str, Any]) -> str | None:
    for key in (
        "source_available_at",
        "event_time",
        "alert_created_at",
        "case_opened_at",
        "confirmed_at",
        "label_available_at",
        "occurred_at",
        "initiated_at",
    ):
        if key in payload and payload[key] is not None:
            return _iso(payload[key])
    return None


def _generic_record(
    run_id: str,
    table_name: str,
    record: Any,
    index: int,
    *,
    source: str = "parquet",
    oracle: bool = False,
) -> BronzeRecord:
    payload = _json_value(record)
    if not isinstance(payload, dict):
        payload = {"value": payload}
    record_id = _record_id(payload, f"{table_name}:{index:012d}")
    return BronzeRecord(
        run_id=run_id,
        source="oracle-parquet" if oracle else source,
        subject=table_name,
        table_name=table_name,
        record_id=record_id,
        record_key=str(payload.get("payment_id")) if payload.get("payment_id") else None,
        contract_fingerprint=None,
        payload=payload,
        raw_payload_b64=None,
        headers={},
        event_time=_event_time(payload),
        received_at=datetime.now(UTC).isoformat(),
    )


def build_bronze_records(
    entities: Any,
    behavior: Any,
    manifest: RunManifest,
    *,
    include_oracle: bool = False,
    registry: AvroContractRegistry | None = None,
    source: str = "parquet",
) -> tuple[BronzeRecord, ...]:
    """Build a deterministic raw envelope from a complete generated run."""

    rows: list[BronzeRecord] = []
    for subject_record in publication_records(behavior, manifest.run_id, registry=registry):
        rows.append(
            BronzeRecord(
                run_id=manifest.run_id,
                source=source,
                subject=subject_record.subject,
                table_name=CONTRACT_TABLES[subject_record.subject],
                record_id=subject_record.record_id,
                record_key=subject_record.key,
                contract_fingerprint=subject_record.fingerprint,
                payload=_json_value(subject_record.datum),
                raw_payload_b64=base64.b64encode(subject_record.value).decode("ascii"),
                headers={key: value.decode("utf-8") for key, value in subject_record.headers},
                event_time=subject_record.observable_time.isoformat(),
                received_at=subject_record.observable_time.isoformat(),
            )
        )
    for table_name in ENTITY_TABLES:
        for index, item in enumerate(getattr(entities, table_name, ())):
            rows.append(_generic_record(manifest.run_id, table_name, item, index))
    for table_name in BEHAVIOR_TABLES:
        values = getattr(behavior, table_name, ())
        is_oracle = table_name in ORACLE_TABLES
        if is_oracle and not include_oracle:
            continue
        output_name = "behavior_profiles" if table_name == "profiles" else table_name
        for index, item in enumerate(values):
            rows.append(
                _generic_record(
                    manifest.run_id,
                    output_name,
                    item,
                    index,
                    oracle=is_oracle,
                )
            )
    return tuple(rows)


def _record_order_key(record: BronzeRecord) -> tuple[object, ...]:
    return (
        record.source,
        record.kafka_topic or "",
        record.kafka_partition if record.kafka_partition is not None else -1,
        record.kafka_offset if record.kafka_offset is not None else -1,
        canonical_json(record.payload),
        record.raw_payload_b64 or "",
    )


def _group_records(records: Iterable[BronzeRecord]) -> dict[tuple[str, str], list[BronzeRecord]]:
    grouped: dict[tuple[str, str], list[BronzeRecord]] = {}
    for record in records:
        grouped.setdefault((record.subject, record.record_id), []).append(record)
    for candidates in grouped.values():
        candidates.sort(key=_record_order_key)
    return grouped


def deduplicate_records(records: Iterable[BronzeRecord]) -> tuple[BronzeRecord, ...]:
    """Apply a stable Silver decision while retaining deterministic winners."""

    grouped = _group_records(records)
    return tuple(grouped[key][0] for key in sorted(grouped))


def silver_rows(records: Iterable[BronzeRecord]) -> tuple[dict[str, Any], ...]:
    """Normalize deduplicated Bronze records without rewriting raw payloads."""

    grouped = _group_records(records)
    result: list[dict[str, Any]] = []
    for key in sorted(grouped):
        candidates = grouped[key]
        record = candidates[0]
        event_time = record.event_time
        result.append(
            {
                **record.as_row(),
                "normalized_event_time": event_time,
                "event_date": event_time[:10] if event_time else None,
                "deduplication_key": f"{record.subject}:{record.record_id}",
                "contributing_bronze_ids": [item.record_id for item in candidates],
                "decision": "accepted",
                "decision_reason": (
                    "first deterministic record for subject and record_id"
                    if len(candidates) == 1
                    else f"deterministic winner among {len(candidates)} Bronze records"
                ),
            }
        )
    return tuple(result)


def logical_fingerprint(rows: Iterable[Mapping[str, Any]]) -> str:
    """Hash logical table content, excluding arrival and broker metadata."""

    canonical = []
    for row in rows:
        canonical.append(
            {key: value for key, value in sorted(row.items()) if key not in TRANSPORT_FIELDS}
        )
    return sha256_json(sorted(canonical, key=canonical_json))


def _require_iceberg() -> tuple[Any, Any, Any]:  # pragma: no cover - optional dependency
    try:
        pa = importlib.import_module("pyarrow")
        load_catalog = importlib.import_module("pyiceberg.catalog").load_catalog
        NoSuchTableError = importlib.import_module("pyiceberg.exceptions").NoSuchTableError
    except ImportError as exc:  # pragma: no cover - optional integration dependency
        raise LakehouseDependencyError(
            "Iceberg output requires the optional 'lakehouse' dependency"
        ) from exc
    return pa, load_catalog, NoSuchTableError


class IcebergLakehouse:  # pragma: no cover - optional integration dependency
    """Small PyIceberg writer with immutable append and snapshot metadata."""

    def __init__(self, environment: LakehouseEnvironment, config: LakehouseConfig | None = None):
        self.environment = environment
        self.config = config or LakehouseConfig(catalog_name=environment.catalog_name)
        self._catalog: Any | None = None

    @property
    def catalog(self) -> Any:
        if self._catalog is None:
            _, load_catalog, _ = _require_iceberg()
            self._catalog = load_catalog(
                self.environment.catalog_name,
                **self.environment.catalog_properties(),
            )
        return self._catalog

    def initialize(self) -> tuple[str, ...]:
        namespaces = tuple(
            f"{self.config.namespace_prefix}_{name}"
            for name in (BRONZE_NAMESPACE, SILVER_NAMESPACE, GOLD_NAMESPACE, ORACLE_NAMESPACE)
        )
        for namespace in namespaces:
            try:
                self.catalog.create_namespace(namespace)
            except Exception as exc:
                if (
                    "already exists" not in str(exc).lower()
                    and "alreadyexists" not in str(exc).lower()
                ):
                    raise
        return namespaces

    def append(self, table: str, rows: Iterable[Mapping[str, Any]]) -> int | None:
        rows_list = [dict(row) for row in rows]
        if not rows_list:
            return None
        pa, _, no_such_table = _require_iceberg()
        namespace, table_name = table.split(".", 1)
        namespace = f"{self.config.namespace_prefix}_{namespace}"
        self.initialize()
        normalized = sorted(
            (
                {
                    key: (
                        json.dumps(value, sort_keys=True, default=str)
                        if isinstance(value, dict | list)
                        else value
                    )
                    for key, value in row.items()
                }
                for row in rows_list
            ),
            key=lambda row: canonical_json(row),
        )
        all_keys = sorted({key for row in normalized for key in row})
        identifier = f"{namespace}.{table_name}"
        try:
            schema = pa.schema([(key, pa.string()) for key in all_keys])
            iceberg_table = self.catalog.load_table(identifier)
        except no_such_table:
            iceberg_table = self.catalog.create_table(identifier, schema=schema)
        else:
            existing_keys = {field.name for field in iceberg_table.schema().fields}
            missing_keys = [key for key in all_keys if key not in existing_keys]
            if missing_keys:
                update = iceberg_table.update_schema()
                for key in missing_keys:
                    update.add_column(key, pa.string())
                update.commit()
                iceberg_table = self.catalog.load_table(identifier)
            all_keys = sorted(existing_keys | set(all_keys))
            schema = pa.schema([(key, pa.string()) for key in all_keys])
        typed_rows = [
            {key: None if row.get(key) is None else str(row.get(key)) for key in all_keys}
            for row in normalized
        ]
        iceberg_table.append(pa.Table.from_pylist(typed_rows, schema=schema))
        snapshot = iceberg_table.current_snapshot()
        return int(snapshot.snapshot_id) if snapshot is not None else None

    def append_stream(
        self,
        table: str,
        rows: Iterable[Mapping[str, Any]],
        *,
        batch_size: int = 10_000,
    ) -> int | None:
        """Append an unbounded row stream in bounded batches."""

        if batch_size < 1:
            raise ValueError("batch_size must be positive")
        batch: list[Mapping[str, Any]] = []
        snapshot: int | None = None
        for row in rows:
            batch.append(row)
            if len(batch) >= batch_size:
                snapshot = self.append(table, batch)
                batch.clear()
        if batch:
            snapshot = self.append(table, batch)
        return snapshot

    def maintenance(
        self,
        table: str,
        operation: str,
        *,
        snapshot_id: int | None = None,
        execute: bool = False,
    ) -> dict[str, object]:
        """Plan or execute one explicitly requested table-maintenance action.

        Dry-run is the default.  Execution is intentionally snapshot-scoped;
        callers must name the snapshot to expire so retained run snapshots
        cannot be removed accidentally.
        """

        supported = {"expire_snapshots", "compact", "remove_orphan_files"}
        if operation not in supported:
            raise LakehouseConfigurationError(
                f"unsupported lakehouse maintenance operation: {operation}"
            )
        plan: dict[str, object] = {
            "table": table,
            "operation": operation,
            "snapshot_id": snapshot_id,
            "execute": execute,
        }
        if not execute:
            plan["dry_run"] = True
            return plan
        if operation == "expire_snapshots" and snapshot_id is None:
            raise LakehouseConfigurationError(
                "expiring snapshots requires --snapshot-id; retained run snapshots are protected"
            )
        if table.count(".") != 1:
            raise LakehouseConfigurationError("table must be a logical namespace.table identifier")
        namespace, table_name = table.split(".", 1)
        identifier = f"{self.config.namespace_prefix}_{namespace}.{table_name}"
        iceberg_table = self.catalog.load_table(identifier)
        if operation == "expire_snapshots":
            manager = getattr(iceberg_table, "manage_snapshots", None)
            if not callable(manager):
                raise LakehouseDependencyError("installed Iceberg client lacks snapshot management")
            assert snapshot_id is not None
            manager().expire_snapshot(snapshot_id).commit()
        else:
            raise LakehouseDependencyError(
                f"{operation} requires the Spark Iceberg maintenance job; "
                "use the local Compose Spark profile"
            )
        plan["dry_run"] = False
        return plan


def _source_checksums(run_dir: Path) -> dict[str, str]:
    return {
        str(path.relative_to(run_dir)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(run_dir.rglob("*"))
        if path.is_file()
        and path.name != "manifest.json"
        and "lakehouse" not in path.relative_to(run_dir).parts
    }


def _validate_source_run(run_dir: Path, manifest: RunManifest) -> None:
    """Validate the immutable source manifest and any checksums it carries."""

    if not manifest.run_id or not manifest.scenario_config_hash:
        raise LakehouseConfigurationError("source run manifest is missing required identity fields")
    expected = manifest.file_checksums
    if not expected:
        return
    actual = _source_checksums(run_dir)
    missing = sorted(set(expected) - set(actual))
    changed = sorted(key for key in expected.keys() & actual.keys() if expected[key] != actual[key])
    if missing or changed:
        details = []
        if missing:
            details.append("missing=" + ",".join(missing))
        if changed:
            details.append("changed=" + ",".join(changed))
        raise LakehouseConfigurationError(
            "source run checksum validation failed: " + "; ".join(details)
        )
    if manifest.output_fingerprint and manifest.output_fingerprint != sha256_json(expected):
        raise LakehouseConfigurationError(
            "source run output fingerprint does not match file checksums"
        )


def _write_immutable_json(path: Path, payload: Mapping[str, Any]) -> None:
    """Write a materialization manifest once and reject conflicting rewrites."""

    serialized = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != serialized:
            raise LakehouseConfigurationError(
                f"immutable lakehouse materialization already exists with different content: {path}"
            )
        return
    path.write_text(serialized, encoding="utf-8")


def _partition_specs(tables: Iterable[str]) -> dict[str, list[str]]:
    return {
        table: (["event_date", "event_family"] if table.startswith("silver.") else ["event_family"])
        for table in tables
    }


def materialize_run(
    run_dir: Path,
    *,
    config: LakehouseConfig | None = None,
    environment: LakehouseEnvironment | None = None,
    write_iceberg: bool = True,
    include_oracle: bool | None = None,
    source_mode: str = "batch",
) -> LakehouseMaterializationResult:
    """Backfill one generated run and optionally commit it to Iceberg."""

    entities, behavior, manifest = load_generated_run(run_dir)
    _validate_source_run(run_dir, manifest)
    return materialize_dataset(
        run_dir,
        entities,
        behavior,
        manifest,
        config=config,
        environment=environment,
        write_iceberg=write_iceberg,
        include_oracle=include_oracle,
        source_mode=source_mode,
    )


def materialize_dataset(
    run_dir: Path,
    entities: Any,
    behavior: Any,
    manifest: RunManifest,
    *,
    config: LakehouseConfig | None = None,
    environment: LakehouseEnvironment | None = None,
    write_iceberg: bool = True,
    include_oracle: bool | None = None,
    source_mode: str = "batch",
) -> LakehouseMaterializationResult:
    """Materialize already-loaded domain objects without regenerating them."""

    settings = config or LakehouseConfig()
    allow_oracle = settings.include_oracle if include_oracle is None else include_oracle
    records = build_bronze_records(entities, behavior, manifest, include_oracle=allow_oracle)
    observable = tuple(item for item in records if item.source != "oracle-parquet")
    oracle = tuple(item for item in records if item.source == "oracle-parquet")
    silver = silver_rows(observable)
    table_rows: dict[str, tuple[Mapping[str, Any], ...]] = {
        "bronze.records": tuple(item.as_row() for item in records),
        "silver.records": silver,
        "gold.customer_features": tuple(
            item.as_row()
            for item in records
            if item.table_name in {"customers", "behavior_profiles"}
        ),
        "gold.fraud_features": tuple(
            item.as_row()
            for item in records
            if item.table_name in {"fraud_alerts", "fraud_cases", "fraud_labels"}
        ),
        "gold.ml_training_snapshots": tuple(
            item.as_row()
            for item in records
            if item.table_name in {"payments", "payment_events", "fraud_labels"}
        ),
        "gold.graph_nodes": (),
        "gold.graph_edges": (),
    }
    dataset_path = run_dir / "ml" / "dataset.parquet"
    if dataset_path.is_file():
        table_rows["gold.ml_training_snapshots"] = tuple(
            {str(key): value for key, value in row.items()}
            for row in pl.read_parquet(dataset_path).to_dicts()
        )
    resolved_configuration = manifest.resolved_configuration
    graph_configuration = (
        resolved_configuration.get("graph") if isinstance(resolved_configuration, dict) else None
    )
    if isinstance(graph_configuration, dict) and graph_configuration.get("enabled", False):
        from fraudtwin.config import SimulationRunConfig
        from fraudtwin.graph import build_graph

        source_config = SimulationRunConfig.model_validate(resolved_configuration)
        graph_dataset = build_graph(
            source_config,
            entities,
            behavior,
            manifest,
            view="observable",
        )
        table_rows["gold.graph_nodes"] = tuple(_json_value(item) for item in graph_dataset.nodes)
        table_rows["gold.graph_edges"] = tuple(_json_value(item) for item in graph_dataset.edges)
    if allow_oracle:
        table_rows["oracle.records"] = tuple(item.as_row() for item in oracle)
    if write_iceberg and environment is None:
        raise LakehouseConfigurationError(
            "an Iceberg environment is required when write_iceberg is enabled"
        )
    writer = IcebergLakehouse(environment, settings) if write_iceberg and environment else None
    snapshots: dict[str, int | None] = {}
    if writer is not None:
        writer.initialize()
        for table, rows in table_rows.items():
            snapshots[table] = writer.append(table, rows)
    all_rows = [row for rows in table_rows.values() for row in rows]
    logical_hash = logical_fingerprint(all_rows)
    source_manifest_hash = sha256_json(manifest.model_dump(mode="json"))
    materialization_id = (
        "LH-"
        + sha256_json(
            {
                "run_id": manifest.run_id,
                "source_mode": source_mode,
                "oracle": allow_oracle,
                "fingerprint": logical_hash,
            }
        )[:16]
    )
    result = LakehouseMaterializationResult(
        materialization_id=materialization_id,
        run_id=manifest.run_id,
        source_mode=source_mode,
        row_counts={table: len(rows) for table, rows in table_rows.items()},
        logical_fingerprint=logical_hash,
        snapshots=snapshots,
        manifest_path=None,
    )
    lakehouse_dir = run_dir / "lakehouse"
    lakehouse_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        **result.as_dict(),
        "source_manifest_hash": source_manifest_hash,
        "source_checksums": _source_checksums(run_dir),
        "include_oracle": allow_oracle,
        "tables": sorted(table_rows),
        "table_identifiers": sorted(table_rows),
        "schema_specs": {
            table: {"columns": sorted(rows[0]) if rows else []}
            for table, rows in table_rows.items()
        },
        "partition_specs": _partition_specs(table_rows),
        "transform_version": "m26.v1",
        "config_version": manifest.config_version,
        "schema_version": "m26.v1",
    }
    manifest_path = lakehouse_dir / f"{materialization_id}.json"
    _write_immutable_json(manifest_path, payload)
    return replace(result, manifest_path=manifest_path)


def verify_materialization(path: Path) -> dict[str, Any]:
    """Validate a materialization manifest without contacting Iceberg."""

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise LakehouseConfigurationError(f"invalid lakehouse manifest: {path}") from exc
    required = {"materialization_id", "run_id", "logical_fingerprint", "snapshots", "tables"}
    missing = sorted(required - set(payload))
    if missing:
        raise LakehouseConfigurationError("lakehouse manifest missing: " + ", ".join(missing))
    return cast(dict[str, Any], payload)


def _decode_kafka_message(message: Any, registry: AvroContractRegistry) -> BronzeRecord:
    if message.error():
        raise LakehouseConfigurationError(str(message.error()))
    headers = {key: (value or b"").decode("utf-8") for key, value in (message.headers() or ())}
    subject = headers.get("fraudtwin-subject")
    if subject not in SUBJECTS:
        raise LakehouseConfigurationError(
            "Kafka message is missing a valid fraudtwin-subject header"
        )
    contract_version = headers.get("fraudtwin-contract-version")
    advertised_fingerprint = headers.get("fraudtwin-contract-fingerprint")
    if not contract_version or not advertised_fingerprint:
        raise LakehouseConfigurationError(
            "Kafka message is missing contract version/fingerprint headers"
        )
    try:
        contract = registry.contract(subject, contract_version)
    except KeyError as exc:
        raise LakehouseConfigurationError(
            f"Kafka message references unknown contract {subject}:{contract_version}"
        ) from exc
    if advertised_fingerprint != contract.canonical_sha256:
        raise LakehouseConfigurationError(
            f"Kafka contract fingerprint mismatch for {subject}:{contract_version}"
        )
    value = message.value() or b""
    if value[:1] != b"\x00" or len(value) < CONFLUENT_HEADER_LENGTH:
        raise LakehouseConfigurationError("Kafka message is not Confluent-framed Avro")
    payload = registry.mapper().decode(subject, value[CONFLUENT_HEADER_LENGTH:])
    return BronzeRecord(
        run_id=headers.get("fraudtwin-run-id", "UNKNOWN"),
        source="kafka",
        subject=subject,
        table_name=CONTRACT_TABLES[subject],
        record_id=headers.get("fraudtwin-record-id", _record_id(payload, "unknown")),
        record_key=(message.key() or b"").decode("utf-8") or None,
        contract_fingerprint=advertised_fingerprint,
        payload=payload,
        raw_payload_b64=base64.b64encode(value).decode("ascii"),
        headers=headers,
        event_time=_event_time(payload),
        received_at=datetime.now(UTC).isoformat(),
        kafka_topic=message.topic(),
        kafka_partition=message.partition(),
        kafka_offset=message.offset(),
    )


def consume_kafka_once(  # pragma: no cover - optional integration dependency
    *,
    environment: LakehouseEnvironment,
    lakehouse: IcebergLakehouse,
    max_messages: int = 100,
    timeout_seconds: float = 5.0,
) -> int:
    """Consume a bounded Kafka batch into immutable Bronze and Silver tables."""

    try:
        confluent_kafka = importlib.import_module("confluent_kafka")
    except ImportError as exc:  # pragma: no cover - optional integration dependency
        raise LakehouseDependencyError(
            "Kafka ingestion requires the optional 'kafka' dependency"
        ) from exc
    consumer_class = vars(confluent_kafka)["Consumer"]
    brokers = os.environ.get("FRAUDTWIN_KAFKA_BOOTSTRAP_SERVERS")
    group = os.environ.get("FRAUDTWIN_LAKEHOUSE_CONSUMER_GROUP", "fraudtwin-lakehouse")
    if not brokers:
        raise LakehouseConfigurationError(
            "Kafka ingestion requires FRAUDTWIN_KAFKA_BOOTSTRAP_SERVERS"
        )
    consumer = consumer_class(
        {
            "bootstrap.servers": brokers,
            "group.id": group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe(
        [
            os.environ.get("FRAUDTWIN_KAFKA_TOPIC_PREFIX", "fraudsim") + "." + suffix
            for suffix in (
                "payment.events.v1",
                "customer.disputes.v1",
                "fraud.alerts.v1",
                "fraud.cases.v1",
                "fraud.case-confirmations.v1",
                "fraud.labels.v1",
            )
        ]
    )
    rows: list[dict[str, Any]] = []
    registry = contract_registry()
    try:
        for _ in range(max_messages):
            message = consumer.poll(timeout_seconds)
            if message is None:
                break
            rows.append(_decode_kafka_message(message, registry).as_row())
    finally:
        consumer.close()
    if rows:
        bronze_snapshot = lakehouse.append("bronze.records", rows)
        silver = silver_rows(_bronze_from_rows(rows))
        silver_snapshot = lakehouse.append("silver.records", silver)
        source_run_ids = sorted({str(row.get("run_id", "UNKNOWN")) for row in rows})
        fingerprint = logical_fingerprint([*rows, *silver])
        materialization_id = (
            "LH-"
            + sha256_json(
                {
                    "source_mode": "kafka",
                    "source_run_ids": source_run_ids,
                    "fingerprint": fingerprint,
                    "snapshots": {
                        "bronze.records": bronze_snapshot,
                        "silver.records": silver_snapshot,
                    },
                }
            )[:16]
        )
        manifest_dir = Path(lakehouse.config.checkpoint_location) / "materializations"
        manifest_dir.mkdir(parents=True, exist_ok=True)
        manifest_path = manifest_dir / f"{materialization_id}.json"
        payload = {
            "materialization_id": materialization_id,
            "run_id": source_run_ids[0] if len(source_run_ids) == 1 else "STREAM",
            "source_run_ids": source_run_ids,
            "source_mode": "kafka",
            "source_manifest_hash": None,
            "source_checksums": {},
            "transform_version": "m26.v1",
            "config_version": "1",
            "tables": ["bronze.records", "silver.records"],
            "table_identifiers": ["bronze.records", "silver.records"],
            "partition_specs": _partition_specs(("bronze.records", "silver.records")),
            "row_counts": {"bronze.records": len(rows), "silver.records": len(silver)},
            "logical_fingerprint": fingerprint,
            "snapshots": {"bronze.records": bronze_snapshot, "silver.records": silver_snapshot},
            "schema_version": "m26.v1",
        }
        _write_immutable_json(manifest_path, payload)
    return len(rows)


def _bronze_from_rows(  # pragma: no cover - optional integration dependency
    rows: Iterable[Mapping[str, Any]],
) -> tuple[BronzeRecord, ...]:
    result = []
    for row in rows:
        payload = json.loads(str(row.get("payload_json", "{}")))
        result.append(
            BronzeRecord(
                run_id=str(row.get("run_id", "UNKNOWN")),
                source=str(row.get("source", "kafka")),
                subject=str(row.get("subject", "")),
                table_name=str(row.get("table_name", "")),
                record_id=str(row.get("record_id", "")),
                record_key=row.get("record_key"),
                contract_fingerprint=row.get("contract_fingerprint"),
                payload=payload,
                raw_payload_b64=row.get("raw_payload_b64"),
                headers=json.loads(str(row.get("headers_json", "{}"))),
                event_time=row.get("event_time"),
                received_at=str(row.get("received_at", "")),
            )
        )
    return tuple(result)


__all__ = [
    "BronzeRecord",
    "IcebergLakehouse",
    "LakehouseConfigurationError",
    "LakehouseDependencyError",
    "LakehouseEnvironment",
    "LakehouseMaterializationResult",
    "build_bronze_records",
    "consume_kafka_once",
    "deduplicate_records",
    "logical_fingerprint",
    "materialize_run",
    "silver_rows",
    "verify_materialization",
]
