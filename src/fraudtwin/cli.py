import json
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Annotated, Literal, cast

import polars as pl
import typer

from fraudtwin.benchmark import (
    BenchmarkRequest,
    SuiteName,
    load_public_pack,
    run_benchmark,
    run_public_benchmark,
    verify_public_benchmark,
)
from fraudtwin.calibration import (
    fit_calibration_profile,
    load_reference_data,
    write_calibration_profile,
)
from fraudtwin.config import (
    SimulationRunConfig,
    _default_config_text,
    config_hash,
    load_config,
)
from fraudtwin.contracts import ContractValidationError, load_contract_registry
from fraudtwin.domain import Account, LedgerEntry, Payment, PaymentEvent, validate_ledger
from fraudtwin.errors import GenerationError
from fraudtwin.generation import generate as generate_library
from fraudtwin.generation import generate_scale as generate_scale_library
from fraudtwin.generation import resume_generation
from fraudtwin.graph import GraphDataset, build_graph, validate_graph, write_graph
from fraudtwin.kafka import publication_records
from fraudtwin.kafka_chaos import KafkaChaosConfig, simulate_delivery
from fraudtwin.lakehouse import (
    IcebergLakehouse,
    LakehouseConfigurationError,
    LakehouseDependencyError,
    LakehouseEnvironment,
    consume_kafka_once,
    materialize_run,
    verify_materialization,
)
from fraudtwin.ml import (
    BenchmarkPack,
    PointInTimeDatasetBuilder,
    evaluate_predictions,
    load_baseline_config,
    load_benchmark_pack,
    load_generated_run,
    load_predictions,
    run_backtest,
    run_model_backtest,
    train_baselines,
    write_backtest,
    write_evaluation,
    write_point_in_time_dataset,
)
from fraudtwin.observability import MetricsSession
from fraudtwin.postgres import database_status, migrate_database
from fraudtwin.quality_benchmark import report_run, run_quality_benchmark
from fraudtwin.replay import ReplayOrder, replay_run, write_replay
from fraudtwin.scale import run_scale_benchmark
from fraudtwin.simulation.graph_fraud import GraphFraudDataset
from fraudtwin.simulation.parquet import (
    write_campaign_dynamics_sidecar,
    write_counterfactual_sidecar,
)

app = typer.Typer(help="Synthetic financial-system and fraud digital twin.")
config_app = typer.Typer(help="Create and validate simulation configuration.")
ml_app = typer.Typer(help="Build local point-in-time ML datasets.")
app.add_typer(config_app, name="config")
app.add_typer(ml_app, name="ml")
graph_app = typer.Typer(help="Build deterministic temporal graph views.")
app.add_typer(graph_app, name="graph")
counterfactual_app = typer.Typer(help="Generate deterministic P03 counterfactual sidecars.")
campaign_app = typer.Typer(help="Evolve deterministic P04 campaign sidecars.")
app.add_typer(counterfactual_app, name="counterfactual")
app.add_typer(campaign_app, name="campaign")
benchmark_app = typer.Typer(help="Run generic M20 suites or immutable M21 public packs.")
app.add_typer(benchmark_app, name="benchmark")
db_app = typer.Typer(help="Manage the optional PostgreSQL operational schema.")
app.add_typer(db_app, name="db")
schema_app = typer.Typer(help="Validate bundled Avro event contracts.")
app.add_typer(schema_app, name="schema")
lakehouse_app = typer.Typer(help="Manage the optional Iceberg lakehouse.")
app.add_typer(lakehouse_app, name="lakehouse")
kafka_app = typer.Typer(help="Exercise deterministic Kafka delivery semantics.")
app.add_typer(kafka_app, name="kafka")


@kafka_app.command("chaos")
def kafka_chaos_command(
    run_id: Annotated[str, typer.Option("--run-id", help="Existing generated run identifier.")],
    boundary: Annotated[
        Literal["producer", "consumer"],
        typer.Option(
            "--boundary", help="Inject faults before producer delivery or after consumer receipt."
        ),
    ] = "producer",
    drop_rate: Annotated[float, typer.Option("--drop-rate", min=0, max=1)] = 0.0,
    duplicate_rate: Annotated[float, typer.Option("--duplicate-rate", min=0, max=1)] = 0.0,
    retry_rate: Annotated[float, typer.Option("--retry-rate", min=0, max=1)] = 0.0,
    delay_seconds: Annotated[int, typer.Option("--delay-seconds", min=0)] = 0,
    reorder_window: Annotated[int, typer.Option("--reorder-window", min=0)] = 0,
    partition_count: Annotated[int, typer.Option("--partition-count", min=1)] = 3,
    partition_skew: Annotated[float, typer.Option("--partition-skew", min=0, max=1)] = 0.0,
    seed: Annotated[int, typer.Option("--seed", min=0)] = 42,
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="Directory containing generated runs.")
    ] = Path("runs"),
) -> None:
    """Simulate deterministic logical Kafka faults and write an audit report."""

    try:
        run_dir = output_dir / run_id
        _, behavior, _ = load_generated_run(run_dir)
        records = publication_records(behavior, run_id)
        result = simulate_delivery(
            records,
            KafkaChaosConfig(
                seed=seed,
                boundary=boundary,
                drop_probability=drop_rate,
                duplicate_probability=duplicate_rate,
                retry_probability=retry_rate,
                max_delay_seconds=delay_seconds,
                reorder_window=reorder_window,
                partition_count=partition_count,
                partition_skew_probability=partition_skew,
            ),
        )
        destination = run_dir / "kafka-chaos"
        destination.mkdir(parents=True, exist_ok=True)
        manifest_path = destination / "manifest.json"
        manifest_path.write_text(
            json.dumps(result.manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        envelopes_path = destination / "envelopes.jsonl"
        with envelopes_path.open("w", encoding="utf-8") as handle:
            for envelope in result.envelopes:
                serialized = envelope.model_dump(mode="python")
                serialized["payload"] = envelope.payload.hex()
                handle.write(json.dumps(serialized, default=str, sort_keys=True) + "\n")
    except (OSError, RuntimeError, ValueError) as exc:
        typer.echo(f"Kafka chaos simulation failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    typer.echo(f"Kafka chaos report: {manifest_path}")
    typer.echo(f"Envelopes: {envelopes_path}")
    typer.echo(
        "Counts: "
        f"sent={result.input_count} emitted={result.emitted_count} "
        f"dropped={result.dropped_count} retried={result.retried_count} "
        f"duplicated={result.duplicated_count} late={result.late_count} "
        f"reordered={result.out_of_order_count} deduplicated={result.deduplicated_count}"
    )


@schema_app.command("validate")
def validate_schema_registry(
    registry: Annotated[
        Path | None,
        typer.Option("--registry", help="Registry directory or registry.yaml path."),
    ] = None,
) -> None:
    """Validate Avro syntax, fingerprints, and FULL_TRANSITIVE compatibility."""

    try:
        loaded = load_contract_registry(registry)
        report = loaded.validate()
    except (ContractValidationError, FileNotFoundError, OSError, ValueError) as exc:
        typer.echo(f"Schema registry validation failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    typer.echo(f"Schema registry is valid: {report.path}")
    typer.echo(f"Subjects: {report.subjects}; versions: {report.versions}")
    for key, fingerprint in sorted(report.fingerprints.items()):
        typer.echo(f"{key}: {fingerprint}")


@db_app.command("migrate")
def postgres_migrate() -> None:
    """Apply packaged PostgreSQL migrations using FRAUDTWIN_POSTGRES_DSN."""

    try:
        version = migrate_database()
    except (OSError, RuntimeError, ValueError) as exc:
        typer.echo(f"PostgreSQL migration failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    typer.echo(f"PostgreSQL schema is at version: {version}")


@db_app.command("status")
def postgres_status() -> None:
    """Show applied PostgreSQL migrations without changing the database."""

    try:
        versions = database_status()
    except (OSError, RuntimeError, ValueError) as exc:
        typer.echo(f"PostgreSQL status failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    for version in versions:
        typer.echo(version)


@lakehouse_app.command("init")
def lakehouse_init() -> None:
    """Create the Bronze/Silver/Gold/oracle Iceberg namespaces."""

    try:
        lakehouse = IcebergLakehouse(LakehouseEnvironment.from_environment())
        for namespace in lakehouse.initialize():
            typer.echo(namespace)
    except (LakehouseConfigurationError, LakehouseDependencyError, OSError, RuntimeError) as exc:
        typer.echo(f"Lakehouse initialization failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc


@lakehouse_app.command("ingest-run")
def lakehouse_ingest_run(
    run_id: Annotated[str, typer.Argument(help="Existing generated run identifier.")],
    runs_dir: Annotated[
        Path, typer.Option("--runs-dir", help="Directory containing generated runs.")
    ] = Path("runs"),
    include_oracle: Annotated[
        bool, typer.Option("--include-oracle", help="Publish the isolated oracle namespace.")
    ] = False,
    local_only: Annotated[
        bool,
        typer.Option("--local-only", help="Build and verify manifests without Iceberg services."),
    ] = False,
) -> None:
    """Backfill one complete generated run into the lakehouse."""

    try:
        environment = None if local_only else LakehouseEnvironment.from_environment()
        result = materialize_run(
            runs_dir / run_id,
            environment=environment,
            write_iceberg=not local_only,
            include_oracle=include_oracle,
        )
    except (
        LakehouseConfigurationError,
        LakehouseDependencyError,
        OSError,
        RuntimeError,
        ValueError,
    ) as exc:
        typer.echo(f"Lakehouse ingestion failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    typer.echo(f"Materialization: {result.materialization_id}")
    typer.echo(f"Manifest: {result.manifest_path}")
    typer.echo(f"Fingerprint: {result.logical_fingerprint}")


@lakehouse_app.command("consume")
def lakehouse_consume(
    max_messages: Annotated[int, typer.Option("--max-messages", min=1)] = 100,
    timeout_seconds: Annotated[float, typer.Option("--timeout-seconds", min=0.1)] = 5.0,
) -> None:
    """Consume a bounded batch of M25 Kafka records into Bronze/Silver."""

    try:
        environment = LakehouseEnvironment.from_environment()
        lakehouse = IcebergLakehouse(environment)
        lakehouse.initialize()
        count = consume_kafka_once(
            environment=environment,
            lakehouse=lakehouse,
            max_messages=max_messages,
            timeout_seconds=timeout_seconds,
        )
    except (
        LakehouseConfigurationError,
        LakehouseDependencyError,
        OSError,
        RuntimeError,
        ValueError,
    ) as exc:
        typer.echo(f"Lakehouse Kafka consumption failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    typer.echo(f"Consumed records: {count}")


@lakehouse_app.command("verify")
def lakehouse_verify(
    manifest: Annotated[Path, typer.Argument(help="Lakehouse materialization manifest JSON.")],
) -> None:
    """Verify the required fields of a lakehouse materialization manifest."""

    try:
        payload = verify_materialization(manifest)
    except (LakehouseConfigurationError, OSError, ValueError) as exc:
        typer.echo(f"Lakehouse verification failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    typer.echo(json.dumps(payload, indent=2, sort_keys=True))


@lakehouse_app.command("maintenance")
def lakehouse_maintenance(
    table: Annotated[str, typer.Argument(help="Logical table, e.g. bronze.records.")],
    operation: Annotated[
        str,
        typer.Argument(help="expire_snapshots, compact, or remove_orphan_files."),
    ],
    snapshot_id: Annotated[
        int | None,
        typer.Option("--snapshot-id", help="Required for snapshot expiration."),
    ] = None,
    execute: Annotated[
        bool,
        typer.Option("--execute", help="Apply the operation; default is a dry-run."),
    ] = False,
) -> None:
    """Plan or explicitly execute a protected Iceberg maintenance action."""

    try:
        environment = LakehouseEnvironment.from_environment()
        result = IcebergLakehouse(environment).maintenance(
            table,
            operation,
            snapshot_id=snapshot_id,
            execute=execute,
        )
    except (LakehouseConfigurationError, LakehouseDependencyError, OSError, RuntimeError) as exc:
        typer.echo(f"Lakehouse maintenance failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    typer.echo(json.dumps(result, indent=2, sort_keys=True))


@app.command("quality-benchmark")
def quality_benchmark_command(
    profile: Annotated[
        str, typer.Option("--profile", help="Bundled M22 profile or YAML path.")
    ] = "standard-v1",
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="Directory for quality benchmark artifacts.")
    ] = Path("runs/quality-benchmarks"),
    adapter: Annotated[
        str | None, typer.Option("--adapter", help="External generator module:factory.")
    ] = None,
    bundle: Annotated[
        Path | None, typer.Option("--bundle", help="Normalized external artifact bundle JSON.")
    ] = None,
    scale_manifest: Annotated[
        Path | None,
        typer.Option(
            "--scale-manifest",
            help="Optional M18 scale benchmark evidence JSON for scalability metrics.",
        ),
    ] = None,
) -> None:
    """Run the generator-quality protocol."""

    try:
        result = run_quality_benchmark(
            profile,
            output_dir=output_dir,
            adapter=adapter,
            bundle=bundle,
            scale_manifest=scale_manifest,
        )
    except (OSError, RuntimeError, ValueError) as exc:
        typer.echo(f"Quality benchmark failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    typer.echo(f"Quality benchmark generated: {result.report_id}")
    typer.echo(f"Report: {result.report_path}")


@app.command("scale-benchmark")
def scale_benchmark_command(
    path: Annotated[Path, typer.Argument(help="Scale-enabled YAML configuration file.")],
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="Directory for generated run artifacts.")
    ] = Path("runs/scale-benchmarks"),
    checkpoint_dir: Annotated[
        Path | None, typer.Option("--checkpoint-dir", help="Directory for chunk checkpoints.")
    ] = None,
    evidence_dir: Annotated[
        Path | None, typer.Option("--evidence-dir", help="Directory for benchmark evidence JSON.")
    ] = None,
) -> None:
    """Run a manual M18 scale job and write machine benchmark evidence."""

    config = _load_or_exit(path)
    try:
        evidence = run_scale_benchmark(
            config,
            output_dir=output_dir,
            checkpoint_dir=checkpoint_dir,
            evidence_dir=evidence_dir,
            command=(
                f"fraudtwin scale-benchmark {path} --output-dir {output_dir} "
                f"--checkpoint-dir {checkpoint_dir or '<checkpoint-dir>'} "
                f"--evidence-dir {evidence_dir or '<evidence-dir>'}"
            ),
        )
    except (OSError, RuntimeError, ValueError) as exc:
        typer.echo(f"Scale benchmark failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    typer.echo(f"Scale benchmark evidence: {evidence}")


@app.command("report")
def report_command(
    run_id: Annotated[str, typer.Argument(help="Existing generated run identifier.")],
    runs_dir: Annotated[
        Path, typer.Option("--runs-dir", help="Directory containing generated runs.")
    ] = Path("runs"),
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="Directory for quality reports.")
    ] = Path("runs/quality-reports"),
) -> None:
    """Report correctness and provenance checks for an existing run."""

    try:
        report_path = report_run(run_id, runs_dir=runs_dir, output_dir=output_dir)
    except (OSError, RuntimeError, ValueError) as exc:
        typer.echo(f"Report failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    typer.echo(f"Report: {report_path}")
    report_payload = json.loads(report_path.read_text(encoding="utf-8"))
    if any(value == "FAIL" for value in report_payload.get("correctness", {}).values()):
        raise typer.Exit(code=1)


def _selected_models(models: list[str] | None) -> tuple[str, ...]:
    return tuple(
        item.strip()
        for value in (models or ["deterministic_heuristic"])
        for item in value.split(",")
        if item.strip()
    )


def _run_generic_benchmark(
    suite: Annotated[
        str,
        typer.Option(
            "--suite",
            help=(
                "Standard suite or all: baseline, temporal, boundary, camouflage, "
                "graph, observability, calibrated, mixed."
            ),
        ),
    ] = "mixed",
    difficulty: Annotated[int, typer.Option("--difficulty", min=1, max=10)] = 7,
    seed: Annotated[int, typer.Option("--seed", min=0)] = 42,
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="Directory for benchmark artifacts.")
    ] = Path("runs/benchmarks"),
    models: Annotated[
        list[str] | None,
        typer.Option(
            "--models",
            help=(
                "Built-in model ID; repeat for multiple models "
                "(comma-separated is also accepted)."
            ),
        ),
    ] = None,
    runners: Annotated[
        list[str] | None,
        typer.Option("--runner", help="External runner module:factory; repeat this option."),
    ] = None,
    calibration_profile: Annotated[
        Path | None, typer.Option("--calibration-profile", help="M16 calibration profile YAML.")
    ] = None,
) -> None:
    """Generate and evaluate a reproducible fraud stress benchmark."""

    selected_models = _selected_models(models)
    try:
        result = run_benchmark(
            BenchmarkRequest(
                suite=cast(SuiteName, suite),
                difficulty=difficulty,
                seed=seed,
                output_dir=output_dir,
                models=selected_models,
                runners=tuple(runners or ()),
                calibration_profile=calibration_profile,
            )
        )
    except (OSError, RuntimeError, ValueError) as exc:
        typer.echo(f"Benchmark generation failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    typer.echo(f"Benchmark generated: {result.benchmark_id}")
    typer.echo(f"Manifest: {result.manifest_path}")
    typer.echo(f"Results: {result.results_path}")
    typer.echo(f"Descriptors: {result.descriptors_path}")


@benchmark_app.callback(invoke_without_command=True)
def benchmark_command(
    ctx: typer.Context,
    suite: Annotated[
        str,
        typer.Option(
            "--suite",
            help=(
                "Legacy M20 suite or all: baseline, temporal, boundary, camouflage, "
                "graph, observability, calibrated, mixed."
            ),
        ),
    ] = "mixed",
    difficulty: Annotated[int, typer.Option("--difficulty", min=1, max=10)] = 7,
    seed: Annotated[int, typer.Option("--seed", min=0)] = 42,
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="Directory for benchmark artifacts.")
    ] = Path("runs/benchmarks"),
    models: Annotated[
        list[str] | None,
        typer.Option("--models", help="Built-in model ID; repeat or comma-separate."),
    ] = None,
    runners: Annotated[
        list[str] | None,
        typer.Option("--runner", help="External runner module:factory; repeat this option."),
    ] = None,
    calibration_profile: Annotated[
        Path | None, typer.Option("--calibration-profile", help="M16 calibration profile YAML.")
    ] = None,
) -> None:
    """Run the backwards-compatible generic M20 benchmark command."""

    if ctx.invoked_subcommand is not None:
        return
    _run_generic_benchmark(
        suite=suite,
        difficulty=difficulty,
        seed=seed,
        output_dir=output_dir,
        models=models,
        runners=runners,
        calibration_profile=calibration_profile,
    )


@benchmark_app.command("run")
def benchmark_pack_run(
    pack_ref: Annotated[str, typer.Argument(help="Immutable pack, e.g. FT-B04-CAMOUFLAGE@0.34.0")],
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="Directory for benchmark artifacts.")
    ] = Path("runs/benchmarks"),
    models: Annotated[
        list[str] | None,
        typer.Option("--models", help="Built-in model ID; repeat or comma-separate."),
    ] = None,
    runners: Annotated[
        list[str] | None,
        typer.Option("--runner", help="External runner module:factory; repeat this option."),
    ] = None,
) -> None:
    """Run one immutable M21 public benchmark pack."""

    try:
        result = run_public_benchmark(
            pack_ref,
            output_dir=output_dir,
            models=_selected_models(models),
            runners=tuple(runners or ()),
        )
    except (OSError, RuntimeError, ValueError) as exc:
        typer.echo(f"Public benchmark failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    typer.echo(f"Public benchmark generated: {result.benchmark_id}")
    typer.echo(f"Manifest: {result.manifest_path}")
    typer.echo(f"Results: {result.results_path}")
    typer.echo(f"Descriptors: {result.descriptors_path}")


@benchmark_app.command("describe")
def benchmark_pack_describe(
    pack_ref: Annotated[str, typer.Argument(help="Immutable pack reference.")],
) -> None:
    """Describe the frozen definition of one M21 public benchmark pack."""

    try:
        pack = load_public_pack(pack_ref)
    except (OSError, ValueError) as exc:
        typer.echo(f"Public benchmark lookup failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    typer.echo(json.dumps(pack.model_dump(mode="json"), indent=2, sort_keys=True))


@benchmark_app.command("verify")
def benchmark_pack_verify(
    run_dir: Annotated[Path, typer.Argument(help="Existing benchmark artifact directory.")],
    pack_ref: Annotated[
        str | None, typer.Option("--pack", help="Optional immutable public-pack reference.")
    ] = None,
) -> None:
    """Verify an existing public benchmark artifact without rerunning it."""

    try:
        result = verify_public_benchmark(run_dir, reference=pack_ref)
    except (OSError, RuntimeError, ValueError) as exc:
        typer.echo(f"Public benchmark verification failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    typer.echo(f"Public benchmark verified: {result['benchmark_id']}")
    typer.echo(f"Pack: {result['pack']}")


def _load_or_exit(
    path: Path, *, calibration_profile_override: Path | None = None
) -> SimulationRunConfig:
    try:
        return load_config(path, calibration_profile_override=calibration_profile_override)
    except (FileNotFoundError, ValueError) as exc:
        typer.echo(f"Configuration error: {exc}", err=True)
        raise typer.Exit(code=1) from exc


@contextmanager
def _metrics_session(
    host: str, port: int | None, hold_seconds: float
) -> Iterator[MetricsSession | None]:
    """Start optional metrics and always release its short-lived server."""

    metrics = MetricsSession(host, port, hold_seconds) if port is not None else None
    if metrics is not None:
        metrics.start()
    try:
        yield metrics
    finally:
        if metrics is not None:
            metrics.finish()


def _parse_optional_timestamp(value: str | None) -> datetime | None:
    """Parse an optional CLI timestamp, preserving ``None`` for omitted filters."""

    return datetime.fromisoformat(value) if value is not None else None


@config_app.command("validate")
def validate_config(
    path: Annotated[Path, typer.Argument(help="YAML configuration file.")],
) -> None:
    """Validate a simulation configuration without running it."""

    config = _load_or_exit(path)
    typer.echo("Configuration is valid.")
    typer.echo(f"Seed: {config.simulation.seed}")
    typer.echo(f"Configuration hash: {config_hash(config)}")


@config_app.command("init")
def init_config(
    path: Annotated[
        Path,
        typer.Argument(help="Destination YAML file for the project-owned template."),
    ] = Path("config.yaml"),
    force: Annotated[
        bool,
        typer.Option("--force", help="Replace an existing file."),
    ] = False,
) -> None:
    """Create a project-owned copy of the packaged minimal configuration."""

    if path.exists() and not force:
        typer.echo(
            f"Configuration file already exists: {path}. Use --force to replace it.",
            err=True,
        )
        raise typer.Exit(code=1)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(_default_config_text(), encoding="utf-8")
    except OSError as exc:
        typer.echo(f"Configuration template could not be written: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    typer.echo(f"Configuration template written: {path}")
    typer.echo(f"Validate it with: fraudtwin config validate {path}")


@app.command()
def generate(
    path: Annotated[Path, typer.Argument(help="YAML configuration file.")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="Directory in which to store run manifests."),
    ] = Path("runs"),
    profile: Annotated[Path | None, typer.Option("--profile")] = None,
    seed: Annotated[int | None, typer.Option("--seed")] = None,
    workers: Annotated[int | None, typer.Option("--workers")] = None,
    checkpoint_dir: Annotated[Path | None, typer.Option("--checkpoint-dir")] = None,
    metrics_host: Annotated[
        str, typer.Option("--metrics-host", help="Address for the optional Prometheus endpoint.")
    ] = "127.0.0.1",
    metrics_port: Annotated[
        int | None,
        typer.Option("--metrics-port", min=1, max=65535, help="Enable Prometheus metrics."),
    ] = None,
    metrics_hold_seconds: Annotated[
        float,
        typer.Option(
            "--metrics-hold-seconds",
            min=0,
            help="Seconds to keep /metrics available after the command completes.",
        ),
    ] = 15.0,
) -> None:
    """Validate a configuration and generate a reproducible batch dataset."""

    config = _load_or_exit(path, calibration_profile_override=profile)
    started = time.monotonic()
    with _metrics_session(metrics_host, metrics_port, metrics_hold_seconds) as metrics:
        try:
            if config.scale.enabled and config.scale.profile != "dev":
                result = generate_scale_library(
                    config,
                    output_dir=output_dir,
                    profile=profile,
                    seed=seed,
                    workers=workers,
                    checkpoint_dir=checkpoint_dir,
                )
            else:
                result = generate_library(
                    config,
                    write=True,
                    output_dir=output_dir,
                    profile=profile,
                    seed=seed,
                    workers=workers,
                    checkpoint_dir=checkpoint_dir,
                )
        except GenerationError as exc:
            if metrics is not None:
                metrics.record_generator_error()
            typer.echo(exc.format_report(), err=True)
            raise typer.Exit(code=1) from exc
        except Exception:
            if metrics is not None:
                metrics.record_generator_error()
            raise
        else:
            if metrics is not None:
                metrics.observe_manifest(result.manifest, time.monotonic() - started)
    manifest = result.manifest
    entity_counts = manifest.entity_counts
    event_counts = manifest.event_counts
    manifest_path = result.manifest_path
    typer.echo(f"Run generated: {manifest.run_id}")
    typer.echo(f"Manifest: {manifest_path}")
    if result.dataset_path is not None and result.dataset_manifest_path is not None:
        typer.echo(f"Dataset: {result.dataset_path}")
        typer.echo(f"Dataset manifest: {result.dataset_manifest_path}")
    typer.echo("Generated entity counts:")
    for entity_name, count in entity_counts.items():
        typer.echo(f"  {entity_name}: {count}")
    typer.echo("Generated payment counts:")
    for event_name, count in event_counts.items():
        typer.echo(f"  {event_name}: {count}")


@app.command()
def resume(
    checkpoint_dir: Annotated[Path, typer.Argument(help="Scale checkpoint directory.")],
) -> None:
    """Resume a deterministic scale run from its checkpoint manifest."""

    try:
        result = resume_generation(checkpoint_dir)
    except (OSError, ValueError) as exc:
        typer.echo(f"Resume failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    typer.echo(f"Run resumed: {result.run_id}")
    typer.echo(f"Manifest: {result.manifest_path}")


@app.command()
def calibrate(
    reference: Annotated[Path, typer.Argument(help="Reference Parquet file.")],
    output: Annotated[Path, typer.Option("--output", help="Output profile YAML.")],
    seed: Annotated[int, typer.Option("--seed", help="Calibration seed.")] = 0,
) -> None:
    """Fit an aggregate-only deterministic calibration profile."""

    try:
        profile = fit_calibration_profile(load_reference_data(reference), seed=seed)
        write_calibration_profile(profile, output)
    except (OSError, ValueError) as exc:
        typer.echo(f"Calibration failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    typer.echo(f"Calibration profile generated: {profile.profile_id}")
    typer.echo(f"Profile: {output}")


@campaign_app.command("evolve")
def evolve_campaign_command(
    config_path: Annotated[Path, typer.Option("--config", help="P04 configuration YAML file.")],
    source_run_id: Annotated[str, typer.Option("--source-run-id", help="Static source run ID.")],
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="Directory containing the source run.")
    ] = Path("runs"),
) -> None:
    """Append a P04 sidecar to a compatible clean static graph run."""

    config = _load_or_exit(config_path)
    source_dir = output_dir / source_run_id
    try:
        entities, behavior, source_manifest = load_generated_run(source_dir)
        source_config = SimulationRunConfig.model_validate(source_manifest.resolved_configuration)
        if source_config.campaign_dynamics.active or source_config.stress.active:
            raise ValueError(
                "standalone P04 requires a source run without active camouflage or P04"
            )
        if config.stress.active:
            raise ValueError("standalone P04 does not accept active camouflage")
        if source_config.quality.profile != "clean" or config.quality.profile != "clean":
            raise ValueError("standalone P04 requires clean quality output")
        if not config.campaign_dynamics.active or not source_config.graph.enabled:
            raise ValueError("standalone P04 requires enabled campaign dynamics and graph source")
        source_values = source_config.model_dump(mode="json")
        requested_values = config.model_dump(mode="json")
        for section in (
            "simulation",
            "population",
            "payments",
            "behavior",
            "card_lifecycle",
            "pix_lifecycle",
            "fraud",
            "graph",
            "quality",
        ):
            if source_values.get(section) != requested_values.get(section):
                raise ValueError(f"standalone P04 base configuration mismatch in {section}")
        graph_dataset = GraphFraudDataset(
            behavior.payments,
            behavior.payment_events,
            behavior.ledger_entries,
            behavior.fraud_records,
            behavior.graph_memberships,
            behavior.graph_patterns,
            behavior.graph_campaigns,
            behavior.graph_evidence,
            behavior.graph_hyperedges,
            behavior.graph_hyperedge_memberships,
        )
        from fraudtwin.campaign_dynamics import evolve_campaigns

        dynamic = evolve_campaigns(config, entities, graph_dataset, source_manifest.run_id)
        root, manifest_path = write_campaign_dynamics_sidecar(
            dynamic, source_dir, source_run_id=source_manifest.run_id
        )
    except (FileNotFoundError, OSError, ValueError) as exc:
        typer.echo(f"Campaign evolution failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    typer.echo(f"Campaign dynamics generated: {root.name}")
    typer.echo(f"Output: {root}")
    typer.echo(f"Manifest: {manifest_path}")


@counterfactual_app.command("generate")
def generate_counterfactual_command(
    config_path: Annotated[Path, typer.Option("--config", help="P03 configuration YAML file.")],
    source_run_id: Annotated[str, typer.Option("--source-run-id", help="Baseline source run ID.")],
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="Directory containing the source run.")
    ] = Path("runs"),
) -> None:
    """Append P03 counterfactuals to a legitimate-only generated run."""

    config = _load_or_exit(config_path)
    source_dir = output_dir / source_run_id
    try:
        entities, behavior, source_manifest = load_generated_run(source_dir)
        source_config = SimulationRunConfig.model_validate(source_manifest.resolved_configuration)
        if source_config.fraud.enabled or source_config.graph.enabled:
            raise ValueError(
                "standalone P03 requires a source run with "
                "fraud.enabled=false and graph.enabled=false"
            )
        if not config.counterfactual.active:
            raise ValueError("standalone P03 requires counterfactual.enabled=true")
        source_values = source_config.model_dump(mode="json")
        requested_values = config.model_dump(mode="json")
        for section in (
            "simulation",
            "population",
            "payments",
            "behavior",
            "card_lifecycle",
            "pix_lifecycle",
        ):
            if source_values.get(section) != requested_values.get(section):
                raise ValueError(f"standalone P03 base configuration mismatch in {section}")
        from fraudtwin.counterfactual import generate_counterfactuals as generate_cf
        from fraudtwin.simulation.payments import PaymentDataset

        dataset = generate_cf(
            config,
            entities,
            PaymentDataset(behavior.payments, behavior.payment_events, behavior.ledger_entries),
            run_id=source_manifest.run_id,
        )
        root, manifest_path = write_counterfactual_sidecar(dataset, source_dir)
    except (OSError, ValueError) as exc:
        typer.echo(f"Counterfactual generation failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    typer.echo(f"Counterfactual generated: {dataset.counterfactual_id}")
    typer.echo(f"Output: {root}")
    typer.echo(f"Manifest: {manifest_path}")


@app.command("validate-ledger")
def validate_ledger_command(
    run_id: Annotated[str, typer.Option("--run-id", help="Generated run identifier.")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="Directory containing generated runs."),
    ] = Path("runs"),
    metrics_host: Annotated[
        str, typer.Option("--metrics-host", help="Address for the optional Prometheus endpoint.")
    ] = "127.0.0.1",
    metrics_port: Annotated[
        int | None,
        typer.Option("--metrics-port", min=1, max=65535, help="Enable Prometheus metrics."),
    ] = None,
    metrics_hold_seconds: Annotated[
        float,
        typer.Option(
            "--metrics-hold-seconds",
            min=0,
            help="Seconds to keep /metrics available after validation.",
        ),
    ] = 15.0,
) -> None:
    """Validate transfer ledger entries for a generated run."""

    run_dir = output_dir / run_id
    with _metrics_session(metrics_host, metrics_port, metrics_hold_seconds) as metrics:
        try:
            accounts = tuple(
                Account.model_validate(row)
                for row in pl.read_parquet(run_dir / "entities" / "accounts.parquet").to_dicts()
            )
            payments = tuple(
                Payment.model_validate(row)
                for row in pl.read_parquet(run_dir / "payments" / "payments.parquet").to_dicts()
            )
            events = tuple(
                PaymentEvent.model_validate(row)
                for row in pl.read_parquet(
                    run_dir / "payments" / "payment_events.parquet"
                ).to_dicts()
            )
            entries = tuple(
                LedgerEntry.model_validate(row)
                for row in pl.read_parquet(run_dir / "ledger" / "ledger_entries.parquet").to_dicts()
            )
            validate_ledger(accounts, payments, events, entries)
        except (FileNotFoundError, OSError, ValueError) as exc:
            if metrics is not None:
                metrics.record_ledger_validation(run_id, failed=True)
            typer.echo(f"Ledger validation failed: {exc}", err=True)
            raise typer.Exit(code=1) from exc
        else:
            if metrics is not None:
                metrics.record_ledger_validation(run_id, failed=False)
            typer.echo(f"Ledger is valid for run {run_id}.")


@graph_app.command("export")
def export_graph_command(
    run_id: Annotated[str, typer.Option("--run-id", help="Existing generated run identifier.")],
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="Directory containing generated runs.")
    ] = Path("runs"),
    as_of: Annotated[
        str | None, typer.Option("--as-of", help="Optional point-in-time ISO-8601 snapshot.")
    ] = None,
    from_time: Annotated[
        str | None, typer.Option("--from", help="Inclusive ISO-8601 event range start.")
    ] = None,
    to_time: Annotated[
        str | None, typer.Option("--to", help="Exclusive ISO-8601 event range end.")
    ] = None,
    view: Annotated[str, typer.Option("--view", help="observable, oracle, or both.")] = "both",
    formats: Annotated[
        str, typer.Option("--format", help="Comma-separated formats: parquet,neo4j,pyg.")
    ] = "parquet,neo4j",
    json_output: Annotated[
        bool, typer.Option("--json", help="Emit machine-readable JSON.")
    ] = False,
) -> None:
    """Export observable and/or oracle graph views from one immutable run."""

    run_dir = output_dir / run_id
    try:
        entities, behavior, source_manifest = load_generated_run(run_dir)
        config = SimulationRunConfig.model_validate(source_manifest.resolved_configuration)
        selected_view = view.lower()
        if selected_view not in {"observable", "oracle", "both"}:
            raise ValueError("--view must be observable, oracle, or both")
        format_values = tuple(
            dict.fromkeys(item.strip().lower() for item in formats.split(",") if item.strip())
        )
        if not format_values or any(
            item not in {"parquet", "neo4j", "pyg"} for item in format_values
        ):
            raise ValueError("--format must contain only parquet, neo4j, or pyg")

        start = _parse_optional_timestamp(from_time)
        end = _parse_optional_timestamp(to_time)
        snapshot = _parse_optional_timestamp(as_of)
        views: tuple[Literal["observable", "oracle"], ...] = (
            ("observable", "oracle")
            if selected_view == "both"
            else (cast(Literal["observable", "oracle"], selected_view),)
        )
        datasets: dict[Literal["observable", "oracle"], GraphDataset] = {
            item: build_graph(
                config,
                entities,
                behavior,
                source_manifest,
                view=item,
                as_of=snapshot,
                from_time=start,
                to_time=end,
                memberships=behavior.graph_memberships,
            )
            for item in views
        }
        destination, manifest = write_graph(
            datasets,
            run_dir / "graphs",
            source_manifest=source_manifest,
            config=config,
            formats=format_values,
        )
    except ValueError as exc:
        typer.echo(f"Graph export failed: {exc}", err=True)
        raise typer.Exit(
            code=3 if "graph" in str(exc).lower() or "pattern" in str(exc).lower() else 2
        ) from exc
    except (FileNotFoundError, OSError, RuntimeError) as exc:
        typer.echo(f"Graph export failed: {exc}", err=True)
        raise typer.Exit(code=4) from exc
    if json_output:
        typer.echo(
            json.dumps(
                {
                    "graph_id": manifest.graph_id,
                    "output": str(destination),
                    "manifest": str(destination / "graph_manifest.json"),
                },
                sort_keys=True,
            )
        )
        return
    typer.echo(f"Graph exported: {manifest.graph_id}")
    typer.echo(f"Output: {destination}")
    typer.echo(f"Manifest: {destination / 'graph_manifest.json'}")


@graph_app.command("validate")
def validate_graph_command(
    run_id: Annotated[str, typer.Option("--run-id")],
    output_dir: Annotated[Path, typer.Option("--output-dir")] = Path("runs"),
) -> None:
    """Validate observable and oracle graph projections for a run."""
    try:
        entities, behavior, manifest = load_generated_run(output_dir / run_id)
        config = SimulationRunConfig.model_validate(manifest.resolved_configuration)
        for view in ("observable", "oracle"):
            validate_graph(build_graph(config, entities, behavior, manifest, view=view))
    except ValueError as exc:
        typer.echo(f"Graph validation failed: {exc}", err=True)
        raise typer.Exit(code=3) from exc
    except (FileNotFoundError, OSError) as exc:
        typer.echo(f"Graph validation failed: {exc}", err=True)
        raise typer.Exit(code=4) from exc
    typer.echo(f"Graph is valid for run {run_id}.")


@ml_app.command("build-dataset")
def build_dataset(
    path: Annotated[Path, typer.Argument(help="YAML configuration file.")],
    run_id: Annotated[str, typer.Option("--run-id", help="Existing generated run identifier.")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="Directory containing the source run."),
    ] = Path("runs"),
    output: Annotated[
        Path | None,
        typer.Option("--output", help="Destination Parquet file."),
    ] = None,
    from_time: Annotated[
        str | None,
        typer.Option("--from", help="Optional ISO-8601 prediction range start."),
    ] = None,
    to_time: Annotated[
        str | None,
        typer.Option("--to", help="Optional ISO-8601 prediction range end."),
    ] = None,
    label_delay_aware: Annotated[
        bool,
        typer.Option("--label-delay-aware", help="Exclude labels unavailable at prediction time."),
    ] = False,
) -> None:
    """Build a deterministic point-in-time dataset from an existing run."""

    config = _load_or_exit(path)
    try:
        start = datetime.fromisoformat(from_time) if from_time is not None else None
        end = datetime.fromisoformat(to_time) if to_time is not None else None
        values = config.model_dump(mode="python")
        dataset_values = values["dataset"]
        if start is not None:
            dataset_values["start"] = start
        if end is not None:
            dataset_values["end"] = end
        if label_delay_aware:
            dataset_values["unresolved_labels"] = "exclude"
        config = SimulationRunConfig.model_validate(values)
        run_dir = output_dir / run_id
        entities, behavior, source_manifest = load_generated_run(run_dir)
        dataset = PointInTimeDatasetBuilder(config, entities, behavior, source_manifest).build()
        destination = output or run_dir / "ml" / "dataset.parquet"
        parquet_path, manifest_path = write_point_in_time_dataset(dataset, destination)
    except (OSError, ValueError) as exc:
        typer.echo(f"Dataset generation failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    typer.echo(f"Dataset generated: {parquet_path}")
    typer.echo(f"Dataset manifest: {manifest_path}")
    typer.echo(f"Rows: {dataset.count}")


def _read_ml_dataset(path: Path) -> list[dict[str, object]]:
    """Read a previously materialized PIT dataset without regenerating its source run."""

    try:
        frame = pl.read_parquet(path)
    except (FileNotFoundError, OSError) as exc:
        raise ValueError(f"dataset does not exist or cannot be read: {path}") from exc
    required = {"dataset_row_id", "prediction_time", "label", "split"}
    if not required.issubset(frame.columns):
        missing = sorted(required - set(frame.columns))
        raise ValueError(f"dataset is missing required columns: {missing}")
    return cast(list[dict[str, object]], frame.to_dicts())


def _infer_source_run_dir(dataset: Path) -> Path | None:
    """Infer a generated run directory for segment enrichment when possible."""

    candidate = dataset.parent.parent if dataset.parent.name == "ml" else None
    return candidate if candidate is not None and (candidate / "manifest.json").is_file() else None


@ml_app.command("train")
def train_baselines_command(
    dataset: Annotated[Path, typer.Argument(help="Point-in-time dataset Parquet file.")],
    config_path: Annotated[
        Path, typer.Option("--config", help="Baseline evaluation YAML configuration.")
    ] = Path("configs/ml-baselines.yaml"),
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="Directory for evaluation artifacts.")
    ] = Path("runs/ml-evaluations"),
) -> None:
    """Train deterministic M19 baseline models on frozen PIT rows."""

    try:
        evaluation_config = load_baseline_config(config_path)
        rows = _read_ml_dataset(dataset)
        result = train_baselines(
            rows, evaluation_config, source_run_dir=_infer_source_run_dir(dataset)
        )
        destination = output_dir / ("EV-" + result.manifest["output_fingerprint"][:16])
        predictions_path, metrics_path, manifest_path = write_evaluation(result, destination)
    except (OSError, RuntimeError, ValueError) as exc:
        typer.echo(f"Baseline training failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    typer.echo(f"Baseline evaluation generated: {destination.name}")
    typer.echo(f"Predictions: {predictions_path}")
    typer.echo(f"Metrics: {metrics_path}")
    typer.echo(f"Manifest: {manifest_path}")


@ml_app.command("evaluate")
def evaluate_predictions_command(
    dataset: Annotated[Path, typer.Argument(help="Point-in-time dataset Parquet file.")],
    predictions: Annotated[
        Path, typer.Argument(help="External predictions Parquet or JSONL file.")
    ],
    config_path: Annotated[
        Path, typer.Option("--config", help="Baseline evaluation YAML configuration.")
    ] = Path("configs/ml-baselines.yaml"),
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="Directory for evaluation artifacts.")
    ] = Path("runs/ml-evaluations"),
) -> None:
    """Evaluate external model predictions against PIT-safe labels."""

    try:
        evaluation_config = load_baseline_config(config_path)
        rows = _read_ml_dataset(dataset)
        records = load_predictions(predictions)
        result = evaluate_predictions(
            rows,
            records,
            evaluation_config,
            source_run_dir=_infer_source_run_dir(dataset),
        )
        destination = output_dir / ("EV-" + result.manifest["output_fingerprint"][:16])
        predictions_path, metrics_path, manifest_path = write_evaluation(result, destination)
    except (OSError, RuntimeError, ValueError) as exc:
        typer.echo(f"External prediction evaluation failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    typer.echo(f"External evaluation generated: {destination.name}")
    typer.echo(f"Predictions: {predictions_path}")
    typer.echo(f"Metrics: {metrics_path}")
    typer.echo(f"Manifest: {manifest_path}")


@app.command("replay")
def replay_command(
    run_id: Annotated[str, typer.Option("--run-id", help="Existing generated run identifier.")],
    from_time: Annotated[str, typer.Option("--from", help="Inclusive ISO-8601 period start.")],
    to_time: Annotated[str, typer.Option("--to", help="Exclusive ISO-8601 period end.")],
    order: Annotated[
        str,
        typer.Option(
            "--order",
            help="Replay ordering: event_time_order or original_delivery.",
        ),
    ] = "event_time_order",
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="Directory containing generated runs."),
    ] = Path("runs"),
) -> None:
    """Replay a selected period from one immutable generated run."""

    try:
        result = replay_run(
            output_dir / run_id,
            datetime.fromisoformat(from_time),
            datetime.fromisoformat(to_time),
            order=cast(ReplayOrder, order),
        )
        envelope_path, manifest_path = write_replay(result, output_dir / run_id / "replays")
    except (OSError, ValueError) as exc:
        typer.echo(f"Replay generation failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    typer.echo(f"Replay generated: {result.manifest.replay_id}")
    typer.echo(f"Events: {result.count}")
    typer.echo(f"Output: {envelope_path.parent}")
    typer.echo(f"Manifest: {manifest_path}")


@ml_app.command("backtest")
def backtest_command(
    path: Annotated[Path, typer.Argument(help="YAML configuration file.")],
    run_id: Annotated[str, typer.Option("--run-id", help="Existing generated run identifier.")],
    benchmark_pack: Annotated[
        Path | None,
        typer.Option("--benchmark-pack", help="Optional versioned benchmark-pack YAML."),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="Directory containing generated runs."),
    ] = Path("runs"),
    models: Annotated[
        str,
        typer.Option(
            "--models",
            help=(
                "Comma-separated M19 models; default deterministic_heuristic "
                "for M10 compatibility."
            ),
        ),
    ] = "deterministic_heuristic",
) -> None:
    """Build deterministic rolling PIT backtest folds from an existing run."""

    config = _load_or_exit(path)
    try:
        pack: BenchmarkPack | None = (
            load_benchmark_pack(benchmark_pack) if benchmark_pack is not None else None
        )
        run_dir = output_dir / run_id
        entities, behavior, source_manifest = load_generated_run(run_dir)
        selected_models = tuple(item.strip() for item in models.split(",") if item.strip())
        result = (
            run_backtest(config, entities, behavior, source_manifest, benchmark_pack=pack)
            if selected_models == ("deterministic_heuristic",)
            else run_model_backtest(
                config,
                entities,
                behavior,
                source_manifest,
                models=selected_models,
                benchmark_pack=pack,
            )
        )
        rows_path, metrics_path, manifest_path = write_backtest(
            result, run_dir / "ml" / "backtests"
        )
    except (OSError, ValueError) as exc:
        typer.echo(f"Backtest generation failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    typer.echo(f"Backtest generated: {result.manifest.backtest_id}")
    typer.echo(f"Rows: {len(result.fold_rows)}")
    typer.echo(f"Fold metrics: {metrics_path}")
    typer.echo(f"Rows artifact: {rows_path}")
    typer.echo(f"Manifest: {manifest_path}")


def main() -> None:
    """Run the command-line application."""

    app()


if __name__ == "__main__":
    main()
