"""Bounded Spark Structured Streaming reference pipeline.

The application intentionally lives outside the FraudTwin package. It reads
the observable PaymentEvent contract from Kafka or Parquet, applies event-time
deduplication and a watermark, and writes local projections suitable for a
laptop smoke test.
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any

WATERMARK = "10 minutes"
WINDOW = "5 minutes"
SLIDE = "1 minute"


def _contract_schema() -> str:
    path = Path(__file__).parents[2] / "contracts/avro/payment-event/1.0.0.avsc"
    return path.read_text(encoding="utf-8")


def _contract_fingerprint() -> str:
    registry = Path(__file__).parents[2] / "contracts/avro/registry.yaml"
    for line in registry.read_text(encoding="utf-8").splitlines():
        if "canonical_sha256:" in line:
            return line.split(":", 1)[1].strip()
    raise RuntimeError("payment-event contract fingerprint is missing")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", choices=("kafka", "parquet"), required=True)
    parser.add_argument(
        "--input", required=True, help="Kafka bootstrap/topic or PaymentEvent Parquet path"
    )
    parser.add_argument(
        "--output", required=True, help="Output directory or Iceberg warehouse root"
    )
    parser.add_argument("--checkpoint", required=True, help="Unique checkpoint directory")
    parser.add_argument("--output-format", choices=("parquet", "iceberg"), default="parquet")
    parser.add_argument(
        "--watermark",
        default=WATERMARK,
        help="Fixed event-time watermark (only 10 minutes is supported).",
    )
    parser.add_argument("--trigger", choices=("available-now", "once"), default="available-now")
    parser.add_argument("--topic", default="fraudsim.payment.events.v1")
    parser.add_argument("--app-name", default="fraudtwin-spark-streaming")
    return parser


def _spark_session(app_name: str) -> Any:
    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:  # pragma: no cover - optional example
        raise RuntimeError("install the lakehouse extra to run the Spark example") from exc
    return SparkSession.builder.appName(app_name).getOrCreate()


def _payment_events(spark: Any, args: argparse.Namespace) -> Any:
    from pyspark.sql import functions as F

    if args.source == "parquet":
        source = spark.readStream.format("parquet").load(args.input)
        return source.select(
            "event_id",
            "event_type",
            "payment_id",
            "account_id",
            "event_time",
            "ingested_at",
            "merchant_id",
            "device_id",
            "amount",
            "currency",
            "simulation_run_id",
        )

    from pyspark.sql.avro.functions import from_avro

    source = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.input)
        .option("subscribe", args.topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )
    header_map = F.map_from_entries(
        F.expr("transform(headers, h -> struct(cast(h.key as string), cast(h.value as string)))")
    )
    # Confluent framing is one magic byte plus a four-byte schema ID.
    decoded = source.withColumn("header_map", header_map).withColumn(
        "event", from_avro(F.expr("substring(value, 6, length(value))"), _contract_schema())
    )
    return decoded.filter(
        F.col("header_map")["fraudtwin-contract-fingerprint"] == F.lit(_contract_fingerprint())
    ).select("event.*")


def _normalized(events: Any) -> Any:
    from pyspark.sql import functions as F

    return events.select(
        F.col("event_id").cast("string"),
        F.col("event_type").cast("string"),
        F.col("payment_id").cast("string"),
        F.col("account_id").cast("string"),
        F.to_timestamp("event_time").alias("event_time"),
        F.to_timestamp("ingested_at").alias("ingested_at"),
        F.col("merchant_id").cast("string"),
        F.col("device_id").cast("string"),
        F.col("amount").cast("double"),
        F.col("currency").cast("string"),
        F.col("simulation_run_id").cast("string"),
    ).withColumn(
        "is_late",
        F.col("ingested_at") > F.col("event_time") + F.expr(f"INTERVAL {WATERMARK}"),
    )


def _write_stream(
    frame: Any, *, args: argparse.Namespace, name: str, output_mode: str = "append"
) -> Any:
    output = Path(args.output) / name
    checkpoint = Path(args.checkpoint) / name
    writer = frame.writeStream.outputMode(output_mode).option("checkpointLocation", str(checkpoint))
    if args.output_format == "iceberg":
        writer = writer.format("iceberg").option("path", str(output))
    else:
        writer = writer.format("parquet").option("path", str(output))
    writer = (
        writer.trigger(availableNow=True)
        if args.trigger == "available-now"
        else writer.trigger(once=True)
    )
    return writer.start()


def run(args: argparse.Namespace) -> dict[str, Any]:
    from pyspark.sql import functions as F

    if args.watermark != WATERMARK:
        raise ValueError(f"the reference pipeline uses a fixed {WATERMARK} watermark")
    started = time.perf_counter()
    spark = _spark_session(args.app_name)
    try:
        normalized = _normalized(_payment_events(spark, args))
        late = normalized.filter(F.col("is_late"))
        accepted = (
            normalized.filter(~F.col("is_late"))
            .withWatermark("event_time", args.watermark)
            .dropDuplicates(["event_id"])
        )
        velocity = accepted.groupBy(F.window("event_time", WINDOW, SLIDE), F.col("account_id")).agg(
            F.count("event_id").alias("transaction_count"),
            F.sum("amount").alias("transaction_amount"),
            F.approx_count_distinct("merchant_id").alias("distinct_merchants"),
            F.approx_count_distinct("device_id").alias("distinct_devices"),
        )
        queries = [
            _write_stream(accepted, args=args, name="silver_events"),
            _write_stream(late, args=args, name="late_events"),
            _write_stream(velocity, args=args, name="gold_account_velocity"),
        ]
        for query in queries:
            query.awaitTermination()
        report = {
            "source": args.source,
            "input": args.input,
            "output_format": args.output_format,
            "watermark": args.watermark,
            "window": WINDOW,
            "slide": SLIDE,
            "query_progress": [query.lastProgress for query in queries if query.lastProgress],
            "elapsed_seconds": time.perf_counter() - started,
        }
        Path(args.output).mkdir(parents=True, exist_ok=True)
        (Path(args.output) / "spark-run-report.json").write_text(
            json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8"
        )
        return report
    finally:
        spark.stop()


def main() -> None:
    run(_parser().parse_args())


if __name__ == "__main__":
    main()
