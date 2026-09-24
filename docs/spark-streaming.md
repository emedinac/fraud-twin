# Spark Structured Streaming

**Level:** Expert<br><br>
**You will:** run the optional bounded Spark reference pipeline over Kafka or<br><br>
Parquet and inspect its normalized, late-event, and velocity outputs.
**Before you start:** [Integration runbooks](integrations.md), Spark basics, and<br><br>
the `lakehouse` extra.
**Services:** Parquet mode is local; Kafka and Iceberg modes may require Docker<br><br>
services.

The Spark integration is an optional reference pipeline. It demonstrates
event-time processing and interoperability without making Spark a dependency
of local generation.

## Run the bounded Parquet path

```console
poetry install -E lakehouse
poetry run fraudtwin generate configs/scale-dev.yaml --output-dir /tmp/fraudtwin-spark
spark-submit examples/spark-streaming/spark_streaming.py \
  --source parquet \
  --input /tmp/fraudtwin-spark/RUN-*/payments/payment_events.parquet \
  --output /tmp/fraudtwin-spark-output \
  --checkpoint /tmp/fraudtwin-spark-checkpoint
```

The application writes `silver_events`, `late_events`,
`gold_account_velocity`, and `spark-run-report.json`. The Gold projection
contains five-minute account windows sliding every minute, with transaction
count, amount, and distinct merchant/device estimates.

## Kafka path

Start the documented streaming profile and publish a clean run, then use:

```console
spark-submit examples/spark-streaming/spark_streaming.py \
  --source kafka --input localhost:9092 \
  --output /tmp/fraudtwin-spark-output \
  --checkpoint /tmp/fraudtwin-spark-checkpoint
```

Kafka records must carry the bundled PaymentEvent contract fingerprint. Invalid or
unknown contract fingerprints and malformed Avro payloads are ignored rather
than projected.

## Time and failure semantics

`event_time` drives windows and a ten-minute watermark. `event_id` is the
deduplication identity. Records whose observable ingestion time is more than
ten minutes after event time are retained in `late_events`; they are not
silently treated as timely feature input. Checkpoints are unique per source
and output path. Remove temporary output and checkpoint directories after a
local experiment.

This example is a bounded interoperability demonstration, not a capacity or
production-readiness claim.

## Next

Use [Integration runbooks](integrations.md) for service startup, or [Data
contracts](data-contracts.rst) to inspect the source event shape.

## Related

- [Kafka reliability](kafka-reliability.md)
- [Architecture](architecture.md)
- [Troubleshooting](troubleshooting.md)
