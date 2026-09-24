# Spark Structured Streaming reference

This example is an optional integration boundary. It does not add Spark to
the base FraudTwin installation or change the deterministic source run. The
application accepts observable `PaymentEvent` Parquet rows or the M25 Kafka
`payment-event` topic and produces the same projections:

```text
silver_events/
late_events/
gold_account_velocity/
spark-run-report.json
```

The pipeline uses event time, a ten-minute watermark, stable `event_id`
deduplication, and five-minute account velocity windows sliding every minute.
Distinct merchant/device counts use Spark's deterministic approximate distinct
aggregation for streaming compatibility.

## Bounded local run

Install the lakehouse extra and generate only the existing `dev` fixture:

```console
poetry install -E lakehouse
poetry run fraudtwin generate configs/scale-dev.yaml --output-dir /tmp/fraudtwin-spark
spark-submit examples/spark-streaming/spark_streaming.py \
  --source parquet \
  --input /tmp/fraudtwin-spark/RUN-*/payments/payment_events.parquet \
  --output /tmp/fraudtwin-spark-output \
  --checkpoint /tmp/fraudtwin-spark-checkpoint
```

For Kafka, start the documented streaming Compose profile, publish a clean
run, and pass the broker address with `--source kafka --input localhost:9092`.
Kafka input requires the Spark Avro and Kafka connector packages available in
the pinned Spark runtime. Iceberg output requires the configured local catalog.

The report records source mode, watermark, query progress, and elapsed time.
The example intentionally makes no throughput or production-readiness claim;
use the bounded `dev` profile on a laptop.
