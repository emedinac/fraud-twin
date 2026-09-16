# Kafka reliability and event-time correctness

FraudTwin's Kafka publisher is intentionally explicit about contract versions,
partition keys, idempotence, retries, acknowledgements, and pacing. The chaos
harness adds deterministic logical-message faults around either boundary:

```python
from pathlib import Path

from fraudtwin import generate
from fraudtwin.config import load_config
from fraudtwin.kafka import publication_records
from fraudtwin.kafka_chaos import KafkaChaosConfig, simulate_delivery

written_run = generate(
    load_config(Path("configs/minimal.yaml")),
    write=True,
    output_dir=Path("/tmp/fraudtwin-kafka-example"),
)
data = written_run.load_data()
records = publication_records(data.behavior, written_run.run_id)
result = simulate_delivery(
    records,
    KafkaChaosConfig(
        boundary="producer",
        seed=17,
        drop_probability=0.02,
        duplicate_probability=0.03,
        retry_probability=0.05,
        max_delay_seconds=30,
        reorder_window=100,
        partition_count=3,
    ),
)
print(result.manifest)
```

Supported logical failures include:

| Fault | What it teaches |
| --- | --- |
| drop/outage | loss handling and reconciliation |
| duplicate/retry | idempotent consumers and stable event IDs |
| delay/late delivery | event-time windows and watermarks |
| reordering | separating business time from arrival order |
| partition skew | hot partitions and consumer imbalance |
| schema change | compatibility checks and reader defaults |

`event_id`, business keys, and payload bytes are preserved. A chaos manifest
reports input/output fingerprints, attempts, transport IDs, topic/partition
counts, and deduplicated totals. This simulates Kafka message semantics, not
physical network packets; use Docker/Linux `tc/netem` separately when testing
socket-level failures.
