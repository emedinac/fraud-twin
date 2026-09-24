# Architecture and trust boundaries

**Level:** Expert<br><br>
**You will:** understand the source-of-truth boundary and how integrations<br><br>
consume, rather than redefine, generated data.
**Before you start:** the [Concepts](concepts.md) guide and a generated run.<br><br>
**Services:** None; integrations are described separately.<br><br>

FraudTwin has one authoritative source of truth: a deterministic simulation
configuration, seed tree, and generated run manifest. Every integration reads
that source; integrations do not redefine payment state, fraud truth, or
point-in-time semantics.

```text
validated config + seed
        |
        v
entities -> behavior -> payments/lifecycles -> ledger
                                      |
                     fraud/workflow/labels/quality
                                      |
        +-----------------------------+--------------------------+
        |                             |                          |
   Parquet source              chunked scale path          observable events
        |                             |                          |
  PIT/graph/ML              checkpoints + sinks       Kafka -> Spark -> Iceberg
```

The observable view contains only information available at the selected
cutoff. Oracle artifacts explain the generated world for evaluation and audit;
they are never silently published to operational contracts.

## Execution boundaries

- **Local generation** uses the dependency-light Python API and writes typed
  Parquet artifacts.
- **Scale execution** writes deterministic shards and chunks, validates
  ledger and identity invariants, and resumes only from validated checkpoints.
  The repository validates this path with bounded fixtures; it does not claim
  laptop support for the 100M or 1B profiles.
- **Spark** is an optional reference consumer under `examples/`. It consumes
  observable PaymentEvent records and produces event-time projections; it is
  not part of the base generator.
- **Extensions** implement stable ports for scenarios, rails, behavior,
  quality faults, and sinks. Their identity and version are captured in the
  run manifest.

See [verified capabilities](verified-capabilities.md) for the exact evidence
behind each supported boundary.

## Next

Continue with [Integration runbooks](integrations.md) or [Extension SDK](extensions.md).

## Related

- [Concepts](concepts.md)
- [Data contracts](data-contracts.rst)
- [Compatibility](compatibility.md)
