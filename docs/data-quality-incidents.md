# Data-quality incidents and replay

**Level:** Intermediate<br><br>
**You will:** inject explicit data faults, replay a bounded interval, and<br><br>
repair downstream projections without rewriting source truth.
**Before you start:** a generated run and the [Concepts](concepts.md) guide.<br><br>
**Services:** None; service-backed checks are optional.<br><br>

Quality faults are applied after business generation, so the canonical source
and oracle remain available for investigation. Select a profile or set one
fault explicitly:

```yaml
quality:
  profile: hostile
  duplicate_event_probability: 0.03
  late_event_probability: 0.20
  out_of_order_probability: 0.20
  outages:
    - source: payment_events
      from_time: 2026-01-01T10:00:00Z
      to_time: 2026-01-01T10:15:00Z
      behavior: BUFFER_AND_FLUSH
```

Inspect `quality_fault_counts`, `quality_fault_rates`, `fault_audit`, and
`quality_diagnostics`. Validate the repaired projection with the ledger,
payment lifecycle, label-observation, graph, and schema validators.

Replay is read-only and preserves source identity:

```console
RUN_ID=RUN-...

fraudtwin replay --run-id "$RUN_ID" \
  --from 2026-01-01T10:00:00Z --to 2026-01-01T11:00:00Z \
  --order original_delivery --output-dir runs/replay
```

Use the observable view for operational behavior and the oracle view only for
offline diagnosis. A repair may rebuild a downstream table or deduplicate a
stream, but it must never rewrite the canonical event or latent fraud truth.

## Next

Continue with [Data and evaluation workflows](workflows.md) or [Drift and shift](drift-and-shift.md).

## Related

- [Data contracts](data-contracts.rst)
- [Kafka reliability](kafka-reliability.md)
- [Scale operations](scale-operations.md)
