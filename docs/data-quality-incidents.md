# Data-quality incidents and replay

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
fraudtwin replay --run-id <run-id> \
  --from 2026-01-01T10:00:00Z --to 2026-01-01T11:00:00Z \
  --order original_delivery --output-dir runs/replay
```

Use the observable view for operational behavior and the oracle view only for
offline diagnosis. A repair may rebuild a downstream table or deduplicate a
stream, but it must never rewrite the canonical event or latent fraud truth.
