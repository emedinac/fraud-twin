# Release and benchmark evidence

**Level:** Expert<br><br>
**You will:** produce bounded, reproducible evidence and interpret release and<br><br>
benchmark claims without overstating hardware capacity.
**Before you start:** [Verified capabilities](verified-capabilities.md).<br><br>
**Services:** None; GitHub and PyPI publishing are release-time services.<br><br>

A release artifact is trustworthy only when the code, documentation, package,
and evidence agree. Pull-request CI runs the test suite before merge. The
release workflow is activated only by a `vX.Y.Z` tag or a merged commit message
containing exactly one `vX.Y.Z` marker; it validates the package metadata and
Avro contracts, verifies the built wheel in a clean environment, builds strict
documentation, and attaches the wheel and source distribution to the GitHub
release before trusted PyPI publication.

## Laptop evidence protocol

```console
poetry run fraudtwin scale-benchmark configs/scale-dev.yaml \
  --output-dir runs/evidence-dev \
  --checkpoint-dir runs/evidence-dev-checkpoint \
  --evidence-dir benchmarks/evidence
```

Commit the resulting evidence only when it contains the resolved configuration
hash, seed, Git revision, package versions, host summary, output fingerprints,
resume result, and explicit `laptop-dev-only` claim scope. Never describe this
result as a 100M/1B capacity measurement.

Benchmark-pack metrics and scale evidence answer different questions:
packs compare deterministic fraud/ML conditions; scale evidence describes
execution and storage behavior. Keep their manifests and claims separate.

## Next

Use [Release readiness](release-readiness.md) to review what is supported, or
follow [Development](development.md) to reproduce the checks locally.

## Related

- [Compatibility](compatibility.md)
- [Verified capabilities](verified-capabilities.md)
- [Migration guides](migration.md)
