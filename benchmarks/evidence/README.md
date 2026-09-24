# Reproducible bounded evidence

This directory is reserved for checked-in benchmark manifests produced from
the `dev` profile. Do not commit generated Parquet runs or evidence from the
`small`, `medium`, `large`, `xlarge`, or `billion` profiles.

Generate a local evidence manifest with:

```console
poetry run fraudtwin scale-benchmark configs/scale-dev.yaml \
  --output-dir /tmp/fraudtwin-evidence-run \
  --checkpoint-dir /tmp/fraudtwin-evidence-checkpoint \
  --evidence-dir benchmarks/evidence
```

Every committed manifest must retain the `laptop-dev-only` claim scope and
include the resolved configuration hash, seed, Git revision, package versions,
host summary, fingerprints, and resume status.
