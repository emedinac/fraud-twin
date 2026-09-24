# Scale operations

**Level:** Expert<br><br>
**You will:** run bounded partitioned jobs, inspect checkpoints, resume safely,<br><br>
and interpret laptop-scale evidence.
**Before you start:** [Configuration](configuration.md) and the scale profile<br><br>
boundary described in [Verified capabilities](verified-capabilities.md).
**Services:** Local storage by default; S3/MinIO is optional.<br><br>

Scale generation is opt-in. Use the `dev` profile for laptop validation:

```console
RUNS_DIR=./runs
CHECKPOINT_DIR=./runs/scale-checkpoint

poetry run fraudtwin generate configs/scale-dev.yaml \
  --output-dir "$RUNS_DIR" \
  --checkpoint-dir "$CHECKPOINT_DIR"
poetry run fraudtwin resume "$CHECKPOINT_DIR"
```

The checkpoint records the resolved plan, seed tree, completed chunks,
physical checksums, partition fingerprints, and reconciliation results. Resume
reuses a chunk only when its configuration, ordinal range, path, and checksum
match. A corrupt or incomplete chunk is regenerated deterministically.

Local storage is the default. The fsspec adapter can publish a completed run
to an S3/MinIO-compatible URI; remote publication must retain the source
manifest and checkpoint as immutable artifacts. Credentials belong in the
environment, never in YAML or manifests.

## Evidence and boundaries

Use `fraudtwin scale-benchmark` to write a machine-readable evidence manifest.
It includes host information, package versions, Git revision, configuration
hash, throughput, peak RSS, output size, and resume status. Evidence generated
from `scale-dev` is laptop-scale evidence only. Do not run `small`, `medium`,
`large`, `xlarge`, or `billion` profiles in CI or on a constrained laptop.

The compatibility in-memory API remains appropriate for small analysis. The
scale API is the boundary for chunked artifacts and checkpointed execution;
downstream reducers and sinks should consume partition readers rather than
calling `GeneratedRun.load_data()` for large workloads.

## Next

Review [Release evidence](release-evidence.md) before interpreting a benchmark,
or use [Troubleshooting](troubleshooting.md) when a checkpoint cannot resume.

## Related

- [Architecture](architecture.md)
- [Configuration](configuration.md)
- [Verified capabilities](verified-capabilities.md)
