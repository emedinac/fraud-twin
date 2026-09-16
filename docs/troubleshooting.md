# Troubleshooting

## Installation and optional extras

Install the base package first, then add only the integration you need:

```console
poetry install
poetry install -E ml -E graph
poetry install -E postgres -E kafka -E lakehouse
```

Optional dependency errors usually mean the corresponding extra is missing;
they do not indicate that the deterministic core is unavailable.

## Configuration validation fails

Run validation before generation and inspect the first reported field:

```console
poetry run fraudtwin config validate configs/minimal.yaml
```

Unknown fields are rejected intentionally. Check required relationships such as
accounts needing customers and institutions, rail weights summing to `1.0`,
and timezone-aware simulation timestamps.

## Labels are missing or unresolved

Check `label_available_at` against `prediction_time`. A label may be true in
oracle data but unavailable operationally. Review `dataset.unresolved_labels`
and the configured workflow/observation delay before changing model code.

## Graph export is empty or incomplete

Confirm that graph scenarios were enabled and that the selected `view`,
`as_of`, `from_time`, and `to_time` lie inside the source run window. Observable
exports intentionally omit future and latent oracle-only relationships.

## Scale runs stop or cannot resume

Use a bounded profile locally, keep the checkpoint directory intact, and resume
from its manifest:

```console
fraudtwin generate configs/scale-1b.yaml --workers 16 --checkpoint-dir .fraudtwin/run
fraudtwin resume .fraudtwin/run
```

Resume validates the resolved configuration and seed tree. Do not edit a
checkpoint or versioned benchmark fixture in place.
