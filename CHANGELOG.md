# Release notes

## 0.2.0 — Milestone 1

- Added deterministic synthetic entity generation for customers, institutions, accounts, cards, merchants, devices, and PIX-like keys.
- Added typed Polars Parquet output, entity counts in manifests, relationship validation, and an opt-in 100k-customer smoke test.

## 0.1.0 — Milestone 0

Initial repository foundation:

- Python 3.12 Poetry package
- Pydantic configuration validation from YAML
- Typer CLI with `config validate` and `generate`
- Deterministic seed handling and configuration hashing
- Reproducible run manifests
- Pytest, Ruff, mypy, coverage, and GitHub Actions CI

Entity, payment, fraud, streaming, and ML functionality are planned for subsequent milestones.
