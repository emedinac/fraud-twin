# Development

Requirements: Python 3.12 and Poetry 2.x.

```bash
poetry install
poetry run pytest
poetry run ruff check .
poetry run ruff format --check .
poetry run mypy src
```

The entity generator can be exercised with:

```bash
poetry run fraudtwin config validate configs/minimal.yaml
poetry run fraudtwin generate configs/minimal.yaml
```

The opt-in 100k-customer smoke test is excluded from the default suite:

```bash
poetry run pytest -m large
```

Follow the milestone order in [`FEATURES.md`](FEATURES.md). Do not add infrastructure before the simulator has correct state and temporal behavior.
