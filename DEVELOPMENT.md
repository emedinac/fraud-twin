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

The behavior smoke tests intentionally use the minimal 100-payment fixture so
they remain fast in local development and CI:

```bash
poetry run pytest tests/test_behavior.py
```

To validate and generate into a temporary directory manually:

```bash
poetry run fraudtwin config validate configs/minimal.yaml
poetry run fraudtwin generate configs/minimal.yaml --output-dir /tmp/fraudtwin-run
```

The behavior section controls `amount_min`, `amount_max`, `active_hours`, `weekday_weights`, `merchant_preference_count`, and `preferred_device_limit`. Generated profiles and payment tables use stable schemas and deterministic IDs. Fraud, chargebacks, streaming, and production infrastructure remain deferred.

Follow the milestone order in [`FEATURES.md`](FEATURES.md). Do not add infrastructure before the simulator has correct state and temporal behavior.
