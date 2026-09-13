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

The behavior smoke tests intentionally use the minimal 100-payment fixture so hey remain fast in local development and CI:

```bash
poetry run pytest tests/test_behavior.py
```

To validate and generate into a temporary directory manually:

```bash
poetry run fraudtwin config validate configs/minimal.yaml
poetry run fraudtwin generate configs/minimal.yaml --output-dir /tmp/fraudtwin-run
```

The behavior section controls `amount_min`, `amount_max`, `active_hours`, `weekday_weights`, `merchant_preference_count`, and `preferred_device_limit`. Generated profiles and payment tables use stable schemas and deterministic IDs. M6 fraud scenarios and the M7 fraud workflow are disabled together when `fraud.enabled` is false. M8 quality injection is controlled independently under `quality`; chargebacks, streaming, and production infrastructure remain deferred.

Card lifecycle tests can be run directly with:

```bash
poetry run pytest tests/test_card_lifecycle.py
```

PIX lifecycle tests can be run directly with:

```bash
poetry run pytest tests/test_pix_lifecycle.py
```

To validate the transfer ledger for a generated run:

```bash
poetry run fraudtwin validate-ledger --run-id <run-id>
```

The `pix_lifecycle` section controls PIX approval, rejection, and return probabilities plus validation, authorization, submission, settlement, receipt, and return delays. All values are validated before generation. PIX lifecycle tests use small datasets and cover ordering, rejection, returns, relationships, schemas, manifests, and ledger reconciliation.

The complete suite, including lightweight lifecycle generation, is:

```bash
poetry run pytest
```

The `card_lifecycle` section controls approval, reversal, and refund probabilities plus authorization, capture, clearing, settlement, reversal, and refund delays in whole seconds. Use `poetry run fraudtwin config validate configs/minimal.yaml` to validate these settings before generation.

Run the focused M6 scenario tests with:

```bash
poetry run pytest tests/test_fraud.py
```

The fraud section controls `enabled`, `target_rate`, `scenario_count`,
`hard_negative_rate`, and per-scenario weights, counts, amounts, durations,
attempt counts, and velocity windows. Validate the configuration and generate
an enabled scenario run into a temporary directory with:

```bash
poetry run fraudtwin config validate configs/minimal.yaml
poetry run fraudtwin generate configs/minimal.yaml --output-dir /tmp/fraudtwin-run
poetry run pytest tests/test_fraud.py tests/test_card_lifecycle.py tests/test_pix_lifecycle.py
```

Follow the milestone order in [`FEATURES.md`](FEATURES.md). Do not add infrastructure before the simulator has correct state and temporal behavior.

Run the focused M7 workflow tests with:

```bash
poetry run pytest tests/test_cases.py
```

For a targeted M7 validation run, use:

```bash
poetry run fraudtwin config validate configs/minimal.yaml
poetry run fraudtwin generate configs/minimal.yaml --output-dir /tmp/fraudtwin-m7
poetry run pytest tests/test_cases.py tests/test_fraud.py tests/test_card_lifecycle.py tests/test_pix_lifecycle.py
poetry run fraudtwin validate-ledger --run-id <run-id> --output-dir /tmp/fraudtwin-m7
```

Run the targeted M8 quality tests with:

```bash
poetry run pytest tests/test_quality.py
poetry run fraudtwin config validate configs/minimal.yaml
poetry run fraudtwin generate configs/minimal.yaml --output-dir /tmp/fraudtwin-m8
poetry run fraudtwin validate-ledger --run-id <run-id> --output-dir /tmp/fraudtwin-m8
```

Use a temporary YAML override to exercise a fault profile without changing the
tracked minimal configuration. The M8 tests cover reproducibility, independent
fault settings, optional-field handling, invalid values, source timing,
delivery ordering, spikes, Parquet schemas, manifests, and CLI generation.
