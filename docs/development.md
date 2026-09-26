# Development guide

**Level:** Expert<br><br>
**You will:** contribute code and documentation while preserving deterministic<br><br>
outputs, contracts, tests, and release quality.
**Before you start:** a repository checkout and basic Git and Poetry skills.<br><br>
**Services:** None for the quality gate.<br><br>

FraudTwin is a small Python package with a deliberately deterministic core. Keep domain rules in `src/fraudtwin/domain`, use cases and orchestration in the simulation/application modules, and integrations at the edges. Changes should make generated data easier to explain, not merely more complex.

## Set up the repository

Requirements are Python 3.12 and Poetry 2.x:

```bash
poetry install
poetry run fraudtwin config validate configs/minimal-v1.yaml
```

Generated runs belong in the ignored `./runs` directory. Do not commit generated data, credentials, or local environment files.

## Quality gate

Run the same checks used by CI before opening a pull request:

```bash
poetry check --strict
poetry run ruff check .
poetry run ruff format --check .
poetry run mypy src
poetry run pytest
poetry build
git diff --check
```

The repository’s checks run only on pull requests targeting `main`; a merged
commit does not rerun the test suite. Protect `main` with a GitHub ruleset that
requires those pull-request checks and disallows direct pushes. GitHub Actions
cannot enforce that restriction by itself.

Version policy is read-only: every PR must raise the version above `main` and
keep `pyproject.toml`, `src/fraudtwin/__init__.py`, and `CHANGELOG.md` in sync.
The workflow never commits or pushes version bumps on your behalf.

To publish, push a `vX.Y.Z` tag from the merged release commit. The tag must
match the versions in `pyproject.toml`, `src/fraudtwin/__init__.py`, and the
top `CHANGELOG.md` heading. Ordinary pushes and commit messages never publish a
release.

## Focused tests

Use the narrowest test while iterating, then run the full suite:

| Area | Test command |
| --- | --- |
| Entities and behavior | `poetry run pytest tests/test_entities.py tests/test_behavior.py` |
| Card lifecycle | `poetry run pytest tests/test_card_lifecycle.py` |
| Pix lifecycle and ledger | `poetry run pytest tests/test_pix_lifecycle.py` |
| Fraud scenarios | `poetry run pytest tests/test_fraud.py` |
| Workflow cases and labels | `poetry run pytest tests/test_cases.py` |
| Data quality | `poetry run pytest tests/test_quality.py` |
| Point-in-time datasets | `poetry run pytest tests/test_dataset.py` |
| Replay and backtesting | `poetry run pytest tests/test_backtesting.py` |
| Baseline ML and prediction adapter | `poetry run pytest tests/test_baseline_ml.py` |
| Graph exports | `poetry run pytest tests/test_graph.py` |
| Difficulty | `poetry run pytest tests/test_difficulty.py` |
| Camouflage | `poetry run pytest tests/test_camouflage.py` |
| Campaign dynamics | `poetry run pytest tests/test_campaign_dynamics.py` |
| Observability | `poetry run pytest tests/test_metrics.py` |

For a generated-run smoke test:

```bash
CONFIG=configs/minimal-v1.yaml
RUN_ID=RUN-...
RUNS_DIR=./runs

poetry run fraudtwin config validate "$CONFIG"
poetry run fraudtwin generate "$CONFIG" --output-dir "$RUNS_DIR"
poetry run fraudtwin validate-ledger \
  --run-id "$RUN_ID" \
  --output-dir "$RUNS_DIR"
```

Scale tests must use at most 1,000 logical events per fixture. Validate scale behavior with small deterministic runs and simulated interruption/resume; do not execute the billion profile in the test suite.

## Working on a feature

1. Read the relevant module and its nearest tests before editing implementation code.
2. Preserve deterministic seeds, stable schemas, and causal timestamps.
3. Add or update focused tests for invariants, not only happy-path output.
4. Update the relevant page under `docs/`, the root README when the user path changes, and [`CHANGELOG.md`](../CHANGELOG.md) for release-facing behavior.
5. Run the quality gate and inspect `git diff --check` before handing off.

## Documentation conventions

Write for a reader who has not seen the repository before. Lead with what a command enables, show the smallest working example, and explain any important constraint immediately after it. Keep release history in `CHANGELOG.md`; the README and `docs/` should teach people how to use the current system.

Every non-tutorial guide begins with a reader contract containing `Level`,
`You will`, `Before you start`, and `Services`. Use the level routes for
navigation and keep one canonical explanation per topic. Prefer feature names
in public prose; reserve internal version or compatibility identifiers for
schemas, manifests, release history, and legacy command paths. End a guide with
`Next` and `Related` links so the reader always has a sensible continuation.

## Next

Run the quality gate, then review [Release evidence](release-evidence.md) for
release-facing changes.

## Related

- [Compatibility](compatibility.md)
- [Migration guides](migration.md)
- [References](references.md)
