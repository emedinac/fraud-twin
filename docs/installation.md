# Installation and support

FraudTwin has a dependency-light core. Install the base package when you need
deterministic generation, configuration validation, Parquet outputs, and the
CLI. Add an extra only for the workflow you are running.

## Supported environments

| Requirement | Support policy |
| --- | --- |
| Python | 3.12 or newer, below 4.0 |
| Operating system | Linux, macOS, and Windows; OS-independent Python package |
| Architecture | 64-bit environments are recommended for Polars and optional ML packages |
| Network | Required only to install packages or reach an external service |
| Docker | Not required for core generation or offline tutorials |

The package metadata is the source of truth for supported Python versions and
dependency ranges. Pin the FraudTwin version and lock file when a researcher
needs byte-for-byte reproducibility.

## Choose an installation

### PyPI user installation

```console
python -m venv .venv
# Linux/macOS
source .venv/bin/activate
# Windows PowerShell: .venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install fraudtwin==0.34.0
fraudtwin --help
```

### Poetry project installation

```console
poetry add fraudtwin==0.34.0
poetry run fraudtwin config validate configs/minimal.yaml
```

### Repository or contributor installation

```console
git clone https://github.com/emedinac/fraud-twin.git
cd fraud-twin
poetry install
poetry run fraudtwin config validate configs/minimal.yaml
```

## Optional capabilities

| Capability | Install | External service | Intended audience |
| --- | --- | --- | --- |
| Generation, Parquet, Avro validation | base | No | Everyone |
| ML baselines and evaluation | `poetry install -E ml` | No | Data/ML scientists |
| PyTorch Geometric conversion | `poetry install -E graph` | No | Graph researchers |
| Scale/DuckDB/Arrow | `poetry install -E scale` | No | Data engineers |
| S3/MinIO-backed scale storage | `poetry install -E scale-storage` | Optional | MLOps/data engineers |
| PostgreSQL persistence | `poetry install -E postgres` | PostgreSQL | Data engineers |
| Kafka publication | `poetry install -E kafka` | Kafka and Schema Registry | MLOps/data engineers |
| Iceberg publication | `poetry install -E lakehouse` | Catalog and object storage | Data engineers |
| Prometheus metrics | `poetry install -E observability` | Prometheus/Grafana optional | MLOps |
| MLflow tracking | `poetry install -E mlflow` | MLflow optional | ML engineers/MLOps |
| FastAPI reference serving | `poetry install -E serving` | No | ML engineers |

Extras are independent. For example, a local ML experiment does not require
Kafka or PostgreSQL:

```console
poetry install -E ml -E mlflow
```

## Verify the environment

Run a bounded generation before starting a long experiment:

```console
poetry run fraudtwin config validate configs/minimal.yaml
poetry run fraudtwin generate configs/minimal.yaml --output-dir /tmp/fraudtwin-smoke
poetry run fraudtwin validate-ledger --run-id /tmp/fraudtwin-smoke
```

The smoke run should produce a manifest, Parquet artifacts, and a stable
fingerprint. Use a temporary directory for this check so it cannot overwrite a
benchmark fixture.

## Resource expectations

The minimal and visualization tutorials are designed for a laptop. Increase
memory and local SSD capacity with the payment target: lifecycle, ledger,
fraud, label, and graph rows can be several times larger than payment rows.
The `dev` scale profile is the bounded 1,000-payment smoke path. Treat the
100M/1B profiles as hardware benchmarks, not default development workloads.

## Common installation failures

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| `No module named fraudtwin` | Wrong interpreter or inactive virtual environment | Run `python -m pip show fraudtwin` and use the same interpreter for the CLI |
| `No module named sklearn` | Missing ML extra | Install `poetry install -E ml` or `python -m pip install 'fraudtwin[ml]'` |
| Torch installation is too large or incompatible | Graph extra is platform-sensitive | Use the PyTorch installation guidance for your platform, then install the matching graph extra |
| Kafka/PostgreSQL connection refused | Service is not running or DSN is wrong | Start the documented Docker profile and run its health check |
| Configuration validation fails | Unknown field, invalid range, or incompatible sections | Run `fraudtwin config validate` and fix the first reported field |

See [troubleshooting](troubleshooting.md) for runtime and integration failures.
