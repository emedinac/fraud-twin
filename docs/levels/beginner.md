# Beginner path

This route assumes Python experience, notebooks, and basic package
installation. You do not need Docker, Kafka, PostgreSQL, or machine-learning
infrastructure.

**Level:** Beginner<br><br>
**You will:** install FraudTwin, generate a small deterministic run, and learn<br><br>
how to read its manifest and Parquet outputs.
**Before you start:** Python 3.12 or newer and a terminal.<br><br>
**Services:** None; every step is local.<br><br>

## Follow this route

1. [Install FraudTwin](../installation.md), using the quick `pip` path or the
   recommended Poetry project workflow.
2. Run [Quickstart](../quickstart.md) and keep the generated run in a temporary
   directory.
3. Read [Concepts](../concepts.md) to understand entities, payments, events,
   labels, and observable versus oracle data.
4. Use [Configuration](../configuration.md) to change one setting at a time.
5. Keep [Troubleshooting](../troubleshooting.md) nearby when a command or
   optional package is unfamiliar.

The existing [getting-started tutorials](../tutorials/getting-started.md) are
the next practical step. They are intentionally unchanged and build on this
route.

## Next

When you can explain a manifest and inspect a Parquet table, move to the
[Intermediate path](intermediate.md).

## Related

- [Data contracts](../data-contracts.rst)
- [Glossary](../glossary.md)
- [Verified capabilities](../verified-capabilities.md)
