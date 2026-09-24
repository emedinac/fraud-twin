# Expert path

This route is for engineers and experienced practitioners who design
integrations, operate data systems, publish extensions, or review release
evidence.

**Level:** Expert<br><br>
**You will:** reason about source-of-truth boundaries, contracts, checkpoints,<br><br>
optional services, extension compatibility, and release claims.
**Before you start:** understand the [Intermediate path](intermediate.md) and<br><br>
the observable/oracle distinction.
**Services:** Most pages have an offline path; Docker or external services are<br><br>
required only for the integration being exercised.

## Follow this route

1. Read [Architecture and trust boundaries](../architecture.md) before adding
   an integration.
2. Use [Integration runbooks](../integrations.md), [Kafka reliability](../kafka-reliability.md),
   and [Spark Structured Streaming](../spark-streaming.md) for platform paths.
3. Use [Scale operations](../scale-operations.md) for bounded checkpoint and
   storage workflows.
4. Read [Extension SDK](../extensions.md) before publishing custom scenarios,
   rails, behavior models, fault injectors, or sinks.
5. Review [Compatibility](../compatibility.md), [Migration](../migration.md),
   and [Release evidence](../release-evidence.md) before distributing results.
6. Use [Development](../development.md) for repository changes and quality
   gates.

## Next

The [API](../api.rst), [CLI](../cli.rst), [configuration reference](../configuration-reference.rst),
and [data contracts](../data-contracts.rst) are the authoritative reference
layer.

## Related

- [Release readiness](../release-readiness.md)
- [Verified capabilities](../verified-capabilities.md)
- [References](../references.md)
