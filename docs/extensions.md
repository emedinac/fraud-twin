# Extension SDK

**Level:** Expert<br><br>
**You will:** publish deterministic scenarios, rails, behavior models, fault<br><br>
injectors, or sinks without importing arbitrary configuration paths.
**Before you start:** the [Architecture](architecture.md) guide and Python<br><br>
packaging experience.
**Services:** None.<br><br>

FraudTwin exposes small, typed ports so domain extensions do not need to edit
the simulator internals. The public contracts are available from
`fraudtwin.extensions`:

```python
from fraudtwin import ExtensionMetadata, FraudScenario

metadata = ExtensionMetadata("acme.account-takeover", "1.0.0")
```

Supported ports are `FraudScenario`, `PaymentRail`, `BehaviorModel`,
`DataFaultInjector`, and `OutputSink`. Each implementation must expose
`ExtensionMetadata`, use only the supplied deterministic seed/context, and
preserve stable logical identities.

## Discovery and compatibility

Packages may register implementations through the
`fraudtwin.extensions` Python entry-point group. FraudTwin loads installed
packages only; configuration never executes arbitrary import strings or
downloads code. Discovery is ordered by extension ID and the manifest records
the extension ID, semantic version, distribution, and distribution version.

Breaking changes require a new major extension version and a documented
compatibility range. Extensions must not expose oracle-only fields through
observable sinks or alter authoritative ledger transitions.

For an application-owned registry, use `ExtensionRegistry` and register the
implementations explicitly before constructing the workflow. Keep the registry
snapshot alongside the run manifest so a result can be reproduced later.

The complete external-package example is in
[`examples/extensions/`](https://github.com/emedinac/fraud-twin/tree/main/examples/extensions).
It includes a minimal fault injector and the corresponding `pyproject.toml`
entry-point declaration to copy into a separately versioned distribution.

## Next

Follow the [Architecture](architecture.md) boundaries, then validate the
extension in a bounded [Development](development.md) workflow.

## Related

- [Compatibility](compatibility.md)
- [Data contracts](data-contracts.rst)
- [Verified capabilities](verified-capabilities.md)
