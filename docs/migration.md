# Migration guides

**Level:** Expert<br><br>
**You will:** move a reproducible run or configuration between package<br><br>
versions and verify compatibility before comparing results.
**Before you start:** the [Compatibility policy](compatibility.md).<br><br>
**Services:** None.<br><br>

FraudTwin treats generated output identity and public schemas as compatibility
boundaries. Upgrade notes belong here when a release changes a public API,
configuration field, output contract, benchmark pack, or reproducibility rule.

## 0.34.0

The current documentation and API reference describe the 0.34.0 package. Use
the version selector to compare examples against another release and consult
the changelog for feature-level notes.

When upgrading, validate an existing configuration, regenerate a bounded
fixture, and compare its manifest and schema fingerprints before adopting a
new benchmark result.

## Next

Review [Release evidence](release-evidence.md) before publishing the upgraded
package or comparing new benchmark output.

## Related

- [Compatibility](compatibility.md)
- [Verified capabilities](verified-capabilities.md)
- [Development](development.md)
