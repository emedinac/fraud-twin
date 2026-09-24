# External extension package example

This directory is a deliberately dependency-free example of the public
extension contract. A real package would expose the implementation through
the `fraudtwin.extensions` entry-point group in its own `pyproject.toml`:

```toml
[project.entry-points."fraudtwin.extensions"]
acme.amount-cap = "acme_fraudtwin:extension"
```

Install that package into the same environment as FraudTwin, then select its
stable ID in a configuration:

```yaml
extensions:
  enabled: true
  selected: [acme.amount-cap]
```

FraudTwin discovers only installed entry points, orders them by extension ID,
and records their identity and configuration hash in `manifest.json`.

