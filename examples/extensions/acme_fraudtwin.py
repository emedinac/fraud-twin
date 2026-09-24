"""Minimal external-package example for the FraudTwin extension SDK."""

from collections.abc import Mapping
from typing import Any

from fraudtwin import ExtensionMetadata


class AmountCapFault:
    """Deterministically annotate records whose amount exceeds a fixed cap."""

    metadata = ExtensionMetadata(
        extension_id="acme.amount-cap",
        extension_version="1.0.0",
        distribution="acme-fraudtwin",
        distribution_version="1.0.0",
    )

    def apply(self, record: Mapping[str, Any], *, seed: int) -> Mapping[str, Any]:
        del seed  # The example is intentionally deterministic without randomness.
        result = dict(record)
        if float(result.get("amount", 0)) > 10_000:
            result["fault_code"] = "AMOUNT_CAP"
        return result


extension = AmountCapFault()
