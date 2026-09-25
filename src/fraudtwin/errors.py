"""Structured errors raised while materializing generated data."""

from collections.abc import Mapping, Sequence
from typing import Any

TROUBLESHOOTING_URL = (
    "https://emedinac.github.io/fraudtwin/latest/troubleshooting.html"
    "#generation-fails-after-configuration-validation"
)


class GenerationError(ValueError):
    """A known failure while turning a valid configuration into generated data.

    The exception remains a :class:`ValueError` for compatibility, while the
    structured attributes let applications handle failures without parsing the
    human-readable message.
    """

    def __init__(
        self,
        *,
        code: str,
        stage: str,
        summary: str,
        context: Mapping[str, Any] | None = None,
        hints: Sequence[str] = (),
        docs_url: str = TROUBLESHOOTING_URL,
    ) -> None:
        self.code = code
        self.stage = stage
        self.summary = summary
        self.context = dict(context or {})
        self.hints = tuple(hints)
        self.suggested_actions = self.hints
        self.docs_url = docs_url
        super().__init__(summary)

    def __str__(self) -> str:
        """Return a concise message suitable for logs and existing callers."""

        return self.summary

    def format_report(self) -> str:
        """Return the actionable report shown by the command-line interface."""

        lines = [f"Generation failed [{self.code}]", "", f"Stage: {self.stage}"]
        for label, value in self.context.items():
            lines.append(f"{label}: {value}")
        lines.extend(["", "What this means:", f"  {self.summary}"])
        if self.hints:
            lines.extend(["", "Possible fixes:"])
            lines.extend(f"  - {hint}" for hint in self.hints)
        lines.extend(["", "Documentation:", f"  {self.docs_url}"])
        return "\n".join(lines)

    def as_dict(self) -> dict[str, Any]:
        """Return the diagnostic fields for structured application logging."""

        return {
            "code": self.code,
            "stage": self.stage,
            "summary": self.summary,
            "context": dict(self.context),
            "hints": list(self.hints),
            "suggested_actions": list(self.suggested_actions),
            "docs_url": self.docs_url,
        }


class LedgerCapacityError(GenerationError):
    """A debit would cross an account's permitted overdraft boundary."""

    code = "LEDGER_OVERDRAFT_EXCEEDED"

    def __init__(
        self,
        *,
        stage: str,
        account_id: str,
        payment_id: str,
        event_id: str,
        debit_amount: float,
        balance_before: float,
        balance_after: float,
        overdraft_limit: float,
    ) -> None:
        self.account_id = account_id
        self.payment_id = payment_id
        self.event_id = event_id
        self.debit_amount = debit_amount
        self.balance_before = balance_before
        self.balance_after = balance_after
        self.overdraft_limit = overdraft_limit
        self.available_debit_capacity = round(balance_before + overdraft_limit, 2)
        self.invariant = "balance_after >= -overdraft_limit"
        super().__init__(
            code=self.code,
            stage=stage,
            summary="Generated debits exceed the account's permitted overdraft boundary.",
            context={
                "Account": account_id,
                "Payment": payment_id,
                "Event": event_id,
                "Debit": debit_amount,
                "Balance before": balance_before,
                "Balance after": balance_after,
                "Allowed overdraft": overdraft_limit,
                "Available debit capacity": self.available_debit_capacity,
                "Failed invariant": self.invariant,
            },
            hints=(
                "reduce payments.daily_target",
                "reduce behavior.amount_max",
                "shorten simulation.duration_days",
                "increase account capacity",
                "use a capacity-aware scenario",
            ),
        )

    def as_dict(self) -> dict[str, Any]:
        """Return the ledger fields both flat and in the generic context."""

        result = super().as_dict()
        result.update(
            {
                "account_id": self.account_id,
                "payment_id": self.payment_id,
                "event_id": self.event_id,
                "debit_amount": self.debit_amount,
                "balance_before": self.balance_before,
                "balance_after": self.balance_after,
                "overdraft_limit": self.overdraft_limit,
                "available_debit_capacity": self.available_debit_capacity,
                "invariant": self.invariant,
            }
        )
        return result
