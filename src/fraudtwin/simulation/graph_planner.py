"""Deterministic participant planning for M11 scenario instances."""

from dataclasses import dataclass

from fraudtwin.config import GraphScenarioConfig, SimulationRunConfig, graph_account_capacity
from fraudtwin.domain import Account, Device, Merchant, NetworkEndpoint, PixKey


@dataclass(frozen=True)
class PlannedScenario:
    """Resources reserved for one scenario instance."""

    scenario: GraphScenarioConfig
    ordinal: int
    accounts: tuple[Account, ...]
    merchants: tuple[Merchant, ...]
    devices: tuple[Device, ...]
    endpoints: tuple[NetworkEndpoint, ...]
    pix_keys: tuple[PixKey, ...]


class GraphScenarioPlanner:
    """Allocate disjoint resources in stable scenario/configuration order."""

    def __init__(
        self,
        config: SimulationRunConfig,
        accounts: tuple[Account, ...],
        merchants: tuple[Merchant, ...] = (),
        devices: tuple[Device, ...] = (),
        endpoints: tuple[NetworkEndpoint, ...] = (),
        pix_keys: tuple[PixKey, ...] = (),
    ) -> None:
        self.config = config
        self.accounts = tuple(sorted(accounts, key=lambda item: item.account_id))
        self.merchants = tuple(sorted(merchants, key=lambda item: item.merchant_id))
        self.devices = tuple(sorted(devices, key=lambda item: item.device_id))
        self.endpoints = tuple(sorted(endpoints, key=lambda item: item.endpoint_id))
        self.pix_keys = tuple(sorted(pix_keys, key=lambda item: item.pix_key_id))

    def plan(self) -> tuple[PlannedScenario, ...]:
        required_accounts = sum(
            scenario.count * graph_account_capacity(scenario)
            for scenario in self.config.graph.scenarios
        )
        if required_accounts > len(self.accounts):
            raise ValueError("graph scenarios require more disjoint accounts than available")
        ip_count = sum(
            scenario.count
            for scenario in self.config.graph.scenarios
            if scenario.type == "SHARED_IP_INFRASTRUCTURE"
        )
        if ip_count > len(self.endpoints):
            raise ValueError("graph scenarios require more IP endpoints than available")
        device_count = sum(
            scenario.count
            for scenario in self.config.graph.scenarios
            if scenario.type == "SHARED_DEVICE_INFRASTRUCTURE"
        )
        if device_count > len(self.devices):
            raise ValueError("graph scenarios require more devices than available")
        merchant_count = sum(
            scenario.count * (scenario.merchant_count or 0)
            for scenario in self.config.graph.scenarios
            if scenario.type == "MERCHANT_CUSTOMER_COMMUNITY"
        )
        if merchant_count > len(self.merchants):
            raise ValueError("merchant-community scenarios require more merchants than available")
        plans: list[PlannedScenario] = []
        used_accounts: set[str] = set()
        used_customers: set[str] = set()
        merchant_cursor = endpoint_cursor = device_cursor = 0
        ordinal = 0
        for scenario in self.config.graph.scenarios:
            for _ in range(scenario.count):
                ordinal += 1
                account_count = graph_account_capacity(scenario)
                accounts = self._allocate_accounts(
                    scenario, account_count, used_accounts, used_customers
                )
                used_accounts.update(item.account_id for item in accounts)
                used_customers.update(item.customer_id for item in accounts)
                minimum_amount = scenario.min_amount or self.config.behavior.amount_min
                if any(a.available_balance < minimum_amount for a in accounts):
                    raise ValueError("graph scenario source account balance is insufficient")
                merchants = self.merchants[
                    merchant_cursor : merchant_cursor + (scenario.merchant_count or 0)
                ]
                merchant_cursor += scenario.merchant_count or 0
                endpoints = self.endpoints[
                    endpoint_cursor : endpoint_cursor
                    + (1 if scenario.type == "SHARED_IP_INFRASTRUCTURE" else 0)
                ]
                endpoint_cursor += 1 if scenario.type == "SHARED_IP_INFRASTRUCTURE" else 0
                devices = (
                    self.devices[device_cursor : device_cursor + 1]
                    if scenario.type == "SHARED_DEVICE_INFRASTRUCTURE"
                    else ()
                )
                device_cursor += 1 if scenario.type == "SHARED_DEVICE_INFRASTRUCTURE" else 0
                keys = tuple(
                    key
                    for key in self.pix_keys
                    if key.account_id in {a.account_id for a in accounts}
                )
                plans.append(
                    PlannedScenario(
                        scenario, ordinal, accounts, merchants, devices, endpoints, keys
                    )
                )
        return tuple(plans)

    def _allocate_accounts(
        self,
        scenario: GraphScenarioConfig,
        count: int,
        used_accounts: set[str],
        used_customers: set[str],
    ) -> tuple[Account, ...]:
        """Select a deterministic, disjoint account set satisfying institution scope."""

        available = [
            item
            for item in self.accounts
            if item.account_id not in used_accounts and item.customer_id not in used_customers
        ]
        if scenario.institution_scope == "SINGLE_INSTITUTION":
            by_institution: dict[str, list[Account]] = {}
            for account in available:
                by_institution.setdefault(account.institution_id, []).append(account)
            groups = [group for _, group in sorted(by_institution.items()) if len(group) >= count]
            if not groups:
                raise ValueError("single-institution scenario lacks account capacity")
            return tuple(groups[0][:count])
        if (
            scenario.institution_scope == "CROSS_INSTITUTION"
            or "CROSS_INSTITUTION" in scenario.modifiers
        ):
            selected: list[Account] = []
            institutions: set[str] = set()
            for account in available:
                if len(selected) == count:
                    break
                if (
                    not selected
                    or account.institution_id not in institutions
                    or len(available) - len(selected) <= count - len(selected)
                ):
                    selected.append(account)
                    institutions.add(account.institution_id)
            if len(selected) < count or len(institutions) < 2:
                raise ValueError("cross-institution scenario requires multiple institutions")
            return tuple(selected)
        if len(available) < count:
            raise ValueError(f"graph scenario requires {count} disjoint accounts")
        return tuple(available[:count])


__all__ = ["GraphScenarioPlanner", "PlannedScenario"]
