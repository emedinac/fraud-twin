"""Deterministic generation of the Milestone 1 entity population."""

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from random import Random
from typing import Literal

from fraudtwin.config import SimulationRunConfig
from fraudtwin.domain import Account, Card, Customer, Device, Institution, Merchant, PixKey
from fraudtwin.seed import create_legacy_entity_stream_rng

_ID_WIDTH = 6
_CUSTOMER_HISTORY_DAYS = 3650
_ACCOUNT_HISTORY_DAYS = 1825
_DEVICE_HISTORY_DAYS = 730
_CARD_ISSUANCE_HISTORY_DAYS = 365
_CARD_VALIDITY_DAYS = 3 * 365
_COUNTRIES = ("BR", "US", "GB", "DE")
_CITIES = ("Aurora", "Boreal", "Cascata", "Dourado")
_RISK_SEGMENTS = ("LOW", "MEDIUM", "HIGH")
Entity = Account | Card | Customer | Device | Institution | Merchant | PixKey
InstitutionType = Literal[
    "BANK", "PSP", "ISSUER", "ACQUIRER", "DIGITAL_BANK", "PAYMENT_INSTITUTION"
]
AccountType = Literal[
    "CHECKING",
    "PAYMENT_ACCOUNT",
    "CREDIT_CARD_ACCOUNT",
    "SAVINGS",
    "PERSONAL_LOAN",
    "BUSINESS_ACCOUNT",
]
DeviceType = Literal["MOBILE", "DESKTOP", "TABLET", "POS_TERMINAL", "ATM"]
PixKeyType = Literal["CPF_LIKE", "PHONE", "EMAIL", "RANDOM", "BUSINESS_ID_LIKE"]


def _entity_id(prefix: str, number: int) -> str:
    return f"{prefix}-{number:0{_ID_WIDTH}d}"


def _entity_stream_rng(seed: int, entity_type: str) -> Random:
    """Return the historical, isolated RNG stream for one entity table."""

    return create_legacy_entity_stream_rng(seed, entity_type)


def _synthetic_datetime(start: datetime, rng: Random, days_back: int) -> datetime:
    """Return a deterministic UTC timestamp at or before simulation start."""

    start_utc = start.astimezone(UTC)
    offset = timedelta(
        days=rng.randint(0, days_back),
        hours=rng.randint(0, 23),
        minutes=rng.randint(0, 59),
    )
    return start_utc - offset


def _synthetic_date(start: datetime, rng: Random, min_age: int, max_age: int) -> date:
    age_days = rng.randint(min_age * 365, max_age * 365)
    return start.astimezone(UTC).date() - timedelta(days=age_days)


@dataclass(frozen=True)
class EntityDataset:
    """Stable, ordered collections of all Milestone 1 entities."""

    customers: tuple[Customer, ...]
    institutions: tuple[Institution, ...]
    accounts: tuple[Account, ...]
    cards: tuple[Card, ...]
    merchants: tuple[Merchant, ...]
    devices: tuple[Device, ...]
    pix_keys: tuple[PixKey, ...]

    @property
    def counts(self) -> dict[str, int]:
        """Return output counts using the population configuration names."""

        return {
            "customers": len(self.customers),
            "institutions": len(self.institutions),
            "accounts": len(self.accounts),
            "cards": len(self.cards),
            "merchants": len(self.merchants),
            "devices": len(self.devices),
            "pix_keys": len(self.pix_keys),
        }

    def tables(self) -> dict[str, tuple[Entity, ...]]:
        """Return entities keyed by their stable output table names."""

        return {
            "customers": self.customers,
            "institutions": self.institutions,
            "accounts": self.accounts,
            "cards": self.cards,
            "merchants": self.merchants,
            "devices": self.devices,
            "pix_keys": self.pix_keys,
        }


class EntityGenerator:
    """Generate a reproducible population from a validated configuration."""

    def __init__(self, config: SimulationRunConfig) -> None:
        self.config = config
        self.start = config.simulation.start.astimezone(UTC)
        self.population = config.population

    def generate(self) -> EntityDataset:
        """Generate all entities in dependency order with isolated streams."""

        customers = self._customers()
        institutions = self._institutions()
        accounts = self._accounts(customers, institutions)
        cards = self._cards(accounts)
        merchants = self._merchants(institutions)
        devices = self._devices()
        pix_keys = self._pix_keys(accounts)
        return EntityDataset(customers, institutions, accounts, cards, merchants, devices, pix_keys)

    def _customers(self) -> tuple[Customer, ...]:
        rng = _entity_stream_rng(self.config.simulation.seed, "customers")
        income_bands = ("LOW", "MIDDLE", "HIGH")
        occupations = ("ENGINEERING", "EDUCATION", "HEALTHCARE", "COMMERCE")
        channels = (("MOBILE",), ("WEB",), ("MOBILE", "WEB"))
        records: list[Customer] = []
        for number in range(1, self.population.customers + 1):
            registration = _synthetic_datetime(self.start, rng, _CUSTOMER_HISTORY_DAYS)
            records.append(
                Customer(
                    customer_id=_entity_id("CUS", number),
                    customer_type=rng.choice(("PERSONAL", "BUSINESS")),
                    customer_status="ACTIVE",
                    date_of_birth=_synthetic_date(self.start, rng, 18, 80),
                    country=rng.choice(_COUNTRIES),
                    city=rng.choice(_CITIES),
                    registration_date=registration,
                    risk_segment=rng.choice(_RISK_SEGMENTS),
                    income_band=rng.choice(income_bands),
                    occupation_category=rng.choice(occupations),
                    preferred_channels=rng.choice(channels),
                    created_at=registration,
                    updated_at=self.start,
                    valid_from=registration,
                    valid_to=None,
                    system_from=registration,
                    system_to=None,
                )
            )
        return tuple(records)

    def _institutions(self) -> tuple[Institution, ...]:
        rng = _entity_stream_rng(self.config.simulation.seed, "institutions")
        institution_types: tuple[InstitutionType, ...] = (
            "BANK",
            "PSP",
            "ISSUER",
            "ACQUIRER",
            "DIGITAL_BANK",
            "PAYMENT_INSTITUTION",
        )
        records: list[Institution] = []
        for number in range(1, self.population.institutions + 1):
            records.append(
                Institution(
                    institution_id=_entity_id("INS", number),
                    institution_type=rng.choice(institution_types),
                    country="BR",
                    institution_code=f"SYN{number:0{_ID_WIDTH}d}",
                    risk_profile=rng.choice(("CONSERVATIVE", "STANDARD", "ELEVATED")),
                    processing_latency_profile=rng.choice(("FAST", "STANDARD", "SLOW")),
                )
            )
        return tuple(records)

    def _accounts(
        self, customers: tuple[Customer, ...], institutions: tuple[Institution, ...]
    ) -> tuple[Account, ...]:
        rng = _entity_stream_rng(self.config.simulation.seed, "accounts")
        account_types: tuple[AccountType, ...] = (
            "CHECKING",
            "PAYMENT_ACCOUNT",
            "CREDIT_CARD_ACCOUNT",
            "SAVINGS",
            "PERSONAL_LOAN",
            "BUSINESS_ACCOUNT",
        )
        records: list[Account] = []
        for number in range(1, self.population.accounts + 1):
            opening = _synthetic_datetime(self.start, rng, _ACCOUNT_HISTORY_DAYS)
            balance = round(rng.uniform(100.0, 25_000.0), 2)
            records.append(
                Account(
                    account_id=_entity_id("ACC", number),
                    customer_id=rng.choice(customers).customer_id,
                    institution_id=rng.choice(institutions).institution_id,
                    account_type=rng.choice(account_types),
                    currency="BRL",
                    opening_date=opening,
                    closing_date=None,
                    status="ACTIVE",
                    credit_limit=round(rng.uniform(0.0, 50_000.0), 2),
                    available_balance=balance,
                    ledger_balance=balance,
                    overdraft_limit=round(rng.uniform(0.0, 5_000.0), 2),
                    created_at=opening,
                    updated_at=self.start,
                    valid_from=opening,
                    valid_to=None,
                    system_from=opening,
                    system_to=None,
                )
            )
        return tuple(records)

    def _cards(self, accounts: tuple[Account, ...]) -> tuple[Card, ...]:
        rng = _entity_stream_rng(self.config.simulation.seed, "cards")
        records: list[Card] = []
        for number in range(1, self.population.cards + 1):
            account = rng.choice(accounts)
            issued = self.start - timedelta(days=rng.randint(0, _CARD_ISSUANCE_HISTORY_DAYS))
            records.append(
                Card(
                    card_id=_entity_id("CARD", number),
                    account_id=account.account_id,
                    customer_id=account.customer_id,
                    scheme=rng.choice(("VISA", "MASTERCARD", "OTHER")),
                    card_type=rng.choice(("DEBIT", "CREDIT", "PREPAID")),
                    status="ACTIVE",
                    issued_at=issued,
                    expires_at=issued + timedelta(days=_CARD_VALIDITY_DAYS),
                    country="BR",
                    network_token_enabled=rng.choice((True, False)),
                    contactless_enabled=True,
                    online_enabled=True,
                    international_enabled=rng.choice((True, False)),
                    daily_limit=round(rng.uniform(500.0, 20_000.0), 2),
                    transaction_limit=round(rng.uniform(100.0, 10_000.0), 2),
                )
            )
        return tuple(records)

    def _merchants(self, institutions: tuple[Institution, ...]) -> tuple[Merchant, ...]:
        rng = _entity_stream_rng(self.config.simulation.seed, "merchants")
        records: list[Merchant] = []
        for number in range(1, self.population.merchants + 1):
            records.append(
                Merchant(
                    merchant_id=_entity_id("MER", number),
                    merchant_name=f"Synthetic Merchant {number:0{_ID_WIDTH}d}",
                    merchant_category_code=rng.choice(("5411", "5311", "5732", "5812")),
                    country="BR",
                    city=rng.choice(_CITIES),
                    risk_segment=rng.choice(_RISK_SEGMENTS),
                    acquirer_id=rng.choice(institutions).institution_id,
                    online_only=rng.choice((True, False)),
                    created_at=_synthetic_datetime(self.start, rng, _ACCOUNT_HISTORY_DAYS),
                )
            )
        return tuple(records)

    def _devices(self) -> tuple[Device, ...]:
        rng = _entity_stream_rng(self.config.simulation.seed, "devices")
        records: list[Device] = []
        device_types: tuple[DeviceType, ...] = (
            "MOBILE",
            "DESKTOP",
            "TABLET",
            "POS_TERMINAL",
            "ATM",
        )
        os_families = ("ANDROID", "IOS", "WINDOWS", "LINUX")
        browser_families = ("CHROMIUM", "SAFARI", "FIREFOX", "EMBEDDED")
        for number in range(1, self.population.devices + 1):
            first_seen = _synthetic_datetime(self.start, rng, _DEVICE_HISTORY_DAYS)
            last_seen = self.start + timedelta(
                days=rng.randint(0, self.config.simulation.duration_days),
                hours=rng.randint(0, 23),
            )
            records.append(
                Device(
                    device_id=_entity_id("DEV", number),
                    device_type=rng.choice(device_types),
                    os_family=rng.choice(os_families),
                    browser_family=rng.choice(browser_families),
                    first_seen_at=first_seen,
                    last_seen_at=last_seen,
                    trusted=rng.choice((True, False)),
                    device_fingerprint=f"synthetic-fingerprint-{number:0{_ID_WIDTH}d}",
                    risk_score=round(rng.random(), 6),
                )
            )
        return tuple(records)

    def _pix_keys(
        self,
        accounts: tuple[Account, ...],
    ) -> tuple[PixKey, ...]:
        rng = _entity_stream_rng(self.config.simulation.seed, "pix_keys")
        records: list[PixKey] = []
        key_types: tuple[PixKeyType, ...] = (
            "CPF_LIKE",
            "PHONE",
            "EMAIL",
            "RANDOM",
            "BUSINESS_ID_LIKE",
        )
        for number in range(1, self.population.pix_keys + 1):
            account = rng.choice(accounts)
            records.append(
                PixKey(
                    pix_key_id=_entity_id("PIX", number),
                    account_id=account.account_id,
                    customer_id=account.customer_id,
                    institution_id=account.institution_id,
                    key_type=rng.choice(key_types),
                    key_hash_or_synthetic_value=f"synthetic-key-{number:0{_ID_WIDTH}d}",
                    created_at=account.opening_date,
                    status="ACTIVE",
                )
            )
        return tuple(records)


def generate_entities(config: SimulationRunConfig) -> EntityDataset:
    """Convenience function for deterministic entity generation."""

    return EntityGenerator(config).generate()
