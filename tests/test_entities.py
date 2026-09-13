from pathlib import Path

import polars as pl
from hypothesis import given
from hypothesis import strategies as st

from fraudtwin.config import load_config
from fraudtwin.simulation import EntityGenerator
from fraudtwin.simulation.parquet import ENTITY_SCHEMAS, write_entity_parquet


@given(
    customers=st.integers(min_value=1, max_value=12),
    institutions=st.integers(min_value=1, max_value=5),
    accounts=st.integers(min_value=1, max_value=24),
    cards=st.integers(min_value=0, max_value=12),
    pix_keys=st.integers(min_value=0, max_value=12),
)
def test_generated_relationships_hold_across_population_sizes(
    customers: int, institutions: int, accounts: int, cards: int, pix_keys: int
) -> None:
    base_config = load_config(Path("configs/minimal.yaml"))
    population = base_config.population.model_copy(
        update={
            "customers": customers,
            "institutions": institutions,
            "accounts": accounts,
            "cards": cards,
            "pix_keys": pix_keys,
        }
    )
    config = base_config.model_copy(update={"population": population})
    dataset = EntityGenerator(config).generate()

    account_ids = {account.account_id for account in dataset.accounts}
    account_by_id = {account.account_id: account for account in dataset.accounts}
    institution_ids = {institution.institution_id for institution in dataset.institutions}
    assert len(account_ids) == accounts
    assert all(account.customer_id for account in dataset.accounts)
    assert all(account.institution_id in institution_ids for account in dataset.accounts)
    assert all(card.account_id in account_ids for card in dataset.cards)
    assert all(
        card.customer_id == account_by_id[card.account_id].customer_id for card in dataset.cards
    )
    assert all(key.account_id in account_ids for key in dataset.pix_keys)
    assert all(
        key.customer_id == account_by_id[key.account_id].customer_id for key in dataset.pix_keys
    )


def test_generator_honors_configured_counts_and_relationships() -> None:
    dataset = EntityGenerator(load_config(Path("configs/minimal.yaml"))).generate()

    assert dataset.counts == {
        "customers": 10,
        "institutions": 3,
        "accounts": 15,
        "cards": 12,
        "merchants": 3,
        "devices": 12,
        "pix_keys": 8,
    }
    assert len({entity.customer_id for entity in dataset.customers}) == len(dataset.customers)
    assert len({entity.institution_id for entity in dataset.institutions}) == len(
        dataset.institutions
    )
    assert len({entity.account_id for entity in dataset.accounts}) == len(dataset.accounts)
    assert len({entity.card_id for entity in dataset.cards}) == len(dataset.cards)
    assert len({entity.merchant_id for entity in dataset.merchants}) == len(dataset.merchants)
    assert len({entity.device_id for entity in dataset.devices}) == len(dataset.devices)
    assert len({entity.pix_key_id for entity in dataset.pix_keys}) == len(dataset.pix_keys)

    customer_ids = {entity.customer_id for entity in dataset.customers}
    institution_ids = {entity.institution_id for entity in dataset.institutions}
    accounts_by_id = {entity.account_id: entity for entity in dataset.accounts}
    assert all(account.customer_id in customer_ids for account in dataset.accounts)
    assert all(account.institution_id in institution_ids for account in dataset.accounts)
    assert all(
        card.account_id in accounts_by_id
        and card.customer_id == accounts_by_id[card.account_id].customer_id
        for card in dataset.cards
    )
    assert all(
        key.account_id in accounts_by_id
        and key.customer_id == accounts_by_id[key.account_id].customer_id
        and key.institution_id == accounts_by_id[key.account_id].institution_id
        for key in dataset.pix_keys
    )
    assert all(merchant.acquirer_id in institution_ids for merchant in dataset.merchants)


def test_same_seed_produces_equivalent_entity_dataset() -> None:
    config = load_config(Path("configs/minimal.yaml"))

    first = EntityGenerator(config).generate()
    second = EntityGenerator(config).generate()

    assert first == second


def test_entity_parquet_files_have_stable_schemas(tmp_path: Path) -> None:
    dataset = EntityGenerator(load_config(Path("configs/minimal.yaml"))).generate()
    paths = write_entity_parquet(dataset, tmp_path / "run-1")

    assert set(paths) == set(ENTITY_SCHEMAS)
    for entity_name, schema in ENTITY_SCHEMAS.items():
        path = tmp_path / "run-1" / "entities" / f"{entity_name}.parquet"
        assert path == paths[entity_name]
        frame = pl.read_parquet(path)
        assert frame.columns == list(schema)
        assert frame.schema == schema
        assert frame.height == dataset.counts[entity_name]
