"""Tests for reading which option contracts Northstar knew were listed by an instant."""

from __future__ import annotations

import ast
import sqlite3
from decimal import Decimal
from pathlib import Path

import pytest
from northstar_application.ports import OptionListedContractQuery, OptionListedContractRepository
from northstar_core.derivatives import ExpirationDate
from northstar_core.foundation.value_objects import ExchangeCode, PointInTime, Symbol
from northstar_core.options import (
    OptionContract,
    OptionProductReference,
    OptionRight,
    OptionStrike,
)

import northstar_infrastructure.market_data.sqlite_option_listed_contracts as module
from northstar_infrastructure.market_data import (
    OptionListingStorageError,
    SQLiteOptionListedContractRepository,
    SQLiteOptionListingStore,
    UpstoxOptionListing,
    UpstoxOptionMasterSnapshot,
    initialize_futures_market_data_schema,
)

_NIFTY = OptionProductReference(Symbol("NIFTY"), ExchangeCode("NSE"))
_EXPIRY = ExpirationDate("2026-10-27")
_CALL, _PUT = OptionRight.CALL, OptionRight.PUT
_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
_FIRST = "2026-10-08T06:59:12.429903Z"
_SECOND = "2026-10-09T05:00:00Z"


def _contract(strike: str, right: OptionRight = _CALL, expiration: str = "2026-10-27"):
    return OptionContract(_NIFTY, ExpirationDate(expiration), OptionStrike(Decimal(strike)), right)


_C22600, _P22600 = _contract("22600"), _contract("22600", _PUT)
_C22550, _P22550 = _contract("22550"), _contract("22550", _PUT)
_C9950 = _contract("9950")
_C22650 = _contract("22650")
_NOVEMBER = _contract("22600", expiration="2026-11-24")


def _sync(database: Path, at: str, sha: str, *contracts: OptionContract) -> None:
    # A stable synthetic key per contract, as the provider keeps one key per contract.
    listings = tuple(
        UpstoxOptionListing(c, f"NSE_FO|{c.expiration_date}-{c.strike}-{c.right.value}", 65)
        for c in sorted(contracts, key=_store_order)
    )
    SQLiteOptionListingStore(database).store(
        UpstoxOptionMasterSnapshot(sha * 64, _URL, PointInTime(at), 100, len(listings), listings)
    )


def _store_order(contract: OptionContract) -> tuple:
    return (contract.expiration_date.value, contract.strike.value, contract.right.value)


@pytest.fixture
def database(tmp_path: Path) -> Path:
    """A first sync at _FIRST and a second at _SECOND that adds the 22650 CALL."""
    path = tmp_path / "northstar.sqlite3"
    first = (_P22600, _C22600, _P22550, _C22550, _C9950, _NOVEMBER)
    _sync(path, _FIRST, "a", *first)
    _sync(path, _SECOND, "b", *first, _C22650)
    return path


def _listed(database: Path, known_by: str, *, provider: str = "upstox", expiration=_EXPIRY):
    return SQLiteOptionListedContractRepository(database, provider=provider).listed_contracts(
        OptionListedContractQuery(_NIFTY, expiration, PointInTime(known_by))
    )


def _schema(database: Path) -> list[tuple]:
    with sqlite3.connect(database) as connection:
        return sorted(connection.execute("SELECT type, name, sql FROM sqlite_master").fetchall())


_FIRST_SET = (_C9950, _C22550, _P22550, _C22600, _P22600)


# ---------------------------------------------------------------------------
# Known by the instant
# ---------------------------------------------------------------------------


def test_the_repository_implements_the_port(database: Path) -> None:
    assert isinstance(
        SQLiteOptionListedContractRepository(database, provider="upstox"),
        OptionListedContractRepository,
    )


def test_listings_first_observed_before_the_instant_are_returned_in_chain_order(
    database: Path,
) -> None:
    assert _listed(database, "2026-10-08T10:10:00Z") == _FIRST_SET


def test_a_listing_first_observed_exactly_at_the_instant_is_known(database: Path) -> None:
    assert _listed(database, _FIRST) == _FIRST_SET


def test_nothing_is_known_before_the_first_observation(database: Path) -> None:
    assert _listed(database, "2026-10-07T10:10:00Z") == ()
    assert _listed(database, "2026-10-08T06:59:12.429902Z") == ()


def test_a_later_listing_appears_only_from_its_own_first_observation(database: Path) -> None:
    assert _C22650 not in _listed(database, "2026-10-08T10:10:00Z")
    assert _C22650 not in _listed(database, "2026-10-09T04:59:59.999999Z")
    later = _listed(database, "2026-10-09T10:10:00Z")
    assert later == (_C9950, _C22550, _P22550, _C22600, _P22600, _C22650)


def test_fractional_seconds_are_compared_as_instants_not_text(database: Path) -> None:
    # As text "...06:59:12Z" sorts after "...06:59:12.429903Z"; as instants it is earlier.
    assert _listed(database, "2026-10-08T06:59:12Z") == ()


def test_an_instant_with_fractional_seconds_after_a_whole_second_is_later(tmp_path: Path) -> None:
    path = tmp_path / "whole.sqlite3"
    _sync(path, "2026-10-08T10:10:00Z", "c", _C22600)

    # As text "...10:10:00.5Z" sorts before "...10:10:00Z"; as instants it is later.
    assert _listed(path, "2026-10-08T10:10:00.5Z") == (_C22600,)


def test_an_equal_instant_in_another_offset_is_the_same_instant(database: Path) -> None:
    assert _listed(database, "2026-10-08T12:29:12.429903+05:30") == _FIRST_SET


# ---------------------------------------------------------------------------
# Exact filtering
# ---------------------------------------------------------------------------


def test_only_the_requested_expiration_is_returned(database: Path) -> None:
    assert _listed(database, "2026-10-08T10:10:00Z", expiration=ExpirationDate("2026-11-24")) == (
        _NOVEMBER,
    )
    assert _listed(database, "2026-10-08T10:10:00Z", expiration=ExpirationDate("2026-10-20")) == ()


def test_only_the_configured_provider_is_read(database: Path) -> None:
    assert _listed(database, "2026-10-08T10:10:00Z", provider="other_provider") == ()


def test_another_product_or_exchange_is_not_returned(database: Path) -> None:
    with sqlite3.connect(database) as connection:
        for product, exchange, key in (("BANKNIFTY", "NSE", "x1"), ("NIFTY", "BSE", "x2")):
            connection.execute(
                "INSERT INTO option_provider_listings VALUES (?,?,?,?,?,?,?,?,?,?)",
                ("upstox", product, exchange, "2026-10-27", "22700", "CALL", f"NSE_FO|{key}",
                 "65", "a" * 64, "2026-10-08T06:59:12.429903Z"),
            )  # fmt: skip

    assert _listed(database, "2026-10-08T10:10:00Z") == _FIRST_SET


def test_strikes_order_numerically(database: Path) -> None:
    contracts = _listed(database, "2026-10-08T10:10:00Z")

    assert [c.strike.value for c in contracts] == [
        Decimal("9950"), Decimal("22550"), Decimal("22550"), Decimal("22600"), Decimal("22600")
    ]  # fmt: skip
    assert [c.right for c in contracts[1:]] == [_CALL, _PUT, _CALL, _PUT]


# ---------------------------------------------------------------------------
# Read-only and absent storage
# ---------------------------------------------------------------------------


def test_a_missing_database_holds_no_listings_and_is_not_created(tmp_path: Path) -> None:
    path = tmp_path / "absent.sqlite3"

    assert _listed(path, "2026-10-08T10:10:00Z") == ()
    assert not path.exists()


def test_a_database_without_the_listing_table_holds_no_listings(tmp_path: Path) -> None:
    path = tmp_path / "futures.sqlite3"
    with sqlite3.connect(path) as connection:
        initialize_futures_market_data_schema(connection)
    before = _schema(path)

    assert _listed(path, "2026-10-08T10:10:00Z") == ()
    assert _schema(path) == before


def test_a_read_changes_nothing(database: Path) -> None:
    before = (_schema(database), database.read_bytes())

    _listed(database, "2026-10-09T10:10:00Z")

    assert (_schema(database), database.read_bytes()) == before


@pytest.mark.parametrize(
    ("column", "value"),
    [("strike", "22600.0"), ("option_right", "CE"), ("established_at", "yesterday"),
     ("exchange_lot_size", "x")],
)  # fmt: skip
def test_a_corrupt_listing_fails_loudly(database: Path, column: str, value: str) -> None:
    with sqlite3.connect(database) as connection:
        connection.execute(
            f"UPDATE option_provider_listings SET {column} = ? "  # noqa: S608
            "WHERE strike = '22550' AND option_right = 'PUT'",
            (value,),
        )

    with pytest.raises(OptionListingStorageError):
        _listed(database, "2026-10-08T10:10:00Z")


def test_the_provider_must_be_a_canonical_name(database: Path) -> None:
    with pytest.raises(TypeError, match="canonical name"):
        SQLiteOptionListedContractRepository(database, provider="Upstox")


def test_the_query_must_be_a_listed_contract_query(database: Path) -> None:
    with pytest.raises(TypeError, match="OptionListedContractQuery"):
        SQLiteOptionListedContractRepository(database, provider="upstox").listed_contracts(
            (_NIFTY, _EXPIRY, PointInTime(_FIRST))  # type: ignore[arg-type]
        )


def test_the_module_writes_nothing_and_reaches_no_master_network_or_clock() -> None:
    source = Path(module.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported = {n.module for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module}
    names = {a.name for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) for a in n.names}

    for statement in (module._SELECT_EXPIRATION,):
        for forbidden in ("INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "REPLACE"):
            assert forbidden not in statement.upper()
    assert not [name for name in imported if "upstox" in name or "futures" in name]
    assert not [name for name in names if name.startswith("initialize")]
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute):
            assert node.attr not in {"now", "utcnow", "today", "monotonic"}
    assert "mode=ro" in source
