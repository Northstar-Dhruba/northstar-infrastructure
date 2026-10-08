"""Tests for reading one option expiration's daily bars at one exact instant."""

from __future__ import annotations

import ast
import sqlite3
from decimal import Decimal
from pathlib import Path

import pytest
from northstar_application.ports import OptionChainDailyBarQuery, OptionChainDailyBarRepository
from northstar_core.derivatives import ExpirationDate
from northstar_core.foundation.value_objects import (
    ExchangeCode,
    PointInTime,
    Quantity,
    Symbol,
    Timeframe,
)
from northstar_core.options import (
    OptionContract,
    OptionOHLCVBar,
    OptionPremium,
    OptionProductReference,
    OptionRight,
    OptionStrike,
)

import northstar_infrastructure.market_data.sqlite_option_chain_daily_bars as module
from northstar_infrastructure.market_data import (
    OptionHistoricalStorageError,
    SQLiteOptionChainDailyBarRepository,
    SQLiteOptionHistoricalMarketDataStore,
    initialize_futures_market_data_schema,
)

_NIFTY = OptionProductReference(Symbol("NIFTY"), ExchangeCode("NSE"))
_BANKNIFTY = OptionProductReference(Symbol("BANKNIFTY"), ExchangeCode("NSE"))
_NIFTY_BSE = OptionProductReference(Symbol("NIFTY"), ExchangeCode("BSE"))
_EXPIRY = ExpirationDate("2026-10-27")
_CALL, _PUT = OptionRight.CALL, OptionRight.PUT
_OCT7, _OCT8, _OCT9 = (
    "2026-10-07T10:10:00Z",
    "2026-10-08T10:10:00Z",
    "2026-10-09T10:10:00Z",
)


def _contract(strike: str, right=_CALL, expiration: str = "2026-10-27", product=_NIFTY):
    return OptionContract(product, ExpirationDate(expiration), OptionStrike(Decimal(strike)), right)


def _bar(contract: OptionContract, instant: str = _OCT8, timeframe: str = "1d", close="132.6"):
    return OptionOHLCVBar(
        contract=contract,
        point_in_time=PointInTime(instant),
        timeframe=Timeframe(timeframe),
        open=OptionPremium(Decimal("191.8")),
        high=OptionPremium(Decimal("226.05")),
        low=OptionPremium(Decimal("122.45")),
        close=OptionPremium(Decimal(close)),
        volume=Quantity(Decimal("40")),
    )


_C9950, _C22550, _P22550 = _contract("9950"), _contract("22550"), _contract("22550", _PUT)
_C22600, _P22600 = _contract("22600"), _contract("22600", _PUT)
_AT_OCT8 = (_bar(_C9950), _bar(_C22550), _bar(_P22550), _bar(_C22600), _bar(_P22600))


@pytest.fixture
def database(tmp_path: Path) -> Path:
    """The Oct-8 cross-section plus bars the query must never return."""
    path = tmp_path / "northstar.sqlite3"
    SQLiteOptionHistoricalMarketDataStore(path).store(
        (
            *reversed(_AT_OCT8),
            _bar(_C22600, _OCT7, close="150"),
            _bar(_C22600, _OCT9, close="140"),
            _bar(_C22600, "2026-10-08T09:10:00Z"),
            _bar(_C22600, _OCT8, timeframe="1h"),
            _bar(_contract("22600", expiration="2026-11-24")),
            _bar(_contract("22600", product=_BANKNIFTY)),
            _bar(_contract("22600", product=_NIFTY_BSE)),
        )
    )
    return path


def _at(database: Path, instant: str = _OCT8, expiration: ExpirationDate = _EXPIRY, product=_NIFTY):
    return SQLiteOptionChainDailyBarRepository(database).daily_bars_at(
        OptionChainDailyBarQuery(product, expiration, PointInTime(instant))
    )


def _schema(database: Path) -> list[tuple]:
    with sqlite3.connect(database) as connection:
        return sorted(connection.execute("SELECT type, name, sql FROM sqlite_master").fetchall())


def test_the_repository_implements_the_port(database: Path) -> None:
    assert isinstance(SQLiteOptionChainDailyBarRepository(database), OptionChainDailyBarRepository)


def test_exactly_the_expirations_daily_bars_at_the_instant_are_returned_in_order(
    database: Path,
) -> None:
    assert _at(database) == _AT_OCT8


def test_another_instant_returns_only_its_own_bars(database: Path) -> None:
    assert _at(database, _OCT7) == (_bar(_C22600, _OCT7, close="150"),)
    assert _at(database, _OCT9) == (_bar(_C22600, _OCT9, close="140"),)


def test_an_instant_without_bars_never_falls_back_to_another(database: Path) -> None:
    assert _at(database, "2026-10-12T10:10:00Z") == ()
    assert _at(database, "2026-10-08T10:10:01Z") == ()


def test_an_equal_instant_in_another_offset_matches(database: Path) -> None:
    assert _at(database, "2026-10-08T15:40:00+05:30") == _AT_OCT8


def test_only_daily_bars_are_returned(database: Path) -> None:
    assert {bar.timeframe for bar in _at(database)} == {Timeframe("1d")}


def test_another_expiration_product_or_exchange_is_separate(database: Path) -> None:
    november = _at(database, expiration=ExpirationDate("2026-11-24"))

    assert november == (_bar(_contract("22600", expiration="2026-11-24")),)
    assert _at(database, product=_BANKNIFTY) == (_bar(_contract("22600", product=_BANKNIFTY)),)
    assert _at(database, product=_NIFTY_BSE) == (_bar(_contract("22600", product=_NIFTY_BSE)),)
    assert _at(database, expiration=ExpirationDate("2026-10-20")) == ()


def test_the_cross_section_is_one_query(database: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    statements: list[str] = []
    connect = sqlite3.connect

    def traced(*args, **kwargs):
        connection = connect(*args, **kwargs)
        connection.set_trace_callback(statements.append)
        return connection

    monkeypatch.setattr(module.sqlite3, "connect", traced)

    _at(database)

    assert [s for s in statements if "option_ohlcv" in s and "sqlite_master" not in s] == [
        statements[-1]
    ]
    assert len(statements) == 2


def test_a_missing_database_holds_no_bars_and_is_not_created(tmp_path: Path) -> None:
    path = tmp_path / "absent.sqlite3"

    assert _at(path) == ()
    assert not path.exists()


def test_a_database_without_the_option_table_holds_no_bars(tmp_path: Path) -> None:
    path = tmp_path / "futures.sqlite3"
    with sqlite3.connect(path) as connection:
        initialize_futures_market_data_schema(connection)
    before = _schema(path)

    assert _at(path) == ()
    assert _schema(path) == before


def test_a_read_changes_nothing(database: Path) -> None:
    before = (_schema(database), database.read_bytes())

    _at(database)

    assert (_schema(database), database.read_bytes()) == before


@pytest.mark.parametrize(
    ("column", "value"),
    [("close_premium", "abc"), ("close_premium", "132.60"), ("volume", "40.5"),
     ("strike", "22600.0")],
)  # fmt: skip
def test_a_corrupt_bar_fails_loudly(database: Path, column: str, value: str) -> None:
    with sqlite3.connect(database) as connection:
        connection.execute(
            f"UPDATE option_ohlcv SET {column} = ? "  # noqa: S608
            "WHERE strike = '22600' AND option_right = 'PUT' AND point_in_time = ?",
            (value, _OCT8),
        )

    with pytest.raises(OptionHistoricalStorageError):
        _at(database)


def test_the_query_must_be_a_chain_daily_bar_query(database: Path) -> None:
    with pytest.raises(TypeError, match="OptionChainDailyBarQuery"):
        SQLiteOptionChainDailyBarRepository(database).daily_bars_at(
            (_NIFTY, _EXPIRY, PointInTime(_OCT8))  # type: ignore[arg-type]
        )


def test_the_module_writes_nothing_and_reads_no_open_interest_or_clock() -> None:
    source = Path(module.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)

    for forbidden in ("INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "REPLACE"):
        assert forbidden not in module._SELECT_AT.upper()
    assert "open_interest" not in source.replace("Raw provider open interest", "")
    names = {a.name for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) for a in n.names}
    assert not [name for name in names if name.startswith("initialize")]
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute):
            assert node.attr not in {"now", "utcnow", "today", "monotonic"}
    assert "mode=ro" in source
