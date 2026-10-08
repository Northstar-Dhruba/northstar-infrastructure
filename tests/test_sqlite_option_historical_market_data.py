"""Tests for the SQLite canonical option daily market data store and repository."""

from __future__ import annotations

import sqlite3
from decimal import ROUND_DOWN, Decimal, localcontext
from pathlib import Path

import pytest
from northstar_application.ports import (
    OptionHistoricalMarketDataConflictError,
    OptionHistoricalMarketDataQuery,
    OptionHistoricalMarketDataRepository,
    OptionHistoricalMarketDataStore,
)
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

import northstar_infrastructure.market_data.sqlite_option_historical_market_data as module
import northstar_infrastructure.market_data.sqlite_option_market_data_schema as schema
from northstar_infrastructure.market_data import (
    OptionHistoricalStorageError,
    SQLiteOptionHistoricalMarketDataRepository,
    SQLiteOptionHistoricalMarketDataStore,
    initialize_futures_market_data_schema,
    initialize_option_market_data_schema,
)
from northstar_infrastructure.persistence import initialize_futures_contract_economics_schema

_NIFTY = OptionProductReference(Symbol("NIFTY"), ExchangeCode("NSE"))
_DAILY = Timeframe("1d")
_T1 = PointInTime("2026-10-08T10:10:00Z")
_T2 = PointInTime("2026-10-09T10:10:00Z")
_T3 = PointInTime("2026-10-12T10:10:00Z")


def _contract(
    expiration: str = "2026-10-27", strike: str = "25000", right: OptionRight = OptionRight.CALL
) -> OptionContract:
    return OptionContract(_NIFTY, ExpirationDate(expiration), OptionStrike(Decimal(strike)), right)


def _p(value: str) -> OptionPremium:
    return OptionPremium(Decimal(value))


def _bar(
    contract: OptionContract | None = None,
    at: PointInTime = _T1,
    close: str = "186.1",
    volume: str = "1200",
    timeframe: Timeframe = _DAILY,
) -> OptionOHLCVBar:
    return OptionOHLCVBar(
        contract=_contract() if contract is None else contract,
        point_in_time=at,
        timeframe=timeframe,
        open=_p("182.35"),
        high=_p("190"),
        low=_p("175.5"),
        close=_p(close),
        volume=Quantity(Decimal(volume)),
    )


_ROW = (
    "NIFTY", "NSE", "2026-10-27", "25000", "CALL", "1d", "2026-10-08T10:10:00Z",
    "182.35", "190", "175.5", "186.1", "1200",
)  # fmt: skip


@pytest.fixture
def database(tmp_path: Path) -> Path:
    return tmp_path / "northstar.sqlite3"


def _store(database: Path, *bars: OptionOHLCVBar) -> int:
    return SQLiteOptionHistoricalMarketDataStore(database).store(bars)


def _get(database: Path, contract: OptionContract | None = None, **window) -> tuple:
    query = OptionHistoricalMarketDataQuery(
        _contract() if contract is None else contract, window.pop("timeframe", _DAILY), **window
    )
    return SQLiteOptionHistoricalMarketDataRepository(database).get_bars(query)


def _query(database: Path, sql: str, parameters: tuple = ()) -> list[tuple]:
    with sqlite3.connect(database) as connection:
        return connection.execute(sql, parameters).fetchall()


def _execute(database: Path, sql: str, parameters: tuple = ()) -> None:
    with sqlite3.connect(database) as connection:
        connection.execute(sql, parameters)


def _rows(database: Path) -> list[tuple]:
    return _query(database, "SELECT * FROM option_ohlcv ORDER BY 3, 4, 5, 7")


def _tables(database: Path) -> set[str]:
    return {row[0] for row in _query(database, "SELECT name FROM sqlite_master WHERE type='table'")}


def _schema(database: Path) -> list[tuple]:
    return sorted(_query(database, "SELECT type, name, sql FROM sqlite_master"))


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


def test_a_store_creates_exactly_the_option_ohlcv_table(database: Path) -> None:
    _store(database, _bar())

    assert _tables(database) == {"option_ohlcv"}


def test_the_table_shape_and_primary_key_are_exact(database: Path) -> None:
    with sqlite3.connect(database) as connection:
        initialize_option_market_data_schema(connection)
    columns = [
        (name, kind, notnull, pk)
        for _, name, kind, notnull, _, pk in _query(database, "PRAGMA table_info(option_ohlcv)")
    ]

    assert columns == [
        ("product_code", "TEXT", 1, 1),
        ("exchange_code", "TEXT", 1, 2),
        ("expiration_date", "TEXT", 1, 3),
        ("strike", "TEXT", 1, 4),
        ("option_right", "TEXT", 1, 5),
        ("timeframe", "TEXT", 1, 6),
        ("point_in_time", "TEXT", 1, 7),
        ("open_premium", "TEXT", 1, 0),
        ("high_premium", "TEXT", 1, 0),
        ("low_premium", "TEXT", 1, 0),
        ("close_premium", "TEXT", 1, 0),
        ("volume", "TEXT", 1, 0),
    ]


def test_the_schema_has_no_open_interest_or_provider_column(database: Path) -> None:
    with sqlite3.connect(database) as connection:
        initialize_option_market_data_schema(connection)
    names = " ".join(row[1] for row in _query(database, "PRAGMA table_info(option_ohlcv)"))

    for forbidden in ("interest", "provider", "instrument", "bid", "ask", "settlement", "iv"):
        assert forbidden not in names.split()


def test_futures_tables_are_left_untouched(database: Path) -> None:
    with sqlite3.connect(database) as connection:
        initialize_futures_market_data_schema(connection)
        initialize_futures_contract_economics_schema(connection)
    before = set(_query(database, "SELECT type, name, sql FROM sqlite_master"))

    _store(database, _bar())

    after = set(_query(database, "SELECT type, name, sql FROM sqlite_master"))
    assert before <= after
    assert {name for _, name, _ in after - before} == {
        "option_ohlcv",
        "sqlite_autoindex_option_ohlcv_1",
    }


def test_the_initializer_is_repeatable(database: Path) -> None:
    _store(database, _bar())
    before = _schema(database)

    with sqlite3.connect(database) as connection:
        initialize_option_market_data_schema(connection)
        initialize_option_market_data_schema(connection)

    assert _schema(database) == before
    assert _get(database) == (_bar(),)


# ---------------------------------------------------------------------------
# Round trip and canonical text
# ---------------------------------------------------------------------------


def test_a_bar_round_trips_as_canonical_text(database: Path) -> None:
    respelled = OptionOHLCVBar(
        contract=_contract(strike="2.5E+4"),
        point_in_time=PointInTime("2026-10-08T15:40:00+05:30"),
        timeframe=_DAILY,
        open=_p("182.350"),
        high=_p("190.0"),
        low=_p("175.50"),
        close=_p("186.10"),
        volume=Quantity(Decimal("1.2E+3")),
    )

    assert _store(database, respelled) == 1

    assert _rows(database) == [_ROW]
    columns = ", ".join(f"typeof({c})" for c in module._COLUMNS)
    assert _query(database, f"SELECT {columns} FROM option_ohlcv") == [("text",) * 12]  # noqa: S608
    assert _get(database) == (_bar(),)


def test_high_precision_premiums_survive_any_caller_context(database: Path) -> None:
    precise = "186.123456789012345678901234567890123"
    bar = OptionOHLCVBar(_contract(), _T1, _DAILY, _p("182"), _p("190"), _p("175"), _p(precise),
                         Quantity(Decimal("1")))  # fmt: skip

    with localcontext() as context:
        context.prec = 6
        context.rounding = ROUND_DOWN
        _store(database, bar)
        found = _get(database)

    assert found == (bar,)
    assert _rows(database)[0][10] == precise


def test_a_zero_premium_bar_round_trips(database: Path) -> None:
    zero = _p("0")
    bar = OptionOHLCVBar(_contract(), _T1, _DAILY, zero, zero, zero, zero, Quantity(Decimal("0")))

    _store(database, bar)

    assert _get(database) == (bar,)


# ---------------------------------------------------------------------------
# Exact contract identity
# ---------------------------------------------------------------------------


def test_calls_puts_strikes_and_expiries_coexist_at_one_instant(database: Path) -> None:
    contracts = [
        _contract(),
        _contract(right=OptionRight.PUT),
        _contract(strike="25050"),
        _contract(expiration="2026-11-24"),
    ]

    assert _store(database, *(_bar(contract) for contract in contracts)) == 4

    for contract in contracts:
        assert _get(database, contract) == (_bar(contract),)


@pytest.mark.parametrize(
    "neighbour",
    [
        _contract(strike="25050"),
        _contract(strike="24950"),
        _contract(right=OptionRight.PUT),
        _contract(expiration="2026-11-24"),
        OptionContract(
            OptionProductReference(Symbol("BANKNIFTY"), ExchangeCode("NSE")),
            ExpirationDate("2026-10-27"),
            OptionStrike(Decimal("25000")),
            OptionRight.CALL,
        ),
    ],
    ids=["higher-strike", "lower-strike", "right", "expiry", "product"],
)
def test_a_neighbouring_contract_never_matches(database: Path, neighbour: OptionContract) -> None:
    _store(database, _bar())

    assert _get(database, neighbour) == ()


def test_another_timeframe_never_matches(database: Path) -> None:
    _store(database, _bar())

    assert _get(database, timeframe=Timeframe("1w")) == ()


def test_bars_are_returned_chronologically_within_the_window(database: Path) -> None:
    _store(database, _bar(at=_T3), _bar(at=_T1), _bar(at=_T2))

    assert [bar.point_in_time for bar in _get(database)] == [_T1, _T2, _T3]
    assert [bar.point_in_time for bar in _get(database, start=_T2)] == [_T2, _T3]
    assert [bar.point_in_time for bar in _get(database, end=_T2)] == [_T1, _T2]


# ---------------------------------------------------------------------------
# Immutability, idempotency and batches
# ---------------------------------------------------------------------------


def test_an_equal_retry_is_idempotent(database: Path) -> None:
    _store(database, _bar())

    assert _store(database, _bar()) == 1
    assert _rows(database) == [_ROW]


@pytest.mark.parametrize(
    "changed", [_bar(close="186.2"), _bar(volume="1201")], ids=["close", "volume"]
)
def test_a_changed_bar_conflicts_and_changes_nothing(
    database: Path, changed: OptionOHLCVBar
) -> None:
    _store(database, _bar())

    with pytest.raises(OptionHistoricalMarketDataConflictError, match="already stored"):
        _store(database, changed)

    assert _rows(database) == [_ROW]


def test_a_conflict_rolls_back_the_whole_batch(database: Path) -> None:
    _store(database, _bar())

    with pytest.raises(OptionHistoricalMarketDataConflictError):
        _store(database, _bar(at=_T2), _bar(close="186.2"))

    assert _rows(database) == [_ROW]


@pytest.mark.parametrize(
    "batch", [(_bar(), _bar()), (_bar(), _bar(close="186.2"))], ids=["equal", "different"]
)
def test_a_batch_repeating_one_key_is_rejected_before_any_write(database: Path, batch) -> None:
    with pytest.raises(OptionHistoricalMarketDataConflictError, match="two bars"):
        SQLiteOptionHistoricalMarketDataStore(database).store(batch)
    assert not database.exists()


def test_an_empty_batch_returns_zero_and_creates_nothing(database: Path) -> None:
    assert _store(database) == 0
    assert not database.exists()


@pytest.mark.parametrize("batch", [[_bar()], (_bar(), "bar"), None])
def test_foreign_batches_are_rejected_before_any_write(database: Path, batch) -> None:
    with pytest.raises(TypeError):
        SQLiteOptionHistoricalMarketDataStore(database).store(batch)
    assert not database.exists()


def test_bars_survive_reopen(tmp_path: Path) -> None:
    path = tmp_path / "reopen.sqlite3"
    bars = (_bar(at=_T1), _bar(at=_T2), _bar(_contract(right=OptionRight.PUT), at=_T1))
    SQLiteOptionHistoricalMarketDataStore(path).store(bars)

    assert (
        SQLiteOptionHistoricalMarketDataRepository(path).get_bars(
            OptionHistoricalMarketDataQuery(_contract(), _DAILY)
        )
        == bars[:2]
    )
    assert SQLiteOptionHistoricalMarketDataStore(path).store(bars) == 3


def _race(monkeypatch: pytest.MonkeyPatch) -> None:
    """The first existence check misses, as if another writer inserted concurrently."""
    real = SQLiteOptionHistoricalMarketDataStore._existing
    calls = {"count": 0}

    def racing(connection, bar):
        calls["count"] += 1
        return None if calls["count"] == 1 else real(connection, bar)

    monkeypatch.setattr(SQLiteOptionHistoricalMarketDataStore, "_existing", staticmethod(racing))


def test_a_race_with_an_equal_bar_is_idempotent(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _store(database, _bar())
    _race(monkeypatch)

    assert _store(database, _bar()) == 1
    assert _rows(database) == [_ROW]


def test_a_race_with_a_different_bar_conflicts(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _store(database, _bar())
    _race(monkeypatch)

    with pytest.raises(OptionHistoricalMarketDataConflictError):
        _store(database, _bar(close="186.2"))
    assert _rows(database) == [_ROW]


# ---------------------------------------------------------------------------
# Corruption
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("column", "value"),
    [
        ("open_premium", "-1"),
        ("close_premium", "186.10"),
        ("close_premium", "abc"),
        ("high_premium", "170"),
        ("volume", "1200.0"),
        ("volume", "12.5"),
        ("point_in_time", "2026-10-08T15:40:00+05:30"),
        ("timeframe", "daily"),
        ("strike", "25000.0"),
        ("option_right", "CE"),
        ("volume", b"1200"),
    ],
)
def test_a_corrupt_row_fails_loudly(database: Path, column: str, value: object) -> None:
    _store(database, _bar())
    _execute(database, f"UPDATE option_ohlcv SET {column} = ?", (value,))  # noqa: S608

    with pytest.raises(OptionHistoricalStorageError):
        module._decode(tuple(_query(database, "SELECT * FROM option_ohlcv")[0]))
    if column not in {"point_in_time", "timeframe", "strike", "option_right"}:
        with pytest.raises(OptionHistoricalStorageError):
            _get(database)


def test_a_store_over_a_corrupt_row_fails_and_repairs_nothing(database: Path) -> None:
    _store(database, _bar())
    _execute(database, "UPDATE option_ohlcv SET volume = '12.5'")

    with pytest.raises(OptionHistoricalStorageError):
        _store(database, _bar())
    assert _rows(database)[0][11] == "12.5"


# ---------------------------------------------------------------------------
# Read-only repository
# ---------------------------------------------------------------------------


def test_a_missing_database_is_empty_and_creates_no_file(database: Path) -> None:
    assert _get(database) == ()
    assert not database.exists()


def test_a_database_without_the_table_is_empty_and_unchanged(database: Path) -> None:
    with sqlite3.connect(database) as connection:
        initialize_futures_market_data_schema(connection)
    before = _schema(database)

    assert _get(database) == ()
    assert _schema(database) == before


def _trace(monkeypatch: pytest.MonkeyPatch) -> list[tuple[tuple, dict, list[str]]]:
    connections: list[tuple[tuple, dict, list[str]]] = []
    real_connect = sqlite3.connect

    def connect(*args, **kwargs):
        statements: list[str] = []
        connections.append((args, kwargs, statements))
        connection = real_connect(*args, **kwargs)
        connection.set_trace_callback(statements.append)
        return connection

    monkeypatch.setattr(module.sqlite3, "connect", connect)
    return connections


def test_a_lookup_is_read_only_and_runs_no_ddl(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _store(database, _bar())

    def forbidden(connection) -> None:
        raise AssertionError("a lookup must never initialize the schema")

    monkeypatch.setattr(module, "initialize_option_market_data_schema", forbidden)
    connections = _trace(monkeypatch)

    assert _get(database) == (_bar(),)
    for args, kwargs, statements in connections:
        assert args[0].startswith("file:") and args[0].endswith("?mode=ro")
        assert kwargs == {"uri": True}
        for statement in statements:
            assert (
                not statement.upper()
                .lstrip()
                .startswith(("CREATE", "ALTER", "DROP", "INSERT", "UPDATE", "DELETE", "BEGIN"))
            )


def test_the_store_writes_inside_begin_immediate(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    connections = _trace(monkeypatch)

    _store(database, _bar(), _bar(at=_T2))

    ((_, _, statements),) = connections
    begin = statements.index("BEGIN IMMEDIATE")
    inserts = [i for i, s in enumerate(statements) if s.startswith("INSERT INTO option_ohlcv")]
    assert len(inserts) == 2 and begin < inserts[0]
    assert statements[-1] == "COMMIT"


def test_every_statement_is_insert_only_text_and_names_no_futures_table(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    connections = _trace(monkeypatch)
    _store(database, _bar(), _bar(at=_T2))
    _store(database, _bar())
    _get(database)

    statements = [s for _, _, executed in connections for s in executed]
    statements += [schema.OPTION_OHLCV_SCHEMA, module._INSERT, module._SELECT_KEY]
    for statement in statements:
        upper = statement.upper()
        for forbidden in ("UPDATE ", "DELETE ", "REPLACE", "ON CONFLICT", "DROP ", "ALTER "):
            assert forbidden not in upper
        for forbidden in (" REAL", " FLOAT", " NUMERIC", " INTEGER", " BLOB"):
            assert forbidden not in upper
        assert "FUTURES_" not in upper


def test_the_adapters_implement_the_application_ports(database: Path) -> None:
    assert isinstance(
        SQLiteOptionHistoricalMarketDataStore(database), OptionHistoricalMarketDataStore
    )
    assert isinstance(
        SQLiteOptionHistoricalMarketDataRepository(database), OptionHistoricalMarketDataRepository
    )


def test_the_repository_rejects_a_foreign_query(database: Path) -> None:
    with pytest.raises(TypeError, match="OptionHistoricalMarketDataQuery"):
        SQLiteOptionHistoricalMarketDataRepository(database).get_bars(_contract())  # type: ignore[arg-type]
