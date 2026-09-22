"""Integration tests for the SQLite futures historical market data adapters.

Every test runs against a real temporary SQLite database. The point is to
exercise the storage engine's own behaviour -- its uniqueness constraint, its
transaction boundaries and its text collation -- rather than a double that
would agree with whatever the adapter happens to do.
"""

from __future__ import annotations

import ast
import sqlite3
from decimal import Decimal, localcontext
from pathlib import Path

import pytest
from northstar_application.ports import (
    FuturesHistoricalMarketDataConflictError,
    FuturesHistoricalMarketDataQuery,
)
from northstar_core.derivatives import ExpirationDate, QuoteValue
from northstar_core.foundation.value_objects import (
    ExchangeCode,
    PointInTime,
    Quantity,
    Symbol,
    Timeframe,
)
from northstar_core.futures import FuturesContract, FuturesOHLCVBar, FuturesProductReference

from northstar_infrastructure.market_data import (
    SQLiteFuturesHistoricalMarketDataRepository,
    SQLiteFuturesHistoricalMarketDataStore,
)

_CME = ExchangeCode("CME")
_NYMEX = ExchangeCode("NYMEX")

_ES = FuturesProductReference(Symbol("ES"), _CME)
_MES = FuturesProductReference(Symbol("MES"), _CME)
_CL = FuturesProductReference(Symbol("CL"), _NYMEX)

_MARCH = ExpirationDate("2026-03-20")
_JUNE = ExpirationDate("2026-06-19")

_ES_MARCH = FuturesContract(_ES, _MARCH)
_MES_MARCH = FuturesContract(_MES, _MARCH)
_ES_JUNE = FuturesContract(_ES, _JUNE)
_CL_MAY = FuturesContract(_CL, ExpirationDate("2020-05-19"))

_DAILY = Timeframe("1d")
_HOURLY = Timeframe("1h")

_HIGH_PRECISION = "5430.12345678901234567890123456789012345678901234567891"
_HIGH_PRECISION_VOLUME = "1234567890123456789012345678901234567891"


@pytest.fixture
def database_path(tmp_path: Path) -> str:
    return str(tmp_path / "futures.sqlite3")


@pytest.fixture
def store(database_path: str) -> SQLiteFuturesHistoricalMarketDataStore:
    return SQLiteFuturesHistoricalMarketDataStore(database_path)


@pytest.fixture
def repository(database_path: str) -> SQLiteFuturesHistoricalMarketDataRepository:
    return SQLiteFuturesHistoricalMarketDataRepository(database_path)


def _instant(day: int, suffix: str = "") -> PointInTime:
    return PointInTime(f"2026-01-{day:02d}T21:00:00{suffix}Z")


def _bar(
    contract: FuturesContract = _ES_MARCH,
    *,
    day: int = 15,
    point_in_time: PointInTime | None = None,
    timeframe: Timeframe = _DAILY,
    open_quote: str = "5430.00",
    high: str = "5450.25",
    low: str = "5425.50",
    close: str = "5442.75",
    volume: str = "1250000",
) -> FuturesOHLCVBar:
    return FuturesOHLCVBar(
        contract=contract,
        point_in_time=point_in_time if point_in_time is not None else _instant(day),
        timeframe=timeframe,
        open=QuoteValue(Decimal(open_quote)),
        high=QuoteValue(Decimal(high)),
        low=QuoteValue(Decimal(low)),
        close=QuoteValue(Decimal(close)),
        volume=Quantity(Decimal(volume)),
    )


def _query(**kwargs: object) -> FuturesHistoricalMarketDataQuery:
    members: dict[str, object] = {"contract": _ES_MARCH, "timeframe": _DAILY}
    members.update(kwargs)
    return FuturesHistoricalMarketDataQuery(**members)  # type: ignore[arg-type]


def _rows(database_path: str, statement: str, parameters: tuple[object, ...] = ()) -> list[tuple]:
    connection = sqlite3.connect(database_path)
    try:
        return connection.execute(statement, parameters).fetchall()
    finally:
        connection.close()


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


def test_the_futures_table_exists_with_the_expected_columns(
    store: SQLiteFuturesHistoricalMarketDataStore, database_path: str
) -> None:
    store.store((_bar(),))

    columns = [row[1] for row in _rows(database_path, "PRAGMA table_info(futures_ohlcv)")]

    assert columns == [
        "product_code",
        "exchange_code",
        "expiration_date",
        "timeframe",
        "point_in_time",
        "open_value",
        "high_value",
        "low_value",
        "close_value",
        "volume",
    ]


def test_every_column_is_declared_not_null(
    store: SQLiteFuturesHistoricalMarketDataStore, database_path: str
) -> None:
    store.store((_bar(),))

    info = _rows(database_path, "PRAGMA table_info(futures_ohlcv)")

    assert all(row[2] == "TEXT" for row in info)
    assert all(row[3] == 1 for row in info)


def test_a_unique_constraint_covers_the_natural_key(
    store: SQLiteFuturesHistoricalMarketDataStore, database_path: str
) -> None:
    store.store((_bar(),))

    unique = [row for row in _rows(database_path, "PRAGMA index_list(futures_ohlcv)") if row[2]]
    assert unique, "no unique index protects the natural key"

    covered = {
        column
        for index in unique
        for column in (row[2] for row in _rows(database_path, f"PRAGMA index_info({index[1]})"))
    }
    assert covered == {
        "product_code",
        "exchange_code",
        "expiration_date",
        "timeframe",
        "point_in_time",
    }


def test_the_database_holds_no_out_of_scope_table(
    store: SQLiteFuturesHistoricalMarketDataStore, database_path: str
) -> None:
    store.store((_bar(),))

    tables = {
        row[0]
        for row in _rows(database_path, "SELECT name FROM sqlite_master WHERE type = 'table'")
        if not row[0].startswith("sqlite_")
    }

    assert tables == {"futures_ohlcv"}
    for forbidden in ("position", "order", "provider", "continuous", "underlying", "symbol_map"):
        assert not any(forbidden in table for table in tables)


def test_no_deferred_column_is_present(
    store: SQLiteFuturesHistoricalMarketDataStore, database_path: str
) -> None:
    store.store((_bar(),))

    columns = {row[1] for row in _rows(database_path, "PRAGMA table_info(futures_ohlcv)")}

    for absent in (
        "adjusted_close",
        "open_interest",
        "currency",
        "multiplier",
        "tick_size",
        "provider_symbol",
        "underlying",
        "id",
        "rowid_identity",
    ):
        assert absent not in columns


@pytest.mark.parametrize(
    "column", ["open_value", "high_value", "low_value", "close_value", "volume"]
)
def test_numeric_evidence_is_stored_as_text(
    store: SQLiteFuturesHistoricalMarketDataStore, database_path: str, column: str
) -> None:
    """A float column would round a quotation and silently change the evidence."""
    store.store((_bar(),))

    ((stored_type,),) = _rows(database_path, f"SELECT typeof({column}) FROM futures_ohlcv")  # noqa: S608

    assert stored_type == "text"


# ---------------------------------------------------------------------------
# Store: accepting evidence
# ---------------------------------------------------------------------------


def test_storing_a_new_bar_returns_one(store: SQLiteFuturesHistoricalMarketDataStore) -> None:
    assert store.store((_bar(),)) == 1


def test_storing_an_empty_batch_returns_zero_and_writes_nothing(
    store: SQLiteFuturesHistoricalMarketDataStore, database_path: str
) -> None:
    assert store.store(()) == 0
    assert not Path(database_path).exists()


def test_storing_a_batch_persists_every_bar(
    store: SQLiteFuturesHistoricalMarketDataStore, database_path: str
) -> None:
    assert store.store(tuple(_bar(day=day) for day in (14, 15, 16))) == 3
    assert _rows(database_path, "SELECT COUNT(*) FROM futures_ohlcv") == [(3,)]


# ---------------------------------------------------------------------------
# Store: idempotency
# ---------------------------------------------------------------------------


def test_an_identical_retry_is_idempotent(
    store: SQLiteFuturesHistoricalMarketDataStore, database_path: str
) -> None:
    bar = _bar()

    assert store.store((bar,)) == 1
    assert store.store((bar,)) == 1
    assert _rows(database_path, "SELECT COUNT(*) FROM futures_ohlcv") == [(1,)]


def test_a_retry_with_an_equal_but_distinct_bar_is_idempotent(
    store: SQLiteFuturesHistoricalMarketDataStore, database_path: str
) -> None:
    store.store((_bar(),))

    assert store.store((_bar(),)) == 1
    assert _rows(database_path, "SELECT COUNT(*) FROM futures_ohlcv") == [(1,)]


def test_equivalent_quote_spellings_are_the_same_evidence(
    store: SQLiteFuturesHistoricalMarketDataStore, database_path: str
) -> None:
    """Comparison is on rebuilt domain values, so 5430.00 and 5430.0000 agree."""
    store.store((_bar(open_quote="5430.00"),))

    assert store.store((_bar(open_quote="5430.0000"),)) == 1
    assert _rows(database_path, "SELECT COUNT(*) FROM futures_ohlcv") == [(1,)]


def test_an_offset_equivalent_instant_is_the_same_key(
    store: SQLiteFuturesHistoricalMarketDataStore, database_path: str
) -> None:
    """PointInTime canonicalizes to UTC, so both spellings are one row."""
    store.store((_bar(point_in_time=PointInTime("2026-01-15T21:00:00Z")),))

    assert store.store((_bar(point_in_time=PointInTime("2026-01-16T02:30:00+05:30")),)) == 1
    assert _rows(database_path, "SELECT COUNT(*) FROM futures_ohlcv") == [(1,)]


def test_a_partially_overlapping_batch_is_accepted_whole(
    store: SQLiteFuturesHistoricalMarketDataStore, database_path: str
) -> None:
    store.store((_bar(day=14), _bar(day=15)))

    assert store.store((_bar(day=15), _bar(day=16))) == 2
    assert _rows(database_path, "SELECT COUNT(*) FROM futures_ohlcv") == [(3,)]


# ---------------------------------------------------------------------------
# Store: conflict and no silent overwrite
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("open_quote", "5431.00"),
        ("high", "5460.25"),
        ("low", "5420.50"),
        ("close", "5443.75"),
    ],
)
def test_a_differing_ohlc_value_under_one_key_conflicts(
    store: SQLiteFuturesHistoricalMarketDataStore, field: str, value: str
) -> None:
    store.store((_bar(),))

    with pytest.raises(FuturesHistoricalMarketDataConflictError):
        store.store((_bar(**{field: value}),))


def test_a_differing_volume_under_one_key_conflicts(
    store: SQLiteFuturesHistoricalMarketDataStore,
) -> None:
    store.store((_bar(volume="1250000"),))

    with pytest.raises(FuturesHistoricalMarketDataConflictError):
        store.store((_bar(volume="1250001"),))


def test_the_original_evidence_survives_a_conflict(
    store: SQLiteFuturesHistoricalMarketDataStore,
    repository: SQLiteFuturesHistoricalMarketDataRepository,
    database_path: str,
) -> None:
    original = _bar(close="5442.75")
    store.store((original,))

    with pytest.raises(FuturesHistoricalMarketDataConflictError):
        store.store((_bar(close="5430.00"),))

    assert repository.get_bars(_query()) == (original,)
    assert _rows(database_path, "SELECT close_value FROM futures_ohlcv") == [("5442.75",)]


def test_a_conflict_late_in_a_batch_rolls_the_whole_batch_back(
    store: SQLiteFuturesHistoricalMarketDataStore, database_path: str
) -> None:
    store.store((_bar(day=15),))

    with pytest.raises(FuturesHistoricalMarketDataConflictError):
        store.store((_bar(day=16), _bar(day=17), _bar(day=15, close="5430.00")))

    assert _rows(database_path, "SELECT point_in_time FROM futures_ohlcv") == [
        (_instant(15).value,)
    ]


def test_a_duplicate_key_inside_a_batch_conflicts_even_when_equal(
    store: SQLiteFuturesHistoricalMarketDataStore, database_path: str
) -> None:
    bar = _bar()

    with pytest.raises(FuturesHistoricalMarketDataConflictError, match="batch contains"):
        store.store((bar, bar))

    assert not Path(database_path).exists(), "preflight must reject before opening the store"


def test_a_duplicate_key_inside_a_batch_is_caught_before_any_write(
    store: SQLiteFuturesHistoricalMarketDataStore, database_path: str
) -> None:
    """Preflight, so an earlier valid bar in the same batch is not persisted."""
    with pytest.raises(FuturesHistoricalMarketDataConflictError, match="batch contains"):
        store.store((_bar(day=14), _bar(day=15), _bar(day=15, close="5430.00")))

    assert not Path(database_path).exists(), "preflight must reject before opening the store"


def test_an_offset_equivalent_duplicate_inside_a_batch_conflicts(
    store: SQLiteFuturesHistoricalMarketDataStore,
) -> None:
    with pytest.raises(FuturesHistoricalMarketDataConflictError, match="batch contains"):
        store.store(
            (
                _bar(point_in_time=PointInTime("2026-01-15T21:00:00Z")),
                _bar(point_in_time=PointInTime("2026-01-16T02:30:00+05:30")),
            )
        )


# ---------------------------------------------------------------------------
# Store: key isolation
# ---------------------------------------------------------------------------


def test_standard_and_micro_contracts_coexist(
    store: SQLiteFuturesHistoricalMarketDataStore, database_path: str
) -> None:
    """ES and MES share venue, underlying and expiry, and are still two rows."""
    assert store.store((_bar(_ES_MARCH), _bar(_MES_MARCH))) == 2
    assert _rows(database_path, "SELECT COUNT(*) FROM futures_ohlcv") == [(2,)]


def test_two_expiries_of_one_product_coexist(
    store: SQLiteFuturesHistoricalMarketDataStore, database_path: str
) -> None:
    assert store.store((_bar(_ES_MARCH), _bar(_ES_JUNE))) == 2
    assert _rows(database_path, "SELECT COUNT(*) FROM futures_ohlcv") == [(2,)]


def test_two_timeframes_at_one_instant_coexist(
    store: SQLiteFuturesHistoricalMarketDataStore, database_path: str
) -> None:
    assert store.store((_bar(timeframe=_DAILY), _bar(timeframe=_HOURLY))) == 2
    assert _rows(database_path, "SELECT COUNT(*) FROM futures_ohlcv") == [(2,)]


def test_a_differing_micro_bar_does_not_conflict_with_the_standard_bar(
    store: SQLiteFuturesHistoricalMarketDataStore,
) -> None:
    store.store((_bar(_ES_MARCH, close="5442.75"),))

    assert store.store((_bar(_MES_MARCH, close="5430.00"),)) == 1


# ---------------------------------------------------------------------------
# Store: exact evidence across close and reopen
# ---------------------------------------------------------------------------


def test_a_negative_session_round_trips_exactly(
    store: SQLiteFuturesHistoricalMarketDataStore, database_path: str
) -> None:
    """The session a Price-based bar could not have recorded at all."""
    bar = _bar(
        _CL_MAY,
        point_in_time=PointInTime("2020-04-20T18:30:00Z"),
        open_quote="-14.00",
        high="-10.50",
        low="-40.32",
        close="-37.63",
        volume="248000",
    )
    store.store((bar,))

    reopened = SQLiteFuturesHistoricalMarketDataRepository(database_path)
    (retrieved,) = reopened.get_bars(_query(contract=_CL_MAY))

    assert retrieved == bar
    assert retrieved.close == QuoteValue(Decimal("-37.63"))
    assert _rows(database_path, "SELECT close_value FROM futures_ohlcv") == [("-37.63",)]


def test_a_negative_bar_re_stored_identically_is_idempotent(
    store: SQLiteFuturesHistoricalMarketDataStore, database_path: str
) -> None:
    members = {
        "point_in_time": PointInTime("2020-04-20T18:30:00Z"),
        "open_quote": "-14.00",
        "high": "-10.50",
        "low": "-40.32",
        "close": "-37.63",
    }
    store.store((_bar(_CL_MAY, **members),))

    assert store.store((_bar(_CL_MAY, **members),)) == 1
    assert _rows(database_path, "SELECT COUNT(*) FROM futures_ohlcv") == [(1,)]


def test_a_high_precision_quote_round_trips_exactly(
    store: SQLiteFuturesHistoricalMarketDataStore, database_path: str
) -> None:
    bar = _bar(open_quote=_HIGH_PRECISION, high="9999", low="0", close=_HIGH_PRECISION)
    store.store((bar,))

    reopened = SQLiteFuturesHistoricalMarketDataRepository(database_path)
    (retrieved,) = reopened.get_bars(_query())

    assert str(retrieved.open.value) == _HIGH_PRECISION
    assert retrieved == bar
    assert _rows(database_path, "SELECT open_value FROM futures_ohlcv") == [(_HIGH_PRECISION,)]


def test_a_high_precision_volume_round_trips_exactly(
    store: SQLiteFuturesHistoricalMarketDataStore, database_path: str
) -> None:
    bar = _bar(volume=_HIGH_PRECISION_VOLUME)
    store.store((bar,))

    reopened = SQLiteFuturesHistoricalMarketDataRepository(database_path)
    (retrieved,) = reopened.get_bars(_query())

    assert str(retrieved.volume.value) == _HIGH_PRECISION_VOLUME
    assert retrieved == bar


@pytest.mark.parametrize("precision", [6, 28, 50])
def test_high_precision_survives_any_ambient_decimal_context(
    store: SQLiteFuturesHistoricalMarketDataStore, database_path: str, precision: int
) -> None:
    """Reading is canonicalization too, so the reader's context must not matter."""
    with localcontext() as context:
        context.prec = precision
        store.store(
            (_bar(open_quote=_HIGH_PRECISION, high="9999", low="0", close=_HIGH_PRECISION),)
        )
        (retrieved,) = SQLiteFuturesHistoricalMarketDataRepository(database_path).get_bars(_query())

    assert str(retrieved.open.value) == _HIGH_PRECISION


def test_a_truncated_high_precision_bar_conflicts_rather_than_merging(
    store: SQLiteFuturesHistoricalMarketDataStore,
) -> None:
    store.store((_bar(open_quote=_HIGH_PRECISION, high="9999", low="0"),))

    with pytest.raises(FuturesHistoricalMarketDataConflictError):
        store.store((_bar(open_quote="5430.123456", high="9999", low="0"),))


def test_reopening_the_database_preserves_exact_equality(
    store: SQLiteFuturesHistoricalMarketDataStore, database_path: str
) -> None:
    bars = tuple(_bar(day=day) for day in (14, 15, 16))
    store.store(bars)

    reopened = SQLiteFuturesHistoricalMarketDataRepository(database_path)

    assert reopened.get_bars(_query()) == bars


def test_a_reopened_store_still_treats_stored_bars_as_present(
    store: SQLiteFuturesHistoricalMarketDataStore, database_path: str
) -> None:
    store.store((_bar(),))

    assert SQLiteFuturesHistoricalMarketDataStore(database_path).store((_bar(),)) == 1
    assert _rows(database_path, "SELECT COUNT(*) FROM futures_ohlcv") == [(1,)]


# ---------------------------------------------------------------------------
# Store: insert only
# ---------------------------------------------------------------------------


def _sql_fragments() -> list[str]:
    """Return every SQL text the adapter module could execute.

    Collected through the AST rather than by matching source text, so prose in
    a docstring can neither cause a false failure nor hide a real statement.
    Two sources are gathered, because a statement can hide in either: the
    literal parts of anything passed to execute(), and every module-level
    string constant, since the SELECT is assembled from one. Docstrings are
    ast.Expr rather than ast.Assign and are therefore excluded, which is what
    keeps this test from failing on its own module's prose.
    """
    import northstar_infrastructure.market_data.sqlite_futures_historical_market_data as module

    tree = ast.parse(Path(module.__file__).read_text(encoding="utf-8"))
    fragments: list[str] = []

    for node in tree.body:
        if isinstance(node, ast.Assign):
            fragments.extend(_string_fragments(node.value))

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        if node.func.attr not in {"execute", "executemany", "executescript"}:
            continue
        for argument in node.args[:1]:
            fragments.extend(_string_fragments(argument))

    return fragments


def _string_fragments(node: ast.AST) -> list[str]:
    """Flatten a literal, an f-string or a concatenation into its text pieces."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return [node.value]
    if isinstance(node, ast.JoinedStr):
        return [piece for value in node.values for piece in _string_fragments(value)]
    if isinstance(node, ast.BinOp):
        return _string_fragments(node.left) + _string_fragments(node.right)
    return []


def test_the_adapter_executes_sql_this_test_can_see() -> None:
    """Guards the sweep below from passing vacuously."""
    fragments = _sql_fragments()

    assert any("INSERT INTO futures_ohlcv" in text for text in fragments)
    assert any("SELECT" in text for text in fragments)
    assert any("BEGIN IMMEDIATE" in text for text in fragments)


@pytest.mark.parametrize(
    "forbidden", ["UPDATE", "REPLACE", "ON CONFLICT", "DELETE", "UPSERT", "DROP"]
)
def test_the_adapter_executes_no_mutating_statement(forbidden: str) -> None:
    """INSERT only: stored evidence is never rewritten or removed."""
    offending = [text for text in _sql_fragments() if forbidden in text.upper()]

    assert not offending, f"adapter executes {forbidden}: {offending}"


# ---------------------------------------------------------------------------
# Repository: isolation
# ---------------------------------------------------------------------------


def test_contracts_are_isolated(
    store: SQLiteFuturesHistoricalMarketDataStore,
    repository: SQLiteFuturesHistoricalMarketDataRepository,
) -> None:
    standard = _bar(_ES_MARCH)
    micro = _bar(_MES_MARCH)
    june = _bar(_ES_JUNE)
    store.store((standard, micro, june))

    assert repository.get_bars(_query(contract=_ES_MARCH)) == (standard,)
    assert repository.get_bars(_query(contract=_MES_MARCH)) == (micro,)
    assert repository.get_bars(_query(contract=_ES_JUNE)) == (june,)


def test_timeframes_are_isolated(
    store: SQLiteFuturesHistoricalMarketDataStore,
    repository: SQLiteFuturesHistoricalMarketDataRepository,
) -> None:
    daily = _bar(timeframe=_DAILY)
    hourly = _bar(timeframe=_HOURLY)
    store.store((daily, hourly))

    assert repository.get_bars(_query(timeframe=_DAILY)) == (daily,)
    assert repository.get_bars(_query(timeframe=_HOURLY)) == (hourly,)


def test_an_unknown_contract_returns_an_empty_tuple(
    store: SQLiteFuturesHistoricalMarketDataStore,
    repository: SQLiteFuturesHistoricalMarketDataRepository,
) -> None:
    store.store((_bar(_ES_MARCH),))

    assert repository.get_bars(_query(contract=_CL_MAY)) == ()


def test_an_empty_database_returns_an_empty_tuple(
    repository: SQLiteFuturesHistoricalMarketDataRepository,
) -> None:
    assert repository.get_bars(_query()) == ()


# ---------------------------------------------------------------------------
# Repository: temporal window
# ---------------------------------------------------------------------------


def test_an_open_window_returns_the_whole_contract_history(
    store: SQLiteFuturesHistoricalMarketDataStore,
    repository: SQLiteFuturesHistoricalMarketDataRepository,
) -> None:
    store.store(tuple(_bar(day=day) for day in (14, 15, 16)))

    assert len(repository.get_bars(_query())) == 3


def test_both_bounds_are_inclusive(
    store: SQLiteFuturesHistoricalMarketDataStore,
    repository: SQLiteFuturesHistoricalMarketDataRepository,
) -> None:
    store.store(tuple(_bar(day=day) for day in (14, 15, 16, 17, 18)))

    result = repository.get_bars(_query(start=_instant(15), end=_instant(17)))

    assert [bar.point_in_time for bar in result] == [_instant(15), _instant(16), _instant(17)]


def test_a_start_only_window_is_open_above(
    store: SQLiteFuturesHistoricalMarketDataStore,
    repository: SQLiteFuturesHistoricalMarketDataRepository,
) -> None:
    store.store(tuple(_bar(day=day) for day in (14, 15, 16)))

    result = repository.get_bars(_query(start=_instant(15)))

    assert [bar.point_in_time for bar in result] == [_instant(15), _instant(16)]


def test_an_end_only_window_is_open_below(
    store: SQLiteFuturesHistoricalMarketDataStore,
    repository: SQLiteFuturesHistoricalMarketDataRepository,
) -> None:
    store.store(tuple(_bar(day=day) for day in (14, 15, 16)))

    result = repository.get_bars(_query(end=_instant(15)))

    assert [bar.point_in_time for bar in result] == [_instant(14), _instant(15)]


def test_equal_endpoints_select_exactly_one_bar(
    store: SQLiteFuturesHistoricalMarketDataStore,
    repository: SQLiteFuturesHistoricalMarketDataRepository,
) -> None:
    store.store(tuple(_bar(day=day) for day in (14, 15, 16)))

    result = repository.get_bars(_query(start=_instant(15), end=_instant(15)))

    assert [bar.point_in_time for bar in result] == [_instant(15)]


def test_an_offset_equivalent_bound_includes_the_matching_instant(
    store: SQLiteFuturesHistoricalMarketDataStore,
    repository: SQLiteFuturesHistoricalMarketDataRepository,
) -> None:
    bar = _bar(point_in_time=PointInTime("2026-01-15T21:00:00Z"))
    store.store((bar,))
    offset_bound = PointInTime("2026-01-16T02:30:00+05:30")

    assert repository.get_bars(_query(start=offset_bound, end=offset_bound)) == (bar,)


def test_a_window_matching_nothing_returns_an_empty_tuple(
    store: SQLiteFuturesHistoricalMarketDataStore,
    repository: SQLiteFuturesHistoricalMarketDataRepository,
) -> None:
    store.store((_bar(day=15),))

    assert repository.get_bars(_query(start=_instant(20), end=_instant(25))) == ()


# ---------------------------------------------------------------------------
# The temporal trap: SQLite text collation disagrees with chronology
# ---------------------------------------------------------------------------


def test_raw_sqlite_text_ordering_is_chronologically_wrong(
    store: SQLiteFuturesHistoricalMarketDataStore,
    repository: SQLiteFuturesHistoricalMarketDataRepository,
    database_path: str,
) -> None:
    """Load-bearing: this is why ORDER BY point_in_time is forbidden.

    A canonical instant omits fractional seconds when they are zero, so
    '...00.1Z' precedes '...00Z' under SQLite's text collation while being the
    later instant.
    """
    whole = _bar(point_in_time=_instant(15))
    fractional = _bar(point_in_time=_instant(15, ".1"), close="5440.00")
    store.store((whole, fractional))

    sql_order = [
        row[0]
        for row in _rows(
            database_path, "SELECT point_in_time FROM futures_ohlcv ORDER BY point_in_time"
        )
    ]
    repository_order = [bar.point_in_time.value for bar in repository.get_bars(_query())]

    assert sql_order == ["2026-01-15T21:00:00.1Z", "2026-01-15T21:00:00Z"]
    assert repository_order == ["2026-01-15T21:00:00Z", "2026-01-15T21:00:00.1Z"]
    assert sql_order != repository_order


def test_raw_sqlite_text_bounds_filter_the_wrong_rows(
    store: SQLiteFuturesHistoricalMarketDataStore,
    repository: SQLiteFuturesHistoricalMarketDataRepository,
    database_path: str,
) -> None:
    """Load-bearing: this is why point_in_time >= / <= is forbidden.

    A window ending at the whole second must exclude the later fractional
    instant. SQL text comparison includes it.
    """
    whole = _bar(point_in_time=_instant(15))
    fractional = _bar(point_in_time=_instant(15, ".1"), close="5440.00")
    store.store((whole, fractional))
    boundary = _instant(15).value

    sql_matched = [
        row[0]
        for row in _rows(
            database_path,
            "SELECT point_in_time FROM futures_ohlcv WHERE point_in_time <= ?",
            (boundary,),
        )
    ]
    repository_matched = [
        bar.point_in_time.value for bar in repository.get_bars(_query(end=_instant(15)))
    ]

    assert sorted(sql_matched) == ["2026-01-15T21:00:00.1Z", "2026-01-15T21:00:00Z"]
    assert repository_matched == ["2026-01-15T21:00:00Z"]


def test_a_sub_second_start_bound_excludes_the_whole_second(
    store: SQLiteFuturesHistoricalMarketDataStore,
    repository: SQLiteFuturesHistoricalMarketDataRepository,
) -> None:
    whole = _bar(point_in_time=_instant(15))
    store.store((whole,))

    assert repository.get_bars(_query(start=_instant(15, ".5"))) == ()
    assert repository.get_bars(_query(end=_instant(15, ".5"))) == (whole,)


# ---------------------------------------------------------------------------
# Repository: ordering
# ---------------------------------------------------------------------------


def test_results_are_returned_oldest_to_newest(
    store: SQLiteFuturesHistoricalMarketDataStore,
    repository: SQLiteFuturesHistoricalMarketDataRepository,
) -> None:
    store.store(tuple(_bar(day=day) for day in (17, 14, 16, 15)))

    result = repository.get_bars(_query())

    assert [bar.point_in_time for bar in result] == [_instant(day) for day in (14, 15, 16, 17)]


def test_ordering_is_independent_of_insertion_order(
    store: SQLiteFuturesHistoricalMarketDataStore,
    repository: SQLiteFuturesHistoricalMarketDataRepository,
    tmp_path: Path,
) -> None:
    days = (14, 15, 16, 17, 18)
    store.store(tuple(_bar(day=day) for day in days))

    other_path = str(tmp_path / "reversed.sqlite3")
    SQLiteFuturesHistoricalMarketDataStore(other_path).store(
        tuple(_bar(day=day) for day in reversed(days))
    )

    assert repository.get_bars(_query()) == SQLiteFuturesHistoricalMarketDataRepository(
        other_path
    ).get_bars(_query())


def test_the_result_is_an_immutable_tuple(
    store: SQLiteFuturesHistoricalMarketDataStore,
    repository: SQLiteFuturesHistoricalMarketDataRepository,
) -> None:
    store.store((_bar(),))

    assert isinstance(repository.get_bars(_query()), tuple)


# ---------------------------------------------------------------------------
# Adapter shape
# ---------------------------------------------------------------------------


def test_the_repository_rejects_a_foreign_query(
    repository: SQLiteFuturesHistoricalMarketDataRepository,
) -> None:
    with pytest.raises(TypeError):
        repository.get_bars("ES@CME")  # type: ignore[arg-type]


def test_connections_do_not_leak_across_many_operations(
    store: SQLiteFuturesHistoricalMarketDataStore,
    repository: SQLiteFuturesHistoricalMarketDataRepository,
) -> None:
    """An unclosed handle would surface as an OS error well before 200 rounds."""
    for day in range(1, 29):
        store.store((_bar(day=day),))
        repository.get_bars(_query())

    assert len(repository.get_bars(_query())) == 28
