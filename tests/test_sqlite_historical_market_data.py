"""Tests for the SQLite historical market data repository."""

import sqlite3
from pathlib import Path

from northstar_application.ports import HistoricalMarketDataQuery
from northstar_core.foundation.value_objects import (
    Currency,
    ExchangeCode,
    PointInTime,
    Price,
    Quantity,
    Symbol,
    Timeframe,
)
from northstar_core.market_data import HistoricalOHLCVBar

from northstar_infrastructure.market_data import (
    HistoricalStorageError,
    SQLiteHistoricalMarketDataRepository,
    SQLiteHistoricalMarketDataStore,
)
from northstar_infrastructure.market_data.sqlite_schema import (
    initialize_historical_market_data_schema,
)


def _create_database(path: Path) -> None:
    with sqlite3.connect(path) as connection:
        initialize_historical_market_data_schema(connection)
        rows = [
            (
                "AAPL",
                "NASDAQ",
                "2026-09-03T00:00:00Z",
                "1d",
                "USD",
                "102",
                "104",
                "100",
                "103",
                "1200",
                None,
            ),
            (
                "AAPL",
                "NASDAQ",
                "2026-09-01T00:00:00Z",
                "1d",
                "USD",
                "100",
                "102",
                "98",
                "101",
                "1000",
                None,
            ),
            (
                "AAPL",
                "NASDAQ",
                "2026-09-02T00:00:00Z",
                "1d",
                "USD",
                "101",
                "103",
                "99",
                "102",
                "1100",
                "101.5",
            ),
        ]
        connection.executemany(
            "INSERT INTO historical_ohlcv VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows
        )


def _query(start: str, end: str) -> HistoricalMarketDataQuery:
    return HistoricalMarketDataQuery(
        Symbol("AAPL"),
        ExchangeCode("NASDAQ"),
        Timeframe("1d"),
        PointInTime(start),
        PointInTime(end),
    )


def _create_fractional_second_database(path: Path) -> None:
    with sqlite3.connect(path) as connection:
        initialize_historical_market_data_schema(connection)
        rows = [
            (
                "AAPL",
                "NASDAQ",
                "2026-09-15T09:30:00.000001Z",
                "1d",
                "USD",
                "101",
                "103",
                "100",
                "102",
                "1000",
                None,
            ),
            (
                "AAPL",
                "NASDAQ",
                "2026-09-15T09:30:00Z",
                "1d",
                "USD",
                "100",
                "102",
                "99",
                "101",
                "900",
                None,
            ),
        ]
        connection.executemany(
            "INSERT INTO historical_ohlcv VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows
        )


def test_repository_returns_inclusive_results_oldest_to_newest(tmp_path: Path) -> None:
    database_path = tmp_path / "history.sqlite"
    _create_database(database_path)

    result = SQLiteHistoricalMarketDataRepository(database_path).get_history(
        _query("2026-09-01T00:00:00Z", "2026-09-03T00:00:00Z")
    )

    assert isinstance(result, tuple)
    assert [bar.point_in_time.value for bar in result] == [
        "2026-09-01T00:00:00Z",
        "2026-09-02T00:00:00Z",
        "2026-09-03T00:00:00Z",
    ]
    assert result[1].adjusted_close is not None


def test_repository_accepts_equal_start_and_end(tmp_path: Path) -> None:
    database_path = tmp_path / "history.sqlite"
    _create_database(database_path)

    result = SQLiteHistoricalMarketDataRepository(database_path).get_history(
        _query("2026-09-02T00:00:00Z", "2026-09-02T00:00:00Z")
    )

    assert len(result) == 1
    assert result[0].close.amount == 102


def test_repository_returns_empty_tuple_for_no_matches(tmp_path: Path) -> None:
    database_path = tmp_path / "history.sqlite"
    _create_database(database_path)

    result = SQLiteHistoricalMarketDataRepository(database_path).get_history(
        _query("2026-10-01T00:00:00Z", "2026-10-02T00:00:00Z")
    )

    assert result == ()


def test_fractional_second_range_returns_both_observations_in_temporal_order(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "fractional-history.sqlite"
    _create_fractional_second_database(database_path)

    result = SQLiteHistoricalMarketDataRepository(database_path).get_history(
        _query("2026-09-15T09:30:00Z", "2026-09-15T09:30:00.000001Z")
    )

    assert [bar.point_in_time.value for bar in result] == [
        "2026-09-15T09:30:00Z",
        "2026-09-15T09:30:00.000001Z",
    ]


def test_fractional_second_equal_range_returns_only_whole_second_observation(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "fractional-history.sqlite"
    _create_fractional_second_database(database_path)

    result = SQLiteHistoricalMarketDataRepository(database_path).get_history(
        _query("2026-09-15T09:30:00Z", "2026-09-15T09:30:00Z")
    )

    assert [bar.point_in_time.value for bar in result] == ["2026-09-15T09:30:00Z"]


def test_fractional_second_start_excludes_earlier_whole_second_observation(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "fractional-history.sqlite"
    _create_fractional_second_database(database_path)

    result = SQLiteHistoricalMarketDataRepository(database_path).get_history(
        _query("2026-09-15T09:30:00.000001Z", "2026-09-15T09:30:00.000001Z")
    )

    assert [bar.point_in_time.value for bar in result] == ["2026-09-15T09:30:00.000001Z"]


def test_schema_rejects_duplicate_logical_observations(tmp_path: Path) -> None:
    database_path = tmp_path / "history.sqlite"
    _create_database(database_path)

    with sqlite3.connect(database_path) as connection:
        try:
            connection.execute(
                "INSERT INTO historical_ohlcv VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "AAPL",
                    "NASDAQ",
                    "2026-09-01T00:00:00Z",
                    "1d",
                    "USD",
                    "100",
                    "102",
                    "98",
                    "101",
                    "1000",
                    None,
                ),
            )
        except sqlite3.IntegrityError:
            pass
        else:
            raise AssertionError("Expected duplicate logical observation to be rejected")


def test_repository_preserves_decimal_precision_and_stored_currency(tmp_path: Path) -> None:
    database_path = tmp_path / "history.sqlite"
    _create_database(database_path)
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            """
            UPDATE historical_ohlcv
            SET currency = ?, open = ?, high = ?, low = ?, close = ?,
                volume = ?, adjusted_close = ?
            WHERE point_in_time = ?
            """,
            (
                "EUR",
                "101.123456789",
                "103.123456789",
                "99.123456789",
                "102.123456789",
                "1100.0000001",
                "101.123456789",
                "2026-09-02T00:00:00Z",
            ),
        )

    result = SQLiteHistoricalMarketDataRepository(database_path).get_history(
        _query("2026-09-02T00:00:00Z", "2026-09-02T00:00:00Z")
    )

    bar = result[0]
    assert bar.open.currency.value == "EUR"
    assert str(bar.open.amount) == "101.123456789"
    assert str(bar.volume.value) == "1100.0000001"
    assert bar.adjusted_close is not None
    assert str(bar.adjusted_close.amount) == "101.123456789"
    assert bar.adjusted_close.currency.value == "EUR"


def test_repository_maps_storage_invariant_failures_without_leaking_sqlite_errors(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "history.sqlite"
    _create_database(database_path)
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            "UPDATE historical_ohlcv SET high = 'invalid' WHERE point_in_time = ?",
            ("2026-09-01T00:00:00Z",),
        )

    try:
        SQLiteHistoricalMarketDataRepository(database_path).get_history(
            _query("2026-09-01T00:00:00Z", "2026-09-03T00:00:00Z")
        )
    except HistoricalStorageError as error:
        assert "storage contains invalid numeric data" in str(error)
        assert "sqlite" not in str(error).casefold()
    else:
        raise AssertionError("Expected invalid storage data failure")


def _make_bar(
    symbol: str = "AAPL",
    exchange: str = "NASDAQ",
    timestamp: str = "2026-09-01T00:00:00Z",
    timeframe: str = "1d",
    currency: str = "USD",
    open_price: str = "100",
    high_price: str = "105",
    low_price: str = "95",
    close_price: str = "102",
    volume: str = "1000",
    adjusted_close: str | None = None,
) -> HistoricalOHLCVBar:
    c = Currency(currency)
    return HistoricalOHLCVBar(
        symbol=Symbol(symbol),
        exchange_code=ExchangeCode(exchange),
        point_in_time=PointInTime(timestamp),
        timeframe=Timeframe(timeframe),
        open=Price(open_price, c),
        high=Price(high_price, c),
        low=Price(low_price, c),
        close=Price(close_price, c),
        volume=Quantity(volume),
        adjusted_close=Price(adjusted_close, c) if adjusted_close else None,
    )


def test_store_empty_tuple_returns_zero_without_creating_database(tmp_path: Path) -> None:
    database_path = tmp_path / "empty.sqlite"
    store = SQLiteHistoricalMarketDataStore(database_path)

    count = store.store(())

    assert count == 0
    assert not database_path.exists()


def test_store_persists_batch_and_allows_repository_retrieval(tmp_path: Path) -> None:
    database_path = tmp_path / "history.sqlite"
    store = SQLiteHistoricalMarketDataStore(database_path)
    repository = SQLiteHistoricalMarketDataRepository(database_path)

    bars = (
        _make_bar(timestamp="2026-09-01T00:00:00Z", close_price="101"),
        _make_bar(timestamp="2026-09-02T00:00:00Z", close_price="102", adjusted_close="101.5"),
        _make_bar(timestamp="2026-09-03T00:00:00Z", close_price="103"),
    )

    count = store.store(bars)

    assert count == 3
    retrieved = repository.get_history(_query("2026-09-01T00:00:00Z", "2026-09-03T00:00:00Z"))
    assert len(retrieved) == 3
    assert retrieved[0].point_in_time.value == "2026-09-01T00:00:00Z"
    assert retrieved[1].adjusted_close is not None
    assert str(retrieved[1].adjusted_close.amount) == "101.5"
    assert retrieved[2].point_in_time.value == "2026-09-03T00:00:00Z"


def test_store_repeated_identical_batch_is_idempotent(tmp_path: Path) -> None:
    database_path = tmp_path / "history.sqlite"
    store = SQLiteHistoricalMarketDataStore(database_path)
    repository = SQLiteHistoricalMarketDataRepository(database_path)

    bars = (
        _make_bar(timestamp="2026-09-01T00:00:00Z", close_price="101"),
        _make_bar(timestamp="2026-09-02T00:00:00Z", close_price="102"),
    )

    count_1 = store.store(bars)
    count_2 = store.store(bars)

    assert count_1 == 2
    assert count_2 == 2

    retrieved = repository.get_history(_query("2026-09-01T00:00:00Z", "2026-09-02T00:00:00Z"))
    assert len(retrieved) == 2


def test_store_upsert_replaces_previous_values_for_same_identity(tmp_path: Path) -> None:
    database_path = tmp_path / "history.sqlite"
    store = SQLiteHistoricalMarketDataStore(database_path)
    repository = SQLiteHistoricalMarketDataRepository(database_path)

    initial_bars = (_make_bar(timestamp="2026-09-01T00:00:00Z", close_price="101", volume="1000"),)
    store.store(initial_bars)

    revised_bars = (
        _make_bar(
            timestamp="2026-09-01T00:00:00Z",
            close_price="105",
            high_price="108",
            volume="2500",
            adjusted_close="104.5",
        ),
    )
    count = store.store(revised_bars)

    assert count == 1
    retrieved = repository.get_history(_query("2026-09-01T00:00:00Z", "2026-09-01T00:00:00Z"))
    assert len(retrieved) == 1
    assert str(retrieved[0].close.amount) == "105"
    assert str(retrieved[0].high.amount) == "108"
    assert str(retrieved[0].volume.value) == "2500"
    assert retrieved[0].adjusted_close is not None
    assert str(retrieved[0].adjusted_close.amount) == "104.5"


def test_store_canonical_point_in_time_normalization(tmp_path: Path) -> None:
    database_path = tmp_path / "history.sqlite"
    store = SQLiteHistoricalMarketDataStore(database_path)
    repository = SQLiteHistoricalMarketDataRepository(database_path)

    bar = _make_bar(timestamp="2026-09-01T14:30:00+05:00")
    assert bar.point_in_time.value == "2026-09-01T09:30:00Z"

    store.store((bar,))

    with sqlite3.connect(database_path) as connection:
        row = connection.execute("SELECT point_in_time FROM historical_ohlcv").fetchone()
        assert row[0] == "2026-09-01T09:30:00Z"

    retrieved = repository.get_history(_query("2026-09-01T09:30:00Z", "2026-09-01T09:30:00Z"))
    assert len(retrieved) == 1
    assert retrieved[0].point_in_time.value == "2026-09-01T09:30:00Z"


def test_store_failure_rolls_back_and_raises_storage_error(tmp_path: Path) -> None:
    database_path = tmp_path / "history.sqlite"
    _create_database(database_path)
    store = SQLiteHistoricalMarketDataStore(database_path)

    # Force database to be read-only by making a read-only URI or corrupting table
    with sqlite3.connect(database_path) as connection:
        connection.execute("DROP TABLE historical_ohlcv")
        connection.execute("CREATE VIEW historical_ohlcv AS SELECT 1 AS symbol")

    bars = (_make_bar(timestamp="2026-09-10T00:00:00Z"),)
    try:
        store.store(bars)
    except HistoricalStorageError as error:
        assert "storage is unavailable" in str(error)
    else:
        raise AssertionError("Expected HistoricalStorageError on failed store")
