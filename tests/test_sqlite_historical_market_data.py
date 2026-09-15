"""Tests for the SQLite historical market data repository."""

import sqlite3
from pathlib import Path

from northstar_application.ports import HistoricalMarketDataQuery
from northstar_core.foundation.value_objects import ExchangeCode, PointInTime, Symbol, Timeframe

from northstar_infrastructure.market_data import (
    HistoricalStorageError,
    SQLiteHistoricalMarketDataRepository,
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
