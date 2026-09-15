"""SQLite-backed historical market data repository."""

from __future__ import annotations

import sqlite3
from decimal import Decimal
from functools import cmp_to_key
from pathlib import Path

from northstar_application.ports import HistoricalMarketDataQuery, HistoricalMarketDataRepository
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


class HistoricalStorageError(RuntimeError):
    """Raised when the local historical market data store is unavailable or malformed."""


class SQLiteHistoricalMarketDataRepository(HistoricalMarketDataRepository):
    """Retrieve historical OHLCV observations from a local SQLite store."""

    def __init__(self, database_path: str | Path) -> None:
        self._database_path = str(database_path)

    def get_history(self, query: HistoricalMarketDataQuery) -> tuple[HistoricalOHLCVBar, ...]:
        """Return inclusive query matches ordered oldest to newest."""
        try:
            with sqlite3.connect(self._database_path) as connection:
                rows = connection.execute(
                    """
                    SELECT symbol, exchange_code, point_in_time, timeframe,
                           currency, open, high, low, close, volume, adjusted_close
                    FROM historical_ohlcv
                    WHERE symbol = ? AND exchange_code = ? AND timeframe = ?
                    """,
                    (query.symbol.value, query.exchange_code.value, query.timeframe.value),
                ).fetchall()
        except sqlite3.Error as exc:
            raise HistoricalStorageError("Historical market data storage is unavailable.") from exc

        try:
            bars = tuple(self._map_row(row) for row in rows)
            bounded = tuple(
                bar
                for bar in bars
                if query.start.compare(bar.point_in_time) <= 0
                and query.end.compare(bar.point_in_time) >= 0
            )
        except HistoricalStorageError:
            raise
        except (TypeError, ValueError) as exc:
            raise HistoricalStorageError(
                "Historical market data storage contains invalid data."
            ) from exc

        return tuple(sorted(bounded, key=cmp_to_key(_compare_bars)))

    @staticmethod
    def _map_row(row: tuple[object, ...]) -> HistoricalOHLCVBar:
        (
            symbol,
            exchange_code,
            point_in_time,
            timeframe,
            currency,
            open_value,
            high_value,
            low_value,
            close_value,
            volume,
            adjusted_close,
        ) = row
        denomination = Currency(str(currency))
        return HistoricalOHLCVBar(
            symbol=Symbol(str(symbol)),
            exchange_code=ExchangeCode(str(exchange_code)),
            point_in_time=PointInTime(str(point_in_time)),
            timeframe=Timeframe(str(timeframe)),
            open=Price(_decimal_text(open_value), denomination),
            high=Price(_decimal_text(high_value), denomination),
            low=Price(_decimal_text(low_value), denomination),
            close=Price(_decimal_text(close_value), denomination),
            volume=Quantity(_decimal_text(volume)),
            adjusted_close=(
                Price(_decimal_text(adjusted_close), denomination)
                if adjusted_close is not None
                else None
            ),
        )


def _compare_bars(left: HistoricalOHLCVBar, right: HistoricalOHLCVBar) -> int:
    return left.point_in_time.compare(right.point_in_time)


def _decimal_text(value: object) -> str:
    try:
        decimal_value = Decimal(str(value))
    except Exception as exc:
        raise HistoricalStorageError(
            "Historical market data storage contains invalid numeric data."
        ) from exc
    if not decimal_value.is_finite():
        raise HistoricalStorageError(
            "Historical market data storage contains invalid numeric data."
        )
    return str(decimal_value)
