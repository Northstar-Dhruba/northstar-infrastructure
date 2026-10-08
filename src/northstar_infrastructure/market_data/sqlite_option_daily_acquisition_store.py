"""Atomic persistence of one option daily acquisition: canonical bars and raw open interest.

Application hands the acquired OptionOHLCVBar batch to an
OptionHistoricalMarketDataStore and knows nothing of provider open interest. This
store implements that port and, in the same SQLite transaction, persists the raw
open interest the source captured from the very candles the bars were built
from. The two commit together or not at all: a sync never ends with canonical
bars stored but their open-interest evidence missing, or the reverse.

Where the open interest comes from
----------------------------------
The source that produced the observations is also an OptionOpenInterestCapture.
Each ``store`` takes its capture exactly once, before anything is written, so a
capture can never be reused by a later store. The capture must correspond to the
bars exactly: one record per bar, from the configured provider, for the bar's
own contract and trading date, and nothing else. A bar's trading date is the
civil date of its session-close instant in the venue's timezone (ADR-014). Any
mismatch is a wiring defect, reported as OptionOpenInterestCaptureError, never a
data gap -- a session without a provider candle has no bar and no open interest.

One transaction
---------------
One connection runs ``BEGIN IMMEDIATE``, creates the two option tables when they
are absent, stores every bar and then every open-interest record with the 4B
option stores' own insert-only row logic, and commits. Any failure rolls the
whole transaction back, table creation included, so a failed first acquisition
leaves no schema behind. An empty batch with an empty capture touches no
database at all.
"""

from __future__ import annotations

import sqlite3
from datetime import date, datetime
from pathlib import Path
from typing import Protocol
from zoneinfo import ZoneInfo

from northstar_application.ports import OptionHistoricalMarketDataStore
from northstar_core.options import OptionContract, OptionOHLCVBar

from northstar_infrastructure.market_data.sqlite_option_historical_market_data import (
    OptionHistoricalStorageError,
    SQLiteOptionHistoricalMarketDataStore,
)
from northstar_infrastructure.market_data.sqlite_option_market_data_schema import (
    OPTION_OHLCV_SCHEMA,
    OPTION_PROVIDER_OPEN_INTEREST_SCHEMA,
)
from northstar_infrastructure.market_data.sqlite_option_provider_open_interest import (
    ProviderOptionOpenInterest,
    SQLiteOptionProviderOpenInterestStore,
)

# Session-close instants are converted back to trading dates in the venue's zone.
_VENUE_ZONES = {"NSE": ZoneInfo("Asia/Kolkata")}


class OptionOpenInterestCaptureError(RuntimeError):
    """Raised when captured open interest does not correspond exactly to the bars stored.

    This is an internal wiring or contract defect between a source and this
    store, never a market-data gap; nothing has been written when it is raised.
    """


class OptionOpenInterestCapture(Protocol):
    """A source that captured raw open interest for the observations it returned."""

    def take_open_interest(self) -> tuple[ProviderOptionOpenInterest, ...]:
        """Return, and forget, the open interest of the most recent successful fetch."""
        ...


class SQLiteOptionDailyAcquisitionStore(OptionHistoricalMarketDataStore):
    """Persist option bars and their captured raw open interest in one transaction."""

    def __init__(
        self,
        database_path: str | Path,
        *,
        open_interest: OptionOpenInterestCapture,
        provider: str,
    ) -> None:
        if not callable(getattr(open_interest, "take_open_interest", None)):
            raise TypeError(
                "SQLiteOptionDailyAcquisitionStore open_interest must be an "
                "OptionOpenInterestCapture."
            )
        if not isinstance(provider, str) or not provider:
            raise TypeError("SQLiteOptionDailyAcquisitionStore provider must be a provider name.")
        self._database_path = str(database_path)
        self._open_interest = open_interest
        self._provider = provider
        self._bars = SQLiteOptionHistoricalMarketDataStore(database_path)

    def store(self, bars: tuple[OptionOHLCVBar, ...]) -> int:
        """Persist the bars and their open interest atomically; never overwrite."""
        captured = self._open_interest.take_open_interest()
        prepared = SQLiteOptionHistoricalMarketDataStore._prepare(bars)
        records = _matching_open_interest(prepared, captured, self._provider)
        if not prepared:
            return 0

        try:
            connection = sqlite3.connect(self._database_path, isolation_level=None)
        except sqlite3.Error as exc:
            raise OptionHistoricalStorageError(
                "Option market data storage is unavailable."
            ) from exc
        try:
            connection.execute("BEGIN IMMEDIATE")
            # Inside the transaction, so a failed first store creates no table.
            connection.execute(OPTION_OHLCV_SCHEMA)
            connection.execute(OPTION_PROVIDER_OPEN_INTEREST_SCHEMA)
            for bar in prepared:
                self._bars._store_one(connection, bar)
            for record in records:
                SQLiteOptionProviderOpenInterestStore._store_one(connection, record)
            connection.commit()
        except sqlite3.Error as exc:
            _rollback(connection)
            raise OptionHistoricalStorageError(
                "Option market data storage is unavailable."
            ) from exc
        except BaseException:
            _rollback(connection)
            raise
        finally:
            connection.close()
        return len(prepared)


def _rollback(connection: sqlite3.Connection) -> None:
    if connection.in_transaction:
        connection.rollback()


def _matching_open_interest(
    bars: tuple[OptionOHLCVBar, ...], captured: object, provider: str
) -> tuple[ProviderOptionOpenInterest, ...]:
    """Return exactly one captured record per bar, in bar order, or refuse."""
    if not isinstance(captured, tuple) or not all(
        isinstance(record, ProviderOptionOpenInterest) for record in captured
    ):
        raise OptionOpenInterestCaptureError(
            "Captured open interest must be a tuple of ProviderOptionOpenInterest."
        )
    by_key: dict[tuple[OptionContract, date], ProviderOptionOpenInterest] = {}
    for record in captured:
        if record.provider != provider:
            raise OptionOpenInterestCaptureError(
                f"Captured open interest is from {record.provider}, not {provider}."
            )
        key = (record.contract, record.trading_date)
        if key in by_key:
            raise OptionOpenInterestCaptureError(
                f"Captured open interest holds two records for {record.contract} on "
                f"{record.trading_date.isoformat()}."
            )
        by_key[key] = record

    matched: list[ProviderOptionOpenInterest] = []
    for bar in bars:
        key = (bar.contract, _trading_date(bar))
        record = by_key.pop(key, None)
        if record is None:
            raise OptionOpenInterestCaptureError(
                f"No open interest was captured for {bar.contract} on {key[1].isoformat()}."
            )
        matched.append(record)
    if by_key:
        contract, day = min(by_key, key=lambda key: key[1])
        raise OptionOpenInterestCaptureError(
            f"Open interest was captured for {contract} on {day.isoformat()} "
            "with no bar to store beside it."
        )
    return tuple(matched)


def _trading_date(bar: OptionOHLCVBar) -> date:
    exchange = bar.contract.product.exchange_code.value
    zone = _VENUE_ZONES.get(exchange)
    if zone is None:
        raise OptionOpenInterestCaptureError(f"No session timezone is known for {exchange}.")
    return datetime.fromisoformat(bar.point_in_time.value).astimezone(zone).date()
