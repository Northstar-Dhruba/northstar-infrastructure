"""SQLite-backed canonical option daily market data store and repository.

A stored option bar is immutable evidence. A differing bar under an existing
natural key may be a provider revision, a correction or two sources disagreeing,
and those look identical to a store, so none is applied silently: it is a
conflict for an explicit reconciliation step to decide. This module therefore
issues INSERT only -- no UPDATE, no REPLACE, no overwriting upsert, no DELETE.

Every value is canonical TEXT: the strike and the four premiums are Core
canonical Decimal spellings, the volume is the canonical Quantity spelling, the
right is ``CALL`` or ``PUT`` and the instant is canonical UTC PointInTime text.
Every read rebuilds the Core values and requires them to reproduce the stored
text exactly, so a corrupt or non-canonical row fails loudly rather than being
normalized.

Equality of canonical text is safe for the natural key, but text ordering of
instants is not: a canonical instant omits zero fractional seconds. Windows and
ordering are therefore applied in Python with PointInTime.compare().

The store creates ``option_ohlcv`` immediately before its first write. The
repository is read-only: it opens the database in SQLite read-only mode, never
runs the schema initializer, and answers an empty tuple when the database file
or the table does not exist. No futures table is read or written here.
"""

from __future__ import annotations

import sqlite3
from contextlib import closing
from decimal import Decimal
from functools import cmp_to_key
from pathlib import Path

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

from northstar_infrastructure.market_data.sqlite_option_market_data_schema import (
    initialize_option_market_data_schema,
)

_KEY_COLUMNS = (
    "product_code",
    "exchange_code",
    "expiration_date",
    "strike",
    "option_right",
    "timeframe",
    "point_in_time",
)
_VALUE_COLUMNS = ("open_premium", "high_premium", "low_premium", "close_premium", "volume")
_COLUMNS = _KEY_COLUMNS + _VALUE_COLUMNS

_SELECT_ALL = f"SELECT {', '.join(_COLUMNS)} FROM option_ohlcv"  # noqa: S608
_SELECT_KEY = f"{_SELECT_ALL} WHERE {' AND '.join(f'{c} = ?' for c in _KEY_COLUMNS)}"
_SELECT_SERIES = f"{_SELECT_ALL} WHERE {' AND '.join(f'{c} = ?' for c in _KEY_COLUMNS[:6])}"
_INSERT = (
    f"INSERT INTO option_ohlcv ({', '.join(_COLUMNS)}) "  # noqa: S608
    f"VALUES ({', '.join('?' for _ in _COLUMNS)})"
)
_TABLE_EXISTS = "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'option_ohlcv'"

_DECODE_ERRORS = (TypeError, ValueError, ArithmeticError)


class OptionHistoricalStorageError(RuntimeError):
    """Raised when the local option market data store is unavailable or malformed."""


def _series_key(contract: OptionContract, timeframe: Timeframe) -> tuple[str, ...]:
    product = contract.product
    return (
        product.product_code.value,
        product.exchange_code.value,
        contract.expiration_date.value,
        str(contract.strike.value),
        contract.right.value,
        timeframe.value,
    )


def _key(bar: OptionOHLCVBar) -> tuple[str, ...]:
    return (*_series_key(bar.contract, bar.timeframe), bar.point_in_time.value)


def _row(bar: OptionOHLCVBar) -> tuple[str, ...]:
    return (
        *_key(bar),
        str(bar.open.value),
        str(bar.high.value),
        str(bar.low.value),
        str(bar.close.value),
        str(bar.volume.value),
    )


def _bar(row: tuple[object, ...]) -> OptionOHLCVBar:
    if len(row) != len(_COLUMNS) or not all(isinstance(value, str) for value in row):
        raise TypeError("Option market data columns must all be text.")
    product, exchange, expiration, strike, right, timeframe, instant, *values = row
    open_text, high_text, low_text, close_text, volume_text = values
    return OptionOHLCVBar(
        contract=OptionContract(
            OptionProductReference(Symbol(product), ExchangeCode(exchange)),
            ExpirationDate(expiration),
            OptionStrike(Decimal(strike)),
            OptionRight(right),
        ),
        point_in_time=PointInTime(instant),
        timeframe=Timeframe(timeframe),
        open=OptionPremium(Decimal(open_text)),
        high=OptionPremium(Decimal(high_text)),
        low=OptionPremium(Decimal(low_text)),
        close=OptionPremium(Decimal(close_text)),
        volume=Quantity(Decimal(volume_text)),
    )


def _decode(row: tuple[object, ...]) -> OptionOHLCVBar:
    """Rebuild a stored bar and require it to reproduce its stored text."""
    try:
        bar = _bar(row)
    except _DECODE_ERRORS as exc:
        raise OptionHistoricalStorageError(
            "Option market data storage contains invalid data."
        ) from exc
    if _row(bar) != row:
        raise OptionHistoricalStorageError(
            "Option market data storage contains non-canonical data."
        )
    return bar


class SQLiteOptionHistoricalMarketDataStore(OptionHistoricalMarketDataStore):
    """Persist canonical option bars into a local SQLite store, insert only."""

    def __init__(self, database_path: str | Path) -> None:
        self._database_path = str(database_path)

    def store(self, bars: tuple[OptionOHLCVBar, ...]) -> int:
        """Persist a batch atomically, never overwriting stored evidence."""
        prepared = self._prepare(bars)
        if not prepared:
            return 0

        try:
            connection = sqlite3.connect(self._database_path, isolation_level=None)
            initialize_option_market_data_schema(connection)
        except sqlite3.Error as exc:
            raise OptionHistoricalStorageError(
                "Option market data storage is unavailable."
            ) from exc
        try:
            connection.execute("BEGIN IMMEDIATE")
            for bar in prepared:
                self._store_one(connection, bar)
            connection.commit()
        except (OptionHistoricalMarketDataConflictError, OptionHistoricalStorageError):
            connection.rollback()
            raise
        except sqlite3.Error as exc:
            connection.rollback()
            raise OptionHistoricalStorageError(
                "Option market data storage is unavailable."
            ) from exc
        finally:
            connection.close()
        return len(prepared)

    @staticmethod
    def _prepare(bars: tuple[OptionOHLCVBar, ...]) -> tuple[OptionOHLCVBar, ...]:
        """Reject foreign values and in-batch duplicate keys before any write."""
        if not isinstance(bars, tuple):
            raise TypeError("SQLiteOptionHistoricalMarketDataStore bars must be a tuple.")
        seen: set[tuple[str, ...]] = set()
        for bar in bars:
            if not isinstance(bar, OptionOHLCVBar):
                raise TypeError(
                    "SQLiteOptionHistoricalMarketDataStore bars must be OptionOHLCVBar values."
                )
            key = _key(bar)
            if key in seen:
                raise OptionHistoricalMarketDataConflictError(
                    "Option market data batch contains two bars sharing one natural key."
                )
            seen.add(key)
        return bars

    def _store_one(self, connection: sqlite3.Connection, bar: OptionOHLCVBar) -> None:
        existing = self._existing(connection, bar)
        if existing is None:
            try:
                connection.execute(_INSERT, _row(bar))
                return
            except sqlite3.IntegrityError:
                # Another writer stored the key first; resolve as if seen.
                existing = self._existing(connection, bar)
                if existing is None:
                    raise
        if _decode(tuple(existing)) != bar:
            raise OptionHistoricalMarketDataConflictError(
                f"A different option bar is already stored for {bar.contract} at "
                f"{bar.point_in_time} ({bar.timeframe})."
            )

    @staticmethod
    def _existing(connection: sqlite3.Connection, bar: OptionOHLCVBar) -> tuple | None:
        row = connection.execute(_SELECT_KEY, _key(bar)).fetchone()
        return None if row is None else tuple(row)


class SQLiteOptionHistoricalMarketDataRepository(OptionHistoricalMarketDataRepository):
    """Retrieve one exact option contract's bars from a local SQLite store, read-only."""

    def __init__(self, database_path: str | Path) -> None:
        self._database_path = str(database_path)

    def get_bars(self, query: OptionHistoricalMarketDataQuery) -> tuple[OptionOHLCVBar, ...]:
        """Return one contract's bars within the window, oldest to newest."""
        if not isinstance(query, OptionHistoricalMarketDataQuery):
            raise TypeError(
                "SQLiteOptionHistoricalMarketDataRepository query must be an "
                "OptionHistoricalMarketDataQuery."
            )
        path = Path(self._database_path)
        if not path.exists():
            return ()
        try:
            # mode=ro can neither create the file nor change its schema.
            read_only = f"{path.absolute().as_uri()}?mode=ro"
            with closing(sqlite3.connect(read_only, uri=True)) as connection:
                if connection.execute(_TABLE_EXISTS).fetchone() is None:
                    return ()
                rows = connection.execute(
                    _SELECT_SERIES, _series_key(query.contract, query.timeframe)
                ).fetchall()
        except sqlite3.Error as exc:
            raise OptionHistoricalStorageError(
                "Option market data storage is unavailable."
            ) from exc

        bars = [_decode(tuple(row)) for row in rows]
        for bar in bars:
            if bar.contract != query.contract or bar.timeframe != query.timeframe:
                raise OptionHistoricalStorageError(
                    f"Option market data storage returned {bar.contract} for {query.contract}."
                )
        selected = [bar for bar in bars if query.covers(bar.point_in_time)]
        selected.sort(key=cmp_to_key(lambda a, b: a.point_in_time.compare(b.point_in_time)))
        return tuple(selected)
