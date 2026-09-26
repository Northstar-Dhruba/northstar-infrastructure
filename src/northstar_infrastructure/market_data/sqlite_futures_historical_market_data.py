"""SQLite-backed futures historical market data store and repository.

A settled futures bar is treated as immutable evidence. A contract has no
splits and no dividends and no adjusted close to restate, so the equity
restatement mechanism does not apply here.

Futures observations can still be corrected: exchanges revise settlement
prices, trades are busted after the fact, session volume is finalised late, and
providers reissue data they got wrong. A differing bar under an existing
natural key may therefore be a genuine correction, two sources disagreeing, or
a back-adjusted continuous series leaking in under a real contract's key. Those
look identical to a store, so none of them is applied silently: the differing
evidence surfaces as a conflict, and an explicit reconciliation step -- one
that does not exist yet -- decides what is true. This module therefore issues
INSERT only, and contains no UPDATE, no REPLACE, no overwriting upsert and no
DELETE.

That deliberately differs from SQLiteHistoricalMarketDataStore, which upserts
because equity history is legitimately restated. The divergence is specified by
FuturesHistoricalMarketDataStore, not chosen here.

Every Decimal-backed value is stored as exact TEXT and rebuilt through
QuoteValue and Quantity, so a quotation round-trips digit for digit and never
passes through a float.

Temporal handling splits equality from ordering. PointInTime canonicalizes to
one UTC spelling, so equal instants always have equal text and TEXT equality is
safe for the natural key. Ordering and range filtering are not safe: a
canonical instant omits fractional seconds when they are zero, so
``...T21:00:00.1Z`` sorts before ``...T21:00:00Z`` as text while being the
later instant. Windowing and ordering are therefore applied in Python through
PointInTime.compare(), never in SQL.
"""

from __future__ import annotations

import sqlite3
from decimal import Decimal
from functools import cmp_to_key
from pathlib import Path

from northstar_application.ports import (
    FuturesHistoricalMarketDataConflictError,
    FuturesHistoricalMarketDataQuery,
    FuturesHistoricalMarketDataRepository,
    FuturesHistoricalMarketDataStore,
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

from northstar_infrastructure.market_data.sqlite_futures_schema import (
    initialize_futures_market_data_schema,
)

_KEY_COLUMNS = (
    "product_code",
    "exchange_code",
    "expiration_date",
    "timeframe",
    "point_in_time",
)

_VALUE_COLUMNS = (
    "open_value",
    "high_value",
    "low_value",
    "close_value",
    "volume",
)

_COLUMNS = _KEY_COLUMNS + _VALUE_COLUMNS

_SELECT_ALL = f"SELECT {', '.join(_COLUMNS)} FROM futures_ohlcv"  # noqa: S608 - fixed identifiers

_KeyColumns = tuple[str, str, str, str, str]


class FuturesHistoricalStorageError(RuntimeError):
    """Raised when the local futures market data store is unavailable or malformed."""


# ---------------------------------------------------------------------------
# Serialization
# ---------------------------------------------------------------------------


def _key_columns(bar: FuturesOHLCVBar) -> _KeyColumns:
    """Return one bar's natural key as its stored column values.

    PointInTime is used by equality only. Its canonical form converts every UTC
    offset to Z and drops trailing fractional zeros, so two spellings of one
    instant produce one key and cannot both be stored.
    """
    product = bar.contract.product
    return (
        product.product_code.value,
        product.exchange_code.value,
        bar.contract.expiration_date.value,
        bar.timeframe.value,
        bar.point_in_time.value,
    )


def _row(bar: FuturesOHLCVBar) -> tuple[str, ...]:
    """Return one bar as its stored column values, all text."""
    return (
        *_key_columns(bar),
        str(bar.open.value),
        str(bar.high.value),
        str(bar.low.value),
        str(bar.close.value),
        str(bar.volume.value),
    )


def _bar(row: tuple[object, ...]) -> FuturesOHLCVBar:
    """Rebuild one bar from its stored column values."""
    (
        product_code,
        exchange_code,
        expiration_date,
        timeframe,
        point_in_time,
        open_value,
        high_value,
        low_value,
        close_value,
        volume,
    ) = row

    contract = FuturesContract(
        product=FuturesProductReference(
            Symbol(str(product_code)),
            ExchangeCode(str(exchange_code)),
        ),
        expiration_date=ExpirationDate(str(expiration_date)),
    )
    return FuturesOHLCVBar(
        contract=contract,
        point_in_time=PointInTime(str(point_in_time)),
        timeframe=Timeframe(str(timeframe)),
        open=QuoteValue(_decimal(open_value)),
        high=QuoteValue(_decimal(high_value)),
        low=QuoteValue(_decimal(low_value)),
        close=QuoteValue(_decimal(close_value)),
        volume=Quantity(_decimal(volume)),
    )


def _decimal(value: object) -> Decimal:
    """Rebuild an exact Decimal from stored text, never through a float."""
    try:
        decimal_value = Decimal(str(value))
    except ArithmeticError as exc:
        raise FuturesHistoricalStorageError(
            "Futures market data storage contains invalid numeric data."
        ) from exc
    if not decimal_value.is_finite():
        raise FuturesHistoricalStorageError(
            "Futures market data storage contains invalid numeric data."
        )
    return decimal_value


def _compare_bars(left: FuturesOHLCVBar, right: FuturesOHLCVBar) -> int:
    """Order bars chronologically by semantic instant, never by stored text."""
    return left.point_in_time.compare(right.point_in_time)


# ---------------------------------------------------------------------------
# Adapters
# ---------------------------------------------------------------------------


class SQLiteFuturesHistoricalMarketDataStore(FuturesHistoricalMarketDataStore):
    """Persist futures observations into a local SQLite store, insert only."""

    def __init__(self, database_path: str | Path) -> None:
        self._database_path = str(database_path)

    def store(self, bars: tuple[FuturesOHLCVBar, ...]) -> int:
        """Persist a batch atomically, never overwriting stored evidence."""
        if not bars:
            return 0

        prepared = self._prepare(bars)

        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            for key, bar in prepared:
                self._store_one(connection, key, bar)
            connection.commit()
        except FuturesHistoricalMarketDataConflictError:
            connection.rollback()
            raise
        except sqlite3.IntegrityError as exc:
            connection.rollback()
            raise FuturesHistoricalMarketDataConflictError(
                "A futures bar violating a storage uniqueness constraint was rejected."
            ) from exc
        except sqlite3.Error as exc:
            connection.rollback()
            raise FuturesHistoricalStorageError(
                "Futures market data storage is unavailable."
            ) from exc
        finally:
            connection.close()

        return len(bars)

    @staticmethod
    def _prepare(
        bars: tuple[FuturesOHLCVBar, ...],
    ) -> tuple[tuple[_KeyColumns, FuturesOHLCVBar], ...]:
        """Reject in-batch duplicate keys before any write is attempted.

        Two bars sharing a key conflict even when they are equal: a caller that
        submits one bar twice in a batch has a defect, and an idempotent
        re-store is a separate call rather than a duplicated element.
        """
        prepared: list[tuple[_KeyColumns, FuturesOHLCVBar]] = []
        seen: set[_KeyColumns] = set()
        for bar in bars:
            key = _key_columns(bar)
            if key in seen:
                raise FuturesHistoricalMarketDataConflictError(
                    "Futures market data batch contains two bars sharing one natural key."
                )
            seen.add(key)
            prepared.append((key, bar))
        return tuple(prepared)

    def _store_one(
        self, connection: sqlite3.Connection, key: _KeyColumns, bar: FuturesOHLCVBar
    ) -> None:
        where = " AND ".join(f"{column} = ?" for column in _KEY_COLUMNS)
        existing = connection.execute(f"{_SELECT_ALL} WHERE {where}", key).fetchone()

        if existing is not None:
            # Compare rebuilt domain values, not stored text: two spellings of
            # one quotation are one value, and only the value decides identity.
            if self._decode(tuple(existing)) != bar:
                raise FuturesHistoricalMarketDataConflictError(
                    "A different futures bar is already stored under this natural key."
                )
            return

        placeholders = ", ".join("?" for _ in _COLUMNS)
        connection.execute(
            f"INSERT INTO futures_ohlcv ({', '.join(_COLUMNS)}) VALUES ({placeholders})",  # noqa: S608
            _row(bar),
        )

    def _connect(self) -> sqlite3.Connection:
        try:
            connection = sqlite3.connect(self._database_path, isolation_level=None)
            initialize_futures_market_data_schema(connection)
        except sqlite3.Error as exc:
            raise FuturesHistoricalStorageError(
                "Futures market data storage is unavailable."
            ) from exc
        return connection

    @staticmethod
    def _decode(row: tuple[object, ...]) -> FuturesOHLCVBar:
        try:
            return _bar(row)
        except FuturesHistoricalStorageError:
            raise
        except (TypeError, ValueError) as exc:
            raise FuturesHistoricalStorageError(
                "Futures market data storage contains invalid data."
            ) from exc


class SQLiteFuturesHistoricalMarketDataRepository(FuturesHistoricalMarketDataRepository):
    """Retrieve one futures contract's observations from a local SQLite store.

    SQL filters only on the four columns whose canonical text is safe for
    equality. The temporal window and the ordering are applied in Python with
    PointInTime.compare(), because optional fractional seconds make stored
    instants lexicographically non-chronological.
    """

    def __init__(self, database_path: str | Path) -> None:
        self._database_path = str(database_path)

    def get_bars(self, query: FuturesHistoricalMarketDataQuery) -> tuple[FuturesOHLCVBar, ...]:
        """Return one contract's observations within the window, oldest to newest."""
        if not isinstance(query, FuturesHistoricalMarketDataQuery):
            raise TypeError(
                "SQLiteFuturesHistoricalMarketDataRepository query must be a "
                "FuturesHistoricalMarketDataQuery."
            )

        product = query.contract.product
        connection = None
        try:
            connection = sqlite3.connect(self._database_path)
            initialize_futures_market_data_schema(connection)
            rows = connection.execute(
                f"""
                {_SELECT_ALL}
                WHERE product_code = ?
                  AND exchange_code = ?
                  AND expiration_date = ?
                  AND timeframe = ?
                """,
                (
                    product.product_code.value,
                    product.exchange_code.value,
                    query.contract.expiration_date.value,
                    query.timeframe.value,
                ),
            ).fetchall()
        except sqlite3.Error as exc:
            raise FuturesHistoricalStorageError(
                "Futures market data storage is unavailable."
            ) from exc
        finally:
            if connection is not None:
                connection.close()

        try:
            bars = [_bar(tuple(row)) for row in rows]
        except FuturesHistoricalStorageError:
            raise
        except (TypeError, ValueError) as exc:
            raise FuturesHistoricalStorageError(
                "Futures market data storage contains invalid data."
            ) from exc

        bounded = [bar for bar in bars if query.covers(bar.point_in_time)]
        return tuple(sorted(bounded, key=cmp_to_key(_compare_bars)))
