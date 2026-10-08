"""Read-only cross-section of one option expiration's daily bars at one instant.

One query reads every stored ``1d`` bar of one product's expiration stamped
exactly at one instant -- no per-contract or per-strike query, and no earlier,
later or latest bar standing in for a missing one. Exact equality on the stored
instant text is safe here: ``option_ohlcv`` stores canonical UTC PointInTime
text, and two equal instants have one canonical spelling. Ordering instants by
text would not be safe, and none is done.

The repository is read-only. It opens SQLite in read-only mode, runs no schema
initializer and no DDL, takes no lock and reads no provider, network or clock.
A database file or ``option_ohlcv`` table that does not exist holds no bars.
Every row is decoded by the option bar store's own canonical decoder, so a
corrupt or non-canonical row fails loudly with OptionHistoricalStorageError.
Raw provider open interest is not read.
"""

from __future__ import annotations

import sqlite3
from contextlib import closing
from pathlib import Path

from northstar_application.ports import OptionChainDailyBarQuery, OptionChainDailyBarRepository
from northstar_core.foundation.value_objects import Timeframe
from northstar_core.options import OptionContract, OptionOHLCVBar, OptionRight

from northstar_infrastructure.market_data.sqlite_option_historical_market_data import (
    _COLUMNS,
    _TABLE_EXISTS,
    OptionHistoricalStorageError,
    _decode,
)

_DAILY = Timeframe("1d")
_SELECT_AT = (
    f"SELECT {', '.join(_COLUMNS)} FROM option_ohlcv "  # noqa: S608
    "WHERE product_code = ? AND exchange_code = ? AND expiration_date = ? "
    "AND timeframe = ? AND point_in_time = ?"
)
_RIGHT_RANK = {OptionRight.CALL: 0, OptionRight.PUT: 1}


def _chain_order(contract: OptionContract) -> tuple:
    return (contract.strike.value, _RIGHT_RANK[contract.right])


class SQLiteOptionChainDailyBarRepository(OptionChainDailyBarRepository):
    """Read one expiration's daily bars stamped exactly at one instant, read-only."""

    def __init__(self, database_path: str | Path) -> None:
        self._database_path = str(database_path)

    def daily_bars_at(self, query: OptionChainDailyBarQuery) -> tuple[OptionOHLCVBar, ...]:
        """Return the expiration's ``1d`` bars stamped exactly at ``as_of``."""
        if not isinstance(query, OptionChainDailyBarQuery):
            raise TypeError(
                "SQLiteOptionChainDailyBarRepository query must be an OptionChainDailyBarQuery."
            )
        path = Path(self._database_path)
        if not path.exists():
            return ()
        product = query.product
        try:
            # mode=ro can neither create the file nor change its schema.
            read_only = f"{path.absolute().as_uri()}?mode=ro"
            with closing(sqlite3.connect(read_only, uri=True)) as connection:
                if connection.execute(_TABLE_EXISTS).fetchone() is None:
                    return ()
                rows = connection.execute(
                    _SELECT_AT,
                    (
                        product.product_code.value,
                        product.exchange_code.value,
                        query.expiration_date.value,
                        _DAILY.value,
                        query.as_of.value,
                    ),
                ).fetchall()
        except sqlite3.Error as exc:
            raise OptionHistoricalStorageError(
                "Option market data storage is unavailable."
            ) from exc

        bars = [_decode(tuple(row)) for row in rows]
        for bar in bars:
            if (
                bar.contract.product != product
                or bar.contract.expiration_date != query.expiration_date
                or bar.timeframe != _DAILY
                or bar.point_in_time != query.as_of
            ):
                raise OptionHistoricalStorageError(
                    f"Option market data storage returned {bar} for "
                    f"{product} {query.expiration_date} at {query.as_of}."
                )
        return tuple(sorted(bars, key=lambda bar: _chain_order(bar.contract)))
