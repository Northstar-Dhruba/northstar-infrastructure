"""Schema definition for the local futures historical market data store.

A bar's natural key is ``(FuturesContract, PointInTime, Timeframe)``, and a
FuturesContract is ``(product reference, expiration date)``, so the key is
stored as five constrained columns. Expressing it as a composite PRIMARY KEY
makes the database itself refuse a duplicate, rather than leaving the rule to
application validation where two writers could race past it.

The key columns are ordered so that the repository's filter -- product,
exchange, expiration, timeframe -- is an exact prefix of the index, with
point_in_time last. Column order inside the constraint does not change which
rows are unique; it only decides which lookups the index can serve.

Every Decimal-backed value is stored as exact TEXT. A float column would round
a quotation on the way in and silently change the evidence, which is the whole
reason QuoteValue exists. There is no currency column: futures are not
universally quoted in currency, so a stored currency would assert something
false for an index-point or points-of-par quotation.

Deliberately absent: provider symbol, underlying, adjusted close, open
interest, multiplier, tick size, and any JSON or pickled blob. Each is either a
provider naming concern, a deferred concept, or evidence that belongs in its
own typed column.
"""

from __future__ import annotations

import sqlite3

FUTURES_MARKET_DATA_SCHEMA = """
CREATE TABLE IF NOT EXISTS futures_ohlcv (
    product_code TEXT NOT NULL,
    exchange_code TEXT NOT NULL,
    expiration_date TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    point_in_time TEXT NOT NULL,
    open_value TEXT NOT NULL,
    high_value TEXT NOT NULL,
    low_value TEXT NOT NULL,
    close_value TEXT NOT NULL,
    volume TEXT NOT NULL,
    PRIMARY KEY (
        product_code,
        exchange_code,
        expiration_date,
        timeframe,
        point_in_time
    )
)
"""


def initialize_futures_market_data_schema(connection: sqlite3.Connection) -> None:
    """Create the local futures market data schema when it is absent."""
    connection.execute(FUTURES_MARKET_DATA_SCHEMA)
    connection.commit()
