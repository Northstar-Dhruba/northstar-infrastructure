"""Schema definition for the local historical research store."""

from __future__ import annotations

import sqlite3

HISTORICAL_MARKET_DATA_SCHEMA = """
CREATE TABLE IF NOT EXISTS historical_ohlcv (
    symbol TEXT NOT NULL,
    exchange_code TEXT NOT NULL,
    point_in_time TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    currency TEXT NOT NULL,
    open TEXT NOT NULL,
    high TEXT NOT NULL,
    low TEXT NOT NULL,
    close TEXT NOT NULL,
    volume TEXT NOT NULL,
    adjusted_close TEXT,
    PRIMARY KEY (symbol, exchange_code, timeframe, point_in_time)
)
"""


def initialize_historical_market_data_schema(connection: sqlite3.Connection) -> None:
    """Create the local historical market data schema when it is absent."""
    connection.execute(HISTORICAL_MARKET_DATA_SCHEMA)
    connection.commit()
