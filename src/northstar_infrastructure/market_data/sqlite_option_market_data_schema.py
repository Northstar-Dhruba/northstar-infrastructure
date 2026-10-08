"""Schemas for the local option daily market data and provider open interest.

``option_ohlcv`` holds canonical OptionOHLCVBar values. A bar's natural key is
``(OptionContract, PointInTime, Timeframe)`` and an OptionContract is product,
exchange, expiration, strike and right, so the key is stored as seven
constrained columns forming one composite PRIMARY KEY. Every Decimal-backed
value -- strike, the four premiums and the contract-count volume -- is exact
canonical TEXT, never REAL.

``option_daily_provider_open_interest`` preserves provider open interest as
Infrastructure-only evidence, keyed by provider, exact contract and trading
date. The value is the exact text of the parsed provider number. It is not
converted to contracts and no unit is claimed for it; it is not canonical
market data and is never exposed through Core or Application.

Both tables are insert-only and created only by their own initializers, which
only their stores call, immediately before a write. Neither is part of the
futures database initializer, and no futures table is read or altered here.
"""

from __future__ import annotations

import sqlite3

OPTION_OHLCV_SCHEMA = """
CREATE TABLE IF NOT EXISTS option_ohlcv (
    product_code TEXT NOT NULL,
    exchange_code TEXT NOT NULL,
    expiration_date TEXT NOT NULL,
    strike TEXT NOT NULL,
    option_right TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    point_in_time TEXT NOT NULL,
    open_premium TEXT NOT NULL,
    high_premium TEXT NOT NULL,
    low_premium TEXT NOT NULL,
    close_premium TEXT NOT NULL,
    volume TEXT NOT NULL,
    PRIMARY KEY (
        product_code,
        exchange_code,
        expiration_date,
        strike,
        option_right,
        timeframe,
        point_in_time
    )
)
"""

OPTION_PROVIDER_OPEN_INTEREST_SCHEMA = """
CREATE TABLE IF NOT EXISTS option_daily_provider_open_interest (
    provider TEXT NOT NULL,
    product_code TEXT NOT NULL,
    exchange_code TEXT NOT NULL,
    expiration_date TEXT NOT NULL,
    strike TEXT NOT NULL,
    option_right TEXT NOT NULL,
    trading_date TEXT NOT NULL,
    open_interest_raw TEXT NOT NULL,
    PRIMARY KEY (
        provider,
        product_code,
        exchange_code,
        expiration_date,
        strike,
        option_right,
        trading_date
    )
)
"""


def initialize_option_market_data_schema(connection: sqlite3.Connection) -> None:
    """Create the canonical option daily bar table when it is absent."""
    connection.execute(OPTION_OHLCV_SCHEMA)
    connection.commit()


def initialize_option_provider_open_interest_schema(connection: sqlite3.Connection) -> None:
    """Create the provider open-interest evidence table when it is absent."""
    connection.execute(OPTION_PROVIDER_OPEN_INTEREST_SCHEMA)
    connection.commit()
