"""Schema definition for the local futures contract economics store.

Economics are contract-level: expiries of one product can carry different point
values, so the key is the complete FuturesContract -- ``(product_code,
exchange_code, expiration_date)`` -- as a composite PRIMARY KEY. Two expiries of
NIFTY@NSE are different keys and coexist, and ES@CME can never collide with an
NSE contract because the exchange is part of every key.

One row holds exactly one FuturesPointValue. The amount is exact canonical
Decimal TEXT, never REAL, so a point value is never rounded on the way in. The
settlement currency is stored once, here, and nowhere else. The expiration is
the canonical ``YYYY-MM-DD`` ExpirationDate text.

Economics are assumed constant for the life of a contract: there is no effective
date, and a stored row is never updated.

This table is separate from ``futures_product_economics``, which is left exactly
as it was. Nothing reads product economics as contract economics.

Deliberately absent: lot size, multiplier, underlying, tick size, tick value,
margin, notional, provider and timestamps.
"""

from __future__ import annotations

import sqlite3

FUTURES_CONTRACT_ECONOMICS_SCHEMA = """
CREATE TABLE IF NOT EXISTS futures_contract_economics (
    product_code TEXT NOT NULL,
    exchange_code TEXT NOT NULL,
    expiration_date TEXT NOT NULL,
    point_value_amount TEXT NOT NULL,
    settlement_currency TEXT NOT NULL,
    PRIMARY KEY (product_code, exchange_code, expiration_date)
)
"""


def initialize_futures_contract_economics_schema(connection: sqlite3.Connection) -> None:
    """Create the local futures contract economics schema when it is absent."""
    connection.execute(FUTURES_CONTRACT_ECONOMICS_SCHEMA)
    connection.commit()
