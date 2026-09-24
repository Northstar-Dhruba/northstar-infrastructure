"""Schema definition for the local futures product economics store.

Economics are product-level: every expiry of a product shares them, so the key
is the FuturesProductReference alone -- ``(product_code, exchange_code)`` -- as
a composite PRIMARY KEY. There is no expiration column, and ES@CME and ES on
another exchange are different keys.

One row holds exactly one FuturesPointValue. The amount is exact canonical
Decimal TEXT, never REAL, so a point value is never rounded on the way in. The
settlement currency is stored once, here, and nowhere else.

Economics are assumed constant for a product reference: there is no effective
date, and a stored row is never updated.

Deliberately absent: expiration, underlying, tick size, tick value, margin,
notional, provider and timestamps.
"""

from __future__ import annotations

import sqlite3

FUTURES_PRODUCT_ECONOMICS_SCHEMA = """
CREATE TABLE IF NOT EXISTS futures_product_economics (
    product_code TEXT NOT NULL,
    exchange_code TEXT NOT NULL,
    point_value_amount TEXT NOT NULL,
    settlement_currency TEXT NOT NULL,
    PRIMARY KEY (product_code, exchange_code)
)
"""


def initialize_futures_product_economics_schema(connection: sqlite3.Connection) -> None:
    """Create the local futures product economics schema when it is absent."""
    connection.execute(FUTURES_PRODUCT_ECONOMICS_SCHEMA)
    connection.commit()
