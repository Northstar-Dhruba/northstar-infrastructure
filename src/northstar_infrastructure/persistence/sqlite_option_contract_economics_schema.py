"""Schema definition for the local option contract economics store.

Economics are contract-level: every expiry, strike and right of an option
product can carry its own point value, so the key is the complete
OptionContract -- ``(product_code, exchange_code, expiration_date, strike,
option_right)`` -- as a composite PRIMARY KEY. A call and a put, two strikes and
two expiries of NIFTY@NSE are all different keys and coexist.

One row holds exactly one OptionPointValue. The strike and the point-value
amount are exact canonical Decimal TEXT, never REAL, so neither is ever rounded
on the way in. Because OptionStrike has a single canonical spelling, text
equality in the key is numeric equality. The right is exactly ``CALL`` or
``PUT``; the column is named ``option_right`` because RIGHT is an SQL join
keyword. The settlement currency is stored once, here, and nowhere else. The
expiration is the canonical ``YYYY-MM-DD`` ExpirationDate text.

Economics are assumed constant for the life of a contract: there is no effective
date, and a stored row is never updated.

This table is created only by its own initializer, lazily, on the first option
economics write; the store calls the initializer immediately before writing.
Reads never create it. It is not part of the futures database initializer, so a
futures-only database stays futures-only, and no futures table is read or
altered here.

Deliberately absent: lot size, multiplier, underlying, premium, contract count,
tick size, margin, fees, provider instrument key and timestamps.
"""

from __future__ import annotations

import sqlite3

OPTION_CONTRACT_ECONOMICS_SCHEMA = """
CREATE TABLE IF NOT EXISTS option_contract_economics (
    product_code TEXT NOT NULL,
    exchange_code TEXT NOT NULL,
    expiration_date TEXT NOT NULL,
    strike TEXT NOT NULL,
    option_right TEXT NOT NULL,
    point_value_amount TEXT NOT NULL,
    settlement_currency TEXT NOT NULL,
    PRIMARY KEY (
        product_code,
        exchange_code,
        expiration_date,
        strike,
        option_right
    )
)
"""


def initialize_option_contract_economics_schema(connection: sqlite3.Connection) -> None:
    """Create the local option contract economics schema when it is absent."""
    connection.execute(OPTION_CONTRACT_ECONOMICS_SCHEMA)
    connection.commit()
