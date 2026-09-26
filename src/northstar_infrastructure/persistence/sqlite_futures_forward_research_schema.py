"""Schema definition for the local futures forward research record store."""

from __future__ import annotations

import sqlite3

FUTURES_FORWARD_RESEARCH_RECORD_SCHEMA = """
CREATE TABLE IF NOT EXISTS futures_forward_research_records (
    product_code TEXT NOT NULL,
    exchange_code TEXT NOT NULL,
    expiration_date TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    decision_instant TEXT NOT NULL,
    strategy_identity TEXT NOT NULL,
    evidence TEXT NOT NULL,
    PRIMARY KEY (
        product_code,
        exchange_code,
        expiration_date,
        timeframe,
        decision_instant,
        strategy_identity
    )
)
"""


def initialize_futures_forward_research_record_schema(connection: sqlite3.Connection) -> None:
    """Create the local futures forward research record schema when it is absent."""
    connection.execute(FUTURES_FORWARD_RESEARCH_RECORD_SCHEMA)
    connection.commit()
