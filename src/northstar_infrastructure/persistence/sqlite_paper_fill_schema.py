"""Schema definition for the local paper fill store.

Every field of a PaperFill is stored as its own column. The record is small and
its shape is stable, so first-class columns keep the stored history queryable
and readable without a serialization format to version.

The one-fill-per-order rule is expressed as a UNIQUE constraint rather than
left to application validation, so the database refuses a second fill for one
order even if two writers race.
"""

from __future__ import annotations

import sqlite3

PAPER_FILL_SCHEMA = """
CREATE TABLE IF NOT EXISTS paper_fills (
    fill_identity TEXT PRIMARY KEY,
    order_identity TEXT NOT NULL UNIQUE,
    portfolio_identity TEXT NOT NULL,
    symbol TEXT NOT NULL,
    exchange_code TEXT NOT NULL,
    side TEXT NOT NULL,
    strategy_identity TEXT NOT NULL,
    decided_at TEXT NOT NULL,
    quantity TEXT NOT NULL,
    price_amount TEXT NOT NULL,
    price_currency TEXT NOT NULL,
    filled_at TEXT NOT NULL
)
"""

PAPER_FILL_PORTFOLIO_INDEX = """
CREATE INDEX IF NOT EXISTS paper_fills_portfolio_identity
ON paper_fills (portfolio_identity)
"""


def initialize_paper_fill_schema(connection: sqlite3.Connection) -> None:
    """Create the local paper fill schema when it is absent."""
    connection.execute(PAPER_FILL_SCHEMA)
    connection.execute(PAPER_FILL_PORTFOLIO_INDEX)
    connection.commit()
