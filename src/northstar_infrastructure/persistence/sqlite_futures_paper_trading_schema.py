"""Schema definition for the local futures paper order and fill store.

Only orders and fills are persisted. An order's status is never stored: it is
pending while it has no fill and filled once it has one. Positions and
portfolios are folded from fills on demand and have no table.

An order row holds exactly the fields of a FuturesPaperOrder. There is no
timeframe column: the order does not carry one, and its identity -- which was
derived from a daily forward record -- already distinguishes it.

``contracts`` is TEXT because FuturesContractCount is an unbounded positive
integer and SQLite INTEGER is signed 64-bit.

A fill row holds only what its parent order cannot supply: its identity, the
order it fills, the fill quote and the fill instant. Portfolio, contract, side,
count, strategy and decision instant are read from the parent order, so a fill
can never disagree with the order it executed. UNIQUE on ``order_identity``
makes the database itself refuse a second fill for one order. The REFERENCES
clause documents the relationship; the store checks the parent explicitly and
does not rely on SQLite foreign-key enforcement, which is off by default.
"""

from __future__ import annotations

import sqlite3

FUTURES_PAPER_ORDER_SCHEMA = """
CREATE TABLE IF NOT EXISTS futures_paper_orders (
    order_identity TEXT NOT NULL PRIMARY KEY,
    portfolio_identity TEXT NOT NULL,
    product_code TEXT NOT NULL,
    exchange_code TEXT NOT NULL,
    expiration_date TEXT NOT NULL,
    side TEXT NOT NULL,
    contracts TEXT NOT NULL,
    strategy_identity TEXT NOT NULL,
    decided_at TEXT NOT NULL
)
"""

FUTURES_PAPER_ORDER_PORTFOLIO_INDEX = """
CREATE INDEX IF NOT EXISTS futures_paper_orders_portfolio_identity
ON futures_paper_orders (portfolio_identity)
"""

FUTURES_PAPER_FILL_SCHEMA = """
CREATE TABLE IF NOT EXISTS futures_paper_fills (
    fill_identity TEXT NOT NULL PRIMARY KEY,
    order_identity TEXT NOT NULL UNIQUE REFERENCES futures_paper_orders (order_identity),
    fill_quote TEXT NOT NULL,
    filled_at TEXT NOT NULL
)
"""


def initialize_futures_paper_trading_schema(connection: sqlite3.Connection) -> None:
    """Create the local futures paper order and fill schema when it is absent."""
    connection.execute(FUTURES_PAPER_ORDER_SCHEMA)
    connection.execute(FUTURES_PAPER_ORDER_PORTFOLIO_INDEX)
    connection.execute(FUTURES_PAPER_FILL_SCHEMA)
    connection.commit()
