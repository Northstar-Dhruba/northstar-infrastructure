"""Schema definition for the local forward research record store."""

from __future__ import annotations

import sqlite3

FORWARD_RESEARCH_RECORD_SCHEMA = """
CREATE TABLE IF NOT EXISTS forward_research_records (
    symbol TEXT NOT NULL,
    exchange_code TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    decision_instant TEXT NOT NULL,
    strategy_identity TEXT NOT NULL,
    evidence TEXT NOT NULL,
    PRIMARY KEY (symbol, exchange_code, timeframe, decision_instant, strategy_identity)
)
"""


def initialize_forward_research_record_schema(connection: sqlite3.Connection) -> None:
    """Create the local forward research record schema when it is absent."""
    connection.execute(FORWARD_RESEARCH_RECORD_SCHEMA)
    connection.commit()
