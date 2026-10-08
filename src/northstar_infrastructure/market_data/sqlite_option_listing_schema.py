"""Schema for the local option provider-listing reference.

Two insert-only tables preserve which provider instrument each exact option
contract was mapped to, and which downloaded master established it, so the
mapping stays explainable after the contract leaves the provider's current
master.

``option_listing_snapshots`` identifies one downloaded master by the SHA-256 of
its exact body. ``first_fetched_at`` is the first time Northstar persisted that
body; fetching identical bytes again later changes nothing. The counts are
properties of the body and are canonical non-negative integer text.

``option_provider_listings`` holds one immutable mapping per provider and exact
OptionContract -- product, exchange, expiration, strike and right -- to one
instrument key and the exchange lot the provider reported. ``UNIQUE (provider,
instrument_key)`` makes one key name one contract.
``established_snapshot_sha256`` and ``established_at`` record the observation
that first established the mapping and are never rewritten. There is no
last-seen column.

Every value is canonical TEXT: the strike is the OptionStrike canonical Decimal
spelling and never REAL, the right is ``CALL`` or ``PUT``, the expiration is
``YYYY-MM-DD``, instants are canonical UTC PointInTime text, hashes are
lowercase hexadecimal.

Both tables are created only by this initializer, which only the option
instrument sync calls, under the database operations lock. They are not part of
the futures database initializer, and no futures table is read or altered here.

Deliberately absent: trading symbol, exchange token, tick size, the provider's
weekly flag, any expiry-series classification and the raw master itself.
"""

from __future__ import annotations

import sqlite3

OPTION_LISTING_SNAPSHOTS_SCHEMA = """
CREATE TABLE IF NOT EXISTS option_listing_snapshots (
    provider TEXT NOT NULL,
    snapshot_sha256 TEXT NOT NULL,
    source_url TEXT NOT NULL,
    first_fetched_at TEXT NOT NULL,
    record_count TEXT NOT NULL,
    option_record_count TEXT NOT NULL,
    PRIMARY KEY (provider, snapshot_sha256)
)
"""

OPTION_PROVIDER_LISTINGS_SCHEMA = """
CREATE TABLE IF NOT EXISTS option_provider_listings (
    provider TEXT NOT NULL,
    product_code TEXT NOT NULL,
    exchange_code TEXT NOT NULL,
    expiration_date TEXT NOT NULL,
    strike TEXT NOT NULL,
    option_right TEXT NOT NULL,
    instrument_key TEXT NOT NULL,
    exchange_lot_size TEXT NOT NULL,
    established_snapshot_sha256 TEXT NOT NULL,
    established_at TEXT NOT NULL,
    PRIMARY KEY (
        provider,
        product_code,
        exchange_code,
        expiration_date,
        strike,
        option_right
    ),
    UNIQUE (provider, instrument_key)
)
"""


def initialize_option_listing_schema(connection: sqlite3.Connection) -> None:
    """Create the option listing reference tables when they are absent."""
    connection.execute(OPTION_LISTING_SNAPSHOTS_SCHEMA)
    connection.execute(OPTION_PROVIDER_LISTINGS_SCHEMA)
    connection.commit()
