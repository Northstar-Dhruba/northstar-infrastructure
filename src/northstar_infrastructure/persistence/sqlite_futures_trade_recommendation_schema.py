"""Schema for the local futures trade recommendation definition store.

Two tables, both insert-only:

- ``futures_recommendation_configurations`` registers each effective generation
  configuration once: an Application profile version, the F-4 policy version
  it names, its validity horizon and the canonical JSON of every F-4 policy
  parameter. ``(profile_version, policy_version)`` is unique so definitions can
  reference the pair, which makes the database itself refuse a definition whose
  policy version is not the one its profile was registered with.
- ``futures_trade_recommendations`` holds each immutable F-2 recommendation
  definition: its identity, the identity inputs as constrained columns, its
  issuance instant and the versioned canonical JSON definition.

There is no lifecycle, step, cursor or audit table here, and no terminality
flag: whether a recommendation is open is derived from its F-2 lifecycle, which
F-5B-2 will persist as insert-only steps. Until then every stored definition is
open.
"""

from __future__ import annotations

import sqlite3

FUTURES_RECOMMENDATION_CONFIGURATION_SCHEMA = """
CREATE TABLE IF NOT EXISTS futures_recommendation_configurations (
    profile_version TEXT NOT NULL PRIMARY KEY,
    policy_version TEXT NOT NULL,
    validity_sessions TEXT NOT NULL,
    policy_parameters TEXT NOT NULL,
    UNIQUE (profile_version, policy_version)
)
"""

FUTURES_RECOMMENDATION_CONFIGURATION_POLICY_INDEX = """
CREATE INDEX IF NOT EXISTS futures_recommendation_configurations_policy_version
ON futures_recommendation_configurations (policy_version)
"""

FUTURES_TRADE_RECOMMENDATION_SCHEMA = """
CREATE TABLE IF NOT EXISTS futures_trade_recommendations (
    recommendation_identity TEXT NOT NULL PRIMARY KEY,
    product_code TEXT NOT NULL,
    exchange_code TEXT NOT NULL,
    expiration_date TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    decision_instant TEXT NOT NULL,
    strategy_identity TEXT NOT NULL,
    policy_version TEXT NOT NULL,
    profile_version TEXT NOT NULL,
    issued_at TEXT NOT NULL,
    definition TEXT NOT NULL,
    UNIQUE (
        product_code,
        exchange_code,
        expiration_date,
        timeframe,
        decision_instant,
        strategy_identity,
        policy_version,
        profile_version
    ),
    FOREIGN KEY (profile_version, policy_version)
        REFERENCES futures_recommendation_configurations (profile_version, policy_version)
)
"""

FUTURES_TRADE_RECOMMENDATION_PRODUCT_INDEX = """
CREATE INDEX IF NOT EXISTS futures_trade_recommendations_product
ON futures_trade_recommendations (product_code, exchange_code)
"""


def initialize_futures_trade_recommendation_schema(connection: sqlite3.Connection) -> None:
    """Create the futures trade recommendation definition schema when it is absent."""
    connection.execute(FUTURES_RECOMMENDATION_CONFIGURATION_SCHEMA)
    connection.execute(FUTURES_RECOMMENDATION_CONFIGURATION_POLICY_INDEX)
    connection.execute(FUTURES_TRADE_RECOMMENDATION_SCHEMA)
    connection.execute(FUTURES_TRADE_RECOMMENDATION_PRODUCT_INDEX)
    connection.commit()
