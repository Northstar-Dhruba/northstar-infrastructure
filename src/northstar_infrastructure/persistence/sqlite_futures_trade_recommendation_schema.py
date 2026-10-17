"""Schema for the local futures trade recommendation store.

Five tables, all insert-only:

- ``futures_recommendation_configurations`` registers each effective generation
  configuration once: an Application profile version, the F-4 policy version
  it names, its validity horizon and the canonical JSON of every F-4 policy
  parameter. ``(profile_version, policy_version)`` is unique so definitions can
  reference the pair, which makes the database itself refuse a definition whose
  policy version is not the one its profile was registered with.
- ``futures_trade_recommendations`` holds each immutable F-2 recommendation
  definition: its identity, the identity inputs as constrained columns, its
  issuance instant and the versioned canonical JSON definition. It has no
  status, cursor or terminality column and is never updated.
- ``futures_trade_recommendation_steps`` holds one row per F-2 lifecycle
  evaluation of a recommendation, numbered contiguously from 1:

  - an OBSERVATION step names the evaluated candle by its ``futures_ohlcv``
    natural key, the trading-session label it was matched to and its ``fdc1``
    fingerprint. It stores no price or volume: the candle stays in
    ``futures_ohlcv`` and is resolved and re-fingerprinted on every read.
  - a TIME_EXPIRY step names no candle; its TimeExpiryEvidence is in
    ``evaluation``.

  ``evaluation`` is versioned canonical JSON of the events the step produced
  (none for a quiet bar) and any time evidence. ``resulting_status`` and
  ``is_terminal`` are the F-2 status after the step, recorded for audit and
  lookup and re-derived from Core on every read; they are never written
  directly. A recommendation is open while it has no terminal step.

- ``futures_recommendation_generation_audits`` holds one row per audited
  generation attempt: its identity, the attempt's contract, strategy,
  configuration versions, attempt instant and session label as constrained
  columns, its generation status, block reason and outcome, and the versioned
  canonical JSON of the whole FuturesRecommendationGenerationAudit. It has no
  foreign key to the configuration registry: a blocked attempt's configuration
  may never have been registered, and its audit registers none.
- ``futures_recommendation_generation_audit_recommendations`` lists the
  recommendation identities an audit names as RECORDED or EXISTING, each with
  a foreign key to its definition, so the database refuses an audit that names
  an unrecorded recommendation. A partial unique index allows at most one
  RECORDED audit per recommendation. Proposed identities are not listed: a
  lost race proposes an identity this attempt never recorded.

A RECORDED audit is inserted in the same transaction as the definitions it
names (I-2); nothing here can express that, so the store enforces it.

The database itself refuses a step for an unrecorded recommendation, a
duplicated sequence number, a second evaluation of one candle, a second
terminal step and a step whose candle columns do not match its kind.

There is deliberately no foreign key to ``futures_ohlcv``: this schema does not
own that table, and SQLite refuses every insert into a child table whose parent
table is absent -- a time-expiry step that names no candle included. Instead the
store requires each candle inside the write transaction and every read requires
it again.
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

FUTURES_TRADE_RECOMMENDATION_STEP_SCHEMA = """
CREATE TABLE IF NOT EXISTS futures_trade_recommendation_steps (
    recommendation_identity TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK (typeof(sequence) = 'integer' AND sequence >= 1),
    step_kind TEXT NOT NULL CHECK (step_kind IN ('OBSERVATION', 'TIME_EXPIRY')),
    product_code TEXT,
    exchange_code TEXT,
    expiration_date TEXT,
    timeframe TEXT,
    point_in_time TEXT,
    trading_date TEXT,
    candle_fingerprint TEXT,
    evaluation TEXT NOT NULL,
    resulting_status TEXT NOT NULL,
    is_terminal INTEGER NOT NULL CHECK (typeof(is_terminal) = 'integer' AND is_terminal IN (0, 1)),
    PRIMARY KEY (recommendation_identity, sequence),
    FOREIGN KEY (recommendation_identity)
        REFERENCES futures_trade_recommendations (recommendation_identity),
    CHECK (
        (
            step_kind = 'OBSERVATION'
            AND product_code IS NOT NULL
            AND exchange_code IS NOT NULL
            AND expiration_date IS NOT NULL
            AND timeframe IS NOT NULL
            AND point_in_time IS NOT NULL
            AND trading_date IS NOT NULL
            AND candle_fingerprint IS NOT NULL
        )
        OR (
            step_kind = 'TIME_EXPIRY'
            AND product_code IS NULL
            AND exchange_code IS NULL
            AND expiration_date IS NULL
            AND timeframe IS NULL
            AND point_in_time IS NULL
            AND trading_date IS NULL
            AND candle_fingerprint IS NULL
        )
    )
)
"""

FUTURES_TRADE_RECOMMENDATION_STEP_OBSERVATION_INDEX = """
CREATE UNIQUE INDEX IF NOT EXISTS futures_trade_recommendation_steps_observation
ON futures_trade_recommendation_steps (recommendation_identity, point_in_time)
WHERE step_kind = 'OBSERVATION'
"""

FUTURES_TRADE_RECOMMENDATION_STEP_TERMINAL_INDEX = """
CREATE UNIQUE INDEX IF NOT EXISTS futures_trade_recommendation_steps_terminal
ON futures_trade_recommendation_steps (recommendation_identity)
WHERE is_terminal = 1
"""

FUTURES_TRADE_RECOMMENDATION_STEP_CANDLE_INDEX = """
CREATE INDEX IF NOT EXISTS futures_trade_recommendation_steps_candle
ON futures_trade_recommendation_steps (
    product_code, exchange_code, expiration_date, timeframe, point_in_time
)
"""


FUTURES_RECOMMENDATION_GENERATION_AUDIT_SCHEMA = """
CREATE TABLE IF NOT EXISTS futures_recommendation_generation_audits (
    audit_identity TEXT NOT NULL PRIMARY KEY,
    evidence_version INTEGER NOT NULL
        CHECK (typeof(evidence_version) = 'integer' AND evidence_version >= 1),
    product_code TEXT NOT NULL,
    exchange_code TEXT NOT NULL,
    expiration_date TEXT NOT NULL,
    strategy_identity TEXT NOT NULL,
    policy_version TEXT NOT NULL,
    profile_version TEXT NOT NULL,
    attempted_at TEXT NOT NULL,
    session_date TEXT,
    generation_status TEXT NOT NULL CHECK (generation_status IN ('COMPLETED', 'BLOCKED')),
    block_reason TEXT,
    outcome TEXT NOT NULL
        CHECK (outcome IN ('RECORDED', 'EXISTING_RETURNED', 'REJECTED', 'BLOCKED')),
    audit TEXT NOT NULL,
    CHECK ((generation_status = 'BLOCKED') = (block_reason IS NOT NULL)),
    CHECK ((generation_status = 'BLOCKED') = (outcome = 'BLOCKED'))
)
"""

FUTURES_RECOMMENDATION_GENERATION_AUDIT_PRODUCT_INDEX = """
CREATE INDEX IF NOT EXISTS futures_recommendation_generation_audits_product
ON futures_recommendation_generation_audits (product_code, exchange_code)
"""

FUTURES_RECOMMENDATION_GENERATION_AUDIT_REFERENCE_SCHEMA = """
CREATE TABLE IF NOT EXISTS futures_recommendation_generation_audit_recommendations (
    audit_identity TEXT NOT NULL,
    recommendation_identity TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('RECORDED', 'EXISTING')),
    PRIMARY KEY (audit_identity, recommendation_identity),
    FOREIGN KEY (audit_identity)
        REFERENCES futures_recommendation_generation_audits (audit_identity),
    FOREIGN KEY (recommendation_identity)
        REFERENCES futures_trade_recommendations (recommendation_identity)
)
"""

FUTURES_RECOMMENDATION_GENERATION_AUDIT_RECORDED_INDEX = """
CREATE UNIQUE INDEX IF NOT EXISTS futures_recommendation_generation_audit_recorded
ON futures_recommendation_generation_audit_recommendations (recommendation_identity)
WHERE role = 'RECORDED'
"""


def initialize_futures_trade_recommendation_schema(connection: sqlite3.Connection) -> None:
    """Create the futures trade recommendation schema when it is absent."""
    connection.execute(FUTURES_RECOMMENDATION_CONFIGURATION_SCHEMA)
    connection.execute(FUTURES_RECOMMENDATION_CONFIGURATION_POLICY_INDEX)
    connection.execute(FUTURES_TRADE_RECOMMENDATION_SCHEMA)
    connection.execute(FUTURES_TRADE_RECOMMENDATION_PRODUCT_INDEX)
    connection.execute(FUTURES_TRADE_RECOMMENDATION_STEP_SCHEMA)
    connection.execute(FUTURES_TRADE_RECOMMENDATION_STEP_OBSERVATION_INDEX)
    connection.execute(FUTURES_TRADE_RECOMMENDATION_STEP_TERMINAL_INDEX)
    connection.execute(FUTURES_TRADE_RECOMMENDATION_STEP_CANDLE_INDEX)
    connection.execute(FUTURES_RECOMMENDATION_GENERATION_AUDIT_SCHEMA)
    connection.execute(FUTURES_RECOMMENDATION_GENERATION_AUDIT_PRODUCT_INDEX)
    connection.execute(FUTURES_RECOMMENDATION_GENERATION_AUDIT_REFERENCE_SCHEMA)
    connection.execute(FUTURES_RECOMMENDATION_GENERATION_AUDIT_RECORDED_INDEX)
    connection.commit()
