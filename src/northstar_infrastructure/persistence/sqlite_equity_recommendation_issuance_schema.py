"""Schema for the durable MODELED recommendation issuance authority (EI-6A).

Three insert-only tables, under ADR-024 sections 7 and 13, the EI-5 design
section 8.2 and the EI-6A SQLite Storage Design v1.0 (sections 2 to 6), in the
SAME SQLite file as the EI-4D generation records they reference. The schema is
additive: it creates only its own tables, index and triggers, adds no trigger
to an EI-4D table, keeps no schema version, and refuses a database that does
not already hold the EI-4D generation tables -- it never creates an empty copy
of the records it references.

- ``equity_recommendation_issuance_ledger`` is the authority-wide recording
  sequence, shared by issuances and declarations: one row per sequence, with
  the kind of record it sequences. Sequences are dense, 1 to N.
- ``equity_recommendation_issuances`` holds one MODELED issued recommendation
  per ``eiris1`` identity, with the structured columns it is checked by. The
  full issued record is never stored: it is re-derived on load from the EI-4D
  disposition its ``generation_key`` references (no new document or format).
- ``equity_recommendation_issuance_declarations`` holds one completeness
  declaration per sequence: "exchange X, trading date D is complete through T".

Instants are kept as canonical UTC PointInTime text and as integer
microseconds since 1970-01-01T00:00:00Z. Canonical text does not sort
chronologically ("...:00.5Z" sorts before "...:00Z"), so every chronological
comparison here uses the integer, which a trigger re-derives from the text
with integer arithmetic only -- the EI-3C ``decision_instant_us`` rule.

What the database enforces, whatever connection writes
------------------------------------------------------
- Uniqueness: a sequence, an issuance identity, a generation key, a
  (strategy, listing, trading date) and a declared instant per session.
- The ledger: each new sequence is the current maximum + 1; the previous
  sequence must already have its record; a record's (sequence, kind) is a
  real composite foreign key to a ledger row of that kind, so a kind mismatch,
  a shared sequence or a missing ledger entry is refused.
- Provenance (defence in depth): an issuance's generation key is a real
  foreign key to its EI-4D disposition, and a trigger requires that
  disposition to be PROPOSED, evaluator ``/2``, with the issuance's proposal
  and assessment fingerprints, strategy, listing and trading date.
- No backdating: an issuance at or before the session's greatest declared
  instant is refused; a declaration not strictly above it is refused.
- Immutability: every UPDATE and DELETE, and every INSERT that would replace a
  row, is refused on all three tables (``INSERT OR REPLACE`` deletes without
  firing delete triggers unless recursive triggers are on, so a replacing
  insert is refused before it can).

What it cannot enforce, and the adapter does
--------------------------------------------
A foreign key proves that a referenced row exists; it cannot prove that every
committed ledger row has its record (a polymorphic reverse reference). That
is the adapter's: ledger entry and record are inserted in one transaction,
checked before the single COMMIT, and the whole ledger is checked, failing
closed, before every read that establishes a watermark and every write
(EI-6A design, section 4.3). The SHA-256 identity, the re-derivation of the
issued record and whether the disposition is authoritative are the
Application's; a foreign key never proves authority.
"""

from __future__ import annotations

import sqlite3

from northstar_application.application_services import EQUITY_RECOMMENDATION_EVALUATOR_V2

from northstar_infrastructure.persistence.sqlite_equity_recommendation_generation_schema import (  # noqa: E501
    ATTEMPTS,
    DISPOSITIONS,
)

LEDGER = "equity_recommendation_issuance_ledger"
ISSUANCES = "equity_recommendation_issuances"
DECLARATIONS = "equity_recommendation_issuance_declarations"
EQUITY_RECOMMENDATION_ISSUANCE_TABLES = (LEDGER, ISSUANCES, DECLARATIONS)
REQUIRED_GENERATION_TABLES = (DISPOSITIONS, ATTEMPTS)

ISSUANCE_KIND = "ISSUANCE"
DECLARATION_KIND = "DECLARATION"

_DATE = "GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'"
_SEQUENCE = "typeof({column}) = 'integer' AND {column} >= 1"


def _instant_us(column: str) -> str:
    """Integer microseconds since the epoch from NEW.<column>'s canonical text, without floats."""
    text = f"NEW.{column}"
    return (
        f"(CAST(strftime('%s', substr({text}, 1, 19)) AS INTEGER) * 1000000 + "
        f"CASE WHEN length({text}) > 20 THEN CAST(substr(substr("
        f"{text}, 21, length({text}) - 21) || '000000', 1, 6) AS INTEGER) ELSE 0 END)"
    )


def _canonical(column: str) -> str:
    """NEW.<column> is canonical UTC text: 'Z', at most six fractional digits, no trailing 0."""
    text = f"NEW.{column}"
    return (
        f"typeof({text}) = 'text' AND {text} GLOB "
        "'[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9]*Z' "
        f"AND (length({text}) = 20 OR (length({text}) BETWEEN 22 AND 27 "
        f"AND substr({text}, 20, 1) = '.' "
        f"AND substr({text}, 21, length({text}) - 21) NOT GLOB '*[^0-9]*' "
        f"AND substr({text}, -2, 1) <> '0'))"
    )


EQUITY_RECOMMENDATION_ISSUANCE_LEDGER_SCHEMA = f"""
CREATE TABLE IF NOT EXISTS {LEDGER} (
    recording_sequence INTEGER NOT NULL PRIMARY KEY
        CHECK ({_SEQUENCE.format(column="recording_sequence")}),
    record_kind TEXT NOT NULL CHECK (record_kind IN ('{ISSUANCE_KIND}', '{DECLARATION_KIND}')),
    UNIQUE (recording_sequence, record_kind)
)
"""

EQUITY_RECOMMENDATION_ISSUANCE_SCHEMA = f"""
CREATE TABLE IF NOT EXISTS {ISSUANCES} (
    issuance_id TEXT NOT NULL PRIMARY KEY CHECK (issuance_id GLOB 'eiris1-sha256:?*'),
    generation_key TEXT NOT NULL UNIQUE CHECK (generation_key GLOB 'eirk1-sha256:?*'),
    proposal_fingerprint TEXT NOT NULL CHECK (proposal_fingerprint GLOB 'eirp1-sha256:?*'),
    assessment_fingerprint TEXT NOT NULL CHECK (assessment_fingerprint GLOB 'eira1-sha256:?*'),
    issuance_kind TEXT NOT NULL CHECK (issuance_kind = 'MODELED'),
    strategy TEXT NOT NULL CHECK (length(strategy) > 0),
    symbol TEXT NOT NULL CHECK (length(symbol) > 0),
    exchange_code TEXT NOT NULL CHECK (length(exchange_code) > 0),
    trading_date TEXT NOT NULL CHECK (trading_date {_DATE}),
    issued_at TEXT NOT NULL CHECK (typeof(issued_at) = 'text'),
    issued_at_us INTEGER NOT NULL CHECK (typeof(issued_at_us) = 'integer'),
    recording_sequence INTEGER NOT NULL UNIQUE
        CHECK ({_SEQUENCE.format(column="recording_sequence")}),
    record_kind TEXT NOT NULL DEFAULT '{ISSUANCE_KIND}' CHECK (record_kind = '{ISSUANCE_KIND}'),
    UNIQUE (strategy, symbol, exchange_code, trading_date),
    FOREIGN KEY (generation_key) REFERENCES {DISPOSITIONS} (generation_key),
    FOREIGN KEY (recording_sequence, record_kind)
        REFERENCES {LEDGER} (recording_sequence, record_kind)
)
"""

EQUITY_RECOMMENDATION_ISSUANCE_DECLARATION_SCHEMA = f"""
CREATE TABLE IF NOT EXISTS {DECLARATIONS} (
    recording_sequence INTEGER NOT NULL PRIMARY KEY
        CHECK ({_SEQUENCE.format(column="recording_sequence")}),
    record_kind TEXT NOT NULL DEFAULT '{DECLARATION_KIND}'
        CHECK (record_kind = '{DECLARATION_KIND}'),
    exchange_code TEXT NOT NULL CHECK (length(exchange_code) > 0),
    trading_date TEXT NOT NULL CHECK (trading_date {_DATE}),
    complete_through TEXT NOT NULL CHECK (typeof(complete_through) = 'text'),
    complete_through_us INTEGER NOT NULL CHECK (typeof(complete_through_us) = 'integer'),
    UNIQUE (exchange_code, trading_date, complete_through_us),
    FOREIGN KEY (recording_sequence, record_kind)
        REFERENCES {LEDGER} (recording_sequence, record_kind)
)
"""

# The declarations' session lookups use the UNIQUE (exchange_code, trading_date,
# complete_through_us) index; the issuances need their own session index.
EQUITY_RECOMMENDATION_ISSUANCE_SESSION_INDEX = f"""
CREATE INDEX IF NOT EXISTS {ISSUANCES}_by_session
ON {ISSUANCES} (exchange_code, trading_date)
"""


def _refuse(name: str, event: str, table: str, message: str, when: str = "") -> str:
    condition = f"\n{when}" if when else ""
    return (
        f"CREATE TRIGGER IF NOT EXISTS {name}\n{event} ON {table}{condition}\n"
        f"BEGIN SELECT RAISE(ABORT, '{message}'); END"
    )


def _has_record(sequence: str) -> str:
    """SQL: the ledger row at ``sequence`` has exactly its own kind of record."""
    return (
        f"EXISTS (SELECT 1 FROM {LEDGER} AS l WHERE l.recording_sequence = {sequence} AND ("
        f"(l.record_kind = '{ISSUANCE_KIND}' AND EXISTS (SELECT 1 FROM {ISSUANCES} AS i "
        "WHERE i.recording_sequence = l.recording_sequence)) OR "
        f"(l.record_kind = '{DECLARATION_KIND}' AND EXISTS (SELECT 1 FROM {DECLARATIONS} AS d "
        "WHERE d.recording_sequence = l.recording_sequence))))"
    )


def _greatest_declared(exchange: str, day: str) -> str:
    return (
        f"(SELECT MAX(d.complete_through_us) FROM {DECLARATIONS} AS d "
        f"WHERE d.exchange_code = {exchange} AND d.trading_date = {day})"
    )


EQUITY_RECOMMENDATION_ISSUANCE_TRIGGERS = (
    *(
        _refuse(f"{table}_no_{verb.lower()}", f"BEFORE {verb}", table, f"{table} is insert-only")
        for table in EQUITY_RECOMMENDATION_ISSUANCE_TABLES
        for verb in ("UPDATE", "DELETE")
    ),
    _refuse(
        f"{LEDGER}_no_replace",
        "BEFORE INSERT",
        LEDGER,
        f"{LEDGER} is insert-only",
        f"WHEN EXISTS (SELECT 1 FROM {LEDGER} AS l "
        "WHERE l.recording_sequence = NEW.recording_sequence)",
    ),
    _refuse(
        f"{LEDGER}_next_sequence",
        "BEFORE INSERT",
        LEDGER,
        "a recording sequence must be the next one",
        f"WHEN NEW.recording_sequence IS NOT (SELECT COALESCE(MAX(l.recording_sequence), 0) + 1 "
        f"FROM {LEDGER} AS l)",
    ),
    _refuse(
        f"{LEDGER}_previous_recorded",
        "BEFORE INSERT",
        LEDGER,
        "the previous recording sequence has no record",
        f"WHEN NEW.recording_sequence > 1 AND NOT {_has_record('NEW.recording_sequence - 1')}",
    ),
    _refuse(
        f"{ISSUANCES}_no_replace",
        "BEFORE INSERT",
        ISSUANCES,
        f"{ISSUANCES} is insert-only",
        f"WHEN EXISTS (SELECT 1 FROM {ISSUANCES} AS i WHERE i.issuance_id = NEW.issuance_id "
        "OR i.generation_key = NEW.generation_key "
        "OR i.recording_sequence = NEW.recording_sequence "
        "OR (i.strategy = NEW.strategy AND i.symbol = NEW.symbol "
        "AND i.exchange_code = NEW.exchange_code AND i.trading_date = NEW.trading_date))",
    ),
    _refuse(
        f"{ISSUANCES}_canonical_instant",
        "BEFORE INSERT",
        ISSUANCES,
        "an issued instant must be canonical UTC text with its exact microseconds",
        f"WHEN NOT ({_canonical('issued_at')}) OR NEW.issued_at_us IS NOT "
        f"{_instant_us('issued_at')}",
    ),
    _refuse(
        f"{ISSUANCES}_source_matches",
        "BEFORE INSERT",
        ISSUANCES,
        "an issuance must rest on the PROPOSED evaluator-2 disposition it names",
        f"WHEN NOT EXISTS (SELECT 1 FROM {DISPOSITIONS} AS d "
        "WHERE d.generation_key = NEW.generation_key "
        "AND d.disposition_kind = 'PROPOSED' "
        f"AND d.evaluator_version = '{EQUITY_RECOMMENDATION_EVALUATOR_V2}' "
        "AND d.proposal_fingerprint = NEW.proposal_fingerprint "
        "AND d.assessment_fingerprint = NEW.assessment_fingerprint "
        "AND d.strategy = NEW.strategy AND d.symbol = NEW.symbol "
        "AND d.exchange_code = NEW.exchange_code AND d.trading_date = NEW.trading_date)",
    ),
    _refuse(
        f"{ISSUANCES}_not_backdated",
        "BEFORE INSERT",
        ISSUANCES,
        "an issuance must follow the declared completeness of its session",
        f"WHEN NEW.issued_at_us <= {_greatest_declared('NEW.exchange_code', 'NEW.trading_date')}",
    ),
    _refuse(
        f"{DECLARATIONS}_no_replace",
        "BEFORE INSERT",
        DECLARATIONS,
        f"{DECLARATIONS} is insert-only",
        f"WHEN EXISTS (SELECT 1 FROM {DECLARATIONS} AS d "
        "WHERE d.recording_sequence = NEW.recording_sequence "
        "OR (d.exchange_code = NEW.exchange_code AND d.trading_date = NEW.trading_date "
        "AND d.complete_through_us = NEW.complete_through_us))",
    ),
    _refuse(
        f"{DECLARATIONS}_canonical_instant",
        "BEFORE INSERT",
        DECLARATIONS,
        "a completeness instant must be canonical UTC text with its exact microseconds",
        f"WHEN NOT ({_canonical('complete_through')}) OR NEW.complete_through_us IS NOT "
        f"{_instant_us('complete_through')}",
    ),
    _refuse(
        f"{DECLARATIONS}_monotone",
        "BEFORE INSERT",
        DECLARATIONS,
        "a declaration must exceed the declared completeness of its session",
        "WHEN NEW.complete_through_us <= "
        f"{_greatest_declared('NEW.exchange_code', 'NEW.trading_date')}",
    ),
)


def _require_generation_tables(connection: sqlite3.Connection) -> None:
    present = {
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name IN (?, ?)",
            REQUIRED_GENERATION_TABLES,
        )
    }
    missing = [name for name in REQUIRED_GENERATION_TABLES if name not in present]
    if missing:
        raise sqlite3.OperationalError(
            "This database holds no EI-4D generation records ("
            + ", ".join(missing)
            + " absent); the issuance authority is kept only beside the dispositions it "
            "references, and no generation table is created here."
        )


def initialize_equity_recommendation_issuance_schema(connection: sqlite3.Connection) -> None:
    """Create the issuance-authority tables, index and triggers when they are absent.

    The EI-4D generation tables must already exist in the same database;
    otherwise sqlite3.OperationalError is raised and nothing is created.
    Existing rows, tables, indexes and triggers are never altered.
    """
    _require_generation_tables(connection)
    for statement in (
        EQUITY_RECOMMENDATION_ISSUANCE_LEDGER_SCHEMA,
        EQUITY_RECOMMENDATION_ISSUANCE_SCHEMA,
        EQUITY_RECOMMENDATION_ISSUANCE_DECLARATION_SCHEMA,
        EQUITY_RECOMMENDATION_ISSUANCE_SESSION_INDEX,
        *EQUITY_RECOMMENDATION_ISSUANCE_TRIGGERS,
    ):
        connection.execute(statement)
    connection.commit()
