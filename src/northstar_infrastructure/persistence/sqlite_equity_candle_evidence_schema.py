"""Schema for the append-only equity candle evidence journal and its snapshots (EI-2B).

Three tables, all insert-only. None of them is the generic daily
``historical_ohlcv`` table, which is untouched and keeps its own upsert path.

- ``equity_candle_journal`` holds one row per journaled record -- a candle
  observation or a finality statement -- under an AUTOINCREMENT ``sequence``,
  so a sequence is never reused and is the only record of insertion order. The
  identity, receipt and placement facts are constrained columns; prices and
  volume are only in ``record``, the versioned canonical JSON, as exact decimal
  text. ``ingestion_id`` is unique.
- ``equity_candle_evidence_snapshots`` holds one row per issued snapshot: its
  identity, manifest fingerprint, scope, instants, watermark, coverage, how
  many journal entries its manifest lists, and the canonical JSON document.
- ``equity_candle_evidence_snapshot_entries`` lists each journal entry a
  snapshot names, with the record fingerprint it was issued with, under a
  foreign key to the journal.

Triggers make the database refuse, whatever connection writes:

- any UPDATE or DELETE of any of the three tables, and an INSERT that would
  replace an existing row (``INSERT OR REPLACE`` deletes without firing delete
  triggers unless recursive triggers are on, so it is refused before it can);
- a journal row inserted below the highest sequence;
- an observation whose source identity is already bound to another listing,
  timeframe, interval end, correction reference, publication time or synthetic
  classification, and a finality statement that differs from its identity's
  earlier statements or classification;
- a snapshot whose watermark is beyond the journal;
- a snapshot entry that is not journaled at or below the snapshot's watermark
  with the same record fingerprint, and any entry beyond the manifest's count,
  so a manifest cannot grow after issuance.

Instants are canonical UTC PointInTime text. They are compared in Python, not
SQL, because canonical text with fractional seconds does not sort
chronologically.
"""

from __future__ import annotations

import sqlite3

EQUITY_CANDLE_JOURNAL_SCHEMA = """
CREATE TABLE IF NOT EXISTS equity_candle_journal (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT CHECK (sequence >= 1),
    entry_kind TEXT NOT NULL CHECK (entry_kind IN ('OBSERVATION', 'FINALITY')),
    ingestion_id TEXT NOT NULL UNIQUE,
    journaled_at TEXT NOT NULL,
    source_id TEXT NOT NULL,
    source_event_id TEXT NOT NULL,
    source_revision TEXT NOT NULL,
    received_at TEXT NOT NULL,
    is_synthetic INTEGER NOT NULL
        CHECK (typeof(is_synthetic) = 'integer' AND is_synthetic IN (0, 1)),
    symbol TEXT,
    exchange_code TEXT,
    timeframe TEXT,
    interval_end TEXT,
    corrects_revision TEXT,
    source_published_at TEXT,
    confirmed_at TEXT,
    payload_fingerprint TEXT NOT NULL,
    record_fingerprint TEXT NOT NULL,
    record TEXT NOT NULL,
    CHECK (
        (
            entry_kind = 'OBSERVATION'
            AND symbol IS NOT NULL
            AND exchange_code IS NOT NULL
            AND timeframe IS NOT NULL
            AND interval_end IS NOT NULL
            AND confirmed_at IS NULL
        )
        OR (
            entry_kind = 'FINALITY'
            AND symbol IS NULL
            AND exchange_code IS NULL
            AND timeframe IS NULL
            AND interval_end IS NULL
            AND corrects_revision IS NULL
            AND source_published_at IS NULL
            AND confirmed_at IS NOT NULL
        )
    )
)
"""

EQUITY_CANDLE_JOURNAL_WINDOW_INDEX = """
CREATE INDEX IF NOT EXISTS equity_candle_journal_window
ON equity_candle_journal (symbol, exchange_code, timeframe, sequence)
WHERE entry_kind = 'OBSERVATION'
"""

EQUITY_CANDLE_JOURNAL_IDENTITY_INDEX = """
CREATE INDEX IF NOT EXISTS equity_candle_journal_identity
ON equity_candle_journal (source_id, source_event_id, source_revision, sequence)
"""

EQUITY_CANDLE_EVIDENCE_SNAPSHOT_SCHEMA = """
CREATE TABLE IF NOT EXISTS equity_candle_evidence_snapshots (
    snapshot_id TEXT NOT NULL PRIMARY KEY,
    manifest_fingerprint TEXT NOT NULL,
    symbol TEXT NOT NULL,
    exchange_code TEXT NOT NULL,
    trading_date TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    window_policy_version TEXT NOT NULL,
    finality_policy_version TEXT NOT NULL,
    as_of TEXT NOT NULL,
    issued_at TEXT NOT NULL,
    watermark INTEGER NOT NULL CHECK (typeof(watermark) = 'integer' AND watermark >= 0),
    evidence_class TEXT NOT NULL CHECK (evidence_class IN ('SYNTHETIC', 'PRODUCTION')),
    coverage TEXT NOT NULL CHECK (
        coverage IN ('NO_INTERVALS_ENDED', 'INCOMPLETE_AS_OF', 'COMPLETE_AS_OF', 'WINDOW_COMPLETE')
    ),
    entry_count INTEGER NOT NULL CHECK (typeof(entry_count) = 'integer' AND entry_count >= 0),
    document TEXT NOT NULL
)
"""

EQUITY_CANDLE_EVIDENCE_SNAPSHOT_SCOPE_INDEX = """
CREATE INDEX IF NOT EXISTS equity_candle_evidence_snapshots_scope
ON equity_candle_evidence_snapshots (symbol, exchange_code, trading_date)
"""

EQUITY_CANDLE_EVIDENCE_SNAPSHOT_ENTRY_SCHEMA = """
CREATE TABLE IF NOT EXISTS equity_candle_evidence_snapshot_entries (
    snapshot_id TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    record_fingerprint TEXT NOT NULL,
    PRIMARY KEY (snapshot_id, sequence),
    FOREIGN KEY (snapshot_id) REFERENCES equity_candle_evidence_snapshots (snapshot_id),
    FOREIGN KEY (sequence) REFERENCES equity_candle_journal (sequence)
)
"""


def _refuse(name: str, event: str, table: str, message: str, when: str = "") -> str:
    condition = f"\n{when}" if when else ""
    return (
        f"CREATE TRIGGER IF NOT EXISTS {name}\n{event} ON {table}{condition}\n"
        f"BEGIN SELECT RAISE(ABORT, '{message}'); END"
    )


_SAME_IDENTITY = (
    "j.source_id = NEW.source_id AND j.source_event_id = NEW.source_event_id "
    "AND j.source_revision = NEW.source_revision"
)

EQUITY_CANDLE_EVIDENCE_TRIGGERS = (
    *(
        _refuse(f"{table}_no_{verb.lower()}", f"BEFORE {verb}", table, f"{table} is append-only")
        for table in (
            "equity_candle_journal",
            "equity_candle_evidence_snapshots",
            "equity_candle_evidence_snapshot_entries",
        )
        for verb in ("UPDATE", "DELETE")
    ),
    _refuse(
        "equity_candle_journal_no_replace",
        "BEFORE INSERT",
        "equity_candle_journal",
        "equity_candle_journal is append-only",
        "WHEN EXISTS (SELECT 1 FROM equity_candle_journal AS j "
        "WHERE j.sequence = NEW.sequence OR j.ingestion_id = NEW.ingestion_id)",
    ),
    _refuse(
        "equity_candle_journal_in_order",
        "AFTER INSERT",
        "equity_candle_journal",
        "equity_candle_journal sequences only increase",
        "WHEN NEW.sequence < (SELECT MAX(sequence) FROM equity_candle_journal)",
    ),
    _refuse(
        "equity_candle_journal_observation_identity",
        "BEFORE INSERT",
        "equity_candle_journal",
        "source identity is bound to other facts",
        "WHEN NEW.entry_kind = 'OBSERVATION' AND EXISTS (SELECT 1 FROM equity_candle_journal "
        f"AS j WHERE {_SAME_IDENTITY} AND (j.is_synthetic <> NEW.is_synthetic OR "
        "(j.entry_kind = 'OBSERVATION' AND (j.symbol IS NOT NEW.symbol "
        "OR j.exchange_code IS NOT NEW.exchange_code OR j.timeframe IS NOT NEW.timeframe "
        "OR j.interval_end IS NOT NEW.interval_end "
        "OR j.corrects_revision IS NOT NEW.corrects_revision "
        "OR j.source_published_at IS NOT NEW.source_published_at))))",
    ),
    _refuse(
        "equity_candle_journal_finality_identity",
        "BEFORE INSERT",
        "equity_candle_journal",
        "source identity is bound to other facts",
        "WHEN NEW.entry_kind = 'FINALITY' AND EXISTS (SELECT 1 FROM equity_candle_journal "
        f"AS j WHERE {_SAME_IDENTITY} AND (j.is_synthetic <> NEW.is_synthetic OR "
        "(j.entry_kind = 'FINALITY' AND j.payload_fingerprint <> NEW.payload_fingerprint)))",
    ),
    _refuse(
        "equity_candle_evidence_snapshots_no_replace",
        "BEFORE INSERT",
        "equity_candle_evidence_snapshots",
        "equity_candle_evidence_snapshots is append-only",
        "WHEN EXISTS (SELECT 1 FROM equity_candle_evidence_snapshots AS s "
        "WHERE s.snapshot_id = NEW.snapshot_id)",
    ),
    _refuse(
        "equity_candle_evidence_snapshots_watermark",
        "BEFORE INSERT",
        "equity_candle_evidence_snapshots",
        "snapshot watermark is beyond the journal",
        "WHEN NEW.watermark > (SELECT COALESCE(MAX(sequence), 0) FROM equity_candle_journal)",
    ),
    _refuse(
        "equity_candle_evidence_snapshot_entries_no_replace",
        "BEFORE INSERT",
        "equity_candle_evidence_snapshot_entries",
        "equity_candle_evidence_snapshot_entries is append-only",
        "WHEN EXISTS (SELECT 1 FROM equity_candle_evidence_snapshot_entries AS e "
        "WHERE e.snapshot_id = NEW.snapshot_id AND e.sequence = NEW.sequence)",
    ),
    _refuse(
        "equity_candle_evidence_snapshot_entries_journaled",
        "BEFORE INSERT",
        "equity_candle_evidence_snapshot_entries",
        "snapshot entry is not journaled at or below its watermark with this fingerprint",
        "WHEN NOT EXISTS (SELECT 1 FROM equity_candle_journal AS j "
        "JOIN equity_candle_evidence_snapshots AS s ON s.snapshot_id = NEW.snapshot_id "
        "WHERE j.sequence = NEW.sequence AND j.sequence <= s.watermark "
        "AND j.record_fingerprint = NEW.record_fingerprint)",
    ),
    _refuse(
        "equity_candle_evidence_snapshot_entries_closed",
        "BEFORE INSERT",
        "equity_candle_evidence_snapshot_entries",
        "snapshot manifest is closed",
        "WHEN (SELECT COUNT(*) FROM equity_candle_evidence_snapshot_entries AS e "
        "WHERE e.snapshot_id = NEW.snapshot_id) >= (SELECT s.entry_count FROM "
        "equity_candle_evidence_snapshots AS s WHERE s.snapshot_id = NEW.snapshot_id)",
    ),
)


def initialize_equity_candle_evidence_schema(connection: sqlite3.Connection) -> None:
    """Create the equity candle evidence schema and its triggers when they are absent."""
    for statement in (
        EQUITY_CANDLE_JOURNAL_SCHEMA,
        EQUITY_CANDLE_JOURNAL_WINDOW_INDEX,
        EQUITY_CANDLE_JOURNAL_IDENTITY_INDEX,
        EQUITY_CANDLE_EVIDENCE_SNAPSHOT_SCHEMA,
        EQUITY_CANDLE_EVIDENCE_SNAPSHOT_SCOPE_INDEX,
        EQUITY_CANDLE_EVIDENCE_SNAPSHOT_ENTRY_SCHEMA,
        *EQUITY_CANDLE_EVIDENCE_TRIGGERS,
    ):
        connection.execute(statement)
    connection.commit()
