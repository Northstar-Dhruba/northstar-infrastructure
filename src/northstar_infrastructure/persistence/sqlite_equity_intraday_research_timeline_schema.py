"""Schema for authoritative equity intraday research timelines (EI-3C-B).

Two insert-only tables, in the same SQLite file as the EI-2B evidence they
reference:

- ``equity_intraday_research_timelines`` holds one row per timeline: its
  strategy, listing and trading date, which are unique together; the
  configuration version; the exact canonical configuration document; and the
  exact canonical EI-3B timeline identity document. It has no head column:
  the head is the last committed entry.
- ``equity_intraday_research_timeline_entries`` holds one row per committed
  entry, under ``(timeline_id, sequence)``: its role and research outcome,
  the EI-2B snapshot it rests on (a real foreign key), the snapshot's
  manifest fingerprint, decision instant and watermark, the research window
  close, the exact canonical EI-3A decision document and ``eir1``
  fingerprint, and the previous and own ``eirt1`` entry fingerprints.

Decision instants
-----------------
``decision_instant`` is canonical UTC PointInTime text, kept exactly for
fingerprint verification. Canonical text with fractional seconds does not
sort chronologically -- "...:00.5Z" sorts before "...:00Z", because "."
precedes "Z", although it is the later instant -- so order is enforced on
``decision_instant_us``: integer microseconds since 1970-01-01T00:00:00Z.
PointInTime carries at most six fractional digits, so microseconds represent
every instant exactly. A trigger re-derives the integer from the text with
integer arithmetic only (``strftime('%s')`` whole seconds, plus the fraction
padded to six digits) and refuses any row where they differ, and refuses
non-canonical text: no "Z", more than six fractional digits, or a trailing
fractional zero.

Triggers make the database refuse, whatever connection writes:

- any UPDATE or DELETE of either table, and any INSERT that would replace a
  row (``INSERT OR REPLACE`` deletes without firing delete triggers unless
  recursive triggers are on, so a replacing insert is refused before it can);
- an entry whose sequence is not the timeline's next, whose previous
  fingerprint is not the previous entry's (NULL exactly at sequence 1),
  whose decision instant does not strictly follow the previous entry's, or
  whose watermark is below it;
- a second CANDIDATE, and a REPEAT_QUALIFYING before any CANDIDATE;
- an entry whose snapshot is not the timeline's: the referenced EI-2B
  snapshot must have the entry's manifest fingerprint, ``as_of``, watermark
  and research window close, the timeline's listing, trading date, timeframe
  and window policy, and SYNTHETIC evidence class;
- an entry whose decision document does not name its outcome, snapshot,
  manifest, instant, watermark, listing, date and the timeline's
  configuration.

Two invariants are not expressible in SQL here and are Python's: the
SHA-256 fingerprints, which the Application re-derives on every load
(EI3-D13), and EI3-D11's rule that an identity row exists only with at least
one entry. The adapter inserts the identity only in the transaction that
commits the first entry, and refuses to load an identity without entries.
"""

from __future__ import annotations

import sqlite3

EQUITY_INTRADAY_RESEARCH_TIMELINE_SCHEMA = """
CREATE TABLE IF NOT EXISTS equity_intraday_research_timelines (
    timeline_id INTEGER PRIMARY KEY AUTOINCREMENT CHECK (timeline_id >= 1),
    strategy TEXT NOT NULL CHECK (length(strategy) > 0),
    symbol TEXT NOT NULL CHECK (length(symbol) > 0),
    exchange_code TEXT NOT NULL CHECK (length(exchange_code) > 0),
    trading_date TEXT NOT NULL
        CHECK (trading_date GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'),
    configuration_version TEXT NOT NULL CHECK (configuration_version GLOB 'synthetic-?*'),
    configuration_document TEXT NOT NULL CHECK (json_valid(configuration_document)),
    identity_document TEXT NOT NULL CHECK (json_valid(identity_document)),
    UNIQUE (strategy, symbol, exchange_code, trading_date),
    CHECK (
        json_extract(configuration_document, '$.strategy') IS strategy
        AND json_extract(configuration_document, '$.version') IS configuration_version
        AND json_extract(identity_document, '$.strategy') IS strategy
        AND json_extract(identity_document, '$.trading_date') IS trading_date
        AND json_extract(identity_document, '$.listing.symbol') IS symbol
        AND json_extract(identity_document, '$.listing.exchange_code') IS exchange_code
        AND json(json_extract(identity_document, '$.configuration'))
            IS json(configuration_document)
    )
)
"""

EQUITY_INTRADAY_RESEARCH_TIMELINE_ENTRY_SCHEMA = """
CREATE TABLE IF NOT EXISTS equity_intraday_research_timeline_entries (
    timeline_id INTEGER NOT NULL,
    sequence INTEGER NOT NULL CHECK (typeof(sequence) = 'integer' AND sequence >= 1),
    role TEXT NOT NULL
        CHECK (role IN ('CANDIDATE', 'REPEAT_QUALIFYING', 'NO_SIGNAL', 'REJECTED')),
    decision_outcome TEXT NOT NULL
        CHECK (decision_outcome IN ('LONG_CANDIDATE', 'NO_SIGNAL', 'REJECTED')),
    snapshot_id TEXT NOT NULL,
    manifest_fingerprint TEXT NOT NULL,
    decision_instant TEXT NOT NULL,
    decision_instant_us INTEGER NOT NULL CHECK (typeof(decision_instant_us) = 'integer'),
    watermark INTEGER NOT NULL CHECK (typeof(watermark) = 'integer' AND watermark >= 0),
    research_window_closes_at TEXT NOT NULL,
    decision_document TEXT NOT NULL CHECK (json_valid(decision_document)),
    decision_fingerprint TEXT NOT NULL CHECK (decision_fingerprint GLOB 'eir1-sha256:?*'),
    previous_fingerprint TEXT CHECK (
        previous_fingerprint IS NULL OR previous_fingerprint GLOB 'eirt1-sha256:?*'
    ),
    entry_fingerprint TEXT NOT NULL UNIQUE CHECK (entry_fingerprint GLOB 'eirt1-sha256:?*'),
    PRIMARY KEY (timeline_id, sequence),
    UNIQUE (timeline_id, decision_instant),
    UNIQUE (timeline_id, decision_instant_us),
    FOREIGN KEY (timeline_id) REFERENCES equity_intraday_research_timelines (timeline_id),
    FOREIGN KEY (snapshot_id) REFERENCES equity_candle_evidence_snapshots (snapshot_id),
    CHECK ((sequence = 1) = (previous_fingerprint IS NULL)),
    CHECK (
        (role IN ('CANDIDATE', 'REPEAT_QUALIFYING') AND decision_outcome = 'LONG_CANDIDATE')
        OR (role = 'NO_SIGNAL' AND decision_outcome = 'NO_SIGNAL')
        OR (role = 'REJECTED' AND decision_outcome = 'REJECTED')
    )
)
"""

EQUITY_INTRADAY_RESEARCH_TIMELINE_CANDIDATE_INDEX = """
CREATE UNIQUE INDEX IF NOT EXISTS equity_intraday_research_timeline_one_candidate
ON equity_intraday_research_timeline_entries (timeline_id)
WHERE role = 'CANDIDATE'
"""

_TIMELINES = "equity_intraday_research_timelines"
_ENTRIES = "equity_intraday_research_timeline_entries"
_PREVIOUS = (
    f"(SELECT {{column}} FROM {_ENTRIES} AS p "
    "WHERE p.timeline_id = NEW.timeline_id AND p.sequence = NEW.sequence - 1)"
)
# Integer microseconds since the epoch, from canonical UTC text, without floats.
_INSTANT_US = (
    "(CAST(strftime('%s', substr(NEW.decision_instant, 1, 19)) AS INTEGER) * 1000000 + "
    "CASE WHEN length(NEW.decision_instant) > 20 THEN CAST(substr(substr("
    "NEW.decision_instant, 21, length(NEW.decision_instant) - 21) || '000000', 1, 6) "
    "AS INTEGER) ELSE 0 END)"
)
_CANONICAL_INSTANT = (
    "NEW.decision_instant GLOB "
    "'[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9]*Z' "
    "AND (length(NEW.decision_instant) = 20 OR ("
    "length(NEW.decision_instant) BETWEEN 22 AND 27 "
    "AND substr(NEW.decision_instant, 20, 1) = '.' "
    "AND substr(NEW.decision_instant, 21, length(NEW.decision_instant) - 21) "
    "NOT GLOB '*[^0-9]*' "
    "AND substr(NEW.decision_instant, -2, 1) <> '0'))"
)


def _refuse(name: str, event: str, table: str, message: str, when: str = "") -> str:
    condition = f"\n{when}" if when else ""
    return (
        f"CREATE TRIGGER IF NOT EXISTS {name}\n{event} ON {table}{condition}\n"
        f"BEGIN SELECT RAISE(ABORT, '{message}'); END"
    )


EQUITY_INTRADAY_RESEARCH_TIMELINE_TRIGGERS = (
    *(
        _refuse(f"{table}_no_{verb.lower()}", f"BEFORE {verb}", table, f"{table} is append-only")
        for table in (_TIMELINES, _ENTRIES)
        for verb in ("UPDATE", "DELETE")
    ),
    _refuse(
        f"{_TIMELINES}_no_replace",
        "BEFORE INSERT",
        _TIMELINES,
        f"{_TIMELINES} is append-only",
        f"WHEN EXISTS (SELECT 1 FROM {_TIMELINES} AS t WHERE t.timeline_id = NEW.timeline_id "
        "OR (t.strategy = NEW.strategy AND t.symbol = NEW.symbol "
        "AND t.exchange_code = NEW.exchange_code AND t.trading_date = NEW.trading_date))",
    ),
    _refuse(
        f"{_ENTRIES}_no_replace",
        "BEFORE INSERT",
        _ENTRIES,
        f"{_ENTRIES} is append-only",
        f"WHEN EXISTS (SELECT 1 FROM {_ENTRIES} AS e WHERE e.entry_fingerprint = "
        "NEW.entry_fingerprint OR (e.timeline_id = NEW.timeline_id AND (e.sequence = "
        "NEW.sequence OR e.decision_instant = NEW.decision_instant "
        "OR e.decision_instant_us = NEW.decision_instant_us)))",
    ),
    _refuse(
        f"{_ENTRIES}_next_sequence",
        "BEFORE INSERT",
        _ENTRIES,
        "a timeline entry must take the next sequence",
        f"WHEN NEW.sequence IS NOT (SELECT COALESCE(MAX(e.sequence), 0) + 1 FROM {_ENTRIES} "
        "AS e WHERE e.timeline_id = NEW.timeline_id)",
    ),
    _refuse(
        f"{_ENTRIES}_chained",
        "BEFORE INSERT",
        _ENTRIES,
        "a timeline entry must chain to the previous entry",
        f"WHEN NEW.previous_fingerprint IS NOT {_PREVIOUS.format(column='entry_fingerprint')}",
    ),
    _refuse(
        f"{_ENTRIES}_canonical_instant",
        "BEFORE INSERT",
        _ENTRIES,
        "a decision instant must be canonical UTC text with its exact microseconds",
        f"WHEN NOT ({_CANONICAL_INSTANT}) OR NEW.decision_instant_us IS NOT {_INSTANT_US}",
    ),
    _refuse(
        f"{_ENTRIES}_instant_increases",
        "BEFORE INSERT",
        _ENTRIES,
        "decision instants must strictly increase",
        f"WHEN NEW.decision_instant_us <= {_PREVIOUS.format(column='decision_instant_us')}",
    ),
    _refuse(
        f"{_ENTRIES}_watermark_never_regresses",
        "BEFORE INSERT",
        _ENTRIES,
        "journal watermarks must never regress",
        f"WHEN NEW.watermark < {_PREVIOUS.format(column='watermark')}",
    ),
    _refuse(
        f"{_ENTRIES}_one_candidate",
        "BEFORE INSERT",
        _ENTRIES,
        "a timeline has at most one CANDIDATE",
        f"WHEN NEW.role = 'CANDIDATE' AND EXISTS (SELECT 1 FROM {_ENTRIES} AS e "
        "WHERE e.timeline_id = NEW.timeline_id AND e.role = 'CANDIDATE')",
    ),
    _refuse(
        f"{_ENTRIES}_repeat_follows_candidate",
        "BEFORE INSERT",
        _ENTRIES,
        "REPEAT_QUALIFYING needs an earlier CANDIDATE",
        f"WHEN NEW.role = 'REPEAT_QUALIFYING' AND NOT EXISTS (SELECT 1 FROM {_ENTRIES} AS e "
        "WHERE e.timeline_id = NEW.timeline_id AND e.role = 'CANDIDATE')",
    ),
    _refuse(
        f"{_ENTRIES}_snapshot_matches",
        "BEFORE INSERT",
        _ENTRIES,
        "a timeline entry must rest on the matching synthetic snapshot of its timeline",
        f"WHEN NOT EXISTS (SELECT 1 FROM equity_candle_evidence_snapshots AS s "
        f"JOIN {_TIMELINES} AS t ON t.timeline_id = NEW.timeline_id "
        "WHERE s.snapshot_id = NEW.snapshot_id "
        "AND s.manifest_fingerprint = NEW.manifest_fingerprint "
        "AND s.as_of = NEW.decision_instant AND s.watermark = NEW.watermark "
        "AND s.evidence_class = 'SYNTHETIC' "
        "AND s.symbol = t.symbol AND s.exchange_code = t.exchange_code "
        "AND s.trading_date = t.trading_date "
        "AND s.timeframe IS json_extract(t.configuration_document, '$.timeframe') "
        "AND s.window_policy_version "
        "IS json_extract(t.configuration_document, '$.window_policy_version') "
        "AND json_extract(s.document, '$.manifest.window.closes_at') "
        "IS NEW.research_window_closes_at)",
    ),
    _refuse(
        f"{_ENTRIES}_decision_matches",
        "BEFORE INSERT",
        _ENTRIES,
        "a timeline entry decision document must describe the entry and its timeline",
        f"WHEN NOT EXISTS (SELECT 1 FROM {_TIMELINES} AS t WHERE t.timeline_id = "
        "NEW.timeline_id "
        "AND json_extract(NEW.decision_document, '$.outcome') IS NEW.decision_outcome "
        "AND json_extract(NEW.decision_document, '$.requested_snapshot_id') "
        "IS NEW.snapshot_id "
        "AND json_extract(NEW.decision_document, '$.evidence.snapshot_id') IS NEW.snapshot_id "
        "AND json_extract(NEW.decision_document, '$.evidence.manifest_fingerprint') "
        "IS NEW.manifest_fingerprint "
        "AND json_extract(NEW.decision_document, '$.evidence.as_of') IS NEW.decision_instant "
        "AND json_extract(NEW.decision_document, '$.evidence.watermark') IS NEW.watermark "
        "AND json_extract(NEW.decision_document, '$.listing.symbol') IS t.symbol "
        "AND json_extract(NEW.decision_document, '$.listing.exchange_code') "
        "IS t.exchange_code "
        "AND json_extract(NEW.decision_document, '$.trading_date') IS t.trading_date "
        "AND json(json_extract(NEW.decision_document, '$.configuration')) "
        "IS json(t.configuration_document))",
    ),
)


def initialize_equity_intraday_research_timeline_schema(connection: sqlite3.Connection) -> None:
    """Create the research timeline tables, index and triggers when they are absent.

    The EI-2B evidence schema must already exist in the same database: entries
    reference its snapshots.
    """
    for statement in (
        EQUITY_INTRADAY_RESEARCH_TIMELINE_SCHEMA,
        EQUITY_INTRADAY_RESEARCH_TIMELINE_ENTRY_SCHEMA,
        EQUITY_INTRADAY_RESEARCH_TIMELINE_CANDIDATE_INDEX,
        *EQUITY_INTRADAY_RESEARCH_TIMELINE_TRIGGERS,
    ):
        connection.execute(statement)
    connection.commit()
