"""Schema for durable equity recommendation lifecycle events (EI-6B, D6B-6).

One insert-only table, under ADR-024 sections 8 to 13 and the EI-6B Durable
Recommendation Lifecycle Persistence Design v1.0 (section 8), in the SAME
SQLite file as the EI-4D generation records and the EI-6A issuance authority
it references. The schema is additive: it creates only its own table and
triggers, adds nothing to an EI-4D or EI-6A object, keeps no schema version,
and refuses a database that does not already hold the EI-6A issuance tables --
it never creates an empty copy of the authority it references.

``equity_recommendation_lifecycle_events`` holds one Core lifecycle event per
row: the parent issuance, the per-issuance event sequence, the structured
columns the database checks it by, and the exact canonical event document
(``northstar.equity-recommendation-lifecycle-event/1``, Application-owned).
The document is the event; the columns restate parts of it so the database
can guard them, and a CHECK ties every one of them to the document.

Instants are canonical UTC PointInTime text; ``evaluated_at_us`` is integer
microseconds since 1970-01-01T00:00:00Z, re-derived by a trigger from the text
with integer arithmetic only (the EI-3C and EI-6A rule), so chronology never
compares text or floats.

What the database enforces, whatever connection writes
------------------------------------------------------
- Provenance (defence in depth): ``issuance_id`` is a real foreign key to the
  EI-6A issuance. A foreign key proves a row exists, never that it is
  authoritative: the adapter re-derives the parent on every load and append.
- Shape: the format; the 11 kinds and 11 states; a state changes exactly when
  the kind is not ``SOURCE_REVISION_RECORDED``; a valid JSON object document
  whose format, sequence, kind, states, evaluated instant and evidence
  reference and interval equal the columns.
- Per-issuance continuity: a new sequence is the issuance's maximum + 1.
  Lifecycle sequences are per issuance; the EI-6A recording-sequence ledger is
  not used and never changed.
- Chronology: canonical instants with their exact microseconds; an event is
  not earlier than the issuance's latest event, nor than the parent's
  ``issued_at_us``.
- Terminal guard: no state-changing kind after the issuance's latest
  state-changing event entered a terminal state. ``SOURCE_REVISION_RECORDED``
  is not refused here: whether Core permits it is Core's.
- Immutability: every UPDATE and DELETE, and every INSERT that would replace a
  row, is refused (``INSERT OR REPLACE`` deletes without firing delete
  triggers unless recursive triggers are on, so a replacing insert is refused
  before it can).

What it does not check
----------------------
Event shapes beyond the columns, the window, the cursor, interval ranges and
revision citations are Core replay's. SQLite holds no authoritative lifecycle
state and does not claim to verify Core's contract.
"""

from __future__ import annotations

import sqlite3

from northstar_application.application_services import (
    EQUITY_RECOMMENDATION_LIFECYCLE_EVENT_FORMAT,
)
from northstar_core.recommendations.equity.lifecycle import (
    EquityLifecycleEventKind,
    EquityLifecycleState,
)

from northstar_infrastructure.persistence.sqlite_equity_recommendation_issuance_schema import (
    EQUITY_RECOMMENDATION_ISSUANCE_TABLES,
    ISSUANCES,
    _canonical,  # the EI-6A canonical-instant rule, reused unchanged
    _instant_us,  # the EI-6A integer-microsecond derivation, reused unchanged
)

LIFECYCLE_EVENTS = "equity_recommendation_lifecycle_events"
REQUIRED_ISSUANCE_TABLES = EQUITY_RECOMMENDATION_ISSUANCE_TABLES

_REVISION = EquityLifecycleEventKind.SOURCE_REVISION_RECORDED.value


def _quoted(values: object) -> str:
    return ", ".join(f"'{value}'" for value in values)  # type: ignore[attr-defined]


_KINDS = _quoted(kind.value for kind in EquityLifecycleEventKind)
_STATES = _quoted(state.value for state in EquityLifecycleState)
_TERMINAL_STATES = _quoted(state.value for state in EquityLifecycleState if state.is_terminal)


def _agrees(path: str, column: str) -> str:
    return f"json_extract(event_document, '$.{path}') IS {column}"


# The document checks run only on valid JSON (CASE short-circuits), so a
# malformed document is a CHECK failure, never a JSON function error.
_DOCUMENT_AGREES = (
    "CASE WHEN json_valid(event_document) THEN ("
    "json_type(event_document) = 'object' "
    "AND json_type(event_document, '$.sequence') = 'integer' "
    "AND json_type(event_document, '$.evidence') = 'object' "
    f"AND {_agrees('format', 'event_format')} "
    f"AND {_agrees('sequence', 'event_sequence')} "
    f"AND {_agrees('kind', 'event_kind')} "
    f"AND {_agrees('from_state', 'from_state')} "
    f"AND {_agrees('to_state', 'to_state')} "
    f"AND {_agrees('evaluated_at', 'evaluated_at')} "
    f"AND {_agrees('evidence.reference', 'evidence_reference')} "
    f"AND {_agrees('evidence.opens_at', 'evidence_opens_at')} "
    f"AND {_agrees('evidence.closes_at', 'evidence_closes_at')}"
    ") ELSE 0 END"
)

EQUITY_RECOMMENDATION_LIFECYCLE_SCHEMA = f"""
CREATE TABLE IF NOT EXISTS {LIFECYCLE_EVENTS} (
    issuance_id TEXT NOT NULL CHECK (issuance_id GLOB 'eiris1-sha256:?*'),
    event_sequence INTEGER NOT NULL
        CHECK (typeof(event_sequence) = 'integer' AND event_sequence >= 1),
    event_format TEXT NOT NULL
        CHECK (event_format = '{EQUITY_RECOMMENDATION_LIFECYCLE_EVENT_FORMAT}'),
    event_kind TEXT NOT NULL CHECK (event_kind IN ({_KINDS})),
    from_state TEXT NOT NULL CHECK (from_state IN ({_STATES})),
    to_state TEXT NOT NULL CHECK (to_state IN ({_STATES})),
    evaluated_at TEXT NOT NULL CHECK (typeof(evaluated_at) = 'text'),
    evaluated_at_us INTEGER NOT NULL CHECK (typeof(evaluated_at_us) = 'integer'),
    evidence_reference TEXT NOT NULL
        CHECK (typeof(evidence_reference) = 'text' AND length(evidence_reference) > 0),
    evidence_opens_at TEXT NOT NULL CHECK (typeof(evidence_opens_at) = 'text'),
    evidence_closes_at TEXT NOT NULL CHECK (typeof(evidence_closes_at) = 'text'),
    event_document TEXT NOT NULL
        CHECK (typeof(event_document) = 'text' AND json_valid(event_document)),
    CHECK ((event_kind = '{_REVISION}') = (from_state = to_state)),
    CHECK ({_DOCUMENT_AGREES}),
    PRIMARY KEY (issuance_id, event_sequence),
    FOREIGN KEY (issuance_id) REFERENCES {ISSUANCES} (issuance_id)
)
"""


def _refuse(name: str, event: str, message: str, when: str = "") -> str:
    condition = f"\n{when}" if when else ""
    return (
        f"CREATE TRIGGER IF NOT EXISTS {name}\n{event} ON {LIFECYCLE_EVENTS}{condition}\n"
        f"BEGIN SELECT RAISE(ABORT, '{message}'); END"
    )


def _same_issuance(select: str) -> str:
    return f"(SELECT {select} FROM {LIFECYCLE_EVENTS} AS e WHERE e.issuance_id = NEW.issuance_id)"


EQUITY_RECOMMENDATION_LIFECYCLE_TRIGGERS = (
    _refuse(f"{LIFECYCLE_EVENTS}_no_update", "BEFORE UPDATE", f"{LIFECYCLE_EVENTS} is insert-only"),
    _refuse(f"{LIFECYCLE_EVENTS}_no_delete", "BEFORE DELETE", f"{LIFECYCLE_EVENTS} is insert-only"),
    _refuse(
        f"{LIFECYCLE_EVENTS}_no_replace",
        "BEFORE INSERT",
        f"{LIFECYCLE_EVENTS} is insert-only",
        f"WHEN EXISTS (SELECT 1 FROM {LIFECYCLE_EVENTS} AS e "
        "WHERE e.issuance_id = NEW.issuance_id AND e.event_sequence = NEW.event_sequence)",
    ),
    _refuse(
        f"{LIFECYCLE_EVENTS}_next_sequence",
        "BEFORE INSERT",
        "a lifecycle event sequence must be the next one of its issuance",
        "WHEN NEW.event_sequence IS NOT "
        f"{_same_issuance('COALESCE(MAX(e.event_sequence), 0) + 1')}",
    ),
    _refuse(
        f"{LIFECYCLE_EVENTS}_canonical_instant",
        "BEFORE INSERT",
        "lifecycle instants must be canonical UTC text with their exact microseconds",
        f"WHEN NOT ({_canonical('evaluated_at')}) "
        f"OR NOT ({_canonical('evidence_opens_at')}) "
        f"OR NOT ({_canonical('evidence_closes_at')}) "
        f"OR NEW.evaluated_at_us IS NOT {_instant_us('evaluated_at')}",
    ),
    _refuse(
        f"{LIFECYCLE_EVENTS}_chronology",
        "BEFORE INSERT",
        "a lifecycle event must not precede the latest event of its issuance",
        f"WHEN NEW.evaluated_at_us < {_same_issuance('MAX(e.evaluated_at_us)')}",
    ),
    _refuse(
        f"{LIFECYCLE_EVENTS}_not_before_issuance",
        "BEFORE INSERT",
        "a lifecycle event must not precede its issuance",
        f"WHEN NEW.evaluated_at_us < (SELECT i.issued_at_us FROM {ISSUANCES} AS i "
        "WHERE i.issuance_id = NEW.issuance_id)",
    ),
    _refuse(
        f"{LIFECYCLE_EVENTS}_terminal",
        "BEFORE INSERT",
        "no lifecycle state may change after a terminal state",
        f"WHEN NEW.event_kind <> '{_REVISION}' AND (SELECT e.to_state FROM {LIFECYCLE_EVENTS} "
        f"AS e WHERE e.issuance_id = NEW.issuance_id AND e.event_kind <> '{_REVISION}' "
        f"ORDER BY e.event_sequence DESC LIMIT 1) IN ({_TERMINAL_STATES})",
    ),
)


def _require_issuance_tables(connection: sqlite3.Connection) -> None:
    present = {
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name IN (?, ?, ?)",
            REQUIRED_ISSUANCE_TABLES,
        )
    }
    missing = [name for name in REQUIRED_ISSUANCE_TABLES if name not in present]
    if missing:
        raise sqlite3.OperationalError(
            "This database holds no EI-6A issuance authority ("
            + ", ".join(missing)
            + " absent); lifecycle events are kept only beside the issuances they "
            "reference, and no issuance table is created here."
        )


def initialize_equity_recommendation_lifecycle_schema(connection: sqlite3.Connection) -> None:
    """Create the lifecycle-event table and triggers when they are absent.

    The EI-6A issuance tables must already exist in the same database;
    otherwise sqlite3.OperationalError is raised and nothing is created.
    Existing rows, tables, indexes and triggers are never altered.
    """
    _require_issuance_tables(connection)
    for statement in (
        EQUITY_RECOMMENDATION_LIFECYCLE_SCHEMA,
        *EQUITY_RECOMMENDATION_LIFECYCLE_TRIGGERS,
    ):
        connection.execute(statement)
    connection.commit()
