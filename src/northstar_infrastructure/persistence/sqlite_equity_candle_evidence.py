"""SQLite equity candle evidence journal and as-issued snapshot store (EI-2B).

Status: an internal persistence capability for synthetic evidence, not an
enabled ingestion workflow. Nothing here calls a provider, NSE or a broker,
and nothing is wired into operations.

Journal
-------
``SQLiteEquityCandleEvidenceJournal.append(record)`` runs in ONE
``BEGIN IMMEDIATE`` transaction, which takes the database write lock before
anything is read, so concurrent appends are serialised by SQLite itself:

1. the row under the record's ingestion id, if any, is read, verified and
   handed to the shared Application retry rule: the same record is
   ALREADY_APPENDED and nothing is written; other content raises
   EquityCandleIngestionConflictError;
2. every row of the record's source identity is read, verified and handed to
   the shared identity rule, which refuses a contradicting record with
   EquityCandleSourceIdentityConflictError and names earlier observations
   with other OHLCV values;
3. the row is inserted. SQLite assigns the next AUTOINCREMENT sequence, and the
   injected clock -- the only clock read in this module -- supplies the
   informative ``journaled_at``. Nothing orders by it.

The schema's triggers enforce the same identity bindings, refuse updates,
deletes and replacements, and refuse a sequence below the highest one, so a
connection that bypasses this adapter cannot rewrite or backdate history.

``read_window`` reads in one transaction and returns only rows at or below
the watermark it is given; a watermark above the journal's is refused.

Snapshots
---------
``SQLiteEquityCandleEvidenceSnapshotStore.issue(snapshot)`` runs in ONE
``BEGIN IMMEDIATE`` transaction. An identical snapshot already stored is an
idempotent no-op (0); other content under its identity is a conflict. Otherwise,
inside the transaction and with the write lock held:

1. the journal at the snapshot's watermark must hold exactly the snapshot's
   entries, record fingerprint for record fingerprint;
2. the window read at the journal's highest committed sequence must still hold
   exactly those entries. A correction, finality statement or candle for the
   window committed after the snapshot's watermark raises
   EquityCandleSnapshotStaleError; entries outside the window do not;
3. the snapshot row and one entry row per listed journal entry are inserted.

Everything commits, or nothing does. Because the write lock is held from the
check to the commit, no append can commit in between: every recorded snapshot
is the window's latest committed evidence at the moment it is recorded.

Every read re-verifies
----------------------
A journal row is decoded from its canonical JSON, which must re-encode
exactly, and its constrained columns and fingerprints must be the ones the
record produces. A snapshot is decoded from its stored document, which
re-derives its manifest fingerprint and identity; the columns and entry rows
must agree; and the journal at its watermark must still hold exactly its
entries. Anything else raises EquityCandleEvidenceStorageError. Nothing is
repaired, recalculated or skipped.

A writer waits at most five seconds for the lock; past it the write fails as a
storage error and nothing is written. Foreign keys are enforced on this
module's connections.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from northstar_application.application_services import (
    EquityCandleAppendOutcome,
    EquityCandleAppendResult,
    EquityCandleEvidenceSnapshot,
    EquityCandleFinalityRecord,
    EquityCandleJournalEntry,
    EquityCandleJournalRecord,
    EquityCandleJournalWindow,
    EquityCandleObservationRecord,
    canonical_equity_candle_json,
    equity_candle_identity_conflicts,
    equity_candle_in_window,
    equity_candle_payload_fingerprint,
    equity_candle_record_document,
    equity_candle_record_fingerprint,
    equity_candle_record_from_document,
    equity_candle_retry,
    equity_candle_snapshot_from_document,
    equity_candle_snapshot_json,
    require_equity_candle_snapshot_current,
    require_equity_candle_snapshot_evidence,
)
from northstar_application.ports import (
    EquityCandleEvidenceJournal,
    EquityCandleEvidenceSnapshotStore,
    EquityCandleEvidenceUnavailableError,
    EquityCandleSnapshotConflictError,
    EquityCandleSnapshotEvidenceError,
    InvalidEquityCandleJournalQueryError,
)
from northstar_core.domain.value_objects import ListingReference
from northstar_core.foundation.value_objects import PointInTime, Timeframe

from northstar_infrastructure.persistence.sqlite_equity_candle_evidence_schema import (
    initialize_equity_candle_evidence_schema,
)

_BUSY_TIMEOUT_SECONDS = 5.0
_DECODE_ERRORS = (KeyError, TypeError, ValueError, ArithmeticError)

JOURNAL_COLUMNS = (
    "sequence",
    "entry_kind",
    "ingestion_id",
    "journaled_at",
    "source_id",
    "source_event_id",
    "source_revision",
    "received_at",
    "is_synthetic",
    "symbol",
    "exchange_code",
    "timeframe",
    "interval_end",
    "corrects_revision",
    "source_published_at",
    "confirmed_at",
    "payload_fingerprint",
    "record_fingerprint",
    "record",
)
SNAPSHOT_COLUMNS = (
    "snapshot_id",
    "manifest_fingerprint",
    "symbol",
    "exchange_code",
    "trading_date",
    "timeframe",
    "window_policy_version",
    "finality_policy_version",
    "as_of",
    "issued_at",
    "watermark",
    "evidence_class",
    "coverage",
    "entry_count",
    "document",
)
_SELECT_JOURNAL = f"SELECT {', '.join(JOURNAL_COLUMNS)} FROM equity_candle_journal"  # noqa: S608
_INSERT_JOURNAL = (
    f"INSERT INTO equity_candle_journal ({', '.join(JOURNAL_COLUMNS[1:])}) "  # noqa: S608
    f"VALUES ({', '.join('?' * (len(JOURNAL_COLUMNS) - 1))})"
)
_INSERT_SNAPSHOT = (
    f"INSERT INTO equity_candle_evidence_snapshots ({', '.join(SNAPSHOT_COLUMNS)}) "  # noqa: S608
    f"VALUES ({', '.join('?' * len(SNAPSHOT_COLUMNS))})"
)
_SELECT_SNAPSHOT = (
    f"SELECT {', '.join(SNAPSHOT_COLUMNS)} FROM equity_candle_evidence_snapshots "  # noqa: S608
    "WHERE snapshot_id = ?"
)
_INSERT_SNAPSHOT_ENTRY = (
    "INSERT INTO equity_candle_evidence_snapshot_entries "
    "(snapshot_id, sequence, record_fingerprint) VALUES (?, ?, ?)"
)
_SELECT_SNAPSHOT_ENTRIES = (
    "SELECT sequence, record_fingerprint FROM equity_candle_evidence_snapshot_entries "
    "WHERE snapshot_id = ? ORDER BY sequence"
)


class EquityCandleEvidenceStorageError(EquityCandleEvidenceUnavailableError):
    """Raised when evidence storage is unavailable or holds invalid data.

    It is the Application port's EquityCandleEvidenceUnavailableError (a
    RuntimeError), so orchestration can tell an operational failure from a
    rejected record without importing Infrastructure.
    """


def _utc_now() -> datetime:
    return datetime.now(UTC)


# ---------------------------------------------------------------------------
# Rows
# ---------------------------------------------------------------------------


def _journal_values(
    record: EquityCandleJournalRecord, journaled_at: PointInTime
) -> tuple[object, ...]:
    """Return a record's journal row without its sequence."""
    if isinstance(record, EquityCandleObservationRecord):
        bar, observation = record.observation.bar, record.observation
        placement: tuple[object, ...] = (
            bar.symbol.value,
            bar.exchange_code.value,
            bar.timeframe.value,
            bar.point_in_time.value,
            observation.corrects_revision,
            None if record.source_published_at is None else record.source_published_at.value,
            None,
        )
    else:
        placement = (None, None, None, None, None, None, record.evidence.confirmed_at.value)
    return (
        record.kind.value,
        record.ingestion_id,
        journaled_at.value,
        *record.source_identity,
        record.received_at.value,
        int(record.is_synthetic),
        *placement,
        equity_candle_payload_fingerprint(record),
        equity_candle_record_fingerprint(record),
        canonical_equity_candle_json(equity_candle_record_document(record)),
    )


def _text(value: Any) -> str:
    if not isinstance(value, str):
        raise TypeError("Stored evidence values must be text.")
    return value


def _entry(row: Any) -> EquityCandleJournalEntry:
    """Decode one journal row; every column must be what its record produces."""
    stored = tuple(row)
    sequence = stored[0]
    if not isinstance(sequence, int) or isinstance(sequence, bool):
        raise TypeError("Stored journal sequences must be integers.")
    record = equity_candle_record_from_document(json.loads(_text(stored[-1])))
    entry = EquityCandleJournalEntry(sequence, PointInTime(_text(stored[3])), record)
    if (sequence, *_journal_values(record, entry.journaled_at)) != stored:
        raise ValueError(
            f"Journal entry {sequence} is stored non-canonically, or its columns or "
            "fingerprints disagree with its record."
        )
    return entry


def _snapshot_row(snapshot: EquityCandleEvidenceSnapshot) -> tuple[object, ...]:
    return (
        snapshot.snapshot_id,
        snapshot.manifest_fingerprint,
        snapshot.listing.symbol.value,
        snapshot.listing.exchange_code.value,
        snapshot.trading_date.isoformat(),
        snapshot.timeframe.value,
        snapshot.window_policy_version,
        snapshot.finality_policy.version,
        snapshot.as_of.value,
        snapshot.issued_at.value,
        snapshot.watermark,
        snapshot.evidence_class.value,
        snapshot.coverage.value,
        len(snapshot.observations) + len(snapshot.finalities),
        equity_candle_snapshot_json(snapshot),
    )


def _snapshot_entries(snapshot: EquityCandleEvidenceSnapshot) -> list[tuple[int, str]]:
    listed = [*snapshot.observations, *snapshot.finalities]
    return sorted((e.sequence, equity_candle_record_fingerprint(e.record)) for e in listed)


# ---------------------------------------------------------------------------
# Queries inside a transaction
# ---------------------------------------------------------------------------


def _top(connection: sqlite3.Connection) -> int:
    value = connection.execute("SELECT COALESCE(MAX(sequence), 0) FROM equity_candle_journal")
    top = value.fetchone()[0]
    if not isinstance(top, int):
        raise TypeError("The journal watermark must be an integer.")
    return top


def _window(
    connection: sqlite3.Connection,
    listing: ListingReference,
    timeframe: Timeframe,
    opens_after: PointInTime,
    closes_at: PointInTime,
    watermark: int,
) -> EquityCandleJournalWindow:
    if watermark > _top(connection):
        raise InvalidEquityCandleJournalQueryError(
            f"Watermark {watermark} is beyond the journal's {_top(connection)}."
        )
    rows = connection.execute(
        f"{_SELECT_JOURNAL} WHERE entry_kind = 'OBSERVATION' AND symbol = ? "
        "AND exchange_code = ? AND timeframe = ? AND sequence <= ? ORDER BY sequence",
        (listing.symbol.value, listing.exchange_code.value, timeframe.value, watermark),
    )
    observations = tuple(
        entry
        for entry in (_entry(row) for row in rows)
        if equity_candle_in_window(entry.record, listing, timeframe, opens_after, closes_at)
    )
    finalities: dict[int, EquityCandleJournalEntry] = {}
    for identity in dict.fromkeys(entry.record.source_identity for entry in observations):
        for row in connection.execute(
            f"{_SELECT_JOURNAL} WHERE entry_kind = 'FINALITY' AND source_id = ? "
            "AND source_event_id = ? AND source_revision = ? AND sequence <= ? ORDER BY sequence",
            (*identity, watermark),
        ):
            entry = _entry(row)
            finalities[entry.sequence] = entry
    return EquityCandleJournalWindow(
        listing,
        timeframe,
        opens_after,
        closes_at,
        watermark,
        observations,
        tuple(finalities[sequence] for sequence in sorted(finalities)),
    )


def _stored_snapshot(
    connection: sqlite3.Connection, snapshot_id: str
) -> EquityCandleEvidenceSnapshot | None:
    """Return the stored snapshot, requiring every stored fact and the journal to agree."""
    row = connection.execute(_SELECT_SNAPSHOT, (snapshot_id,)).fetchone()
    if row is None:
        return None
    stored = tuple(row)
    snapshot = equity_candle_snapshot_from_document(json.loads(_text(stored[-1])))
    if _snapshot_row(snapshot) != stored:
        raise ValueError(
            f"Snapshot {snapshot_id!r} is stored non-canonically, under another identity, or "
            "with columns that disagree with its manifest."
        )
    entries = [tuple(e) for e in connection.execute(_SELECT_SNAPSHOT_ENTRIES, (snapshot_id,))]
    if entries != _snapshot_entries(snapshot):
        raise ValueError(f"Snapshot {snapshot_id!r} has entry rows that disagree.")
    require_equity_candle_snapshot_evidence(
        snapshot,
        _window(
            connection,
            snapshot.listing,
            snapshot.timeframe,
            snapshot.window_opens_at,
            snapshot.window_closes_at,
            snapshot.watermark,
        ),
    )
    return snapshot


# ---------------------------------------------------------------------------
# Transactions
# ---------------------------------------------------------------------------


def _connect(database_path: str) -> sqlite3.Connection:
    try:
        connection = sqlite3.connect(
            database_path, timeout=_BUSY_TIMEOUT_SECONDS, isolation_level=None
        )
        connection.execute("PRAGMA foreign_keys = ON")
        initialize_equity_candle_evidence_schema(connection)
    except sqlite3.Error as exc:
        raise EquityCandleEvidenceStorageError(
            "Equity candle evidence storage is unavailable."
        ) from exc
    return connection


def _close(connection: sqlite3.Connection) -> None:
    connection.close()


def _rollback(connection: sqlite3.Connection) -> None:
    """Roll back an open transaction; never mask the failure that led here.

    A connection that already failed or closed cannot even report whether a
    transaction is open, so nothing is asserted about it: a transaction that
    never committed ends rolled back when its connection closes.
    """
    try:
        if connection.in_transaction:
            connection.rollback()
    except sqlite3.Error:
        pass


def _discard(connection: sqlite3.Connection) -> None:
    """Close a connection as a failure propagates; a close failure never replaces it."""
    try:
        _close(connection)
    except sqlite3.Error:
        pass


def _write[T](database_path: str, work: Callable[[sqlite3.Connection], T]) -> T:
    """Run ``work`` in one BEGIN IMMEDIATE transaction; commit it, or roll all of it back.

    A failure -- in the work or at COMMIT -- is reported as it was classified
    before cleanup: rolling back or closing the connection never replaces it.
    After a confirmed COMMIT the connection is closed normally, and a failure
    to close it is reported as itself.
    """
    connection = _connect(database_path)
    try:
        try:
            connection.execute("BEGIN IMMEDIATE")
            outcome = work(connection)
            connection.commit()
        except sqlite3.Error as exc:
            _rollback(connection)
            raise EquityCandleEvidenceStorageError(
                f"Equity candle evidence storage refused the write: {exc}"
            ) from exc
        except BaseException:
            _rollback(connection)
            raise
    except BaseException:
        _discard(connection)
        raise
    _close(connection)
    return outcome


def _read[T](database_path: str, work: Callable[[sqlite3.Connection], T]) -> T:
    """Run ``work`` in one read transaction, so every read sees one database state."""
    try:
        with closing(_connect(database_path)) as connection:
            connection.execute("BEGIN")
            try:
                return work(connection)
            finally:
                connection.rollback()
    except sqlite3.Error as exc:
        raise EquityCandleEvidenceStorageError(
            "Equity candle evidence storage is unavailable."
        ) from exc


def _verified[T](work: Callable[[], T], subject: str) -> T:
    try:
        return work()
    except _DECODE_ERRORS as exc:
        if isinstance(exc, InvalidEquityCandleJournalQueryError):
            raise
        raise EquityCandleEvidenceStorageError(f"{subject} has invalid evidence: {exc}") from exc


# ---------------------------------------------------------------------------
# Adapters
# ---------------------------------------------------------------------------


class SQLiteEquityCandleEvidenceJournal(EquityCandleEvidenceJournal):
    """Append-only SQLite journal of equity candle observations and finality statements."""

    def __init__(self, database_path: str | Path, clock: Callable[[], datetime] = _utc_now) -> None:
        if not callable(clock):
            raise TypeError("SQLiteEquityCandleEvidenceJournal clock must be callable.")
        self._database_path = str(database_path)
        self._clock = clock

    def _journaled_at(self) -> PointInTime:
        now = self._clock()
        if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() is None:
            raise ValueError(
                "SQLiteEquityCandleEvidenceJournal clock must return an aware datetime."
            )
        return PointInTime(now.astimezone(UTC).isoformat().replace("+00:00", "Z"))

    def append(self, record: EquityCandleJournalRecord) -> EquityCandleAppendResult:
        """Append ``record`` in one transaction, or return the entry a retry repeats."""
        if not isinstance(record, EquityCandleObservationRecord | EquityCandleFinalityRecord):
            raise TypeError("record must be an equity candle observation or finality record.")

        def work(connection: sqlite3.Connection) -> EquityCandleAppendResult:
            def entries(
                sql: str, parameters: tuple[object, ...]
            ) -> tuple[EquityCandleJournalEntry, ...]:
                rows = connection.execute(f"{_SELECT_JOURNAL} {sql}", parameters).fetchall()
                return _verified(lambda: tuple(_entry(row) for row in rows), "The journal")

            same = entries("WHERE ingestion_id = ?", (record.ingestion_id,))
            retry = equity_candle_retry(record, same[0] if same else None)
            if retry is not None:
                return EquityCandleAppendResult(EquityCandleAppendOutcome.ALREADY_APPENDED, retry)
            conflicts = equity_candle_identity_conflicts(
                record,
                entries(
                    "WHERE source_id = ? AND source_event_id = ? AND source_revision = ? "
                    "ORDER BY sequence",
                    record.source_identity,
                ),
            )
            journaled_at = self._journaled_at()
            cursor = connection.execute(_INSERT_JOURNAL, _journal_values(record, journaled_at))
            sequence = cursor.lastrowid
            if not isinstance(sequence, int):
                raise EquityCandleEvidenceStorageError("The journal assigned no sequence.")
            return EquityCandleAppendResult(
                EquityCandleAppendOutcome.APPENDED,
                EquityCandleJournalEntry(sequence, journaled_at, record),
                conflicts,
            )

        return _write(self._database_path, work)

    def watermark(self) -> int:
        """Return the highest committed journal sequence, 0 when empty."""
        return _read(self._database_path, _top)

    def read_window(
        self,
        listing: ListingReference,
        timeframe: Timeframe,
        opens_after: PointInTime,
        closes_at: PointInTime,
        watermark: int,
    ) -> EquityCandleJournalWindow:
        """Return the window's evidence journaled at or below ``watermark``."""
        if not isinstance(listing, ListingReference):
            raise TypeError("listing must be a ListingReference.")
        if not isinstance(timeframe, Timeframe):
            raise TypeError("timeframe must be a Timeframe.")
        if not isinstance(opens_after, PointInTime) or not isinstance(closes_at, PointInTime):
            raise TypeError("window bounds must be PointInTime values.")
        if not isinstance(watermark, int) or isinstance(watermark, bool) or watermark < 0:
            raise InvalidEquityCandleJournalQueryError("watermark must be an integer >= 0.")
        return _read(
            self._database_path,
            lambda connection: _verified(
                lambda: _window(connection, listing, timeframe, opens_after, closes_at, watermark),
                "The journal",
            ),
        )


class SQLiteEquityCandleEvidenceSnapshotStore(EquityCandleEvidenceSnapshotStore):
    """Immutable SQLite store of as-issued equity candle evidence snapshots."""

    def __init__(self, database_path: str | Path) -> None:
        self._database_path = str(database_path)

    def issue(self, snapshot: EquityCandleEvidenceSnapshot) -> int:
        """Record ``snapshot`` in one transaction: 1 when recorded, 0 when already identical."""
        if not isinstance(snapshot, EquityCandleEvidenceSnapshot):
            raise TypeError("snapshot must be an EquityCandleEvidenceSnapshot.")

        def work(connection: sqlite3.Connection) -> int:
            stored = _verified(
                lambda: _stored_snapshot(connection, snapshot.snapshot_id),
                f"Snapshot {snapshot.snapshot_id}",
            )
            if stored is not None:
                if stored != snapshot:
                    raise EquityCandleSnapshotConflictError(
                        f"Snapshot {snapshot.snapshot_id} is already issued with other content."
                    )
                return 0
            try:
                evidence = _verified(
                    lambda: _window(
                        connection,
                        snapshot.listing,
                        snapshot.timeframe,
                        snapshot.window_opens_at,
                        snapshot.window_closes_at,
                        snapshot.watermark,
                    ),
                    "The journal",
                )
            except InvalidEquityCandleJournalQueryError as exc:
                raise EquityCandleSnapshotEvidenceError(str(exc)) from exc
            require_equity_candle_snapshot_evidence(snapshot, evidence)
            current = _verified(
                lambda: _window(
                    connection,
                    snapshot.listing,
                    snapshot.timeframe,
                    snapshot.window_opens_at,
                    snapshot.window_closes_at,
                    _top(connection),
                ),
                "The journal",
            )
            require_equity_candle_snapshot_current(snapshot, current)
            connection.execute(_INSERT_SNAPSHOT, _snapshot_row(snapshot))
            for sequence, fingerprint in _snapshot_entries(snapshot):
                connection.execute(
                    _INSERT_SNAPSHOT_ENTRY, (snapshot.snapshot_id, sequence, fingerprint)
                )
            return 1

        return _write(self._database_path, work)

    def get_snapshot(self, snapshot_id: str) -> EquityCandleEvidenceSnapshot | None:
        """Return the snapshot issued under ``snapshot_id``, verified, if any."""
        if not isinstance(snapshot_id, str):
            raise TypeError("snapshot_id must be text.")

        def work(connection: sqlite3.Connection) -> EquityCandleEvidenceSnapshot | None:
            try:
                return _stored_snapshot(connection, snapshot_id)
            except (*_DECODE_ERRORS, EquityCandleSnapshotEvidenceError) as exc:
                raise EquityCandleEvidenceStorageError(
                    f"Snapshot {snapshot_id!r} has invalid evidence: {exc}"
                ) from exc

        return _read(self._database_path, work)
