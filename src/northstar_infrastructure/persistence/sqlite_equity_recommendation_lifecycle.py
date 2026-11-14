"""SQLite lifecycle store for MODELED equity recommendations (EI-6B, D6B-6).

Implements the Application's ``EquityRecommendationLifecycleStore`` (ADR-024
sections 8 to 13; EI-6B Durable Recommendation Lifecycle Persistence Design
v1.0, sections 4, 5, 8 and 10) in the SAME database file as the EI-4D
generation records and the EI-6A issuance authority. MODELED and synthetic
only: no ACTUAL issuance, order, fill, position or monitoring exists here. It
is an internal persistence capability, wired into no operation, and calls no
provider, broker or network.

Ownership
---------
The Application owns the canonical event document and its strict decoding
(``decode_equity_lifecycle_event_document``), the verified rehydration
(``equity_recorded_lifecycle``), the window derivation
(``equity_issued_lifecycle_window``), the append authorization and its
validator, and the append rule (``equity_lifecycle_append_decision``); Core owns
the lifecycle state machine and its replay. This adapter applies them
unchanged and computes no window, transition or outcome of its own. It owns
durability, transactions, locking, rehydration from rows and the database's
own guards (sqlite_equity_recommendation_lifecycle_schema).

Parent provenance
-----------------
A foreign key is necessary, not sufficient. Every load and append re-derives
the parent issuance through the accepted EI-6A boundary, in the same
transaction: the whole EI-6A ledger is checked, the stored issuance is
re-derived from its EI-4D disposition (decoded at level A) with every column
compared, and the window is derived from that issued record and disposition.
Corrupt parent authority raises the EI-6A error
InvalidStoredEquityRecommendationIssuanceAuthorityError unchanged.

Authorized appends (D6B-5)
--------------------------
The only write entry is ``append(authorization)``. It first calls the
Application's public validator; a missing, foreign, unissued or changed
authorization raises UnverifiedEquityRecommendationLifecycleAppendError
**before any connection opens**. The rows written are the validated canonical
documents. There is no other public write.

Append transaction
------------------
One ``BEGIN IMMEDIATE`` transaction, which takes the write lock before
reading, so SQLite serializes writers:

1. the parent issuance is re-derived; it must equal the authorization's;
2. the window is derived, and the stored events are loaded, checked and
   replayed by Core (as for a load);
3. the Application's append rule decides: APPENDED proceeds; EQUIVALENT and
   CONFLICT write nothing;
4. Core replays the prior history plus the new events with the derived
   window, and the whole batch is inserted;
5. the issuance's events are confirmed to be exactly ``1..len(prior) +
   len(events)``, and the transaction commits once.

Any failure before COMMIT rolls back: no partial batch is ever authoritative.

Loads
-----
One read transaction over one consistent snapshot: the parent is re-derived
(``None`` when unknown), the window derived, the rows read in sequence order;
sequences must be exactly 1..n (``HISTORY``); each document must decode
strictly and its columns equal the ones it states (``EVENT``); Core replays
the whole history (``HISTORY``). A window that cannot be derived is
``WINDOW``. Reading never writes, repairs, migrates or skips anything.

Failures
--------
- EquityRecommendationLifecycleStoreUnavailableError: the database could not
  be opened or read, holds no EI-6A issuance authority, foreign keys could not
  be confirmed, or the write lock was not obtained within the busy timeout;
  nothing was written.
- InvalidStoredEquityRecommendationLifecycleError: the stored lifecycle is
  corrupt (``EVENT``, ``HISTORY`` or ``WINDOW``). Permanent; never a conflict,
  never equivalent; nothing is written or repaired.
- InvalidStoredEquityRecommendationIssuanceAuthorityError (EI-6A D6-5): the
  parent issuance authority is corrupt. Raised unchanged.
- InvalidEquityRecommendationLifecycleWriteError: the authorization names an
  issuance the store does not hold, or another parent or window than stored,
  or the database refused a row the checks admitted; rolled back, nothing
  written.
- EquityRecommendationLifecycleCommitUnconfirmedError: COMMIT itself failed;
  whether the append is durable is unknown, and the Application reconciles by
  reload. It is never evidence of a rollback.

Cleanup -- rolling back or closing a connection that may already have failed
-- never replaces the failure being reported. The adapter never retries,
sleeps, reads a clock or works in the background.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any, NoReturn

from northstar_application.application_services import (
    EQUITY_RECOMMENDATION_LIFECYCLE_EVENT_FORMAT,
    EquityRecordedIssuance,
    InvalidEquityLifecycleEventDocumentError,
    InvalidEquityLifecycleWindowProvenanceError,
    InvalidEquityRecommendationRecordError,
    VerifiedEquityRecommendationLifecycleAppend,
    decode_equity_lifecycle_event_document,
    equity_issued_lifecycle_window,
    equity_lifecycle_append_decision,
    equity_recorded_lifecycle,
    require_verified_equity_recommendation_lifecycle_append,
)
from northstar_application.ports import (
    EquityRecommendationLifecycleCommitUnconfirmedError,
    EquityRecommendationLifecycleStore,
    EquityRecommendationLifecycleStoreUnavailableError,
    EquityRecommendationLifecycleWrite,
    EquityRecommendationLifecycleWriteOutcome,
    EquityRecordedLifecycle,
    InvalidEquityRecommendationLifecycleWriteError,
    InvalidStoredEquityRecommendationIssuanceAuthorityError,
    InvalidStoredEquityRecommendationLifecycleError,
)
from northstar_core.recommendations.equity.lifecycle import (
    EquityLifecycleEvent,
    EquityLifecycleWindow,
    EquityRecommendationLifecycle,
    InvalidEquityRecommendationLifecycleError,
)

from northstar_infrastructure.persistence.sqlite_equity_recommendation_generation import (
    _stored_disposition,  # the EI-4D stored-record decoding, reused unchanged
)
from northstar_infrastructure.persistence.sqlite_equity_recommendation_issuance import (
    _SELECT_DISPOSITION,
    _SELECT_ISSUANCES,
    _consistent_ledger,  # the EI-6A whole-ledger check, reused unchanged
    _stored_issuance,  # the EI-6A issuance re-derivation, reused unchanged
    instant_microseconds,
)
from northstar_infrastructure.persistence.sqlite_equity_recommendation_lifecycle_schema import (
    LIFECYCLE_EVENTS,
    initialize_equity_recommendation_lifecycle_schema,
)

_BUSY_TIMEOUT_SECONDS = 5.0
_W = EquityRecommendationLifecycleWriteOutcome
_Integrity = InvalidStoredEquityRecommendationLifecycleError
_Authority = InvalidStoredEquityRecommendationIssuanceAuthorityError

LIFECYCLE_EVENT_COLUMNS = (
    "issuance_id",
    "event_sequence",
    "event_format",
    "event_kind",
    "from_state",
    "to_state",
    "evaluated_at",
    "evaluated_at_us",
    "evidence_reference",
    "evidence_opens_at",
    "evidence_closes_at",
    "event_document",
)

_SELECT_EVENTS = (
    f"SELECT {', '.join(LIFECYCLE_EVENT_COLUMNS)} FROM {LIFECYCLE_EVENTS} "  # noqa: S608
    "WHERE issuance_id = ? ORDER BY event_sequence"
)
_INSERT_EVENT = (
    f"INSERT INTO {LIFECYCLE_EVENTS} ({', '.join(LIFECYCLE_EVENT_COLUMNS)}) "  # noqa: S608
    f"VALUES ({', '.join('?' * len(LIFECYCLE_EVENT_COLUMNS))})"
)
_EVENT_SHAPE = (
    f"SELECT COUNT(*), MIN(event_sequence), MAX(event_sequence) FROM {LIFECYCLE_EVENTS} "  # noqa: S608
    "WHERE issuance_id = ?"
)


# ---------------------------------------------------------------------------
# Rows
# ---------------------------------------------------------------------------


def lifecycle_event_row(
    issuance_id: str, event: EquityLifecycleEvent, document: str
) -> tuple[object, ...]:
    """Return the columns one event is stored as, in LIFECYCLE_EVENT_COLUMNS order.

    ``document`` must be the event's canonical document; the columns restate
    parts of it, and the schema's CHECKs tie them to it.
    """
    if not isinstance(event, EquityLifecycleEvent):
        raise TypeError("event must be an EquityLifecycleEvent.")
    return (
        issuance_id,
        event.sequence,
        EQUITY_RECOMMENDATION_LIFECYCLE_EVENT_FORMAT,
        event.kind.value,
        event.from_state.value,
        event.to_state.value,
        event.evaluated_at.value,
        instant_microseconds(event.evaluated_at),
        event.evidence.reference,
        event.evidence.opens_at.value,
        event.evidence.closes_at.value,
        document,
    )


def _typed(values: Sequence[object]) -> tuple[tuple[type, object], ...]:
    """Compare stored values with their types, so 1, 1.0 and True never pass for one another."""
    return tuple((type(value), value) for value in values)


def _corrupt(category: str, message: str) -> NoReturn:
    raise _Integrity(category, message)


def _parent(
    connection: sqlite3.Connection, issuance_id: str
) -> tuple[EquityRecordedIssuance, Any] | None:
    """Re-derive the parent issuance and decode its disposition, through the EI-6A boundary.

    ``None`` when no issuance has this identity. Corrupt authority raises the
    EI-6A D6-5 error unchanged.
    """
    _consistent_ledger(connection)
    row = connection.execute(
        _SELECT_ISSUANCES + " WHERE issuance_id = ?", (issuance_id,)
    ).fetchone()
    if row is None:
        return None
    recorded = _stored_issuance(connection, row)
    found = connection.execute(
        _SELECT_DISPOSITION, (recorded.recommendation.generation_key,)
    ).fetchone()
    try:
        if found is None:
            raise InvalidEquityRecommendationRecordError("no stored disposition")
        disposition = _stored_disposition(found)
    except InvalidEquityRecommendationRecordError as exc:
        raise _Authority(
            _Authority.ISSUANCE,
            f"The stored issuance's disposition does not decode ({type(exc).__name__}).",
            issuance_id=issuance_id,
        ) from exc
    return recorded, disposition


def _window(recorded: EquityRecordedIssuance, disposition: Any) -> EquityLifecycleWindow:
    """The Application's governed window derivation; a failure is permanent ``WINDOW``."""
    try:
        return equity_issued_lifecycle_window(recorded.recommendation, disposition)
    except InvalidEquityLifecycleWindowProvenanceError as exc:
        raise _Integrity(
            _Integrity.WINDOW, f"The lifecycle window cannot be derived: {exc}"
        ) from exc


def _stored_lifecycle(
    connection: sqlite3.Connection,
    recorded: EquityRecordedIssuance,
    window: EquityLifecycleWindow,
) -> EquityRecordedLifecycle:
    """Rehydrate the stored lifecycle: contiguous, strictly decoded, column-checked, replayed."""
    issuance_id = recorded.recommendation.issuance_id
    rows = [tuple(row) for row in connection.execute(_SELECT_EVENTS, (issuance_id,))]
    sequences = [row[1] for row in rows]
    if _typed(sequences) != _typed(range(1, len(rows) + 1)):
        _corrupt(_Integrity.HISTORY, "The stored event sequences are not exactly 1..n.")
    for position, row in enumerate(rows, start=1):
        try:
            event = decode_equity_lifecycle_event_document(row[-1])
        except InvalidEquityLifecycleEventDocumentError as exc:
            raise _Integrity(
                _Integrity.EVENT,
                f"Stored lifecycle event {position} does not decode ({type(exc).__name__}).",
            ) from exc
        if _typed(lifecycle_event_row(issuance_id, event, row[-1])) != _typed(row):
            _corrupt(
                _Integrity.EVENT,
                f"Stored lifecycle event {position} has columns that disagree with its document.",
            )
    return equity_recorded_lifecycle(recorded, window, [row[-1] for row in rows])


# ---------------------------------------------------------------------------
# Connections and transactions
# ---------------------------------------------------------------------------


def _unavailable(exc: BaseException) -> EquityRecommendationLifecycleStoreUnavailableError:
    return EquityRecommendationLifecycleStoreUnavailableError(
        f"The lifecycle store is unavailable; nothing was written: {exc}"
    )


def _connect(database_path: Path) -> sqlite3.Connection:
    """Open the existing issuance-authority database; never create one.

    Foreign keys are turned on and confirmed on this connection, and the
    lifecycle objects are added when absent. A database without the EI-6A
    issuance tables is refused: the lifecycle store never creates the
    authority it references.
    """
    try:
        connection = sqlite3.connect(
            f"{database_path.resolve().as_uri()}?mode=rw",
            timeout=_BUSY_TIMEOUT_SECONDS,
            isolation_level=None,
            uri=True,
        )
    except sqlite3.Error as exc:
        raise _unavailable(exc) from exc
    try:
        connection.execute("PRAGMA foreign_keys = ON")
        if connection.execute("PRAGMA foreign_keys").fetchone() != (1,):
            raise sqlite3.OperationalError("foreign keys could not be enforced")
        initialize_equity_recommendation_lifecycle_schema(connection)
    except sqlite3.Error as exc:
        connection.close()
        raise _unavailable(exc) from exc
    return connection


def _commit(connection: sqlite3.Connection) -> None:
    """Commit the open transaction: the one boundary whose failure is unconfirmed."""
    connection.execute("COMMIT")


def _close(connection: sqlite3.Connection) -> None:
    connection.close()


def _rollback(connection: sqlite3.Connection) -> None:
    """Roll back an open transaction; never mask the failure that led here."""
    try:
        if connection.in_transaction:
            connection.execute("ROLLBACK")
    except sqlite3.Error:
        pass


def _discard(connection: sqlite3.Connection) -> None:
    """Close a connection as a failure propagates; a close failure never replaces it."""
    try:
        _close(connection)
    except sqlite3.Error:
        pass


def _confirm_appended(connection: sqlite3.Connection, issuance_id: str, expected: int) -> None:
    """Before COMMIT: the issuance's events are exactly 1..expected."""
    count, lowest, greatest = connection.execute(_EVENT_SHAPE, (issuance_id,)).fetchone()
    if (count, lowest, greatest) != (expected, 1, expected):
        raise InvalidEquityRecommendationLifecycleWriteError(
            "The appended events are not exactly the expected sequences; nothing is committed."
        )


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class SQLiteEquityRecommendationLifecycleStore(EquityRecommendationLifecycleStore):
    """Durable lifecycles of MODELED issuances, in the EI-4D and EI-6A database.

    ``database_path`` must be the existing database holding the EI-6A issuance
    authority: there is no second database setting. See the module docstring.
    """

    def __init__(self, database_path: str | Path) -> None:
        self._database_path = Path(database_path)

    def load_lifecycle(self, issuance_id: str) -> EquityRecordedLifecycle | None:
        """Return the verified, replayed lifecycle of ``issuance_id``, or None when unknown."""
        if not isinstance(issuance_id, str):
            raise TypeError("issuance_id must be text.")

        def work(connection: sqlite3.Connection) -> EquityRecordedLifecycle | None:
            parent = _parent(connection, issuance_id)
            if parent is None:
                return None
            recorded, disposition = parent
            return _stored_lifecycle(connection, recorded, _window(recorded, disposition))

        return self._read(work)

    def _read[T](self, work: Callable[[sqlite3.Connection], T]) -> T:
        """Run ``work`` in one read transaction over one consistent snapshot."""
        connection = _connect(self._database_path)
        try:
            try:
                connection.execute("BEGIN")
                return work(connection)
            except sqlite3.Error as exc:
                raise _unavailable(exc) from exc
            finally:
                _rollback(connection)
        finally:
            _discard(connection)

    def append(
        self, authorization: VerifiedEquityRecommendationLifecycleAppend
    ) -> EquityRecommendationLifecycleWrite:
        """Append exactly the events ``authorization`` covers, by compare-and-append.

        The Application validator runs before any connection opens. See the
        module docstring for the transaction.
        """
        verified = require_verified_equity_recommendation_lifecycle_append(authorization)
        issuance_id = verified.issuance_id
        rows = [
            lifecycle_event_row(issuance_id, event, document)
            for event, document in zip(verified.events, verified.event_documents, strict=True)
        ]
        expected = verified.expected_position + len(rows)

        def work(
            connection: sqlite3.Connection,
        ) -> tuple[EquityRecommendationLifecycleWrite, bool]:
            parent = _parent(connection, issuance_id)
            if parent is None:
                raise InvalidEquityRecommendationLifecycleWriteError(
                    "The store holds no issuance the authorization names; nothing was written."
                )
            recorded, disposition = parent
            if recorded != verified.issuance:
                raise InvalidEquityRecommendationLifecycleWriteError(
                    "The stored parent issuance is not the authorization's; nothing was written."
                )
            window = _window(recorded, disposition)
            stored = _stored_lifecycle(connection, recorded, window)
            outcome = equity_lifecycle_append_decision(stored, verified)
            if outcome is not _W.APPENDED:
                return EquityRecommendationLifecycleWrite(outcome, stored), False
            try:
                lifecycle = EquityRecommendationLifecycle(window, verified.prior + verified.events)
            except InvalidEquityRecommendationLifecycleError as exc:
                raise InvalidEquityRecommendationLifecycleWriteError(
                    f"Core does not replay the authorized history; nothing was written: {exc}"
                ) from exc
            for row in rows:
                connection.execute(_INSERT_EVENT, row)
            _confirm_appended(connection, issuance_id, expected)
            written = EquityRecordedLifecycle(recorded, lifecycle)
            return EquityRecommendationLifecycleWrite(_W.APPENDED, written), True

        return self._write(work)

    def _write[W](self, work: Callable[[sqlite3.Connection], tuple[W, bool]]) -> W:
        """Run ``work`` in one BEGIN IMMEDIATE transaction; commit once, only if it wrote.

        On failure the connection is discarded without letting cleanup replace
        the typed failure; after a confirmed COMMIT it is closed normally.
        """
        connection = _connect(self._database_path)
        try:
            try:
                connection.execute("BEGIN IMMEDIATE")
                written, wrote = work(connection)
            except sqlite3.IntegrityError as exc:
                _rollback(connection)
                raise InvalidEquityRecommendationLifecycleWriteError(
                    f"The database refused the append; nothing was written: {exc}"
                ) from exc
            except sqlite3.Error as exc:
                _rollback(connection)
                raise _unavailable(exc) from exc
            except BaseException:
                _rollback(connection)
                raise
            if not wrote:
                _rollback(connection)
            else:
                try:
                    _commit(connection)
                except Exception as exc:
                    _rollback(connection)
                    raise EquityRecommendationLifecycleCommitUnconfirmedError(
                        f"COMMIT failed; whether the lifecycle append is durable is unknown: {exc}"
                    ) from exc
        except BaseException:
            _discard(connection)
            raise
        _close(connection)
        return written
