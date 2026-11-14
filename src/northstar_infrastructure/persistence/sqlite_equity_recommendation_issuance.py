"""SQLite issuance authority for MODELED equity recommendations (EI-6A).

Implements the Application's ``EquityRecommendationIssuanceReader`` and
``EquityRecommendationIssuanceWriter`` (ADR-024 sections 4 to 7 and 13; EI-5
design sections 8 and 10; EI-6A SQLite Storage Design v1.0) in the SAME
database file as the EI-4D generation records. MODELED and synthetic only: no
ACTUAL issuance, order, fill, position, lifecycle event or monitoring exists
here. It is an internal persistence capability, wired into no operation, and
calls no provider, broker or network.

Ownership
---------
The Application owns eligibility, identity, the issued record, the writer
rules (``equity_issuance_write_decision``, ``equity_declaration_write_decision``)
and the history semantics (``equity_issuance_session_view``); this adapter
applies them unchanged. It owns durability, transactions, locking, sequence
allocation, rehydration and the database's own guards
(sqlite_equity_recommendation_issuance_schema).

Authorized writes (ADR-024 section 4; ADR-023 pattern)
------------------------------------------------------
The only write entries are ``write_issuance(authorization)`` and
``write_declaration(authorization)``. Each first calls the Application's
public validator; a missing, foreign, unissued or changed authorization raises
UnverifiedEquityRecommendationIssuanceWriteError **before any connection
opens**. There is no other public write.

Writes
------
Each write runs in ONE ``BEGIN IMMEDIATE`` transaction, which takes the write
lock before reading, so SQLite serializes writers:

1. the whole ledger is checked (dense 1..N, every entry with exactly its own
   kind of record, every record with its ledger entry);
2. the stored state the rules need is loaded and re-derived: for an issuance,
   the rows with its identity, generation key or (strategy, listing, date),
   and its session's declarations; for a declaration, its session's
   declarations;
3. the Application's rule decides the outcome;
4. only ISSUED or DECLARED allocates the next sequence and inserts the ledger
   entry, then its record, in that order;
5. the new sequence is confirmed to be the ledger maximum with exactly one
   record, and the transaction commits once. Any other outcome writes nothing.

The first committed record for an identity, generation key or session stays
authoritative; a later conflicting write is reported, never replaces it.

Reads
-----
Every read runs in one read transaction, so a view is built from one
consistent snapshot, and checks the whole ledger first. A stored issuance is
returned only after its full record is re-derived from the EI-4D disposition
it references -- decoded through the EI-4D stored-record decoding, PROPOSED,
evaluator ``/2``, then ``equity_issued_recommendation`` -- and every stored
column equals the re-derived record's. Reading never writes, repairs or
migrates anything.

Failures
--------
- EquityRecommendationIssuanceUnavailableError: the database could not be
  opened or read, is not the generation database, foreign keys could not be
  confirmed, or the write lock was not obtained within the busy timeout;
  nothing was written.
- InvalidStoredEquityRecommendationIssuanceAuthorityError (D6-5): stored
  state is corrupt -- ``ISSUANCE`` (does not decode or re-derive),
  ``DECLARATION`` or ``LEDGER`` (inconsistent, orphaned or invalid). Permanent;
  nothing is written or repaired. A corrupt row under the *offered* issuance
  identity is instead the writer outcome STORED_ISSUANCE_INVALID.
- InvalidEquityRecommendationIssuanceWriteError: the database refused a row
  the rules admitted (a constraint or trigger); rolled back, nothing written.
- EquityIssuanceWatermarkBeyondAuthorityError: a view asked for beyond the
  authority's current sequence.
- EquityRecommendationIssuanceCommitUnconfirmedError: COMMIT itself failed;
  whether the write is durable is unknown, and the Application reconciles by
  reload (EI4-D9-G). It is never evidence of a rollback.

Cleanup -- rolling back or closing a connection that may already have failed
-- never replaces the failure being reported. The adapter never retries,
sleeps, reads a clock or works in the background.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, NoReturn

from northstar_application.application_services import (
    EQUITY_RECOMMENDATION_EVALUATOR_V2,
    EquityIssuanceCompletenessDeclaration,
    EquityIssuedRecommendation,
    EquityRecommendationDispositionKind,
    EquityRecordedCompletenessDeclaration,
    EquityRecordedIssuance,
    InvalidEquityIssuedRecommendationError,
    InvalidEquityRecommendationRecordError,
    VerifiedEquityIssuanceCompletenessDeclaration,
    VerifiedEquityRecommendationIssuance,
    equity_declaration_write_decision,
    equity_issuance_session_view,
    equity_issuance_write_decision,
    equity_issued_recommendation,
    require_verified_equity_issuance_completeness_declaration,
    require_verified_equity_recommendation_issuance,
)
from northstar_application.ports import (
    EquityIssuanceDeclarationOutcome,
    EquityIssuanceDeclarationWrite,
    EquityIssuanceWatermarkBeyondAuthorityError,
    EquityRecommendationIssuanceCommitUnconfirmedError,
    EquityRecommendationIssuanceOutcome,
    EquityRecommendationIssuanceReader,
    EquityRecommendationIssuanceUnavailableError,
    EquityRecommendationIssuanceWrite,
    EquityRecommendationIssuanceWriter,
    EquitySessionRecommendationHistory,
    InvalidEquityRecommendationIssuanceWriteError,
    InvalidStoredEquityRecommendationIssuanceAuthorityError,
)
from northstar_core.domain.value_objects import ListingReference
from northstar_core.foundation.value_objects import ExchangeCode, PointInTime
from northstar_core.strategy import StrategyIdentity

from northstar_infrastructure.persistence.sqlite_equity_recommendation_generation import (
    DISPOSITION_COLUMNS,
    _stored_disposition,  # the EI-4D stored-record decoding, reused unchanged
)
from northstar_infrastructure.persistence.sqlite_equity_recommendation_generation_schema import (  # noqa: E501
    DISPOSITIONS,
)
from northstar_infrastructure.persistence.sqlite_equity_recommendation_issuance_schema import (
    DECLARATION_KIND,
    DECLARATIONS,
    ISSUANCE_KIND,
    ISSUANCES,
    LEDGER,
    initialize_equity_recommendation_issuance_schema,
)

_BUSY_TIMEOUT_SECONDS = 5.0
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_ONE_MICROSECOND = timedelta(microseconds=1)
_DECODE_ERRORS = (KeyError, TypeError, ValueError, AttributeError)
_I = EquityRecommendationIssuanceOutcome
_D = EquityIssuanceDeclarationOutcome
_Integrity = InvalidStoredEquityRecommendationIssuanceAuthorityError

ISSUANCE_COLUMNS = (
    "issuance_id",
    "generation_key",
    "proposal_fingerprint",
    "assessment_fingerprint",
    "issuance_kind",
    "strategy",
    "symbol",
    "exchange_code",
    "trading_date",
    "issued_at",
    "issued_at_us",
)
_ISSUANCE_STORED = (*ISSUANCE_COLUMNS, "recording_sequence", "record_kind")
DECLARATION_COLUMNS = ("exchange_code", "trading_date", "complete_through", "complete_through_us")
_DECLARATION_STORED = ("recording_sequence", "record_kind", *DECLARATION_COLUMNS)

_SELECT_ISSUANCES = f"SELECT {', '.join(_ISSUANCE_STORED)} FROM {ISSUANCES}"  # noqa: S608
_SELECT_DECLARATIONS = f"SELECT {', '.join(_DECLARATION_STORED)} FROM {DECLARATIONS}"  # noqa: S608
_SELECT_DISPOSITION = (
    f"SELECT {', '.join(DISPOSITION_COLUMNS)} FROM {DISPOSITIONS} "  # noqa: S608
    "WHERE generation_key = ?"
)
_INSERT_LEDGER = f"INSERT INTO {LEDGER} (recording_sequence, record_kind) VALUES (?, ?)"  # noqa: S608
_INSERT_ISSUANCE = (
    f"INSERT INTO {ISSUANCES} ({', '.join(_ISSUANCE_STORED)}) "  # noqa: S608
    f"VALUES ({', '.join('?' * len(_ISSUANCE_STORED))})"
)
_INSERT_DECLARATION = (
    f"INSERT INTO {DECLARATIONS} ({', '.join(_DECLARATION_STORED)}) "  # noqa: S608
    f"VALUES ({', '.join('?' * len(_DECLARATION_STORED))})"
)
_SESSION = " WHERE exchange_code = ? AND trading_date = ? ORDER BY recording_sequence"


# ---------------------------------------------------------------------------
# Rows
# ---------------------------------------------------------------------------


def instant_microseconds(point: PointInTime) -> int:
    """Return ``point`` as integer microseconds since 1970-01-01T00:00:00Z, exactly."""
    if not isinstance(point, PointInTime):
        raise TypeError("point must be a PointInTime.")
    moment = datetime.fromisoformat(point.value.replace("Z", "+00:00"))
    return (moment - _EPOCH) // _ONE_MICROSECOND


def issuance_row(issued: EquityIssuedRecommendation) -> tuple[object, ...]:
    """Return the structured columns ``issued`` is stored as, in ISSUANCE_COLUMNS order."""
    if not isinstance(issued, EquityIssuedRecommendation):
        raise TypeError("issued must be an EquityIssuedRecommendation.")
    return (
        issued.issuance_id,
        issued.generation_key,
        issued.proposal_fingerprint,
        issued.assessment_fingerprint,
        issued.issuance_kind.value,
        issued.strategy.identity,
        issued.listing.symbol.value,
        issued.listing.exchange_code.value,
        issued.trading_date.isoformat(),
        issued.issued_at.value,
        instant_microseconds(issued.issued_at),
    )


def declaration_row(declaration: EquityIssuanceCompletenessDeclaration) -> tuple[object, ...]:
    """Return the columns ``declaration`` is stored as, in DECLARATION_COLUMNS order."""
    if not isinstance(declaration, EquityIssuanceCompletenessDeclaration):
        raise TypeError("declaration must be an EquityIssuanceCompletenessDeclaration.")
    return (
        declaration.exchange_code.value,
        declaration.trading_date.isoformat(),
        declaration.complete_through.value,
        instant_microseconds(declaration.complete_through),
    )


def _corrupt(category: str, message: str, issuance_id: object = None) -> NoReturn:
    known = issuance_id if isinstance(issuance_id, str) and issuance_id else None
    raise _Integrity(category, message, issuance_id=known)


def _stored_issuance(connection: sqlite3.Connection, row: Any) -> EquityRecordedIssuance:
    """Re-derive a stored issuance from the disposition it references; every column must match.

    Nothing is repaired: any failure is an ISSUANCE integrity failure naming
    the stored identity.
    """
    stored = tuple(row)
    issuance_id = stored[0]
    found = connection.execute(_SELECT_DISPOSITION, (stored[1],)).fetchone()
    if found is None:
        _corrupt(
            _Integrity.ISSUANCE, "The stored issuance names no stored disposition.", issuance_id
        )
    try:
        disposition = _stored_disposition(found)
    except InvalidEquityRecommendationRecordError as exc:
        raise _Integrity(
            _Integrity.ISSUANCE,
            f"The stored issuance's disposition does not decode ({type(exc).__name__}).",
            issuance_id=issuance_id if isinstance(issuance_id, str) and issuance_id else None,
        ) from exc
    if (
        disposition.kind is not EquityRecommendationDispositionKind.PROPOSED
        or disposition.evaluator_version != EQUITY_RECOMMENDATION_EVALUATOR_V2
    ):
        _corrupt(
            _Integrity.ISSUANCE,
            "The stored issuance names a disposition that is not issuable.",
            issuance_id,
        )
    try:
        derived = equity_issued_recommendation(disposition)
        recorded = EquityRecordedIssuance(derived, stored[len(ISSUANCE_COLUMNS)])
    except InvalidEquityIssuedRecommendationError as exc:
        raise _Integrity(
            _Integrity.ISSUANCE,
            f"The stored issuance does not re-derive ({type(exc).__name__}).",
            issuance_id=issuance_id if isinstance(issuance_id, str) and issuance_id else None,
        ) from exc
    if issuance_row(derived) != stored[: len(ISSUANCE_COLUMNS)]:
        _corrupt(
            _Integrity.ISSUANCE,
            "The stored issuance has columns that disagree with the record its disposition "
            "derives.",
            issuance_id,
        )
    return recorded


def _stored_declaration(row: Any) -> EquityRecordedCompletenessDeclaration:
    """Construct a stored declaration; its text and integer instants must agree."""
    stored = tuple(row)
    try:
        exchange, day, through = stored[2], stored[3], stored[4]
        if not all(isinstance(value, str) for value in (exchange, day, through)):
            raise TypeError("text columns hold other types")
        declaration = EquityIssuanceCompletenessDeclaration(
            ExchangeCode(exchange), date.fromisoformat(day), PointInTime(through)
        )
        if declaration_row(declaration) != stored[2:]:
            raise ValueError("columns disagree with the declaration they state")
        return EquityRecordedCompletenessDeclaration(declaration, stored[0])
    except (*_DECODE_ERRORS, InvalidEquityIssuedRecommendationError) as exc:
        raise _Integrity(
            _Integrity.DECLARATION,
            f"A stored completeness declaration is corrupt ({type(exc).__name__}).",
        ) from exc


# ---------------------------------------------------------------------------
# The ledger (EI-6A design, section 4.3)
# ---------------------------------------------------------------------------

_LEDGER_SHAPE = (
    f"SELECT COUNT(*), MAX(recording_sequence), "  # noqa: S608
    "SUM(CASE WHEN typeof(recording_sequence) <> 'integer' OR recording_sequence < 1 "
    f"OR record_kind NOT IN ('{ISSUANCE_KIND}', '{DECLARATION_KIND}') THEN 1 ELSE 0 END) "
    f"FROM {LEDGER}"
)
_LEDGER_ORPHANS = (
    f"SELECT COUNT(*) FROM {LEDGER} AS l WHERE NOT ("  # noqa: S608
    f"(l.record_kind = '{ISSUANCE_KIND}' AND EXISTS (SELECT 1 FROM {ISSUANCES} AS i "
    "WHERE i.recording_sequence = l.recording_sequence)) OR "
    f"(l.record_kind = '{DECLARATION_KIND}' AND EXISTS (SELECT 1 FROM {DECLARATIONS} AS d "
    "WHERE d.recording_sequence = l.recording_sequence)))"
)
_UNLINKED = (
    "SELECT COUNT(*) FROM {table} AS r WHERE r.record_kind IS NOT '{kind}' "  # noqa: S608
    "OR NOT EXISTS (SELECT 1 FROM " + LEDGER + " AS l "
    "WHERE l.recording_sequence = r.recording_sequence AND l.record_kind = '{kind}')"
)
_UNLINKED_ISSUANCES = _UNLINKED.format(table=ISSUANCES, kind=ISSUANCE_KIND)
_UNLINKED_DECLARATIONS = _UNLINKED.format(table=DECLARATIONS, kind=DECLARATION_KIND)


def _consistent_ledger(connection: sqlite3.Connection) -> int:
    """Return the authority's position N after checking the whole ledger; fail closed.

    The committed sequences must be exactly 1..N, each with exactly its own
    kind of record, and every record must have its ledger entry. A foreign key
    cannot prove the first two directions, so they are checked here, before
    every watermark-establishing read and every write.
    """
    count, greatest, invalid = connection.execute(_LEDGER_SHAPE).fetchone()
    position = 0 if greatest is None else greatest
    if invalid or not isinstance(position, int) or count != position:
        _corrupt(_Integrity.LEDGER, "The recording-sequence ledger is not exactly 1..N.")
    if connection.execute(_LEDGER_ORPHANS).fetchone()[0]:
        _corrupt(_Integrity.LEDGER, "A committed ledger entry has no record of its kind.")
    if (
        connection.execute(_UNLINKED_ISSUANCES).fetchone()[0]
        or connection.execute(_UNLINKED_DECLARATIONS).fetchone()[0]
    ):
        _corrupt(_Integrity.LEDGER, "A stored record has no ledger entry of its kind.")
    return position


def _confirm_recorded(connection: sqlite3.Connection, sequence: int, table: str) -> None:
    """Before COMMIT: the new sequence is the ledger maximum with exactly one record."""
    greatest = connection.execute(f"SELECT MAX(recording_sequence) FROM {LEDGER}").fetchone()[0]  # noqa: S608
    found = connection.execute(
        f"SELECT COUNT(*) FROM {table} WHERE recording_sequence = ?",  # noqa: S608
        (sequence,),
    ).fetchone()[0]
    if greatest != sequence or found != 1:
        _corrupt(
            _Integrity.LEDGER,
            "The allocated sequence is not exactly one new record; nothing is committed.",
        )


# ---------------------------------------------------------------------------
# Connections and transactions
# ---------------------------------------------------------------------------


def _unavailable(exc: BaseException) -> EquityRecommendationIssuanceUnavailableError:
    return EquityRecommendationIssuanceUnavailableError(
        f"The issuance authority is unavailable; nothing was written: {exc}"
    )


def _connect(database_path: Path) -> sqlite3.Connection:
    """Open the existing generation database; never create one.

    Foreign keys are turned on and confirmed on this connection, and the
    issuance-authority objects are added when absent. A database without the
    EI-4D generation tables is refused.
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
        initialize_equity_recommendation_issuance_schema(connection)
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


def _session_declarations(
    connection: sqlite3.Connection, exchange: str, day: str
) -> list[EquityRecordedCompletenessDeclaration]:
    return [
        _stored_declaration(row)
        for row in connection.execute(_SELECT_DECLARATIONS + _SESSION, (exchange, day))
    ]


def _session_issuances(
    connection: sqlite3.Connection, exchange: str, day: str
) -> list[EquityRecordedIssuance]:
    rows = connection.execute(_SELECT_ISSUANCES + _SESSION, (exchange, day)).fetchall()
    return [_stored_issuance(connection, row) for row in rows]


def _require_session(exchange_code: object, trading_date: object) -> tuple[str, str]:
    if not isinstance(exchange_code, ExchangeCode):
        raise TypeError("exchange_code must be an ExchangeCode.")
    if isinstance(trading_date, datetime) or not isinstance(trading_date, date):
        raise TypeError("trading_date must be a plain date.")
    return exchange_code.value, trading_date.isoformat()


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class SQLiteEquityRecommendationIssuanceAuthority(
    EquityRecommendationIssuanceReader, EquityRecommendationIssuanceWriter
):
    """The durable MODELED issuance authority, in the EI-4D generation database.

    ``database_path`` must be the existing database holding the EI-4D
    generation records: there is no second database setting. See the module
    docstring.
    """

    def __init__(self, database_path: str | Path) -> None:
        self._database_path = Path(database_path)

    # -- reads ----------------------------------------------------------------

    def load_issuance(self, issuance_id: str) -> EquityRecordedIssuance | None:
        """Return the recorded issuance with ``issuance_id``, re-derived, or None."""
        if not isinstance(issuance_id, str):
            raise TypeError("issuance_id must be text.")

        def work(connection: sqlite3.Connection) -> EquityRecordedIssuance | None:
            _consistent_ledger(connection)
            row = connection.execute(
                _SELECT_ISSUANCES + " WHERE issuance_id = ?", (issuance_id,)
            ).fetchone()
            return None if row is None else _stored_issuance(connection, row)

        return self._read(work)

    def load_session_issuance(
        self, strategy: StrategyIdentity, listing: ListingReference, trading_date: date
    ) -> EquityRecordedIssuance | None:
        """Return the recorded issuance for the strategy, listing and date, or None."""
        if not isinstance(strategy, StrategyIdentity):
            raise TypeError("strategy must be a StrategyIdentity.")
        if not isinstance(listing, ListingReference):
            raise TypeError("listing must be a ListingReference.")
        exchange, day = _require_session(listing.exchange_code, trading_date)
        key = (strategy.identity, listing.symbol.value, exchange, day)

        def work(connection: sqlite3.Connection) -> EquityRecordedIssuance | None:
            _consistent_ledger(connection)
            row = connection.execute(
                _SELECT_ISSUANCES + " WHERE strategy = ? AND symbol = ? AND exchange_code = ? "
                "AND trading_date = ?",
                key,
            ).fetchone()
            return None if row is None else _stored_issuance(connection, row)

        return self._read(work)

    def session_view(
        self, exchange_code: ExchangeCode, trading_date: date
    ) -> EquitySessionRecommendationHistory | None:
        """Return the session's view at the current position, or None when undeclared."""
        exchange, day = _require_session(exchange_code, trading_date)

        def work(connection: sqlite3.Connection) -> EquitySessionRecommendationHistory | None:
            position = _consistent_ledger(connection)
            return self._view(connection, exchange_code, trading_date, position, exchange, day)

        return self._read(work)

    def session_view_at(
        self, exchange_code: ExchangeCode, trading_date: date, watermark: int
    ) -> EquitySessionRecommendationHistory | None:
        """Return the session's view at ``watermark``; refused beyond the current position."""
        exchange, day = _require_session(exchange_code, trading_date)
        if isinstance(watermark, bool) or not isinstance(watermark, int):
            raise TypeError("watermark must be an integer.")
        if watermark < 0:
            raise ValueError("watermark must be a non-negative integer.")

        def work(connection: sqlite3.Connection) -> EquitySessionRecommendationHistory | None:
            position = _consistent_ledger(connection)
            if watermark > position:
                raise EquityIssuanceWatermarkBeyondAuthorityError(
                    f"Watermark {watermark} is beyond the authority's position {position}."
                )
            return self._view(connection, exchange_code, trading_date, watermark, exchange, day)

        return self._read(work)

    @staticmethod
    def _view(
        connection: sqlite3.Connection,
        exchange_code: ExchangeCode,
        trading_date: date,
        watermark: int,
        exchange: str,
        day: str,
    ) -> EquitySessionRecommendationHistory | None:
        declarations = _session_declarations(connection, exchange, day)
        issuances = _session_issuances(connection, exchange, day)
        return equity_issuance_session_view(
            exchange_code,
            trading_date,
            watermark,
            [recorded.history_record() for recorded in issuances],
            declarations,
        )

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

    # -- authorized writes (ADR-024 section 4) --------------------------------

    def write_issuance(
        self, authorization: VerifiedEquityRecommendationIssuance
    ) -> EquityRecommendationIssuanceWrite:
        """Record exactly the issued recommendation ``authorization`` covers.

        The Application validator runs before any connection opens. See the
        module docstring for the transaction.
        """
        issuance = require_verified_equity_recommendation_issuance(authorization).issuance
        values = issuance_row(issuance)
        own = issuance.issuance_id
        session = values[5:9]

        def work(
            connection: sqlite3.Connection,
        ) -> tuple[EquityRecommendationIssuanceWrite, bool]:
            position = _consistent_ledger(connection)
            held: list[EquityRecordedIssuance] = []
            found = connection.execute(
                _SELECT_ISSUANCES + " WHERE issuance_id = ?", (own,)
            ).fetchone()
            if found is not None:
                try:
                    held.append(_stored_issuance(connection, found))
                except _Integrity:
                    # Governed: never adopted, replaced or repaired (EI-5 design 8.3).
                    return EquityRecommendationIssuanceWrite(
                        _I.STORED_ISSUANCE_INVALID, None
                    ), False
            others = connection.execute(
                _SELECT_ISSUANCES + " WHERE issuance_id <> ? AND (generation_key = ? OR "
                "(strategy = ? AND symbol = ? AND exchange_code = ? AND trading_date = ?)) "
                "ORDER BY recording_sequence",
                (own, issuance.generation_key, *session),
            ).fetchall()
            held += [_stored_issuance(connection, row) for row in others]
            declarations = _session_declarations(connection, session[2], session[3])
            outcome, stored = equity_issuance_write_decision(issuance, held, declarations)
            if outcome is not _I.ISSUED:
                return EquityRecommendationIssuanceWrite(outcome, stored), False
            sequence = position + 1
            connection.execute(_INSERT_LEDGER, (sequence, ISSUANCE_KIND))
            connection.execute(_INSERT_ISSUANCE, (*values, sequence, ISSUANCE_KIND))
            _confirm_recorded(connection, sequence, ISSUANCES)
            recorded = EquityRecordedIssuance(issuance, sequence)
            return EquityRecommendationIssuanceWrite(_I.ISSUED, recorded), True

        return self._write(work)

    def write_declaration(
        self, authorization: VerifiedEquityIssuanceCompletenessDeclaration
    ) -> EquityIssuanceDeclarationWrite:
        """Record exactly the completeness declaration ``authorization`` covers.

        Validated before any connection opens, as ``write_issuance``.
        """
        declaration = require_verified_equity_issuance_completeness_declaration(
            authorization
        ).declaration
        values = declaration_row(declaration)

        def work(connection: sqlite3.Connection) -> tuple[EquityIssuanceDeclarationWrite, bool]:
            position = _consistent_ledger(connection)
            held = _session_declarations(connection, values[0], values[1])
            outcome, recorded = equity_declaration_write_decision(declaration, held)
            if outcome is not _D.DECLARED:
                assert recorded is not None
                return EquityIssuanceDeclarationWrite(outcome, recorded), False
            sequence = position + 1
            connection.execute(_INSERT_LEDGER, (sequence, DECLARATION_KIND))
            connection.execute(_INSERT_DECLARATION, (sequence, DECLARATION_KIND, *values))
            _confirm_recorded(connection, sequence, DECLARATIONS)
            written = EquityRecordedCompletenessDeclaration(declaration, sequence)
            return EquityIssuanceDeclarationWrite(_D.DECLARED, written), True

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
                raise InvalidEquityRecommendationIssuanceWriteError(
                    f"The database refused the write; nothing was written: {exc}"
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
                    raise EquityRecommendationIssuanceCommitUnconfirmedError(
                        "COMMIT failed; whether the issuance-authority write is durable is "
                        f"unknown: {exc}"
                    ) from exc
        except BaseException:
            _discard(connection)
            raise
        _close(connection)
        return written
