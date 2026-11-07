"""SQLite store of immutable equity recommendation-generation records (EI-4D-B).

Implements the Application's EquityRecommendationGenerationStore under the
owner decisions EI4-D9-A to EI4-D9-J (EI-4 Decision Register, section 9,
approved 2026-11-07) and ADR-021. Synthetic generation records only: a stored
PROPOSED disposition is never an issued recommendation, never a lifecycle
state and never written to the issued-recommendation history (EI4-D9-H,
EI4-D9-J). It is an internal persistence capability, wired into no
operation, and calls no provider, NSE, broker or network.

One database authority
----------------------
It uses the SAME database file as the EI-2B evidence and the EI-3C research
timelines: dispositions reference their CANDIDATE entry and snapshot by real
foreign keys (sqlite_equity_recommendation_generation_schema). It never
creates a database file, and it refuses a database that does not already hold
the EI-2B and EI-3C tables, so it can never write beside an empty copy of the
evidence. Every connection turns foreign keys on and confirms they are on.

Records, not decisions
----------------------
Records are the Application's EquityRecommendationGenerationDisposition and
EquityRecommendationGenerationAttempt; their construction decodes and
verifies every document and fingerprint. A stored row is returned only by
constructing its record and confirming that every structured column is what
that record produces; otherwise InvalidEquityRecommendationRecordError -- or
IncompatibleEquityRecommendationRecordError for an unsupported format or
evaluator version -- is raised. Nothing is repaired, migrated or skipped.
Whether a record rests on authoritative evidence and reproduces is decided by
the Application's verifier, never here.

Writes
------
``record_generation`` and ``record_attempt`` each run in ONE ``BEGIN
IMMEDIATE`` transaction, which takes the write lock before reading, so SQLite
serializes writers (EI4-D9-E, EI4-D9-F). For ``record_generation``:

1. the disposition stored under the key is read and constructed;
2. none: the disposition -- with its proposal, in the same row -- is inserted;
3. one equal to the offered record (whole-record equality: both fingerprints,
   both documents, the kind AND the evaluator version, which no fingerprint
   covers): nothing is written for it -- EQUIVALENT;
4. any other: it is never replaced -- CONFLICT;
5. the attempt is inserted unless its identity is already stored, which must
   then hold the same attempt;
6. the transaction commits once.

``record_attempt`` inserts an attempt-only result unless already stored; it
never creates a disposition. The schema's constraints and triggers check
every inserted row again; uniqueness is the database's, not this module's.

Failures
--------
- EquityRecommendationGenerationStoreUnavailableError: the database could
  not be opened or read, or is not the evidence database; nothing was written.
- EquityRecommendationGenerationNotCommittedError: the transaction ended
  before COMMIT -- the write lock not obtained within five seconds, or a row
  refused; nothing was written.
- InvalidEquityRecommendationRecordError (or its Incompatible subclass): a
  stored row does not decode; the transaction was rolled back.
- EquityRecommendationGenerationCommitUnconfirmedError: COMMIT itself failed.
  SQLite may or may not have made the transaction durable; nothing is
  asserted, and the Application reconciles by reload (EI4-D9-G).

The store never retries, sleeps, reads a clock or works in the background.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable
from contextlib import closing
from pathlib import Path
from typing import Any, NoReturn

from northstar_application.application_services import (
    EquityRecommendationDispositionKind,
    EquityRecommendationGenerationAttempt,
    EquityRecommendationGenerationDisposition,
    InvalidEquityRecommendationRecordError,
)
from northstar_application.ports import (
    EquityRecommendationGenerationCommitUnconfirmedError,
    EquityRecommendationGenerationNotCommittedError,
    EquityRecommendationGenerationStore,
    EquityRecommendationGenerationStoreUnavailableError,
    EquityRecommendationStoreOutcome,
    EquityRecommendationStoreWrite,
)

from northstar_infrastructure.persistence.sqlite_equity_candle_evidence_schema import (
    initialize_equity_candle_evidence_schema,
)
from northstar_infrastructure.persistence.sqlite_equity_intraday_research_timeline_schema import (  # noqa: E501
    initialize_equity_intraday_research_timeline_schema,
)
from northstar_infrastructure.persistence.sqlite_equity_recommendation_generation_schema import (  # noqa: E501
    ATTEMPTS,
    DISPOSITIONS,
    initialize_equity_recommendation_generation_schema,
)

_BUSY_TIMEOUT_SECONDS = 5.0
_DECODE_ERRORS = (KeyError, TypeError, ValueError, AttributeError)
_OUTCOME = EquityRecommendationStoreOutcome

DISPOSITION_COLUMNS = (
    "generation_key",
    "disposition_kind",
    "evaluator_version",
    "assessment_format",
    "assessment_fingerprint",
    "assessment_document",
    "proposal_format",
    "proposal_fingerprint",
    "proposal_document",
    "inputs_fingerprint",
    "policy_set_version",
    "policy_set_fingerprint",
    "strategy",
    "research_configuration_version",
    "symbol",
    "exchange_code",
    "trading_date",
    "decision_as_of",
    "research_decision_fingerprint",
    "candidate_entry_fingerprint",
    "candidate_sequence",
    "evidence_snapshot_id",
    "manifest_fingerprint",
)
ATTEMPT_COLUMNS = (
    "attempt_identity",
    "attempt_format",
    "attempt_document",
    "status",
    "strategy",
    "symbol",
    "exchange_code",
    "trading_date",
    "policy_set_version",
    "generation_key",
    "disposition_kind",
    "assessment_fingerprint",
    "disposition_key",
)
_SELECT_DISPOSITION = (
    f"SELECT {', '.join(DISPOSITION_COLUMNS)} FROM {DISPOSITIONS} "  # noqa: S608
    "WHERE generation_key = ?"
)
_INSERT_DISPOSITION = (
    f"INSERT INTO {DISPOSITIONS} ({', '.join(DISPOSITION_COLUMNS)}) "  # noqa: S608
    f"VALUES ({', '.join('?' * len(DISPOSITION_COLUMNS))})"
)
_SELECT_ATTEMPT = (
    f"SELECT {', '.join(ATTEMPT_COLUMNS)} FROM {ATTEMPTS} "  # noqa: S608
    "WHERE attempt_identity = ?"
)
_INSERT_ATTEMPT = (
    f"INSERT INTO {ATTEMPTS} ({', '.join(ATTEMPT_COLUMNS)}) "  # noqa: S608
    f"VALUES ({', '.join('?' * len(ATTEMPT_COLUMNS))})"
)


def _invalid(message: str) -> NoReturn:
    raise InvalidEquityRecommendationRecordError(message)


# ---------------------------------------------------------------------------
# Rows
# ---------------------------------------------------------------------------


def disposition_row(
    disposition: EquityRecommendationGenerationDisposition,
) -> tuple[object, ...]:
    """Return the row ``disposition`` is stored as, in DISPOSITION_COLUMNS order.

    Every structured column is read from the record's verified assessment
    document; the documents themselves are kept as their exact canonical text.
    """
    if not isinstance(disposition, EquityRecommendationGenerationDisposition):
        raise TypeError("disposition must be an EquityRecommendationGenerationDisposition.")
    document = disposition.assessment.document
    inputs, request = document["inputs"], document["request"]
    candidate = inputs["candidate"]
    source = candidate["source"]
    proposal = document["proposal"]
    return (
        disposition.generation_key,
        disposition.kind.value,
        disposition.evaluator_version,
        document["format"],
        disposition.assessment_fingerprint,
        disposition.assessment_document,
        None if proposal is None else proposal["format"],
        disposition.proposal_fingerprint,
        disposition.proposal_document,
        document["inputs_fingerprint"],
        inputs["policy_set"]["reference"]["version"],
        inputs["policy_set_fingerprint"],
        request["strategy"],
        source["research_configuration_version"],
        request["listing"]["symbol"],
        request["listing"]["exchange_code"],
        request["trading_date"],
        source["decision_as_of"],
        source["research_decision_fingerprint"],
        source["candidate_entry_fingerprint"],
        candidate["candidate_sequence"],
        source["evidence_snapshot_id"],
        candidate["manifest_fingerprint"],
    )


def attempt_row(attempt: EquityRecommendationGenerationAttempt) -> tuple[object, ...]:
    """Return the row ``attempt`` is stored as, in ATTEMPT_COLUMNS order."""
    if not isinstance(attempt, EquityRecommendationGenerationAttempt):
        raise TypeError("attempt must be an EquityRecommendationGenerationAttempt.")
    document = json.loads(attempt.attempt_document)
    request = document["request"]
    kind = attempt.disposition_kind
    return (
        attempt.attempt_identity,
        document["format"],
        attempt.attempt_document,
        attempt.status.value,
        request["strategy"],
        request["listing"]["symbol"],
        request["listing"]["exchange_code"],
        request["trading_date"],
        request["policy_set"]["version"],
        attempt.generation_key,
        None if kind is None else kind.value,
        attempt.assessment_fingerprint,
        None if kind is None else attempt.generation_key,
    )


def _decoded[T](build: Callable[[], T], subject: str) -> T:
    """Construct a record from stored values; any failure is an invalid stored record."""
    try:
        return build()
    except InvalidEquityRecommendationRecordError:
        raise
    except _DECODE_ERRORS as exc:
        raise InvalidEquityRecommendationRecordError(f"{subject} does not decode: {exc}") from exc


def _stored_disposition(row: Any) -> EquityRecommendationGenerationDisposition:
    """Construct the stored disposition; every column must be what it produces."""
    stored = tuple(row)

    def build() -> EquityRecommendationGenerationDisposition:
        record = EquityRecommendationGenerationDisposition(
            generation_key=stored[0],
            kind=EquityRecommendationDispositionKind(stored[1]),
            evaluator_version=stored[2],
            assessment_fingerprint=stored[4],
            assessment_document=stored[5],
            proposal_fingerprint=stored[7],
            proposal_document=stored[8],
        )
        if disposition_row(record) != stored:
            _invalid("The stored disposition has columns that disagree with its documents.")
        return record

    return _decoded(build, "The stored disposition")


def _stored_attempt(row: Any) -> EquityRecommendationGenerationAttempt:
    """Construct the stored attempt; every column must be what it produces."""
    stored = tuple(row)

    def build() -> EquityRecommendationGenerationAttempt:
        record = EquityRecommendationGenerationAttempt(
            attempt_identity=stored[0], attempt_document=stored[2]
        )
        if attempt_row(record) != stored:
            _invalid("The stored attempt has columns that disagree with its document.")
        return record

    return _decoded(build, "The stored attempt")


# ---------------------------------------------------------------------------
# Connections and transactions
# ---------------------------------------------------------------------------


def _unavailable(exc: BaseException) -> EquityRecommendationGenerationStoreUnavailableError:
    return EquityRecommendationGenerationStoreUnavailableError(
        f"Generation storage is unavailable; nothing was written: {exc}"
    )


def _connect(database_path: Path) -> sqlite3.Connection:
    """Open the existing evidence database; never create one.

    Foreign keys are turned on and confirmed on this connection. The EI-2B and
    EI-3C initializers only restore their own missing indexes or triggers --
    their tables must already exist -- and then the generation schema is
    added when absent.
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
        initialize_equity_recommendation_generation_schema(connection)
        initialize_equity_candle_evidence_schema(connection)
        initialize_equity_intraday_research_timeline_schema(connection)
    except sqlite3.Error as exc:
        connection.close()
        raise _unavailable(exc) from exc
    return connection


def _commit(connection: sqlite3.Connection) -> None:
    """Commit the open transaction: the one boundary whose failure is unconfirmed."""
    connection.commit()


def _insert_disposition(connection: sqlite3.Connection, values: tuple[object, ...]) -> None:
    connection.execute(_INSERT_DISPOSITION, values)


def _insert_attempt(connection: sqlite3.Connection, values: tuple[object, ...]) -> None:
    connection.execute(_INSERT_ATTEMPT, values)


def _rollback(connection: sqlite3.Connection) -> None:
    if connection.in_transaction:
        try:
            connection.rollback()
        except sqlite3.Error:
            pass  # Closing the connection rolls the transaction back.


def _attempt_unless_stored(
    connection: sqlite3.Connection,
    attempt: EquityRecommendationGenerationAttempt,
    values: tuple[object, ...],
) -> bool:
    """Insert ``attempt`` unless its identity is stored; return whether it was inserted."""
    row = connection.execute(_SELECT_ATTEMPT, (attempt.attempt_identity,)).fetchone()
    if row is None:
        _insert_attempt(connection, values)
        return True
    if _stored_attempt(row) != attempt:
        _invalid(f"Another attempt is stored under {attempt.attempt_identity}.")
    return False


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class SQLiteEquityRecommendationGenerationStore(EquityRecommendationGenerationStore):
    """Insert-only SQLite store of generation dispositions and logical attempts.

    ``database_path`` must be the existing EI-2B and EI-3C evidence database:
    there is no second database setting.
    """

    def __init__(self, database_path: str | Path) -> None:
        self._database_path = Path(database_path)

    # -- reads ----------------------------------------------------------------

    def load_disposition(
        self, generation_key: str
    ) -> EquityRecommendationGenerationDisposition | None:
        """Return the committed disposition for ``generation_key``, or None."""
        if not isinstance(generation_key, str):
            raise TypeError("generation_key must be text.")
        row = self._read(_SELECT_DISPOSITION, generation_key)
        return None if row is None else _stored_disposition(row)

    def load_attempt(self, attempt_identity: str) -> EquityRecommendationGenerationAttempt | None:
        """Return the committed attempt with ``attempt_identity``, or None."""
        if not isinstance(attempt_identity, str):
            raise TypeError("attempt_identity must be text.")
        row = self._read(_SELECT_ATTEMPT, attempt_identity)
        return None if row is None else _stored_attempt(row)

    def _read(self, statement: str, key: str) -> Any:
        try:
            with closing(_connect(self._database_path)) as connection:
                return connection.execute(statement, (key,)).fetchone()
        except sqlite3.Error as exc:
            raise _unavailable(exc) from exc

    # -- writes ---------------------------------------------------------------

    def record_generation(
        self,
        disposition: EquityRecommendationGenerationDisposition,
        attempt: EquityRecommendationGenerationAttempt,
    ) -> EquityRecommendationStoreWrite:
        """Atomically record ``disposition`` (with its proposal) and ``attempt``."""
        disposition_values = disposition_row(disposition)
        attempt_values = attempt_row(attempt)
        if (attempt.generation_key, attempt.assessment_fingerprint, attempt.disposition_kind) != (
            disposition.generation_key,
            disposition.assessment_fingerprint,
            disposition.kind,
        ):
            raise ValueError("The attempt is not the attempt of this disposition.")

        def work(connection: sqlite3.Connection) -> EquityRecommendationStoreWrite:
            row = connection.execute(_SELECT_DISPOSITION, (disposition.generation_key,)).fetchone()
            if row is None:
                _insert_disposition(connection, disposition_values)
                inserted = _attempt_unless_stored(connection, attempt, attempt_values)
                return EquityRecommendationStoreWrite(_OUTCOME.COMMITTED, None, inserted)
            stored = _stored_disposition(row)
            inserted = _attempt_unless_stored(connection, attempt, attempt_values)
            outcome = _OUTCOME.EQUIVALENT if stored == disposition else _OUTCOME.CONFLICT
            return EquityRecommendationStoreWrite(outcome, stored, inserted)

        return self._write(work)

    def record_attempt(
        self, attempt: EquityRecommendationGenerationAttempt
    ) -> EquityRecommendationStoreWrite:
        """Record an attempt-only result; never a disposition."""
        values = attempt_row(attempt)
        if attempt.disposition_kind is not None:
            raise ValueError("An attempt with a disposition kind is recorded with its disposition.")

        def work(connection: sqlite3.Connection) -> EquityRecommendationStoreWrite:
            inserted = _attempt_unless_stored(connection, attempt, values)
            outcome = _OUTCOME.COMMITTED if inserted else _OUTCOME.EQUIVALENT
            return EquityRecommendationStoreWrite(outcome, None, inserted)

        return self._write(work)

    def _write(
        self, work: Callable[[sqlite3.Connection], EquityRecommendationStoreWrite]
    ) -> EquityRecommendationStoreWrite:
        """Run ``work`` in one BEGIN IMMEDIATE transaction and commit it once."""
        connection = _connect(self._database_path)
        try:
            try:
                connection.execute("BEGIN IMMEDIATE")
                written = work(connection)
            except sqlite3.Error as exc:
                _rollback(connection)
                raise EquityRecommendationGenerationNotCommittedError(
                    f"Generation storage refused the write; nothing was written: {exc}"
                ) from exc
            except BaseException:
                _rollback(connection)
                raise
            try:
                _commit(connection)
            except Exception as exc:
                _rollback(connection)
                raise EquityRecommendationGenerationCommitUnconfirmedError(
                    f"COMMIT failed; whether the generation records are durable is unknown: {exc}"
                ) from exc
            return written
        finally:
            connection.close()
