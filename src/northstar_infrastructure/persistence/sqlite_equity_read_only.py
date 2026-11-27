"""Strictly read-only SQLite connections for the EI-7 equity readers (EI-7D).

The EI-7 private read path inspects persisted synthetic equity state; it never
writes it (EI-7 Decision Register, section 13.9; finding A-5). The existing
EI-2B to EI-6B adapters open their connections read-write and initialize their
schemas -- DDL and a commit -- on every call, so none of their connection paths
is used here. This module is the one connection path of the EI-7D readers. It
follows the accepted EI-6C snapshot-catalog precedent.

Enforcement
-----------
- The database is opened through a SQLite URI built from the resolved absolute
  path by ``Path.as_uri`` (percent-encoded, so spaces, ``#``, ``%``, ``&``,
  ``=`` and Windows drive paths are exact), with ``mode=ro``: SQLite opens the
  database file read-only and can neither create a missing file nor write the
  existing one, whatever is executed on the connection. A path that cannot be
  resolved (for example one holding a NUL character) is refused as
  unavailability before anything is opened.
- ``PRAGMA query_only = ON`` is then set and **confirmed**: the connection
  refuses every statement that would change the database, including DDL and
  schema-changing pragmas. Either guard alone refuses writes; both are held.
- Nothing here creates, initializes, migrates, repairs, checkpoints or vacuums
  anything, changes the journal mode, or reads a clock. There is no fallback to
  ``mode=rw`` or ``mode=rwc`` and no ``immutable`` flag: locking and recovery
  are never bypassed.

Journal modes
-------------
The accepted writers run SQLite's rollback journal (``delete``), the mode these
guarantees are verified under. Two cases a strictly read-only connection cannot
serve without changing persisted state are refused as unavailability:

- **A hot rollback journal** (a writer stopped mid-transaction): SQLite itself
  refuses the read with ``SQLITE_READONLY_ROLLBACK`` and leaves the database
  and journal files untouched (EI-6C L8). A later read-write connection of an
  accepted writer recovers it; until then every read is unavailable.
- **A write-ahead-log database**: any SQLite read of a WAL database -- even
  ``PRAGMA journal_mode`` -- opens its ``-wal`` and ``-shm`` files, creating
  them when absent and leaving them behind, which is persistent state a reader
  must not create. The database header (bytes 18 and 19, the file-format
  write and read versions: 1 rollback journal, 2 WAL) is therefore read
  before SQLite opens the file, and anything other than the rollback journal
  is refused. No accepted writer selects WAL. A file shorter than the
  100-byte header, or without the SQLite magic, is left to SQLite, which
  refuses it (SQLITE_CORRUPT or SQLITE_NOTADB) without creating anything.

What is and is not claimed
--------------------------
The non-mutation guarantee is the one verified under the rollback journal:
the database bytes, its schema and its journal are never changed and no
auxiliary file is created by a reader. It is not an unconditional claim about
the filesystem:

- **Header-check-to-open race.** The header is read, then SQLite opens the
  file. A writer that switched the database to WAL in between (no accepted
  writer does) would be read in WAL mode, and SQLite could then create its
  ``-wal`` and ``-shm`` files. The residual mode-transition risk is a CF5
  decision: "The initial Equity read runtime supports the verified
  rollback-journal operating model. WAL-mode databases are refused by the
  EI-7D readers. The supported deployment journal policy and residual
  mode-transition risk must be finalized under CF5 before EI-7E exposure."
- Opening a file for reading may update filesystem metadata such as its
  access time, which is outside SQLite and outside this guarantee.

Engine-level failures
---------------------
Since SI-B (EI7-SI1, approved 2026-11-28), ``classify_read_failure``
classifies every ``sqlite3.Error`` of the shared transaction by its SQLite
**result code** -- the primary code in the low eight bits of an extended
code -- never by message, error name or path:

- SQLITE_CORRUPT, any extended variant: the Application's permanent
  ``EquityReadStorageIntegrityError(CORRUPT)``;
- SQLITE_NOTADB: ``EquityReadStorageIntegrityError(UNUSABLE)`` -- the
  configured target is not a usable store, whether damaged or misconfigured;
- everything else -- SQLITE_BUSY, SQLITE_LOCKED, SQLITE_CANTOPEN,
  SQLITE_READONLY_ROLLBACK (a hot journal), I/O errors, a missing table -- and
  any error without a usable integer code, including this module's pre-open
  refusals: the calling reader's typed unavailability, with SQLite's error
  name in its message.

The permanent error is the Application-owned contract (SI-A), imported from
``northstar_application.ports``, with the engine error as its ``__cause__``
and only its fixed text as its message; it names no record, since the
storage could not establish one. The accepted EI-4D, EI-6A and EI-6B writer
adapters are unchanged. Corruption on a page no query of the call touches is
not detected (no full ``PRAGMA integrity_check`` is run; EI7-SI2).

Transactions
------------
``read_only_transaction(database_path, work, unavailable)`` opens one fresh
connection, runs ``work`` inside one ``BEGIN`` ... ``ROLLBACK`` read
transaction, so every query of ``work`` sees one consistent database state,
and closes the connection. Nothing is ever committed. A ``sqlite3.Error``
anywhere -- opening, confirming read-only mode, ``BEGIN``, a query, ending the
transaction or closing -- is reported as ``classify_read_failure(exc,
unavailable)``: the permanent storage contract or the caller's typed
unavailability, as above; any other failure (a typed integrity failure of the
caller's verified decoding, an interrupt) propagates unchanged. On every
failure the transaction is rolled back and the connection closed by the
accepted EI-2B cleanup helpers, which never replace the failure being
reported. A read that cannot end cleanly returns nothing.

Failure messages name no database path and carry no stored content.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from pathlib import Path

from northstar_application.ports import (
    EquityReadStorageIntegrityCondition,
    EquityReadStorageIntegrityError,
)

from northstar_infrastructure.persistence.sqlite_equity_candle_evidence import (
    _discard,  # EI-2B cleanup that never masks the failure being reported, reused unchanged
    _rollback,
)

READ_ONLY_BUSY_TIMEOUT_SECONDS = 5.0
_HEADER_MAGIC = b"SQLite format 3\x00"
_HEADER_SIZE = 100
_ROLLBACK_JOURNAL = 1  # the file-format write and read version of the rollback journal


class ReadOnlyDatabaseRefusedError(sqlite3.OperationalError):
    """A database a strictly read-only connection must not open; reported as unavailability.

    Its message names no path and carries no stored content.
    """


def read_only_uri(database_path: Path) -> str:
    """The ``mode=ro`` SQLite URI of ``database_path``, percent-encoded from its absolute path."""
    if not isinstance(database_path, Path):
        raise TypeError("database_path must be a Path.")
    return f"{database_path.resolve().as_uri()}?mode=ro"


def _require_rollback_journal(database_path: Path) -> None:
    """Refuse a database whose header is not the rollback journal's, before SQLite opens it."""
    try:
        with database_path.open("rb") as handle:
            header = handle.read(_HEADER_SIZE)
    except (OSError, ValueError) as exc:
        raise ReadOnlyDatabaseRefusedError(
            f"The database file cannot be opened for reading ({type(exc).__name__})."
        ) from exc
    if (
        len(header) == _HEADER_SIZE
        and header.startswith(_HEADER_MAGIC)
        and (header[18], header[19]) != (_ROLLBACK_JOURNAL, _ROLLBACK_JOURNAL)
    ):
        raise ReadOnlyDatabaseRefusedError(
            "The database is not in rollback-journal mode; a read-only reader would create "
            "write-ahead-log files, so nothing was read."
        )


def connect_read_only(database_path: Path) -> sqlite3.Connection:
    """Open the existing database read-only, with ``query_only`` confirmed; never create it.

    Raises sqlite3.Error (ReadOnlyDatabaseRefusedError for an unresolvable
    path, a refused journal mode or an unreadable file); a connection that was
    opened is closed first.
    """
    try:
        uri = read_only_uri(database_path)
    except (OSError, ValueError) as exc:
        raise ReadOnlyDatabaseRefusedError(
            f"The database path cannot be resolved ({type(exc).__name__})."
        ) from exc
    _require_rollback_journal(database_path)
    connection = sqlite3.connect(
        uri, timeout=READ_ONLY_BUSY_TIMEOUT_SECONDS, isolation_level=None, uri=True
    )
    try:
        connection.execute("PRAGMA query_only = ON")
        if connection.execute("PRAGMA query_only").fetchone() != (1,):
            raise ReadOnlyDatabaseRefusedError("Read-only mode could not be confirmed.")
    except BaseException:
        _discard(connection)
        raise
    return connection


def sqlite_failure_name(exc: sqlite3.Error) -> str:
    """A path-free description of a SQLite failure: its error name and SQLite's message."""
    if isinstance(exc, ReadOnlyDatabaseRefusedError):
        return str(exc)
    name = getattr(exc, "sqlite_errorname", None) or type(exc).__name__
    return f"{name}: {exc}"


_PERMANENT_STORAGE_FAILURES = {
    sqlite3.SQLITE_CORRUPT: EquityReadStorageIntegrityCondition.CORRUPT,
    sqlite3.SQLITE_NOTADB: EquityReadStorageIntegrityCondition.UNUSABLE,
}


def classify_read_failure(
    exc: sqlite3.Error, unavailable: Callable[[sqlite3.Error], Exception]
) -> Exception:
    """The failure a reader reports for ``exc``: permanent storage, or its family's outage.

    By the SQLite **result code** alone (EI7-SI1; SI-B): an extended code
    carries its primary code in the low eight bits, so every SQLITE_CORRUPT
    variant is the Application's permanent ``EquityReadStorageIntegrityError``
    (CORRUPT) and SQLITE_NOTADB is UNUSABLE. Every other code -- busy, locked,
    cannot open, read-only (a hot journal), I/O, a missing table -- and any
    error without a usable integer code, including this module's own pre-open
    refusals, stays ``unavailable(exc)``. No message, name or path is read.
    """
    code = getattr(exc, "sqlite_errorcode", None)
    if (
        not isinstance(exc, ReadOnlyDatabaseRefusedError)
        and isinstance(code, int)
        and not isinstance(code, bool)
        and code >= 0
    ):
        condition = _PERMANENT_STORAGE_FAILURES.get(code & 0xFF)
        if condition is not None:
            return EquityReadStorageIntegrityError(condition)
    return unavailable(exc)


def read_only_transaction[T](
    database_path: Path,
    work: Callable[[sqlite3.Connection], T],
    unavailable: Callable[[sqlite3.Error], Exception],
) -> T:
    """Run ``work`` in one read transaction on a fresh read-only connection; see the module."""
    try:
        connection = connect_read_only(database_path)
    except sqlite3.Error as exc:
        raise classify_read_failure(exc, unavailable) from exc
    try:
        try:
            connection.execute("BEGIN")
            result = work(connection)
            connection.execute("ROLLBACK")
        except sqlite3.Error as exc:
            raise classify_read_failure(exc, unavailable) from exc
    except BaseException:
        _rollback(connection)
        _discard(connection)
        raise
    try:
        connection.close()
    except sqlite3.Error as exc:
        raise classify_read_failure(exc, unavailable) from exc
    return result
