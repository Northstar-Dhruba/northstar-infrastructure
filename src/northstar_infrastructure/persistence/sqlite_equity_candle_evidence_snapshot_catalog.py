"""Read-only SQLite catalog of EI-2B evidence snapshots for lifecycle monitoring (EI-6C).

Implements the Application's ``EquityCandleEvidenceSnapshotCatalog`` (EI-6C
Lifecycle Monitoring and Snapshot Selection Design v1.0, sections 4, 5.1, 5.2
and 6.4) over the existing EI-2B table ``equity_candle_evidence_snapshots``
and its scope index. No schema, table, index, trigger or column is added. It
is an internal persistence capability, wired into no operation, and calls no
provider, broker or network.

Read-only
---------
Every connection is opened with ``mode=ro`` -- it can neither create a
missing file nor write -- and ``PRAGMA query_only`` is confirmed on. The EI-2B
adapter's connection path initializes its schema (DDL and a commit), so the
catalog never uses it. Nothing here inserts, updates, deletes, migrates,
repairs, checkpoints or reads a clock.

``list_snapshots(scope, monitoring_instant, limit)``
----------------------------------------------------
1. **One read transaction fixes the answer.** Inside one ``BEGIN`` ...
   ``ROLLBACK`` the snapshot, entry and journal tables must exist, and every
   row of the scope is read through the scope index: symbol, exchange code
   and trading date, then timeframe, window policy version and evidence class
   by equality, all as bound parameters. Every such row is checked, whatever
   its instants: its identity and manifest fingerprint must be text, its
   ``as_of`` and ``issued_at`` canonical PointInTime text, its watermark a
   non-negative integer, and its ``issued_at`` must not precede its
   ``as_of`` (the EI-2B invariant). A malformed row of the scope is
   ``METADATA``; it is never skipped, because its eligibility cannot be known.
2. **Eligibility** is decided on parsed instants (``PointInTime.compare``),
   never on stored text: ``as_of <= T`` **and** ``issued_at <= T``; equality
   is eligible. Stored text with fractional seconds does not sort
   chronologically. ``issued_at`` is declared by the issuing caller, not
   proven (design section 5.1).
3. **The limit counts eligible rows**, after both guards and before any
   grouping or selection. Every row of the scope is read; more than ``limit``
   eligible rows raises EquityCandleEvidenceSnapshotCatalogLimitExceededError
   and returns nothing. No SQL ``LIMIT`` is used, and no prefix is returned.
4. **Every eligible row is verified** by the EI-2B verified decoding
   (``_stored_snapshot``, reused unchanged): the document re-derives its
   identity and manifest fingerprint, every column agrees with it, its entry
   rows agree, and the journal at its watermark holds exactly its evidence.
   The verified snapshot must then restate the claim read in step 1 and the
   requested scope. Each verification is its own short read transaction, so
   no lock is held across the whole check; the snapshot, entry and journal
   rows are append-only under the EI-2B triggers, so the row verified is the
   row listed. A failure is ``INTEGRITY`` (``METADATA`` when the stored
   document does not decode, ``INCOMPLETE`` when a listed row is gone).
5. Entries are built from the verified snapshots and returned ordered by
   parsed ``as_of``, then ``snapshot_id``. Same-instant rows are all
   returned: none is deduplicated, chosen or dropped -- selection belongs to
   the Application.

Consistency guarantee
---------------------
The set, the count and the claims come from one read transaction. A snapshot
committed after it is not in the answer; a snapshot whose transaction has not
committed, or rolled back, is never seen. The monitor's later
``get_snapshot`` calls use their own connections: they re-read the same
append-only rows, and any row whose content changed (only possible by
bypassing the triggers) fails verification closed. Nothing more is claimed.

Failures
--------
- EquityCandleEvidenceSnapshotCatalogUnavailableError (retryable): the file
  is missing or cannot be opened, the evidence tables are absent, the
  database is locked or busy beyond the five-second busy timeout, read-only
  mode could not be confirmed, or another SQLite error occurred. Nothing is
  claimed about the contents.
- InvalidEquityCandleEvidenceSnapshotCatalogError (permanent):
  ``METADATA``, ``INTEGRITY`` or ``INCOMPLETE`` as above; SQLite's own
  ``SQLITE_CORRUPT`` or ``SQLITE_NOTADB`` is ``INCOMPLETE``.
- EquityCandleEvidenceSnapshotCatalogLimitExceededError: step 3.

Cleanup -- rolling back or closing a connection that may already have failed
-- never replaces the failure being reported. Messages name no file path.
"""

from __future__ import annotations

import json
import sqlite3
from functools import cmp_to_key
from pathlib import Path
from typing import Any, NoReturn

from northstar_application.application_services import (
    EquityCandleEvidenceSnapshot,
    equity_candle_snapshot_from_document,
)
from northstar_application.ports import (
    EquityCandleEvidenceSnapshotCatalog,
    EquityCandleEvidenceSnapshotCatalogEntry,
    EquityCandleEvidenceSnapshotCatalogLimitExceededError,
    EquityCandleEvidenceSnapshotCatalogScope,
    EquityCandleEvidenceSnapshotCatalogUnavailableError,
    EquityCandleSnapshotEvidenceError,
    InvalidEquityCandleEvidenceSnapshotCatalogError,
)
from northstar_core.foundation.value_objects import PointInTime

from northstar_infrastructure.persistence.sqlite_equity_candle_evidence import (
    _DECODE_ERRORS,
    _discard,  # EI-2B cleanup that never masks the failure being reported, reused unchanged
    _rollback,
    _stored_snapshot,  # the EI-2B verified snapshot decoding, reused unchanged
)

_BUSY_TIMEOUT_SECONDS = 5.0
_Invalid = InvalidEquityCandleEvidenceSnapshotCatalogError
_CORRUPT = frozenset({sqlite3.SQLITE_CORRUPT, sqlite3.SQLITE_NOTADB})

EVIDENCE_TABLES = (
    "equity_candle_journal",
    "equity_candle_evidence_snapshots",
    "equity_candle_evidence_snapshot_entries",
)
_TABLE_COUNT = "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name IN (?, ?, ?)"
_SCOPE_ROWS = (
    "SELECT snapshot_id, manifest_fingerprint, as_of, issued_at, watermark "
    "FROM equity_candle_evidence_snapshots "
    "WHERE symbol = ? AND exchange_code = ? AND trading_date = ? "
    "AND timeframe = ? AND window_policy_version = ? AND evidence_class = ? "
    "ORDER BY snapshot_id"
)
_SELECT_DOCUMENT = "SELECT document FROM equity_candle_evidence_snapshots WHERE snapshot_id = ?"


class _Claim:
    """One row of the scope as read in the listing transaction."""

    __slots__ = ("snapshot_id", "manifest_fingerprint", "as_of", "issued_at", "watermark")

    def __init__(self, row: tuple[Any, ...]) -> None:
        snapshot_id, manifest_fingerprint, as_of, issued_at, watermark = row
        for name, value in (
            ("snapshot_id", snapshot_id),
            ("manifest_fingerprint", manifest_fingerprint),
        ):
            if not isinstance(value, str) or not value:
                _metadata(f"A snapshot row of the scope has an invalid {name}.")
        self.snapshot_id: str = snapshot_id
        self.manifest_fingerprint: str = manifest_fingerprint
        self.as_of = _instant(snapshot_id, "as_of", as_of)
        self.issued_at = _instant(snapshot_id, "issued_at", issued_at)
        if self.issued_at.compare(self.as_of) < 0:  # EI-2B: a snapshot is issued at or after as_of
            _metadata(f"Snapshot {snapshot_id} is stored as issued before its as_of.")
        if isinstance(watermark, bool) or not isinstance(watermark, int) or watermark < 0:
            _metadata(f"Snapshot {snapshot_id} has an invalid stored watermark.")
        self.watermark: int = watermark

    def eligible(self, monitoring_instant: PointInTime) -> bool:
        """``as_of <= T`` and ``issued_at <= T``, on parsed instants; equality is eligible."""
        return (
            self.as_of.compare(monitoring_instant) <= 0
            and self.issued_at.compare(monitoring_instant) <= 0
        )


def _metadata(message: str) -> NoReturn:
    raise _Invalid(_Invalid.METADATA, message)


def _instant(snapshot_id: str, name: str, value: object) -> PointInTime:
    """Stored instants must be canonical PointInTime text; anything else is ``METADATA``."""
    if not isinstance(value, str):
        _metadata(f"Snapshot {snapshot_id} has a non-text stored {name}.")
    try:
        point = PointInTime(value)
    except ValueError:
        _metadata(f"Snapshot {snapshot_id} has an invalid stored {name}.")
    if point.value != value:
        _metadata(f"Snapshot {snapshot_id} has a non-canonical stored {name}.")
    return point


def _by_instant(
    first: EquityCandleEvidenceSnapshotCatalogEntry,
    second: EquityCandleEvidenceSnapshotCatalogEntry,
) -> int:
    return first.as_of.compare(second.as_of) or (first.snapshot_id > second.snapshot_id) - (
        first.snapshot_id < second.snapshot_id
    )


# ---------------------------------------------------------------------------
# Inside the connection
# ---------------------------------------------------------------------------


def _listed(
    connection: sqlite3.Connection,
    scope: EquityCandleEvidenceSnapshotCatalogScope,
    monitoring_instant: PointInTime,
    limit: int,
) -> list[_Claim]:
    """Step 1 to 3, in ONE read transaction: the eligible claims of the scope, or a refusal."""
    connection.execute("BEGIN")
    (tables,) = connection.execute(_TABLE_COUNT, EVIDENCE_TABLES).fetchone()
    if tables != len(EVIDENCE_TABLES):
        raise EquityCandleEvidenceSnapshotCatalogUnavailableError(
            "The database holds no EI-2B evidence snapshot tables; nothing was listed."
        )
    parameters = (
        scope.listing.symbol.value,
        scope.listing.exchange_code.value,
        scope.trading_date.isoformat(),
        scope.timeframe.value,
        scope.window_policy_version,
        scope.evidence_class.value,
    )
    eligible: list[_Claim] = []
    count = 0
    for row in connection.execute(_SCOPE_ROWS, parameters):
        claim = _Claim(tuple(row))
        if claim.eligible(monitoring_instant):
            count += 1
            if count <= limit:
                eligible.append(claim)
    connection.execute("ROLLBACK")
    if count > limit:
        raise EquityCandleEvidenceSnapshotCatalogLimitExceededError(
            limit,
            f"{count} snapshots are eligible at {monitoring_instant} for the scope, more "
            f"than the governed limit of {limit}; nothing was listed.",
        )
    return eligible


def _verified(
    connection: sqlite3.Connection,
    claim: _Claim,
    scope: EquityCandleEvidenceSnapshotCatalogScope,
) -> EquityCandleEvidenceSnapshotCatalogEntry:
    """Step 4, in its own read transaction: the EI-2B verified snapshot behind ``claim``."""
    connection.execute("BEGIN")
    try:
        snapshot = _stored_snapshot(connection, claim.snapshot_id)
    except (*_DECODE_ERRORS, EquityCandleSnapshotEvidenceError) as exc:
        category = _Invalid.INTEGRITY if _decodes(connection, claim) else _Invalid.METADATA
        raise _Invalid(
            category,
            f"Snapshot {claim.snapshot_id} does not verify as stored ({type(exc).__name__}).",
        ) from exc
    connection.execute("ROLLBACK")
    if snapshot is None:
        raise _Invalid(
            _Invalid.INCOMPLETE, f"Listed snapshot {claim.snapshot_id} is no longer stored."
        )
    if not _restates(snapshot, claim, scope):
        raise _Invalid(
            _Invalid.INTEGRITY,
            f"Snapshot {claim.snapshot_id} does not restate its listed scope and claims.",
        )
    return EquityCandleEvidenceSnapshotCatalogEntry(
        snapshot.snapshot_id,
        snapshot.listing,
        snapshot.trading_date,
        snapshot.timeframe,
        snapshot.window_policy_version,
        snapshot.evidence_class,
        snapshot.as_of,
        snapshot.issued_at,
        snapshot.watermark,
        snapshot.manifest_fingerprint,
    )


def _decodes(connection: sqlite3.Connection, claim: _Claim) -> bool:
    """Whether the stored document decodes at all (failure path only, to classify it)."""
    try:
        (document,) = connection.execute(_SELECT_DOCUMENT, (claim.snapshot_id,)).fetchone()
        equity_candle_snapshot_from_document(json.loads(document))
    except (*_DECODE_ERRORS, EquityCandleSnapshotEvidenceError):
        return False
    return True


def _restates(
    snapshot: EquityCandleEvidenceSnapshot,
    claim: _Claim,
    scope: EquityCandleEvidenceSnapshotCatalogScope,
) -> bool:
    return (
        snapshot.snapshot_id,
        snapshot.manifest_fingerprint,
        snapshot.as_of,
        snapshot.issued_at,
        snapshot.watermark,
        snapshot.listing,
        snapshot.trading_date,
        snapshot.timeframe,
        snapshot.window_policy_version,
        snapshot.evidence_class,
    ) == (
        claim.snapshot_id,
        claim.manifest_fingerprint,
        claim.as_of,
        claim.issued_at,
        claim.watermark,
        scope.listing,
        scope.trading_date,
        scope.timeframe,
        scope.window_policy_version,
        scope.evidence_class,
    )


# ---------------------------------------------------------------------------
# Connections
# ---------------------------------------------------------------------------


def _failure(exc: sqlite3.Error) -> Exception:
    """A SQLite failure: corrupt storage is permanent; anything else is unavailability."""
    code = getattr(exc, "sqlite_errorcode", None)
    name = getattr(exc, "sqlite_errorname", None) or type(exc).__name__
    if isinstance(code, int) and code & 0xFF in _CORRUPT:
        return _Invalid(
            _Invalid.INCOMPLETE, f"The evidence database cannot be read consistently ({name})."
        )
    return EquityCandleEvidenceSnapshotCatalogUnavailableError(
        f"The snapshot catalog is unavailable ({name}); nothing was listed."
    )


def _connect(database_path: Path) -> sqlite3.Connection:
    """Open the existing database read-only; never create, initialize or write it."""
    connection = sqlite3.connect(
        f"{database_path.resolve().as_uri()}?mode=ro",
        timeout=_BUSY_TIMEOUT_SECONDS,
        isolation_level=None,
        uri=True,
    )
    try:
        connection.execute("PRAGMA query_only = ON")
        if connection.execute("PRAGMA query_only").fetchone() != (1,):
            raise sqlite3.OperationalError("read-only mode could not be confirmed")
    except BaseException:
        _discard(connection)
        raise
    return connection


# ---------------------------------------------------------------------------
# The adapter
# ---------------------------------------------------------------------------


class SQLiteEquityCandleEvidenceSnapshotCatalog(EquityCandleEvidenceSnapshotCatalog):
    """Read-only listing of a session's eligible EI-2B snapshots at a monitoring instant."""

    def __init__(self, database_path: str | Path) -> None:
        if not isinstance(database_path, str | Path):
            raise TypeError("database_path must be a str or Path.")
        self._database_path = Path(database_path)

    def list_snapshots(
        self,
        scope: EquityCandleEvidenceSnapshotCatalogScope,
        monitoring_instant: PointInTime,
        limit: int,
    ) -> tuple[EquityCandleEvidenceSnapshotCatalogEntry, ...]:
        """Return every snapshot of ``scope`` eligible at ``T``, verified; see the module."""
        if not isinstance(scope, EquityCandleEvidenceSnapshotCatalogScope):
            raise TypeError("scope must be an EquityCandleEvidenceSnapshotCatalogScope.")
        if not isinstance(monitoring_instant, PointInTime):
            raise TypeError("monitoring_instant must be a PointInTime.")
        if isinstance(limit, bool) or not isinstance(limit, int):
            raise TypeError("limit must be an integer.")
        if limit < 1:
            raise ValueError("limit must be at least 1.")
        try:
            connection = _connect(self._database_path)
        except sqlite3.Error as exc:
            raise _failure(exc) from exc
        try:
            try:
                claims = _listed(connection, scope, monitoring_instant, limit)
                entries = [_verified(connection, claim, scope) for claim in claims]
            except sqlite3.Error as exc:
                raise _failure(exc) from exc
        except BaseException:
            _rollback(connection)
            _discard(connection)
            raise
        try:
            connection.close()
        except sqlite3.Error as exc:
            raise _failure(exc) from exc
        return tuple(sorted(entries, key=cmp_to_key(_by_instant)))
