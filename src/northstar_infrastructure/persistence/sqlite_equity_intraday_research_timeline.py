"""SQLite authoritative equity intraday research timeline store (EI-3C-B).

Implements the Application's EquityIntradayResearchTimelineStore under the
owner decisions recorded on 2026-11-07 (EI-3 Decision Register, section 11):
EI3-D11, EI3-D12 and EI3-D13. Synthetic research only. It is an internal
persistence capability, wired into no operation, and calls no provider, NSE
or broker.

It uses the SAME database file as the EI-2B evidence journal and snapshot
store: every entry references its EI-2B snapshot by a real foreign key, and
each connection creates the EI-2B schema before its own. A database without
the snapshot cannot hold the entry.

Records, not research
---------------------
The store keeps and returns StoredEquityIntradayResearchTimeline records
exactly: the canonical configuration, identity and decision documents as the
text they were committed as. It never builds, evaluates or repairs a research
decision; the Application re-derives every decision and fingerprint from the
immutable EI-2B evidence on load (EI3-D13). Its own checks are structural:
the configuration document must decode to the EI-3A configuration it
canonically encodes, from which the EI-3B identity document is derived; each
instant must be canonical PointInTime text; and every column must be what its
record produces.

``load`` reads in one transaction. It returns None when no timeline exists,
or the whole record in sequence order. An identity without entries, a gap, or
any row that does not decode exactly raises
EquityIntradayResearchTimelineStorageError; nothing is skipped.

``append(timeline, expected_head_fingerprint)`` runs in ONE ``BEGIN IMMEDIATE``
transaction, which takes the write lock before reading, so SQLite serializes
writers (EI3-D12):

1. the stored identity under the record's strategy, listing and date is read
   and verified; another configuration document raises
   EquityIntradayResearchTimelineConfigurationConflictError;
2. the stored head -- the last entry's fingerprint, None when none -- must be
   ``expected_head_fingerprint``, and the record must repeat every stored
   entry exactly; otherwise EquityIntradayResearchTimelineStaleError;
3. the identity is inserted when absent -- only here, in the transaction that
   inserts the first entry (EI3-D11) -- and then every new entry, in sequence;
4. the transaction commits once.

The schema's constraints and triggers check every inserted row again.

Commit outcomes
---------------
Returning means committed. Stale and configuration conflict errors, and
EquityIntradayResearchTimelineStorageError -- an unavailable database, a lock
not obtained within five seconds, a refused row -- mean the transaction was
rolled back before COMMIT: nothing was written. A failure from COMMIT itself
raises EquityIntradayResearchTimelineCommitUnconfirmedError and asserts
nothing: SQLite may or may not have made the transaction durable, and the
Application reconciles against reloaded history. The store never retries.

Cleanup -- rolling back or closing a connection that may already have failed
-- never replaces the outcome being reported and is never taken as evidence
that a transaction was rolled back (L9-T, as W2 for the EI-4D-B store). After
a confirmed COMMIT the connection is closed normally, and a failure to close
it is reported as itself, never as an unconfirmed commit.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable
from contextlib import closing
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, NoReturn

from northstar_application.application_services import (
    EquityIntradayResearchOutcome,
    EquityIntradayResearchTimelineIdentity,
    EquityIntradayResearchTimelineRole,
    EquityOpeningRangeBreakoutConfiguration,
    StoredEquityIntradayResearchTimeline,
    StoredEquityIntradayResearchTimelineEntry,
    canonical_equity_candle_json,
)
from northstar_application.ports import (
    EquityIntradayResearchTimelineCommitUnconfirmedError,
    EquityIntradayResearchTimelineConfigurationConflictError,
    EquityIntradayResearchTimelineStaleError,
    EquityIntradayResearchTimelineStore,
    EquityIntradayResearchTimelineUnavailableError,
)
from northstar_core.domain.value_objects import ListingReference
from northstar_core.foundation.value_objects import ExchangeCode, PointInTime, Symbol, Timeframe
from northstar_core.strategy import StrategyIdentity

from northstar_infrastructure.persistence.sqlite_equity_candle_evidence_schema import (
    initialize_equity_candle_evidence_schema,
)
from northstar_infrastructure.persistence.sqlite_equity_intraday_research_timeline_schema import (  # noqa: E501
    initialize_equity_intraday_research_timeline_schema,
)

_BUSY_TIMEOUT_SECONDS = 5.0
_DECODE_ERRORS = (KeyError, TypeError, ValueError, ArithmeticError)
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_MICROSECOND = timedelta(microseconds=1)

TIMELINE_COLUMNS = (
    "timeline_id",
    "strategy",
    "symbol",
    "exchange_code",
    "trading_date",
    "configuration_version",
    "configuration_document",
    "identity_document",
)
ENTRY_COLUMNS = (
    "timeline_id",
    "sequence",
    "role",
    "decision_outcome",
    "snapshot_id",
    "manifest_fingerprint",
    "decision_instant",
    "decision_instant_us",
    "watermark",
    "research_window_closes_at",
    "decision_document",
    "decision_fingerprint",
    "previous_fingerprint",
    "entry_fingerprint",
)
_TABLE = "equity_intraday_research_timelines"
_ENTRY_TABLE = "equity_intraday_research_timeline_entries"
_SELECT_TIMELINE = (
    f"SELECT {', '.join(TIMELINE_COLUMNS)} FROM {_TABLE} "  # noqa: S608
    "WHERE strategy = ? AND symbol = ? AND exchange_code = ? AND trading_date = ?"
)
_INSERT_TIMELINE = (
    f"INSERT INTO {_TABLE} ({', '.join(TIMELINE_COLUMNS[1:])}) "  # noqa: S608
    f"VALUES ({', '.join('?' * (len(TIMELINE_COLUMNS) - 1))})"
)
_SELECT_ENTRIES = (
    f"SELECT {', '.join(ENTRY_COLUMNS)} FROM {_ENTRY_TABLE} "  # noqa: S608
    "WHERE timeline_id = ? ORDER BY sequence"
)
_INSERT_ENTRY = (
    f"INSERT INTO {_ENTRY_TABLE} ({', '.join(ENTRY_COLUMNS)}) "  # noqa: S608
    f"VALUES ({', '.join('?' * len(ENTRY_COLUMNS))})"
)


class EquityIntradayResearchTimelineStorageError(EquityIntradayResearchTimelineUnavailableError):
    """Raised when timeline storage is unavailable, refuses a write or holds invalid data.

    It is the Application port's EquityIntradayResearchTimelineUnavailableError:
    raised by ``append``, nothing was committed.
    """


def _invalid(message: str) -> NoReturn:
    raise ValueError(message)


def _refuse_float(value: str) -> NoReturn:
    raise ValueError(f"Stored configuration documents hold no floating-point numbers ({value}).")


# ---------------------------------------------------------------------------
# Values
# ---------------------------------------------------------------------------


def decision_instant_microseconds(instant: PointInTime) -> int:
    """Return ``instant`` as exact integer microseconds since 1970-01-01T00:00:00Z.

    PointInTime holds at most six fractional digits, so the value is exact.
    The arithmetic is integer-only: timedelta floor division by a microsecond.
    """
    if not isinstance(instant, PointInTime):
        raise TypeError("instant must be a PointInTime.")
    moment = datetime.fromisoformat(instant.value.replace("Z", "+00:00"))
    return (moment - _EPOCH) // _MICROSECOND


def _point(text: object) -> PointInTime:
    if not isinstance(text, str):
        raise TypeError("Stored instants must be text.")
    instant = PointInTime(text)
    if instant.value != text:
        _invalid(f"Stored instant {text!r} is not canonical UTC text.")
    return instant


def _identity(
    strategy: StrategyIdentity,
    listing: ListingReference,
    trading_date: date,
    configuration_document: str,
) -> tuple[str, str]:
    """Return the configuration version and canonical EI-3B identity document.

    The configuration document must decode to the EI-3A configuration whose
    canonical encoding it is, under ``strategy``.
    """
    document = json.loads(
        configuration_document, parse_float=_refuse_float, parse_constant=_refuse_float
    )
    if not isinstance(document, dict):
        _invalid("A configuration document must be a JSON object.")
    volume_multiplier = document["volume_multiplier"]
    if not isinstance(volume_multiplier, str):
        _invalid("A configuration volume multiplier must be exact decimal text.")
    configuration = EquityOpeningRangeBreakoutConfiguration(
        strategy=StrategyIdentity(document["strategy"]),
        version=document["version"],
        opening_range_bars=document["opening_range_bars"],
        volume_multiplier=Decimal(volume_multiplier),
        minimum_baseline_bars=document["minimum_baseline_bars"],
        timeframe=Timeframe(document["timeframe"]),
        window_policy_version=document["window_policy_version"],
    )
    if canonical_equity_candle_json(configuration.document()) != configuration_document:
        _invalid("The configuration document is not its configuration's canonical encoding.")
    if configuration.strategy != strategy:
        _invalid("The configuration document names another strategy.")
    identity = EquityIntradayResearchTimelineIdentity(listing, trading_date, configuration)
    return configuration.version, canonical_equity_candle_json(identity.document())


def _timeline_values(
    timeline: StoredEquityIntradayResearchTimeline,
) -> tuple[object, ...]:
    """Return the identity row without its timeline_id."""
    version, identity_document = _identity(
        timeline.strategy,
        timeline.listing,
        timeline.trading_date,
        timeline.configuration_document,
    )
    return (
        timeline.strategy.identity,
        timeline.listing.symbol.value,
        timeline.listing.exchange_code.value,
        timeline.trading_date.isoformat(),
        version,
        timeline.configuration_document,
        identity_document,
    )


def _entry_values(
    timeline_id: int, entry: StoredEquityIntradayResearchTimelineEntry
) -> tuple[object, ...]:
    return (
        timeline_id,
        entry.sequence,
        entry.role.value,
        entry.decision_outcome.value,
        entry.snapshot_id,
        entry.manifest_fingerprint,
        entry.decision_instant.value,
        decision_instant_microseconds(entry.decision_instant),
        entry.watermark,
        entry.research_window_closes_at.value,
        entry.decision_document,
        entry.decision_fingerprint,
        entry.previous_fingerprint,
        entry.entry_fingerprint,
    )


def _stored_entry(row: Any) -> StoredEquityIntradayResearchTimelineEntry:
    """Decode one entry row; every column must be what its record produces."""
    stored = tuple(row)
    timeline_id, sequence = stored[0], stored[1]
    if not isinstance(timeline_id, int) or not isinstance(sequence, int):
        raise TypeError("Stored timeline identities and sequences must be integers.")
    entry = StoredEquityIntradayResearchTimelineEntry(
        sequence=sequence,
        role=EquityIntradayResearchTimelineRole(stored[2]),
        decision_outcome=EquityIntradayResearchOutcome(stored[3]),
        snapshot_id=stored[4],
        manifest_fingerprint=stored[5],
        decision_instant=_point(stored[6]),
        watermark=stored[8],
        research_window_closes_at=_point(stored[9]),
        decision_document=stored[10],
        decision_fingerprint=stored[11],
        previous_fingerprint=stored[12],
        entry_fingerprint=stored[13],
    )
    if _entry_values(timeline_id, entry) != stored:
        _invalid(f"Timeline entry {sequence} has columns that disagree with its record.")
    return entry


def _stored_timeline(
    connection: sqlite3.Connection, row: Any
) -> StoredEquityIntradayResearchTimeline:
    """Decode an identity row and every entry; the whole record or an error."""
    stored = tuple(row)
    timeline_id = stored[0]
    if not isinstance(timeline_id, int):
        raise TypeError("Stored timeline identities must be integers.")
    for value in stored[1:]:
        if not isinstance(value, str):
            raise TypeError("Stored timeline identity values must be text.")
    entries = tuple(
        _stored_entry(entry) for entry in connection.execute(_SELECT_ENTRIES, (timeline_id,))
    )
    if [e.sequence for e in entries] != list(range(1, len(entries) + 1)):
        _invalid("Stored timeline entries are not numbered 1 to n.")
    timeline = StoredEquityIntradayResearchTimeline(
        strategy=StrategyIdentity(stored[1]),
        listing=ListingReference(Symbol(stored[2]), ExchangeCode(stored[3])),
        trading_date=date.fromisoformat(stored[4]),
        configuration_document=stored[6],
        entries=entries,
    )
    if (timeline_id, *_timeline_values(timeline)) != stored:
        _invalid("The stored timeline identity disagrees with its configuration document.")
    return timeline


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
        initialize_equity_intraday_research_timeline_schema(connection)
    except sqlite3.Error as exc:
        raise EquityIntradayResearchTimelineStorageError(
            "Research timeline storage is unavailable."
        ) from exc
    return connection


def _commit(connection: sqlite3.Connection) -> None:
    """Commit the open transaction: the one boundary whose failure is unconfirmed."""
    connection.commit()


def _insert_entry(connection: sqlite3.Connection, values: tuple[object, ...]) -> None:
    connection.execute(_INSERT_ENTRY, values)


def _close(connection: sqlite3.Connection) -> None:
    connection.close()


def _rollback(connection: sqlite3.Connection) -> None:
    """Roll back an open transaction; never mask the failure that led here.

    A connection that already failed or closed -- for example around COMMIT --
    cannot even report whether a transaction is open, so nothing is asserted
    about it: a transaction that never reached COMMIT ends rolled back when its
    connection closes, and one whose COMMIT failed stays unconfirmed.
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


def _verified[T](work: Callable[[], T], subject: str) -> T:
    try:
        return work()
    except _DECODE_ERRORS as exc:
        raise EquityIntradayResearchTimelineStorageError(
            f"{subject} holds invalid data: {exc}"
        ) from exc


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
        raise EquityIntradayResearchTimelineStorageError(
            "Research timeline storage is unavailable."
        ) from exc


def _key(
    strategy: StrategyIdentity, listing: ListingReference, trading_date: date
) -> tuple[str, str, str, str]:
    return (
        strategy.identity,
        listing.symbol.value,
        listing.exchange_code.value,
        trading_date.isoformat(),
    )


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class SQLiteEquityIntradayResearchTimelineStore(EquityIntradayResearchTimelineStore):
    """Append-only SQLite store of authoritative research timelines.

    ``database_path`` must be the EI-2B evidence database: there is no second
    database setting.
    """

    def __init__(self, database_path: str | Path) -> None:
        self._database_path = str(database_path)

    def load(
        self, strategy: StrategyIdentity, listing: ListingReference, trading_date: date
    ) -> StoredEquityIntradayResearchTimeline | None:
        """Return the whole stored timeline, or None when no entry was ever committed."""
        if not isinstance(strategy, StrategyIdentity):
            raise TypeError("strategy must be a StrategyIdentity.")
        if not isinstance(listing, ListingReference):
            raise TypeError("listing must be a ListingReference.")
        if type(trading_date) is not date:
            raise TypeError("trading_date must be a plain date.")

        def work(connection: sqlite3.Connection) -> StoredEquityIntradayResearchTimeline | None:
            row = connection.execute(
                _SELECT_TIMELINE, _key(strategy, listing, trading_date)
            ).fetchone()
            if row is None:
                return None
            return _verified(lambda: _stored_timeline(connection, row), "The stored timeline")

        return _read(self._database_path, work)

    def append(
        self,
        timeline: StoredEquityIntradayResearchTimeline,
        expected_head_fingerprint: str | None,
    ) -> None:
        """Commit ``timeline``'s new entries only if the stored head is the expected one."""
        if not isinstance(timeline, StoredEquityIntradayResearchTimeline):
            raise TypeError("timeline must be a StoredEquityIntradayResearchTimeline.")
        if expected_head_fingerprint is not None and not isinstance(expected_head_fingerprint, str):
            raise TypeError("expected_head_fingerprint must be text or None.")
        identity_values = _verified(
            lambda: _timeline_values(timeline), "The timeline record to append"
        )
        connection = _connect(self._database_path)
        try:
            try:
                connection.execute("BEGIN IMMEDIATE")
                self._compare_and_insert(
                    connection, timeline, expected_head_fingerprint, identity_values
                )
            except sqlite3.Error as exc:
                _rollback(connection)
                raise EquityIntradayResearchTimelineStorageError(
                    f"Research timeline storage refused the write; nothing was written: {exc}"
                ) from exc
            except BaseException:
                _rollback(connection)
                raise
            try:
                _commit(connection)
            except Exception as exc:
                _rollback(connection)
                raise EquityIntradayResearchTimelineCommitUnconfirmedError(
                    "COMMIT failed; whether the research timeline entries are durable is "
                    f"unknown: {exc}"
                ) from exc
        except BaseException:
            _discard(connection)
            raise
        _close(connection)

    @staticmethod
    def _compare_and_insert(
        connection: sqlite3.Connection,
        timeline: StoredEquityIntradayResearchTimeline,
        expected_head: str | None,
        identity_values: tuple[object, ...],
    ) -> None:
        row = connection.execute(
            _SELECT_TIMELINE, _key(timeline.strategy, timeline.listing, timeline.trading_date)
        ).fetchone()
        committed: tuple[StoredEquityIntradayResearchTimelineEntry, ...] = ()
        if row is not None:
            stored = _verified(lambda: _stored_timeline(connection, row), "The stored timeline")
            if stored.configuration_document != timeline.configuration_document:
                raise EquityIntradayResearchTimelineConfigurationConflictError(
                    "The stored timeline holds another configuration; nothing was written."
                )
            committed = stored.entries
        head = committed[-1].entry_fingerprint if committed else None
        if head != expected_head:
            raise EquityIntradayResearchTimelineStaleError(
                f"The stored head is {head}, not the expected {expected_head}; nothing was written."
            )
        if timeline.entries[: len(committed)] != committed:
            raise EquityIntradayResearchTimelineStaleError(
                "The record does not repeat the committed history; nothing was written."
            )
        new = timeline.entries[len(committed) :]
        if not new:
            raise EquityIntradayResearchTimelineStorageError(
                "The record appends no entry; nothing was written (EI3-D11)."
            )
        timeline_id = row[0] if row is not None else None
        if timeline_id is None:
            timeline_id = connection.execute(_INSERT_TIMELINE, identity_values).lastrowid
            if not isinstance(timeline_id, int):
                raise EquityIntradayResearchTimelineStorageError("No timeline identity assigned.")
        for entry in new:
            _insert_entry(connection, _entry_values(timeline_id, entry))
