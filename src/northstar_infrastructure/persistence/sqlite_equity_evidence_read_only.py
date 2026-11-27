"""Strictly read-only SQLite readers of equity research timelines and evidence (EI-7D).

Read-only implementations, over the EXISTING EI-2B/EI-3C database, of the two
reader-only protocols the EI-7C research and detail queries depend on:

- ``SQLiteReadOnlyEquityIntradayResearchTimelineReader`` --
  ``EquityIntradayResearchTimelineReader``: ``load(strategy, listing,
  trading_date)``, the EI-3C timeline read by exact identity;
- ``SQLiteReadOnlyEquityCandleEvidenceSnapshotReader`` --
  ``EquityCandleEvidenceSnapshotReader``: ``get_snapshot(snapshot_id)``, the
  EI-2B snapshot read by exact identity.

Neither can append research history or issue a snapshot: no such method exists
here, none is inherited and no write-capable adapter is constructed or called
(EI7-FC6: no refusing ``issue()`` either). Neither evaluates research, reads
market data or decides an evidence class: the Application rehydrates every
research decision (EI3-D13) and gates every snapshot's evidence class
(EI7-EC1) itself. They are not wired into any operation.

Connections and verification
----------------------------
Every call is one read transaction on a fresh ``mode=ro`` connection with
``PRAGMA query_only`` confirmed (``sqlite_equity_read_only``): no DDL, no
schema initialization, never a commit. Inside it the accepted verified
decodings run unchanged on the read-only connection:

- the EI-3C ``_stored_timeline``: the identity row and every entry, numbered
  1..n, every column equal to what its record produces, the configuration
  document the canonical encoding of its EI-3A configuration under the
  timeline's strategy, and the EI-3B identity document derived from it;
- the EI-2B ``_stored_snapshot``: the stored document re-derives its manifest
  fingerprint and identity, every column (the evidence class included) agrees
  with it, its entry rows agree, and the journal at its watermark still holds
  exactly its evidence.

Source-faithful classification
------------------------------
The existing write-capable adapters report stored data that does not verify as
*unavailability* (EquityIntradayResearchTimelineStorageError,
EquityCandleEvidenceStorageError); their contracts are unchanged. These
readers instead separate the two, as the Application ports already allow:

- **timeline:** stored history that does not verify raises
  InvalidStoredEquityIntradayResearchTimelineError (permanent); storage that
  cannot be read raises EquityIntradayResearchTimelineUnavailableError;
- **snapshot:** a stored snapshot that exists but does not verify -- does not
  decode, re-derive its fingerprints or identity, agree with its columns or
  entry rows, or agree with its journal -- raises
  InvalidStoredEquityCandleEvidenceSnapshotError (EI-7C: permanent, a
  subclass of the EI-2B unavailability error, so existing handlers keep their
  behaviour); storage that cannot be read raises the EI-2B
  EquityCandleEvidenceUnavailableError itself. This closes the EI-7C
  limitation: an EI-7 query can tell a corrupt snapshot (``invalid``) from an
  outage (``unavailable``).

Absence is absence: no timeline is None, no snapshot under an identity is
None; nothing is fabricated. A verified snapshot is returned exactly as
issued, with its own evidence class -- a PRODUCTION snapshot is never
relabelled SYNTHETIC and never hidden as missing; the EI-7C queries refuse it.

"Storage that cannot be read" is: an absent file, no such tables, a lock held
beyond the busy timeout, a hot journal, a database not in rollback-journal
mode, or any other SQLite error. Messages carry no stored content and no path.
Nothing here reads a clock, retries, sleeps or calls out.
"""

from __future__ import annotations

import sqlite3
from datetime import date
from pathlib import Path

from northstar_application.application_services import (
    EquityCandleEvidenceSnapshot,
    StoredEquityIntradayResearchTimeline,
)
from northstar_application.ports import (
    EquityCandleEvidenceSnapshotReader,
    EquityCandleEvidenceUnavailableError,
    EquityIntradayResearchTimelineReader,
    EquityIntradayResearchTimelineUnavailableError,
    InvalidStoredEquityCandleEvidenceSnapshotError,
    InvalidStoredEquityIntradayResearchTimelineError,
)
from northstar_core.domain.value_objects import ListingReference
from northstar_core.strategy import StrategyIdentity

from northstar_infrastructure.persistence.sqlite_equity_candle_evidence import (
    _DECODE_ERRORS as _SNAPSHOT_DECODE_ERRORS,
)
from northstar_infrastructure.persistence.sqlite_equity_candle_evidence import (
    _stored_snapshot,  # the EI-2B verified snapshot decoding, reused unchanged
)
from northstar_infrastructure.persistence.sqlite_equity_intraday_research_timeline import (
    _DECODE_ERRORS as _TIMELINE_DECODE_ERRORS,
)
from northstar_infrastructure.persistence.sqlite_equity_intraday_research_timeline import (
    _SELECT_TIMELINE,
    _key,
    _stored_timeline,  # the EI-3C verified timeline decoding, reused unchanged
)
from northstar_infrastructure.persistence.sqlite_equity_read_only import (
    read_only_transaction,
    sqlite_failure_name,
)


def _path(database_path: object) -> Path:
    if not isinstance(database_path, str | Path):
        raise TypeError("database_path must be a str or Path.")
    return Path(database_path)


def _timeline_unavailable(exc: sqlite3.Error) -> EquityIntradayResearchTimelineUnavailableError:
    return EquityIntradayResearchTimelineUnavailableError(
        f"Research timeline storage is unavailable; nothing was read ({sqlite_failure_name(exc)})."
    )


def _evidence_unavailable(exc: sqlite3.Error) -> EquityCandleEvidenceUnavailableError:
    return EquityCandleEvidenceUnavailableError(
        f"Evidence snapshot storage is unavailable; nothing was read ({sqlite_failure_name(exc)})."
    )


class SQLiteReadOnlyEquityIntradayResearchTimelineReader(EquityIntradayResearchTimelineReader):
    """The EI-3C timeline read over a strictly read-only connection; no append exists."""

    def __init__(self, database_path: str | Path) -> None:
        self._database_path = _path(database_path)

    def load(
        self, strategy: StrategyIdentity, listing: ListingReference, trading_date: date
    ) -> StoredEquityIntradayResearchTimeline | None:
        """Return the whole verified stored timeline, or None when none was committed."""
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
            try:
                return _stored_timeline(connection, row)
            except _TIMELINE_DECODE_ERRORS as exc:
                raise InvalidStoredEquityIntradayResearchTimelineError(
                    f"The stored research timeline does not verify ({type(exc).__name__})."
                ) from exc

        return read_only_transaction(self._database_path, work, _timeline_unavailable)


class SQLiteReadOnlyEquityCandleEvidenceSnapshotReader(EquityCandleEvidenceSnapshotReader):
    """The EI-2B snapshot read over a strictly read-only connection; no issue exists."""

    def __init__(self, database_path: str | Path) -> None:
        self._database_path = _path(database_path)

    def get_snapshot(self, snapshot_id: str) -> EquityCandleEvidenceSnapshot | None:
        """Return the snapshot issued under ``snapshot_id``, verified, or None."""
        if not isinstance(snapshot_id, str):
            raise TypeError("snapshot_id must be text.")

        def work(connection: sqlite3.Connection) -> EquityCandleEvidenceSnapshot | None:
            try:
                return _stored_snapshot(connection, snapshot_id)
            except _SNAPSHOT_DECODE_ERRORS as exc:
                raise InvalidStoredEquityCandleEvidenceSnapshotError(
                    f"Stored snapshot {snapshot_id} does not verify ({type(exc).__name__})."
                ) from exc

        return read_only_transaction(self._database_path, work, _evidence_unavailable)
