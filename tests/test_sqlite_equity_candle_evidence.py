"""EI-2B: SQLite append-only equity candle evidence journal and as-issued snapshots.

The research window is the real EI-1C-3 chain over the accepted production
NSE venue reference and explicitly synthetic listing applicability for M&M on
2026-08-12. Every candle, source identity and finality statement is invented
synthetic test evidence -- not NSE, provider or broker data -- and nothing
here opens a network connection. A snapshot is a record of what was known; it
is not a decision, signal, recommendation or order.
"""

from __future__ import annotations

import ast
import dataclasses
import inspect
import json
import socket
import sqlite3
import threading
from contextlib import closing
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from northstar_application.application_services import (
    EquityCandleAppendOutcome,
    EquityCandleCoverage,
    EquityCandleEvidenceClass,
    EquityCandleFinalityEvidence,
    EquityCandleFinalityPolicy,
    EquityCandleFinalityRecord,
    EquityCandleFinding,
    EquityCandleIntervalOutcome,
    EquityCandleObservation,
    EquityCandleObservationRecord,
    EquityCandleSnapshotRefusedError,
    EquityCandleSnapshotReplayOutcome,
    EvaluateEquityResearchWindowUseCase,
    IssueEquityCandleEvidenceSnapshotUseCase,
    ReplayEquityCandleEvidenceSnapshotUseCase,
)
from northstar_application.ports import (
    EquityCandleIngestionConflictError,
    EquityCandleSnapshotEvidenceError,
    EquityCandleSnapshotStaleError,
    EquityCandleSourceIdentityConflictError,
    EquityListingApplicability,
    EquityListingApplicabilityClassification,
    EquityReferenceProvenanceKind,
    InvalidEquityCandleJournalQueryError,
    UnknownEquityReferenceProvenanceError,
)
from northstar_core.domain.value_objects import ListingReference
from northstar_core.foundation.value_objects import (
    Currency,
    ExchangeCode,
    PointInTime,
    Price,
    Quantity,
    Symbol,
    Timeframe,
)
from northstar_core.market_data import HistoricalOHLCVBar

from northstar_infrastructure.market_data import (
    NoApprovedEquityListingApplicabilitySource,
    NSEEquityResearchReferenceProvenance,
    NSEEquityVenueDayReferenceSource,
    NSEEquityVenueSessionReference,
    SyntheticListingApplicabilityRecord,
    SyntheticListingApplicabilitySource,
)
from northstar_infrastructure.market_data import (
    nse_equity_reference_provenance as provenance_module,
)
from northstar_infrastructure.market_data.nse_accepted_equity_session_evidence import (
    PRODUCTION_NSE_EQUITY_SESSION_CATALOG,
)
from northstar_infrastructure.market_data.sqlite_schema import (
    initialize_historical_market_data_schema,
)
from northstar_infrastructure.persistence import (
    EquityCandleEvidenceStorageError,
    SQLiteEquityCandleEvidenceJournal,
    SQLiteEquityCandleEvidenceSnapshotStore,
    initialize_equity_candle_evidence_schema,
)
from northstar_infrastructure.persistence import (
    sqlite_equity_candle_evidence as adapter_module,
)
from northstar_infrastructure.persistence import (
    sqlite_equity_candle_evidence_schema as schema_module,
)

_NSE = ExchangeCode("NSE")
_INR = Currency("INR")
_M_AND_M = ListingReference(Symbol("M&M"), _NSE)
_RELIANCE = ListingReference(Symbol("RELIANCE"), _NSE)
_DAY = date(2026, 8, 12)
_FIRST_END = datetime(2026, 8, 12, 3, 50, tzinfo=UTC)
_LATE = datetime(2026, 8, 12, 12, 0, tzinfo=UTC)
_SOURCE = "synthetic-source"
_SYNTHETIC = EquityCandleFinalityPolicy(
    "synthetic-ei2b-sqlite/1", frozenset({_SOURCE}), permits_synthetic=True
)
_OUTCOME = EquityCandleIntervalOutcome
_TABLES = (
    "equity_candle_journal",
    "equity_candle_evidence_snapshots",
    "equity_candle_evidence_snapshot_entries",
)


@pytest.fixture(autouse=True)
def _refuse_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("EI-2B tests must not open network connections.")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)


@pytest.fixture
def database(tmp_path: Path) -> Path:
    return tmp_path / "equity-evidence.sqlite3"


class _Clock:
    def __init__(self) -> None:
        self.ticks = 0

    def __call__(self) -> datetime:
        self.ticks += 1
        return datetime(2026, 8, 13, tzinfo=UTC) + timedelta(seconds=self.ticks)


def _journal(database: Path) -> SQLiteEquityCandleEvidenceJournal:
    return SQLiteEquityCandleEvidenceJournal(database, clock=_Clock())


def _pit(value: datetime) -> PointInTime:
    return PointInTime(value.isoformat().replace("+00:00", "Z"))


def _end(index: int) -> datetime:
    return _FIRST_END + timedelta(minutes=5 * (index - 1))


def _synthetic_applicability(version: str = "ei2b-sqlite/1") -> SyntheticListingApplicabilitySource:
    return SyntheticListingApplicabilitySource(
        version,
        (
            SyntheticListingApplicabilityRecord(
                _M_AND_M,
                _DAY,
                EquityListingApplicabilityClassification.ELIGIBLE_FOR_NORMAL_RESEARCH_WINDOW,
                "synthetic test statement",
            ),
        ),
    )


_VENUE = NSEEquityVenueDayReferenceSource.production()
_APPLICABILITY = _synthetic_applicability()
_PROVENANCE = NSEEquityResearchReferenceProvenance(_VENUE, _APPLICABILITY)
_SYN = EquityReferenceProvenanceKind.SYNTHETIC
_PROD = EquityReferenceProvenanceKind.ACCEPTED_PRODUCTION


def _window(  # type: ignore[no-untyped-def]
    as_of: datetime,
    listing: ListingReference = _M_AND_M,
    venue=_VENUE,  # type: ignore[no-untyped-def]
    applicability=_APPLICABILITY,  # type: ignore[no-untyped-def]
):
    use_case = EvaluateEquityResearchWindowUseCase(venue, applicability)
    return use_case.execute(listing, _DAY, _pit(as_of))


def _observation(
    index: int,
    *,
    ingestion: str | None = None,
    revision: str = "r1",
    event: str | None = None,
    prices: tuple[str, str, str, str] = ("1600.05", "1604.5", "1598", "1602.25"),
    received: datetime | None = None,
    corrects: str | None = None,
    published: datetime | None = None,
    listing: ListingReference = _M_AND_M,
) -> EquityCandleObservationRecord:
    o, h, low, c = (Price(Decimal(p), _INR) for p in prices)
    bar = HistoricalOHLCVBar(
        symbol=listing.symbol,
        exchange_code=listing.exchange_code,
        point_in_time=_pit(_end(index)),
        timeframe=Timeframe("5m"),
        open=o,
        high=h,
        low=low,
        close=c,
        volume=Quantity(Decimal("12500")),
    )
    return EquityCandleObservationRecord(
        ingestion_id=ingestion or f"obs-{index}-{event}-{revision}",
        observation=EquityCandleObservation(
            bar=bar,
            source_id=_SOURCE,
            source_event_id=event or f"evt-{index}",
            source_revision=revision,
            received_at=_pit(received or _end(index) + timedelta(seconds=5)),
            finality=None,
            corrects_revision=corrects,
        ),
        is_synthetic=True,
        source_published_at=None if published is None else _pit(published),
    )


def _finality(
    index: int,
    *,
    ingestion: str | None = None,
    revision: str = "r1",
    event: str | None = None,
    confirmed: datetime | None = None,
    received: datetime | None = None,
) -> EquityCandleFinalityRecord:
    confirmed = confirmed or _end(index) + timedelta(seconds=10)
    return EquityCandleFinalityRecord(
        ingestion_id=ingestion or f"fin-{index}-{event}-{revision}",
        evidence=EquityCandleFinalityEvidence(
            source_id=_SOURCE,
            source_event_id=event or f"evt-{index}",
            source_revision=revision,
            confirmed_at=_pit(confirmed),
            evidence_reference="synthetic finality statement",
            is_synthetic=True,
        ),
        received_at=_pit(received or confirmed + timedelta(seconds=1)),
    )


def _final_candle(journal: SQLiteEquityCandleEvidenceJournal, index: int) -> None:
    journal.append(_observation(index))
    journal.append(_finality(index))


def _issue(  # type: ignore[no-untyped-def]
    database: Path,
    as_of: datetime,
    *,
    issued: datetime | None = None,
    journal: SQLiteEquityCandleEvidenceJournal | None = None,
):
    use_case = IssueEquityCandleEvidenceSnapshotUseCase(
        journal or _journal(database),
        SQLiteEquityCandleEvidenceSnapshotStore(database),
        _INR,
        _SYNTHETIC,
        reference_provenance=_PROVENANCE,
    )
    return use_case.execute(
        _window(as_of), _pit(as_of), _pit(issued or as_of + timedelta(minutes=1))
    )


def _replay(database: Path, snapshot_id: str, as_of: datetime):  # type: ignore[no-untyped-def]
    return ReplayEquityCandleEvidenceSnapshotUseCase(
        _journal(database),
        SQLiteEquityCandleEvidenceSnapshotStore(database),
        _INR,
        _SYNTHETIC,
        reference_provenance=_PROVENANCE,
    ).execute(snapshot_id, _window(as_of))


def _raw(database: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(database, isolation_level=None)
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def _count(database: Path, table: str) -> int:
    with closing(_raw(database)) as connection:
        return connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]  # noqa: S608


def _integrity(database: Path) -> None:
    with closing(_raw(database)) as connection:
        assert connection.execute("PRAGMA integrity_check").fetchall() == [("ok",)]
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []


# ----------------------------------------------------------------------
# A. Schema
# ----------------------------------------------------------------------


def test_the_schema_is_additive_and_leaves_daily_ohlcv_untouched(database: Path) -> None:
    with closing(sqlite3.connect(database)) as connection:
        initialize_historical_market_data_schema(connection)
        connection.execute(
            "INSERT INTO historical_ohlcv VALUES "
            "('M&M', 'NSE', '2026-08-12T00:00:00Z', '1d', 'INR', '1', '2', '1', '2', '10', NULL)"
        )
        connection.commit()
        before = connection.execute("SELECT sql FROM sqlite_master WHERE name = 'historical_ohlcv'")
        daily_schema = before.fetchone()
        initialize_equity_candle_evidence_schema(connection)
        initialize_equity_candle_evidence_schema(connection)
        after = connection.execute("SELECT sql FROM sqlite_master WHERE name = 'historical_ohlcv'")
        assert after.fetchone() == daily_schema
        assert connection.execute("SELECT * FROM historical_ohlcv").fetchall() == [
            ("M&M", "NSE", "2026-08-12T00:00:00Z", "1d", "INR", "1", "2", "1", "2", "10", None)
        ]
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
            )
        }
        triggers = {
            row[0]
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'trigger'")
        }
    assert tables == {"historical_ohlcv", *_TABLES}
    assert len(triggers) == len(schema_module.EQUITY_CANDLE_EVIDENCE_TRIGGERS) == 15
    assert {f"{table}_no_update" for table in _TABLES} <= triggers
    assert {f"{table}_no_delete" for table in _TABLES} <= triggers
    _integrity(database)


# ----------------------------------------------------------------------
# B. Journal append, retries and identity conflicts
# ----------------------------------------------------------------------


def test_scenario_a_an_original_observation_is_journaled_with_full_provenance(
    database: Path,
) -> None:
    journal = _journal(database)
    record = _observation(1, published=_end(1) + timedelta(seconds=2))
    appended = journal.append(record)
    finality = journal.append(_finality(1))
    assert appended.outcome is EquityCandleAppendOutcome.APPENDED
    assert (appended.entry.sequence, finality.entry.sequence) == (1, 2)
    assert appended.entry.journaled_at == PointInTime("2026-08-13T00:00:01Z")
    with closing(_raw(database)) as connection:
        row = connection.execute(
            "SELECT entry_kind, ingestion_id, source_id, source_event_id, source_revision, "
            "received_at, source_published_at, is_synthetic, symbol, exchange_code, timeframe, "
            "interval_end, payload_fingerprint, record_fingerprint, record "
            "FROM equity_candle_journal WHERE sequence = 1"
        ).fetchone()
    assert row[:12] == (
        "OBSERVATION",
        "obs-1-None-r1",
        _SOURCE,
        "evt-1",
        "r1",
        "2026-08-12T03:50:05Z",
        "2026-08-12T03:50:02Z",
        1,
        "M&M",
        "NSE",
        "5m",
        "2026-08-12T03:50:00Z",
    )
    assert row[12].startswith("ecp1-sha256:") and row[13].startswith("ecr1-sha256:")
    payload = json.loads(row[14])["payload"]
    assert (payload["open"], payload["high"], payload["low"], payload["close"]) == (
        "1600.05",
        "1604.5",
        "1598",
        "1602.25",
    )
    reopened = SQLiteEquityCandleEvidenceJournal(database)
    window = _window(_LATE)
    read = reopened.read_window(
        _M_AND_M, Timeframe("5m"), window.window_opens_at, window.window_closes_at, 2
    )
    assert read.observations == (appended.entry,)
    assert read.finalities == (finality.entry,)
    _integrity(database)


def test_a_retry_writes_nothing_and_a_reused_ingestion_identity_is_refused(
    database: Path,
) -> None:
    journal = _journal(database)
    first = journal.append(_observation(1, ingestion="receipt-1"))
    again = SQLiteEquityCandleEvidenceJournal(database).append(
        _observation(1, ingestion="receipt-1")
    )
    assert again.outcome is EquityCandleAppendOutcome.ALREADY_APPENDED
    assert again.entry == first.entry
    with pytest.raises(EquityCandleIngestionConflictError):
        journal.append(_observation(1, ingestion="receipt-1", prices=("1", "2", "1", "2")))
    assert journal.watermark() == 1


def test_identical_and_conflicting_receipts_of_one_identity_are_both_kept(database: Path) -> None:
    journal = _journal(database)
    journal.append(_observation(1, ingestion="receipt-1"))
    repeat = journal.append(_observation(1, ingestion="receipt-2"))
    other = journal.append(
        _observation(1, ingestion="receipt-3", prices=("1600", "1605", "1598", "1601"))
    )
    journal.append(_finality(1))
    assert repeat.conflicting_values == ()
    assert other.conflicting_values == (1, 2)
    snapshot = _issue(database, _LATE)
    assert snapshot.intervals[0].outcome is _OUTCOME.CONFLICTING_VALUES
    assert snapshot.intervals[0].observation_sequences == (1, 2, 3)
    assert snapshot.intervals[0].acceptable_sequences == ()


def test_a_contradicting_source_identity_is_refused_by_adapter_and_database(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    journal = _journal(database)
    journal.append(_observation(1, event="evt-x"))
    journal.append(_finality(1, event="evt-x"))
    with pytest.raises(EquityCandleSourceIdentityConflictError):
        journal.append(_observation(2, event="evt-x", ingestion="receipt-2"))
    with pytest.raises(EquityCandleSourceIdentityConflictError):
        journal.append(_finality(1, event="evt-x", ingestion="fin-2", confirmed=_end(2)))
    monkeypatch.setattr(adapter_module, "equity_candle_identity_conflicts", lambda *_: ())
    with pytest.raises(EquityCandleEvidenceStorageError, match="bound to other facts"):
        journal.append(_observation(2, event="evt-x", ingestion="receipt-3"))
    with pytest.raises(EquityCandleEvidenceStorageError, match="bound to other facts"):
        journal.append(_finality(1, event="evt-x", ingestion="fin-3", confirmed=_end(2)))
    assert journal.watermark() == 2


def test_revisions_and_corrections_are_kept_and_never_resolved(database: Path) -> None:
    journal = _journal(database)
    _final_candle(journal, 1)
    journal.append(_observation(2))
    journal.append(_finality(2))
    journal.append(_observation(1, revision="r2"))
    journal.append(_finality(1, revision="r2"))
    journal.append(
        _observation(2, revision="r2", corrects="r1", prices=("1601", "1605", "1599", "1603"))
    )
    journal.append(_finality(2, revision="r2"))
    snapshot = _issue(database, _LATE)
    assert snapshot.intervals[0].outcome is _OUTCOME.UNRESOLVED_REVISIONS
    assert snapshot.intervals[1].outcome is _OUTCOME.UNRESOLVED_REVISIONS
    assert snapshot.intervals[1].observation_sequences == (3, 7)
    assert all(i.acceptable_sequences == () for i in snapshot.intervals[:2])
    assert _count(database, "equity_candle_journal") == 8


def test_out_of_order_arrival_is_ordered_only_by_journal_sequence(database: Path) -> None:
    journal = _journal(database)
    journal.append(_finality(2))
    journal.append(_observation(2))
    journal.append(_observation(1, received=_end(1) + timedelta(seconds=1)))
    journal.append(_finality(1))
    snapshot = _issue(database, _LATE)
    assert snapshot.observation_sequences == (2, 3)
    assert snapshot.finality_sequences == (1, 4)
    assert [i.outcome for i in snapshot.intervals[:2]] == [_OUTCOME.ACCEPTABLE] * 2


def test_reading_beyond_the_journal_and_a_naive_clock_are_refused(database: Path) -> None:
    journal = _journal(database)
    window = _window(_LATE)
    with pytest.raises(InvalidEquityCandleJournalQueryError):
        journal.read_window(
            _M_AND_M, Timeframe("5m"), window.window_opens_at, window.window_closes_at, 1
        )
    naive = SQLiteEquityCandleEvidenceJournal(database, clock=lambda: datetime(2026, 8, 13))
    with pytest.raises(ValueError, match="aware"):
        naive.append(_observation(1))
    assert journal.watermark() == 0


# ----------------------------------------------------------------------
# C. Database append-only protection and integrity
# ----------------------------------------------------------------------


def _populated(database: Path):  # type: ignore[no-untyped-def]
    journal = _journal(database)
    _final_candle(journal, 1)
    return _issue(database, _LATE)


@pytest.mark.parametrize(
    "statement",
    [
        "UPDATE equity_candle_journal SET record = record WHERE sequence = 1",
        "UPDATE equity_candle_journal SET received_at = '2026-08-12T03:49:00Z'",
        "DELETE FROM equity_candle_journal WHERE sequence = 2",
        "UPDATE equity_candle_evidence_snapshots SET coverage = 'WINDOW_COMPLETE'",
        "DELETE FROM equity_candle_evidence_snapshots",
        "UPDATE equity_candle_evidence_snapshot_entries SET record_fingerprint = 'x'",
        "DELETE FROM equity_candle_evidence_snapshot_entries",
        "INSERT OR REPLACE INTO equity_candle_journal "
        "SELECT * FROM equity_candle_journal WHERE sequence = 1",
        "INSERT OR REPLACE INTO equity_candle_evidence_snapshots "
        "SELECT * FROM equity_candle_evidence_snapshots",
        "INSERT OR REPLACE INTO equity_candle_evidence_snapshot_entries "
        "SELECT * FROM equity_candle_evidence_snapshot_entries",
    ],
)
def test_the_database_refuses_to_rewrite_history(database: Path, statement: str) -> None:
    snapshot = _populated(database)
    with closing(_raw(database)) as connection, pytest.raises(sqlite3.IntegrityError):
        connection.execute(statement)
    assert SQLiteEquityCandleEvidenceSnapshotStore(database).get_snapshot(snapshot.snapshot_id) == (
        snapshot
    )
    assert _count(database, "equity_candle_journal") == 2
    _integrity(database)


def test_the_database_refuses_backdated_sequences_and_expanded_manifests(database: Path) -> None:
    snapshot = _populated(database)
    _journal(database).append(_observation(2))
    with closing(_raw(database)) as connection:
        row = list(connection.execute("SELECT * FROM equity_candle_journal WHERE sequence = 3"))[0]
        insert = f"INSERT INTO equity_candle_journal VALUES ({', '.join('?' * len(row))})"
        connection.execute(insert, (100, row[1], "later-jump", *row[3:]))
        with pytest.raises(sqlite3.IntegrityError, match="only increase"):
            connection.execute(insert, (50, row[1], "backdated", *row[3:]))
        with pytest.raises(sqlite3.IntegrityError, match="manifest is closed|watermark"):
            connection.execute(
                "INSERT INTO equity_candle_evidence_snapshot_entries VALUES (?, ?, ?)",
                (snapshot.snapshot_id, 3, row[17]),
            )
        with pytest.raises(sqlite3.IntegrityError, match="beyond the journal"):
            connection.execute(
                "INSERT INTO equity_candle_evidence_snapshots "
                "SELECT 'other', manifest_fingerprint, symbol, exchange_code, trading_date, "
                "timeframe, window_policy_version, finality_policy_version, as_of, issued_at, "
                "999, evidence_class, coverage, entry_count, document "
                "FROM equity_candle_evidence_snapshots"
            )
    with closing(_raw(database)) as connection:
        sequences = [
            r[0]
            for r in connection.execute(
                "SELECT sequence FROM equity_candle_journal ORDER BY sequence"
            )
        ]
    assert sequences == [1, 2, 3, 100]
    assert _count(database, "equity_candle_evidence_snapshot_entries") == 2


@pytest.mark.parametrize(
    "corruption",
    [
        "UPDATE equity_candle_journal SET record = replace(record, '1602.25', '1702.25') "
        "WHERE sequence = 1",
        "UPDATE equity_candle_journal SET received_at = '2026-08-12T03:50:06Z' WHERE sequence = 1",
        "UPDATE equity_candle_evidence_snapshots SET coverage = 'WINDOW_COMPLETE'",
        "UPDATE equity_candle_evidence_snapshots SET document = replace(document, "
        "'\"watermark\":2', '\"watermark\":1')",
        "DELETE FROM equity_candle_evidence_snapshot_entries WHERE sequence = 2",
        "INSERT INTO equity_candle_journal (entry_kind, ingestion_id, journaled_at, source_id, "
        "source_event_id, source_revision, received_at, is_synthetic, symbol, exchange_code, "
        "timeframe, interval_end, payload_fingerprint, record_fingerprint, record) "
        "SELECT entry_kind, 'smuggled', journaled_at, source_id, 'evt-9', source_revision, "
        "received_at, is_synthetic, symbol, exchange_code, timeframe, interval_end, "
        "payload_fingerprint, record_fingerprint, record FROM equity_candle_journal "
        "WHERE sequence = 1",
    ],
)
def test_corrupted_evidence_fails_closed_and_is_never_repaired(
    database: Path, corruption: str
) -> None:
    snapshot = _populated(database)
    with closing(_raw(database)) as connection:
        for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'trigger'"):
            connection.execute(f"DROP TRIGGER {row[0]}")
        connection.execute(corruption)
    store = SQLiteEquityCandleEvidenceSnapshotStore(database)
    if "smuggled" in corruption:
        # A row slipped in with a backdated identity is above the watermark;
        # the issued snapshot is unaffected but the forged row cannot be read.
        assert store.get_snapshot(snapshot.snapshot_id) == snapshot
        window = _window(_LATE)
        with pytest.raises(EquityCandleEvidenceStorageError):
            _journal(database).read_window(
                _M_AND_M, Timeframe("5m"), window.window_opens_at, window.window_closes_at, 3
            )
        return
    with pytest.raises(EquityCandleEvidenceStorageError):
        store.get_snapshot(snapshot.snapshot_id)


def test_a_failed_issuance_rolls_back_entirely(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    journal = _journal(database)
    _final_candle(journal, 1)
    real = adapter_module._snapshot_entries

    def forged(snapshot):  # type: ignore[no-untyped-def]
        entries = real(snapshot)
        return [*entries[:-1], (entries[-1][0], "ecr1-sha256:" + "0" * 64)]

    monkeypatch.setattr(adapter_module, "_snapshot_entries", forged)
    with pytest.raises(EquityCandleEvidenceStorageError, match="fingerprint"):
        _issue(database, _LATE)
    assert _count(database, "equity_candle_evidence_snapshots") == 0
    assert _count(database, "equity_candle_evidence_snapshot_entries") == 0
    _integrity(database)


def test_a_failed_append_rolls_back_entirely(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    journal = _journal(database)
    journal.append(_observation(1))

    def fail_after_insert(*_args: object, **_kwargs: object) -> None:
        raise sqlite3.OperationalError("disk I/O error after the insert")

    monkeypatch.setattr(adapter_module, "EquityCandleAppendResult", fail_after_insert)
    with pytest.raises(EquityCandleEvidenceStorageError, match="disk I/O"):
        journal.append(_observation(2))
    assert journal.watermark() == 1
    assert _count(database, "equity_candle_journal") == 1


# ----------------------------------------------------------------------
# D. Snapshots: scenarios B-E, chronology and concurrency
# ----------------------------------------------------------------------


def test_scenario_b_a_snapshot_reopens_identically_from_a_fresh_connection(
    database: Path,
) -> None:
    journal = _journal(database)
    _final_candle(journal, 1)
    as_of = _end(1) + timedelta(minutes=1)
    snapshot = _issue(database, as_of)
    assert snapshot.coverage is EquityCandleCoverage.COMPLETE_AS_OF
    assert snapshot.evidence_class is EquityCandleEvidenceClass.SYNTHETIC
    assert snapshot.observations[0].finding is EquityCandleFinding.ACCEPTABLE
    reopened = SQLiteEquityCandleEvidenceSnapshotStore(database).get_snapshot(snapshot.snapshot_id)
    assert reopened == snapshot
    assert reopened is not None
    assert reopened.manifest_fingerprint == snapshot.manifest_fingerprint
    assert reopened.observations[0].record == _observation(1)
    assert _replay(database, snapshot.snapshot_id, as_of).outcome is (
        EquityCandleSnapshotReplayOutcome.REPRODUCED
    )
    assert SQLiteEquityCandleEvidenceSnapshotStore(database).issue(snapshot) == 0
    with closing(_raw(database)) as connection:
        row = connection.execute(
            "SELECT watermark, evidence_class, coverage, entry_count, manifest_fingerprint "
            "FROM equity_candle_evidence_snapshots"
        ).fetchone()
    assert row == (2, "SYNTHETIC", "COMPLETE_AS_OF", 2, snapshot.manifest_fingerprint)
    _integrity(database)


def test_scenario_c_a_later_correction_leaves_the_issued_snapshot_unchanged(
    database: Path,
) -> None:
    journal = _journal(database)
    _final_candle(journal, 1)
    as_of = _end(1) + timedelta(minutes=1)
    original = _issue(database, as_of)
    journal.append(
        _observation(
            1,
            revision="r2",
            corrects="r1",
            prices=("1600", "1606", "1597", "1604"),
            received=_end(1) + timedelta(seconds=30),
        )
    )
    journal.append(_finality(1, revision="r2", confirmed=_end(1) + timedelta(seconds=31)))
    store = SQLiteEquityCandleEvidenceSnapshotStore(database)
    assert store.get_snapshot(original.snapshot_id) == original
    assert _replay(database, original.snapshot_id, as_of).outcome is (
        EquityCandleSnapshotReplayOutcome.REPRODUCED
    )
    later = _issue(database, as_of, issued=as_of + timedelta(hours=1))
    assert later.intervals[0].outcome is _OUTCOME.UNRESOLVED_REVISIONS
    assert later.intervals[0].acceptable_sequences == ()
    assert later.observation_sequences == (1, 3)
    with closing(_raw(database)) as connection:
        stored = [row[0] for row in connection.execute("SELECT record FROM equity_candle_journal")]
    assert len(stored) == 4
    assert json.loads(stored[0])["payload"]["close"] == "1602.25"
    assert json.loads(stored[2])["payload"]["close"] == "1604"


def test_scenario_d_backdated_ingestion_cannot_enter_an_earlier_snapshot(database: Path) -> None:
    journal = _journal(database)
    _final_candle(journal, 1)
    as_of = _end(2) + timedelta(minutes=1)
    earlier = _issue(database, as_of)
    assert earlier.intervals[1].outcome is _OUTCOME.MISSING
    journal.append(_observation(2, received=_end(2) + timedelta(seconds=1), published=_end(2)))
    journal.append(_finality(2, confirmed=_end(2) + timedelta(seconds=2)))
    reopened = SQLiteEquityCandleEvidenceSnapshotStore(database).get_snapshot(earlier.snapshot_id)
    assert reopened == earlier
    assert reopened is not None and reopened.intervals[1].outcome is _OUTCOME.MISSING
    assert _replay(database, earlier.snapshot_id, as_of).outcome is (
        EquityCandleSnapshotReplayOutcome.REPRODUCED
    )
    later = _issue(database, as_of, issued=as_of + timedelta(minutes=10))
    assert (earlier.watermark, later.watermark) == (2, 4)
    assert later.intervals[1].outcome is _OUTCOME.ACCEPTABLE


def test_finality_arriving_after_issuance_never_alters_the_snapshot(database: Path) -> None:
    journal = _journal(database)
    journal.append(_observation(1))
    as_of = _end(1) + timedelta(minutes=1)
    snapshot = _issue(database, as_of)
    assert snapshot.intervals[0].outcome is _OUTCOME.UNFINALIZED
    journal.append(_finality(1, confirmed=_end(1) + timedelta(seconds=10)))
    assert SQLiteEquityCandleEvidenceSnapshotStore(database).get_snapshot(snapshot.snapshot_id) == (
        snapshot
    )
    # A later snapshot at the same instant may use it: it claims receipt before as_of.
    later = _issue(database, as_of, issued=as_of + timedelta(hours=2))
    assert later.intervals[0].outcome is _OUTCOME.ACCEPTABLE
    # A statement received after the observation instant is never composed, even
    # though its source confirmation time is earlier.
    journal.append(_observation(2))
    journal.append(_finality(2, confirmed=_end(2) + timedelta(seconds=10), received=_LATE))
    late = _issue(database, _end(2) + timedelta(minutes=1), issued=_LATE)
    assert late.observations[1].finding is EquityCandleFinding.FINALITY_MISSING
    assert late.finality_sequences == (2, 4)


def test_scenario_e_a_complete_69_interval_synthetic_window_survives_reopening(
    database: Path,
) -> None:
    journal = _journal(database)
    for index in range(1, 70):
        _final_candle(journal, index)
    midday = _end(30) + timedelta(minutes=1)
    partial = _issue(database, midday)
    complete = _issue(database, _LATE)
    assert len(complete.intervals) == 69
    assert complete.intervals[0].closes_at == PointInTime("2026-08-12T03:50:00Z")
    assert complete.intervals[-1].closes_at == PointInTime("2026-08-12T09:30:00Z")
    assert all(i.outcome is _OUTCOME.ACCEPTABLE for i in complete.intervals)
    assert complete.coverage is EquityCandleCoverage.WINDOW_COMPLETE
    assert partial.coverage is EquityCandleCoverage.COMPLETE_AS_OF
    assert complete.evidence_class is EquityCandleEvidenceClass.SYNTHETIC
    for observation in complete.observations:
        evidence = next(
            f.record.evidence
            for f in complete.finalities
            if f.sequence in observation.finality_sequences
        )
        assert evidence.confirmed_at.compare(observation.record.observation.bar.point_in_time) > 0
        assert evidence.confirmed_at.compare(complete.as_of) <= 0
    _integrity(database)
    copy = database.with_name("reopened.sqlite3")
    with closing(sqlite3.connect(database)) as source, closing(sqlite3.connect(copy)) as target:
        source.backup(target)
    store = SQLiteEquityCandleEvidenceSnapshotStore(copy)
    assert store.get_snapshot(complete.snapshot_id) == complete
    assert store.get_snapshot(partial.snapshot_id) == partial
    assert _replay(copy, complete.snapshot_id, _LATE).outcome is (
        EquityCandleSnapshotReplayOutcome.REPRODUCED
    )
    rebuilt = database.with_name("rebuilt.sqlite3")
    rebuilt_journal = _journal(rebuilt)
    for index in range(1, 70):
        _final_candle(rebuilt_journal, index)
    assert _issue(rebuilt, _LATE).manifest_fingerprint == complete.manifest_fingerprint


def test_an_ineligible_stock_or_date_is_never_snapshotted(database: Path) -> None:
    journal = _journal(database)
    _final_candle(journal, 1)
    window = _window(_LATE, _RELIANCE)
    assert not window.is_eligible
    use_case = IssueEquityCandleEvidenceSnapshotUseCase(
        journal,
        SQLiteEquityCandleEvidenceSnapshotStore(database),
        _INR,
        _SYNTHETIC,
        reference_provenance=_PROVENANCE,
    )
    with pytest.raises(EquityCandleSnapshotRefusedError):
        use_case.execute(window, _pit(_LATE), _pit(_LATE))
    assert _count(database, "equity_candle_evidence_snapshots") == 0


def test_the_store_refuses_a_snapshot_that_is_not_the_journal_at_its_watermark(
    database: Path,
) -> None:
    journal = _journal(database)
    _final_candle(journal, 1)
    snapshot = _issue(database, _LATE)
    other = database.with_name("other.sqlite3")
    other_journal = _journal(other)
    _final_candle(other_journal, 1)
    other_journal.append(_observation(2))
    with pytest.raises(EquityCandleSnapshotEvidenceError, match="exactly"):
        SQLiteEquityCandleEvidenceSnapshotStore(other).issue(
            dataclasses.replace(snapshot, watermark=3)
        )
    empty = database.with_name("empty.sqlite3")
    with pytest.raises(EquityCandleSnapshotEvidenceError, match="beyond"):
        SQLiteEquityCandleEvidenceSnapshotStore(empty).issue(snapshot)
    assert _count(other, "equity_candle_evidence_snapshots") == 0


def test_uncommitted_ingestion_is_invisible_and_cannot_join_a_snapshot(database: Path) -> None:
    journal = _journal(database)
    _final_candle(journal, 1)
    writer = _raw(database)
    try:
        writer.execute("BEGIN IMMEDIATE")
        values = adapter_module._journal_values(_observation(2), _pit(_LATE))
        writer.execute(adapter_module._INSERT_JOURNAL, values)
        assert journal.watermark() == 2
        window = _window(_LATE)
        evidence = journal.read_window(
            _M_AND_M, Timeframe("5m"), window.window_opens_at, window.window_closes_at, 2
        )
        assert len(evidence.observations) == 1
    finally:
        writer.rollback()
        writer.close()
    snapshot = _issue(database, _LATE)
    assert snapshot.observation_sequences == (1,)
    assert journal.watermark() == 2


class _Paused(SQLiteEquityCandleEvidenceJournal):
    """Pauses the first issuance attempt after it has read the journal at its watermark.

    It signals ``read_done`` and waits for ``resume``; later attempts run
    straight through. Synchronisation is explicit -- no sleeps, no timing.
    """

    def __init__(self, database: Path) -> None:
        super().__init__(database, clock=_Clock())
        self.read_done = threading.Event()
        self.resume = threading.Event()
        self.reads: list[int] = []

    def read_window(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        window = super().read_window(*args, **kwargs)
        self.reads.append(window.watermark)
        if len(self.reads) == 1:
            self.read_done.set()
            assert self.resume.wait(timeout=30), "the test never resumed issuance"
        return window


def _issue_in_thread(database: Path, journal: _Paused, outcome: dict) -> threading.Thread:  # type: ignore[type-arg]
    def run() -> None:
        try:
            outcome["snapshot"] = _issue(database, _LATE, journal=journal)
        except BaseException as exc:  # noqa: BLE001
            outcome["error"] = exc

    thread = threading.Thread(target=run)
    thread.start()
    return thread


def test_a_correction_committed_between_watermark_and_issuance_is_never_omitted(
    database: Path,
) -> None:
    _final_candle(_journal(database), 1)
    paused = _Paused(database)
    outcome: dict[str, object] = {}
    thread = _issue_in_thread(database, paused, outcome)
    assert paused.read_done.wait(timeout=30)
    # Issuance holds evidence at watermark 2. Another connection commits a
    # correction for the same interval at sequence 3 and its finality at 4.
    other = SQLiteEquityCandleEvidenceJournal(database)
    correction = other.append(
        _observation(1, revision="r2", corrects="r1", prices=("1600", "1606", "1597", "1604"))
    )
    other.append(_finality(1, revision="r2"))
    assert correction.entry.sequence == 3
    paused.resume.set()
    thread.join(timeout=30)
    assert not thread.is_alive()
    assert "error" not in outcome
    snapshot = outcome["snapshot"]
    # The first attempt (watermark 2) was refused as stale; the retry re-read at 4.
    assert paused.reads == [2, 4]
    assert snapshot.watermark == 4  # type: ignore[attr-defined]
    assert snapshot.observation_sequences == (1, 3)  # type: ignore[attr-defined]
    assert snapshot.intervals[0].outcome is _OUTCOME.UNRESOLVED_REVISIONS  # type: ignore[attr-defined]
    assert _count(database, "equity_candle_evidence_snapshots") == 1
    with closing(_raw(database)) as connection:
        assert connection.execute(
            "SELECT watermark FROM equity_candle_evidence_snapshots"
        ).fetchall() == [(4,)]
    _integrity(database)


def test_an_unrelated_commit_between_watermark_and_issuance_does_not_block_it(
    database: Path,
) -> None:
    _final_candle(_journal(database), 1)
    paused = _Paused(database)
    outcome: dict[str, object] = {}
    thread = _issue_in_thread(database, paused, outcome)
    assert paused.read_done.wait(timeout=30)
    SQLiteEquityCandleEvidenceJournal(database).append(
        _observation(1, listing=_RELIANCE, ingestion="other-listing", event="evt-other")
    )
    paused.resume.set()
    thread.join(timeout=30)
    assert "error" not in outcome
    snapshot = outcome["snapshot"]
    assert paused.reads == [2]
    assert snapshot.watermark == 2  # type: ignore[attr-defined]
    assert snapshot.intervals[0].outcome is _OUTCOME.ACCEPTABLE  # type: ignore[attr-defined]
    assert SQLiteEquityCandleEvidenceJournal(database).watermark() == 3


def test_the_store_refuses_a_stale_snapshot_and_writes_nothing(database: Path) -> None:
    journal = _journal(database)
    _final_candle(journal, 1)
    snapshot = _issue(database, _LATE)
    other = database.with_name("stale.sqlite3")
    other_journal = _journal(other)
    _final_candle(other_journal, 1)
    other_journal.append(_finality(1, ingestion="late-finality-repeat"))
    with pytest.raises(EquityCandleSnapshotStaleError, match="gained evidence"):
        SQLiteEquityCandleEvidenceSnapshotStore(other).issue(snapshot)
    assert _count(other, "equity_candle_evidence_snapshots") == 0
    assert _count(other, "equity_candle_evidence_snapshot_entries") == 0
    _integrity(other)


def test_issuance_stops_after_bounded_attempts_when_the_window_keeps_changing(
    database: Path,
) -> None:
    _final_candle(_journal(database), 1)

    class _Churning(SQLiteEquityCandleEvidenceJournal):
        calls = 0

        def read_window(self, *args, **kwargs):  # type: ignore[no-untyped-def]
            window = super().read_window(*args, **kwargs)
            type(self).calls += 1
            SQLiteEquityCandleEvidenceJournal(database).append(
                _observation(1, revision=f"r{self.calls + 1}")
            )
            return window

    with pytest.raises(EquityCandleSnapshotStaleError, match="3 issuance attempts"):
        _issue(database, _LATE, journal=_Churning(database))
    assert _Churning.calls == 3
    assert _count(database, "equity_candle_evidence_snapshots") == 0


# ----------------------------------------------------------------------
# Reference provenance
# ----------------------------------------------------------------------


def _test_venue() -> NSEEquityVenueDayReferenceSource:
    """A venue source over a copy of the production catalog -- not the production object."""
    catalog = dataclasses.replace(PRODUCTION_NSE_EQUITY_SESSION_CATALOG)
    assert catalog == PRODUCTION_NSE_EQUITY_SESSION_CATALOG and not catalog.is_production
    return NSEEquityVenueDayReferenceSource(NSEEquityVenueSessionReference(catalog))


def test_production_venue_and_synthetic_applicability_are_classified_from_their_sources() -> None:
    window = _window(_LATE)
    provenance = _PROVENANCE.classify(window)
    assert (provenance.venue, provenance.applicability) == (_PROD, _SYN)
    assert provenance.venue_reference.startswith("nse-accepted-equity-session-evidence")
    assert provenance.applicability_reference == _APPLICABILITY.authority


def test_a_non_production_catalog_is_synthetic_even_with_identical_content() -> None:
    venue = _test_venue()
    window = _window(_LATE, venue=venue)
    provenance = NSEEquityResearchReferenceProvenance(venue, _APPLICABILITY).classify(window)
    assert (provenance.venue, provenance.applicability) == (_SYN, _SYN)


def test_mixed_references_never_make_a_production_snapshot(database: Path) -> None:
    _final_candle(_journal(database), 1)
    snapshot = _issue(database, _LATE)
    assert (snapshot.venue_provenance, snapshot.applicability_provenance) == (_PROD, _SYN)
    assert snapshot.evidence_class is EquityCandleEvidenceClass.SYNTHETIC
    reopened = SQLiteEquityCandleEvidenceSnapshotStore(database).get_snapshot(snapshot.snapshot_id)
    assert reopened is not None and reopened.applicability_provenance is _SYN


class _RenamedApplicability(SyntheticListingApplicabilitySource):
    """Synthetic records under a production-looking reference name."""

    def applicability(self, listing, trading_date):  # type: ignore[no-untyped-def]
        answer = super().applicability(listing, trading_date)
        return dataclasses.replace(answer, evidence_reference="nse-approved-applicability/1")


def test_renamed_or_unknown_references_fail_closed(database: Path) -> None:
    _final_candle(_journal(database), 1)
    renamed_source = _RenamedApplicability(
        "ei2b-sqlite/1",
        (_synthetic_applicability()._records[(_M_AND_M, _DAY)],),  # noqa: SLF001
    )
    renamed_window = _window(_LATE, applicability=renamed_source)
    assert renamed_window.is_eligible
    # A subclass is not a known source, whatever it answers.
    with pytest.raises(UnknownEquityReferenceProvenanceError, match="no known provenance"):
        NSEEquityResearchReferenceProvenance(_VENUE, renamed_source)
    # The known synthetic source did not produce the renamed reference.
    with pytest.raises(UnknownEquityReferenceProvenanceError, match="not produced"):
        _PROVENANCE.classify(renamed_window)
    # Another version of a synthetic source is another reference.
    other_version = _window(_LATE, applicability=_synthetic_applicability("ei2b-renamed/1"))
    with pytest.raises(UnknownEquityReferenceProvenanceError, match="not produced"):
        _PROVENANCE.classify(other_version)
    # A window from another venue catalog is not this source's either.
    with pytest.raises(UnknownEquityReferenceProvenanceError, match="venue reference"):
        _PROVENANCE.classify(_window(_LATE, venue=_test_venue()))
    use_case = IssueEquityCandleEvidenceSnapshotUseCase(
        _journal(database),
        SQLiteEquityCandleEvidenceSnapshotStore(database),
        _INR,
        _SYNTHETIC,
        reference_provenance=_PROVENANCE,
    )
    for window in (renamed_window, other_version):
        with pytest.raises(UnknownEquityReferenceProvenanceError):
            use_case.execute(window, _pit(_LATE), _pit(_LATE))
    assert _count(database, "equity_candle_evidence_snapshots") == 0


def test_unknown_source_types_and_missing_provenance_are_refused(database: Path) -> None:
    class _Other(NoApprovedEquityListingApplicabilitySource):
        def applicability(self, listing, trading_date):  # type: ignore[no-untyped-def]
            return EquityListingApplicability(
                listing,
                trading_date,
                EquityListingApplicabilityClassification.ELIGIBLE_FOR_NORMAL_RESEARCH_WINDOW,
                "looks-approved/1",
                "x",
            )

    with pytest.raises(UnknownEquityReferenceProvenanceError):
        NSEEquityResearchReferenceProvenance(_VENUE, _Other())
    with pytest.raises(UnknownEquityReferenceProvenanceError):
        NSEEquityResearchReferenceProvenance(object(), _APPLICABILITY)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        IssueEquityCandleEvidenceSnapshotUseCase(  # type: ignore[call-arg]
            _journal(database), SQLiteEquityCandleEvidenceSnapshotStore(database), _INR, _SYNTHETIC
        )


def test_accepted_production_references_are_never_eligible_and_approve_no_finality(
    database: Path,
) -> None:
    production = NSEEquityResearchReferenceProvenance(
        _VENUE, NoApprovedEquityListingApplicabilitySource()
    )
    window = _window(_LATE, applicability=NoApprovedEquityListingApplicabilitySource())
    assert not window.is_eligible
    assert production.classify(window).is_accepted_production
    _final_candle(_journal(database), 1)
    use_case = IssueEquityCandleEvidenceSnapshotUseCase(
        _journal(database),
        SQLiteEquityCandleEvidenceSnapshotStore(database),
        _INR,
        reference_provenance=production,
    )
    with pytest.raises(EquityCandleSnapshotRefusedError):
        use_case.execute(window, _pit(_LATE), _pit(_LATE))
    # The production venue with the current no-approved finality policy.
    snapshot = IssueEquityCandleEvidenceSnapshotUseCase(
        _journal(database),
        SQLiteEquityCandleEvidenceSnapshotStore(database),
        _INR,
        reference_provenance=_PROVENANCE,
    ).execute(_window(_LATE), _pit(_LATE), _pit(_LATE))
    assert snapshot.finality_policy.version == "no-approved-equity-candle-finality/1"
    assert snapshot.venue_provenance is _PROD
    assert snapshot.evidence_class is EquityCandleEvidenceClass.SYNTHETIC
    assert snapshot.observations[0].finding is EquityCandleFinding.SYNTHETIC_FINALITY_NOT_PERMITTED
    assert snapshot.intervals[0].outcome is _OUTCOME.UNFINALIZED


def test_a_writer_holding_the_lock_makes_issuance_fail_without_writing(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    journal = _journal(database)
    _final_candle(journal, 1)
    window = _window(_LATE)
    monkeypatch.setattr(adapter_module, "_BUSY_TIMEOUT_SECONDS", 0.05)
    holder = _raw(database)
    try:
        holder.execute("BEGIN IMMEDIATE")
        with pytest.raises(EquityCandleEvidenceStorageError):
            IssueEquityCandleEvidenceSnapshotUseCase(
                journal,
                SQLiteEquityCandleEvidenceSnapshotStore(database),
                _INR,
                _SYNTHETIC,
                reference_provenance=_PROVENANCE,
            ).execute(window, _pit(_LATE), _pit(_LATE))
    finally:
        holder.rollback()
        holder.close()
    assert _count(database, "equity_candle_evidence_snapshots") == 0


def test_concurrent_appends_receive_distinct_increasing_sequences(database: Path) -> None:
    _journal(database).watermark()
    errors: list[BaseException] = []

    def ingest(offset: int) -> None:
        journal = SQLiteEquityCandleEvidenceJournal(database)
        try:
            for index in range(1, 11):
                journal.append(
                    _observation(index, ingestion=f"t{offset}-{index}", event=f"e{offset}-{index}")
                )
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=ingest, args=(n,)) for n in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert errors == []
    with closing(_raw(database)) as connection:
        sequences = [
            row[0]
            for row in connection.execute(
                "SELECT sequence FROM equity_candle_journal ORDER BY sequence"
            )
        ]
    assert sequences == list(range(1, 41))
    _integrity(database)


# ----------------------------------------------------------------------
# E. Scope
# ----------------------------------------------------------------------


@pytest.mark.parametrize("module", [adapter_module, schema_module, provenance_module])
def test_adapters_import_no_provider_network_or_broker(module) -> None:  # type: ignore[no-untyped-def]
    tree = ast.parse(inspect.getsource(module))
    imported = {node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)}
    imported |= {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    for name in imported:
        assert name is not None
        for forbidden in ("socket", "urllib", "requests", "http", "upstox", "databento", "yahoo"):
            assert forbidden not in name, name
        assert "broker" not in name and "recommendation" not in name, name
    source = inspect.getsource(module)
    assert "historical_ohlcv" not in source.replace("historical_ohlcv`` table", "")
