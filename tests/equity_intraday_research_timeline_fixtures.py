"""Shared synthetic fixtures for the EI-3C-B research timeline tests.

The chain is the real one, over one temporary SQLite file: EI-1C-3
eligibility over the accepted production NSE venue reference and explicitly
synthetic applicability; synthetic candles and finality from
SyntheticEquityCandleEventSource, journaled by EI-2C ingestion; EI-2B
snapshots issued through the EI-2C issuance workflow; the EI-3C-A advancement
use case over the EI-3C-B SQLite store. Every price and identity is invented,
and the calibration (opening range 3 bars, volume multiplier 1) is a synthetic
example, never an approved parameter.

The trading dates are ones the production venue reference states as normal
sessions; tests confirm eligibility rather than assume it.
"""

from __future__ import annotations

import sqlite3
from contextlib import closing
from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from northstar_application.application_services import (
    AdvanceEquityIntradayResearchTimelineUseCase,
    EquityCandleEvidenceSnapshot,
    EquityCandleFinalityPolicy,
    EquityCandleJournalRecord,
    EquityIntradayResearchTimelineAdvance,
    EquityOpeningRangeBreakoutConfiguration,
    EquityResearchEvidenceOutcome,
    EvaluateEquityResearchWindowUseCase,
    IngestSyntheticEquityCandleEventsUseCase,
    IssueEquityCandleEvidenceSnapshotUseCase,
    IssueEquityResearchEvidenceUseCase,
)
from northstar_application.ports import (
    EquityIntradayResearchTimelineStore,
    EquityListingApplicabilityClassification,
)
from northstar_core.domain.value_objects import ListingReference
from northstar_core.foundation.value_objects import Currency, ExchangeCode, PointInTime, Symbol
from northstar_core.strategy import StrategyIdentity

from northstar_infrastructure.market_data import (
    NSEEquityResearchReferenceProvenance,
    NSEEquityVenueDayReferenceSource,
    SyntheticEquityCandleEventSource,
    SyntheticListingApplicabilityRecord,
    SyntheticListingApplicabilitySource,
)
from northstar_infrastructure.persistence import (
    SQLiteEquityCandleEvidenceJournal,
    SQLiteEquityCandleEvidenceSnapshotStore,
    SQLiteEquityIntradayResearchTimelineStore,
)

NSE = ExchangeCode("NSE")
INR = Currency("INR")
M_AND_M = ListingReference(Symbol("M&M"), NSE)
RELIANCE = ListingReference(Symbol("RELIANCE"), NSE)
DAY = date(2026, 8, 12)
OTHER_DAY = date(2026, 9, 7)
IST = timezone(timedelta(hours=5, minutes=30))
SOURCE = "synthetic-ei3c-sqlite-source"
VERSION = "ei3c-sqlite/1"
POLICY = EquityCandleFinalityPolicy("synthetic-ei3c-sqlite/1", frozenset({SOURCE}), True)
VENUE = NSEEquityVenueDayReferenceSource.production()
_ELIGIBLE = EquityListingApplicabilityClassification.ELIGIBLE_FOR_NORMAL_RESEARCH_WINDOW
APPLICABILITY = SyntheticListingApplicabilitySource(
    VERSION,
    tuple(
        SyntheticListingApplicabilityRecord(listing, day, _ELIGIBLE, "synthetic")
        for listing, day in ((M_AND_M, DAY), (RELIANCE, DAY), (M_AND_M, OTHER_DAY))
    ),
)
PROVENANCE = NSEEquityResearchReferenceProvenance(VENUE, APPLICABILITY)
CONFIG = EquityOpeningRangeBreakoutConfiguration(
    strategy=StrategyIdentity("ei3c-sqlite-orb-vwap-volume"),
    version="synthetic-ei3c-sqlite/1",
    opening_range_bars=3,
    volume_multiplier=Decimal("1"),
    minimum_baseline_bars=3,
)

# Bars 1-3 form the opening range (high 102); bar 4 (ends 09:35 IST) breaks
# out and bars 5 and 6 qualify again. Volumes are the source's, rising by 5
# per bar, so each bar's volume exceeds the mean of the bars before it.
MORNING: dict[int, tuple[str, str, str, str]] = {
    1: ("100", "101", "99", "100.5"),
    2: ("100.5", "102", "100", "101.5"),
    3: ("101.5", "102", "100.5", "101"),
    4: ("101", "103", "101", "102.5"),
    5: ("102.5", "104", "102", "103.5"),
    6: ("103.5", "105", "103", "104.5"),
}
CORRECTED_BAR_2 = ("100.5", "102.5", "100", "101.75")


def pit(value: datetime) -> PointInTime:
    return PointInTime(value.astimezone(UTC).isoformat().replace("+00:00", "Z"))


def ist(hour: int, minute: int, second: int = 0, day: date = DAY) -> PointInTime:
    return pit(datetime(day.year, day.month, day.day, hour, minute, second, tzinfo=IST))


def interval_end(index: int, day: date = DAY) -> PointInTime:
    """Return the end of five-minute bar ``index``; bar 1 ends 09:20 IST."""
    start = datetime(day.year, day.month, day.day, 9, 15, tzinfo=IST)
    return pit(start + timedelta(minutes=5 * index))


def events(listing: ListingReference = M_AND_M) -> SyntheticEquityCandleEventSource:
    return SyntheticEquityCandleEventSource(VERSION, SOURCE, listing)


class Clock:
    """Deterministic journal clock: one second per append, from a fixed instant."""

    def __init__(self) -> None:
        self.ticks = 0

    def __call__(self) -> datetime:
        self.ticks += 1
        return datetime(2026, 11, 7, tzinfo=UTC) + timedelta(seconds=self.ticks)


class Harness:
    """Freshly created adapters and use cases over one database file -- a 'process'."""

    def __init__(self, database: Path, clock: Clock | None = None) -> None:
        self.database = database
        self.journal = SQLiteEquityCandleEvidenceJournal(database, clock=clock or Clock())
        self.snapshots = SQLiteEquityCandleEvidenceSnapshotStore(database)
        self.timelines = SQLiteEquityIntradayResearchTimelineStore(database)
        self.ingestion = IngestSyntheticEquityCandleEventsUseCase(self.journal, frozenset({SOURCE}))
        self.issuance = IssueEquityResearchEvidenceUseCase(
            EvaluateEquityResearchWindowUseCase(VENUE, APPLICABILITY),
            IssueEquityCandleEvidenceSnapshotUseCase(
                self.journal, self.snapshots, INR, POLICY, reference_provenance=PROVENANCE
            ),
        )

    def ingest(self, records: tuple[EquityCandleJournalRecord, ...] | list) -> None:  # type: ignore[type-arg]
        self.ingestion.execute(records)

    def candles(
        self,
        prices: dict[int, tuple[str, str, str, str]] = MORNING,
        listing: ListingReference = M_AND_M,
        day: date = DAY,
    ) -> None:
        source = events(listing)
        records: list[EquityCandleJournalRecord] = []
        for index, values in sorted(prices.items()):
            end = interval_end(index, day)
            records += [source.observation(end, prices=values), source.finality(end)]
        self.ingest(records)

    def correct_bar_2(self) -> None:
        """Journal a revision of bar 2 and its finality: an unresolved source revision."""
        source, end = events(), interval_end(2)
        self.ingest(
            [
                source.observation(end, revision="r2", corrects="r1", prices=CORRECTED_BAR_2),
                source.finality(end, revision="r2"),
            ]
        )

    def unrelated(self, index: int = 1) -> None:
        """Journal a candle outside M&M's window: the next M&M snapshot gets a higher watermark."""
        source, end = events(RELIANCE), interval_end(index, OTHER_DAY)
        self.ingest([source.observation(end, prices=MORNING[1])])

    def issue(
        self,
        as_of: PointInTime,
        issued_at: PointInTime | None = None,
        listing: ListingReference = M_AND_M,
        day: date = DAY,
    ) -> EquityCandleEvidenceSnapshot:
        issued = self.issuance.execute(listing, day, as_of, issued_at or as_of)
        assert issued.outcome is EquityResearchEvidenceOutcome.ISSUED, issued.outcome
        assert issued.snapshot is not None
        return issued.snapshot

    def at(self, *minutes: int) -> list[EquityCandleEvidenceSnapshot]:
        """Issue one M&M snapshot at each 09:MM IST instant."""
        return [self.issue(ist(9, minute)) for minute in minutes]

    def writer(
        self,
        configuration: EquityOpeningRangeBreakoutConfiguration = CONFIG,
        timelines: EquityIntradayResearchTimelineStore | None = None,
    ) -> AdvanceEquityIntradayResearchTimelineUseCase:
        return AdvanceEquityIntradayResearchTimelineUseCase(
            timelines or self.timelines, self.snapshots, configuration
        )

    def advance(
        self,
        snapshots: list[EquityCandleEvidenceSnapshot],
        configuration: EquityOpeningRangeBreakoutConfiguration = CONFIG,
        listing: ListingReference = M_AND_M,
        day: date = DAY,
        timelines: EquityIntradayResearchTimelineStore | None = None,
    ) -> EquityIntradayResearchTimelineAdvance:
        return self.writer(configuration, timelines).execute(
            listing, day, [s.snapshot_id for s in snapshots]
        )


def raw(database: Path) -> sqlite3.Connection:
    """A direct connection with foreign keys on, as the adapters use."""
    connection = sqlite3.connect(database, isolation_level=None)
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def count(database: Path, table: str) -> int:
    with closing(raw(database)) as connection:
        return connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]  # type: ignore[no-any-return]  # noqa: S608


def entry_rows(database: Path) -> list[dict[str, Any]]:
    with closing(raw(database)) as connection:
        connection.row_factory = sqlite3.Row
        return [
            dict(row)
            for row in connection.execute(
                "SELECT * FROM equity_intraday_research_timeline_entries "
                "ORDER BY timeline_id, sequence"
            )
        ]


def check_invariants(database: Path) -> None:
    """Inspect stored rows directly: every timeline invariant must hold."""
    with closing(raw(database)) as connection:
        assert connection.execute("PRAGMA integrity_check").fetchall() == [("ok",)]
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
        keys = connection.execute(
            "SELECT strategy, symbol, exchange_code, trading_date, COUNT(*) "
            "FROM equity_intraday_research_timelines GROUP BY 1, 2, 3, 4"
        ).fetchall()
        assert all(row[4] == 1 for row in keys)
        identities = [
            row[0]
            for row in connection.execute(
                "SELECT timeline_id FROM equity_intraday_research_timelines"
            )
        ]
    rows = entry_rows(database)
    for timeline_id in identities:
        entries = [row for row in rows if row["timeline_id"] == timeline_id]
        assert entries, "an identity row without entries"
        assert [e["sequence"] for e in entries] == list(range(1, len(entries) + 1))
        assert entries[0]["previous_fingerprint"] is None
        for earlier, later in zip(entries, entries[1:], strict=False):
            assert later["previous_fingerprint"] == earlier["entry_fingerprint"]
            assert later["decision_instant_us"] > earlier["decision_instant_us"]
            assert later["watermark"] >= earlier["watermark"]
        assert sum(e["role"] == "CANDIDATE" for e in entries) <= 1
        assert len({e["decision_instant"] for e in entries}) == len(entries)
        first_long = next((e for e in entries if e["decision_outcome"] == "LONG_CANDIDATE"), None)
        for entry in entries:
            if entry["decision_outcome"] == "LONG_CANDIDATE":
                expected = "CANDIDATE" if entry is first_long else "REPEAT_QUALIFYING"
                assert entry["role"] == expected
