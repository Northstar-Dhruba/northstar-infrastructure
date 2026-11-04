"""EI-2C acceptance: synthetic intraday ingestion, persistence and replay over SQLite.

The chain is the real one: EI-1C-3 eligibility over the accepted production
NSE venue reference and explicitly synthetic M&M applicability for
2026-08-12; EI-2C ingestion into the EI-2B SQLite journal; EI-2B issuance and
replay with EI-2A validation under a versioned synthetic finality policy.

Every candle, source identity and finality statement is invented by
SyntheticEquityCandleEventSource. Nothing is NSE, provider or broker data,
nothing opens a network connection, and nothing is a decision, signal or
order.

The harness runs every scenario against fresh temporary databases, through
freshly created adapters, and returns a structured, JSON-serialisable report.
It runs once for the scenario tests and again, on new databases, to show the
report -- fingerprints included -- is identical. Determinism rests on fixed
inputs: explicit instants and identities, a deterministic journal clock, and
the same first-time submission order (journal sequences are part of the
manifest).
"""

from __future__ import annotations

import dataclasses
import json
import socket
import sqlite3
import threading
from collections.abc import Callable
from contextlib import closing
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from northstar_application.application_services import (
    EquityCandleCoverage,
    EquityCandleEvidenceClass,
    EquityCandleFinalityPolicy,
    EquityCandleFinding,
    EquityCandleIngestionOutcome,
    EquityCandleIntervalOutcome,
    EquityCandleJournalRecord,
    EquityCandleSnapshotReplayOutcome,
    EquityResearchEvidenceIssue,
    EvaluateEquityResearchWindowUseCase,
    IngestSyntheticEquityCandleEventsUseCase,
    IssueEquityCandleEvidenceSnapshotUseCase,
    IssueEquityResearchEvidenceUseCase,
    ReplayEquityCandleEvidenceSnapshotUseCase,
)
from northstar_application.ports import (
    EquityListingApplicabilityClassification,
    EquityReferenceProvenanceKind,
)
from northstar_core.domain.value_objects import ListingReference
from northstar_core.foundation.value_objects import (
    Currency,
    ExchangeCode,
    PointInTime,
    Symbol,
    Timeframe,
)

from northstar_infrastructure.market_data import (
    NoApprovedEquityListingApplicabilitySource,
    NSEEquityResearchReferenceProvenance,
    NSEEquityVenueDayReferenceSource,
    SyntheticEquityCandleEventSource,
    SyntheticListingApplicabilityRecord,
    SyntheticListingApplicabilitySource,
)
from northstar_infrastructure.persistence import (
    EquityCandleEvidenceStorageError,
    SQLiteEquityCandleEvidenceJournal,
    SQLiteEquityCandleEvidenceSnapshotStore,
)

_NSE = ExchangeCode("NSE")
_INR = Currency("INR")
_M_AND_M = ListingReference(Symbol("M&M"), _NSE)
_DAY = date(2026, 8, 12)
_SOURCE = "synthetic-ei2c-acceptance-source"
_VERSION = "ei2c-acceptance/1"
_POLICY = EquityCandleFinalityPolicy("synthetic-ei2c-acceptance/1", frozenset({_SOURCE}), True)
_ELIGIBLE = EquityListingApplicabilityClassification.ELIGIBLE_FOR_NORMAL_RESEARCH_WINDOW
_VENUE = NSEEquityVenueDayReferenceSource.production()
_APPLICABILITY = SyntheticListingApplicabilitySource(
    _VERSION, (SyntheticListingApplicabilityRecord(_M_AND_M, _DAY, _ELIGIBLE, "synthetic"),)
)
_PROVENANCE = NSEEquityResearchReferenceProvenance(_VENUE, _APPLICABILITY)
_EVENTS = SyntheticEquityCandleEventSource(_VERSION, _SOURCE, _M_AND_M)
_ING = EquityCandleIngestionOutcome
_OUT = EquityCandleIntervalOutcome
_COV = EquityCandleCoverage
_PARTIAL_AS_OF = PointInTime("2026-08-12T04:36:00Z")  # 10:06 IST: 10 intervals ended
_CLOSE_AS_OF = PointInTime("2026-08-12T10:00:00Z")  # 15:30 IST: all 69 ended


def _pit(value: datetime) -> PointInTime:
    return PointInTime(value.astimezone(UTC).isoformat().replace("+00:00", "Z"))


def _later(instant: PointInTime, **delta: float) -> PointInTime:
    return _pit(datetime.fromisoformat(instant.value.replace("Z", "+00:00")) + timedelta(**delta))


_ENDS = tuple(
    interval.closes_at
    for interval in EvaluateEquityResearchWindowUseCase(_VENUE, _APPLICABILITY)
    .execute(_M_AND_M, _DAY, _CLOSE_AS_OF)
    .intervals
)


def _block_network(patch: pytest.MonkeyPatch) -> None:
    def refuse(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("EI-2C acceptance must not open network connections.")

    patch.setattr(socket.socket, "connect", refuse)
    patch.setattr(socket, "create_connection", refuse)


@pytest.fixture(autouse=True)
def _refuse_network(monkeypatch: pytest.MonkeyPatch) -> None:
    _block_network(monkeypatch)


# ----------------------------------------------------------------------
# Workflow over fresh adapters
# ----------------------------------------------------------------------


class _Clock:
    """Deterministic journal clock: one second per append, from a fixed instant."""

    def __init__(self) -> None:
        self.ticks = 0

    def __call__(self) -> datetime:
        self.ticks += 1
        return datetime(2026, 8, 13, tzinfo=UTC) + timedelta(seconds=self.ticks)


class _Workflow:
    """Freshly created adapters and use cases over one database file -- a 'process'."""

    def __init__(
        self,
        database: Path,
        clock: _Clock,
        journal: SQLiteEquityCandleEvidenceJournal | None = None,
        applicability: object = _APPLICABILITY,
    ) -> None:
        self.database = database
        self.journal = journal or SQLiteEquityCandleEvidenceJournal(database, clock=clock)
        self.store = SQLiteEquityCandleEvidenceSnapshotStore(database)
        self.ingestion = IngestSyntheticEquityCandleEventsUseCase(
            self.journal, frozenset({_SOURCE})
        )
        self.evaluate = EvaluateEquityResearchWindowUseCase(_VENUE, applicability)  # type: ignore[arg-type]
        self.issuance = IssueEquityResearchEvidenceUseCase(
            self.evaluate,
            IssueEquityCandleEvidenceSnapshotUseCase(
                self.journal, self.store, _INR, _POLICY, reference_provenance=_PROVENANCE
            ),
        )
        self.replayer = ReplayEquityCandleEvidenceSnapshotUseCase(
            self.journal, self.store, _INR, _POLICY, reference_provenance=_PROVENANCE
        )

    def ingest(self, events: tuple[EquityCandleJournalRecord, ...] | list):  # type: ignore[no-untyped-def,type-arg]
        return self.ingestion.execute(events)

    def issue(
        self,
        as_of: PointInTime,
        issued_at: PointInTime | None = None,
        listing: ListingReference = _M_AND_M,
        day: date = _DAY,
    ) -> EquityResearchEvidenceIssue:
        return self.issuance.execute(listing, day, as_of, issued_at or _later(as_of, minutes=5))

    def replay(self, snapshot_id: str, as_of: PointInTime) -> EquityCandleSnapshotReplayOutcome:
        window = self.evaluate.execute(_M_AND_M, _DAY, as_of)
        return self.replayer.execute(snapshot_id, window).outcome


def _integrity(database: Path) -> dict[str, Any]:
    with closing(sqlite3.connect(database)) as connection:
        connection.execute("PRAGMA foreign_keys = ON")
        integrity = connection.execute("PRAGMA integrity_check").fetchall()
        foreign = connection.execute("PRAGMA foreign_key_check").fetchall()
        rows = {
            table: connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]  # noqa: S608
            for table in (
                "equity_candle_journal",
                "equity_candle_evidence_snapshots",
                "equity_candle_evidence_snapshot_entries",
            )
        }
    assert integrity == [("ok",)] and foreign == []
    return {"integrity_check": "ok", "foreign_key_check": "ok", "rows": rows}


def _summary(issue: EquityResearchEvidenceIssue) -> dict[str, Any]:
    snapshot = issue.snapshot
    summary: dict[str, Any] = {"outcome": issue.outcome.value}
    if snapshot is None:
        summary["rejection"] = (
            None if issue.window.rejection is None else issue.window.rejection.value
        )
        return summary
    summary.update(
        snapshot_id=snapshot.snapshot_id,
        manifest_fingerprint=snapshot.manifest_fingerprint,
        watermark=snapshot.watermark,
        coverage=snapshot.coverage.value,
        evidence_class=snapshot.evidence_class.value,
        venue_provenance=snapshot.venue_provenance.value,
        applicability_provenance=snapshot.applicability_provenance.value,
        acceptable=len(issue.acceptable),
        not_ended=len(issue.not_ended),
        missing=[end.value for end in issue.missing],
        unfinalized=[end.value for end in issue.unfinalized],
        conflicted=[end.value for end in issue.conflicted],
    )
    return summary


def _counts(report) -> dict[str, int]:  # type: ignore[no-untyped-def]
    return {o.value: report.count(o) for o in _ING if report.count(o)}


def _full_session() -> tuple[EquityCandleJournalRecord, ...]:
    return _EVENTS.session(_ENDS)


# ----------------------------------------------------------------------
# Scenarios
# ----------------------------------------------------------------------


def _scenario_a(root: Path) -> dict[str, Any]:
    """Partial session: 10 intervals ended, all 10 final; the 59 future ones not required."""
    workflow = _Workflow(root / "a.sqlite3", _Clock())
    ingested = workflow.ingest(_EVENTS.session(_ENDS[:10]))
    issue = workflow.issue(_PARTIAL_AS_OF)
    return {
        "ingestion": _counts(ingested),
        "snapshot": _summary(issue),
        "replay": workflow.replay(issue.snapshot.snapshot_id, _PARTIAL_AS_OF).value,  # type: ignore[union-attr]
    }


def _scenario_b(root: Path) -> dict[str, Any]:
    """Full session; ended intervals without finality are never treated as final."""
    workflow = _Workflow(root / "b.sqlite3", _Clock())
    observations = tuple(_EVENTS.observation(end) for end in _ENDS)
    workflow.ingest(observations)
    timing_only = workflow.issue(_CLOSE_AS_OF)
    workflow.ingest(tuple(_EVENTS.finality(end) for end in _ENDS))
    complete = workflow.issue(_CLOSE_AS_OF, _later(_CLOSE_AS_OF, minutes=10))
    snapshot = complete.snapshot
    assert snapshot is not None
    return {
        "timing_only": _summary(timing_only),
        "complete": _summary(complete),
        "first_end": snapshot.intervals[0].closes_at.value,
        "last_end": snapshot.intervals[-1].closes_at.value,
        "replay": workflow.replay(snapshot.snapshot_id, _CLOSE_AS_OF).value,
        "integrity": _integrity(workflow.database),
    }


def _scenario_c(root: Path) -> dict[str, Any]:
    """Missing finality for one candle; late finality never alters the earlier snapshot."""
    workflow = _Workflow(root / "c.sqlite3", _Clock())
    gap = _ENDS[6]
    workflow.ingest(_EVENTS.session(_ENDS, without_finality=frozenset({gap})))
    earlier = workflow.issue(_CLOSE_AS_OF)
    assert earlier.snapshot is not None
    finding = next(
        o.finding
        for o in earlier.snapshot.observations
        if o.record.observation.bar.point_in_time == gap
    )
    late_receipt = PointInTime("2026-08-12T10:30:00Z")
    workflow.ingest((_EVENTS.finality(gap, received_at=late_receipt),))
    same_instant = workflow.issue(_CLOSE_AS_OF, _later(late_receipt, minutes=1))
    later_instant = workflow.issue(PointInTime("2026-08-12T11:00:00Z"))
    reopened = _Workflow(workflow.database, _Clock())
    return {
        "earlier": _summary(earlier),
        "missing_finality_finding": finding.value,
        "same_instant_after_late_finality": _summary(same_instant),
        "later_instant": _summary(later_instant),
        "earlier_unchanged": reopened.store.get_snapshot(earlier.snapshot.snapshot_id)
        == earlier.snapshot,
        "earlier_replay": reopened.replay(earlier.snapshot.snapshot_id, _CLOSE_AS_OF).value,
    }


_CORRECTED_PRICES = ("1530.25", "1534", "1529", "1531.5")


def _scenario_d(root: Path) -> dict[str, Any]:
    """A later corrected revision: both kept, nothing chosen, the original snapshot intact."""
    workflow = _Workflow(root / "d.sqlite3", _Clock())
    workflow.ingest(_full_session())
    original = workflow.issue(_CLOSE_AS_OF)
    target = _ENDS[4]
    correction = workflow.ingest(
        (
            _EVENTS.observation(
                target,
                revision="r2",
                corrects="r1",
                prices=_CORRECTED_PRICES,
                received_at=PointInTime("2026-08-12T10:05:00Z"),
            ),
            _EVENTS.finality(
                target, revision="r2", received_at=PointInTime("2026-08-12T10:06:00Z")
            ),
        )
    )
    later = workflow.issue(PointInTime("2026-08-12T10:30:00Z"))
    assert original.snapshot is not None and later.snapshot is not None
    revisions = sorted(
        (
            o.record.observation.source_revision,
            o.record.observation.corrects_revision,
            o.record.observation.bar.close.amount.to_eng_string(),
        )
        for o in later.snapshot.observations
        if o.record.observation.bar.point_in_time == target
    )
    return {
        "original": _summary(original),
        "correction_ingestion": _counts(correction),
        "later": _summary(later),
        "revisions_at_target": [list(r) for r in revisions],
        "original_unchanged": workflow.store.get_snapshot(original.snapshot.snapshot_id)
        == original.snapshot,
        "original_replay": workflow.replay(original.snapshot.snapshot_id, _CLOSE_AS_OF).value,
        "snapshot_ids": [original.snapshot.snapshot_id, later.snapshot.snapshot_id],
    }


def _scenario_e(root: Path) -> dict[str, Any]:
    """Backdated late ingestion never enters a snapshot issued before it was journaled."""
    workflow = _Workflow(root / "e.sqlite3", _Clock())
    gap = _ENDS[19]
    workflow.ingest(_EVENTS.session(_ENDS, without_observation=frozenset({gap})))
    earlier = workflow.issue(_CLOSE_AS_OF)
    backdated = workflow.ingest((_EVENTS.observation(gap, published_at=gap), _EVENTS.finality(gap)))
    later = workflow.issue(_CLOSE_AS_OF, PointInTime("2026-08-12T12:00:00Z"))
    assert earlier.snapshot is not None
    backdated_entry = backdated.results[0]
    return {
        "earlier": _summary(earlier),
        "backdated_received_at": _EVENTS.observation(gap).received_at.value,
        "backdated_sequence": backdated_entry.sequence,
        "later": _summary(later),
        "earlier_unchanged": workflow.store.get_snapshot(earlier.snapshot.snapshot_id)
        == earlier.snapshot,
        "earlier_replay": workflow.replay(earlier.snapshot.snapshot_id, _CLOSE_AS_OF).value,
    }


def _scenario_f(root: Path) -> dict[str, Any]:
    """Idempotent replay of a whole batch, and explicit conflicts for changed content."""
    workflow = _Workflow(root / "f.sqlite3", _Clock())
    first = workflow.ingest(_full_session())
    issued = workflow.issue(_CLOSE_AS_OF)
    again = _Workflow(workflow.database, _Clock()).ingest(_full_session())
    reissued = workflow.issue(_CLOSE_AS_OF)
    original = _EVENTS.observation(_ENDS[0])
    moved = _EVENTS.observation(_ENDS[2])
    conflicts = workflow.ingest(
        (
            _EVENTS.observation(_ENDS[0], prices=_CORRECTED_PRICES),
            dataclasses.replace(
                moved,
                ingestion_id="ei2c/moved-identity",
                observation=dataclasses.replace(
                    moved.observation, source_event_id=original.observation.source_event_id
                ),
            ),
            dataclasses.replace(original, ingestion_id="ei2c/not-synthetic", is_synthetic=False),
        )
    )
    after = workflow.issue(_CLOSE_AS_OF, _later(_CLOSE_AS_OF, hours=1))
    window = workflow.evaluate.execute(_M_AND_M, _DAY, _CLOSE_AS_OF)
    stored = workflow.journal.read_window(
        _M_AND_M,
        Timeframe("5m"),
        window.window_opens_at,  # type: ignore[arg-type]
        window.window_closes_at,  # type: ignore[arg-type]
        workflow.journal.watermark(),
    )
    return {
        "first": _counts(first),
        "resubmitted": _counts(again),
        "resubmitted_sequences_match": [r.sequence for r in again.results]
        == [r.sequence for r in first.results],
        "reissued_same_snapshot": reissued.snapshot == issued.snapshot,
        "conflicts": [r.outcome.value for r in conflicts.results],
        "first_record_unchanged": stored.observations[0].record == original,
        "watermark": workflow.journal.watermark(),
        "fingerprint_unchanged": after.snapshot.manifest_fingerprint  # type: ignore[union-attr]
        == issued.snapshot.manifest_fingerprint,  # type: ignore[union-attr]
        "integrity": _integrity(workflow.database),
    }


class _Interrupting(SQLiteEquityCandleEvidenceJournal):
    """Fails one append with a storage error -- before or after it commits."""

    def __init__(self, database: Path, clock: _Clock, fail_on: int, after_commit: bool) -> None:
        super().__init__(database, clock=clock)
        self.fail_on, self.after_commit, self.calls = fail_on, after_commit, 0

    def append(self, record):  # type: ignore[no-untyped-def]
        self.calls += 1
        if self.calls != self.fail_on:
            return super().append(record)
        if self.after_commit:
            super().append(record)
            raise EquityCandleEvidenceStorageError("simulated lost acknowledgement after commit")
        raise EquityCandleEvidenceStorageError("simulated storage failure before commit")


def _scenario_g(root: Path) -> dict[str, Any]:
    """Interrupted ingestion, restart and resubmission; batch boundaries do not matter."""
    events = _full_session()
    reference = _Workflow(root / "g-reference.sqlite3", _Clock())
    reference.ingest(events)
    expected = reference.issue(_CLOSE_AS_OF).snapshot
    assert expected is not None
    outcome: dict[str, Any] = {"reference_fingerprint": expected.manifest_fingerprint}
    for name, after_commit in (("before_commit", False), ("after_commit", True)):
        database, clock = root / f"g-{name}.sqlite3", _Clock()
        interrupted = _Workflow(database, clock, _Interrupting(database, clock, 50, after_commit))
        first = interrupted.ingest(events)
        durable = SQLiteEquityCandleEvidenceJournal(database).watermark()
        restarted = _Workflow(database, clock)
        resumed = restarted.ingest(events)
        issued = restarted.issue(_CLOSE_AS_OF).snapshot
        assert issued is not None
        outcome[name] = {
            "first": _counts(first),
            "resume_from": first.resume_from,
            "durable_after_interruption": durable,
            "resumed": _counts(resumed),
            "snapshot_matches_uninterrupted": issued == expected,
        }
    split, clock = root / "g-split.sqlite3", _Clock()
    for start in range(0, len(events), 25):
        _Workflow(split, clock).ingest(events[start : start + 25])
    resumed_tail = _Workflow(split, clock).ingest(events[100:])
    split_snapshot = _Workflow(split, clock).issue(_CLOSE_AS_OF).snapshot
    outcome["split_batches_match"] = split_snapshot == expected
    outcome["split_tail_resubmission"] = _counts(resumed_tail)
    return outcome


def _scenario_h(root: Path, scenario_d: dict[str, Any]) -> dict[str, Any]:
    """Restart over scenario D's database with fresh connections and adapters."""
    database = root / "d.sqlite3"
    fresh = _Workflow(database, _Clock())
    as_ofs = (_CLOSE_AS_OF, PointInTime("2026-08-12T10:30:00Z"))
    snapshots = [fresh.store.get_snapshot(sid) for sid in scenario_d["snapshot_ids"]]
    window = fresh.evaluate.execute(_M_AND_M, _DAY, _CLOSE_AS_OF)
    history = fresh.journal.read_window(
        _M_AND_M,
        Timeframe("5m"),
        window.window_opens_at,  # type: ignore[arg-type]
        window.window_closes_at,  # type: ignore[arg-type]
        fresh.journal.watermark(),
    )
    target = _ENDS[4]
    return {
        "fingerprints": [s.manifest_fingerprint for s in snapshots],  # type: ignore[union-attr]
        "fingerprints_match": [s.manifest_fingerprint for s in snapshots]  # type: ignore[union-attr]
        == [
            scenario_d["original"]["manifest_fingerprint"],
            scenario_d["later"]["manifest_fingerprint"],
        ],
        "replays": [
            fresh.replay(s.snapshot_id, as_of).value  # type: ignore[union-attr]
            for s, as_of in zip(snapshots, as_ofs, strict=True)
        ],
        "revisions_at_target": sorted(
            e.record.observation.source_revision  # type: ignore[union-attr]
            for e in history.observations
            if e.record.observation.bar.point_in_time == target  # type: ignore[union-attr]
        ),
        "integrity": _integrity(database),
    }


_INELIGIBLE = (
    ("unknown stock/date", _M_AND_M, date(2026, 8, 13)),
    ("non-NSE listing", ListingReference(Symbol("M&M"), ExchangeCode("BSE")), _DAY),
    ("weekday holiday", _M_AND_M, date(2026, 9, 14)),
    ("weekend holiday", _M_AND_M, date(2026, 8, 15)),
    ("unsupported weekend", _M_AND_M, date(2026, 8, 16)),
    ("special session", _M_AND_M, date(2026, 11, 8)),
    *((f"coverage gap {day}", _M_AND_M, day) for day in (date(2026, 8, d) for d in range(3, 12))),
    ("after reference coverage", _M_AND_M, date(2026, 10, 21)),
)


def _scenario_i(root: Path) -> dict[str, Any]:
    """Ineligible venue or stock/date: nothing issued, even with synthetic eligibility."""
    eligible_everywhere = SyntheticListingApplicabilitySource(
        "ei2c-ineligible/1",
        tuple(
            SyntheticListingApplicabilityRecord(listing, day, _ELIGIBLE, "synthetic")
            for name, listing, day in _INELIGIBLE
            if name != "unknown stock/date"
        ),
    )
    database = root / "i.sqlite3"
    workflow = _Workflow(database, _Clock(), applicability=eligible_everywhere)
    assert workflow.journal.watermark() == 0  # creates the schema; nothing is journaled
    rejections = {}
    for name, listing, day in _INELIGIBLE:
        as_of = PointInTime(f"{day.isoformat()}T12:00:00Z")
        rejections[name] = _summary(workflow.issue(as_of, listing=listing, day=day))
    production = _Workflow(
        database, _Clock(), applicability=NoApprovedEquityListingApplicabilitySource()
    )
    rejections["production applicability"] = _summary(production.issue(_CLOSE_AS_OF))
    return {"rejections": rejections, "snapshots": _integrity(database)["rows"]}


class _PausedJournal(SQLiteEquityCandleEvidenceJournal):
    """Pauses the first issuance attempt after its read; explicit events, no sleeps."""

    def __init__(self, database: Path, clock: _Clock) -> None:
        super().__init__(database, clock=clock)
        self.read_done, self.resume = threading.Event(), threading.Event()
        self.reads: list[int] = []

    def read_window(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        window = super().read_window(*args, **kwargs)
        self.reads.append(window.watermark)
        if len(self.reads) == 1:
            self.read_done.set()
            assert self.resume.wait(timeout=60), "the harness never resumed issuance"
        return window


def _scenario_j(root: Path) -> dict[str, Any]:
    """A correction committed between the watermark read and issuance is never omitted."""
    database, clock = root / "j.sqlite3", _Clock()
    _Workflow(database, clock).ingest(_full_session())
    paused = _PausedJournal(database, clock)
    issuing = _Workflow(database, clock, paused)
    result: dict[str, Any] = {}

    def run() -> None:
        try:
            result["issue"] = issuing.issue(_CLOSE_AS_OF)
        except BaseException as exc:  # noqa: BLE001 - surfaced in the report below
            result["error"] = repr(exc)

    thread = threading.Thread(target=run)
    thread.start()
    assert paused.read_done.wait(timeout=60)
    target = _ENDS[4]
    concurrent = _Workflow(database, clock).ingest(
        (
            _EVENTS.observation(
                target,
                revision="r2",
                corrects="r1",
                prices=_CORRECTED_PRICES,
                received_at=PointInTime("2026-08-12T09:59:00Z"),
            ),
        )
    )
    paused.resume.set()
    thread.join(timeout=60)
    assert not thread.is_alive() and "error" not in result, result.get("error")
    return {
        "concurrent_append": _counts(concurrent),
        "attempt_watermarks": paused.reads,
        "issued": _summary(result["issue"]),
        "snapshots": _integrity(database)["rows"]["equity_candle_evidence_snapshots"],
    }


def _acceptance(root: Path) -> dict[str, Any]:
    root.mkdir(parents=True, exist_ok=True)
    report: dict[str, Any] = {"listing": "M&M@NSE", "trading_date": _DAY.isoformat()}
    report["interval_ends"] = {
        "count": len(_ENDS),
        "first": _ENDS[0].value,
        "last": _ENDS[-1].value,
    }
    scenarios: list[tuple[str, Callable[[Path], dict[str, Any]]]] = [
        ("A_partial_session", _scenario_a),
        ("B_full_session", _scenario_b),
        ("C_missing_finality", _scenario_c),
        ("D_later_correction", _scenario_d),
        ("E_backdated_ingestion", _scenario_e),
        ("F_idempotent_replay", _scenario_f),
        ("G_interrupted_ingestion", _scenario_g),
    ]
    for name, scenario in scenarios:
        report[name] = scenario(root)
    report["H_restart_and_replay"] = _scenario_h(root, report["D_later_correction"])
    report["I_ineligible"] = _scenario_i(root)
    report["J_concurrent_issuance"] = _scenario_j(root)
    return report


@pytest.fixture(scope="module")
def report(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    root = tmp_path_factory.mktemp("ei2c-acceptance")
    with pytest.MonkeyPatch.context() as patch:
        _block_network(patch)
        result = _acceptance(root / "run-1")
    (root / "ei2c-acceptance-report.json").write_text(
        json.dumps(result, indent=2, sort_keys=True), encoding="utf-8"
    )
    return result


# ----------------------------------------------------------------------
# Assertions over the report
# ----------------------------------------------------------------------

_SYN = EquityReferenceProvenanceKind.SYNTHETIC.value
_PROD = EquityReferenceProvenanceKind.ACCEPTED_PRODUCTION.value


def test_the_window_has_69_five_minute_interval_ends(report: dict[str, Any]) -> None:
    assert report["interval_ends"] == {
        "count": 69,
        "first": "2026-08-12T03:50:00Z",
        "last": "2026-08-12T09:30:00Z",
    }


def test_scenario_a_partial_session(report: dict[str, Any]) -> None:
    a = report["A_partial_session"]
    assert a["ingestion"] == {"APPENDED_OBSERVATION": 10, "APPENDED_FINALITY": 10}
    snapshot = a["snapshot"]
    assert snapshot["outcome"] == "ISSUED"
    assert (snapshot["acceptable"], snapshot["not_ended"]) == (10, 59)
    assert snapshot["coverage"] == _COV.COMPLETE_AS_OF.value
    assert snapshot["evidence_class"] == EquityCandleEvidenceClass.SYNTHETIC.value
    assert (snapshot["venue_provenance"], snapshot["applicability_provenance"]) == (_PROD, _SYN)
    assert a["replay"] == "REPRODUCED"


def test_scenario_b_full_session_and_no_finality_from_timing(report: dict[str, Any]) -> None:
    b = report["B_full_session"]
    timing = b["timing_only"]
    assert timing["acceptable"] == 0 and len(timing["unfinalized"]) == 69
    assert timing["coverage"] == _COV.INCOMPLETE_AS_OF.value
    complete = b["complete"]
    assert complete["acceptable"] == 69 and complete["coverage"] == _COV.WINDOW_COMPLETE.value
    assert complete["manifest_fingerprint"].startswith("ecm1-sha256:")
    assert (b["first_end"], b["last_end"]) == ("2026-08-12T03:50:00Z", "2026-08-12T09:30:00Z")
    assert b["replay"] == "REPRODUCED"
    assert b["integrity"]["rows"]["equity_candle_journal"] == 138


def test_scenario_c_missing_then_late_finality(report: dict[str, Any]) -> None:
    c = report["C_missing_finality"]
    gap = "2026-08-12T04:20:00Z"
    assert c["earlier"]["unfinalized"] == [gap]
    assert c["earlier"]["coverage"] == _COV.INCOMPLETE_AS_OF.value
    assert c["missing_finality_finding"] == EquityCandleFinding.FINALITY_MISSING.value
    # Finality received at 10:30 is not retroactively visible at 10:00.
    assert c["same_instant_after_late_finality"]["unfinalized"] == [gap]
    assert c["later_instant"]["coverage"] == _COV.WINDOW_COMPLETE.value
    assert c["earlier_unchanged"] is True and c["earlier_replay"] == "REPRODUCED"


def test_scenario_d_later_correction(report: dict[str, Any]) -> None:
    d = report["D_later_correction"]
    assert d["original"]["coverage"] == _COV.WINDOW_COMPLETE.value
    assert d["correction_ingestion"] == {"APPENDED_OBSERVATION": 1, "APPENDED_FINALITY": 1}
    assert d["later"]["conflicted"] == ["2026-08-12T04:10:00Z"]
    assert d["later"]["coverage"] == _COV.INCOMPLETE_AS_OF.value
    assert d["revisions_at_target"] == [["r1", None, "1513.25"], ["r2", "r1", "1531.5"]]
    assert d["original_unchanged"] is True and d["original_replay"] == "REPRODUCED"


def test_scenario_e_backdated_ingestion(report: dict[str, Any]) -> None:
    e = report["E_backdated_ingestion"]
    assert e["earlier"]["missing"] == ["2026-08-12T05:25:00Z"]
    assert e["backdated_received_at"] == "2026-08-12T05:25:05Z"
    assert e["backdated_sequence"] > e["earlier"]["watermark"]
    assert e["later"]["coverage"] == _COV.WINDOW_COMPLETE.value
    assert e["later"]["watermark"] > e["earlier"]["watermark"]
    assert e["earlier_unchanged"] is True and e["earlier_replay"] == "REPRODUCED"


def test_scenario_f_idempotent_replay_and_conflicts(report: dict[str, Any]) -> None:
    f = report["F_idempotent_replay"]
    assert f["first"] == {"APPENDED_OBSERVATION": 69, "APPENDED_FINALITY": 69}
    assert f["resubmitted"] == {"IDEMPOTENT_RETRY": 138}
    assert f["resubmitted_sequences_match"] is True
    assert f["reissued_same_snapshot"] is True
    assert f["conflicts"] == [
        _ING.REJECTED_INGESTION_CONFLICT.value,
        _ING.REJECTED_SOURCE_IDENTITY_CONFLICT.value,
        _ING.REJECTED_INVALID.value,
    ]
    assert f["first_record_unchanged"] is True and f["watermark"] == 138
    assert f["fingerprint_unchanged"] is True


def test_scenario_g_interrupted_ingestion_and_restart(report: dict[str, Any]) -> None:
    g = report["G_interrupted_ingestion"]
    before, after = g["before_commit"], g["after_commit"]
    for case, durable in ((before, 49), (after, 50)):
        assert case["resume_from"] == 49
        assert case["first"]["FAILED_PERSISTENCE"] == 1
        assert case["first"]["NOT_ATTEMPTED"] == 88
        assert case["durable_after_interruption"] == durable
        assert case["resumed"]["IDEMPOTENT_RETRY"] == durable
        assert sum(case["resumed"].values()) == 138
        assert case["snapshot_matches_uninterrupted"] is True
    assert g["split_batches_match"] is True
    assert g["split_tail_resubmission"] == {"IDEMPOTENT_RETRY": 38}


def test_scenario_h_restart_and_replay(report: dict[str, Any]) -> None:
    h = report["H_restart_and_replay"]
    assert h["fingerprints_match"] is True
    assert h["replays"] == ["REPRODUCED", "REPRODUCED"]
    assert h["revisions_at_target"] == ["r1", "r2"]
    assert h["integrity"]["integrity_check"] == "ok"
    assert h["integrity"]["foreign_key_check"] == "ok"


def test_scenario_i_ineligible_windows_issue_nothing(report: dict[str, Any]) -> None:
    rejections = report["I_ineligible"]["rejections"]
    expected = {
        "unknown stock/date": "LISTING_APPLICABILITY_UNKNOWN",
        "non-NSE listing": "POLICY_NOT_APPLICABLE",
        "weekday holiday": "VENUE_CLOSED",
        "weekend holiday": "VENUE_CLOSED",
        "unsupported weekend": "VENUE_DAY_NOT_STATED",
        "special session": "SPECIAL_SESSION_UNSUPPORTED",
        "after reference coverage": "OUTSIDE_REFERENCE_COVERAGE",
        "production applicability": "LISTING_APPLICABILITY_UNKNOWN",
    }
    for day in range(3, 12):
        # The weekend inside the 2026-08-03..11 gap is not stated; weekdays have no
        # common continuous boundary. Either way the date fails closed.
        weekend = date(2026, 8, day).weekday() >= 5
        expected[f"coverage gap 2026-08-{day:02d}"] = (
            "VENUE_DAY_NOT_STATED" if weekend else "COMMON_BOUNDARY_UNSUPPORTED"
        )
    assert {name: r["rejection"] for name, r in rejections.items()} == expected
    assert all(r["outcome"] == "WINDOW_INELIGIBLE" for r in rejections.values())
    assert report["I_ineligible"]["snapshots"]["equity_candle_evidence_snapshots"] == 0


def test_scenario_j_concurrent_issuance(report: dict[str, Any]) -> None:
    j = report["J_concurrent_issuance"]
    assert j["concurrent_append"] == {"APPENDED_OBSERVATION": 1}
    assert j["attempt_watermarks"] == [138, 139]
    assert j["issued"]["watermark"] == 139
    assert j["issued"]["conflicted"] == ["2026-08-12T04:10:00Z"]
    assert j["snapshots"] == 1


def test_the_acceptance_run_is_deterministic_on_fresh_databases(
    report: dict[str, Any], tmp_path: Path
) -> None:
    second = _acceptance(tmp_path / "run-2")
    assert second == report
    assert json.loads(json.dumps(second, sort_keys=True)) == second
