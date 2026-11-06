"""EI-3C-B: the SQLite authoritative research timeline store.

Every test runs on a temporary SQLite file through the real EI-2B journal and
snapshot store, the real EI-2C issuance workflow and the real EI-3C-A
advancement use case (tests/equity_intraday_research_timeline_fixtures.py).
Concurrency is coordinated with threading.Event, never sleeps. Where a test
must alter committed rows, it drops a trigger on its own disposable database,
says so, and the adapter's next connection recreates the trigger. Foreign
keys stay on in every connection unless a test says otherwise.
"""

from __future__ import annotations

import ast
import dataclasses
import hashlib
import inspect
import json
import re
import socket
import sqlite3
import threading
from collections.abc import Callable
from contextlib import closing
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from northstar_application.application_services import (
    EquityCandleEvidenceSnapshot,
    EquityIntradayResearchScheduleOutcome,
    EquityIntradayResearchScheduleRefusal,
    EquityIntradayResearchTimeline,
    EquityIntradayResearchTimelineAdvance,
    EquityIntradayResearchTimelineAdvanceStatus,
    EquityIntradayResearchTimelineEntry,
    EquityIntradayResearchTimelineIdentity,
    EquityIntradayResearchTimelineRole,
    EquityOpeningRangeBreakoutConfiguration,
    EquityOpeningRangeBreakoutEvaluator,
    ScheduleEquityIntradayResearchUseCase,
    StoredEquityIntradayResearchTimeline,
    StoredEquityIntradayResearchTimelineEntry,
    canonical_equity_candle_json,
    rehydrate_equity_intraday_research_timeline,
    stored_equity_intraday_research_timeline,
)
from northstar_application.ports import (
    EquityIntradayResearchHistoricalEvaluationUnavailableError,
    EquityIntradayResearchTimelineCommitUnconfirmedError,
    EquityIntradayResearchTimelineConfigurationConflictError,
    EquityIntradayResearchTimelineStaleError,
    EquityIntradayResearchTimelineUnavailableError,
    InvalidStoredEquityIntradayResearchTimelineError,
)
from northstar_core.foundation.value_objects import PointInTime
from northstar_core.strategy import StrategyIdentity

from northstar_infrastructure.persistence import (
    EquityIntradayResearchTimelineStorageError,
    SQLiteEquityIntradayResearchTimelineStore,
    decision_instant_microseconds,
    initialize_equity_candle_evidence_schema,
    initialize_equity_intraday_research_timeline_schema,
)
from northstar_infrastructure.persistence import (
    sqlite_equity_intraday_research_timeline as adapter_module,
)
from northstar_infrastructure.persistence import (
    sqlite_equity_intraday_research_timeline_schema as schema_module,
)
from tests.equity_intraday_research_timeline_fixtures import (
    CONFIG,
    DAY,
    M_AND_M,
    OTHER_DAY,
    RELIANCE,
    Clock,
    Harness,
    check_invariants,
    count,
    entry_rows,
    ist,
    raw,
)

_STATUS = EquityIntradayResearchTimelineAdvanceStatus
_ROLE = EquityIntradayResearchTimelineRole
_DONE = EquityIntradayResearchScheduleOutcome
_WHY = EquityIntradayResearchScheduleRefusal
_IDENTITY = EquityIntradayResearchTimelineIdentity(M_AND_M, DAY, CONFIG)
_TIMELINES = "equity_intraday_research_timelines"
_ENTRIES = "equity_intraday_research_timeline_entries"


@pytest.fixture(autouse=True)
def _refuse_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("EI-3C-B tests must not open network connections.")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)


@pytest.fixture
def database(tmp_path: Path) -> Path:
    return tmp_path / "research.sqlite3"


def _morning(database: Path) -> Harness:
    harness = Harness(database)
    harness.candles()
    return harness


def _intended(
    harness: Harness,
    snapshots: list[EquityCandleEvidenceSnapshot],
    configuration: EquityOpeningRangeBreakoutConfiguration = CONFIG,
) -> tuple[StoredEquityIntradayResearchTimeline, str | None]:
    """The record EI-3C-A would append for ``snapshots``, and the head it expects."""
    identity = EquityIntradayResearchTimelineIdentity(M_AND_M, DAY, configuration)
    stored = harness.timelines.load(configuration.strategy, M_AND_M, DAY)
    current = (
        EquityIntradayResearchTimeline(identity)
        if stored is None
        else rehydrate_equity_intraday_research_timeline(identity, stored, harness.snapshots)
    )
    schedule = ScheduleEquityIntradayResearchUseCase(harness.snapshots, configuration).advance(
        current, [s.snapshot_id for s in snapshots]
    )
    return stored_equity_intraday_research_timeline(schedule.timeline), current.head_fingerprint


def _roles(
    result: EquityIntradayResearchTimelineAdvance,
) -> list[EquityIntradayResearchTimelineRole]:
    assert result.timeline is not None
    return [entry.role for entry in result.timeline.entries]


def _results(result: EquityIntradayResearchTimelineAdvance) -> list[tuple[object, object]]:
    return [(item.outcome, item.refusal) for item in result.items]


def _stored_roles(database: Path) -> list[str]:
    return [row["role"] for row in entry_rows(database)]


# ----------------------------------------------------------------------
# A. Schema and instants
# ----------------------------------------------------------------------


def test_the_schema_is_idempotent_and_shares_the_evidence_database(database: Path) -> None:
    harness = _morning(database)
    evidence = {
        table: count(database, table)
        for table in ("equity_candle_journal", "equity_candle_evidence_snapshots")
    }
    for _ in range(3):
        assert harness.timelines.load(CONFIG.strategy, M_AND_M, DAY) is None
        with closing(raw(database)) as connection:
            initialize_equity_candle_evidence_schema(connection)
            initialize_equity_intraday_research_timeline_schema(connection)
    with closing(raw(database)) as connection:
        triggers = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'trigger' AND name LIKE ?",
                ("equity_intraday_research_%",),
            )
        }
        foreign = {
            (row[2], row[3], row[4])
            for row in connection.execute(f"PRAGMA foreign_key_list({_ENTRIES})")
        }
    assert len(triggers) == 15
    assert foreign == {
        ("equity_candle_evidence_snapshots", "snapshot_id", "snapshot_id"),
        (_TIMELINES, "timeline_id", "timeline_id"),
    }
    assert {t: count(database, t) for t in evidence} == evidence
    assert count(database, _TIMELINES) == 0


@pytest.mark.parametrize(
    ("text", "microseconds"),
    [
        ("1970-01-01T00:00:00Z", 0),
        ("1969-12-31T23:59:59.999999Z", -1),
        ("2026-08-12T04:06:00Z", 1_786_507_560_000_000),
        ("2026-08-12T04:06:00.5Z", 1_786_507_560_500_000),
        ("2026-08-12T04:06:00.000001Z", 1_786_507_560_000_001),
        ("2026-08-12T04:06:00.123456Z", 1_786_507_560_123_456),
    ],
)
def test_decision_instants_are_exact_integer_microseconds(
    database: Path, text: str, microseconds: int
) -> None:
    instant = PointInTime(text)
    assert instant.value == text
    assert decision_instant_microseconds(instant) == microseconds
    expression = schema_module._INSTANT_US.replace("NEW.decision_instant", ":instant")
    canonical = schema_module._CANONICAL_INSTANT.replace("NEW.decision_instant", ":instant")
    with closing(raw(database)) as connection:
        assert connection.execute(f"SELECT {expression}", {"instant": text}).fetchone() == (
            microseconds,
        )
        assert connection.execute(f"SELECT {canonical}", {"instant": text}).fetchone() == (1,)


def test_canonical_text_order_is_not_chronological_but_the_integer_key_is() -> None:
    earlier, later = PointInTime("2026-08-12T04:06:00Z"), PointInTime("2026-08-12T04:06:00.5Z")
    assert later.value < earlier.value  # "." sorts before "Z"
    assert decision_instant_microseconds(later) > decision_instant_microseconds(earlier)


# ----------------------------------------------------------------------
# B. The port, through the EI-3C-A use case
# ----------------------------------------------------------------------


def test_the_first_commit_establishes_the_identity_and_load_returns_the_exact_record(
    database: Path,
) -> None:
    harness = _morning(database)
    (s36,) = harness.at(36)
    result = harness.advance([s36])
    assert result.status is _STATUS.COMMITTED and _roles(result) == [_ROLE.CANDIDATE]
    assert (count(database, _TIMELINES), count(database, _ENTRIES)) == (1, 1)
    assert result.timeline is not None
    loaded = SQLiteEquityIntradayResearchTimelineStore(database).load(CONFIG.strategy, M_AND_M, DAY)
    assert loaded == stored_equity_intraday_research_timeline(result.timeline)
    with closing(raw(database)) as connection:
        row = connection.execute(
            f"SELECT configuration_version, configuration_document, identity_document "
            f"FROM {_TIMELINES}"
        ).fetchone()
    assert row == (
        CONFIG.version,
        canonical_equity_candle_json(CONFIG.document()),
        canonical_equity_candle_json(_IDENTITY.document()),
    )
    stored = entry_rows(database)[0]
    decision = result.timeline.entries[0].decision
    assert stored["decision_document"] == canonical_equity_candle_json(decision.document())
    assert (
        stored["decision_fingerprint"]
        == decision.fingerprint
        == ("eir1-sha256:" + hashlib.sha256(stored["decision_document"].encode()).hexdigest())
    )
    assert stored["entry_fingerprint"] == result.timeline.entries[0].fingerprint
    assert stored["snapshot_id"] == s36.snapshot_id and stored["previous_fingerprint"] is None


def test_an_advance_that_appends_nothing_creates_no_identity(database: Path) -> None:
    harness = _morning(database)
    harness.candles(listing=RELIANCE)
    other = harness.issue(ist(9, 36), listing=RELIANCE)
    result = harness.writer().execute(M_AND_M, DAY, ["ecs1-sha256:" + "0" * 64, other.snapshot_id])
    assert result.status is _STATUS.NOTHING_TO_COMMIT and result.timeline is None
    assert _results(result) == [
        (_DONE.REFUSED, _WHY.SNAPSHOT_UNAVAILABLE),
        (_DONE.REFUSED, _WHY.OTHER_LISTING_OR_DATE),
    ]
    assert (count(database, _TIMELINES), count(database, _ENTRIES)) == (0, 0)


def test_stale_heads_and_unrepeated_history_are_refused_without_writing(database: Path) -> None:
    harness = _morning(database)
    s31, s36 = harness.at(31, 36)
    harness.advance([s31])
    intended, head = _intended(harness, [s36])
    assert head is not None
    store = harness.timelines
    for expected in (None, "eirt1-sha256:" + "0" * 64):
        with pytest.raises(EquityIntradayResearchTimelineStaleError, match="head"):
            store.append(intended, expected)
    altered = dataclasses.replace(
        intended,
        entries=(dataclasses.replace(intended.entries[0], watermark=0), intended.entries[1]),
    )
    with pytest.raises(EquityIntradayResearchTimelineStaleError, match="repeat"):
        store.append(altered, head)
    assert count(database, _ENTRIES) == 1
    store.append(intended, head)
    assert _stored_roles(database) == ["NO_SIGNAL", "CANDIDATE"]
    with pytest.raises(EquityIntradayResearchTimelineStorageError, match="appends no entry"):
        store.append(intended, intended.head_fingerprint)
    assert count(database, _ENTRIES) == 2


@pytest.mark.parametrize(
    "changes",
    [
        {"version": "synthetic-ei3c-sqlite/2", "volume_multiplier": Decimal("2")},
        {"volume_multiplier": Decimal("1.0")},
    ],
)
def test_another_configuration_under_the_same_key_is_refused(database: Path, changes) -> None:  # type: ignore[no-untyped-def]
    harness = _morning(database)
    s36, s41 = harness.at(36, 41)
    harness.advance([s36])
    before = count(database, _ENTRIES)
    changed = dataclasses.replace(CONFIG, **changes)
    result = harness.advance([s41], configuration=changed)
    assert result.status is _STATUS.CONFIGURATION_CONFLICT
    assert _results(result) == [(_DONE.REFUSED, _WHY.CONFIGURATION_MISMATCH)]
    fresh = Harness(database, Clock())
    empty = EquityIntradayResearchTimeline(
        EquityIntradayResearchTimelineIdentity(M_AND_M, DAY, changed)
    )
    schedule = ScheduleEquityIntradayResearchUseCase(fresh.snapshots, changed).advance(
        empty, [s41.snapshot_id]
    )
    with pytest.raises(EquityIntradayResearchTimelineConfigurationConflictError):
        fresh.timelines.append(stored_equity_intraday_research_timeline(schedule.timeline), None)
    assert (count(database, _TIMELINES), count(database, _ENTRIES)) == (1, before)
    with closing(raw(database)) as connection:
        assert connection.execute(f"SELECT configuration_document FROM {_TIMELINES}").fetchone()[
            0
        ] == canonical_equity_candle_json(CONFIG.document())


# ----------------------------------------------------------------------
# C. Atomicity, interruption, contention and commit outcomes
# ----------------------------------------------------------------------


class _Interrupted(BaseException):
    """Test-only: stands for the process being stopped mid-write."""


def _fail_insert(monkeypatch: pytest.MonkeyPatch, on_call: int, error: BaseException) -> None:
    real = adapter_module._insert_entry
    calls = {"n": 0}

    def insert(connection: sqlite3.Connection, values: tuple[object, ...]) -> None:
        calls["n"] += 1
        if calls["n"] == on_call:
            raise error
        real(connection, values)

    monkeypatch.setattr(adapter_module, "_insert_entry", insert)


def test_a_failed_first_commit_leaves_no_identity_and_appends_are_all_or_nothing(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = _morning(database)
    s31, s36, s41 = harness.at(31, 36, 41)
    intended, head = _intended(harness, [s31, s36, s41])
    _fail_insert(monkeypatch, 2, sqlite3.OperationalError("test: disk full"))
    with pytest.raises(EquityIntradayResearchTimelineStorageError, match="nothing was written"):
        harness.timelines.append(intended, head)
    assert (count(database, _TIMELINES), count(database, _ENTRIES)) == (0, 0)
    _fail_insert(monkeypatch, 1, _Interrupted())
    with pytest.raises(_Interrupted):
        harness.timelines.append(intended, head)
    assert (count(database, _TIMELINES), count(database, _ENTRIES)) == (0, 0)
    monkeypatch.undo()
    harness.advance([s31])
    intended, head = _intended(harness, [s36, s41])
    _fail_insert(monkeypatch, 2, sqlite3.OperationalError("test: disk full"))
    with pytest.raises(EquityIntradayResearchTimelineStorageError):
        harness.timelines.append(intended, head)
    assert _stored_roles(database) == ["NO_SIGNAL"]
    check_invariants(database)


def test_a_writer_that_stops_before_commit_leaves_nothing(database: Path) -> None:
    harness = _morning(database)
    s36, s41 = harness.at(36, 41)
    intended, _ = _intended(harness, [s36])
    harness.timelines.load(CONFIG.strategy, M_AND_M, DAY)  # creates the schema
    writer = raw(database)
    writer.execute("BEGIN IMMEDIATE")
    cursor = writer.execute(
        adapter_module._INSERT_TIMELINE, adapter_module._timeline_values(intended)
    )
    writer.execute(
        adapter_module._INSERT_ENTRY,
        adapter_module._entry_values(cursor.lastrowid, intended.entries[0]),  # type: ignore[arg-type]
    )
    writer.close()  # Test-only: the connection ends without COMMIT, as a stopped process does.
    assert (count(database, _TIMELINES), count(database, _ENTRIES)) == (0, 0)
    assert harness.advance([s36, s41]).status is _STATUS.COMMITTED
    check_invariants(database)


def test_lock_contention_fails_without_writing(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = _morning(database)
    (s36,) = harness.at(36)
    harness.timelines.load(CONFIG.strategy, M_AND_M, DAY)
    monkeypatch.setattr(adapter_module, "_BUSY_TIMEOUT_SECONDS", 0.05)
    holder = raw(database)
    try:
        holder.execute("BEGIN IMMEDIATE")
        with pytest.raises(EquityIntradayResearchTimelineUnavailableError) as raised:
            harness.advance([s36])
        assert isinstance(raised.value, EquityIntradayResearchTimelineStorageError)
        assert not isinstance(raised.value, EquityIntradayResearchTimelineCommitUnconfirmedError)
    finally:
        holder.rollback()
        holder.close()
    assert (count(database, _TIMELINES), count(database, _ENTRIES)) == (0, 0)


def _commit_once(
    monkeypatch: pytest.MonkeyPatch,
    behaviour: Callable[[Callable[[sqlite3.Connection], None], sqlite3.Connection], None],
) -> None:
    """Test-only: replace the next COMMIT, once, with ``behaviour``."""
    real = adapter_module._commit
    armed = {"yes": True}

    def commit(connection: sqlite3.Connection) -> None:
        if not armed["yes"]:
            return real(connection)
        armed["yes"] = False
        return behaviour(real, connection)

    monkeypatch.setattr(adapter_module, "_commit", commit)


def _lost_before(
    real: Callable[[sqlite3.Connection], None], connection: sqlite3.Connection
) -> None:
    raise sqlite3.OperationalError("test: COMMIT failed")


def _lost_after(real: Callable[[sqlite3.Connection], None], connection: sqlite3.Connection) -> None:
    real(connection)
    raise sqlite3.OperationalError("test: acknowledgment lost after COMMIT")


@pytest.mark.parametrize(("behaviour", "durable"), [(_lost_before, 0), (_lost_after, 1)])
def test_a_commit_failure_is_unconfirmed_never_a_rollback_claim(
    database: Path,
    monkeypatch: pytest.MonkeyPatch,
    behaviour,
    durable: int,  # type: ignore[no-untyped-def]
) -> None:
    harness = _morning(database)
    (s36,) = harness.at(36)
    intended, head = _intended(harness, [s36])
    _commit_once(monkeypatch, behaviour)
    with pytest.raises(EquityIntradayResearchTimelineCommitUnconfirmedError) as raised:
        harness.timelines.append(intended, head)
    assert not isinstance(raised.value, EquityIntradayResearchTimelineUnavailableError)
    assert count(database, _ENTRIES) == durable


def test_the_use_case_reconciles_unconfirmed_commits_against_sqlite(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = _morning(database)
    s31, s36, s46, s51 = harness.at(31, 36, 46, 51)
    _commit_once(monkeypatch, _lost_after)
    confirmed = harness.advance([s31])
    assert confirmed.status is _STATUS.COMMITTED and confirmed.reconciled
    assert confirmed.attempts == 1 and _stored_roles(database) == ["NO_SIGNAL"]
    _commit_once(monkeypatch, _lost_before)
    retried = harness.advance([s36])
    assert retried.status is _STATUS.COMMITTED and not retried.reconciled
    assert retried.attempts == 2 and _stored_roles(database) == ["NO_SIGNAL", "CANDIDATE"]

    def then_another_writer(
        real: Callable[[sqlite3.Connection], None], connection: sqlite3.Connection
    ) -> None:
        real(connection)
        other = Harness(database, Clock()).advance([s51])
        assert other.status is _STATUS.COMMITTED
        raise sqlite3.OperationalError("test: acknowledgment lost")

    _commit_once(monkeypatch, then_another_writer)
    later = harness.advance([s46])
    # The unconfirmed s46 entry is found at sequence 3 although the head has
    # moved on to the other writer's 09:51 entry (REJECTED: bar 7 is missing).
    assert later.status is _STATUS.COMMITTED and later.reconciled and later.attempts == 1
    assert later.timeline is not None
    assert [e.snapshot_id for e in later.timeline.entries][2:] == [
        s46.snapshot_id,
        s51.snapshot_id,
    ]
    assert _stored_roles(database) == ["NO_SIGNAL", "CANDIDATE", "REPEAT_QUALIFYING", "REJECTED"]
    check_invariants(database)


def test_a_database_without_the_source_snapshot_cannot_hold_the_entry(
    database: Path, tmp_path: Path
) -> None:
    harness = _morning(database)
    (s36,) = harness.at(36)
    intended, head = _intended(harness, [s36])
    elsewhere = tmp_path / "elsewhere.sqlite3"
    with pytest.raises(EquityIntradayResearchTimelineStorageError, match="nothing was written"):
        SQLiteEquityIntradayResearchTimelineStore(elsewhere).append(intended, head)
    assert count(elsewhere, _TIMELINES) == 0
    assert count(elsewhere, "equity_candle_evidence_snapshots") == 0


# ----------------------------------------------------------------------
# D. Direct SQL cannot bypass the history rules
# ----------------------------------------------------------------------


def _timeline_id(database: Path) -> int:
    with closing(raw(database)) as connection:
        return connection.execute(f"SELECT timeline_id FROM {_TIMELINES}").fetchone()[0]  # type: ignore[no-any-return]


def _head(database: Path) -> dict[str, Any]:
    return entry_rows(database)[-1]


def _row_for(
    database: Path,
    snapshot: EquityCandleEvidenceSnapshot,
    role: EquityIntradayResearchTimelineRole,
) -> dict[str, Any]:
    """A row EI-3C-A would produce for ``snapshot`` after the stored head."""
    head = _head(database)
    decision = EquityOpeningRangeBreakoutEvaluator(CONFIG).evaluate(M_AND_M, DAY, snapshot)
    entry = EquityIntradayResearchTimelineEntry(
        _IDENTITY,
        head["sequence"] + 1,
        role,
        decision,
        snapshot.window_closes_at,
        head["entry_fingerprint"],
    )
    record = StoredEquityIntradayResearchTimelineEntry(
        sequence=entry.sequence,
        role=role,
        decision_outcome=decision.outcome,
        snapshot_id=snapshot.snapshot_id,
        manifest_fingerprint=snapshot.manifest_fingerprint,
        decision_instant=snapshot.as_of,
        watermark=snapshot.watermark,
        research_window_closes_at=snapshot.window_closes_at,
        decision_document=canonical_equity_candle_json(decision.document()),
        decision_fingerprint=decision.fingerprint,
        previous_fingerprint=entry.previous_fingerprint,
        entry_fingerprint=entry.fingerprint,
    )
    values = adapter_module._entry_values(_timeline_id(database), record)
    return dict(zip(adapter_module.ENTRY_COLUMNS, values, strict=True))


def _insert(database: Path, row: dict[str, Any], verb: str = "INSERT") -> None:
    columns = ", ".join(row)
    with closing(raw(database)) as connection:
        connection.execute(
            f"{verb} INTO {_ENTRIES} ({columns}) VALUES ({', '.join('?' * len(row))})",  # noqa: S608
            tuple(row.values()),
        )


class _Raw:
    """A committed NO_SIGNAL/CANDIDATE timeline and spare snapshots for raw writes."""

    def __init__(self, database: Path) -> None:
        self.database = database
        self.harness = _morning(database)
        self.s46 = self.harness.issue(ist(9, 46))  # lower watermark than what follows
        self.harness.unrelated()
        self.s31, self.s34, self.s36, self.s41 = self.harness.at(31, 34, 36, 41)
        self.harness.candles(listing=RELIANCE)
        self.reliance = self.harness.issue(ist(9, 41), listing=RELIANCE)
        self.harness.advance([self.s31, self.s36])
        self.valid = _row_for(database, self.s41, _ROLE.REPEAT_QUALIFYING)


def test_a_valid_raw_row_is_accepted(database: Path) -> None:
    fixture = _Raw(database)
    _insert(database, fixture.valid)
    assert _stored_roles(database) == ["NO_SIGNAL", "CANDIDATE", "REPEAT_QUALIFYING"]
    check_invariants(database)


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"sequence": 4}, "next sequence|chain to the previous"),
        ({"previous_fingerprint": "eirt1-sha256:" + "0" * 64}, "chain to the previous"),
        ({"role": "CANDIDATE"}, "at most one CANDIDATE"),
        ({"decision_instant_us": "+1"}, "exact microseconds"),
        ({"manifest_fingerprint": "ecm1-sha256:" + "0" * 64}, "snapshot|decision document"),
        ({"snapshot_id": "ecs1-sha256:" + "0" * 64}, "snapshot|decision document|FOREIGN"),
        ({"watermark": 0}, "regress|snapshot|decision document"),
        ({"research_window_closes_at": "2026-08-12T10:00:00Z"}, "matching synthetic snapshot"),
        ({"decision_document": "outcome"}, "decision document must describe"),
        (
            {"decision_instant": "2026-08-12T04:12:00Z", "decision_instant_us": "auto"},
            "snapshot|decision document",
        ),
    ],
)
def test_raw_rows_that_break_the_history_rules_are_refused(
    database: Path, changes: dict[str, Any], message: str
) -> None:
    fixture = _Raw(database)
    row = dict(fixture.valid)
    for column, value in changes.items():
        if value == "+1":
            row[column] += 1
        elif value == "auto":
            row[column] = decision_instant_microseconds(PointInTime(row["decision_instant"]))
        elif value == "outcome":
            document = json.loads(row["decision_document"])
            document["outcome"] = "NO_SIGNAL"
            row[column] = canonical_equity_candle_json(document)
        else:
            row[column] = value
    with pytest.raises(sqlite3.IntegrityError, match=message):
        _insert(database, row)
    assert count(database, _ENTRIES) == 2
    check_invariants(database)


def test_raw_rows_for_an_earlier_instant_a_regressed_watermark_or_another_listing_are_refused(
    database: Path,
) -> None:
    fixture = _Raw(database)
    earlier = _row_for(database, fixture.s34, _ROLE.NO_SIGNAL)
    with pytest.raises(sqlite3.IntegrityError, match="strictly increase"):
        _insert(database, earlier)
    regressed = _row_for(database, fixture.s46, _ROLE.REPEAT_QUALIFYING)
    assert regressed["watermark"] < _head(database)["watermark"]
    with pytest.raises(sqlite3.IntegrityError, match="regress"):
        _insert(database, regressed)
    other = dict(fixture.valid)
    reliance = fixture.reliance
    other.update(
        snapshot_id=reliance.snapshot_id,
        manifest_fingerprint=reliance.manifest_fingerprint,
        watermark=reliance.watermark,
        decision_document=fixture.valid["decision_document"]
        .replace(fixture.s41.snapshot_id, reliance.snapshot_id)
        .replace(fixture.s41.manifest_fingerprint, reliance.manifest_fingerprint),
    )
    with pytest.raises(sqlite3.IntegrityError, match="snapshot|decision document"):
        _insert(database, other)
    assert count(database, _ENTRIES) == 2


def test_a_raw_row_on_non_synthetic_evidence_is_refused(database: Path) -> None:
    fixture = _Raw(database)
    forged = "ecs1-sha256:" + "f" * 64
    columns = (
        "manifest_fingerprint, symbol, exchange_code, trading_date, timeframe, "
        "window_policy_version, finality_policy_version, as_of, issued_at, watermark, "
        "coverage, entry_count, document"
    )
    with closing(raw(database)) as connection:
        # Test-only: a PRODUCTION-classed copy of s41's snapshot row under another identity.
        connection.execute(
            f"INSERT INTO equity_candle_evidence_snapshots (snapshot_id, evidence_class, "  # noqa: S608
            f"{columns}) SELECT ?, 'PRODUCTION', {columns} FROM "
            "equity_candle_evidence_snapshots WHERE snapshot_id = ?",
            (forged, fixture.s41.snapshot_id),
        )
    row = dict(fixture.valid)
    row["snapshot_id"] = forged
    row["decision_document"] = row["decision_document"].replace(fixture.s41.snapshot_id, forged)
    with pytest.raises(sqlite3.IntegrityError, match="matching synthetic snapshot"):
        _insert(database, row)


def test_repeat_qualifying_needs_an_earlier_candidate(database: Path) -> None:
    harness = _morning(database)
    s31, s36 = harness.at(31, 36)
    harness.advance([s31])
    with pytest.raises(sqlite3.IntegrityError, match="earlier CANDIDATE"):
        _insert(database, _row_for(database, s36, _ROLE.REPEAT_QUALIFYING))


@pytest.mark.parametrize(
    "statement",
    [
        f"UPDATE {_TIMELINES} SET configuration_version = 'synthetic-other/1'",
        f"UPDATE {_ENTRIES} SET role = 'REJECTED'",
        f"DELETE FROM {_ENTRIES} WHERE sequence = 2",
        f"DELETE FROM {_TIMELINES}",
        f"INSERT OR REPLACE INTO {_ENTRIES} SELECT * FROM {_ENTRIES} WHERE sequence = 1",
        f"INSERT OR REPLACE INTO {_TIMELINES} SELECT * FROM {_TIMELINES}",
        f"INSERT INTO {_TIMELINES} (strategy, symbol, exchange_code, trading_date, "
        "configuration_version, configuration_document, identity_document) SELECT strategy, "
        "symbol, exchange_code, '2026-09-07', 'synthetic-other/1', configuration_document, "
        f"identity_document FROM {_TIMELINES}",
    ],
)
def test_the_database_refuses_to_rewrite_history(database: Path, statement: str) -> None:
    fixture = _Raw(database)
    before = entry_rows(database)
    with closing(raw(database)) as connection, pytest.raises(sqlite3.IntegrityError):
        connection.execute(statement)
    assert entry_rows(database) == before and count(database, _TIMELINES) == 1
    assert fixture.harness.timelines.load(CONFIG.strategy, M_AND_M, DAY) is not None


def test_an_identity_row_without_entries_fails_closed_on_load(database: Path) -> None:
    harness = _morning(database)
    (s36,) = harness.at(36)
    intended, _ = _intended(harness, [s36])
    harness.timelines.load(CONFIG.strategy, M_AND_M, DAY)
    with closing(raw(database)) as connection:
        # Test-only: direct SQL can insert an identity alone; the store refuses to load it.
        connection.execute(
            adapter_module._INSERT_TIMELINE, adapter_module._timeline_values(intended)
        )
    with pytest.raises(EquityIntradayResearchTimelineStorageError, match="invalid data"):
        harness.timelines.load(CONFIG.strategy, M_AND_M, DAY)


# ----------------------------------------------------------------------
# E. Tampered history fails closed (triggers dropped on disposable databases)
# ----------------------------------------------------------------------


def _bypass(database: Path, *statements: str, foreign_keys: bool = True) -> None:
    """Test-only: drop the guarding triggers and alter committed rows directly."""
    with closing(sqlite3.connect(database, isolation_level=None)) as connection:
        connection.execute(f"PRAGMA foreign_keys = {'ON' if foreign_keys else 'OFF'}")
        for trigger in (
            f"{_ENTRIES}_no_update",
            "equity_candle_evidence_snapshots_no_delete",
            "equity_candle_evidence_snapshot_entries_no_delete",
        ):
            connection.execute(f"DROP TRIGGER IF EXISTS {trigger}")
        for statement in statements:
            connection.execute(statement)


def _tampered() -> list[tuple[str, type[Exception], Callable[[_Raw], list[str]]]]:
    def document(fixture: _Raw, **changes: Any) -> str:
        row = entry_rows(fixture.database)[1]
        decoded = json.loads(row["decision_document"])
        decoded.update(changes)
        return canonical_equity_candle_json(decoded)

    def update(column: str, value: str, sequence: int = 2) -> str:
        return f"UPDATE {_ENTRIES} SET {column} = '{value}' WHERE sequence = {sequence}"

    def rehashed(text: str) -> str:
        return "eir1-sha256:" + hashlib.sha256(text.encode()).hexdigest()

    def other_format(fixture: _Raw) -> list[str]:
        text = document(fixture, format="northstar.equity-intraday-research-decision/2")
        return [update("decision_document", text), update("decision_fingerprint", rehashed(text))]

    return [
        (
            "decision document",
            InvalidStoredEquityIntradayResearchTimelineError,
            lambda f: [update("decision_document", document(f, requested_snapshot_id="x"))],
        ),
        (
            "entry fingerprint",
            InvalidStoredEquityIntradayResearchTimelineError,
            lambda f: [update("entry_fingerprint", "eirt1-sha256:" + "0" * 64)],
        ),
        (
            "decision format",
            EquityIntradayResearchHistoricalEvaluationUnavailableError,
            other_format,
        ),
        (
            "instant key",
            EquityIntradayResearchTimelineStorageError,
            lambda f: [f"UPDATE {_ENTRIES} SET decision_instant_us = decision_instant_us + 1"],
        ),
    ]


@pytest.mark.parametrize(
    ("name", "error", "statements"), _tampered(), ids=[t[0] for t in _tampered()]
)
def test_tampered_history_fails_closed_and_writes_nothing(
    database: Path,
    name: str,
    error: type[Exception],
    statements,  # type: ignore[no-untyped-def]
) -> None:
    fixture = _Raw(database)
    _bypass(database, *statements(fixture))
    before = count(database, _ENTRIES)
    with pytest.raises(error):
        fixture.harness.advance([fixture.s41])
    assert count(database, _ENTRIES) == before
    with closing(raw(database)) as connection:  # the adapter recreated the dropped trigger
        assert connection.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE name = ?", (f"{_ENTRIES}_no_update",)
        ).fetchone() == (1,)


def test_a_missing_source_snapshot_fails_closed(database: Path) -> None:
    fixture = _Raw(database)
    snapshot = fixture.s31.snapshot_id
    _bypass(
        database,
        f"DELETE FROM equity_candle_evidence_snapshot_entries WHERE snapshot_id = '{snapshot}'",
        f"DELETE FROM equity_candle_evidence_snapshots WHERE snapshot_id = '{snapshot}'",
        foreign_keys=False,
    )
    with pytest.raises(InvalidStoredEquityIntradayResearchTimelineError, match="unavailable"):
        fixture.harness.advance([fixture.s41])
    assert count(database, _ENTRIES) == 2


# ----------------------------------------------------------------------
# F. Concurrent writers (EI3-D12)
# ----------------------------------------------------------------------


class _Paused(SQLiteEquityIntradayResearchTimelineStore):
    """Pauses each of the first ``pauses`` appends after EI-3C-A evaluated, before writing.

    It signals ``arrived[k]`` and waits for ``go[k]``. Synchronisation is
    explicit -- no sleeps, no timing.
    """

    def __init__(self, database: Path, pauses: int = 1) -> None:
        super().__init__(database)
        self.arrived = [threading.Event() for _ in range(pauses)]
        self.go = [threading.Event() for _ in range(pauses)]
        self.calls = 0

    def append(
        self, timeline: StoredEquityIntradayResearchTimeline, expected_head_fingerprint: str | None
    ) -> None:
        index = self.calls
        self.calls += 1
        if index < len(self.arrived):
            self.arrived[index].set()
            assert self.go[index].wait(timeout=30), "the test never resumed the writer"
        super().append(timeline, expected_head_fingerprint)


def _race(
    database: Path,
    loser: list[EquityCandleEvidenceSnapshot],
    winners: list[list[EquityCandleEvidenceSnapshot]],
    loser_listing: Any = M_AND_M,
    winner_listing: Any = M_AND_M,
) -> EquityIntradayResearchTimelineAdvance:
    """``loser`` evaluates first; each ``winners`` batch commits while it waits to write."""
    paused = _Paused(database, len(winners))
    outcome: dict[str, Any] = {}

    def run() -> None:
        try:
            outcome["result"] = Harness(database, Clock()).advance(
                loser, listing=loser_listing, timelines=paused
            )
        except BaseException as exc:  # noqa: BLE001
            outcome["error"] = exc

    thread = threading.Thread(target=run)
    thread.start()
    for index, batch in enumerate(winners):
        assert paused.arrived[index].wait(timeout=30)
        committed = Harness(database, Clock()).advance(batch, listing=winner_listing)
        assert committed.status is _STATUS.COMMITTED
        paused.go[index].set()
    thread.join(timeout=60)
    assert not thread.is_alive() and "error" not in outcome, outcome.get("error")
    check_invariants(database)
    return outcome["result"]  # type: ignore[no-any-return]


def test_a_two_first_candidate_writers_leave_one_candidate(database: Path) -> None:
    harness = _morning(database)
    s36, s41 = harness.at(36, 41)
    result = _race(database, [s41], [[s36]])
    assert result.status is _STATUS.COMMITTED and result.attempts == 2
    assert _roles(result) == [_ROLE.CANDIDATE, _ROLE.REPEAT_QUALIFYING]
    assert [row["snapshot_id"] for row in entry_rows(database)] == [
        s36.snapshot_id,
        s41.snapshot_id,
    ]


def test_b_the_same_snapshot_is_a_duplicate_after_the_retry(database: Path) -> None:
    harness = _morning(database)
    (s36,) = harness.at(36)
    result = _race(database, [s36], [[s36]])
    assert result.status is _STATUS.NOTHING_TO_COMMIT and result.attempts == 2
    assert _results(result) == [(_DONE.DUPLICATE, None)]
    assert count(database, _ENTRIES) == 1


def test_c_the_first_committed_equivalent_snapshot_stays_authoritative(database: Path) -> None:
    harness = _morning(database)
    x = harness.issue(ist(9, 36))
    y = harness.issue(ist(9, 36), issued_at=ist(9, 40))
    smaller, larger = sorted((x, y), key=lambda s: s.snapshot_id)
    assert smaller.manifest_fingerprint == larger.manifest_fingerprint
    result = _race(database, [smaller], [[larger]])
    assert _results(result) == [(_DONE.DUPLICATE, None)]
    assert [row["snapshot_id"] for row in entry_rows(database)] == [larger.snapshot_id]


def test_d_conflicting_manifests_are_refused_together_and_after_a_commit(database: Path) -> None:
    harness = _morning(database)
    a = harness.issue(ist(9, 36))
    harness.unrelated()
    b = harness.issue(ist(9, 36), issued_at=ist(9, 37))
    assert a.as_of == b.as_of and a.manifest_fingerprint != b.manifest_fingerprint
    together = harness.advance([a, b])
    assert together.status is _STATUS.NOTHING_TO_COMMIT
    assert _results(together) == [(_DONE.REFUSED, _WHY.CONFLICTING_EVIDENCE_FOR_INSTANT)] * 2
    assert count(database, _TIMELINES) == 0
    result = _race(database, [b], [[a]])
    assert _results(result) == [(_DONE.REFUSED, _WHY.CONFLICTING_EVIDENCE_FOR_INSTANT)]
    assert [row["snapshot_id"] for row in entry_rows(database)] == [a.snapshot_id]


def test_e_a_candidate_writer_recomputes_its_role_after_a_no_signal_commit(
    database: Path,
) -> None:
    harness = _morning(database)
    s31, s36 = harness.at(31, 36)
    result = _race(database, [s36], [[s31]])
    assert result.status is _STATUS.COMMITTED
    assert _stored_roles(database) == ["NO_SIGNAL", "CANDIDATE"]
    assert result.items[0].entry is not None and result.items[0].entry.sequence == 2


def test_f_an_earlier_instant_after_a_later_commit_is_refused(database: Path) -> None:
    harness = _morning(database)
    s36, s41 = harness.at(36, 41)
    result = _race(database, [s36], [[s41]])
    assert result.status is _STATUS.NOTHING_TO_COMMIT
    assert _results(result) == [(_DONE.REFUSED, _WHY.BEFORE_TIMELINE_HEAD)]
    assert _stored_roles(database) == ["CANDIDATE"]


def test_g_stale_heads_on_every_attempt_exhaust_the_bounded_retries(database: Path) -> None:
    harness = _morning(database)
    s31, s36, s41, s46 = harness.at(31, 36, 41, 46)
    result = _race(database, [s46], [[s31], [s36], [s41]])
    assert result.status is _STATUS.STALE_AFTER_RETRIES and result.attempts == 3
    assert result.timeline is None and not result.commit_confirmed
    assert [row["snapshot_id"] for row in entry_rows(database)] == [
        s31.snapshot_id,
        s36.snapshot_id,
        s41.snapshot_id,
    ]


def test_h_writers_of_independent_timelines_never_go_stale(database: Path) -> None:
    harness = _morning(database)
    harness.candles(listing=RELIANCE)
    (s36,) = harness.at(36)
    reliance = harness.issue(ist(9, 36), listing=RELIANCE)
    result = _race(database, [s36], [[reliance]], winner_listing=RELIANCE)
    assert result.status is _STATUS.COMMITTED and result.attempts == 1
    assert count(database, _TIMELINES) == 2


def test_i_unsynchronised_writers_still_leave_a_valid_history(database: Path) -> None:
    harness = _morning(database)
    batches = [[s] for s in harness.at(31, 36, 41, 46)]
    start = threading.Barrier(len(batches))
    results: list[Any] = []

    def run(batch: list[EquityCandleEvidenceSnapshot]) -> None:
        start.wait(timeout=30)
        try:
            results.append(Harness(database, Clock()).advance(batch))
        except BaseException as exc:  # noqa: BLE001
            results.append(exc)

    threads = [threading.Thread(target=run, args=(batch,)) for batch in batches]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=120)
    assert all(isinstance(r, EquityIntradayResearchTimelineAdvance) for r in results), results
    check_invariants(database)
    assert sum(role == "CANDIDATE" for role in _stored_roles(database)) <= 1


# ----------------------------------------------------------------------
# G. Isolation
# ----------------------------------------------------------------------


def test_listings_dates_and_strategies_keep_independent_timelines(database: Path) -> None:
    harness = _morning(database)
    harness.candles(listing=RELIANCE)
    harness.candles(day=OTHER_DAY)
    other_strategy = dataclasses.replace(CONFIG, strategy=StrategyIdentity("ei3c-sqlite-other"))
    runs = [
        (M_AND_M, DAY, CONFIG, harness.issue(ist(9, 36))),
        (RELIANCE, DAY, CONFIG, harness.issue(ist(9, 36), listing=RELIANCE)),
        (M_AND_M, OTHER_DAY, CONFIG, harness.issue(ist(9, 36, day=OTHER_DAY), day=OTHER_DAY)),
        (M_AND_M, DAY, other_strategy, harness.issue(ist(9, 41))),
    ]
    for listing, day, configuration, snapshot in runs:
        result = harness.advance([snapshot], configuration=configuration, listing=listing, day=day)
        assert result.status is _STATUS.COMMITTED and _roles(result) == [_ROLE.CANDIDATE]
    assert count(database, _TIMELINES) == 4
    for listing, day, configuration, _ in runs:
        stored = harness.timelines.load(configuration.strategy, listing, day)
        assert stored is not None and len(stored.entries) == 1
        assert (stored.listing, stored.trading_date) == (listing, day)
    check_invariants(database)


def _date_rows(database: Path) -> list[tuple[str, str]]:
    with closing(raw(database)) as connection:
        return connection.execute(
            f"SELECT symbol, trading_date FROM {_TIMELINES} ORDER BY timeline_id"
        ).fetchall()


def test_a_reopened_store_loads_what_another_process_committed(database: Path) -> None:
    harness = _morning(database)
    committed = harness.advance(harness.at(31, 36, 41))
    reopened = SQLiteEquityIntradayResearchTimelineStore(database)
    stored = reopened.load(CONFIG.strategy, M_AND_M, DAY)
    assert stored is not None and committed.timeline is not None
    assert stored == stored_equity_intraday_research_timeline(committed.timeline)
    assert reopened.load(CONFIG.strategy, M_AND_M, date(2026, 10, 19)) is None
    assert _date_rows(database) == [("M&M", "2026-08-12")]


# ----------------------------------------------------------------------
# H. Scope
# ----------------------------------------------------------------------


@pytest.mark.parametrize("module", [adapter_module, schema_module])
def test_the_store_stays_a_persistence_adapter(module) -> None:  # type: ignore[no-untyped-def]
    source = inspect.getsource(module)
    docstring = ast.get_docstring(ast.parse(source), clean=False)
    assert docstring is not None
    source = source.replace(docstring, "")  # prose may name what the code never does
    for forbidden in (
        "OR REPLACE INTO",
        "ON CONFLICT",
        "foreign_keys = OFF",
        "EquityOpeningRangeBreakoutEvaluator",
        "rehydrate",
        "Schedule",
        "socket",
        "http",
        "broker",
        "paper",
        "futures",
    ):
        assert forbidden not in source, forbidden
    calls = {
        getattr(node.func, "id", None)
        for node in ast.walk(ast.parse(inspect.getsource(module)))
        if isinstance(node, ast.Call)
    }
    assert not calls & {"float", "eval", "exec"}
    imported = set(re.findall(r"^\s*(?:from|import) ([\w.]+)", source, re.MULTILINE))
    assert all(
        name.split(".")[0]
        in {
            "__future__",
            "json",
            "sqlite3",
            "collections",
            "contextlib",
            "datetime",
            "decimal",
            "pathlib",
            "typing",
            "northstar_application",
            "northstar_core",
            "northstar_infrastructure",
        }
        for name in imported
    ), imported
