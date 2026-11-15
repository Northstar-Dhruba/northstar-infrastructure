"""EI-6C: the merged Application monitor over the SQLite catalog and the EI-6B store.

Acceptance of the EI-6C Lifecycle Monitoring and Snapshot Selection Design
v1.0 (section 12) at layer S, composed end to end: the caller's instant T ->
MonitorEquityRecommendationLifecyclesUseCase (merged Application PR-1) ->
SQLiteEquityCandleEvidenceSnapshotCatalog (read-only) -> authenticated EI-2B
snapshots -> chronological selection -> RecordEquityRecommendationLifecycleUseCase
-> SQLiteEquityRecommendationLifecycleStore -> Core replay. Test names carry
the MS identifiers; the separate-process rows are
tests/test_equity_lifecycle_monitoring_sqlite_processes.py.

The issued TCS record (tests/equity_recommendation_lifecycle_fixtures.py):
issued 09:43:30 IST, eligible from 09:45, entry zone 252.25-253.5, stop 249,
target 262.5, latest entry bar 10:45 (window ends 10:50), modeled exit 14:00.
The research snapshots at 09:36 and 09:41 precede the issuance and are never
candidates; the one at 09:46 is the first candidate and opens the entry window.

Every lifecycle event is decided by Core through the real recorder and
written through the EI-6B adapter's authorized ``append``. Raw SQL appears
only for TEST-ONLY corruption of a disposable copy. Nothing here opens a
network connection or touches a production database.
"""

from __future__ import annotations

import socket
import sqlite3
from collections.abc import Iterator
from contextlib import closing
from pathlib import Path
from typing import Any

import pytest
from northstar_application.application_services import (
    EQUITY_LIFECYCLE_MONITORING_POLICY,
    EquityLifecycleMonitoringDeclaredSessionScope,
    EquityLifecycleMonitoringDiagnosticKind,
    EquityLifecycleMonitoringExplicitScope,
    EquityLifecycleMonitoringOutcome,
    EquityLifecycleMonitoringRefusal,
    EquityLifecycleMonitoringStrategyListingScope,
    EquityLifecycleRecordingOutcome,
    EquityLifecycleRecordingRefusal,
    decode_equity_lifecycle_event_document,
)
from northstar_core.recommendations.equity.lifecycle import (
    EquityLifecycleEventKind,
    EquityLifecycleState,
)

from northstar_infrastructure.persistence import SQLiteEquityRecommendationLifecycleStore
from northstar_infrastructure.persistence import (
    sqlite_equity_recommendation_lifecycle as lifecycle_module,
)
from tests.equity_lifecycle_monitoring_fixtures import (
    SpyRecorder,
    copy_world,
    foreign_currency_snapshot,
    instant,
    issue,
    lifecycle_rows,
    result,
    run,
)
from tests.equity_recommendation_acceptance_fixtures import (
    DAY,
    INFY,
    NSE,
    SNAPSHOTS,
    TCS,
    bar_end,
    bypass,
    check_database,
    events,
    ist,
    raw,
    schema_objects,
)
from tests.equity_recommendation_issuance_fixtures import establish_infy_breakout, infy
from tests.equity_recommendation_lifecycle_fixtures import (
    CANCEL_BAR,
    ENTRY_BAR,
    LIFECYCLE_EVENTS,
    QUIET_BAR,
    TARGET_BAR,
    LifecycleWorld,
    lifecycle_world,
)

M = EquityLifecycleMonitoringOutcome
K = EquityLifecycleEventKind
S = EquityLifecycleState
_O = EquityLifecycleRecordingOutcome
_R = EquityLifecycleRecordingRefusal
_MR = EquityLifecycleMonitoringRefusal
_LATE = EquityLifecycleMonitoringDiagnosticKind.LATE_HISTORICAL_SNAPSHOT_IGNORED
CROWD = 297  # with the three research snapshots: 300 eligible at 09:46


@pytest.fixture(autouse=True)
def _refuse_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("EI-6C tests must not open network connections.")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)


@pytest.fixture(scope="module")
def template(tmp_path_factory: pytest.TempPathFactory) -> LifecycleWorld:
    """The EI-6B world; its lifecycle objects added by an ordinary EI-6B load (empty history)."""
    world = lifecycle_world(tmp_path_factory.mktemp("ei6c-monitoring") / "template.sqlite3")
    loaded = world.store().load_lifecycle(world.issuance_id)
    assert loaded is not None and loaded.lifecycle.events == ()
    return world


@pytest.fixture
def world(template: LifecycleWorld, tmp_path: Path) -> LifecycleWorld:
    return copy_world(template, tmp_path / "ei6c.sqlite3")


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------


def _documents(world: LifecycleWorld, issuance_id: str | None = None) -> list[str]:
    with closing(raw(world.database)) as connection:
        return [
            row[0]
            for row in connection.execute(
                f"SELECT event_document FROM {LIFECYCLE_EVENTS} WHERE issuance_id = ? "  # noqa: S608
                "ORDER BY event_sequence",
                (issuance_id or world.issuance_id,),
            )
        ]


def _events(world: LifecycleWorld) -> list[Any]:
    return [decode_equity_lifecycle_event_document(d) for d in _documents(world)]


def _kinds(world: LifecycleWorld) -> list[K]:
    return [event.kind for event in _events(world)]


def _steps(outcome: Any) -> list[tuple[str, _O]]:
    return [(step.as_of.value, step.outcome) for step in outcome.steps]


def _snapshot_count(world: LifecycleWorld) -> int:
    with closing(raw(world.database)) as connection:
        return connection.execute(f"SELECT COUNT(*) FROM {SNAPSHOTS}").fetchone()[0]  # noqa: S608


def _progressed(world: LifecycleWorld) -> dict[str, str]:
    """Bars 7 (entry) and 8 (target); snapshots 09:46:30, 09:51 and 09:56."""
    return {"opened": world.opened(), "activated": world.activated(), "targeted": world.targeted()}


def _twin_below(world: LifecycleWorld, anchor: str, at: Any) -> str:
    """An equivalent snapshot at the anchor's instant with a LOWER identity (issued later)."""
    for second in range(1, 120):
        twin = issue(world, at, instant(at, 60 + second))
        if twin < anchor:
            return twin
    raise AssertionError("no lower-identity twin found")  # pragma: no cover


# ----------------------------------------------------------------------
# MS-01, MS-05, MS-06, MS-07, MS-08: the monitored path on real SQLite
# ----------------------------------------------------------------------


def test_ms01_no_eligible_candidate_records_nothing(world: LifecycleWorld) -> None:
    objects = schema_objects(world.database)
    outcome = result(world, ist(9, 45))  # 09:36 and 09:41 are listed, but precede issuance
    assert (outcome.outcome, outcome.steps, outcome.state) == (
        M.NO_ELIGIBLE_SNAPSHOT,
        (),
        S.RECORDED,
    )
    assert lifecycle_rows(world.database) == []
    assert schema_objects(world.database) == objects


def test_ms08_ms38_monitoring_reaches_a_terminal_state_one_snapshot_per_recorder_call(
    world: LifecycleWorld,
) -> None:
    snapshots = _progressed(world)
    spy = SpyRecorder(world.database)
    outcome = result(world, ist(9, 57), spy)
    assert _steps(outcome) == [
        (ist(9, 46).value, _O.APPENDED),  # research snapshot: ENTRY_WINDOW_OPENED
        (ist(9, 46, 30).value, _O.NO_NEW_EVENTS),
        (ist(9, 51).value, _O.APPENDED),
        (ist(9, 56).value, _O.APPENDED),
    ]
    assert (outcome.outcome, outcome.state) == (M.APPENDED, S.CLOSED_TARGET)
    assert all(len(call) == 1 for call in spy.calls) and len(spy.calls) == 4
    assert [call[0] for call in spy.calls][1:] == list(snapshots.values())
    assert _kinds(world) == [
        K.ENTRY_WINDOW_OPENED,
        K.ENTRY_ACTIVATED,
        K.TARGET_REACHED,
    ]
    assert [e.sequence for e in outcome.appended] == [1, 2, 3]
    reloaded = SQLiteEquityRecommendationLifecycleStore(world.database).load_lifecycle(
        world.issuance_id
    )
    assert reloaded is not None and reloaded.lifecycle.state is S.CLOSED_TARGET
    check_database(world.database)


def test_ms05_ms06_a_rerun_and_a_lower_instant_write_nothing(world: LifecycleWorld) -> None:
    _progressed(world)
    result(world, ist(9, 57))
    before = lifecycle_rows(world.database)
    for at in (ist(9, 57), ist(9, 52), ist(9, 47)):
        again = result(world, at)
        assert again.outcome.is_success and again.appended == ()
        assert lifecycle_rows(world.database) == before


def test_ms07_quiet_evaluations_leave_no_row_cursor_or_checkpoint(world: LifecycleWorld) -> None:
    world.opened()  # 09:46:30: nothing new after the window opened
    first = result(world, ist(9, 47))
    assert _steps(first) == [
        (ist(9, 46).value, _O.APPENDED),
        (ist(9, 46, 30).value, _O.NO_NEW_EVENTS),
    ]
    rows, objects = lifecycle_rows(world.database), schema_objects(world.database)
    for _ in range(2):  # re-evaluated on every run: no quiet-evaluation cursor exists
        quiet = result(world, ist(9, 48))
        assert (quiet.outcome, _steps(quiet)) == (
            M.NO_NEW_EVENTS,
            [(ist(9, 46, 30).value, _O.NO_NEW_EVENTS)],
        )
    assert lifecycle_rows(world.database) == rows
    assert schema_objects(world.database) == objects  # no checkpoint, cursor or run table


# ----------------------------------------------------------------------
# MS-04, MS-34, MS-36, MS-40: run frequency and declared issuance instants
# ----------------------------------------------------------------------


def test_ms04_split_runs_equal_one_later_run_when_prefixes_are_stable(
    world: LifecycleWorld, tmp_path: Path
) -> None:
    _progressed(world)
    single = copy_world(world, tmp_path / "single.sqlite3")
    for at in (ist(9, 47), ist(9, 52), ist(9, 57)):
        result(world, at)
    result(single, ist(9, 57))
    assert _documents(world) == _documents(single)


def test_ms34_ms36_a_fixed_catalog_with_unstable_eligibility_is_path_dependent(
    world: LifecycleWorld, tmp_path: Path
) -> None:
    world.bars({7: ENTRY_BAR, 8: TARGET_BAR})
    a = issue(world, ist(9, 56))  # as_of 09:56, issued 09:56
    b = issue(world, ist(9, 51), ist(10, 0))  # as_of 09:51, declared issued 10:00
    single = copy_world(world, tmp_path / "single.sqlite3")
    first = result(world, ist(9, 57))
    assert first.state is S.CLOSED_TARGET
    late = result(world, ist(10, 1))
    assert late.appended == () and [(d.kind, d.snapshot_id) for d in late.diagnostics] == [
        (_LATE, b)
    ]
    one = result(single, ist(10, 1))
    assert one.state is S.CLOSED_TARGET and one.diagnostics == ()
    split_activation, single_activation = (
        next(e for e in _events(w) if e.kind is K.ENTRY_ACTIVATED) for w in (world, single)
    )
    assert (split_activation.evaluated_at, split_activation.evidence.reference) == (ist(9, 56), a)
    assert (single_activation.evaluated_at, single_activation.evidence.reference) == (
        ist(9, 51),
        b,
    )  # accepted path dependence: no convergence is attempted or required


def test_ms40_a_future_declared_snapshot_is_selected_only_from_its_declared_issuance(
    world: LifecycleWorld,
) -> None:
    world.bars({7: ENTRY_BAR})
    future = issue(world, ist(9, 51), ist(10, 0))
    before = result(world, ist(9, 59, 59))  # retrospective: as_of <= T, issued_at > T
    assert future not in before.recorded_snapshot_ids and before.state is S.WAITING_FOR_ENTRY
    at = result(world, ist(10, 0))  # issued_at == T
    assert [s.snapshot_id for s in at.steps] == [future]
    assert at.state is S.ACTIVE and _events(world)[-1].evaluated_at == ist(9, 51)


def test_ms36_ms37_ms28_late_evidence_after_an_event_never_rewrites_history(
    world: LifecycleWorld,
) -> None:
    world.bars({7: ENTRY_BAR})
    anchor = issue(world, ist(9, 51))
    assert result(world, ist(9, 52)).state is S.ACTIVE
    recorded = _documents(world)
    # MS-36: an older snapshot declared issued after the anchor: diagnosed, not evaluated.
    old = issue(world, ist(9, 49), ist(9, 58))
    late = result(world, ist(9, 58))
    assert [(d.kind, d.snapshot_id) for d in late.diagnostics] == [(_LATE, old)]
    assert late.appended == () and _documents(world) == recorded
    # MS-37: an equivalent snapshot with a lower identity: the cited reference is kept.
    twin = _twin_below(world, anchor, ist(9, 51))
    kept = result(world, ist(9, 59))
    assert kept.appended == () and _documents(world) == recorded
    assert _events(world)[-1].evidence.reference == anchor != twin
    # MS-28: another manifest at the anchor's instant: a conflict; nothing appended.
    world.bars({1: QUIET_BAR}, listing=INFY)  # the journal grows: a later watermark
    rival = issue(world, ist(9, 51), ist(9, 59, 30))
    stopped = result(world, ist(10, 0))
    assert (stopped.outcome, stopped.stopped_at, stopped.steps) == (
        M.STOPPED_AT_CONFLICT,
        ist(9, 51),
        (),
    )
    assert {anchor, rival} <= set(stopped.stop_snapshot_ids)
    assert _documents(world) == recorded  # the committed event stays authoritative


def test_ms13_ms14_a_later_instant_revision_after_a_terminal_state_is_recorded_once(
    world: LifecycleWorld,
) -> None:
    _progressed(world)
    assert result(world, ist(9, 57)).state is S.CLOSED_TARGET
    source, end = events(), bar_end(8)
    world.evidence().ingestion.execute(
        [
            source.observation(
                end, revision="r2", corrects="r1", prices=("253.4", "263.5", "253.1", "262.9")
            ),
            source.finality(end, revision="r2"),
        ]
    )
    world.snapshot(9, 58)
    revised = result(world, ist(9, 59))
    (revision,) = revised.appended
    assert (revision.kind, revision.sequence, revised.state) == (
        K.SOURCE_REVISION_RECORDED,
        4,
        S.CLOSED_TARGET,
    )
    assert result(world, ist(9, 59)).appended == ()  # recorded once
    assert [r[1] for r in lifecycle_rows(world.database)] == [1, 2, 3, 4]


def test_ms14_a_terminal_lifecycle_records_no_state_change(world: LifecycleWorld) -> None:
    world.bars({7: CANCEL_BAR})
    world.snapshot(9, 51)
    assert result(world, ist(9, 52)).state is S.CANCELLED_BEFORE_ENTRY
    before = lifecycle_rows(world.database)
    world.bars({8: ENTRY_BAR})  # a later touch of the zone
    world.snapshot(9, 56)
    later = result(world, ist(9, 57))
    assert (later.outcome, later.state, later.appended) == (
        M.NO_NEW_EVENTS,
        S.CANCELLED_BEFORE_ENTRY,
        (),
    )
    assert lifecycle_rows(world.database) == before


# ----------------------------------------------------------------------
# MS-09, MS-10, MS-12, MS-15: O-2 option A (delta = 0), EI5-D7, O-3 option (b)
# ----------------------------------------------------------------------


def test_ms09_ms10_ms12_the_entry_window_boundary_blocks_exactly_then_is_terminal(
    world: LifecycleWorld,
) -> None:
    world.bars({7: QUIET_BAR})
    world.snapshot(10, 1)  # bars 8 and 9 not yet journaled: blocked before the boundary
    blocked = result(world, ist(10, 1))
    assert (blocked.state, _kinds(world)) == (S.WAITING_FOR_ENTRY, [K.ENTRY_WINDOW_OPENED])
    world.bars({index: QUIET_BAR for index in range(8, 19)})  # the bar ending 10:50 never comes
    world.snapshot(10, 50)
    at_boundary = result(world, ist(10, 50))
    assert at_boundary.state is S.WAITING_FOR_ENTRY and at_boundary.appended == ()
    world.snapshot(10, 51)
    after = result(world, ist(10, 51))  # T is the snapshot's instant: no allowance, no deferral
    assert after.state is S.INCOMPLETE_EVIDENCE
    assert [(e.kind, e.evaluated_at) for e in after.appended] == [
        (K.EVIDENCE_INCOMPLETE, ist(10, 51))
    ]


def test_ms09_the_modeled_exit_boundary_blocks_exactly_then_is_terminal(
    world: LifecycleWorld,
) -> None:
    world.bars({7: ENTRY_BAR})
    world.snapshot(9, 51)
    assert result(world, ist(9, 52)).state is S.ACTIVE
    world.bars({index: QUIET_BAR for index in range(8, 57)})  # the bar ending 14:00 never comes
    world.snapshot(14, 0)
    at_exit = result(world, ist(14, 0))
    assert at_exit.state is S.ACTIVE and at_exit.appended == ()
    world.snapshot(14, 1)
    after = result(world, ist(14, 1))
    assert after.state is S.INCOMPLETE_EVIDENCE
    assert [e.kind for e in after.appended] == [K.EVIDENCE_INCOMPLETE]


def test_ms15_ms10_late_gap_filling_after_incomplete_evidence_records_nothing(
    world: LifecycleWorld,
) -> None:
    world.snapshot(10, 51)  # bar 7 onwards never arrived
    incomplete = result(world, ist(10, 51))
    assert incomplete.state is S.INCOMPLETE_EVIDENCE
    before, objects = lifecycle_rows(world.database), schema_objects(world.database)
    world.bars({index: QUIET_BAR for index in range(7, 20)})  # the gap, filled late
    world.snapshot(10, 52)
    late = result(world, ist(10, 53))
    assert (late.outcome, late.appended, late.state) == (
        M.NO_NEW_EVENTS,
        (),
        S.INCOMPLETE_EVIDENCE,
    )
    assert lifecycle_rows(world.database) == before and schema_objects(world.database) == objects


# ----------------------------------------------------------------------
# MS-21, MS-27, MS-38, MS-39: stops keep the committed prefix
# ----------------------------------------------------------------------


def test_ms21_ms38_a_later_ei5c_refusal_keeps_the_committed_prefix(
    world: LifecycleWorld, tmp_path: Path
) -> None:
    world.bars({7: ENTRY_BAR})
    world.snapshot(9, 51)
    refused = foreign_currency_snapshot(world, ist(9, 53))  # every catalog column matches
    world.bars({8: TARGET_BAR})
    never = world.snapshot(9, 56)
    spy = SpyRecorder(world.database)
    stopped = result(world, ist(9, 57), spy)
    assert (stopped.outcome, stopped.refusal, stopped.stopped_at) == (
        M.STOPPED_AT_SNAPSHOT_REFUSAL,
        _R.SNAPSHOT_WINDOW_MISMATCH,
        ist(9, 53),
    )
    assert [call[0] for call in spy.calls][-1] == refused and (never,) not in spy.calls
    assert stopped.state is S.ACTIVE and [e.sequence for e in stopped.appended] == [1, 2]
    durable = lifecycle_rows(world.database)
    assert [r[1] for r in durable] == [1, 2]
    restarted = copy_world(world, tmp_path / "restart.sqlite3")  # a restart: the file alone
    again = result(restarted, ist(9, 57))
    assert (again.outcome, again.appended) == (M.STOPPED_AT_SNAPSHOT_REFUSAL, ())
    assert lifecycle_rows(restarted.database) == durable  # no compensating rewrite


def test_ms27_watermark_regression_against_selected_evidence_and_the_anchor(
    world: LifecycleWorld,
) -> None:
    world.bars({7: ENTRY_BAR})
    later = issue(world, ist(9, 56))  # issued first: the lower watermark
    world.bars({1: QUIET_BAR}, listing=INFY)
    issue(world, ist(9, 52))  # issued second: a higher watermark, earlier instant
    stopped = result(world, ist(9, 57))
    assert (stopped.outcome, stopped.stopped_at, stopped.stop_snapshot_ids) == (
        M.STOPPED_AT_REGRESSION,
        ist(9, 56),
        (later,),
    )
    assert stopped.state is S.ACTIVE and _kinds(world) == [
        K.ENTRY_WINDOW_OPENED,
        K.ENTRY_ACTIVATED,
    ]
    rows = lifecycle_rows(world.database)
    again = result(world, ist(9, 58))  # now relative to the anchor's cited snapshot
    assert (again.outcome, again.appended) == (M.STOPPED_AT_REGRESSION, ())
    assert lifecycle_rows(world.database) == rows


def test_ms39_an_unexpected_failure_fails_the_issuance_closed_and_keeps_the_prefix(
    world: LifecycleWorld,
) -> None:
    _progressed(world)
    spy = SpyRecorder(world.database, fail_at=3, failure=RuntimeError("unexpected Core failure"))
    failed = result(world, ist(9, 57), spy)
    assert failed.outcome is M.ISSUANCE_EVALUATION_FAILED and "RuntimeError" in failed.cause
    assert len(spy.calls) == 3 and [e.sequence for e in failed.appended] == [1]
    assert [r[1] for r in lifecycle_rows(world.database)] == [1]
    interrupt = SpyRecorder(world.database, fail_at=1, failure=KeyboardInterrupt())
    with pytest.raises(KeyboardInterrupt):
        result(world, ist(9, 57), interrupt)
    assert [r[1] for r in lifecycle_rows(world.database)] == [1]


# ----------------------------------------------------------------------
# MS-19: commit uncertainty, reconciled by the recorder only
# ----------------------------------------------------------------------


def test_ms19_a_lost_acknowledgement_reconciles_to_equivalent_exactly_once(
    world: LifecycleWorld, monkeypatch: pytest.MonkeyPatch
) -> None:
    real, lost = lifecycle_module._commit, []  # noqa: SLF001

    def commit(connection: sqlite3.Connection) -> None:
        real(connection)
        if not lost:
            lost.append(1)
            raise sqlite3.OperationalError("test: COMMIT acknowledgment lost")

    monkeypatch.setattr(lifecycle_module, "_commit", commit)
    outcome = result(world, ist(9, 47))
    (step,) = outcome.steps
    assert (step.outcome, step.reconciled, outcome.outcome) == (_O.EQUIVALENT, True, M.EQUIVALENT)
    assert [r[1] for r in lifecycle_rows(world.database)] == [1]  # exactly once


def test_ms19_unresolved_commit_uncertainty_is_reported_never_retried_by_the_monitor(
    world: LifecycleWorld, monkeypatch: pytest.MonkeyPatch
) -> None:
    def lost(connection: sqlite3.Connection) -> None:
        raise sqlite3.OperationalError("test: COMMIT lost before it took effect")

    monkeypatch.setattr(lifecycle_module, "_commit", lost)
    spy = SpyRecorder(world.database)
    outcome = result(world, ist(9, 47), spy)
    assert outcome.outcome is M.UNRESOLVED and len(spy.calls) == 1  # no blind retry
    assert lifecycle_rows(world.database) == []  # nothing claimed, nothing written


def test_h2_a_held_write_lock_is_a_typed_retryable_refusal_and_nothing_is_written(
    world: LifecycleWorld, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(lifecycle_module, "_BUSY_TIMEOUT_SECONDS", 0.05)
    holder = raw(world.database)
    try:
        holder.execute("BEGIN IMMEDIATE")  # another writer holds the lock; reads go on
        refused = result(world, ist(9, 47))
        assert (refused.outcome, refused.refusal, refused.retryable) == (
            M.REFUSED,
            _R.STORE_UNAVAILABLE,
            True,
        )
    finally:
        holder.execute("ROLLBACK")
        holder.close()
    assert lifecycle_rows(world.database) == []
    assert result(world, ist(9, 47)).outcome is M.APPENDED


# ----------------------------------------------------------------------
# MS-20, MS-23, G4: isolation between issuances
# ----------------------------------------------------------------------


@pytest.fixture
def two(world: LifecycleWorld) -> Iterator[tuple[LifecycleWorld, str]]:
    """A second MODELED issuance (INFY) in the same file."""
    establish_infy_breakout(world.world)
    other = world.world.issue(world.world.proposed(**infy()))
    assert other.outcome.value == "ISSUED"
    yield world, other.issuance.issuance_id


def test_ms20_ms23_corrupt_lifecycle_authority_is_typed_for_that_issuance_only(
    two: tuple[LifecycleWorld, str],
) -> None:
    world, infy_id = two
    assert result(world, ist(9, 47)).outcome is M.APPENDED
    (document,) = _documents(world)
    bypass(
        world.database,
        (
            f"UPDATE {LIFECYCLE_EVENTS} SET event_document = ? WHERE issuance_id = ?",  # noqa: S608
            (document.replace(",", ", ", 1), world.issuance_id),  # TEST-ONLY: non-canonical
        ),
        drop=(f"{LIFECYCLE_EVENTS}_no_update",),
        checks=False,
    )
    report = run(
        world.database,
        ist(9, 47),
        EquityLifecycleMonitoringExplicitScope((world.issuance_id, infy_id, world.issuance_id)),
    )
    by_id = {r.issuance_id: r for r in report.results}
    assert list(by_id) == sorted({world.issuance_id, infy_id})  # de-duplicated, ordered
    tcs = by_id[world.issuance_id]
    assert (tcs.outcome, tcs.refusal, tcs.retryable) == (
        M.REFUSED,
        _R.STORED_LIFECYCLE_INVALID,
        False,
    )
    assert by_id[infy_id].outcome is M.APPENDED  # the other issuance proceeds


def test_g4_catalog_corruption_of_one_listing_never_suppresses_another(
    two: tuple[LifecycleWorld, str],
) -> None:
    world, infy_id = two
    with closing(raw(world.database)) as connection:
        (infy_snapshot,) = connection.execute(
            f"SELECT snapshot_id FROM {SNAPSHOTS} WHERE symbol = 'INFY' AND as_of = ?",  # noqa: S608
            (ist(9, 46).value,),
        ).fetchone()
    bypass(
        world.database,
        (f"UPDATE {SNAPSHOTS} SET as_of = 'corrupt' WHERE snapshot_id = ?", (infy_snapshot,)),  # noqa: S608
        drop=(f"{SNAPSHOTS}_no_update",),
    )
    report = run(
        world.database,
        ist(9, 47),
        EquityLifecycleMonitoringExplicitScope((world.issuance_id, infy_id, "unknown-issuance")),
    )
    by_id = {r.issuance_id: r for r in report.results}
    assert (by_id[infy_id].outcome, by_id[infy_id].refusal) == (M.REFUSED, _MR.CATALOG_INVALID)
    assert by_id[infy_id].retryable is False
    assert by_id[world.issuance_id].outcome is M.APPENDED
    assert (by_id["unknown-issuance"].outcome, by_id["unknown-issuance"].refusal) == (
        M.REFUSED,
        _R.ISSUANCE_NOT_FOUND,
    )
    assert _documents(world, infy_id) == []


# ----------------------------------------------------------------------
# MS-30: the governed limit through monitoring
# ----------------------------------------------------------------------


@pytest.fixture(scope="module")
def crowded_template(
    template: LifecycleWorld, tmp_path_factory: pytest.TempPathFactory
) -> LifecycleWorld:
    """297 real snapshots at 09:30:00 + k s: with the research ones, 300 eligible at 09:46."""
    crowded = copy_world(template, tmp_path_factory.mktemp("ei6c-crowd") / "crowd.sqlite3")
    for k in range(CROWD):
        issue(crowded, instant(ist(9, 30), k))
    return crowded


def test_ms30_300_eligible_proceed_and_301_refuse_with_no_recorder_call(
    crowded_template: LifecycleWorld, tmp_path: Path
) -> None:
    world = copy_world(crowded_template, tmp_path / "crowd.sqlite3")
    at_limit = result(world, ist(9, 46))  # exactly 300 eligible
    assert at_limit.outcome is M.APPENDED and _kinds(world) == [K.ENTRY_WINDOW_OPENED]
    rows = lifecycle_rows(world.database)
    world.opened()  # the 301st eligible
    spy = SpyRecorder(world.database)
    refused = result(world, ist(9, 47), spy)
    assert refused.outcome is M.CATALOG_LIMIT_EXCEEDED and "301" in refused.cause
    assert spy.calls == [] and lifecycle_rows(world.database) == rows


# ----------------------------------------------------------------------
# MS-25, MS-32, MS-33, MS-31, MS-24
# ----------------------------------------------------------------------


def test_ms25_a_non_synthetic_row_is_never_selected(world: LifecycleWorld) -> None:
    with closing(raw(world.database)) as connection:
        connection.row_factory = sqlite3.Row
        row = dict(
            connection.execute(
                f"SELECT * FROM {SNAPSHOTS} WHERE symbol = 'TCS' AND as_of = ?",  # noqa: S608
                (ist(9, 46).value,),
            ).fetchone()
        )
    clone = {**row, "snapshot_id": "ecs1-sha256:" + "e" * 64, "evidence_class": "PRODUCTION"}
    bypass(
        world.database,
        (
            f"INSERT INTO {SNAPSHOTS} ({', '.join(clone)}) "  # noqa: S608
            f"VALUES ({', '.join('?' * len(clone))})",
            tuple(clone.values()),
        ),
    )  # TEST-ONLY: a PRODUCTION-class row at the same instant
    outcome = result(world, ist(9, 47))
    assert outcome.outcome is M.APPENDED
    assert clone["snapshot_id"] not in outcome.recorded_snapshot_ids


def test_ms32_the_three_scope_modes_against_the_persisted_authority(
    world: LifecycleWorld,
) -> None:
    listing = EquityLifecycleMonitoringStrategyListingScope(world.issuance.strategy, TCS, DAY)
    declared = EquityLifecycleMonitoringDeclaredSessionScope(NSE, DAY)
    # Declared only through b (before the issuance): the session view omits it.
    assert run(world.database, ist(9, 47), declared).results == ()
    found = run(world.database, ist(9, 47), listing).results  # found before any declaration
    assert [(r.issuance_id, r.outcome) for r in found] == [(world.issuance_id, M.APPENDED)]
    assert world.world.declare(ist(9, 44)).outcome.value == "DECLARED"
    (covered,) = run(world.database, ist(9, 48), declared).results
    assert (covered.issuance_id, covered.outcome) == (world.issuance_id, M.NO_ELIGIBLE_SNAPSHOT)
    earlier = EquityLifecycleMonitoringDeclaredSessionScope(NSE, DAY, watermark=1)
    assert run(world.database, ist(9, 48), earlier).results == ()  # the view at watermark 1


def test_ms33_history_recorded_directly_through_ei6b_stays_authoritative(
    world: LifecycleWorld,
) -> None:
    direct = world.record(world.opened())  # EI-6B recorder, before any monitoring
    assert direct.state is S.WAITING_FOR_ENTRY
    recorded = _documents(world)
    world.bars({7: ENTRY_BAR})
    world.snapshot(9, 51)
    continued = result(world, ist(9, 52))
    assert continued.state is S.ACTIVE and _documents(world)[:1] == recorded
    assert continued.diagnostics == ()  # the 09:46 research snapshot was issued earlier


def test_ms31_the_report_names_the_one_fixed_policy(world: LifecycleWorld) -> None:
    report = run(
        world.database, ist(9, 47), EquityLifecycleMonitoringExplicitScope((world.issuance_id,))
    )
    assert report.policy == EQUITY_LIFECYCLE_MONITORING_POLICY
    assert report.policy == "northstar.equity-lifecycle-monitoring-policy/1"


def test_ms24_monitoring_writes_lifecycle_rows_only_through_the_ei6b_append(
    world: LifecycleWorld, monkeypatch: pytest.MonkeyPatch
) -> None:
    _progressed(world)
    real, appends = SQLiteEquityRecommendationLifecycleStore.append, []

    def counting(self: Any, authorization: Any) -> Any:
        appends.append(authorization)
        return real(self, authorization)

    monkeypatch.setattr(SQLiteEquityRecommendationLifecycleStore, "append", counting)
    snapshots, objects = _snapshot_count(world), schema_objects(world.database)
    outcome = result(world, ist(9, 57))
    appended_steps = [s for s in outcome.steps if s.outcome is _O.APPENDED]
    assert len(appends) == len(appended_steps) == 3
    assert _snapshot_count(world) == snapshots  # the catalog and monitor issue nothing
    assert schema_objects(world.database) == objects
