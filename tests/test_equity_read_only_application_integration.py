"""EI-7D: the merged EI-7C query use cases over the real read-only SQLite adapters.

The most direct EI-7D acceptance evidence: the Application's EI-7C use cases,
unchanged, composed here (and nowhere in production code) over the EI-7D
read-only adapters on real persisted synthetic state. It covers, within the
local Application/Infrastructure boundary only, EI7-N05 to N12, N14 to N17
and the local parts of N20 and N21. No API, Web, operator or cross-repository
behaviour is claimed.

Every record is written by the accepted chain (tests/equity_read_only_fixtures.py)
with fixed modeled instants; raw SQL appears only for labelled TEST-ONLY
corruption, deletions and plants of disposable copies -- each the only way to
reach a state the accepted writers and the database guards refuse. Nothing
here opens a network connection, reads market data or touches a production
database.
"""

from __future__ import annotations

import socket
import sqlite3
from collections.abc import Callable, Iterator
from contextlib import closing
from datetime import date
from pathlib import Path
from typing import Any

import pytest
from northstar_application.application_services import (
    AdvanceEquityIntradayResearchTimelineUseCase,
    EquityEvidencePanelReason,
    EquityEvidencePanelStatus,
    EquityEvidenceRole,
    EquityGenerationNotQueryableReason,
    EquityLifecycleSummaryStatus,
    EquityReadQueryRefusal,
    EquityReadQueryRefusedError,
    EquityResearchGenerationStatus,
    EquityResearchIssuanceStatus,
    EquityResearchTimelineStatus,
    EquitySessionDeclarationState,
    GetEquityLifecycleSummaryUseCase,
    GetEquityRecommendationDetailUseCase,
    GetEquityResearchTimelineUseCase,
    ListEquityDeclaredSessionRecommendationsUseCase,
    ListEquityRecordedSessionIssuancesUseCase,
)
from northstar_core.domain.value_objects import ListingReference
from northstar_core.foundation.value_objects import Symbol
from northstar_core.recommendations.equity.lifecycle import (
    EquityLifecycleOutcomeClass,
    EquityLifecycleState,
)

from northstar_infrastructure.persistence import (
    SQLiteEquityCandleEvidenceSnapshotStore,
    SQLiteEquityIntradayResearchTimelineStore,
)
from northstar_infrastructure.persistence import sqlite_equity_read_only as connection_module
from northstar_infrastructure.persistence.sqlite_equity_intraday_research_timeline import (
    _timeline_values,
)
from tests.equity_lifecycle_monitoring_fixtures import copy_world
from tests.equity_read_only_fixtures import (
    LATER_DECLARATION,
    SessionWorld,
    base_world,
    copy_session,
    corrupt,
    file_state,
    plant_production_snapshot,
    readers,
    research_snapshot_ids,
    schema_state,
    session_world,
)
from tests.equity_recommendation_acceptance_fixtures import (
    CONFIG,
    DAY,
    DISPOSITIONS,
    ENTRIES,
    INFY,
    NSE,
    SNAPSHOTS,
    TCS,
    TIMELINES,
    ist,
    raw,
)
from tests.equity_recommendation_issuance_fixtures import ISSUANCES, B
from tests.equity_recommendation_lifecycle_fixtures import LifecycleWorld

S = EquityLifecycleState
_R = EquityReadQueryRefusal
_P = EquityEvidencePanelStatus
_REASON = EquityEvidencePanelReason
LIFECYCLE_EVENTS = "equity_recommendation_lifecycle_events"
WIPRO = ListingReference(Symbol("WIPRO"), NSE)
_UNKNOWN_ISSUANCE = "eiris1-sha256:" + "0" * 64


@pytest.fixture(autouse=True)
def _refuse_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("EI-7D tests must not open network connections.")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)


@pytest.fixture(scope="module")
def session_template(tmp_path_factory: pytest.TempPathFactory) -> SessionWorld:
    return session_world(tmp_path_factory.mktemp("ei7d-integration") / "template.sqlite3")


@pytest.fixture
def session(session_template: SessionWorld, tmp_path: Path) -> SessionWorld:
    return copy_session(session_template, tmp_path / "session.sqlite3")


@pytest.fixture(scope="module")
def base_template(tmp_path_factory: pytest.TempPathFactory) -> LifecycleWorld:
    return base_world(tmp_path_factory.mktemp("ei7d-integration-base") / "base.sqlite3")


@pytest.fixture
def base(base_template: LifecycleWorld, tmp_path: Path) -> LifecycleWorld:
    return copy_world(base_template, tmp_path / "base.sqlite3")


class Queries:
    """The EI-7C use cases, each composed over the EI-7D read-only adapters of one file."""

    def __init__(self, database: Path, snapshots: Any = None) -> None:
        r = readers(database)
        snapshot_reader = snapshots if snapshots is not None else r["snapshots"]
        self.recorded = ListEquityRecordedSessionIssuancesUseCase(r["sessions"])
        self.declared = ListEquityDeclaredSessionRecommendationsUseCase(r["sessions"])
        self.detail = GetEquityRecommendationDetailUseCase(
            r["lifecycles"], r["generations"], snapshot_reader
        )
        self.summary = GetEquityLifecycleSummaryUseCase(r["lifecycles"])
        self.research = GetEquityResearchTimelineUseCase(
            r["timelines"], snapshot_reader, r["issuances"], r["generations"]
        )


def _refused(call: Callable[[], Any]) -> EquityReadQueryRefusal:
    with pytest.raises(EquityReadQueryRefusedError) as raised:
        call()
    return raised.value.reason


def _panels(view: Any) -> dict[str, tuple[Any, Any, Any]]:
    return {p.snapshot_id: (p.status, p.reason, p.roles) for p in view.evidence}


def _cited(database: Path, issuance_id: str) -> list[str]:
    with closing(raw(database)) as connection:
        return [
            r[0]
            for r in connection.execute(
                f"SELECT evidence_reference FROM {LIFECYCLE_EVENTS} "  # noqa: S608
                "WHERE issuance_id = ? ORDER BY event_sequence",
                (issuance_id,),
            )
        ]


def _delete_snapshot(database: Path, snapshot_id: str) -> None:
    """TEST-ONLY: a cited snapshot removed from a disposable copy (only by bypassing guards)."""
    corrupt(
        database,
        "DELETE FROM equity_candle_evidence_snapshot_entries WHERE snapshot_id = ?",
        (snapshot_id,),
        drop=("equity_candle_evidence_snapshot_entries_no_delete",),
    )
    corrupt(
        database,
        f"DELETE FROM {SNAPSHOTS} WHERE snapshot_id = ?",  # noqa: S608
        (snapshot_id,),
        drop=(f"{SNAPSHOTS}_no_delete",),
    )


def _corrupt_snapshot(database: Path, snapshot_id: str) -> None:
    """TEST-ONLY: one stored snapshot column disagrees with its document."""
    corrupt(
        database,
        f"UPDATE {SNAPSHOTS} SET watermark = watermark + 1 WHERE snapshot_id = ?",  # noqa: S608
        (snapshot_id,),
        drop=(f"{SNAPSHOTS}_no_update",),
        checks=False,
    )


@pytest.fixture
def statements(monkeypatch: pytest.MonkeyPatch) -> Iterator[list[str]]:
    """Every SQL statement the EI-7D reader connections execute after confirming query_only."""
    real, traced = connection_module.connect_read_only, []

    def connect(database_path: Path) -> sqlite3.Connection:
        connection = real(database_path)
        connection.set_trace_callback(traced.append)
        return connection

    monkeypatch.setattr(connection_module, "connect_read_only", connect)
    yield traced


# ======================================================================
# Session discovery: recorded and declared, W and N (N07, N15, N16)
# ======================================================================


def test_stored_issuances_are_discovered_in_recording_order_with_n_and_w(
    session: SessionWorld,
) -> None:
    view = Queries(session.database).recorded.execute(NSE, DAY)
    assert (view.recording_position, view.recording_watermark) == (5, 5)
    assert view.declaration is EquitySessionDeclarationState.DECLARED
    assert view.complete_through == LATER_DECLARATION
    assert [i.issuance for i in view.items] == list(session.issuances)
    assert all(i.covered_by_declaration for i in view.items)


def test_recorded_and_declared_orders_remain_distinct(session: SessionWorld) -> None:
    q = Queries(session.database)
    recorded = [i.issuance.recommendation.issuance_id for i in q.recorded.execute(NSE, DAY).items]
    declared = [r.recommendation.issuance_id for r in q.declared.execute(NSE, DAY).items]
    assert sorted(recorded) == sorted(declared) and recorded != declared
    assert declared == sorted(declared)  # one issued instant: EI-6A orders by issuance_id


def test_n07_recorded_before_the_declaration_covers_it(session: SessionWorld) -> None:
    q = Queries(session.database)
    recorded = q.recorded.execute(NSE, DAY, 4)
    assert recorded.complete_through == B and len(recorded.items) == 3
    assert not any(i.covered_by_declaration for i in recorded.items)
    declared = q.declared.execute(NSE, DAY, 4)
    assert declared.declaration is EquitySessionDeclarationState.DECLARED
    assert declared.items == () and declared.history is not None


def test_an_undeclared_session_is_represented_without_none(session: SessionWorld) -> None:
    q = Queries(session.database)
    for view in (q.recorded.execute(NSE, date(2026, 8, 20)), q.declared.execute(NSE, DAY, 0)):
        assert view.declaration is EquitySessionDeclarationState.NONE
        assert view.complete_through is None and view.items == ()


def test_a_historical_watermark_is_replayed_exactly_after_later_commits(
    session: SessionWorld,
) -> None:
    q = Queries(session.database)
    at_three = (q.recorded.execute(NSE, DAY, 3), q.declared.execute(NSE, DAY, 3))
    assert session.world.world.declare(ist(9, 45)).outcome.value == "DECLARED"  # sequence 6
    again = (q.recorded.execute(NSE, DAY, 3), q.declared.execute(NSE, DAY, 3))
    for before, after in zip(at_three, again, strict=True):
        assert after.recording_position == 6 and before.recording_position == 5
        assert (after.recording_watermark, after.items, after.complete_through) == (
            before.recording_watermark,
            before.items,
            before.complete_through,
        )
    assert q.recorded.execute(NSE, DAY).recording_watermark == 6


@pytest.mark.parametrize(
    ("watermark", "reason"),
    [
        (6, _R.WATERMARK_AHEAD),
        (2**63 - 1, _R.WATERMARK_AHEAD),
        (2**63, _R.WATERMARK_AHEAD),
        (-1, _R.UNSUPPORTED_SCOPE),
    ],
)
def test_n15_watermark_errors(session: SessionWorld, watermark: int, reason: Any) -> None:
    q = Queries(session.database)
    for use_case in (q.recorded, q.declared):
        assert _refused(lambda u=use_case: u.execute(NSE, DAY, watermark)) is reason


# ======================================================================
# Recommendation detail (N05, N06, N08, N09, N10, N11, N12)
# ======================================================================


def test_verified_detail_states_the_issuance_lineage_lifecycle_and_evidence(
    session: SessionWorld,
) -> None:
    recorded = session.terminal
    view = Queries(session.database).detail.execute(recorded.recommendation.issuance_id)
    assert view.issuance == recorded
    assert view.lineage.issuance == recorded
    assert view.lineage.generation_key == recorded.recommendation.generation_key
    assert view.lifecycle.state is S.CLOSED_TARGET
    assert [e.sequence for e in view.lifecycle.events] == [1, 2, 3]
    candidate = research_snapshot_ids(session.database)[1]
    cited = _cited(session.database, recorded.recommendation.issuance_id)
    panels = _panels(view)
    assert set(panels) == {candidate, *cited}
    assert panels[candidate] == (_P.AVAILABLE, None, (EquityEvidenceRole.CANDIDATE,))
    for snapshot_id in cited:
        assert panels[snapshot_id] == (_P.AVAILABLE, None, (EquityEvidenceRole.LIFECYCLE_EVENT,))


def test_n06_an_issuance_without_lifecycle_events(session: SessionWorld) -> None:
    view = Queries(session.database).detail.execute(session.quiet.recommendation.issuance_id)
    assert view.lifecycle.state is S.RECORDED and view.lifecycle.events == ()
    assert [p.roles for p in view.evidence] == [(EquityEvidenceRole.CANDIDATE,)]


def test_n08_active_and_terminal_modeled_lifecycles_and_independent_summaries(
    session: SessionWorld,
) -> None:
    q = Queries(session.database)
    expected = {
        session.terminal: S.CLOSED_TARGET,
        session.active: S.ACTIVE,
        session.quiet: S.RECORDED,
    }
    for recorded, state in expected.items():
        identity = recorded.recommendation.issuance_id
        assert q.detail.execute(identity).lifecycle.state is state
        summary = q.summary.execute(identity)
        assert (summary.status, summary.state, summary.is_terminal) == (
            EquityLifecycleSummaryStatus.AVAILABLE,
            state,
            state.is_terminal,
        )


def test_n09_terminal_incomplete_evidence(base: LifecycleWorld) -> None:
    snapshot = base.snapshot(10, 51)
    assert base.record(snapshot).state is S.INCOMPLETE_EVIDENCE
    view = Queries(base.database).detail.execute(base.issuance_id)
    assert view.lifecycle.state is S.INCOMPLETE_EVIDENCE and view.lifecycle.state.is_terminal
    assert view.lifecycle.state.outcome_class is EquityLifecycleOutcomeClass.UNRESOLVED
    last = view.lifecycle.events[-1]
    assert last.kind.value == "EVIDENCE_INCOMPLETE" and last.deficiencies  # listed
    assert not any(hasattr(e, name) for e in view.lifecycle.events for name in ("price", "exit"))
    assert _panels(view)[snapshot][0] is _P.AVAILABLE


def test_n05_an_unknown_issuance_is_not_found(session: SessionWorld) -> None:
    q = Queries(session.database)
    assert _refused(lambda: q.detail.execute(_UNKNOWN_ISSUANCE)) is _R.ISSUANCE_NOT_FOUND
    assert _refused(lambda: q.summary.execute(_UNKNOWN_ISSUANCE)) is _R.ISSUANCE_NOT_FOUND


def test_n10_a_missing_cited_snapshot_degrades_only_its_panel(session: SessionWorld) -> None:
    identity = session.terminal.recommendation.issuance_id
    missing = _cited(session.database, identity)[0]
    _delete_snapshot(session.database, missing)
    view = Queries(session.database).detail.execute(identity)
    assert _panels(view)[missing][:2] == (_P.UNAVAILABLE, _REASON.EVIDENCE_NOT_FOUND)
    assert view.lifecycle.state is S.CLOSED_TARGET  # required provenance intact


def test_n10_a_missing_candidate_snapshot_is_a_broken_reference(session: SessionWorld) -> None:
    candidate = research_snapshot_ids(session.database)[1]
    _delete_snapshot(session.database, candidate)
    view = Queries(session.database).detail.execute(session.quiet.recommendation.issuance_id)
    assert _panels(view)[candidate][:2] == (_P.INVALID, _REASON.EVIDENCE_REFERENCE_BROKEN)


def test_corrupt_optional_evidence_is_invalid_with_the_new_reader_not_unavailable(
    session: SessionWorld,
) -> None:
    identity = session.terminal.recommendation.issuance_id
    target = _cited(session.database, identity)[1]
    _corrupt_snapshot(session.database, target)
    view = Queries(session.database).detail.execute(identity)
    assert _panels(view)[target][:2] == (_P.INVALID, _REASON.EVIDENCE_INVALID)
    # The EI-7C limitation, closed: the old EI-2B adapter reports the same row as an outage.
    legacy = SQLiteEquityCandleEvidenceSnapshotStore(session.database)
    old = Queries(session.database, snapshots=legacy).detail.execute(identity)
    assert _panels(old)[target][:2] == (_P.UNAVAILABLE, _REASON.DEPENDENCY_UNAVAILABLE)


@pytest.mark.parametrize("corruption", ["issuance", "disposition", "lifecycle"])
def test_n11_corrupt_required_authority_fails_the_whole_detail(
    session: SessionWorld, corruption: str
) -> None:
    identity = session.terminal.recommendation.issuance_id
    if corruption == "issuance":
        corrupt(
            session.database,
            f"UPDATE {ISSUANCES} SET issued_at_us = issued_at_us + 1 WHERE issuance_id = ?",  # noqa: S608
            (identity,),
            drop=(f"{ISSUANCES}_no_update",),
        )
        expected = _R.PERSISTED_AUTHORITY_INVALID
    elif corruption == "disposition":
        corrupt(
            session.database,
            f"UPDATE {DISPOSITIONS} SET symbol = 'INFY' WHERE generation_key = ?",  # noqa: S608
            (session.terminal.recommendation.generation_key,),
            drop=(f"{DISPOSITIONS}_no_update",),
            checks=False,
        )
        expected = _R.PERSISTED_AUTHORITY_INVALID
    else:
        corrupt(
            session.database,
            f"UPDATE {LIFECYCLE_EVENTS} SET evaluated_at_us = 1 "  # noqa: S608
            "WHERE issuance_id = ? AND event_sequence = 2",
            (identity,),
            drop=(f"{LIFECYCLE_EVENTS}_no_update",),
            checks=False,
        )
        expected = _R.LIFECYCLE_HISTORY_INVALID
    q = Queries(session.database)
    assert _refused(lambda: q.detail.execute(identity)) is expected
    if corruption == "lifecycle":
        assert q.summary.execute(identity).status is EquityLifecycleSummaryStatus.INVALID


def test_n11_corrupt_authority_fails_both_session_projections(session: SessionWorld) -> None:
    corrupt(
        session.database,
        f"UPDATE {ISSUANCES} SET symbol = 'INFY' WHERE recording_sequence = 3",  # noqa: S608
        drop=(f"{ISSUANCES}_no_update",),
        checks=False,
    )
    q = Queries(session.database)
    for use_case in (q.recorded, q.declared):
        assert _refused(lambda u=use_case: u.execute(NSE, DAY)) is _R.PERSISTED_AUTHORITY_INVALID


def test_n12_dependency_unavailable_is_retryable_on_every_query(tmp_path: Path) -> None:
    q = Queries(tmp_path / "absent.sqlite3")
    for call in (
        lambda: q.recorded.execute(NSE, DAY),
        lambda: q.declared.execute(NSE, DAY),
        lambda: q.detail.execute(_UNKNOWN_ISSUANCE),
        lambda: q.research.execute(CONFIG.strategy, TCS, DAY),
    ):
        with pytest.raises(EquityReadQueryRefusedError) as raised:
            call()
        assert raised.value.reason is _R.DEPENDENCY_UNAVAILABLE and raised.value.retryable
    summary = q.summary.execute(_UNKNOWN_ISSUANCE)
    assert summary.status is EquityLifecycleSummaryStatus.UNAVAILABLE
    assert list(tmp_path.iterdir()) == []


def test_n12_unavailable_evidence_degrades_only_the_panels(session: SessionWorld) -> None:
    absent = readers(session.database.with_name("absent.sqlite3"))["snapshots"]
    view = Queries(session.database, snapshots=absent).detail.execute(
        session.terminal.recommendation.issuance_id
    )
    assert {p.reason for p in view.evidence} == {_REASON.DEPENDENCY_UNAVAILABLE}
    assert view.lifecycle.state is S.CLOSED_TARGET


def test_n21_a_list_then_a_detail_may_differ_by_appended_events_and_the_prefix_is_stable(
    session: SessionWorld,
) -> None:
    """Separate queries are separate transactions: an issuance W never freezes lifecycles."""
    q = Queries(session.database)
    identity = session.active.recommendation.issuance_id
    listed = q.recorded.execute(NSE, DAY, 5)
    before = q.detail.execute(identity).lifecycle.events
    targeted = _cited(session.database, session.terminal.recommendation.issuance_id)[2]
    appended = session.world.record(targeted, issuance_id=identity)  # the accepted EI-6B path
    assert appended.outcome.value == "APPENDED"
    after = q.detail.execute(identity).lifecycle
    assert after.events[: len(before)] == before and len(after.events) > len(before)
    assert after.state is S.CLOSED_TARGET
    assert q.recorded.execute(NSE, DAY, 5) == listed  # the session list at W is unchanged


# ======================================================================
# Research timelines (FC2, FC3)
# ======================================================================


def test_a_research_timeline_is_rehydrated_with_its_issued_lineage(session: SessionWorld) -> None:
    view = Queries(session.database).research.execute(CONFIG.strategy, TCS, DAY)
    assert view.timeline_status is EquityResearchTimelineStatus.STORED
    assert view.configuration == CONFIG and len(view.entries) == 3
    assert view.generation_status is EquityResearchGenerationStatus.PROPOSED
    assert view.issuance_status is EquityResearchIssuanceStatus.ISSUED
    assert view.issuance == session.terminal and view.lineage is not None


def test_missing_generation_identity_remains_not_queryable(session: SessionWorld) -> None:
    q = Queries(session.database)
    infy = q.research.execute(CONFIG.strategy, INFY, DAY)  # a CANDIDATE, never issued
    assert infy.timeline_status is EquityResearchTimelineStatus.STORED
    assert infy.generation_status is EquityResearchGenerationStatus.NOT_QUERYABLE
    assert infy.not_queryable_reason is EquityGenerationNotQueryableReason.NO_RECORDED_ISSUANCE
    assert infy.lineage is None and infy.issuance is None
    unknown = q.research.execute(CONFIG.strategy, WIPRO, DAY)
    assert unknown.timeline_status is EquityResearchTimelineStatus.UNKNOWN
    assert unknown.not_queryable_reason is EquityGenerationNotQueryableReason.NO_RESEARCH_TIMELINE


def test_a_stored_decision_that_no_longer_reproduces_fails_the_research_query(
    session: SessionWorld,
) -> None:
    with closing(raw(session.database)) as connection:
        (timeline,) = connection.execute(
            f"SELECT timeline_id FROM {TIMELINES} WHERE strategy = ? AND symbol = 'INFY'",  # noqa: S608
            (CONFIG.strategy.identity,),
        ).fetchone()
    corrupt(
        session.database,
        f"UPDATE {ENTRIES} SET decision_fingerprint = ? WHERE timeline_id = ? AND sequence = 1",  # noqa: S608
        ("eir1-sha256:" + "0" * 64, timeline),
        drop=(f"{ENTRIES}_no_update",),
    )
    q = Queries(session.database)
    assert _refused(lambda: q.research.execute(CONFIG.strategy, INFY, DAY)) is (
        _R.PERSISTED_AUTHORITY_INVALID
    )


def test_a_structurally_corrupt_timeline_is_invalid_with_the_new_reader(
    session: SessionWorld,
) -> None:
    with closing(raw(session.database)) as connection:
        (timeline,) = connection.execute(
            f"SELECT timeline_id FROM {TIMELINES} WHERE strategy = ? AND symbol = 'INFY'",  # noqa: S608
            (CONFIG.strategy.identity,),
        ).fetchone()
    corrupt(
        session.database,
        f"UPDATE {ENTRIES} SET role = 'NOT_A_ROLE' WHERE timeline_id = ? AND sequence = 1",  # noqa: S608
        (timeline,),
        drop=(f"{ENTRIES}_no_update",),
        checks=False,
    )
    q = Queries(session.database)
    assert _refused(lambda: q.research.execute(CONFIG.strategy, INFY, DAY)) is (
        _R.PERSISTED_AUTHORITY_INVALID
    )
    # The old EI-3C adapter would make the same row an outage (DEPENDENCY_UNAVAILABLE).
    r = readers(session.database)
    old = GetEquityResearchTimelineUseCase(
        SQLiteEquityIntradayResearchTimelineStore(session.database),
        r["snapshots"],
        r["issuances"],
        r["generations"],
    )
    assert _refused(lambda: old.execute(CONFIG.strategy, INFY, DAY)) is _R.DEPENDENCY_UNAVAILABLE


def test_a_corrupt_research_snapshot_fails_the_research_query_as_invalid(
    session: SessionWorld,
) -> None:
    _corrupt_snapshot(session.database, research_snapshot_ids(session.database)[0])
    q = Queries(session.database)
    assert _refused(lambda: q.research.execute(CONFIG.strategy, TCS, DAY)) is (
        _R.PERSISTED_AUTHORITY_INVALID
    )


# ======================================================================
# N14: non-SYNTHETIC evidence never escapes into a synthetic view
# ======================================================================


def test_n14_the_accepted_chain_and_the_database_never_admit_a_production_research_entry(
    base: LifecycleWorld,
) -> None:
    production = plant_production_snapshot(base)
    evidence = base.evidence()
    advanced = AdvanceEquityIntradayResearchTimelineUseCase(
        evidence.timelines, evidence.snapshots, CONFIG
    ).execute(INFY, DAY, [production.snapshot_id])
    assert advanced.status.value == "NOTHING_TO_COMMIT"  # the accepted EI-3C path refuses it
    assert Queries(base.database).research.execute(CONFIG.strategy, INFY, DAY).timeline_status is (
        EquityResearchTimelineStatus.UNKNOWN
    )


def _plant_production_research(base: LifecycleWorld) -> str:
    """TEST-ONLY: an INFY timeline citing a PRODUCTION snapshot, guards dropped to plant it.

    The EI-3C trigger ``..._snapshot_matches`` refuses any entry whose snapshot is
    not SYNTHETIC, so this state exists only by bypassing the database guards.
    """
    production = plant_production_snapshot(base)
    stored = SQLiteEquityIntradayResearchTimelineStore(base.database).load(
        CONFIG.strategy, TCS, DAY
    )
    assert stored is not None
    first = stored.entries[0]
    from dataclasses import replace

    entry = replace(
        first,
        snapshot_id=production.snapshot_id,
        manifest_fingerprint=production.manifest_fingerprint,
        watermark=production.watermark,
        previous_fingerprint=None,
        entry_fingerprint="eirt1-sha256:" + "e" * 64,  # entry fingerprints are unique per file
    )
    timeline = replace(stored, listing=INFY, entries=(entry,))
    values = _timeline_values(timeline)
    with closing(raw(base.database)) as connection:
        cursor = connection.execute(
            f"INSERT INTO {TIMELINES} (strategy, symbol, exchange_code, trading_date, "  # noqa: S608
            "configuration_version, configuration_document, identity_document) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            values,
        )
        timeline_id = cursor.lastrowid
        connection.execute(f"DROP TRIGGER {ENTRIES}_decision_matches")
        # Same instant, watermark, manifest, listing and window: only the class differs.
        with pytest.raises(sqlite3.IntegrityError, match="matching synthetic snapshot"):
            _insert_entry(connection, timeline_id, entry)
        connection.execute(f"DROP TRIGGER {ENTRIES}_snapshot_matches")
        _insert_entry(connection, timeline_id, entry)
    return production.snapshot_id


def _insert_entry(connection: sqlite3.Connection, timeline_id: Any, entry: Any) -> None:
    from northstar_infrastructure.persistence.sqlite_equity_intraday_research_timeline import (
        _INSERT_ENTRY,
        _entry_values,
    )

    connection.execute(_INSERT_ENTRY, _entry_values(timeline_id, entry))


def test_n14_a_production_snapshot_refuses_the_research_query_before_any_outcome(
    base: LifecycleWorld,
) -> None:
    planted = _plant_production_research(base)
    stored = readers(base.database)["timelines"].load(CONFIG.strategy, INFY, DAY)
    assert stored is not None and stored.entries[0].snapshot_id == planted  # read as stored
    q = Queries(base.database)
    assert _refused(lambda: q.research.execute(CONFIG.strategy, INFY, DAY)) is (
        _R.EVIDENCE_CLASS_UNSUPPORTED
    )


def test_n14_a_production_snapshot_cited_by_a_lifecycle_refuses_the_detail(
    base: LifecycleWorld,
) -> None:
    opened = base.opened()
    assert base.record(opened).outcome.value == "APPENDED"
    production = plant_production_snapshot(base)
    assert len(production.snapshot_id) == len(opened)
    corrupt(  # TEST-ONLY: the recorded event re-pointed at the PRODUCTION snapshot
        base.database,
        f"UPDATE {LIFECYCLE_EVENTS} SET evidence_reference = ?, "  # noqa: S608
        "event_document = replace(event_document, ?, ?) WHERE issuance_id = ?",
        (production.snapshot_id, opened, production.snapshot_id, base.issuance_id),
        drop=(f"{LIFECYCLE_EVENTS}_no_update",),
    )
    q = Queries(base.database)
    assert readers(base.database)["lifecycles"].load_lifecycle(base.issuance_id) is not None
    assert _refused(lambda: q.detail.execute(base.issuance_id)) is _R.EVIDENCE_CLASS_UNSUPPORTED


# ======================================================================
# N17: no read writes, initializes, issues, generates, declares or monitors
# ======================================================================


def _counts(database: Path) -> dict[str, int]:
    with closing(raw(database)) as connection:
        return {
            table: connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]  # noqa: S608
            for (table,) in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'equity_%'"
            ).fetchall()
        }


def test_n17_every_query_leaves_the_database_unchanged_and_executes_only_reads(
    session: SessionWorld, statements: list[str]
) -> None:
    before = (file_state(session.database), schema_state(session.database))
    counts = _counts(session.database)
    q = Queries(session.database)
    for watermark in (None, 0, 3, 5):
        q.recorded.execute(NSE, DAY, watermark)
        q.declared.execute(NSE, DAY, watermark)
    for recorded in session.issuances:
        q.detail.execute(recorded.recommendation.issuance_id)
        q.summary.execute(recorded.recommendation.issuance_id)
    for listing in (TCS, INFY, WIPRO):
        q.research.execute(CONFIG.strategy, listing, DAY)
    assert (file_state(session.database), schema_state(session.database)) == before
    assert _counts(session.database) == counts  # nothing issued, generated, declared or appended
    assert statements and set(statements) >= {"BEGIN", "ROLLBACK"}
    for statement in statements:
        assert statement in ("BEGIN", "ROLLBACK") or statement.lstrip().upper().startswith(
            "SELECT"
        ), statement
