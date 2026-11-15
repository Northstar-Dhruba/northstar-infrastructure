"""EI-6C: lifecycle monitoring across separate OS processes, on real SQLite.

Acceptance of the EI-6C Lifecycle Monitoring and Snapshot Selection Design
v1.0 (section 12) at layer P: MS-16, MS-17, MS-18 and the P parts of MS-22,
with read consistency of the SQLite catalog under a concurrent writer.

Every worker is an independent interpreter -- the ``spawn`` start method on
every platform, or ``subprocess`` with the test's own interpreter -- that
composes the merged monitor, the SQLite catalog and the EI-6B store from the
database path alone; nothing is shared with the parent but the file.
Synchronization is by barriers and queues with bounded timeouts; no sleep is
an oracle. Nothing here opens a network connection or touches a production
database.
"""

from __future__ import annotations

import json
import multiprocessing
import os
import socket
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
from northstar_application.application_services import (
    EquityLifecycleMonitoringOutcome,
    decode_equity_lifecycle_event_document,
)
from northstar_core.foundation.value_objects import PointInTime
from northstar_core.recommendations.equity.lifecycle import EquityLifecycleState

from northstar_infrastructure.persistence import (
    SQLiteEquityCandleEvidenceSnapshotCatalog,
    SQLiteEquityRecommendationLifecycleStore,
)
from tests.equity_lifecycle_monitoring_fixtures import (
    CATALOG_SCOPE,
    copy_world,
    issue,
    lifecycle_rows,
    monitor_in_another_process,
    monitor_then_exit_before_commit,
    read_repeatedly,
    result,
    summary,
)
from tests.equity_recommendation_acceptance_fixtures import check_database, ist
from tests.equity_recommendation_lifecycle_fixtures import (
    ENTRY_BAR,
    TARGET_BAR,
    LifecycleWorld,
    lifecycle_world,
)

M = EquityLifecycleMonitoringOutcome
S = EquityLifecycleState
_INFRA_ROOT = Path(__file__).resolve().parents[1]
_TIMEOUT = 240
# MS-16: what a concurrent caller may legitimately observe (design section 6.3).
_ADMISSIBLE = {"APPENDED", "EQUIVALENT", "NO_NEW_EVENTS", "CONFLICT", "REFUSED", "UNRESOLVED"}


@pytest.fixture(autouse=True)
def _refuse_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("EI-6C tests must not open network connections.")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)


@pytest.fixture(scope="module")
def template(tmp_path_factory: pytest.TempPathFactory) -> LifecycleWorld:
    world = lifecycle_world(tmp_path_factory.mktemp("ei6c-processes") / "template.sqlite3")
    assert world.store().load_lifecycle(world.issuance_id) is not None  # lifecycle objects added
    return world


@pytest.fixture
def world(template: LifecycleWorld, tmp_path: Path) -> LifecycleWorld:
    return copy_world(template, tmp_path / "ei6c.sqlite3")


def _stored(world: LifecycleWorld) -> list[str]:
    return [row[-1] for row in lifecycle_rows(world.database)]


def _replays(world: LifecycleWorld) -> Any:
    """The authoritative history, re-derived and replayed by Core through the EI-6B store."""
    loaded = SQLiteEquityRecommendationLifecycleStore(world.database).load_lifecycle(
        world.issuance_id
    )
    assert loaded is not None
    sequences = [e.sequence for e in loaded.lifecycle.events]
    assert sequences == list(range(1, len(sequences) + 1))  # no duplicate, no gap
    return loaded.lifecycle


def _race(*workers: tuple[Path, dict[str, Any], str]) -> dict[str, dict[str, Any]]:
    """Run each (database, request, name) in a spawned monitor; first appends released together."""
    context = multiprocessing.get_context("spawn")  # the Windows start method, everywhere
    start, results = context.Barrier(len(workers)), context.Queue()
    processes = [
        context.Process(
            target=monitor_in_another_process,
            args=(str(database), json.dumps(request), start, results, name),
        )
        for database, request, name in workers
    ]
    for process in processes:
        process.start()
    reported: dict[str, tuple[int, Any, Any]] = {}
    try:
        for _ in processes:
            name, pid, report, error = results.get(timeout=_TIMEOUT)
            reported[name] = (pid, report, error)
    finally:
        for process in processes:
            process.join(timeout=60)
            if process.is_alive():  # pragma: no cover -- diagnostic cleanup only
                process.terminate()
                process.join(timeout=10)
    assert all(p.exitcode == 0 for p in processes), [p.exitcode for p in processes]
    assert all(error is None for *_, error in reported.values()), reported
    assert len({pid for pid, *_ in reported.values()} | {os.getpid()}) == len(workers) + 1
    return {name: report for name, (_, report, _) in reported.items()}


# ----------------------------------------------------------------------
# MS-16, MS-17: concurrent monitors
# ----------------------------------------------------------------------


def test_ms16_two_monitors_over_the_same_evidence_commit_one_history(
    world: LifecycleWorld,
) -> None:
    world.opened()
    world.activated()
    request = {"issuance_id": world.issuance_id, "monitoring_instant": ist(9, 52).value}
    reported = _race((world.database, request, "first"), (world.database, request, "second"))
    assert {r["outcome"] for r in reported.values()} <= _ADMISSIBLE
    lifecycle = _replays(world)  # one Core-valid history
    assert lifecycle.state is S.ACTIVE and len(lifecycle.events) == 2
    stored = _stored(world)
    appended = [d for r in reported.values() for d in r["appended"]]
    assert sorted(appended) == sorted(stored)  # every stored event was appended exactly once
    assert any(r["outcome"] == "APPENDED" for r in reported.values())
    check_database(world.database)


def test_ms17_divergent_monitors_with_different_instants_and_views_first_commit_wins(
    world: LifecycleWorld,
) -> None:
    world.record(world.opened())  # the common prior history: ENTRY_WINDOW_OPENED
    world.bars({7: ENTRY_BAR, 8: TARGET_BAR})
    issue(world, ist(9, 51))  # both views hold it
    issue(world, ist(9, 50, 30), ist(9, 57))  # only the 09:57 view holds it, and it is earlier
    a = {"issuance_id": world.issuance_id, "monitoring_instant": ist(9, 52).value}
    b = {"issuance_id": world.issuance_id, "monitoring_instant": ist(9, 57).value}
    reported = _race((world.database, a, "a"), (world.database, b, "b"))
    outcomes = {name: report["outcome"] for name, report in reported.items()}
    assert sorted(outcomes.values()) == ["APPENDED", "CONFLICT"], reported
    winner = next(name for name, outcome in outcomes.items() if outcome == "APPENDED")
    loser = next(name for name in outcomes if name != winner)
    assert reported[loser]["appended"] == []  # the loser wrote nothing
    stored = _stored(world)
    assert stored[1:] == reported[winner]["appended"]  # the first commit is the history
    activation = decode_equity_lifecycle_event_document(stored[1])
    expected = ist(9, 51) if winner == "a" else ist(9, 50, 30)
    assert activation.evaluated_at == expected  # no scheduling-independent winner assumed
    _replays(world)
    check_database(world.database)


# ----------------------------------------------------------------------
# MS-18: a process that ends before COMMIT
# ----------------------------------------------------------------------


def test_ms18_a_monitor_process_ending_before_commit_leaves_nothing_and_a_later_run_completes(
    world: LifecycleWorld,
) -> None:
    world.opened()
    payload = json.dumps({"issuance_id": world.issuance_id, "monitoring_instant": ist(9, 47).value})
    context = multiprocessing.get_context("spawn")
    process = context.Process(
        target=monitor_then_exit_before_commit, args=(str(world.database), payload)
    )
    process.start()
    process.join(timeout=_TIMEOUT)
    assert process.exitcode == 17  # it reached the COMMIT seam with the batch inserted
    assert lifecycle_rows(world.database) == []
    check_database(world.database)
    completed = result(world, ist(9, 47))
    assert completed.outcome is M.APPENDED and [r[1] for r in lifecycle_rows(world.database)] == [1]


# ----------------------------------------------------------------------
# MS-22, H1: a fresh operating-system process
# ----------------------------------------------------------------------

_FRESH_PROCESS = r"""
import json, os, sys
from pathlib import Path
from northstar_core.foundation.value_objects import PointInTime
from northstar_infrastructure.persistence import SQLiteEquityCandleEvidenceSnapshotCatalog
from tests.equity_lifecycle_monitoring_fixtures import (
    CATALOG_SCOPE, lifecycle_rows, result_for, summary,
)

path, issuance_id, at = Path(sys.argv[1]), sys.argv[2], sys.argv[3]
catalog = SQLiteEquityCandleEvidenceSnapshotCatalog(path)
listed = catalog.list_snapshots(CATALOG_SCOPE, PointInTime(at), 300)
outcome = result_for(path, {"issuance_id": issuance_id, "monitoring_instant": at})
print(json.dumps({
    "pid": os.getpid(),
    "listed": [e.snapshot_id for e in listed],
    "summary": summary(outcome),
    "events": len(lifecycle_rows(path)),
}))
"""


def _fresh_process(database: Path, issuance_id: str, at: PointInTime) -> dict[str, Any]:
    completed = subprocess.run(  # the test's own interpreter, running fixed code only
        [sys.executable, "-c", _FRESH_PROCESS, str(database), issuance_id, at.value],
        cwd=_INFRA_ROOT,
        env=dict(os.environ, PYTHONHASHSEED="4242"),
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    return json.loads(completed.stdout)


def test_ms22_a_fresh_process_reproduces_the_selection_and_outcomes_from_the_file(
    world: LifecycleWorld, tmp_path: Path
) -> None:
    world.opened()
    world.activated()
    world.targeted()
    other = copy_world(world, tmp_path / "other.sqlite3")  # identical authoritative inputs
    here = result(world, ist(9, 57))
    listed = SQLiteEquityCandleEvidenceSnapshotCatalog(world.database).list_snapshots(
        CATALOG_SCOPE, ist(9, 57), 300
    )
    fresh = _fresh_process(other.database, other.issuance_id, ist(9, 57))
    assert fresh["pid"] != os.getpid()
    assert fresh["listed"] == [e.snapshot_id for e in listed]
    assert fresh["summary"] == json.loads(json.dumps(summary(here)))  # same steps and events
    assert _stored(other) == _stored(world)
    # Restart over existing history: rehydrated, nothing duplicated, the terminal state holds.
    before = lifecycle_rows(world.database)
    again = _fresh_process(world.database, world.issuance_id, ist(9, 57))
    assert again["summary"]["state"] == "CLOSED_TARGET" and again["summary"]["appended"] == []
    assert again["events"] == 3 and lifecycle_rows(world.database) == before


# ----------------------------------------------------------------------
# E3, I6: catalog readers in other processes while a writer issues
# ----------------------------------------------------------------------


def test_e3_i6_readers_in_other_processes_see_only_whole_committed_states(
    world: LifecycleWorld,
) -> None:
    states = [[e.snapshot_id for e in _catalog_answer(world)]]
    context = multiprocessing.get_context("spawn")
    start, results = context.Barrier(3), context.Queue()
    readers = [
        context.Process(
            target=read_repeatedly,
            args=(str(world.database), ist(15, 0).value, 30, start, results),
        )
        for _ in range(2)
    ]
    for reader in readers:
        reader.start()
    start.wait(60)
    for minute in range(50, 58):  # the writer: eight snapshots, each its own transaction
        issue(world, ist(9, minute))
        states.append([e.snapshot_id for e in _catalog_answer(world)])
    reports = [results.get(timeout=_TIMEOUT) for _ in readers]
    for reader in readers:
        reader.join(timeout=60)
        assert reader.exitcode == 0
    for _pid, answers, errors in reports:
        assert errors == [] and len(answers) == 30
        for answer in answers:
            assert answer in states  # a whole committed state: never partial, never dirty
        positions = [states.index(answer) for answer in answers]
        assert positions == sorted(positions)  # each reader only ever moves forward


def _catalog_answer(world: LifecycleWorld) -> Any:
    return SQLiteEquityCandleEvidenceSnapshotCatalog(world.database).list_snapshots(
        CATALOG_SCOPE, ist(15, 0), 300
    )
