"""EI-7D: the read-only readers across separate operating-system processes, on real SQLite.

R8 and EI7-N20 at the Infrastructure layer: a fresh interpreter, built from
the database path alone, reads the sealed file and obtains the same
authoritative identities, sequences and immutable records as this process,
without changing a byte; a historical watermark replays identically after a
later commit. EI7-N21: readers in other processes, racing the accepted
declaration writer, only ever observe whole committed states, moving forward.

Workers are independent interpreters -- ``subprocess`` with the test's own
interpreter, or the ``spawn`` start method -- sharing nothing with this
process but the file. Synchronization is by barriers and queues with bounded
timeouts; no sleep is an oracle. Nothing here opens a network connection or
touches a production database.
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

from tests.equity_read_only_fixtures import (
    SessionWorld,
    copy_session,
    file_state,
    read_sessions_repeatedly,
    session_answer,
    session_world,
)
from tests.equity_recommendation_acceptance_fixtures import ist

_INFRA_ROOT = Path(__file__).resolve().parents[1]
_TIMEOUT = 240


@pytest.fixture(autouse=True)
def _refuse_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("EI-7D tests must not open network connections.")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)


@pytest.fixture(scope="module")
def template(tmp_path_factory: pytest.TempPathFactory) -> SessionWorld:
    return session_world(tmp_path_factory.mktemp("ei7d-processes") / "template.sqlite3")


@pytest.fixture
def session(template: SessionWorld, tmp_path: Path) -> SessionWorld:
    directory = tmp_path / "sealed copy"  # a space in the path, as in the EI-4D goldens
    directory.mkdir()
    return copy_session(template, directory / "ei7d session.sqlite3")


# The fresh process runs fixed code only: every answer from the path alone.
_FRESH_PROCESS = r"""
import json, os, sys
from pathlib import Path
from northstar_application.application_services import (
    GetEquityRecommendationDetailUseCase, GetEquityResearchTimelineUseCase,
)
from tests.equity_read_only_fixtures import readers, session_answer
from tests.equity_recommendation_acceptance_fixtures import CONFIG, DAY, INFY, TCS

path = Path(sys.argv[1])
watermark = None if sys.argv[2] == "-" else int(sys.argv[2])
r = readers(path)
session = session_answer(path, watermark)
recorded = r["sessions"].recorded_session_issuances(
    TCS.exchange_code, DAY, limit=50, watermark=watermark
)
detail = GetEquityRecommendationDetailUseCase(r["lifecycles"], r["generations"], r["snapshots"])
research = GetEquityResearchTimelineUseCase(
    r["timelines"], r["snapshots"], r["issuances"], r["generations"]
)
items = []
for issued in recorded.issuances:
    identity = issued.recommendation.issuance_id
    lifecycle = r["lifecycles"].load_lifecycle(identity)
    view = detail.execute(identity)
    items.append({
        "issuance_id": identity,
        "sequence": issued.recording_sequence,
        "generation_key": issued.recommendation.generation_key,
        "proposal": r["generations"].load_disposition(
            issued.recommendation.generation_key
        ).proposal_fingerprint,
        "state": lifecycle.lifecycle.state.value,
        "events": [e.sequence for e in lifecycle.lifecycle.events],
        "panels": sorted((p.snapshot_id, p.status.value) for p in view.evidence),
    })
timelines = {}
for listing in (TCS, INFY):
    stored = r["timelines"].load(CONFIG.strategy, listing, DAY)
    timelines[listing.symbol.value] = {
        "entries": [e.entry_fingerprint for e in stored.entries],
        "research": research.execute(CONFIG.strategy, listing, DAY).generation_status.value,
    }
print(json.dumps({
    "pid": os.getpid(), "session": session, "items": items, "timelines": timelines,
}))
"""


def _fresh_process(database: Path, watermark: int | None = None, seed: str = "4242") -> Any:
    completed = subprocess.run(  # the test's own interpreter, running fixed code only
        [
            sys.executable,
            "-c",
            _FRESH_PROCESS,
            str(database),
            "-" if watermark is None else str(watermark),
        ],
        cwd=_INFRA_ROOT,
        env=dict(os.environ, PYTHONHASHSEED=seed),
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    return json.loads(completed.stdout)


def _here(session: SessionWorld, watermark: int | None = None) -> dict[str, Any]:
    """The same answer as the fresh process, computed in this process."""
    from northstar_application.application_services import (
        GetEquityRecommendationDetailUseCase,
        GetEquityResearchTimelineUseCase,
    )

    from tests.equity_read_only_fixtures import readers
    from tests.equity_recommendation_acceptance_fixtures import CONFIG, DAY, INFY, TCS

    r = readers(session.database)
    recorded = r["sessions"].recorded_session_issuances(
        TCS.exchange_code, DAY, limit=50, watermark=watermark
    )
    detail = GetEquityRecommendationDetailUseCase(r["lifecycles"], r["generations"], r["snapshots"])
    research = GetEquityResearchTimelineUseCase(
        r["timelines"], r["snapshots"], r["issuances"], r["generations"]
    )
    items = []
    for issued in recorded.issuances:
        identity = issued.recommendation.issuance_id
        lifecycle = r["lifecycles"].load_lifecycle(identity)
        view = detail.execute(identity)
        disposition = r["generations"].load_disposition(issued.recommendation.generation_key)
        items.append(
            {
                "issuance_id": identity,
                "sequence": issued.recording_sequence,
                "generation_key": issued.recommendation.generation_key,
                "proposal": disposition.proposal_fingerprint,
                "state": lifecycle.lifecycle.state.value,
                "events": [e.sequence for e in lifecycle.lifecycle.events],
                "panels": sorted([p.snapshot_id, p.status.value] for p in view.evidence),
            }
        )
    timelines = {}
    for listing in (TCS, INFY):
        stored = r["timelines"].load(CONFIG.strategy, listing, DAY)
        timelines[listing.symbol.value] = {
            "entries": [e.entry_fingerprint for e in stored.entries],
            "research": research.execute(CONFIG.strategy, listing, DAY).generation_status.value,
        }
    return {
        "session": session_answer(session.database, watermark),
        "items": items,
        "timelines": timelines,
    }


def test_r8_a_fresh_process_reads_the_same_authority_and_changes_nothing(
    session: SessionWorld,
) -> None:
    before = file_state(session.database)
    fresh = _fresh_process(session.database)
    again = _fresh_process(session.database, seed="777")  # another hash seed
    assert fresh["pid"] != os.getpid() and again["pid"] != fresh["pid"]
    here = _here(session)
    for answer in (fresh, again):
        assert {k: answer[k] for k in ("session", "items", "timelines")} == here
    assert here["session"] == [5, 5, [1, 5], [2, 3, 4]]
    assert [i["state"] for i in here["items"]] == ["CLOSED_TARGET", "ACTIVE", "RECORDED"]
    assert file_state(session.database) == before  # no process wrote, initialized or recovered


def test_n20_a_historical_watermark_replays_identically_in_a_fresh_process_after_commits(
    session: SessionWorld,
) -> None:
    at_three = _fresh_process(session.database, watermark=3)
    assert at_three["session"] == [5, 3, [1], [2, 3]]
    assert session.world.world.declare(ist(9, 45)).outcome.value == "DECLARED"  # sequence 6
    replay = _fresh_process(session.database, watermark=3)
    assert replay["session"] == [6, 3, [1], [2, 3]]  # N observed now, W replayed
    assert replay["items"] == at_three["items"] and replay["timelines"] == at_three["timelines"]


def test_n21_readers_in_other_processes_see_only_whole_committed_states_moving_forward(
    session: SessionWorld,
) -> None:
    states = [session_answer(session.database)]
    context = multiprocessing.get_context("spawn")  # the Windows start method, everywhere
    start, results = context.Barrier(3), context.Queue()
    workers = [
        context.Process(
            target=read_sessions_repeatedly,
            args=(str(session.database), 25, start, results),
        )
        for _ in range(2)
    ]
    for worker in workers:
        worker.start()
    try:
        start.wait(60)
        for minute in range(45, 53):  # the accepted writer: eight declarations, one each
            declared = session.world.world.declare(ist(9, minute))
            assert declared.outcome.value == "DECLARED", declared
            states.append(session_answer(session.database))
        reports = [results.get(timeout=_TIMEOUT) for _ in workers]
    finally:
        for worker in workers:
            worker.join(timeout=60)
            if worker.is_alive():  # pragma: no cover -- diagnostic cleanup only
                worker.terminate()
                worker.join(timeout=10)
    assert all(worker.exitcode == 0 for worker in workers)
    for _pid, answers, errors in reports:
        assert errors == [] and len(answers) == 25
        for answer in answers:
            assert answer in states  # a whole committed state: never partial, never dirty
        positions = [answer[0] for answer in answers]
        assert positions == sorted(positions)  # each reader only moves forward
    assert states[-1][0] == 13 and states[-1][3] == [2, 3, 4]
