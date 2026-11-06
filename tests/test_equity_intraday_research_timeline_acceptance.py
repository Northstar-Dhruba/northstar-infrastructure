"""EI-3C acceptance: authoritative synthetic research timelines over SQLite, end to end.

One temporary SQLite file holds everything: the EI-2B schema and journal,
synthetic candles and finality from SyntheticEquityCandleEventSource ingested
through EI-2C, EI-2B snapshots issued through the EI-2C workflow under EI-1C-3
eligibility (accepted production NSE venue reference, explicitly synthetic
M&M applicability, 2026-08-12), and the EI-3C-B timeline store driven by the
merged EI-3C-A advancement use case. Nothing is mocked; every adapter is
closed between "processes" and reopened from the file.

Every price, identity and calibration is synthetic. A CANDIDATE is a research
label, never a recommendation, order or fill, and nothing opens a network
connection.
"""

from __future__ import annotations

import socket
from pathlib import Path
from typing import Any

import pytest
from northstar_application.application_services import (
    EquityIntradayResearchOutcome,
    EquityIntradayResearchRejection,
    EquityIntradayResearchScheduleOutcome,
    EquityIntradayResearchTimelineAdvanceStatus,
    EquityIntradayResearchTimelineIdentity,
    EquityIntradayResearchTimelineRole,
    EvaluateEquityIntradayResearchUseCase,
    rehydrate_equity_intraday_research_timeline,
    stored_equity_intraday_research_timeline,
)

from tests.equity_intraday_research_timeline_fixtures import (
    CONFIG,
    DAY,
    M_AND_M,
    Clock,
    Harness,
    check_invariants,
    count,
    entry_rows,
)

_STATUS = EquityIntradayResearchTimelineAdvanceStatus
_ROLE = EquityIntradayResearchTimelineRole
_DONE = EquityIntradayResearchScheduleOutcome
_IDENTITY = EquityIntradayResearchTimelineIdentity(M_AND_M, DAY, CONFIG)
_ENTRIES = "equity_intraday_research_timeline_entries"


@pytest.fixture(autouse=True)
def _refuse_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("EI-3C acceptance must not open network connections.")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)


def _scenario(database: Path) -> dict[str, Any]:
    """Run the acceptance scenario on a fresh database and return its fingerprints."""
    # 1-3. Evidence: the EI-2B schema, synthetic journal evidence, issued snapshots.
    first = Harness(database, Clock())
    first.candles()
    s31, s36, s41 = first.at(31, 36, 41)

    # 4-8. The store and use case advance NO_SIGNAL, then CANDIDATE, then REPEAT_QUALIFYING.
    committed = []
    for snapshot, role in (
        (s31, _ROLE.NO_SIGNAL),
        (s36, _ROLE.CANDIDATE),
        (s41, _ROLE.REPEAT_QUALIFYING),
    ):
        result = first.advance([snapshot])
        assert result.status is _STATUS.COMMITTED and result.commit_confirmed
        assert result.appended[-1].role is role
        committed.append(result)
    final = committed[-1].timeline
    assert final is not None
    candidate = final.candidate
    assert candidate is not None and candidate.snapshot_id == s36.snapshot_id
    del first  # 9. Every adapter opens and closes its own connections per operation.

    # 10-11. A fresh 'process' reloads and rehydrates the authoritative history.
    second = Harness(database, Clock())
    stored = second.timelines.load(CONFIG.strategy, M_AND_M, DAY)
    assert stored is not None
    assert stored == stored_equity_intraday_research_timeline(final)
    reloaded = rehydrate_equity_intraday_research_timeline(_IDENTITY, stored, second.snapshots)

    # 12-15. Every decision, entry fingerprint, the chain, head and candidate reproduce.
    research = EvaluateEquityIntradayResearchUseCase(second.snapshots, CONFIG)
    for entry, snapshot in zip(reloaded.entries, (s31, s36, s41), strict=True):
        decision = research.execute(M_AND_M, DAY, snapshot.snapshot_id)
        assert entry.decision == decision and entry.decision.fingerprint == decision.fingerprint
    assert [e.fingerprint for e in reloaded.entries] == [e.fingerprint for e in final.entries]
    assert [e.previous_fingerprint for e in reloaded.entries] == [
        None,
        *[e.fingerprint for e in reloaded.entries[:-1]],
    ]
    assert reloaded.head_fingerprint == final.head_fingerprint == stored.head_fingerprint
    assert reloaded.candidate == candidate
    assert reloaded.document() == final.document()

    # 16-17. The original snapshots again: duplicates, and nothing written.
    rows = entry_rows(database)
    retried = second.advance([s31, s36, s41])
    assert retried.status is _STATUS.NOTHING_TO_COMMIT
    assert [item.outcome for item in retried.items] == [_DONE.DUPLICATE] * 3
    assert entry_rows(database) == rows

    # 18-19. A later correction leaves an unresolved revision: a REJECTED entry.
    third = Harness(database, Clock())
    third.correct_bar_2()
    (s46,) = third.at(46)
    corrected = third.advance([s46])
    assert corrected.status is _STATUS.COMMITTED
    rejected = corrected.appended[-1]
    assert rejected.role is _ROLE.REJECTED
    assert rejected.decision.outcome is EquityIntradayResearchOutcome.REJECTED
    assert rejected.decision.reasons == (
        EquityIntradayResearchRejection.UNRESOLVED_SOURCE_REVISION,
    )
    assert rejected.previous_fingerprint == final.head_fingerprint

    # 20. The earlier CANDIDATE is unchanged, row for row.
    assert corrected.timeline is not None
    assert corrected.timeline.candidate == candidate
    assert entry_rows(database)[:3] == rows
    check_invariants(database)
    assert count(database, _ENTRIES) == 4
    return {
        "snapshots": [s.snapshot_id for s in (s31, s36, s41, s46)],
        "decisions": [e.decision.fingerprint for e in corrected.timeline.entries],
        "entries": [e.fingerprint for e in corrected.timeline.entries],
        "roles": [e.role.value for e in corrected.timeline.entries],
        "head": corrected.timeline.head_fingerprint,
    }


def test_authoritative_research_history_survives_restart_retry_and_correction(
    tmp_path: Path,
) -> None:
    report = _scenario(tmp_path / "acceptance.sqlite3")
    assert report["roles"] == ["NO_SIGNAL", "CANDIDATE", "REPEAT_QUALIFYING", "REJECTED"]
    assert all(p.startswith("eir1-sha256:") for p in report["decisions"])
    assert all(p.startswith("eirt1-sha256:") for p in report["entries"])


def test_the_same_inputs_reproduce_the_same_fingerprints_on_a_new_database(
    tmp_path: Path,
) -> None:
    assert _scenario(tmp_path / "first.sqlite3") == _scenario(tmp_path / "second.sqlite3")
