"""Synthetic fixtures for EI-6C lifecycle monitoring, on real SQLite.

EI-6C Infrastructure acceptance runs the merged Application monitor over one
disposable SQLite file, through merged use cases and adapters only:

    EI-6B world (tests/equity_recommendation_lifecycle_fixtures.py: one MODELED
    TCS issuance at 09:43:30 IST, research snapshots at 09:36, 09:41, 09:46)
    -> SQLiteEquityCandleEvidenceSnapshotCatalog (EI-6C, read-only)
    -> MonitorEquityRecommendationLifecyclesUseCase (EI-6C Application)
    -> RecordEquityRecommendationLifecycleUseCase (EI-6B Application)
    -> SQLiteEquityRecommendationLifecycleStore (EI-6B Infrastructure)

Every lifecycle event is decided by Core through the real recorder and written
through the adapter's authorized ``append``; the monitor composes them and
writes nothing itself. Everything is SYNTHETIC TEST DATA. Nothing here opens a
network connection, reads a clock, or touches a production database.

TEST-ONLY helpers:

- ``SpyRecorder`` is the real recorder, keeping each call's snapshot
  identities (and optionally raising at a chosen call, as an unexpected
  failure would);
- ``FirstAppendBarrierStore`` (spawned workers only) is the real lifecycle
  adapter whose FIRST ``append`` waits at a barrier, so two monitors are known
  to hold authorizations over the same prior history before either appends;
- ``foreign_currency_snapshot`` issues a real snapshot through the EI-2B use
  case with another currency: it matches every catalog column, and the
  recorder refuses it by the EI-5C rule.
"""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
from collections.abc import Sequence
from contextlib import closing
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from northstar_application.application_services import (
    NSE_EQUITY_INTRADAY_V1_RESEARCH_WINDOW,
    EquityCandleEvidenceClass,
    EquityLifecycleMonitoringExplicitScope,
    EquityLifecycleMonitoringReport,
    EquityLifecycleMonitoringRequest,
    EquityLifecycleMonitoringResult,
    EvaluateEquityResearchWindowUseCase,
    IssueEquityCandleEvidenceSnapshotUseCase,
    IssueEquityResearchEvidenceUseCase,
    MonitorEquityRecommendationLifecyclesUseCase,
    RecordEquityRecommendationLifecycleUseCase,
    equity_lifecycle_event_documents,
)
from northstar_application.application_services.monitor_equity_recommendation_lifecycles import (
    EquityLifecycleMonitoringScope,  # the scope union; not re-exported by the package
)
from northstar_application.ports import EquityCandleEvidenceSnapshotCatalogScope
from northstar_core.domain.value_objects import ListingReference
from northstar_core.foundation.value_objects import Currency, PointInTime, Timeframe

from northstar_infrastructure.persistence import (
    SQLiteEquityCandleEvidenceSnapshotCatalog,
    SQLiteEquityCandleEvidenceSnapshotStore,
    SQLiteEquityRecommendationGenerationStore,
    SQLiteEquityRecommendationLifecycleStore,
)
from northstar_infrastructure.persistence import (
    sqlite_equity_recommendation_lifecycle as lifecycle_module,
)
from tests.equity_recommendation_acceptance_fixtures import (
    APPLICABILITY,
    DAY,
    FINALITY,
    PROVENANCE,
    TCS,
    VENUE,
    Pipeline,
)
from tests.equity_recommendation_issuance_fixtures import (
    IssuanceWorld,
    authority,
    verify_stored,
)
from tests.equity_recommendation_lifecycle_fixtures import (
    LIFECYCLE_EVENTS,
    LifecycleWorld,
    recorder,
)

CATALOG_SCOPE = EquityCandleEvidenceSnapshotCatalogScope(
    TCS,
    DAY,
    Timeframe("5m"),
    NSE_EQUITY_INTRADAY_V1_RESEARCH_WINDOW.version,
    EquityCandleEvidenceClass.SYNTHETIC,
)
"""The catalog scope of the TCS issuance, as the monitor derives it."""


def monitor(
    database: Path,
    recording: RecordEquityRecommendationLifecycleUseCase | None = None,
    catalog: Any = None,
) -> MonitorEquityRecommendationLifecyclesUseCase:
    """The merged EI-6C monitor over ``database``, composed from the path alone."""
    return MonitorEquityRecommendationLifecyclesUseCase(
        authority(database),
        SQLiteEquityRecommendationGenerationStore(database),
        SQLiteEquityCandleEvidenceSnapshotStore(database),
        catalog if catalog is not None else SQLiteEquityCandleEvidenceSnapshotCatalog(database),
        SQLiteEquityRecommendationLifecycleStore(database),
        recording if recording is not None else recorder(database),
    )


class SpyRecorder(RecordEquityRecommendationLifecycleUseCase):
    """TEST-ONLY: the real recorder, keeping each call; raising ``failure`` at call ``fail_at``."""

    def __init__(
        self, database: Path, fail_at: int | None = None, failure: BaseException | None = None
    ) -> None:
        super().__init__(  # the collaborators of tests.equity_recommendation_lifecycle_fixtures
            authority(database),
            verify_stored(database),
            SQLiteEquityCandleEvidenceSnapshotStore(database),
            SQLiteEquityRecommendationLifecycleStore(database),
            3,
        )
        self.calls: list[tuple[str, ...]] = []
        self._fail_at = fail_at
        self._failure = failure

    def execute(self, issuance_id: str, snapshot_ids: Sequence[str]):  # type: ignore[no-untyped-def]
        self.calls.append(tuple(snapshot_ids))
        if self._fail_at is not None and len(self.calls) == self._fail_at:
            assert self._failure is not None
            raise self._failure
        return super().execute(issuance_id, snapshot_ids)


def instant(base: PointInTime, seconds: float) -> PointInTime:
    """``base`` shifted by ``seconds``, as canonical PointInTime text."""
    moment = datetime.fromisoformat(base.value.replace("Z", "+00:00")) + timedelta(seconds=seconds)
    return PointInTime(moment.isoformat().replace("+00:00", "Z"))


def issue(
    world: LifecycleWorld,
    as_of: PointInTime,
    issued_at: PointInTime | None = None,
    listing: ListingReference = TCS,
) -> str:
    """Issue a real EI-2B snapshot through the EI-2B use case; return its identity."""
    issued = world.evidence().issuance.execute(listing, DAY, as_of, issued_at or as_of)
    assert issued.outcome.value == "ISSUED", issued.outcome
    assert issued.snapshot is not None
    return issued.snapshot.snapshot_id


def foreign_currency_snapshot(world: LifecycleWorld, as_of: PointInTime) -> str:
    """A real snapshot in USD: every catalog column matches; EI-5C refuses its window."""
    evidence = world.evidence()
    issuance = IssueEquityResearchEvidenceUseCase(
        EvaluateEquityResearchWindowUseCase(VENUE, APPLICABILITY),
        IssueEquityCandleEvidenceSnapshotUseCase(
            evidence.journal,
            evidence.snapshots,
            Currency("USD"),
            FINALITY,
            reference_provenance=PROVENANCE,
        ),
    )
    issued = issuance.execute(TCS, DAY, as_of, as_of)
    assert issued.outcome.value == "ISSUED" and issued.snapshot is not None
    return issued.snapshot.snapshot_id


def copy_world(template: LifecycleWorld, database: Path) -> LifecycleWorld:
    """A disposable copy of ``template``'s file (rollback journal: the file is the database)."""
    shutil.copyfile(template.database, database)
    return LifecycleWorld(IssuanceWorld(Pipeline(database)), template.issuance)


def run(
    database: Path,
    monitoring_instant: PointInTime,
    scope: EquityLifecycleMonitoringScope,
    recording: RecordEquityRecommendationLifecycleUseCase | None = None,
) -> EquityLifecycleMonitoringReport:
    return monitor(database, recording).execute(
        EquityLifecycleMonitoringRequest(scope, monitoring_instant)
    )


def result(
    world: LifecycleWorld,
    monitoring_instant: PointInTime,
    recording: RecordEquityRecommendationLifecycleUseCase | None = None,
) -> EquityLifecycleMonitoringResult:
    """Monitor the world's one issuance (EXPLICIT) at ``monitoring_instant``."""
    report = run(
        world.database,
        monitoring_instant,
        EquityLifecycleMonitoringExplicitScope((world.issuance_id,)),
        recording,
    )
    (only,) = report.results
    return only


def summary(outcome: EquityLifecycleMonitoringResult) -> dict[str, Any]:
    """A plain, comparable rendering of one monitoring result (for other processes too)."""
    return {
        "issuance_id": outcome.issuance_id,
        "outcome": outcome.outcome.value,
        "steps": [(s.snapshot_id, s.as_of.value, s.outcome.value) for s in outcome.steps],
        "appended": list(equity_lifecycle_event_documents(outcome.appended)),
        "state": None if outcome.state is None else outcome.state.value,
        "stopped_at": None if outcome.stopped_at is None else outcome.stopped_at.value,
        "refusal": None if outcome.refusal is None else str(outcome.refusal),
        "diagnostics": [(d.kind.value, d.snapshot_id) for d in outcome.diagnostics],
    }


# ---------------------------------------------------------------------------
# Separate OS processes (spawned; the Windows start method, everywhere)
# ---------------------------------------------------------------------------


class FirstAppendBarrierStore(SQLiteEquityRecommendationLifecycleStore):
    """TEST-ONLY, spawned workers: the real adapter; its FIRST append waits at ``start``."""

    def __init__(self, database: Path, start: Any) -> None:
        super().__init__(database)
        self._start: Any = start

    def append(self, authorization):  # type: ignore[no-untyped-def]
        if self._start is not None:
            start, self._start = self._start, None
            start.wait(60)
        return super().append(authorization)


def monitor_in_another_process(
    database: str, payload: str, start: Any, results: Any, name: str
) -> None:
    """Spawned worker: compose the real monitor from the path alone; first append after ``start``.

    ``payload`` is JSON: the issuance identity and the monitoring instant.
    Nothing is shared with the parent but the file.
    """
    request = json.loads(payload)
    path = Path(database)
    recording = recorder(path, FirstAppendBarrierStore(path, start))
    try:
        outcome = result_for(path, request, recording)
        results.put((name, os.getpid(), summary(outcome), None))
    except BaseException as exc:  # noqa: BLE001 -- reported to the parent, never swallowed
        results.put((name, os.getpid(), None, repr(exc)))


def result_for(
    path: Path,
    request: dict[str, Any],
    recording: RecordEquityRecommendationLifecycleUseCase | None = None,
) -> EquityLifecycleMonitoringResult:
    report = run(
        path,
        PointInTime(request["monitoring_instant"]),
        EquityLifecycleMonitoringExplicitScope((request["issuance_id"],)),
        recording,
    )
    (only,) = report.results
    return only


def monitor_then_exit_before_commit(database: str, payload: str) -> None:
    """Spawned worker: the real monitor, whose process ends abruptly where COMMIT would run.

    The first event batch is inserted inside the EI-6B append transaction; the
    process then exits without committing or rolling back. SQLite must leave
    nothing.
    """
    request = json.loads(payload)

    def exit_before_commit(connection: Any) -> None:
        os._exit(17)

    lifecycle_module._commit = exit_before_commit  # type: ignore[assignment]  # this process only
    result_for(Path(database), request)
    os._exit(0)  # pragma: no cover -- never reached: the commit seam ends the process


def read_repeatedly(database: str, at: str, rounds: int, start: Any, results: Any) -> None:
    """Spawned reader: list the TCS scope ``rounds`` times after ``start``; report every answer."""
    catalog = SQLiteEquityCandleEvidenceSnapshotCatalog(Path(database))
    start.wait(60)
    answers, errors = [], []
    for _ in range(rounds):
        try:
            listed = catalog.list_snapshots(CATALOG_SCOPE, PointInTime(at), 300)
            answers.append([entry.snapshot_id for entry in listed])
        except Exception as exc:  # noqa: BLE001 -- reported to the parent, never swallowed
            errors.append(repr(exc))
    results.put((os.getpid(), answers, errors))


def lifecycle_rows(database: Path) -> list[tuple[Any, ...]]:
    with closing(sqlite3.connect(database)) as connection:
        return connection.execute(
            f"SELECT * FROM {LIFECYCLE_EVENTS} ORDER BY issuance_id, event_sequence"  # noqa: S608
        ).fetchall()
