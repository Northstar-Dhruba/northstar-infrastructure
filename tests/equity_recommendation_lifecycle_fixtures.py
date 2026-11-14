"""Synthetic fixtures for the EI-6B durable lifecycle store, on real SQLite.

EI-6B acceptance runs the real chain over one disposable SQLite file, through
merged use cases and adapters only:

    EI-6A world (tests/equity_recommendation_issuance_fixtures.py: EI-4E
    research, declaration through b, EI-4C v2 generation, EI-4D recording,
    the real issuer, SQLiteEquityRecommendationIssuanceAuthority)
    -> synthetic TCS bars after the issuance, EI-2C ingestion, EI-2B snapshots
    -> RecordEquityRecommendationLifecycleUseCase (EI-6B Application)
    -> SQLiteEquityRecommendationLifecycleStore (EI-6B Infrastructure)

Every lifecycle event is produced by Core through the real recording use case
and written through the adapter's authorized ``append``. The issued TCS
record: eligible from 09:45 IST, entry zone 252.25-253.5, stop 249, target
262.5, latest entry bar 10:45 (window ends 10:50), modeled exit 14:00.

Two TEST-ONLY stores wrap the real adapter; neither constructs, copies or
registers an authorization, and neither touches the Application's private
issuer:

- ``AppendCapture`` keeps the GENUINE authorization the real use case issues
  and writes nothing (it reports the store unavailable), so a test can hand
  that authorization to the real adapter later -- stale, retried, or with a
  fault seam;
- ``BarrierLifecycleStore`` (spawned workers only) is the real adapter whose
  ``append`` first waits at a barrier, so two processes are known to hold
  authorizations over the same prior history before either appends.

Everything is SYNTHETIC TEST DATA. Nothing here opens a network connection,
reads a clock, or touches a production database.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from northstar_application.application_services import (
    EquityIssuedRecommendation,
    EquityLifecycleRecordingRefusal,
    EquityLifecycleRecordingRefusedError,
    EquityLifecycleRecordingResult,
    RecordEquityRecommendationLifecycleUseCase,
    equity_lifecycle_event_documents,
)
from northstar_application.ports import (
    EquityRecommendationLifecycleStore,
    EquityRecommendationLifecycleStoreUnavailableError,
)
from northstar_core.domain.value_objects import ListingReference

from northstar_infrastructure.persistence import (
    SQLiteEquityCandleEvidenceSnapshotStore,
    SQLiteEquityRecommendationLifecycleStore,
)
from northstar_infrastructure.persistence import (
    sqlite_equity_recommendation_lifecycle as adapter_module,
)
from tests.equity_recommendation_acceptance_fixtures import TCS, Evidence, ist
from tests.equity_recommendation_issuance_fixtures import (
    B,
    IssuanceWorld,
    authority,
    issuance_world,
    verify_stored,
)

LIFECYCLE_EVENTS = "equity_recommendation_lifecycle_events"

# SYNTHETIC bars after the issuance (bar 7 opens 09:45 IST, at eligible_from).
ENTRY_BAR = ("253.8", "254.2", "253", "253.4")  # touches the entry zone only
TARGET_BAR = ("253.4", "263", "253.1", "262.8")  # reaches the target only
CANCEL_BAR = ("251", "252", "248.5", "249.5")  # touches the stop, not the zone
QUIET_BAR = ("251", "251.5", "250.5", "251")  # touches nothing


def store(database: Path) -> SQLiteEquityRecommendationLifecycleStore:
    return SQLiteEquityRecommendationLifecycleStore(database)


def recorder(
    database: Path,
    lifecycles: EquityRecommendationLifecycleStore | None = None,
    max_writes: int = 3,
) -> RecordEquityRecommendationLifecycleUseCase:
    """The real recording use case over ``database``, from its path alone."""
    return RecordEquityRecommendationLifecycleUseCase(
        authority(database),
        verify_stored(database),
        SQLiteEquityCandleEvidenceSnapshotStore(database),
        lifecycles if lifecycles is not None else store(database),
        max_writes,
    )


class AppendCapture(EquityRecommendationLifecycleStore):
    """TEST-ONLY: reads through the real adapter; keeps each authorization; writes nothing."""

    def __init__(self, database: Path) -> None:
        self._store = store(database)
        self.authorizations: list[Any] = []

    def load_lifecycle(self, issuance_id: str):  # type: ignore[no-untyped-def]
        return self._store.load_lifecycle(issuance_id)

    def append(self, authorization):  # type: ignore[no-untyped-def]
        self.authorizations.append(authorization)
        raise EquityRecommendationLifecycleStoreUnavailableError(
            "test: authorization captured; nothing written"
        )


class KeepingLifecycleStore(SQLiteEquityRecommendationLifecycleStore):
    """TEST-ONLY: the real adapter, keeping each authorization it is handed."""

    def __init__(self, database: Path) -> None:
        super().__init__(database)
        self.authorizations: list[Any] = []

    def append(self, authorization):  # type: ignore[no-untyped-def]
        self.authorizations.append(authorization)
        return super().append(authorization)


@dataclass
class LifecycleWorld:
    """One MODELED TCS issuance (EI-6A sequence 2) in one file, with its lifecycle store."""

    world: IssuanceWorld
    issuance: EquityIssuedRecommendation

    @property
    def database(self) -> Path:
        return self.world.database

    @property
    def issuance_id(self) -> str:
        return self.issuance.issuance_id

    def evidence(self) -> Evidence:
        return self.world.pipeline.evidence()

    def bars(
        self, prices: dict[int, tuple[str, str, str, str]], listing: ListingReference = TCS
    ) -> None:
        """Journal SYNTHETIC final bars, by five-minute index (bar 7 opens 09:45 IST)."""
        self.evidence().candles(prices, listing)

    def snapshot(
        self, hour: int, minute: int, second: int = 0, listing: ListingReference = TCS
    ) -> str:
        return self.evidence().issue(ist(hour, minute, second), listing).snapshot_id

    def store(self) -> SQLiteEquityRecommendationLifecycleStore:
        return store(self.database)

    def record(
        self, *snapshot_ids: str, issuance_id: str | None = None, **options: Any
    ) -> EquityLifecycleRecordingResult:
        return recorder(self.database, **options).execute(
            issuance_id or self.issuance_id, snapshot_ids
        )

    def capture(self, *snapshot_ids: str, issuance_id: str | None = None) -> Any:
        """The genuine authorization the real use case issues for ``snapshot_ids``; none written."""
        capture = AppendCapture(self.database)
        try:
            recorder(self.database, capture).execute(issuance_id or self.issuance_id, snapshot_ids)
        except EquityLifecycleRecordingRefusedError as refused:
            assert refused.reason is EquityLifecycleRecordingRefusal.STORE_UNAVAILABLE, refused
        else:  # pragma: no cover -- a capture always refuses
            raise AssertionError("the capture store did not receive an append")
        (authorization,) = capture.authorizations
        return authorization

    # -- the standard synthetic path ----------------------------------------

    def opened(self) -> str:
        """09:46:30: bar 7 has not ended -> ENTRY_WINDOW_OPENED only."""
        return self.snapshot(9, 46, 30)

    def activated(self) -> str:
        """Bar 7 final, touching the zone; 09:51 -> ENTRY_ACTIVATED."""
        self.bars({7: ENTRY_BAR})
        return self.snapshot(9, 51)

    def targeted(self) -> str:
        """Bar 8 final, reaching the target; 09:56 -> TARGET_REACHED."""
        self.bars({8: TARGET_BAR})
        return self.snapshot(9, 56)


def lifecycle_world(database: Path) -> LifecycleWorld:
    """Declared through b (sequence 1), the TCS proposal issued (sequence 2); no events."""
    world = issuance_world(database)
    assert world.declare(B).outcome.value == "DECLARED"
    issued = world.issue(world.proposed())
    assert (issued.outcome.value, issued.recording_sequence) == ("ISSUED", 2), issued.detail
    return LifecycleWorld(world, issued.issuance)


# ---------------------------------------------------------------------------
# Separate OS processes (spawned; the Windows start method, everywhere)
# ---------------------------------------------------------------------------


class BarrierLifecycleStore(SQLiteEquityRecommendationLifecycleStore):
    """TEST-ONLY, spawned workers: the real adapter, whose append first waits at ``start``."""

    def __init__(self, database: Path, start: Any) -> None:
        super().__init__(database)
        self._start = start

    def append(self, authorization):  # type: ignore[no-untyped-def]
        self._start.wait(60)
        return super().append(authorization)


def record_in_another_process(
    database: str, payload: str, start: Any, results: Any, name: str
) -> None:
    """Spawned worker: build the real recorder from the path alone; append after ``start``.

    ``payload`` is JSON: the issuance identity and the snapshot identities. The
    authorization is issued in this process by the real use case over the
    history it loaded; the barrier sits inside ``append``, after that load.
    Nothing is shared with the parent but the file.
    """
    request = json.loads(payload)
    path = Path(database)
    use_case = recorder(path, BarrierLifecycleStore(path, start))
    try:
        result = use_case.execute(request["issuance_id"], tuple(request["snapshots"]))
        documents = list(equity_lifecycle_event_documents(result.new_events))
        report = (result.outcome.value, result.state.value, documents)
        results.put((name, os.getpid(), report, None))
    except BaseException as exc:  # noqa: BLE001 -- reported to the parent, never swallowed
        results.put((name, os.getpid(), None, repr(exc)))


def record_then_exit_before_commit(database: str, payload: str) -> None:
    """Spawned worker: the real recorder, whose process ends abruptly where COMMIT would run.

    The batch is inserted inside the open transaction; the process then exits
    without committing or rolling back. SQLite must leave nothing.
    """
    request = json.loads(payload)

    def exit_before_commit(connection: Any) -> None:
        os._exit(17)

    adapter_module._commit = exit_before_commit  # type: ignore[assignment]  # this process only
    recorder(Path(database)).execute(request["issuance_id"], tuple(request["snapshots"]))
    os._exit(0)  # pragma: no cover -- never reached: the commit seam ends the process
