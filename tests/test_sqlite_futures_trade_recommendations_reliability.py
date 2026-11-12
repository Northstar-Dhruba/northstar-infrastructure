"""L9-F: futures recommendation cleanup never replaces a write's typed outcome.

Regression tests for the two write paths of the SQLite futures recommendation
module, following W2 (EI-4D-B), L9-T (EI-3C) and L9-C (EI-2B):

- ``_write``, shared by ``SQLiteFuturesTradeRecommendationStore.record`` and
  ``SQLiteFuturesRecommendationGenerationAuditStore.record_audit``: its
  exception handlers read ``in_transaction`` and called ``rollback()``
  unguarded, and its ``finally`` closed the connection unguarded;
- ``SQLiteFuturesTradeRecommendationLifecycleStore.record_progress``: its
  typed-error handler called ``rollback()`` unguarded, its sqlite3-error
  handler read ``in_transaction`` and rolled back unguarded, and its
  ``finally`` closed unguarded.

On a connection that failed or closed, reading ``in_transaction`` or rolling
back raises sqlite3.ProgrammingError; a failing rollback or close raises
sqlite3.OperationalError. Any of them replaced the typed outcome the
Application acts on: generation re-reads the race winners on
FuturesTradeRecommendationConflictError; monitoring reports
CONCURRENT_PROGRESS on FuturesTradeRecommendationLifecycleConflictError; the
storage, configuration, exclusivity, audit and evidence errors propagate.

The existing contracts are kept, not redefined: a COMMIT failure is still
FuturesTradeRecommendationStorageError (this port has no commit-unconfirmed
type), and after a confirmed COMMIT a close failure is still reported as
itself.

Every test uses real temporary SQLite databases, the real adapters and -- for
integration -- the real Application generation and monitoring use cases, over
the synthetic fixtures of the existing futures suites. Faults go through the
module's ``_connect`` seam, which opens only write connections: ``_Faulty`` is
a test-only stand-in that delegates every call to the real connection. It can
refuse or interrupt an INSERT, or lose the COMMIT before or after it takes
effect; and once the write has failed -- at the injected failure, or at the
first ``in_transaction`` or ``rollback`` access, which only failure paths make
-- it applies the cleanup fault: really closing the connection, or making
``in_transaction``, ``rollback()`` or ``close()`` raise. Every claim about what
is durable is checked on a fresh connection. Nothing sleeps.
"""

from __future__ import annotations

import socket
import sqlite3
from contextlib import closing
from datetime import date
from pathlib import Path
from typing import Any

import pytest
from northstar_application.application_services import (
    FuturesRecommendationMonitoringStopReason,
    FuturesTradeRecommendationGenerationProfile,
    GenerateFuturesTradeRecommendationsUseCase,
    MonitorFuturesTradeRecommendationsUseCase,
)
from northstar_application.ports import (
    FuturesRecommendationGenerationAuditConflictError,
    FuturesTradeRecommendationConfigurationConflictError,
    FuturesTradeRecommendationConflictError,
    FuturesTradeRecommendationExclusivityError,
    FuturesTradeRecommendationLifecycleConflictError,
    FuturesTradeRecommendationLifecycleEvidenceError,
)
from northstar_core.foundation.value_objects import PointInTime

import tests.test_futures_recommendation_monitoring_sqlite_acceptance as acceptance
import tests.test_sqlite_futures_trade_recommendation_lifecycles as life
import tests.test_sqlite_futures_trade_recommendations as recs
from northstar_infrastructure.market_data import (
    SQLiteFuturesHistoricalMarketDataRepository,
)
from northstar_infrastructure.persistence import (
    FuturesTradeRecommendationStorageError,
    SQLiteFuturesForwardResearchRecordRepository,
    SQLiteFuturesForwardResearchRecordStore,
    SQLiteFuturesRecommendationGenerationAuditStore,
    SQLiteFuturesTradeRecommendationLifecycleStore,
    SQLiteFuturesTradeRecommendationStore,
)
from northstar_infrastructure.persistence import (
    sqlite_futures_trade_recommendations as adapter_module,
)
from tests.futures_recommendation_audit_fixtures import recording_audit

_DEFINITIONS = "futures_trade_recommendations"
_CONFIGURATIONS = "futures_recommendation_configurations"
_AUDITS = "futures_recommendation_generation_audits"
_STEPS = "futures_trade_recommendation_steps"
_FAULTS = ["none", "closed", "in_transaction", "rollback", "close"]
_STOP = FuturesRecommendationMonitoringStopReason


@pytest.fixture(autouse=True)
def _refuse_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("L9-F reliability tests must not open network connections.")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)


class _Interrupted(BaseException):
    """Test-only: stands for the process being stopped mid-write."""


class _Faulty:
    """Test-only stand-in for an adapter write connection: delegates to a real one.

    Write behaviour (only after ``BEGIN IMMEDIATE``):

    - ``insert_failure``: the first INSERT raises it instead of executing;
    - ``commit``: ``"lost-before"`` raises at COMMIT without committing;
      ``"lost-after"`` really commits, then raises (the acknowledgment is lost).

    Cleanup fault, applied once the write has failed (``failed``):

    - ``"none"``: nothing more goes wrong;
    - ``"closed"``: the real connection is really closed (lost), so SQLite
      rolls an uncommitted transaction back and ``in_transaction`` and
      ``rollback()`` raise sqlite3.ProgrammingError;
    - ``"in_transaction"``: reading ``in_transaction`` raises ProgrammingError;
    - ``"rollback"``: ``rollback()`` raises sqlite3.OperationalError;
    - ``"close"``: ``close()`` really closes, then reports a failure.
    """

    def __init__(
        self,
        real: sqlite3.Connection,
        fault: str,
        *,
        insert_failure: BaseException | None = None,
        commit: str | None = None,
        failed: bool = False,
    ) -> None:
        self.real = real
        self.fault = fault
        self.insert_failure = insert_failure
        self.commit_mode = commit
        self.failed = failed
        self.writing = False
        self.closes = 0

    def __getattr__(self, name: str) -> Any:
        return getattr(self.real, name)

    def _fail(self) -> None:
        if self.writing and not self.failed:
            self.failed = True
            if self.fault == "closed":
                self.real.close()

    def execute(self, sql: str, *parameters: Any) -> sqlite3.Cursor:
        if sql == "BEGIN IMMEDIATE":
            self.writing = True
        elif self.writing and self.insert_failure is not None and sql.startswith("INSERT INTO"):
            failure, self.insert_failure = self.insert_failure, None
            self._fail()
            raise failure
        return self.real.execute(sql, *parameters)

    def commit(self) -> None:
        if self.writing and self.commit_mode == "lost-before":
            self._fail()
            raise sqlite3.OperationalError("test: connection lost at COMMIT")
        self.real.commit()
        if self.writing and self.commit_mode == "lost-after":
            self._fail()
            raise sqlite3.OperationalError("test: acknowledgment lost after COMMIT")

    @property
    def in_transaction(self) -> bool:
        self._fail()
        if self.failed and self.fault == "in_transaction":
            raise sqlite3.ProgrammingError("test: connection state cannot be inspected")
        return self.real.in_transaction

    def rollback(self) -> None:
        self._fail()
        if self.failed and self.fault == "rollback":
            raise sqlite3.OperationalError("test: rollback failed")
        self.real.rollback()

    def close(self) -> None:
        self.closes += 1
        self.real.close()
        if self.failed and self.fault == "close":
            raise sqlite3.OperationalError("test: close failed")


def _faulty_connections(
    monkeypatch: pytest.MonkeyPatch, fault: str, *, only: int | None = None, **write: Any
) -> list[_Faulty]:
    """Test-only: adapter write connections become ``_Faulty`` (only the ``only``-th if given)."""
    real_connect = adapter_module._connect
    made: list[_Faulty] = []
    opened = {"n": 0}

    def connect(database_path: str) -> Any:
        opened["n"] += 1
        if only is not None and opened["n"] != only:
            return real_connect(database_path)
        connection = _Faulty(real_connect(database_path), fault, **write)
        made.append(connection)
        return connection

    monkeypatch.setattr(adapter_module, "_connect", connect)
    return made


def _is_closed(connection: sqlite3.Connection) -> bool:
    try:
        connection.execute("SELECT 1")
    except sqlite3.ProgrammingError:
        return True
    return False


def _no_cleanup_error(error: BaseException) -> None:
    """No secondary cleanup error appears anywhere in the reported chain."""
    seen: BaseException | None = error
    while seen is not None:
        assert not isinstance(seen, sqlite3.ProgrammingError), repr(seen)
        assert "close failed" not in str(seen) and "rollback failed" not in str(seen), repr(seen)
        seen = seen.__cause__ or seen.__context__


def _rows(database: Path, *tables: str) -> dict[str, list[tuple[object, ...]]]:
    with closing(sqlite3.connect(database)) as connection:
        existing = {
            row[0]
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        return {
            table: (
                connection.execute(f"SELECT * FROM {table} ORDER BY 1").fetchall()  # noqa: S608
                if table in existing
                else []
            )
            for table in tables
        }


_TABLES = (_CONFIGURATIONS, _DEFINITIONS, _AUDITS, _STEPS)


def _assert_released(database: Path) -> None:
    """No transaction or lock survives: a fresh connection takes the exclusive lock at once."""
    with closing(sqlite3.connect(database, timeout=0, isolation_level=None)) as connection:
        connection.execute("BEGIN EXCLUSIVE")
        connection.rollback()
        assert connection.execute("PRAGMA integrity_check").fetchall() == [("ok",)]
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []


def _write_faults(primary: str) -> dict[str, Any]:
    if primary == "storage-refused":
        return {"insert_failure": sqlite3.OperationalError("test: refused before COMMIT")}
    if primary == "interrupted":
        return {"insert_failure": _Interrupted()}
    if primary.startswith("commit-"):
        return {"commit": primary.removeprefix("commit-")}
    return {}


def _assert_outcome(
    error: BaseException, expected: type[BaseException], primary: str, write: dict[str, Any]
) -> None:
    assert type(error) is expected  # the exact outcome, never a sibling or a cleanup error
    _no_cleanup_error(error)
    if primary == "storage-refused":
        assert error.__cause__ is write["insert_failure"]
    if primary.startswith("commit-"):
        assert isinstance(error.__cause__, sqlite3.OperationalError)
        assert "test:" in str(error.__cause__)


def _added(before: dict[str, list[Any]], after: dict[str, list[Any]]) -> dict[str, int]:
    for table, rows in before.items():
        assert after[table][: len(rows)] == rows or set(rows) <= set(after[table]), table
    return {table: len(after[table]) - len(rows) for table, rows in before.items()}


# ----------------------------------------------------------------------
# _write through ``record``: every primary outcome survives every fault
# ----------------------------------------------------------------------

_RECORD_PRIMARIES = {
    # name: (expected error, durable (configurations, definitions, audits) added)
    "configuration-conflict": (FuturesTradeRecommendationConfigurationConflictError, (0, 0, 0)),
    "identity-conflict": (FuturesTradeRecommendationConflictError, (0, 0, 0)),
    "exclusivity": (FuturesTradeRecommendationExclusivityError, (0, 0, 0)),
    "storage-refused": (FuturesTradeRecommendationStorageError, (0, 0, 0)),
    "interrupted": (_Interrupted, (0, 0, 0)),
    "commit-lost-before": (FuturesTradeRecommendationStorageError, (0, 0, 0)),
    "commit-lost-after": (FuturesTradeRecommendationStorageError, (0, 1, 1)),
}


def _record_attempt(database: Path, primary: str) -> Any:
    """Set up what ``primary`` needs; return the recording attempt to make."""
    recs._record(database, recs._recommendation(recs._BANK_OCT))  # pre-existing evidence
    if primary == "configuration-conflict":
        reused = FuturesTradeRecommendationGenerationProfile(
            recs._PROFILE.version, 7, recs._POLICY.version
        )
        return lambda: recs._record(
            database, recs._recommendation(recs._OCT, profile=reused), profile=reused
        )
    if primary == "identity-conflict":
        recs._record(database, recs._recommendation(issued_at="2026-10-07T11:00:00Z"))
        return lambda: recs._record(
            database, recs._recommendation(issued_at="2026-10-07T12:00:00Z")
        )
    if primary == "exclusivity":
        recs._record(database, recs._recommendation(recs._OCT))  # NIFTY is now open
        return lambda: recs._record(database, recs._recommendation(recs._NOV))
    return lambda: recs._record(database, recs._recommendation(recs._OCT))


@pytest.mark.parametrize("fault", _FAULTS)
@pytest.mark.parametrize("primary", sorted(_RECORD_PRIMARIES))
def test_l9f_record_keeps_its_typed_outcome(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, primary: str, fault: str
) -> None:
    database = tmp_path / "recommendations.sqlite3"
    attempt = _record_attempt(database, primary)
    before = _rows(database, *_TABLES)
    expected, durable = _RECORD_PRIMARIES[primary]
    write = _write_faults(primary)
    connections = _faulty_connections(monkeypatch, fault, **write)
    with pytest.raises(expected) as raised:
        attempt()
    _assert_outcome(raised.value, expected, primary, write)
    (written,) = connections
    assert written.closes == 1 and _is_closed(written.real)  # closed or discarded, exactly once
    added = _added(before, _rows(database, *_TABLES))
    assert (added[_CONFIGURATIONS], added[_DEFINITIONS], added[_AUDITS]) == durable
    assert added[_STEPS] == 0  # no definition without its audit, nothing partial
    _assert_released(database)


# ----------------------------------------------------------------------
# _write through ``record_audit``
# ----------------------------------------------------------------------

_AUDIT_PRIMARIES = {
    "audit-conflict": (FuturesRecommendationGenerationAuditConflictError, 0),
    "storage-refused": (FuturesTradeRecommendationStorageError, 0),
    "interrupted": (_Interrupted, 0),
    "commit-lost-before": (FuturesTradeRecommendationStorageError, 0),
    "commit-lost-after": (FuturesTradeRecommendationStorageError, 1),
}


def _existing_returned(recommendation: Any, **overrides: Any) -> Any:
    """A standalone EXISTING_RETURNED audit of an attempt that found ``recommendation``."""
    return recording_audit(
        recommendation,
        profile=recs._PROFILE,
        recorded=(),
        existing=(recommendation.identity,),
        **overrides,
    )


@pytest.mark.parametrize("fault", _FAULTS)
@pytest.mark.parametrize("primary", sorted(_AUDIT_PRIMARIES))
def test_l9f_record_audit_keeps_its_typed_outcome(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, primary: str, fault: str
) -> None:
    database = tmp_path / "recommendations.sqlite3"
    recommendation = recs._recommendation(recs._OCT)
    recs._record(database, recommendation)
    audit = _existing_returned(recommendation)
    if primary == "audit-conflict":
        SQLiteFuturesRecommendationGenerationAuditStore(database).record_audit(audit)
        audit = _existing_returned(recommendation, proposed=())  # same identity, other content
    before = _rows(database, *_TABLES)
    expected, durable = _AUDIT_PRIMARIES[primary]
    write = _write_faults(primary)
    connections = _faulty_connections(monkeypatch, fault, **write)
    with pytest.raises(expected) as raised:
        SQLiteFuturesRecommendationGenerationAuditStore(database).record_audit(audit)
    _assert_outcome(raised.value, expected, primary, write)
    (written,) = connections
    assert written.closes == 1 and _is_closed(written.real)
    added = _added(before, _rows(database, *_TABLES))
    assert added == {_CONFIGURATIONS: 0, _DEFINITIONS: 0, _AUDITS: durable, _STEPS: 0}
    _assert_released(database)


# ----------------------------------------------------------------------
# record_progress: every primary outcome survives every fault
# ----------------------------------------------------------------------

_PROGRESS_PRIMARIES = {
    "lifecycle-conflict": (FuturesTradeRecommendationLifecycleConflictError, 0),
    "lifecycle-evidence": (FuturesTradeRecommendationLifecycleEvidenceError, 0),
    "storage-invalid-data": (FuturesTradeRecommendationStorageError, 0),
    "storage-refused": (FuturesTradeRecommendationStorageError, 0),
    "interrupted": (_Interrupted, 0),
    "commit-lost-before": (FuturesTradeRecommendationStorageError, 0),
    "commit-lost-after": (FuturesTradeRecommendationStorageError, 2),
}


@pytest.mark.parametrize("fault", _FAULTS)
@pytest.mark.parametrize("primary", sorted(_PROGRESS_PRIMARIES))
def test_l9f_record_progress_keeps_its_typed_outcome(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, primary: str, fault: str
) -> None:
    database = tmp_path / "northstar.sqlite3"
    initial = life._recorded(database)
    after_8 = life._advance(database, initial, life._observe(8))  # pre-existing history
    previous, steps = after_8, (life._observe(9), life._observe(10, life._ENTRY))
    life._candles(database, *steps)
    if primary == "lifecycle-conflict":
        previous = initial  # stale: the stored lifecycle has moved on
        steps = (life._observe(9),)
    elif primary == "lifecycle-evidence":
        steps = (life._observe(11),)  # no stored candle for it
    elif primary == "storage-invalid-data":
        # TEST-ONLY SIMULATION: the stored candle cannot be decoded (corrupt market data).
        def corrupt(*_args: object) -> Any:
            raise ValueError("test: corrupt stored candle")

        monkeypatch.setattr(adapter_module, "load_candle", corrupt)
    before = _rows(database, *_TABLES)
    expected, durable = _PROGRESS_PRIMARIES[primary]
    write = _write_faults(primary)
    connections = _faulty_connections(monkeypatch, fault, **write)
    with pytest.raises(expected) as raised:
        life._progress(database, previous, *steps)
    _assert_outcome(raised.value, expected, primary, write)
    (written,) = connections
    assert written.closes == 1 and _is_closed(written.real)
    added = _added(before, _rows(database, *_TABLES))
    assert added == {_CONFIGURATIONS: 0, _DEFINITIONS: 0, _AUDITS: 0, _STEPS: durable}
    expected_lifecycle = life._apply(after_8, *steps) if durable else after_8
    assert life._stored(database) == expected_lifecycle  # replay from a fresh connection
    _assert_released(database)


# ----------------------------------------------------------------------
# The rollback helper; closing after a confirmed commit; ordinary paths
# ----------------------------------------------------------------------


def test_l9f_rollback_never_raises_on_a_connection_that_cannot_be_inspected(
    tmp_path: Path,
) -> None:
    database = tmp_path / "recommendations.sqlite3"
    recs._record(database, recs._recommendation())
    closed = sqlite3.connect(database, isolation_level=None)
    closed.execute("BEGIN IMMEDIATE")
    closed.close()  # SQLite rolls the open transaction back on close
    with pytest.raises(sqlite3.ProgrammingError):
        _ = closed.in_transaction  # the secondary error the handlers used to let escape
    assert adapter_module._rollback(closed) is None  # nothing raised, nothing claimed
    with closing(sqlite3.connect(database, isolation_level=None)) as live:
        for fault in ("in_transaction", "rollback"):
            stand_in = _Faulty(live, fault, failed=True)
            live.execute("BEGIN IMMEDIATE")
            assert adapter_module._rollback(stand_in) is None  # type: ignore[arg-type]
            live.rollback()
        live.execute("BEGIN IMMEDIATE")  # an inspectable connection is rolled back as before
        adapter_module._rollback(live)
        assert not live.in_transaction
        adapter_module._rollback(live)  # no transaction: nothing to do
        assert not live.in_transaction
    _assert_released(database)


@pytest.mark.parametrize("operation", ["record", "record_audit", "record_progress"])
def test_l9f_a_close_failure_after_a_confirmed_commit_is_not_suppressed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, operation: str
) -> None:
    """The existing contract: after a confirmed COMMIT a close failure is reported as itself."""
    database = tmp_path / "northstar.sqlite3"
    initial = life._recorded(database)
    recommendation = initial.recommendation
    life._candles(database, life._observe(8))
    before = _rows(database, *_TABLES)
    connections = _faulty_connections(monkeypatch, "close", failed=True)
    with pytest.raises(sqlite3.OperationalError, match="close failed"):
        if operation == "record":
            recs._record(database, recs._recommendation(recs._BANK_OCT))
        elif operation == "record_audit":
            SQLiteFuturesRecommendationGenerationAuditStore(database).record_audit(
                _existing_returned(recommendation)
            )
        else:
            life._progress(database, initial, life._observe(8))
    (written,) = connections
    assert written.closes == 1
    added = _added(before, _rows(database, *_TABLES))  # the commit is durable, never re-labelled
    assert sum(added.values()) >= 1
    _assert_released(database)


def test_l9f_every_write_connection_is_closed_on_ordinary_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = tmp_path / "northstar.sqlite3"
    connections = _faulty_connections(monkeypatch, "none")
    initial = life._recorded(database)  # record
    recommendation = initial.recommendation
    assert life._record(database, recommendation) == 1  # idempotent replay
    with pytest.raises(FuturesTradeRecommendationExclusivityError):
        life._record(database, life._recommendation(life._NOV))
    audits = SQLiteFuturesRecommendationGenerationAuditStore(database)
    assert audits.record_audit(_existing_returned(recommendation)) == 1
    assert audits.record_audit(_existing_returned(recommendation)) == 0  # idempotent
    after = life._advance(database, initial, life._observe(8), life._observe(9))  # progress
    assert life._progress(database, initial, life._observe(8), life._observe(9)) == 0
    with pytest.raises(FuturesTradeRecommendationLifecycleConflictError):
        life._progress(database, initial, life._observe(8))
    assert len(connections) == 8
    assert all(c.closes == 1 and _is_closed(c.real) for c in connections)
    assert life._stored(database) == after  # restart: a fresh repository replays it exactly
    _assert_released(database)


# ----------------------------------------------------------------------
# F. The real Application use cases reach their existing outcomes
# ----------------------------------------------------------------------


def _prepared(database: Path) -> acceptance.Database:
    """The acceptance database with reviewed history and research, before generation."""
    db = acceptance.Database(database)
    history = acceptance._history()
    db.store_bars(*history)
    db.approve(*history)
    SQLiteFuturesForwardResearchRecordStore(db.path).store(
        (acceptance._research(tuple(bar for _, bar in history)),)
    )
    return db


def _generate(db: acceptance.Database, store: SQLiteFuturesTradeRecommendationStore) -> Any:
    """The acceptance generation, with the recommendation store supplied."""
    return GenerateFuturesTradeRecommendationsUseCase(
        SQLiteFuturesForwardResearchRecordRepository(db.path),
        SQLiteFuturesHistoricalMarketDataRepository(db.path),
        acceptance._CALENDAR,
        acceptance.OperatorApprovedFuturesDailyBarFinalityPolicy(acceptance._DECISION_DATE),
        acceptance.Cutoffs(),
        db.repository(),
        store,
        SQLiteFuturesRecommendationGenerationAuditStore(db.path),
    ).execute(
        contract=acceptance._OCT,
        strategy=acceptance._STRATEGY,
        policy=acceptance._POLICY,
        profile=acceptance._PROFILE,
        issued_at=PointInTime("2026-10-07T12:00:00Z"),
        reviewed_candles=acceptance._approvals(db.approved),
    )


class _CompetitorFirst(SQLiteFuturesTradeRecommendationStore):
    """Test-only coordination: another recording commits the same batch first."""

    def record(self, recommendations, *, policy, profile, audit):  # type: ignore[no-untyped-def]
        competitor = recording_audit(*recommendations, profile=profile)
        assert competitor != audit  # the same batch, claimed by another recording's audit
        SQLiteFuturesTradeRecommendationStore(self._database_path).record(
            recommendations, policy=policy, profile=profile, audit=competitor
        )
        return super().record(recommendations, policy=policy, profile=profile, audit=audit)


@pytest.mark.parametrize("fault", ["closed", "in_transaction", "rollback", "close"])
def test_l9f_generation_returns_the_race_winner_on_a_same_identity_conflict(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    db = _prepared(tmp_path / "northstar.sqlite3")
    _faulty_connections(monkeypatch, fault, only=2)  # the generation's own record (2nd write)
    result = _generate(db, _CompetitorFirst(db.path))  # no untyped error escapes
    assert result.recorded == ()
    (winner,) = result.existing  # the race winner, re-read and verified
    assert db.repository().get_recommendation(winner.identity) == winner
    assert len(db.rows(_DEFINITIONS)) == 1  # nothing of the losing batch was kept
    _assert_released(db.path)


@pytest.mark.parametrize("fault", ["closed", "in_transaction", "rollback", "close"])
def test_l9f_generation_propagates_the_typed_storage_failure_and_can_run_again(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    db = _prepared(tmp_path / "northstar.sqlite3")
    refused = sqlite3.OperationalError("test: refused before COMMIT")
    _faulty_connections(monkeypatch, fault, only=1, insert_failure=refused)
    with pytest.raises(FuturesTradeRecommendationStorageError) as raised:
        db.generate()
    assert raised.value.__cause__ is refused
    _no_cleanup_error(raised.value)
    assert db.rows(_DEFINITIONS) == [] and db.rows(_AUDITS) == []  # nothing partial
    monkeypatch.setattr(adapter_module, "_connect", _real_connect)
    (recommendation,) = db.generate().recorded  # a fresh run records it, once
    assert len(db.rows(_DEFINITIONS)) == 1
    assert db.repository().get_recommendation(recommendation.identity) == recommendation


_real_connect = adapter_module._connect


class _CompetingProgress(SQLiteFuturesTradeRecommendationLifecycleStore):
    """Test-only coordination: another monitor records the first step first."""

    def record_progress(self, previous, current, *, steps):  # type: ignore[no-untyped-def]
        first = life._apply(previous, steps[0])
        recorded = SQLiteFuturesTradeRecommendationLifecycleStore(self._database_path)
        assert recorded.record_progress(previous, first, steps=steps[:1]) == 1
        return super().record_progress(previous, current, steps=steps)


def _monitor(db: acceptance.Database, store: Any, as_of: str, final_through: date) -> Any:
    return MonitorFuturesTradeRecommendationsUseCase(
        db.repository(),
        store,
        SQLiteFuturesHistoricalMarketDataRepository(db.path),
        acceptance._CALENDAR,
        acceptance.OperatorApprovedFuturesDailyBarFinalityPolicy(final_through),
    ).execute(
        product=acceptance._NIFTY,
        as_of=PointInTime(as_of),
        reviewed_candles=acceptance._approvals(db.approved),
        coverage_declarations=(),
    )


@pytest.mark.parametrize("fault", ["closed", "in_transaction", "rollback", "close"])
def test_l9f_monitoring_reports_concurrent_progress_on_a_lifecycle_conflict(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    db = acceptance.Database(tmp_path / "northstar.sqlite3")
    _, window = acceptance._generated(db)
    pairs = [(session, acceptance._bar(session, *acceptance._QUIET)) for session in window[:2]]
    db.store_bars(*pairs)
    db.approve(*pairs)
    _faulty_connections(monkeypatch, fault, only=2)  # the monitor's own write (2nd)
    store = _CompetingProgress(db.path)
    result = _monitor(db, store, acceptance._after(window[1]), window[1].trading_date)
    (outcome,) = result.outcomes  # no untyped error escapes
    assert outcome.stop_reason is _STOP.CONCURRENT_PROGRESS and outcome.steps_recorded == 0
    assert len(db.rows(_STEPS)) == 1  # only the competitor's step; nothing duplicated
    _assert_released(db.path)


@pytest.mark.parametrize("fault", ["closed", "in_transaction", "rollback", "close"])
def test_l9f_monitoring_propagates_the_typed_storage_failure_and_resumes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    db = acceptance.Database(tmp_path / "northstar.sqlite3")
    _, window = acceptance._generated(db)
    pairs = [(session, acceptance._bar(session, *acceptance._QUIET)) for session in window[:2]]
    db.store_bars(*pairs)
    db.approve(*pairs)
    refused = sqlite3.OperationalError("test: refused before COMMIT")
    _faulty_connections(monkeypatch, fault, only=1, insert_failure=refused)
    with pytest.raises(FuturesTradeRecommendationStorageError) as raised:
        db.monitor(acceptance._after(window[1]), window[1].trading_date)
    assert raised.value.__cause__ is refused
    _no_cleanup_error(raised.value)
    assert db.rows(_STEPS) == []  # no partial steps
    monkeypatch.setattr(adapter_module, "_connect", _real_connect)
    resumed = db.monitor(acceptance._after(window[1]), window[1].trading_date)
    assert resumed.outcomes[0].steps_recorded == 2 and len(db.rows(_STEPS)) == 2
    _assert_released(db.path)
