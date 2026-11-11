"""L9-T: EI-3C timeline cleanup never replaces an append's typed outcome.

Regression tests for the EI-3C SQLite research-timeline store's transaction
cleanup, following the W2 fix to the EI-4D-B generation store. A connection
that fails or closes around COMMIT cannot even report whether a transaction is
open: reading ``in_transaction`` raises sqlite3.ProgrammingError. Before L9-T
that secondary error escaped the rollback helper -- and a failing close
escaped the write's ``finally`` -- replacing the typed outcome the Application
advance use case acts on: CommitUnconfirmed (which it reconciles by reload),
the storage error, a stale head or a configuration conflict.

Every test uses a real temporary evidence database through the real EI-2B
journal and snapshot store, the real EI-2C issuance workflow and the real
EI-3C advance use case (tests/equity_intraday_research_timeline_fixtures.py).
Faults go through the adapter's module-level seams -- ``_connect``,
``_insert_entry`` and ``_commit``, as the EI-3C suite already uses -- and
``_Faulty``, a test-only stand-in that delegates every call to the real
connection and, once the write has failed, makes the cleanup fail: it really
closes the connection, or makes ``in_transaction``, ``rollback()`` or
``close()`` raise. A commit that "took effect" really committed; every claim
about what is durable is checked on a fresh connection. Nothing sleeps.
"""

from __future__ import annotations

import dataclasses
import socket
import sqlite3
from collections.abc import Callable
from contextlib import closing
from pathlib import Path
from typing import Any

import pytest
from northstar_application.application_services import (
    EquityIntradayResearchTimeline,
    EquityIntradayResearchTimelineAdvanceStatus,
    EquityIntradayResearchTimelineIdentity,
    EquityIntradayResearchTimelineRole,
    ScheduleEquityIntradayResearchUseCase,
    StoredEquityIntradayResearchTimeline,
    rehydrate_equity_intraday_research_timeline,
    stored_equity_intraday_research_timeline,
)
from northstar_application.ports import (
    EquityIntradayResearchTimelineCommitUnconfirmedError,
    EquityIntradayResearchTimelineConfigurationConflictError,
    EquityIntradayResearchTimelineStaleError,
    EquityIntradayResearchTimelineUnavailableError,
)

from northstar_infrastructure.persistence import (
    EquityIntradayResearchTimelineStorageError,
    SQLiteEquityIntradayResearchTimelineStore,
)
from northstar_infrastructure.persistence import (
    sqlite_equity_intraday_research_timeline as adapter_module,
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
    raw,
)

_STATUS = EquityIntradayResearchTimelineAdvanceStatus
_ROLE = EquityIntradayResearchTimelineRole
_TIMELINES = "equity_intraday_research_timelines"
_ENTRIES = "equity_intraday_research_timeline_entries"
_WRONG_HEAD = "eirt1-sha256:" + "0" * 64
_Commit = Callable[[sqlite3.Connection], None]


@pytest.fixture(autouse=True)
def _refuse_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("L9-T reliability tests must not open network connections.")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)


@pytest.fixture
def database(tmp_path: Path) -> Path:
    return tmp_path / "research.sqlite3"


class _Interrupted(BaseException):
    """Test-only: stands for the process being stopped mid-write."""


class _Faulty:
    """Test-only stand-in for an adapter connection: delegates to a real one.

    Once ``fail`` is called -- at the moment the injected primary failure
    happens -- the cleanup that follows meets ``fault``:

    - ``"none"``: nothing more goes wrong;
    - ``"closed"``: the real connection is really closed (lost), so SQLite
      rolls an uncommitted transaction back and ``in_transaction`` raises
      sqlite3.ProgrammingError;
    - ``"in_transaction"``: reading ``in_transaction`` raises ProgrammingError
      (the connection's state cannot be inspected);
    - ``"rollback"``: ``rollback()`` raises sqlite3.OperationalError;
    - ``"close"``: ``close()`` really closes, then reports a failure.

    ``closes`` counts the adapter's close calls; ``writing`` records that the
    connection began a write transaction.
    """

    def __init__(self, real: sqlite3.Connection, fault: str, failed: bool = False) -> None:
        self.real = real
        self.fault = fault
        self.failed = failed
        self.writing = False
        self.closes = 0

    def execute(self, sql: str, *parameters: Any) -> sqlite3.Cursor:
        if sql == "BEGIN IMMEDIATE":
            self.writing = True
        return self.real.execute(sql, *parameters)

    def commit(self) -> None:
        self.real.commit()

    @property
    def in_transaction(self) -> bool:
        if self.failed and self.fault == "in_transaction":
            raise sqlite3.ProgrammingError("test: connection state cannot be inspected")
        return self.real.in_transaction

    def rollback(self) -> None:
        if self.failed and self.fault == "rollback":
            raise sqlite3.OperationalError("test: rollback failed")
        self.real.rollback()

    def close(self) -> None:
        self.closes += 1
        self.real.close()
        if self.failed and self.fault == "close":
            raise sqlite3.OperationalError("test: close failed")

    def fail(self) -> None:
        self.failed = True
        if self.fault == "closed":
            self.real.close()


def _faulty_connections(
    monkeypatch: pytest.MonkeyPatch, fault: str, failed: bool = False
) -> list[_Faulty]:
    """Test-only: every adapter connection is a ``_Faulty`` around a real one."""
    real_connect = adapter_module._connect
    made: list[_Faulty] = []

    def connect(database_path: str) -> Any:
        connection = _Faulty(real_connect(database_path), fault, failed)
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


def _no_programming_error(error: BaseException) -> None:
    """The secondary cleanup error appears nowhere in the reported chain."""
    seen: BaseException | None = error
    while seen is not None:
        assert not isinstance(seen, sqlite3.ProgrammingError), repr(seen)
        assert "close failed" not in str(seen) and "rollback failed" not in str(seen)
        seen = seen.__cause__ or seen.__context__


def _rows(database: Path) -> dict[str, list[tuple[object, ...]]]:
    with closing(raw(database)) as connection:
        return {
            table: connection.execute(f"SELECT * FROM {table} ORDER BY 1").fetchall()  # noqa: S608
            for table in (_TIMELINES, _ENTRIES)
        }


def _assert_released(database: Path) -> None:
    """No transaction or lock survives: a fresh connection takes the exclusive lock at once."""
    with closing(sqlite3.connect(database, timeout=0, isolation_level=None)) as connection:
        connection.execute("BEGIN EXCLUSIVE")
        connection.rollback()
        assert connection.execute("PRAGMA integrity_check").fetchall() == [("ok",)]
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []


def _morning(database: Path) -> Harness:
    harness = Harness(database)
    harness.candles()
    return harness


def _intended(
    harness: Harness, snapshots: list[Any], configuration: Any = CONFIG
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


def _conflicting(harness: Harness, snapshot: Any) -> StoredEquityIntradayResearchTimeline:
    """A first entry under another configuration of the same strategy, listing and date."""
    changed = dataclasses.replace(CONFIG, version="synthetic-ei3c-sqlite/l9t")
    empty = EquityIntradayResearchTimeline(
        EquityIntradayResearchTimelineIdentity(M_AND_M, DAY, changed)
    )
    schedule = ScheduleEquityIntradayResearchUseCase(harness.snapshots, changed).advance(
        empty, [snapshot.snapshot_id]
    )
    return stored_equity_intraday_research_timeline(schedule.timeline)


# ----------------------------------------------------------------------
# Primary failures, injected at the real seams
# ----------------------------------------------------------------------


def _insert_fails(monkeypatch: pytest.MonkeyPatch, error: BaseException) -> None:
    def insert(connection: Any, values: tuple[object, ...]) -> None:
        connection.fail()
        raise error

    monkeypatch.setattr(adapter_module, "_insert_entry", insert)


def _compare_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    """The real compare-and-insert refuses (stale head, other configuration); then cleanup."""
    real = SQLiteEquityIntradayResearchTimelineStore._compare_and_insert

    def compare(connection: Any, *arguments: Any) -> None:
        try:
            real(connection, *arguments)
        except BaseException:
            connection.fail()
            raise

    monkeypatch.setattr(
        SQLiteEquityIntradayResearchTimelineStore, "_compare_and_insert", staticmethod(compare)
    )


def _commit_fails(monkeypatch: pytest.MonkeyPatch, *, takes_effect: bool) -> list[int]:
    """The next COMMIT fails, before or after it really takes effect; returns COMMIT calls."""
    real = adapter_module._commit
    calls: list[int] = []

    def commit(connection: Any) -> None:
        calls.append(1)
        if len(calls) > 1:
            return real(connection)
        if takes_effect:
            real(connection)
        connection.fail()
        raise sqlite3.OperationalError("test: connection lost at COMMIT")

    monkeypatch.setattr(adapter_module, "_commit", commit)
    return calls


_PRIMARIES = [
    "storage",
    "interrupted",
    "stale",
    "configuration-conflict",
    "unconfirmed-not-committed",
    "unconfirmed-committed",
]
_FAULTS = ["none", "closed", "in_transaction", "rollback", "close"]


# ----------------------------------------------------------------------
# A, B, C, D. Every primary outcome survives every cleanup fault
# ----------------------------------------------------------------------


@pytest.mark.parametrize("fault", _FAULTS)
@pytest.mark.parametrize("primary", _PRIMARIES)
def test_l9t_cleanup_never_replaces_the_typed_outcome(
    database: Path, monkeypatch: pytest.MonkeyPatch, primary: str, fault: str
) -> None:
    harness = _morning(database)
    s31, s36 = harness.at(31, 36)
    assert harness.advance([s31]).status is _STATUS.COMMITTED  # pre-existing history
    before = _rows(database)
    record, head = _intended(harness, [s36])
    if primary == "stale":
        head = _WRONG_HEAD
    elif primary == "configuration-conflict":
        record, head = _conflicting(harness, s36), None
    store = SQLiteEquityIntradayResearchTimelineStore(database)

    refused = sqlite3.OperationalError("test: refused before COMMIT")
    commits: list[int] = []
    expected: type[BaseException]
    if primary == "storage":
        _insert_fails(monkeypatch, refused)
        expected = EquityIntradayResearchTimelineStorageError
    elif primary == "interrupted":
        _insert_fails(monkeypatch, _Interrupted())
        expected = _Interrupted
    elif primary in ("stale", "configuration-conflict"):
        _compare_fails(monkeypatch)
        expected = (
            EquityIntradayResearchTimelineStaleError
            if primary == "stale"
            else EquityIntradayResearchTimelineConfigurationConflictError
        )
    else:
        commits = _commit_fails(monkeypatch, takes_effect=primary == "unconfirmed-committed")
        expected = EquityIntradayResearchTimelineCommitUnconfirmedError
    connections = _faulty_connections(monkeypatch, fault)

    with pytest.raises(expected) as raised:
        store.append(record, head)
    error = raised.value
    assert type(error) is expected  # never a subclass or sibling: the exact outcome
    _no_programming_error(error)
    if primary == "storage":
        assert error.__cause__ is refused and "nothing was written" in str(error)
    if primary.startswith("unconfirmed"):
        assert not isinstance(error, EquityIntradayResearchTimelineUnavailableError)
        assert isinstance(error.__cause__, sqlite3.OperationalError)
        assert commits == [1]  # the adapter never retried
    (written,) = [c for c in connections if c.writing]
    assert written.closes == 1 and _is_closed(written.real)  # closed or discarded, exactly once

    # Authoritative inspection: fresh connections. A new stand-in is not failed, so it is
    # transparent, and the patched insert, compare and commit seams are not reached by a load.
    after = _rows(database)
    if primary == "unconfirmed-committed":
        assert len(after[_ENTRIES]) == len(before[_ENTRIES]) + 1  # committed once, not twice
        assert all(row in after[table] for table, rows in before.items() for row in rows)
        loaded = SQLiteEquityIntradayResearchTimelineStore(database).load(
            CONFIG.strategy, M_AND_M, DAY
        )
        assert loaded == record
    else:
        assert after == before  # nothing written; pre-existing rows unchanged
    _assert_released(database)
    check_invariants(database)


# ----------------------------------------------------------------------
# B. The rollback helper on connections that cannot be inspected
# ----------------------------------------------------------------------


def test_l9t_rollback_never_raises_on_a_connection_that_cannot_be_inspected(
    database: Path,
) -> None:
    _morning(database)  # the evidence database exists
    closed = sqlite3.connect(database, isolation_level=None)
    closed.execute("BEGIN IMMEDIATE")
    closed.close()  # SQLite rolls the open transaction back on close
    with pytest.raises(sqlite3.ProgrammingError):
        _ = closed.in_transaction  # the secondary error the helper used to let escape
    assert adapter_module._rollback(closed) is None  # nothing raised, nothing claimed
    _assert_released(database)
    with closing(sqlite3.connect(database, isolation_level=None)) as live:
        stand_in = _Faulty(live, "in_transaction", failed=True)
        live.execute("BEGIN IMMEDIATE")
        assert adapter_module._rollback(stand_in) is None  # type: ignore[arg-type]
        live.rollback()
        failing = _Faulty(live, "rollback", failed=True)
        live.execute("BEGIN IMMEDIATE")
        assert adapter_module._rollback(failing) is None  # type: ignore[arg-type]
        live.rollback()
        # An inspectable connection is still rolled back exactly as before.
        live.execute("BEGIN IMMEDIATE")
        adapter_module._rollback(live)
        assert not live.in_transaction
        adapter_module._rollback(live)  # no transaction: nothing to do
        assert not live.in_transaction
    _assert_released(database)


# ----------------------------------------------------------------------
# C. Closing after a confirmed commit; every write path closes
# ----------------------------------------------------------------------


def test_l9t_a_close_failure_after_a_confirmed_commit_is_not_suppressed(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """W2 parity: after a confirmed COMMIT a close failure is reported as itself."""
    harness = _morning(database)
    (s36,) = harness.at(36)
    record, head = _intended(harness, [s36])
    real_connect = adapter_module._connect
    connections = _faulty_connections(monkeypatch, "close", failed=True)
    with pytest.raises(sqlite3.OperationalError, match="close failed") as raised:
        SQLiteEquityIntradayResearchTimelineStore(database).append(record, head)
    assert not isinstance(raised.value, EquityIntradayResearchTimelineCommitUnconfirmedError)
    (written,) = [c for c in connections if c.writing]
    assert written.closes == 1
    monkeypatch.setattr(adapter_module, "_connect", real_connect)  # plain connections again
    loaded = SQLiteEquityIntradayResearchTimelineStore(database).load(CONFIG.strategy, M_AND_M, DAY)
    assert loaded == record  # the commit is durable; it is never re-labelled unconfirmed
    _assert_released(database)


def test_l9t_every_connection_is_closed_on_ordinary_paths(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = _morning(database)
    s31, s36, s41 = harness.at(31, 36, 41)
    connections = _faulty_connections(monkeypatch, "none")
    assert harness.advance([s31]).status is _STATUS.COMMITTED  # valid write
    assert harness.advance([s36, s41]).status is _STATUS.COMMITTED  # sequential writes
    assert harness.advance([s36]).status is _STATUS.NOTHING_TO_COMMIT  # equivalent retry
    record, head = _intended(harness, [s41])
    with pytest.raises(EquityIntradayResearchTimelineStorageError, match="appends no entry"):
        harness.timelines.append(record, head)
    with pytest.raises(EquityIntradayResearchTimelineStaleError):
        harness.timelines.append(record, _WRONG_HEAD)
    with pytest.raises(EquityIntradayResearchTimelineConfigurationConflictError):
        harness.timelines.append(_conflicting(harness, s31), None)
    assert len([c for c in connections if c.writing]) == 5
    assert all(c.closes == 1 and _is_closed(c.real) for c in connections)  # reads and writes
    assert [row["role"] for row in entry_rows(database)] == [
        _ROLE.NO_SIGNAL.value,
        _ROLE.CANDIDATE.value,
        _ROLE.REPEAT_QUALIFYING.value,
    ]
    restarted = Harness(database, Clock())  # a new "process" over the same file
    assert restarted.timelines.load(CONFIG.strategy, M_AND_M, DAY) == harness.timelines.load(
        CONFIG.strategy, M_AND_M, DAY
    )
    check_invariants(database)
    _assert_released(database)


# ----------------------------------------------------------------------
# E. The real Application advance use case reconciles the typed outcome
# ----------------------------------------------------------------------


def _lost_at_commit(
    monkeypatch: pytest.MonkeyPatch, *, takes_effect: bool, times: int
) -> list[int]:
    """The next ``times`` COMMITs lose their real connection: before or after taking effect."""
    real = adapter_module._commit
    calls: list[int] = []

    def commit(connection: sqlite3.Connection) -> None:
        calls.append(1)
        if len(calls) > times:
            return real(connection)
        if takes_effect:
            real(connection)
        connection.close()
        raise sqlite3.OperationalError("test: connection lost at COMMIT")

    monkeypatch.setattr(adapter_module, "_commit", commit)
    return calls


def test_l9t_e1_a_durable_commit_whose_connection_was_lost_reconciles_as_committed(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = _morning(database)
    s31, s36 = harness.at(31, 36)
    harness.advance([s31])
    commits = _lost_at_commit(monkeypatch, takes_effect=True, times=1)
    result = harness.advance([s36])
    assert result.status is _STATUS.COMMITTED and result.reconciled  # found on reload
    assert result.attempts == 1 and commits == [1]
    assert [row["role"] for row in entry_rows(database)] == ["NO_SIGNAL", "CANDIDATE"]
    again = harness.advance([s36])  # the same evidence again appends nothing
    assert again.status is _STATUS.NOTHING_TO_COMMIT
    assert count(database, _ENTRIES) == 2  # no duplicate
    check_invariants(database)
    _assert_released(database)


def test_l9t_e2_an_absent_commit_whose_connection_was_lost_is_retried_once(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = _morning(database)
    s31, s36 = harness.at(31, 36)
    harness.advance([s31])
    commits = _lost_at_commit(monkeypatch, takes_effect=False, times=1)
    result = harness.advance([s36])
    assert result.status is _STATUS.COMMITTED and not result.reconciled
    assert result.attempts == 2 and commits == [1, 1]  # absent on reload, then committed
    assert [row["role"] for row in entry_rows(database)] == ["NO_SIGNAL", "CANDIDATE"]
    check_invariants(database)
    _assert_released(database)


def test_l9t_e2_commits_that_never_take_effect_exhaust_the_bounded_retries(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = _morning(database)
    s31, s36 = harness.at(31, 36)
    harness.advance([s31])
    before = _rows(database)
    commits = _lost_at_commit(monkeypatch, takes_effect=False, times=99)
    result = harness.advance([s36])
    assert result.status is _STATUS.STALE_AFTER_RETRIES and not result.commit_confirmed
    assert result.timeline is None and not result.reconciled  # no fabricated success
    assert len(commits) == result.attempts == 3  # bounded by the Application, never the adapter
    assert _rows(database) == before
    _assert_released(database)
