"""L9-C: EI-2B candle-evidence cleanup never replaces a write's typed outcome.

Regression tests for the EI-2B SQLite journal and snapshot store's shared
write transaction (``_write``), following W2 (EI-4D-B) and L9-T (EI-3C).
Before L9-C its exception handlers read ``in_transaction`` and called
``rollback()`` unguarded, and its ``finally`` closed the connection unguarded.
On a connection that failed or closed, reading ``in_transaction`` raises
sqlite3.ProgrammingError; a failing rollback or close raises
sqlite3.OperationalError. Any of them replaced the typed outcome the
Application acts on: the storage error (ingestion FAILED_PERSISTENCE), an
ingestion or source-identity conflict (REJECTED_*), a stale snapshot (bounded
retry, then STALE_AFTER_RETRIES), and the snapshot evidence and conflict
errors.

The adapter's existing contract is kept, not redefined: a COMMIT failure is
still the storage error (there is no commit-unconfirmed type in this port --
the ingestion workflow already treats FAILED_PERSISTENCE as "not confirmed",
never as "not committed"), and after a confirmed COMMIT a close failure is
still reported as itself.

Every test uses real temporary evidence databases through the real EI-2B
journal and snapshot store and the real EI-2C ingestion and issuance use
cases (tests/equity_intraday_research_timeline_fixtures.py). Faults go through
the adapter's ``_connect`` seam: ``_Faulty`` is a test-only stand-in that
delegates every call to the real connection. On a write connection it can
refuse an INSERT, interrupt it, or lose the COMMIT before or after it takes
effect; and once cleanup begins -- the first ``in_transaction`` or
``rollback`` access on a write connection, which only failure paths make --
it applies the cleanup fault: really closing the connection, or making
``in_transaction``, ``rollback()`` or ``close()`` raise. Every claim about what
is durable is checked on a fresh connection. Nothing sleeps.
"""

from __future__ import annotations

import dataclasses
import socket
import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Any

import pytest
from northstar_application.application_services import (
    EquityCandleAppendOutcome,
    EquityCandleEvidenceSnapshot,
    EquityCandleIngestionOutcome,
    EquityResearchEvidenceOutcome,
)
from northstar_application.ports import (
    EquityCandleEvidenceUnavailableError,
    EquityCandleIngestionConflictError,
    EquityCandleSnapshotConflictError,
    EquityCandleSnapshotEvidenceError,
    EquityCandleSnapshotStaleError,
    EquityCandleSourceIdentityConflictError,
)

from northstar_infrastructure.persistence import (
    EquityCandleEvidenceStorageError,
    SQLiteEquityCandleEvidenceJournal,
    SQLiteEquityCandleEvidenceSnapshotStore,
)
from northstar_infrastructure.persistence import (
    sqlite_equity_candle_evidence as adapter_module,
)
from tests.equity_intraday_research_timeline_fixtures import (
    DAY,
    M_AND_M,
    Clock,
    Harness,
    events,
    interval_end,
    ist,
    raw,
)

_JOURNAL = "equity_candle_journal"
_SNAPSHOTS = "equity_candle_evidence_snapshots"
_ENTRIES = "equity_candle_evidence_snapshot_entries"
_TABLES = (_JOURNAL, _SNAPSHOTS, _ENTRIES)
_ING = EquityCandleIngestionOutcome
_FAULTS = ["none", "closed", "in_transaction", "rollback", "close"]


@pytest.fixture(autouse=True)
def _refuse_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("L9-C reliability tests must not open network connections.")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)


class _Interrupted(BaseException):
    """Test-only: stands for the process being stopped mid-write."""


class _Faulty:
    """Test-only stand-in for an adapter connection: delegates to a real one.

    Write behaviour (only on a connection that began ``BEGIN IMMEDIATE``):

    - ``insert_failure``: the first INSERT raises it instead of executing;
    - ``commit``: ``"lost-before"`` raises at COMMIT without committing;
      ``"lost-after"`` really commits, then raises (the acknowledgment is lost).

    Cleanup fault, applied once cleanup begins (``failed``):

    - ``"none"``: nothing more goes wrong;
    - ``"closed"``: the real connection is really closed (lost), so SQLite
      rolls an uncommitted transaction back and ``in_transaction`` raises
      sqlite3.ProgrammingError;
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

    def _cleanup_begins(self) -> None:
        if self.writing and not self.failed:
            self.failed = True
            if self.fault == "closed":
                self.real.close()

    def execute(self, sql: str, *parameters: Any) -> sqlite3.Cursor:
        if sql == "BEGIN IMMEDIATE":
            self.writing = True
        elif self.writing and self.insert_failure is not None and sql.startswith("INSERT INTO"):
            failure, self.insert_failure = self.insert_failure, None
            raise failure
        return self.real.execute(sql, *parameters)

    def commit(self) -> None:
        if self.writing and self.commit_mode == "lost-before":
            raise sqlite3.OperationalError("test: connection lost at COMMIT")
        self.real.commit()
        if self.writing and self.commit_mode == "lost-after":
            raise sqlite3.OperationalError("test: acknowledgment lost after COMMIT")

    @property
    def in_transaction(self) -> bool:
        self._cleanup_begins()
        if self.failed and self.fault == "in_transaction":
            raise sqlite3.ProgrammingError("test: connection state cannot be inspected")
        return self.real.in_transaction

    def rollback(self) -> None:
        self._cleanup_begins()
        if self.failed and self.fault == "rollback":
            raise sqlite3.OperationalError("test: rollback failed")
        self.real.rollback()

    def close(self) -> None:
        self.closes += 1
        self.real.close()
        if self.failed and self.fault == "close":
            raise sqlite3.OperationalError("test: close failed")


def _faulty_connections(monkeypatch: pytest.MonkeyPatch, fault: str, **write: Any) -> list[_Faulty]:
    """Test-only: every adapter connection is a ``_Faulty`` around a real one."""
    real_connect = adapter_module._connect
    made: list[_Faulty] = []

    def connect(database_path: str) -> Any:
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


def _rows(database: Path) -> dict[str, list[tuple[object, ...]]]:
    with closing(raw(database)) as connection:
        return {
            table: connection.execute(f"SELECT * FROM {table} ORDER BY 1").fetchall()  # noqa: S608
            for table in _TABLES
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


def _unissued_snapshot(tmp_path: Path, as_of: Any = None) -> EquityCandleEvidenceSnapshot:
    """A real snapshot of the synthetic morning, issued only in a scratch database."""
    return _morning(tmp_path / "scratch.sqlite3").issue(as_of or ist(9, 36))


# ----------------------------------------------------------------------
# Primary outcomes, each produced by real evidence or a real-seam fault
# ----------------------------------------------------------------------

_JOURNAL_PRIMARIES = {
    # name: (expected error, durable journal rows added)
    "ingestion-conflict": (EquityCandleIngestionConflictError, 0),
    "source-identity-conflict": (EquityCandleSourceIdentityConflictError, 0),
    "storage-refused": (EquityCandleEvidenceStorageError, 0),
    "interrupted": (_Interrupted, 0),
    "commit-lost-before": (EquityCandleEvidenceStorageError, 0),
    "commit-lost-after": (EquityCandleEvidenceStorageError, 1),
}
_SNAPSHOT_PRIMARIES = {
    # name: (expected error, durable snapshot rows added)
    "snapshot-stale": (EquityCandleSnapshotStaleError, 0),
    "snapshot-evidence": (EquityCandleSnapshotEvidenceError, 0),
    "snapshot-conflict": (EquityCandleSnapshotConflictError, 0),
    "storage-refused": (EquityCandleEvidenceStorageError, 0),
    "interrupted": (_Interrupted, 0),
    "commit-lost-before": (EquityCandleEvidenceStorageError, 0),
    "commit-lost-after": (EquityCandleEvidenceStorageError, 1),
}


def _write_faults(primary: str) -> dict[str, Any]:
    if primary == "storage-refused":
        return {"insert_failure": sqlite3.OperationalError("test: refused before COMMIT")}
    if primary == "interrupted":
        return {"insert_failure": _Interrupted()}
    if primary.startswith("commit-"):
        return {"commit": primary.removeprefix("commit-")}
    return {}


def _journal_record(primary: str) -> Any:
    source = events()
    if primary == "ingestion-conflict":  # the same ingestion id with other content
        return source.observation(interval_end(1), prices=("1", "2", "1", "2"))
    if primary == "source-identity-conflict":  # bar 1's source identity bound to bar 30
        record = source.observation(interval_end(30))
        return dataclasses.replace(
            record,
            ingestion_id="l9c-contradicting-receipt",
            observation=dataclasses.replace(
                record.observation, source_event_id=source.source_event_id(interval_end(1))
            ),
        )
    return source.observation(interval_end(30))  # a new candle: an ordinary append


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


# ----------------------------------------------------------------------
# A, B, C, D. Every primary outcome survives every cleanup fault
# ----------------------------------------------------------------------


@pytest.mark.parametrize("fault", _FAULTS)
@pytest.mark.parametrize("primary", sorted(_JOURNAL_PRIMARIES))
def test_l9c_journal_append_keeps_its_typed_outcome(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, primary: str, fault: str
) -> None:
    database = tmp_path / "evidence.sqlite3"
    _morning(database)
    before = _rows(database)
    expected, durable = _JOURNAL_PRIMARIES[primary]
    record = _journal_record(primary)
    write = _write_faults(primary)
    connections = _faulty_connections(monkeypatch, fault, **write)
    with pytest.raises(expected) as raised:
        SQLiteEquityCandleEvidenceJournal(database, clock=Clock()).append(record)
    _assert_outcome(raised.value, expected, primary, write)
    (written,) = [c for c in connections if c.writing]
    assert written.closes == 1 and _is_closed(written.real)  # closed or discarded, exactly once
    after = _rows(database)
    assert len(after[_JOURNAL]) == len(before[_JOURNAL]) + durable  # once, or not at all
    assert after[_JOURNAL][: len(before[_JOURNAL])] == before[_JOURNAL]  # history unchanged
    assert after[_SNAPSHOTS] == before[_SNAPSHOTS] and after[_ENTRIES] == before[_ENTRIES]
    _assert_released(database)


@pytest.mark.parametrize("fault", _FAULTS)
@pytest.mark.parametrize("primary", sorted(_SNAPSHOT_PRIMARIES))
def test_l9c_snapshot_issue_keeps_its_typed_outcome(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, primary: str, fault: str
) -> None:
    snapshot = _unissued_snapshot(tmp_path)
    database = tmp_path / "evidence.sqlite3"
    target = _morning(database)  # the identical journal: the snapshot is valid here
    if primary == "snapshot-stale":
        target.correct_bar_2()  # the window gained evidence after the snapshot's watermark
    elif primary == "snapshot-evidence":
        database = tmp_path / "short.sqlite3"
        Harness(database).ingest([events().observation(interval_end(1))])  # below its watermark
    elif primary == "snapshot-conflict":
        # TEST-ONLY SIMULATION: a snapshot id is derived from its content, so another
        # snapshot under the same id is reachable only from a tampered row. The stored
        # read returns another real snapshot under this id to reach the conflict branch.
        other = _morning(tmp_path / "other.sqlite3").issue(ist(9, 41))
        real_stored = adapter_module._stored_snapshot

        def stored(connection: Any, snapshot_id: str) -> Any:
            if snapshot_id == snapshot.snapshot_id:
                return other
            return real_stored(connection, snapshot_id)

        monkeypatch.setattr(adapter_module, "_stored_snapshot", stored)
    before = _rows(database)
    expected, durable = _SNAPSHOT_PRIMARIES[primary]
    write = _write_faults(primary)
    connections = _faulty_connections(monkeypatch, fault, **write)
    with pytest.raises(expected) as raised:
        SQLiteEquityCandleEvidenceSnapshotStore(database).issue(snapshot)
    _assert_outcome(raised.value, expected, primary, write)
    (written,) = [c for c in connections if c.writing]
    assert written.closes == 1 and _is_closed(written.real)
    after = _rows(database)
    assert after[_JOURNAL] == before[_JOURNAL]
    assert len(after[_SNAPSHOTS]) == len(before[_SNAPSHOTS]) + durable
    if durable:  # the snapshot and all its entry rows committed together, once
        listed = len(adapter_module._snapshot_entries(snapshot))
        assert listed > 0 and len(after[_ENTRIES]) == len(before[_ENTRIES]) + listed
    else:
        assert after[_ENTRIES] == before[_ENTRIES]  # no partial snapshot
    _assert_released(database)


# ----------------------------------------------------------------------
# A. The rollback helper on connections that cannot be inspected
# ----------------------------------------------------------------------


def test_l9c_rollback_never_raises_on_a_connection_that_cannot_be_inspected(
    tmp_path: Path,
) -> None:
    database = tmp_path / "evidence.sqlite3"
    _morning(database)
    closed = sqlite3.connect(database, isolation_level=None)
    closed.execute("BEGIN IMMEDIATE")
    closed.close()  # SQLite rolls the open transaction back on close
    with pytest.raises(sqlite3.ProgrammingError):
        _ = closed.in_transaction  # the secondary error the handlers used to let escape
    assert adapter_module._rollback(closed) is None  # nothing raised, nothing claimed
    _assert_released(database)
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


# ----------------------------------------------------------------------
# D. Closing after a confirmed commit; every path closes its connection
# ----------------------------------------------------------------------


@pytest.mark.parametrize("operation", ["append", "issue"])
def test_l9c_a_close_failure_after_a_confirmed_commit_is_not_suppressed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, operation: str
) -> None:
    """The existing contract: after a confirmed COMMIT a close failure is reported as itself."""
    snapshot = _unissued_snapshot(tmp_path)
    database = tmp_path / "evidence.sqlite3"
    _morning(database)
    before = _rows(database)
    connections = _faulty_connections(monkeypatch, "close", failed=True)
    with pytest.raises(sqlite3.OperationalError, match="close failed"):
        if operation == "append":
            SQLiteEquityCandleEvidenceJournal(database, clock=Clock()).append(
                events().observation(interval_end(30))
            )
        else:
            SQLiteEquityCandleEvidenceSnapshotStore(database).issue(snapshot)
    (written,) = [c for c in connections if c.writing]
    assert written.closes == 1
    after = _rows(database)  # the commit is durable: it is never re-labelled
    if operation == "append":
        assert len(after[_JOURNAL]) == len(before[_JOURNAL]) + 1
    else:
        assert len(after[_SNAPSHOTS]) == len(before[_SNAPSHOTS]) + 1
    _assert_released(database)


def test_l9c_every_connection_is_closed_on_ordinary_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = tmp_path / "evidence.sqlite3"
    harness = _morning(database)
    connections = _faulty_connections(monkeypatch, "none")
    journal = SQLiteEquityCandleEvidenceJournal(database, clock=Clock())
    new = events().observation(interval_end(30))
    assert journal.append(new).outcome is EquityCandleAppendOutcome.APPENDED
    assert journal.append(new).outcome is EquityCandleAppendOutcome.ALREADY_APPENDED
    with pytest.raises(EquityCandleIngestionConflictError):
        journal.append(_journal_record("ingestion-conflict"))
    snapshot = harness.issue(ist(9, 36))  # the real issuance workflow, through the stand-ins
    assert SQLiteEquityCandleEvidenceSnapshotStore(database).issue(snapshot) == 0  # idempotent
    harness.correct_bar_2()
    with pytest.raises(EquityCandleSnapshotStaleError):
        SQLiteEquityCandleEvidenceSnapshotStore(database).issue(
            dataclasses.replace(snapshot, issued_at=ist(9, 37))
        )
    assert len([c for c in connections if c.writing]) >= 6
    assert all(c.closes == 1 and _is_closed(c.real) for c in connections)  # reads and writes
    reopened = SQLiteEquityCandleEvidenceSnapshotStore(database).get_snapshot(snapshot.snapshot_id)
    assert reopened == snapshot  # replay from a fresh connection: unchanged
    _assert_released(database)


# ----------------------------------------------------------------------
# E. The real Application workflows reach their existing outcomes
# ----------------------------------------------------------------------


@pytest.mark.parametrize("fault", ["closed", "in_transaction", "rollback", "close"])
@pytest.mark.parametrize("primary", ["storage-refused", "commit-lost-after"])
def test_l9c_ingestion_reports_failed_persistence_and_resumes_without_duplicates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, primary: str, fault: str
) -> None:
    database = tmp_path / "evidence.sqlite3"
    harness = _morning(database)
    before = _rows(database)
    batch = [events().observation(interval_end(30)), events().finality(interval_end(30))]
    real_connect = adapter_module._connect
    armed = {"yes": True}

    def connect(database_path: str) -> Any:  # only the batch's first write is faulty
        if not armed["yes"]:
            return real_connect(database_path)
        armed["yes"] = False
        return _Faulty(real_connect(database_path), fault, **_write_faults(primary))

    monkeypatch.setattr(adapter_module, "_connect", connect)
    report = harness.ingestion.execute(batch)  # no untyped error escapes the workflow
    assert [r.outcome for r in report.results] == [_ING.FAILED_PERSISTENCE, _ING.NOT_ATTEMPTED]
    assert report.resume_from == 0
    detail = report.results[0].detail  # the typed storage error's own message
    assert "refused the write" in detail and "test:" in detail
    assert not any(t in detail for t in ("close failed", "rollback failed", "closed database"))
    durable = primary == "commit-lost-after"  # "not confirmed" is not "not committed"
    assert len(_rows(database)[_JOURNAL]) == len(before[_JOURNAL]) + durable
    resumed = harness.ingestion.execute(batch)  # resubmission resolves the unknown outcome
    first = _ING.IDEMPOTENT_RETRY if durable else _ING.APPENDED_OBSERVATION
    assert [r.outcome for r in resumed.results] == [first, _ING.APPENDED_FINALITY]
    assert len(_rows(database)[_JOURNAL]) == len(before[_JOURNAL]) + 2  # no duplicate
    _assert_released(database)


@pytest.mark.parametrize("fault", ["closed", "in_transaction", "rollback", "close"])
def test_l9c_issuance_retries_a_stale_snapshot_and_reports_stale_after_retries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    database = tmp_path / "evidence.sqlite3"
    harness = _morning(database)
    before = _rows(database)

    # TEST-ONLY SIMULATION: the window gains evidence before every issuance attempt.
    def always_stale(snapshot: Any, current: Any) -> None:
        raise EquityCandleSnapshotStaleError("test: the window gained evidence")

    monkeypatch.setattr(adapter_module, "require_equity_candle_snapshot_current", always_stale)
    connections = _faulty_connections(monkeypatch, fault)
    issued = harness.issuance.execute(M_AND_M, DAY, ist(9, 36), ist(9, 36))
    assert issued.outcome is EquityResearchEvidenceOutcome.STALE_AFTER_RETRIES
    assert issued.snapshot is None  # no fabricated snapshot
    assert len([c for c in connections if c.writing]) == 3  # the bounded retries all ran
    assert _rows(database) == before
    _assert_released(database)


@pytest.mark.parametrize("fault", ["closed", "in_transaction", "rollback", "close"])
def test_l9c_issuance_propagates_the_typed_storage_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    database = tmp_path / "evidence.sqlite3"
    harness = _morning(database)
    before = _rows(database)
    refused = sqlite3.OperationalError("test: refused before COMMIT")
    _faulty_connections(monkeypatch, fault, insert_failure=refused)
    with pytest.raises(EquityCandleEvidenceUnavailableError) as raised:
        harness.issuance.execute(M_AND_M, DAY, ist(9, 36), ist(9, 36))
    assert type(raised.value) is EquityCandleEvidenceStorageError
    assert raised.value.__cause__ is refused
    _no_cleanup_error(raised.value)
    assert _rows(database) == before  # no snapshot, no partial entries
    _assert_released(database)
