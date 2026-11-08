"""W2: EI-4D-B cleanup never replaces a write's typed failure.

Regression tests for the EI-4D-B SQLite generation store's transaction
cleanup. A connection that fails or closes around COMMIT cannot even report
whether a transaction is open: reading ``in_transaction`` raises
sqlite3.ProgrammingError. Before W2 that secondary error escaped the rollback
helper and replaced the typed failure -- NotCommitted, CommitUnconfirmed, or
whatever stopped the write -- with an untyped one the Application recorder
never reconciles.

Every test uses a real temporary evidence database (the EI-4D-B World
fixture). Faults go through the adapter's module-level test seams: ``_commit``
and ``_insert_attempt`` as in the EI-4D-B suite, and ``_close``. A connection
is made unusable by really closing it; a commit that "took effect" really
committed through the real ``_commit``; and every claim about what is durable
is checked on a fresh connection. Nothing sleeps.
"""

from __future__ import annotations

import socket
import sqlite3
from collections.abc import Callable
from contextlib import closing
from pathlib import Path
from typing import Any

import pytest
from northstar_application.application_services import (
    EquityRecommendationGenerationDisposition,
    EquityRecommendationRecordingOutcome,
    equity_recommendation_attempt,
)
from northstar_application.ports import (
    EquityRecommendationGenerationCommitUnconfirmedError,
    EquityRecommendationGenerationNotCommittedError,
    EquityRecommendationGenerationStoreUnavailableError,
    EquityRecommendationStoreOutcome,
)

from northstar_infrastructure.persistence import SQLiteEquityRecommendationGenerationStore
from northstar_infrastructure.persistence import (
    sqlite_equity_recommendation_generation as adapter_module,
)
from tests.equity_intraday_research_timeline_fixtures import count, raw
from tests.equity_recommendation_generation_fixtures import ATTEMPTS, DISPOSITIONS, World, records

_O = EquityRecommendationRecordingOutcome
_W = EquityRecommendationStoreOutcome
_Commit = Callable[[sqlite3.Connection], None]


@pytest.fixture(autouse=True)
def _refuse_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("W2 reliability tests must not open network connections.")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)


@pytest.fixture
def world(tmp_path: Path) -> World:
    return World(tmp_path / "northstar.sqlite3")


class _Interrupted(BaseException):
    """Test-only: stands for the process being stopped mid-write."""


def _rows(database: Path) -> dict[str, list[tuple[object, ...]]]:
    with closing(raw(database)) as connection:
        return {
            table: connection.execute(f"SELECT * FROM {table} ORDER BY 1").fetchall()  # noqa: S608
            for table in (DISPOSITIONS, ATTEMPTS)
        }


def _counts(database: Path) -> tuple[int, int]:
    return count(database, DISPOSITIONS), count(database, ATTEMPTS)


def _assert_released(database: Path) -> None:
    """No transaction or lock survives: a fresh connection takes the exclusive lock at once."""
    with closing(sqlite3.connect(database, timeout=0, isolation_level=None)) as connection:
        connection.execute("BEGIN EXCLUSIVE")
        connection.rollback()
        assert connection.execute("PRAGMA integrity_check").fetchall() == [("ok",)]
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []


def _no_programming_error(error: BaseException) -> None:
    """The secondary closed-connection error appears nowhere in the reported chain."""
    seen: BaseException | None = error
    while seen is not None:
        assert not isinstance(seen, sqlite3.ProgrammingError), repr(seen)
        seen = seen.__cause__ or seen.__context__


def _seed(world: World) -> dict[str, list[tuple[object, ...]]]:
    """Commit an unrelated generation (another key) so pre-existing rows can be compared."""
    disposition, attempt = records(world.not_eligible())
    assert world.store().record_generation(disposition, attempt).outcome is _W.COMMITTED
    return _rows(world.database)


def _commits(
    monkeypatch: pytest.MonkeyPatch, behaviour: Callable[[_Commit, sqlite3.Connection], None]
) -> list[int]:
    """Test-only: the next COMMIT runs ``behaviour`` once; returns the list of COMMIT calls."""
    real = adapter_module._commit
    calls: list[int] = []

    def commit(connection: sqlite3.Connection) -> None:
        calls.append(1)
        if len(calls) == 1:
            return behaviour(real, connection)
        return real(connection)

    monkeypatch.setattr(adapter_module, "_commit", commit)
    return calls


def _connection_lost_before_commit(real: _Commit, connection: sqlite3.Connection) -> None:
    """The connection dies at COMMIT before it takes effect; closing rolls it back."""
    connection.close()
    raise sqlite3.OperationalError("test: connection lost at COMMIT")


def _connection_lost_after_commit(real: _Commit, connection: sqlite3.Connection) -> None:
    """COMMIT really takes effect; then the connection and the acknowledgment are lost."""
    real(connection)
    connection.close()
    raise sqlite3.OperationalError("test: connection lost after COMMIT")


def _generation(world: World) -> Callable[[SQLiteEquityRecommendationGenerationStore], Any]:
    disposition, attempt = records(world.proposed())
    return lambda store: store.record_generation(disposition, attempt)


def _attempt(world: World) -> Callable[[SQLiteEquityRecommendationGenerationStore], Any]:
    only = equity_recommendation_attempt(world.attempt_only())
    return lambda store: store.record_attempt(only)


_OPERATIONS = {"record_generation": (_generation, (1, 1)), "record_attempt": (_attempt, (0, 1))}


# ----------------------------------------------------------------------
# A. The rollback helper on a connection that cannot report its state
# ----------------------------------------------------------------------


def test_w2_a_rollback_never_raises_on_a_connection_that_cannot_be_inspected(
    world: World,
) -> None:
    closed = sqlite3.connect(world.database, isolation_level=None)
    closed.execute("BEGIN IMMEDIATE")
    closed.close()  # SQLite rolls the open transaction back on close
    with pytest.raises(sqlite3.ProgrammingError):
        _ = closed.in_transaction  # the secondary error the helper used to let escape
    assert adapter_module._rollback(closed) is None  # nothing raised, nothing claimed
    _assert_released(world.database)
    # An inspectable connection is still rolled back exactly as before.
    with closing(sqlite3.connect(world.database, isolation_level=None)) as live:
        live.execute("BEGIN IMMEDIATE")
        adapter_module._rollback(live)
        assert not live.in_transaction
        adapter_module._rollback(live)  # no transaction: nothing to do
        assert not live.in_transaction


# ----------------------------------------------------------------------
# A, C, E, H. Failures before COMMIT keep their classification
# ----------------------------------------------------------------------


@pytest.mark.parametrize("operation", sorted(_OPERATIONS))
@pytest.mark.parametrize("lose_connection", [False, True], ids=["open", "connection-lost"])
def test_w2_c_e_a_refusal_before_commit_stays_a_definite_non_commit(
    world: World, monkeypatch: pytest.MonkeyPatch, operation: str, lose_connection: bool
) -> None:
    before = _seed(world)
    build, _ = _OPERATIONS[operation]
    write = build(world)
    refused = sqlite3.OperationalError("test: refused before COMMIT")

    def insert(connection: sqlite3.Connection, values: tuple[object, ...]) -> None:
        if lose_connection:
            connection.close()
        raise refused

    monkeypatch.setattr(adapter_module, "_insert_attempt", insert)
    commits = _commits(monkeypatch, _connection_lost_after_commit)
    with pytest.raises(
        EquityRecommendationGenerationNotCommittedError, match="nothing was written"
    ) as raised:
        write(world.store())
    assert raised.value.__cause__ is refused
    _no_programming_error(raised.value)
    assert commits == []  # COMMIT was never reached
    assert _rows(world.database) == before  # nothing written, pre-existing rows unchanged
    _assert_released(world.database)


@pytest.mark.parametrize("lose_connection", [False, True], ids=["open", "connection-lost"])
def test_w2_a_an_interruption_before_commit_is_never_replaced(
    world: World, monkeypatch: pytest.MonkeyPatch, lose_connection: bool
) -> None:
    before = _seed(world)
    write = _generation(world)

    def insert(connection: sqlite3.Connection, values: tuple[object, ...]) -> None:
        if lose_connection:
            connection.close()
        raise _Interrupted

    monkeypatch.setattr(adapter_module, "_insert_attempt", insert)
    with pytest.raises(_Interrupted) as raised:
        write(world.store())
    _no_programming_error(raised.value)
    assert _rows(world.database) == before
    _assert_released(world.database)


# ----------------------------------------------------------------------
# B, D, H. A connection lost at COMMIT stays CommitUnconfirmed
# ----------------------------------------------------------------------


@pytest.mark.parametrize("operation", sorted(_OPERATIONS))
@pytest.mark.parametrize(
    ("behaviour", "durable"),
    [(_connection_lost_before_commit, False), (_connection_lost_after_commit, True)],
    ids=["not-committed", "committed"],
)
def test_w2_b_d_a_connection_lost_at_commit_stays_unconfirmed(
    world: World,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
    behaviour: Callable[[_Commit, sqlite3.Connection], None],
    durable: bool,
) -> None:
    before = _seed(world)
    build, added = _OPERATIONS[operation]
    write = build(world)
    commits = _commits(monkeypatch, behaviour)
    with pytest.raises(EquityRecommendationGenerationCommitUnconfirmedError) as raised:
        write(world.store())
    assert not isinstance(
        raised.value,
        EquityRecommendationGenerationNotCommittedError
        | EquityRecommendationGenerationStoreUnavailableError,
    )
    assert isinstance(raised.value.__cause__, sqlite3.OperationalError)
    _no_programming_error(raised.value)
    assert commits == [1]  # the adapter never retried
    # Authoritative inspection: a fresh connection, never the failed one.
    seeded = (len(before[DISPOSITIONS]), len(before[ATTEMPTS]))
    expected = tuple(s + a * durable for s, a in zip(seeded, added, strict=True))
    assert _counts(world.database) == expected  # committed once or not at all; no duplicate
    after = _rows(world.database)
    assert all(row in after[table] for table, rows in before.items() for row in rows)
    _assert_released(world.database)
    # Re-offering the same records through a fresh adapter (real COMMIT from now on):
    # idempotent when the first COMMIT took effect, a first commit when it did not.
    replay = write(SQLiteEquityRecommendationGenerationStore(world.database))
    assert replay.outcome is (_W.EQUIVALENT if durable else _W.COMMITTED)
    assert _counts(world.database) == tuple(s + a for s, a in zip(seeded, added, strict=True))


def test_w2_b_the_committed_records_are_exactly_the_offered_ones(
    world: World, monkeypatch: pytest.MonkeyPatch
) -> None:
    disposition, attempt = records(world.proposed())
    _commits(monkeypatch, _connection_lost_after_commit)
    with pytest.raises(EquityRecommendationGenerationCommitUnconfirmedError):
        world.store().record_generation(disposition, attempt)
    fresh = SQLiteEquityRecommendationGenerationStore(world.database)
    assert fresh.load_disposition(disposition.generation_key) == disposition
    assert fresh.load_attempt(attempt.attempt_identity) == attempt
    assert _counts(world.database) == (1, 1)


# ----------------------------------------------------------------------
# J. The EI-4D-A recorder reconciles the corrected typed failure
# ----------------------------------------------------------------------


@pytest.mark.parametrize("attempt_only", [False, True], ids=["generation", "attempt-only"])
@pytest.mark.parametrize(
    ("behaviour", "outcome", "writes"),
    [
        (_connection_lost_after_commit, _O.EQUIVALENT_COMMITTED, 1),
        (_connection_lost_before_commit, _O.COMMITTED, 2),
    ],
    ids=["committed", "not-committed"],
)
def test_w2_j_the_recorder_reconciles_a_connection_lost_at_commit(
    world: World,
    monkeypatch: pytest.MonkeyPatch,
    attempt_only: bool,
    behaviour: Callable[[_Commit, sqlite3.Connection], None],
    outcome: EquityRecommendationRecordingOutcome,
    writes: int,
) -> None:
    audit = world.attempt_only() if attempt_only else world.proposed()
    expected = (0, 1) if attempt_only else (1, 1)
    commits = _commits(monkeypatch, behaviour)
    result = world.recorder().execute(audit)
    assert (result.outcome, result.writes, result.attempt_recorded) == (outcome, writes, True)
    if outcome is _O.EQUIVALENT_COMMITTED:
        assert result.reconciled is True
    assert len(commits) == writes  # bounded by the Application, never by the adapter
    assert _counts(world.database) == expected
    assert world.recorder().execute(audit).outcome is _O.EQUIVALENT_COMMITTED
    assert _counts(world.database) == expected  # no duplicate


class _CompetitorBeforeReload(SQLiteEquityRecommendationGenerationStore):
    """Test-only coordination: another adapter commits before this store's first reload."""

    def __init__(self, database: Path, competitor: Callable[[], None]) -> None:
        super().__init__(database)
        self._competitor: Callable[[], None] | None = competitor

    def load_disposition(
        self, generation_key: str
    ) -> EquityRecommendationGenerationDisposition | None:
        if self._competitor is not None:
            competitor, self._competitor = self._competitor, None
            competitor()
        return super().load_disposition(generation_key)


def test_w2_j_a_competitor_found_on_reload_is_a_conflict_and_never_overwritten(
    world: World, monkeypatch: pytest.MonkeyPatch
) -> None:
    unresolved, unresolved_attempt = records(world.unresolved())

    def competitor() -> None:
        written = SQLiteEquityRecommendationGenerationStore(world.database).record_generation(
            unresolved, unresolved_attempt
        )
        assert written.outcome is _W.COMMITTED

    _commits(monkeypatch, _connection_lost_before_commit)
    result = world.recorder(_CompetitorBeforeReload(world.database, competitor)).execute(
        world.proposed()
    )
    assert (result.outcome, result.reconciled) == (_O.CONTENT_CONFLICT, True)
    assert result.authoritative == unresolved
    fresh = SQLiteEquityRecommendationGenerationStore(world.database)
    assert fresh.load_disposition(unresolved.generation_key) == unresolved
    assert _counts(world.database) == (1, 1)


# ----------------------------------------------------------------------
# G. Closing a connection never masks the outcome; nothing leaks
# ----------------------------------------------------------------------


def _failing_close(monkeypatch: pytest.MonkeyPatch) -> list[sqlite3.Connection]:
    """Test-only: every adapter close really closes, then reports a failure."""
    closed: list[sqlite3.Connection] = []

    def close(connection: sqlite3.Connection) -> None:
        connection.close()
        closed.append(connection)
        raise sqlite3.OperationalError("test: close failed")

    monkeypatch.setattr(adapter_module, "_close", close)
    return closed


@pytest.mark.parametrize(
    ("behaviour", "durable"),
    [(_connection_lost_before_commit, 0), (_connection_lost_after_commit, 1)],
    ids=["not-committed", "committed"],
)
def test_w2_g_a_close_failure_never_masks_commit_unconfirmed(
    world: World,
    monkeypatch: pytest.MonkeyPatch,
    behaviour: Callable[[_Commit, sqlite3.Connection], None],
    durable: int,
) -> None:
    write = _generation(world)
    _commits(monkeypatch, behaviour)
    closed = _failing_close(monkeypatch)
    with pytest.raises(EquityRecommendationGenerationCommitUnconfirmedError) as raised:
        write(world.store())
    assert isinstance(raised.value.__cause__, sqlite3.OperationalError)
    assert "close failed" not in str(raised.value)
    assert len(closed) == 1
    assert _counts(world.database) == (durable, durable)
    _assert_released(world.database)


def test_w2_g_a_close_failure_never_masks_a_definite_non_commit(
    world: World, monkeypatch: pytest.MonkeyPatch
) -> None:
    write = _generation(world)
    refused = sqlite3.OperationalError("test: refused before COMMIT")

    def insert(connection: sqlite3.Connection, values: tuple[object, ...]) -> None:
        raise refused

    monkeypatch.setattr(adapter_module, "_insert_attempt", insert)
    _failing_close(monkeypatch)
    with pytest.raises(EquityRecommendationGenerationNotCommittedError) as raised:
        write(world.store())
    assert raised.value.__cause__ is refused
    assert _counts(world.database) == (0, 0)
    _assert_released(world.database)


def test_w2_g_a_close_failure_after_a_confirmed_commit_is_not_suppressed(
    world: World, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Unchanged behaviour: after a confirmed COMMIT a close failure is reported as itself."""
    disposition, attempt = records(world.proposed())
    _failing_close(monkeypatch)
    with pytest.raises(sqlite3.OperationalError, match="close failed"):
        world.store().record_generation(disposition, attempt)
    fresh = SQLiteEquityRecommendationGenerationStore(world.database)
    assert fresh.load_disposition(disposition.generation_key) == disposition
    assert _counts(world.database) == (1, 1)
    _assert_released(world.database)


def test_w2_g_every_write_path_closes_its_connection(
    world: World, monkeypatch: pytest.MonkeyPatch
) -> None:
    opened: list[sqlite3.Connection] = []
    real_connect = adapter_module._connect

    def connect(database_path: Path) -> sqlite3.Connection:
        connection = real_connect(database_path)
        opened.append(connection)
        return connection

    monkeypatch.setattr(adapter_module, "_connect", connect)
    disposition, attempt = records(world.proposed())
    unresolved, unresolved_attempt = records(world.unresolved())
    only = equity_recommendation_attempt(world.attempt_only())
    store = world.store()
    assert store.record_generation(disposition, attempt).outcome is _W.COMMITTED
    assert store.record_generation(disposition, attempt).outcome is _W.EQUIVALENT
    assert store.record_generation(unresolved, unresolved_attempt).outcome is _W.CONFLICT
    assert store.record_attempt(only).outcome is _W.COMMITTED

    def refuse(connection: sqlite3.Connection, values: tuple[object, ...]) -> None:
        raise sqlite3.OperationalError("test: refused before COMMIT")

    other, other_attempt = records(world.not_eligible())
    with monkeypatch.context() as patch:
        patch.setattr(adapter_module, "_insert_attempt", refuse)
        with pytest.raises(EquityRecommendationGenerationNotCommittedError):
            store.record_generation(other, other_attempt)
    for behaviour in (_connection_lost_before_commit, _connection_lost_after_commit):
        with monkeypatch.context() as patch:
            _commits(patch, behaviour)
            with pytest.raises(EquityRecommendationGenerationCommitUnconfirmedError):
                store.record_generation(other, other_attempt)
    assert len(opened) == 7
    for connection in opened:
        with pytest.raises(sqlite3.ProgrammingError, match="closed"):
            connection.execute("SELECT 1")
    # proposed (1,1); EQUIVALENT adds nothing; the CONFLICT records only the loser's
    # attempt (1,2); attempt-only (1,3); refused and lost-before add nothing; lost-after
    # committed the other generation (2,4).
    assert _counts(world.database) == (2, 4)
    _assert_released(world.database)
