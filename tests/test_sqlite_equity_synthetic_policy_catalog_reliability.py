"""L9-P: policy-catalog cleanup never replaces a publication's typed outcome.

Regression tests for the EI4-D10 SQLite synthetic policy catalog, following
W2, L9-T, L9-C and L9-F. Its rollback helper was already guarded (EI4-D10-S2),
so a failing ``in_transaction`` or ``rollback()`` never masked anything. Two
closes were not:

- ``publish`` closed its connection in a plain ``finally``: a ``close()`` that
  raised while a typed failure propagated -- NotCommitted, CommitUnconfirmed
  (which the publish use case reconciles by reload), an invalid stored binding
  -- replaced it with a raw sqlite3 error;
- ``_connect`` closed a connection whose setup failed, unguarded, before
  raising the catalog-unavailable error, so a failing close replaced that too.

The existing contract is kept, not redefined: COMMIT failure stays
CommitUnconfirmed; and after a confirmed COMMIT -- or a CONFLICT or EQUIVALENT
publication, which roll back and return a definite result -- a close failure
is still reported as itself.

Every test uses a real temporary evidence database, the real catalog adapter,
and -- for integration -- the real Application publish use case. Faults go
through the adapter's ``_connect`` seam (and, for the setup failure inside
``_connect``, its module-level ``sqlite3`` reference): ``_Faulty`` is a
test-only stand-in that delegates every call to the real connection. It can
refuse or interrupt an INSERT, or lose the COMMIT before or after it takes
effect; and once the write has failed -- at the injected failure, or at the
first ``in_transaction`` or ``rollback`` access -- it applies the cleanup
fault: really closing the connection, or making ``in_transaction``,
``rollback()`` or ``close()`` raise. A stored binding is corrupted only by the
existing labelled test-only ``bypass()``. Every claim about what is durable is
checked on a fresh connection. Nothing sleeps.
"""

from __future__ import annotations

import socket
import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Any

import pytest
from northstar_application.application_services import (
    EquitySyntheticPolicyPublicationOutcome,
    InvalidEquitySyntheticPolicyBindingError,
    PublishEquitySyntheticPoliciesUseCase,
)
from northstar_application.ports import (
    EquitySyntheticPolicyCatalogCommitUnconfirmedError,
    EquitySyntheticPolicyCatalogNotCommittedError,
    EquitySyntheticPolicyCatalogOutcome,
    EquitySyntheticPolicyCatalogUnavailableError,
)

from northstar_infrastructure.persistence import SQLiteEquitySyntheticPolicyCatalogStore
from northstar_infrastructure.persistence import (
    sqlite_equity_synthetic_policy_catalog as adapter_module,
)
from tests.equity_recommendation_acceptance_fixtures import bypass
from tests.equity_synthetic_policy_catalog_fixtures import (
    CATALOG_TABLES,
    DEFINITIONS,
    bundle,
    evidence_database,
)

_W = EquitySyntheticPolicyCatalogOutcome
_P = EquitySyntheticPolicyPublicationOutcome
_FAULTS = ["none", "closed", "in_transaction", "rollback", "close"]


@pytest.fixture(autouse=True)
def _refuse_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("L9-P reliability tests must not open network connections.")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)


@pytest.fixture
def database(tmp_path: Path) -> Path:
    return evidence_database(tmp_path / "evidence.sqlite3")


class _Interrupted(BaseException):
    """Test-only: stands for the process being stopped mid-publication."""


class _Faulty:
    """Test-only stand-in for an adapter connection: delegates to a real one.

    Write behaviour (only after ``BEGIN IMMEDIATE``):

    - ``insert_failure``: the first INSERT raises it instead of executing;
    - ``commit``: ``"lost-before"`` raises at COMMIT without committing;
      ``"lost-after"`` really commits, then raises (the acknowledgment is lost).

    Cleanup fault, applied once the write has failed (``failed``):

    - ``"none"``: nothing more goes wrong;
    - ``"closed"``: the real connection is really closed (lost);
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
    """Test-only: adapter connections become ``_Faulty`` (only the ``only``-th if given)."""
    real_connect = adapter_module._connect
    made: list[_Faulty] = []
    opened = {"n": 0}

    def connect(database_path: Path) -> Any:
        opened["n"] += 1
        if only is not None and opened["n"] != only:
            return real_connect(database_path)
        connection = _Faulty(real_connect(database_path), fault, **write)
        made.append(connection)
        return connection

    monkeypatch.setattr(adapter_module, "_connect", connect)
    return made


class _SQLite:
    """Test-only stand-in for the adapter module's ``sqlite3``: connections close badly."""

    def __init__(self) -> None:
        self.made: list[_Faulty] = []

    def connect(self, *arguments: Any, **keywords: Any) -> Any:
        connection = _Faulty(sqlite3.connect(*arguments, **keywords), "close", failed=True)
        self.made.append(connection)
        return connection

    def __getattr__(self, name: str) -> Any:
        return getattr(sqlite3, name)


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


def _catalog(database: Path) -> dict[str, list[tuple[object, ...]]]:
    """Every catalog row; a table not yet created by the adapter holds none."""
    with closing(sqlite3.connect(database)) as connection:
        tables = {r[0] for r in connection.execute("SELECT name FROM sqlite_master")}
        return {
            table: (
                connection.execute(f"SELECT * FROM {table} ORDER BY 1, 2").fetchall()  # noqa: S608
                if table in tables
                else []
            )
            for table in CATALOG_TABLES
        }


def _assert_released(database: Path) -> None:
    """No transaction or lock survives: a fresh connection takes the exclusive lock at once."""
    with closing(sqlite3.connect(database, timeout=0, isolation_level=None)) as connection:
        connection.execute("BEGIN EXCLUSIVE")
        connection.rollback()
        assert connection.execute("PRAGMA integrity_check").fetchall() == [("ok",)]
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []


def _corrupt_timing_policy(database: Path) -> None:
    """TEST-ONLY: the stored TIMING_POLICY binding no longer matches its fingerprint."""
    bypass(
        database,
        (
            f"UPDATE {DEFINITIONS} SET definition_fingerprint = ? WHERE policy_type = ?",  # noqa: S608
            ("eipd1-sha256:" + "0" * 64, "TIMING_POLICY"),
        ),
        drop=(f"{DEFINITIONS}_no_update",),
        checks=False,
    )


def _write_faults(primary: str) -> dict[str, Any]:
    if primary == "storage-refused":
        return {"insert_failure": sqlite3.OperationalError("test: refused before COMMIT")}
    if primary == "interrupted":
        return {"insert_failure": _Interrupted()}
    if primary.startswith("commit-"):
        return {"commit": primary.removeprefix("commit-")}
    return {}


# ----------------------------------------------------------------------
# A, B, C, D. Every typed publication failure survives every cleanup fault
# ----------------------------------------------------------------------

_PRIMARIES = {
    # name: (expected error, durable (publications, definitions, sets) added)
    "storage-refused": (EquitySyntheticPolicyCatalogNotCommittedError, (0, 0, 0)),
    "invalid-stored-binding": (InvalidEquitySyntheticPolicyBindingError, (0, 0, 0)),
    "interrupted": (_Interrupted, (0, 0, 0)),
    "commit-lost-before": (EquitySyntheticPolicyCatalogCommitUnconfirmedError, (0, 0, 0)),
    "commit-lost-after": (EquitySyntheticPolicyCatalogCommitUnconfirmedError, (1, 0, 1)),
}


@pytest.mark.parametrize("fault", _FAULTS)
@pytest.mark.parametrize("primary", sorted(_PRIMARIES))
def test_l9p_publish_keeps_its_typed_outcome(
    database: Path, monkeypatch: pytest.MonkeyPatch, primary: str, fault: str
) -> None:
    store = SQLiteEquitySyntheticPolicyCatalogStore(database)
    # Pre-existing evidence: set/2 shares every component with set/1.
    assert store.publish(bundle("set/2")).outcome is _W.COMMITTED
    if primary == "invalid-stored-binding":
        _corrupt_timing_policy(database)
    before = _catalog(database)
    expected, durable = _PRIMARIES[primary]
    write = _write_faults(primary)
    connections = _faulty_connections(monkeypatch, fault, **write)
    with pytest.raises(expected) as raised:
        store.publish(bundle())
    error = raised.value
    assert type(error) is expected  # the exact outcome, never a sibling or a cleanup error
    _no_cleanup_error(error)
    if primary == "storage-refused":
        assert error.__cause__ is write["insert_failure"]
    if primary.startswith("commit-"):
        assert isinstance(error.__cause__, sqlite3.OperationalError)
    (connection,) = connections
    assert connection.closes == 1 and _is_closed(connection.real)  # closed or discarded once
    after = _catalog(database)
    for table, rows in before.items():
        assert set(rows) <= set(after[table]), table  # stored bindings are never changed
    added = tuple(len(after[t]) - len(before[t]) for t in CATALOG_TABLES)
    assert added == durable  # the whole publication once, or nothing at all
    if any(durable):
        reopened = SQLiteEquitySyntheticPolicyCatalogStore(database)
        assert reopened.load_policy_set(bundle().policy_set.identity) == bundle().policy_set
    _assert_released(database)


def test_l9p_a_failing_open_stays_catalog_unavailable(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``_connect``'s own cleanup: a setup failure, then a close failure, stays typed."""
    shim = _SQLite()
    setup_failure = sqlite3.OperationalError("test: schema setup failed")

    def fail(_connection: object) -> None:
        raise setup_failure

    monkeypatch.setattr(adapter_module, "sqlite3", shim)
    monkeypatch.setattr(adapter_module, "initialize_equity_synthetic_policy_catalog_schema", fail)
    store = SQLiteEquitySyntheticPolicyCatalogStore(database)
    for operation in (
        lambda: store.publish(bundle()),
        lambda: store.load_definition(bundle().components[0].identity),
    ):
        with pytest.raises(EquitySyntheticPolicyCatalogUnavailableError) as raised:
            operation()
        assert raised.value.__cause__ is setup_failure
        _no_cleanup_error(raised.value)
    assert len(shim.made) == 2 and all(c.closes == 1 and _is_closed(c.real) for c in shim.made)
    assert _catalog(database) == {table: [] for table in CATALOG_TABLES}
    _assert_released(database)


# ----------------------------------------------------------------------
# C. Definite outcomes and their close; every path closes its connection
# ----------------------------------------------------------------------


@pytest.mark.parametrize("result", ["COMMITTED", "EQUIVALENT", "CONFLICT"])
def test_l9p_a_close_failure_after_a_definite_outcome_is_not_suppressed(
    database: Path, monkeypatch: pytest.MonkeyPatch, result: str
) -> None:
    """The existing contract: after a definite outcome a close failure is reported as itself."""
    store = SQLiteEquitySyntheticPolicyCatalogStore(database)
    if result != "COMMITTED":
        assert store.publish(bundle()).outcome is _W.COMMITTED
    before = _catalog(database)
    offered = bundle(latency_seconds=240) if result == "CONFLICT" else bundle()
    connections = _faulty_connections(monkeypatch, "close", failed=True)
    with pytest.raises(sqlite3.OperationalError, match="close failed") as raised:
        store.publish(offered)
    assert not isinstance(raised.value, EquitySyntheticPolicyCatalogCommitUnconfirmedError)
    (connection,) = connections
    assert connection.closes == 1
    after = _catalog(database)
    if result == "COMMITTED":  # durable: it is never re-labelled unconfirmed
        assert tuple(len(after[t]) for t in CATALOG_TABLES) == (1, 5, 1)
    else:  # nothing was written; the stored bindings are unchanged
        assert after == before
    _assert_released(database)


def test_l9p_every_connection_is_closed_on_ordinary_paths(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    connections = _faulty_connections(monkeypatch, "none")
    store = SQLiteEquitySyntheticPolicyCatalogStore(database)
    assert store.publish(bundle()).outcome is _W.COMMITTED
    assert store.publish(bundle()).outcome is _W.EQUIVALENT  # idempotent retry
    conflict = store.publish(bundle(latency_seconds=240))
    assert conflict.outcome is _W.CONFLICT and conflict.conflicts  # the stored ones, unchanged
    assert store.publish(bundle("set/2")).outcome is _W.COMMITTED  # components shared
    timing = bundle().components[-1]
    assert store.load_definition(timing.identity) is not None
    assert store.load_policy_set(bundle().policy_set.identity) == bundle().policy_set
    assert len([c for c in connections if c.writing]) == 4
    assert all(c.closes == 1 and _is_closed(c.real) for c in connections)  # reads and writes
    restarted = SQLiteEquitySyntheticPolicyCatalogStore(database)  # a new "process"
    assert restarted.load_policy_set(bundle("set/2").policy_set.identity) == (
        bundle("set/2").policy_set
    )
    assert tuple(len(rows) for rows in _catalog(database).values()) == (2, 5, 2)
    _assert_released(database)


# ----------------------------------------------------------------------
# E. The real Application publish use case reaches its existing outcomes
# ----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("primary", "outcome", "writes", "reconciled", "durable"),
    [
        ("commit-lost-after", _P.EQUIVALENT_PUBLISHED, 1, True, True),
        ("commit-lost-before", _P.PUBLISHED, 2, False, True),
        ("storage-refused", _P.NOT_PUBLISHED, 1, False, False),
        ("invalid-stored-binding", _P.STORED_BINDING_INVALID, 1, False, False),
    ],
)
def test_l9p_the_publish_use_case_reaches_its_outcome_despite_a_failing_close(
    database: Path,
    monkeypatch: pytest.MonkeyPatch,
    primary: str,
    outcome: EquitySyntheticPolicyPublicationOutcome,
    writes: int,
    reconciled: bool,
    durable: bool,
) -> None:
    store = SQLiteEquitySyntheticPolicyCatalogStore(database)
    if primary == "invalid-stored-binding":
        assert store.publish(bundle()).outcome is _W.COMMITTED
        _corrupt_timing_policy(database)
    before = _catalog(database)
    _faulty_connections(monkeypatch, "close", only=1, **_write_faults(primary))
    result = PublishEquitySyntheticPoliciesUseCase(store).execute(bundle())
    assert (result.outcome, result.writes, result.reconciled) == (outcome, writes, reconciled)
    after = _catalog(database)
    if primary == "invalid-stored-binding":
        assert after == before  # nothing repaired, nothing written
    else:
        published = tuple(len(after[t]) for t in CATALOG_TABLES)
        assert published == ((1, 5, 1) if durable else (0, 0, 0))  # once, never twice
    _assert_released(database)


def test_l9p_the_publish_use_case_reports_an_unavailable_catalog_despite_a_failing_close(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(adapter_module, "sqlite3", _SQLite())

    def fail(_connection: object) -> None:
        raise sqlite3.OperationalError("test: schema setup failed")

    monkeypatch.setattr(adapter_module, "initialize_equity_synthetic_policy_catalog_schema", fail)
    store = SQLiteEquitySyntheticPolicyCatalogStore(database)
    result = PublishEquitySyntheticPoliciesUseCase(store).execute(bundle())
    assert (result.outcome, result.writes) == (_P.CATALOG_UNAVAILABLE, 1)  # never retried
    assert _catalog(database) == {table: [] for table in CATALOG_TABLES}
