"""EI-7D: the strictly read-only SQLite connection and its non-mutation proof, on real SQLite.

Acceptance of the EI-7D read-only connection contract (EI-7 Decision Register,
section 13.9; EI-6C precedent): ``mode=ro`` and a confirmed ``query_only``;
no file, schema, journal or sidecar is created or changed by any reader on any
path (R1, R2, R4 to R7); a read-only file is served (R3); failures are typed,
leave no transaction open and no lock held (R9); cleanup never replaces the
failure being reported; and the accepted writers keep working beside the
readers (R10).

Every database is a disposable copy of a fixture built through the accepted
chain (tests/equity_read_only_fixtures.py). Raw SQL appears only where a test
says so: TEST-ONLY crash images, journal-mode changes, corruption and the
writes a read-only connection must refuse. Nothing here opens a network
connection or touches a production database.
"""

from __future__ import annotations

import os
import shutil
import socket
import sqlite3
import stat
from collections.abc import Callable, Iterator
from contextlib import closing, contextmanager
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
from northstar_application.ports import (
    EquityCandleEvidenceUnavailableError,
    EquityIntradayResearchTimelineUnavailableError,
    EquityRecommendationGenerationStoreUnavailableError,
    EquityRecommendationIssuanceUnavailableError,
    EquityRecommendationLifecycleStoreUnavailableError,
)

from northstar_infrastructure.persistence import (
    SQLiteEquityRecommendationIssuanceAuthority,
    SQLiteEquityRecommendationLifecycleStore,
)
from northstar_infrastructure.persistence import sqlite_equity_read_only as connection_module
from tests.equity_lifecycle_monitoring_fixtures import copy_world
from tests.equity_read_only_fixtures import (
    LATER_DECLARATION,
    base_world,
    convert_to_wal,
    crash_image,
    file_state,
    readers,
    research_snapshot_ids,
    schema_state,
    sidecars,
)
from tests.equity_recommendation_acceptance_fixtures import CONFIG, DAY, NSE, TCS, raw
from tests.equity_recommendation_lifecycle_fixtures import LifecycleWorld

_UNAVAILABLE = {
    "issuances": EquityRecommendationIssuanceUnavailableError,
    "sessions": EquityRecommendationIssuanceUnavailableError,
    "lifecycles": EquityRecommendationLifecycleStoreUnavailableError,
    "generations": EquityRecommendationGenerationStoreUnavailableError,
    "timelines": EquityIntradayResearchTimelineUnavailableError,
    "snapshots": EquityCandleEvidenceUnavailableError,
}


@pytest.fixture(autouse=True)
def _refuse_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("EI-7D tests must not open network connections.")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)


@pytest.fixture(scope="module")
def template(tmp_path_factory: pytest.TempPathFactory) -> LifecycleWorld:
    return base_world(tmp_path_factory.mktemp("ei7d-connection") / "template.sqlite3")


@pytest.fixture
def world(template: LifecycleWorld, tmp_path: Path) -> LifecycleWorld:
    """A disposable copy, alone in its own directory, so every sibling file is observed."""
    directory = tmp_path / "sealed"
    directory.mkdir()
    return copy_world(template, directory / "ei7d.sqlite3")


def _every_read(world: LifecycleWorld) -> dict[str, Callable[[], Any]]:
    """One call of every reader method, over ``world``'s file."""
    r = readers(world.database)
    issuance = r["issuances"].load_issuance(world.issuance_id)
    assert issuance is not None
    generation_key = issuance.recommendation.generation_key
    snapshot_id = research_snapshot_ids(world.database)[1]
    return {
        "issuances": lambda: r["issuances"].load_issuance(world.issuance_id),
        "session_issuance": lambda: r["issuances"].load_session_issuance(CONFIG.strategy, TCS, DAY),
        "session_view": lambda: r["issuances"].session_view(NSE, DAY),
        "session_view_at": lambda: r["issuances"].session_view_at(NSE, DAY, 1),
        "sessions": lambda: r["sessions"].recorded_session_issuances(NSE, DAY, limit=50),
        "lifecycles": lambda: r["lifecycles"].load_lifecycle(world.issuance_id),
        "generations": lambda: r["generations"].load_disposition(generation_key),
        "attempts": lambda: r["generations"].load_attempt("unknown-attempt"),
        "timelines": lambda: r["timelines"].load(CONFIG.strategy, TCS, DAY),
        "snapshots": lambda: r["snapshots"].get_snapshot(snapshot_id),
    }


def _failing_calls(database: Path) -> dict[str, Callable[[], Any]]:
    """One call per reader family over ``database``, each expected to be unavailable."""
    r = readers(database)
    return {
        "issuances": lambda: r["issuances"].load_issuance("eiris1-sha256:" + "0" * 64),
        "sessions": lambda: r["sessions"].recorded_session_issuances(NSE, DAY, limit=50),
        "lifecycles": lambda: r["lifecycles"].load_lifecycle("eiris1-sha256:" + "0" * 64),
        "generations": lambda: r["generations"].load_disposition("eirk1-sha256:" + "0" * 64),
        "timelines": lambda: r["timelines"].load(CONFIG.strategy, TCS, DAY),
        "snapshots": lambda: r["snapshots"].get_snapshot("ecs1-sha256:" + "0" * 64),
    }


def _assert_every_family_unavailable(database: Path) -> None:
    for family, call in _failing_calls(database).items():
        with pytest.raises(_UNAVAILABLE[family]) as raised:
            call()
        assert str(database.parent) not in str(raised.value), family  # no path is exposed
        assert raised.value.__cause__ is not None, family


# ----------------------------------------------------------------------
# The connection: mode=ro and a confirmed query_only (R6)
# ----------------------------------------------------------------------


def test_r6_the_connection_is_opened_read_only_and_query_only(world: LifecycleWorld) -> None:
    uri = connection_module.read_only_uri(world.database)
    assert uri.startswith("file:///") and uri.endswith("?mode=ro")
    connection = connection_module.connect_read_only(world.database)
    try:
        assert connection.execute("PRAGMA query_only").fetchone() == (1,)
        assert connection.isolation_level is None  # no implicit transaction is ever opened
    finally:
        connection.close()


@pytest.mark.parametrize(
    "statement",
    [
        "CREATE TABLE ei7d_probe (x INTEGER)",
        "CREATE INDEX ei7d_probe ON equity_recommendation_issuances (symbol)",
        "DROP TRIGGER equity_recommendation_issuances_no_update",
        "ALTER TABLE equity_recommendation_issuances ADD COLUMN x TEXT",
        "INSERT INTO equity_recommendation_issuance_ledger VALUES (99, 'ISSUANCE')",
        "UPDATE equity_recommendation_issuances SET symbol = 'X'",
        "DELETE FROM equity_recommendation_issuance_ledger",
        "REPLACE INTO equity_recommendation_issuance_ledger VALUES (1, 'DECLARATION')",
        "PRAGMA user_version = 7",
        "PRAGMA journal_mode = WAL",
        "VACUUM",
    ],
)
def test_r6_a_write_through_the_reader_connection_is_refused_and_changes_nothing(
    world: LifecycleWorld, statement: str
) -> None:
    before, schema = file_state(world.database), schema_state(world.database)
    connection = connection_module.connect_read_only(world.database)
    try:
        with pytest.raises(sqlite3.OperationalError, match="readonly|read-only|query_only"):
            connection.execute(statement)
    finally:
        connection.close()
    assert file_state(world.database) == before and schema_state(world.database) == schema


def test_r6_mode_ro_still_refuses_writes_if_query_only_were_switched_off(
    world: LifecycleWorld,
) -> None:
    """The two guards are independent: mode=ro alone refuses a write."""
    before = file_state(world.database)
    connection = connection_module.connect_read_only(world.database)
    try:
        connection.execute("PRAGMA query_only = OFF")  # TEST-ONLY: never done by a reader
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            connection.execute("CREATE TABLE ei7d_probe (x INTEGER)")
    finally:
        connection.close()
    assert file_state(world.database) == before


def test_r6_query_only_that_cannot_be_confirmed_refuses_the_connection(
    world: LifecycleWorld, monkeypatch: pytest.MonkeyPatch
) -> None:
    class _Unconfirmed:
        def __init__(self, real: sqlite3.Connection) -> None:
            self._real, self.closed = real, False

        def execute(self, statement: str, *args: Any) -> Any:
            if statement == "PRAGMA query_only":
                return self._real.execute("SELECT 0")
            return self._real.execute(statement, *args)

        def close(self) -> None:
            self._real.close()
            self.closed = True

    opened: list[_Unconfirmed] = []
    real_connect = sqlite3.connect

    def connect(*args: Any, **kwargs: Any) -> Any:
        opened.append(_Unconfirmed(real_connect(*args, **kwargs)))
        return opened[-1]

    monkeypatch.setattr(connection_module.sqlite3, "connect", connect)
    with pytest.raises(EquityRecommendationIssuanceUnavailableError, match="Read-only mode"):
        readers(world.database)["issuances"].load_issuance(world.issuance_id)
    assert opened and opened[0].closed


# ----------------------------------------------------------------------
# R1, R2, R7: every reader on every path leaves bytes, schema and sidecars unchanged
# ----------------------------------------------------------------------


def test_r1_r2_r7_every_reader_method_leaves_the_file_schema_and_sidecars_unchanged(
    world: LifecycleWorld,
) -> None:
    calls = _every_read(world)
    before, schema = file_state(world.database), schema_state(world.database)
    for name, call in calls.items():
        call()
        assert file_state(world.database) == before, name  # bytes and every sibling file
        assert schema_state(world.database) == schema, name  # objects and both versions
    assert sidecars(world.database) == []  # no journal, -wal or -shm was created or left


def test_r1_reads_repeated_many_times_leave_the_file_byte_identical(
    world: LifecycleWorld,
) -> None:
    calls = list(_every_read(world).values())
    before = file_state(world.database)
    answers = [call() for call in calls]
    for _ in range(5):
        assert [call() for call in calls] == answers
    assert file_state(world.database) == before


def test_r1_a_failing_read_leaves_the_file_unchanged(world: LifecycleWorld) -> None:
    before = file_state(world.database)
    reader = readers(world.database)["sessions"]
    with pytest.raises(Exception, match="beyond"):
        reader.recorded_session_issuances(NSE, DAY, limit=50, watermark=99)
    assert file_state(world.database) == before


# ----------------------------------------------------------------------
# R4, R5: a missing database or missing tables are typed and never created
# ----------------------------------------------------------------------


def test_r4_a_missing_database_is_unavailable_and_never_created(tmp_path: Path) -> None:
    missing = tmp_path / "missing.sqlite3"
    _assert_every_family_unavailable(missing)
    assert list(tmp_path.iterdir()) == []


def test_r4_a_missing_directory_is_unavailable_and_never_created(tmp_path: Path) -> None:
    missing = tmp_path / "absent" / "missing.sqlite3"
    _assert_every_family_unavailable(missing)
    assert list(tmp_path.iterdir()) == []


def test_r5_a_database_without_the_equity_tables_is_unavailable_and_unchanged(
    tmp_path: Path,
) -> None:
    other = tmp_path / "other.sqlite3"
    with closing(sqlite3.connect(other)) as connection:
        connection.execute("CREATE TABLE unrelated (x INTEGER)")
        connection.commit()
    before, schema = file_state(other), schema_state(other)
    _assert_every_family_unavailable(other)
    assert file_state(other) == before and schema_state(other) == schema  # nothing initialized


def test_r5_an_empty_file_is_unavailable_and_stays_empty(tmp_path: Path) -> None:
    empty = tmp_path / "empty.sqlite3"
    empty.touch()
    _assert_every_family_unavailable(empty)
    assert empty.read_bytes() == b"" and sidecars(empty) == []


def test_r5_a_database_without_the_lifecycle_tables_refuses_only_the_lifecycle_reader(
    template: LifecycleWorld, tmp_path: Path
) -> None:
    """A file from before EI-6B: the issuance reads work; the lifecycle read is unavailable."""
    database = tmp_path / "pre-ei6b.sqlite3"
    shutil.copyfile(template.database, database)
    with closing(raw(database)) as connection:  # TEST-ONLY: the EI-6B objects removed
        for (name,) in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'trigger' "
            "AND tbl_name = 'equity_recommendation_lifecycle_events'"
        ).fetchall():
            connection.execute(f"DROP TRIGGER {name}")
        connection.execute("DROP TABLE equity_recommendation_lifecycle_events")
    before = file_state(database)
    r = readers(database)
    assert r["issuances"].load_issuance(template.issuance_id) is not None
    with pytest.raises(EquityRecommendationLifecycleStoreUnavailableError, match="no such table"):
        r["lifecycles"].load_lifecycle(template.issuance_id)
    assert file_state(database) == before  # the reader never adds the lifecycle schema


def test_r5_the_existing_adapters_would_initialize_what_the_readers_never_touch(
    tmp_path: Path,
) -> None:
    """Contrast (finding A-5): an accepted read-write adapter adds its schema on a read."""
    database = tmp_path / "generation-only.sqlite3"
    with closing(sqlite3.connect(database)) as connection:
        connection.execute("CREATE TABLE equity_recommendation_generation_dispositions (x)")
        connection.execute("CREATE TABLE equity_recommendation_generation_attempts (x)")
        connection.commit()
    before = schema_state(database)
    with pytest.raises(EquityRecommendationIssuanceUnavailableError):
        readers(database)["issuances"].load_issuance("eiris1-sha256:" + "0" * 64)
    assert schema_state(database) == before
    SQLiteEquityRecommendationIssuanceAuthority(database).load_issuance("eiris1-sha256:" + "0" * 64)
    assert schema_state(database) != before  # the write-capable path ran DDL


# ----------------------------------------------------------------------
# R3: an operating-system read-only file
# ----------------------------------------------------------------------


def test_r3_a_read_only_file_is_served_by_every_reader_and_refuses_ordinary_writes(
    world: LifecycleWorld,
) -> None:
    """On Windows S_IREAD sets the read-only attribute; the OS itself refuses writes."""
    calls = _every_read(world)
    baseline = {name: call() for name, call in calls.items()}
    mode = world.database.stat().st_mode
    os.chmod(world.database, stat.S_IREAD)
    try:
        writer = sqlite3.connect(world.database, isolation_level=None)
        try:  # control: the file is genuinely read-only to an ordinary connection
            with pytest.raises(sqlite3.OperationalError, match="readonly"):
                writer.execute("PRAGMA user_version = 3")
        finally:
            writer.close()
        before = file_state(world.database)
        assert {name: call() for name, call in calls.items()} == baseline
        assert file_state(world.database) == before
    finally:
        os.chmod(world.database, mode)


# ----------------------------------------------------------------------
# R7: hot journals and write-ahead logging
# ----------------------------------------------------------------------


def test_e1_the_fixture_database_runs_the_rollback_journal(world: LifecycleWorld) -> None:
    with closing(raw(world.database)) as connection:
        assert connection.execute("PRAGMA journal_mode").fetchone() == ("delete",)
    assert world.database.read_bytes()[18:20] == b"\x01\x01"


def test_r7_a_hot_journal_is_unavailable_and_neither_file_is_touched(
    world: LifecycleWorld, tmp_path: Path
) -> None:
    crashed = tmp_path / "crashed"
    crashed.mkdir()
    image = crashed / "image.sqlite3"
    hot = crash_image(world.database, image)
    before = file_state(image)
    assert sidecars(image) == [hot.name]
    _assert_every_family_unavailable(image)
    with pytest.raises(EquityRecommendationIssuanceUnavailableError, match="READONLY_ROLLBACK"):
        readers(image)["issuances"].load_issuance(world.issuance_id)
    assert file_state(image) == before  # no recovery, no deletion, no repair
    # An accepted read-write adapter recovers it; afterwards the readers answer as before.
    expected = readers(world.database)["issuances"].load_issuance(world.issuance_id)
    assert SQLiteEquityRecommendationIssuanceAuthority(image).load_issuance(world.issuance_id)
    assert sidecars(image) == []
    assert readers(image)["issuances"].load_issuance(world.issuance_id) == expected


def test_r7_a_wal_database_is_unavailable_and_no_wal_or_shm_file_is_created(
    world: LifecycleWorld,
) -> None:
    convert_to_wal(world.database)
    before = file_state(world.database)
    _assert_every_family_unavailable(world.database)
    with pytest.raises(EquityRecommendationIssuanceUnavailableError, match="rollback-journal"):
        readers(world.database)["issuances"].load_issuance(world.issuance_id)
    assert file_state(world.database) == before and sidecars(world.database) == []


def test_r7_the_wal_refusal_happens_before_sqlite_opens_the_file(
    world: LifecycleWorld, monkeypatch: pytest.MonkeyPatch
) -> None:
    convert_to_wal(world.database)
    opened: list[object] = []
    monkeypatch.setattr(connection_module.sqlite3, "connect", lambda *a, **k: opened.append(a))
    with pytest.raises(EquityRecommendationIssuanceUnavailableError):
        readers(world.database)["issuances"].load_issuance(world.issuance_id)
    assert opened == []


def test_r7_a_file_that_is_not_a_database_is_unavailable_and_unchanged(tmp_path: Path) -> None:
    garbage = tmp_path / "garbage.sqlite3"
    garbage.write_bytes(b"\x00not a database" * 512)
    before = file_state(garbage)
    _assert_every_family_unavailable(garbage)
    assert file_state(garbage) == before


# ----------------------------------------------------------------------
# URI correctness
# ----------------------------------------------------------------------


@pytest.mark.parametrize(
    "name", ["with space", "hash#mark", "percent%41", "amp&eq=x;semi", "unicode-ünï"]
)
def test_uri_sensitive_directory_and_file_names_are_opened_exactly(
    template: LifecycleWorld, tmp_path: Path, name: str
) -> None:
    directory = tmp_path / f"dir {name}"
    directory.mkdir()
    database = directory / f"db {name}.sqlite3"
    shutil.copyfile(template.database, database)
    uri = connection_module.read_only_uri(database)
    assert "#" not in uri and " " not in uri and uri.count("?") == 1  # percent-encoded
    expected = readers(template.database)["issuances"].load_issuance(template.issuance_id)
    before = file_state(database)
    assert readers(database)["issuances"].load_issuance(template.issuance_id) == expected
    assert file_state(database) == before
    assert sorted(p.name for p in tmp_path.iterdir()) == [directory.name]  # nothing else made


def test_a_relative_path_is_resolved_to_the_same_file(
    template: LifecycleWorld, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    shutil.copyfile(template.database, tmp_path / "relative.sqlite3")
    monkeypatch.chdir(tmp_path)
    reader = readers(Path("relative.sqlite3"))["issuances"]
    assert reader.load_issuance(template.issuance_id) is not None


# ----------------------------------------------------------------------
# R9: failures are typed, close the connection and leave no lock
# ----------------------------------------------------------------------


class _Proxy:
    """TEST-ONLY: a reader connection whose cleanup can be made to fail."""

    def __init__(self, connection: sqlite3.Connection, failing: bool) -> None:
        self._connection, self._failing, self.closed = connection, failing, False

    def execute(self, *args: Any) -> Any:
        return self._connection.execute(*args)

    @property
    def in_transaction(self) -> bool:
        return self._connection.in_transaction

    def rollback(self) -> None:
        self._connection.rollback()
        if self._failing:
            raise sqlite3.OperationalError("test: rollback failed")

    def close(self) -> None:
        self._connection.close()
        self.closed = True
        if self._failing:
            raise sqlite3.OperationalError("test: close failed")


@contextmanager
def _proxied(monkeypatch: pytest.MonkeyPatch, failing: bool) -> Iterator[list[_Proxy]]:
    real, opened = connection_module.connect_read_only, []

    def connect(database_path: Path) -> Any:
        opened.append(_Proxy(real(database_path), failing))
        return opened[-1]

    monkeypatch.setattr(connection_module, "connect_read_only", connect)
    yield opened
    monkeypatch.setattr(connection_module, "connect_read_only", real)


def _unlocked(database: Path) -> None:
    """No reader left a lock: an exclusive writer lock is obtained immediately."""
    with closing(sqlite3.connect(database, timeout=0, isolation_level=None)) as connection:
        connection.execute("BEGIN EXCLUSIVE")
        connection.execute("ROLLBACK")


def test_r9_a_sqlite_failure_mid_read_is_unavailable_closes_and_unlocks(
    world: LifecycleWorld, monkeypatch: pytest.MonkeyPatch
) -> None:
    from northstar_infrastructure.persistence import (
        sqlite_equity_recommendation_read_only as recommendation_module,
    )

    def failing(connection: sqlite3.Connection) -> int:
        connection.execute("SELECT COUNT(*) FROM equity_recommendation_issuance_ledger")
        raise sqlite3.OperationalError("test: disk I/O error")

    with _proxied(monkeypatch, failing=False) as opened:
        monkeypatch.setattr(recommendation_module, "_consistent_ledger", failing)
        with pytest.raises(EquityRecommendationIssuanceUnavailableError, match="disk I/O") as e:
            readers(world.database)["issuances"].load_issuance(world.issuance_id)
    assert isinstance(e.value.__cause__, sqlite3.OperationalError)
    assert opened[0].closed
    _unlocked(world.database)


def test_r9_an_integrity_failure_propagates_typed_closes_and_unlocks(
    world: LifecycleWorld, monkeypatch: pytest.MonkeyPatch
) -> None:
    from northstar_application.ports import InvalidStoredEquityRecommendationIssuanceAuthorityError

    with closing(raw(world.database)) as connection:  # TEST-ONLY: an orphaned ledger entry
        connection.execute("DROP TRIGGER equity_recommendation_issuance_ledger_previous_recorded")
        connection.execute(
            "INSERT INTO equity_recommendation_issuance_ledger VALUES (3, 'ISSUANCE')"
        )
    with _proxied(monkeypatch, failing=False) as opened:
        with pytest.raises(InvalidStoredEquityRecommendationIssuanceAuthorityError) as raised:
            readers(world.database)["issuances"].load_issuance(world.issuance_id)
    assert raised.value.category == "LEDGER"
    assert opened[0].closed
    _unlocked(world.database)


def test_r9_a_cleanup_failure_never_replaces_the_primary_failure(
    world: LifecycleWorld, monkeypatch: pytest.MonkeyPatch
) -> None:
    from northstar_application.ports import InvalidStoredEquityRecommendationIssuanceAuthorityError

    with closing(raw(world.database)) as connection:  # TEST-ONLY: an orphaned ledger entry
        connection.execute("DROP TRIGGER equity_recommendation_issuance_ledger_previous_recorded")
        connection.execute(
            "INSERT INTO equity_recommendation_issuance_ledger VALUES (3, 'ISSUANCE')"
        )
    with _proxied(monkeypatch, failing=True) as opened:
        with pytest.raises(InvalidStoredEquityRecommendationIssuanceAuthorityError):
            readers(world.database)["issuances"].load_issuance(world.issuance_id)
    assert opened[0].closed  # closed, though its close reported a failure
    _unlocked(world.database)


def test_r9_an_interrupt_propagates_unchanged_and_the_connection_is_closed(
    world: LifecycleWorld, monkeypatch: pytest.MonkeyPatch
) -> None:
    from northstar_infrastructure.persistence import (
        sqlite_equity_recommendation_read_only as recommendation_module,
    )

    def interrupted(connection: sqlite3.Connection) -> int:
        raise KeyboardInterrupt

    with _proxied(monkeypatch, failing=False) as opened:
        monkeypatch.setattr(recommendation_module, "_consistent_ledger", interrupted)
        with pytest.raises(KeyboardInterrupt):
            readers(world.database)["issuances"].load_issuance(world.issuance_id)
    assert opened[0].closed
    _unlocked(world.database)


def test_r9_a_close_failure_after_a_complete_read_returns_nothing(
    world: LifecycleWorld, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _proxied(monkeypatch, failing=True):
        with pytest.raises(EquityRecommendationIssuanceUnavailableError, match="close failed"):
            readers(world.database)["issuances"].load_issuance(world.issuance_id)


def test_r9_a_lock_held_beyond_the_busy_timeout_is_unavailable_and_reads_resume(
    world: LifecycleWorld, monkeypatch: pytest.MonkeyPatch
) -> None:
    reader = readers(world.database)["sessions"]
    baseline = reader.recorded_session_issuances(NSE, DAY, limit=50)
    monkeypatch.setattr(connection_module, "READ_ONLY_BUSY_TIMEOUT_SECONDS", 0.05)
    holder = raw(world.database)
    try:
        holder.execute("BEGIN EXCLUSIVE")  # another connection holds the database
        with pytest.raises(EquityRecommendationIssuanceUnavailableError, match="SQLITE_BUSY"):
            reader.recorded_session_issuances(NSE, DAY, limit=50)
    finally:
        holder.execute("ROLLBACK")
        holder.close()
    assert reader.recorded_session_issuances(NSE, DAY, limit=50) == baseline


# ----------------------------------------------------------------------
# R10: the accepted writers keep working beside the readers
# ----------------------------------------------------------------------


def test_r10_accepted_writers_still_write_and_readers_see_only_committed_states(
    world: LifecycleWorld,
) -> None:
    r = readers(world.database)
    before = r["sessions"].recorded_session_issuances(NSE, DAY, limit=50)
    assert before.recording_position == 2
    recorded = world.record(world.opened())  # the accepted EI-6B append
    assert recorded.outcome.value == "APPENDED"
    declared = world.world.declare(LATER_DECLARATION)  # the accepted EI-6A declaration write
    assert declared.outcome.value == "DECLARED"
    after = r["sessions"].recorded_session_issuances(NSE, DAY, limit=50)
    assert after.recording_position == 3 and len(after.declarations) == 2
    lifecycle = r["lifecycles"].load_lifecycle(world.issuance_id)
    assert lifecycle == SQLiteEquityRecommendationLifecycleStore(world.database).load_lifecycle(
        world.issuance_id
    )
    replay = r["sessions"].recorded_session_issuances(NSE, DAY, limit=50, watermark=2)
    assert replay == replace(before, recording_position=3)  # N observed now; W replayed exactly


# ----------------------------------------------------------------------
# Engine-level SQLite conditions (EI-7D final audit, check 1)
# ----------------------------------------------------------------------
#
# Every sqlite3.Error is the calling reader's typed unavailability, with SQLite's
# error name in the message and the SQLite error as the cause. The accepted reader
# ports have no permanent-integrity form for a storage-engine fault, so engine
# corruption is classified as the accepted EI-4D/EI-6A/EI-6B adapters classify
# it: a documented limitation pending an Application contract decision. These
# tests pin that classification so it can only change deliberately.


def _damage_page(database: Path, table: str) -> None:
    """TEST-ONLY: overwrite the root b-tree page of ``table`` in a disposable copy."""
    with closing(sqlite3.connect(f"{database.resolve().as_uri()}?mode=ro", uri=True)) as c:
        page_size = c.execute("PRAGMA page_size").fetchone()[0]
        (root,) = c.execute(
            "SELECT rootpage FROM sqlite_master WHERE name = ?", (table,)
        ).fetchone()
    with database.open("r+b") as handle:
        handle.seek((root - 1) * page_size)
        handle.write(b"\xa5" * page_size)


def _engine_calls(world: LifecycleWorld) -> dict[str, tuple[str, Callable[[], Any]]]:
    """For each damaged table: the reader family whose own read must hit it, and the call."""
    r = readers(world.database)
    issuance = r["issuances"].load_issuance(world.issuance_id)
    assert issuance is not None
    key = issuance.recommendation.generation_key
    snapshot_id = research_snapshot_ids(world.database)[1]
    return {
        "equity_recommendation_issuance_ledger": (
            "sessions",
            lambda: r["sessions"].recorded_session_issuances(NSE, DAY, limit=50),
        ),
        "equity_recommendation_issuances": (
            "issuances",
            lambda: r["issuances"].load_issuance(world.issuance_id),
        ),
        "equity_recommendation_generation_dispositions": (
            "generations",
            lambda: r["generations"].load_disposition(key),
        ),
        "equity_intraday_research_timeline_entries": (
            "timelines",
            lambda: r["timelines"].load(CONFIG.strategy, TCS, DAY),
        ),
        "equity_candle_evidence_snapshots": (
            "snapshots",
            lambda: r["snapshots"].get_snapshot(snapshot_id),
        ),
    }


@pytest.mark.parametrize(
    "table",
    [
        "equity_recommendation_issuance_ledger",
        "equity_recommendation_issuances",
        "equity_recommendation_generation_dispositions",
        "equity_intraday_research_timeline_entries",
        "equity_candle_evidence_snapshots",
    ],
)
def test_engine_sqlite_corrupt_is_typed_unavailability_with_the_engine_cause(
    world: LifecycleWorld, table: str
) -> None:
    family, call = _engine_calls(world)[table]
    _damage_page(world.database, table)
    before = file_state(world.database)
    with pytest.raises(_UNAVAILABLE[family]) as raised:
        call()  # never data, never None, never an empty answer
    cause = raised.value.__cause__
    assert isinstance(cause, sqlite3.DatabaseError)
    assert cause.sqlite_errorname == "SQLITE_CORRUPT"  # type: ignore[attr-defined]
    assert "SQLITE_CORRUPT" in str(raised.value)  # named, not silent
    assert str(world.database.parent) not in str(raised.value)  # no path
    assert file_state(world.database) == before  # nothing repaired or written


def test_engine_corruption_reaches_ei7c_as_a_typed_refusal_never_an_untyped_leak(
    world: LifecycleWorld,
) -> None:
    """Documented limitation: EI-7C has only DEPENDENCY_UNAVAILABLE for an engine fault."""
    from northstar_application.application_services import (
        EquityReadQueryRefusal,
        EquityReadQueryRefusedError,
        GetEquityRecommendationDetailUseCase,
        ListEquityRecordedSessionIssuancesUseCase,
    )

    _damage_page(world.database, "equity_recommendation_issuances")
    r = readers(world.database)
    for call in (
        lambda: ListEquityRecordedSessionIssuancesUseCase(r["sessions"]).execute(NSE, DAY),
        lambda: GetEquityRecommendationDetailUseCase(
            r["lifecycles"], r["generations"], r["snapshots"]
        ).execute(world.issuance_id),
    ):
        with pytest.raises(EquityReadQueryRefusedError) as raised:
            call()
        assert raised.value.reason is EquityReadQueryRefusal.DEPENDENCY_UNAVAILABLE
        assert "SQLITE" not in str(raised.value) and str(world.database) not in str(raised.value)


def test_engine_corruption_classification_equals_the_accepted_writer_adapters(
    world: LifecycleWorld,
) -> None:
    _damage_page(world.database, "equity_recommendation_issuances")
    with pytest.raises(EquityRecommendationIssuanceUnavailableError):
        readers(world.database)["issuances"].load_issuance(world.issuance_id)
    with pytest.raises(EquityRecommendationLifecycleStoreUnavailableError):
        readers(world.database)["lifecycles"].load_lifecycle(world.issuance_id)
    with pytest.raises(EquityRecommendationIssuanceUnavailableError):  # accepted, unchanged
        SQLiteEquityRecommendationIssuanceAuthority(world.database).load_issuance(world.issuance_id)
    with pytest.raises(EquityRecommendationLifecycleStoreUnavailableError):
        SQLiteEquityRecommendationLifecycleStore(world.database).load_lifecycle(world.issuance_id)


@pytest.mark.parametrize(
    ("name", "content", "errorname"),
    [
        ("truncated-with-magic", lambda data: data[:50], "SQLITE_CORRUPT"),
        ("header-only", lambda data: data[:100], "SQLITE_CORRUPT"),
        ("not-a-database", lambda data: b"\x00garbage" * 512, "SQLITE_NOTADB"),
    ],
)
def test_engine_truncated_and_foreign_files_are_unavailable_and_create_nothing(
    template: LifecycleWorld, tmp_path: Path, name: str, content: Any, errorname: str
) -> None:
    database = tmp_path / name / "db.sqlite3"
    database.parent.mkdir()
    database.write_bytes(content(template.database.read_bytes()))
    before = file_state(database)
    _assert_every_family_unavailable(database)
    with pytest.raises(EquityRecommendationIssuanceUnavailableError) as raised:
        readers(database)["issuances"].load_issuance(template.issuance_id)
    assert raised.value.__cause__.sqlite_errorname == errorname  # type: ignore[union-attr]
    assert file_state(database) == before and sidecars(database) == []


def test_engine_sqlite_cantopen_is_unavailable_even_without_the_header_check(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """mode=ro itself never creates the file: proven with the header check bypassed."""
    monkeypatch.setattr(connection_module, "_require_rollback_journal", lambda path: None)
    with pytest.raises(EquityRecommendationIssuanceUnavailableError, match="SQLITE_CANTOPEN"):
        readers(tmp_path / "missing.sqlite3")["issuances"].load_issuance("x")
    assert list(tmp_path.iterdir()) == []


def test_engine_sqlite_locked_is_unavailability(
    world: LifecycleWorld, monkeypatch: pytest.MonkeyPatch
) -> None:
    from northstar_infrastructure.persistence import (
        sqlite_equity_recommendation_read_only as recommendation_module,
    )

    def locked(connection: sqlite3.Connection) -> int:
        error = sqlite3.OperationalError("database table is locked")
        error.sqlite_errorcode = sqlite3.SQLITE_LOCKED  # type: ignore[attr-defined]
        error.sqlite_errorname = "SQLITE_LOCKED"  # type: ignore[attr-defined]
        raise error

    monkeypatch.setattr(recommendation_module, "_consistent_ledger", locked)
    with pytest.raises(EquityRecommendationIssuanceUnavailableError, match="SQLITE_LOCKED"):
        readers(world.database)["issuances"].load_issuance(world.issuance_id)
    _unlocked(world.database)


def test_engine_an_unresolvable_path_is_typed_unavailability_never_a_raw_error(
    tmp_path: Path,
) -> None:
    _assert_every_family_unavailable(tmp_path / "bad\x00name.sqlite3")
    assert list(tmp_path.iterdir()) == []


def test_engine_corruption_on_a_page_no_query_touches_is_not_detected(
    world: LifecycleWorld,
) -> None:
    """Documented limitation: no full integrity_check runs; reads verify what they read."""
    _damage_page(world.database, "equity_recommendation_lifecycle_events")  # an empty table
    loaded = readers(world.database)["lifecycles"].load_lifecycle(world.issuance_id)
    assert loaded is not None and loaded.lifecycle.events == ()


# ----------------------------------------------------------------------
# No hidden connection (EI-7D final audit, check 4)
# ----------------------------------------------------------------------


def test_every_read_opens_exactly_one_read_only_connection_and_nothing_else(
    world: LifecycleWorld, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = _every_read(world)
    real, opened = sqlite3.connect, []

    def connect(*args: Any, **kwargs: Any) -> sqlite3.Connection:
        opened.append((args, kwargs))
        return real(*args, **kwargs)

    monkeypatch.setattr(sqlite3, "connect", connect)
    for name, call in calls.items():
        opened.clear()
        call()
        assert len(opened) == 1, name  # no reused decoder opened its own connection
        (database,), options = opened[0]
        assert database.endswith("?mode=ro") and options["uri"] is True, name
        assert options["isolation_level"] is None, name
