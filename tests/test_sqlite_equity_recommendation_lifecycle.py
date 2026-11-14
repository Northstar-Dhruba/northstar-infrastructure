"""EI-6B: the durable SQLite lifecycle store, on real SQLite.

Acceptance of the EI-6B Durable Recommendation Lifecycle Persistence Design
v1.0 (section 12) at layer S: RR-01, the state part of RR-04, RR-05, the S
parts of RR-06, LS-01 to LS-17 and RV-04 where the design gives them an S
part, and the architecture rows for the adapter. Test names carry the
scenario identifiers; the C, E and A parts are EI-6B Application PR-1's.

Every success path is the real chain over one disposable SQLite file
(tests/equity_recommendation_lifecycle_fixtures.py): the EI-6A world, synthetic
bars after the issuance, real EI-2B snapshots, the real recording use case
(Core decides every event) and the adapter's authorized ``append``. No
lifecycle event is inserted by raw SQL on a success path. Raw SQL appears
only where a test says so: refused direct writes, statements tried inside a
transaction that is rolled back, and TEST-ONLY tampering of a disposable
file. Separate OS processes use the ``spawn`` start method on every platform.

Monitoring, snapshot selection, a monitoring cursor (EI-6C), restatement
(EI-8), orders, fills and positions are not here. Nothing here opens a
network connection or touches a production database.
"""

from __future__ import annotations

import ast
import copy
import dataclasses
import hashlib
import inspect
import json
import multiprocessing
import os
import socket
import sqlite3
import subprocess
import sys
from collections.abc import Callable, Iterator
from contextlib import closing, contextmanager
from pathlib import Path
from typing import Any

import pytest
from northstar_application.application_services import (
    EquityLifecycleRecordingOutcome,
    EquityLifecycleRecordingRefusal,
    EquityLifecycleRecordingRefusedError,
    EquityLifecycleRecordingUnresolvedError,
    EquityRecordedIssuance,
    InvalidEquityLifecycleWindowProvenanceError,
    VerifiedEquityRecommendationLifecycleAppend,
    decode_equity_lifecycle_event_document,
    equity_issued_lifecycle_window,
    equity_lifecycle_event_document,
    equity_lifecycle_event_documents,
    equity_lifecycle_window,
)
from northstar_application.ports import (
    EquityRecommendationLifecycleStore,
    EquityRecommendationLifecycleStoreUnavailableError,
    EquityRecommendationLifecycleWriteOutcome,
    InvalidEquityRecommendationLifecycleWriteError,
    InvalidStoredEquityRecommendationIssuanceAuthorityError,
    InvalidStoredEquityRecommendationLifecycleError,
    UnverifiedEquityRecommendationLifecycleAppendError,
)
from northstar_core.foundation.value_objects import PointInTime
from northstar_core.recommendations.equity.lifecycle import (
    EquityLifecycleEvent,
    EquityLifecycleEventKind,
    EquityLifecycleEvidence,
    EquityLifecycleState,
    EquityRevisionMateriality,
)

import northstar_infrastructure
from northstar_infrastructure import persistence
from northstar_infrastructure.persistence import (
    SQLiteEquityRecommendationLifecycleStore,
    initialize_equity_recommendation_lifecycle_schema,
)
from northstar_infrastructure.persistence import (
    sqlite_equity_recommendation_issuance as issuance_module,
)
from northstar_infrastructure.persistence import (
    sqlite_equity_recommendation_lifecycle as adapter_module,
)
from northstar_infrastructure.persistence import (
    sqlite_equity_recommendation_lifecycle_schema as schema_module,
)
from tests.equity_recommendation_acceptance_fixtures import (
    DISPOSITIONS,
    INFY,
    bar_end,
    check_database,
    events,
    ist,
    raw,
    rows,
    schema_objects,
)
from tests.equity_recommendation_issuance_fixtures import (
    DECLARATIONS,
    ISSUANCES,
    LEDGER,
    B,
    authority,
    establish_infy_breakout,
    infy,
    issuance_world,
)
from tests.equity_recommendation_lifecycle_fixtures import (
    CANCEL_BAR,
    ENTRY_BAR,
    LIFECYCLE_EVENTS,
    QUIET_BAR,
    TARGET_BAR,
    KeepingLifecycleStore,
    LifecycleWorld,
    lifecycle_world,
    record_in_another_process,
    record_then_exit_before_commit,
    store,
)

K = EquityLifecycleEventKind
S = EquityLifecycleState
_O = EquityLifecycleRecordingOutcome
_R = EquityLifecycleRecordingRefusal
_W = EquityRecommendationLifecycleWriteOutcome
_Integrity = InvalidStoredEquityRecommendationLifecycleError
_Authority = InvalidStoredEquityRecommendationIssuanceAuthorityError
_FORMAT = "northstar.equity-recommendation-lifecycle-event/1"
_INFRA_ROOT = Path(__file__).resolve().parents[1]
_T = LIFECYCLE_EVENTS
_TRIGGERS = tuple(
    f"{_T}_{name}"
    for name in (
        "no_update",
        "no_delete",
        "no_replace",
        "next_sequence",
        "canonical_instant",
        "chronology",
        "not_before_issuance",
        "terminal",
    )
)


@pytest.fixture(autouse=True)
def _refuse_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("EI-6B tests must not open network connections.")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)


@pytest.fixture
def world(tmp_path: Path) -> LifecycleWorld:
    """One MODELED TCS issuance; no lifecycle event yet."""
    return lifecycle_world(tmp_path / "ei6b.sqlite3")


@pytest.fixture
def opened(world: LifecycleWorld) -> LifecycleWorld:
    """ENTRY_WINDOW_OPENED recorded at 09:46:30 IST (sequence 1)."""
    result = world.record(world.opened())
    assert (result.outcome, result.state) == (_O.APPENDED, S.WAITING_FOR_ENTRY)
    return world


@pytest.fixture
def closed(opened: LifecycleWorld) -> LifecycleWorld:
    """ENTRY_ACTIVATED (09:51) and TARGET_REACHED (09:56): terminal CLOSED_TARGET."""
    assert opened.record(opened.activated()).state is S.ACTIVE
    assert opened.record(opened.targeted()).state is S.CLOSED_TARGET
    return opened


# ----------------------------------------------------------------------
# Inspection and TEST-ONLY raw access to the disposable file
# ----------------------------------------------------------------------


def _rows(database: Path) -> list[tuple[Any, ...]]:
    with closing(raw(database)) as connection:
        return connection.execute(
            f"SELECT * FROM {_T} ORDER BY issuance_id, event_sequence"  # noqa: S608
        ).fetchall()


def _documents(database: Path, issuance_id: str) -> list[str]:
    with closing(raw(database)) as connection:
        return [
            row[0]
            for row in connection.execute(
                f"SELECT event_document FROM {_T} WHERE issuance_id = ? "  # noqa: S608
                "ORDER BY event_sequence",
                (issuance_id,),
            )
        ]


def _stored_events(world: LifecycleWorld) -> list[EquityLifecycleEvent]:
    return [
        decode_equity_lifecycle_event_document(d)
        for d in _documents(world.database, world.issuance_id)
    ]


def _file_state(database: Path) -> tuple[str, int]:
    data = database.read_bytes()
    return hashlib.sha256(data).hexdigest(), len(data)


def _raw_transaction(
    database: Path,
    *statements: tuple[str, tuple[object, ...]],
    drop: tuple[str, ...] = (),
    checks: bool = True,
) -> None:
    """TEST-ONLY raw SQL in ONE transaction; a refused statement rolls it all back.

    Dropped triggers stay dropped until the adapter's next connection
    recreates them. Never evidence of the supported write path.
    """
    with closing(sqlite3.connect(database, isolation_level=None)) as connection:
        connection.execute("PRAGMA foreign_keys = ON")
        if not checks:
            connection.execute("PRAGMA ignore_check_constraints = ON")
        for trigger in drop:
            connection.execute(f"DROP TRIGGER IF EXISTS {trigger}")
        connection.execute("BEGIN IMMEDIATE")
        try:
            for statement, parameters in statements:
                connection.execute(statement, parameters)
        except BaseException:
            connection.execute("ROLLBACK")
            raise
        connection.execute("COMMIT")


def _admitted(
    database: Path, *statements: tuple[str, tuple[object, ...]], drop: tuple[str, ...] = ()
) -> None:
    """TEST-ONLY: run raw statements in a transaction that is always rolled back.

    It raises what the database raises; returning means the database admitted
    every statement. ``drop`` removes triggers inside the same transaction, so
    one guard can be probed alone; the rollback restores them. Nothing is ever
    committed.
    """
    with closing(sqlite3.connect(database, isolation_level=None)) as connection:
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("BEGIN IMMEDIATE")
        try:
            for trigger in drop:
                connection.execute(f"DROP TRIGGER {trigger}")
            for statement, parameters in statements:
                connection.execute(statement, parameters)
        finally:
            connection.execute("ROLLBACK")


def _row(issuance_id: str, event: EquityLifecycleEvent) -> tuple[object, ...]:
    return adapter_module.lifecycle_event_row(
        issuance_id, event, equity_lifecycle_event_document(event)
    )


def _insert(row: tuple[object, ...]) -> tuple[str, tuple[object, ...]]:
    return (adapter_module._INSERT_EVENT, row)  # noqa: SLF001 -- test-only


def _with_text(row: tuple[object, ...], column: str, text: str) -> tuple[object, ...]:
    """The row with one instant column and its document field set to ``text`` verbatim."""
    names = adapter_module.LIFECYCLE_EVENT_COLUMNS
    document = json.loads(row[-1])
    if column == "evaluated_at":
        document["evaluated_at"] = text
    else:
        document["evidence"][column.removeprefix("evidence_")] = text
    changed = list(row)
    changed[names.index(column)] = text
    changed[-1] = json.dumps(document, sort_keys=True, separators=(",", ":"))
    return tuple(changed)


def _update_document(
    world: LifecycleWorld, sequence: int, document: str, checks: bool = True, **columns: object
) -> None:
    """TEST-ONLY tampering: rewrite one stored event's document (and columns)."""
    assignments = ", ".join(["event_document = ?", *(f"{name} = ?" for name in columns)])
    _raw_transaction(
        world.database,
        (
            f"UPDATE {_T} SET {assignments} WHERE issuance_id = ? AND event_sequence = ?",  # noqa: S608
            (document, *columns.values(), world.issuance_id, sequence),
        ),
        drop=(f"{_T}_no_update",),
        checks=checks,
    )


@contextmanager
def _refused(match: str | None = None) -> Iterator[None]:
    with pytest.raises(sqlite3.IntegrityError, match=match):
        yield


def _expect_corrupt(world: LifecycleWorld, category: str, authorization: Any) -> None:
    """Load, append and the recording use case all meet the permanent failure; nothing changes."""
    before = _rows(world.database)
    with pytest.raises(_Integrity) as loaded:
        world.store().load_lifecycle(world.issuance_id)
    assert (loaded.value.category, loaded.value.retryable) == (category, False)
    with pytest.raises(_Integrity) as appended:
        world.store().append(authorization)
    assert appended.value.category == category
    with pytest.raises(EquityLifecycleRecordingRefusedError) as refused:
        world.record(world.snapshot(10, 30))
    assert (refused.value.reason, refused.value.retryable) == (_R.STORED_LIFECYCLE_INVALID, False)
    assert _rows(world.database) == before  # preserved; never repaired, adopted or extended


# ----------------------------------------------------------------------
# LS-16: additive schema in the EI-4D/EI-6A file; foreign files refused
# ----------------------------------------------------------------------

_EI6B_OBJECTS = {_T, *_TRIGGERS}


def test_ls16_the_schema_is_additive_idempotent_and_refuses_a_database_without_ei6a(
    world: LifecycleWorld, tmp_path: Path
) -> None:
    database = world.database
    before_objects = schema_objects(database)
    assert not any("lifecycle" in name for name in before_objects)  # EI-6A only, so far
    before_rows = {t: rows(database, t) for t in (DISPOSITIONS, LEDGER, ISSUANCES, DECLARATIONS)}
    with closing(raw(database)) as connection:
        version = connection.execute("PRAGMA user_version").fetchone()
        initialize_equity_recommendation_lifecycle_schema(connection)
        initialize_equity_recommendation_lifecycle_schema(connection)  # idempotent
        assert connection.execute("PRAGMA user_version").fetchone() == version  # no version
        indexes = connection.execute(f"PRAGMA index_list({_T})").fetchall()
        assert [(i[2], i[3]) for i in indexes] == [(1, "pk")]  # ordered reads by issuance
    after = schema_objects(database)
    assert set(after) - set(before_objects) == _EI6B_OBJECTS
    assert {name: after[name] for name in before_objects} == before_objects  # nothing altered
    assert {t: rows(database, t) for t in before_rows} == before_rows

    # The adapter's own connections add nothing more and alter nothing.
    assert world.store().load_lifecycle(world.issuance_id) is not None
    assert world.store().load_lifecycle(world.issuance_id) is not None
    assert schema_objects(database) == after
    # EI-6A keeps working beside it: a later declaration and its views.
    assert world.world.declare(ist(10, 0)).outcome.value == "DECLARED"
    assert authority(database).load_issuance(world.issuance_id) is not None
    check_database(database)

    # An EI-4D database without the EI-6A issuance authority is refused.
    ei4d_only = issuance_world(tmp_path / "ei4d-only.sqlite3").database
    ei4d_objects = schema_objects(ei4d_only)
    with closing(sqlite3.connect(ei4d_only)) as connection:
        with pytest.raises(sqlite3.OperationalError, match="no EI-6A issuance authority"):
            initialize_equity_recommendation_lifecycle_schema(connection)
    with pytest.raises(EquityRecommendationLifecycleStoreUnavailableError):
        store(ei4d_only).load_lifecycle(world.issuance_id)
    assert schema_objects(ei4d_only) == ei4d_objects  # neither EI-6A nor EI-6B created
    foreign = tmp_path / "foreign.sqlite3"
    sqlite3.connect(foreign).close()
    with pytest.raises(EquityRecommendationLifecycleStoreUnavailableError):
        store(foreign).load_lifecycle(world.issuance_id)
    assert schema_objects(foreign) == {}
    missing = tmp_path / "missing.sqlite3"
    with pytest.raises(EquityRecommendationLifecycleStoreUnavailableError):
        store(missing).load_lifecycle(world.issuance_id)
    assert not missing.exists()  # never creates a database


# ----------------------------------------------------------------------
# LS-03, LS-01, LS-02: reads and authorized appends through the real chain
# ----------------------------------------------------------------------


def test_ls03_a_known_issuance_without_events_is_an_empty_recorded_lifecycle(
    world: LifecycleWorld,
) -> None:
    loaded = world.store().load_lifecycle(world.issuance_id)
    assert loaded is not None
    assert (loaded.lifecycle.state, loaded.lifecycle.events, loaded.position) == (S.RECORDED, (), 0)
    assert loaded.issuance == authority(world.database).load_issuance(world.issuance_id)
    assert loaded.issuance.recording_sequence == 2
    assert _rows(world.database) == []  # reading writes nothing


def test_ls03_b_an_unknown_issuance_is_none_and_the_recorder_refuses_it(
    world: LifecycleWorld,
) -> None:
    unknown = "eiris1-sha256:" + "0" * 64
    assert world.store().load_lifecycle(unknown) is None
    with pytest.raises(EquityLifecycleRecordingRefusedError) as refused:
        world.record(world.opened(), issuance_id=unknown)
    assert (refused.value.reason, refused.value.retryable) == (_R.ISSUANCE_NOT_FOUND, False)
    with pytest.raises(TypeError):
        world.store().load_lifecycle(None)  # type: ignore[arg-type]
    assert _rows(world.database) == []


def test_ls01_ls02_a_appends_store_sequences_1_to_n_that_reload_through_core(
    world: LifecycleWorld,
) -> None:
    first = world.record(world.opened())
    assert (first.outcome, first.writes, first.reconciled) == (_O.APPENDED, 1, False)
    assert [e.kind for e in first.new_events] == [K.ENTRY_WINDOW_OPENED]
    later = [world.record(world.activated()), world.record(world.targeted())]
    assert [r.outcome for r in later] == [_O.APPENDED, _O.APPENDED]
    assert [r.state for r in later] == [S.ACTIVE, S.CLOSED_TARGET]

    stored = _rows(world.database)
    assert [(r[1], r[3], r[4], r[5]) for r in stored] == [
        (1, "ENTRY_WINDOW_OPENED", "RECORDED", "WAITING_FOR_ENTRY"),
        (2, "ENTRY_ACTIVATED", "WAITING_FOR_ENTRY", "ACTIVE"),
        (3, "TARGET_REACHED", "ACTIVE", "CLOSED_TARGET"),
    ]
    loaded = world.store().load_lifecycle(world.issuance_id)
    assert loaded is not None and loaded == later[-1].recorded
    assert loaded.lifecycle.state is S.CLOSED_TARGET
    # Each row is exactly the Application's canonical document and its columns.
    documents = equity_lifecycle_event_documents(loaded.lifecycle.events)
    assert tuple(_documents(world.database, world.issuance_id)) == documents
    for row, event, document in zip(stored, loaded.lifecycle.events, documents, strict=True):
        assert row == adapter_module.lifecycle_event_row(world.issuance_id, event, document)
        assert json.loads(document)["format"] == _FORMAT
    check_database(world.database)


def test_ls02_b_quiet_and_repeated_evaluations_write_and_allocate_nothing(
    opened: LifecycleWorld,
) -> None:
    before_rows, before_objects = _rows(opened.database), schema_objects(opened.database)
    quiet = opened.snapshot(9, 48)  # bar 7 still open: nothing to evaluate
    repeated = _stored_events(opened)[0].evidence.reference
    for snapshot in (quiet, quiet, repeated, quiet):
        result = opened.record(snapshot)
        assert (result.outcome, result.new_events, result.writes) == (_O.NO_NEW_EVENTS, (), 0)
        assert result.state is S.WAITING_FOR_ENTRY
    assert _rows(opened.database) == before_rows  # no row, no sequence allocated
    assert schema_objects(opened.database) == before_objects  # no cursor or offset table
    assert not any(
        word in name for name in before_objects for word in ("cursor", "offset", "monitor")
    )
    # The next event still takes the next sequence.
    assert [e.sequence for e in opened.record(opened.activated()).new_events] == [2]


# ----------------------------------------------------------------------
# LS-04: parent provenance
# ----------------------------------------------------------------------


def test_ls04_a_the_foreign_key_refuses_an_event_for_a_non_issued_identity(
    opened: LifecycleWorld,
) -> None:
    (event,) = _stored_events(opened)
    stranger = "eiris1-sha256:" + "1" * 64
    with _refused("FOREIGN KEY"):
        _admitted(opened.database, _insert(_row(stranger, event)))
    assert [r[0] for r in _rows(opened.database)] == [opened.issuance_id]


def test_ls04_b_corrupt_parent_authority_raises_the_d6_5_error_on_load_and_append(
    opened: LifecycleWorld,
) -> None:
    authorization = opened.capture(opened.activated())
    lifecycle_rows = _rows(opened.database)
    _raw_transaction(
        opened.database,
        (f"UPDATE {ISSUANCES} SET issued_at_us = issued_at_us + 1", ()),  # noqa: S608
        drop=(f"{ISSUANCES}_no_update",),
    )
    with pytest.raises(_Authority) as loaded:
        opened.store().load_lifecycle(opened.issuance_id)
    assert (loaded.value.category, loaded.value.issuance_id) == ("ISSUANCE", opened.issuance_id)
    with pytest.raises(_Authority):
        opened.store().append(authorization)
    with pytest.raises(EquityLifecycleRecordingRefusedError) as refused:
        opened.record(opened.snapshot(9, 52))
    assert refused.value.reason is _R.STORED_AUTHORITY_INVALID
    assert _rows(opened.database) == lifecycle_rows  # nothing written or repaired


def test_ls04_b_an_inconsistent_ei6a_ledger_fails_lifecycle_reads_and_appends_closed(
    opened: LifecycleWorld,
) -> None:
    authorization = opened.capture(opened.activated())
    _raw_transaction(  # TEST-ONLY: a committed ledger entry with no record
        opened.database,
        (issuance_module._INSERT_LEDGER, (3, "DECLARATION")),  # noqa: SLF001
    )
    for action in (
        lambda: opened.store().load_lifecycle(opened.issuance_id),
        lambda: opened.store().append(authorization),
    ):
        with pytest.raises(_Authority) as raised:
            action()
        assert raised.value.category == "LEDGER"
    assert len(_rows(opened.database)) == 1


def test_ls04_c_a_matching_identifier_alone_is_not_authority(
    world: LifecycleWorld, tmp_path: Path
) -> None:
    authorization = world.capture(world.opened())
    # Another file holds an issuance with the same identity, recorded at another sequence.
    other = issuance_world(tmp_path / "other.sqlite3")
    assert other.declare(PointInTime("2026-08-19T04:13:29.999998Z")).outcome.value == "DECLARED"
    assert other.declare(B).outcome.value == "DECLARED"
    issued = other.issue(other.proposed())
    assert (issued.issuance.issuance_id, issued.recording_sequence) == (world.issuance_id, 3)
    with pytest.raises(InvalidEquityRecommendationLifecycleWriteError, match="not the auth"):
        store(other.database).append(authorization)
    # A file that holds no such issuance at all.
    bare = issuance_world(tmp_path / "bare.sqlite3")
    assert bare.declare(B).outcome.value == "DECLARED"
    with pytest.raises(InvalidEquityRecommendationLifecycleWriteError, match="holds no issuance"):
        store(bare.database).append(authorization)
    assert _rows(other.database) == _rows(bare.database) == []
    assert world.store().append(authorization).outcome is _W.APPENDED  # its own file accepts it


# ----------------------------------------------------------------------
# LS-05: unauthorized appends are refused before any connection opens
# ----------------------------------------------------------------------


def _tamper(obj: object, name: str, value: object) -> None:
    object.__setattr__(obj, name, value)


def _later(event: EquityLifecycleEvent, seconds: int = 1) -> EquityLifecycleEvent:
    moved = ist(9, 51, seconds)
    return dataclasses.replace(event, evaluated_at=moved)


_MUTATIONS: dict[str, Callable[[Any], object]] = {
    "none": lambda a: None,
    "a plain object": lambda a: object(),
    "a raw dictionary": lambda a: {"issuance_id": a.issuance_id, "events": []},
    "a raw canonical document": lambda a: a.event_documents[0],
    "raw Core events": lambda a: a.events,
    "built directly (never issued)": lambda a: VerifiedEquityRecommendationLifecycleAppend(
        a.issuance, a.window, a.prior, a.events
    ),
    "copied": lambda a: copy.copy(a),
    "dataclasses.replace": lambda a: dataclasses.replace(a),
    "prior events changed": lambda a: _tamper(a, "prior", ()) or a,
    "new events changed": lambda a: _tamper(a, "events", (_later(a.events[0]),)) or a,
    "nested evidence changed": lambda a: (
        _tamper(
            a,
            "events",
            (
                dataclasses.replace(
                    a.events[0],
                    evidence=dataclasses.replace(a.events[0].evidence, observations=("forged",)),
                ),
            ),
        )
        or a
    ),
    "parent issuance changed": lambda a: (
        _tamper(a, "issuance", EquityRecordedIssuance(a.issuance.recommendation, 99)) or a
    ),
    "window changed": lambda a: (
        _tamper(a, "window", dataclasses.replace(a.window, entry_window_ends_at=ist(10, 55))) or a
    ),
    "sequence position changed": lambda a: (
        _tamper(a, "events", (dataclasses.replace(a.events[0], sequence=3),)) or a
    ),
    "covered documents changed": lambda a: (
        _tamper(a, "event_documents", (a.prior_documents[0],)) or a
    ),
}


@pytest.mark.parametrize("mutation", sorted(_MUTATIONS))
def test_ls05_an_unauthorized_append_is_refused_before_any_connection_opens(
    opened: LifecycleWorld, monkeypatch: pytest.MonkeyPatch, mutation: str
) -> None:
    activated = opened.activated()
    offered = _MUTATIONS[mutation](opened.capture(activated))
    genuine = opened.capture(activated)  # captured before the spy: captures read the store
    before = _file_state(opened.database)
    connections: list[Path] = []
    real = adapter_module._connect  # noqa: SLF001

    def spy(path: Path) -> sqlite3.Connection:
        connections.append(path)
        return real(path)

    monkeypatch.setattr(adapter_module, "_connect", spy)
    with pytest.raises(UnverifiedEquityRecommendationLifecycleAppendError):
        opened.store().append(offered)  # type: ignore[arg-type]
    assert connections == []  # refused before SQLite was touched
    assert _file_state(opened.database) == before
    assert opened.store().append(genuine).outcome is _W.APPENDED  # the spy is live
    assert len(connections) == 1


# ----------------------------------------------------------------------
# LS-06, LS-07, LS-08: the database's own guards (defence in depth)
# ----------------------------------------------------------------------


def test_ls06_update_delete_and_replacing_inserts_are_refused(closed: LifecycleWorld) -> None:
    before = _rows(closed.database)
    first = before[0]
    key = (closed.issuance_id, 1)
    for statement, parameters in (
        (f"UPDATE {_T} SET evidence_reference = evidence_reference", ()),
        (f"UPDATE {_T} SET event_sequence = 9 WHERE issuance_id = ? AND event_sequence = ?", key),
        (f"DELETE FROM {_T} WHERE issuance_id = ? AND event_sequence = ?", key),
        (f"DELETE FROM {_T}", ()),
    ):
        with _refused("insert-only"):
            _admitted(closed.database, (statement, parameters))  # noqa: S608
    insert = adapter_module._INSERT_EVENT  # noqa: SLF001
    for statement in (
        insert,
        insert.replace("INSERT", "INSERT OR REPLACE", 1),
        insert.replace("INSERT", "REPLACE", 1),
        insert.replace("INSERT", "INSERT OR IGNORE", 1),
        insert + " ON CONFLICT (issuance_id, event_sequence) DO UPDATE SET event_kind = 'X'",
    ):
        with _refused():  # refused, by whichever overlapping guard fires first
            _admitted(closed.database, (statement, first))
        with _refused("insert-only"):  # ... and by the no-replace trigger alone
            _admitted(
                closed.database,
                (statement, first),
                drop=tuple(f"{_T}_{t}" for t in ("next_sequence", "chronology", "terminal")),
            )
    assert schema_objects(closed.database).keys() >= _EI6B_OBJECTS  # every probe rolled back
    assert _rows(closed.database) == before  # nothing replaced, deleted or rewritten


def test_ls07_sequences_are_contiguous_per_issuance_and_leave_the_ei6a_ledger_unchanged(
    opened: LifecycleWorld,
) -> None:
    (event,) = _stored_events(opened)
    duplicate = _row(opened.issuance_id, event)
    gap = _row(opened.issuance_id, dataclasses.replace(event, sequence=3))
    zero = (*duplicate[:1], 0, *duplicate[2:])
    for row, match in ((duplicate, "insert-only|the next one"), (gap, "the next one"), (zero, "")):
        with _refused(match or None):
            _admitted(opened.database, _insert(row))
    assert len(_rows(opened.database)) == 1

    # A second issuance in the same file has its own sequence domain.
    establish_infy_breakout(opened.world)
    other = opened.world.issue(opened.world.proposed(**infy()))
    assert (other.outcome.value, other.recording_sequence) == ("ISSUED", 3)
    ledger = rows(opened.database, LEDGER)
    infy_id = other.issuance.issuance_id
    result = opened.record(opened.snapshot(9, 46, 30, listing=INFY), issuance_id=infy_id)
    assert [e.sequence for e in result.new_events] == [1]
    assert [e.sequence for e in opened.record(opened.activated()).new_events] == [2]
    by_issuance = [(r[0], r[1]) for r in _rows(opened.database)]
    assert sorted(by_issuance) == sorted(
        [(opened.issuance_id, 1), (opened.issuance_id, 2), (infy_id, 1)]
    )
    assert rows(opened.database, LEDGER) == ledger  # lifecycle events never enter the ledger
    check_database(opened.database)


def test_ls08_chronology_uses_integer_microseconds_and_the_parent_issued_at(
    world: LifecycleWorld,
) -> None:
    authorization = world.capture(world.opened())
    (event,) = authorization.events  # ENTRY_WINDOW_OPENED, evaluated 04:16:30Z
    issued_at = world.issuance.issued_at.value
    assert issued_at == "2026-08-19T04:13:30Z"

    def at(text: str, sequence: int = 1) -> tuple[object, ...]:
        moved = dataclasses.replace(event, sequence=sequence, evaluated_at=PointInTime(text))
        return _row(world.issuance_id, moved)

    # Not before the parent's issued_at, compared as integers.
    with _refused("must not precede its issuance"):
        _admitted(world.database, _insert(at("2026-08-19T04:13:29.999999Z")))
    _admitted(world.database, _insert(at(issued_at)))  # exactly at it: admitted
    _admitted(world.database, _insert(at("2026-08-19T04:13:30.5Z")))  # text sorts it earlier

    assert world.store().append(authorization).outcome is _W.APPENDED
    # Not before the issuance's latest event; equal instants are admitted.
    with _refused("must not precede the latest event"):
        _admitted(world.database, _insert(at("2026-08-19T04:16:29.999999Z", 2)))
    _admitted(world.database, _insert(at("2026-08-19T04:16:30Z", 2)))
    _admitted(world.database, _insert(at("2026-08-19T04:16:30.5Z", 2)))  # text sorts earlier

    # Canonical text with exactly its microseconds.
    base = at("2026-08-19T04:16:31Z", 2)
    names = adapter_module.LIFECYCLE_EVENT_COLUMNS
    disagreeing = list(base)
    exact = adapter_module.instant_microseconds(PointInTime("2026-08-19T04:16:31Z"))
    disagreeing[names.index("evaluated_at_us")] = exact + 1
    with _refused("canonical UTC text"):
        _admitted(world.database, _insert(tuple(disagreeing)))
    for column, text in (
        ("evaluated_at", "2026-08-19T04:16:31.500Z"),
        ("evaluated_at", "2026-08-19T04:16:31+00:00"),
        ("evaluated_at", "2026-08-19 04:16:31Z"),
        ("evidence_opens_at", "2026-08-19T04:15:00.0Z"),
        ("evidence_closes_at", "2026-08-19T09:50:00+05:30"),
    ):
        with _refused("canonical UTC text"):
            _admitted(world.database, _insert(_with_text(base, column, text)))
    assert len(_rows(world.database)) == 1  # every probe was rolled back
    assert _rows(world.database)[0][7] == adapter_module.instant_microseconds(event.evaluated_at)


# ----------------------------------------------------------------------
# LS-09, LS-10, RR-05: strict rehydration and permanent corruption
# ----------------------------------------------------------------------


def _canonical(document: dict[str, Any]) -> str:
    return json.dumps(document, sort_keys=True, separators=(",", ":"))


def _event_corruptions(text: str) -> dict[str, tuple[str, dict[str, object], bool]]:
    """(document, changed columns, CHECKs on) for each stored-event corruption."""
    document = json.loads(text)
    extra, missing, float_sequence, boolean = (dict(document) for _ in range(4))
    extra["note"] = "x"
    del missing["materiality"]
    boolean["cited_sequence"] = True
    later = "2026-08-19T04:16:30.000Z"
    return {
        "whitespace": (json.dumps(document, sort_keys=True, indent=1), {}, True),
        "unsorted keys": (
            json.dumps(dict(reversed(list(document.items()))), separators=(",", ":")),
            {},
            True,
        ),
        "extra key": (_canonical(extra), {}, True),
        "missing key": (_canonical(missing), {}, True),
        "duplicate key": (text[:-1] + ',"reason":null}', {}, True),
        "boolean for integer": (_canonical(boolean), {}, True),
        "non-canonical instant": (
            text.replace(document["evaluated_at"], later),
            {"evaluated_at": later},
            True,
        ),
        "integer microseconds disagree": (text, {"evaluated_at_us": 1}, True),
        # These the database's CHECKs refuse; with CHECKs off, rehydration still does.
        "unsupported format": (
            text.replace(_FORMAT, _FORMAT[:-1] + "2"),
            {"event_format": _FORMAT[:-1] + "2"},
            False,
        ),
        "unknown kind": (
            text.replace("ENTRY_WINDOW_OPENED", "ENTRY_WINDOW_REOPENED"),
            {"event_kind": "ENTRY_WINDOW_REOPENED"},
            False,
        ),
        "float sequence": (text.replace('"sequence":1', '"sequence":1.0'), {}, False),
        "column disagrees with document": (text, {"evidence_reference": "forged"}, False),
    }


_EVENT_CASES = (
    "boolean for integer",
    "column disagrees with document",
    "duplicate key",
    "extra key",
    "float sequence",
    "integer microseconds disagree",
    "missing key",
    "non-canonical instant",
    "unknown kind",
    "unsorted keys",
    "unsupported format",
    "whitespace",
)


@pytest.mark.parametrize("case", _EVENT_CASES)
def test_ls09_a_stored_event_that_is_not_strictly_canonical_is_permanent_event_corruption(
    opened: LifecycleWorld, case: str
) -> None:
    authorization = opened.capture(opened.activated())
    (text,) = _documents(opened.database, opened.issuance_id)
    corruptions = _event_corruptions(text)
    assert set(corruptions) == set(_EVENT_CASES)
    document, columns, checks = corruptions[case]
    if not checks:  # the database refuses it outright ...
        with _refused("CHECK"):
            _update_document(opened, 1, document, **columns)
    _update_document(opened, 1, document, checks=checks, **columns)  # ... TEST-ONLY bypass
    _expect_corrupt(opened, "EVENT", authorization)


def test_ls10_a_column_disagreeing_with_its_document_is_refused_by_check(
    opened: LifecycleWorld,
) -> None:
    (event,) = _stored_events(opened)
    row = _row(opened.issuance_id, dataclasses.replace(event, sequence=2))
    names = adapter_module.LIFECYCLE_EVENT_COLUMNS
    for column, value in (
        ("event_kind", "ENTRY_ACTIVATED"),
        ("to_state", "ACTIVE"),
        ("evaluated_at", "2026-08-19T04:16:31Z"),
        ("evidence_reference", "another"),
        ("evidence_opens_at", "2026-08-19T04:20:00Z"),
        ("event_document", "not json"),
        ("event_document", "[1]"),
    ):
        changed = list(row)
        changed[names.index(column)] = value
        if column == "evaluated_at":
            changed[names.index("evaluated_at_us")] = adapter_module.instant_microseconds(
                PointInTime(value)
            )
        with _refused("CHECK"):
            _admitted(opened.database, _insert(tuple(changed)))
    for column in ("event_document", "evidence_reference", "evidence_closes_at", "event_format"):
        missing = list(row)
        missing[names.index(column)] = None
        with _refused("NOT NULL|CHECK|canonical"):  # a BEFORE trigger may refuse it first
            _admitted(opened.database, _insert(tuple(missing)))
    assert len(_rows(opened.database)) == 1


Planted = tuple[Any, Callable[[], None]]


def _gap(w: LifecycleWorld) -> Planted:
    w.record(w.activated())
    authorization = w.capture(w.targeted())
    return authorization, lambda: _raw_transaction(
        w.database,
        (f"DELETE FROM {_T} WHERE event_sequence = 1", ()),  # noqa: S608
        drop=(f"{_T}_no_delete",),
    )


def _repeated_open(w: LifecycleWorld) -> Planted:
    authorization = w.capture(w.activated())
    (first,) = _stored_events(w)  # the database admits it: event shapes are Core's
    again = dataclasses.replace(first, sequence=2)
    return authorization, lambda: _raw_transaction(w.database, _insert(_row(w.issuance_id, again)))


def _wrong_transition(w: LifecycleWorld) -> Planted:
    w.bars({7: ENTRY_BAR, 8: TARGET_BAR})
    authorization = w.capture(w.snapshot(9, 56))  # [ENTRY_ACTIVATED, TARGET_REACHED]
    target = dataclasses.replace(authorization.events[1], sequence=2)
    return authorization, lambda: _raw_transaction(w.database, _insert(_row(w.issuance_id, target)))


def _decreasing(w: LifecycleWorld) -> Planted:
    activated = w.activated()
    authorization = w.capture(activated)
    w.record(activated)
    w.record(w.targeted())  # [OPENED 09:46:30, ACTIVATED 09:51, TARGET_REACHED 09:56]
    activation = _stored_events(w)[1]
    earlier = EquityLifecycleEvent(  # a revision evaluated before the latest event
        sequence=4,
        kind=K.SOURCE_REVISION_RECORDED,
        from_state=S.CLOSED_TARGET,
        to_state=S.CLOSED_TARGET,
        evaluated_at=ist(9, 52),
        evidence=EquityLifecycleEvidence(
            "revised", activation.evidence.opens_at, activation.evidence.closes_at
        ),
        cited_sequence=2,
        superseded_evidence=activation.evidence,
        materiality=EquityRevisionMateriality.UNASSESSED,
    )
    return authorization, lambda: _raw_transaction(
        w.database, _insert(_row(w.issuance_id, earlier)), drop=(f"{_T}_chronology",)
    )


def _after_terminal(w: LifecycleWorld) -> Planted:
    activated = w.activated()
    authorization = w.capture(activated)
    w.record(activated)
    w.record(w.targeted())
    again = dataclasses.replace(authorization.events[0], sequence=4, evaluated_at=ist(9, 57))
    return authorization, lambda: _raw_transaction(
        w.database, _insert(_row(w.issuance_id, again)), drop=(f"{_T}_terminal",)
    )


_HISTORY_CORRUPTIONS: dict[str, Callable[[LifecycleWorld], Planted]] = {
    "sequence gap": _gap,
    "repeated ENTRY_WINDOW_OPENED": _repeated_open,
    "transition from the wrong state": _wrong_transition,
    "decreasing evaluation instants": _decreasing,
    "state change after a terminal state": _after_terminal,
}


@pytest.mark.parametrize("case", sorted(_HISTORY_CORRUPTIONS))
def test_ls10_a_stored_history_core_refuses_is_permanent_history_corruption(
    opened: LifecycleWorld, case: str
) -> None:
    authorization, corrupt = _HISTORY_CORRUPTIONS[case](opened)
    corrupt()
    _expect_corrupt(opened, "HISTORY", authorization)


def test_rr05_a_tampered_stored_event_is_refused_on_reload_and_preserved(
    opened: LifecycleWorld,
) -> None:
    authorization = opened.capture(opened.activated())
    (text,) = _documents(opened.database, opened.issuance_id)
    shifted = "2026-08-19T04:10:00Z"  # ENTRY_WINDOW_OPENED must cite the bar at eligible_from
    document = json.loads(text)
    document["evidence"]["opens_at"] = shifted
    _update_document(opened, 1, _canonical(document), evidence_opens_at=shifted)
    tampered = _rows(opened.database)
    _expect_corrupt(opened, "HISTORY", authorization)
    assert _rows(opened.database) == tampered


def test_rr05_d6b4_limitation_a_canonical_core_valid_rewrite_is_not_detected(
    opened: LifecycleWorld,
) -> None:
    """Documents D6B-4: there is no content fingerprint and no cryptographic tamper detection.

    A privileged rewrite that stays canonical, agrees with its columns and
    still replays through Core is not detectable by any EI-6B check.
    """
    (text,) = _documents(opened.database, opened.issuance_id)
    document = json.loads(text)
    forged = "ecs1-sha256:" + "f" * 64
    document["evidence"]["reference"] = forged
    _update_document(opened, 1, _canonical(document), evidence_reference=forged)
    loaded = opened.store().load_lifecycle(opened.issuance_id)
    assert loaded is not None and loaded.lifecycle.events[0].evidence.reference == forged


# ----------------------------------------------------------------------
# LS-11: atomic multi-event batches
# ----------------------------------------------------------------------


def _three_event_batch(world: LifecycleWorld) -> Any:
    world.bars({7: ENTRY_BAR, 8: TARGET_BAR})
    authorization = world.capture(world.snapshot(9, 56))
    assert [e.kind for e in authorization.events] == [
        K.ENTRY_WINDOW_OPENED,
        K.ENTRY_ACTIVATED,
        K.TARGET_REACHED,
    ]
    return authorization


def test_ls11_a_row_refused_mid_batch_persists_none_and_the_next_append_reuses_the_sequences(
    world: LifecycleWorld,
) -> None:
    authorization = _three_event_batch(world)
    _raw_transaction(  # TEST-ONLY: refuse the second row of any batch
        world.database,
        (
            f"CREATE TRIGGER test_refuse_second BEFORE INSERT ON {_T} "
            "WHEN NEW.event_sequence = 2 BEGIN SELECT RAISE(ABORT, 'test: refused'); END",
            (),
        ),
    )
    with pytest.raises(InvalidEquityRecommendationLifecycleWriteError, match="test: refused"):
        world.store().append(authorization)
    assert _rows(world.database) == []  # the first row went with the batch
    _raw_transaction(world.database, ("DROP TRIGGER test_refuse_second", ()))
    written = world.store().append(authorization)
    assert written.outcome is _W.APPENDED
    assert [r[1] for r in _rows(world.database)] == [1, 2, 3]  # the same sequences, no gap


@pytest.mark.parametrize(
    "failure", [RuntimeError("test: after the inserts"), sqlite3.OperationalError("test: I/O")]
)
def test_ls11_a_failure_after_the_inserts_before_commit_persists_none(
    world: LifecycleWorld, monkeypatch: pytest.MonkeyPatch, failure: BaseException
) -> None:
    authorization = _three_event_batch(world)
    seen: list[int] = []

    def fail(connection: sqlite3.Connection, issuance_id: str, expected: int) -> None:
        seen.append(connection.execute(f"SELECT COUNT(*) FROM {_T}").fetchone()[0])  # noqa: S608
        raise failure

    monkeypatch.setattr(adapter_module, "_confirm_appended", fail)
    expected = (
        EquityRecommendationLifecycleStoreUnavailableError
        if isinstance(failure, sqlite3.Error)
        else RuntimeError
    )
    with pytest.raises(expected):
        world.store().append(authorization)
    assert seen == [3]  # inserted inside the transaction ...
    assert _rows(world.database) == []  # ... and rolled back with it
    monkeypatch.undo()
    assert world.store().append(authorization).outcome is _W.APPENDED
    assert [r[1] for r in _rows(world.database)] == [1, 2, 3]


def test_ls11_a_process_that_ends_before_commit_leaves_nothing(world: LifecycleWorld) -> None:
    world.bars({7: ENTRY_BAR, 8: TARGET_BAR})
    snapshot = world.snapshot(9, 56)
    payload = json.dumps({"issuance_id": world.issuance_id, "snapshots": [snapshot]})
    context = multiprocessing.get_context("spawn")
    process = context.Process(
        target=record_then_exit_before_commit, args=(str(world.database), payload)
    )
    process.start()
    process.join(timeout=240)
    assert process.exitcode == 17  # it died with the batch inserted and uncommitted
    assert _rows(world.database) == []
    check_database(world.database)
    result = world.record(snapshot)
    assert (result.outcome, [e.sequence for e in result.new_events]) == (_O.APPENDED, [1, 2, 3])


def test_ls11_raw_batches_with_a_bad_later_row_are_refused_whole(world: LifecycleWorld) -> None:
    authorization = _three_event_batch(world)
    good = [_row(world.issuance_id, e) for e in authorization.events]
    early = dataclasses.replace(authorization.events[1], evaluated_at=ist(9, 50))
    late_first = dataclasses.replace(authorization.events[0], evaluated_at=ist(9, 57))
    for batch, match in (
        ([good[0], good[2]], "the next one"),  # a gap
        ([good[0], good[0]], "insert-only|the next one"),  # a duplicate sequence
        ([_row(world.issuance_id, late_first), _row(world.issuance_id, early)], "latest event"),
    ):
        with _refused(match):
            _raw_transaction(world.database, *(_insert(r) for r in batch))
        assert _rows(world.database) == []
    # Inconsistent batches never reach the database through the supported path: Core refuses.
    with pytest.raises(UnverifiedEquityRecommendationLifecycleAppendError):
        VerifiedEquityRecommendationLifecycleAppend(
            authorization.issuance,
            authorization.window,
            (),
            (authorization.events[0], authorization.events[2]),
        )


# ----------------------------------------------------------------------
# LS-12: commit uncertainty, through the real recording use case
# ----------------------------------------------------------------------


def test_ls12_a_lost_acknowledgement_reconciles_to_equivalent_with_one_batch(
    world: LifecycleWorld, monkeypatch: pytest.MonkeyPatch
) -> None:
    snapshot = world.opened()
    real, lost = adapter_module._commit, []  # noqa: SLF001

    def commit(connection: sqlite3.Connection) -> None:
        real(connection)
        lost.append(1)
        raise sqlite3.OperationalError("test: COMMIT acknowledgment lost")

    monkeypatch.setattr(adapter_module, "_commit", commit)
    result = world.record(snapshot)
    assert (result.outcome, result.writes, result.reconciled) == (_O.EQUIVALENT, 1, True)
    assert lost == [1] and [r[1] for r in _rows(world.database)] == [1]  # exactly once
    monkeypatch.undo()
    assert result.recorded == world.store().load_lifecycle(world.issuance_id)
    check_database(world.database)


def test_ls12_a_later_writer_after_an_uncertain_commit_still_confirms_the_prefix(
    world: LifecycleWorld, monkeypatch: pytest.MonkeyPatch
) -> None:
    opened, activated = world.opened(), world.activated()
    real, busy, nested = adapter_module._commit, [], []  # noqa: SLF001

    def commit(connection: sqlite3.Connection) -> None:
        real(connection)
        if busy:
            return  # the later writer's own commit
        busy.append(1)
        nested.append(world.record(activated))  # another writer appends on top
        raise sqlite3.OperationalError("test: COMMIT acknowledgment lost")

    monkeypatch.setattr(adapter_module, "_commit", commit)
    keeping = KeepingLifecycleStore(world.database)
    result = world.record(opened, lifecycles=keeping)
    monkeypatch.undo()
    assert nested[0].outcome is _O.APPENDED
    assert (result.outcome, result.reconciled) == (_O.EQUIVALENT, True)
    assert [e.kind for e in result.recorded.lifecycle.events] == [
        K.ENTRY_WINDOW_OPENED,
        K.ENTRY_ACTIVATED,
    ]
    # The store's own rule stays exact: the same authorization now meets a longer history.
    (authorization,) = keeping.authorizations
    assert world.store().append(authorization).outcome is _W.CONFLICT
    assert [r[1] for r in _rows(world.database)] == [1, 2]


def test_ls12_a_commit_lost_before_effect_is_retried_within_the_bound(
    world: LifecycleWorld, monkeypatch: pytest.MonkeyPatch
) -> None:
    snapshot = world.opened()
    real, failures = adapter_module._commit, []  # noqa: SLF001

    def lost_once(connection: sqlite3.Connection) -> None:
        if not failures:
            failures.append(1)
            raise sqlite3.OperationalError("test: COMMIT lost before it took effect")
        real(connection)

    monkeypatch.setattr(adapter_module, "_commit", lost_once)
    result = world.record(snapshot)
    assert (result.outcome, result.writes, result.reconciled) == (_O.APPENDED, 2, False)
    assert [r[1] for r in _rows(world.database)] == [1]


def test_ls12_unresolved_commit_uncertainty_claims_nothing(
    world: LifecycleWorld, monkeypatch: pytest.MonkeyPatch
) -> None:
    snapshot = world.opened()

    def lost(connection: sqlite3.Connection) -> None:
        raise sqlite3.OperationalError("test: COMMIT lost before it took effect")

    monkeypatch.setattr(adapter_module, "_commit", lost)
    with pytest.raises(EquityLifecycleRecordingUnresolvedError):
        world.record(snapshot)  # three writes, each reload finds the prior: bounded
    assert _rows(world.database) == []

    real_connect, failed = adapter_module._connect, []  # noqa: SLF001

    def lost_then_unreadable(connection: sqlite3.Connection) -> None:
        failed.append(1)
        raise sqlite3.OperationalError("test: COMMIT lost")

    def connect(database_path: Path) -> sqlite3.Connection:
        if failed:
            raise EquityRecommendationLifecycleStoreUnavailableError("test: reload cannot open")
        return real_connect(database_path)

    monkeypatch.setattr(adapter_module, "_commit", lost_then_unreadable)
    monkeypatch.setattr(adapter_module, "_connect", connect)
    with pytest.raises(EquityLifecycleRecordingUnresolvedError):
        world.record(snapshot)
    monkeypatch.undo()
    assert _rows(world.database) == []


def test_ls12_a_conflicting_history_found_on_reload_is_conflict(
    world: LifecycleWorld, monkeypatch: pytest.MonkeyPatch
) -> None:
    ours = world.opened()
    theirs = world.activated()  # 09:51: OPENED and ACTIVATED, evaluated at 09:51
    competing: list[Any] = []

    def lost_while_another_commits(connection: sqlite3.Connection) -> None:
        connection.execute("ROLLBACK")  # ours never took effect ...
        if not competing:
            monkeypatch.setattr(adapter_module, "_commit", real)
            competing.append(world.record(theirs))  # ... and another history committed
        raise sqlite3.OperationalError("test: COMMIT lost")

    real = adapter_module._commit  # noqa: SLF001
    monkeypatch.setattr(adapter_module, "_commit", lost_while_another_commits)
    result = world.record(ours)
    assert competing[0].outcome is _O.APPENDED
    assert (result.outcome, result.reconciled) == (_O.CONFLICT, True)
    assert [e.evaluated_at for e in result.recorded.lifecycle.events] == [ist(9, 51)] * 2


def test_ls12_corruption_met_on_reload_is_permanent(
    world: LifecycleWorld, monkeypatch: pytest.MonkeyPatch
) -> None:
    snapshot = world.opened()
    real = adapter_module._commit  # noqa: SLF001

    def commit_then_corrupt(connection: sqlite3.Connection) -> None:
        real(connection)
        (text,) = _documents(world.database, world.issuance_id)
        _update_document(world, 1, json.dumps(json.loads(text), sort_keys=True, indent=1))
        raise sqlite3.OperationalError("test: COMMIT acknowledgment lost")

    monkeypatch.setattr(adapter_module, "_commit", commit_then_corrupt)
    with pytest.raises(_Integrity) as raised:
        world.record(snapshot)
    assert raised.value.category == "EVENT"  # never adopted; whether it committed is not claimed


# ----------------------------------------------------------------------
# LS-13, LS-14, RR-06: retries, stale writers and separate OS processes
# ----------------------------------------------------------------------


def test_ls13_an_identical_retry_is_equivalent_also_from_a_reopened_store(
    opened: LifecycleWorld,
) -> None:
    authorization = opened.capture(opened.activated())
    first = opened.store().append(authorization)
    assert first.outcome is _W.APPENDED
    after = _rows(opened.database)
    for _ in range(2):  # new adapter instances, as after a restart: no state is kept
        again = SQLiteEquityRecommendationLifecycleStore(opened.database).append(authorization)
        assert (again.outcome, again.recorded) == (_W.EQUIVALENT, first.recorded)
    assert _rows(opened.database) == after  # nothing inserted


def test_ls14_a_stale_writer_and_a_same_length_divergent_history_are_conflicts(
    opened: LifecycleWorld,
) -> None:
    opened.bars({7: ENTRY_BAR})
    early, late = opened.snapshot(9, 51), opened.snapshot(9, 52)
    stale = opened.capture(early)  # ACTIVATED evaluated 09:51
    divergent = opened.capture(late)  # ACTIVATED evaluated 09:52, the same length
    assert opened.store().append(divergent).outcome is _W.APPENDED
    written = opened.store().append(stale)
    assert written.outcome is _W.CONFLICT  # same length, other content: never by count
    assert written.recorded == opened.store().load_lifecycle(opened.issuance_id)
    assert [e.evaluated_at for e in _stored_events(opened)][1] == ist(9, 52)

    opened.bars({8: TARGET_BAR})
    assert opened.record(opened.snapshot(9, 56)).state is S.CLOSED_TARGET
    assert opened.store().append(stale).outcome is _W.CONFLICT  # history advanced further
    assert len(_rows(opened.database)) == 3  # the stale events were never adopted


def _race(*workers: tuple[Path, dict[str, Any], str]) -> dict[str, tuple[Any, ...]]:
    """Run each (database, request, name) in a spawned recorder, released together at append."""
    context = multiprocessing.get_context("spawn")  # the Windows start method, everywhere
    start, results = context.Barrier(len(workers)), context.Queue()
    processes = [
        context.Process(
            target=record_in_another_process,
            args=(str(database), json.dumps(request), start, results, name),
        )
        for database, request, name in workers
    ]
    for process in processes:
        process.start()
    reported = {}
    for _ in processes:
        name, pid, report, error = results.get(timeout=240)
        reported[name] = (pid, report, error)
    for process in processes:
        process.join(timeout=60)
        assert process.exitcode == 0
    assert all(error is None for *_, error in reported.values()), reported
    assert len({pid for pid, *_ in reported.values()} | {os.getpid()}) == len(workers) + 1
    return {name: report for name, (_, report, _) in reported.items()}


def test_ls13_rr06_two_processes_appending_identical_events_one_appended_one_equivalent(
    opened: LifecycleWorld,
) -> None:
    request = {"issuance_id": opened.issuance_id, "snapshots": [opened.activated()]}
    reported = _race((opened.database, request, "first"), (opened.database, request, "second"))
    assert sorted(outcome for outcome, _, _ in reported.values()) == ["APPENDED", "EQUIVALENT"]
    assert reported["first"][2] == reported["second"][2]  # the same authorized batch
    assert [r[1] for r in _rows(opened.database)] == [1, 2]  # no duplicate
    assert _documents(opened.database, opened.issuance_id)[1:] == reported["first"][2]
    # A fresh process evaluating the same snapshot afterwards sees it recorded.
    (later,) = _race((opened.database, request, "later")).values()
    assert later[:2] == ("NO_NEW_EVENTS", "ACTIVE")
    check_database(opened.database)


def test_ls14_rr06_two_processes_appending_conflicting_events_the_first_commit_wins(
    opened: LifecycleWorld,
) -> None:
    activated = opened.activated()  # 09:51: ACTIVATED
    opened.bars({8: TARGET_BAR})
    targeted = opened.snapshot(9, 56)  # 09:56: ACTIVATED and TARGET_REACHED
    reported = _race(
        (opened.database, {"issuance_id": opened.issuance_id, "snapshots": [activated]}, "a"),
        (opened.database, {"issuance_id": opened.issuance_id, "snapshots": [targeted]}, "b"),
    )
    outcomes = {name: report[0] for name, report in reported.items()}
    assert sorted(outcomes.values()) == ["APPENDED", "CONFLICT"]
    winner = next(name for name, outcome in outcomes.items() if outcome == "APPENDED")
    stored = _documents(opened.database, opened.issuance_id)
    assert stored[1:] == reported[winner][2]  # only the first committed batch persists
    assert (
        opened.store().load_lifecycle(opened.issuance_id).lifecycle.state.value
        == (  # type: ignore[union-attr]
            reported[winner][1]
        )
    )
    check_database(opened.database)


def test_ls07_two_processes_appending_for_separate_issuances_stay_independent(
    world: LifecycleWorld,
) -> None:
    establish_infy_breakout(world.world)
    other = world.world.issue(world.world.proposed(**infy()))
    infy_id = other.issuance.issuance_id
    reported = _race(
        (world.database, {"issuance_id": world.issuance_id, "snapshots": [world.opened()]}, "tcs"),
        (
            world.database,
            {"issuance_id": infy_id, "snapshots": [world.snapshot(9, 46, 30, listing=INFY)]},
            "infy",
        ),
    )
    assert [report[0] for report in reported.values()] == ["APPENDED", "APPENDED"]
    assert sorted((r[0], r[1]) for r in _rows(world.database)) == sorted(
        [(world.issuance_id, 1), (infy_id, 1)]
    )
    assert _documents(world.database, world.issuance_id) == reported["tcs"][2]
    assert _documents(world.database, infy_id) == reported["infy"][2]


def test_a_write_lock_not_obtained_is_typed_unavailable_and_reads_go_on(
    opened: LifecycleWorld, monkeypatch: pytest.MonkeyPatch
) -> None:
    activated = opened.activated()
    authorization = opened.capture(activated)
    before = _rows(opened.database)
    monkeypatch.setattr(adapter_module, "_BUSY_TIMEOUT_SECONDS", 0.05)
    holder = raw(opened.database)
    try:
        holder.execute("BEGIN IMMEDIATE")  # another writer holds the lock
        with pytest.raises(EquityRecommendationLifecycleStoreUnavailableError):
            opened.store().append(authorization)
        with pytest.raises(EquityLifecycleRecordingRefusedError) as refused:
            opened.record(activated)
        assert (refused.value.reason, refused.value.retryable) == (_R.STORE_UNAVAILABLE, True)
        assert opened.store().load_lifecycle(opened.issuance_id) is not None  # reads go on
    finally:
        holder.execute("ROLLBACK")
        holder.close()
    assert _rows(opened.database) == before
    assert opened.store().append(authorization).outcome is _W.APPENDED


# ----------------------------------------------------------------------
# LS-15, RV-04: terminal authority and O-3 option (b)
# ----------------------------------------------------------------------


def test_ls15_a_a_state_change_after_a_terminal_state_is_refused_before_and_by_the_database(
    closed: LifecycleWorld,
) -> None:
    before = _rows(closed.database)
    stored = _stored_events(closed)
    assert stored[-1].to_state is S.CLOSED_TARGET
    later = closed.record(closed.snapshot(9, 58))
    assert (later.outcome, later.state) == (_O.NO_NEW_EVENTS, S.CLOSED_TARGET)
    again = dataclasses.replace(stored[-1], sequence=4, evaluated_at=ist(9, 58))
    with pytest.raises(UnverifiedEquityRecommendationLifecycleAppendError, match="terminal"):
        loaded = closed.store().load_lifecycle(closed.issuance_id)
        assert loaded is not None
        VerifiedEquityRecommendationLifecycleAppend(
            loaded.issuance, loaded.lifecycle.window, tuple(stored), (again,)
        )
    with _refused("after a terminal state"):  # the raw path: the trigger
        _admitted(closed.database, _insert(_row(closed.issuance_id, again)))
    assert _rows(closed.database) == before


def test_ls15_b_a_core_permitted_revision_after_a_terminal_state_is_recorded_durably(
    closed: LifecycleWorld,
) -> None:
    source, end = events(), bar_end(8)
    closed.evidence().ingestion.execute(
        [
            source.observation(
                end, revision="r2", corrects="r1", prices=("253.4", "263.5", "253.1", "262.9")
            ),
            source.finality(end, revision="r2"),
        ]
    )
    snapshot = closed.snapshot(9, 57)
    result = closed.record(snapshot)
    assert (result.outcome, result.state) == (_O.APPENDED, S.CLOSED_TARGET)
    (revision,) = result.new_events
    assert (revision.kind, revision.sequence, revision.cited_sequence) == (
        K.SOURCE_REVISION_RECORDED,
        4,
        3,
    )
    assert revision.materiality is EquityRevisionMateriality.UNASSESSED
    reloaded = SQLiteEquityRecommendationLifecycleStore(closed.database).load_lifecycle(
        closed.issuance_id
    )
    assert reloaded is not None and reloaded.lifecycle.state is S.CLOSED_TARGET
    assert reloaded == result.recorded
    assert closed.record(snapshot).outcome is _O.NO_NEW_EVENTS  # recorded once
    assert [r[1] for r in _rows(closed.database)] == [1, 2, 3, 4]


def test_ls15_c_rv04_late_gap_filling_evidence_after_incomplete_evidence_records_nothing(
    world: LifecycleWorld,
) -> None:
    incomplete = world.record(world.snapshot(10, 51))  # bar 7 onwards never arrived
    assert [e.kind for e in incomplete.new_events] == [
        K.ENTRY_WINDOW_OPENED,
        K.EVIDENCE_INCOMPLETE,
    ]
    assert incomplete.state is S.INCOMPLETE_EVIDENCE
    before, objects = _rows(world.database), schema_objects(world.database)
    assert before[0][7] == before[1][7]  # equal consecutive instants are stored as they are
    world.bars({index: QUIET_BAR for index in range(7, 20)})  # the gap, filled late
    late = world.record(world.snapshot(10, 52))
    assert (late.outcome, late.new_events, late.state) == (
        _O.NO_NEW_EVENTS,
        (),
        S.INCOMPLETE_EVIDENCE,
    )
    assert _rows(world.database) == before  # byte-identical: nothing appended or replaced
    assert schema_objects(world.database) == objects  # no cursor, no reconciliation kind
    assert {r[3] for r in before} <= {kind.value for kind in K}

    # Core's refusal of a revision citing EVIDENCE_INCOMPLETE holds on reload: TEST-ONLY plant.
    cited = decode_equity_lifecycle_event_document(before[1][-1])
    forged = EquityLifecycleEvent(
        sequence=3,
        kind=K.SOURCE_REVISION_RECORDED,
        from_state=S.INCOMPLETE_EVIDENCE,
        to_state=S.INCOMPLETE_EVIDENCE,
        evaluated_at=ist(10, 52),
        evidence=EquityLifecycleEvidence(
            "forged-gap-fill", cited.evidence.opens_at, cited.evidence.closes_at
        ),
        cited_sequence=2,
        superseded_evidence=cited.evidence,
        materiality=EquityRevisionMateriality.UNASSESSED,
    )
    _raw_transaction(world.database, _insert(_row(world.issuance_id, forged)))  # SQL admits it
    with pytest.raises(_Integrity) as raised:
        world.store().load_lifecycle(world.issuance_id)
    assert raised.value.category == "HISTORY"


def test_ls15_terminal_states_reached_on_sqlite_are_final_after_restart(
    world: LifecycleWorld,
) -> None:
    world.bars({7: CANCEL_BAR})
    result = world.record(world.snapshot(9, 51))
    assert [e.kind for e in result.new_events] == [K.ENTRY_WINDOW_OPENED, K.ENTRY_CANCELLED]
    assert result.state is S.CANCELLED_BEFORE_ENTRY
    before = _rows(world.database)
    world.bars({8: ENTRY_BAR})  # a later touch of the zone changes nothing
    later = world.record(world.snapshot(9, 56))
    assert (later.outcome, later.state) == (_O.NO_NEW_EVENTS, S.CANCELLED_BEFORE_ENTRY)
    reloaded = store(world.database).load_lifecycle(world.issuance_id)
    assert reloaded is not None and reloaded.lifecycle.is_terminal
    assert _rows(world.database) == before


# ----------------------------------------------------------------------
# LS-16, RR-01, RR-04: restart and a fresh operating-system process
# ----------------------------------------------------------------------

_FRESH_PROCESS = r"""
import json, os, sys
from pathlib import Path
from northstar_application.application_services import equity_lifecycle_event_documents
from northstar_infrastructure.persistence import SQLiteEquityRecommendationLifecycleStore

recorded = SQLiteEquityRecommendationLifecycleStore(Path(sys.argv[1])).load_lifecycle(sys.argv[2])
lifecycle = recorded.lifecycle
print(json.dumps({
    "pid": os.getpid(),
    "issuance_id": recorded.issuance_id,
    "recording_sequence": recorded.issuance.recording_sequence,
    "issuance": recorded.issuance.recommendation.content(),
    "state": lifecycle.state.value,
    "terminal": lifecycle.is_terminal,
    "sequences": [event.sequence for event in lifecycle.events],
    "references": [event.evidence.reference for event in lifecycle.events],
    "documents": list(equity_lifecycle_event_documents(lifecycle.events)),
    "window_ends": lifecycle.window.entry_window_ends_at.value,
}))
"""


def _fresh_process(database: Path, issuance_id: str) -> dict[str, Any]:
    completed = subprocess.run(  # the test's own interpreter, running fixed code only
        [sys.executable, "-c", _FRESH_PROCESS, str(database), issuance_id],
        cwd=_INFRA_ROOT,
        env=dict(os.environ, PYTHONHASHSEED="4242"),
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    return json.loads(completed.stdout)


def test_ls16_rr01_rr04_lifecycles_replay_identically_after_restart_and_in_a_fresh_process(
    closed: LifecycleWorld,
) -> None:
    original = closed.store().load_lifecycle(closed.issuance_id)
    assert original is not None
    before = _file_state(closed.database)
    # RR-01: new adapter instances, as after a restart.
    for _ in range(2):
        reloaded = SQLiteEquityRecommendationLifecycleStore(closed.database).load_lifecycle(
            closed.issuance_id
        )
        assert reloaded == original and reloaded is not original
    # RR-04 (state): a separate OS process, from the file alone.
    fresh = _fresh_process(closed.database, closed.issuance_id)
    assert fresh["pid"] != os.getpid()
    assert fresh == {
        "pid": fresh["pid"],
        "issuance_id": closed.issuance_id,
        "recording_sequence": original.issuance.recording_sequence,
        "issuance": original.issuance.recommendation.content(),
        "state": "CLOSED_TARGET",
        "terminal": True,
        "sequences": [1, 2, 3],
        "references": [e.evidence.reference for e in original.lifecycle.events],
        "documents": list(equity_lifecycle_event_documents(original.lifecycle.events)),
        "window_ends": "2026-08-19T05:20:00Z",
    }
    assert _file_state(closed.database) == before  # loads wrote, repaired and migrated nothing


# ----------------------------------------------------------------------
# LS-17: the authoritative window
# ----------------------------------------------------------------------


def test_ls17_a_b_the_stored_window_derives_from_the_issued_record_and_equals_ei5c(
    opened: LifecycleWorld,
) -> None:
    loaded = opened.store().load_lifecycle(opened.issuance_id)
    assert loaded is not None
    disposition = opened.world.pipeline.store().load_disposition(opened.issuance.generation_key)
    assert disposition is not None and disposition.proposal is not None
    expected = equity_issued_lifecycle_window(opened.issuance, disposition)
    assert loaded.lifecycle.window == expected
    assert loaded.lifecycle.window == equity_lifecycle_window(  # EI-5C's shared derivation
        disposition.proposal, disposition.candidate
    )
    assert expected.entry_window_ends_at == PointInTime("2026-08-19T05:20:00Z")
    assert opened.capture(opened.activated()).window == expected


def test_ls17_c_a_window_that_cannot_be_derived_is_permanent_window_corruption(
    opened: LifecycleWorld, monkeypatch: pytest.MonkeyPatch
) -> None:
    """TEST SEAM (disclosed): the governed derivation is made to refuse inside the adapter.

    A real stored disposition that passes EI-4D level-A decoding and EI-6A
    re-derivation always yields a window, so no planted row reaches
    ``WINDOW``; the seam proves the mapping and that nothing is repaired.
    """
    authorization = opened.capture(opened.activated())

    def refuse(*_args: object) -> None:
        raise InvalidEquityLifecycleWindowProvenanceError("test: provenance refused")

    monkeypatch.setattr(adapter_module, "equity_issued_lifecycle_window", refuse)
    _expect_corrupt(opened, "WINDOW", authorization)


def test_ls17_d_e_the_adapter_takes_no_window_input_and_computes_no_window() -> None:
    adapter = SQLiteEquityRecommendationLifecycleStore
    assert list(inspect.signature(adapter.load_lifecycle).parameters) == ["self", "issuance_id"]
    assert list(inspect.signature(adapter.append).parameters) == ["self", "authorization"]
    tree = ast.parse(inspect.getsource(adapter_module))
    calls = {
        getattr(node.func, "id", getattr(node.func, "attr", None))
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
    }
    attributes = {node.attr for node in ast.walk(tree) if isinstance(node, ast.Attribute)}
    assert "equity_issued_lifecycle_window" in calls
    assert "EquityLifecycleWindow" not in calls
    assert not attributes & {
        "entry_bar_openings",
        "bar_ends",
        "research_window_closes_at",
        "entry_window_ends_at",
        "latest_entry_bar_opens_at",
    }


# ----------------------------------------------------------------------
# Synthetic end to end, on real SQLite
# ----------------------------------------------------------------------


def test_e2e_the_real_sqlite_vertical_slice(world: LifecycleWorld) -> None:
    database = world.database
    objects_before = set(schema_objects(database))
    # EI-6B: an empty RECORDED lifecycle of the EI-6A issuance of the EI-4D disposition.
    empty = world.store().load_lifecycle(world.issuance_id)
    assert empty is not None and empty.lifecycle.state is S.RECORDED
    assert empty.issuance.recommendation.generation_key == world.issuance.generation_key
    assert world.record(world.opened()).state is S.WAITING_FOR_ENTRY
    activated = world.activated()
    identical = world.capture(activated)
    stale = world.capture(world.snapshot(9, 52))
    assert world.record(activated).outcome is _O.APPENDED
    assert world.store().append(identical).outcome is _W.EQUIVALENT
    assert world.store().append(stale).outcome is _W.CONFLICT
    reopened = SQLiteEquityRecommendationLifecycleStore(database).load_lifecycle(world.issuance_id)
    assert reopened is not None and reopened.lifecycle.state is S.ACTIVE
    assert world.record(world.targeted()).state is S.CLOSED_TARGET
    assert world.record(world.snapshot(10, 0)).outcome is _O.NO_NEW_EVENTS  # terminal
    fresh = _fresh_process(database, world.issuance_id)
    assert (fresh["state"], fresh["sequences"]) == ("CLOSED_TARGET", [1, 2, 3])
    # No other authority was written, and nothing is an order, a fill or a position.
    assert set(schema_objects(database)) - objects_before == _EI6B_OBJECTS
    assert len(rows(database, ISSUANCES)) == 1 and len(rows(database, DISPOSITIONS)) == 1
    for name in _EI6B_OBJECTS:
        assert not any(w in name for w in ("order", "fill", "position", "cursor", "monitor"))
    for document in _documents(database, world.issuance_id):
        assert set(json.loads(document)) == {
            "format",
            "sequence",
            "kind",
            "from_state",
            "to_state",
            "evaluated_at",
            "evidence",
            "touched",
            "ordering",
            "reason",
            "deficiencies",
            "cited_sequence",
            "superseded_evidence",
            "materiality",
        }
    check_database(database)


# ----------------------------------------------------------------------
# AR: the adapter's surface and the authorized-write boundary
# ----------------------------------------------------------------------


def test_ar_the_adapter_exposes_exactly_the_port_operations() -> None:
    adapter = SQLiteEquityRecommendationLifecycleStore
    assert issubclass(adapter, EquityRecommendationLifecycleStore)
    public = {name for name in vars(adapter) if not name.startswith("_")}
    assert public == {"load_lifecycle", "append"}
    assert list(inspect.signature(adapter).parameters) == ["database_path"]
    assert "SQLiteEquityRecommendationLifecycleStore" in persistence.__all__
    assert "initialize_equity_recommendation_lifecycle_schema" in persistence.__all__


def _function(tree: ast.Module, cls: str, name: str) -> ast.FunctionDef:
    (adapter,) = [n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == cls]
    (method,) = [n for n in adapter.body if isinstance(n, ast.FunctionDef) and n.name == name]
    return method


def test_ar_append_validates_first_and_is_the_only_insert() -> None:
    tree = ast.parse(inspect.getsource(adapter_module))
    method = _function(tree, "SQLiteEquityRecommendationLifecycleStore", "append")
    body = [
        s
        for s in method.body
        if not (isinstance(s, ast.Expr) and isinstance(s.value, ast.Constant))
    ]
    assert "require_verified_equity_recommendation_lifecycle_append" in ast.unparse(body[0])
    users = {
        function.name
        for function in ast.walk(tree)
        if isinstance(function, ast.FunctionDef)
        for node in ast.walk(function)
        if isinstance(node, ast.Name) and node.id == "_INSERT_EVENT"
    }
    assert users == {"append", "work"}  # the insert is inside append's transaction only
    # No public function accepts raw events, dictionaries or documents to write.
    public = {
        n.name for n in tree.body if isinstance(n, ast.FunctionDef) and not n.name.startswith("_")
    }
    assert public == {"lifecycle_event_row"}  # a pure column mapping; it writes nothing


def test_ar_the_lifecycle_modules_reuse_the_application_and_reach_nothing_else() -> None:
    root = Path(northstar_infrastructure.__file__).parent
    offenders = []
    for path in sorted(root.rglob("*.py")):
        relative = path.relative_to(root).as_posix()
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and getattr(node.func, "id", None) == (
                "SQLiteEquityRecommendationLifecycleStore"
            ):
                offenders.append(f"{relative}: constructs the adapter (no composition root)")
        if relative.startswith("persistence/sqlite_equity_recommendation_lifecycle"):
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom | ast.Import):
                    modules = (
                        [node.module or ""]
                        if isinstance(node, ast.ImportFrom)
                        else [a.name for a in node.names]
                    )
                    for module in modules:
                        if any(
                            w in module
                            for w in (
                                "broker",
                                "execution",
                                "paper",
                                "http",
                                "socket",
                                "random",
                                "time",
                                "monitor",
                            )
                        ):
                            offenders.append(f"{relative}: imports {module}")
                if isinstance(node, ast.Attribute) and node.attr in {"now", "utcnow", "today"}:
                    offenders.append(f"{relative}: reads a clock")
    assert offenders == []
    source = inspect.getsource(adapter_module)
    for name in (
        "require_verified_equity_recommendation_lifecycle_append",
        "decode_equity_lifecycle_event_document",
        "equity_recorded_lifecycle",
        "equity_lifecycle_append_decision",
        "equity_issued_lifecycle_window",
    ):
        assert f"    {name},\n" in source  # imported from the Application, never redefined
        assert f"def {name}" not in source
    # No second state machine: the adapter names no lifecycle state or transition itself.
    assert "EquityLifecycleState" not in source and "EquityLifecycleEventKind" not in source
    assert schema_module.LIFECYCLE_EVENTS == _T
