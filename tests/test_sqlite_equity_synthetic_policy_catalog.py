"""EI4-D10-S2: the immutable SQLite synthetic policy catalog (ADR-022).

Every test runs on a disposable SQLite file holding the EI-2B and EI-3C schema,
through the real SQLiteEquitySyntheticPolicyCatalogStore and, where stated,
the real S1 Application use cases. Concurrency uses independent connections in
threads coordinated by threading.Barrier and Event, and separate spawned OS
processes coordinated by a multiprocessing Barrier -- never sleeps.

Faults are injected only through these labelled test-only means:

- the adapter's module-level ``_connect``, ``_insert_publication``,
  ``_insert_definition``, ``_insert_set`` and ``_commit`` seams and
  ``_BUSY_TIMEOUT_SECONDS``, replaced with monkeypatch for one test;
- ``bypass()``, a raw connection that drops a trigger or relaxes CHECK or
  foreign-key enforcement on its own disposable database.

Nothing here touches a network, deployment or production database.
"""

from __future__ import annotations

import ast
import dataclasses
import hashlib
import inspect
import json
import multiprocessing
import os
import re
import socket
import sqlite3
import threading
from collections.abc import Callable
from contextlib import closing
from pathlib import Path
from typing import Any

import pytest
from northstar_application.application_services import (
    EQUITY_SYNTHETIC_POLICY_COMPONENT_TYPES,
    EQUITY_SYNTHETIC_POLICY_NAMESPACE,
    AuditEquityRecommendationPolicyBindingsUseCase,
    CatalogEquitySyntheticRecommendationReferences,
    EquityRecommendationPolicyBindingStatus,
    EquitySyntheticPolicyBindingIdentity,
    EquitySyntheticPolicyBundle,
    EquitySyntheticPolicyPublicationOutcome,
    EquitySyntheticPolicyType,
    IncompatibleEquitySyntheticPolicyBindingError,
    InvalidEquitySyntheticPolicyBindingError,
    PublishEquitySyntheticPoliciesUseCase,
    canonical_equity_recommendation_json,
    equity_recommendation_disposition,
    equity_recommendation_fingerprint,
)
from northstar_application.ports import (
    EquitySyntheticPolicyCatalogCommitUnconfirmedError,
    EquitySyntheticPolicyCatalogNotCommittedError,
    EquitySyntheticPolicyCatalogOutcome,
    EquitySyntheticPolicyCatalogStore,
    EquitySyntheticPolicyCatalogUnavailableError,
    EquitySyntheticPolicyPublicationHistory,
    EquitySyntheticPolicyReferenceConflictError,
)

from northstar_infrastructure import persistence
from northstar_infrastructure.persistence import SQLiteEquitySyntheticPolicyCatalogStore
from northstar_infrastructure.persistence import (
    sqlite_equity_synthetic_policy_catalog as adapter_module,
)
from northstar_infrastructure.persistence import (
    sqlite_equity_synthetic_policy_catalog_schema as schema_module,
)
from tests.equity_recommendation_acceptance_fixtures import (
    DISPOSITIONS,
    ENTRIES,
    SNAPSHOTS,
    TIMELINES,
    Pipeline,
    bypass,
    check_database,
    establish_research,
    raw,
    rows,
    schema_objects,
)
from tests.equity_synthetic_policy_catalog_fixtures import (
    CATALOG_TABLES,
    DEFINITIONS,
    POLICY_SETS,
    PUBLICATIONS,
    bundle,
    evidence_database,
    publish_in_another_process,
)

_W = EquitySyntheticPolicyCatalogOutcome
_P = EquitySyntheticPolicyPublicationOutcome
_T = EquitySyntheticPolicyType
_NS = EQUITY_SYNTHETIC_POLICY_NAMESPACE
_Commit = Callable[[sqlite3.Connection], None]


@pytest.fixture(autouse=True)
def _refuse_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("EI4-D10-S2 tests must not open network connections.")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)


@pytest.fixture
def database(tmp_path: Path) -> Path:
    return evidence_database(tmp_path / "evidence.sqlite3")


def _store(database: Path) -> SQLiteEquitySyntheticPolicyCatalogStore:
    return SQLiteEquitySyntheticPolicyCatalogStore(database)


def _catalog(database: Path) -> dict[str, list[dict[str, Any]]]:
    return {table: rows(database, table) for table in CATALOG_TABLES}


def _counts(database: Path) -> tuple[int, int, int]:
    """(publications, definitions, sets)."""
    catalog = _catalog(database)
    return tuple(len(catalog[t]) for t in (PUBLICATIONS, DEFINITIONS, POLICY_SETS))  # type: ignore[return-value]


def _publisher(database: Path, store: Any = None) -> PublishEquitySyntheticPoliciesUseCase:
    return PublishEquitySyntheticPoliciesUseCase(store or _store(database))


def _relabelled() -> EquitySyntheticPolicyBundle:
    """The same set and timing labels, other timing content (the EI-4E AA-1 case)."""
    return bundle(latency_seconds=240)


# ----------------------------------------------------------------------
# Schema: creation, additivity, uniqueness, foreign keys, insert-only
# ----------------------------------------------------------------------


def test_a_fresh_evidence_database_acquires_the_catalog_schema(database: Path) -> None:
    before = schema_objects(database)
    assert _store(database).load_definition(bundle().components[0].identity) is None
    added = {n: kind for n, (kind, _) in schema_objects(database).items() if n not in before}
    assert {n for n, kind in added.items() if kind == "table"} == set(CATALOG_TABLES)
    assert sum(kind == "trigger" for kind in added.values()) == 12
    assert {n: v for n, v in schema_objects(database).items() if n in before} == before
    check_database(database)


def test_the_store_never_creates_a_database_or_a_second_authority(tmp_path: Path) -> None:
    absent = tmp_path / "absent.sqlite3"
    identity = bundle().components[0].identity
    with pytest.raises(EquitySyntheticPolicyCatalogUnavailableError):
        _store(absent).load_definition(identity)
    assert not absent.exists()
    empty = tmp_path / "empty.sqlite3"
    sqlite3.connect(empty).close()
    for operation in (
        lambda: _store(empty).load_definition(identity),
        lambda: _store(empty).publish(bundle()),
    ):
        with pytest.raises(EquitySyntheticPolicyCatalogUnavailableError, match="no EI-2B"):
            operation()
    assert schema_objects(empty) == {}  # no catalog tables beside missing evidence


def test_initialization_is_idempotent_and_never_rebuilds(database: Path) -> None:
    assert _publisher(database).execute(bundle()).is_published
    objects, catalog = schema_objects(database), _catalog(database)
    for _ in range(3):
        _store(database).load_policy_set(bundle().policy_set.identity)
        with closing(raw(database)) as connection:
            schema_module.initialize_equity_synthetic_policy_catalog_schema(connection)
    assert schema_objects(database) == objects and _catalog(database) == catalog


def test_the_set_columns_follow_the_approved_component_types() -> None:
    assert tuple(schema_module.SET_COMPONENT_COLUMNS) == tuple(
        t.value for t in EQUITY_SYNTHETIC_POLICY_COMPONENT_TYPES
    )


def _insert(
    database: Path, table: str, columns: tuple[str, ...], values: tuple[object, ...]
) -> None:
    with closing(raw(database)) as connection:
        connection.execute(
            f"INSERT INTO {table} ({', '.join(columns)}) "  # noqa: S608
            f"VALUES ({', '.join('?' * len(columns))})",
            values,
        )


def test_the_database_refuses_duplicate_identities(database: Path) -> None:
    published = bundle()
    assert _publisher(database).execute(published).is_published
    component = published.component(_T.TIMING_POLICY)
    other = bundle(latency_seconds=240).component(_T.TIMING_POLICY)  # same identity
    for values in (
        adapter_module.definition_row(component, 1),
        adapter_module.definition_row(other, 1),
    ):
        with pytest.raises(sqlite3.IntegrityError, match="insert-only"):
            _insert(database, DEFINITIONS, adapter_module.DEFINITION_COLUMNS, values)
    bypass(database, drop=(f"{DEFINITIONS}_no_replace",))  # test-only
    with pytest.raises(sqlite3.IntegrityError, match="UNIQUE"):  # the key itself
        _insert(
            database,
            DEFINITIONS,
            adapter_module.DEFINITION_COLUMNS,
            adapter_module.definition_row(other, 1),
        )
    assert _counts(database) == (1, 5, 1)


def test_a_set_needs_its_publication_and_its_five_component_bindings(database: Path) -> None:
    offered = bundle()
    _store(database).load_definition(offered.components[0].identity)  # creates the schema
    publication = (
        1,
        _NS,
        offered.policy_set.version.value,
        offered.policy_set.policy_set_fingerprint,
    )
    with pytest.raises(sqlite3.IntegrityError, match="newest publication"):  # no publication
        _insert(
            database,
            POLICY_SETS,
            adapter_module.SET_COLUMNS,
            adapter_module.set_row(offered.policy_set, 1),
        )
    _insert(database, PUBLICATIONS, adapter_module.PUBLICATION_COLUMNS, publication)
    with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY"):  # no components
        _insert(
            database,
            POLICY_SETS,
            adapter_module.SET_COLUMNS,
            adapter_module.set_row(offered.policy_set, 1),
        )
    with pytest.raises(sqlite3.IntegrityError, match="next publication sequence"):
        _insert(database, PUBLICATIONS, adapter_module.PUBLICATION_COLUMNS, (3, *publication[1:]))
    assert _counts(database) == (1, 0, 0)


@pytest.mark.parametrize(
    ("column", "value", "match"),
    [
        ("namespace", "northstar.other", "CHECK"),
        ("policy_type", "POLICY_SET", "CHECK"),
        ("version_label", "synthetic-ei4e-timing/1 ", "CHECK"),
        ("version_label", "ei4e-timing/1", "CHECK"),
        ("definition_fingerprint", "eipd1-sha256:" + "Z" * 64, "CHECK"),
        ("definition_format", "other", "CHECK"),
    ],
)
def test_the_database_checks_every_binding_column(
    database: Path, column: str, value: str, match: str
) -> None:
    offered = bundle()
    _store(database).load_definition(offered.components[0].identity)
    _insert(
        database,
        PUBLICATIONS,
        adapter_module.PUBLICATION_COLUMNS,
        (1, _NS, offered.policy_set.version.value, offered.policy_set.policy_set_fingerprint),
    )
    row = list(adapter_module.definition_row(offered.component(_T.TIMING_POLICY), 1))
    row[adapter_module.DEFINITION_COLUMNS.index(column)] = value
    with pytest.raises(sqlite3.IntegrityError, match=match):
        _insert(database, DEFINITIONS, adapter_module.DEFINITION_COLUMNS, tuple(row))
    assert _counts(database) == (1, 0, 0)


_IMMUTABLE = [
    f"UPDATE {DEFINITIONS} SET definition_document = definition_document",
    f"UPDATE {DEFINITIONS} SET version_label = version_label",
    f"DELETE FROM {DEFINITIONS}",
    f"INSERT OR REPLACE INTO {DEFINITIONS} SELECT * FROM {DEFINITIONS}",
    f"REPLACE INTO {DEFINITIONS} SELECT * FROM {DEFINITIONS}",
    f"UPDATE {POLICY_SETS} SET policy_set_fingerprint = policy_set_fingerprint",
    f"DELETE FROM {POLICY_SETS}",
    f"INSERT OR REPLACE INTO {POLICY_SETS} SELECT * FROM {POLICY_SETS}",
    f"UPDATE {PUBLICATIONS} SET publication_sequence = publication_sequence",
    f"DELETE FROM {PUBLICATIONS}",
    f"INSERT OR REPLACE INTO {PUBLICATIONS} SELECT * FROM {PUBLICATIONS}",
]


@pytest.mark.parametrize("statement", _IMMUTABLE)
def test_the_database_refuses_to_change_or_delete_any_binding(
    database: Path, statement: str
) -> None:
    assert _publisher(database).execute(bundle()).is_published
    before = _catalog(database)
    # A replacing insert into the publications may meet the next-sequence trigger first;
    # either refusal leaves every row unchanged.
    with (
        closing(raw(database)) as connection,
        pytest.raises(sqlite3.IntegrityError, match="insert-only|next publication sequence"),
    ):
        connection.execute(statement)
    assert _catalog(database) == before


# ----------------------------------------------------------------------
# Canonical storage and verified reads
# ----------------------------------------------------------------------


def test_published_bindings_are_stored_canonically_and_reload_exactly(database: Path) -> None:
    offered = bundle()
    assert _store(database).publish(offered).outcome is _W.COMMITTED
    store = _store(database)  # a fresh adapter
    for component in offered.components:
        assert store.load_definition(component.identity) == component
    assert store.load_policy_set(offered.policy_set.identity) == offered.policy_set
    stored = {r["policy_type"]: r for r in rows(database, DEFINITIONS)}
    for component in offered.components:
        row = stored[component.policy_type.value]
        assert row["definition_document"] == component.canonical_json()
        assert row["definition_fingerprint"] == component.fingerprint
        assert row["definition_fingerprint"] == equity_recommendation_fingerprint(
            json.loads(row["definition_document"])
        )  # the Application's fingerprint, re-derived
        assert row["publication_sequence"] == 1
    (set_row,) = rows(database, POLICY_SETS)
    assert set_row["reference_document"] == offered.policy_set.reference_json()
    assert set_row["policy_set_fingerprint"] == offered.resolved.fingerprint  # existing eirs1
    assert rows(database, PUBLICATIONS) == [
        {
            "publication_sequence": 1,
            "namespace": _NS,
            "set_version_label": offered.policy_set.version.value,
            "policy_set_fingerprint": offered.resolved.fingerprint,
        }
    ]
    check_database(database)


def test_missing_bindings_and_wrong_lookups(database: Path) -> None:
    offered = bundle()
    store = _store(database)
    assert store.load_definition(offered.components[0].identity) is None
    assert store.load_policy_set(offered.policy_set.identity) is None
    with pytest.raises(ValueError, match="load_policy_set"):
        store.load_definition(offered.policy_set.identity)
    with pytest.raises(ValueError, match="POLICY_SET"):
        store.load_policy_set(offered.components[0].identity)
    with pytest.raises(TypeError):
        store.load_definition("synthetic-ei4e-timing/1")  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        store.publish(offered.resolved)  # type: ignore[arg-type]
    assert _counts(database) == (0, 0, 0)  # reads never publish


def _set_document(
    database: Path, policy_type: str, document: dict[str, Any], fingerprint: str | None = None
) -> None:
    """Test-only: replace a stored component's document (and fingerprint), checks relaxed."""
    text = canonical_equity_recommendation_json(document)
    statements = [
        (
            f"UPDATE {DEFINITIONS} SET definition_document = ? WHERE policy_type = ?",  # noqa: S608
            (text, policy_type),
        )
    ]
    if fingerprint is not None:
        statements.append(
            (
                f"UPDATE {DEFINITIONS} SET definition_fingerprint = ? WHERE policy_type = ?",  # noqa: S608
                (fingerprint, policy_type),
            )
        )
    bypass(database, *statements, drop=(f"{DEFINITIONS}_no_update",), checks=False)


def _restamped(document: dict[str, Any]) -> str:
    """The fingerprint that a tampered document would need to pass the hash check."""
    return (
        "eipd1-sha256:"
        + hashlib.sha256(canonical_equity_recommendation_json(document).encode()).hexdigest()
    )


def _corruptions() -> dict[
    str, tuple[Callable[[dict[str, Any]], tuple[dict[str, Any], str | None]], type[Exception]]
]:
    def keep(d: dict[str, Any]) -> dict[str, Any]:
        return json.loads(json.dumps(d))

    def fingerprint(d: dict[str, Any]) -> tuple[dict[str, Any], str | None]:
        return keep(d), "eipd1-sha256:" + "0" * 64

    def unsupported(d: dict[str, Any]) -> tuple[dict[str, Any], str | None]:
        return keep(d) | {"format": "northstar.equity-synthetic-policy-definition/2"}, None

    def decimal(d: dict[str, Any]) -> tuple[dict[str, Any], str | None]:
        changed = keep(d)
        changed["definition"]["latest_signal_bar_end"] = "10:30:00.000000"
        return changed, _restamped(changed)

    def namespace(d: dict[str, Any]) -> tuple[dict[str, Any], str | None]:
        changed = keep(d) | {"namespace": "northstar.other"}
        return changed, _restamped(changed)

    def extra(d: dict[str, Any]) -> tuple[dict[str, Any], str | None]:
        changed = keep(d) | {"note": "x"}
        return changed, _restamped(changed)

    def missing(d: dict[str, Any]) -> tuple[dict[str, Any], str | None]:
        changed = {k: v for k, v in keep(d).items() if k != "policy_type"}
        return changed, _restamped(changed)

    invalid, incompatible = (
        InvalidEquitySyntheticPolicyBindingError,
        IncompatibleEquitySyntheticPolicyBindingError,
    )
    return {
        "fingerprint mismatch": (fingerprint, invalid),
        "unsupported format": (unsupported, incompatible),
        "non-canonical value": (decimal, invalid),
        "wrong namespace": (namespace, invalid),
        "extra field": (extra, invalid),
        "missing field": (missing, invalid),
    }


@pytest.mark.parametrize("case", list(_corruptions()))
def test_a_corrupted_stored_binding_fails_closed_and_is_never_repaired(
    database: Path, case: str
) -> None:
    offered = bundle()
    assert _publisher(database).execute(offered).is_published
    timing = offered.component(_T.TIMING_POLICY)
    corrupt, error = _corruptions()[case]
    document, fingerprint = corrupt(timing.document())
    _set_document(database, "TIMING_POLICY", document, fingerprint)
    corrupted = _catalog(database)
    with pytest.raises(error) as raised:
        _store(database).load_definition(timing.identity)
    if error is InvalidEquitySyntheticPolicyBindingError:
        assert not isinstance(raised.value, IncompatibleEquitySyntheticPolicyBindingError)
    with pytest.raises(error):  # the set is verified through its components
        _store(database).load_policy_set(offered.policy_set.identity)
    with pytest.raises(error):  # a publication meeting it is refused, never a conflict
        _store(database).publish(offered)
    result = _publisher(database).execute(offered)
    expected = (
        _P.STORED_BINDING_INCOMPATIBLE
        if error is IncompatibleEquitySyntheticPolicyBindingError
        else _P.STORED_BINDING_INVALID
    )
    assert (result.outcome, result.writes) == (expected, 1)
    with pytest.raises(EquitySyntheticPolicyReferenceConflictError):
        CatalogEquitySyntheticRecommendationReferences(_store(database)).timing_policy(
            timing.version
        )
    assert _catalog(database) == corrupted  # nothing repaired, nothing written


def test_an_inconsistent_stored_set_is_invalid_evidence(database: Path) -> None:
    offered = bundle()
    assert _publisher(database).execute(offered).is_published
    other = bundle(latency_seconds=240).resolved.fingerprint
    bypass(  # test-only: the set's eirs1 no longer matches its components
        database,
        (f"UPDATE {POLICY_SETS} SET policy_set_fingerprint = ?", (other,)),  # noqa: S608
        drop=(f"{POLICY_SETS}_no_update",),
    )
    with pytest.raises(InvalidEquitySyntheticPolicyBindingError, match="eirs1"):
        _store(database).load_policy_set(offered.policy_set.identity)
    bypass(  # test-only: a component the set names disappears (foreign keys off)
        database,
        (f"DELETE FROM {DEFINITIONS} WHERE policy_type = 'RISK_POLICY'", ()),  # noqa: S608
        drop=(f"{DEFINITIONS}_no_delete",),
        foreign_keys=False,
    )
    with pytest.raises(InvalidEquitySyntheticPolicyBindingError, match="missing RISK_POLICY"):
        _store(database).load_policy_set(offered.policy_set.identity)
    result = _publisher(database).execute(offered)
    assert result.outcome is _P.STORED_BINDING_INVALID  # never a conflict, never repaired


# ----------------------------------------------------------------------
# Atomic publication, idempotence and conflicts
# ----------------------------------------------------------------------


def test_first_publication_commits_and_an_identical_one_is_equivalent(database: Path) -> None:
    offered = bundle()
    first = _store(database).publish(offered)
    assert (first.outcome, first.conflicts) == (_W.COMMITTED, ())
    catalog = _catalog(database)
    again = _store(database).publish(bundle())
    assert (again.outcome, again.conflicts) == (_W.EQUIVALENT, ())
    assert _catalog(database) == catalog and _counts(database) == (1, 5, 1)
    use_case = _publisher(database)
    assert use_case.execute(offered).outcome is _P.EQUIVALENT_PUBLISHED


def test_a_conflicting_component_writes_none_of_the_bundle(database: Path) -> None:
    original = bundle()
    assert _publisher(database).execute(original).is_published
    catalog = _catalog(database)
    written = _store(database).publish(_relabelled())
    assert written.outcome is _W.CONFLICT
    assert set(written.conflicts) == {original.component(_T.TIMING_POLICY), original.policy_set}
    assert _catalog(database) == catalog  # original unchanged; no partial component or set
    result = _publisher(database).execute(_relabelled())
    assert result.outcome is _P.POLICY_REFERENCE_CONFLICT and result.writes == 1
    assert _catalog(database) == catalog


def test_a_conflicting_set_writes_none_of_the_bundle(database: Path) -> None:
    original = bundle()
    assert _publisher(database).execute(original).is_published
    catalog = _catalog(database)
    other = bundle(timing="timing/on-opening")  # same set label, another (new) timing label
    written = _store(database).publish(other)
    assert written.outcome is _W.CONFLICT and written.conflicts == (original.policy_set,)
    assert _catalog(database) == catalog  # the new timing label was not published either


def test_bundles_share_identical_components(database: Path) -> None:
    assert _store(database).publish(bundle()).outcome is _W.COMMITTED
    assert _store(database).publish(bundle("set/2")).outcome is _W.COMMITTED
    assert _counts(database) == (2, 5, 2)
    assert {r["publication_sequence"] for r in rows(database, DEFINITIONS)} == {1}
    assert [r["publication_sequence"] for r in rows(database, POLICY_SETS)] == [1, 2]
    other_timing = bundle("set/3", timing="timing/late")
    assert _store(database).publish(other_timing).outcome is _W.COMMITTED
    assert _counts(database) == (3, 6, 3)  # only the new component is added


def test_one_label_under_two_types_is_two_bindings(database: Path) -> None:
    base = bundle().resolved
    shared = base.generation_policy.version  # the generation policy's label
    risk = dataclasses.replace(base.risk_policy, version=shared)  # synthetic: same label
    reference = dataclasses.replace(base.reference, risk_policy_version=shared)
    offered = EquitySyntheticPolicyBundle.of(
        dataclasses.replace(base, reference=reference, risk_policy=risk)
    )
    assert _store(database).publish(offered).outcome is _W.COMMITTED
    store = _store(database)
    for policy_type in (_T.GENERATION_POLICY, _T.RISK_POLICY):
        identity = EquitySyntheticPolicyBindingIdentity(_NS, policy_type, shared)
        assert store.load_definition(identity) == offered.component(policy_type)
    labels = [r["version_label"] for r in rows(database, DEFINITIONS)]
    assert labels.count(shared.value) == 2  # one label, two independent bindings


def test_reads_and_resolution_never_publish(database: Path) -> None:
    offered = bundle()
    resolver = CatalogEquitySyntheticRecommendationReferences(_store(database))
    assert resolver.timing_policy(offered.component(_T.TIMING_POLICY).version) is None
    assert _counts(database) == (0, 0, 0)
    assert _publisher(database).execute(offered).is_published
    assert resolver.timing_policy(offered.component(_T.TIMING_POLICY).version) == (
        offered.component(_T.TIMING_POLICY).definition
    )
    assert _counts(database) == (1, 5, 1)


# ----------------------------------------------------------------------
# Independent-connection concurrency (threads)
# ----------------------------------------------------------------------


def _together(database: Path, bundles: dict[str, EquitySyntheticPolicyBundle]) -> dict[str, Any]:
    start = threading.Barrier(len(bundles))
    results: dict[str, Any] = {}

    def run(name: str) -> None:
        store = _store(database)  # its own adapter; its own connection per operation
        start.wait(timeout=30)
        try:
            results[name] = store.publish(bundles[name])
        except BaseException as exc:  # noqa: BLE001 -- reported below, never swallowed
            results[name] = exc

    threads = [threading.Thread(target=run, args=(name,)) for name in bundles]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=60)
    assert not any(t.is_alive() for t in threads)
    assert not any(isinstance(r, BaseException) for r in results.values()), results
    check_database(database)
    return results


def test_identical_concurrent_publications_store_one_binding_each(database: Path) -> None:
    _store(database).load_definition(bundle().components[0].identity)  # schema first
    results = _together(database, {"one": bundle(), "two": bundle()})
    assert sorted(r.outcome.value for r in results.values()) == ["COMMITTED", "EQUIVALENT"]
    assert _counts(database) == (1, 5, 1)


def test_conflicting_concurrent_publications_commit_exactly_one(database: Path) -> None:
    _store(database).load_definition(bundle().components[0].identity)
    offered = {"original": bundle(), "relabelled": _relabelled()}
    results = _together(database, offered)
    winner = next(n for n, r in results.items() if r.outcome is _W.COMMITTED)
    loser = next(n for n in offered if n != winner)
    assert results[loser].outcome is _W.CONFLICT
    assert offered[winner].component(_T.TIMING_POLICY) in results[loser].conflicts
    timing = _store(database).load_definition(offered[winner].component(_T.TIMING_POLICY).identity)
    assert timing == offered[winner].component(_T.TIMING_POLICY)
    assert _counts(database) == (1, 5, 1)


def test_concurrent_bundles_sharing_components_both_commit(database: Path) -> None:
    _store(database).load_definition(bundle().components[0].identity)
    results = _together(database, {"set/1": bundle(), "set/2": bundle("set/2")})
    assert {r.outcome for r in results.values()} == {_W.COMMITTED}
    assert _counts(database) == (2, 5, 2)


def test_a_writer_that_cannot_take_the_lock_writes_nothing(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _store(database).load_definition(bundle().components[0].identity)
    monkeypatch.setattr(adapter_module, "_BUSY_TIMEOUT_SECONDS", 0.05)
    holder = raw(database)
    try:
        holder.execute("BEGIN IMMEDIATE")  # another connection holds the write lock
        with pytest.raises(EquitySyntheticPolicyCatalogNotCommittedError, match="locked"):
            _store(database).publish(bundle())
        result = _publisher(database).execute(bundle())
        assert (result.outcome, result.writes) == (_P.NOT_PUBLISHED, 1)  # no retry
    finally:
        holder.rollback()
        holder.close()
    assert _counts(database) == (0, 0, 0)
    assert _store(database).publish(bundle()).outcome is _W.COMMITTED


def test_a_waiting_writer_and_a_reader_see_only_committed_bindings(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    offered = bundle()
    _store(database).load_definition(offered.components[0].identity)
    holding, release = threading.Event(), threading.Event()
    real = adapter_module._insert_set  # noqa: SLF001
    results: dict[str, Any] = {}

    def insert(connection: sqlite3.Connection, values: tuple[object, ...]) -> None:
        real(connection, values)
        if threading.current_thread().name == "holder":
            holding.set()  # inside BEGIN IMMEDIATE, every row inserted, not committed
            assert release.wait(timeout=30)

    monkeypatch.setattr(adapter_module, "_insert_set", insert)

    def write(name: str) -> None:
        results[name] = _store(database).publish(offered)

    holder = threading.Thread(target=write, args=("holder",), name="holder")
    holder.start()
    assert holding.wait(timeout=30)
    assert _store(database).load_policy_set(offered.policy_set.identity) is None  # uncommitted
    waiter = threading.Thread(target=write, args=("waiter",), name="waiter")
    waiter.start()
    release.set()
    holder.join(timeout=60)
    waiter.join(timeout=60)
    assert results["holder"].outcome is _W.COMMITTED
    assert results["waiter"].outcome is _W.EQUIVALENT
    assert _counts(database) == (1, 5, 1)
    check_database(database)


# ----------------------------------------------------------------------
# Separate OS processes (spawn; Windows-compatible)
# ----------------------------------------------------------------------


@pytest.mark.parametrize("contest", ["identical", "conflicting"])
def test_separate_processes_are_serialized_by_the_database(database: Path, contest: str) -> None:
    _store(database).load_definition(bundle().components[0].identity)
    specs = {
        "first": ("set/1", "timing/1", None),
        "second": ("set/1", "timing/1", None if contest == "identical" else 240),
    }
    context = multiprocessing.get_context("spawn")  # the Windows start method, everywhere
    start, results = context.Barrier(2), context.Queue()
    processes = [
        context.Process(
            target=publish_in_another_process,
            args=(str(database), spec, start, results, name),
        )
        for name, spec in specs.items()
    ]
    for process in processes:
        process.start()
    reported = {}
    for _ in processes:
        name, pid, outcome, error = results.get(timeout=180)
        reported[name] = (pid, outcome, error)
    for process in processes:
        process.join(timeout=60)
        assert process.exitcode == 0
    assert all(error is None for _, _, error in reported.values()), reported
    pids = {pid for pid, _, _ in reported.values()}
    assert len(pids) == 2 and os.getpid() not in pids  # two real, separate processes
    outcomes = sorted(outcome for _, outcome, _ in reported.values())
    expected = ["COMMITTED", "EQUIVALENT"] if contest == "identical" else ["COMMITTED", "CONFLICT"]
    assert outcomes == expected
    # Reopen from a fresh adapter: exactly one binding per identity, the winner's.
    winner = next(n for n, (_, o, _) in reported.items() if o == "COMMITTED")
    set_version, timing, latency = specs[winner]
    won = bundle(set_version, timing=timing, latency_seconds=latency)
    store = _store(database)
    assert store.load_policy_set(won.policy_set.identity) == won.policy_set
    assert store.load_definition(won.component(_T.TIMING_POLICY).identity) == (
        won.component(_T.TIMING_POLICY)
    )
    assert _counts(database) == (1, 5, 1)
    check_database(database)


# ----------------------------------------------------------------------
# Transaction failures and commit uncertainty, reconciled by the real S1 use case
# ----------------------------------------------------------------------


class _Interrupted(BaseException):
    """Test-only: stands for the process being stopped mid-write."""


def test_a_failure_before_any_transaction_is_unavailability(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _store(database).load_definition(bundle().components[0].identity)
    monkeypatch.setattr(adapter_module, "_BUSY_TIMEOUT_SECONDS", 0.05)
    holder = raw(database)
    try:
        holder.execute("BEGIN EXCLUSIVE")  # the file cannot even be read
        for operation in (
            lambda: _store(database).publish(bundle()),
            lambda: _store(database).load_definition(bundle().components[0].identity),
        ):
            with pytest.raises(EquitySyntheticPolicyCatalogUnavailableError):
                operation()
        result = _publisher(database).execute(bundle())
        assert (result.outcome, result.writes) == (_P.CATALOG_UNAVAILABLE, 1)  # never retried
    finally:
        holder.rollback()
        holder.close()
    assert _counts(database) == (0, 0, 0)


@pytest.mark.parametrize(
    ("seam", "error"),
    [
        ("_insert_publication", sqlite3.OperationalError("test: disk I/O error")),
        ("_insert_definition", sqlite3.OperationalError("test: disk full")),
        ("_insert_set", sqlite3.OperationalError("test: disk full")),
        ("_insert_set", _Interrupted()),
    ],
    ids=["after BEGIN", "after the publication row", "after every component", "process stopped"],
)
def test_a_failure_before_commit_rolls_back_the_whole_bundle(
    database: Path, monkeypatch: pytest.MonkeyPatch, seam: str, error: BaseException
) -> None:
    assert _store(database).publish(bundle()).outcome is _W.COMMITTED  # set/1, sequence 1
    catalog = _catalog(database)

    def fail(connection: sqlite3.Connection, values: tuple[object, ...]) -> None:
        raise error

    monkeypatch.setattr(adapter_module, seam, fail)
    offered = bundle("set/3", timing="timing/late")  # a new set and a new component
    if isinstance(error, _Interrupted):
        with pytest.raises(_Interrupted):
            _store(database).publish(offered)
    else:
        with pytest.raises(
            EquitySyntheticPolicyCatalogNotCommittedError, match="nothing was written"
        ):
            _store(database).publish(offered)
        result = _publisher(database).execute(offered)
        assert (result.outcome, result.writes) == (_P.NOT_PUBLISHED, 1)
    assert _catalog(database) == catalog  # no partial bundle; earlier bindings unchanged
    monkeypatch.undo()
    assert _store(database).publish(offered).outcome is _W.COMMITTED
    assert [r["publication_sequence"] for r in rows(database, PUBLICATIONS)] == [1, 2]
    check_database(database)


def test_a_real_constraint_violation_rolls_back_the_whole_bundle(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    real = adapter_module._insert_set  # noqa: SLF001

    def violating(connection: sqlite3.Connection, values: tuple[object, ...]) -> None:
        bad = list(values)
        bad[adapter_module.SET_COLUMNS.index("timing_policy_version")] = "synthetic-other"
        real(connection, tuple(bad))

    monkeypatch.setattr(adapter_module, "_insert_set", violating)
    with pytest.raises(EquitySyntheticPolicyCatalogNotCommittedError) as raised:
        _store(database).publish(bundle())
    assert isinstance(raised.value.__cause__, sqlite3.IntegrityError)  # the database refused
    assert _counts(database) == (0, 0, 0)


def _commit_once(
    monkeypatch: pytest.MonkeyPatch, behaviour: Callable[[_Commit, sqlite3.Connection], None]
) -> None:
    """Test-only: replace the next COMMIT, once, with ``behaviour``."""
    real = adapter_module._commit  # noqa: SLF001
    armed = [True]

    def commit(connection: sqlite3.Connection) -> None:
        if not armed[0]:
            return real(connection)
        armed[0] = False
        return behaviour(real, connection)

    monkeypatch.setattr(adapter_module, "_commit", commit)


def _not_committed(real: _Commit, connection: sqlite3.Connection) -> None:
    raise sqlite3.OperationalError("test: COMMIT failed before it took effect")


def _acknowledgment_lost(real: _Commit, connection: sqlite3.Connection) -> None:
    real(connection)
    raise sqlite3.OperationalError("test: acknowledgment lost after COMMIT")


def _connection_lost(real: _Commit, connection: sqlite3.Connection) -> None:
    real(connection)
    connection.close()
    raise sqlite3.ProgrammingError("test: the connection failed around COMMIT")


@pytest.mark.parametrize(
    ("behaviour", "durable"),
    [(_not_committed, 0), (_acknowledgment_lost, 1), (_connection_lost, 1)],
    ids=["not committed", "acknowledgment lost", "connection lost"],
)
def test_a_commit_failure_is_unconfirmed_never_a_rollback_claim(
    database: Path, monkeypatch: pytest.MonkeyPatch, behaviour: Any, durable: int
) -> None:
    _commit_once(monkeypatch, behaviour)
    with pytest.raises(EquitySyntheticPolicyCatalogCommitUnconfirmedError) as raised:
        _store(database).publish(bundle())
    assert not isinstance(
        raised.value,
        EquitySyntheticPolicyCatalogNotCommittedError
        | EquitySyntheticPolicyCatalogUnavailableError,
    )
    assert _counts(database) == (durable, 5 * durable, durable)
    reopened = _store(database).load_policy_set(bundle().policy_set.identity)  # a fresh adapter
    assert (reopened == bundle().policy_set) is bool(durable)


@pytest.mark.parametrize(
    ("behaviour", "outcome", "writes"),
    [
        (_acknowledgment_lost, _P.EQUIVALENT_PUBLISHED, 1),
        (_connection_lost, _P.EQUIVALENT_PUBLISHED, 1),
        (_not_committed, _P.PUBLISHED, 2),
    ],
    ids=["acknowledgment lost", "connection lost", "not committed"],
)
def test_the_application_reconciles_an_unconfirmed_commit_on_real_sqlite(
    database: Path, monkeypatch: pytest.MonkeyPatch, behaviour: Any, outcome: Any, writes: int
) -> None:
    _commit_once(monkeypatch, behaviour)
    result = _publisher(database).execute(bundle())
    assert (result.outcome, result.writes, result.reconciled) == (outcome, writes, writes == 1)
    assert _counts(database) == (1, 5, 1)  # no duplicate publication
    assert [r["publication_sequence"] for r in rows(database, PUBLICATIONS)] == [1]


class _CompetitorBeforeReload(SQLiteEquitySyntheticPolicyCatalogStore):
    """Test-only coordination: another writer commits before this store's first reload."""

    def __init__(self, database: Path, competitor: Callable[[], None]) -> None:
        super().__init__(database)
        self._competitor: Callable[[], None] | None = competitor

    def load_definition(self, identity):  # type: ignore[no-untyped-def]
        if self._competitor is not None:
            competitor, self._competitor = self._competitor, None
            competitor()
        return super().load_definition(identity)


def test_reconciliation_finds_a_competitors_conflicting_binding(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def competitor() -> None:  # an independent adapter and connection
        assert _store(database).publish(_relabelled()).outcome is _W.COMMITTED

    store = _CompetitorBeforeReload(database, competitor)
    _commit_once(monkeypatch, _not_committed)
    result = _publisher(database, store).execute(bundle())
    assert (result.outcome, result.reconciled, result.writes) == (
        _P.POLICY_REFERENCE_CONFLICT,
        True,
        1,
    )  # permanent: never retried
    assert _store(database).load_definition(
        _relabelled().component(_T.TIMING_POLICY).identity
    ) == _relabelled().component(_T.TIMING_POLICY)
    assert _counts(database) == (1, 5, 1)


def test_reconciliation_that_cannot_read_the_catalog_is_unresolved(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(adapter_module, "_BUSY_TIMEOUT_SECONDS", 0.05)
    holder = raw(database)

    def lost_and_locked(real: _Commit, connection: sqlite3.Connection) -> None:
        real(connection)
        holder.execute("BEGIN EXCLUSIVE")  # no reader can open the database now
        raise sqlite3.OperationalError("test: acknowledgment lost after COMMIT")

    _commit_once(monkeypatch, lost_and_locked)
    try:
        result = _publisher(database).execute(bundle())
    finally:
        holder.rollback()
        holder.close()
    assert (result.outcome, result.reconciled) == (_P.PUBLICATION_RECONCILIATION_UNRESOLVED, True)
    assert _counts(database) == (1, 5, 1)  # it had committed; nothing was claimed or repeated


def test_publication_writes_stay_within_the_application_bound(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = []

    def never(connection: sqlite3.Connection) -> None:
        calls.append(1)
        raise sqlite3.OperationalError("test: COMMIT failed before it took effect")

    monkeypatch.setattr(adapter_module, "_commit", never)
    result = _publisher(database).execute(bundle())
    assert (result.outcome, result.writes, len(calls)) == (_P.RETRY_LIMIT_REACHED, 3, 3)
    assert _counts(database) == (0, 0, 0)


# ----------------------------------------------------------------------
# Restart, existing evidence and the historical proof boundary
# ----------------------------------------------------------------------


def test_bindings_survive_restart_and_a_file_copy(tmp_path: Path) -> None:
    database = evidence_database(tmp_path / "before.sqlite3")
    offered = bundle()
    assert _publisher(database).execute(offered).is_published
    moved = tmp_path / "moved.sqlite3"
    database.rename(moved)  # Windows refuses this while any connection is open
    store = _store(moved)
    for component in offered.components:
        assert store.load_definition(component.identity) == component
    assert store.load_policy_set(offered.policy_set.identity) == offered.policy_set
    assert _publisher(moved).execute(offered).outcome is _P.EQUIVALENT_PUBLISHED
    check_database(moved)


def test_an_existing_evidence_and_generation_database_is_extended_additively(
    tmp_path: Path,
) -> None:
    pipeline = Pipeline(tmp_path / "ei4e.sqlite3")
    establish_research(pipeline)
    audit = pipeline.generate()
    disposition = equity_recommendation_disposition(audit)
    assert disposition is not None and audit.inputs is not None
    assert pipeline.recorder().execute(audit).outcome.value == "COMMITTED"
    database = pipeline.database
    objects = schema_objects(database)
    evidence = {t: rows(database, t) for t in (SNAPSHOTS, TIMELINES, ENTRIES, DISPOSITIONS)}

    store = _store(database)
    assert (
        store.load_policy_set(
            EquitySyntheticPolicyBundle.of(audit.inputs.policy_set).policy_set.identity
        )
        is None
    )
    assert _counts(database) == (0, 0, 0)  # no backfill from the stored disposition
    assert {n: v for n, v in schema_objects(database).items() if n in objects} == objects

    published = EquitySyntheticPolicyBundle.of(audit.inputs.policy_set)
    assert _publisher(database).execute(published).is_published
    for table, before in evidence.items():
        assert rows(database, table) == before, table  # nothing relinked or rewritten
    assert published.policy_set.policy_set_fingerprint == disposition.policy_set_fingerprint
    assert disposition.generation_key == audit.generation_key  # eirk1 unchanged
    verified = pipeline.verify_stored().execute(disposition.generation_key)
    assert verified is not None and verified.evaluation_reproduced
    with closing(raw(database)) as connection, pytest.raises(sqlite3.IntegrityError):
        connection.execute(f"DELETE FROM {SNAPSHOTS}")  # noqa: S608 -- EI-2B triggers active
    # The S1 audit over real SQLite: a binding published today is no historical proof.
    audited = AuditEquityRecommendationPolicyBindingsUseCase(
        pipeline.store(), _store(database)
    ).execute([disposition.generation_key])
    (record,) = audited.records
    assert {f.status for f in record.findings} == {
        EquityRecommendationPolicyBindingStatus.BINDING_NOT_ESTABLISHED
    }
    assert all(f.catalog_fingerprint == f.recorded_fingerprint for f in record.findings)
    check_database(database)


def test_no_publication_history_is_implemented_from_the_catalog_sequence() -> None:
    assert not issubclass(
        SQLiteEquitySyntheticPolicyCatalogStore, EquitySyntheticPolicyPublicationHistory
    )
    assert issubclass(SQLiteEquitySyntheticPolicyCatalogStore, EquitySyntheticPolicyCatalogStore)
    for module in (adapter_module, schema_module):
        assert "EquitySyntheticPolicyPublicationHistory(" not in inspect.getsource(module)
        assert "published_before" not in inspect.getsource(module)


# ----------------------------------------------------------------------
# Package API and boundaries
# ----------------------------------------------------------------------


def test_the_adapter_and_schema_are_exported() -> None:
    for name in (
        "SQLiteEquitySyntheticPolicyCatalogStore",
        "initialize_equity_synthetic_policy_catalog_schema",
        "EQUITY_SYNTHETIC_POLICY_DEFINITION_SCHEMA",
        "EQUITY_SYNTHETIC_POLICY_PUBLICATION_SCHEMA",
        "EQUITY_SYNTHETIC_POLICY_SET_SCHEMA",
        "SQLiteEquityRecommendationGenerationStore",  # existing exports remain
    ):
        assert name in persistence.__all__, name


@pytest.mark.parametrize("module", [adapter_module, schema_module])
def test_the_catalog_stays_a_persistence_adapter(module) -> None:  # type: ignore[no-untyped-def]
    source = inspect.getsource(module)
    docstring = ast.get_docstring(ast.parse(source), clean=False)
    assert docstring is not None
    code = source.replace(docstring, "")  # prose may name what the code never does
    for forbidden in (
        "OR REPLACE",
        "OR IGNORE",
        "REPLACE INTO",
        "ON CONFLICT",
        "foreign_keys = OFF",
        "ignore_check_constraints",
        "writable_schema",
        "hashlib",
        "sleep",
        "while ",
        "Thread",
        "socket",
        "http",
        "broker",
        "record_generation",
        "record_attempt",
        "datetime",
    ):
        assert forbidden not in code, forbidden
    imported = set(re.findall(r"^\s*(?:from|import) ([\w.]+)", code, re.MULTILINE))
    assert all(
        name.split(".")[0]
        in {
            "__future__",
            "sqlite3",
            "collections",
            "contextlib",
            "pathlib",
            "typing",
            "northstar_application",
            "northstar_infrastructure",
        }
        for name in imported
    ), imported
