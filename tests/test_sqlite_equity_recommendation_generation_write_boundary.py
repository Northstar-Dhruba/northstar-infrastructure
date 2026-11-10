"""EI4-D11-W2-I: the SQLite generation adapter accepts only authorized writes (ADR-023).

Tests of the enforced Infrastructure boundary, on real temporary SQLite files:

- ``write_generation`` and ``write_attempt`` call the Application validators
  first, so a missing, foreign, unissued, copied, look-alike or content-changed
  authorization is refused with UnverifiedEquityRecommendationGenerationWriteError
  before ``_connect`` -- the adapter's real connection seam, counted here -- is
  ever called;
- the legacy ``record_generation`` and ``record_attempt`` refuse the same way;
- an authorized write persists exactly the validated canonical content, even
  when the caller's decoded document is mutated after validation;
- the existing transaction, conflict, reconciliation and replay behaviour holds.

Every authorization comes from the real verified recorder through
``World.authorize`` (tests/equity_recommendation_write_authorization_fixtures):
nothing here constructs, copies into validity or registers one. Where a test
alters an already-issued authorization with ``object.__setattr__``, that is a
labelled simulation of privileged tampering -- outside the EI4-D11 trust
boundary -- showing the validator still refuses what it can detect. Nested
document mutation needs no privilege. Synthetic data only; no network.
"""

from __future__ import annotations

import dataclasses
import socket
import sqlite3
import types
from collections.abc import Iterator
from contextlib import closing, contextmanager
from datetime import date
from pathlib import Path
from typing import Any

import pytest
from northstar_application.application_services import (
    EquityRecommendationRecordingOutcome,
    VerifiedEquityRecommendationAttemptWrite,
    VerifiedEquityRecommendationGenerationWrite,
    equity_recommendation_attempt,
    equity_recommendation_disposition,
)
from northstar_application.ports import (
    EquityRecommendationStoreOutcome,
    UnverifiedEquityRecommendationGenerationWriteError,
)

from northstar_infrastructure.persistence import (
    sqlite_equity_recommendation_generation as adapter_module,
)
from tests.equity_intraday_research_timeline_fixtures import count, raw
from tests.equity_recommendation_acceptance_fixtures import (
    Pipeline,
    establish_research,
)
from tests.equity_recommendation_acceptance_fixtures import (
    rows as acceptance_rows,
)
from tests.equity_recommendation_generation_fixtures import ATTEMPTS, DISPOSITIONS, World, records
from tests.equity_recommendation_write_authorization_fixtures import plant_generation_records

_O = EquityRecommendationRecordingOutcome
_W = EquityRecommendationStoreOutcome
_Unverified = UnverifiedEquityRecommendationGenerationWriteError


@pytest.fixture(autouse=True)
def _refuse_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("EI4-D11-W2-I tests must not open network connections.")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)


@pytest.fixture
def world(tmp_path: Path) -> World:
    return World(tmp_path / "northstar.sqlite3")


@pytest.fixture
def connections(monkeypatch: pytest.MonkeyPatch) -> list[Path]:
    """Every call of the adapter's real connection seam, ``_connect``."""
    opened: list[Path] = []
    real = adapter_module._connect

    def connect(database_path: Path) -> sqlite3.Connection:
        opened.append(database_path)
        return real(database_path)

    monkeypatch.setattr(adapter_module, "_connect", connect)
    return opened


def _evidence(database: Path) -> tuple[list[tuple[Any, ...]], list[tuple[Any, ...]]]:
    with closing(raw(database)) as connection:
        return tuple(  # type: ignore[return-value]
            connection.execute(f"SELECT * FROM {table} ORDER BY 1").fetchall()  # noqa: S608
            for table in (DISPOSITIONS, ATTEMPTS)
        )


def _assert_unlocked(database: Path) -> None:
    """No transaction is open anywhere: a fresh connection takes the exclusive lock at once."""
    with closing(sqlite3.connect(database, timeout=0, isolation_level=None)) as connection:
        connection.execute("BEGIN EXCLUSIVE")
        connection.rollback()


def _tamper(obj: object, name: str, value: object) -> None:
    """TEST-ONLY privileged tampering with a frozen record (outside the trust boundary)."""
    object.__setattr__(obj, name, value)


@contextmanager
def _refused_before_sqlite(world: World, opened: list[Path]) -> Iterator[None]:
    """Expect the typed refusal, no connection, no transaction and unchanged evidence."""
    before = _evidence(world.database)
    calls = len(opened)
    with pytest.raises(_Unverified):
        yield
    assert len(opened) == calls, "a refused write opened a SQLite connection"
    assert _evidence(world.database) == before
    _assert_unlocked(world.database)


def _schema(world: World) -> None:
    assert world.store().load_disposition("eirk1-sha256:" + "0" * 64) is None


# ----------------------------------------------------------------------
# Refusals before SQLite (required cases 1-19)
# ----------------------------------------------------------------------


def test_missing_wrong_type_and_look_alike_authorizations_are_refused(
    world: World, connections: list[Path]
) -> None:
    _schema(world)
    generation = world.authorize(world.proposed())
    attempt = world.authorize(world.attempt_only())
    lookalike = types.SimpleNamespace(
        **{f.name: getattr(generation, f.name) for f in dataclasses.fields(generation)},
        disposition=generation.disposition,
    )
    store = world.store()
    for candidate in (None, generation.disposition, attempt, lookalike):  # cases 1, 3, 6, 13
        with _refused_before_sqlite(world, connections):
            store.write_generation(candidate)
    for candidate in (None, generation, attempt.attempt, lookalike):  # cases 2, 3, 6, 14
        with _refused_before_sqlite(world, connections):
            store.write_attempt(candidate)


def test_unissued_and_copied_authorizations_are_refused(
    world: World, connections: list[Path]
) -> None:
    _schema(world)
    generation = world.authorize(world.proposed())
    attempt = world.authorize(world.attempt_only())
    unissued = VerifiedEquityRecommendationGenerationWrite(  # case 4: public construction
        generation.verified, generation.policy, generation.attempt
    )
    store = world.store()
    for candidate in (unissued, dataclasses.replace(generation)):  # cases 4 and 5
        with _refused_before_sqlite(world, connections):
            store.write_generation(candidate)
    for candidate in (
        VerifiedEquityRecommendationAttemptWrite(attempt.attempt),
        dataclasses.replace(attempt),
    ):
        with _refused_before_sqlite(world, connections):
            store.write_attempt(candidate)


@pytest.mark.parametrize(
    "field_name",
    ["assessment_document", "proposal_document", "kind", "evaluator_version"],
)
def test_a_disposition_changed_after_issuance_is_refused(
    world: World, connections: list[Path], field_name: str
) -> None:
    _schema(world)
    authorization = world.authorize(world.proposed())
    other, _ = records(world.unresolved())  # the same key, other content and kind
    assert other.generation_key == authorization.disposition.generation_key
    value = {"evaluator_version": "northstar.equity-recommendation-evaluator/2"}.get(
        field_name, getattr(other, field_name)
    )
    _tamper(authorization.disposition, field_name, value)  # cases 8, 10, 11 (and content)
    with _refused_before_sqlite(world, connections):
        world.store().write_generation(authorization)


def test_changed_nested_assessment_attempt_or_policy_identity_is_refused(
    world: World, connections: list[Path]
) -> None:
    _schema(world)
    nested = world.authorize(world.proposed())
    nested.disposition.assessment.document["reasons"] = ["CORE_FORMATION_REJECTED"]  # case 7
    with _refused_before_sqlite(world, connections):
        world.store().write_generation(nested)
    swapped = world.authorize(world.proposed())
    _tamper(swapped, "attempt", equity_recommendation_attempt(world.unresolved()))  # case 9
    with _refused_before_sqlite(world, connections):
        world.store().write_generation(swapped)
    policy = world.authorize(world.proposed())
    other_set = world.authorize(world.not_eligible())  # another published policy set
    _tamper(policy, "policy", other_set.policy)  # case 12
    with _refused_before_sqlite(world, connections):
        world.store().write_generation(policy)
    attempt = world.authorize(world.attempt_only())
    other_attempt = equity_recommendation_attempt(world.run(day=date(2026, 10, 19)))
    assert other_attempt.attempt_identity != attempt.attempt.attempt_identity
    _tamper(attempt, "attempt", other_attempt)
    with _refused_before_sqlite(world, connections):
        world.store().write_attempt(attempt)


def test_the_legacy_raw_writes_refuse_before_sqlite(world: World, connections: list[Path]) -> None:
    _schema(world)
    disposition, attempt = records(world.proposed())
    only = equity_recommendation_attempt(world.attempt_only())
    store = world.store()
    with _refused_before_sqlite(world, connections):  # cases 15, 17, 18, 19
        store.record_generation(disposition, attempt)
    with _refused_before_sqlite(world, connections):  # case 16
        store.record_attempt(only)
    with _refused_before_sqlite(world, connections):  # not even with a valid authorization's parts
        authorization = world.authorize(world.proposed())
        store.record_generation(authorization.disposition, authorization.attempt)
    assert count(world.database, DISPOSITIONS) == count(world.database, ATTEMPTS) == 0


# ----------------------------------------------------------------------
# What an authorized write persists (cases 20-22)
# ----------------------------------------------------------------------


def _stored_row(database: Path, key: str) -> tuple[Any, ...]:
    columns = ", ".join(adapter_module.DISPOSITION_COLUMNS)
    with closing(raw(database)) as connection:
        return tuple(
            connection.execute(
                f"SELECT {columns} FROM {DISPOSITIONS} WHERE generation_key = ?",  # noqa: S608
                (key,),
            ).fetchone()
        )


def test_an_authorized_write_persists_exactly_the_validated_content(world: World) -> None:
    authorization = world.authorize(world.proposed())
    expected = adapter_module.disposition_row(authorization.disposition)
    written = world.store().write_generation(authorization)
    assert (written.outcome, written.attempt_inserted) == (_W.COMMITTED, True)  # case 20
    assert _stored_row(world.database, authorization.disposition.generation_key) == expected
    store = world.store()  # case 22: reads need no authorization
    assert store.load_disposition(authorization.disposition.generation_key) == (
        authorization.disposition
    )
    assert store.load_attempt(authorization.attempt.attempt_identity) == authorization.attempt


def test_mutation_between_validation_and_persistence_is_never_committed(
    world: World, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Case 21: the caller's decoded document is changed right AFTER the validator
    # accepted it (a deterministic seam at the adapter's validator call). The
    # adapter writes from its own snapshot of the canonical text, so the change
    # never reaches SQLite: the originally verified content is what is committed.
    authorization = world.authorize(world.proposed())
    expected = adapter_module.disposition_row(authorization.disposition)
    real = adapter_module.require_verified_equity_recommendation_generation_write

    def validate_then_mutate(candidate: object) -> Any:
        validated = real(candidate)
        document = validated.disposition.assessment.document
        document["inputs"]["candidate"]["candidate_sequence"] = 2  # a different CANDIDATE
        document["request"]["listing"]["symbol"] = "RELIANCE"
        return validated

    monkeypatch.setattr(
        adapter_module,
        "require_verified_equity_recommendation_generation_write",
        validate_then_mutate,
    )
    assert world.store().write_generation(authorization).outcome is _W.COMMITTED
    assert _stored_row(world.database, authorization.disposition.generation_key) == expected
    stored = world.store().load_disposition(authorization.disposition.generation_key)
    assert stored is not None and stored.assessment.document["request"]["listing"]["symbol"] != (
        "RELIANCE"
    )


# ----------------------------------------------------------------------
# Positive paths through the enforced adapter
# ----------------------------------------------------------------------


def test_authorized_generation_and_attempt_only_writes_keep_their_semantics(
    world: World,
) -> None:
    proposed = world.authorize(world.proposed())
    only = world.authorize(world.attempt_only())
    store = world.store()
    assert store.write_generation(proposed).outcome is _W.COMMITTED
    assert (count(world.database, DISPOSITIONS), count(world.database, ATTEMPTS)) == (1, 1)
    assert store.write_attempt(only).outcome is _W.COMMITTED  # the attempt alone
    assert (count(world.database, DISPOSITIONS), count(world.database, ATTEMPTS)) == (1, 2)
    again = store.write_generation(proposed)
    assert (again.outcome, again.attempt_inserted) == (_W.EQUIVALENT, False)
    assert store.write_attempt(only).outcome is _W.EQUIVALENT
    conflict = store.write_generation(world.authorize(world.unresolved()))
    assert conflict.outcome is _W.CONFLICT and conflict.stored == proposed.disposition
    assert count(world.database, DISPOSITIONS) == 1  # the first writer stays


def test_the_recorder_writes_through_the_enforced_adapter_and_a_catalog_failure_writes_nothing(
    world: World, connections: list[Path]
) -> None:
    assert world.recorder().execute(world.proposed()).outcome is _O.COMMITTED
    assert connections, "the recorder's authorized write reached SQLite"
    audit = world.not_eligible()  # generated against the published catalog
    before = _evidence(world.database)
    calls = len(connections)
    world_catalog = world.catalog
    world.catalog = lambda: _unreadable_catalog(world.database)  # type: ignore[method-assign]
    try:
        result = world.recorder().execute(audit)
    finally:
        world.catalog = world_catalog  # type: ignore[method-assign]
    assert result.outcome is _O.POLICY_REFERENCE_UNAVAILABLE and result.writes == 0
    assert len(connections) == calls and _evidence(world.database) == before


def _unreadable_catalog(database: Path) -> Any:
    from northstar_infrastructure.persistence import SQLiteEquitySyntheticPolicyCatalogStore

    return SQLiteEquitySyntheticPolicyCatalogStore(database.with_name("no-such-catalog.sqlite3"))


# ----------------------------------------------------------------------
# End to end: the whole synthetic chain through the enforced boundary
# ----------------------------------------------------------------------


def test_the_synthetic_chain_writes_only_through_the_enforced_boundary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pipeline = Pipeline(tmp_path / "ei4-d11-w2i.sqlite3")
    establish_research(pipeline)  # EI-2B snapshots, EI-3C timeline, explicit EI4-D10 policies
    audit = pipeline.generate()  # EI-4C, checked against the published catalog
    disposition = equity_recommendation_disposition(audit)
    assert disposition is not None
    # The recorder verifies source, replay and policy, issues the authorization,
    # and the adapter validates it before committing exactly that evidence.
    assert pipeline.recorder().execute(audit).outcome is _O.COMMITTED
    (row,) = acceptance_rows(pipeline.database, "equity_recommendation_generation_dispositions")
    assert row["assessment_document"] == disposition.assessment_document
    # A raw direct adapter write is refused.
    other = equity_recommendation_attempt(audit)
    with pytest.raises(_Unverified):
        pipeline.store().record_generation(disposition, other)
    # Restart: the file is renamed (Windows refuses while any connection is open).
    moved = tmp_path / "after-restart.sqlite3"
    pipeline.database.rename(moved)
    restarted = Pipeline(moved)
    assert restarted.store().load_disposition(disposition.generation_key) == disposition
    assert restarted.recorder().execute(restarted.generate()).outcome is _O.EQUIVALENT_COMMITTED

    # Frozen replay never needs the policy catalog.
    def refuse(*_args: object) -> None:
        raise AssertionError("historical replay must not read the policy catalog")

    from northstar_infrastructure.persistence import SQLiteEquitySyntheticPolicyCatalogStore

    monkeypatch.setattr(SQLiteEquitySyntheticPolicyCatalogStore, "load_definition", refuse)
    monkeypatch.setattr(SQLiteEquitySyntheticPolicyCatalogStore, "load_policy_set", refuse)
    verified = restarted.verify_stored().execute(disposition.generation_key)
    assert verified is not None and verified.evaluation_reproduced


def test_a_record_planted_before_enforcement_keeps_first_writer_authority(world: World) -> None:
    # TEST-ONLY LEGACY-EVIDENCE SETUP: another verifiable assessment under the
    # genuine key, planted by raw SQL (outside the boundary). Invalid planted
    # evidence is EI-4E e20's second test (acceptance faults module).
    proposed, _ = records(world.proposed())
    unresolved, unresolved_attempt = records(world.unresolved())
    _schema(world)
    plant_generation_records(world.database, unresolved, unresolved_attempt)
    before = _evidence(world.database)
    result = world.recorder().execute(world.proposed())
    assert result.outcome is _O.CONTENT_CONFLICT and not result.is_stored
    assert result.authoritative == unresolved  # verified, so reported, never replaced
    assert _evidence(world.database)[0] == before[0]  # the occupied key is not overwritten
    assert proposed.generation_key == unresolved.generation_key
