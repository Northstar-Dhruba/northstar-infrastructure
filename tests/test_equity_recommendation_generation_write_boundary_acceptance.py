"""EI4-D11-W4-I: end-to-end acceptance of the final generation write boundary (ADR-023).

The E-layer rows of the EI4-D11 acceptance matrix that the final boundary
adds, on real temporary SQLite through the real synthetic chain: EI-2C
synthetic candles, EI-2B snapshots, EI-3C timeline and CANDIDATE, explicit
EI4-D10 publication into the same file, EI-4C generation, the verified EI-4D-A
recorder, its Application-issued authorization, the adapter's validation, the
atomic commit, restart and frozen replay.

Rows evidenced here, alongside the existing EI-4E, EI4-D10-S4 and W2-I
modules that this module does not repeat:

- 1, 16: what is committed is exactly what the recorder authorized;
- 2, 18: a genuine attempt-only result is recorded with no policy evidence;
- 3, 28, 30: on the real chain the adapter has no raw entry, and fabricated,
  copied, swapped or crossed authorizations open no connection and write
  nothing, while the genuine path is unaffected;
- 14, 19, 23: equivalence, conflict, replay, audit and restart never rewrite
  committed evidence or the schema;
- 25: recording issues nothing and creates no history, lifecycle or order;
- 33: attempt-only evidence survives restart, and no authorization persists.

Authorizations come only from the real recorder: a store that observes the
authorization the recorder hands it and then writes through the real adapter,
or ``Pipeline.authorize``. Where a test alters an issued authorization with
``object.__setattr__`` that is labelled TEST-ONLY privileged tampering,
outside the trust boundary. Synthetic data only; no network, provider, NSE or
broker. Windows-only acceptance (EI4D11-DQ8); nothing here says anything about
profitability or trading readiness.
"""

from __future__ import annotations

import dataclasses
import json
import socket
import sqlite3
from collections.abc import Iterator
from contextlib import closing, contextmanager
from pathlib import Path
from typing import Any

import pytest
from northstar_application.application_services import (
    AuditEquityRecommendationPolicyBindingsUseCase,
    EquityRecommendationEligibilityAudit,
    EquityRecommendationRecordingOutcome,
    VerifiedEquityRecommendationAttemptWrite,
    VerifiedEquityRecommendationGenerationWrite,
    equity_recommendation_attempt,
    equity_recommendation_disposition,
    require_verified_equity_recommendation_attempt_write,
    require_verified_equity_recommendation_generation_write,
)
from northstar_application.ports import UnverifiedEquityRecommendationGenerationWriteError

from northstar_infrastructure.persistence import (
    SQLiteEquityRecommendationGenerationStore,
    SQLiteEquitySyntheticPolicyCatalogStore,
)
from northstar_infrastructure.persistence import (
    sqlite_equity_recommendation_generation as adapter_module,
)
from tests.equity_recommendation_acceptance_fixtures import (
    ATTEMPTS,
    DISPOSITIONS,
    TCS,
    History,
    Pipeline,
    check_database,
    counts,
    establish_research,
    ist,
    prior,
    raw,
    rows,
    schema_objects,
)

_O = EquityRecommendationRecordingOutcome
_Unverified = UnverifiedEquityRecommendationGenerationWriteError


@pytest.fixture(autouse=True)
def _refuse_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("EI4-D11-W4-I acceptance must not open network connections.")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)


@pytest.fixture
def pipeline(tmp_path: Path) -> Pipeline:
    return Pipeline(tmp_path / "ei4-d11-w4i.sqlite3")


class _Observed(SQLiteEquityRecommendationGenerationStore):
    """The real adapter, keeping each authorization the verified recorder hands it.

    It overrides nothing about writing: each call goes on to the real
    ``write_generation`` or ``write_attempt``, which validates and commits.
    """

    def __init__(self, database: Path) -> None:
        super().__init__(database)
        self.authorizations: list[Any] = []

    def write_generation(self, authorization):  # type: ignore[no-untyped-def]
        self.authorizations.append(authorization)
        return super().write_generation(authorization)

    def write_attempt(self, authorization):  # type: ignore[no-untyped-def]
        self.authorizations.append(authorization)
        return super().write_attempt(authorization)


def _proposed(pipeline: Pipeline) -> EquityRecommendationEligibilityAudit:
    pipeline.history = History()
    audit = pipeline.generate()
    assert audit.status.value == "PROPOSED"
    return audit


def _unresolved(pipeline: Pipeline) -> EquityRecommendationEligibilityAudit:
    """The PROPOSED key's candidate and policy set, with a prior same-listing issuance."""
    pipeline.history = History(records=(prior("earlier", TCS, ist(9, 30), 1),))
    try:
        return pipeline.generate()
    finally:
        pipeline.history = History()


def _attempt_only(pipeline: Pipeline) -> EquityRecommendationEligibilityAudit:
    pipeline.history = History(mode="missing")
    try:
        audit = pipeline.generate()
    finally:
        pipeline.history = History()
    assert audit.generation_key is None and equity_recommendation_disposition(audit) is None
    return audit


def _evidence(database: Path) -> tuple[list[tuple[Any, ...]], ...]:
    """Every generation row, with its rowid, exactly as stored."""
    with closing(raw(database)) as connection:
        return tuple(
            connection.execute(f"SELECT rowid, * FROM {table} ORDER BY rowid").fetchall()  # noqa: S608
            for table in (DISPOSITIONS, ATTEMPTS)
        )


@contextmanager
def _catalog_forbidden(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    def refuse(*_args: object) -> None:
        raise AssertionError("this operation must not read the policy catalog")

    with monkeypatch.context() as patch:
        patch.setattr(SQLiteEquitySyntheticPolicyCatalogStore, "load_definition", refuse)
        patch.setattr(SQLiteEquitySyntheticPolicyCatalogStore, "load_policy_set", refuse)
        yield


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


# ----------------------------------------------------------------------
# Rows 1, 16, 19: exactly what was authorized is committed, once
# ----------------------------------------------------------------------


def test_w4i_1_19_the_chain_commits_exactly_what_the_recorder_authorized(
    pipeline: Pipeline,
) -> None:
    establish_research(pipeline)  # EI-2C/2B/3C, then explicit EI4-D10 publication
    audit = _proposed(pipeline)
    store = _Observed(pipeline.database)
    result = pipeline.recorder(store).execute(audit)
    assert (result.outcome, result.writes, result.attempt_recorded) == (_O.COMMITTED, 1, True)
    (authorization,) = store.authorizations
    assert type(authorization) is VerifiedEquityRecommendationGenerationWrite
    assert require_verified_equity_recommendation_generation_write(authorization) is authorization
    assert authorization.disposition == equity_recommendation_disposition(audit)
    assert authorization.policy.is_verified  # the EI4-D10 binding it was issued on
    # The committed rows are exactly the authorized records' rows: nothing more.
    (disposition_row,) = rows(pipeline.database, DISPOSITIONS)
    (attempt_row,) = rows(pipeline.database, ATTEMPTS)
    assert tuple(disposition_row[c] for c in adapter_module.DISPOSITION_COLUMNS) == (
        adapter_module.disposition_row(authorization.disposition)
    )
    assert tuple(attempt_row[c] for c in adapter_module.ATTEMPT_COLUMNS) == (
        adapter_module.attempt_row(authorization.attempt)
    )
    assert attempt_row["generation_key"] == disposition_row["generation_key"]  # no orphan
    check_database(pipeline.database)  # integrity and foreign keys
    # Row 19: the same verified operation again is equivalent; nothing is added.
    before = _evidence(pipeline.database)
    again = pipeline.recorder(store).execute(_proposed(pipeline))
    assert again.outcome is _O.EQUIVALENT_COMMITTED and again.writes == 1
    assert _evidence(pipeline.database) == before


# ----------------------------------------------------------------------
# Rows 2, 18: attempt-only recording needs no policy evidence
# ----------------------------------------------------------------------


def test_w4i_2_a_genuine_attempt_only_result_is_recorded_without_policy_evidence(
    pipeline: Pipeline, monkeypatch: pytest.MonkeyPatch
) -> None:
    establish_research(pipeline, publish=False)  # nothing published at all
    audit = _attempt_only(pipeline)
    attempt = equity_recommendation_attempt(audit)
    store = _Observed(pipeline.database)
    with _catalog_forbidden(monkeypatch):  # recording consults no policy evidence
        result = pipeline.recorder(store).execute(audit)
    assert (result.outcome, result.attempt_recorded) == (_O.COMMITTED, True)
    (authorization,) = store.authorizations
    assert type(authorization) is VerifiedEquityRecommendationAttemptWrite
    assert require_verified_equity_recommendation_attempt_write(authorization) is authorization
    assert authorization.covered == (attempt.attempt_identity, attempt.attempt_document)
    # No assessment, proposal or disposition is fabricated; the attempt keeps its
    # identity and its own status, one the schema already allowed.
    assert counts(pipeline.database) == (0, 1)
    (row,) = rows(pipeline.database, ATTEMPTS)
    assert row["attempt_identity"] == attempt.attempt_identity
    assert row["status"] == json.loads(attempt.attempt_document)["status"]
    assert (row["generation_key"], row["disposition_key"]) == (None, None)
    repeated = _attempt_only(pipeline)  # generation itself may consult the catalog
    with _catalog_forbidden(monkeypatch):
        again = pipeline.recorder(store).execute(repeated)
    assert (again.outcome, again.attempt_recorded) == (_O.EQUIVALENT_COMMITTED, True)
    assert counts(pipeline.database) == (0, 1)
    check_database(pipeline.database)


# ----------------------------------------------------------------------
# Rows 3, 28, 30: no raw entry, and nothing unauthorized reaches SQLite
# ----------------------------------------------------------------------


def test_w4i_3_30_the_real_chain_has_no_raw_entry_and_refuses_fabrications(
    pipeline: Pipeline, connections: list[Path]
) -> None:
    establish_research(pipeline)
    proposed = _proposed(pipeline)
    only_audit = _attempt_only(pipeline)
    generation = pipeline.authorize(proposed)  # genuine, from the real recorder
    attempt = pipeline.authorize(only_audit)
    swapped = pipeline.authorize(only_audit)
    # TEST-ONLY privileged tampering: the genuine attempt authorization made to
    # carry another attempt (the PROPOSED operation's own).
    other = equity_recommendation_attempt(proposed)
    assert other.attempt_identity != attempt.attempt.attempt_identity
    object.__setattr__(swapped, "attempt", other)
    store = pipeline.store()
    assert store.load_disposition(generation.disposition.generation_key) is None  # the schema
    assert not hasattr(store, "record_generation") and not hasattr(store, "record_attempt")
    before, calls = _evidence(pipeline.database), len(connections)
    fabricated_attempts = (
        None,
        attempt.attempt,  # the raw record
        VerifiedEquityRecommendationAttemptWrite(attempt.attempt),  # constructed, never issued
        dataclasses.replace(attempt),  # a copy of an issued one
        swapped,  # issued, then made to cover another attempt
        generation,  # the generation authorization on the attempt-only entry
    )
    for candidate in fabricated_attempts:
        with pytest.raises(_Unverified):
            store.write_attempt(candidate)  # type: ignore[arg-type]
    for candidate in (attempt, (generation.disposition, generation.attempt)):
        with pytest.raises(_Unverified):
            store.write_generation(candidate)  # type: ignore[arg-type]
    assert len(connections) == calls  # row 28: no connection, so no transaction or lock
    assert _evidence(pipeline.database) == before == ([], [])
    # Row 30: the genuine attempt-only path is unaffected.
    assert store.write_attempt(attempt).outcome.value == "COMMITTED"
    assert counts(pipeline.database) == (0, 1)


# ----------------------------------------------------------------------
# Rows 14, 22, 23: committed evidence and the schema are never rewritten
# ----------------------------------------------------------------------


def test_w4i_14_23_later_operations_never_rewrite_committed_evidence(
    pipeline: Pipeline, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    establish_research(pipeline)
    proposed = _proposed(pipeline)
    disposition = equity_recommendation_disposition(proposed)
    assert disposition is not None
    assert pipeline.recorder().execute(proposed).outcome is _O.COMMITTED
    assert pipeline.recorder().execute(_attempt_only(pipeline)).outcome is _O.COMMITTED
    committed = _evidence(pipeline.database)
    schema = schema_objects(pipeline.database)
    # Equivalence, a conflict under the occupied key (row 14), verification and the
    # read-only policy audit ...
    assert pipeline.recorder().execute(_proposed(pipeline)).outcome is _O.EQUIVALENT_COMMITTED
    conflict = pipeline.recorder().execute(_unresolved(pipeline))
    assert conflict.outcome is _O.CONTENT_CONFLICT and conflict.authoritative == disposition
    assert pipeline.verify_stored().execute(disposition.generation_key) is not None
    audited = AuditEquityRecommendationPolicyBindingsUseCase(
        pipeline.store(), pipeline.catalog()
    ).execute([disposition.generation_key])
    assert len(audited.records) == 1
    # ... and a restart with frozen replay that never reads the catalog (row 22).
    moved = tmp_path / "after-restart.sqlite3"
    pipeline.database.rename(moved)
    restarted = Pipeline(moved)
    with _catalog_forbidden(monkeypatch):
        replayed = restarted.verify_stored().execute(disposition.generation_key)
    assert replayed is not None and replayed.evaluation_reproduced
    # Every row committed before is still there, byte-identical, under its rowid;
    # only the conflict's own attempt was added. The schema is unchanged.
    after = _evidence(moved)
    for table_before, table_after in zip(committed, after, strict=True):
        assert table_after[: len(table_before)] == table_before
    assert (len(after[0]), len(after[1])) == (1, len(committed[1]) + 1)
    assert schema_objects(moved) == schema
    check_database(moved)


# ----------------------------------------------------------------------
# Row 25: nothing is issued, and no history, lifecycle or order is written
# ----------------------------------------------------------------------


def test_w4i_25_recording_issues_nothing_and_writes_no_history_or_orders(
    pipeline: Pipeline,
) -> None:
    establish_research(pipeline)
    history = History()
    pipeline.history = history
    audit = pipeline.generate()
    tables_before = set(schema_objects(pipeline.database))
    result = pipeline.recorder().execute(audit)
    assert result.outcome is _O.COMMITTED
    disposition = result.disposition
    assert disposition is not None and not disposition.is_issued
    assert disposition.proposal is not None and not disposition.proposal.is_actionable
    assert history.asked  # the issued-recommendation history was consulted, read-only
    objects = schema_objects(pipeline.database)
    added = set(objects) - tables_before
    assert all(name.startswith("equity_recommendation_generation") for name in added), added
    forbidden = {"issued", "issuance", "lifecycle", "broker", "order", "orders"}
    forbidden |= {"position", "positions", "fill", "fills", "pnl"}
    tables = [name for name, (kind, _) in objects.items() if kind == "table"]
    for name in tables:  # no issuance, lifecycle, broker, order, position or fill table
        assert not set(name.lower().split("_")) & forbidden, name


# ----------------------------------------------------------------------
# Row 33: attempt-only evidence survives restart; no authorization persists
# ----------------------------------------------------------------------


def test_w4i_33_attempt_only_evidence_survives_restart_and_no_authorization_persists(
    pipeline: Pipeline, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    establish_research(pipeline)
    audit = _attempt_only(pipeline)
    attempt = equity_recommendation_attempt(audit)
    assert pipeline.recorder().execute(audit).outcome is _O.COMMITTED
    committed = _evidence(pipeline.database)
    moved = tmp_path / "restarted.sqlite3"
    pipeline.database.rename(moved)  # Windows refuses while any connection is open
    restarted = Pipeline(moved)
    assert restarted.store().load_attempt(attempt.attempt_identity) == attempt
    repeated = _attempt_only(restarted)  # generation itself may consult the catalog
    with _catalog_forbidden(monkeypatch):
        again = restarted.recorder().execute(repeated)
    assert (again.outcome, again.attempt_recorded) == (_O.EQUIVALENT_COMMITTED, True)
    assert _evidence(moved) == committed  # re-recording changed no row
    with closing(raw(moved)) as connection:
        columns = {
            column[1]
            for table in (DISPOSITIONS, ATTEMPTS)
            for column in connection.execute(f"PRAGMA table_info({table})")  # noqa: S608
        }
    assert not [c for c in columns if "authoriz" in c or "token" in c]  # nothing persisted
    check_database(moved)
