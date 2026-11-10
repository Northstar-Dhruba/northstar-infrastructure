"""EI-4E acceptance under faults: atomicity, uncertain COMMIT, concurrency and tampering.

Every record here is produced by the real chain (tests/equity_recommendation_
acceptance_fixtures.py) and recorded through the real EI-4D-A recorder and
EI-4D-B SQLite store on a disposable file. Faults are injected only where a
test says so, through these labelled test-only seams, none of which is a
production shortcut:

- the adapter's module-level ``_connect``, ``_insert_disposition``,
  ``_insert_attempt`` and ``_commit`` functions (the EI-4D-B transaction test
  seam), replaced with monkeypatch for one test;
- ``bypass``: a separate raw connection to the disposable file that drops a
  trigger or relaxes CHECK or foreign-key enforcement before writing;
- a forged but internally consistent record built from public Application
  types, to show what SQLite cannot see and the Application verifier does.

Concurrency uses independent connections -- threads coordinated by
threading.Barrier, and separate spawned OS processes coordinated by a
multiprocessing Barrier -- and never a sleep. Test names carry the EI-4E prompt
section they evidence (e17 to e21).
"""

from __future__ import annotations

import json
import multiprocessing
import os
import socket
import sqlite3
import threading
from collections.abc import Callable
from contextlib import closing
from dataclasses import replace
from decimal import Decimal
from itertools import groupby
from pathlib import Path
from typing import Any

import pytest
from northstar_application.application_services import (
    EquityRecommendationDispositionKind,
    EquityRecommendationEligibilityAudit,
    EquityRecommendationGenerationAttempt,
    EquityRecommendationGenerationDisposition,
    EquityRecommendationGenerationStatus,
    EquityRecommendationInputs,
    EquityRecommendationRecordingOutcome,
    EquityRecommendationSourceVerificationError,
    IncompatibleEquityRecommendationRecordError,
    InvalidEquityRecommendationRecordError,
    assess_equity_recommendation_inputs,
    equity_recommendation_attempt,
    equity_recommendation_disposition,
)
from northstar_application.ports import (
    EquityRecommendationGenerationNotCommittedError,
    EquityRecommendationStoreOutcome,
    UnverifiedEquityRecommendationGenerationWriteError,
)
from northstar_core.foundation.value_objects import Price

from northstar_infrastructure.persistence import SQLiteEquityRecommendationGenerationStore
from northstar_infrastructure.persistence import (
    sqlite_equity_recommendation_generation as adapter_module,
)
from tests.equity_recommendation_acceptance_fixtures import (
    ATTEMPTS,
    DISPOSITIONS,
    INR,
    TCS,
    History,
    Pipeline,
    Research,
    bypass,
    check_database,
    counts,
    documents,
    establish_research,
    ist,
    prior,
    raw,
    record_in_another_process,
    rows,
    schema_objects,
)
from tests.equity_recommendation_write_authorization_fixtures import plant_generation_records

_O = EquityRecommendationRecordingOutcome
_S = EquityRecommendationGenerationStatus
_K = EquityRecommendationDispositionKind
_W = EquityRecommendationStoreOutcome
_Records = tuple[EquityRecommendationGenerationDisposition, EquityRecommendationGenerationAttempt]
_Commit = Callable[[sqlite3.Connection], None]
_FUTURE_EVALUATOR = "northstar.equity-recommendation-evaluator/2"


@pytest.fixture(autouse=True)
def _refuse_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("EI-4E acceptance must not open network connections.")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)


@pytest.fixture
def pipeline(tmp_path: Path) -> Pipeline:
    return Pipeline(tmp_path / "ei4e-faults.sqlite3")


@pytest.fixture
def research(pipeline: Pipeline) -> Research:
    return establish_research(pipeline)


def _records(audit: EquityRecommendationEligibilityAudit) -> _Records:
    disposition = equity_recommendation_disposition(audit)
    assert disposition is not None
    return disposition, equity_recommendation_attempt(audit)


def _proposed(pipeline: Pipeline) -> EquityRecommendationEligibilityAudit:
    pipeline.history = History()
    audit = pipeline.generate()
    assert audit.status is _S.PROPOSED
    return audit


def _unresolved(pipeline: Pipeline) -> EquityRecommendationEligibilityAudit:
    """The PROPOSED key's candidate and policy set, with a prior same-listing issuance."""
    pipeline.history = History(records=(prior("earlier", TCS, ist(9, 30), 1),))
    try:
        audit = pipeline.generate()
    finally:
        pipeline.history = History()
    assert audit.status is _S.EVALUATION_UNRESOLVED
    return audit


def _not_eligible(pipeline: Pipeline) -> EquityRecommendationEligibilityAudit:
    audit = pipeline.generate(generation="generation/strict", set_version="set/strict")
    assert audit.status is _S.NOT_ELIGIBLE and audit.generation_key is not None
    return audit


def _attempt_only(pipeline: Pipeline) -> EquityRecommendationEligibilityAudit:
    pipeline.history = History(mode="missing")
    try:
        audit = pipeline.generate()
    finally:
        pipeline.history = History()
    assert audit.generation_key is None
    return audit


# ----------------------------------------------------------------------
# e17. Atomicity and rollback on real SQLite
# ----------------------------------------------------------------------


def _traced(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Test-only: record every statement each adapter connection runs after it opens."""
    statements: list[str] = []
    real = adapter_module._connect  # noqa: SLF001

    def connect(path: Path) -> sqlite3.Connection:
        connection = real(path)
        connection.set_trace_callback(statements.append)
        return connection

    monkeypatch.setattr(adapter_module, "_connect", connect)
    return statements


def _verbs(statements: list[str]) -> list[str]:
    """The traced statements, one per statement executed.

    SQLite's statement trace reports an INSERT again as each of its row
    triggers runs, so consecutive repeats of one statement are collapsed.
    """

    def verb(statement: str) -> str:
        words = statement.split()
        if words[0] == "INSERT":
            return f"INSERT {words[2]}"
        return " ".join(words[:2]) if words[0] == "BEGIN" else words[0]

    return [verb(s) for s, _ in groupby(statements)]


def test_e17_a_disposition_its_proposal_and_its_attempt_commit_in_one_transaction(
    pipeline: Pipeline, research: Research, monkeypatch: pytest.MonkeyPatch
) -> None:
    pipeline.store().load_disposition("eirk1-sha256:" + "0" * 64)  # schema, untraced
    statements = _traced(monkeypatch)
    result = pipeline.recorder().execute(_proposed(pipeline))
    assert (result.outcome, result.writes) == (_O.COMMITTED, 1)
    writes = [v for v in _verbs(statements) if v not in {"SELECT"}]
    assert writes == [
        "BEGIN IMMEDIATE",
        f"INSERT {DISPOSITIONS}",
        f"INSERT {ATTEMPTS}",
        "COMMIT",
    ]  # one transaction; the proposal travels in the disposition row
    assert not any(s.startswith(("SAVEPOINT", "ROLLBACK", "RELEASE")) for s in statements)
    assert sum(s.startswith("BEGIN") for s in statements) == 1  # no hidden second transaction
    statements.clear()
    conflict = pipeline.recorder().execute(_unresolved(pipeline))
    assert conflict.outcome is _O.CONTENT_CONFLICT
    assert [v for v in _verbs(statements) if v != "SELECT"] == [
        "BEGIN IMMEDIATE",
        f"INSERT {ATTEMPTS}",  # only the conflicting attempt is appended
        "COMMIT",
    ]
    assert counts(pipeline.database) == (1, 2)
    check_database(pipeline.database)


class _Interrupted(BaseException):
    """Test-only: stands for the process being stopped mid-write."""


@pytest.mark.parametrize(
    ("seam", "error"),
    [
        ("_insert_disposition", sqlite3.OperationalError("test: disk I/O error")),
        ("_insert_attempt", sqlite3.OperationalError("test: disk full")),
        ("_insert_attempt", _Interrupted()),
    ],
    ids=["before any insert completes", "after the disposition insert", "process stopped"],
)
def test_e17_a_failure_before_commit_leaves_no_partial_generation_record(
    pipeline: Pipeline,
    research: Research,
    monkeypatch: pytest.MonkeyPatch,
    seam: str,
    error: BaseException,
) -> None:
    def fail(connection: sqlite3.Connection, values: tuple[object, ...]) -> None:
        raise error

    audit = _proposed(pipeline)
    monkeypatch.setattr(adapter_module, seam, fail)
    if isinstance(error, _Interrupted):
        with pytest.raises(_Interrupted):  # never swallowed into an outcome
            pipeline.recorder().execute(audit)
    else:
        result = pipeline.recorder().execute(audit)
        assert (result.outcome, result.attempt_recorded) == (_O.NOT_COMMITTED, False)
        assert not result.is_stored and result.authoritative is None
    assert counts(pipeline.database) == (0, 0)  # no disposition, proposal or attempt
    monkeypatch.undo()
    assert pipeline.recorder().execute(audit).outcome is _O.COMMITTED
    assert counts(pipeline.database) == (1, 1)
    check_database(pipeline.database)


def test_e17_a_real_constraint_violation_rolls_back_the_whole_write(
    pipeline: Pipeline, research: Research, monkeypatch: pytest.MonkeyPatch
) -> None:
    real = adapter_module._insert_attempt  # noqa: SLF001

    def violating(connection: sqlite3.Connection, values: tuple[object, ...]) -> None:
        bad = list(values)
        bad[adapter_module.ATTEMPT_COLUMNS.index("status")] = "ISSUED"  # no such status
        real(connection, tuple(bad))

    monkeypatch.setattr(adapter_module, "_insert_attempt", violating)
    authorization = pipeline.authorize(_proposed(pipeline))  # issued by the verified recorder
    with pytest.raises(EquityRecommendationGenerationNotCommittedError) as raised:
        pipeline.store().write_generation(authorization)
    assert isinstance(raised.value.__cause__, sqlite3.IntegrityError)  # the database refused
    assert counts(pipeline.database) == (0, 0)  # the inserted disposition was rolled back


def test_e17_attempt_only_failures_are_reported_never_claimed_as_audited(
    pipeline: Pipeline, research: Research, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fail(connection: sqlite3.Connection, values: tuple[object, ...]) -> None:
        raise sqlite3.OperationalError("test: disk full")

    audit = _attempt_only(pipeline)
    monkeypatch.setattr(adapter_module, "_insert_attempt", fail)
    result = pipeline.recorder().execute(audit)
    assert (result.outcome, result.attempt_recorded) == (_O.NOT_COMMITTED, False)
    assert counts(pipeline.database) == (0, 0)


def test_e17_a_proposal_never_exists_without_its_disposition(
    pipeline: Pipeline, research: Research
) -> None:
    proposed, _ = _records(_proposed(pipeline))
    pipeline.store().load_disposition(proposed.generation_key)  # creates the schema
    tables = {n for n, (kind, _) in schema_objects(pipeline.database).items() if kind == "table"}
    assert {t for t in tables if "recommendation" in t} == {DISPOSITIONS, ATTEMPTS}
    row = list(adapter_module.disposition_row(proposed))
    for column in ("proposal_document", "proposal_fingerprint", "proposal_format"):
        row[adapter_module.DISPOSITION_COLUMNS.index(column)] = None
    columns = adapter_module.DISPOSITION_COLUMNS
    with closing(raw(pipeline.database)) as connection, pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            f"INSERT INTO {DISPOSITIONS} ({', '.join(columns)}) "  # noqa: S608
            f"VALUES ({', '.join('?' * len(columns))})",
            tuple(row),
        )
    assert counts(pipeline.database) == (0, 0)


# ----------------------------------------------------------------------
# e18. Commit-unconfirmed recovery, through the real recorder and adapter
# ----------------------------------------------------------------------


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


class _CompetitorBeforeReload(SQLiteEquityRecommendationGenerationStore):
    """Test-only coordination: another writer commits before this store's first reload."""

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


def _competitor(pipeline: Pipeline, authorization: Any) -> Callable[[], None]:
    def commit() -> None:  # an independent adapter and connection, its own verified write
        written = SQLiteEquityRecommendationGenerationStore(pipeline.database).write_generation(
            authorization
        )
        assert written.outcome is _W.COMMITTED

    return commit


def test_e18_commit_did_not_occur_and_a_verified_absence_allows_a_bounded_retry(
    pipeline: Pipeline, research: Research, monkeypatch: pytest.MonkeyPatch
) -> None:
    _commit_once(monkeypatch, _not_committed)
    result = pipeline.recorder().execute(_proposed(pipeline))
    assert (result.outcome, result.writes, result.attempt_recorded) == (_O.COMMITTED, 2, True)
    assert counts(pipeline.database) == (1, 1)  # no duplicate disposition or attempt


def test_e18_a_lost_acknowledgment_is_reconciled_as_committed_by_reload(
    pipeline: Pipeline, research: Research, monkeypatch: pytest.MonkeyPatch
) -> None:
    _commit_once(monkeypatch, _acknowledgment_lost)
    result = pipeline.recorder().execute(_proposed(pipeline))
    assert (result.outcome, result.reconciled, result.writes) == (_O.EQUIVALENT_COMMITTED, True, 1)
    assert result.attempt_recorded is True
    assert counts(pipeline.database) == (1, 1)


@pytest.mark.parametrize("competing", ["equivalent", "different"])
def test_e18_another_writers_commit_found_on_reload_is_equivalent_or_a_conflict(
    pipeline: Pipeline, research: Research, monkeypatch: pytest.MonkeyPatch, competing: str
) -> None:
    proposed = _proposed(pipeline)
    their_audit = proposed if competing == "equivalent" else _unresolved(pipeline)
    theirs = _records(their_audit)
    competing_write = pipeline.authorize(their_audit)
    store = _CompetitorBeforeReload(pipeline.database, _competitor(pipeline, competing_write))
    _commit_once(monkeypatch, _not_committed)
    result = pipeline.recorder(store).execute(proposed)
    assert result.reconciled and result.writes == 1
    assert result.authoritative == theirs[0]
    if competing == "equivalent":
        assert (result.outcome, result.attempt_recorded) == (_O.EQUIVALENT_COMMITTED, True)
        assert counts(pipeline.database) == (1, 1)
    else:
        assert result.outcome is _O.CONTENT_CONFLICT and not result.is_stored
        assert result.attempt_recorded is False  # this write rolled back; never claimed
        assert rows(pipeline.database, DISPOSITIONS)[0]["disposition_kind"] == "UNRESOLVED"
        assert counts(pipeline.database) == (1, 1)


def test_e18_an_unavailable_reload_leaves_the_commit_unresolved(
    pipeline: Pipeline, research: Research, monkeypatch: pytest.MonkeyPatch
) -> None:
    audit = _proposed(pipeline)
    monkeypatch.setattr(adapter_module, "_BUSY_TIMEOUT_SECONDS", 0.05)
    holder = raw(pipeline.database)

    def lost_and_locked(real: _Commit, connection: sqlite3.Connection) -> None:
        real(connection)
        holder.execute("BEGIN EXCLUSIVE")  # no reader can open the database now
        raise sqlite3.OperationalError("test: acknowledgment lost after COMMIT")

    _commit_once(monkeypatch, lost_and_locked)
    try:
        result = pipeline.recorder().execute(audit)
    finally:
        holder.rollback()
        holder.close()
    assert result.outcome is _O.COMMIT_RECONCILIATION_UNRESOLVED and not result.is_stored
    assert (result.attempt_recorded, result.reconciled) == (None, True)
    assert counts(pipeline.database) == (1, 1)  # it had committed; nothing was claimed


def test_e18_an_invalid_record_found_on_reload_leaves_the_commit_unresolved(
    pipeline: Pipeline, research: Research, monkeypatch: pytest.MonkeyPatch
) -> None:
    audit = _proposed(pipeline)

    def lost_and_corrupted(real: _Commit, connection: sqlite3.Connection) -> None:
        real(connection)
        bypass(  # test-only: the stored row stops decoding before the reload
            pipeline.database,
            (
                f"UPDATE {DISPOSITIONS} SET assessment_fingerprint = ?",
                ("eira1-sha256:" + "0" * 64,),
            ),  # noqa: S608
            drop=(f"{DISPOSITIONS}_no_update",),
        )
        raise sqlite3.OperationalError("test: acknowledgment lost after COMMIT")

    _commit_once(monkeypatch, lost_and_corrupted)
    result = pipeline.recorder().execute(audit)
    assert result.outcome is _O.COMMIT_RECONCILIATION_UNRESOLVED
    assert (result.attempt_recorded, result.reconciled, result.writes) == (None, True, 1)
    assert counts(pipeline.database) == (1, 1)  # nothing retried, repaired or duplicated


@pytest.mark.parametrize(
    ("behaviour", "outcome", "writes"),
    [
        (_acknowledgment_lost, _O.EQUIVALENT_COMMITTED, 1),
        (_not_committed, _O.COMMITTED, 2),
    ],
    ids=["acknowledgment lost", "not committed"],
)
def test_e18_attempt_only_commit_uncertainty_reconciles_by_attempt_identity(
    pipeline: Pipeline,
    research: Research,
    monkeypatch: pytest.MonkeyPatch,
    behaviour: Any,
    outcome: Any,
    writes: int,
) -> None:
    audit = _attempt_only(pipeline)
    _commit_once(monkeypatch, behaviour)
    result = pipeline.recorder().execute(audit)
    assert (result.outcome, result.writes, result.attempt_recorded) == (outcome, writes, True)
    assert counts(pipeline.database) == (0, 1)


def test_e18_retries_are_bounded_and_never_duplicate(
    pipeline: Pipeline, research: Research, monkeypatch: pytest.MonkeyPatch
) -> None:
    def never(connection: sqlite3.Connection) -> None:
        raise sqlite3.OperationalError("test: COMMIT failed before it took effect")

    monkeypatch.setattr(adapter_module, "_commit", never)
    for audit in (_proposed(pipeline), _attempt_only(pipeline)):
        result = pipeline.recorder(max_writes=3).execute(audit)
        assert (result.outcome, result.writes) == (_O.RETRY_LIMIT_REACHED, 3)
        assert not result.is_stored
    assert counts(pipeline.database) == (0, 0)


# ----------------------------------------------------------------------
# e19. Concurrent writers: independent connections, threads and processes
# ----------------------------------------------------------------------


def _together(
    pipeline: Pipeline, audits: dict[str, EquityRecommendationEligibilityAudit]
) -> dict[str, Any]:
    """Record each audit on its own thread and connection, released together by a barrier."""
    start = threading.Barrier(len(audits))
    results: dict[str, Any] = {}

    def run(name: str) -> None:
        recorder = pipeline.recorder()  # its own adapters and connections
        start.wait(timeout=30)
        try:
            results[name] = recorder.execute(audits[name])
        except BaseException as exc:  # noqa: BLE001 -- reported below, never swallowed
            results[name] = exc

    threads = [threading.Thread(target=run, args=(name,)) for name in audits]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=60)
    assert not any(t.is_alive() for t in threads)
    assert not any(isinstance(r, BaseException) for r in results.values()), results
    check_database(pipeline.database)
    return results


def test_e19_two_writers_of_equivalent_content_store_it_once(
    pipeline: Pipeline, research: Research
) -> None:
    audit = _proposed(pipeline)
    results = _together(pipeline, {"one": audit, "two": audit})
    outcomes = sorted(r.outcome.value for r in results.values())
    assert outcomes == ["COMMITTED", "EQUIVALENT_COMMITTED"]
    assert all(r.attempt_recorded is True for r in results.values())
    assert counts(pipeline.database) == (1, 1)


def test_e19_two_conflicting_writers_leave_one_authoritative_disposition(
    pipeline: Pipeline, research: Research
) -> None:
    audits = {"proposed": _proposed(pipeline), "unresolved": _unresolved(pipeline)}
    results = _together(pipeline, audits)
    winner = next(name for name, r in results.items() if r.outcome is _O.COMMITTED)
    loser = next(name for name in audits if name != winner)
    assert results[loser].outcome is _O.CONTENT_CONFLICT
    assert results[loser].authoritative == equity_recommendation_disposition(audits[winner])
    assert results[loser].attempt_recorded is True  # the loser is audited
    (row,) = rows(pipeline.database, DISPOSITIONS)
    assert row["disposition_kind"] == {"proposed": "PROPOSED", "unresolved": "UNRESOLVED"}[winner]
    assert counts(pipeline.database) == (1, 2)


def test_e19_writers_of_distinct_policy_set_identities_both_commit(
    pipeline: Pipeline, research: Research
) -> None:
    audits = {"set/1": _proposed(pipeline), "set/strict": _not_eligible(pipeline)}
    results = _together(pipeline, audits)
    assert {r.outcome for r in results.values()} == {_O.COMMITTED}
    assert counts(pipeline.database) == (2, 2)


def test_e19_a_writer_that_cannot_take_the_lock_writes_nothing(
    pipeline: Pipeline, research: Research, monkeypatch: pytest.MonkeyPatch
) -> None:
    audit = _proposed(pipeline)
    pipeline.store().load_disposition(audit.generation_key)  # type: ignore[arg-type]
    monkeypatch.setattr(adapter_module, "_BUSY_TIMEOUT_SECONDS", 0.05)
    holder = raw(pipeline.database)
    try:
        holder.execute("BEGIN IMMEDIATE")  # another connection holds the write lock
        result = pipeline.recorder().execute(audit)
        assert (result.outcome, result.attempt_recorded) == (_O.NOT_COMMITTED, False)
    finally:
        holder.rollback()
        holder.close()
    assert counts(pipeline.database) == (0, 0)
    assert pipeline.recorder().execute(audit).outcome is _O.COMMITTED


@pytest.mark.parametrize("contest", ["equivalent", "conflicting"])
def test_e19_separate_os_processes_are_serialized_by_the_database(
    pipeline: Pipeline, research: Research, contest: str
) -> None:
    proposed = _proposed(pipeline)
    other = proposed if contest == "equivalent" else _unresolved(pipeline)
    offered = {"proposed": _records(proposed), "other": _records(other)}
    pipeline.store().load_disposition(proposed.generation_key)  # type: ignore[arg-type]
    context = multiprocessing.get_context("spawn")  # the Windows start method, everywhere
    start, results = context.Barrier(2), context.Queue()
    processes = [
        context.Process(
            target=record_in_another_process,
            args=(str(pipeline.database), documents(*records), start, results, name),
        )
        for name, records in offered.items()
    ]
    for process in processes:
        process.start()
    reported = {}
    for _ in processes:
        name, pid, outcome, attempt_recorded, error = results.get(timeout=180)
        reported[name] = (pid, outcome, attempt_recorded, error)
    for process in processes:
        process.join(timeout=60)
        assert process.exitcode == 0
    assert all(error is None for _, _, _, error in reported.values()), reported
    pids = {pid for pid, _, _, _ in reported.values()}
    assert len(pids) == 2 and os.getpid() not in pids  # two real, separate processes
    outcomes = sorted(outcome for _, outcome, _, _ in reported.values())
    if contest == "equivalent":
        assert outcomes == ["COMMITTED", "EQUIVALENT_COMMITTED"]
        assert counts(pipeline.database) == (1, 1)
    else:
        assert outcomes == ["COMMITTED", "CONTENT_CONFLICT"]
        winner = next(n for n, (_, o, _, _) in reported.items() if o == "COMMITTED")
        (row,) = rows(pipeline.database, DISPOSITIONS)
        assert row["assessment_fingerprint"] == offered[winner][0].assessment_fingerprint
        assert counts(pipeline.database) == (1, 2)
    assert all(recorded is True for _, _, recorded, _ in reported.values())
    check_database(pipeline.database)


# ----------------------------------------------------------------------
# e20. Tampering and immutable history
# ----------------------------------------------------------------------


@pytest.fixture
def three_kinds(pipeline: Pipeline, research: Research) -> dict[_K, _Records]:
    """One committed disposition of each kind, under three distinct policy-set identities."""
    stored = {}
    for kind, make in (
        (_K.UNRESOLVED, _unresolved),
        (_K.NOT_ELIGIBLE, _not_eligible),
        (_K.PROPOSED, lambda p: p.generate(set_version="set/2")),
    ):
        audit = make(pipeline)
        assert pipeline.recorder().execute(audit).outcome is _O.COMMITTED
        stored[kind] = _records(audit)
        assert stored[kind][0].kind is kind
    return stored


def _statements(research: Research, pipeline: Pipeline) -> list[str]:
    timeline = research.advance.timeline
    assert timeline is not None
    repeat = timeline.entries[2].fingerprint
    s46 = research.s46.snapshot_id
    return [
        f"UPDATE {DISPOSITIONS} SET disposition_kind = 'PROPOSED' "
        "WHERE disposition_kind = 'UNRESOLVED'",
        f"UPDATE {DISPOSITIONS} SET evaluator_version = '{_FUTURE_EVALUATOR}'",
        f"UPDATE {DISPOSITIONS} SET candidate_entry_fingerprint = '{repeat}'",
        f"UPDATE {DISPOSITIONS} SET evidence_snapshot_id = '{s46}'",
        f"UPDATE {DISPOSITIONS} SET assessment_document = assessment_document",
        f"UPDATE {DISPOSITIONS} SET proposal_document = NULL",
        f"DELETE FROM {DISPOSITIONS}",
        f"INSERT OR REPLACE INTO {DISPOSITIONS} SELECT * FROM {DISPOSITIONS}",
        f"REPLACE INTO {DISPOSITIONS} SELECT * FROM {DISPOSITIONS}",
        f"UPDATE {ATTEMPTS} SET status = 'PROPOSED'",
        f"DELETE FROM {ATTEMPTS}",
        f"INSERT OR REPLACE INTO {ATTEMPTS} SELECT * FROM {ATTEMPTS}",
    ]


def test_e20_direct_sql_cannot_change_replace_or_delete_a_generation_record(
    pipeline: Pipeline, research: Research, three_kinds: dict[_K, _Records]
) -> None:
    before = (rows(pipeline.database, DISPOSITIONS), rows(pipeline.database, ATTEMPTS))
    refused = []
    with closing(raw(pipeline.database)) as connection:
        for statement in _statements(research, pipeline):
            with pytest.raises(sqlite3.IntegrityError, match="insert-only"):
                connection.execute(statement)
            refused.append(statement)
    assert len(refused) == 12
    assert (rows(pipeline.database, DISPOSITIONS), rows(pipeline.database, ATTEMPTS)) == before
    kinds = sorted(r["disposition_kind"] for r in before[0])
    assert kinds == ["NOT_ELIGIBLE", "PROPOSED", "UNRESOLVED"]
    check_database(pipeline.database)


def _assessment_with(document: str, **changes: object) -> str:
    loaded = json.loads(document)
    loaded.update(changes)
    return json.dumps(loaded, sort_keys=True, separators=(",", ":"))


@pytest.mark.parametrize(
    ("case", "error", "outcome"),
    [
        ("invalid fingerprint", InvalidEquityRecommendationRecordError, _O.STORED_RECORD_INVALID),
        (
            "non-canonical document",
            InvalidEquityRecommendationRecordError,
            _O.STORED_RECORD_INVALID,
        ),
        (
            "unsupported format",
            IncompatibleEquityRecommendationRecordError,
            _O.STORED_RECORD_INCOMPATIBLE,
        ),
        (
            "unsupported evaluator",
            IncompatibleEquityRecommendationRecordError,
            _O.STORED_RECORD_INCOMPATIBLE,
        ),
        ("empty evaluator", InvalidEquityRecommendationRecordError, _O.STORED_RECORD_INVALID),
    ],
)
def test_e20_a_corrupted_stored_disposition_fails_closed_and_is_never_repaired(
    pipeline: Pipeline, research: Research, case: str, error: type[Exception], outcome: Any
) -> None:
    audit = _proposed(pipeline)
    disposition, _ = _records(audit)
    assert pipeline.recorder().execute(audit).outcome is _O.COMMITTED
    document = disposition.assessment_document
    column, value, checks = {
        "invalid fingerprint": ("assessment_fingerprint", "eira1-sha256:" + "0" * 64, True),
        "non-canonical document": (
            "assessment_document",
            json.dumps(json.loads(document), sort_keys=True, indent=1),
            True,
        ),
        "unsupported format": (
            "assessment_document",
            _assessment_with(document, format="northstar.equity-recommendation-assessment/2"),
            False,
        ),
        "unsupported evaluator": ("evaluator_version", _FUTURE_EVALUATOR, True),
        "empty evaluator": ("evaluator_version", "", False),
    }[case]
    bypass(  # TEST-ONLY corruption of the disposable database
        pipeline.database,
        (f"UPDATE {DISPOSITIONS} SET {column} = ?", (value,)),  # noqa: S608
        drop=(f"{DISPOSITIONS}_no_update",),
        checks=checks,
    )
    corrupted = rows(pipeline.database, DISPOSITIONS)
    with pytest.raises(error) as raised:
        pipeline.store().load_disposition(disposition.generation_key)
    if error is InvalidEquityRecommendationRecordError:
        assert not isinstance(raised.value, IncompatibleEquityRecommendationRecordError)
    with pytest.raises(error):
        pipeline.verify_stored().execute(disposition.generation_key)
    result = pipeline.recorder().execute(pipeline.generate())
    assert result.outcome is outcome and not result.is_stored
    assert result.attempt_recorded is None and result.authoritative is None
    assert rows(pipeline.database, DISPOSITIONS) == corrupted  # never repaired or replaced
    assert counts(pipeline.database) == (1, 1)


def test_e20_a_missing_source_is_a_structural_break_and_fails_verification(
    pipeline: Pipeline, research: Research
) -> None:
    audit = _proposed(pipeline)
    disposition, _ = _records(audit)
    assert pipeline.recorder().execute(audit).outcome is _O.COMMITTED
    snapshot = research.s41.snapshot_id
    bypass(  # TEST-ONLY: remove the CANDIDATE's snapshot with foreign keys off
        pipeline.database,
        ("DELETE FROM equity_candle_evidence_snapshot_entries WHERE snapshot_id = ?", (snapshot,)),
        ("DELETE FROM equity_candle_evidence_snapshots WHERE snapshot_id = ?", (snapshot,)),
        drop=(
            "equity_candle_evidence_snapshots_no_delete",
            "equity_candle_evidence_snapshot_entries_no_delete",
        ),
        foreign_keys=False,
    )
    with closing(raw(pipeline.database)) as connection:
        violations = {row[0] for row in connection.execute("PRAGMA foreign_key_check")}
    assert DISPOSITIONS in violations  # SQLite reports the break
    assert pipeline.store().load_disposition(disposition.generation_key) == disposition
    with pytest.raises(EquityRecommendationSourceVerificationError):
        pipeline.verify_stored().execute(disposition.generation_key)
    again = pipeline.generate()  # the research itself no longer re-verifies
    assert again.status is _S.RESEARCH_INTEGRITY_FAILURE and again.generation_key is None


def _forged(audit: EquityRecommendationEligibilityAudit, **candidate: object) -> _Records:
    """TEST-ONLY: an internally consistent record whose candidate evidence was altered.

    Built from public Application types: every document is canonical and every
    fingerprint re-derives, so only source verification can tell.
    """
    inputs = audit.inputs
    assert inputs is not None
    forged_inputs = EquityRecommendationInputs(
        candidate=replace(inputs.candidate, **candidate),  # type: ignore[arg-type]
        policy_set=inputs.policy_set,
        listing_eligibility=inputs.listing_eligibility,
        venue_day=inputs.venue_day,
        issued_history=inputs.issued_history,  # the same decision instant, so the same view
    )
    forged = assess_equity_recommendation_inputs(audit.request, forged_inputs)
    return _records(forged)


_CONTRADICTIONS = pytest.mark.parametrize(
    "field_value",
    [
        ("signal_close", Price(Decimal("253.55"), INR)),
        ("opening_range_low", Price(Decimal("249.5"), INR)),
    ],
    ids=["signal close", "opening-range low"],
)


@_CONTRADICTIONS
def test_e20_an_unauthorized_adapter_write_of_contradictory_evidence_is_refused(
    pipeline: Pipeline,
    research: Research,
    field_value: tuple[str, object],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # EI4-D11 (ADR-023), guarantee 1: the direct adapter write that EI-4E's AA-2
    # found committing this forgery is refused before SQLite is opened.
    audit = _proposed(pipeline)
    name, value = field_value
    forged, forged_attempt = _forged(audit, **{name: value})
    pipeline.store().load_disposition(forged.generation_key)  # creates the schema
    before = (rows(pipeline.database, DISPOSITIONS), rows(pipeline.database, ATTEMPTS))
    opened: list[object] = []
    real_connect = adapter_module._connect  # noqa: SLF001

    def connect(database_path: Path) -> sqlite3.Connection:
        opened.append(database_path)
        return real_connect(database_path)

    monkeypatch.setattr(adapter_module, "_connect", connect)
    with pytest.raises(UnverifiedEquityRecommendationGenerationWriteError):
        pipeline.store().record_generation(forged, forged_attempt)
    # Nor can the forgery obtain an authorization: the recorder refuses it.
    refused = pipeline.recorder().record(forged, forged_attempt)
    assert refused.outcome is _O.SOURCE_EVIDENCE_INVALID and refused.writes == 0
    assert opened == []  # no connection was ever opened to write it
    monkeypatch.setattr(adapter_module, "_connect", real_connect)
    assert (rows(pipeline.database, DISPOSITIONS), rows(pipeline.database, ATTEMPTS)) == before
    assert counts(pipeline.database) == (0, 0)


@_CONTRADICTIONS
def test_e20_contradictory_candidate_evidence_is_caught_by_the_application_verifier(
    pipeline: Pipeline, research: Research, field_value: tuple[str, object]
) -> None:
    audit = _proposed(pipeline)
    name, value = field_value
    forged, forged_attempt = _forged(audit, **{name: value})
    genuine, _ = _records(audit)
    assert forged.generation_key == genuine.generation_key  # same candidate references
    assert forged.assessment_fingerprint != genuine.assessment_fingerprint
    pipeline.store().load_disposition(forged.generation_key)  # creates the schema
    refused = pipeline.recorder().record(forged, forged_attempt)
    assert refused.outcome is _O.SOURCE_EVIDENCE_INVALID and refused.writes == 0
    assert counts(pipeline.database) == (0, 0)  # refused before any write
    # EI4-D11-W2-I: the adapter no longer accepts it (see the test above). This test
    # keeps EI-4E's AA-2 consequence: such evidence, once committed -- here planted
    # by TEST-ONLY LEGACY-EVIDENCE SETUP, standing for a write before enforcement or
    # by privileged access -- still fails closed. Every structural reference matches,
    # so SQLite accepts the rows; verification on reload is the Application's.
    plant_generation_records(pipeline.database, forged, forged_attempt)
    with pytest.raises(EquityRecommendationSourceVerificationError, match=name):
        pipeline.verify_stored().execute(forged.generation_key)
    forged_rows = rows(pipeline.database, DISPOSITIONS)
    # The genuine record now meets other content under its key that does not verify:
    # it is reported as invalid, never adopted as an authoritative conflict winner.
    genuine_result = pipeline.recorder().execute(audit)
    assert genuine_result.outcome is _O.STORED_RECORD_INVALID and not genuine_result.is_stored
    assert genuine_result.authoritative is None and name in genuine_result.detail
    assert genuine_result.attempt_recorded is True  # its attempt committed with the refusal
    assert rows(pipeline.database, DISPOSITIONS) == forged_rows  # never overwritten
    assert counts(pipeline.database) == (1, 2)
    check_database(pipeline.database)


# ----------------------------------------------------------------------
# e21. W1 regression on real SQLite
# ----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("corruption", "error", "outcome"),
    [
        ("reordered document", InvalidEquityRecommendationRecordError, _O.STORED_RECORD_INVALID),
        (
            "unsupported format",
            IncompatibleEquityRecommendationRecordError,
            _O.STORED_RECORD_INCOMPATIBLE,
        ),
    ],
)
def test_e21_a_corrupt_stored_attempt_is_typed_by_the_adapter_and_mapped_by_the_recorder(
    pipeline: Pipeline, research: Research, corruption: str, error: type[Exception], outcome: Any
) -> None:
    audit = _attempt_only(pipeline)
    attempt = equity_recommendation_attempt(audit)
    assert pipeline.recorder().execute(audit).outcome is _O.COMMITTED
    loaded = json.loads(attempt.attempt_document)
    if corruption == "reordered document":
        text, checks = json.dumps(loaded, sort_keys=True, indent=1), True
    else:
        loaded["format"] = "northstar.equity-recommendation-generation-attempt/2"
        text, checks = json.dumps(loaded, sort_keys=True, separators=(",", ":")), False
    bypass(  # TEST-ONLY corruption of the disposable database
        pipeline.database,
        (f"UPDATE {ATTEMPTS} SET attempt_document = ?", (text,)),  # noqa: S608
        drop=(f"{ATTEMPTS}_no_update",),
        checks=checks,
    )
    corrupted = rows(pipeline.database, ATTEMPTS)
    # The Infrastructure adapter raises the typed error, on read and on write.
    with pytest.raises(error) as raised:
        pipeline.store().load_attempt(attempt.attempt_identity)
    if error is InvalidEquityRecommendationRecordError:
        assert not isinstance(raised.value, IncompatibleEquityRecommendationRecordError)
    with pytest.raises(error):  # through the authorized path: the stored row still fails
        pipeline.store().write_attempt(pipeline.authorize(_attempt_only(pipeline)))
    # The Application recorder maps it: not stored, and the attempt's state is unknown.
    result = pipeline.recorder().execute(_attempt_only(pipeline))
    assert result.outcome is outcome
    assert not result.is_stored
    assert result.attempt_recorded is None
    assert rows(pipeline.database, ATTEMPTS) == corrupted  # never repaired or replaced
    assert counts(pipeline.database) == (0, 1)  # no other record created


def test_e21_a_corrupt_stored_disposition_leaves_the_attempt_state_unknown(
    pipeline: Pipeline, research: Research
) -> None:
    audit = _proposed(pipeline)
    assert pipeline.recorder().execute(audit).outcome is _O.COMMITTED
    bypass(  # TEST-ONLY
        pipeline.database,
        (f"UPDATE {DISPOSITIONS} SET assessment_fingerprint = ?", ("eira1-sha256:" + "1" * 64,)),  # noqa: S608
        drop=(f"{DISPOSITIONS}_no_update",),
    )
    result = pipeline.recorder().execute(_proposed(pipeline))
    assert (result.outcome, result.attempt_recorded, result.writes) == (
        _O.STORED_RECORD_INVALID,
        None,
        1,
    )
    assert counts(pipeline.database) == (1, 1)
