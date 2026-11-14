"""EI-6A: the durable SQLite issuance and declaration authority, on real SQLite.

Acceptance of the EI-6A SQLite Storage Design v1.0 (section 12): the EI-6A rows
of the EI-5 matrix at layer S -- CC-01 to CC-06, RR-03, the view part of RR-04
and the S parts of ID-02, ID-03, BD-01, HW-03 and HW-08 -- and the storage
scenarios SA-01 to SA-13. Test names carry the scenario identifiers.

Every success path is the real chain over one disposable SQLite file
(tests/equity_recommendation_issuance_fixtures.py): the EI-4E research
fixtures, the real declarer, EI-4C version-2 generation against the
authoritative history source, the EI-4D-A recorder, the real issuer, and the
adapter's authorized ``write_issuance`` / ``write_declaration``. No issuance
or declaration is inserted by raw SQL on a success path. Raw SQL appears only
where a test says so: refused direct writes, and TEST-ONLY tampering of a
disposable file. Separate OS processes use the ``spawn`` start method on
every platform.

Lifecycle persistence (EI-6B: RR-01, the state part of RR-04, RR-05, RR-06)
and monitoring (EI-6C) are not here. Nothing here opens a network connection
or touches a production database.
"""

from __future__ import annotations

import ast
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
    AuthoritativeEquitySessionRecommendationHistorySource,
    DeclareEquitySessionIssuanceCompletenessUseCase,
    EquityIssuedRecommendation,
    EquityRecommendationIssuanceRefusal,
    EquityRecommendationIssuanceRefusedError,
    EquityRecommendationIssuanceUnresolvedError,
    EquityRecommendationRecordingOutcome,
    IssueEquityTradeRecommendationUseCase,
    equity_issued_recommendation,
)
from northstar_application.ports import (
    EquityIssuanceDeclarationOutcome,
    EquityIssuanceWatermarkBeyondAuthorityError,
    EquityRecommendationHistoryUnavailableError,
    EquityRecommendationIssuanceOutcome,
    EquityRecommendationIssuanceReader,
    EquityRecommendationIssuanceUnavailableError,
    EquityRecommendationIssuanceWriter,
    InvalidEquityRecommendationIssuanceWriteError,
    InvalidStoredEquityRecommendationIssuanceAuthorityError,
    UnverifiedEquityRecommendationIssuanceWriteError,
)
from northstar_core.foundation.value_objects import PointInTime

import northstar_infrastructure
from northstar_infrastructure import persistence
from northstar_infrastructure.persistence import (
    SQLiteEquityRecommendationIssuanceAuthority,
    initialize_equity_recommendation_issuance_schema,
)
from northstar_infrastructure.persistence import (
    sqlite_equity_recommendation_issuance as adapter_module,
)
from tests.equity_recommendation_acceptance_fixtures import (
    DAY,
    DISPOSITIONS,
    NSE,
    VENUE,
    check_database,
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
    IssuanceWorld,
    T,
    authority,
    declare,
    declare_in_another_process,
    establish_infy_breakout,
    infy,
    issuance_world,
    issue,
    issue_in_another_process,
    verify_stored,
)

_I = EquityRecommendationIssuanceOutcome
_D = EquityIssuanceDeclarationOutcome
_R = EquityRecommendationIssuanceRefusal
_Integrity = InvalidStoredEquityRecommendationIssuanceAuthorityError
_REFERENCE = "northstar.equity-recommendation-issuance-authority/1"
_INFRA_ROOT = Path(__file__).resolve().parents[1]
_ISSUANCE_INSERT_TRIGGERS = tuple(
    f"{ISSUANCES}_{name}"
    for name in ("no_replace", "canonical_instant", "source_matches", "not_backdated")
)


@pytest.fixture(autouse=True)
def _refuse_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("EI-6A tests must not open network connections.")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)


@pytest.fixture
def world(tmp_path: Path) -> IssuanceWorld:
    return issuance_world(tmp_path / "ei6a.sqlite3")


@pytest.fixture
def declared(world: IssuanceWorld) -> IssuanceWorld:
    """Declared complete exactly through b (sequence 1)."""
    assert world.declare(B).outcome is _D.DECLARED
    return world


@pytest.fixture
def issued(declared: IssuanceWorld) -> tuple[IssuanceWorld, str, EquityIssuedRecommendation]:
    """One MODELED issuance of the TCS proposal (sequence 2)."""
    key = declared.proposed()
    result = declared.issue(key)
    assert (result.outcome, result.recording_sequence) == (_I.ISSUED, 2), result.detail
    return declared, key, result.issuance


# ----------------------------------------------------------------------
# Inspection and TEST-ONLY raw access to the disposable file
# ----------------------------------------------------------------------


def _ledger(database: Path) -> list[tuple[int, str]]:
    return [(r["recording_sequence"], r["record_kind"]) for r in rows(database, LEDGER)]


def _state(database: Path) -> tuple[Any, ...]:
    return tuple(rows(database, table) for table in (LEDGER, ISSUANCES, DECLARATIONS))


def _file_state(database: Path) -> tuple[str, int]:
    data = database.read_bytes()
    return hashlib.sha256(data).hexdigest(), len(data)


def _view(view: Any) -> Any:
    """A plain, comparable form of a session view (None stays None)."""
    if view is None:
        return None
    return [
        view.complete_through.value,
        view.recording_watermark,
        view.reference,
        [
            [r.recommendation_key, r.listing.symbol.value, r.issued_at.value, r.recording_sequence]
            for r in view.recommendations
        ],
    ]


def _views(database: Path) -> list[Any]:
    store = authority(database)
    position = len(_ledger(database))
    return [_view(store.session_view_at(NSE, DAY, w)) for w in range(position + 1)]


def _issued_record(world: IssuanceWorld, key: str) -> EquityIssuedRecommendation:
    disposition = world.pipeline.store().load_disposition(key)
    assert disposition is not None
    return equity_issued_recommendation(disposition)


def _raw_transaction(
    database: Path,
    *statements: tuple[str, tuple[object, ...]],
    drop: tuple[str, ...] = (),
    foreign_keys: bool = True,
    checks: bool = True,
) -> None:
    """TEST-ONLY raw SQL in ONE transaction; a refused statement rolls it all back.

    Dropped triggers stay dropped until the adapter's next connection
    recreates them. Never evidence of the supported write path.
    """
    with closing(sqlite3.connect(database, isolation_level=None)) as connection:
        connection.execute(f"PRAGMA foreign_keys = {'ON' if foreign_keys else 'OFF'}")
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


def _ledger_insert(sequence: object, kind: object) -> tuple[str, tuple[object, ...]]:
    return (adapter_module._INSERT_LEDGER, (sequence, kind))  # noqa: SLF001 -- test-only


def _issuance_insert(values: tuple[object, ...], sequence: object, kind: object = "ISSUANCE"):  # type: ignore[no-untyped-def]
    return (adapter_module._INSERT_ISSUANCE, (*values, sequence, kind))  # noqa: SLF001


def _declaration_insert(sequence: object, through: str, kind: object = "DECLARATION"):  # type: ignore[no-untyped-def]
    point = PointInTime(through)
    values = (NSE.value, DAY.isoformat(), point.value, adapter_module.instant_microseconds(point))
    return (adapter_module._INSERT_DECLARATION, (sequence, kind, *values))  # noqa: SLF001


@contextmanager
def _refused(match: str | None = None) -> Iterator[None]:
    with pytest.raises(sqlite3.IntegrityError, match=match):
        yield


# ----------------------------------------------------------------------
# SA-12: additive initialization in the EI-4D file; foreign files refused
# ----------------------------------------------------------------------

_EI6A_OBJECTS = {
    LEDGER,
    ISSUANCES,
    DECLARATIONS,
    f"{ISSUANCES}_by_session",
    *(f"{t}_no_{verb}" for t in (LEDGER, ISSUANCES, DECLARATIONS) for verb in ("update", "delete")),
    f"{LEDGER}_no_replace",
    f"{LEDGER}_next_sequence",
    f"{LEDGER}_previous_recorded",
    *_ISSUANCE_INSERT_TRIGGERS,
    f"{DECLARATIONS}_no_replace",
    f"{DECLARATIONS}_canonical_instant",
    f"{DECLARATIONS}_monotone",
}


def test_sa12_initialization_is_additive_idempotent_and_refuses_a_foreign_database(
    world: IssuanceWorld, tmp_path: Path
) -> None:
    database = world.database
    before_objects = schema_objects(database)
    before_rows = {t: rows(database, t) for t in (DISPOSITIONS, "equity_candle_evidence_snapshots")}
    with closing(raw(database)) as connection:
        version = connection.execute("PRAGMA user_version").fetchone()
        initialize_equity_recommendation_issuance_schema(connection)
        initialize_equity_recommendation_issuance_schema(connection)  # idempotent
        assert connection.execute("PRAGMA user_version").fetchone() == version  # no schema version
    after = schema_objects(database)
    assert set(after) - set(before_objects) == _EI6A_OBJECTS
    assert {name: after[name] for name in before_objects} == before_objects  # nothing altered
    assert {t: rows(database, t) for t in before_rows} == before_rows
    assert all("lifecycle" not in name for name in after)  # EI-6B is not here

    foreign = tmp_path / "foreign.sqlite3"
    with closing(sqlite3.connect(foreign)) as connection:
        with pytest.raises(sqlite3.OperationalError, match="no EI-4D generation records"):
            initialize_equity_recommendation_issuance_schema(connection)
    assert schema_objects(foreign) == {}
    with pytest.raises(EquityRecommendationIssuanceUnavailableError):
        authority(foreign).session_view(NSE, DAY)
    missing = tmp_path / "missing.sqlite3"
    with pytest.raises(EquityRecommendationIssuanceUnavailableError):
        authority(missing).session_view(NSE, DAY)
    assert not missing.exists()  # never creates a database


# ----------------------------------------------------------------------
# Issuance and declaration writes through the real use cases
# ----------------------------------------------------------------------


def test_an_issuance_is_durable_once_and_equivalent_retries_write_nothing(
    issued: tuple[IssuanceWorld, str, EquityIssuedRecommendation],
) -> None:
    world, key, issuance = issued
    assert _ledger(world.database) == [(1, "DECLARATION"), (2, "ISSUANCE")]
    (row,) = rows(world.database, ISSUANCES)
    assert row == dict(
        zip(
            (*adapter_module.ISSUANCE_COLUMNS, "recording_sequence", "record_kind"),
            (*adapter_module.issuance_row(issuance), 2, "ISSUANCE"),
            strict=True,
        )
    )
    assert row["issuance_kind"] == "MODELED" and row["issued_at"] == T.value
    before = _file_state(world.database)
    again = world.issue(key)
    assert (again.outcome, again.recording_sequence, again.writes) == (_I.EQUIVALENT, 2, 1)
    assert _file_state(world.database) == before  # nothing written, no sequence allocated
    store = authority(world.database)
    assert store.load_issuance(issuance.issuance_id) == again.authoritative
    assert (
        store.load_session_issuance(issuance.strategy, issuance.listing, DAY) == again.authoritative
    )
    assert store.load_issuance("eiris1-sha256:" + "0" * 64) is None
    check_database(world.database)


def test_another_policy_set_for_the_session_is_session_strategy_already_issued(
    declared: IssuanceWorld,
) -> None:
    first, second = declared.proposed(), declared.proposed(set_version="set/2")
    assert declared.issue(first).outcome is _I.ISSUED
    result = declared.issue(second)
    assert result.outcome is _I.SESSION_STRATEGY_ALREADY_ISSUED
    assert (
        result.authoritative is not None
        and result.authoritative.recommendation.generation_key == first
    )
    assert len(rows(declared.database, ISSUANCES)) == 1 and len(_ledger(declared.database)) == 2


def test_bd01_s_backdating_is_refused_by_the_writer_and_by_the_database(
    declared: IssuanceWorld,
) -> None:
    key = declared.proposed()
    assert declared.declare(ist(10, 30)).outcome is _D.DECLARED  # covers t = 09:43:30
    before = _state(declared.database)
    result = declared.issue(key)
    assert (result.outcome, result.authoritative) == (_I.BACKDATED_ISSUANCE, None)
    assert _state(declared.database) == before
    values = adapter_module.issuance_row(_issued_record(declared, key))
    with _refused("follow the declared completeness"):  # the writer bypassed
        _raw_transaction(
            declared.database, _ledger_insert(3, "ISSUANCE"), _issuance_insert(values, 3)
        )
    assert _state(declared.database) == before


def test_bd02_s_an_issuance_at_exactly_the_declared_instant_is_refused_by_the_database(
    declared: IssuanceWorld,
) -> None:
    key = declared.proposed()
    assert declared.declare(T).outcome is _D.DECLARED  # complete through exactly t
    assert declared.issue(key).outcome is _I.BACKDATED_ISSUANCE
    values = adapter_module.issuance_row(_issued_record(declared, key))
    with _refused("follow the declared completeness"):
        _raw_transaction(
            declared.database, _ledger_insert(3, "ISSUANCE"), _issuance_insert(values, 3)
        )
    assert rows(declared.database, ISSUANCES) == []


def test_an_identical_retry_stays_equivalent_after_completeness_advances(
    issued: tuple[IssuanceWorld, str, EquityIssuedRecommendation],
) -> None:
    world, key, _ = issued
    assert world.declare(ist(10, 30)).outcome is _D.DECLARED
    assert world.issue(key).outcome is _I.EQUIVALENT  # identity before backdating


def test_sa11_declarations_are_strictly_monotone_and_equal_retries_write_nothing(
    world: IssuanceWorld,
) -> None:
    first = world.declare(B)
    assert (first.outcome, first.recorded.recording_sequence) == (_D.DECLARED, 1)  # type: ignore[union-attr]
    before = _file_state(world.database)
    assert world.declare(B).outcome is _D.DECLARATION_EQUIVALENT
    regression = world.declare(ist(9, 30))
    assert regression.outcome is _D.COMPLETENESS_REGRESSION
    assert regression.recorded.declaration.complete_through == B  # type: ignore[union-attr]
    assert _file_state(world.database) == before  # neither allocates a sequence
    assert world.declare(ist(10, 0)).outcome is _D.DECLARED
    assert _ledger(world.database) == [(1, "DECLARATION"), (2, "DECLARATION")]
    for through in (ist(9, 30).value, ist(10, 0).value):  # lower, and equal, bypassing the writer
        with _refused():
            _raw_transaction(
                world.database, _ledger_insert(3, "DECLARATION"), _declaration_insert(3, through)
            )
    assert _ledger(world.database) == [(1, "DECLARATION"), (2, "DECLARATION")]


def test_sa13_chronology_uses_integer_microseconds_never_text(world: IssuanceWorld) -> None:
    later, earlier = "2026-08-19T04:30:00.5Z", "2026-08-19T04:30:00Z"
    assert earlier > later  # textually, the earlier instant sorts after the later one
    assert world.declare(PointInTime(later)).outcome is _D.DECLARED
    assert world.declare(PointInTime(earlier)).outcome is _D.COMPLETENESS_REGRESSION
    with _refused("exceed the declared completeness"):
        _raw_transaction(
            world.database, _ledger_insert(2, "DECLARATION"), _declaration_insert(2, earlier)
        )
    (row,) = rows(world.database, DECLARATIONS)
    assert row["complete_through_us"] == 1787113800500000  # exact, from text, without floats
    with _refused("canonical UTC text"):  # a trailing fractional zero is not canonical
        _raw_transaction(
            world.database,
            _ledger_insert(2, "DECLARATION"),
            (
                adapter_module._INSERT_DECLARATION,  # noqa: SLF001
                (
                    2,
                    "DECLARATION",
                    "NSE",
                    "2026-08-19",
                    "2026-08-19T04:40:00.50Z",
                    1787114400500000,
                ),
            ),
        )
    with _refused("canonical UTC text"):  # text and integer disagree by one microsecond
        _raw_transaction(
            world.database,
            _ledger_insert(2, "DECLARATION"),
            (
                adapter_module._INSERT_DECLARATION,  # noqa: SLF001
                (2, "DECLARATION", "NSE", "2026-08-19", "2026-08-19T04:40:00Z", 1787114400000001),
            ),
        )
    assert adapter_module.instant_microseconds(PointInTime("1969-12-31T23:59:59.5Z")) == -500000
    assert adapter_module.instant_microseconds(PointInTime("0001-01-01T00:00:00Z")) == (
        -62135596800000000
    )


def test_sa13_s_backdating_compares_integers_where_text_would_admit(
    declared: IssuanceWorld,
) -> None:
    key = declared.proposed()
    covering = "2026-08-19T04:13:30.5Z"  # 09:43:30.5 IST: after t, yet textually before it
    assert T.value > covering
    assert declared.declare(PointInTime(covering)).outcome is _D.DECLARED
    values = adapter_module.issuance_row(_issued_record(declared, key))
    with _refused("follow the declared completeness"):
        _raw_transaction(
            declared.database, _ledger_insert(3, "ISSUANCE"), _issuance_insert(values, 3)
        )
    assert declared.issue(key).outcome is _I.BACKDATED_ISSUANCE


# ----------------------------------------------------------------------
# SA-02: unauthorized writes are refused before any SQLite connection opens
# ----------------------------------------------------------------------


@pytest.fixture
def connections(monkeypatch: pytest.MonkeyPatch) -> list[Path]:
    """Every call of the adapter's real connection seam, ``_connect``."""
    opened: list[Path] = []
    real = adapter_module._connect  # noqa: SLF001

    def connect(database_path: Path) -> sqlite3.Connection:
        opened.append(database_path)
        return real(database_path)

    monkeypatch.setattr(adapter_module, "_connect", connect)
    return opened


class _Capture(SQLiteEquityRecommendationIssuanceAuthority):
    """TEST-ONLY: keeps the authorizations the real use cases issue; writes nothing."""

    def __init__(self, database: Path) -> None:
        super().__init__(database)
        self.authorizations: list[Any] = []

    def write_issuance(self, authorization):  # type: ignore[no-untyped-def]
        self.authorizations.append(authorization)
        raise EquityRecommendationIssuanceUnavailableError("test: captured; nothing written")

    def write_declaration(self, authorization):  # type: ignore[no-untyped-def]
        self.authorizations.append(authorization)
        raise EquityRecommendationIssuanceUnavailableError("test: captured; nothing written")


def _captured_issuance(world: IssuanceWorld, key: str) -> Any:
    capture = _Capture(world.database)
    use_case = IssueEquityTradeRecommendationUseCase(
        verify_stored(world.database), capture, capture
    )
    with pytest.raises(EquityRecommendationIssuanceRefusedError):
        use_case.execute(key)
    (authorization,) = capture.authorizations
    return authorization


def _captured_declaration(world: IssuanceWorld, through: PointInTime) -> Any:
    capture = _Capture(world.database)
    with pytest.raises(EquityRecommendationIssuanceUnavailableError):
        DeclareEquitySessionIssuanceCompletenessUseCase(VENUE, capture, capture).execute(
            NSE, DAY, through
        )
    (authorization,) = capture.authorizations
    return authorization


def _tamper(obj: object, name: str, value: object) -> None:
    """TEST-ONLY privileged tampering with a frozen record (outside the trust boundary)."""
    object.__setattr__(obj, name, value)


def test_sa02_unauthorized_writes_are_refused_before_sqlite_and_change_nothing(
    declared: IssuanceWorld, connections: list[Path]
) -> None:
    key = declared.proposed()
    store = authority(declared.database)
    genuine_issuance = _captured_issuance(declared, key)
    changed_issuance = _captured_issuance(declared, key)
    _tamper(changed_issuance.issuance, "modeled_exit_at", ist(15, 0))
    genuine_declaration = _captured_declaration(declared, ist(10, 0))
    changed_declaration = _captured_declaration(declared, ist(10, 0))
    _tamper(changed_declaration.declaration, "complete_through", ist(11, 0))
    before, opened = _file_state(declared.database), len(connections)
    for write, authorization in (
        (store.write_issuance, None),
        (store.write_issuance, object()),
        (store.write_issuance, genuine_declaration),
        (store.write_issuance, changed_issuance),
        (store.write_declaration, None),
        (store.write_declaration, genuine_issuance),
        (store.write_declaration, changed_declaration),
    ):
        with pytest.raises(UnverifiedEquityRecommendationIssuanceWriteError):
            write(authorization)
    assert len(connections) == opened, "a refused write opened a SQLite connection"
    assert _file_state(declared.database) == before
    # The captures were genuine: the same authorizations write normally.
    assert store.write_issuance(genuine_issuance).outcome is _I.ISSUED
    assert store.write_declaration(genuine_declaration).outcome is _D.DECLARED
    assert _ledger(declared.database) == [(1, "DECLARATION"), (2, "ISSUANCE"), (3, "DECLARATION")]


def test_a_verified_issuance_whose_disposition_is_not_in_this_file_is_refused_by_the_database(
    declared: IssuanceWorld, tmp_path: Path
) -> None:
    """Mis-wiring: the verifier read another file. The database's provenance guards refuse."""
    authorization = _captured_issuance(declared, declared.proposed())
    other = issuance_world(tmp_path / "another-authority.sqlite3")
    assert other.declare(B).outcome is _D.DECLARED
    before = _file_state(other.database)
    with pytest.raises(InvalidEquityRecommendationIssuanceWriteError, match="refused the write"):
        authority(other.database).write_issuance(authorization)
    assert _file_state(other.database) == before  # rolled back; nothing written
    assert _ledger(other.database) == [(1, "DECLARATION")]


# ----------------------------------------------------------------------
# SA-01, ID-03 (S): provenance and identity shape, enforced by the database
# ----------------------------------------------------------------------


def test_sa01_an_issuance_must_rest_on_its_proposed_evaluator2_disposition(
    declared: IssuanceWorld,
) -> None:
    key = declared.proposed()
    issued = _issued_record(declared, key)
    good = adapter_module.issuance_row(issued)
    not_eligible = declared.generate(generation="generation/strict", set_version="set/strict")
    assert declared.pipeline.recorder().execute(not_eligible).outcome is (
        EquityRecommendationRecordingOutcome.COMMITTED
    )
    version1 = declared.pipeline.generate(set_version="set/2")  # evaluator /1, synthetic history
    assert declared.pipeline.recorder().execute(version1).outcome is (
        EquityRecommendationRecordingOutcome.COMMITTED
    )
    v1 = declared.pipeline.store().load_disposition(version1.generation_key)  # type: ignore[arg-type]
    ne = declared.pipeline.store().load_disposition(not_eligible.generation_key)  # type: ignore[arg-type]
    assert v1 is not None and ne is not None
    assert v1.evaluator_version.endswith("/1") and ne.kind.value == "NOT_ELIGIBLE"

    def variant(**changes: object) -> tuple[object, ...]:
        columns = dict(zip(adapter_module.ISSUANCE_COLUMNS, good, strict=True))
        columns.update(changes)
        return tuple(columns.values())

    cases = {
        "missing disposition": variant(generation_key="eirk1-sha256:" + "9" * 64),
        "not PROPOSED": variant(
            generation_key=ne.generation_key, assessment_fingerprint=ne.assessment_fingerprint
        ),
        "evaluator /1": variant(
            generation_key=v1.generation_key,
            proposal_fingerprint=v1.proposal_fingerprint,
            assessment_fingerprint=v1.assessment_fingerprint,
        ),
        "other assessment": variant(assessment_fingerprint=ne.assessment_fingerprint),
        "other proposal": variant(proposal_fingerprint="eirp1-sha256:" + "3" * 64),
        "other strategy": variant(strategy="ei6a-another-strategy"),
        "other listing": variant(symbol="INFY"),
        "other date": variant(trading_date="2026-08-20"),
    }
    for name, values in cases.items():
        with _refused():
            _raw_transaction(
                declared.database, _ledger_insert(2, "ISSUANCE"), _issuance_insert(values, 2)
            )
        assert rows(declared.database, ISSUANCES) == [], name
    with _refused("FOREIGN KEY"):  # the foreign key alone, with the provenance trigger dropped
        _raw_transaction(
            declared.database,
            _ledger_insert(2, "ISSUANCE"),
            _issuance_insert(cases["missing disposition"], 2),
            drop=(f"{ISSUANCES}_source_matches",),
        )
    assert _ledger(declared.database) == [(1, "DECLARATION")]


def test_id03_s_the_identity_shape_is_enforced_by_the_database(
    declared: IssuanceWorld,
) -> None:
    key = declared.proposed()
    good = adapter_module.issuance_row(_issued_record(declared, key))
    assert str(good[0]).startswith("eiris1-sha256:")
    for identity in ("eiri1-sha256:" + "a" * 64, "eiris1-sha256:", "x"):
        with _refused("CHECK constraint failed"):
            _raw_transaction(
                declared.database,
                _ledger_insert(2, "ISSUANCE"),
                _issuance_insert((identity, *good[1:]), 2),
                drop=_ISSUANCE_INSERT_TRIGGERS,
            )
    with _refused("CHECK constraint failed"):  # MODELED only
        _raw_transaction(
            declared.database,
            _ledger_insert(2, "ISSUANCE"),
            _issuance_insert((*good[:4], "ACTUAL", *good[5:]), 2),
            drop=_ISSUANCE_INSERT_TRIGGERS,
        )
    assert rows(declared.database, ISSUANCES) == []
    assert declared.issue(key).outcome is _I.ISSUED  # the adapter restores its triggers


# ----------------------------------------------------------------------
# SA-03: append-only, no replacement
# ----------------------------------------------------------------------


def test_sa03_update_delete_and_replacement_are_refused_on_every_table(
    issued: tuple[IssuanceWorld, str, EquityIssuedRecommendation],
) -> None:
    world, _, issuance = issued
    before = _state(world.database)
    values = adapter_module.issuance_row(issuance)
    attempts = [
        (f"UPDATE {LEDGER} SET record_kind = 'DECLARATION' WHERE recording_sequence = 2", ()),
        (f"DELETE FROM {LEDGER}", ()),
        (f"INSERT OR REPLACE INTO {LEDGER} VALUES (1, 'DECLARATION')", ()),
        (f"UPDATE {ISSUANCES} SET issued_at_us = issued_at_us + 1", ()),
        (f"DELETE FROM {ISSUANCES}", ()),
        (
            f"INSERT OR REPLACE INTO {ISSUANCES} VALUES ({', '.join('?' * 13)})",
            (*values, 2, "ISSUANCE"),
        ),
        (f"UPDATE {DECLARATIONS} SET complete_through_us = 0", ()),
        (f"DELETE FROM {DECLARATIONS}", ()),
        (
            f"INSERT OR REPLACE INTO {DECLARATIONS} VALUES (?, ?, ?, ?, ?, ?)",
            _declaration_insert(1, B.value)[1],
        ),
    ]
    for statement in attempts:
        with _refused("insert-only|must be the next one|must exceed"):
            _raw_transaction(world.database, statement)
    assert _state(world.database) == before
    check_database(world.database)


# ----------------------------------------------------------------------
# SA-06: the database refuses invalid ledger states
# ----------------------------------------------------------------------


def test_sa06_the_database_refuses_duplicate_out_of_order_mismatched_and_unlinked_sequences(
    issued: tuple[IssuanceWorld, str, EquityIssuedRecommendation],
) -> None:
    world, _, issuance = issued
    database = world.database
    before = _state(database)
    with _refused():  # a duplicate sequence
        _raw_transaction(database, _ledger_insert(2, "DECLARATION"))
    with _refused("must be the next one|previous recording sequence"):  # out of order
        _raw_transaction(database, _ledger_insert(4, "DECLARATION"))
    with _refused("must be the next one"):  # the next-sequence trigger on its own
        _raw_transaction(
            database, _ledger_insert(4, "DECLARATION"), drop=(f"{LEDGER}_previous_recorded",)
        )
    with closing(raw(database)) as connection:  # restore the dropped trigger
        initialize_equity_recommendation_issuance_schema(connection)
    with _refused("CHECK constraint failed"):  # an unapproved kind
        _raw_transaction(database, _ledger_insert(3, "LIFECYCLE"))
    with _refused("FOREIGN KEY"):  # a declaration under an ISSUANCE ledger entry
        _raw_transaction(
            database, _ledger_insert(3, "ISSUANCE"), _declaration_insert(3, ist(10, 0).value)
        )
    with _refused("FOREIGN KEY"):  # a declaration with no ledger entry
        _raw_transaction(database, _declaration_insert(3, ist(10, 0).value))
    with _refused("CHECK constraint failed"):  # a record claiming another kind
        _raw_transaction(
            database,
            _ledger_insert(3, "DECLARATION"),
            _declaration_insert(3, ist(10, 0).value, kind="ISSUANCE"),
        )
    values = adapter_module.issuance_row(issuance)
    with _refused():  # the issued record again under a new sequence
        _raw_transaction(database, _ledger_insert(3, "ISSUANCE"), _issuance_insert(values, 3))
    assert _state(database) == before
    _raw_transaction(database, _ledger_insert(3, "DECLARATION"))  # TEST-ONLY: a committed orphan
    with _refused("previous recording sequence has no record"):
        _raw_transaction(
            database, _ledger_insert(4, "DECLARATION"), _declaration_insert(4, ist(11, 0).value)
        )


# ----------------------------------------------------------------------
# SA-07: planted inconsistent state fails closed, typed, and is never repaired
# ----------------------------------------------------------------------


def _every_read(
    database: Path, issuance_id: str, issuance: EquityIssuedRecommendation
) -> list[Any]:
    store = authority(database)
    return [
        lambda: store.session_view(NSE, DAY),
        lambda: store.session_view_at(NSE, DAY, 1),
        lambda: store.load_issuance(issuance_id),
        lambda: store.load_session_issuance(issuance.strategy, issuance.listing, DAY),
        lambda: AuthoritativeEquitySessionRecommendationHistorySource(store).session_history(
            NSE, DAY, B
        ),
    ]


_PLANTED: dict[str, Callable[[Path, tuple[object, ...]], None]] = {
    "orphan ledger entry": lambda db, _v: _raw_transaction(db, _ledger_insert(3, "ISSUANCE")),
    "ledger kind mismatch": lambda db, _v: _raw_transaction(
        db,
        (f"UPDATE {LEDGER} SET record_kind = 'DECLARATION' WHERE recording_sequence = 2", ()),
        drop=(f"{LEDGER}_no_update",),
        foreign_keys=False,
    ),
    "record without its ledger entry": lambda db, _v: _raw_transaction(
        db,
        (f"DELETE FROM {LEDGER} WHERE recording_sequence = 2", ()),
        drop=(f"{LEDGER}_no_delete",),
        foreign_keys=False,
    ),
    "gap in the sequence": lambda db, _v: _raw_transaction(
        db,
        (f"UPDATE {LEDGER} SET recording_sequence = 5 WHERE recording_sequence = 2", ()),
        (f"UPDATE {ISSUANCES} SET recording_sequence = 5", ()),
        drop=(f"{LEDGER}_no_update", f"{ISSUANCES}_no_update"),
        foreign_keys=False,
    ),
}


@pytest.mark.parametrize("case", sorted(_PLANTED))
def test_sa07_an_inconsistent_ledger_fails_every_read_and_write_closed(
    issued: tuple[IssuanceWorld, str, EquityIssuedRecommendation], case: str
) -> None:
    world, key, issuance = issued
    database = world.database
    _PLANTED[case](database, adapter_module.issuance_row(issuance))
    planted = _state(database)
    for read in _every_read(database, issuance.issuance_id, issuance):
        with pytest.raises(_Integrity) as raised:
            read()
        assert raised.value.category == "LEDGER" and raised.value.retryable is False
    with pytest.raises(_Integrity):
        declare(database, ist(10, 0))
    refused = pytest.raises(EquityRecommendationIssuanceRefusedError)
    with refused as caught:
        issue(database, key)
    assert (caught.value.reason, caught.value.retryable) == (_R.STORED_AUTHORITY_INVALID, False)
    with pytest.raises(_Integrity):  # generation over it raises and records no audit
        world.generate()
    assert _state(database) == planted  # never repaired


def test_sa07_a_corrupt_declaration_is_a_typed_declaration_failure(
    issued: tuple[IssuanceWorld, str, EquityIssuedRecommendation],
) -> None:
    world, key, issuance = issued
    _raw_transaction(
        world.database,
        (f"UPDATE {DECLARATIONS} SET complete_through_us = complete_through_us + 1", ()),
        drop=(f"{DECLARATIONS}_no_update",),
    )
    planted = _state(world.database)
    for read in _every_read(world.database, issuance.issuance_id, issuance)[:2]:
        with pytest.raises(_Integrity) as raised:
            read()
        assert raised.value.category == "DECLARATION"
    with pytest.raises(_Integrity) as raised:
        declare(world.database, ist(10, 0))
    assert raised.value.category == "DECLARATION"
    assert _state(world.database) == planted


# ----------------------------------------------------------------------
# SA-04, SA-05: a stored issuance that does not re-derive
# ----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("column", "change"),
    [
        ("issued_at", ("2026-08-19T04:13:31Z", 1787112811000000)),  # text and integer agree
        ("issued_at_us", (None, 1)),  # text and integer disagree
    ],
    ids=["another issued instant", "integer one microsecond off"],
)
def test_sa04_a_tampered_issuance_is_refused_typed_and_preserved(
    declared: IssuanceWorld, column: str, change: tuple[str | None, int]
) -> None:
    key = declared.proposed()
    authorization = _captured_issuance(declared, key)
    store = authority(declared.database)
    assert store.write_issuance(authorization).outcome is _I.ISSUED
    text, microseconds = change
    statement = (
        f"UPDATE {ISSUANCES} SET issued_at = ?, issued_at_us = ?"
        if text is not None
        else f"UPDATE {ISSUANCES} SET issued_at_us = issued_at_us + ?"
    )
    parameters = (text, microseconds) if text is not None else (microseconds,)
    _raw_transaction(declared.database, (statement, parameters), drop=(f"{ISSUANCES}_no_update",))
    tampered = _state(declared.database)
    issuance_id = authorization.issuance.issuance_id
    for read in _every_read(declared.database, issuance_id, authorization.issuance):
        with pytest.raises(_Integrity) as raised:
            read()
        assert (raised.value.category, raised.value.issuance_id) == ("ISSUANCE", issuance_id)
    written = store.write_issuance(authorization)  # the writer, under the offered identity
    assert (written.outcome, written.stored) == (_I.STORED_ISSUANCE_INVALID, None)
    with pytest.raises(EquityRecommendationIssuanceRefusedError) as refused:
        issue(declared.database, key)  # the issuer meets it reading the history first
    assert refused.value.reason is _R.STORED_AUTHORITY_INVALID
    assert _state(declared.database) == tampered  # preserved; never adopted or repaired


def test_sa05_an_undecodable_referenced_disposition_fails_loading_and_is_never_repaired(
    issued: tuple[IssuanceWorld, str, EquityIssuedRecommendation],
) -> None:
    world, key, issuance = issued
    _raw_transaction(
        world.database,
        (
            f"UPDATE {DISPOSITIONS} SET assessment_document = '{{}}' WHERE generation_key = ?",
            (key,),
        ),
        drop=("equity_recommendation_generation_dispositions_no_update",),
        checks=False,
    )
    tampered = _state(world.database), rows(world.database, DISPOSITIONS)
    with pytest.raises(_Integrity) as raised:
        authority(world.database).load_issuance(issuance.issuance_id)
    assert (raised.value.category, raised.value.issuance_id) == ("ISSUANCE", issuance.issuance_id)
    with pytest.raises(_Integrity):
        authority(world.database).session_view(NSE, DAY)
    assert (_state(world.database), rows(world.database, DISPOSITIONS)) == tampered


# ----------------------------------------------------------------------
# SA-08, SA-09, CC-05, CC-06: failures, lock contention and commit uncertainty
# ----------------------------------------------------------------------


@pytest.mark.parametrize(
    "failure",
    [RuntimeError("test: failure after the ledger insert"), sqlite3.OperationalError("disk I/O")],
    ids=["unexpected failure", "SQLite failure"],
)
def test_sa08_a_failure_after_allocation_commits_nothing_and_leaves_no_gap(
    declared: IssuanceWorld, monkeypatch: pytest.MonkeyPatch, failure: BaseException
) -> None:
    key = declared.proposed()
    before = _state(declared.database)
    seen: list[list[tuple[int, str]]] = []

    def fail(connection: sqlite3.Connection, sequence: int, table: str) -> None:
        seen.append(connection.execute(f"SELECT * FROM {LEDGER}").fetchall())  # noqa: S608
        raise failure

    monkeypatch.setattr(adapter_module, "_confirm_recorded", fail)
    if isinstance(failure, sqlite3.Error):
        with pytest.raises(EquityRecommendationIssuanceRefusedError) as refused:
            declared.issue(key)
        assert refused.value.reason is _R.AUTHORITY_UNAVAILABLE
    else:
        with pytest.raises(RuntimeError):
            declared.issue(key)
    assert seen == [[(1, "DECLARATION"), (2, "ISSUANCE")]]  # allocated inside the transaction
    assert _state(declared.database) == before  # ... and rolled back with it
    monkeypatch.undo()
    assert declared.issue(key).recording_sequence == 2  # the same next number; no gap


def test_cc05_a_write_lock_not_acquired_writes_nothing_and_is_typed_unavailable(
    declared: IssuanceWorld, monkeypatch: pytest.MonkeyPatch
) -> None:
    key = declared.proposed()
    before = _state(declared.database)
    monkeypatch.setattr(adapter_module, "_BUSY_TIMEOUT_SECONDS", 0.05)
    holder = raw(declared.database)
    try:
        holder.execute("BEGIN IMMEDIATE")  # another writer holds the lock
        with pytest.raises(EquityRecommendationIssuanceRefusedError) as refused:
            declared.issue(key)
        assert (refused.value.reason, refused.value.retryable) == (_R.AUTHORITY_UNAVAILABLE, True)
        with pytest.raises(EquityRecommendationIssuanceUnavailableError):
            declared.declare(ist(10, 0))
        assert _view(authority(declared.database).session_view(NSE, DAY)) is not None  # reads go on
    finally:
        holder.execute("ROLLBACK")
        holder.close()
    assert _state(declared.database) == before
    assert declared.issue(key).outcome is _I.ISSUED


def _lost_acknowledgment(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    real, lost = adapter_module._commit, []  # noqa: SLF001

    def commit(connection: sqlite3.Connection) -> None:
        real(connection)
        lost.append(1)
        raise sqlite3.OperationalError("test: COMMIT acknowledgment lost")

    monkeypatch.setattr(adapter_module, "_commit", commit)
    return lost


def test_cc06_a_commit_that_raises_after_taking_effect_reconciles_by_reload(
    declared: IssuanceWorld, monkeypatch: pytest.MonkeyPatch
) -> None:
    key = declared.proposed()
    lost = _lost_acknowledgment(monkeypatch)
    result = declared.issue(key)
    assert (result.outcome, result.writes, result.reconciled) == (_I.EQUIVALENT, 1, True)
    assert result.recording_sequence == 2 and lost == [1]
    declaration = declared.declare(ist(10, 0))
    assert (declaration.outcome, declaration.reconciled) == (_D.DECLARATION_EQUIVALENT, True)
    monkeypatch.undo()
    assert _ledger(declared.database) == [(1, "DECLARATION"), (2, "ISSUANCE"), (3, "DECLARATION")]
    check_database(declared.database)


def test_sa09_commit_uncertainty_is_bounded_and_claims_nothing(
    declared: IssuanceWorld, monkeypatch: pytest.MonkeyPatch
) -> None:
    key = declared.proposed()
    before = _state(declared.database)

    def lost(connection: sqlite3.Connection) -> None:
        raise sqlite3.OperationalError("test: COMMIT lost before it took effect")

    monkeypatch.setattr(adapter_module, "_commit", lost)
    with pytest.raises(EquityRecommendationIssuanceUnresolvedError):
        declared.issue(key)  # three writes, each reload finds nothing: bounded
    assert _state(declared.database) == before

    real_connect, failed = adapter_module._connect, []  # noqa: SLF001

    def lost_then_unreadable(connection: sqlite3.Connection) -> None:
        failed.append(1)
        raise sqlite3.OperationalError("test: COMMIT lost")

    def connect(database_path: Path) -> sqlite3.Connection:
        if failed:
            raise EquityRecommendationIssuanceUnavailableError("test: the reload cannot open")
        return real_connect(database_path)

    monkeypatch.setattr(adapter_module, "_commit", lost_then_unreadable)
    monkeypatch.setattr(adapter_module, "_connect", connect)
    with pytest.raises(EquityRecommendationIssuanceUnresolvedError):
        declared.issue(key)
    monkeypatch.undo()
    assert _state(declared.database) == before


def test_corruption_met_reconciling_an_unconfirmed_commit_is_never_adopted(
    declared: IssuanceWorld, monkeypatch: pytest.MonkeyPatch
) -> None:
    key = declared.proposed()
    real = adapter_module._commit  # noqa: SLF001

    def commit_then_corrupt(connection: sqlite3.Connection) -> None:
        real(connection)
        _raw_transaction(declared.database, _ledger_insert(3, "DECLARATION"))  # TEST-ONLY orphan
        raise sqlite3.OperationalError("test: COMMIT acknowledgment lost")

    monkeypatch.setattr(adapter_module, "_commit", commit_then_corrupt)
    with pytest.raises(_Integrity) as raised:
        declared.issue(key)
    assert raised.value.category == "LEDGER"  # permanent; whether it committed is not claimed


# ----------------------------------------------------------------------
# SA-10, HW-08 (S), RR-03: views, restart and an unreadable authority
# ----------------------------------------------------------------------


def test_sa10_view_semantics_on_sqlite(world: IssuanceWorld) -> None:
    store = authority(world.database)
    assert store.session_view(NSE, DAY) is None  # undeclared: unknown, never empty
    assert store.session_view_at(NSE, DAY, 0) is None
    with pytest.raises(EquityIssuanceWatermarkBeyondAuthorityError):
        store.session_view_at(NSE, DAY, 1)  # durable HW-07 evidence
    assert world.declare(B).outcome is _D.DECLARED
    view = store.session_view(NSE, DAY)
    assert _view(view) == [B.value, 1, _REFERENCE, []]  # declared, nothing visible: empty
    assert store.session_view_at(NSE, DAY, 0) is None
    with pytest.raises(EquityIssuanceWatermarkBeyondAuthorityError):
        store.session_view_at(NSE, DAY, 2)
    with pytest.raises(ValueError):
        store.session_view_at(NSE, DAY, -1)
    with pytest.raises(TypeError):
        store.session_view_at(NSE, DAY, True)  # type: ignore[arg-type]
    before = _file_state(world.database)
    _views(world.database)
    assert _file_state(world.database) == before  # reads change nothing


def test_hw08_s_an_unreadable_authority_is_history_unavailable(tmp_path: Path) -> None:
    garbage = tmp_path / "not-a-database.sqlite3"
    garbage.write_bytes(b"not a sqlite database, just bytes" * 64)
    for path in (garbage, tmp_path / "absent.sqlite3"):
        source = AuthoritativeEquitySessionRecommendationHistorySource(authority(path))
        with pytest.raises(EquityRecommendationHistoryUnavailableError):
            source.session_history(NSE, DAY, B)
    assert not (tmp_path / "absent.sqlite3").exists()


def _hw03_session(world: IssuanceWorld) -> dict[str, Any]:
    """The EI5-D4 example on SQLite: A and B issued, then a declaration covering both.

    A is TCS and B is INFY, both at t (09:43:30), so neither is in the other's
    assessment. The declaration at sequence 4 is complete through 10:00 and
    lists both. A later issuance C is not constructible here: once A and B are
    issued, EI4-D8 refuses every later same-session proposal
    (EXPOSURE_POLICY_UNRESOLVED), so later recording is a further declaration.
    """
    assert world.declare(B).outcome is _D.DECLARED
    a = world.issue(world.proposed())
    assert (a.outcome, a.recording_sequence) == (_I.ISSUED, 2)
    establish_infy_breakout(world)
    b = world.issue(world.proposed(**infy()))
    assert (b.outcome, b.recording_sequence) == (_I.ISSUED, 3), b.detail
    assert world.declare(ist(10, 0)).outcome is _D.DECLARED
    return {"a": a.issuance, "b": b.issuance}


def test_hw03_s_a_view_keeps_its_content_after_later_recording(world: IssuanceWorld) -> None:
    session = _hw03_session(world)
    a, b = session["a"], session["b"]
    listed = sorted(
        ([a.issuance_id, "TCS", T.value, 2], [b.issuance_id, "INFY", T.value, 3]),
        key=lambda record: record[0],  # (issued_at, key) order; one instant
    )
    store = authority(world.database)
    at_4 = _view(store.session_view_at(NSE, DAY, 4))
    assert at_4 == [ist(10, 0).value, 4, _REFERENCE, listed]
    assert _view(store.session_view_at(NSE, DAY, 3)) == [B.value, 3, _REFERENCE, []]
    later = world.declare(ist(10, 30))
    assert later.outcome is _D.DECLARED
    assert later.recorded is not None and later.recorded.recording_sequence == 5
    assert _view(store.session_view_at(NSE, DAY, 4)) == at_4  # unchanged after later recording
    assert _view(store.session_view(NSE, DAY)) == [ist(10, 30).value, 5, _REFERENCE, listed]
    check_database(world.database)


_FRESH_PROCESS = """
import json, sys
from pathlib import Path
from northstar_core.foundation.value_objects import ExchangeCode
from datetime import date
from northstar_infrastructure.persistence import SQLiteEquityRecommendationIssuanceAuthority
store = SQLiteEquityRecommendationIssuanceAuthority(Path(sys.argv[1]))
nse, day = ExchangeCode("NSE"), date(2026, 8, 19)
def plain(view):
    if view is None:
        return None
    return [view.complete_through.value, view.recording_watermark, view.reference,
            [[r.recommendation_key, r.listing.symbol.value, r.issued_at.value,
              r.recording_sequence] for r in view.recommendations]]
out = {"views": [plain(store.session_view_at(nse, day, w)) for w in range(int(sys.argv[2]) + 1)],
       "current": plain(store.session_view(nse, day)),
       "issuances": {}}
for issuance_id in json.loads(sys.argv[3]):
    found = store.load_issuance(issuance_id)
    out["issuances"][issuance_id] = [found.recommendation.issuance_id, found.recording_sequence,
                                     found.recommendation.content()]
print(json.dumps(out, sort_keys=True))
"""


def test_rr03_rr04_id02_views_and_identities_reproduce_after_restart_and_in_a_fresh_process(
    world: IssuanceWorld,
) -> None:
    session = _hw03_session(world)
    views = _views(world.database)
    issued = {i.issuance_id: i for i in (session["a"], session["b"])}
    before = _file_state(world.database)
    # RR-03: new adapter instances, as after a restart.
    assert _views(world.database) == views
    assert _view(authority(world.database).session_view(NSE, DAY)) == views[-1]
    # RR-04 (views), ID-02 (S): a separate OS process, from the file alone.
    completed = subprocess.run(  # the test's own interpreter, running fixed code only
        [
            sys.executable,
            "-c",
            _FRESH_PROCESS,
            str(world.database),
            str(len(views) - 1),
            json.dumps(sorted(issued)),
        ],
        cwd=_INFRA_ROOT,
        env=dict(os.environ, PYTHONHASHSEED="4242"),
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    fresh = json.loads(completed.stdout)
    assert fresh["views"] == views and fresh["current"] == views[-1]
    for issuance_id, issuance in issued.items():
        stored = authority(world.database).load_issuance(issuance_id)
        assert stored is not None
        assert fresh["issuances"][issuance_id] == [
            issuance.issuance_id,
            stored.recording_sequence,
            issuance.content(),
        ]
    assert _file_state(world.database) == before  # nothing repaired or migrated on read


# ----------------------------------------------------------------------
# CC-01 to CC-04: separate OS processes on one file
# ----------------------------------------------------------------------


def _race(*workers: tuple[Callable[..., None], Path, str, str]) -> dict[str, tuple[Any, ...]]:
    """Run each (target, database, payload, name) in a spawned process, released together."""
    context = multiprocessing.get_context("spawn")  # the Windows start method, everywhere
    start, results = context.Barrier(len(workers)), context.Queue()
    processes = [
        context.Process(target=target, args=(str(database), payload, start, results, name))
        for target, database, payload, name in workers
    ]
    for process in processes:
        process.start()
    reported = {}
    for _ in processes:
        name, pid, outcome, sequence, error = results.get(timeout=240)
        reported[name] = (pid, outcome, sequence, error)
    for process in processes:
        process.join(timeout=60)
        assert process.exitcode == 0
    assert all(error is None for *_, error in reported.values()), reported
    assert len({pid for pid, *_ in reported.values()} | {os.getpid()}) == len(workers) + 1
    return reported


def test_cc01_two_processes_issue_the_same_disposition_one_row_the_other_equivalent(
    declared: IssuanceWorld,
) -> None:
    key = declared.proposed()
    reported = _race(
        (issue_in_another_process, declared.database, key, "first"),
        (issue_in_another_process, declared.database, key, "second"),
    )
    assert sorted(outcome for _, outcome, _, _ in reported.values()) == ["EQUIVALENT", "ISSUED"]
    assert {sequence for _, _, sequence, _ in reported.values()} == {2}
    assert len(rows(declared.database, ISSUANCES)) == 1
    # And a retry from yet another process, after both have exited: still equivalent.
    (retry,) = _race((issue_in_another_process, declared.database, key, "retry")).values()
    assert retry[1:3] == ("EQUIVALENT", 2)
    check_database(declared.database)


def test_cc02_two_processes_issue_different_policy_sets_for_one_session_exactly_one_issued(
    declared: IssuanceWorld,
) -> None:
    keys = {"set/1": declared.proposed(), "set/2": declared.proposed(set_version="set/2")}
    reported = _race(
        *((issue_in_another_process, declared.database, key, name) for name, key in keys.items())
    )
    outcomes = sorted(outcome for _, outcome, _, _ in reported.values())
    assert outcomes == ["ISSUED", "SESSION_STRATEGY_ALREADY_ISSUED"]
    winner = next(name for name, (_, outcome, _, _) in reported.items() if outcome == "ISSUED")
    (row,) = rows(declared.database, ISSUANCES)
    assert row["generation_key"] == keys[winner]  # the first committed record stays authoritative
    assert _ledger(declared.database) == [(1, "DECLARATION"), (2, "ISSUANCE")]


def test_cc03_an_issuance_racing_a_declaration_is_listed_or_refused_never_hidden(
    declared: IssuanceWorld,
) -> None:
    key = declared.proposed()
    covering = ist(10, 0)
    reported = _race(
        (issue_in_another_process, declared.database, key, "issuance"),
        (declare_in_another_process, declared.database, covering.value, "declaration"),
    )
    assert reported["declaration"][1] == "DECLARED"
    view = _view(authority(declared.database).session_view(NSE, DAY))
    if reported["issuance"][1] == "ISSUED":
        assert reported["issuance"][2] < reported["declaration"][2]
        assert [r[0] for r in view[3]] == [_issued_record(declared, key).issuance_id]
    else:
        assert reported["issuance"][1] == "BACKDATED_ISSUANCE"
        assert rows(declared.database, ISSUANCES) == [] and view[3] == []
    assert view[0] == covering.value
    check_database(declared.database)


@pytest.mark.parametrize("order", ["issuance first", "declaration first"])
def test_cc03_both_orderings_deterministically(declared: IssuanceWorld, order: str) -> None:
    key = declared.proposed()
    if order == "issuance first":
        assert declared.issue(key).outcome is _I.ISSUED
        assert declared.declare(ist(10, 0)).outcome is _D.DECLARED
        assert len(_view(authority(declared.database).session_view(NSE, DAY))[3]) == 1
    else:
        assert declared.declare(ist(10, 0)).outcome is _D.DECLARED
        assert declared.issue(key).outcome is _I.BACKDATED_ISSUANCE
        assert _view(authority(declared.database).session_view(NSE, DAY))[3] == []


def test_cc04_concurrent_declarations_stay_monotone(world: IssuanceWorld) -> None:
    lower, higher = ist(10, 0), ist(10, 30)
    reported = _race(
        (declare_in_another_process, world.database, lower.value, "lower"),
        (declare_in_another_process, world.database, higher.value, "higher"),
    )
    assert reported["higher"][1] == "DECLARED"
    assert reported["lower"][1] in {"DECLARED", "COMPLETENESS_REGRESSION"}
    stored = rows(world.database, DECLARATIONS)
    instants = [r["complete_through_us"] for r in stored]
    assert instants == sorted(instants) and len(set(instants)) == len(instants)
    assert stored[-1]["complete_through"] == higher.value
    assert _ledger(world.database) == [(i + 1, "DECLARATION") for i in range(len(stored))]
    if reported["lower"][1] == "DECLARED":
        assert reported["lower"][2] < reported["higher"][2]


# ----------------------------------------------------------------------
# AR: the adapter's surface and the write boundary
# ----------------------------------------------------------------------

_PORT_METHODS = {
    "load_issuance",
    "load_session_issuance",
    "session_view",
    "session_view_at",
    "write_issuance",
    "write_declaration",
}
_VALIDATORS = {
    "write_issuance": "require_verified_equity_recommendation_issuance",
    "write_declaration": "require_verified_equity_issuance_completeness_declaration",
}


def test_ar_the_adapter_exposes_exactly_the_port_operations() -> None:
    adapter = SQLiteEquityRecommendationIssuanceAuthority
    assert issubclass(adapter, EquityRecommendationIssuanceReader)
    assert issubclass(adapter, EquityRecommendationIssuanceWriter)
    public = {name for name in vars(adapter) if not name.startswith("_")}
    assert public == _PORT_METHODS
    assert list(inspect.signature(adapter).parameters) == ["database_path"]
    assert "SQLiteEquityRecommendationIssuanceAuthority" in persistence.__all__


@pytest.mark.parametrize("entry", sorted(_VALIDATORS))
def test_ar_each_write_entry_validates_first(entry: str) -> None:
    tree = ast.parse(inspect.getsource(adapter_module))
    (adapter,) = [n for n in tree.body if isinstance(n, ast.ClassDef)]
    (method,) = [n for n in adapter.body if isinstance(n, ast.FunctionDef) and n.name == entry]
    body = [
        s
        for s in method.body
        if not (isinstance(s, ast.Expr) and isinstance(s.value, ast.Constant))
    ]
    first = ast.unparse(body[0])
    assert _VALIDATORS[entry] in first, first


def test_ar_production_source_constructs_the_adapter_nowhere_and_reaches_no_execution() -> None:
    root = Path(northstar_infrastructure.__file__).parent
    offenders = []
    for path in sorted(root.rglob("*.py")):
        relative = path.relative_to(root).as_posix()
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and getattr(node.func, "id", None) == (
                "SQLiteEquityRecommendationIssuanceAuthority"
            ):
                offenders.append(f"{relative}: constructs the adapter (no composition root)")
        if relative.startswith("persistence/sqlite_equity_recommendation_issuance"):
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom | ast.Import):
                    modules = (
                        [node.module or ""]
                        if isinstance(node, ast.ImportFrom)
                        else [a.name for a in node.names]
                    )
                    for module in modules:
                        if any(
                            w in module for w in ("broker", "execution", "paper", "http", "socket")
                        ):
                            offenders.append(f"{relative}: imports {module}")
    assert offenders == []
