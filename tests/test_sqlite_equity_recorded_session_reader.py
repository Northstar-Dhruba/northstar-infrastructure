"""EI-7D: bounded recorded-session discovery over the real SQLite issuance authority.

Acceptance of ``SQLiteReadOnlyEquityRecommendationIssuanceReader`` against the EI-7
Decision Register, section 13.2 (EI7-CF1, EI7-P1, EI7-P2, EI7-P6, EI7-P7,
EI7-FC1): one read transaction, ledger first, W never advanced, declarations
at W, the bounded count through the session index, overflow before any
re-derivation, every listed issuance verified, ascending recording sequence.
Transaction consistency and its limits (section 15 of the EI-7D instruction)
are proven with real concurrent connections.

The session world (tests/equity_read_only_fixtures.py) holds sequence 1, a
declaration through b; sequences 2 to 4, three issuances at t; sequence 5, a
declaration covering them. The crowded world holds sequence 1, a declaration,
and sequences 2 to 52, fifty-one genuine issuances of one session -- so W = 51
lists exactly fifty and N = 52 lists fifty-one. Every record is written by the
accepted chain; raw SQL appears only for labelled TEST-ONLY corruption and
concurrent writers. Nothing here opens a network connection or touches a
production database.
"""

from __future__ import annotations

import socket
import sqlite3
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import replace
from datetime import date
from pathlib import Path
from typing import Any

import pytest
from northstar_application.application_services import (
    EQUITY_RECOMMENDATION_ISSUANCE_AUTHORITY_REFERENCE,
    EquityReadQueryRefusal,
    EquityReadQueryRefusedError,
    EquitySessionDeclarationState,
    ListEquityDeclaredSessionRecommendationsUseCase,
    ListEquityRecordedSessionIssuancesUseCase,
    equity_issuance_session_view,
)
from northstar_application.ports import (
    EQUITY_RECORDED_SESSION_DISCOVERY_LIMIT,
    EquityIssuanceWatermarkBeyondAuthorityError,
    EquityRecommendationIssuanceUnavailableError,
    EquityRecordedSessionDiscoveryLimitExceededError,
    EquityRecordedSessionIssuanceReader,
    EquityRecordedSessionIssuances,
    InvalidStoredEquityRecommendationIssuanceAuthorityError,
)
from northstar_core.foundation.value_objects import ExchangeCode

from northstar_infrastructure.persistence import (
    SQLiteEquityRecommendationIssuanceAuthority,
    SQLiteReadOnlyEquityRecommendationIssuanceReader,
)
from northstar_infrastructure.persistence import sqlite_equity_read_only as connection_module
from northstar_infrastructure.persistence import (
    sqlite_equity_recommendation_issuance as issuance_module,
)
from northstar_infrastructure.persistence import (
    sqlite_equity_recommendation_read_only as reader_module,
)
from tests.equity_lifecycle_monitoring_fixtures import copy_world
from tests.equity_read_only_fixtures import (
    LATER_DECLARATION,
    SessionWorld,
    base_world,
    copy_session,
    corrupt,
    file_state,
    issue_all,
    propose_strategies,
    schema_state,
    session_world,
)
from tests.equity_recommendation_acceptance_fixtures import DAY, NSE, ist, raw
from tests.equity_recommendation_issuance_fixtures import DECLARATIONS, ISSUANCES, LEDGER, B
from tests.equity_recommendation_lifecycle_fixtures import LifecycleWorld

LIMIT = EQUITY_RECORDED_SESSION_DISCOVERY_LIMIT
_Integrity = InvalidStoredEquityRecommendationIssuanceAuthorityError
_R = EquityReadQueryRefusal
_OTHER_DAY = date(2026, 8, 20)


@pytest.fixture(autouse=True)
def _refuse_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("EI-7D tests must not open network connections.")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)


@pytest.fixture(scope="module")
def session_template(tmp_path_factory: pytest.TempPathFactory) -> SessionWorld:
    return session_world(tmp_path_factory.mktemp("ei7d-session") / "template.sqlite3")


@pytest.fixture
def session(session_template: SessionWorld, tmp_path: Path) -> SessionWorld:
    return copy_session(session_template, tmp_path / "session.sqlite3")


@pytest.fixture(scope="module")
def crowded_template(tmp_path_factory: pytest.TempPathFactory) -> LifecycleWorld:
    """Declared through b (1); TCS issued (2); fifty more strategies issued (3..52)."""
    world = base_world(tmp_path_factory.mktemp("ei7d-crowded") / "crowded.sqlite3")
    recorded = issue_all(world, propose_strategies(world, range(1, 51)))
    assert [r.recording_sequence for r in recorded] == list(range(3, 53))
    return world


@pytest.fixture
def crowded(crowded_template: LifecycleWorld, tmp_path: Path) -> LifecycleWorld:
    return copy_world(crowded_template, tmp_path / "crowded.sqlite3")


def _reader(database: Path) -> SQLiteReadOnlyEquityRecommendationIssuanceReader:
    return SQLiteReadOnlyEquityRecommendationIssuanceReader(database)


def _discover(database: Path, **options: Any) -> EquityRecordedSessionIssuances:
    return _reader(database).recorded_session_issuances(NSE, DAY, limit=LIMIT, **options)


def _sequences(result: EquityRecordedSessionIssuances) -> list[int]:
    return [r.recording_sequence for r in result.issuances]


def _declared(result: EquityRecordedSessionIssuances) -> list[int]:
    return [r.recording_sequence for r in result.declarations]


def _set(database: Path, table: str, column: str, value: object, sequence: int) -> None:
    """TEST-ONLY corruption: rewrite one column of the record at ``sequence``."""
    corrupt(
        database,
        f"UPDATE {table} SET {column} = ? WHERE recording_sequence = ?",  # noqa: S608
        (value, sequence),
        drop=(f"{table}_no_update",),
        checks=False,
    )


def _orphan_ledger_entry(database: Path, sequence: int) -> None:
    """TEST-ONLY corruption: a committed ledger entry with no record of its kind."""
    corrupt(
        database,
        f"INSERT INTO {LEDGER} (recording_sequence, record_kind) VALUES (?, 'ISSUANCE')",  # noqa: S608
        (sequence,),
    )


@pytest.fixture
def traced(monkeypatch: pytest.MonkeyPatch) -> Iterator[list[str]]:
    """Every SQL statement any EI-7D reader connection executes, as SQLite traced it."""
    real, statements = connection_module.connect_read_only, []

    def connect(database_path: Path) -> sqlite3.Connection:
        connection = real(database_path)
        connection.set_trace_callback(statements.append)
        return connection

    monkeypatch.setattr(connection_module, "connect_read_only", connect)
    yield statements


# ----------------------------------------------------------------------
# The port, the arguments and the 2**63 - 1 bound (EI7-P7)
# ----------------------------------------------------------------------


def test_the_adapter_implements_the_merged_port_with_its_exact_signature() -> None:
    import inspect

    assert issubclass(
        SQLiteReadOnlyEquityRecommendationIssuanceReader, EquityRecordedSessionIssuanceReader
    )
    assert inspect.signature(
        SQLiteReadOnlyEquityRecommendationIssuanceReader.recorded_session_issuances
    ) == inspect.signature(EquityRecordedSessionIssuanceReader.recorded_session_issuances)


@pytest.mark.parametrize(
    ("options", "error"),
    [
        ({"limit": 51}, ValueError),
        ({"limit": 0}, ValueError),
        ({"limit": -1}, ValueError),
        ({"limit": True}, TypeError),
        ({"limit": 50.0}, TypeError),
        ({"limit": "50"}, TypeError),
        ({"limit": 50, "watermark": -1}, ValueError),
        ({"limit": 50, "watermark": True}, TypeError),
        ({"limit": 50, "watermark": 1.0}, TypeError),
        ({"limit": 50, "watermark": "1"}, TypeError),
    ],
)
def test_invalid_limits_and_watermarks_are_refused_before_any_connection(
    session: SessionWorld,
    monkeypatch: pytest.MonkeyPatch,
    options: dict[str, Any],
    error: type[Exception],
) -> None:
    monkeypatch.setattr(reader_module, "read_only_transaction", _never)
    with pytest.raises(error):
        _reader(session.database).recorded_session_issuances(NSE, DAY, **options)


def _never(*_args: object, **_kwargs: object) -> Any:
    raise AssertionError("no connection may be opened")


@pytest.mark.parametrize(
    ("exchange_code", "trading_date"),
    [("NSE", DAY), (NSE, "2026-08-19"), (NSE, ist(9, 15))],
)
def test_untyped_sessions_are_refused_before_any_connection(
    session: SessionWorld, monkeypatch: pytest.MonkeyPatch, exchange_code: Any, trading_date: Any
) -> None:
    monkeypatch.setattr(reader_module, "read_only_transaction", _never)
    with pytest.raises(TypeError):
        _reader(session.database).recorded_session_issuances(
            exchange_code, trading_date, limit=LIMIT
        )


@pytest.mark.parametrize("watermark", [2**63, 2**63 + 1, 10**30])
def test_p7_a_watermark_above_the_int64_maximum_is_ahead_before_any_sql_binding(
    session: SessionWorld, monkeypatch: pytest.MonkeyPatch, watermark: int
) -> None:
    monkeypatch.setattr(reader_module, "read_only_transaction", _never)
    with pytest.raises(EquityIssuanceWatermarkBeyondAuthorityError):
        _discover(session.database, watermark=watermark)


def test_p7_the_int64_maximum_itself_is_bound_safely_and_is_ahead_of_the_authority(
    session: SessionWorld,
) -> None:
    with pytest.raises(EquityIssuanceWatermarkBeyondAuthorityError, match="beyond"):
        _discover(session.database, watermark=2**63 - 1)  # no OverflowError escapes


def test_p7_the_documented_precedence_of_an_int64_overflow_over_a_corrupt_ledger(
    session: SessionWorld,
) -> None:
    """Above 2**63 - 1 is refused before the read (as EI-7C refuses it before the reader)."""
    _orphan_ledger_entry(session.database, 6)
    with pytest.raises(EquityIssuanceWatermarkBeyondAuthorityError):
        _discover(session.database, watermark=2**63)
    with pytest.raises(_Integrity) as raised:  # any bindable watermark: LEDGER first
        _discover(session.database, watermark=2**63 - 1)
    assert raised.value.category == _Integrity.LEDGER


# ----------------------------------------------------------------------
# W, N and declarations
# ----------------------------------------------------------------------


def test_zero_recorded_records_and_no_declaration_is_a_result_never_none(
    session: SessionWorld,
) -> None:
    result = _reader(session.database).recorded_session_issuances(NSE, _OTHER_DAY, limit=LIMIT)
    assert result == EquityRecordedSessionIssuances(
        NSE, _OTHER_DAY, 5, 5, (), (), EQUITY_RECOMMENDATION_ISSUANCE_AUTHORITY_REFERENCE
    )
    assert not result.is_declared and result.complete_through is None


def test_another_exchange_is_an_empty_undeclared_result(session: SessionWorld) -> None:
    result = _reader(session.database).recorded_session_issuances(
        ExchangeCode("BSE"), DAY, limit=LIMIT
    )
    assert (result.issuances, result.declarations, result.recording_position) == ((), (), 5)


def test_w_defaults_to_n_and_equals_an_explicit_w_of_n(session: SessionWorld) -> None:
    latest = _discover(session.database)
    assert (latest.recording_position, latest.recording_watermark) == (5, 5)
    assert _sequences(latest) == [2, 3, 4] and _declared(latest) == [1, 5]
    assert _discover(session.database, watermark=5) == latest


def test_w_zero_lists_nothing_and_states_the_current_position(session: SessionWorld) -> None:
    result = _discover(session.database, watermark=0)
    assert (result.recording_position, result.recording_watermark) == (5, 0)
    assert (result.issuances, result.declarations) == ((), ())


@pytest.mark.parametrize(
    ("watermark", "issuances", "declarations"),
    [(1, [], [1]), (2, [2], [1]), (3, [2, 3], [1]), (4, [2, 3, 4], [1]), (5, [2, 3, 4], [1, 5])],
)
def test_a_historical_w_below_n_is_replayed_exactly_and_never_advanced(
    session: SessionWorld, watermark: int, issuances: list[int], declarations: list[int]
) -> None:
    result = _discover(session.database, watermark=watermark)
    assert (result.recording_position, result.recording_watermark) == (5, watermark)
    assert _sequences(result) == issuances and _declared(result) == declarations


def test_a_watermark_above_n_is_ahead_and_never_silently_advanced(session: SessionWorld) -> None:
    with pytest.raises(EquityIssuanceWatermarkBeyondAuthorityError, match="6 is beyond .* 5"):
        _discover(session.database, watermark=6)


def test_a_recorded_issuance_before_its_declaration_and_a_declaration_at_w(
    session: SessionWorld,
) -> None:
    """At W = 4 the issuances are recorded but not declared; at W = 5 they are covered."""
    recorded = ListEquityRecordedSessionIssuancesUseCase(_reader(session.database))
    declared = ListEquityDeclaredSessionRecommendationsUseCase(_reader(session.database))
    before = recorded.execute(NSE, DAY, 4)
    assert before.declaration is EquitySessionDeclarationState.DECLARED
    assert before.complete_through == B
    assert [item.covered_by_declaration for item in before.items] == [False, False, False]
    assert declared.execute(NSE, DAY, 4).items == ()
    after = recorded.execute(NSE, DAY, 5)
    assert after.complete_through == LATER_DECLARATION
    assert [item.covered_by_declaration for item in after.items] == [True, True, True]
    assert len(declared.execute(NSE, DAY, 5).items) == 3


def test_the_declaration_state_is_the_authoritative_value_at_w(session: SessionWorld) -> None:
    for watermark, expected in ((0, None), (1, B), (4, B), (5, LATER_DECLARATION)):
        assert _discover(session.database, watermark=watermark).complete_through == expected


# ----------------------------------------------------------------------
# Ordering
# ----------------------------------------------------------------------


def test_issuances_are_in_ascending_recording_sequence_and_stable(session: SessionWorld) -> None:
    first = _discover(session.database)
    assert _sequences(first) == sorted(_sequences(first))
    assert all(_discover(session.database) == first for _ in range(3))
    assert first.issuances == session.issuances  # the identities and records the writer recorded


def test_the_modeled_issuance_order_and_the_recording_order_differ_and_both_are_kept(
    session: SessionWorld,
) -> None:
    result = _discover(session.database)
    recording = [r.recommendation.issuance_id for r in result.issuances]
    ei6a = equity_issuance_session_view(
        NSE,
        DAY,
        5,
        [r.history_record() for r in result.issuances],
        result.declarations,
    )
    assert ei6a is not None
    modeled = [record.recommendation_key for record in ei6a.recommendations]
    assert sorted(modeled) == sorted(recording) and modeled != recording  # distinct orders
    view = ListEquityDeclaredSessionRecommendationsUseCase(_reader(session.database)).execute(
        NSE, DAY
    )
    assert [r.recommendation.issuance_id for r in view.items] == modeled
    listed = ListEquityRecordedSessionIssuancesUseCase(_reader(session.database)).execute(NSE, DAY)
    assert [i.issuance.recommendation.issuance_id for i in listed.items] == recording


def test_the_legacy_ei6a_view_is_unchanged_and_agrees_with_the_declared_projection(
    session: SessionWorld,
) -> None:
    legacy = SQLiteEquityRecommendationIssuanceAuthority(session.database)
    for watermark in range(6):
        view = ListEquityDeclaredSessionRecommendationsUseCase(_reader(session.database)).execute(
            NSE, DAY, watermark
        )
        assert view.history == legacy.session_view_at(NSE, DAY, watermark)


# ----------------------------------------------------------------------
# The bound: exactly 50, 51, and never a partial list (EI7-P2, EI7-FC1)
# ----------------------------------------------------------------------


def test_exactly_fifty_recorded_issuances_are_listed_complete_and_verified(
    crowded: LifecycleWorld,
) -> None:
    result = _discover(crowded.database, watermark=51)
    assert (result.recording_position, result.recording_watermark) == (52, 51)
    assert _sequences(result) == list(range(2, 52)) and len(result.issuances) == LIMIT
    legacy = SQLiteEquityRecommendationIssuanceAuthority(crowded.database)
    for recorded in result.issuances:  # each re-derived exactly as EI-6A re-derives it
        assert legacy.load_issuance(recorded.recommendation.issuance_id) == recorded


def test_fifty_one_recorded_issuances_are_refused_with_no_partial_list(
    crowded: LifecycleWorld, traced: list[str]
) -> None:
    with pytest.raises(EquityRecordedSessionDiscoveryLimitExceededError) as raised:
        _discover(crowded.database)
    assert raised.value.limit == LIMIT and not raised.value.retryable
    disposition_reads = [s for s in traced if "equity_recommendation_generation_dispositions" in s]
    assert disposition_reads == []  # refused before any issuance was re-derived
    assert not any("LIMIT" in s.upper().split() for s in traced)  # no SQL LIMIT, no prefix


def test_both_ei7c_projections_refuse_an_overflow_and_none_returns_fifty(
    crowded: LifecycleWorld,
) -> None:
    recorded = ListEquityRecordedSessionIssuancesUseCase(_reader(crowded.database))
    declared = ListEquityDeclaredSessionRecommendationsUseCase(_reader(crowded.database))
    for use_case in (recorded, declared):
        with pytest.raises(EquityReadQueryRefusedError) as raised:
            use_case.execute(NSE, DAY)
        assert raised.value.reason is _R.DISCOVERY_LIMIT_EXCEEDED and not raised.value.retryable
    assert len(recorded.execute(NSE, DAY, 51).items) == LIMIT  # complete at the boundary
    assert declared.execute(NSE, DAY, 51).items == ()  # declared only through b: none covered


def test_the_bound_restricts_presentation_only_the_authority_still_holds_fifty_one(
    crowded: LifecycleWorld,
) -> None:
    assert crowded.world.declare(ist(9, 44)).outcome.value == "DECLARED"  # sequence 53
    legacy = SQLiteEquityRecommendationIssuanceAuthority(crowded.database).session_view(NSE, DAY)
    assert legacy is not None and len(legacy.recommendations) == 51  # EI-6A unchanged


def test_p6_a_corrupt_ledger_takes_precedence_over_an_overflow(crowded: LifecycleWorld) -> None:
    _orphan_ledger_entry(crowded.database, 53)
    with pytest.raises(_Integrity) as raised:
        _discover(crowded.database)
    assert raised.value.category == _Integrity.LEDGER


def test_p6_an_overflow_may_be_reported_before_a_corrupt_issuance_row_is_found(
    crowded: LifecycleWorld,
) -> None:
    """The accepted EI7-P6 limitation: an overflow is not a corruption verdict."""
    _set(crowded.database, ISSUANCES, "issued_at", "2026-08-19T04:13:31Z", 30)  # t is :30
    with pytest.raises(EquityRecordedSessionDiscoveryLimitExceededError):
        _discover(crowded.database)  # 51 at N: refused before row 30 is re-derived
    with pytest.raises(_Integrity) as raised:  # 50 at W = 51: every row verified
        _discover(crowded.database, watermark=51)
    assert raised.value.category == _Integrity.ISSUANCE


def test_the_session_count_and_rows_use_the_session_index_with_bound_parameters(
    session: SessionWorld,
) -> None:
    with closing(raw(session.database)) as connection:
        for statement in (reader_module._SESSION_COUNT_AT, reader_module._SESSION_ISSUANCES_AT):  # noqa: SLF001
            plan = connection.execute(f"EXPLAIN QUERY PLAN {statement}", ("NSE", "x", 1))
            assert any(f"{ISSUANCES}_by_session" in str(row) for row in plan), statement
            assert statement.count("?") == 3 and "LIMIT" not in statement.upper()


# ----------------------------------------------------------------------
# Corruption and availability
# ----------------------------------------------------------------------


def test_a_corrupt_issuance_row_within_the_bound_is_an_issuance_failure(
    session: SessionWorld,
) -> None:
    _set(session.database, ISSUANCES, "symbol", "INFY", 3)
    with pytest.raises(_Integrity) as raised:
        _discover(session.database)
    assert raised.value.category == _Integrity.ISSUANCE
    assert raised.value.issuance_id == session.active.recommendation.issuance_id
    assert _discover(session.database, watermark=2).issuances == (session.terminal,)


def test_a_corrupt_declaration_at_or_below_w_is_a_declaration_failure(
    session: SessionWorld,
) -> None:
    _set(session.database, DECLARATIONS, "complete_through_us", 1, 5)
    with pytest.raises(_Integrity) as raised:
        _discover(session.database)
    assert raised.value.category == _Integrity.DECLARATION


def test_a_declaration_recorded_after_w_is_not_read_at_w(session: SessionWorld) -> None:
    """EI7-P6 step 3 reads and verifies the declarations at or below W only."""
    _set(session.database, DECLARATIONS, "complete_through_us", 1, 5)
    assert _declared(_discover(session.database, watermark=4)) == [1]


def test_a_ledger_gap_is_a_ledger_failure(session: SessionWorld) -> None:
    corrupt(
        session.database,
        f"DELETE FROM {LEDGER} WHERE recording_sequence = 5",  # noqa: S608
        drop=(f"{LEDGER}_no_delete",),
    )
    with pytest.raises(_Integrity) as raised:
        _discover(session.database, watermark=0)  # whole-ledger: even an empty W fails closed
    assert raised.value.category == _Integrity.LEDGER


def test_missing_issuance_tables_are_unavailable_and_nothing_is_created(
    session: SessionWorld,
) -> None:
    with closing(raw(session.database)) as connection:  # TEST-ONLY: a pre-EI-6A file
        connection.execute("PRAGMA foreign_keys = OFF")
        for table in ("equity_recommendation_lifecycle_events", DECLARATIONS, ISSUANCES, LEDGER):
            connection.execute(f"DROP TABLE {table}")
    before = schema_state(session.database)
    with pytest.raises(EquityRecommendationIssuanceUnavailableError, match="no such table"):
        _discover(session.database)
    assert schema_state(session.database) == before


def test_an_unavailable_database_is_typed_and_retryable_by_contract(tmp_path: Path) -> None:
    with pytest.raises(EquityRecommendationIssuanceUnavailableError):
        _discover(tmp_path / "absent.sqlite3")
    with pytest.raises(EquityReadQueryRefusedError) as raised:
        ListEquityRecordedSessionIssuancesUseCase(_reader(tmp_path / "absent.sqlite3")).execute(
            NSE, DAY
        )
    assert raised.value.reason is _R.DEPENDENCY_UNAVAILABLE and raised.value.retryable
    assert list(tmp_path.iterdir()) == []


# ----------------------------------------------------------------------
# No write, no DDL: what the connection actually executed
# ----------------------------------------------------------------------


def test_the_reader_executes_only_reads_in_one_transaction_and_changes_nothing(
    session: SessionWorld, traced: list[str]
) -> None:
    before, schema = file_state(session.database), schema_state(session.database)
    _discover(session.database, watermark=4)
    # Traced from after the confirmed query_only (see the connection tests, R6).
    assert traced[0] == "BEGIN" and traced[-1] == "ROLLBACK"
    assert traced.count("BEGIN") == 1 and traced.count("ROLLBACK") == 1
    for statement in traced[1:-1]:
        assert statement.lstrip().upper().startswith("SELECT"), statement
    assert file_state(session.database) == before and schema_state(session.database) == schema


# ----------------------------------------------------------------------
# Transaction consistency and its limits
# ----------------------------------------------------------------------


def test_n_then_n_plus_one_replay_at_n_is_stable_and_the_latest_read_moves_forward(
    session: SessionWorld,
) -> None:
    at_n = _discover(session.database)
    assert session.world.world.declare(ist(9, 45)).recorded.recording_sequence == 6  # type: ignore[union-attr]
    replay = _discover(session.database, watermark=5)
    assert replay == replace(at_n, recording_position=6)  # same set, same declarations
    latest = _discover(session.database)
    assert (latest.recording_position, _declared(latest)) == (6, [1, 5, 6])
    assert latest.issuances == at_n.issuances


def test_no_write_can_commit_inside_the_discovery_transaction(
    session: SessionWorld, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The accepted declaration writer, racing the read, cannot mix states into one answer."""
    baseline = _discover(session.database)
    monkeypatch.setattr(issuance_module, "_BUSY_TIMEOUT_SECONDS", 0.05)
    real, attempts = reader_module._stored_declaration, []  # noqa: SLF001

    def declaration(row: Any) -> Any:
        if not attempts:  # mid-read: the shared lock is held
            try:
                outcome = session.world.world.declare(ist(9, 45))
                attempts.append(outcome.outcome.value)
            except Exception as exc:  # noqa: BLE001 -- recorded for the assertion
                attempts.append(type(exc).__name__)
        return real(row)

    monkeypatch.setattr(reader_module, "_stored_declaration", declaration)
    assert _discover(session.database) == baseline
    assert attempts and attempts[0] != "DECLARED"  # the writer could not commit meanwhile
    monkeypatch.setattr(reader_module, "_stored_declaration", real)
    assert _discover(session.database) == baseline  # and nothing was committed
    assert session.world.world.declare(ist(9, 45)).outcome.value == "DECLARED"  # later: accepted


def test_an_uncommitted_write_is_never_visible(session: SessionWorld) -> None:
    baseline = _discover(session.database)
    writer = raw(session.database)
    try:
        writer.execute("BEGIN IMMEDIATE")
        writer.execute(f"INSERT INTO {LEDGER} VALUES (6, 'DECLARATION')")  # noqa: S608
        through = ist(9, 45)
        writer.execute(
            f"INSERT INTO {DECLARATIONS} VALUES (6, 'DECLARATION', 'NSE', '2026-08-19', ?, ?)",  # noqa: S608
            (through.value, issuance_module.instant_microseconds(through)),
        )
        assert _discover(session.database) == baseline  # no dirty read
        writer.execute("ROLLBACK")
    finally:
        writer.close()
    assert _discover(session.database) == baseline


def test_concurrent_readers_all_see_the_same_committed_state(session: SessionWorld) -> None:
    baseline = _discover(session.database)
    with ThreadPoolExecutor(max_workers=6) as pool:
        answers = list(pool.map(lambda _: _discover(session.database), range(24)))
    assert all(answer == baseline for answer in answers)


def test_a_writer_blocked_by_a_read_reports_its_typed_failure_and_writes_nothing(
    session: SessionWorld,
) -> None:
    """While a read transaction holds its shared lock, a writer cannot commit."""
    reader = connection_module.connect_read_only(session.database)
    try:
        reader.execute("BEGIN")
        reader.execute(f"SELECT COUNT(*) FROM {LEDGER}").fetchone()  # noqa: S608
        writer = sqlite3.connect(session.database, timeout=0.05, isolation_level=None)
        try:
            writer.execute("BEGIN IMMEDIATE")
            writer.execute(f"INSERT INTO {LEDGER} VALUES (6, 'DECLARATION')")  # noqa: S608
            with pytest.raises(sqlite3.OperationalError, match="locked"):
                writer.execute("COMMIT")
            writer.execute("ROLLBACK")
        finally:
            writer.close()
        reader.execute("ROLLBACK")
    finally:
        reader.close()
    assert _discover(session.database).recording_position == 5
