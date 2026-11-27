"""SI-B: SQLite engine-error classification of the EI-7D read-only readers.

Acceptance of the EI-7 Decision Register, section 14.4 (EI7-SI1) at the
Infrastructure layer: the one shared read-only transaction classifies a
``sqlite3.Error`` by its SQLite **result code** (the primary code in the low
eight bits of an extended code), never by text:

- SQLITE_CORRUPT (any extended code) -> ``EquityReadStorageIntegrityError(CORRUPT)``;
- SQLITE_NOTADB -> ``EquityReadStorageIntegrityError(UNUSABLE)``;
- everything else, and any error without a usable code -> the reader family's
  existing unavailability error.

Three kinds of evidence, kept apart:

- **real SQLite** -- disposable copies of the accepted fixture worlds whose
  bytes are deliberately damaged (TEST-ONLY), each fixture's actual engine
  code asserted before its classification is;
- **injection at the shared boundary** -- a coded ``sqlite3.DatabaseError``
  raised by the read-only connection's own ``execute``, to cover every
  extended code and every reader method deterministically;
- **real Application integration** -- the merged EI-7C query use cases (with
  the SI-A mapping) composed over the read-only adapters on the damaged files.

Nothing here touches a development, deployment or operational database.
"""

from __future__ import annotations

import ast
import inspect
import socket
import sqlite3
from collections.abc import Callable
from contextlib import closing
from pathlib import Path
from typing import Any

import pytest
from northstar_application.application_services import (
    EquityReadQueryRefusal,
    EquityReadQueryRefusedError,
    GetEquityLifecycleSummaryUseCase,
    GetEquityRecommendationDetailUseCase,
    GetEquityResearchTimelineUseCase,
    ListEquityDeclaredSessionRecommendationsUseCase,
    ListEquityRecordedSessionIssuancesUseCase,
)
from northstar_application.ports import (
    EquityCandleEvidenceUnavailableError,
    EquityIntradayResearchTimelineUnavailableError,
    EquityReadStorageIntegrityCondition,
    EquityReadStorageIntegrityError,
    EquityRecommendationGenerationStoreUnavailableError,
    EquityRecommendationIssuanceUnavailableError,
    EquityRecommendationLifecycleStoreUnavailableError,
    InvalidStoredEquityCandleEvidenceSnapshotError,
    InvalidStoredEquityIntradayResearchTimelineError,
    InvalidStoredEquityRecommendationIssuanceAuthorityError,
    InvalidStoredEquityRecommendationLifecycleError,
)

from northstar_infrastructure.persistence import sqlite_equity_read_only as connection_module
from tests.equity_lifecycle_monitoring_fixtures import copy_world
from tests.equity_read_only_fixtures import (
    SessionWorld,
    base_world,
    convert_to_wal,
    copy_session,
    corrupt,
    crash_image,
    damage_page,
    file_state,
    readers,
    research_snapshot_ids,
    schema_state,
    session_world,
    sidecars,
)
from tests.equity_recommendation_acceptance_fixtures import (
    CONFIG,
    DAY,
    DISPOSITIONS,
    ENTRIES,
    NSE,
    SNAPSHOTS,
    TCS,
    raw,
)
from tests.equity_recommendation_issuance_fixtures import DECLARATIONS, ISSUANCES, LEDGER
from tests.equity_recommendation_lifecycle_fixtures import LifecycleWorld

_C = EquityReadStorageIntegrityCondition
_R = EquityReadQueryRefusal
_LIFECYCLE_EVENTS = "equity_recommendation_lifecycle_events"
_UNAVAILABLE = {
    "issuances": EquityRecommendationIssuanceUnavailableError,
    "sessions": EquityRecommendationIssuanceUnavailableError,
    "lifecycles": EquityRecommendationLifecycleStoreUnavailableError,
    "generations": EquityRecommendationGenerationStoreUnavailableError,
    "timelines": EquityIntradayResearchTimelineUnavailableError,
    "snapshots": EquityCandleEvidenceUnavailableError,
}
_SENSITIVE = (
    "C:/secret/northstar-equity.sqlite3",
    "SELECT * FROM equity_recommendation_issuances",
    "connection-7f3a91",
    "btreeInitPage() returns error code 11",
)
_REFUSAL_TEXT = "PERSISTED_AUTHORITY_INVALID: the persisted read storage failed permanently"


@pytest.fixture(autouse=True)
def _refuse_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("SI-B tests must not open network connections.")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)


@pytest.fixture(scope="module")
def template(tmp_path_factory: pytest.TempPathFactory) -> SessionWorld:
    return session_world(tmp_path_factory.mktemp("si-b") / "template.sqlite3")


@pytest.fixture
def session(template: SessionWorld, tmp_path: Path) -> SessionWorld:
    directory = tmp_path / "sealed"
    directory.mkdir()
    return copy_session(template, directory / "si-b.sqlite3")


@pytest.fixture(scope="module")
def base_template(tmp_path_factory: pytest.TempPathFactory) -> LifecycleWorld:
    return base_world(tmp_path_factory.mktemp("si-b-base") / "base.sqlite3")


@pytest.fixture
def base(base_template: LifecycleWorld, tmp_path: Path) -> LifecycleWorld:
    return copy_world(base_template, tmp_path / "base.sqlite3")


def _calls(session: SessionWorld) -> dict[str, tuple[str, Callable[[], Any]]]:
    """Every read method of the six EI-7D interfaces, by name: (family, call)."""
    r = readers(session.database)
    terminal = session.terminal.recommendation
    snapshot_id = research_snapshot_ids(session.database)[1]
    with closing(raw(session.database)) as connection:
        (attempt,) = connection.execute(
            "SELECT attempt_identity FROM equity_recommendation_generation_attempts LIMIT 1"
        ).fetchone()
    return {
        "load_issuance": ("issuances", lambda: r["issuances"].load_issuance(terminal.issuance_id)),
        "load_session_issuance": (
            "issuances",
            lambda: r["issuances"].load_session_issuance(terminal.strategy, TCS, DAY),
        ),
        "session_view": ("issuances", lambda: r["issuances"].session_view(NSE, DAY)),
        "session_view_at": ("issuances", lambda: r["issuances"].session_view_at(NSE, DAY, 3)),
        "recorded_session_issuances": (
            "sessions",
            lambda: r["sessions"].recorded_session_issuances(NSE, DAY, limit=50, watermark=4),
        ),
        "load_lifecycle": (
            "lifecycles",
            lambda: r["lifecycles"].load_lifecycle(terminal.issuance_id),
        ),
        "load_disposition": (
            "generations",
            lambda: r["generations"].load_disposition(terminal.generation_key),
        ),
        "load_attempt": ("generations", lambda: r["generations"].load_attempt(attempt)),
        "load": ("timelines", lambda: r["timelines"].load(CONFIG.strategy, TCS, DAY)),
        "get_snapshot": ("snapshots", lambda: r["snapshots"].get_snapshot(snapshot_id)),
    }


def _assert_permanent(error: BaseException, condition: _C) -> None:
    assert type(error) is EquityReadStorageIntegrityError, type(error)
    assert error.condition is condition and error.retryable is False
    assert isinstance(error.__cause__, sqlite3.Error)  # the engine error, for diagnosis only
    for family in _UNAVAILABLE.values():
        assert not isinstance(error, family)


def _coded(code: int, message: str = "test: injected engine failure") -> sqlite3.DatabaseError:
    error = sqlite3.DatabaseError(message)
    error.sqlite_errorcode = code  # type: ignore[attr-defined]
    error.sqlite_errorname = "TEST"  # type: ignore[attr-defined]
    return error


# ======================================================================
# The classifier: result codes, never text
# ======================================================================

_PERMANENT_CODES = [
    ("SQLITE_CORRUPT", _C.CORRUPT),
    ("SQLITE_CORRUPT_VTAB", _C.CORRUPT),
    ("SQLITE_CORRUPT_SEQUENCE", _C.CORRUPT),
    ("SQLITE_CORRUPT_INDEX", _C.CORRUPT),
    ("SQLITE_NOTADB", _C.UNUSABLE),
]
_AVAILABILITY_CODES = [
    "SQLITE_BUSY",
    "SQLITE_BUSY_RECOVERY",
    "SQLITE_BUSY_SNAPSHOT",
    "SQLITE_LOCKED",
    "SQLITE_LOCKED_SHAREDCACHE",
    "SQLITE_CANTOPEN",
    "SQLITE_CANTOPEN_ISDIR",
    "SQLITE_READONLY",
    "SQLITE_READONLY_ROLLBACK",
    "SQLITE_READONLY_RECOVERY",
    "SQLITE_IOERR",
    "SQLITE_IOERR_READ",
    "SQLITE_ERROR",
    "SQLITE_FULL",
    "SQLITE_PERM",
]


def _unavailable(exc: sqlite3.Error) -> Exception:
    return EquityRecommendationIssuanceUnavailableError("test: family unavailability")


@pytest.mark.parametrize(("name", "condition"), _PERMANENT_CODES)
def test_k01_corrupt_and_notadb_codes_including_extended_ones_are_permanent(
    name: str, condition: _C
) -> None:
    code = getattr(sqlite3, name)
    failure = connection_module.classify_read_failure(_coded(code), _unavailable)
    assert type(failure) is EquityReadStorageIntegrityError and failure.condition is condition
    if name.endswith(("_VTAB", "_SEQUENCE", "_INDEX")):
        assert code != sqlite3.SQLITE_CORRUPT and code & 0xFF == sqlite3.SQLITE_CORRUPT


@pytest.mark.parametrize("name", _AVAILABILITY_CODES)
def test_k02_every_other_code_keeps_the_family_unavailability(name: str) -> None:
    failure = connection_module.classify_read_failure(_coded(getattr(sqlite3, name)), _unavailable)
    assert isinstance(failure, EquityRecommendationIssuanceUnavailableError)


@pytest.mark.parametrize("code", [None, "11", 11.0, True, -11, object()])
def test_k03_an_error_without_a_usable_code_keeps_the_family_unavailability(code: object) -> None:
    error = sqlite3.DatabaseError("database disk image is malformed")  # text never decides
    if code is not None:
        error.sqlite_errorcode = code  # type: ignore[attr-defined]
    failure = connection_module.classify_read_failure(error, _unavailable)
    assert isinstance(failure, EquityRecommendationIssuanceUnavailableError)


def test_k03_text_and_names_never_decide() -> None:
    error = _coded(sqlite3.SQLITE_BUSY, "SQLITE_CORRUPT: file is not a database, malformed")
    error.sqlite_errorname = "SQLITE_CORRUPT"  # type: ignore[attr-defined]
    failure = connection_module.classify_read_failure(error, _unavailable)
    assert isinstance(failure, EquityRecommendationIssuanceUnavailableError)


def test_k04_a_pre_open_refusal_is_never_permanent_even_if_it_carried_a_code() -> None:
    refusal = connection_module.ReadOnlyDatabaseRefusedError("refused before SQLite opened it")
    refusal.sqlite_errorcode = sqlite3.SQLITE_NOTADB  # type: ignore[attr-defined]
    failure = connection_module.classify_read_failure(refusal, _unavailable)
    assert isinstance(failure, EquityRecommendationIssuanceUnavailableError)


def test_k05_the_permanent_error_says_nothing_of_the_engine_message() -> None:
    failure = connection_module.classify_read_failure(
        _coded(sqlite3.SQLITE_CORRUPT_INDEX, " | ".join(_SENSITIVE)), _unavailable
    )
    for fragment in _SENSITIVE:
        assert fragment not in str(failure) and fragment not in repr(failure)


# ======================================================================
# Every reader method, through the shared boundary (injected codes)
# ======================================================================


class _FailingConnection:
    """TEST-ONLY: the real read-only connection whose first SELECT reports ``code``."""

    def __init__(self, real: sqlite3.Connection, code: int) -> None:
        self._real, self._code, self.closed = real, code, False

    def execute(self, statement: str, *args: Any) -> Any:
        if statement.lstrip().upper().startswith("SELECT"):
            raise _coded(self._code, " | ".join(_SENSITIVE))
        return self._real.execute(statement, *args)

    @property
    def in_transaction(self) -> bool:
        return self._real.in_transaction

    def rollback(self) -> None:
        self._real.rollback()

    def close(self) -> None:
        self._real.close()
        self.closed = True


def _inject(monkeypatch: pytest.MonkeyPatch, code: int) -> list[_FailingConnection]:
    real, opened = connection_module.connect_read_only, []

    def connect(database_path: Path) -> Any:
        opened.append(_FailingConnection(real(database_path), code))
        return opened[-1]

    monkeypatch.setattr(connection_module, "connect_read_only", connect)
    return opened


_METHODS = [
    "load_issuance",
    "load_session_issuance",
    "session_view",
    "session_view_at",
    "recorded_session_issuances",
    "load_lifecycle",
    "load_disposition",
    "load_attempt",
    "load",
    "get_snapshot",
]


@pytest.mark.parametrize("method", _METHODS)
@pytest.mark.parametrize(
    ("code", "condition"),
    [(sqlite3.SQLITE_CORRUPT_INDEX, _C.CORRUPT), (sqlite3.SQLITE_NOTADB, _C.UNUSABLE)],
    ids=["corrupt-index", "notadb"],
)
def test_g01_every_reader_method_raises_the_permanent_contract(
    session: SessionWorld, monkeypatch: pytest.MonkeyPatch, method: str, code: int, condition: _C
) -> None:
    family, call = _calls(session)[method]
    before = file_state(session.database)
    opened = _inject(monkeypatch, code)
    with pytest.raises(EquityReadStorageIntegrityError) as raised:
        call()
    _assert_permanent(raised.value, condition)
    assert raised.value.__cause__.sqlite_errorcode == code  # type: ignore[union-attr]
    assert opened and all(c.closed for c in opened)  # cleanup ran; nothing left open
    assert file_state(session.database) == before


@pytest.mark.parametrize("method", _METHODS)
def test_g02_every_reader_method_keeps_its_family_unavailability_for_other_codes(
    session: SessionWorld, monkeypatch: pytest.MonkeyPatch, method: str
) -> None:
    family, call = _calls(session)[method]
    _inject(monkeypatch, sqlite3.SQLITE_BUSY_SNAPSHOT)
    with pytest.raises(_UNAVAILABLE[family]) as raised:
        call()
    assert not isinstance(raised.value, EquityReadStorageIntegrityError)


# ======================================================================
# Real SQLite: damaged files, actual engine codes asserted first
# ======================================================================

_REAL_TABLES = [
    ("load_issuance", ISSUANCES),
    ("recorded_session_issuances", LEDGER),
    ("load_lifecycle", ISSUANCES),
    ("load_disposition", DISPOSITIONS),
    ("load", ENTRIES),
    ("get_snapshot", SNAPSHOTS),
]


@pytest.mark.parametrize(("method", "table"), _REAL_TABLES)
def test_r01_a_damaged_page_read_by_each_family_is_permanent_corruption(
    session: SessionWorld, method: str, table: str
) -> None:
    family, call = _calls(session)[method]
    damage_page(session.database, table)
    before, sides = file_state(session.database), sidecars(session.database)
    with pytest.raises(EquityReadStorageIntegrityError) as raised:
        call()
    cause = raised.value.__cause__
    assert cause.sqlite_errorcode & 0xFF == sqlite3.SQLITE_CORRUPT  # type: ignore[union-attr]
    _assert_permanent(raised.value, _C.CORRUPT)
    assert str(session.database.parent) not in str(raised.value)
    assert file_state(session.database) == before and sidecars(session.database) == sides


@pytest.mark.parametrize(
    ("name", "content", "code", "condition"),
    [
        ("truncated-with-magic", lambda data: data[:50], sqlite3.SQLITE_CORRUPT, _C.CORRUPT),
        ("header-only", lambda data: data[:100], sqlite3.SQLITE_CORRUPT, _C.CORRUPT),
        ("not-a-database", lambda data: b"\x00garbage" * 512, sqlite3.SQLITE_NOTADB, _C.UNUSABLE),
    ],
)
def test_r02_damaged_and_foreign_files_through_every_family(
    template: SessionWorld,
    tmp_path: Path,
    name: str,
    content: Any,
    code: int,
    condition: _C,
) -> None:
    database = tmp_path / name / "db.sqlite3"
    database.parent.mkdir()
    database.write_bytes(content(template.database.read_bytes()))
    before = file_state(database)
    for family, call in _family_calls(database).items():
        with pytest.raises(EquityReadStorageIntegrityError) as raised:
            call()
        assert raised.value.__cause__.sqlite_errorcode & 0xFF == code, family  # type: ignore[union-attr]
        _assert_permanent(raised.value, condition)
    assert file_state(database) == before and sidecars(database) == []


def _family_calls(database: Path) -> dict[str, Callable[[], Any]]:
    r = readers(database)
    unknown = "eiris1-sha256:" + "0" * 64
    return {
        "issuances": lambda: r["issuances"].load_issuance(unknown),
        "sessions": lambda: r["sessions"].recorded_session_issuances(NSE, DAY, limit=50),
        "lifecycles": lambda: r["lifecycles"].load_lifecycle(unknown),
        "generations": lambda: r["generations"].load_disposition("eirk1-sha256:" + "0" * 64),
        "timelines": lambda: r["timelines"].load(CONFIG.strategy, TCS, DAY),
        "snapshots": lambda: r["snapshots"].get_snapshot("ecs1-sha256:" + "0" * 64),
    }


def test_r03_a_damaged_page_no_query_reads_stays_undetected(base: LifecycleWorld) -> None:
    """EI7-SI2: reads verify what they read; no whole-file check runs."""
    damage_page(base.database, _LIFECYCLE_EVENTS)  # an empty table: no read touches it
    loaded = readers(base.database)["lifecycles"].load_lifecycle(base.issuance_id)
    assert loaded is not None and loaded.lifecycle.events == ()


# ======================================================================
# Real SQLite: availability conditions keep their classification
# ======================================================================


def _assert_unavailable_everywhere(database: Path) -> None:
    for family, call in _family_calls(database).items():
        with pytest.raises(_UNAVAILABLE[family]) as raised:
            call()
        assert not isinstance(raised.value, EquityReadStorageIntegrityError), family


def test_u01_busy_is_unavailability(session: SessionWorld, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(connection_module, "READ_ONLY_BUSY_TIMEOUT_SECONDS", 0.05)
    holder = raw(session.database)
    try:
        holder.execute("BEGIN EXCLUSIVE")
        with pytest.raises(EquityRecommendationIssuanceUnavailableError) as raised:
            readers(session.database)["sessions"].recorded_session_issuances(NSE, DAY, limit=50)
        assert raised.value.__cause__.sqlite_errorcode & 0xFF == sqlite3.SQLITE_BUSY  # type: ignore[union-attr]
    finally:
        holder.execute("ROLLBACK")
        holder.close()


def test_u02_cantopen_missing_file_and_missing_directory_are_unavailability(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _assert_unavailable_everywhere(tmp_path / "missing.sqlite3")
    _assert_unavailable_everywhere(tmp_path / "absent" / "missing.sqlite3")
    monkeypatch.setattr(connection_module, "_require_rollback_journal", lambda path: None)
    with pytest.raises(EquityRecommendationIssuanceUnavailableError) as raised:
        readers(tmp_path / "missing.sqlite3")["issuances"].load_issuance("x")
    assert raised.value.__cause__.sqlite_errorcode & 0xFF == sqlite3.SQLITE_CANTOPEN  # type: ignore[union-attr]
    assert list(tmp_path.iterdir()) == []


def test_u03_a_hot_journal_is_unavailability_and_untouched(
    session: SessionWorld, tmp_path: Path
) -> None:
    crashed = tmp_path / "crashed"
    crashed.mkdir()
    image = crashed / "image.sqlite3"
    crash_image(session.database, image)
    before = file_state(image)
    _assert_unavailable_everywhere(image)
    with pytest.raises(EquityRecommendationIssuanceUnavailableError) as raised:
        readers(image)["issuances"].load_issuance("x")
    code = raised.value.__cause__.sqlite_errorcode  # type: ignore[union-attr]
    assert code == sqlite3.SQLITE_READONLY_ROLLBACK and code & 0xFF == sqlite3.SQLITE_READONLY
    assert file_state(image) == before


def test_u04_missing_tables_wal_refusal_nul_path_and_empty_file_are_unavailability(
    session: SessionWorld, tmp_path: Path
) -> None:
    other = tmp_path / "other.sqlite3"
    with closing(sqlite3.connect(other)) as connection:
        connection.execute("CREATE TABLE unrelated (x INTEGER)")
        connection.commit()
    before = schema_state(other)
    _assert_unavailable_everywhere(other)  # SQLITE_ERROR: no such table
    assert schema_state(other) == before
    empty = tmp_path / "empty.sqlite3"
    empty.touch()
    _assert_unavailable_everywhere(empty)
    _assert_unavailable_everywhere(tmp_path / "bad\x00name.sqlite3")
    convert_to_wal(session.database)
    _assert_unavailable_everywhere(session.database)
    assert sidecars(session.database) == []


# ======================================================================
# Corrupt records stay record errors; corrupt storage is the new contract
# ======================================================================


def _set(database: Path, table: str, assignment: str, where: str) -> None:
    corrupt(
        database,
        f"UPDATE {table} SET {assignment} WHERE {where}",  # noqa: S608
        drop=(f"{table}_no_update",),
        checks=False,
    )


@pytest.mark.parametrize(
    ("method", "table", "assignment", "where", "error"),
    [
        ("load_issuance", ISSUANCES, "symbol = 'INFY'", "recording_sequence = 2",
         InvalidStoredEquityRecommendationIssuanceAuthorityError),
        ("recorded_session_issuances", DECLARATIONS, "complete_through_us = 1",
         "recording_sequence = 1", InvalidStoredEquityRecommendationIssuanceAuthorityError),
        ("load_lifecycle", _LIFECYCLE_EVENTS, "evaluated_at_us = 1", "event_sequence = 2",
         InvalidStoredEquityRecommendationLifecycleError),
        ("load_disposition", DISPOSITIONS, "symbol = 'INFY'", "1 = 1", Exception),
        ("load", ENTRIES, "role = 'NOT_A_ROLE'", "sequence = 1",
         InvalidStoredEquityIntradayResearchTimelineError),
        ("get_snapshot", SNAPSHOTS, "watermark = watermark + 1", "1 = 1",
         InvalidStoredEquityCandleEvidenceSnapshotError),
    ],
)  # fmt: skip
def test_j01_record_corruption_keeps_its_accepted_error_never_the_storage_contract(
    session: SessionWorld, method: str, table: str, assignment: str, where: str, error: type
) -> None:
    from northstar_application.application_services import InvalidEquityRecommendationRecordError

    expected = InvalidEquityRecommendationRecordError if error is Exception else error
    _, call = _calls(session)[method]
    _set(session.database, table, assignment, where)
    with pytest.raises(expected) as raised:
        call()
    assert not isinstance(raised.value, EquityReadStorageIntegrityError)


# ======================================================================
# Real SQLite + the merged EI-7C queries (SI-A mapping)
# ======================================================================


def _queries(database: Path) -> dict[str, Callable[[str], Any]]:
    r = readers(database)
    return {
        "recorded": lambda _: ListEquityRecordedSessionIssuancesUseCase(r["sessions"]).execute(
            NSE, DAY
        ),
        "declared": lambda _: ListEquityDeclaredSessionRecommendationsUseCase(
            r["sessions"]
        ).execute(NSE, DAY, 4),
        "detail": lambda identity: GetEquityRecommendationDetailUseCase(
            r["lifecycles"], r["generations"], r["snapshots"]
        ).execute(identity),
        "summary": lambda identity: GetEquityLifecycleSummaryUseCase(r["lifecycles"]).execute(
            identity
        ),
        "research": lambda _: GetEquityResearchTimelineUseCase(
            r["timelines"], r["snapshots"], r["issuances"], r["generations"]
        ).execute(CONFIG.strategy, TCS, DAY),
    }


def _assert_query_refused(call: Callable[[], Any], code: int) -> None:
    with pytest.raises(EquityReadQueryRefusedError) as raised:
        call()
    refused = raised.value
    assert (refused.reason, refused.retryable) == (_R.PERSISTED_AUTHORITY_INVALID, False)
    assert str(refused) == _REFUSAL_TEXT  # fixed: no driver message, path or record
    contract = refused.__cause__
    assert type(contract) is EquityReadStorageIntegrityError
    assert contract.__cause__.sqlite_errorcode & 0xFF == code  # type: ignore[union-attr]


@pytest.mark.parametrize(
    ("table", "queries"),
    [
        (ISSUANCES, ["recorded", "declared", "detail", "summary", "research"]),
        (DISPOSITIONS, ["detail"]),
        (ENTRIES, ["research"]),
        (SNAPSHOTS, ["detail", "research"]),
    ],
    ids=["issuances", "dispositions", "timeline-entries", "snapshots"],
)
def test_i01_real_corruption_at_each_reader_boundary_refuses_the_whole_query(
    session: SessionWorld, table: str, queries: list[str]
) -> None:
    identity = session.terminal.recommendation.issuance_id
    damage_page(session.database, table)
    before = file_state(session.database)
    for name in queries:
        _assert_query_refused(lambda n=name: _queries(session.database)[n](identity), 11)
    assert file_state(session.database) == before


def test_i02_a_damaged_snapshot_page_never_becomes_an_invalid_evidence_panel(
    session: SessionWorld,
) -> None:
    """The detail's lifecycle and generation reads succeed; only the panel read fails."""
    identity = session.terminal.recommendation.issuance_id
    damage_page(session.database, SNAPSHOTS)
    r = readers(session.database)
    assert r["lifecycles"].load_lifecycle(identity) is not None  # required authority readable
    _assert_query_refused(lambda: _queries(session.database)["detail"](identity), 11)


def test_i03_damaged_issuances_never_become_not_queryable_research(
    session: SessionWorld,
) -> None:
    """The timeline and its snapshots read; the issuance read fails: never NOT_QUERYABLE."""
    damage_page(session.database, ISSUANCES)
    assert readers(session.database)["timelines"].load(CONFIG.strategy, TCS, DAY) is not None
    _assert_query_refused(lambda: _queries(session.database)["research"](""), 11)


def test_i04_a_foreign_file_refuses_every_query_as_unusable_storage(
    template: SessionWorld, tmp_path: Path
) -> None:
    database = tmp_path / "foreign.sqlite3"
    database.write_bytes(b"\x00garbage" * 512)
    for name, query in _queries(database).items():
        _assert_query_refused(lambda q=query: q(template.terminal.recommendation.issuance_id), 26)
        assert name


def test_i05_ordinary_unavailability_stays_retryable_through_the_queries(tmp_path: Path) -> None:
    for name, query in _queries(tmp_path / "absent.sqlite3").items():
        if name == "summary":  # an item status, not a refusal: next test
            continue
        with pytest.raises(EquityReadQueryRefusedError) as raised:
            query("eiris1-sha256:" + "0" * 64)
        assert (raised.value.reason, raised.value.retryable) == (_R.DEPENDENCY_UNAVAILABLE, True)


def test_i05_an_unavailable_lifecycle_summary_is_still_an_item_status(tmp_path: Path) -> None:
    summary = _queries(tmp_path / "absent.sqlite3")["summary"]("eiris1-sha256:" + "0" * 64)
    assert summary.status.value == "UNAVAILABLE"


# ======================================================================
# Architecture: one classifier, the Application contract, codes only
# ======================================================================

_PERSISTENCE = Path(inspect.getfile(connection_module)).parent


def test_a01_the_contract_is_imported_from_the_application_ports_package() -> None:
    tree = ast.parse(Path(inspect.getfile(connection_module)).read_text(encoding="utf-8"))
    sources = {
        node.module: {a.name for a in node.names}
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
    }
    assert {"EquityReadStorageIntegrityCondition", "EquityReadStorageIntegrityError"} <= sources[
        "northstar_application.ports"
    ]
    assert not any("application_services" in (m or "") for m in sources)


def test_a02_no_infrastructure_module_defines_its_own_storage_integrity_error() -> None:
    for path in _PERSISTENCE.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        names = {n.name for n in ast.walk(tree) if isinstance(n, ast.ClassDef)}
        assert not {n for n in names if "StorageIntegrity" in n}, path.name


def test_a03_the_classifier_reads_only_the_result_code() -> None:
    source = inspect.getsource(connection_module.classify_read_failure)
    tree = ast.parse(source)
    attributes = {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)}
    constants = {n.value for n in ast.walk(tree) if isinstance(n, ast.Constant)}
    assert "sqlite_errorname" not in attributes and "args" not in attributes
    assert "sqlite_errorname" not in constants
    assert not any(isinstance(n, ast.Call) and getattr(n.func, "id", "") == "str"
                   for n in ast.walk(tree))  # fmt: skip
    assert "sqlite_errorcode" in constants  # read by getattr, validated, masked


def test_a04_every_sqlite_error_boundary_of_the_shared_transaction_is_classified() -> None:
    tree = ast.parse(inspect.getsource(connection_module.read_only_transaction))
    handlers = [
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.ExceptHandler) and "sqlite3.Error" in ast.unparse(n.type or n)
    ]
    assert len(handlers) == 3
    for handler in handlers:
        raised = [n for n in ast.walk(handler) if isinstance(n, ast.Raise)]
        assert raised and all("classify_read_failure" in ast.unparse(r.exc) for r in raised)


def test_a05_the_readers_reach_sqlite_only_through_the_shared_transaction() -> None:
    from northstar_infrastructure.persistence import (
        sqlite_equity_evidence_read_only,
        sqlite_equity_recommendation_read_only,
    )

    for module in (sqlite_equity_recommendation_read_only, sqlite_equity_evidence_read_only):
        source = Path(inspect.getfile(module)).read_text(encoding="utf-8")
        assert "read_only_transaction" in source
        assert "sqlite3.connect" not in source and "except sqlite3" not in source
