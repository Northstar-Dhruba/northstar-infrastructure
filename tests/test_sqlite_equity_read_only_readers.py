"""EI-7D: the read-only issuance, lifecycle, generation, timeline and snapshot readers.

For each reader, on real SQLite: it implements exactly the merged Application
reader port and nothing else; it answers exactly what the accepted
write-capable adapter answers on the same persisted fixture (semantic domain
objects compared, never row order); its typed failures are the source-faithful
ones; and it changes nothing. The timeline and snapshot readers separate
corrupt stored data (permanent integrity) from unavailable storage, which the
existing EI-2B and EI-3C adapters conflate -- the EI-7C limitation closed here.

Fixtures are built by the accepted chain (tests/equity_read_only_fixtures.py);
raw SQL appears only for labelled TEST-ONLY corruption of disposable copies
and for the pinned version-1 historical reference (planted exactly as
test_sqlite_equity_recommendation_v1_persisted_replay plants it). Nothing here
opens a network connection or touches a production database.
"""

from __future__ import annotations

import inspect
import json
import socket
import sqlite3
from contextlib import closing
from datetime import date
from pathlib import Path
from typing import Any

import pytest
from northstar_application.application_services import (
    EQUITY_RECOMMENDATION_EVALUATOR_VERSION,
    EquityCandleEvidenceClass,
    IncompatibleEquityRecommendationRecordError,
    InvalidEquityRecommendationRecordError,
)
from northstar_application.ports import (
    EquityCandleEvidenceSnapshotReader,
    EquityCandleEvidenceUnavailableError,
    EquityIntradayResearchTimelineReader,
    EquityIntradayResearchTimelineUnavailableError,
    EquityIssuanceWatermarkBeyondAuthorityError,
    EquityRecommendationGenerationRecordReader,
    EquityRecommendationGenerationStoreUnavailableError,
    EquityRecommendationIssuanceReader,
    EquityRecommendationLifecycleReader,
    EquityRecommendationLifecycleStoreUnavailableError,
    EquityRecordedSessionIssuanceReader,
    InvalidStoredEquityCandleEvidenceSnapshotError,
    InvalidStoredEquityIntradayResearchTimelineError,
    InvalidStoredEquityRecommendationIssuanceAuthorityError,
    InvalidStoredEquityRecommendationLifecycleError,
)
from northstar_core.domain.value_objects import ListingReference
from northstar_core.foundation.value_objects import Symbol
from northstar_core.recommendations.equity.lifecycle import EquityLifecycleState

from northstar_infrastructure.persistence import (
    EquityCandleEvidenceStorageError,
    EquityIntradayResearchTimelineStorageError,
    SQLiteEquityCandleEvidenceSnapshotStore,
    SQLiteEquityIntradayResearchTimelineStore,
    SQLiteEquityRecommendationGenerationStore,
    SQLiteEquityRecommendationIssuanceAuthority,
    SQLiteEquityRecommendationLifecycleStore,
    SQLiteReadOnlyEquityCandleEvidenceSnapshotReader,
    SQLiteReadOnlyEquityIntradayResearchTimelineReader,
    SQLiteReadOnlyEquityRecommendationGenerationReader,
    SQLiteReadOnlyEquityRecommendationIssuanceReader,
    SQLiteReadOnlyEquityRecommendationLifecycleReader,
)
from northstar_infrastructure.persistence import sqlite_equity_read_only as connection_module
from tests.equity_lifecycle_monitoring_fixtures import copy_world
from tests.equity_read_only_fixtures import (
    SessionWorld,
    base_world,
    configuration,
    copy_session,
    corrupt,
    file_state,
    plant_production_snapshot,
    schema_state,
    session_world,
)
from tests.equity_recommendation_acceptance_fixtures import (
    ATTEMPTS,
    CONFIG,
    DAY,
    DISPOSITIONS,
    ENTRIES,
    INFY,
    NSE,
    SNAPSHOTS,
    TCS,
    raw,
)
from tests.equity_recommendation_generation_fixtures import World as GenerationWorld
from tests.equity_recommendation_issuance_fixtures import DECLARATIONS, ISSUANCES, LEDGER
from tests.equity_recommendation_lifecycle_fixtures import LifecycleWorld
from tests.test_sqlite_equity_recommendation_v1_persisted_replay import (
    _KEY as _V1_KEY,
)
from tests.test_sqlite_equity_recommendation_v1_persisted_replay import (
    _KEYED as _V1_KEYED,
)
from tests.test_sqlite_equity_recommendation_v1_persisted_replay import (
    _KEYLESS as _V1_KEYLESS,
)
from tests.test_sqlite_equity_recommendation_v1_persisted_replay import (
    _plant_reference,
)

S = EquityLifecycleState
LIFECYCLE_EVENTS = "equity_recommendation_lifecycle_events"
_Authority = InvalidStoredEquityRecommendationIssuanceAuthorityError
_Lifecycle = InvalidStoredEquityRecommendationLifecycleError
_UNKNOWN_ISSUANCE = "eiris1-sha256:" + "0" * 64
_UNKNOWN_SNAPSHOT = "ecs1-sha256:" + "0" * 64


@pytest.fixture(autouse=True)
def _refuse_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("EI-7D tests must not open network connections.")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)


@pytest.fixture(scope="module")
def session_template(tmp_path_factory: pytest.TempPathFactory) -> SessionWorld:
    return session_world(tmp_path_factory.mktemp("ei7d-readers") / "template.sqlite3")


@pytest.fixture
def session(session_template: SessionWorld, tmp_path: Path) -> SessionWorld:
    return copy_session(session_template, tmp_path / "session.sqlite3")


@pytest.fixture(scope="module")
def base_template(tmp_path_factory: pytest.TempPathFactory) -> LifecycleWorld:
    return base_world(tmp_path_factory.mktemp("ei7d-base") / "base.sqlite3")


@pytest.fixture
def base(base_template: LifecycleWorld, tmp_path: Path) -> LifecycleWorld:
    return copy_world(base_template, tmp_path / "base.sqlite3")


def _public(cls: type) -> set[str]:
    return {name for name, _ in inspect.getmembers(cls) if not name.startswith("_")}


def _sealed[T](database: Path, read: Any) -> T:
    """Run ``read``; the file, its siblings and its schema must be unchanged by it."""
    before, schema = file_state(database), schema_state(database)
    answer = read()
    assert file_state(database) == before and schema_state(database) == schema
    return answer


def _column(database: Path, table: str, key: str, value: object) -> Any:
    with closing(raw(database)) as connection:
        return connection.execute(
            f"SELECT * FROM {table} WHERE {key} = ?",  # noqa: S608
            (value,),
        ).fetchone()


def _timeline_id(database: Path, strategy: str, symbol: str) -> int:
    with closing(raw(database)) as connection:
        (found,) = connection.execute(
            "SELECT timeline_id FROM equity_intraday_research_timelines "
            "WHERE strategy = ? AND symbol = ?",
            (strategy, symbol),
        ).fetchone()
    return found


def _snapshot_ids(database: Path) -> list[str]:
    with closing(raw(database)) as connection:
        return [r[0] for r in connection.execute(f"SELECT snapshot_id FROM {SNAPSHOTS}")]  # noqa: S608


# ======================================================================
# The ports: exactly the reads, nothing else
# ======================================================================


_LEGACY_ISSUANCE_READS = {"load_issuance", "load_session_issuance", "session_view"}


@pytest.mark.parametrize(
    ("adapter", "ports"),
    [
        (  # one adapter, two ports (EI-7 design, section 23.7)
            SQLiteReadOnlyEquityRecommendationIssuanceReader,
            {
                EquityRecommendationIssuanceReader: {*_LEGACY_ISSUANCE_READS, "session_view_at"},
                EquityRecordedSessionIssuanceReader: {"recorded_session_issuances"},
            },
        ),
        (
            SQLiteReadOnlyEquityRecommendationLifecycleReader,
            {EquityRecommendationLifecycleReader: {"load_lifecycle"}},
        ),
        (
            SQLiteReadOnlyEquityRecommendationGenerationReader,
            {EquityRecommendationGenerationRecordReader: {"load_disposition", "load_attempt"}},
        ),
        (
            SQLiteReadOnlyEquityIntradayResearchTimelineReader,
            {EquityIntradayResearchTimelineReader: {"load"}},
        ),
        (
            SQLiteReadOnlyEquityCandleEvidenceSnapshotReader,
            {EquityCandleEvidenceSnapshotReader: {"get_snapshot"}},
        ),
    ],
)
def test_each_reader_implements_its_merged_ports_and_exposes_only_their_reads(
    adapter: type, ports: dict[type, set[str]], tmp_path: Path
) -> None:
    instance = adapter(tmp_path / "never-opened.sqlite3")
    for port, reads in ports.items():
        assert issubclass(adapter, port)
        assert isinstance(instance, port)  # structural for the protocols, nominal for the ABCs
        for name in reads:
            assert inspect.signature(getattr(adapter, name)) == inspect.signature(
                getattr(port, name)
            )
    assert _public(adapter) == set().union(*ports.values())
    assert list(tmp_path.iterdir()) == []  # construction opens nothing


def test_no_reader_is_a_write_capable_store() -> None:
    from northstar_application.ports import (
        EquityCandleEvidenceSnapshotStore,
        EquityIntradayResearchTimelineStore,
        EquityRecommendationGenerationStore,
        EquityRecommendationIssuanceWriter,
        EquityRecommendationLifecycleStore,
    )

    stores = (
        EquityCandleEvidenceSnapshotStore,
        EquityIntradayResearchTimelineStore,
        EquityRecommendationGenerationStore,
        EquityRecommendationIssuanceWriter,
        EquityRecommendationLifecycleStore,
    )
    for reader in (
        SQLiteReadOnlyEquityRecommendationIssuanceReader,
        SQLiteReadOnlyEquityRecommendationLifecycleReader,
        SQLiteReadOnlyEquityRecommendationGenerationReader,
        SQLiteReadOnlyEquityIntradayResearchTimelineReader,
        SQLiteReadOnlyEquityCandleEvidenceSnapshotReader,
    ):
        assert not issubclass(reader, stores), reader
        for write in ("issue", "append", "write_issuance", "write_declaration",
                      "write_generation", "write_attempt", "record"):  # fmt: skip
            assert not hasattr(reader, write), (reader, write)


@pytest.mark.parametrize("database_path", [None, 1, b"bytes"])
def test_a_reader_needs_a_path(database_path: Any) -> None:
    with pytest.raises(TypeError):
        SQLiteReadOnlyEquityRecommendationIssuanceReader(database_path)
    with pytest.raises(TypeError):
        SQLiteReadOnlyEquityCandleEvidenceSnapshotReader(database_path)


# ======================================================================
# Issuance reader (EI-6A)
# ======================================================================


def test_issuance_reads_equal_the_accepted_authority_on_every_method(
    session: SessionWorld,
) -> None:
    legacy = SQLiteEquityRecommendationIssuanceAuthority(session.database)
    reader = SQLiteReadOnlyEquityRecommendationIssuanceReader(session.database)
    for recorded in session.issuances:
        identity = recorded.recommendation.issuance_id
        assert _sealed(session.database, lambda i=identity: reader.load_issuance(i)) == recorded
        assert legacy.load_issuance(identity) == recorded
        issued = recorded.recommendation
        found = reader.load_session_issuance(issued.strategy, issued.listing, issued.trading_date)
        assert found == recorded
    assert reader.load_issuance(_UNKNOWN_ISSUANCE) is None
    assert reader.load_session_issuance(CONFIG.strategy, INFY, DAY) is None
    assert reader.session_view(NSE, DAY) == legacy.session_view(NSE, DAY)
    assert reader.session_view(NSE, date(2026, 8, 20)) is None  # undeclared: None (EI-6A)
    for watermark in range(6):
        assert reader.session_view_at(NSE, DAY, watermark) == legacy.session_view_at(
            NSE, DAY, watermark
        )


@pytest.mark.parametrize("watermark", [6, 2**63 - 1, 2**63, 10**30])
def test_issuance_watermark_refusals_equal_the_accepted_authority(
    session: SessionWorld, watermark: int
) -> None:
    for adapter in (
        SQLiteEquityRecommendationIssuanceAuthority(session.database),
        SQLiteReadOnlyEquityRecommendationIssuanceReader(session.database),
    ):
        with pytest.raises(EquityIssuanceWatermarkBeyondAuthorityError):
            adapter.session_view_at(NSE, DAY, watermark)


@pytest.mark.parametrize(
    ("statement", "parameters", "drop", "category"),
    [
        (f"INSERT INTO {LEDGER} VALUES (6, 'ISSUANCE')", (), (), _Authority.LEDGER),  # noqa: S608
        (
            f"UPDATE {ISSUANCES} SET symbol = 'INFY' WHERE recording_sequence = 3",  # noqa: S608
            (),
            (f"{ISSUANCES}_no_update",),
            _Authority.ISSUANCE,
        ),
        (
            f"UPDATE {DECLARATIONS} SET complete_through_us = 1 WHERE recording_sequence = 5",  # noqa: S608
            (),
            (f"{DECLARATIONS}_no_update",),
            _Authority.DECLARATION,
        ),
    ],
    ids=["ledger", "issuance", "declaration"],
)
def test_issuance_corruption_is_classified_exactly_as_the_accepted_authority_does(
    session: SessionWorld,
    statement: str,
    parameters: tuple[object, ...],
    drop: tuple[str, ...],
    category: str,
) -> None:
    corrupt(session.database, statement, parameters, drop=drop, checks=False)
    raised = []
    for adapter in (
        SQLiteReadOnlyEquityRecommendationIssuanceReader(session.database),
        SQLiteEquityRecommendationIssuanceAuthority(session.database),
    ):
        with pytest.raises(_Authority) as caught:
            adapter.session_view_at(NSE, DAY, 5)
        raised.append((caught.value.category, caught.value.issuance_id))
    assert raised[0] == raised[1] and raised[0][0] == category


def test_issuance_precedence_of_a_corrupt_ledger_over_a_huge_watermark_is_preserved(
    session: SessionWorld,
) -> None:
    corrupt(session.database, f"INSERT INTO {LEDGER} VALUES (6, 'ISSUANCE')")  # noqa: S608
    with pytest.raises(_Authority) as raised:
        SQLiteReadOnlyEquityRecommendationIssuanceReader(session.database).session_view_at(
            NSE, DAY, 10**30
        )
    assert raised.value.category == _Authority.LEDGER  # as EI-6A: the ledger first


def test_issuance_arguments_are_typed_before_any_connection(tmp_path: Path) -> None:
    reader = SQLiteReadOnlyEquityRecommendationIssuanceReader(tmp_path / "x.sqlite3")
    for call in (
        lambda: reader.load_issuance(1),  # type: ignore[arg-type]
        lambda: reader.load_session_issuance("s", TCS, DAY),  # type: ignore[arg-type]
        lambda: reader.load_session_issuance(CONFIG.strategy, "TCS", DAY),  # type: ignore[arg-type]
        lambda: reader.session_view("NSE", DAY),  # type: ignore[arg-type]
        lambda: reader.session_view_at(NSE, DAY, True),
    ):
        with pytest.raises(TypeError):
            call()
    with pytest.raises(ValueError):
        reader.session_view_at(NSE, DAY, -1)
    assert list(tmp_path.iterdir()) == []


# ======================================================================
# Lifecycle reader (EI-6B)
# ======================================================================


def test_lifecycle_reads_equal_the_accepted_store_terminal_active_and_without_events(
    session: SessionWorld,
) -> None:
    legacy = SQLiteEquityRecommendationLifecycleStore(session.database)
    reader = SQLiteReadOnlyEquityRecommendationLifecycleReader(session.database)
    expected = {
        session.terminal: (S.CLOSED_TARGET, 3),
        session.active: (S.ACTIVE, 2),
        session.quiet: (S.RECORDED, 0),  # issued, no lifecycle event yet (N06)
    }
    for recorded, (state, position) in expected.items():
        identity = recorded.recommendation.issuance_id
        loaded = _sealed(session.database, lambda i=identity: reader.load_lifecycle(i))
        assert loaded == legacy.load_lifecycle(identity)
        assert loaded.issuance == recorded  # the parent re-derived in the same read
        assert (loaded.lifecycle.state, loaded.position) == (state, position)
        events = loaded.lifecycle.events
        assert [e.sequence for e in events] == list(range(1, position + 1))
        modeled = [e.evaluated_at for e in events]
        assert all(a.compare(b) <= 0 for a, b in zip(modeled, modeled[1:], strict=False))
    assert reader.load_lifecycle(_UNKNOWN_ISSUANCE) is None


def test_lifecycle_terminal_incomplete_evidence_equals_the_accepted_store(
    base: LifecycleWorld,
) -> None:
    incomplete = base.record(base.snapshot(10, 51))  # bar 7 onwards never arrived
    assert incomplete.state is S.INCOMPLETE_EVIDENCE
    loaded = SQLiteReadOnlyEquityRecommendationLifecycleReader(base.database).load_lifecycle(
        base.issuance_id
    )
    assert loaded == incomplete.recorded and loaded.lifecycle.state.is_terminal
    assert loaded.lifecycle.state.outcome_class is not None


def test_lifecycle_parent_and_history_are_read_in_one_transaction(
    session: SessionWorld, monkeypatch: pytest.MonkeyPatch
) -> None:
    from northstar_infrastructure.persistence import (
        sqlite_equity_recommendation_read_only as reader_module,
    )

    seen: list[object] = []
    real_parent, real_history = reader_module._parent, reader_module._stored_lifecycle  # noqa: SLF001

    def parent(connection: sqlite3.Connection, issuance_id: str) -> Any:
        seen.append((connection, connection.in_transaction))
        return real_parent(connection, issuance_id)

    def history(connection: sqlite3.Connection, *args: Any) -> Any:
        seen.append((connection, connection.in_transaction))
        return real_history(connection, *args)

    monkeypatch.setattr(reader_module, "_parent", parent)
    monkeypatch.setattr(reader_module, "_stored_lifecycle", history)
    SQLiteReadOnlyEquityRecommendationLifecycleReader(session.database).load_lifecycle(
        session.terminal.recommendation.issuance_id
    )
    (first, open_first), (second, open_second) = seen
    assert first is second and open_first and open_second


def _event_column(database: Path, issuance_id: str, sequence: int, **columns: object) -> None:
    corrupt(
        database,
        f"UPDATE {LIFECYCLE_EVENTS} SET "  # noqa: S608
        + ", ".join(f"{name} = ?" for name in columns)
        + " WHERE issuance_id = ? AND event_sequence = ?",
        (*columns.values(), issuance_id, sequence),
        drop=(f"{LIFECYCLE_EVENTS}_no_update",),
        checks=False,
    )


@pytest.mark.parametrize(
    ("change", "category"),
    [
        ({"evaluated_at_us": 1}, _Lifecycle.EVENT),
        ({"event_sequence": 7}, _Lifecycle.HISTORY),
    ],
    ids=["column-disagrees", "sequence-gap"],
)
def test_lifecycle_corruption_is_classified_exactly_as_the_accepted_store_does(
    session: SessionWorld, change: dict[str, object], category: str
) -> None:
    identity = session.terminal.recommendation.issuance_id
    _event_column(session.database, identity, 2, **change)
    for adapter in (
        SQLiteReadOnlyEquityRecommendationLifecycleReader(session.database),
        SQLiteEquityRecommendationLifecycleStore(session.database),
    ):
        with pytest.raises(_Lifecycle) as raised:
            adapter.load_lifecycle(identity)
        assert raised.value.category == category


def test_lifecycle_corrupt_parent_authority_is_the_ei6a_error_unchanged(
    session: SessionWorld,
) -> None:
    corrupt(
        session.database,
        f"UPDATE {ISSUANCES} SET symbol = 'INFY' WHERE recording_sequence = 2",  # noqa: S608
        drop=(f"{ISSUANCES}_no_update",),
        checks=False,
    )
    with pytest.raises(_Authority) as raised:
        SQLiteReadOnlyEquityRecommendationLifecycleReader(session.database).load_lifecycle(
            session.terminal.recommendation.issuance_id
        )
    assert raised.value.category == _Authority.ISSUANCE


def test_lifecycle_unavailable_storage_is_typed_and_distinct_from_corruption(
    tmp_path: Path,
) -> None:
    with pytest.raises(EquityRecommendationLifecycleStoreUnavailableError) as raised:
        SQLiteReadOnlyEquityRecommendationLifecycleReader(tmp_path / "x.sqlite3").load_lifecycle(
            _UNKNOWN_ISSUANCE
        )
    assert not isinstance(raised.value, _Lifecycle | _Authority)


# ======================================================================
# Generation reader (EI-4D), including the frozen version-1 reference
# ======================================================================


def _generation_keys(database: Path) -> tuple[list[str], list[str]]:
    with closing(raw(database)) as connection:
        keys = [r[0] for r in connection.execute(f"SELECT generation_key FROM {DISPOSITIONS}")]  # noqa: S608
        attempts = [r[0] for r in connection.execute(f"SELECT attempt_identity FROM {ATTEMPTS}")]  # noqa: S608
    return keys, attempts


def test_generation_reads_equal_the_accepted_store_for_every_stored_record(
    session: SessionWorld,
) -> None:
    legacy = SQLiteEquityRecommendationGenerationStore(session.database)
    reader = SQLiteReadOnlyEquityRecommendationGenerationReader(session.database)
    keys, attempts = _generation_keys(session.database)
    assert len(keys) == 3 and len(attempts) >= 3
    for key in keys:
        loaded = _sealed(session.database, lambda k=key: reader.load_disposition(k))
        assert loaded == legacy.load_disposition(key) and loaded.generation_key == key
    for identity in attempts:
        assert reader.load_attempt(identity) == legacy.load_attempt(identity)
    assert reader.load_disposition("eirk1-sha256:" + "0" * 64) is None
    assert reader.load_attempt("eirat1-sha256:" + "0" * 64) is None
    issued = {r.recommendation.generation_key for r in session.issuances}
    assert issued == set(keys)  # each issuance's exact generation key resolves


def test_generation_the_frozen_version_1_reference_reads_back_unchanged(tmp_path: Path) -> None:
    world = GenerationWorld(tmp_path / "v1.sqlite3")
    _plant_reference(world)
    legacy = SQLiteEquityRecommendationGenerationStore(world.database)
    reader = SQLiteReadOnlyEquityRecommendationGenerationReader(world.database)
    disposition = _sealed(world.database, lambda: reader.load_disposition(_V1_KEY))
    assert disposition == legacy.load_disposition(_V1_KEY)
    assert disposition.evaluator_version == EQUITY_RECOMMENDATION_EVALUATOR_VERSION  # /1
    for identity in (_V1_KEYED, _V1_KEYLESS):
        assert reader.load_attempt(identity) == legacy.load_attempt(identity)


@pytest.mark.parametrize(
    ("column", "value", "error"),
    [
        ("symbol", "INFY", InvalidEquityRecommendationRecordError),
        ("assessment_document", "{}", InvalidEquityRecommendationRecordError),
        (
            "evaluator_version",
            "northstar.equity-recommendation-evaluator/9",
            IncompatibleEquityRecommendationRecordError,
        ),
    ],
)
def test_generation_corruption_is_the_record_error_exactly_as_the_accepted_store(
    session: SessionWorld, column: str, value: str, error: type[Exception]
) -> None:
    key = session.quiet.recommendation.generation_key
    corrupt(
        session.database,
        f"UPDATE {DISPOSITIONS} SET {column} = ? WHERE generation_key = ?",  # noqa: S608
        (value, key),
        drop=(f"{DISPOSITIONS}_no_update",),
        checks=False,
    )
    for adapter in (
        SQLiteReadOnlyEquityRecommendationGenerationReader(session.database),
        SQLiteEquityRecommendationGenerationStore(session.database),
    ):
        with pytest.raises(error):
            adapter.load_disposition(key)


def test_generation_unavailable_storage_is_typed(tmp_path: Path) -> None:
    reader = SQLiteReadOnlyEquityRecommendationGenerationReader(tmp_path / "x.sqlite3")
    for call in (lambda: reader.load_disposition("k"), lambda: reader.load_attempt("a")):
        with pytest.raises(EquityRecommendationGenerationStoreUnavailableError):
            call()
    assert list(tmp_path.iterdir()) == []


# ======================================================================
# Research timeline reader (EI-3C)
# ======================================================================


def test_timeline_reads_equal_the_accepted_store_for_every_stored_timeline(
    session: SessionWorld,
) -> None:
    legacy = SQLiteEquityIntradayResearchTimelineStore(session.database)
    reader = SQLiteReadOnlyEquityIntradayResearchTimelineReader(session.database)
    for strategy, listing in (
        (CONFIG.strategy, TCS),
        (configuration(1).strategy, TCS),
        (configuration(2).strategy, TCS),
        (CONFIG.strategy, INFY),
    ):
        loaded = _sealed(session.database, lambda s=strategy, li=listing: reader.load(s, li, DAY))
        assert loaded is not None and loaded == legacy.load(strategy, listing, DAY)
        assert [e.sequence for e in loaded.entries] == list(range(1, len(loaded.entries) + 1))
    assert reader.load(CONFIG.strategy, TCS, date(2026, 8, 20)) is None
    assert reader.load(CONFIG.strategy, ListingReference(Symbol("WIPRO"), NSE), DAY) is None


@pytest.mark.parametrize(
    ("column", "value"),
    [("decision_instant_us", 1), ("role", "UNKNOWN_ROLE"), ("sequence", 9)],
)
def test_timeline_corruption_is_integrity_here_and_unavailability_in_the_old_store(
    session: SessionWorld, column: str, value: object
) -> None:
    timeline = _timeline_id(session.database, CONFIG.strategy.identity, "TCS")
    corrupt(
        session.database,
        f"UPDATE {ENTRIES} SET {column} = ? WHERE timeline_id = ? AND sequence = 2",  # noqa: S608
        (value, timeline),
        drop=(f"{ENTRIES}_no_update",),
        checks=False,
    )
    with pytest.raises(InvalidStoredEquityIntradayResearchTimelineError) as raised:
        SQLiteReadOnlyEquityIntradayResearchTimelineReader(session.database).load(
            CONFIG.strategy, TCS, DAY
        )
    assert not isinstance(raised.value, EquityIntradayResearchTimelineUnavailableError)
    with pytest.raises(EquityIntradayResearchTimelineStorageError):  # the accepted contract
        SQLiteEquityIntradayResearchTimelineStore(session.database).load(CONFIG.strategy, TCS, DAY)


def test_timeline_decision_fingerprints_are_verified_by_application_rehydration_not_here(
    session: SessionWorld,
) -> None:
    """EI3-D13: the reader returns stored records; re-deriving decisions is the Application's.

    A decision fingerprint that no longer matches is structurally consistent
    text, so both stores return it exactly as stored; the EI-7C research query
    refuses it by rehydration (tests/test_equity_read_only_application_integration).
    """
    timeline = _timeline_id(session.database, CONFIG.strategy.identity, "TCS")
    corrupt(
        session.database,
        f"UPDATE {ENTRIES} SET decision_fingerprint = ? WHERE timeline_id = ? AND sequence = 2",  # noqa: S608
        ("eir1-sha256:" + "0" * 64, timeline),
        drop=(f"{ENTRIES}_no_update",),
    )
    loaded = SQLiteReadOnlyEquityIntradayResearchTimelineReader(session.database).load(
        CONFIG.strategy, TCS, DAY
    )
    assert loaded == SQLiteEquityIntradayResearchTimelineStore(session.database).load(
        CONFIG.strategy, TCS, DAY
    )
    assert loaded is not None and loaded.entries[1].decision_fingerprint.endswith("0" * 64)


def test_timeline_a_configuration_document_that_is_not_canonical_is_integrity(
    session: SessionWorld,
) -> None:
    timeline = _timeline_id(session.database, CONFIG.strategy.identity, "INFY")
    row = _column(session.database, "equity_intraday_research_timelines", "timeline_id", timeline)
    loose = json.dumps(json.loads(row[6]), indent=1)  # same content, not its canonical encoding
    corrupt(
        session.database,
        "UPDATE equity_intraday_research_timelines SET configuration_document = ? "
        "WHERE timeline_id = ?",
        (loose, timeline),
        drop=("equity_intraday_research_timelines_no_update",),
        checks=False,
    )
    with pytest.raises(InvalidStoredEquityIntradayResearchTimelineError):
        SQLiteReadOnlyEquityIntradayResearchTimelineReader(session.database).load(
            CONFIG.strategy, INFY, DAY
        )


def test_timeline_unavailable_storage_is_typed_and_not_integrity(tmp_path: Path) -> None:
    with pytest.raises(EquityIntradayResearchTimelineUnavailableError) as raised:
        SQLiteReadOnlyEquityIntradayResearchTimelineReader(tmp_path / "x.sqlite3").load(
            CONFIG.strategy, TCS, DAY
        )
    assert not isinstance(raised.value, InvalidStoredEquityIntradayResearchTimelineError)


def test_timeline_arguments_are_typed(tmp_path: Path) -> None:
    reader = SQLiteReadOnlyEquityIntradayResearchTimelineReader(tmp_path / "x.sqlite3")
    with pytest.raises(TypeError):
        reader.load("s", TCS, DAY)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        reader.load(CONFIG.strategy, TCS, "2026-08-19")  # type: ignore[arg-type]


# ======================================================================
# Evidence snapshot reader (EI-2B): the EI-7C limitation closed
# ======================================================================


def test_snapshot_reads_equal_the_accepted_store_for_every_stored_snapshot(
    session: SessionWorld,
) -> None:
    legacy = SQLiteEquityCandleEvidenceSnapshotStore(session.database)
    reader = SQLiteReadOnlyEquityCandleEvidenceSnapshotReader(session.database)
    identities = _snapshot_ids(session.database)
    assert len(identities) >= 9
    for identity in identities:
        loaded = _sealed(session.database, lambda i=identity: reader.get_snapshot(i))
        assert loaded == legacy.get_snapshot(identity) and loaded.snapshot_id == identity
        assert loaded.evidence_class is EquityCandleEvidenceClass.SYNTHETIC
    assert reader.get_snapshot(_UNKNOWN_SNAPSHOT) is None  # absent: never a placeholder


def test_snapshot_a_production_class_snapshot_is_returned_as_issued_never_relabelled(
    base: LifecycleWorld,
) -> None:
    production = plant_production_snapshot(base)
    loaded = SQLiteReadOnlyEquityCandleEvidenceSnapshotReader(base.database).get_snapshot(
        production.snapshot_id
    )
    assert loaded == production
    assert loaded.evidence_class is EquityCandleEvidenceClass.PRODUCTION  # gated by EI-7C


def _snapshot_column(database: Path, snapshot_id: str, **columns: object) -> None:
    corrupt(
        database,
        f"UPDATE {SNAPSHOTS} SET "  # noqa: S608
        + ", ".join(f"{name} = ?" for name in columns)
        + " WHERE snapshot_id = ?",
        (*columns.values(), snapshot_id),
        drop=(f"{SNAPSHOTS}_no_update",),
        checks=False,
    )


def _research_snapshot(database: Path) -> str:
    from tests.equity_read_only_fixtures import research_snapshot_ids

    return research_snapshot_ids(database)[1]  # the 09:41 CANDIDATE snapshot


@pytest.mark.parametrize(
    "corruption",
    ["column", "evidence-class", "document", "non-canonical", "entry-row", "journal-row"],
)
def test_snapshot_corruption_is_the_integrity_subclass_never_plain_unavailability(
    session: SessionWorld, corruption: str
) -> None:
    database, target = session.database, _research_snapshot(session.database)
    row = _column(database, SNAPSHOTS, "snapshot_id", target)
    document = row[-1]
    if corruption == "column":
        _snapshot_column(database, target, watermark=row[10] + 1)
    elif corruption == "evidence-class":
        _snapshot_column(database, target, evidence_class="PRODUCTION")
    elif corruption == "document":
        _snapshot_column(database, target, document=document.replace('"TCS"', '"INFY"', 1))
    elif corruption == "non-canonical":
        _snapshot_column(database, target, document=json.dumps(json.loads(document), indent=1))
    elif corruption == "entry-row":
        corrupt(
            database,
            "DELETE FROM equity_candle_evidence_snapshot_entries WHERE snapshot_id = ? "
            "AND sequence = (SELECT MIN(sequence) FROM equity_candle_evidence_snapshot_entries "
            "WHERE snapshot_id = ?)",
            (target, target),
            drop=("equity_candle_evidence_snapshot_entries_no_delete",),
        )
    else:
        corrupt(
            database,
            "DELETE FROM equity_candle_journal WHERE sequence = (SELECT MIN(sequence) FROM "
            "equity_candle_evidence_snapshot_entries WHERE snapshot_id = ?)",
            (target,),
            drop=("equity_candle_journal_no_delete",),
        )
    with pytest.raises(InvalidStoredEquityCandleEvidenceSnapshotError) as raised:
        _sealed(
            database,
            lambda: SQLiteReadOnlyEquityCandleEvidenceSnapshotReader(database).get_snapshot(target),
        )
    assert not raised.value.retryable
    assert isinstance(raised.value, EquityCandleEvidenceUnavailableError)  # EI-2B handlers hold
    assert document not in str(raised.value)  # no stored content in the message
    with pytest.raises(EquityCandleEvidenceStorageError) as old:  # the accepted contract
        SQLiteEquityCandleEvidenceSnapshotStore(database).get_snapshot(target)
    assert not isinstance(old.value, InvalidStoredEquityCandleEvidenceSnapshotError)


def test_snapshot_a_row_moved_to_another_identity_is_integrity(session: SessionWorld) -> None:
    database, target = session.database, _research_snapshot(session.database)
    moved = "ecs1-sha256:" + "a" * 64
    corrupt(
        database,
        f"UPDATE {SNAPSHOTS} SET snapshot_id = ? WHERE snapshot_id = ?",  # noqa: S608
        (moved, target),
        drop=(f"{SNAPSHOTS}_no_update",),
        checks=False,
    )
    reader = SQLiteReadOnlyEquityCandleEvidenceSnapshotReader(database)
    with pytest.raises(InvalidStoredEquityCandleEvidenceSnapshotError):
        reader.get_snapshot(moved)  # its document derives another identity
    assert reader.get_snapshot(target) is None  # and nothing is stored under the original


def test_snapshot_unavailable_storage_is_the_base_error_and_never_the_integrity_subclass(
    tmp_path: Path,
) -> None:
    reader = SQLiteReadOnlyEquityCandleEvidenceSnapshotReader(tmp_path / "x.sqlite3")
    with pytest.raises(EquityCandleEvidenceUnavailableError) as raised:
        reader.get_snapshot(_UNKNOWN_SNAPSHOT)
    assert not isinstance(raised.value, InvalidStoredEquityCandleEvidenceSnapshotError)
    assert list(tmp_path.iterdir()) == []


def test_snapshot_a_lock_is_unavailability_not_integrity(
    session: SessionWorld, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(connection_module, "READ_ONLY_BUSY_TIMEOUT_SECONDS", 0.05)
    target = _research_snapshot(session.database)
    holder = raw(session.database)
    try:
        holder.execute("BEGIN EXCLUSIVE")
        with pytest.raises(EquityCandleEvidenceUnavailableError) as raised:
            SQLiteReadOnlyEquityCandleEvidenceSnapshotReader(session.database).get_snapshot(target)
        assert not isinstance(raised.value, InvalidStoredEquityCandleEvidenceSnapshotError)
    finally:
        holder.execute("ROLLBACK")
        holder.close()


def test_snapshot_missing_evidence_tables_are_unavailable_and_unchanged(tmp_path: Path) -> None:
    database = tmp_path / "other.sqlite3"
    with closing(sqlite3.connect(database)) as connection:
        connection.execute("CREATE TABLE unrelated (x INTEGER)")
        connection.commit()
    before = schema_state(database)
    with pytest.raises(EquityCandleEvidenceUnavailableError) as raised:
        SQLiteReadOnlyEquityCandleEvidenceSnapshotReader(database).get_snapshot(_UNKNOWN_SNAPSHOT)
    assert not isinstance(raised.value, InvalidStoredEquityCandleEvidenceSnapshotError)
    assert schema_state(database) == before
