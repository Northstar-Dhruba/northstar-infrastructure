"""Read-side generation-audit completeness and same-database integrity (F-5D-1).

Every committed recommendation definition is readable only with exactly one
valid RECORDED generation audit that recorded it. Invalid states are built in
isolated synthetic databases with raw SQL -- foreign keys are off on a plain
connection -- never by weakening a production constraint.

Synthetic data only: contracts, instants, levels, candles, policy and profile
are fixtures. Nothing here reads a provider.
"""

from __future__ import annotations

import dataclasses
import sqlite3
from contextlib import closing
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from northstar_application.application_services import (
    FuturesRecommendationGenerationAudit,
    FuturesTradeRecommendationGenerationProfile,
    FuturesTradeRecommendationIdentityService,
)
from northstar_application.ports import (
    FuturesDailyBarFinality,
    FuturesDailyBarFinalityOutcome,
    FuturesTradeRecommendationExclusivityError,
    FuturesTradeRecommendationLifecycleEvidenceError,
    FuturesTradeRecommendationObservationStep,
    FuturesTradeRecommendationQuery,
    FuturesTradeRecommendationTimeExpiryStep,
    apply_futures_trade_recommendation_lifecycle_step,
)
from northstar_core.derivatives import ExpirationDate, QuoteValue
from northstar_core.foundation.value_objects import (
    ExchangeCode,
    PointInTime,
    Quantity,
    Symbol,
    Timeframe,
)
from northstar_core.futures import FuturesContract, FuturesOHLCVBar, FuturesProductReference
from northstar_core.recommendations.futures import (
    CoverageDeficiency,
    EntryZone,
    FuturesRecommendationGenerationPolicy,
    FuturesResearchDecisionReference,
    FuturesTradeRecommendation,
    FuturesTradeRecommendationLifecycle,
    ObservationCoverage,
    PriceTarget,
    RecommendationPolicyVersion,
    RecommendationStatus,
    RecommendationValidity,
    StopLoss,
    TimeExpiryEvidence,
    TradeDirection,
)
from northstar_core.strategy import ExplanationReason, StrategyIdentity

import northstar_infrastructure.persistence.sqlite_futures_trade_recommendations as module
from northstar_infrastructure.market_data import SQLiteFuturesHistoricalMarketDataStore
from northstar_infrastructure.persistence import (
    FuturesTradeRecommendationStorageError,
    SQLiteFuturesRecommendationGenerationAuditStore,
    SQLiteFuturesTradeRecommendationLifecycleStore,
    SQLiteFuturesTradeRecommendationRepository,
    SQLiteFuturesTradeRecommendationStore,
)
from northstar_infrastructure.persistence import (
    sqlite_futures_recommendation_generation_audits as audits,
)
from tests.futures_recommendation_audit_fixtures import SYNTHETIC_SESSION, recording_audit

_NIFTY = FuturesProductReference(Symbol("NIFTY"), ExchangeCode("NSE"))
_OCT = FuturesContract(_NIFTY, ExpirationDate("2026-10-27"))
_NOV = FuturesContract(_NIFTY, ExpirationDate("2026-11-24"))
_DEC = FuturesContract(_NIFTY, ExpirationDate("2026-12-29"))
_SGX_OCT = FuturesContract(
    FuturesProductReference(Symbol("NIFTY"), ExchangeCode("SGX")), ExpirationDate("2026-10-27")
)
_STRATEGY = StrategyIdentity("synthetic-directional-v1")
_POLICY = FuturesRecommendationGenerationPolicy(
    version=RecommendationPolicyVersion("f4-synthetic-fixture-v1"),
    approved_strategies=(_STRATEGY,),
    lookback_changes=19,
    entry_multiplier=Decimal("0.25"),
    stop_multiplier=Decimal("1.5"),
    target_multiplier=Decimal("3"),
    price_increment=Decimal("0.05"),
    min_reward_to_risk=Decimal("1.5"),
    short_enabled=True,
)
_PROFILE = FuturesTradeRecommendationGenerationProfile(
    "f5a-synthetic-profile-v1", 5, _POLICY.version
)
_QUERY = FuturesTradeRecommendationQuery(_NIFTY)


def _q(value: str) -> QuoteValue:
    return QuoteValue(Decimal(value))


def _recommendation(
    contract: FuturesContract = _OCT,
    *,
    decided: str = "2026-10-07T10:00:00Z",
    issued_at: str = "2026-10-07T12:00:00Z",
) -> FuturesTradeRecommendation:
    """LONG: entry 24378-24388, stop 24347.75, target 24448.45, valid 10-08 to 10-14."""
    source = FuturesResearchDecisionReference(
        contract, Timeframe("1d"), PointInTime(decided), _STRATEGY
    )
    return FuturesTradeRecommendation(
        identity=FuturesTradeRecommendationIdentityService().identity(source, _PROFILE),
        contract=contract,
        direction=TradeDirection.LONG,
        entry=EntryZone(_q("24378"), _q("24388")),
        stop=StopLoss(_q("24347.75")),
        target=PriceTarget(_q("24448.45")),
        validity=RecommendationValidity(
            PointInTime("2026-10-08T10:00:00Z"),
            PointInTime("2026-10-14T10:00:00Z"),
            PointInTime("2026-10-27T10:00:00Z"),
        ),
        source=source,
        policy_version=_POLICY.version,
        issued_at=PointInTime(issued_at),
        thesis=(ExplanationReason("Synthetic thesis"),),
    )


def _bar(day: int, low: str, high: str, contract: FuturesContract = _OCT) -> FuturesOHLCVBar:
    middle = (Decimal(low) + Decimal(high)) / 2
    return FuturesOHLCVBar(
        contract=contract,
        point_in_time=PointInTime(f"2026-10-{day:02d}T10:00:00Z"),
        timeframe=Timeframe("1d"),
        open=QuoteValue(middle),
        high=_q(high),
        low=_q(low),
        close=QuoteValue(middle),
        volume=Quantity(Decimal("1250")),
    )


def _observe(day: int, low: str = "24400", high: str = "24420", contract=_OCT):
    return FuturesTradeRecommendationObservationStep(
        _bar(day, low, high, contract), date(2026, 10, day)
    )


@pytest.fixture
def database(tmp_path: Path) -> Path:
    return tmp_path / "northstar.sqlite3"


def _record(database: Path, *recommendations: FuturesTradeRecommendation, audit=None) -> int:
    return SQLiteFuturesTradeRecommendationStore(database).record(
        recommendations,
        policy=_POLICY,
        profile=_PROFILE,
        audit=audit or recording_audit(*recommendations, profile=_PROFILE),
    )


def _repository(database: Path) -> SQLiteFuturesTradeRecommendationRepository:
    return SQLiteFuturesTradeRecommendationRepository(database)


def _progress(database: Path, previous, *steps) -> FuturesTradeRecommendationLifecycle:
    current = previous
    for step in steps:
        current = apply_futures_trade_recommendation_lifecycle_step(current, step)
    bars = tuple(s.bar for s in steps if isinstance(s, FuturesTradeRecommendationObservationStep))
    if bars:
        SQLiteFuturesHistoricalMarketDataStore(database).store(bars)
    SQLiteFuturesTradeRecommendationLifecycleStore(database).record_progress(
        previous, current, steps=steps
    )
    return current


def _expire(database: Path, recommendation: FuturesTradeRecommendation) -> None:
    initial = FuturesTradeRecommendationLifecycle(recommendation)
    _progress(
        database,
        initial,
        FuturesTradeRecommendationTimeExpiryStep(
            TimeExpiryEvidence(
                as_of=PointInTime("2026-10-14T12:00:00Z"),
                evaluated_through=None,
                coverage=ObservationCoverage.INCOMPLETE_OR_UNVERIFIED_COVERAGE,
                attested_by="synthetic coverage fixture",
                deficiency=CoverageDeficiency.MISSING_OBSERVATIONS,
            )
        ),
    )


def _rows(database: Path, table: str) -> list[tuple]:
    with closing(sqlite3.connect(database)) as connection:
        return connection.execute(f"SELECT * FROM {table} ORDER BY 1, 2").fetchall()  # noqa: S608


def _execute(database: Path, *statements: tuple[str, tuple]) -> None:
    """Tamper with raw SQL on a plain connection, where foreign keys are off."""
    with closing(sqlite3.connect(database)) as connection:
        for sql, parameters in statements:
            connection.execute(sql, parameters)
        connection.commit()


def _everything(database: Path) -> tuple:
    return tuple(
        _rows(database, table)
        for table in (
            "futures_recommendation_configurations",
            "futures_trade_recommendations",
            "futures_trade_recommendation_steps",
            "futures_recommendation_generation_audits",
            "futures_recommendation_generation_audit_recommendations",
        )
    )


def _assert_unreadable(database: Path, recommendation: FuturesTradeRecommendation, match=None):
    with pytest.raises(FuturesTradeRecommendationStorageError, match=match):
        _repository(database).get_recommendation(recommendation.identity)
    with pytest.raises(FuturesTradeRecommendationStorageError, match=match):
        _repository(database).get_lifecycles(_QUERY)


def _recorded_audit_identity(database: Path, recommendation: FuturesTradeRecommendation) -> str:
    ((identity,),) = (
        row[:1]
        for row in _rows(database, "futures_recommendation_generation_audit_recommendations")
        if row[1] == recommendation.identity.identity and row[2] == "RECORDED"
    )
    return identity


def _replace_audit(
    database: Path,
    recommendation: FuturesTradeRecommendation,
    audit: FuturesRecommendationGenerationAudit,
) -> None:
    """Swap a recommendation's RECORDED audit for ``audit`` -- isolated tampering only."""
    old = _recorded_audit_identity(database, recommendation)
    with closing(sqlite3.connect(database)) as connection:
        connection.execute(
            "DELETE FROM futures_recommendation_generation_audit_recommendations "
            "WHERE audit_identity = ?",
            (old,),
        )
        connection.execute(
            "DELETE FROM futures_recommendation_generation_audits WHERE audit_identity = ?", (old,)
        )
        audits.insert_audit(connection, audit)
        connection.commit()


def _legacy(database: Path, recommendation: FuturesTradeRecommendation) -> None:
    """Insert a definition as a pre-audit build would have: canonical, but never audited."""
    with closing(sqlite3.connect(database)) as connection:
        module.initialize_futures_trade_recommendation_schema(connection)
    configuration_row = module._configuration_row(_POLICY, _PROFILE)
    row = module._definition_row(
        recommendation, _PROFILE.version, module._configuration_digest(configuration_row)
    )
    with closing(sqlite3.connect(database)) as connection:
        if not connection.execute(
            "SELECT 1 FROM futures_recommendation_configurations WHERE profile_version = ?",
            (_PROFILE.version,),
        ).fetchone():
            connection.execute(module._INSERT_CONFIGURATION, configuration_row)
        connection.execute(module._INSERT_DEFINITION, row)
        connection.commit()


# ---------------------------------------------------------------------------
# Valid associations
# ---------------------------------------------------------------------------


def test_a_recorded_recommendation_reads_with_its_audit(database: Path) -> None:
    recommendation = _recommendation()
    _record(database, recommendation)

    assert _repository(database).get_recommendation(recommendation.identity) == recommendation
    (lifecycle,) = _repository(database).get_lifecycles(_QUERY)
    assert lifecycle == FuturesTradeRecommendationLifecycle(recommendation)


def test_a_multi_step_terminal_lifecycle_reads_with_its_audit(database: Path) -> None:
    recommendation = _recommendation()
    _record(database, recommendation)
    closed = _progress(
        database,
        FuturesTradeRecommendationLifecycle(recommendation),
        _observe(8),
        _observe(9, "24380", "24400"),
        _observe(12, "24400", "24450"),
    )

    (lifecycle,) = SQLiteFuturesTradeRecommendationRepository(str(database)).get_lifecycles(_QUERY)
    assert lifecycle == closed and lifecycle.status is RecommendationStatus.CLOSED_TARGET


def test_sequential_recommendations_after_terminal_progression_all_read(database: Path) -> None:
    created = []
    for contract in (_OCT, _NOV, _DEC):
        recommendation = _recommendation(contract)
        _record(database, recommendation)
        _expire(database, recommendation)
        created.append(recommendation)

    lifecycles = _repository(database).get_lifecycles(_QUERY)

    assert {lc.recommendation for lc in lifecycles} == set(created)
    assert all(lc.is_terminal for lc in lifecycles)


def test_a_replayed_recording_returns_the_same_audited_definition(database: Path) -> None:
    recommendation = _recommendation()
    _record(database, recommendation)
    before = _everything(database)

    assert _record(database, recommendation) == 1
    assert _everything(database) == before
    assert _repository(database).get_recommendation(recommendation.identity) == recommendation


def test_a_multi_recommendation_batch_is_one_product_and_refused_by_p1(database: Path) -> None:
    """An audited batch is one contract's attempt, so two recommendations meet P-1.

    The batch's first definition is inserted, unaudited, inside the transaction;
    it is never read back, so the refusal is the business exclusivity error, not
    a storage error, and nothing is committed.
    """
    first = _recommendation()
    sibling = _recommendation(decided="2026-10-06T10:00:00Z")

    with pytest.raises(FuturesTradeRecommendationExclusivityError):
        _record(database, first, sibling)
    assert _everything(database) == ([], [], [], [], [])


def test_recording_after_a_terminal_recommendation_replays_it_with_its_audit(
    database: Path,
) -> None:
    first = _recommendation(_OCT)
    _record(database, first)
    _expire(database, first)

    assert _record(database, _recommendation(_NOV)) == 1
    assert len(_repository(database).get_lifecycles(_QUERY)) == 2


# ---------------------------------------------------------------------------
# Missing, contradictory and corrupt audit evidence
# ---------------------------------------------------------------------------


def test_a_legacy_unaudited_definition_fails_closed_everywhere(database: Path) -> None:
    legacy = _recommendation()
    _legacy(database, legacy)
    before = _everything(database)

    _assert_unreadable(database, legacy, match="0 RECORDED")
    with pytest.raises(FuturesTradeRecommendationStorageError):
        _record(database, _recommendation(_NOV))
    with pytest.raises(FuturesTradeRecommendationStorageError):
        SQLiteFuturesTradeRecommendationLifecycleStore(database).record_progress(
            FuturesTradeRecommendationLifecycle(legacy),
            FuturesTradeRecommendationLifecycle(legacy).expire(
                TimeExpiryEvidence(
                    as_of=PointInTime("2026-10-14T12:00:00Z"),
                    evaluated_through=None,
                    coverage=ObservationCoverage.INCOMPLETE_OR_UNVERIFIED_COVERAGE,
                    attested_by="synthetic",
                    deficiency=CoverageDeficiency.MISSING_OBSERVATIONS,
                )
            ),
            steps=(
                FuturesTradeRecommendationTimeExpiryStep(
                    TimeExpiryEvidence(
                        as_of=PointInTime("2026-10-14T12:00:00Z"),
                        evaluated_through=None,
                        coverage=ObservationCoverage.INCOMPLETE_OR_UNVERIFIED_COVERAGE,
                        attested_by="synthetic",
                        deficiency=CoverageDeficiency.MISSING_OBSERVATIONS,
                    )
                ),
            ),
        )
    assert _everything(database) == before  # nothing backfilled, repaired or deleted


def test_a_missing_recorded_reference_fails_closed(database: Path) -> None:
    recommendation = _recommendation()
    _record(database, recommendation)
    _execute(
        database,
        (
            "DELETE FROM futures_recommendation_generation_audit_recommendations "
            "WHERE role = 'RECORDED'",
            (),
        ),
    )

    _assert_unreadable(database, recommendation, match="0 RECORDED")


def test_a_reference_to_an_absent_audit_fails_closed(database: Path) -> None:
    recommendation = _recommendation()
    _record(database, recommendation)
    _execute(database, ("DELETE FROM futures_recommendation_generation_audits", ()))

    _assert_unreadable(database, recommendation, match="missing")


def test_a_recorded_reference_to_a_non_recorded_audit_fails_closed(database: Path) -> None:
    recommendation = _recommendation()
    _record(database, recommendation)
    existing = dataclasses.replace(
        recording_audit(recommendation, profile=_PROFILE),
        attempted_at=PointInTime("2026-10-07T13:00:00Z"),
        proposed=(),
        recorded=(),
        existing=(recommendation.identity,),
    )
    _replace_audit(database, recommendation, existing)
    _execute(
        database,
        (
            "UPDATE futures_recommendation_generation_audit_recommendations SET role = 'RECORDED'",
            (),
        ),
    )

    _assert_unreadable(database, recommendation)


@pytest.mark.parametrize(
    "overrides",
    [
        {"attempted_at": PointInTime("2026-10-07T12:00:01Z")},
        {"strategy": StrategyIdentity("synthetic-other-v1"), "candidates": ()},
        {"policy_version": RecommendationPolicyVersion("f4-synthetic-fixture-v2")},
        {"profile_version": "f5a-synthetic-profile-v2"},
        {
            "contract": _NOV,
            "finality": FuturesDailyBarFinality(
                _NOV, SYNTHETIC_SESSION, FuturesDailyBarFinalityOutcome.FINAL, "synthetic"
            ),
            "candidates": (),
        },
        {
            "contract": _SGX_OCT,
            "finality": FuturesDailyBarFinality(
                _SGX_OCT, SYNTHETIC_SESSION, FuturesDailyBarFinalityOutcome.FINAL, "synthetic"
            ),
            "candidates": (),
        },
    ],
    ids=["issued-at", "strategy", "policy", "profile", "contract", "exchange"],
)
def test_an_audit_that_does_not_describe_the_definition_fails_closed(
    database: Path, overrides: dict
) -> None:
    recommendation = _recommendation()
    _record(database, recommendation)
    _replace_audit(
        database, recommendation, recording_audit(recommendation, profile=_PROFILE, **overrides)
    )

    _assert_unreadable(database, recommendation, match="does not record this definition")


def test_an_audit_recording_another_identity_fails_closed(database: Path) -> None:
    recommendation = _recommendation()
    other = _recommendation(decided="2026-10-06T10:00:00Z")
    _record(database, recommendation)
    audit = recording_audit(other, profile=_PROFILE)
    _replace_audit(database, recommendation, audit)
    _execute(
        database,
        (
            "INSERT INTO futures_recommendation_generation_audit_recommendations "
            "VALUES (?, ?, 'EXISTING')",
            (audit.identity, recommendation.identity.identity),
        ),
        (
            "UPDATE futures_recommendation_generation_audit_recommendations SET role = 'RECORDED' "
            "WHERE recommendation_identity = ?",
            (recommendation.identity.identity,),
        ),
    )

    _assert_unreadable(database, recommendation)


@pytest.mark.parametrize(
    "sql",
    [
        "UPDATE futures_recommendation_generation_audits SET audit = 'not json'",
        "UPDATE futures_recommendation_generation_audits SET audit = replace(audit, ',', ', ')",
        "UPDATE futures_trade_recommendations "
        "SET definition = replace(definition, '24448.45', '24448.50')",
        "UPDATE futures_recommendation_configurations SET validity_sessions = '6'",
    ],
    ids=["corrupt-audit-json", "non-canonical-audit-json", "modified-definition", "config-seal"],
)
def test_corrupt_evidence_still_fails_closed(database: Path, sql: str) -> None:
    recommendation = _recommendation()
    _record(database, recommendation)
    _execute(database, (sql, ()))

    _assert_unreadable(database, recommendation)


def test_a_second_recorded_audit_is_refused_by_the_database(database: Path) -> None:
    recommendation = _recommendation()
    _record(database, recommendation)
    second = dataclasses.replace(
        recording_audit(recommendation, profile=_PROFILE),
        attempted_at=PointInTime("2026-10-07T12:30:00Z"),
    )

    with closing(sqlite3.connect(database)) as connection, pytest.raises(sqlite3.IntegrityError):
        audits.insert_audit(connection, second)


def test_two_recorded_audits_are_refused_on_read_even_without_the_index(database: Path) -> None:
    recommendation = _recommendation()
    _record(database, recommendation)
    _execute(database, ("DROP INDEX futures_recommendation_generation_audit_recorded", ()))
    second = dataclasses.replace(
        recording_audit(recommendation, profile=_PROFILE),
        attempted_at=PointInTime("2026-10-07T12:30:00Z"),
    )
    with closing(sqlite3.connect(database)) as connection:
        audits.insert_audit(connection, second)
        connection.commit()

    # Every read re-runs the schema initializer, whose CREATE UNIQUE INDEX IF NOT
    # EXISTS cannot rebuild the dropped index over the duplicate: the database is
    # refused as unavailable before any definition is decoded. Either way it fails
    # closed with the typed storage error.
    _assert_unreadable(database, recommendation)


# ---------------------------------------------------------------------------
# Atomic recording is unchanged
# ---------------------------------------------------------------------------


def test_a_failed_audit_insert_commits_no_definition(database: Path) -> None:
    recommendation = _recommendation()
    with closing(sqlite3.connect(database)) as connection:
        module.initialize_futures_trade_recommendation_schema(connection)
    _execute(
        database,
        (
            "CREATE TRIGGER fail_audit BEFORE INSERT ON futures_recommendation_generation_audits "
            "BEGIN SELECT RAISE(ABORT, 'synthetic audit failure'); END",
            (),
        ),
    )

    with pytest.raises(FuturesTradeRecommendationStorageError):
        _record(database, recommendation)
    assert _repository(database).get_recommendation(recommendation.identity) is None
    assert _everything(database) == ([], [], [], [], [])


def test_a_committed_recording_is_immediately_readable(database: Path) -> None:
    recommendation = _recommendation()

    assert _record(database, recommendation) == 1

    assert _repository(database).get_recommendation(recommendation.identity) == recommendation
    assert len(_rows(database, "futures_recommendation_generation_audits")) == 1


def test_cross_expiry_exclusivity_is_unchanged(database: Path) -> None:
    _record(database, _recommendation(_OCT))
    before = _everything(database)

    with pytest.raises(FuturesTradeRecommendationExclusivityError):
        _record(database, _recommendation(_NOV))
    assert _everything(database) == before


# ---------------------------------------------------------------------------
# One shared SQLite database (G3)
# ---------------------------------------------------------------------------


def test_definitions_audits_steps_and_candles_share_one_database(database: Path) -> None:
    recommendation = _recommendation()
    _record(database, recommendation)
    progressed = _progress(
        database, FuturesTradeRecommendationLifecycle(recommendation), _observe(8)
    )

    with closing(sqlite3.connect(database)) as connection:
        tables = {
            row[0]
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        }
    assert {
        "futures_ohlcv",
        "futures_trade_recommendations",
        "futures_recommendation_generation_audits",
        "futures_recommendation_generation_audit_recommendations",
        "futures_trade_recommendation_steps",
    } <= tables
    (lifecycle,) = SQLiteFuturesTradeRecommendationRepository(str(database)).get_lifecycles(_QUERY)
    assert lifecycle == progressed


def test_candles_stored_in_another_database_file_are_refused(tmp_path: Path) -> None:
    recommendations_db = tmp_path / "a" / "northstar.sqlite3"
    market_db = tmp_path / "b" / "northstar.sqlite3"
    recommendations_db.parent.mkdir()
    market_db.parent.mkdir()
    recommendation = _recommendation()
    _record(recommendations_db, recommendation)
    step = _observe(8)
    SQLiteFuturesHistoricalMarketDataStore(market_db).store((step.bar,))
    initial = FuturesTradeRecommendationLifecycle(recommendation)

    with pytest.raises(FuturesTradeRecommendationLifecycleEvidenceError):
        SQLiteFuturesTradeRecommendationLifecycleStore(recommendations_db).record_progress(
            initial, initial.advance(step.bar), steps=(step,)
        )
    assert _rows(recommendations_db, "futures_trade_recommendation_steps") == []


def test_a_candle_removed_from_the_shared_database_fails_reconstruction(database: Path) -> None:
    recommendation = _recommendation()
    _record(database, recommendation)
    _progress(database, FuturesTradeRecommendationLifecycle(recommendation), _observe(8))
    _execute(database, ("DELETE FROM futures_ohlcv", ()))

    with pytest.raises(FuturesTradeRecommendationStorageError):
        _repository(database).get_lifecycles(_QUERY)


def test_the_standalone_audit_store_shares_the_database(database: Path) -> None:
    recommendation = _recommendation()
    _record(database, recommendation)
    retry = dataclasses.replace(
        recording_audit(recommendation, profile=_PROFILE),
        attempted_at=PointInTime("2026-10-07T13:00:00Z"),
        proposed=(),
        recorded=(),
        existing=(recommendation.identity,),
    )

    assert SQLiteFuturesRecommendationGenerationAuditStore(database).record_audit(retry) == 1
    assert _repository(database).get_recommendation(recommendation.identity) == recommendation
