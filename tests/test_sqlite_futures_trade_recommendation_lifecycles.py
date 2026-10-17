"""Tests for durable futures trade recommendation lifecycle progress (F-5B-2A).

Synthetic data only: the contracts, instants, levels, bars, policy, profile and
time evidence are non-production fixtures. Nothing here reads a provider.
"""

from __future__ import annotations

import ast
import json
import sqlite3
import threading
from contextlib import closing
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from northstar_application.application_services import (
    FuturesTradeRecommendationGenerationProfile,
    FuturesTradeRecommendationIdentityService,
    futures_daily_candle_fingerprint,
)
from northstar_application.ports import (
    FuturesTradeRecommendationConfigurationConflictError,
    FuturesTradeRecommendationExclusivityError,
    FuturesTradeRecommendationLifecycleConflictError,
    FuturesTradeRecommendationLifecycleEvidenceError,
    FuturesTradeRecommendationLifecycleStore,
    FuturesTradeRecommendationObservationStep,
    FuturesTradeRecommendationQuery,
    FuturesTradeRecommendationTimeExpiryStep,
    InvalidFuturesTradeRecommendationLifecycleProgressError,
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
    IntrabarOrdering,
    ObservationCoverage,
    PriceTarget,
    RecommendationBoundary,
    RecommendationEventKind,
    RecommendationPolicyVersion,
    RecommendationStatus,
    RecommendationValidity,
    StopLoss,
    TimeExpiryEvidence,
    TradeDirection,
)
from northstar_core.strategy import ExplanationReason, StrategyIdentity

import northstar_infrastructure.persistence.sqlite_futures_trade_recommendation_steps as steps
import northstar_infrastructure.persistence.sqlite_futures_trade_recommendations as module
from northstar_infrastructure.market_data import SQLiteFuturesHistoricalMarketDataStore
from northstar_infrastructure.persistence import (
    FuturesTradeRecommendationStorageError,
    SQLiteFuturesTradeRecommendationLifecycleStore,
    SQLiteFuturesTradeRecommendationRepository,
    SQLiteFuturesTradeRecommendationStore,
)
from tests.futures_recommendation_audit_fixtures import recording_audit

Observation = FuturesTradeRecommendationObservationStep
Expiry = FuturesTradeRecommendationTimeExpiryStep
Lifecycle = FuturesTradeRecommendationLifecycle

_NIFTY = FuturesProductReference(Symbol("NIFTY"), ExchangeCode("NSE"))
_OCT = FuturesContract(_NIFTY, ExpirationDate("2026-10-27"))
_NOV = FuturesContract(_NIFTY, ExpirationDate("2026-11-24"))
_DEC = FuturesContract(_NIFTY, ExpirationDate("2026-12-29"))
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


def _q(value: str) -> QuoteValue:
    return QuoteValue(Decimal(value))


def _recommendation(
    contract: FuturesContract = _OCT,
    *,
    issued_at: str = "2026-10-07T12:00:00Z",
) -> FuturesTradeRecommendation:
    """LONG: entry 24378-24388, stop 24347.75, target 24448.45, valid 10-08 to 10-14."""
    source = FuturesResearchDecisionReference(
        contract, Timeframe("1d"), PointInTime("2026-10-07T10:00:00Z"), _STRATEGY
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
        thesis=(ExplanationReason("Synthetic thesis", ("synthetic signal",)),),
    )


_QUIET = ("24400", "24420")
_ENTRY = ("24380", "24400")
_STOP = ("24340", "24390")
_TARGET = ("24400", "24450")
_BOTH = ("24340", "24450")


def _bar(
    day: int,
    prices: tuple[str, str] = _QUIET,
    contract: FuturesContract = _OCT,
    timeframe: str = "1d",
) -> FuturesOHLCVBar:
    low, high = prices
    middle = (Decimal(low) + Decimal(high)) / 2
    return FuturesOHLCVBar(
        contract=contract,
        point_in_time=PointInTime(f"2026-10-{day:02d}T10:00:00Z"),
        timeframe=Timeframe(timeframe),
        open=QuoteValue(middle),
        high=_q(high),
        low=_q(low),
        close=QuoteValue(middle),
        volume=Quantity(Decimal("1250")),
    )


def _observe(day: int, prices: tuple[str, str] = _QUIET, **bar: object) -> Observation:
    return Observation(_bar(day, prices, **bar), date(2026, 10, day))  # type: ignore[arg-type]


def _expire(
    evaluated_through: int | None,
    coverage: ObservationCoverage = ObservationCoverage.INCOMPLETE_OR_UNVERIFIED_COVERAGE,
    *,
    as_of: str = "2026-10-14T12:00:00Z",
) -> Expiry:
    complete = coverage is ObservationCoverage.COMPLETE_VERIFIED_COVERAGE
    return Expiry(
        TimeExpiryEvidence(
            as_of=PointInTime(as_of),
            evaluated_through=(
                None
                if evaluated_through is None
                else PointInTime(f"2026-10-{evaluated_through:02d}T10:00:00Z")
            ),
            coverage=coverage,
            attested_by="synthetic coverage fixture",
            deficiency=None if complete else CoverageDeficiency.MISSING_OBSERVATIONS,
        )
    )


def _apply(lifecycle: Lifecycle, *evaluations: object) -> Lifecycle:
    for step in evaluations:
        lifecycle = apply_futures_trade_recommendation_lifecycle_step(lifecycle, step)  # type: ignore[arg-type]
    return lifecycle


@pytest.fixture
def database(tmp_path: Path) -> Path:
    return tmp_path / "northstar.sqlite3"


def _lifecycle_store(database: Path) -> SQLiteFuturesTradeRecommendationLifecycleStore:
    return SQLiteFuturesTradeRecommendationLifecycleStore(database)


def _record(database: Path, *recommendations: FuturesTradeRecommendation) -> int:
    return SQLiteFuturesTradeRecommendationStore(database).record(
        recommendations,
        policy=_POLICY,
        profile=_PROFILE,
        audit=recording_audit(*recommendations, profile=_PROFILE),
    )


def _recorded(
    database: Path, recommendation: FuturesTradeRecommendation | None = None
) -> Lifecycle:
    recommendation = recommendation or _recommendation()
    _record(database, recommendation)
    return Lifecycle(recommendation)


def _candles(database: Path, *evaluations: object) -> None:
    bars = tuple(step.bar for step in evaluations if isinstance(step, Observation))
    SQLiteFuturesHistoricalMarketDataStore(database).store(bars)


def _progress(database: Path, previous: Lifecycle, *evaluations: object) -> int:
    current = _apply(previous, *evaluations)
    return _lifecycle_store(database).record_progress(
        previous,
        current,
        steps=evaluations,  # type: ignore[arg-type]
    )


def _advance(database: Path, previous: Lifecycle, *evaluations: object) -> Lifecycle:
    _candles(database, *evaluations)
    assert _progress(database, previous, *evaluations) == len(evaluations)
    return _apply(previous, *evaluations)


def _lifecycles(database: Path) -> tuple[Lifecycle, ...]:
    return SQLiteFuturesTradeRecommendationRepository(database).get_lifecycles(
        FuturesTradeRecommendationQuery(_NIFTY)
    )


def _stored(database: Path) -> Lifecycle:
    (lifecycle,) = _lifecycles(database)
    return lifecycle


def _rows(database: Path, table: str = "futures_trade_recommendation_steps") -> list[tuple]:
    with closing(sqlite3.connect(database)) as connection:
        return connection.execute(f"SELECT * FROM {table} ORDER BY 1, 2").fetchall()  # noqa: S608


def _execute(database: Path, sql: str, parameters: tuple = ()) -> None:
    with closing(sqlite3.connect(database)) as connection:
        connection.execute(sql, parameters)
        connection.commit()


# ---------------------------------------------------------------------------
# Progress persistence
# ---------------------------------------------------------------------------


def test_a_definition_without_steps_reconstructs_waiting(database: Path) -> None:
    initial = _recorded(database)

    assert _stored(database) == initial
    assert _stored(database).status is RecommendationStatus.WAITING_FOR_ENTRY
    assert _rows(database) == []


def test_quiet_observations_are_recorded_and_advance_the_cursor(database: Path) -> None:
    current = _advance(database, _recorded(database), _observe(8), _observe(9))

    stored = _stored(database)
    assert stored == current
    assert stored.events == ()
    assert stored.last_observation == _bar(9)
    assert [(row[1], row[2], row[10], row[11], row[12]) for row in _rows(database)] == [
        (
            1,
            "OBSERVATION",
            '{"events":[],"time_evidence":null,"version":1}',
            "WAITING_FOR_ENTRY",
            0,
        ),
        (
            2,
            "OBSERVATION",
            '{"events":[],"time_evidence":null,"version":1}',
            "WAITING_FOR_ENTRY",
            0,
        ),
    ]


def test_an_observation_row_references_its_candle_by_key_label_and_fingerprint(
    database: Path,
) -> None:
    recommendation = _recommendation()
    _advance(database, _recorded(database, recommendation), _observe(8))

    ((row),) = _rows(database)
    assert row[:10] == (
        recommendation.identity.identity,
        1,
        "OBSERVATION",
        "NIFTY",
        "NSE",
        "2026-10-27",
        "1d",
        "2026-10-08T10:00:00Z",
        "2026-10-08",
        futures_daily_candle_fingerprint(_OCT, date(2026, 10, 8), _bar(8)),
    )


@pytest.mark.parametrize(
    "evaluations, status, kinds",
    [
        ((_observe(8, _ENTRY),), RecommendationStatus.ACTIVE, ["ENTRY_ACTIVATED"]),
        (
            (_observe(8, _ENTRY), _observe(9, _STOP)),
            RecommendationStatus.CLOSED_STOP,
            ["ENTRY_ACTIVATED", "STOP_REACHED"],
        ),
        (
            (_observe(8, _ENTRY), _observe(9, _TARGET)),
            RecommendationStatus.CLOSED_TARGET,
            ["ENTRY_ACTIVATED", "TARGET_REACHED"],
        ),
        (
            (_observe(8, _ENTRY), _observe(9, _BOTH)),
            RecommendationStatus.CLOSED_STOP,
            ["ENTRY_ACTIVATED", "STOP_REACHED"],
        ),
        ((_observe(8, _STOP),), RecommendationStatus.INDETERMINATE_ENTRY, ["ENTRY_INDETERMINATE"]),
        (
            (_observe(8), _observe(14)),
            RecommendationStatus.EXPIRED_BEFORE_ENTRY,
            ["VALIDITY_EXPIRED"],
        ),
        (
            (_observe(8), _observe(14, _ENTRY)),
            RecommendationStatus.EXPIRED_AFTER_ACTIVATION,
            ["ENTRY_ACTIVATED", "VALIDITY_EXPIRED"],
        ),
        (
            (_observe(8), _expire(8, ObservationCoverage.COMPLETE_VERIFIED_COVERAGE)),
            RecommendationStatus.EXPIRED_BEFORE_ENTRY,
            ["VALIDITY_EXPIRED_BY_TIME"],
        ),
        (
            (_expire(None),),
            RecommendationStatus.EXPIRED_WITH_INCOMPLETE_COVERAGE,
            ["VALIDITY_EXPIRED_BY_TIME"],
        ),
    ],
    ids=[
        "entry",
        "stop",
        "target",
        "modeled-stop-first",
        "ambiguous-entry",
        "bar-expiry",
        "entry-and-expiry-on-one-bar",
        "verified-time-expiry",
        "incomplete-time-expiry",
    ],
)
def test_every_f2_outcome_round_trips_exactly(
    database: Path, evaluations: tuple, status: RecommendationStatus, kinds: list[str]
) -> None:
    current = _advance(database, _recorded(database), *evaluations)

    stored = _stored(database)
    assert stored == current
    assert stored.status is status
    assert stored.is_terminal is status.is_terminal
    assert [event.kind.value for event in stored.events] == kinds
    assert stored.last_observation == current.last_observation
    final = _rows(database)[-1]
    assert (final[11], final[12]) == (status.value, int(status.is_terminal))


def test_the_modeled_stop_first_outcome_keeps_its_ordering(database: Path) -> None:
    _advance(database, _recorded(database), _observe(8, _ENTRY), _observe(9, _BOTH))

    stop = _stored(database).events[-1]
    assert stop.kind is RecommendationEventKind.STOP_REACHED
    assert stop.touched == (RecommendationBoundary.STOP, RecommendationBoundary.TARGET)
    assert stop.ordering is IntrabarOrdering.MODELED_STOP_FIRST
    assert stop.observation == _bar(9, _BOTH)


def test_an_ambiguous_entry_keeps_its_unknown_ordering(database: Path) -> None:
    _advance(database, _recorded(database), _observe(8, _STOP))

    (event,) = _stored(database).events
    assert event.ordering is IntrabarOrdering.UNKNOWN
    assert event.touched == (RecommendationBoundary.ENTRY, RecommendationBoundary.STOP)


def test_an_entry_and_expiry_on_one_bar_is_one_step_with_two_events(database: Path) -> None:
    _advance(database, _recorded(database), _observe(14, _ENTRY))

    ((row),) = _rows(database)
    assert [event["kind"] for event in json.loads(row[10])["events"]] == [
        "ENTRY_ACTIVATED",
        "VALIDITY_EXPIRED",
    ]


def test_time_expiry_evidence_is_persisted_as_supplied_without_a_candle(database: Path) -> None:
    current = _advance(database, _recorded(database), _expire(None))

    ((row),) = _rows(database)
    assert row[2] == "TIME_EXPIRY"
    assert row[3:10] == (None,) * 7
    assert json.loads(row[10])["time_evidence"] == {
        "as_of": "2026-10-14T12:00:00Z",
        "evaluated_through": None,
        "coverage": "INCOMPLETE_OR_UNVERIFIED_COVERAGE",
        "attested_by": "synthetic coverage fixture",
        "deficiency": "MISSING_OBSERVATIONS",
    }
    assert row[11] == "EXPIRED_WITH_INCOMPLETE_COVERAGE"
    assert _stored(database).events[0].time_evidence == current.events[0].time_evidence


def test_consecutive_progress_numbers_steps_contiguously(database: Path) -> None:
    lifecycle = _advance(database, _recorded(database), _observe(8))
    lifecycle = _advance(database, lifecycle, _observe(9), _observe(12, _ENTRY))
    lifecycle = _advance(database, lifecycle, _observe(13, _TARGET))

    assert [row[1] for row in _rows(database)] == [1, 2, 3, 4]
    assert _stored(database) == lifecycle


def test_an_out_of_order_observation_is_refused(database: Path) -> None:
    lifecycle = _advance(database, _recorded(database), _observe(9))
    _candles(database, _observe(8))

    with pytest.raises(InvalidFuturesTradeRecommendationLifecycleProgressError):
        _lifecycle_store(database).record_progress(lifecycle, lifecycle, steps=(_observe(8),))
    assert len(_rows(database)) == 1


def test_progress_after_a_terminal_step_is_refused(database: Path) -> None:
    closed = _advance(database, _recorded(database), _observe(8, _ENTRY), _observe(9, _STOP))
    _candles(database, _observe(12))

    with pytest.raises(InvalidFuturesTradeRecommendationLifecycleProgressError):
        _lifecycle_store(database).record_progress(closed, closed, steps=(_observe(12),))
    with pytest.raises(InvalidFuturesTradeRecommendationLifecycleProgressError):
        _lifecycle_store(database).record_progress(closed, closed, steps=(_expire(9),))
    assert _stored(database) == closed


def test_no_raw_market_data_is_copied_into_steps(database: Path) -> None:
    _advance(database, _recorded(database), _observe(8, _ENTRY), _observe(9, _TARGET))

    for row in _rows(database):
        payload = json.loads(row[10])
        assert set(payload) == {"version", "events", "time_evidence"}
        for event in payload["events"]:
            assert set(event) == {"kind", "touched", "ordering", "expiry_reason"}
        for price in ("24380", "24400", "24450", "24390", "24425", "1250"):
            assert price not in row[10]


# ---------------------------------------------------------------------------
# Idempotency and conflicts
# ---------------------------------------------------------------------------


def test_identical_progress_again_is_an_idempotent_no_op(database: Path) -> None:
    initial = _recorded(database)
    _advance(database, initial, _observe(8), _observe(9, _ENTRY))
    before = _rows(database)

    assert _progress(database, initial, _observe(8), _observe(9, _ENTRY)) == 0
    assert _rows(database) == before


def test_a_stale_previous_is_refused_and_newer_progress_kept(database: Path) -> None:
    initial = _recorded(database)
    current = _advance(database, initial, _observe(8), _observe(9, _ENTRY))
    _candles(database, _observe(12))

    with pytest.raises(FuturesTradeRecommendationLifecycleConflictError):
        _progress(database, initial, _observe(8))
    with pytest.raises(FuturesTradeRecommendationLifecycleConflictError):
        _progress(database, _apply(initial, _observe(8)), _observe(12))
    assert _stored(database) == current


def test_already_recorded_steps_from_another_previous_are_a_conflict(database: Path) -> None:
    """Steps (9) from either 8 or nothing reach one quiet state; only the true start is a retry."""
    initial = _recorded(database)
    _advance(database, initial, _observe(8), _observe(9))

    with pytest.raises(FuturesTradeRecommendationLifecycleConflictError):
        _progress(database, initial, _observe(9))


def test_progress_beyond_an_already_recorded_request_is_a_conflict(database: Path) -> None:
    initial = _recorded(database)
    after_8 = _advance(database, initial, _observe(8))
    _advance(database, after_8, _observe(9))

    with pytest.raises(FuturesTradeRecommendationLifecycleConflictError):
        _progress(database, initial, _observe(8))


def test_progress_of_an_unrecorded_recommendation_is_a_conflict(database: Path) -> None:
    _recorded(database)
    other = Lifecycle(_recommendation(_NOV))
    _candles(database, _observe(8, contract=_NOV))

    with pytest.raises(FuturesTradeRecommendationLifecycleConflictError, match="not recorded"):
        _progress(database, other, _observe(8, contract=_NOV))


def test_progress_of_a_differently_defined_recommendation_is_a_conflict(database: Path) -> None:
    _recorded(database)
    redefined = Lifecycle(_recommendation(issued_at="2026-10-07T13:00:00Z"))
    _candles(database, _observe(8))

    with pytest.raises(FuturesTradeRecommendationLifecycleConflictError, match="different"):
        _progress(database, redefined, _observe(8))
    assert _rows(database) == []


# ---------------------------------------------------------------------------
# Referenced candles
# ---------------------------------------------------------------------------


def test_a_missing_candle_is_refused_and_nothing_is_written(database: Path) -> None:
    initial = _recorded(database)
    _candles(database, _observe(8))

    with pytest.raises(FuturesTradeRecommendationLifecycleEvidenceError, match="holds no"):
        _progress(database, initial, _observe(8), _observe(9))
    assert _rows(database) == []


def test_a_database_without_market_data_refuses_observations(database: Path) -> None:
    initial = _recorded(database)

    with pytest.raises(FuturesTradeRecommendationLifecycleEvidenceError):
        _progress(database, initial, _observe(8))
    assert _progress(database, initial, _expire(None)) == 1


def test_a_candle_differing_from_the_stored_one_is_refused(database: Path) -> None:
    initial = _recorded(database)
    _candles(database, _observe(8))
    revised = Observation(_bar(8, ("24401", "24420")), date(2026, 10, 8))

    with pytest.raises(FuturesTradeRecommendationLifecycleEvidenceError, match="not the"):
        _progress(database, initial, revised)
    assert _rows(database) == []


@pytest.mark.parametrize(
    "step",
    [_observe(8, timeframe="1h"), _observe(8, contract=_NOV)],
    ids=["other-timeframe", "other-contract"],
)
def test_a_candle_of_another_series_is_refused(database: Path, step: Observation) -> None:
    initial = _recorded(database)
    _candles(database, step, _observe(8))

    with pytest.raises(InvalidFuturesTradeRecommendationLifecycleProgressError):
        _lifecycle_store(database).record_progress(initial, initial, steps=(step,))
    assert _rows(database) == []


# ---------------------------------------------------------------------------
# Terminal state and product exclusivity (ADR-018, P-1)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "evaluations",
    [
        (_observe(8),),
        (_observe(8, _ENTRY),),
        (_observe(8), _observe(9), _observe(12, _ENTRY), _observe(13)),
    ],
    ids=["waiting-after-quiet-bar", "active", "active-after-several-bars"],
)
def test_an_open_lifecycle_still_blocks_its_product(database: Path, evaluations: tuple) -> None:
    _advance(database, _recorded(database), *evaluations)

    with pytest.raises(FuturesTradeRecommendationExclusivityError):
        _record(database, _recommendation(_NOV))


def test_a_long_expired_validity_without_a_terminal_step_still_blocks(database: Path) -> None:
    _recorded(database)

    with pytest.raises(FuturesTradeRecommendationExclusivityError):
        _record(database, _recommendation(_NOV))


@pytest.mark.parametrize(
    "evaluations",
    [
        (_observe(8, _ENTRY), _observe(9, _STOP)),
        (_observe(8, _ENTRY), _observe(9, _TARGET)),
        (_observe(8, _ENTRY), _observe(9, _BOTH)),
        (_observe(8, _STOP),),
        (_observe(14),),
        (_expire(None),),
        (_observe(8), _expire(8, ObservationCoverage.COMPLETE_VERIFIED_COVERAGE)),
    ],
    ids=[
        "stop",
        "target",
        "modeled-stop-first",
        "indeterminate",
        "bar-expiry",
        "incomplete-time-expiry",
        "verified-time-expiry",
    ],
)
def test_a_terminal_step_releases_the_product_without_touching_history(
    database: Path, evaluations: tuple
) -> None:
    closed = _advance(database, _recorded(database), *evaluations)
    definitions, history = _rows(database, "futures_trade_recommendations"), _rows(database)

    assert _record(database, _recommendation(_NOV)) == 1

    assert set(definitions) < set(_rows(database, "futures_trade_recommendations"))
    assert _rows(database) == history
    lifecycles = {lc.recommendation.contract: lc for lc in _lifecycles(database)}
    assert lifecycles[_OCT] == closed
    assert lifecycles[_NOV].status is RecommendationStatus.WAITING_FOR_ENTRY


def test_a_released_product_admits_only_one_new_open_recommendation(database: Path) -> None:
    _advance(database, _recorded(database), _observe(8, _STOP))
    _record(database, _recommendation(_NOV))

    with pytest.raises(FuturesTradeRecommendationExclusivityError):
        _record(database, _recommendation(_DEC))


def test_a_terminal_lifecycle_reconstructs_after_restart(database: Path) -> None:
    closed = _advance(database, _recorded(database), _observe(8, _ENTRY), _observe(9, _TARGET))

    restarted = SQLiteFuturesTradeRecommendationRepository(str(database))
    (lifecycle,) = restarted.get_lifecycles(FuturesTradeRecommendationQuery(_NIFTY))
    assert lifecycle == closed
    assert lifecycle.status is RecommendationStatus.CLOSED_TARGET


# ---------------------------------------------------------------------------
# Reconstruction fails closed
# ---------------------------------------------------------------------------


def _history(database: Path) -> Lifecycle:
    """Three steps: quiet 8, quiet 9, entry 12 -- an ACTIVE recommendation."""
    return _advance(database, _recorded(database), _observe(8), _observe(9), _observe(12, _ENTRY))


_STEPS = "UPDATE futures_trade_recommendation_steps SET "


@pytest.mark.parametrize(
    "sql",
    [
        "DELETE FROM futures_ohlcv WHERE point_in_time = '2026-10-12T10:00:00Z'",
        "UPDATE futures_ohlcv SET high_value = '24421' "
        "WHERE point_in_time = '2026-10-08T10:00:00Z'",
        "UPDATE futures_ohlcv SET volume = '1251' WHERE point_in_time = '2026-10-09T10:00:00Z'",
        "DROP TABLE futures_ohlcv",
        _STEPS + "evaluation = 'not json' WHERE sequence = 1",
        _STEPS + "evaluation = replace(evaluation, ',', ', ') WHERE sequence = 3",
        _STEPS + "evaluation = replace(evaluation, '\"version\":1', '\"version\":2')",
        _STEPS + "evaluation = replace(evaluation, 'ENTRY_ACTIVATED', 'ENTRY_INDETERMINATE')",
        _STEPS + 'evaluation = \'{"events":[],"time_evidence":null,"version":1}\' '
        "WHERE sequence = 3",
        _STEPS + "evaluation = (SELECT evaluation FROM futures_trade_recommendation_steps "
        "WHERE sequence = 3) WHERE sequence = 1",
        _STEPS + "resulting_status = 'ACTIVE' WHERE sequence = 1",
        _STEPS + "is_terminal = 1 WHERE sequence = 3",
        "DELETE FROM futures_trade_recommendation_steps WHERE sequence = 2",
        _STEPS + "sequence = 13 WHERE sequence = 3",
        _STEPS + "trading_date = '2026-10-09' WHERE sequence = 1",
        _STEPS + "candle_fingerprint = 'fdc1-sha256:' || hex(zeroblob(32)) WHERE sequence = 1",
        _STEPS + "expiration_date = '2026-11-24' WHERE sequence = 1",
        _STEPS + "point_in_time = '2026-10-08T15:30:00+05:30' WHERE sequence = 1",
        "UPDATE futures_recommendation_configurations SET validity_sessions = '6'",
    ],
    ids=[
        "missing-candle",
        "altered-candle-price",
        "altered-candle-volume",
        "market-data-table-gone",
        "corrupt-json",
        "non-canonical-json",
        "unknown-version",
        "tampered-event-kind",
        "dropped-event",
        "invented-event",
        "false-status",
        "false-terminal-flag",
        "missing-sequence",
        "renumbered-sequence",
        "tampered-trading-date",
        "tampered-fingerprint",
        "candle-of-another-contract",
        "non-canonical-instant",
        "configuration-seal",
    ],
)
def test_tampered_lifecycle_evidence_fails_closed(database: Path, sql: str) -> None:
    current = _history(database)
    if "DROP" not in sql:
        _candles(database, _observe(13))
    _execute(database, sql)
    before = _rows(database)

    with pytest.raises(FuturesTradeRecommendationStorageError):
        _lifecycles(database)
    with pytest.raises(FuturesTradeRecommendationStorageError):
        _progress(database, current, _observe(13))
    # A changed configuration row is refused even earlier, as a reused profile version.
    with pytest.raises(
        (
            FuturesTradeRecommendationStorageError,
            FuturesTradeRecommendationConfigurationConflictError,
        )
    ):
        _record(database, _recommendation(_NOV))
    assert _rows(database) == before


def test_reordered_events_within_one_step_fail_closed(database: Path) -> None:
    _advance(database, _recorded(database), _observe(14, _ENTRY))
    ((row),) = _rows(database)
    payload = json.loads(row[10])
    payload["events"].reverse()
    reordered = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    _execute(database, _STEPS + "evaluation = ?", (reordered,))

    with pytest.raises(FuturesTradeRecommendationStorageError, match="events"):
        _lifecycles(database)


@pytest.mark.parametrize(
    "field, value",
    [
        ("evaluated_through", "2026-10-09T10:00:00Z"),
        ("as_of", "2026-10-13T12:00:00Z"),
        ("coverage", "COMPLETE_VERIFIED_COVERAGE"),
        ("deficiency", "SOMETHING_ELSE"),
        ("attested_by", "   "),
        ("as_of", "2026-10-14T17:30:00+05:30"),
    ],
    ids=[
        "evaluated-through-mismatch",
        "premature-as-of",
        "complete-with-deficiency",
        "unknown-deficiency",
        "blank-attestation",
        "non-canonical-as-of",
    ],
)
def test_invalid_time_expiry_evidence_fails_closed(database: Path, field: str, value: str) -> None:
    _advance(database, _recorded(database), _observe(8), _expire(8))
    row = _rows(database)[-1]
    payload = json.loads(row[10])
    payload["time_evidence"][field] = value
    tampered = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    _execute(database, _STEPS + "evaluation = ? WHERE sequence = 2", (tampered,))

    with pytest.raises(FuturesTradeRecommendationStorageError):
        _lifecycles(database)


def test_a_time_expiry_without_its_evidence_fails_closed(database: Path) -> None:
    _advance(database, _recorded(database), _expire(None))
    _execute(
        database, _STEPS + "evaluation = replace(evaluation, '\"time_evidence\":{', '\"x\":{')"
    )

    with pytest.raises(FuturesTradeRecommendationStorageError):
        _lifecycles(database)


def test_a_corrupt_other_recommendation_blocks_recording_on_its_product(database: Path) -> None:
    _advance(database, _recorded(database), _observe(8, _STOP))
    _execute(database, _STEPS + "resulting_status = 'CLOSED_TARGET'")

    with pytest.raises(FuturesTradeRecommendationStorageError):
        _record(database, _recommendation(_NOV))
    assert len(_rows(database, "futures_trade_recommendations")) == 1


# ---------------------------------------------------------------------------
# Concurrency: separate connections, serialised by BEGIN IMMEDIATE
# ---------------------------------------------------------------------------


def _race(database: Path, attempts: list) -> list[object]:
    """Run each attempt on its own thread and connection while a writer lock is held.

    Every attempt starts behind the held lock, so all of them meet SQLite's
    writer serialisation; the outcomes are then classified, never ordered.
    """
    holder = sqlite3.connect(database, isolation_level=None)
    holder.execute("BEGIN IMMEDIATE")
    barrier = threading.Barrier(len(attempts) + 1)
    outcomes: list[object] = [None] * len(attempts)

    def run(index: int, attempt) -> None:
        barrier.wait()
        try:
            outcomes[index] = attempt()
        except Exception as exc:  # noqa: BLE001 -- the outcome is what is classified
            outcomes[index] = exc

    threads = [threading.Thread(target=run, args=(i, a)) for i, a in enumerate(attempts)]
    for thread in threads:
        thread.start()
    barrier.wait()
    holder.rollback()
    holder.close()
    for thread in threads:
        thread.join(timeout=30)
    return outcomes


def test_two_writers_racing_with_the_same_progress_record_it_once(database: Path) -> None:
    initial = _recorded(database)
    evaluations = (_observe(8), _observe(9, _ENTRY))
    _candles(database, *evaluations)

    outcomes = _race(database, [lambda: _progress(database, initial, *evaluations)] * 2)

    assert sorted(outcomes) == [0, 2]  # type: ignore[type-var]
    assert [row[1] for row in _rows(database)] == [1, 2]
    assert _stored(database) == _apply(initial, *evaluations)


def test_two_writers_racing_with_different_progress_leave_one_history(database: Path) -> None:
    active = _advance(database, _recorded(database), _observe(8, _ENTRY))
    stop, quiet = _observe(9, _STOP), _observe(12)
    _candles(database, stop, quiet)

    outcomes = _race(
        database,
        [lambda: _progress(database, active, stop), lambda: _progress(database, active, quiet)],
    )

    assert sorted(type(o).__name__ for o in outcomes) == [
        "FuturesTradeRecommendationLifecycleConflictError",
        "int",
    ]
    winner = stop if outcomes[0] == 1 else quiet
    assert _stored(database) == _apply(active, winner)
    assert [row[1] for row in _rows(database)] == [1, 2]


def test_terminal_progress_racing_a_new_recommendation_never_leaves_two_open(
    database: Path,
) -> None:
    active = _advance(database, _recorded(database), _observe(8, _ENTRY))
    stop = _observe(9, _STOP)
    _candles(database, stop)

    outcomes = _race(
        database,
        [
            lambda: _progress(database, active, stop),
            lambda: _record(database, _recommendation(_NOV)),
        ],
    )

    assert outcomes[0] == 1
    assert outcomes[1] == 1 or isinstance(outcomes[1], FuturesTradeRecommendationExclusivityError)
    assert sum(not lifecycle.is_terminal for lifecycle in _lifecycles(database)) <= 1


def test_concurrent_cross_expiry_creation_after_release_leaves_one_open(database: Path) -> None:
    _advance(database, _recorded(database), _observe(8, _STOP))

    outcomes = _race(
        database,
        [
            lambda: _record(database, _recommendation(_NOV)),
            lambda: _record(database, _recommendation(_DEC)),
        ],
    )

    assert sorted(type(o).__name__ for o in outcomes) == [
        "FuturesTradeRecommendationExclusivityError",
        "int",
    ]
    assert sum(not lifecycle.is_terminal for lifecycle in _lifecycles(database)) == 1


def test_a_writer_that_cannot_get_the_lock_fails_and_writes_nothing(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    initial = _recorded(database)
    _candles(database, _observe(8))
    monkeypatch.setattr(module, "_BUSY_TIMEOUT_SECONDS", 0.2)
    holder = sqlite3.connect(database, isolation_level=None)
    holder.execute("BEGIN IMMEDIATE")
    try:
        with pytest.raises(FuturesTradeRecommendationStorageError):
            _progress(database, initial, _observe(8))
    finally:
        holder.rollback()
        holder.close()

    assert _rows(database) == []


def test_a_failure_part_way_through_the_inserts_writes_nothing(database: Path) -> None:
    initial = _recorded(database)
    evaluations = (_observe(8), _observe(9), _observe(12))
    _candles(database, *evaluations)
    _execute(
        database,
        "CREATE TRIGGER fail_third_step BEFORE INSERT ON futures_trade_recommendation_steps "
        "WHEN NEW.sequence = 3 BEGIN SELECT RAISE(ABORT, 'synthetic failure'); END",
    )

    with pytest.raises(FuturesTradeRecommendationStorageError):
        _progress(database, initial, *evaluations)
    assert _rows(database) == []

    _execute(database, "DROP TRIGGER fail_third_step")
    assert _progress(database, initial, *evaluations) == 3


# ---------------------------------------------------------------------------
# Architecture and hygiene
# ---------------------------------------------------------------------------


def _strings(path: str) -> str:
    tree = ast.parse(Path(path).read_text(encoding="utf-8"))
    docstrings = {
        id(node.body[0].value)
        for node in ast.walk(tree)
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef))
        and node.body
        and isinstance(node.body[0], ast.Expr)
        and isinstance(node.body[0].value, ast.Constant)
    }
    return " ".join(
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and id(node) not in docstrings
    ).upper()


def _imports(path: str) -> set[str]:
    tree = ast.parse(Path(path).read_text(encoding="utf-8"))
    return {
        node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.module
    } | {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }


def test_the_store_inserts_steps_and_the_step_module_writes_nothing() -> None:
    store_sql = _strings(module.__file__)
    step_sql = _strings(steps.__file__)

    assert "INSERT INTO FUTURES_TRADE_RECOMMENDATION_STEPS" in store_sql
    for forbidden in ("UPDATE ", "DELETE ", "REPLACE", "ON CONFLICT", "UPSERT", "JOURNAL_MODE"):
        assert forbidden not in store_sql
        assert forbidden not in step_sql
    assert "INSERT " not in step_sql


def test_the_step_module_depends_on_no_provider_clock_or_execution() -> None:
    for name in _imports(steps.__file__):
        assert name.split(".")[0] not in {
            "requests",
            "urllib",
            "socket",
            "time",
            "random",
            "uuid",
            "pickle",
            "databento",
        }, name
        for fragment in ("upstox", "paper_trading", "execution", "market_data", "yahoo"):
            assert fragment not in name, name


def test_the_lifecycle_adapter_implements_the_application_port() -> None:
    assert issubclass(
        SQLiteFuturesTradeRecommendationLifecycleStore, FuturesTradeRecommendationLifecycleStore
    )
