"""Acceptance: futures historical research against the real SQLite store.

Every test here writes canonical daily bars through
SQLiteFuturesHistoricalMarketDataStore into a temporary database file and runs
the production research pipeline -- replay, analysis, measurement, run, metrics
and report -- over SQLiteFuturesHistoricalMarketDataRepository. Nothing is
faked, mocked or fetched: there is no provider, no network and no credential,
so this runs in the normal suite.

The central claim is look-ahead safety under counterfactual future data. A run
frozen at a cutoff T is taken; bars after T are then appended to the same
database, chosen to be extreme enough to flip a decision if they leaked; and the
run at T is taken again and required to be identical. Two controls keep that
claim honest: a run whose cutoff admits the appended bars must change, and a
different bar *before* the decision, written to a fresh database, must change
the decision too.

The history is session-daily on weekdays only, with one mid-week holiday
missing, so observations are counted rather than calendar days.
"""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from northstar_application.application_services import (
    BuildFuturesHistoricalResearchReportUseCase,
    CalculateFuturesHistoricalResearchMetricsUseCase,
    FuturesHistoricalResearchReport,
    FuturesHistoricalResearchRun,
    RunFuturesHistoricalResearchUseCase,
)
from northstar_application.ports import (
    FuturesHistoricalMarketDataConflictError,
    FuturesHistoricalMarketDataQuery,
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
from northstar_core.strategy import (
    FuturesAssetAnalysisGenerator,
    FuturesRecommendationOutcomeUnavailableReason,
    RecommendationAction,
    ResearchHorizon,
    Strategy,
    StrategyIdentity,
)

from northstar_infrastructure.market_data import (
    SQLiteFuturesHistoricalMarketDataRepository,
    SQLiteFuturesHistoricalMarketDataStore,
)

_CME = ExchangeCode("CME")
_ES_DEC = FuturesContract(FuturesProductReference(Symbol("ES"), _CME), ExpirationDate("2026-12-18"))
_ES_MAR = FuturesContract(FuturesProductReference(Symbol("ES"), _CME), ExpirationDate("2027-03-19"))
_MES_DEC = FuturesContract(
    FuturesProductReference(Symbol("MES"), _CME), ExpirationDate("2026-12-18")
)
_DAILY = Timeframe("1d")
_STRATEGY = Strategy(StrategyIdentity("sqlite-acceptance"))
_H1, _H3 = ResearchHorizon(1), ResearchHorizon(3)
_HORIZONS = (_H3, _H1)  # deliberately not ascending

_BUY = RecommendationAction("BUY")
_SELL = RecommendationAction("SELL")
_HOLD = RecommendationAction("HOLD")
_INSUFFICIENT = FuturesRecommendationOutcomeUnavailableReason.INSUFFICIENT_FUTURE_OBSERVATIONS
_UNDEFINED = FuturesRecommendationOutcomeUnavailableReason.UNDEFINED_RETURN_BASIS

_HOLIDAY = date(2026, 6, 19)


def _session_dates(count: int) -> list[date]:
    """Weekday session dates from 2026-06-01, skipping weekends and one holiday."""
    dates: list[date] = []
    day = date(2026, 6, 1)
    while len(dates) < count:
        if day.weekday() < 5 and day != _HOLIDAY:
            dates.append(day)
        day += timedelta(days=1)
    return dates


_DATES = _session_dates(30)


def _instant(session: int) -> PointInTime:
    """Completion instant of the ``session``-th stored session (1-based)."""
    return PointInTime(f"{_DATES[session - 1].isoformat()}T21:00:00Z")


def _bar(
    session: int,
    close: str,
    volume: str = "1000",
    *,
    contract: FuturesContract = _ES_DEC,
    instant: PointInTime | None = None,
) -> FuturesOHLCVBar:
    quote = Decimal(close)
    return FuturesOHLCVBar(
        contract=contract,
        point_in_time=instant if instant is not None else _instant(session),
        timeframe=_DAILY,
        open=QuoteValue(quote),
        high=QuoteValue(quote + 2),
        low=QuoteValue(quote - 2),
        close=QuoteValue(quote),
        volume=Quantity(Decimal(volume)),
    )


# Phase A: B1..B25. Fifteen flat sessions, then ten rising ones: the twentieth
# session is the first decision and reads strongly bullish.
_PHASE_A_CLOSES = ["7600"] * 15 + [str(7601 + index) for index in range(10)]
_PHASE_A = tuple(_bar(session, close) for session, close in enumerate(_PHASE_A_CLOSES, start=1))

# Phase B: B26..B30, strictly after the cutoff. A collapse through zero on
# enormous volume -- a strongly bearish reading had it reached any earlier
# decision's window.
_PHASE_B = tuple(
    _bar(session, close, "250000")
    for session, close in zip(
        range(26, 31), ["3000", "-500", "-4000", "-7500", "-11000"], strict=True
    )
)

_T = _instant(25)
_T2 = _instant(30)
_D = 20  # the first decision, and the one tracked through the counterfactual


def _store(path: Path, bars: tuple[FuturesOHLCVBar, ...]) -> int:
    return SQLiteFuturesHistoricalMarketDataStore(path).store(bars)


def _repository(path: Path) -> SQLiteFuturesHistoricalMarketDataRepository:
    return SQLiteFuturesHistoricalMarketDataRepository(path)


def _research(
    path: Path,
    available_through: PointInTime,
    *,
    contract: FuturesContract = _ES_DEC,
    horizons: tuple[ResearchHorizon, ...] = _HORIZONS,
) -> FuturesHistoricalResearchRun:
    return RunFuturesHistoricalResearchUseCase(
        _repository(path), FuturesAssetAnalysisGenerator()
    ).execute(contract, _DAILY, _STRATEGY, horizons, available_through)


def _report(run: FuturesHistoricalResearchRun) -> FuturesHistoricalResearchReport:
    return BuildFuturesHistoricalResearchReportUseCase().execute(run)


def _result_at(run: FuturesHistoricalResearchRun, session: int):
    (result,) = [
        result
        for result in run.analysis_results
        if result.recommendation.point_in_time.compare(_instant(session)) == 0
    ]
    return result


def _outcome(run: FuturesHistoricalResearchRun, session: int, horizon: ResearchHorizon):
    (outcome,) = [
        outcome
        for outcome in run.outcomes
        if outcome.decision_instant.compare(_instant(session)) == 0 and outcome.horizon == horizon
    ]
    return outcome


@pytest.fixture
def database(tmp_path: Path) -> Path:
    """Phase A persisted, with other contracts' bars as noise at the same instants."""
    path = tmp_path / "futures_research.sqlite3"
    assert _store(path, _PHASE_A) == len(_PHASE_A)
    noise = tuple(
        _bar(session, str(Decimal(close) * -3), "99999", contract=contract)
        for contract in (_ES_MAR, _MES_DEC)
        for session, close in enumerate(_PHASE_A_CLOSES, start=1)
    )
    assert _store(path, noise) == len(noise)
    return path


# ---------------------------------------------------------------------------
# Persistence round trip
# ---------------------------------------------------------------------------


def test_the_research_input_is_exactly_what_was_written(database: Path) -> None:
    stored = _repository(database).get_bars(FuturesHistoricalMarketDataQuery(_ES_DEC, _DAILY))

    assert stored == _PHASE_A
    for session in (1, 15, 20, 25):
        bar = stored[session - 1]
        assert bar == _PHASE_A[session - 1]
        assert bar.close.value == Decimal(_PHASE_A_CLOSES[session - 1])
        assert bar.contract == _ES_DEC


def test_the_store_keeps_its_insert_only_semantics(database: Path) -> None:
    store = SQLiteFuturesHistoricalMarketDataStore(database)

    assert store.store(_PHASE_A) == len(_PHASE_A)  # identical re-store is idempotent
    with pytest.raises(FuturesHistoricalMarketDataConflictError):
        store.store((_bar(10, "9999"),))  # a different bar under an existing key

    stored = _repository(database).get_bars(FuturesHistoricalMarketDataQuery(_ES_DEC, _DAILY))
    assert stored == _PHASE_A


# ---------------------------------------------------------------------------
# Baseline run at T
# ---------------------------------------------------------------------------


def test_the_baseline_run_describes_exactly_the_evidence_up_to_its_cutoff(
    database: Path,
) -> None:
    run = _research(database, _T)

    assert run.contract == _ES_DEC
    assert run.timeframe == _DAILY
    assert run.strategy_identity == _STRATEGY.strategy_identity
    assert run.available_through.compare(_T) == 0
    assert run.horizons == _HORIZONS

    # Sessions 1..19 are warm-up; decisions are sessions 20..25.
    decisions = [result.recommendation.point_in_time for result in run.analysis_results]
    assert decisions == [_instant(session) for session in range(20, 26)]
    assert all(decision.compare(_T) <= 0 for decision in decisions)
    for result, session in zip(run.analysis_results, range(20, 26), strict=True):
        context = result.market_observation_context
        assert context.recent_closes == tuple(bar.close for bar in _PHASE_A[session - 20 : session])
        assert context.latest_quote == _PHASE_A[session - 1].close

    evaluated = [outcome for outcome in run.outcomes if outcome.evaluation_instant is not None]
    assert evaluated
    assert all(outcome.evaluation_instant.compare(_T) <= 0 for outcome in evaluated)


def test_the_first_decision_is_bullish_and_its_horizons_are_counted_in_sessions(
    database: Path,
) -> None:
    run = _research(database, _T)

    assert _result_at(run, _D).recommendation.action == _BUY

    one = _outcome(run, _D, _H1)
    three = _outcome(run, _D, _H3)
    assert one.evaluation_instant == _instant(21)
    assert three.evaluation_instant == _instant(23)

    # Session 22 is Wednesday 1 July; three sessions later is Monday 6 July,
    # five calendar days away across a weekend that has no stored bar.
    across_weekend = _outcome(run, 22, _H3)
    assert _DATES[21] == date(2026, 7, 1)
    assert _DATES[24] == date(2026, 7, 6)
    assert across_weekend.evaluation_instant == _instant(25)
    assert one.decision_quote == QuoteValue(Decimal("7605"))
    assert one.evaluation_quote == QuoteValue(Decimal("7606"))
    assert one.unavailable_reason is None
    assert three.unavailable_reason is None


def test_horizons_beyond_the_cutoff_are_insufficient(database: Path) -> None:
    run = _research(database, _T)

    assert _outcome(run, 25, _H1).unavailable_reason == _INSUFFICIENT
    for session in (23, 24, 25):
        assert _outcome(run, session, _H3).unavailable_reason == _INSUFFICIENT
    assert _outcome(run, 22, _H3).unavailable_reason is None


def test_metrics_and_report_account_for_every_outcome_at_the_cutoff(database: Path) -> None:
    run = _research(database, _T)
    metrics = CalculateFuturesHistoricalResearchMetricsUseCase().execute(run)
    report = _report(run)

    assert [entry.horizon for entry in metrics] == list(_HORIZONS)
    three, one = metrics
    assert (three.total_count, three.measured_count) == (6, 3)
    assert three.insufficient_future_observations_count == 3
    assert (one.total_count, one.measured_count) == (6, 5)
    assert one.insufficient_future_observations_count == 1
    for entry in metrics:
        assert entry.undefined_return_basis_count == 0
        assert entry.total_count == (
            entry.measured_count
            + entry.insufficient_future_observations_count
            + entry.undefined_return_basis_count
        )
    assert sum(entry.total_count for entry in metrics) == len(run.outcomes)

    assert report.run is run
    assert report.metrics == metrics
    assert report.available_through.compare(_T) == 0
    assert report.decision_count == 6
    assert report.last_decision_instant == _instant(25)


def test_an_offset_spelled_cutoff_produces_the_same_run(database: Path) -> None:
    offset = PointInTime(f"{_DATES[24].isoformat()}T16:00:00-05:00")

    assert _research(database, offset) == _research(database, _T)


# ---------------------------------------------------------------------------
# The counterfactual: appended future data cannot rewrite a frozen run
# ---------------------------------------------------------------------------


def test_bars_appended_after_the_cutoff_cannot_change_the_run_at_the_cutoff(
    database: Path,
) -> None:
    run_1 = _research(database, _T)
    metrics_1 = CalculateFuturesHistoricalResearchMetricsUseCase().execute(run_1)
    report_1 = _report(run_1)
    result_at_d = _result_at(run_1, _D)

    assert _store(database, _PHASE_B) == len(_PHASE_B)
    stored = _repository(database).get_bars(FuturesHistoricalMarketDataQuery(_ES_DEC, _DAILY))
    assert stored == _PHASE_A + _PHASE_B  # the database now physically holds more

    run_2 = _research(database, _T)
    metrics_2 = CalculateFuturesHistoricalResearchMetricsUseCase().execute(run_2)
    report_2 = _report(run_2)

    assert run_2 == run_1
    assert metrics_2 == metrics_1
    assert report_2 == report_1
    assert _result_at(run_2, _D) == result_at_d
    assert _result_at(run_2, _D).recommendation.action == _BUY
    future = {bar.point_in_time for bar in _PHASE_B}
    for result in run_2.analysis_results:
        assert not future & {result.market_observation_context.observed_at}
        assert not set(result.market_observation_context.recent_closes) & {
            bar.close for bar in _PHASE_B
        }


def test_a_cutoff_that_admits_the_appended_bars_does_change_the_research(
    database: Path,
) -> None:
    """Negative control: the counterfactual data is genuinely influential."""
    run_at_t = _research(database, _T)
    _store(database, _PHASE_B)

    run_at_t2 = _research(database, _T2)

    assert run_at_t2 != run_at_t
    # The earlier decisions are untouched...
    assert run_at_t2.analysis_results[: len(run_at_t.analysis_results)] == (
        run_at_t.analysis_results
    )
    # ...a formerly insufficient horizon is now measured, and into a collapse...
    resolved = _outcome(run_at_t2, 25, _H1)
    assert _outcome(run_at_t, 25, _H1).unavailable_reason == _INSUFFICIENT
    assert resolved.evaluation_instant == _instant(26)
    assert resolved.forward_return is not None
    assert resolved.forward_return.value < 0
    # ...and the collapse, once visible, reads bearish.
    assert _result_at(run_at_t2, 30).recommendation.action == _SELL

    metrics_t = CalculateFuturesHistoricalResearchMetricsUseCase().execute(run_at_t)
    metrics_t2 = CalculateFuturesHistoricalResearchMetricsUseCase().execute(run_at_t2)
    assert metrics_t2 != metrics_t
    # Horizon 1 at T2: eleven decisions. Those taken on the negative quotes of
    # sessions 27..29 have an undefined return basis; session 30 has no future.
    one = metrics_t2[1]
    assert one.horizon == _H1
    assert (one.total_count, one.measured_count) == (11, 7)
    assert one.undefined_return_basis_count == 3
    assert one.insufficient_future_observations_count == 1


def test_the_appended_collapse_would_flip_decision_d_if_it_reached_its_window(
    tmp_path: Path,
) -> None:
    """Load-bearing check on the leak itself.

    D's window is sessions 1..20. Replace its last five sessions with the five
    appended collapse bars -- exactly what a leak into D's window would look
    like -- and the decision at session 20 turns from BUY to SELL.
    """
    leaked_closes = [*_PHASE_A_CLOSES[:15], *(str(bar.close.value) for bar in _PHASE_B)]
    leaked = tuple(
        _bar(session, close, "250000" if session > 15 else "1000")
        for session, close in enumerate(leaked_closes, start=1)
    )
    assert len(leaked) == 20
    path = tmp_path / "leaked.sqlite3"
    _store(path, leaked)

    run = _research(path, _instant(20))

    assert _result_at(run, 20).recommendation.action == _SELL


# ---------------------------------------------------------------------------
# Negative control inside the visible range
# ---------------------------------------------------------------------------


def test_a_different_bar_before_the_decision_changes_the_decision(
    tmp_path: Path, database: Path
) -> None:
    """A fresh database differs only at session 19; decision D must follow it."""
    altered_closes = list(_PHASE_A_CLOSES)
    altered_closes[18] = "7700"  # session 19 now above session 20's 7605
    altered = tuple(_bar(session, close) for session, close in enumerate(altered_closes, start=1))
    path = tmp_path / "altered.sqlite3"
    _store(path, altered)

    baseline = _result_at(_research(database, _instant(_D)), _D)
    counterfactual = _result_at(_research(path, _instant(_D)), _D)

    assert baseline.recommendation.action == _BUY
    assert counterfactual.recommendation.action == _HOLD
    assert counterfactual != baseline


# ---------------------------------------------------------------------------
# Contract isolation
# ---------------------------------------------------------------------------


def test_other_contracts_in_the_same_database_never_enter_the_run(
    tmp_path: Path, database: Path
) -> None:
    clean = tmp_path / "clean.sqlite3"
    _store(clean, _PHASE_A)

    noisy_run = _research(database, _T)

    assert noisy_run == _research(clean, _T)
    for result in noisy_run.analysis_results:
        assert result.recommendation.contract == _ES_DEC
        assert all(quote.value > 0 for quote in result.market_observation_context.recent_closes)


def test_the_other_contracts_are_researchable_on_their_own(database: Path) -> None:
    """Proof the noise is really there and really different."""
    mar = _research(database, _T, contract=_ES_MAR)

    assert mar.contract == _ES_MAR
    assert len(mar.analysis_results) == 6
    assert mar.analysis_results[0].market_observation_context.latest_quote.value < 0


# ---------------------------------------------------------------------------
# Zero and negative quotations
# ---------------------------------------------------------------------------


def test_zero_and_negative_quotations_round_trip_and_follow_core_basis_semantics(
    tmp_path: Path,
) -> None:
    """Twenty-one sessions below zero, ending at exactly zero, then one more."""
    closes = [str(-40 + index * 2) for index in range(20)] + ["0", "5"]
    closes[19] = "-1.5"  # the first decision sits on a negative quote
    bars = tuple(_bar(session, close) for session, close in enumerate(closes, start=1))
    path = tmp_path / "negative.sqlite3"
    _store(path, bars)

    stored = _repository(path).get_bars(FuturesHistoricalMarketDataQuery(_ES_DEC, _DAILY))
    assert stored == bars
    assert stored[20].close == QuoteValue(Decimal("0"))
    assert stored[0].close == QuoteValue(Decimal("-40"))

    run = _research(path, _instant(22), horizons=(_H1,))

    assert _outcome(run, 20, _H1).unavailable_reason == _UNDEFINED  # -1.5 -> 0
    assert _outcome(run, 21, _H1).unavailable_reason == _UNDEFINED  # 0 -> 5
    assert _outcome(run, 22, _H1).unavailable_reason == _INSUFFICIENT
    (metrics,) = CalculateFuturesHistoricalResearchMetricsUseCase().execute(run)
    assert (metrics.total_count, metrics.measured_count) == (3, 0)
    assert metrics.undefined_return_basis_count == 2
    assert metrics.insufficient_future_observations_count == 1
    assert metrics.average_forward_return is None


# ---------------------------------------------------------------------------
# Timestamp semantics through the real adapter
# ---------------------------------------------------------------------------


def test_research_receives_semantic_order_despite_the_text_ordering_trap(
    tmp_path: Path,
) -> None:
    """Session 21 completes half a second after session 20, in the same second.

    As stored text ``...21:00:00.5Z`` sorts before ``...21:00:00Z``, so a
    repository ordering by its TEXT column would put session 21 first. Both are
    written in reverse order to make any reliance on insertion order visible.
    """
    closes = list(_PHASE_A_CLOSES[:21])
    whole = _instant(20)
    fractional = PointInTime(f"{_DATES[19].isoformat()}T21:00:00.5Z")
    assert fractional.value < whole.value

    bars = [_bar(session, close) for session, close in enumerate(closes[:20], start=1)]
    bars.append(_bar(21, closes[20], instant=fractional))
    path = tmp_path / "fractional.sqlite3"
    _store(path, tuple(reversed(bars)))

    run = _research(path, fractional, horizons=(_H1,))

    assert [result.recommendation.point_in_time for result in run.analysis_results] == [
        whole,
        fractional,
    ]
    first = _outcome(run, 20, _H1)
    assert first.evaluation_instant == fractional
    assert first.evaluation_quote == bars[-1].close
