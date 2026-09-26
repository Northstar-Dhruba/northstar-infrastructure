"""Acceptance: futures forward research against the real SQLite stores.

Market history is written through SQLiteFuturesHistoricalMarketDataStore and
frozen decisions through SQLiteFuturesForwardResearchRecordStore, both into one
temporary database file. The production use cases -- freeze, measure, run,
metrics and report -- read them back through the real SQLite repositories.
Every adapter is constructed afresh for every call, so nothing depends on
process memory. There is no provider, no network and no credential.

The main scenario plays one forward-testing timeline in order:

1. Phase A: sessions 1..25 for ES Dec 2026 -- fifteen flat, then a steady rise
   -- plus MES Dec 2026 noise. Decisions are frozen at D = session 25 for two
   strategies on ES and one on MES. Every ES decision reads BUY.
2. Session 26 arrives. At cutoff T1 = session 26 horizon 1 is measured and
   horizon 3 is pending.
3. Sessions 27..31 arrive: a collapse through zero on enormous volume. Freezing
   again at D must return the original decision; freezing at D2 = session 31
   must produce a new SELL decision.
4. Sessions 32..35 arrive, after horizon 3's selected observation. The horizon
   3 measurement must not move; longer horizons must see the new bars.

Sessions are weekdays only, with 19 June also missing, so horizons count stored
observations rather than calendar days.
"""

from __future__ import annotations

import sqlite3
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest
from northstar_application.application_services import (
    BuildFuturesForwardResearchReportUseCase,
    CalculateFuturesForwardResearchMetricsUseCase,
    ForwardResearchMeasurementState,
    FreezeFuturesForwardResearchDecisionUseCase,
    FuturesForwardResearchMeasurement,
    FuturesForwardResearchRecord,
    FuturesForwardResearchRun,
    MeasureFuturesForwardResearchRecordUseCase,
    RunFuturesForwardResearchUseCase,
)
from northstar_application.ports import (
    FuturesForwardResearchRecordConflictError,
    FuturesForwardResearchRecordQuery,
    FuturesHistoricalMarketDataQuery,
)
from northstar_core.derivatives import ExpirationDate, QuoteValue
from northstar_core.foundation.value_objects import (
    ExchangeCode,
    Percentage,
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
from northstar_infrastructure.persistence import (
    SQLiteFuturesForwardResearchRecordRepository,
    SQLiteFuturesForwardResearchRecordStore,
)

_CME = ExchangeCode("CME")
_ES_DEC = FuturesContract(FuturesProductReference(Symbol("ES"), _CME), ExpirationDate("2026-12-18"))
_ES_MAR = FuturesContract(FuturesProductReference(Symbol("ES"), _CME), ExpirationDate("2027-03-19"))
_MES_DEC = FuturesContract(
    FuturesProductReference(Symbol("MES"), _CME), ExpirationDate("2026-12-18")
)
_DAILY = Timeframe("1d")
_ES_QUERY = FuturesForwardResearchRecordQuery(_ES_DEC, _DAILY)
_ALPHA = Strategy(StrategyIdentity("alpha"))
_BETA = Strategy(StrategyIdentity("beta"))
_H1, _H3, _H5 = ResearchHorizon(1), ResearchHorizon(3), ResearchHorizon(5)
_HORIZONS = (_H3, _H1)  # deliberately not ascending

_BUY = RecommendationAction("BUY")
_SELL = RecommendationAction("SELL")
_HOLD = RecommendationAction("HOLD")
_PENDING = ForwardResearchMeasurementState.PENDING
_MEASURED = ForwardResearchMeasurementState.MEASURED
_UNAVAILABLE = ForwardResearchMeasurementState.UNAVAILABLE
_UNDEFINED = FuturesRecommendationOutcomeUnavailableReason.UNDEFINED_RETURN_BASIS


def _session_dates(count: int) -> list[date]:
    dates: list[date] = []
    day = date(2026, 6, 1)
    while len(dates) < count:
        if day.weekday() < 5 and day != date(2026, 6, 19):
            dates.append(day)
        day += timedelta(days=1)
    return dates


_DATES = _session_dates(40)


def _instant(session: int) -> PointInTime:
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


_PHASE_A_CLOSES = ["7600"] * 15 + [str(7601 + index) for index in range(10)]
_PHASE_A = tuple(_bar(session, close) for session, close in enumerate(_PHASE_A_CLOSES, start=1))
_SESSION_26 = (_bar(26, "7611"),)
_COLLAPSE = tuple(
    _bar(session, close, "250000")
    for session, close in zip(
        range(27, 32), ["3000", "-500", "-4000", "-7500", "-11000"], strict=True
    )
)
_AFTERSHOCK = tuple(
    _bar(session, close, "500000")
    for session, close in zip(range(32, 36), ["99999", "0", "50000", "-20000"], strict=True)
)
_MES_NOISE = tuple(_bar(session, "5000", "999", contract=_MES_DEC) for session in range(1, 26))
# Same product and exchange as ES Dec, another expiry: what an expiry-blind read would leak.
_ES_MAR_NOISE = tuple(_bar(session, "-3000", "77", contract=_ES_MAR) for session in range(1, 36))

_D, _T1, _T2, _D2 = _instant(25), _instant(26), _instant(28), _instant(31)


# ---------------------------------------------------------------------------
# Production adapters and use cases, rebuilt for every call
# ---------------------------------------------------------------------------


def _store_bars(path: Path, bars: tuple[FuturesOHLCVBar, ...]) -> int:
    return SQLiteFuturesHistoricalMarketDataStore(path).store(bars)


def _freeze(
    path: Path,
    as_of: PointInTime,
    strategy: Strategy = _ALPHA,
    contract: FuturesContract = _ES_DEC,
) -> FuturesForwardResearchRecord | None:
    return FreezeFuturesForwardResearchDecisionUseCase(
        SQLiteFuturesHistoricalMarketDataRepository(path),
        FuturesAssetAnalysisGenerator(),
        SQLiteFuturesForwardResearchRecordStore(path),
    ).execute(contract, _DAILY, strategy, as_of)


def _measure(
    path: Path, record: FuturesForwardResearchRecord, horizon: ResearchHorizon, cutoff: PointInTime
) -> FuturesForwardResearchMeasurement:
    return MeasureFuturesForwardResearchRecordUseCase(
        SQLiteFuturesHistoricalMarketDataRepository(path)
    ).execute(record, horizon, cutoff)


def _run(
    path: Path,
    cutoff: PointInTime,
    horizons: tuple[ResearchHorizon, ...] = _HORIZONS,
    query: FuturesForwardResearchRecordQuery = _ES_QUERY,
) -> FuturesForwardResearchRun:
    return RunFuturesForwardResearchUseCase(
        SQLiteFuturesForwardResearchRecordRepository(path),
        SQLiteFuturesHistoricalMarketDataRepository(path),
    ).execute(query, horizons, cutoff)


def _records(path: Path, contract: FuturesContract = _ES_DEC) -> tuple:
    return SQLiteFuturesForwardResearchRecordRepository(path).get_records(
        FuturesForwardResearchRecordQuery(contract, _DAILY)
    )


def _forward_rows(path: Path) -> int:
    with sqlite3.connect(path) as connection:
        return connection.execute(
            "SELECT COUNT(*) FROM futures_forward_research_records"
        ).fetchone()[0]


def _tables(path: Path) -> set[str]:
    with sqlite3.connect(path) as connection:
        return {row[0] for row in connection.execute("SELECT name FROM sqlite_master")}


def _metrics(run: FuturesForwardResearchRun) -> tuple:
    return CalculateFuturesForwardResearchMetricsUseCase().execute(run)


def _report(run: FuturesForwardResearchRun):
    return BuildFuturesForwardResearchReportUseCase().execute(run)


def _states(run: FuturesForwardResearchRun) -> list:
    return [measurement.state for measurement in run.measurements]


# ---------------------------------------------------------------------------
# The main forward timeline
# ---------------------------------------------------------------------------


@pytest.fixture
def scenario(tmp_path: Path) -> SimpleNamespace:
    path = tmp_path / "forward.sqlite3"
    s = SimpleNamespace(path=path)

    # Phase A and its decisions at D.
    assert _store_bars(path, _PHASE_A) == 25
    assert _store_bars(path, _MES_NOISE) == 25
    assert _store_bars(path, _ES_MAR_NOISE) == 35
    s.f1 = _freeze(path, _D, _ALPHA)
    s.f1_beta = _freeze(path, _D, _BETA)
    s.mes = _freeze(path, _D, _ALPHA, _MES_DEC)
    s.rows_after_first_freeze = _forward_rows(path)
    s.reloaded = _records(path)
    s.retry = _freeze(path, _D, _ALPHA)
    s.rows_after_retry = _forward_rows(path)

    # Session 26: horizon 1 reachable, horizon 3 not.
    _store_bars(path, _SESSION_26)
    s.pending = _measure(path, s.f1, _H3, _T1)
    s.pending_again = _measure(path, s.f1, _H3, _T1)
    s.tables_before_derived = _tables(path)
    s.rows_before_derived = _forward_rows(path)
    s.run_t1 = _run(path, _T1)
    s.metrics_t1 = _metrics(s.run_t1)
    s.report_t1 = _report(s.run_t1)
    s.tables_after_derived = _tables(path)
    s.rows_after_derived = _forward_rows(path)

    # Sessions 27..31: the collapse.
    _store_bars(path, _COLLAPSE)
    s.f1_after_collapse = _freeze(path, _D, _ALPHA)
    s.rows_after_refreeze = _forward_rows(path)
    s.run_t1_after_collapse = _run(path, _T1)
    s.metrics_t1_after_collapse = _metrics(s.run_t1_after_collapse)
    s.report_t1_after_collapse = _report(s.run_t1_after_collapse)
    s.measured = _measure(path, s.f1, _H3, _T2)
    s.run_t2 = _run(path, _T2)
    s.f2 = _freeze(path, _D2, _ALPHA)
    s.run_t1_after_f2 = _run(path, _T1)
    s.f2_pending = _measure(path, s.f2, _H1, _D2)

    # Sessions 32..35: after horizon 3's selected observation.
    _store_bars(path, _AFTERSHOCK)
    s.measured_later = _measure(path, s.f1, _H3, _instant(35))
    s.h5 = _measure(path, s.f1, _H5, _instant(35))
    s.f2_unavailable = _measure(path, s.f2, _H1, _instant(32))
    s.run_late = _run(path, _instant(40))
    s.report_late = _report(s.run_late)
    s.final_records = _records(path)
    return s


# -- F1 and its freezing ------------------------------------------------------


def test_the_first_decision_is_a_daily_buy_frozen_at_d(scenario: SimpleNamespace) -> None:
    f1 = scenario.f1

    assert isinstance(f1, FuturesForwardResearchRecord)
    assert f1.decision_instant == _D
    assert f1.timeframe == _DAILY
    assert f1.contract == _ES_DEC
    assert f1.strategy_identity == StrategyIdentity("alpha")
    assert f1.result.recommendation.action == _BUY
    assert f1.result.market_observation_context.recent_closes == tuple(
        bar.close for bar in _PHASE_A[-20:]
    )
    assert scenario.rows_after_first_freeze == 3  # alpha and beta on ES, alpha on MES


def test_the_frozen_decisions_reload_exactly_through_a_fresh_repository(
    scenario: SimpleNamespace,
) -> None:
    assert scenario.reloaded == (scenario.f1, scenario.f1_beta)
    assert scenario.reloaded[0] is not scenario.f1


def test_an_identical_freeze_is_an_idempotent_retry(scenario: SimpleNamespace) -> None:
    assert scenario.retry == scenario.f1
    assert scenario.rows_after_retry == scenario.rows_after_first_freeze


def test_the_collapse_cannot_change_the_decision_frozen_at_d(scenario: SimpleNamespace) -> None:
    """The central frozen-decision look-ahead proof."""
    after = scenario.f1_after_collapse

    assert after == scenario.f1
    assert after.result.recommendation.action == _BUY
    context = after.result.market_observation_context
    assert context.observed_at == _D
    future_closes = {bar.close for bar in (*_SESSION_26, *_COLLAPSE)}
    assert not set(context.recent_closes) & future_closes
    assert scenario.rows_after_refreeze == scenario.rows_after_first_freeze


def test_a_later_as_of_freezes_a_new_sell_decision_beside_f1(scenario: SimpleNamespace) -> None:
    f2 = scenario.f2

    assert f2.decision_instant == _D2
    assert f2.natural_key != scenario.f1.natural_key
    assert f2.result.recommendation.action == _SELL
    assert f2.result.market_observation_context.latest_quote == QuoteValue(Decimal("-11000"))
    assert scenario.final_records == (scenario.f1, scenario.f1_beta, f2)


# -- Measurement lifecycle ----------------------------------------------------


def test_horizon_three_is_pending_at_t1_and_repeats_equally(scenario: SimpleNamespace) -> None:
    assert scenario.pending.state is _PENDING
    assert scenario.pending.outcome.evaluation_instant is None
    assert scenario.pending_again == scenario.pending


def test_horizon_three_becomes_measured_when_its_observation_is_stored(
    scenario: SimpleNamespace,
) -> None:
    measured = scenario.measured

    assert measured.state is _MEASURED
    assert measured.record == scenario.f1
    assert measured.outcome.evaluation_instant == _instant(28)  # third stored session after D
    assert measured.outcome.evaluation_quote == QuoteValue(Decimal("-500"))
    # A positive decision followed by a negative quote is measured, not unavailable.
    assert measured.outcome.forward_return.value < -100


def test_the_measured_horizon_does_not_move_when_later_bars_arrive(
    scenario: SimpleNamespace,
) -> None:
    assert scenario.measured_later == scenario.measured


def test_a_longer_horizon_does_see_those_later_bars(scenario: SimpleNamespace) -> None:
    """Guard: the stability check is load-bearing, and horizons count sessions."""
    h5 = scenario.h5

    assert h5.state is _MEASURED
    assert h5.outcome.evaluation_instant == _instant(30)
    assert _DATES[29] == date(2026, 7, 13)  # five sessions, seven calendar days after D
    assert _DATES[24] == date(2026, 7, 6)
    assert h5.outcome.evaluation_quote == QuoteValue(Decimal("-7500"))


def test_a_negative_decision_is_pending_then_unavailable(scenario: SimpleNamespace) -> None:
    assert scenario.f2_pending.state is _PENDING
    assert scenario.f2_unavailable.state is _UNAVAILABLE
    assert scenario.f2_unavailable.outcome.unavailable_reason == _UNDEFINED
    assert scenario.f2_unavailable.outcome.evaluation_quote == QuoteValue(Decimal("99999"))


# -- Runs, metrics and reports -------------------------------------------------


def test_the_t1_run_holds_only_decisions_at_or_before_t1_in_canonical_order(
    scenario: SimpleNamespace,
) -> None:
    run = scenario.run_t1

    assert run.available_through == _T1
    assert run.records == (scenario.f1, scenario.f1_beta)
    assert [(m.record.strategy_identity.identity, m.horizon) for m in run.measurements] == [
        ("alpha", _H3),
        ("alpha", _H1),
        ("beta", _H3),
        ("beta", _H1),
    ]
    assert _states(run) == [_PENDING, _MEASURED, _PENDING, _MEASURED]
    assert all(
        m.outcome.evaluation_instant.compare(_T1) <= 0
        for m in run.measurements
        if m.outcome.evaluation_instant is not None
    )


def test_market_data_after_t1_cannot_change_the_t1_run_metrics_or_report(
    scenario: SimpleNamespace,
) -> None:
    assert scenario.run_t1_after_collapse == scenario.run_t1
    assert scenario.metrics_t1_after_collapse == scenario.metrics_t1
    assert scenario.report_t1_after_collapse == scenario.report_t1


def test_a_decision_frozen_after_t1_does_not_enter_the_t1_run(scenario: SimpleNamespace) -> None:
    assert scenario.run_t1_after_f2 == scenario.run_t1
    assert scenario.f2 not in scenario.run_t1_after_f2.records
    assert scenario.f2 in scenario.run_late.records


def test_a_later_cutoff_changes_the_derived_view_but_not_the_decision(
    scenario: SimpleNamespace,
) -> None:
    at_t2 = scenario.run_t2

    assert _states(at_t2) == [_MEASURED, _MEASURED, _MEASURED, _MEASURED]
    assert _states(scenario.run_t1) != _states(at_t2)
    assert at_t2.records == scenario.run_t1.records
    assert at_t2.records[0] == scenario.f1


def test_metrics_group_by_strategy_and_horizon_over_measured_returns_only(
    scenario: SimpleNamespace,
) -> None:
    metrics = scenario.metrics_t1

    assert [(m.strategy_identity.identity, m.horizon) for m in metrics] == [
        ("alpha", _H3),
        ("alpha", _H1),
        ("beta", _H3),
        ("beta", _H1),
    ]
    for entry in metrics:
        assert entry.total_count == (
            entry.measured_count + entry.pending_count + entry.undefined_return_basis_count
        )
    alpha_h3, alpha_h1, _, beta_h1 = metrics
    assert (alpha_h3.pending_count, alpha_h3.average_forward_return) == (1, None)
    assert alpha_h1.measured_count == 1
    assert alpha_h1.average_forward_return == scenario.run_t1.measurements[1].outcome.forward_return
    assert beta_h1.average_forward_return == alpha_h1.average_forward_return


def test_the_report_describes_the_run_and_keeps_its_cutoff(scenario: SimpleNamespace) -> None:
    report = scenario.report_late

    assert report.run == scenario.run_late
    assert report.contract == _ES_DEC
    assert report.timeframe == _DAILY
    assert report.available_through == _instant(40)  # later than every decision and bar
    assert report.last_decision_instant == _D2
    assert report.first_decision_instant == _D
    assert report.horizons == _HORIZONS
    assert report.record_count == 3
    assert report.strategy_identities == (StrategyIdentity("alpha"), StrategyIdentity("beta"))


def test_measurement_run_metrics_and_report_persist_nothing(scenario: SimpleNamespace) -> None:
    assert scenario.tables_after_derived == scenario.tables_before_derived
    assert scenario.rows_after_derived == scenario.rows_before_derived
    user_tables = {name for name in scenario.tables_after_derived if not name.startswith("sqlite_")}
    assert user_tables == {"futures_ohlcv", "futures_forward_research_records"}


# -- Isolation ----------------------------------------------------------------


def test_the_other_contracts_bars_and_decisions_never_enter_the_es_view(
    scenario: SimpleNamespace,
) -> None:
    assert scenario.mes.contract == _MES_DEC
    assert scenario.mes.result.recommendation.action == _HOLD
    assert _records(scenario.path, _MES_DEC) == (scenario.mes,)
    for record in scenario.run_late.records:
        assert record.contract == _ES_DEC
        closes = record.result.market_observation_context.recent_closes
        assert all(quote.value not in (5000, -3000) for quote in closes)
    stored = SQLiteFuturesHistoricalMarketDataRepository(scenario.path).get_bars(
        FuturesHistoricalMarketDataQuery(_ES_DEC, _DAILY)
    )
    assert all(bar.contract == _ES_DEC for bar in stored)


def test_two_strategies_at_one_instant_are_distinct_frozen_decisions(
    scenario: SimpleNamespace,
) -> None:
    alpha, beta = scenario.reloaded

    assert alpha.decision_instant == beta.decision_instant == _D
    assert alpha.natural_key != beta.natural_key
    assert (alpha.strategy_identity.identity, beta.strategy_identity.identity) == ("alpha", "beta")


# ---------------------------------------------------------------------------
# Restart, conflict, and compact paths
# ---------------------------------------------------------------------------


def test_everything_survives_closing_and_reopening_the_database(tmp_path: Path) -> None:
    path = tmp_path / "restart.sqlite3"
    _store_bars(path, _PHASE_A + _SESSION_26)
    frozen = _freeze(path, _D)
    run_before = _run(path, _T1)

    # Every adapter above is gone; only the file remains.
    reopened_records = SQLiteFuturesForwardResearchRecordRepository(path)
    reopened_market = SQLiteFuturesHistoricalMarketDataRepository(path)

    assert reopened_records.get_records(_ES_QUERY) == (frozen,)
    assert reopened_market.get_bars(FuturesHistoricalMarketDataQuery(_ES_DEC, _DAILY)) == (
        _PHASE_A + _SESSION_26
    )
    assert (
        RunFuturesForwardResearchUseCase(reopened_records, reopened_market).execute(
            _ES_QUERY, _HORIZONS, _T1
        )
        == run_before
    )


def test_a_different_decision_under_the_same_key_conflicts_in_real_storage(
    tmp_path: Path,
) -> None:
    original_path = tmp_path / "original.sqlite3"
    _store_bars(original_path, _PHASE_A)
    f1 = _freeze(original_path, _D)

    altered = list(_PHASE_A)
    altered[18] = _bar(19, "7700")  # a visible pre-D bar, in a fresh history
    other_path = tmp_path / "altered.sqlite3"
    _store_bars(other_path, tuple(altered))
    f1_modified = _freeze(other_path, _D)

    assert f1_modified.natural_key == f1.natural_key
    assert f1_modified != f1
    with pytest.raises(FuturesForwardResearchRecordConflictError):
        SQLiteFuturesForwardResearchRecordStore(original_path).store((f1_modified,))
    assert _records(original_path) == (f1,)
    assert _forward_rows(original_path) == 1


def test_a_zero_decision_quote_round_trips_and_ends_unavailable(tmp_path: Path) -> None:
    path = tmp_path / "zero.sqlite3"
    closes = [str(-40 + 2 * index) for index in range(19)] + ["0"]
    _store_bars(path, tuple(_bar(session, close) for session, close in enumerate(closes, start=1)))
    record = _freeze(path, _instant(20))
    assert record.result.market_observation_context.latest_quote == QuoteValue(Decimal("0"))
    assert _records(path) == (record,)

    before = _measure(path, record, _H1, _instant(20))
    _store_bars(path, (_bar(21, "5"),))
    after = _measure(path, record, _H1, _instant(21))

    assert before.state is _PENDING
    assert after.state is _UNAVAILABLE
    assert after.outcome.unavailable_reason == _UNDEFINED
    assert after.outcome.decision_quote == QuoteValue(Decimal("0"))


def test_a_positive_decision_into_a_negative_quote_measures_minus_150_percent(
    tmp_path: Path,
) -> None:
    path = tmp_path / "negative.sqlite3"
    closes = ["5"] * 15 + ["6", "7", "8", "9", "10"]
    _store_bars(path, tuple(_bar(session, close) for session, close in enumerate(closes, start=1)))
    record = _freeze(path, _instant(20))
    _store_bars(path, (_bar(21, "-5"),))

    measurement = _measure(path, record, _H1, _instant(21))

    assert record.result.market_observation_context.latest_quote == QuoteValue(Decimal("10"))
    assert measurement.state is _MEASURED
    assert measurement.outcome.forward_return == Percentage(Decimal("-150"))


def test_semantic_timestamp_order_holds_through_every_real_adapter(tmp_path: Path) -> None:
    """Session 21 completes half a second after session 20, in the same second.

    As stored text ``...21:00:00.5Z`` sorts before ``...21:00:00Z``. Bars are
    written in reverse; decisions are frozen at the later instant first.
    """
    path = tmp_path / "trap.sqlite3"
    whole = _instant(20)
    fractional = PointInTime(f"{_DATES[19].isoformat()}T21:00:00.5Z")
    assert fractional.value < whole.value
    bars = [*_PHASE_A[:20], _bar(21, "7606", instant=fractional)]
    _store_bars(path, tuple(reversed(bars)))

    stored = SQLiteFuturesHistoricalMarketDataRepository(path).get_bars(
        FuturesHistoricalMarketDataQuery(_ES_DEC, _DAILY)
    )
    assert [bar.point_in_time for bar in stored[-2:]] == [whole, fractional]

    late = _freeze(path, fractional)
    early = _freeze(path, whole)
    assert (early.decision_instant, late.decision_instant) == (whole, fractional)
    assert _records(path) == (early, late)

    measurement = _measure(path, early, _H1, fractional)
    assert measurement.outcome.evaluation_instant == fractional
    assert measurement.outcome.evaluation_quote == QuoteValue(Decimal("7606"))

    run = _run(path, fractional, (_H1,))
    assert run.records == (early, late)
