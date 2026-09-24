"""Acceptance: futures paper trading against the real SQLite stores.

Market history, frozen forward decisions, paper orders and paper fills all live
in real temporary SQLite files and are written and read only through the
production adapters. Every adapter is constructed afresh for every call, so
nothing depends on process memory. There is no provider, no network, no
calendar and no credential.

The main scenario plays one ES Dec 2026 paper-trading timeline for strategy
alpha in portfolio ``futures-paper-alpha`` with a target of one contract:

1. Sessions 1..25: fifteen flat closes, then a steady rise. D1 = session 25
   freezes BUY. Executed at exactly D1 it is a pending BUY 1 order; the
   decision bar is never a fill.
2. Session 26 (F1) arrives with an OPEN far from every other quote. The retry
   settles the order at F1.open; the portfolio is long 1 at F1.open.
3. Session 27 collapses: D2 freezes SELL. Against long 1 the target of short 1
   needs SELL 2, which fills at session 28's OPEN.
4. Session 28 is still bearish: D3 freezes SELL, the target is already met.
5. Session 29 is unchanged: D4 freezes HOLD, exposure stays short 1.
6. A paper run at T = session 29 reproduces all of that. Sessions 30 and 31
   arrive later; D5 = session 30 freezes BUY. T is unchanged by any of it;
   later cutoffs see D5 pending, then filled.

Beta decisions, an ES Mar series and a beta portfolio share the database as
noise. Compact secondary databases cover the paths the main signal fixture
cannot produce naturally.

Accepted 9.9 debt, deliberately not tested as a failure: no-action outcomes are
not persisted, so a historical back-fill could turn a previously derived HOLD
or TARGET_ALREADY_MET into an action on a later rerun.
"""

from __future__ import annotations

import shutil
import sqlite3
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest
from northstar_application.application_services import (
    BuildFuturesPaperPortfolioUseCase,
    BuildFuturesPaperTradingReportUseCase,
    CalculateFuturesPaperTradingMetricsUseCase,
    FreezeFuturesForwardResearchDecisionUseCase,
    FuturesExecutionIntentNoIntentReason,
    FuturesForwardResearchRecord,
    FuturesPaperExecutionIdentityService,
    FuturesPaperTradingDecisionResult,
    FuturesPaperTradingRun,
    FuturesPaperTradingStrategyContractMetrics,
    RunFuturesPaperTradingDecisionUseCase,
    RunFuturesPaperTradingUseCase,
)
from northstar_application.ports import (
    FuturesForwardResearchRecordQuery,
    FuturesHistoricalMarketDataQuery,
    FuturesPaperFillConflictError,
    FuturesPaperFillQuery,
    FuturesPaperOrderConflictError,
    FuturesPaperOrderQuery,
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
from northstar_core.paper_trading import (
    FuturesContractCount,
    FuturesExecutionIntent,
    FuturesPaperFill,
    FuturesPaperOrder,
    FuturesPaperPortfolio,
    FuturesPosition,
    OrderSide,
    PaperFillIdentity,
    PaperOrderIdentity,
    PaperPortfolioIdentity,
)
from northstar_core.strategy import (
    FuturesAssetAnalysisGenerator,
    RecommendationAction,
    Strategy,
    StrategyIdentity,
)

from northstar_infrastructure.market_data import (
    SQLiteFuturesHistoricalMarketDataRepository,
    SQLiteFuturesHistoricalMarketDataStore,
    initialize_futures_market_data_schema,
)
from northstar_infrastructure.persistence import (
    SQLiteFuturesForwardResearchRecordRepository,
    SQLiteFuturesForwardResearchRecordStore,
    SQLiteFuturesPaperFillRepository,
    SQLiteFuturesPaperFillStore,
    SQLiteFuturesPaperOrderRepository,
    SQLiteFuturesPaperOrderStore,
    initialize_futures_forward_research_record_schema,
    initialize_futures_paper_trading_schema,
)

_CME = ExchangeCode("CME")
_ES_DEC = FuturesContract(FuturesProductReference(Symbol("ES"), _CME), ExpirationDate("2026-12-18"))
_ES_MAR = FuturesContract(FuturesProductReference(Symbol("ES"), _CME), ExpirationDate("2027-03-19"))
_DAILY = Timeframe("1d")
_ALPHA = Strategy(StrategyIdentity("alpha"))
_BETA = Strategy(StrategyIdentity("beta"))
_GAMMA = Strategy(StrategyIdentity("gamma"))
_ALPHA_PORTFOLIO = PaperPortfolioIdentity("futures-paper-alpha")
_BETA_PORTFOLIO = PaperPortfolioIdentity("futures-paper-beta")
_ONE = FuturesContractCount(1)

BUY = OrderSide.BUY
SELL = OrderSide.SELL
_HOLD = FuturesExecutionIntentNoIntentReason.HOLD
_MET = FuturesExecutionIntentNoIntentReason.TARGET_ALREADY_MET


def _session_dates(count: int) -> list[date]:
    dates: list[date] = []
    day = date(2026, 6, 1)
    while len(dates) < count:
        if day.weekday() < 5 and day != date(2026, 6, 19):
            dates.append(day)
        day += timedelta(days=1)
    return dates


_DATES = _session_dates(40)


def _instant(session: int, clock: str = "21:00:00") -> PointInTime:
    return PointInTime(f"{_DATES[session - 1].isoformat()}T{clock}Z")


def _bar(
    session: int,
    close: str,
    *,
    open_: str | None = None,
    high: str | None = None,
    low: str | None = None,
    volume: str = "1000",
    contract: FuturesContract = _ES_DEC,
    instant: PointInTime | None = None,
) -> FuturesOHLCVBar:
    closing = Decimal(close)
    opening = Decimal(open_) if open_ is not None else closing
    return FuturesOHLCVBar(
        contract=contract,
        point_in_time=instant if instant is not None else _instant(session),
        timeframe=_DAILY,
        open=QuoteValue(opening),
        high=QuoteValue(Decimal(high) if high is not None else max(opening, closing) + 2),
        low=QuoteValue(Decimal(low) if low is not None else min(opening, closing) - 2),
        close=QuoteValue(closing),
        volume=Quantity(Decimal(volume)),
    )


_PHASE_A_CLOSES = ["7600"] * 15 + [str(7601 + index) for index in range(10)]


def _phase_a(contract: FuturesContract = _ES_DEC, sessions: int = 25) -> tuple:
    return tuple(
        _bar(session, close, contract=contract)
        for session, close in enumerate(_PHASE_A_CLOSES[:sessions], start=1)
    )


# F1: an OPEN far from D1's close and from F1's own high, low and close.
_F1_BAR = _bar(26, "7611", open_="7650", high="7700", low="7600")
_D2_BAR = _bar(27, "3000", open_="7600", high="7610", low="2990", volume="250000")
_F2_BAR = _bar(28, "2900", open_="2950", high="3050", low="2800", volume="250000")
_S29_BAR = _bar(29, "2900", open_="2900", high="2950", low="2850")
_S30_BAR = _bar(30, "30000", open_="2950", high="30010", low="2900", volume="999999")
_S31_BAR = _bar(31, "30200", open_="30500", high="31000", low="30100")
_ES_MAR_NOISE = tuple(_bar(s, "-3000", volume="77", contract=_ES_MAR) for s in range(1, 32))

_D1, _F1, _D2, _F2, _D3, _D4 = (_instant(s) for s in (25, 26, 27, 28, 28, 29))
_T = _instant(29)
_D5, _F5 = _instant(30), _instant(31)
_LATE = PointInTime("2026-08-31T00:00:00Z")


# ---------------------------------------------------------------------------
# Production adapters and use cases, rebuilt for every call
# ---------------------------------------------------------------------------


def _initialize(path: Path) -> None:
    with sqlite3.connect(path) as connection:
        initialize_futures_market_data_schema(connection)
        initialize_futures_forward_research_record_schema(connection)
        initialize_futures_paper_trading_schema(connection)


def _store_bars(path: Path, *bars: FuturesOHLCVBar) -> int:
    return SQLiteFuturesHistoricalMarketDataStore(path).store(tuple(bars))


def _freeze(
    path: Path,
    as_of: PointInTime,
    strategy: Strategy = _ALPHA,
    contract: FuturesContract = _ES_DEC,
) -> FuturesForwardResearchRecord:
    record = FreezeFuturesForwardResearchDecisionUseCase(
        SQLiteFuturesHistoricalMarketDataRepository(path),
        FuturesAssetAnalysisGenerator(),
        SQLiteFuturesForwardResearchRecordStore(path),
    ).execute(contract, _DAILY, strategy, as_of)
    assert record is not None
    return record


def _ports(path: Path, **overrides: object) -> dict:
    ports: dict[str, object] = {
        "forward_repository": SQLiteFuturesForwardResearchRecordRepository(path),
        "market_repository": SQLiteFuturesHistoricalMarketDataRepository(path),
        "order_store": SQLiteFuturesPaperOrderStore(path),
        "order_repository": SQLiteFuturesPaperOrderRepository(path),
        "fill_store": SQLiteFuturesPaperFillStore(path),
        "fill_repository": SQLiteFuturesPaperFillRepository(path),
    }
    ports.update(overrides)
    return ports


def _decide(
    path: Path,
    record: FuturesForwardResearchRecord,
    through: PointInTime,
    *,
    portfolio: PaperPortfolioIdentity = _ALPHA_PORTFOLIO,
    target: FuturesContractCount = _ONE,
    **overrides: object,
) -> FuturesPaperTradingDecisionResult:
    return RunFuturesPaperTradingDecisionUseCase(**_ports(path, **overrides)).execute(
        record, portfolio, target, through
    )


def _paper_run(
    path: Path,
    cutoff: PointInTime,
    *,
    strategy: Strategy = _ALPHA,
    portfolio: PaperPortfolioIdentity = _ALPHA_PORTFOLIO,
    target: FuturesContractCount = _ONE,
    contract: FuturesContract = _ES_DEC,
) -> FuturesPaperTradingRun:
    return RunFuturesPaperTradingUseCase(**_ports(path)).execute(
        FuturesForwardResearchRecordQuery(contract, _DAILY),
        portfolio,
        strategy.strategy_identity,
        target,
        cutoff,
    )


def _metrics(run: FuturesPaperTradingRun) -> tuple:
    return CalculateFuturesPaperTradingMetricsUseCase().execute(run)


def _report(run: FuturesPaperTradingRun):
    return BuildFuturesPaperTradingReportUseCase().execute(run)


def _orders(path: Path, portfolio: PaperPortfolioIdentity = _ALPHA_PORTFOLIO) -> tuple:
    return SQLiteFuturesPaperOrderRepository(path).get_orders(FuturesPaperOrderQuery(portfolio))


def _fills(path: Path, portfolio: PaperPortfolioIdentity = _ALPHA_PORTFOLIO) -> tuple:
    return SQLiteFuturesPaperFillRepository(path).get_fills(FuturesPaperFillQuery(portfolio))


def _records(path: Path, contract: FuturesContract = _ES_DEC) -> tuple:
    return SQLiteFuturesForwardResearchRecordRepository(path).get_records(
        FuturesForwardResearchRecordQuery(contract, _DAILY)
    )


def _bars(path: Path, contract: FuturesContract = _ES_DEC) -> tuple:
    return SQLiteFuturesHistoricalMarketDataRepository(path).get_bars(
        FuturesHistoricalMarketDataQuery(contract, _DAILY)
    )


def _portfolio(
    path: Path,
    as_of: PointInTime,
    portfolio: PaperPortfolioIdentity = _ALPHA_PORTFOLIO,
    strategy: Strategy = _ALPHA,
) -> FuturesPaperPortfolio:
    return BuildFuturesPaperPortfolioUseCase().execute(
        portfolio, strategy.strategy_identity, _fills(path, portfolio), as_of
    )


def _query(path: Path, sql: str, parameters: tuple = ()) -> list[tuple]:
    with sqlite3.connect(path) as connection:
        return connection.execute(sql, parameters).fetchall()


def _count(path: Path, table: str) -> int:
    return _query(path, f"SELECT COUNT(*) FROM {table}")[0][0]  # noqa: S608


def _paper_rows(path: Path) -> tuple[list[tuple], list[tuple]]:
    return (
        sorted(_query(path, "SELECT * FROM futures_paper_orders")),
        sorted(_query(path, "SELECT * FROM futures_paper_fills")),
    )


def _order_id(record: FuturesForwardResearchRecord, portfolio=_ALPHA_PORTFOLIO):
    return FuturesPaperExecutionIdentityService().order_identity(record, portfolio)


def _position(net: int, entry: str, contract: FuturesContract = _ES_DEC) -> FuturesPosition:
    return FuturesPosition(contract, net, QuoteValue(Decimal(entry)))


# ---------------------------------------------------------------------------
# The main paper-trading timeline
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def scenario(tmp_path_factory: pytest.TempPathFactory) -> SimpleNamespace:
    path = tmp_path_factory.mktemp("paper") / "paper.sqlite3"
    s = SimpleNamespace(path=path)
    _initialize(path)
    s.fresh_schema = {
        name: sql for name, sql in _query(path, "SELECT name, sql FROM sqlite_master")
    }

    # Phase A and the D1 freezes, plus beta and ES Mar noise.
    assert _store_bars(path, *_phase_a()) == 25
    assert _store_bars(path, *_ES_MAR_NOISE) == 31
    s.d1 = _freeze(path, _D1)
    s.beta_d1 = _freeze(path, _D1, _BETA)
    s.mar_d1 = _freeze(path, _D1, _ALPHA, _ES_MAR)
    s.records_after_freeze = _records(path)
    s.paper_counts_after_freeze = (
        _count(path, "futures_paper_orders"),
        _count(path, "futures_paper_fills"),
    )

    # D1 executed at exactly D1: pending.
    s.d1_pending = _decide(path, s.d1, _D1)
    s.counts_after_d1 = (_count(path, "futures_paper_orders"), _count(path, "futures_paper_fills"))

    # F1 arrives; the same frozen decision settles at F1.open, then retries.
    _store_bars(path, _F1_BAR)
    s.d1_filled = _decide(path, s.d1, _F1)
    s.counts_after_f1 = (_count(path, "futures_paper_orders"), _count(path, "futures_paper_fills"))
    s.portfolio_at_f1 = _portfolio(path, _F1)
    s.d1_retry = _decide(path, s.d1, _F1)
    s.counts_after_retry = (
        _count(path, "futures_paper_orders"),
        _count(path, "futures_paper_fills"),
    )
    s.d1_facts = (_orders(path), _fills(path))

    # D2: collapse, SELL; reversal SELL 2, pending at D2, filled at F2.
    _store_bars(path, _D2_BAR)
    s.d2 = _freeze(path, _D2)
    s.d2_pending = _decide(path, s.d2, _D2)
    _store_bars(path, _F2_BAR)
    s.d2_filled = _decide(path, s.d2, _F2)
    s.portfolio_at_f2 = _portfolio(path, _F2)

    # D3: still bearish, target already met. D4: unchanged close, HOLD.
    s.d3 = _freeze(path, _D3)
    s.d3_result = _decide(path, s.d3, _D3)
    _store_bars(path, _S29_BAR)
    s.d4 = _freeze(path, _D4)
    s.d4_result = _decide(path, s.d4, _D4)

    # Beta in its own portfolio, then an attempt to mix it into alpha's.
    s.beta_result = _decide(path, s.beta_d1, _T, portfolio=_BETA_PORTFOLIO)
    s.rows_before_mixed = _paper_rows(path)
    with pytest.raises(ValueError) as mixed:
        _decide(path, s.beta_d1, _T)
    s.mixed_error = mixed.value
    s.rows_after_mixed = _paper_rows(path)

    # The paper run at T.
    s.run_t = _paper_run(path, _T)
    s.metrics_t = _metrics(s.run_t)
    s.report_t = _report(s.run_t)
    s.rows_at_t = _paper_rows(path)

    # Restart: a copied file read by brand-new adapters.
    s.copy = path.with_name("restarted.sqlite3")
    shutil.copy(path, s.copy)
    s.restart = SimpleNamespace(
        bars=_bars(s.copy),
        records=_records(s.copy),
        orders=_orders(s.copy),
        fills=_fills(s.copy),
        portfolio=_portfolio(s.copy, _T),
        run=_paper_run(s.copy, _T),
    )
    s.restart.metrics = _metrics(s.restart.run)
    s.restart.report = _report(s.restart.run)
    s.before_restart = SimpleNamespace(
        bars=_bars(path),
        records=_records(path),
        orders=_orders(path),
        fills=_fills(path),
        portfolio=_portfolio(path, _T),
    )

    # Later market data, then a later frozen decision, never leak into T.
    _store_bars(path, _S30_BAR, _S31_BAR)
    s.run_t_after_bars = _paper_run(path, _T)
    s.metrics_t_after_bars = _metrics(s.run_t_after_bars)
    s.report_t_after_bars = _report(s.run_t_after_bars)
    s.d5 = _freeze(path, _D5)
    s.run_t_after_d5 = _paper_run(path, _T)

    # Later cutoffs evolve.
    s.run_d5 = _paper_run(path, _D5)
    s.metrics_d5 = _metrics(s.run_d5)
    s.rows_at_d5 = _paper_rows(path)
    s.run_f5 = _paper_run(path, _F5)
    s.run_late = _paper_run(path, _LATE)
    s.metrics_late = _metrics(s.run_late)
    s.report_late = _report(s.run_late)
    s.rows_final = _paper_rows(path)
    s.tables = {
        name for (name,) in _query(path, "SELECT name FROM sqlite_master WHERE type='table'")
    }
    s.final_schema = {
        name: sql for name, sql in _query(path, "SELECT name, sql FROM sqlite_master")
    }
    return s


# -- D1 freeze and pending order ----------------------------------------------


def test_d1_is_a_persisted_buy_with_no_paper_facts_yet(scenario) -> None:
    assert scenario.d1.result.recommendation.action == RecommendationAction("BUY")
    assert scenario.d1.decision_instant == _D1
    assert scenario.records_after_freeze == (scenario.d1, scenario.beta_d1)
    assert scenario.paper_counts_after_freeze == (0, 0)


def test_d1_at_its_own_instant_is_a_pending_buy_1(scenario) -> None:
    result = scenario.d1_pending

    assert result.portfolio_before.positions == ()
    assert result.order.identity == _order_id(scenario.d1)
    assert (result.order.intent.side, result.order.intent.contracts) == (BUY, _ONE)
    assert result.fill is None
    assert scenario.counts_after_d1 == (1, 0)


# -- Next-bar OPEN fill --------------------------------------------------------


def test_the_next_bar_open_fills_the_pending_order(scenario) -> None:
    fill = scenario.d1_filled.fill

    assert _F1_BAR.open not in (_F1_BAR.close, _F1_BAR.high, _F1_BAR.low)
    assert _F1_BAR.open != _phase_a()[-1].close
    assert fill.fill_quote == _F1_BAR.open == QuoteValue(Decimal("7650"))
    assert fill.filled_at == _F1
    assert fill.order_identity == scenario.d1_pending.order.identity
    assert scenario.counts_after_f1 == (1, 1)
    assert scenario.portfolio_at_f1.positions == (_position(1, "7650"),)


def test_a_retry_is_idempotent(scenario) -> None:
    assert scenario.d1_retry == scenario.d1_filled
    assert scenario.counts_after_retry == (1, 1)
    assert scenario.d1_facts == ((scenario.d1_filled.order,), (scenario.d1_filled.fill,))


def test_the_retry_never_sees_its_own_fill(scenario) -> None:
    """The D1 fill is after D1, so D1's own portfolio stays flat and BUY 1 recurs."""
    assert scenario.d1_filled.fill.filled_at.compare(_D1) > 0
    for result in (scenario.d1_filled, scenario.d1_retry):
        assert result.portfolio_before.positions == ()
        assert result.decision.no_intent_reason is None
        assert result.decision.intent.contracts == _ONE


# -- D2 reversal ---------------------------------------------------------------


def test_d2_is_a_sell_that_reverses_long_1_with_sell_2(scenario) -> None:
    pending = scenario.d2_pending

    assert scenario.d2.result.recommendation.action == RecommendationAction("SELL")
    assert pending.portfolio_before.positions == (_position(1, "7650"),)
    assert pending.order.intent.side is SELL
    assert pending.order.intent.contracts == FuturesContractCount(2)
    assert pending.fill is None


def test_the_reversal_fills_at_f2_open_and_leaves_short_1(scenario) -> None:
    filled = scenario.d2_filled

    assert filled.fill.fill_quote == _F2_BAR.open
    assert filled.fill.filled_at == _F2
    assert scenario.portfolio_at_f2.positions == (_position(-1, "2950"),)
    assert filled.order.identity != scenario.d1_filled.order.identity
    assert filled.fill.identity != scenario.d1_filled.fill.identity
    assert scenario.before_restart.orders[0] == scenario.d1_filled.order
    assert scenario.before_restart.fills[0] == scenario.d1_filled.fill


def test_d3_sell_against_short_1_is_target_already_met(scenario) -> None:
    result = scenario.d3_result

    assert scenario.d3.result.recommendation.action == RecommendationAction("SELL")
    assert result.decision.no_intent_reason is _MET
    assert (result.order, result.fill) == (None, None)
    assert result.portfolio_before.positions == (_position(-1, "2950"),)


def test_d4_hold_keeps_the_short(scenario) -> None:
    result = scenario.d4_result

    assert scenario.d4.result.recommendation.action == RecommendationAction("HOLD")
    assert result.decision.no_intent_reason is _HOLD
    assert (result.order, result.fill) == (None, None)
    assert result.portfolio_before.positions == (_position(-1, "2950"),)


# -- Isolation -----------------------------------------------------------------


def test_beta_executes_only_in_its_own_portfolio(scenario) -> None:
    beta = scenario.beta_result

    assert beta.order.identity == _order_id(scenario.beta_d1, _BETA_PORTFOLIO)
    assert beta.fill.fill_quote == _F1_BAR.open
    assert all(
        order.intent.strategy_identity == _ALPHA.strategy_identity
        for order in scenario.run_t.orders
    )


def test_a_mixed_strategy_portfolio_is_rejected_before_any_write(scenario) -> None:
    assert "one paper portfolio belongs to one strategy" in str(scenario.mixed_error)
    assert scenario.rows_after_mixed == scenario.rows_before_mixed


def test_the_es_mar_series_and_decision_stay_separate(scenario) -> None:
    assert _records(scenario.path, _ES_MAR) == (scenario.mar_d1,)
    assert {bar.contract for bar in scenario.before_restart.bars} == {_ES_DEC}
    assert {r.record.contract for r in scenario.run_t.results} == {_ES_DEC}
    assert [entry.contract for entry in scenario.metrics_t] == [_ES_DEC]


# -- Paper run, metrics, report at T ------------------------------------------


def test_the_run_at_t_replays_the_whole_alpha_timeline(scenario) -> None:
    run = scenario.run_t

    assert [r.record for r in run.results] == [scenario.d1, scenario.d2, scenario.d3, scenario.d4]
    assert all(r.available_through == _T for r in run.results)
    assert run.portfolio.as_of == _T
    assert run.portfolio.positions == (_position(-1, "2950"),)
    assert run.orders == (scenario.d1_filled.order, scenario.d2_filled.order)
    assert run.fills == (scenario.d1_filled.fill, scenario.d2_filled.fill)
    assert scenario.rows_at_t == scenario.rows_after_mixed


def test_the_metrics_at_t_count_execution_facts(scenario) -> None:
    assert scenario.metrics_t == (
        FuturesPaperTradingStrategyContractMetrics(
            contract=_ES_DEC,
            strategy_identity=_ALPHA.strategy_identity,
            decision_count=4,
            hold_count=1,
            target_already_met_count=1,
            order_count=2,
            filled_order_count=2,
            pending_order_count=0,
            contracts_bought=1,
            contracts_sold=2,  # the reversal traded 2, not the final short size of 1
            current_net_contracts=-1,
        ),
    )


def test_the_report_at_t(scenario) -> None:
    report = scenario.report_t

    assert report.run == scenario.run_t
    assert report.metrics == scenario.metrics_t
    assert (report.portfolio_identity, report.strategy_identity) == (
        _ALPHA_PORTFOLIO,
        _ALPHA.strategy_identity,
    )
    assert (report.contract, report.timeframe) == (_ES_DEC, _DAILY)
    assert report.target_contracts == _ONE
    assert report.available_through == _T
    assert (report.decision_count, report.order_count, report.fill_count) == (4, 2, 2)
    assert report.pending_order_count == 0
    assert report.positions == scenario.run_t.portfolio.positions
    assert (report.first_decision_instant, report.last_decision_instant) == (_D1, _D4)


# -- Restart -------------------------------------------------------------------


def test_everything_reloads_exactly_after_restart(scenario) -> None:
    restart, before = scenario.restart, scenario.before_restart

    assert restart.bars == before.bars
    assert restart.records == before.records
    assert restart.orders == before.orders
    assert restart.fills == before.fills
    assert restart.portfolio == before.portfolio
    assert restart.run == scenario.run_t
    assert restart.metrics == scenario.metrics_t
    assert restart.report == scenario.report_t


# -- Same and later cutoffs ----------------------------------------------------


def test_later_market_data_and_decisions_never_leak_into_t(scenario) -> None:
    assert scenario.run_t_after_bars == scenario.run_t
    assert scenario.metrics_t_after_bars == scenario.metrics_t
    assert scenario.report_t_after_bars == scenario.report_t
    assert scenario.d5.decision_instant.compare(_T) > 0
    assert scenario.run_t_after_d5 == scenario.run_t


def test_later_cutoffs_execute_d5_pending_then_filled(scenario) -> None:
    assert scenario.d5.result.recommendation.action == RecommendationAction("BUY")
    pending = scenario.run_d5.results[-1]
    filled = scenario.run_f5.results[-1]

    assert (pending.order.intent.side, pending.order.intent.contracts.value) == (BUY, 2)
    assert pending.fill is None
    assert scenario.run_d5.portfolio.positions == (_position(-1, "2950"),)
    assert filled.order == pending.order
    assert filled.fill.fill_quote == _S31_BAR.open
    assert scenario.run_f5.portfolio.positions == (_position(1, "30500"),)


def test_pending_metrics_count_no_executed_contracts(scenario) -> None:
    (entry,) = scenario.metrics_d5

    assert (entry.order_count, entry.filled_order_count, entry.pending_order_count) == (3, 2, 1)
    assert (entry.contracts_bought, entry.contracts_sold) == (1, 2)
    assert entry.current_net_contracts == -1


def test_earlier_facts_are_never_rewritten(scenario) -> None:
    orders_t, fills_t = scenario.rows_at_t
    orders_final, fills_final = scenario.rows_final

    assert set(orders_t) <= set(orders_final)
    assert set(fills_t) <= set(fills_final)
    assert len(orders_final) == len(orders_t) + 1
    assert len(fills_final) == len(fills_t) + 1


def test_the_report_keeps_an_explicit_later_cutoff(scenario) -> None:
    report = scenario.report_late

    assert report.available_through == _LATE
    assert report.last_decision_instant == _D5
    assert report.run.fills[-1].filled_at == _F5
    assert report.positions == (_position(1, "30500"),)
    assert scenario.metrics_late[0].contracts_bought == 3
    assert scenario.metrics_late[0].contracts_sold == 2
    assert scenario.metrics_late[0].order_count == (
        scenario.metrics_late[0].filled_order_count + scenario.metrics_late[0].pending_order_count
    )


# -- Schema surface ------------------------------------------------------------


def test_only_orders_and_fills_are_added_and_existing_tables_are_untouched(scenario) -> None:
    assert scenario.tables == {
        "futures_ohlcv",
        "futures_forward_research_records",
        "futures_paper_orders",
        "futures_paper_fills",
    }
    assert scenario.final_schema == scenario.fresh_schema


# ---------------------------------------------------------------------------
# Compact secondary databases
# ---------------------------------------------------------------------------


def _fresh(tmp_path: Path, name: str, *bars: FuturesOHLCVBar) -> Path:
    path = tmp_path / f"{name}.sqlite3"
    _initialize(path)
    _store_bars(path, *(bars or _phase_a()))
    return path


def test_only_persisted_frozen_records_execute(tmp_path: Path) -> None:
    source = _fresh(tmp_path, "source")
    target = _fresh(tmp_path, "target")
    unpersisted = _freeze(source, _D1)

    with pytest.raises(ValueError, match="not persisted"):
        _decide(target, unpersisted, _D1)
    assert _paper_rows(target) == ([], [])

    SQLiteFuturesForwardResearchRecordStore(target).store((unpersisted,))
    accepted = _decide(target, unpersisted, _D1)
    assert accepted.order is not None

    # Same natural key, different evidence: session 10 differs inside the window.
    shifted = [_bar(s, "7599") if s == 10 else bar for s, bar in enumerate(_phase_a(), start=1)]
    impostor = _freeze(_fresh(tmp_path, "impostor", *shifted), _D1)
    assert impostor.natural_key == unpersisted.natural_key
    rows = _paper_rows(target)
    with pytest.raises(ValueError, match="differs from the frozen"):
        _decide(target, impostor, _D1, portfolio=PaperPortfolioIdentity("other"))
    assert _paper_rows(target) == rows


def test_one_frozen_decision_runs_independently_in_two_portfolios(tmp_path: Path) -> None:
    path = _fresh(tmp_path, "portfolios", *_phase_a(), _F1_BAR)
    record = _freeze(path, _D1)
    a, b = PaperPortfolioIdentity("a"), PaperPortfolioIdentity("b")

    first = _decide(path, record, _F1, portfolio=a)
    second = _decide(path, record, _F1, portfolio=b, target=FuturesContractCount(3))

    assert first.order.identity != second.order.identity
    assert _orders(path, a) == (first.order,)
    assert _orders(path, b) == (second.order,)
    assert _portfolio(path, _F1, a).positions == (_position(1, "7650"),)
    assert _portfolio(path, _F1, b).positions == (_position(3, "7650"),)


def test_a_back_filled_next_bar_conflicts_with_the_persisted_fill(tmp_path: Path) -> None:
    f2 = _bar(27, "7612", open_="7777", high="7800", low="7600")
    path = _fresh(tmp_path, "backfill", *_phase_a(), f2)
    record = _freeze(path, _D1)
    original = _decide(path, record, _instant(27)).fill
    assert original.filled_at == _instant(27)
    assert original.fill_quote == QuoteValue(Decimal("7777"))
    rows = _paper_rows(path)

    _store_bars(path, _F1_BAR)  # the missing session between D1 and the old fill bar
    with pytest.raises(FuturesPaperFillConflictError):
        _decide(path, record, _instant(27))
    with pytest.raises(FuturesPaperFillConflictError):
        _paper_run(path, _instant(27))

    assert _paper_rows(path) == rows
    assert _fills(path) == (original,)


def _seed_long(path: Path, net: int) -> None:
    """Store an earlier filled BUY through the real stores, owed to no frozen record."""
    order = FuturesPaperOrder(
        PaperOrderIdentity("seed-order"),
        FuturesExecutionIntent(
            _ALPHA_PORTFOLIO,
            _ES_DEC,
            BUY,
            FuturesContractCount(net),
            _ALPHA.strategy_identity,
            _instant(10),
        ),
    )
    fill = FuturesPaperFill(
        PaperFillIdentity("seed-fill"),
        order.identity,
        order.intent,
        order.intent.contracts,
        QuoteValue(Decimal("7600")),
        _instant(11),
    )
    assert SQLiteFuturesPaperOrderStore(path).store((order,)) == 1
    assert SQLiteFuturesPaperFillStore(path).store((fill,)) == 1


def test_a_research_buy_over_target_executes_and_counts_a_sale(tmp_path: Path) -> None:
    path = _fresh(tmp_path, "over-target", *_phase_a(), _F1_BAR)
    record = _freeze(path, _D1)
    _seed_long(path, 3)

    run = _paper_run(path, _F1, target=FuturesContractCount(2))
    (entry,) = _metrics(run)

    assert record.result.recommendation.action == RecommendationAction("BUY")
    assert (run.results[0].order.intent.side, run.results[0].order.intent.contracts.value) == (
        SELL,
        1,
    )
    assert (entry.contracts_bought, entry.contracts_sold) == (0, 1)
    assert entry.order_count == 1  # the seed order belongs to no selected decision
    assert entry.current_net_contracts == 2


@pytest.mark.parametrize("opening", ["0", "-37.63"])
def test_zero_and_negative_next_bar_opens_fill_normally(tmp_path: Path, opening: str) -> None:
    f1 = _bar(26, "7611", open_=opening)
    path = _fresh(tmp_path, "signed", *_phase_a(), f1)
    _freeze(path, _D1)

    run = _paper_run(path, _F1)
    report = _report(run)

    assert _fills(path)[0].fill_quote == QuoteValue(Decimal(opening))
    assert run.portfolio.positions == (_position(1, opening),)
    assert report.fill_count == 1
    assert report.metrics[0].contracts_bought == 1


def test_a_count_beyond_sqlite_integer_survives_the_pipeline(tmp_path: Path) -> None:
    huge = 9_223_372_036_854_775_807 * 10 + 3
    path = _fresh(tmp_path, "huge", *_phase_a(), _F1_BAR)
    _freeze(path, _D1)

    run = _paper_run(path, _F1, target=FuturesContractCount(huge))

    assert _orders(path)[0].intent.contracts.value == huge
    assert _query(path, "SELECT contracts, typeof(contracts) FROM futures_paper_orders") == [
        (str(huge), "text")
    ]
    assert type(run.portfolio.positions[0].net_contracts) is int
    assert run.portfolio.positions[0].net_contracts == huge
    assert _metrics(run)[0].contracts_bought == huge


def test_timestamps_are_ordered_semantically_everywhere(tmp_path: Path) -> None:
    x, y = 25, 26
    whole_x, half_x = _instant(x), _instant(x, "21:00:00.5")
    whole_y, quarter_y = _instant(y), _instant(y, "21:00:00.25")
    dec_bars = (*_phase_a(sessions=24), _bar(x, "7610"), _bar(y, "7611", instant=quarter_y))
    mar_bars = (
        *_phase_a(_ES_MAR, sessions=24),
        _bar(x, "7610", contract=_ES_MAR),
        _bar(x, "7611", contract=_ES_MAR, instant=half_x),
        _bar(y, "7612", open_="7660", contract=_ES_MAR),
    )
    path = _fresh(tmp_path, "timestamps", *dec_bars, *mar_bars)

    next_day = (_DATES[x - 1] + timedelta(days=1)).isoformat()
    offset_x = PointInTime(f"{next_day}T02:30:00+05:30")  # == whole_x
    dec = _freeze(path, offset_x)
    mar = _freeze(path, half_x, _ALPHA, _ES_MAR)
    beta_mar = _freeze(path, whole_x, _BETA, _ES_MAR)
    gamma_whole = _freeze(path, whole_x, _GAMMA, _ES_MAR)
    gamma_half = _freeze(path, half_x, _GAMMA, _ES_MAR)
    assert dec.decision_instant == whole_x

    _decide(path, dec, quarter_y)
    _decide(path, mar, quarter_y)
    gamma_run = _paper_run(
        path,
        quarter_y,
        strategy=_GAMMA,
        portfolio=PaperPortfolioIdentity("gamma"),
        contract=_ES_MAR,
    )

    assert half_x.value < whole_x.value and quarter_y.value < whole_y.value  # the text trap
    assert [bar.point_in_time for bar in _bars(path, _ES_MAR)][-3:] == [whole_x, half_x, whole_y]
    assert _records(path, _ES_MAR) == (beta_mar, gamma_whole, mar, gamma_half)
    assert [o.intent.decided_at for o in _orders(path)] == [whole_x, half_x]
    assert [(f.contract, f.filled_at) for f in _fills(path)] == [
        (_ES_MAR, whole_y),
        (_ES_DEC, quarter_y),
    ]
    assert [r.record.decision_instant for r in gamma_run.results] == [whole_x, half_x]
    by_text = [
        row[0]
        for row in _query(
            path,
            "SELECT f.filled_at FROM futures_paper_fills AS f "
            "JOIN futures_paper_orders AS o ON o.order_identity = f.order_identity "
            "WHERE o.portfolio_identity = ? ORDER BY f.filled_at",
            (_ALPHA_PORTFOLIO.identity,),
        )
    ]
    assert by_text == [quarter_y.value, whole_y.value]


def test_pending_orders_settle_across_contracts(tmp_path: Path) -> None:
    path = _fresh(tmp_path, "contracts", *_phase_a(), *_phase_a(_ES_MAR))
    dec = _freeze(path, _D1)
    mar = _freeze(path, _D1, _ALPHA, _ES_MAR)
    _decide(path, dec, _D1)
    _decide(path, mar, _D1)
    assert _fills(path) == ()

    _store_bars(path, _F1_BAR, _bar(26, "7611", open_="7640", contract=_ES_MAR))
    run = _paper_run(path, _F1)

    assert run.portfolio.positions == (_position(1, "7650"), _position(1, "7640", _ES_MAR))
    assert {fill.contract for fill in _fills(path)} == {_ES_DEC, _ES_MAR}
    assert [entry.contract for entry in _metrics(run)] == [_ES_DEC]


def test_fill_parent_integrity_and_order_conflicts(tmp_path: Path) -> None:
    path = _fresh(tmp_path, "integrity", *_phase_a(), _F1_BAR)
    record = _freeze(path, _D1)
    order = _decide(path, record, _D1).order
    fills = SQLiteFuturesPaperFillStore(path)

    def fill_of(target: FuturesPaperOrder, *, fill_id: str, intent=None, quote: str = "7650"):
        intent = intent or target.intent
        return FuturesPaperFill(
            PaperFillIdentity(fill_id),
            target.identity,
            intent,
            intent.contracts,
            QuoteValue(Decimal(quote)),
            _F1,
        )

    orphan = FuturesPaperOrder(PaperOrderIdentity("never-stored"), order.intent)
    with pytest.raises(FuturesPaperFillConflictError):
        fills.store((fill_of(orphan, fill_id="orphan"),))
    other_intent = FuturesExecutionIntent(
        order.intent.portfolio_identity,
        order.intent.contract,
        SELL,
        order.intent.contracts,
        order.intent.strategy_identity,
        order.intent.decided_at,
    )
    with pytest.raises(FuturesPaperFillConflictError):
        fills.store((fill_of(order, fill_id="impostor", intent=other_intent),))
    assert _count(path, "futures_paper_fills") == 0

    genuine = _decide(path, record, _F1).fill
    with pytest.raises(FuturesPaperFillConflictError):
        fills.store((fill_of(order, fill_id="second", quote="1"),))
    with pytest.raises(FuturesPaperFillConflictError):
        fills.store((fill_of(order, fill_id=genuine.identity.identity, quote="1"),))
    assert _fills(path) == (genuine,)

    resized = FuturesPaperOrder(
        order.identity,
        FuturesExecutionIntent(
            order.intent.portfolio_identity,
            order.intent.contract,
            BUY,
            FuturesContractCount(2),
            order.intent.strategy_identity,
            order.intent.decided_at,
        ),
    )
    with pytest.raises(FuturesPaperOrderConflictError):
        SQLiteFuturesPaperOrderStore(path).store((resized,))
    assert _orders(path) == (order,)


def test_a_changed_target_under_the_same_decision_conflicts(tmp_path: Path) -> None:
    path = _fresh(tmp_path, "changed", *_phase_a())
    record = _freeze(path, _D1)
    original = _decide(path, record, _D1).order

    with pytest.raises(FuturesPaperOrderConflictError):
        _decide(path, record, _D1, target=FuturesContractCount(2))

    assert _orders(path) == (original,)


def test_an_order_whose_fill_write_failed_settles_later(tmp_path: Path) -> None:
    path = _fresh(tmp_path, "resume", *_phase_a(), _F1_BAR)
    record = _freeze(path, _D1)

    class UnavailableFillStore(SQLiteFuturesPaperFillStore):
        def store(self, fills):
            raise RuntimeError("fill storage unavailable")

    with pytest.raises(RuntimeError):
        _decide(path, record, _F1, fill_store=UnavailableFillStore(path))
    assert (_count(path, "futures_paper_orders"), _count(path, "futures_paper_fills")) == (1, 0)

    resumed = _decide(path, record, _F1)

    assert resumed.fill.fill_quote == _F1_BAR.open
    assert (_count(path, "futures_paper_orders"), _count(path, "futures_paper_fills")) == (1, 1)
