"""Opt-in synthetic benchmark: futures recommendation replay cost (F-5D-1, G5).

Run explicitly from the repository root; pytest never collects this module::

    .venv/Scripts/python -m tests.benchmarks.benchmark_futures_recommendation_replay
    .venv/Scripts/python -m tests.benchmarks.benchmark_futures_recommendation_replay --quick

Workloads: N terminal recommendations on one product (1, 10, 100, 500), each
with S lifecycle observations (5, 20). P-1 holds throughout: every historical
recommendation is terminal -- its S-th quiet bar is its ``valid_through`` bar,
so F-2 expires it -- before the measured recording adds one more.

Fixture setup is NOT measured. It writes canonical rows directly through the
production encoders (definition rows, ``insert_audit``, ``step_row``, the OHLCV
store) in bulk, because recording N histories one by one through ``record``
is itself the quadratic replay being measured. Every definition has its valid
RECORDED audit; every step references a stored candle with its fingerprint;
reads verify all of it exactly as in production.

Measured, each against a fresh adapter in a fresh connection:

1. ``get_lifecycles`` for the product (N terminal histories).
2. ``SQLiteFuturesTradeRecommendationStore.record`` of one new recommendation,
   whose exclusivity check replays every committed history inside its
   ``BEGIN IMMEDIATE`` transaction.
3. One ``MonitorFuturesTradeRecommendationsUseCase.execute`` that reads all
   N + 1 lifecycles and records one observation of the new recommendation.

Statement counts and transaction durations come from SQLite's own trace
callback, attached by wrapping ``sqlite3.connect`` in this process only;
production code is unchanged. A transaction's duration is the interval between
the trace of its ``BEGIN IMMEDIATE`` and of its ``COMMIT``/``ROLLBACK``: the time
this process held SQLite's write lock. Statement timestamps are taken when
each statement starts, so the figure excludes nothing after COMMIT is issued.

Synthetic data only: an every-day calendar, quiet candles, non-production
policy and profile. No provider, network or clock-dependent decision.
"""

from __future__ import annotations

import argparse
import sqlite3
import tempfile
import time
from contextlib import closing
from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

from northstar_application.application_services import (
    FuturesCandleApproval,
    FuturesCandleApprovalSet,
    FuturesReviewedCandleVerification,
    FuturesTradeRecommendationGenerationProfile,
    FuturesTradeRecommendationIdentityService,
    MonitorFuturesTradeRecommendationsUseCase,
    OperatorApprovedFuturesDailyBarFinalityPolicy,
    futures_daily_candle_fingerprint,
)
from northstar_application.ports import (
    FuturesTradeRecommendationObservationStep,
    FuturesTradeRecommendationQuery,
    FuturesTradingSession,
    FuturesTradingSessionResolver,
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
    EntryZone,
    FuturesRecommendationGenerationPolicy,
    FuturesResearchDecisionReference,
    FuturesTradeRecommendation,
    FuturesTradeRecommendationLifecycle,
    PriceTarget,
    RecommendationPolicyVersion,
    RecommendationValidity,
    StopLoss,
    TradeDirection,
)
from northstar_core.strategy import ExplanationReason, StrategyIdentity

import northstar_infrastructure.persistence.sqlite_futures_trade_recommendation_steps as steps
import northstar_infrastructure.persistence.sqlite_futures_trade_recommendations as store_module
from northstar_infrastructure.market_data import (
    SQLiteFuturesHistoricalMarketDataRepository,
    SQLiteFuturesHistoricalMarketDataStore,
)
from northstar_infrastructure.persistence import (
    SQLiteFuturesTradeRecommendationLifecycleStore,
    SQLiteFuturesTradeRecommendationRepository,
    SQLiteFuturesTradeRecommendationStore,
)
from northstar_infrastructure.persistence import (
    sqlite_futures_recommendation_generation_audits as audits,
)
from tests.futures_recommendation_audit_fixtures import recording_audit

RECOMMENDATIONS = (1, 10, 100, 500)
OBSERVATIONS = (5, 20)
BUSY_TIMEOUT_SECONDS = store_module._BUSY_TIMEOUT_SECONDS

_NIFTY = FuturesProductReference(Symbol("NIFTY"), ExchangeCode("NSE"))
_CONTRACT = FuturesContract(_NIFTY, ExpirationDate("2099-12-31"))
_STRATEGY = StrategyIdentity("synthetic-directional-v1")
_POLICY = FuturesRecommendationGenerationPolicy(
    version=RecommendationPolicyVersion("f4-synthetic-benchmark-v1"),
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
    "f5a-synthetic-benchmark-profile-v1", 5, _POLICY.version
)
_START = date(2000, 1, 3)
_CUTOFF = PointInTime("2099-12-31T10:00:00Z")


# -- an every-day synthetic calendar -----------------------------------------------------


def _session(day: date) -> FuturesTradingSession:
    text = day.isoformat()
    return FuturesTradingSession(
        day, PointInTime(f"{text}T03:45:00Z"), PointInTime(f"{text}T10:00:00Z")
    )


class EveryDayCalendar(FuturesTradingSessionResolver):
    """Every date is a session closing at 10:00Z; synthetic, never a real venue."""

    def resolve(self, product, trading_date):
        return _session(trading_date)

    def sessions_in_range(self, product, start_date, end_date):
        days = (end_date - start_date).days + 1
        return tuple(_session(start_date + timedelta(days=n)) for n in range(max(days, 0)))


# -- fixtures --------------------------------------------------------------------------


def _q(value: str) -> QuoteValue:
    return QuoteValue(Decimal(value))


def _recommendation(decision_day: date, observations: int) -> FuturesTradeRecommendation:
    """LONG, decided at ``decision_day``'s close, valid for the next ``observations`` days."""
    source = FuturesResearchDecisionReference(
        _CONTRACT, Timeframe("1d"), _session(decision_day).closes_at, _STRATEGY
    )
    return FuturesTradeRecommendation(
        identity=FuturesTradeRecommendationIdentityService().identity(source, _PROFILE),
        contract=_CONTRACT,
        direction=TradeDirection.LONG,
        entry=EntryZone(_q("24378"), _q("24388")),
        stop=StopLoss(_q("24347.75")),
        target=PriceTarget(_q("24448.45")),
        validity=RecommendationValidity(
            _session(decision_day + timedelta(days=1)).closes_at,
            _session(decision_day + timedelta(days=observations)).closes_at,
            _CUTOFF,
        ),
        source=source,
        policy_version=_POLICY.version,
        issued_at=PointInTime(f"{decision_day.isoformat()}T12:00:00Z"),
        thesis=(ExplanationReason("Synthetic benchmark thesis"),),
    )


def _quiet(day: date) -> FuturesOHLCVBar:
    return FuturesOHLCVBar(
        contract=_CONTRACT,
        point_in_time=_session(day).closes_at,
        timeframe=Timeframe("1d"),
        open=_q("24410"),
        high=_q("24420"),
        low=_q("24400"),
        close=_q("24410"),
        volume=Quantity(Decimal("1250")),
    )


def _build(path: Path, count: int, observations: int) -> date:
    """Write ``count`` terminal histories canonically; return the next free decision day."""
    with closing(sqlite3.connect(path)) as connection:
        store_module.initialize_futures_trade_recommendation_schema(connection)
    configuration = store_module._configuration_row(_POLICY, _PROFILE)
    digest = store_module._configuration_digest(configuration)
    bars: list[FuturesOHLCVBar] = []
    rows: list[tuple] = []
    definitions: list[tuple] = []
    recorded = []
    day = _START
    for _ in range(count):
        recommendation = _recommendation(day, observations)
        lifecycle = FuturesTradeRecommendationLifecycle(recommendation)
        for offset in range(1, observations + 1):
            bar = _quiet(day + timedelta(days=offset))
            step = FuturesTradeRecommendationObservationStep(bar, bar_day(bar))
            after = lifecycle.advance(bar)
            rows.append(steps.step_row(recommendation, offset, lifecycle, step, after))
            bars.append(bar)
            lifecycle = after
        assert lifecycle.is_terminal, "a historical recommendation must be terminal (P-1)"
        definitions.append(store_module._definition_row(recommendation, _PROFILE.version, digest))
        recorded.append(recommendation)
        day += timedelta(days=observations + 1)
    SQLiteFuturesHistoricalMarketDataStore(path).store(tuple(bars))
    with closing(sqlite3.connect(path)) as connection:
        connection.execute(store_module._INSERT_CONFIGURATION, configuration)
        for row in definitions:
            connection.execute(store_module._INSERT_DEFINITION, row)
        for recommendation in recorded:
            audits.insert_audit(connection, recording_audit(recommendation, profile=_PROFILE))
        connection.executemany(
            "INSERT INTO futures_trade_recommendation_steps VALUES "
            f"({', '.join('?' * len(steps.STEP_COLUMNS))})",
            rows,
        )
        connection.commit()
    return day


def bar_day(bar: FuturesOHLCVBar) -> date:
    return date.fromisoformat(bar.point_in_time.value[:10])


# -- tracing ---------------------------------------------------------------------------


@dataclass
class Trace:
    """SQLite statements and write transactions seen by this process."""

    statements: int = 0
    transactions: list[float] = field(default_factory=list)
    _open: dict[int, float] = field(default_factory=dict)

    def callback(self, key: int):
        def traced(statement: str) -> None:
            now = time.perf_counter()
            self.statements += 1
            head = statement.lstrip().upper()
            if head.startswith("BEGIN IMMEDIATE"):
                self._open[key] = now
            elif head.startswith(("COMMIT", "ROLLBACK")) and key in self._open:
                self.transactions.append(now - self._open.pop(key))

        return traced


_REAL_CONNECT = sqlite3.connect
_ACTIVE: list[Trace] = []


def _traced_connect(*args, **kwargs):
    connection = _REAL_CONNECT(*args, **kwargs)
    if _ACTIVE:
        connection.set_trace_callback(_ACTIVE[-1].callback(id(connection)))
    return connection


def _measure(operation) -> tuple[float, Trace, object]:
    trace = Trace()
    _ACTIVE.append(trace)
    try:
        started = time.perf_counter()
        result = operation()
        elapsed = time.perf_counter() - started
    finally:
        _ACTIVE.pop()
    return elapsed, trace, result


# -- one workload ----------------------------------------------------------------------


@dataclass(frozen=True)
class Result:
    count: int
    observations: int
    file_bytes: int
    read_seconds: float
    read_statements: int
    record_seconds: float
    record_statements: int
    record_transaction_seconds: float | None
    monitor_seconds: float
    monitor_statements: int
    monitor_transaction_seconds: float | None


def run(count: int, observations: int, directory: Path) -> Result:
    path = directory / f"replay-{count}-{observations}.sqlite3"
    next_day = _build(path, count, observations)
    file_bytes = path.stat().st_size

    read_seconds, read_trace, lifecycles = _measure(
        lambda: SQLiteFuturesTradeRecommendationRepository(path).get_lifecycles(
            FuturesTradeRecommendationQuery(_NIFTY)
        )
    )
    assert len(lifecycles) == count and all(lc.is_terminal for lc in lifecycles)

    new = _recommendation(next_day, observations)
    record_seconds, record_trace, accepted = _measure(
        lambda: SQLiteFuturesTradeRecommendationStore(path).record(
            (new,),
            policy=_POLICY,
            profile=_PROFILE,
            audit=recording_audit(new, profile=_PROFILE),
        )
    )
    assert accepted == 1

    first = _quiet(next_day + timedelta(days=1))
    SQLiteFuturesHistoricalMarketDataStore(path).store((first,))
    approvals = FuturesReviewedCandleVerification(
        FuturesCandleApprovalSet(
            _NIFTY,
            (
                FuturesCandleApproval(
                    _CONTRACT,
                    bar_day(first),
                    futures_daily_candle_fingerprint(_CONTRACT, bar_day(first), first),
                ),
            ),
        )
    )
    monitor = MonitorFuturesTradeRecommendationsUseCase(
        SQLiteFuturesTradeRecommendationRepository(path),
        SQLiteFuturesTradeRecommendationLifecycleStore(path),
        SQLiteFuturesHistoricalMarketDataRepository(path),
        EveryDayCalendar(),
        OperatorApprovedFuturesDailyBarFinalityPolicy(bar_day(first)),
    )
    as_of = PointInTime(f"{bar_day(first).isoformat()}T12:00:00Z")
    monitor_seconds, monitor_trace, monitored = _measure(
        lambda: monitor.execute(product=_NIFTY, as_of=as_of, reviewed_candles=approvals)
    )
    (outcome,) = monitored.outcomes
    assert outcome.steps_recorded == 1, outcome

    return Result(
        count=count,
        observations=observations,
        file_bytes=file_bytes,
        read_seconds=read_seconds,
        read_statements=read_trace.statements,
        record_seconds=record_seconds,
        record_statements=record_trace.statements,
        record_transaction_seconds=max(record_trace.transactions, default=None),
        monitor_seconds=monitor_seconds,
        monitor_statements=monitor_trace.statements,
        monitor_transaction_seconds=max(monitor_trace.transactions, default=None),
    )


def _seconds(value: float | None) -> str:
    return "unavailable" if value is None else f"{value:.3f}"


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--quick", action="store_true", help="run only N=1, S=5")
    arguments = parser.parse_args(argv)
    workloads = (
        ((1, 5),)
        if arguments.quick
        else tuple((n, s) for s in OBSERVATIONS for n in RECOMMENDATIONS)
    )
    sqlite3.connect = _traced_connect  # this process only; production code is unchanged
    try:
        with tempfile.TemporaryDirectory() as directory:
            print(
                "N    S   file_KiB  read_s  read_stmts  record_s  record_stmts  record_txn_s  "
                "monitor_s  monitor_stmts  monitor_txn_s"
            )
            for count, observations in workloads:
                r = run(count, observations, Path(directory))
                print(
                    f"{r.count:<4} {r.observations:<3} {r.file_bytes / 1024:>8.0f}  "
                    f"{r.read_seconds:>6.3f}  {r.read_statements:>10}  "
                    f"{r.record_seconds:>8.3f}  {r.record_statements:>12}  "
                    f"{_seconds(r.record_transaction_seconds):>12}  "
                    f"{r.monitor_seconds:>9.3f}  {r.monitor_statements:>13}  "
                    f"{_seconds(r.monitor_transaction_seconds):>13}",
                    flush=True,
                )
            print(f"SQLite busy timeout: {BUSY_TIMEOUT_SECONDS:.1f} s")
    finally:
        sqlite3.connect = _REAL_CONNECT


if __name__ == "__main__":
    main()
