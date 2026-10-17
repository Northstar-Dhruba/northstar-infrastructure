"""SQLite acceptance: generation through durable lifecycle monitoring (F-5C).

SYNTHETIC data only: the bars, research, candle approvals, finality dates,
policy and profile are fixtures. The calendar is Northstar's NSE futures
calendar reference; no provider is contacted and nothing is published.

One temporary SQLite database holds everything the adapters share: stored
futures bars, frozen research, recommendation definitions, generation audits
and lifecycle steps. The real Application use cases run against the real
SQLite adapters:

1. GenerateFuturesTradeRecommendationsUseCase records a LONG recommendation
   and its RECORDED audit in one transaction, from VERIFIED, reviewed history;
2. MonitorFuturesTradeRecommendationsUseCase advances it through final,
   operator-approved stored candles, idempotently, to a terminal outcome;
3. a fresh repository reconstructs the exact lifecycle, and the product is
   released for another recommendation only after the terminal step.
"""

from __future__ import annotations

import sqlite3
from contextlib import closing
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from northstar_application.application_services import (
    FuturesAnalysisResult,
    FuturesCandleApproval,
    FuturesCandleApprovalSet,
    FuturesForwardResearchRecord,
    FuturesRecommendationGenerationAudit,
    FuturesRecommendationMonitoringStopReason,
    FuturesReviewedCandleVerification,
    FuturesTradeRecommendationGenerationProfile,
    FuturesTradeRecommendationIdentityService,
    GenerateFuturesTradeRecommendationsUseCase,
    MonitorFuturesTradeRecommendationsUseCase,
    OperatorApprovedFuturesDailyBarFinalityPolicy,
    futures_daily_candle_fingerprint,
)
from northstar_application.ports import (
    FuturesContractCutoff,
    FuturesContractCutoffSource,
    FuturesTradeRecommendationExclusivityError,
    FuturesTradeRecommendationQuery,
    FuturesTradingSession,
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
    RecommendationStatus,
    RecommendationValidity,
    StopLoss,
    TradeDirection,
)
from northstar_core.strategy import (
    ExplanationReason,
    FuturesAssetAnalysis,
    FuturesMarketObservationContext,
    FuturesRecommendation,
    RecommendationAction,
    StrategyIdentity,
)

import northstar_infrastructure.persistence.sqlite_futures_trade_recommendations as module
from northstar_infrastructure.market_data import (
    NSEFuturesTradingSessionResolver,
    SQLiteFuturesHistoricalMarketDataRepository,
    SQLiteFuturesHistoricalMarketDataStore,
)
from northstar_infrastructure.persistence import (
    FuturesTradeRecommendationStorageError,
    SQLiteFuturesForwardResearchRecordRepository,
    SQLiteFuturesForwardResearchRecordStore,
    SQLiteFuturesRecommendationGenerationAuditRepository,
    SQLiteFuturesRecommendationGenerationAuditStore,
    SQLiteFuturesTradeRecommendationLifecycleStore,
    SQLiteFuturesTradeRecommendationRepository,
    SQLiteFuturesTradeRecommendationStore,
)
from tests.futures_recommendation_audit_fixtures import recording_audit

Stop = FuturesRecommendationMonitoringStopReason

_NIFTY = FuturesProductReference(Symbol("NIFTY"), ExchangeCode("NSE"))
_OCT = FuturesContract(_NIFTY, ExpirationDate("2026-10-27"))
_NOV = FuturesContract(_NIFTY, ExpirationDate("2026-11-24"))
_DAILY = Timeframe("1d")
_STRATEGY = StrategyIdentity("synthetic-directional-v1")
_DECISION_DATE = date(2026, 10, 7)
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
# Twenty-five closes ending at the decision session: F-4 then derives the
# LONG setup entry 24378-24388, stop 24347.75, target 24448.45.
_CLOSES = (
    Decimal("23900"),
    Decimal("23950"),
    Decimal("23925"),
    Decimal("23975"),
    Decimal("23990"),
    *(Decimal(24000 + 20 * step) for step in range(19)),
    Decimal("24383"),
)
_QUIET = ("24400", "24420")
_ENTRY = ("24380", "24400")
_TARGET = ("24400", "24450")

_CALENDAR = NSEFuturesTradingSessionResolver()


def _session(day: date) -> FuturesTradingSession:
    session = _CALENDAR.resolve(_NIFTY, day)
    assert session is not None, day
    return session


def _q(value: str | Decimal) -> QuoteValue:
    return QuoteValue(Decimal(value))


def _bar(
    session: FuturesTradingSession, low: str | Decimal, high: str | Decimal
) -> FuturesOHLCVBar:
    middle = (Decimal(low) + Decimal(high)) / 2
    return FuturesOHLCVBar(
        contract=_OCT,
        point_in_time=session.closes_at,
        timeframe=_DAILY,
        open=_q(middle),
        high=_q(high),
        low=_q(low),
        close=_q(middle),
        volume=Quantity(Decimal("1250")),
    )


def _history() -> tuple[tuple[FuturesTradingSession, FuturesOHLCVBar], ...]:
    sessions = _CALENDAR.sessions_in_range(
        _NIFTY, _DECISION_DATE - timedelta(days=60), _DECISION_DATE
    )[-len(_CLOSES) :]
    return tuple(
        (session, _bar(session, close - 5, close + 5))
        for session, close in zip(sessions, _CLOSES, strict=True)
    )


def _research(bars: tuple[FuturesOHLCVBar, ...]) -> FuturesForwardResearchRecord:
    window, last = bars[-20:], bars[-1]
    context = FuturesMarketObservationContext(
        contract=_OCT,
        timeframe=_DAILY,
        observed_at=last.point_in_time,
        latest_quote=last.close,
        previous_close=window[-2].close,
        latest_volume=last.volume,
        session_high=last.high,
        session_low=last.low,
        recent_closes=tuple(bar.close for bar in window),
        recent_volumes=tuple(bar.volume for bar in window),
    )
    recommendation = FuturesRecommendation(
        RecommendationAction("BUY"),
        FuturesAssetAnalysis(_OCT, last.point_in_time, ("strong bullish",)),
        _STRATEGY,
        last.point_in_time,
    )
    return FuturesForwardResearchRecord(FuturesAnalysisResult(recommendation, context))


def _approvals(
    pairs: list[tuple[FuturesTradingSession, FuturesOHLCVBar]],
) -> FuturesReviewedCandleVerification:
    return FuturesReviewedCandleVerification(
        FuturesCandleApprovalSet(
            _NIFTY,
            tuple(
                FuturesCandleApproval(
                    _OCT,
                    session.trading_date,
                    futures_daily_candle_fingerprint(_OCT, session.trading_date, bar),
                )
                for session, bar in pairs
            ),
        )
    )


class Cutoffs(FuturesContractCutoffSource):
    """The verified last session of the October contract: its last session by expiry."""

    def resolve(self, contract: FuturesContract) -> FuturesContractCutoff | None:
        expiry = date.fromisoformat(contract.expiration_date.value)
        last = _CALENDAR.sessions_in_range(_NIFTY, expiry - timedelta(days=10), expiry)[-1]
        return FuturesContractCutoff(contract, last, "synthetic verified reference")


class Database:
    """One SQLite file shared by every adapter, and the approved candles so far."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.approved: list[tuple[FuturesTradingSession, FuturesOHLCVBar]] = []

    def store_bars(self, *pairs: tuple[FuturesTradingSession, FuturesOHLCVBar]) -> None:
        SQLiteFuturesHistoricalMarketDataStore(self.path).store(tuple(bar for _, bar in pairs))

    def approve(self, *pairs: tuple[FuturesTradingSession, FuturesOHLCVBar]) -> None:
        self.approved.extend(pairs)

    def repository(self) -> SQLiteFuturesTradeRecommendationRepository:
        return SQLiteFuturesTradeRecommendationRepository(self.path)

    def generate(self):
        return GenerateFuturesTradeRecommendationsUseCase(
            SQLiteFuturesForwardResearchRecordRepository(self.path),
            SQLiteFuturesHistoricalMarketDataRepository(self.path),
            _CALENDAR,
            OperatorApprovedFuturesDailyBarFinalityPolicy(_DECISION_DATE),
            Cutoffs(),
            self.repository(),
            SQLiteFuturesTradeRecommendationStore(self.path),
            SQLiteFuturesRecommendationGenerationAuditStore(self.path),
        ).execute(
            contract=_OCT,
            strategy=_STRATEGY,
            policy=_POLICY,
            profile=_PROFILE,
            issued_at=PointInTime("2026-10-07T12:00:00Z"),
            reviewed_candles=_approvals(self.approved),
        )

    def monitor(self, as_of: str, final_through: date, approved=None, declarations=()):
        return MonitorFuturesTradeRecommendationsUseCase(
            self.repository(),
            SQLiteFuturesTradeRecommendationLifecycleStore(self.path),
            SQLiteFuturesHistoricalMarketDataRepository(self.path),
            _CALENDAR,
            OperatorApprovedFuturesDailyBarFinalityPolicy(final_through),
        ).execute(
            product=_NIFTY,
            as_of=PointInTime(as_of),
            reviewed_candles=_approvals(self.approved if approved is None else approved),
            coverage_declarations=declarations,
        )

    def rows(self, table: str) -> list[tuple]:
        with closing(sqlite3.connect(self.path)) as connection:
            return connection.execute(f"SELECT * FROM {table} ORDER BY 1, 2").fetchall()  # noqa: S608

    def execute(self, sql: str) -> None:
        with closing(sqlite3.connect(self.path)) as connection:
            connection.execute(sql)
            connection.commit()


@pytest.fixture
def db(tmp_path: Path) -> Database:
    return Database(tmp_path / "northstar.sqlite3")


def _generated(
    db: Database,
) -> tuple[FuturesTradeRecommendation, tuple[FuturesTradingSession, ...]]:
    """Generate and durably record the LONG recommendation; return it and its window."""
    history = _history()
    db.store_bars(*history)
    db.approve(*history)
    SQLiteFuturesForwardResearchRecordStore(db.path).store(
        (_research(tuple(bar for _, bar in history)),)
    )

    result = db.generate()

    (recommendation,) = result.recorded
    assert recommendation.direction is TradeDirection.LONG
    assert (recommendation.entry, recommendation.stop, recommendation.target) == (
        EntryZone(_q("24378"), _q("24388")),
        StopLoss(_q("24347.75")),
        PriceTarget(_q("24448.45")),
    )
    audit = SQLiteFuturesRecommendationGenerationAuditRepository(db.path).get_audit(
        FuturesRecommendationGenerationAudit.of(result).identity
    )
    assert audit == FuturesRecommendationGenerationAudit.of(result)
    assert audit.recorded == (recommendation.identity,)

    validity = recommendation.validity
    window = tuple(
        session
        for session in _CALENDAR.sessions_in_range(
            _NIFTY, _DECISION_DATE, _DECISION_DATE + timedelta(days=30)
        )
        if session.closes_at.compare(validity.eligible_from) >= 0
        and session.closes_at.compare(validity.valid_through) <= 0
    )
    assert len(window) == _PROFILE.validity_sessions
    return recommendation, window


def _after(session: FuturesTradingSession) -> str:
    """Return an instant two hours after the session's resolved close."""
    close = datetime.fromisoformat(session.closes_at.value.replace("Z", "+00:00"))
    return (close + timedelta(hours=2)).isoformat().replace("+00:00", "Z")


def test_generation_to_terminal_monitoring_round_trips_through_sqlite(db: Database) -> None:
    recommendation, window = _generated(db)
    definition = db.rows("futures_trade_recommendations")
    audits = db.rows("futures_recommendation_generation_audits")
    quiet = [(window[0], _bar(window[0], *_QUIET)), (window[1], _bar(window[1], *_QUIET))]
    db.store_bars(*quiet)
    db.approve(*quiet)

    first = db.monitor(_after(window[1]), window[1].trading_date)

    (outcome,) = first.outcomes
    assert outcome.stop_reason is Stop.UP_TO_DATE and outcome.steps_recorded == 2
    assert first.open_anomaly is False
    expected = FuturesTradeRecommendationLifecycle(recommendation)
    for _, bar in quiet:
        expected = expected.advance(bar)
    (lifecycle,) = SQLiteFuturesTradeRecommendationRepository(str(db.path)).get_lifecycles(
        FuturesTradeRecommendationQuery(_NIFTY)
    )
    assert lifecycle == expected
    assert lifecycle.events == () and lifecycle.last_observation == quiet[-1][1]
    steps = db.rows("futures_trade_recommendation_steps")
    assert [(row[1], row[8], row[11]) for row in steps] == [
        (1, window[0].trading_date.isoformat(), "WAITING_FOR_ENTRY"),
        (2, window[1].trading_date.isoformat(), "WAITING_FOR_ENTRY"),
    ]

    again = db.monitor(_after(window[1]), window[1].trading_date)
    assert again.outcomes[0].steps_recorded == 0
    assert db.rows("futures_trade_recommendation_steps") == steps

    november = _november()
    with pytest.raises(FuturesTradeRecommendationExclusivityError):
        _record_november(db, november)

    moves = [(window[2], _bar(window[2], *_ENTRY)), (window[3], _bar(window[3], *_TARGET))]
    db.store_bars(*moves)
    db.approve(*moves)
    final = db.monitor(_after(window[3]), window[3].trading_date)

    (closed,) = final.outcomes
    assert closed.stop_reason is Stop.TERMINAL and closed.steps_recorded == 2
    assert closed.status_after is RecommendationStatus.CLOSED_TARGET
    for _, bar in moves:
        expected = expected.advance(bar)
    (lifecycle,) = db.repository().get_lifecycles(FuturesTradeRecommendationQuery(_NIFTY))
    assert lifecycle == expected and lifecycle.status is RecommendationStatus.CLOSED_TARGET
    assert [row[1] for row in db.rows("futures_trade_recommendation_steps")] == [1, 2, 3, 4]

    assert _record_november(db, november) == 1
    assert definition[0] in db.rows("futures_trade_recommendations")
    assert set(audits) <= set(db.rows("futures_recommendation_generation_audits"))
    assert db.repository().get_recommendation(recommendation.identity) == recommendation


def _november() -> FuturesTradeRecommendation:
    source = FuturesResearchDecisionReference(
        _NOV, _DAILY, _session(date(2026, 10, 16)).closes_at, _STRATEGY
    )
    return FuturesTradeRecommendation(
        identity=FuturesTradeRecommendationIdentityService().identity(source, _PROFILE),
        contract=_NOV,
        direction=TradeDirection.LONG,
        entry=EntryZone(_q("24378"), _q("24388")),
        stop=StopLoss(_q("24347.75")),
        target=PriceTarget(_q("24448.45")),
        validity=RecommendationValidity(
            _session(date(2026, 10, 19)).closes_at,
            _session(date(2026, 10, 23)).closes_at,
            _session(date(2026, 11, 23)).closes_at,
        ),
        source=source,
        policy_version=_POLICY.version,
        issued_at=PointInTime("2026-10-16T12:00:00Z"),
        thesis=(ExplanationReason("Synthetic November thesis"),),
    )


def _record_november(db: Database, recommendation: FuturesTradeRecommendation) -> int:
    return SQLiteFuturesTradeRecommendationStore(db.path).record(
        (recommendation,),
        policy=_POLICY,
        profile=_PROFILE,
        audit=recording_audit(recommendation, profile=_PROFILE),
    )


@pytest.mark.parametrize("gate", ["unapproved", "not-final"])
def test_an_unapproved_or_non_final_candle_cannot_advance_monitoring(
    db: Database, gate: str
) -> None:
    _, window = _generated(db)
    pair = (window[0], _bar(window[0], *_ENTRY))
    db.store_bars(pair)
    approved = list(db.approved) if gate == "unapproved" else [*db.approved, pair]
    final_through = window[0].trading_date if gate == "unapproved" else _DECISION_DATE

    result = db.monitor(_after(window[0]), final_through, approved=approved)

    expected = Stop.UNAPPROVED_CANDLE if gate == "unapproved" else Stop.SESSION_NOT_FINAL
    assert result.outcomes[0].stop_reason is expected
    assert db.rows("futures_trade_recommendation_steps") == []


def test_an_altered_referenced_candle_is_refused_without_a_replacement(db: Database) -> None:
    _, window = _generated(db)
    pair = (window[0], _bar(window[0], *_QUIET))
    db.store_bars(pair)
    db.approve(pair)
    db.monitor(_after(window[0]), window[0].trading_date)
    steps = db.rows("futures_trade_recommendation_steps")
    db.execute(
        "UPDATE futures_ohlcv SET high_value = '24421' "
        f"WHERE point_in_time = '{window[0].closes_at.value}'"
    )

    with pytest.raises(FuturesTradeRecommendationStorageError):
        db.monitor(_after(window[1]), window[1].trading_date)
    assert db.rows("futures_trade_recommendation_steps") == steps


def test_a_failed_lifecycle_write_persists_no_partial_steps(db: Database) -> None:
    _, window = _generated(db)
    pairs = [(session, _bar(session, *_QUIET)) for session in window[:3]]
    db.store_bars(*pairs)
    db.approve(*pairs)
    db.execute(
        "CREATE TRIGGER fail_third_step BEFORE INSERT ON futures_trade_recommendation_steps "
        "WHEN NEW.sequence = 3 BEGIN SELECT RAISE(ABORT, 'synthetic failure'); END"
    )

    with pytest.raises(FuturesTradeRecommendationStorageError):
        db.monitor(_after(window[2]), window[2].trading_date)
    assert db.rows("futures_trade_recommendation_steps") == []

    db.execute("DROP TRIGGER fail_third_step")
    assert db.monitor(_after(window[2]), window[2].trading_date).outcomes[0].steps_recorded == 3


def test_monitoring_uses_one_database_shared_with_market_data(db: Database) -> None:
    _generated(db)

    with closing(sqlite3.connect(db.path)) as connection:
        tables = {
            row[0]
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
    assert {
        "futures_ohlcv",
        "futures_trade_recommendations",
        "futures_trade_recommendation_steps",
        "futures_recommendation_generation_audits",
    } <= tables
    assert module._BUSY_TIMEOUT_SECONDS == 5.0
