"""S-1A acceptance: synthetic NSE cash-equity research through the generic pipeline.

These tests prove that the existing generic historical research services run
unchanged for NSE-listed equities denominated in INR:

    synthetic bars -> IngestHistoricalMarketDataUseCase
    -> SQLiteHistoricalMarketDataStore (pytest temporary storage)
    -> ReplayHistoricalMarketDataUseCase over SQLiteHistoricalMarketDataRepository
    -> EvaluateHistoricalResearchUseCase with HistoricalSnapshotEvaluatorService
       (AssetAnalysisGenerator + Strategy, the existing directional rule)
    -> RunHistoricalResearchUseCase with MeasureRecommendationOutcomeUseCase
    -> CalculateHistoricalResearchMetricsUseCase

Only the provider is replaced: an in-memory HistoricalMarketDataSource serves
the invented bars. Every other step is the real implementation.

Fixture data is invented
------------------------
Every price and volume in this module is invented for this test. None is
copied from, derived from or calibrated against real market data. RELIANCE and
TCS are used only as NSE identities that the current Symbol contract accepts.

Fixture sessions are not an NSE calendar
----------------------------------------
Session ``n`` completes at 10:00:00Z on the ``n``-th consecutive calendar date
from 2031-01-01. 10:00Z equals 15:30 IST and is used here only as a synthetic
normal-session close convention. The dates are fixture ordinals: they include
weekends, model no NSE cash-market holiday or special session, and establish
no exchange fact. Northstar has no NSE cash-session resolver yet.

What the outcomes mean
----------------------
BUY, SELL and HOLD are research decisions of the existing Strategy, not trade
setups. Forward returns are factual close-to-close market movement, not
realised profit and loss: no order, fill, position, cost or slippage exists
anywhere in this pipeline.
"""

from __future__ import annotations

import dataclasses
import socket
import sqlite3
from datetime import date, timedelta
from decimal import Decimal, localcontext
from pathlib import Path

import pytest
from northstar_application.application_services import (
    CalculateHistoricalResearchMetricsUseCase,
    EvaluateHistoricalResearchUseCase,
    HistoricalDataContractViolationError,
    HistoricalResearchEvaluation,
    HistoricalResearchHorizonMetrics,
    HistoricalResearchRun,
    HistoricalSnapshotEvaluatorService,
    IngestHistoricalMarketDataUseCase,
    MeasureRecommendationOutcomeUseCase,
    RecommendationOutcomeUnavailableReason,
    ReplayHistoricalMarketDataUseCase,
    RunHistoricalResearchUseCase,
)
from northstar_application.ports import HistoricalMarketDataQuery, HistoricalMarketDataSource
from northstar_core.domain.value_objects import ListingReference
from northstar_core.foundation.exceptions.validation import InvalidPriceError
from northstar_core.foundation.value_objects import (
    Currency,
    ExchangeCode,
    PointInTime,
    Price,
    Quantity,
    Symbol,
    Timeframe,
)
from northstar_core.market_data import (
    HistoricalOHLCVBar,
    HistoricalReplaySnapshot,
    InvalidHistoricalOHLCVBarError,
    InvalidHistoricalReplaySnapshotError,
)
from northstar_core.strategy import (
    AssetAnalysisGenerator,
    InvalidMarketObservationContextError,
    ResearchHorizon,
    Strategy,
    StrategyIdentity,
)

from northstar_infrastructure.market_data import (
    SQLiteHistoricalMarketDataRepository,
    SQLiteHistoricalMarketDataStore,
)

_NSE = ExchangeCode("NSE")
_INR = Currency("INR")
_DAILY = Timeframe("1d")
_STRATEGY = StrategyIdentity("s1a-synthetic-nse-research")
_HORIZONS = (ResearchHorizon(1), ResearchHorizon(5))

# HistoricalSnapshotEvaluatorService refuses snapshots with fewer daily bars.
_MINIMUM_HISTORY = 20

# Synthetic session ordinals; see "Fixture sessions are not an NSE calendar".
_FIRST_FIXTURE_DATE = date(2031, 1, 1)
_SYNTHETIC_CLOSE_UTC = "10:00:00Z"

_WICK = Decimal("1.25")
_LAST_SESSION = 32
_FUTURE_LAST_SESSION = 40

# Invented series. RELIANCE rises by 4.75 every session on steady volume, so
# every decision is bullish except session 31, whose volume falls below its
# twenty-session average. TCS falls by 6.50 every session on steady volume.
_RELIANCE_FIRST_CLOSE = Decimal("640.00")
_RELIANCE_STEP = Decimal("4.75")
_RELIANCE_VOLUMES = (Decimal("250000"),) * 30 + (Decimal("120000"), Decimal("400000"))
_TCS_FIRST_CLOSE = Decimal("905.00")
_TCS_STEP = Decimal("-6.50")
_TCS_VOLUMES = (Decimal("180000"),) * _LAST_SESSION

# Invented sessions 33-40 for RELIANCE: a sharp fall on heavy volume, used only
# to prove that later data never alters an earlier decision.
_RELIANCE_FUTURE_STEP = Decimal("-30.00")
_RELIANCE_FUTURE_VOLUMES = (Decimal("900000"),) * (_FUTURE_LAST_SESSION - _LAST_SESSION)

# Decisions exist for sessions 20..32, the first having twenty completed bars.
_RELIANCE_ACTIONS = ("BUY",) * 11 + ("HOLD", "BUY")
_TCS_ACTIONS = ("SELL",) * 13
_ACTION_SIGNALS = {
    "BUY": ("strong bullish",),
    "SELL": ("strong bearish",),
    "HOLD": ("neutral trend",),
}


class _FixtureSource(HistoricalMarketDataSource):
    """Provider stand-in that returns its invented bars verbatim.

    Returning the bars unfiltered lets a test hand ingestion exactly the
    contract violation it wants ingestion to refuse.
    """

    def __init__(self, bars: tuple[HistoricalOHLCVBar, ...]) -> None:
        self._bars = bars

    def fetch_history(self, query: HistoricalMarketDataQuery) -> tuple[HistoricalOHLCVBar, ...]:
        return self._bars


@pytest.fixture(autouse=True)
def _refuse_network(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fail any attempt to open a network connection from this module."""

    def refuse(*args: object, **kwargs: object) -> None:
        raise AssertionError("S-1A acceptance must not open network connections.")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)


@pytest.fixture
def database_path(tmp_path: Path) -> Path:
    return tmp_path / "s1a_nse_equity_research.sqlite3"


@pytest.fixture
def ingested_database(database_path: Path) -> Path:
    _ingest(database_path, _reliance_bars())
    _ingest(database_path, _tcs_bars())
    return database_path


def _session(number: int) -> PointInTime:
    fixture_date = _FIRST_FIXTURE_DATE + timedelta(days=number - 1)
    return PointInTime(f"{fixture_date.isoformat()}T{_SYNTHETIC_CLOSE_UTC}")


def _bar(
    symbol: str,
    session: int,
    open_amount: Decimal,
    close_amount: Decimal,
    volume: Decimal,
    *,
    exchange_code: ExchangeCode = _NSE,
    currency: Currency = _INR,
) -> HistoricalOHLCVBar:
    return HistoricalOHLCVBar(
        symbol=Symbol(symbol),
        exchange_code=exchange_code,
        point_in_time=_session(session),
        timeframe=_DAILY,
        open=Price(open_amount, currency),
        high=Price(max(open_amount, close_amount) + _WICK, currency),
        low=Price(min(open_amount, close_amount) - _WICK, currency),
        close=Price(close_amount, currency),
        volume=Quantity(volume),
    )


def _trend(
    symbol: str,
    first_session: int,
    first_close: Decimal,
    step: Decimal,
    volumes: tuple[Decimal, ...],
    *,
    exchange_code: ExchangeCode = _NSE,
) -> tuple[HistoricalOHLCVBar, ...]:
    """Bars whose close moves by ``step`` each session, opening at the prior close."""
    bars = []
    previous_close = first_close - step
    for offset, volume in enumerate(volumes):
        close = first_close + step * offset
        bars.append(
            _bar(
                symbol,
                first_session + offset,
                previous_close,
                close,
                volume,
                exchange_code=exchange_code,
            )
        )
        previous_close = close
    return tuple(bars)


def _reliance_bars() -> tuple[HistoricalOHLCVBar, ...]:
    return _trend("RELIANCE", 1, _RELIANCE_FIRST_CLOSE, _RELIANCE_STEP, _RELIANCE_VOLUMES)


def _reliance_future_bars() -> tuple[HistoricalOHLCVBar, ...]:
    last_close = _reliance_bars()[-1].close.amount
    return _trend(
        "RELIANCE",
        _LAST_SESSION + 1,
        last_close + _RELIANCE_FUTURE_STEP,
        _RELIANCE_FUTURE_STEP,
        _RELIANCE_FUTURE_VOLUMES,
    )


def _tcs_bars() -> tuple[HistoricalOHLCVBar, ...]:
    return _trend("TCS", 1, _TCS_FIRST_CLOSE, _TCS_STEP, _TCS_VOLUMES)


def _query(
    symbol: str,
    through_session: int = _LAST_SESSION,
    *,
    exchange_code: ExchangeCode = _NSE,
) -> HistoricalMarketDataQuery:
    return HistoricalMarketDataQuery(
        symbol=Symbol(symbol),
        exchange_code=exchange_code,
        timeframe=_DAILY,
        start=_session(1),
        end=_session(through_session),
    )


def _ingest(database_path: Path, bars: tuple[HistoricalOHLCVBar, ...]):
    query = HistoricalMarketDataQuery(
        symbol=bars[0].symbol,
        exchange_code=bars[0].exchange_code,
        timeframe=_DAILY,
        start=bars[0].point_in_time,
        end=bars[-1].point_in_time,
    )
    use_case = IngestHistoricalMarketDataUseCase(
        _FixtureSource(bars), SQLiteHistoricalMarketDataStore(database_path)
    )
    return use_case.execute(query)


def _evaluator() -> HistoricalSnapshotEvaluatorService:
    return HistoricalSnapshotEvaluatorService(Strategy(_STRATEGY), AssetAnalysisGenerator())


def _replay(database_path: Path, symbol: str, through_session: int = _LAST_SESSION):
    repository = SQLiteHistoricalMarketDataRepository(database_path)
    return ReplayHistoricalMarketDataUseCase(repository).execute(_query(symbol, through_session))


def _evaluate(
    database_path: Path, symbol: str, through_session: int = _LAST_SESSION
) -> tuple[HistoricalResearchEvaluation, ...]:
    """Evaluate every replay snapshot that meets the evaluator's minimum history.

    The warm-up exclusion is the caller's job under the current contracts, as
    in the existing Epic 6 look-ahead acceptance test.
    """
    snapshots = _replay(database_path, symbol, through_session)
    evaluable = tuple(
        snapshot for snapshot in snapshots if len(snapshot.observations) >= _MINIMUM_HISTORY
    )
    return EvaluateHistoricalResearchUseCase(_evaluator()).execute(evaluable)


def _run(
    database_path: Path,
    symbol: str,
    through_session: int = _LAST_SESSION,
    *,
    available_through: PointInTime | None = None,
) -> HistoricalResearchRun:
    repository = SQLiteHistoricalMarketDataRepository(database_path)
    return RunHistoricalResearchUseCase(MeasureRecommendationOutcomeUseCase(repository)).execute(
        _evaluate(database_path, symbol, through_session),
        _HORIZONS,
        _DAILY,
        available_through if available_through is not None else _session(through_session),
    )


def _actions(evaluations: tuple[HistoricalResearchEvaluation, ...]) -> tuple[str, ...]:
    return tuple(evaluation.result.recommendation.action.value for evaluation in evaluations)


def _stored_rows(database_path: Path) -> list[tuple[str, ...]]:
    with sqlite3.connect(database_path) as connection:
        return connection.execute(
            "SELECT symbol, exchange_code, timeframe, currency, point_in_time, close "
            "FROM historical_ohlcv ORDER BY symbol, point_in_time"
        ).fetchall()


# A. Identity and currency


def test_stored_rows_carry_canonical_nse_identity_and_inr(ingested_database: Path) -> None:
    rows = _stored_rows(ingested_database)

    assert {row[:4] for row in rows} == {
        ("RELIANCE", "NSE", "1d", "INR"),
        ("TCS", "NSE", "1d", "INR"),
    }
    assert sum(row[0] == "RELIANCE" for row in rows) == _LAST_SESSION
    assert sum(row[0] == "TCS" for row in rows) == _LAST_SESSION


@pytest.mark.parametrize("symbol, bars", [("RELIANCE", _reliance_bars), ("TCS", _tcs_bars)])
def test_repository_returns_the_ingested_bars_exactly(
    ingested_database: Path, symbol: str, bars
) -> None:
    repository = SQLiteHistoricalMarketDataRepository(ingested_database)

    assert repository.get_history(_query(symbol)) == bars()


@pytest.mark.parametrize("symbol", ["RELIANCE", "TCS"])
def test_every_decision_is_attributed_to_its_nse_listing_in_inr(
    ingested_database: Path, symbol: str
) -> None:
    listing = ListingReference(symbol=Symbol(symbol), exchange_code=_NSE)

    for evaluation in _evaluate(ingested_database, symbol):
        result = evaluation.result
        context = result.market_observation_context
        assert result.recommendation.asset_analysis.listing_reference == listing
        assert context.listing_reference == listing
        prices = (
            context.latest_price,
            context.previous_close,
            context.daily_high,
            context.daily_low,
            *context.recent_closes,
        )
        assert {price.currency for price in prices} == {_INR}


def test_the_same_symbol_on_another_exchange_is_a_separate_listing(
    ingested_database: Path,
) -> None:
    bse_bars = _trend(
        "RELIANCE",
        1,
        Decimal("10.00"),
        Decimal("0.05"),
        (Decimal("7"),) * _LAST_SESSION,
        exchange_code=ExchangeCode("BSE"),
    )
    _ingest(ingested_database, bse_bars)
    repository = SQLiteHistoricalMarketDataRepository(ingested_database)

    assert repository.get_history(_query("RELIANCE")) == _reliance_bars()
    assert repository.get_history(_query("RELIANCE", exchange_code=ExchangeCode("BSE"))) == bse_bars
    assert _actions(_evaluate(ingested_database, "RELIANCE")) == _RELIANCE_ACTIONS


# B. Ingestion and persistence


@pytest.mark.parametrize("bars", [_reliance_bars, _tcs_bars])
def test_ingestion_accepts_the_complete_synthetic_batch(database_path: Path, bars) -> None:
    result = _ingest(database_path, bars())

    assert result.acquired_count == _LAST_SESSION
    assert result.stored_count == _LAST_SESSION


def test_the_database_lives_only_in_pytest_temporary_storage(
    tmp_path: Path, ingested_database: Path
) -> None:
    assert ingested_database.is_relative_to(tmp_path)
    assert ingested_database.exists()
    assert sorted(path.name for path in tmp_path.iterdir()) == [ingested_database.name]


def test_reingesting_identical_bars_creates_no_duplicate_rows(ingested_database: Path) -> None:
    before = _stored_rows(ingested_database)

    _ingest(ingested_database, _reliance_bars())

    assert _stored_rows(ingested_database) == before


def test_a_conflicting_reingested_bar_replaces_by_logical_identity(
    ingested_database: Path,
) -> None:
    """Current store contract: last write wins, without adding a row.

    The correction is not separately audited; that is a known gap, not a
    behavior this slice changes.
    """
    original = _reliance_bars()[24]
    corrected = _bar("RELIANCE", 25, original.open.amount, Decimal("756.00"), Decimal("250000"))
    row_count = len(_stored_rows(ingested_database))

    _ingest(ingested_database, (corrected,))
    stored = SQLiteHistoricalMarketDataRepository(ingested_database).get_history(_query("RELIANCE"))

    assert len(_stored_rows(ingested_database)) == row_count
    assert stored[24] == corrected
    assert stored[24] != original


def test_a_batch_with_a_duplicate_session_is_refused_before_storage(
    database_path: Path,
) -> None:
    bars = _reliance_bars()
    conflicting = _bar("RELIANCE", 5, Decimal("700.00"), Decimal("701.00"), Decimal("1"))

    with pytest.raises(HistoricalDataContractViolationError, match="strictly ordered"):
        _ingest(database_path, bars[:5] + (conflicting,) + bars[5:])

    assert not database_path.exists()


# C. Research


@pytest.mark.parametrize(
    "symbol, expected", [("RELIANCE", _RELIANCE_ACTIONS), ("TCS", _TCS_ACTIONS)]
)
def test_existing_strategy_produces_the_expected_research_actions(
    ingested_database: Path, symbol: str, expected: tuple[str, ...]
) -> None:
    evaluations = _evaluate(ingested_database, symbol)

    assert _actions(evaluations) == expected
    assert evaluations[0].replay_instant == _session(_MINIMUM_HISTORY)
    assert evaluations[-1].replay_instant == _session(_LAST_SESSION)


def test_buy_sell_and_hold_are_all_produced(ingested_database: Path) -> None:
    actions = set(_actions(_evaluate(ingested_database, "RELIANCE")))
    actions |= set(_actions(_evaluate(ingested_database, "TCS")))

    assert actions == {"BUY", "SELL", "HOLD"}


def test_hold_is_produced_when_volume_does_not_confirm_a_rise(ingested_database: Path) -> None:
    evaluations = _evaluate(ingested_database, "RELIANCE")
    hold = next(e for e in evaluations if e.replay_instant == _session(31))
    context = hold.result.market_observation_context

    assert context.latest_price > context.previous_close
    assert context.latest_volume == Quantity(Decimal("120000"))
    assert hold.result.recommendation.action.value == "HOLD"


@pytest.mark.parametrize("symbol", ["RELIANCE", "TCS"])
def test_explanation_and_source_references_are_retained(
    ingested_database: Path, symbol: str
) -> None:
    for evaluation in _evaluate(ingested_database, symbol):
        recommendation = evaluation.result.recommendation
        explanation = evaluation.result.explanation
        signals = _ACTION_SIGNALS[recommendation.action.value]

        assert recommendation.strategy_identity == _STRATEGY
        assert recommendation.point_in_time == evaluation.replay_instant
        assert recommendation.asset_analysis.summarized_signals == signals
        assert explanation.recommendation == recommendation
        assert len(explanation.reasons) == 1
        assert explanation.reasons[0].supporting_signals == signals


def test_research_is_deterministic_across_independent_databases(tmp_path: Path) -> None:
    outcomes = []
    for name in ("first.sqlite3", "second.sqlite3"):
        path = tmp_path / name
        _ingest(path, _reliance_bars())
        _ingest(path, _tcs_bars())
        runs = (_run(path, "RELIANCE"), _run(path, "TCS"))
        metrics = tuple(CalculateHistoricalResearchMetricsUseCase().execute(run) for run in runs)
        outcomes.append((runs, metrics))

    assert outcomes[0] == outcomes[1]


# D. Point-in-time correctness


@pytest.mark.parametrize("symbol, bars", [("RELIANCE", _reliance_bars), ("TCS", _tcs_bars)])
def test_each_decision_sees_only_bars_completed_by_its_instant(
    ingested_database: Path, symbol: str, bars
) -> None:
    fixture = bars()

    for snapshot in _replay(ingested_database, symbol):
        assert all(
            bar.point_in_time.compare(snapshot.replay_instant) <= 0 for bar in snapshot.observations
        )

    for session, evaluation in enumerate(_evaluate(ingested_database, symbol), start=20):
        context = evaluation.result.market_observation_context
        visible = fixture[:session]
        assert context.observed_at == _session(session) == evaluation.replay_instant
        assert context.latest_price == visible[-1].close
        assert context.previous_close == visible[-2].close
        assert context.recent_closes == tuple(bar.close for bar in visible[-_MINIMUM_HISTORY:])
        assert context.recent_volumes == tuple(bar.volume for bar in visible[-_MINIMUM_HISTORY:])


def test_appending_divergent_future_sessions_never_changes_earlier_decisions(
    ingested_database: Path,
) -> None:
    before = _run(ingested_database, "RELIANCE")

    _ingest(ingested_database, _reliance_future_bars())
    after = _run(ingested_database, "RELIANCE", _FUTURE_LAST_SESSION)
    decided_before = len(before.evaluations)

    assert after.evaluations[:decided_before] == before.evaluations
    # The appended sessions are divergent enough to produce different decisions.
    assert _actions(after.evaluations[decided_before:]) == ("HOLD",) * 3 + ("SELL",) * 5
    # Measurement may complete with more data; the decision it measures may not change.
    last_before = before.measurements[-1]
    last_after = after.measurements[2 * decided_before - 1]
    assert last_before.recommendation == last_after.recommendation
    assert not last_before.is_measured
    assert last_after.is_measured


def test_a_snapshot_cannot_contain_a_bar_after_its_instant() -> None:
    bars = _reliance_bars()

    with pytest.raises(InvalidHistoricalReplaySnapshotError):
        HistoricalReplaySnapshot(_session(20), bars[:21])


def test_a_snapshot_cannot_contain_a_duplicate_logical_bar() -> None:
    bars = _reliance_bars()
    duplicate = _bar("RELIANCE", 20, Decimal("700.00"), Decimal("702.00"), Decimal("1"))

    with pytest.raises(InvalidHistoricalReplaySnapshotError):
        HistoricalReplaySnapshot(_session(20), bars[:20] + (duplicate,))


def test_a_decision_requires_twenty_completed_sessions(ingested_database: Path) -> None:
    snapshots = _replay(ingested_database, "RELIANCE")

    with pytest.raises(ValueError, match="at least 20 daily bars"):
        _evaluator().evaluate(snapshots[_MINIMUM_HISTORY - 2])
    assert _evaluator().evaluate(snapshots[_MINIMUM_HISTORY - 1]) is not None
    # Evaluation does not skip warm-up snapshots on the caller's behalf.
    with pytest.raises(ValueError, match="at least 20 daily bars"):
        EvaluateHistoricalResearchUseCase(_evaluator()).execute(snapshots)


# E. Measurement


@pytest.mark.parametrize("symbol", ["RELIANCE", "TCS"])
def test_every_decision_is_measured_at_every_requested_horizon_in_order(
    ingested_database: Path, symbol: str
) -> None:
    run = _run(ingested_database, symbol)

    assert run.horizons == _HORIZONS
    assert len(run.measurements) == len(run.evaluations) * len(_HORIZONS)
    expected = [
        (evaluation.result.recommendation, horizon)
        for evaluation in run.evaluations
        for horizon in _HORIZONS
    ]
    assert [(m.recommendation, m.horizon) for m in run.measurements] == expected


def test_forward_return_is_same_basis_close_to_close_movement(ingested_database: Path) -> None:
    bars = _reliance_bars()
    run = _run(ingested_database, "RELIANCE")
    # The first decision (session 20) measured five sessions later (session 25).
    outcome = run.measurements[1].outcome

    decision_close = bars[19].close.amount
    evaluation_close = bars[24].close.amount
    with localcontext() as context:
        context.prec = 28
        expected = (evaluation_close - decision_close) / decision_close * 100

    assert outcome.decision_price == bars[19].close
    assert outcome.evaluation_instant == _session(25)
    assert outcome.evaluation_price == bars[24].close
    assert outcome.forward_return.value == expected


@pytest.mark.parametrize("symbol", ["RELIANCE", "TCS"])
def test_incomplete_future_coverage_remains_explicit(ingested_database: Path, symbol: str) -> None:
    run = _run(ingested_database, symbol)
    one, five = CalculateHistoricalResearchMetricsUseCase().execute(run)

    unmeasured = [
        (m.recommendation.point_in_time, m.horizon.observations)
        for m in run.measurements
        if not m.is_measured
    ]
    # Measurements are ordered by decision first, then horizon.
    assert unmeasured == [(_session(s), 5) for s in range(28, 32)] + [
        (_session(32), 1),
        (_session(32), 5),
    ]
    assert all(
        m.unavailable is RecommendationOutcomeUnavailableReason.INSUFFICIENT_FUTURE_OBSERVATIONS
        for m in run.measurements
        if not m.is_measured
    )
    assert (one.total_count, one.measured_count, one.insufficient_future_observations_count) == (
        13,
        12,
        1,
    )
    assert (five.total_count, five.measured_count, five.insufficient_future_observations_count) == (
        13,
        8,
        5,
    )
    assert one.currency_mismatch_count == five.currency_mismatch_count == 0


def test_no_future_data_yields_no_statistics_rather_than_zero(ingested_database: Path) -> None:
    run = _run(ingested_database, "RELIANCE", available_through=_session(19))

    for metrics in CalculateHistoricalResearchMetricsUseCase().execute(run):
        assert metrics.measured_count == 0
        assert metrics.insufficient_future_observations_count == metrics.total_count == 13
        assert metrics.average_forward_return is None
        assert metrics.median_forward_return is None
        assert metrics.minimum_forward_return is None
        assert metrics.maximum_forward_return is None


def test_returns_are_factual_market_movement_not_trading_pnl(ingested_database: Path) -> None:
    tcs = _run(ingested_database, "TCS")
    reliance = _run(ingested_database, "RELIANCE")

    # SELL decisions on a falling series measure a negative movement: no sign
    # inversion or action interpretation is applied.
    assert all(m.outcome.forward_return.value < 0 for m in tcs.measurements if m.is_measured)
    assert all(m.outcome.forward_return.value > 0 for m in reliance.measurements if m.is_measured)
    assert {field.name for field in dataclasses.fields(HistoricalResearchHorizonMetrics)} == {
        "horizon",
        "total_count",
        "measured_count",
        "insufficient_future_observations_count",
        "currency_mismatch_count",
        "average_forward_return",
        "median_forward_return",
        "minimum_forward_return",
        "maximum_forward_return",
    }


def test_metrics_are_deterministic_for_the_same_run(ingested_database: Path) -> None:
    run = _run(ingested_database, "RELIANCE")
    calculate = CalculateHistoricalResearchMetricsUseCase()

    assert calculate.execute(run) == calculate.execute(run)
    assert calculate.execute(run) == calculate.execute(_run(ingested_database, "RELIANCE"))


# F. Safety and fail-closed behavior


def test_network_connections_are_refused_throughout_this_module() -> None:
    with pytest.raises(AssertionError, match="must not open network connections"):
        socket.create_connection(("127.0.0.1", 9))


def test_an_invalid_bar_cannot_be_constructed() -> None:
    with pytest.raises(InvalidHistoricalOHLCVBarError):
        HistoricalOHLCVBar(
            symbol=Symbol("RELIANCE"),
            exchange_code=_NSE,
            point_in_time=_session(1),
            timeframe=_DAILY,
            open=Price(Decimal("640.00"), _INR),
            high=Price(Decimal("639.00"), _INR),
            low=Price(Decimal("638.00"), _INR),
            close=Price(Decimal("640.00"), _INR),
            volume=Quantity(Decimal("1")),
        )
    with pytest.raises(InvalidHistoricalOHLCVBarError):
        HistoricalOHLCVBar(
            symbol=Symbol("RELIANCE"),
            exchange_code=_NSE,
            point_in_time=_session(1),
            timeframe=_DAILY,
            open=Price(Decimal("640.00"), _INR),
            high=Price(Decimal("641.00"), _INR),
            low=Price(Decimal("639.00"), _INR),
            close=Price(Decimal("640.00"), Currency("USD")),
            volume=Quantity(Decimal("1")),
        )


def test_float_prices_are_refused() -> None:
    with pytest.raises(InvalidPriceError):
        Price(640.0, _INR)


@pytest.mark.parametrize(
    "bars",
    [
        lambda: _tcs_bars()[:3],
        lambda: _trend(
            "RELIANCE",
            1,
            _RELIANCE_FIRST_CLOSE,
            _RELIANCE_STEP,
            (Decimal("1"),) * 3,
            exchange_code=ExchangeCode("BSE"),
        ),
    ],
    ids=["other-symbol", "other-exchange"],
)
def test_source_bars_for_another_listing_are_refused(database_path: Path, bars) -> None:
    use_case = IngestHistoricalMarketDataUseCase(
        _FixtureSource(bars()), SQLiteHistoricalMarketDataStore(database_path)
    )

    with pytest.raises(HistoricalDataContractViolationError, match="does not match query"):
        use_case.execute(_query("RELIANCE", 3))

    assert not database_path.exists()


def test_a_snapshot_mixing_listings_is_refused() -> None:
    mixed = _reliance_bars()[:20] + (_tcs_bars()[19],)

    with pytest.raises(ValueError, match="incompatible market identities"):
        _evaluator().evaluate(HistoricalReplaySnapshot(_session(20), mixed))


def test_a_currency_change_is_refused_for_decisions_and_never_measured(
    ingested_database: Path,
) -> None:
    last = _reliance_bars()[-1]
    usd_bar = _bar(
        "RELIANCE",
        _LAST_SESSION + 1,
        Decimal("9.00"),
        Decimal("9.50"),
        Decimal("250000"),
        currency=Currency("USD"),
    )
    _ingest(ingested_database, (usd_bar,))
    snapshots = _replay(ingested_database, "RELIANCE", _LAST_SESSION + 1)

    with pytest.raises(InvalidMarketObservationContextError):
        _evaluator().evaluate(snapshots[-1])

    decision = _evaluator().evaluate(snapshots[-2])
    measurement = MeasureRecommendationOutcomeUseCase(
        SQLiteHistoricalMarketDataRepository(ingested_database)
    ).execute(decision, ResearchHorizon(1), _DAILY, _session(_LAST_SESSION + 1))
    assert decision.market_observation_context.latest_price == last.close
    assert measurement.unavailable is RecommendationOutcomeUnavailableReason.CURRENCY_MISMATCH
