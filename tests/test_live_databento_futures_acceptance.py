"""Live acceptance for the real futures historical pipeline.

    real Databento (GLBX.MDP3, ohlcv-1m)
      -> real exchange-calendar session resolver
      -> canonical 1m FuturesOHLCVBar values
      -> Application daily aggregation and acquisition
      -> real SQLite futures store
      -> real repository reload
      -> exact canonical 1d FuturesOHLCVBar

Every component on that path is the production implementation. Nothing here is
faked, which is the whole point: the unit suites already prove each piece in
isolation, and this proves they agree about the same session.

Opt-in only
-----------
These tests are skipped unless NORTHSTAR_LIVE_DATABENTO is set, so an ordinary
pytest run performs zero Databento requests. To run them in PowerShell:

    $env:NORTHSTAR_LIVE_DATABENTO = "1"
    $env:DATABENTO_API_KEY = "<your key>"
    uv run pytest tests/test_live_databento_futures_acceptance.py -v

If DATABENTO_API_KEY is absent from the environment, a test-only helper reads
it from this repository's gitignored .env. Production code never does that --
the adapter takes its key as a constructor argument and reads no environment at
all, which a unit test asserts through the AST.

The key is never printed, never written to a fixture and never placed in an
assertion message.

Observed on 2026-09-23 (provenance, deliberately not asserted)
---------------------------------------------------------------
These are what a passing run produced. They are recorded so a future failure
can be compared against something, and left unasserted because a provider may
legitimately restate history.

    ES Dec 2026, session 2026-09-15  (provider locator ESZ6)
      1380 minute bars, first completing 2026-09-14T22:01Z,
      last completing 2026-09-15T21:00Z -- an hour before the session close,
      because Globex halts 16:00-17:00 Chicago.
      daily  O=7699.25 H=7701 L=7643.5 C=7663.75 V=1082661 @ 2026-09-15T22:00Z

    ES Dec 2026, session 2026-07-03  (early close)
      93 minute bars, last completing exactly 2026-07-03T17:00Z
      daily  O=7592 H=7630.25 L=7591.5 C=7623.75 V=132 @ 2026-07-03T17:00Z

The first of those is worth keeping in view: Databento's own UTC-day ohlcv-1d
bar for the same date reported O=7696.00 C=7665.50 V=1083921, which differs in
open, close and volume. That is the schema this pipeline deliberately does not
use, and the gap is the evidence for why.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pytest
from northstar_application.application_services import (
    AcquireFuturesDailyHistoryUseCase,
    AggregateFuturesDailySessionBarUseCase,
    FuturesDailyAcquisitionResult,
)
from northstar_application.ports import (
    FuturesDailyHistoricalAcquisitionQuery,
    FuturesHistoricalMarketDataQuery,
)
from northstar_core.derivatives import ExpirationDate
from northstar_core.foundation.value_objects import (
    ExchangeCode,
    PointInTime,
    Symbol,
    Timeframe,
)
from northstar_core.futures import FuturesContract, FuturesOHLCVBar, FuturesProductReference

from northstar_infrastructure.market_data import (
    DatabentoFuturesHistoricalMarketDataSource,
    ExchangeCalendarFuturesTradingSessionResolver,
    SQLiteFuturesHistoricalMarketDataRepository,
    SQLiteFuturesHistoricalMarketDataStore,
)

_LIVE_FLAG = "NORTHSTAR_LIVE_DATABENTO"
_KEY_NAME = "DATABENTO_API_KEY"

pytestmark = pytest.mark.skipif(
    not os.environ.get(_LIVE_FLAG),
    reason=f"live Databento acceptance is opt-in; set {_LIVE_FLAG}=1 to enable",
)

# The contract and session whose identity the Epic 9.6a probe confirmed.
_ES_DEC_2026 = FuturesContract(
    FuturesProductReference(Symbol("ES"), ExchangeCode("CME")),
    ExpirationDate("2026-12-18"),
)
_TRADING_DATE = date(2026, 9, 15)
_EXPECTED_OPEN = PointInTime("2026-09-14T22:00:00Z")
_EXPECTED_CLOSE = PointInTime("2026-09-15T22:00:00Z")

_EARLY_CLOSE_DATE = date(2026, 7, 3)
_EARLY_CLOSE_INSTANT = PointInTime("2026-07-03T17:00:00Z")

_MINUTE = Timeframe("1m")
_DAILY = Timeframe("1d")


def _api_key() -> str:
    """Return the live key from the environment, or the gitignored .env.

    Test-only. Production takes its key as an argument and reads neither.
    """
    from_environment = os.environ.get(_KEY_NAME)
    if from_environment and from_environment.strip():
        return from_environment.strip()

    env_file = Path(__file__).resolve().parents[1] / ".env"
    if env_file.exists():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            name, _, value = line.strip().partition("=")
            if name.strip() == _KEY_NAME:
                candidate = value.strip().strip('"').strip("'")
                if candidate:
                    return candidate

    pytest.skip(f"{_KEY_NAME} is not available")
    raise AssertionError("unreachable")


@dataclass(frozen=True)
class LiveRun:
    """Everything one live acquisition produced, so tests share a single fetch."""

    database_path: str
    session_open: PointInTime
    session_close: PointInTime
    provider_symbol: str
    minute_bars: tuple[FuturesOHLCVBar, ...]
    independently_folded: FuturesOHLCVBar
    first_result: FuturesDailyAcquisitionResult
    rerun_result: FuturesDailyAcquisitionResult
    reloaded: tuple[FuturesOHLCVBar, ...]
    reloaded_after_rerun: tuple[FuturesOHLCVBar, ...]


def _acquire(tmp_path_factory: pytest.TempPathFactory, trading_date: date, label: str) -> LiveRun:
    """Run the whole real pipeline once for one session."""
    database_path = str(tmp_path_factory.mktemp(label) / "futures.sqlite3")

    source = DatabentoFuturesHistoricalMarketDataSource(_api_key())
    resolver = ExchangeCalendarFuturesTradingSessionResolver()
    aggregator = AggregateFuturesDailySessionBarUseCase()
    store = SQLiteFuturesHistoricalMarketDataStore(database_path)
    repository = SQLiteFuturesHistoricalMarketDataRepository(database_path)

    session = resolver.resolve(_ES_DEC_2026.product, trading_date)
    assert session is not None, f"{trading_date} is not a CME session"

    use_case = AcquireFuturesDailyHistoryUseCase(resolver, source, aggregator, store)
    query = FuturesDailyHistoricalAcquisitionQuery(_ES_DEC_2026, trading_date, trading_date)

    first_result = use_case.execute(query)

    reload_query = FuturesHistoricalMarketDataQuery(
        contract=_ES_DEC_2026, timeframe=_DAILY, start=session.opens_at, end=session.closes_at
    )
    reloaded = repository.get_bars(reload_query)

    # Independent truth: fetch the same session's minutes again and fold them
    # with the same Application use case, outside the acquisition path.
    minute_bars = source.fetch_session_bars(_ES_DEC_2026, session)
    independently_folded = aggregator.execute(_ES_DEC_2026, session, minute_bars)
    assert independently_folded is not None

    rerun_result = use_case.execute(query)
    reloaded_after_rerun = repository.get_bars(reload_query)

    return LiveRun(
        database_path=database_path,
        session_open=session.opens_at,
        session_close=session.closes_at,
        provider_symbol=source._resolved[_ES_DEC_2026],  # noqa: SLF001 - reported for debugging
        minute_bars=minute_bars,
        independently_folded=independently_folded,
        first_result=first_result,
        rerun_result=rerun_result,
        reloaded=reloaded,
        reloaded_after_rerun=reloaded_after_rerun,
    )


@pytest.fixture(scope="module")
def live(tmp_path_factory: pytest.TempPathFactory) -> LiveRun:
    """One live acquisition shared by every assertion, to keep cost tiny."""
    return _acquire(tmp_path_factory, _TRADING_DATE, "live_normal")


@pytest.fixture(scope="module")
def live_early_close(tmp_path_factory: pytest.TempPathFactory) -> LiveRun:
    return _acquire(tmp_path_factory, _EARLY_CLOSE_DATE, "live_early_close")


# ---------------------------------------------------------------------------
# Session resolution
# ---------------------------------------------------------------------------


def test_the_real_calendar_resolves_the_expected_session(live: LiveRun) -> None:
    assert live.session_open == _EXPECTED_OPEN
    assert live.session_close == _EXPECTED_CLOSE


def test_the_contract_resolved_to_one_provider_symbol(live: LiveRun) -> None:
    """The symbol is a provider locator, never Northstar identity."""
    assert isinstance(live.provider_symbol, str)
    assert live.provider_symbol


# ---------------------------------------------------------------------------
# Acquisition result
# ---------------------------------------------------------------------------


def test_one_session_produced_one_daily_bar(live: LiveRun) -> None:
    assert live.first_result.session_count == 1
    assert live.first_result.daily_bar_count == 1


def test_exactly_one_daily_bar_was_persisted(live: LiveRun) -> None:
    assert len(live.reloaded) == 1


# ---------------------------------------------------------------------------
# The persisted daily bar
# ---------------------------------------------------------------------------


def test_the_persisted_bar_carries_the_requested_identity(live: LiveRun) -> None:
    (daily,) = live.reloaded

    assert daily.contract == _ES_DEC_2026
    assert daily.timeframe == _DAILY
    assert daily.point_in_time == _EXPECTED_CLOSE
    assert daily.natural_key == (_ES_DEC_2026, _EXPECTED_CLOSE, _DAILY)


def test_the_persisted_bar_has_valid_ohlc_ordering(live: LiveRun) -> None:
    (daily,) = live.reloaded

    assert daily.high >= daily.open
    assert daily.high >= daily.close
    assert daily.high >= daily.low
    assert daily.low <= daily.open
    assert daily.low <= daily.close


def test_the_persisted_volume_is_a_positive_whole_contract_count(live: LiveRun) -> None:
    (daily,) = live.reloaded
    volume = daily.volume.value

    assert volume > 0
    assert volume == volume.to_integral_value()


def test_the_persisted_bar_equals_an_independent_fold_exactly(live: LiveRun) -> None:
    """The acceptance's central claim, with no tolerance and no floats."""
    (daily,) = live.reloaded

    assert daily == live.independently_folded
    assert daily.open == live.independently_folded.open
    assert daily.high == live.independently_folded.high
    assert daily.low == live.independently_folded.low
    assert daily.close == live.independently_folded.close
    assert daily.volume == live.independently_folded.volume


# ---------------------------------------------------------------------------
# Minute-bar boundary proof
# ---------------------------------------------------------------------------


def test_the_session_returned_minute_bars(live: LiveRun) -> None:
    assert live.minute_bars
    assert all(bar.timeframe == _MINUTE for bar in live.minute_bars)
    assert all(bar.contract == _ES_DEC_2026 for bar in live.minute_bars)


def test_every_minute_bar_completes_inside_the_session(live: LiveRun) -> None:
    """opens_at < point_in_time <= closes_at, for real provider data."""
    for bar in live.minute_bars:
        assert live.session_open.compare(bar.point_in_time) < 0
        assert bar.point_in_time.compare(live.session_close) <= 0


def test_no_minute_bar_completes_at_the_session_open(live: LiveRun) -> None:
    assert all(bar.point_in_time != live.session_open for bar in live.minute_bars)


def test_no_minute_bar_completes_after_the_session_close(live: LiveRun) -> None:
    assert all(bar.point_in_time.compare(live.session_close) <= 0 for bar in live.minute_bars)


def test_minute_bars_are_strictly_chronological(live: LiveRun) -> None:
    for earlier, later in zip(live.minute_bars, live.minute_bars[1:], strict=False):
        assert earlier.point_in_time.compare(later.point_in_time) < 0


def test_minute_continuity_is_not_required(live: LiveRun) -> None:
    """Sparse no-trade minutes are valid; the fold must not assume otherwise."""
    assert len(live.minute_bars) <= 24 * 60


# ---------------------------------------------------------------------------
# Idempotent rerun
# ---------------------------------------------------------------------------


def test_the_rerun_succeeds_with_the_same_counts(live: LiveRun) -> None:
    assert live.rerun_result.session_count == 1
    assert live.rerun_result.daily_bar_count == 1
    assert live.rerun_result == live.first_result


def test_the_rerun_left_exactly_one_row(live: LiveRun) -> None:
    import sqlite3

    connection = sqlite3.connect(live.database_path)
    try:
        (rows,) = connection.execute("SELECT COUNT(*) FROM futures_ohlcv").fetchone()
    finally:
        connection.close()

    assert rows == 1
    assert len(live.reloaded_after_rerun) == 1


def test_the_reloaded_bar_is_unchanged_after_the_rerun(live: LiveRun) -> None:
    assert live.reloaded_after_rerun == live.reloaded
    assert live.reloaded_after_rerun[0] == live.independently_folded


# ---------------------------------------------------------------------------
# Early close: proves the pipeline never uses open + 24h
# ---------------------------------------------------------------------------


def test_the_early_close_session_completes_at_seventeen_hundred(
    live_early_close: LiveRun,
) -> None:
    assert live_early_close.session_close == _EARLY_CLOSE_INSTANT
    assert live_early_close.session_open == PointInTime("2026-07-02T22:00:00Z")


def test_the_early_close_daily_bar_is_stamped_at_the_real_close(
    live_early_close: LiveRun,
) -> None:
    """open + 24h would be 22:00Z; the calendar says 17:00Z."""
    assert live_early_close.first_result.session_count == 1

    if live_early_close.first_result.daily_bar_count == 0:
        pytest.skip("no trades in the early-close session for this contract")

    (daily,) = live_early_close.reloaded
    assert daily.point_in_time == _EARLY_CLOSE_INSTANT
    assert daily.point_in_time != PointInTime("2026-07-03T22:00:00Z")


def test_the_early_close_rerun_is_idempotent(live_early_close: LiveRun) -> None:
    assert live_early_close.rerun_result == live_early_close.first_result
    assert live_early_close.reloaded_after_rerun == live_early_close.reloaded


def test_every_early_close_minute_completes_inside_its_session(
    live_early_close: LiveRun,
) -> None:
    for bar in live_early_close.minute_bars:
        assert live_early_close.session_open.compare(bar.point_in_time) < 0
        assert bar.point_in_time.compare(_EARLY_CLOSE_INSTANT) <= 0


# ---------------------------------------------------------------------------
# Out-of-life smoke
# ---------------------------------------------------------------------------


def test_a_pre_listing_session_yields_no_bars(live: LiveRun) -> None:
    """A valid contract outside its listed life is empty, not an error."""
    source = DatabentoFuturesHistoricalMarketDataSource(_api_key())
    resolver = ExchangeCalendarFuturesTradingSessionResolver()

    session = resolver.resolve(_ES_DEC_2026.product, date(2015, 6, 2))
    assert session is not None

    assert source.fetch_session_bars(_ES_DEC_2026, session) == ()


# ---------------------------------------------------------------------------
# Security
# ---------------------------------------------------------------------------


def test_no_secret_reaches_the_database_path_or_repr(live: LiveRun) -> None:
    key = _api_key()
    source = DatabentoFuturesHistoricalMarketDataSource(key)

    assert key not in live.database_path
    assert key not in repr(source)
    assert key not in live.provider_symbol
