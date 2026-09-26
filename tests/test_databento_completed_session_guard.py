"""Tests for the completed-session guard on Databento daily acquisition.

A session still in progress must never reach the provider, because the fold
would stamp whatever minutes exist so far with the session close and persist
it as completed-session evidence. The clock is injected; no network is used.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta, timezone
from pathlib import Path

import pytest
from databento_dbn import InstrumentClass
from northstar_application.application_services import (
    AcquireFuturesDailyHistoryUseCase,
    AggregateFuturesDailySessionBarUseCase,
)
from northstar_application.ports import (
    FuturesDailyHistoricalAcquisitionQuery,
    FuturesTradingSession,
    FuturesTradingSessionResolver,
)
from northstar_core.derivatives import ExpirationDate
from northstar_core.foundation.value_objects import ExchangeCode, PointInTime, Symbol
from northstar_core.futures import FuturesContract, FuturesProductReference

from northstar_infrastructure.market_data import (
    DatabentoFuturesHistoricalMarketDataSource,
    FuturesTradingSessionInProgressError,
    SQLiteFuturesHistoricalMarketDataStore,
)

_API_KEY = "db-SECRET-NEVER-LOGGED-0123456789"
_ES_DEC = FuturesContract(
    FuturesProductReference(Symbol("ES"), ExchangeCode("CME")), ExpirationDate("2026-12-18")
)
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_DEC_EXPIRY_NS = int((datetime(2026, 12, 18, 14, 30, tzinfo=UTC) - _EPOCH).total_seconds()) * (
    10**9
)


def _utc(text: str) -> datetime:
    return datetime.fromisoformat(text.replace("Z", "+00:00"))


def _session(trading_date: date, opens: str, closes: str) -> FuturesTradingSession:
    return FuturesTradingSession(trading_date, PointInTime(opens), PointInTime(closes))


# Sessions open on the previous civil day; D3 closes early.
_D1 = _session(date(2026, 9, 14), "2026-09-13T22:00:00Z", "2026-09-14T21:00:00Z")
_D2 = _session(date(2026, 9, 15), "2026-09-14T22:00:00Z", "2026-09-15T21:00:00Z")
_EARLY = _session(date(2026, 11, 27), "2026-11-26T23:00:00Z", "2026-11-27T18:15:00Z")


@dataclass
class FakeDefinition:
    raw_symbol: str = "ESZ6"
    asset: str = "ES"
    exchange: str = "XCME"
    expiration: int = _DEC_EXPIRY_NS
    instrument_class: object = InstrumentClass.FUTURE
    security_type: str = "FUT"


@dataclass
class FakeOhlcv:
    ts_event: int
    open: int = 7_660_000_000_000
    high: int = 7_700_000_000_000
    low: int = 7_500_000_000_000
    close: int = 7_663_000_000_000
    volume: int = 100


def _minutes_after_open(session: FuturesTradingSession, count: int) -> list[FakeOhlcv]:
    opens_ns = int((_utc(session.opens_at.value) - _EPOCH).total_seconds()) * 10**9
    return [FakeOhlcv(ts_event=opens_ns + index * 60 * 10**9) for index in range(count)]


@dataclass
class FakeTimeseries:
    bars_by_start: dict[str, list[FakeOhlcv]] = field(default_factory=dict)
    calls: list[dict] = field(default_factory=list)

    def get_range(self, **parameters):
        self.calls.append(parameters)
        if parameters["schema"] == "definition":
            return [FakeDefinition()]
        return list(self.bars_by_start.get(parameters["start"].isoformat(), []))

    @property
    def bar_calls(self) -> list[dict]:
        return [call for call in self.calls if call["schema"] == "ohlcv-1m"]


@dataclass
class FakeMetadata:
    calls: list[str] = field(default_factory=list)

    def get_dataset_range(self, dataset):
        self.calls.append(dataset)
        edge = {"start": "2010-06-06T00:00:00Z", "end": "2026-09-22T23:12:02Z"}
        return {**edge, "schema": {"definition": edge}}


@dataclass
class FakeClient:
    timeseries: FakeTimeseries = field(default_factory=FakeTimeseries)
    metadata: FakeMetadata = field(default_factory=FakeMetadata)

    @property
    def request_count(self) -> int:
        return len(self.timeseries.calls) + len(self.metadata.calls)


def _client(*sessions: FuturesTradingSession, minutes: int = 3) -> FakeClient:
    client = FakeClient()
    for session in sessions:
        client.timeseries.bars_by_start[_utc(session.opens_at.value).isoformat()] = (
            _minutes_after_open(session, minutes)
        )
    return client


def _source(client: FakeClient, now: datetime) -> DatabentoFuturesHistoricalMarketDataSource:
    return DatabentoFuturesHistoricalMarketDataSource(_API_KEY, client=client, clock=lambda: now)


class Resolver(FuturesTradingSessionResolver):
    def __init__(self, *sessions: FuturesTradingSession) -> None:
        self.sessions = sessions

    def resolve(self, product, trading_date):
        return next((s for s in self.sessions if s.trading_date == trading_date), None)

    def sessions_in_range(self, product, start_date, end_date):
        return tuple(s for s in self.sessions if start_date <= s.trading_date <= end_date)


class CountingAggregator(AggregateFuturesDailySessionBarUseCase):
    def __init__(self) -> None:
        self.calls = 0

    def execute(self, contract, session, bars):
        self.calls += 1
        return super().execute(contract, session, bars)


class CountingStore(SQLiteFuturesHistoricalMarketDataStore):
    def __init__(self, path: Path) -> None:
        super().__init__(path)
        self.calls = 0

    def store(self, bars):
        self.calls += 1
        return super().store(bars)


@dataclass
class Acquisition:
    client: FakeClient
    aggregator: CountingAggregator
    store: CountingStore
    path: Path

    def rows(self) -> list[tuple]:
        with sqlite3.connect(self.path) as connection:
            return connection.execute(
                "SELECT point_in_time, close_value FROM futures_ohlcv ORDER BY 1"
            ).fetchall()


def _acquire(
    tmp_path: Path,
    now: datetime,
    *sessions: FuturesTradingSession,
    client: FakeClient | None = None,
) -> tuple[Acquisition, object]:
    client = client if client is not None else _client(*sessions)
    tmp_path.mkdir(parents=True, exist_ok=True)
    path = tmp_path / "acquisition.sqlite3"
    acquisition = Acquisition(client, CountingAggregator(), CountingStore(path), path)
    use_case = AcquireFuturesDailyHistoryUseCase(
        Resolver(*sessions), _source(client, now), acquisition.aggregator, acquisition.store
    )
    query = FuturesDailyHistoricalAcquisitionQuery(
        _ES_DEC, sessions[0].trading_date, sessions[-1].trading_date
    )
    try:
        return acquisition, use_case.execute(query)
    except FuturesTradingSessionInProgressError as error:
        return acquisition, error


def _assert_untouched(acquisition: Acquisition) -> None:
    assert acquisition.client.request_count == 0
    assert acquisition.aggregator.calls == 0
    assert acquisition.store.calls == 0


# ---------------------------------------------------------------------------
# Before, at and after the resolved close
# ---------------------------------------------------------------------------


def test_a_session_in_progress_never_reaches_the_provider_or_the_store(tmp_path: Path) -> None:
    # The provider could hand back three early minutes; they must never become a daily bar.
    acquisition, outcome = _acquire(tmp_path, _utc("2026-09-15T14:30:00Z"), _D2)

    assert isinstance(outcome, FuturesTradingSessionInProgressError)
    _assert_untouched(acquisition)
    assert not acquisition.path.exists() or acquisition.rows() == []


def test_the_exact_close_instant_is_still_in_progress(tmp_path: Path) -> None:
    acquisition, outcome = _acquire(tmp_path, _utc("2026-09-15T21:00:00Z"), _D2)

    assert isinstance(outcome, FuturesTradingSessionInProgressError)
    _assert_untouched(acquisition)


def test_the_first_instant_after_the_close_acquires_normally(tmp_path: Path) -> None:
    acquisition, outcome = _acquire(tmp_path, _utc("2026-09-15T21:00:00.000001Z"), _D2)

    assert (outcome.session_count, outcome.daily_bar_count) == (1, 1)
    assert len(acquisition.client.timeseries.bar_calls) == 1
    assert acquisition.rows() == [("2026-09-15T21:00:00Z", "7663")]


def test_a_session_that_has_not_started_is_refused(tmp_path: Path) -> None:
    acquisition, outcome = _acquire(tmp_path, _utc("2026-09-10T12:00:00Z"), _D2)

    assert isinstance(outcome, FuturesTradingSessionInProgressError)
    _assert_untouched(acquisition)


def test_the_trading_date_alone_never_implies_completion(tmp_path: Path) -> None:
    # Later civil date in Asia, still before the absolute close.
    tokyo = timezone(timedelta(hours=9))
    before = datetime(2026, 9, 16, 5, 59, 59, tzinfo=tokyo)
    at_close = datetime(2026, 9, 16, 6, 0, 0, tzinfo=tokyo)
    after = datetime(2026, 9, 16, 6, 0, 1, tzinfo=tokyo)

    for now in (before, at_close):
        acquisition, outcome = _acquire(tmp_path / now.strftime("%H%M%S"), now, _D2)
        assert isinstance(outcome, FuturesTradingSessionInProgressError)
        _assert_untouched(acquisition)
    acquisition, outcome = _acquire(tmp_path / "after", after, _D2)
    assert outcome.daily_bar_count == 1


def test_an_early_close_is_eligible_right_after_its_resolved_close(tmp_path: Path) -> None:
    at_close, after = _utc("2026-11-27T18:15:00Z"), _utc("2026-11-27T18:15:01Z")

    refused, outcome = _acquire(tmp_path / "at", at_close, _EARLY)
    assert isinstance(outcome, FuturesTradingSessionInProgressError)
    _assert_untouched(refused)

    # Well before a normal 21:00Z close would have passed.
    accepted, outcome = _acquire(tmp_path / "after", after, _EARLY)
    assert outcome.daily_bar_count == 1
    assert accepted.rows()[0][0] == "2026-11-27T18:15:00Z"


def test_the_error_carries_the_facts_an_operator_needs_and_no_secret(tmp_path: Path) -> None:
    now = _utc("2026-09-15T14:30:00Z")
    _, error = _acquire(tmp_path, now, _D2)

    assert error.trading_date == date(2026, 9, 15)
    assert error.session_close == PointInTime("2026-09-15T21:00:00Z")
    assert error.current_utc == now
    assert "2026-09-15" in str(error) and "2026-09-15T21:00:00Z" in str(error)
    assert _API_KEY not in str(error) and _API_KEY not in repr(error)
    assert isinstance(error, ValueError)
    assert not isinstance(error, RuntimeError)


# ---------------------------------------------------------------------------
# Ranges and retries
# ---------------------------------------------------------------------------


def test_a_range_keeps_completed_sessions_and_never_stores_the_open_one(tmp_path: Path) -> None:
    in_progress = _session(date(2026, 9, 16), "2026-09-15T22:00:00Z", "2026-09-16T21:00:00Z")
    now = _utc("2026-09-16T15:00:00Z")

    acquisition, outcome = _acquire(tmp_path, now, _D1, _D2, in_progress)

    assert isinstance(outcome, FuturesTradingSessionInProgressError)
    assert outcome.trading_date == date(2026, 9, 16)
    assert [call["start"] for call in acquisition.client.timeseries.bar_calls] == [
        _utc(_D1.opens_at.value),
        _utc(_D2.opens_at.value),
    ]
    assert acquisition.aggregator.calls == 2
    assert acquisition.rows() == [
        ("2026-09-14T21:00:00Z", "7663"),
        ("2026-09-15T21:00:00Z", "7663"),
    ]


def test_a_completed_acquisition_rerun_stays_idempotent(tmp_path: Path) -> None:
    now = _utc("2026-09-20T00:00:00Z")
    first, outcome = _acquire(tmp_path, now, _D1, _D2)
    rows = first.rows()

    second, rerun = _acquire(tmp_path, now + timedelta(days=30), _D1, _D2)

    assert (outcome.daily_bar_count, rerun.daily_bar_count) == (2, 2)
    assert second.rows() == rows


# ---------------------------------------------------------------------------
# Clock handling
# ---------------------------------------------------------------------------


def test_a_naive_clock_is_refused_before_any_provider_request() -> None:
    client = _client(_D2)
    source = DatabentoFuturesHistoricalMarketDataSource(
        _API_KEY, client=client, clock=lambda: datetime(2030, 1, 1)
    )

    with pytest.raises(TypeError, match="aware datetime"):
        source.fetch_session_bars(_ES_DEC, _D2)
    assert client.request_count == 0


def test_the_clock_must_be_callable() -> None:
    with pytest.raises(TypeError, match="clock must be callable"):
        DatabentoFuturesHistoricalMarketDataSource(_API_KEY, client=FakeClient(), clock="now")


def test_the_default_clock_is_utc_wall_time() -> None:
    import northstar_infrastructure.market_data.databento_futures_historical_market_data as module

    before = datetime.now(UTC)
    now = module._utc_now()

    assert now.tzinfo is UTC
    assert before <= now <= datetime.now(UTC)


def test_the_guard_error_is_exported_without_its_helpers() -> None:
    import northstar_infrastructure.market_data as market_data

    assert "FuturesTradingSessionInProgressError" in market_data.__all__
    for private in ("_utc_now", "_require_completed"):
        assert not hasattr(market_data, private)
