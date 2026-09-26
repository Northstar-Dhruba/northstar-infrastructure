"""Contract and integration tests for YahooHistoricalMarketDataSource."""

import json
from datetime import UTC, date, datetime
from pathlib import Path
from urllib.error import HTTPError

import pytest
from northstar_application.ports import (
    HistoricalMarketDataQuery,
    TradingSessionResolutionError,
    TradingSessionResolver,
)
from northstar_core.foundation.value_objects import (
    ExchangeCode,
    PointInTime,
    Symbol,
    Timeframe,
)

from northstar_infrastructure.exchanges import ExchangeCalendarTradingSessionResolver
from northstar_infrastructure.market_data import (
    HistoricalMarketDataSourceError,
    SQLiteHistoricalMarketDataRepository,
    SQLiteHistoricalMarketDataStore,
    YahooHistoricalMarketDataSource,
)


class FakeTradingSessionResolver(TradingSessionResolver):
    """Deterministic test double for TradingSessionResolver."""

    def __init__(
        self,
        schedule: dict[tuple[ExchangeCode, date], PointInTime | None] | None = None,
        default_close_hour: int = 20,
    ) -> None:
        self.schedule = schedule or {}
        self.default_close_hour = default_close_hour
        self.calls: list[tuple[ExchangeCode, date]] = []

    def resolve_session_close(
        self, exchange_code: ExchangeCode, trading_date: date
    ) -> PointInTime | None:
        self.calls.append((exchange_code, trading_date))
        if (exchange_code, trading_date) in self.schedule:
            return self.schedule[(exchange_code, trading_date)]
        return PointInTime(f"{trading_date.isoformat()}T{self.default_close_hour:02d}:00:00Z")


class ErrorTradingSessionResolver(TradingSessionResolver):
    """Test double that raises TradingSessionResolutionError."""

    def __init__(self, error_message: str = "Internal calendar lookup failed") -> None:
        self.error_message = error_message

    def resolve_session_close(
        self, exchange_code: ExchangeCode, trading_date: date
    ) -> PointInTime | None:
        raise TradingSessionResolutionError(self.error_message)


def _make_query(
    symbol: str = "AAPL",
    exchange: str = "NASDAQ",
    timeframe: str = "1d",
    start: str = "2026-09-01T20:00:00Z",
    end: str = "2026-09-05T20:00:00Z",
) -> HistoricalMarketDataQuery:
    return HistoricalMarketDataQuery(
        symbol=Symbol(symbol),
        exchange_code=ExchangeCode(exchange),
        timeframe=Timeframe(timeframe),
        start=PointInTime(start),
        end=PointInTime(end),
    )


def _sample_payload(
    timestamps: list[int] | None = None,
    opens: list[float | None] | None = None,
    highs: list[float | None] | None = None,
    lows: list[float | None] | None = None,
    closes: list[float | None] | None = None,
    volumes: list[int | None] | None = None,
    adjcloses: list[float | None] | None = None,
    currency: str = "USD",
    exchange_name: str = "NASDAQ",
    symbol: str = "AAPL",
    include_adjclose: bool = True,
    exchange_timezone: str = "America/New_York",
    gmt_offset: int = -14400,
) -> bytes:
    if timestamps is None:
        # 2026-09-01 to 2026-09-05 in America/New_York (13:30 UTC = 09:30 EDT)
        base_ts = int(datetime(2026, 9, 1, 13, 30, 0, tzinfo=UTC).timestamp())
        timestamps = [base_ts + i * 86400 for i in range(5)]
    count = len(timestamps)
    if opens is None:
        opens = [100.0 + i for i in range(count)]
    if highs is None:
        highs = [105.0 + i for i in range(count)]
    if lows is None:
        lows = [95.0 + i for i in range(count)]
    if closes is None:
        closes = [102.0 + i for i in range(count)]
    if volumes is None:
        volumes = [1000 + i * 100 for i in range(count)]
    if adjcloses is None and include_adjclose:
        adjcloses = [101.5 + i for i in range(count)]

    quote_obj = {
        "open": opens,
        "high": highs,
        "low": lows,
        "close": closes,
        "volume": volumes,
    }
    indicators: dict = {"quote": [quote_obj]}
    if include_adjclose and adjcloses is not None:
        indicators["adjclose"] = [{"adjclose": adjcloses}]

    chart_result = {
        "meta": {
            "currency": currency,
            "symbol": symbol,
            "exchangeName": exchange_name,
            "instrumentType": "EQUITY",
            "exchangeTimezoneName": exchange_timezone,
            "gmtoffset": gmt_offset,
        },
        "timestamp": timestamps,
        "indicators": indicators,
    }

    return json.dumps({"chart": {"result": [chart_result]}}).encode()


def test_fetch_history_constructs_point_in_time_from_session_resolver() -> None:
    # Provider timestamps represent 00:00:00Z session start markers
    payload = _sample_payload()
    resolver = FakeTradingSessionResolver(default_close_hour=20)
    source = YahooHistoricalMarketDataSource(resolver, fetch=lambda url, timeout: payload)
    query = _make_query()

    bars = source.fetch_history(query)

    assert isinstance(bars, tuple)
    assert len(bars) == 5
    # Must use resolved completion instant (20:00:00Z), NEVER provider marker (00:00:00Z)
    assert bars[0].point_in_time.value == "2026-09-01T20:00:00Z"
    assert bars[4].point_in_time.value == "2026-09-05T20:00:00Z"
    assert str(bars[0].open) == "100 USD"
    assert str(bars[0].close) == "102 USD"
    assert bars[0].adjusted_close is not None
    assert str(bars[0].adjusted_close) == "101.5 USD"


def test_fetch_history_preserves_early_close_completion_instant() -> None:
    payload = _sample_payload()
    exchange = ExchangeCode("NASDAQ")
    early_close_date = date(2026, 9, 3)
    early_close_pit = PointInTime("2026-09-03T18:00:00Z")

    resolver = FakeTradingSessionResolver(
        schedule={(exchange, early_close_date): early_close_pit},
        default_close_hour=20,
    )
    source = YahooHistoricalMarketDataSource(resolver, fetch=lambda url, timeout: payload)
    query = _make_query()

    bars = source.fetch_history(query)

    assert len(bars) == 5
    assert bars[2].point_in_time == early_close_pit
    assert bars[2].point_in_time.value == "2026-09-03T18:00:00Z"


def test_fetch_window_case1_marker_before_query_start_with_matching_completion() -> None:
    # Case 1: Yahoo marker occurs at 00:00:00Z on 2026-09-01. Query start is 2026-09-01T20:00:00Z.
    # Resolved completion matches query.start (20:00:00Z). Bar MUST be included.
    payload = _sample_payload()
    resolver = FakeTradingSessionResolver(default_close_hour=20)
    source = YahooHistoricalMarketDataSource(resolver, fetch=lambda url, timeout: payload)
    query = _make_query(start="2026-09-01T20:00:00Z", end="2026-09-01T20:00:00Z")

    bars = source.fetch_history(query)

    assert len(bars) == 1
    assert bars[0].point_in_time.value == "2026-09-01T20:00:00Z"


def test_fetch_window_case2_case3_case4_boundary_inclusivity() -> None:
    # Covers:
    # Case 2: resolved completion == query.end (2026-09-03T20:00:00Z) -> returned
    # Case 3: resolved completion < query.start (2026-08-31T20:00:00Z) -> excluded
    # Case 4: resolved completion > query.end (2026-09-04T20:00:00Z) -> excluded
    base_ts = int(datetime(2026, 8, 31, 13, 30, 0, tzinfo=UTC).timestamp())
    timestamps = [base_ts + i * 86400 for i in range(5)]  # Aug 31, Sep 1, Sep 2, Sep 3, Sep 4
    payload = _sample_payload(timestamps=timestamps)

    resolver = FakeTradingSessionResolver(default_close_hour=20)
    source = YahooHistoricalMarketDataSource(resolver, fetch=lambda url, timeout: payload)
    query = _make_query(start="2026-09-01T20:00:00Z", end="2026-09-03T20:00:00Z")

    bars = source.fetch_history(query)

    assert len(bars) == 3
    assert [b.point_in_time.value for b in bars] == [
        "2026-09-01T20:00:00Z",
        "2026-09-02T20:00:00Z",
        "2026-09-03T20:00:00Z",
    ]


def test_fetch_history_empty_result_on_no_data() -> None:
    payload = json.dumps({"chart": {"result": []}}).encode()
    resolver = FakeTradingSessionResolver()
    source = YahooHistoricalMarketDataSource(resolver, fetch=lambda url, timeout: payload)
    query = _make_query()

    bars = source.fetch_history(query)

    assert bars == ()


def test_fetch_history_missing_adjusted_close_indicator_maps_to_none() -> None:
    payload = _sample_payload(include_adjclose=False)
    resolver = FakeTradingSessionResolver()
    source = YahooHistoricalMarketDataSource(resolver, fetch=lambda url, timeout: payload)
    query = _make_query()

    bars = source.fetch_history(query)

    assert len(bars) == 5
    assert all(b.adjusted_close is None for b in bars)


def test_fetch_history_currency_mapping_and_preservation() -> None:
    payload = _sample_payload(currency="EUR")
    resolver = FakeTradingSessionResolver()
    source = YahooHistoricalMarketDataSource(resolver, fetch=lambda url, timeout: payload)
    query = _make_query()

    bars = source.fetch_history(query)

    assert bars[0].open.currency.value == "EUR"
    assert bars[0].close.currency.value == "EUR"
    assert bars[0].adjusted_close is not None
    assert bars[0].adjusted_close.currency.value == "EUR"


def test_fetch_history_skips_absent_nontrading_entries() -> None:
    base_ts = int(datetime(2026, 9, 1, 13, 30, 0, tzinfo=UTC).timestamp())
    timestamps = [base_ts, base_ts + 86400, base_ts + 2 * 86400]
    opens = [100.0, None, 102.0]
    highs = [105.0, None, 107.0]
    lows = [95.0, None, 97.0]
    closes = [102.0, None, 104.0]
    volumes = [1000, None, 1200]
    adjcloses = [101.5, None, 103.5]

    payload = _sample_payload(
        timestamps=timestamps,
        opens=opens,
        highs=highs,
        lows=lows,
        closes=closes,
        volumes=volumes,
        adjcloses=adjcloses,
    )
    resolver = FakeTradingSessionResolver()
    source = YahooHistoricalMarketDataSource(resolver, fetch=lambda url, timeout: payload)
    query = _make_query(start="2026-09-01T20:00:00Z", end="2026-09-03T20:00:00Z")

    bars = source.fetch_history(query)

    assert len(bars) == 2
    assert [b.point_in_time.value for b in bars] == [
        "2026-09-01T20:00:00Z",
        "2026-09-03T20:00:00Z",
    ]


def test_fetch_history_rejects_completed_bar_when_resolver_returns_none() -> None:
    payload = _sample_payload()
    exchange = ExchangeCode("NASDAQ")
    target_date = date(2026, 9, 2)

    # Resolver indicates no trading session on a date where Yahoo has a completed bar
    resolver = FakeTradingSessionResolver(
        schedule={(exchange, target_date): None},
        default_close_hour=20,
    )
    source = YahooHistoricalMarketDataSource(resolver, fetch=lambda url, timeout: payload)
    query = _make_query()

    with pytest.raises(
        HistoricalMarketDataSourceError,
        match=r"Trading session data is inconsistent for exchange NASDAQ on 2026-09-02\.",
    ):
        source.fetch_history(query)


def test_fetch_history_translates_trading_session_resolution_error() -> None:
    payload = _sample_payload()
    resolver = ErrorTradingSessionResolver()
    source = YahooHistoricalMarketDataSource(resolver, fetch=lambda url, timeout: payload)
    query = _make_query()

    with pytest.raises(
        HistoricalMarketDataSourceError,
        match=r"Failed to resolve trading session for exchange NASDAQ on 2026-09-01\.",
    ):
        source.fetch_history(query)


@pytest.mark.parametrize("provider_exchange", ("NMS", "NYQ", "BOM", "LON"))
def test_fetch_history_rejects_unproven_provider_exchange_identifiers(
    provider_exchange: str,
) -> None:
    payload = _sample_payload(exchange_name=provider_exchange)
    resolver = FakeTradingSessionResolver()
    source = YahooHistoricalMarketDataSource(resolver, fetch=lambda url, timeout: payload)

    with pytest.raises(
        HistoricalMarketDataSourceError,
        match=rf"Yahoo Finance returned exchange {provider_exchange}, "
        r"expected venue matching NASDAQ\.",
    ):
        source.fetch_history(_make_query())


def test_fetch_history_rejects_exchange_mismatch() -> None:
    payload = _sample_payload(exchange_name="NYSE")
    resolver = FakeTradingSessionResolver()
    source = YahooHistoricalMarketDataSource(resolver, fetch=lambda url, timeout: payload)
    query = _make_query(exchange="NASDAQ")

    with pytest.raises(
        HistoricalMarketDataSourceError,
        match=r"Yahoo Finance returned exchange NYSE, expected venue matching NASDAQ\.",
    ):
        source.fetch_history(query)


def test_fetch_history_rejects_unknown_exchange_in_query() -> None:
    resolver = FakeTradingSessionResolver()
    source = YahooHistoricalMarketDataSource(resolver)
    query = _make_query(exchange="UNKNOWN")

    with pytest.raises(
        HistoricalMarketDataSourceError,
        match=r"Unsupported exchange venue for Yahoo Finance historical data: UNKNOWN\.",
    ):
        source.fetch_history(query)


def test_fetch_history_rejects_missing_exchange_metadata() -> None:
    payload = _sample_payload(exchange_name="")
    resolver = FakeTradingSessionResolver()
    source = YahooHistoricalMarketDataSource(resolver, fetch=lambda url, timeout: payload)
    query = _make_query()

    with pytest.raises(HistoricalMarketDataSourceError, match="incomplete exchange metadata"):
        source.fetch_history(query)


def test_fetch_history_rejects_symbol_mismatch() -> None:
    payload = _sample_payload(symbol="AAPL")
    resolver = FakeTradingSessionResolver()
    source = YahooHistoricalMarketDataSource(resolver, fetch=lambda url, timeout: payload)
    query = _make_query(symbol="MSFT")

    with pytest.raises(
        HistoricalMarketDataSourceError,
        match=r"Yahoo Finance returned data for symbol AAPL, expected MSFT\.",
    ):
        source.fetch_history(query)


def test_fetch_history_rejects_incomplete_bar_data() -> None:
    base_ts = int(datetime(2026, 9, 1, 13, 30, 0, tzinfo=UTC).timestamp())
    timestamps = [base_ts]
    # open present, close missing
    payload = _sample_payload(
        timestamps=timestamps,
        opens=[100.0],
        highs=[105.0],
        lows=[95.0],
        closes=[None],
        volumes=[1000],
    )
    resolver = FakeTradingSessionResolver()
    source = YahooHistoricalMarketDataSource(resolver, fetch=lambda url, timeout: payload)

    with pytest.raises(HistoricalMarketDataSourceError, match="incomplete bar observations"):
        source.fetch_history(_make_query())


def test_fetch_history_rejects_invalid_timestamp() -> None:
    payload = _sample_payload(timestamps=["invalid_timestamp"])  # type: ignore[list-item]
    resolver = FakeTradingSessionResolver()
    source = YahooHistoricalMarketDataSource(resolver, fetch=lambda url, timeout: payload)

    with pytest.raises(HistoricalMarketDataSourceError, match="invalid observation timestamp"):
        source.fetch_history(_make_query())


def test_fetch_history_rejects_invalid_numeric_data() -> None:
    base_ts = int(datetime(2026, 9, 1, 13, 30, 0, tzinfo=UTC).timestamp())
    payload = _sample_payload(
        timestamps=[base_ts],
        opens=[100.0],
        highs=["not_a_number"],  # type: ignore[list-item]
        lows=[95.0],
        closes=[102.0],
        volumes=[1000],
    )
    resolver = FakeTradingSessionResolver()
    source = YahooHistoricalMarketDataSource(resolver, fetch=lambda url, timeout: payload)

    with pytest.raises(HistoricalMarketDataSourceError, match="invalid high data"):
        source.fetch_history(_make_query())


def test_fetch_history_rejects_invalid_currency() -> None:
    payload = _sample_payload(currency="INVALID_CURRENCY")
    resolver = FakeTradingSessionResolver()
    source = YahooHistoricalMarketDataSource(resolver, fetch=lambda url, timeout: payload)

    with pytest.raises(HistoricalMarketDataSourceError, match="invalid currency metadata"):
        source.fetch_history(_make_query())


def test_fetch_history_maps_provider_transport_failure() -> None:
    def fetch(url: str, timeout: float) -> bytes:
        raise HTTPError(url, 503, "Service Unavailable", {}, None)

    resolver = FakeTradingSessionResolver()
    source = YahooHistoricalMarketDataSource(resolver, fetch=fetch)

    with pytest.raises(HistoricalMarketDataSourceError, match="provider is unavailable"):
        source.fetch_history(_make_query())


def test_fetch_history_rejects_unsupported_timeframe() -> None:
    resolver = FakeTradingSessionResolver()
    source = YahooHistoricalMarketDataSource(resolver)
    query = _make_query(timeframe="1h")

    with pytest.raises(HistoricalMarketDataSourceError, match="Unsupported timeframe: 1h"):
        source.fetch_history(query)


def test_constructor_validates_session_resolver() -> None:
    with pytest.raises(TypeError, match="session_resolver cannot be None"):
        YahooHistoricalMarketDataSource(None)  # type: ignore[arg-type]

    with pytest.raises(TypeError, match="must be a TradingSessionResolver instance"):
        YahooHistoricalMarketDataSource("invalid_resolver")  # type: ignore[arg-type]


def test_source_and_store_end_to_end_replay_integration(tmp_path: Path) -> None:
    """Integration: Yahoo Source -> Canonical Bars -> SQLite Store -> SQLite Replay."""
    payload = _sample_payload()
    resolver = FakeTradingSessionResolver(default_close_hour=20)
    source = YahooHistoricalMarketDataSource(resolver, fetch=lambda url, timeout: payload)
    query = _make_query(start="2026-09-01T20:00:00Z", end="2026-09-05T20:00:00Z")

    # 1. Fetch from source
    bars = source.fetch_history(query)
    assert len(bars) == 5

    # 2. Store to SQLite
    db_path = tmp_path / "replay.sqlite"
    store = SQLiteHistoricalMarketDataStore(db_path)
    stored_count = store.store(bars)
    assert stored_count == 5

    # 3. Retrieve from SQLite Repository
    repository = SQLiteHistoricalMarketDataRepository(db_path)
    replayed = repository.get_history(query)

    assert len(replayed) == 5
    assert replayed == bars
    for original, loaded in zip(bars, replayed, strict=True):
        assert original.symbol == loaded.symbol
        assert original.exchange_code == loaded.exchange_code
        assert original.point_in_time == loaded.point_in_time
        # Replayed point_in_time must match resolved session completion, NOT 00:00:00Z
        assert original.point_in_time.value.endswith("T20:00:00Z")
        assert original.timeframe == loaded.timeframe
        assert original.open == loaded.open
        assert original.high == loaded.high
        assert original.low == loaded.low
        assert original.close == loaded.close
        assert original.volume == loaded.volume
        assert original.adjusted_close == loaded.adjusted_close


def test_source_with_concrete_exchange_calendar_resolver_end_to_end(tmp_path: Path) -> None:
    """Integration: Yahoo Source with real ExchangeCalendarTradingSessionResolver."""
    # Generate payload for 4 trading days: Tue 2026-09-01 through Fri 2026-09-04
    base_ts = int(datetime(2026, 9, 1, 13, 30, 0, tzinfo=UTC).timestamp())
    timestamps = [base_ts + i * 86400 for i in range(4)]
    payload = _sample_payload(timestamps=timestamps)

    resolver = ExchangeCalendarTradingSessionResolver()
    source = YahooHistoricalMarketDataSource(resolver, fetch=lambda url, timeout: payload)
    query = _make_query(start="2026-09-01T20:00:00Z", end="2026-09-04T20:00:00Z")

    bars = source.fetch_history(query)
    assert len(bars) == 4
    assert [b.point_in_time.value for b in bars] == [
        "2026-09-01T20:00:00Z",
        "2026-09-02T20:00:00Z",
        "2026-09-03T20:00:00Z",
        "2026-09-04T20:00:00Z",
    ]

    db_path = tmp_path / "real_resolver.sqlite"
    store = SQLiteHistoricalMarketDataStore(db_path)
    store.store(bars)

    repository = SQLiteHistoricalMarketDataRepository(db_path)
    replayed = repository.get_history(query)
    assert len(replayed) == 4
    assert replayed == bars
