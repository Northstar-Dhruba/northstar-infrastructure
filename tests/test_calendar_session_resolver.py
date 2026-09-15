"""Contract and deterministic tests for ExchangeCalendarTradingSessionResolver."""

from __future__ import annotations

from datetime import date

import pytest
from northstar_application.ports import TradingSessionResolutionError
from northstar_core.foundation.value_objects import ExchangeCode, PointInTime

from northstar_infrastructure.exchanges import ExchangeCalendarTradingSessionResolver


def test_resolver_resolves_regular_nasdaq_session_close() -> None:
    resolver = ExchangeCalendarTradingSessionResolver()
    # 2026-09-15 is a regular Tuesday during US EDT (UTC-4), regular close at 16:00 EDT -> 20:00 UTC
    session_date = date(2026, 9, 15)
    result = resolver.resolve_session_close(ExchangeCode("NASDAQ"), session_date)

    assert result is not None
    assert isinstance(result, PointInTime)
    assert result.value == "2026-09-15T20:00:00Z"


def test_resolver_resolves_regular_nyse_session_close() -> None:
    resolver = ExchangeCalendarTradingSessionResolver()
    session_date = date(2026, 9, 15)
    result = resolver.resolve_session_close(ExchangeCode("NYSE"), session_date)

    assert result is not None
    assert result.value == "2026-09-15T20:00:00Z"


def test_resolver_handles_dst_transition_correctly() -> None:
    resolver = ExchangeCalendarTradingSessionResolver()
    # Summer (EDT, UTC-4): 16:00 EDT -> 20:00:00Z
    summer_date = date(2026, 6, 15)
    summer_result = resolver.resolve_session_close(ExchangeCode("NASDAQ"), summer_date)
    assert summer_result is not None
    assert summer_result.value == "2026-06-15T20:00:00Z"

    # Winter (EST, UTC-5): 16:00 EST -> 21:00:00Z
    winter_date = date(2026, 1, 15)
    winter_result = resolver.resolve_session_close(ExchangeCode("NASDAQ"), winter_date)
    assert winter_result is not None
    assert winter_result.value == "2026-01-15T21:00:00Z"


def test_resolver_returns_none_for_weekends() -> None:
    resolver = ExchangeCalendarTradingSessionResolver()
    # 2026-09-12 is Saturday, 2026-09-13 is Sunday
    saturday = date(2026, 9, 12)
    sunday = date(2026, 9, 13)

    assert resolver.resolve_session_close(ExchangeCode("NASDAQ"), saturday) is None
    assert resolver.resolve_session_close(ExchangeCode("NASDAQ"), sunday) is None


def test_resolver_returns_none_for_exchange_holidays() -> None:
    resolver = ExchangeCalendarTradingSessionResolver()
    # 2026-12-25 is Christmas Day
    christmas = date(2026, 12, 25)
    # 2026-01-01 is New Year's Day
    new_years = date(2026, 1, 1)

    assert resolver.resolve_session_close(ExchangeCode("NASDAQ"), christmas) is None
    assert resolver.resolve_session_close(ExchangeCode("NASDAQ"), new_years) is None


def test_resolver_correctly_resolves_early_close_sessions() -> None:
    resolver = ExchangeCalendarTradingSessionResolver()
    # Black Friday (2026-11-27) US equity markets close early at 13:00 EST -> 18:00 UTC
    black_friday = date(2026, 11, 27)

    result = resolver.resolve_session_close(ExchangeCode("NASDAQ"), black_friday)
    assert result is not None
    assert result.value == "2026-11-27T18:00:00Z"


def test_resolver_resolves_international_exchanges() -> None:
    resolver = ExchangeCalendarTradingSessionResolver()
    session_date = date(2026, 9, 15)

    # LSE (London): close at 16:30 BST (UTC+1 during summer) -> 15:30:00Z
    lse_result = resolver.resolve_session_close(ExchangeCode("LSE"), session_date)
    assert lse_result is not None
    assert lse_result.value == "2026-09-15T15:30:00Z"

    # BSE (Bombay): close at 15:30 IST (UTC+5:30) -> 10:00:00Z
    bse_result = resolver.resolve_session_close(ExchangeCode("BSE"), session_date)
    assert bse_result is not None
    assert bse_result.value == "2026-09-15T10:00:00Z"


def test_resolver_raises_error_for_unsupported_exchange() -> None:
    resolver = ExchangeCalendarTradingSessionResolver()
    session_date = date(2026, 9, 15)

    with pytest.raises(
        TradingSessionResolutionError,
        match=r"^Unsupported exchange venue: UNKNOWN\.$",
    ):
        resolver.resolve_session_close(ExchangeCode("UNKNOWN"), session_date)


def test_resolver_raises_sanitized_error_when_internal_calendar_resolution_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resolver = ExchangeCalendarTradingSessionResolver()
    session_date = date(2026, 9, 15)

    # Simulate an unexpected internal calendar engine failure
    def failing_get_calendar(name: str) -> None:
        raise RuntimeError("Internal pandas/calendar library failure with secret path /tmp/foo")

    monkeypatch.setattr(
        "northstar_infrastructure.exchanges.calendar_session_resolver.xcals.get_calendar",
        failing_get_calendar,
    )

    try:
        resolver.resolve_session_close(ExchangeCode("NASDAQ"), session_date)
    except TradingSessionResolutionError as exc:
        message = str(exc)
        assert message == "Trading session resolution failed for exchange NASDAQ on 2026-09-15."
        assert "pandas" not in message
        assert "secret path" not in message
        # Verify exception chaining preserves original exception for debugging
        assert exc.__cause__ is not None
        assert "Internal pandas" in str(exc.__cause__)
    else:
        raise AssertionError("Expected TradingSessionResolutionError")


def test_canonical_exchange_code_aliases_resolve_identically() -> None:
    resolver = ExchangeCalendarTradingSessionResolver()
    session_date = date(2026, 9, 15)

    # NASDAQ and XNAS
    nasdaq_close = resolver.resolve_session_close(ExchangeCode("NASDAQ"), session_date)
    xnas_close = resolver.resolve_session_close(ExchangeCode("XNAS"), session_date)
    assert nasdaq_close == xnas_close == PointInTime("2026-09-15T20:00:00Z")

    # NYSE and XNYS
    nyse_close = resolver.resolve_session_close(ExchangeCode("NYSE"), session_date)
    xnys_close = resolver.resolve_session_close(ExchangeCode("XNYS"), session_date)
    assert nyse_close == xnys_close == PointInTime("2026-09-15T20:00:00Z")

    # BSE and XBOM
    bse_close = resolver.resolve_session_close(ExchangeCode("BSE"), session_date)
    xbom_close = resolver.resolve_session_close(ExchangeCode("XBOM"), session_date)
    assert bse_close == xbom_close == PointInTime("2026-09-15T10:00:00Z")

    # LSE and XLON
    lse_close = resolver.resolve_session_close(ExchangeCode("LSE"), session_date)
    xlon_close = resolver.resolve_session_close(ExchangeCode("XLON"), session_date)
    assert lse_close == xlon_close == PointInTime("2026-09-15T15:30:00Z")


def test_resolver_validates_argument_types() -> None:
    resolver = ExchangeCalendarTradingSessionResolver()

    with pytest.raises(TypeError, match="exchange_code must be an ExchangeCode instance"):
        resolver.resolve_session_close("NASDAQ", date(2026, 9, 15))  # type: ignore[arg-type]

    with pytest.raises(TypeError, match="trading_date must be a datetime.date instance"):
        resolver.resolve_session_close(ExchangeCode("NASDAQ"), "2026-09-15")  # type: ignore[arg-type]
