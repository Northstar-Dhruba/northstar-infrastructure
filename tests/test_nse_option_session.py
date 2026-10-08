"""Tests for the NSE option session reference and resolver.

Real-date cases use the production NSE F&O calendar and the cited NIFTY option
regime. Fail-closed paths the real data cannot reach use clearly synthetic,
constructor-injected facts; no real holiday or special session is invented.
"""

from __future__ import annotations

import ast
import socket
from dataclasses import fields, replace
from datetime import date, datetime, time
from pathlib import Path

import pytest
from northstar_application.ports import (
    OptionTradingSession,
    OptionTradingSessionResolutionError,
    OptionTradingSessionResolver,
)
from northstar_core.foundation.value_objects import ExchangeCode, PointInTime, Symbol
from northstar_core.futures import FuturesProductReference
from northstar_core.options import OptionProductReference

import northstar_infrastructure.market_data as market_data
import northstar_infrastructure.market_data.nse_option_session as module
from northstar_infrastructure.market_data import NSEOptionTradingSessionResolver
from northstar_infrastructure.market_data import nse_futures_calendar_reference as calendar
from northstar_infrastructure.market_data import nse_option_session_reference as reference
from northstar_infrastructure.market_data.nse_option_session_reference import (
    OptionSessionRegime,
)

_NIFTY = OptionProductReference(Symbol("NIFTY"), ExchangeCode("NSE"))
_REGIME = reference.REGIMES[0]


@pytest.fixture
def resolver() -> NSEOptionTradingSessionResolver:
    return NSEOptionTradingSessionResolver()


def _synthetic(**overrides) -> NSEOptionTradingSessionResolver:
    facts = {
        "holidays": calendar.HOLIDAYS,
        "special_sessions": calendar.SPECIAL_SESSIONS,
        "loaded_years": calendar.LOADED_YEARS,
        "regimes": reference.REGIMES,
    }
    facts.update(overrides)
    return NSEOptionTradingSessionResolver(**facts)


def _ist(day: date, clock: str) -> PointInTime:
    return PointInTime(f"{day.isoformat()}T{clock}:00+05:30")


def _expected(day: date) -> OptionTradingSession:
    return OptionTradingSession(day, _ist(day, "09:15"), _ist(day, "15:40"))


# ---------------------------------------------------------------------------
# The reference
# ---------------------------------------------------------------------------


def test_exactly_the_current_nifty_regime_is_encoded() -> None:
    assert reference.REGIMES == (
        OptionSessionRegime(
            product_code="NIFTY",
            exchange_code="NSE",
            effective_from=date(2026, 8, 3),
            opens=time(9, 15),
            closes=time(15, 40),
            source=(
                "NSE Equity Derivatives market timings (Normal Market 09:15-15:40); "
                "NSE/FAOP/74467 (2026-05-29): close 15:40 effective 2026-08-03"
            ),
        ),
    )


def test_the_regime_has_no_pre_open_or_listing_field() -> None:
    assert [field.name for field in fields(OptionSessionRegime)] == [
        "product_code",
        "exchange_code",
        "effective_from",
        "opens",
        "closes",
        "source",
    ]


def test_the_option_open_is_not_the_futures_pre_open() -> None:
    futures_from_2026_08 = [r for r in calendar.REGIMES if r.effective_from == date(2026, 8, 3)]
    assert futures_from_2026_08 and futures_from_2026_08[0].opens == time(9, 0)
    assert _REGIME.opens == time(9, 15)


@pytest.mark.parametrize(
    ("regimes", "message"),
    [
        ((), "no option session regime"),
        ((replace(_REGIME, source=""),), "no source"),
        ((replace(_REGIME, product_code="nifty"),), "not canonical"),
        ((replace(_REGIME, opens=time(15, 40)),), "does not open before it closes"),
        ((replace(_REGIME, effective_from=datetime(2026, 8, 3)),), "plain date"),
        ((_REGIME, _REGIME), "strictly ascending"),
        (("09:15-15:40",), "OptionSessionRegime"),
    ],
)
def test_invalid_regime_data_is_refused(regimes: tuple, message: str) -> None:
    with pytest.raises(OptionTradingSessionResolutionError, match=message):
        reference.validate(regimes)


# ---------------------------------------------------------------------------
# Normal sessions
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("day", [date(2026, 10, 8), date(2026, 10, 9), date(2026, 8, 3)])
def test_a_normal_trading_day_is_a_0915_to_1540_ist_session(
    resolver: NSEOptionTradingSessionResolver, day: date
) -> None:
    (session,) = resolver.sessions_in_range(_NIFTY, day, day)

    assert session == _expected(day)
    assert session.closes_at == PointInTime(f"{day.isoformat()}T10:10:00Z")
    assert session.opens_at == PointInTime(f"{day.isoformat()}T03:45:00Z")


def test_a_week_skips_the_weekend_in_order(resolver: NSEOptionTradingSessionResolver) -> None:
    sessions = resolver.sessions_in_range(_NIFTY, date(2026, 10, 8), date(2026, 10, 13))

    assert [s.trading_date for s in sessions] == [
        date(2026, 10, 8),
        date(2026, 10, 9),
        date(2026, 10, 12),
        date(2026, 10, 13),
    ]


@pytest.mark.parametrize("day", [date(2026, 10, 10), date(2026, 10, 11)])
def test_a_weekend_is_not_a_session(resolver: NSEOptionTradingSessionResolver, day: date) -> None:
    assert resolver.sessions_in_range(_NIFTY, day, day) == ()


@pytest.mark.parametrize(
    "holiday", [date(2026, 10, 2), date(2026, 10, 20), date(2026, 11, 24), date(2026, 12, 25)]
)
def test_a_published_holiday_is_not_a_session(
    resolver: NSEOptionTradingSessionResolver, holiday: date
) -> None:
    assert holiday in calendar.HOLIDAYS
    assert resolver.sessions_in_range(_NIFTY, holiday, holiday) == ()


# ---------------------------------------------------------------------------
# Fail closed
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("start", "end"),
    [
        (date(2026, 7, 31), date(2026, 7, 31)),
        (date(2026, 8, 1), date(2026, 8, 3)),
        (date(2025, 10, 1), date(2025, 10, 31)),
    ],
    ids=["friday-before", "weekend-before", "a-year-before"],
)
def test_a_date_before_the_option_regime_fails_closed(
    resolver: NSEOptionTradingSessionResolver, start: date, end: date
) -> None:
    with pytest.raises(OptionTradingSessionResolutionError, match="effective from 2026-08-03"):
        resolver.sessions_in_range(_NIFTY, start, end)


def test_an_unloaded_year_fails_closed_before_any_answer(
    resolver: NSEOptionTradingSessionResolver,
) -> None:
    with pytest.raises(OptionTradingSessionResolutionError, match="year 2027 is not loaded"):
        resolver.sessions_in_range(_NIFTY, date(2026, 12, 28), date(2027, 1, 4))


def test_the_unresolved_muhurat_session_fails_closed(
    resolver: NSEOptionTradingSessionResolver,
) -> None:
    assert calendar.SPECIAL_SESSIONS[date(2026, 11, 8)].opens is None

    with pytest.raises(OptionTradingSessionResolutionError, match="2026-11-08 is an NSE special"):
        resolver.sessions_in_range(_NIFTY, date(2026, 11, 2), date(2026, 11, 13))


def test_a_range_not_touching_the_muhurat_is_unaffected(
    resolver: NSEOptionTradingSessionResolver,
) -> None:
    sessions = resolver.sessions_in_range(_NIFTY, date(2026, 11, 9), date(2026, 11, 13))

    assert [s.trading_date for s in sessions] == [
        date(2026, 11, 9),
        date(2026, 11, 11),
        date(2026, 11, 12),
        date(2026, 11, 13),
    ]


def test_a_special_session_on_a_weekday_fails_closed_even_with_timings() -> None:
    """Synthetic: a special session on regular Wednesday 2026-10-14."""
    resolver = _synthetic(special_sessions={*calendar.SPECIAL_SESSIONS, date(2026, 10, 14)})

    with pytest.raises(OptionTradingSessionResolutionError, match="2026-10-14 is an NSE special"):
        resolver.sessions_in_range(_NIFTY, date(2026, 10, 12), date(2026, 10, 16))


def test_a_range_is_never_partly_answered() -> None:
    """Synthetic special session at the end of an otherwise good range."""
    resolver = _synthetic(special_sessions={date(2026, 10, 16)})

    with pytest.raises(OptionTradingSessionResolutionError):
        resolver.sessions_in_range(_NIFTY, date(2026, 10, 12), date(2026, 10, 16))


@pytest.mark.parametrize(
    "product",
    [
        OptionProductReference(Symbol("BANKNIFTY"), ExchangeCode("NSE")),
        OptionProductReference(Symbol("NIFTY"), ExchangeCode("BSE")),
    ],
)
def test_unsupported_products_fail_closed(
    resolver: NSEOptionTradingSessionResolver, product: OptionProductReference
) -> None:
    with pytest.raises(OptionTradingSessionResolutionError, match="Unsupported option product"):
        resolver.sessions_in_range(product, date(2026, 10, 8), date(2026, 10, 8))


@pytest.mark.parametrize(
    "product", [FuturesProductReference(Symbol("NIFTY"), ExchangeCode("NSE")), "NIFTY@NSE"]
)
def test_a_non_option_product_is_rejected(resolver, product) -> None:
    with pytest.raises(TypeError, match="OptionProductReference"):
        resolver.sessions_in_range(product, date(2026, 10, 8), date(2026, 10, 8))


@pytest.mark.parametrize(
    ("start", "end"),
    [(datetime(2026, 10, 8), date(2026, 10, 9)), ("2026-10-08", date(2026, 10, 9))],
)
def test_non_dates_are_rejected(resolver, start, end) -> None:
    with pytest.raises(TypeError, match="plain date"):
        resolver.sessions_in_range(_NIFTY, start, end)


def test_a_reversed_range_fails(resolver: NSEOptionTradingSessionResolver) -> None:
    with pytest.raises(OptionTradingSessionResolutionError, match="is after"):
        resolver.sessions_in_range(_NIFTY, date(2026, 10, 9), date(2026, 10, 8))


# ---------------------------------------------------------------------------
# Construction and boundaries
# ---------------------------------------------------------------------------


def test_the_resolver_implements_the_port_and_is_exported(resolver) -> None:
    assert isinstance(resolver, OptionTradingSessionResolver)
    assert "NSEOptionTradingSessionResolver" in market_data.__all__


@pytest.mark.parametrize(
    ("name", "value"),
    [("holidays", {"2026-10-20"}), ("special_sessions", {1}), ("loaded_years", {"2026"})],
)
def test_injected_facts_are_type_checked(name: str, value: object) -> None:
    with pytest.raises(TypeError):
        _synthetic(**{name: value})


def _tree(source) -> ast.Module:
    return ast.parse(Path(source.__file__).read_text(encoding="utf-8"))


def test_only_day_level_calendar_facts_are_read() -> None:
    read = {
        node.attr
        for node in ast.walk(_tree(module))
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "calendar_reference"
    }

    assert read == {"HOLIDAYS", "SPECIAL_SESSIONS", "LOADED_YEARS"}


@pytest.mark.parametrize("source", [module, reference], ids=["resolver", "reference"])
def test_no_futures_session_clock_network_or_provider_dependency(source) -> None:
    tree = _tree(source)
    modules = {n.module for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module} | {
        a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names
    }
    names = {a.name for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) for a in n.names}

    assert "nse_futures_session" not in " ".join(modules)
    assert not [name for name in names if name.startswith("Futures")]
    assert "REGIMES" not in names
    for name in modules:
        assert not name.startswith("northstar_core.futures")
        assert name.split(".")[0] not in {"time", "socket", "requests", "httpx", "urllib"}
        assert "upstox" not in name
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            assert node.func.attr not in {"now", "utcnow", "today", "monotonic"}


def test_resolution_uses_no_network(resolver, monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(*args, **kwargs):
        raise AssertionError("network access attempted")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)

    assert len(resolver.sessions_in_range(_NIFTY, date(2026, 10, 5), date(2026, 10, 9))) == 5


@pytest.mark.parametrize("zone", ["UTC", "Asia/Kolkata", "America/Los_Angeles"])
def test_resolution_is_independent_of_the_host_timezone(
    monkeypatch: pytest.MonkeyPatch, zone: str
) -> None:
    monkeypatch.setenv("TZ", zone)

    (session,) = NSEOptionTradingSessionResolver().sessions_in_range(
        _NIFTY, date(2026, 10, 8), date(2026, 10, 8)
    )
    assert session.closes_at == PointInTime("2026-10-08T10:10:00Z")
