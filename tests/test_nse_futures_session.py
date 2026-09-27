"""Tests for the NSE futures trading-session resolver and its reference data.

Every expected instant is a literal UTC PointInTime established from the NSE
circulars cited in ``nse_futures_calendar_reference``, never recomputed from the
resolver's own rules, so a changed regime date or time fails here rather than
quietly re-stamping every session.
"""

from __future__ import annotations

import ast
import socket
import time as time_module
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
from northstar_application.ports import (
    FuturesSessionResolutionError,
    FuturesTradingSession,
    FuturesTradingSessionResolver,
)
from northstar_core.derivatives import ExpirationDate
from northstar_core.foundation.value_objects import ExchangeCode, PointInTime, Symbol
from northstar_core.futures import FuturesContract, FuturesProductReference

import northstar_infrastructure.market_data as market_data
import northstar_infrastructure.market_data.nse_futures_calendar_reference as reference
import northstar_infrastructure.market_data.nse_futures_session as module
from northstar_infrastructure.market_data import (
    ExchangeCalendarFuturesTradingSessionResolver,
    NSEFuturesTradingSessionResolver,
)
from northstar_infrastructure.market_data.nse_futures_calendar_reference import (
    SessionRegime,
    SpecialSession,
    TradingHoliday,
    validate,
)

_NIFTY = FuturesProductReference(Symbol("NIFTY"), ExchangeCode("NSE"))
_BANKNIFTY = FuturesProductReference(Symbol("BANKNIFTY"), ExchangeCode("NSE"))
_ES = FuturesProductReference(Symbol("ES"), ExchangeCode("CME"))
_IST = ZoneInfo("Asia/Kolkata")


@pytest.fixture
def resolver() -> NSEFuturesTradingSessionResolver:
    return NSEFuturesTradingSessionResolver()


def _session(label: date, opens: str, closes: str) -> FuturesTradingSession:
    return FuturesTradingSession(label, PointInTime(opens), PointInTime(closes))


# ---------------------------------------------------------------------------
# Session regimes: load-bearing transition dates
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("label", "opens", "closes"),
    [
        # A. Last date before the futures pre-open: 09:15-15:30 IST.
        (date(2025, 12, 5), "2025-12-05T03:45:00Z", "2025-12-05T10:00:00Z"),
        # B. First pre-open date: 09:00-15:30 IST.
        (date(2025, 12, 8), "2025-12-08T03:30:00Z", "2025-12-08T10:00:00Z"),
        # C. Last date before the 15:40 close: 09:00-15:30 IST.
        (date(2026, 7, 31), "2026-07-31T03:30:00Z", "2026-07-31T10:00:00Z"),
        # D. First 15:40 close: 09:00-15:40 IST.
        (date(2026, 8, 3), "2026-08-03T03:30:00Z", "2026-08-03T10:10:00Z"),
    ],
    ids=["A-2025-12-05", "B-2025-12-08", "C-2026-07-31", "D-2026-08-03"],
)
def test_regime_boundaries_are_pinned(
    resolver: NSEFuturesTradingSessionResolver, label: date, opens: str, closes: str
) -> None:
    assert resolver.resolve(_NIFTY, label) == _session(label, opens, closes)


def test_the_first_loaded_regime_opens_at_0915_and_closes_at_1530(
    resolver: NSEFuturesTradingSessionResolver,
) -> None:
    assert resolver.resolve(_NIFTY, date(2024, 1, 2)) == _session(
        date(2024, 1, 2), "2024-01-02T03:45:00Z", "2024-01-02T10:00:00Z"
    )


def test_the_september_2026_pre_open_revision_keeps_the_0900_start(
    resolver: NSEFuturesTradingSessionResolver,
) -> None:
    assert resolver.resolve(_NIFTY, date(2026, 9, 25)) == _session(
        date(2026, 9, 25), "2026-09-25T03:30:00Z", "2026-09-25T10:10:00Z"
    )


def test_the_regimes_are_exactly_the_three_published_ones() -> None:
    assert [
        (r.effective_from, r.opens.isoformat(), r.closes.isoformat()) for r in reference.REGIMES
    ] == [
        (date(2024, 1, 1), "09:15:00", "15:30:00"),
        (date(2025, 12, 8), "09:00:00", "15:30:00"),
        (date(2026, 8, 3), "09:00:00", "15:40:00"),
    ]


def test_the_daily_bar_point_in_time_is_the_session_close(
    resolver: NSEFuturesTradingSessionResolver,
) -> None:
    session = resolver.resolve(_NIFTY, date(2026, 9, 25))

    assert session.closes_at == PointInTime("2026-09-25T15:40:00+05:30")
    assert session.opens_at.compare(session.closes_at) < 0


def test_every_session_label_is_its_ist_civil_date(
    resolver: NSEFuturesTradingSessionResolver,
) -> None:
    for session in resolver.sessions_in_range(_NIFTY, date(2024, 1, 1), date(2026, 11, 7)):
        for instant in (session.opens_at, session.closes_at):
            utc = datetime.fromisoformat(instant.value.replace("Z", "+00:00"))
            local = utc.astimezone(_IST)
            assert local.date() == session.trading_date
            assert local.utcoffset() == timedelta(hours=5, minutes=30)


# ---------------------------------------------------------------------------
# Holidays
# ---------------------------------------------------------------------------

_HOLIDAYS_2026 = [
    date(2026, 1, 15),
    date(2026, 1, 26),
    date(2026, 3, 3),
    date(2026, 3, 26),
    date(2026, 3, 31),
    date(2026, 4, 3),
    date(2026, 4, 14),
    date(2026, 5, 1),
    date(2026, 5, 28),
    date(2026, 6, 26),
    date(2026, 9, 14),
    date(2026, 10, 2),
    date(2026, 10, 20),
    date(2026, 11, 10),
    date(2026, 11, 24),
    date(2026, 12, 25),
]


@pytest.mark.parametrize("holiday", _HOLIDAYS_2026, ids=lambda d: d.isoformat())
def test_every_2026_weekday_holiday_has_no_session(
    resolver: NSEFuturesTradingSessionResolver, holiday: date
) -> None:
    assert holiday.weekday() < 5
    assert resolver.resolve(_NIFTY, holiday) is None


def test_the_2026_holidays_are_exactly_the_annual_list_plus_its_amendment() -> None:
    loaded = sorted(day for day in reference.HOLIDAYS if day.year == 2026)

    assert loaded == _HOLIDAYS_2026


@pytest.mark.parametrize(
    ("holiday", "source"),
    [
        (date(2026, 1, 15), "NSE/FAOP/72262"),
        (date(2024, 1, 22), "NSE/FAOP/60337"),
        (date(2024, 5, 20), "NSE/FAOP/61517"),
        (date(2024, 11, 20), "NSE/FAOP/64959"),
    ],
    ids=lambda value: value.isoformat() if isinstance(value, date) else value,
)
def test_holidays_added_by_amendment_are_closed_and_cite_the_amendment(
    resolver: NSEFuturesTradingSessionResolver, holiday: date, source: str
) -> None:
    (entry,) = [h for h in reference.holidays() if h.day == holiday]

    assert entry.source == source
    assert resolver.resolve(_NIFTY, holiday) is None


@pytest.mark.parametrize(
    "holiday",
    [date(2024, 3, 8), date(2024, 8, 15), date(2025, 2, 26), date(2025, 8, 27), date(2025, 12, 25)],
    ids=lambda d: d.isoformat(),
)
def test_earlier_years_holidays_have_no_session(
    resolver: NSEFuturesTradingSessionResolver, holiday: date
) -> None:
    assert resolver.resolve(_NIFTY, holiday) is None


def test_the_weekdays_around_a_holiday_are_sessions(
    resolver: NSEFuturesTradingSessionResolver,
) -> None:
    assert resolver.resolve(_NIFTY, date(2026, 1, 14)) == _session(
        date(2026, 1, 14), "2026-01-14T03:30:00Z", "2026-01-14T10:00:00Z"
    )
    assert resolver.resolve(_NIFTY, date(2026, 1, 15)) is None
    assert resolver.resolve(_NIFTY, date(2026, 1, 16)) == _session(
        date(2026, 1, 16), "2026-01-16T03:30:00Z", "2026-01-16T10:00:00Z"
    )


@pytest.mark.parametrize(
    ("expiry_session", "holiday"),
    [(date(2026, 3, 30), date(2026, 3, 31)), (date(2026, 11, 23), date(2026, 11, 24))],
    ids=["march-2026", "november-2026"],
)
def test_a_last_tuesday_holiday_leaves_the_monday_before_as_the_last_session(
    resolver: NSEFuturesTradingSessionResolver, expiry_session: date, holiday: date
) -> None:
    """The monthly expiry moves to the previous trading day; the calendar must agree."""
    assert holiday.weekday() == 1 and expiry_session.weekday() == 0
    assert resolver.resolve(_NIFTY, holiday) is None
    assert resolver.resolve(_NIFTY, expiry_session) is not None


def test_the_three_day_diwali_week_2026_is_resolved_exactly(
    resolver: NSEFuturesTradingSessionResolver,
) -> None:
    week = resolver.sessions_in_range(_NIFTY, date(2026, 10, 19), date(2026, 10, 23))

    assert [s.trading_date for s in week] == [
        date(2026, 10, 19),
        date(2026, 10, 21),
        date(2026, 10, 22),
        date(2026, 10, 23),
    ]


# ---------------------------------------------------------------------------
# Weekends and special sessions
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "weekend", [date(2026, 9, 26), date(2026, 9, 27), date(2024, 6, 1)], ids=lambda d: d.isoformat()
)
def test_an_ordinary_weekend_has_no_session(
    resolver: NSEFuturesTradingSessionResolver, weekend: date
) -> None:
    assert weekend.weekday() >= 5
    assert resolver.resolve(_NIFTY, weekend) is None


@pytest.mark.parametrize(
    ("label", "opens", "closes"),
    [
        # Sunday Union Budget 2026: pre-open 09:00, normal market to 15:30.
        (date(2026, 2, 1), "2026-02-01T03:30:00Z", "2026-02-01T10:00:00Z"),
        # Saturday Union Budget 2025: standard timings, before the futures pre-open.
        (date(2025, 2, 1), "2025-02-01T03:45:00Z", "2025-02-01T10:00:00Z"),
        # Saturday live session from the primary site, the split plan withdrawn.
        (date(2024, 1, 20), "2024-01-20T03:45:00Z", "2024-01-20T10:00:00Z"),
        # Saturday DR-switchover sessions: 09:15-10:00 and 11:30-12:30 IST.
        (date(2024, 3, 2), "2024-03-02T03:45:00Z", "2024-03-02T07:00:00Z"),
        (date(2024, 5, 18), "2024-05-18T03:45:00Z", "2024-05-18T07:00:00Z"),
        # Muhurat sessions on weekday holidays.
        (date(2024, 11, 1), "2024-11-01T12:30:00Z", "2024-11-01T13:30:00Z"),
        (date(2025, 10, 21), "2025-10-21T08:15:00Z", "2025-10-21T09:15:00Z"),
    ],
    ids=lambda value: value.isoformat() if isinstance(value, date) else None,
)
def test_published_special_sessions_resolve_at_their_notified_times(
    resolver: NSEFuturesTradingSessionResolver, label: date, opens: str, closes: str
) -> None:
    assert resolver.resolve(_NIFTY, label) == _session(label, opens, closes)


def test_the_2026_budget_sunday_is_a_session_although_it_is_a_sunday(
    resolver: NSEFuturesTradingSessionResolver,
) -> None:
    budget = date(2026, 2, 1)

    assert budget.weekday() == 6
    assert resolver.resolve(_NIFTY, budget) is not None
    assert budget in {s.trading_date for s in resolver.sessions_in_range(_NIFTY, budget, budget)}


def test_a_muhurat_session_overrides_its_holiday(
    resolver: NSEFuturesTradingSessionResolver,
) -> None:
    assert date(2025, 10, 21) in reference.HOLIDAYS
    assert resolver.resolve(_NIFTY, date(2025, 10, 21)) is not None
    assert resolver.resolve(_NIFTY, date(2025, 10, 22)) is None


def test_the_unnotified_2026_muhurat_fails_closed(
    resolver: NSEFuturesTradingSessionResolver,
) -> None:
    with pytest.raises(FuturesSessionResolutionError, match="2026-11-08.*no notified timings"):
        resolver.resolve(_NIFTY, date(2026, 11, 8))


def test_a_range_containing_the_unnotified_muhurat_fails_closed(
    resolver: NSEFuturesTradingSessionResolver,
) -> None:
    with pytest.raises(FuturesSessionResolutionError, match="2026-11-08"):
        resolver.sessions_in_range(_NIFTY, date(2026, 11, 2), date(2026, 11, 13))

    before = resolver.sessions_in_range(_NIFTY, date(2026, 11, 2), date(2026, 11, 7))
    assert [s.trading_date for s in before][-1] == date(2026, 11, 6)


def test_the_unnotified_muhurat_is_recorded_without_invented_times() -> None:
    muhurat = reference.SPECIAL_SESSIONS[date(2026, 11, 8)]

    assert (muhurat.opens, muhurat.closes) == (None, None)


# ---------------------------------------------------------------------------
# Loaded years
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("year", "end", "expected"),
    [
        (2024, date(2024, 12, 31), 249),
        (2025, date(2025, 12, 31), 249),
        (2026, date(2026, 11, 7), 210),
    ],
)
def test_each_loaded_year_resolves_its_pinned_session_count(
    resolver: NSEFuturesTradingSessionResolver, year: int, end: date, expected: int
) -> None:
    """Weekdays, minus weekday holidays, plus special sessions on non-weekdays or holidays."""
    assert len(resolver.sessions_in_range(_NIFTY, date(year, 1, 1), end)) == expected


def test_exactly_2024_to_2026_are_loaded() -> None:
    assert reference.LOADED_YEARS == frozenset({2024, 2025, 2026})


@pytest.mark.parametrize(
    "unloaded", [date(2027, 1, 5), date(2023, 12, 29)], ids=lambda d: d.isoformat()
)
def test_an_unloaded_years_weekday_is_never_guessed(
    resolver: NSEFuturesTradingSessionResolver, unloaded: date
) -> None:
    assert unloaded.weekday() < 5
    with pytest.raises(FuturesSessionResolutionError, match=f"year {unloaded.year} is not loaded"):
        resolver.resolve(_NIFTY, unloaded)


def test_an_unloaded_years_weekend_is_not_answered_either(
    resolver: NSEFuturesTradingSessionResolver,
) -> None:
    with pytest.raises(FuturesSessionResolutionError, match="not loaded"):
        resolver.resolve(_NIFTY, date(2027, 1, 2))


def test_a_range_reaching_an_unloaded_year_is_refused_whole(
    resolver: NSEFuturesTradingSessionResolver,
) -> None:
    with pytest.raises(FuturesSessionResolutionError, match="2027 is not loaded"):
        resolver.sessions_in_range(_NIFTY, date(2026, 12, 28), date(2027, 1, 8))


# ---------------------------------------------------------------------------
# Venue guard and inputs
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("venue", ["CME", "BSE", "NFO", "MCX"])
def test_a_non_nse_venue_is_refused(resolver: NSEFuturesTradingSessionResolver, venue: str) -> None:
    product = FuturesProductReference(Symbol("NIFTY"), ExchangeCode(venue))

    with pytest.raises(FuturesSessionResolutionError, match=f"Unsupported futures venue: {venue}"):
        resolver.resolve(product, date(2026, 9, 25))
    with pytest.raises(FuturesSessionResolutionError, match="Unsupported futures venue"):
        resolver.sessions_in_range(product, date(2026, 9, 21), date(2026, 9, 25))


def test_es_at_cme_never_receives_an_nse_session(
    resolver: NSEFuturesTradingSessionResolver,
) -> None:
    with pytest.raises(FuturesSessionResolutionError, match="Supported venues are \\['NSE'\\]"):
        resolver.resolve(_ES, date(2026, 9, 25))


def test_the_cme_resolver_still_refuses_nse() -> None:
    with pytest.raises(FuturesSessionResolutionError, match="Unsupported futures venue: NSE"):
        ExchangeCalendarFuturesTradingSessionResolver().resolve(_NIFTY, date(2026, 9, 25))


def test_every_nse_product_and_expiry_shares_one_session(
    resolver: NSEFuturesTradingSessionResolver,
) -> None:
    october = FuturesContract(_NIFTY, ExpirationDate("2026-10-27"))
    november = FuturesContract(_NIFTY, ExpirationDate("2026-11-23"))
    day = date(2026, 9, 25)

    assert (
        resolver.resolve(october.product, day)
        == resolver.resolve(november.product, day)
        == resolver.resolve(_BANKNIFTY, day)
    )


@pytest.mark.parametrize(
    "product",
    [ExchangeCode("NSE"), "NIFTY@NSE", FuturesContract(_NIFTY, ExpirationDate("2026-10-27")), None],
    ids=["exchange-code", "string", "contract", "none"],
)
def test_the_product_must_be_a_product_reference(
    resolver: NSEFuturesTradingSessionResolver, product: object
) -> None:
    with pytest.raises(TypeError, match="FuturesProductReference"):
        resolver.resolve(product, date(2026, 9, 25))


@pytest.mark.parametrize(
    "value", [datetime(2026, 9, 25, 9, 0), "2026-09-25", None], ids=["datetime", "string", "none"]
)
def test_dates_must_be_plain_dates(resolver: NSEFuturesTradingSessionResolver, value) -> None:
    with pytest.raises(TypeError, match="datetime.date"):
        resolver.resolve(_NIFTY, value)
    with pytest.raises(TypeError, match="datetime.date"):
        resolver.sessions_in_range(_NIFTY, value, date(2026, 9, 25))
    with pytest.raises(TypeError, match="datetime.date"):
        resolver.sessions_in_range(_NIFTY, date(2026, 9, 21), value)


# ---------------------------------------------------------------------------
# Range contract
# ---------------------------------------------------------------------------


def test_a_range_is_inclusive_ascending_and_matches_resolve(
    resolver: NSEFuturesTradingSessionResolver,
) -> None:
    start, end = date(2026, 9, 11), date(2026, 9, 21)

    sessions = resolver.sessions_in_range(_NIFTY, start, end)
    labels = [s.trading_date for s in sessions]

    assert labels == [
        date(2026, 9, 11),
        date(2026, 9, 15),
        date(2026, 9, 16),
        date(2026, 9, 17),
        date(2026, 9, 18),
        date(2026, 9, 21),
    ]
    assert labels == sorted(set(labels))
    for session in sessions:
        assert resolver.resolve(_NIFTY, session.trading_date) == session


def test_a_range_of_only_non_sessions_is_empty(
    resolver: NSEFuturesTradingSessionResolver,
) -> None:
    assert resolver.sessions_in_range(_NIFTY, date(2026, 9, 26), date(2026, 9, 27)) == ()


def test_an_inverted_range_raises(resolver: NSEFuturesTradingSessionResolver) -> None:
    with pytest.raises(FuturesSessionResolutionError, match="is after end"):
        resolver.sessions_in_range(_NIFTY, date(2026, 9, 25), date(2026, 9, 24))


def test_the_resolver_implements_the_application_port() -> None:
    assert issubclass(NSEFuturesTradingSessionResolver, FuturesTradingSessionResolver)


def test_repeated_calls_are_stable(resolver: NSEFuturesTradingSessionResolver) -> None:
    first = resolver.sessions_in_range(_NIFTY, date(2026, 1, 1), date(2026, 3, 31))

    assert (
        NSEFuturesTradingSessionResolver().sessions_in_range(
            _NIFTY, date(2026, 1, 1), date(2026, 3, 31)
        )
        == first
    )


# ---------------------------------------------------------------------------
# Reference data integrity
# ---------------------------------------------------------------------------

_REGIME = SessionRegime(
    date(2024, 1, 1), reference.REGIMES[0].opens, reference.REGIMES[0].closes, "s"
)
_OPEN, _CLOSE = _REGIME.opens, _REGIME.closes


@pytest.mark.parametrize(
    ("regimes", "holidays", "specials", "message"),
    [
        ((_REGIME,), (TradingHoliday(date(2026, 9, 26), "Sat", "s"),), (), "falls on a weekend"),
        (
            (_REGIME,),
            (
                TradingHoliday(date(2026, 9, 25), "a", "s"),
                TradingHoliday(date(2026, 9, 25), "b", "s"),
            ),
            (),
            "listed twice",
        ),
        ((_REGIME,), (TradingHoliday(date(2027, 1, 5), "x", "s"),), (), "outside the loaded years"),
        ((_REGIME,), (), (SpecialSession(date(2026, 2, 1), "x", _OPEN, None, "s"),), "only one"),
        ((_REGIME,), (), (SpecialSession(date(2026, 2, 1), "x", _CLOSE, _OPEN, "s"),), "before"),
        ((_REGIME, _REGIME), (), (), "strictly ascending"),
        (
            (SessionRegime(date(2025, 1, 1), _OPEN, _CLOSE, "s"),),
            (),
            (),
            "before the first session regime",
        ),
    ],
    ids=[
        "weekend-holiday",
        "duplicate-holiday",
        "unloaded-holiday",
        "half-timed-special",
        "inverted-special",
        "unordered-regimes",
        "uncovered-year",
    ],
)
def test_invalid_reference_data_is_refused(regimes, holidays, specials, message: str) -> None:
    with pytest.raises(FuturesSessionResolutionError, match=message):
        validate(regimes, holidays, specials, frozenset({2024, 2025, 2026}))


def test_every_loaded_holiday_and_special_session_cites_an_nse_circular() -> None:
    for entry in (*reference.holidays(), *reference.SPECIAL_SESSIONS.values()):
        assert entry.source.startswith(("NSE/FAOP/", "NSE/MSD/")), entry


def test_every_loaded_holiday_resolves_to_no_session_unless_a_special_session_overrides_it(
    resolver: NSEFuturesTradingSessionResolver,
) -> None:
    for holiday in reference.HOLIDAYS:
        special = reference.SPECIAL_SESSIONS.get(holiday)
        if special is None:
            assert resolver.resolve(_NIFTY, holiday) is None
        else:
            assert resolver.resolve(_NIFTY, holiday) is not None


# ---------------------------------------------------------------------------
# Determinism, independence and boundaries
# ---------------------------------------------------------------------------


def test_no_network_is_used(
    resolver: NSEFuturesTradingSessionResolver, monkeypatch: pytest.MonkeyPatch
) -> None:
    def refuse(*args, **kwargs):
        raise AssertionError("network access attempted")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)

    assert len(resolver.sessions_in_range(_NIFTY, date(2024, 1, 1), date(2026, 11, 7))) == 708


@pytest.mark.skipif(not hasattr(time_module, "tzset"), reason="tzset is POSIX-only")
@pytest.mark.parametrize("zone", ["America/Chicago", "Asia/Kolkata", "Pacific/Kiritimati", "UTC"])
def test_the_host_timezone_does_not_move_a_session(
    resolver: NSEFuturesTradingSessionResolver, monkeypatch: pytest.MonkeyPatch, zone: str
) -> None:
    monkeypatch.setenv("TZ", zone)
    time_module.tzset()
    try:
        assert resolver.resolve(_NIFTY, date(2026, 8, 3)) == _session(
            date(2026, 8, 3), "2026-08-03T03:30:00Z", "2026-08-03T10:10:00Z"
        )
    finally:
        monkeypatch.delenv("TZ", raising=False)
        time_module.tzset()


def _tree(source) -> ast.Module:
    return ast.parse(Path(source.__file__).read_text(encoding="utf-8"))


def _imports(tree: ast.Module) -> set[str]:
    return {
        node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.module
    } | {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }


@pytest.mark.parametrize("source", [module, reference], ids=["resolver", "reference"])
def test_no_calendar_library_network_or_clock_is_imported(source) -> None:
    for name in _imports(_tree(source)):
        assert name.split(".")[0] not in {
            "exchange_calendars",
            "pandas_market_calendars",
            "requests",
            "urllib",
            "http",
            "socket",
            "time",
            "os",
            "databento",
            "json",
        }, name


@pytest.mark.parametrize("source", [module, reference], ids=["resolver", "reference"])
def test_no_bse_calendar_or_local_clock_is_referenced(source) -> None:
    tree = _tree(source)
    docstrings = {
        id(node.body[0].value)
        for node in ast.walk(tree)
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef))
        and node.body
        and isinstance(node.body[0], ast.Expr)
    }
    strings = [
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and id(node) not in docstrings
    ]
    calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
    ]

    assert not [s for s in strings if "XBOM" in s or "BSE" in s]
    assert not [c for c in calls if c.func.attr in {"now", "today", "utcnow", "localtime"}]
    assert not [c for c in calls if c.func.attr == "astimezone" and not c.args]


def test_the_resolver_is_exported_beside_the_unchanged_cme_resolver() -> None:
    assert "NSEFuturesTradingSessionResolver" in market_data.__all__
    assert "ExchangeCalendarFuturesTradingSessionResolver" in market_data.__all__
    assert ExchangeCalendarFuturesTradingSessionResolver._VENUE_CALENDARS == {
        "CME": "CME",
        "CBOT": "CBOT",
        "NYMEX": "NYMEX",
        "COMEX": "COMEX",
    }
    assert not hasattr(NSEFuturesTradingSessionResolver, "_VENUE_CALENDARS")
