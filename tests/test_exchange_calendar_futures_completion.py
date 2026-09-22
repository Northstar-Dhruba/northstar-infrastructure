"""Integration tests for the exchange-calendar futures completion resolver.

These run against the real exchange_calendars library rather than a double, so
they exercise the actual CME schedule: its holidays, its early closes and its
daylight-saving transitions. A double would only agree with whatever the
adapter already does.

The expected instants are pinned as literals. They were established
independently from the calendar during the Epic 9.6a probe, so a library
upgrade that silently moved a session close fails here rather than quietly
re-stamping every stored bar.
"""

from __future__ import annotations

import datetime as dt
from datetime import date

import exchange_calendars as xcals
import pytest
from northstar_application.ports import (
    FuturesDailyBarCompletionResolver,
    FuturesSessionResolutionError,
)
from northstar_core.foundation.value_objects import ExchangeCode, PointInTime, Symbol
from northstar_core.futures import FuturesContract, FuturesProductReference

from northstar_infrastructure.market_data import (
    ExchangeCalendarFuturesDailyBarCompletionResolver,
)

_ES = FuturesProductReference(Symbol("ES"), ExchangeCode("CME"))
_MES = FuturesProductReference(Symbol("MES"), ExchangeCode("CME"))
_ZB = FuturesProductReference(Symbol("ZB"), ExchangeCode("CBOT"))
_CL = FuturesProductReference(Symbol("CL"), ExchangeCode("NYMEX"))
_GC = FuturesProductReference(Symbol("GC"), ExchangeCode("COMEX"))

_SUPPORTED = (_ES, _ZB, _CL, _GC)


@pytest.fixture
def resolver() -> ExchangeCalendarFuturesDailyBarCompletionResolver:
    return ExchangeCalendarFuturesDailyBarCompletionResolver()


# ---------------------------------------------------------------------------
# Pinned session completions
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("trading_date", "expected"),
    [
        (date(2026, 3, 6), "2026-03-06T23:00:00Z"),
        (date(2026, 3, 9), "2026-03-09T22:00:00Z"),
        (date(2026, 9, 15), "2026-09-15T22:00:00Z"),
    ],
    ids=["dst_winter", "dst_summer", "normal"],
)
def test_normal_and_dst_sessions_resolve_to_pinned_instants(
    resolver: ExchangeCalendarFuturesDailyBarCompletionResolver,
    trading_date: date,
    expected: str,
) -> None:
    assert resolver.resolve_completion(_ES, trading_date) == PointInTime(expected)


@pytest.mark.parametrize(
    ("trading_date", "expected"),
    [
        (date(2026, 7, 3), "2026-07-03T17:00:00Z"),
        (date(2026, 11, 27), "2026-11-27T18:00:00Z"),
    ],
    ids=["independence_day_observed", "day_after_thanksgiving"],
)
def test_early_close_sessions_resolve_to_pinned_instants(
    resolver: ExchangeCalendarFuturesDailyBarCompletionResolver,
    trading_date: date,
    expected: str,
) -> None:
    assert resolver.resolve_completion(_ES, trading_date) == PointInTime(expected)


def test_daylight_saving_shifts_the_utc_instant_by_one_hour(
    resolver: ExchangeCalendarFuturesDailyBarCompletionResolver,
) -> None:
    """Both are 17:00 Chicago; only the UTC offset moves."""
    winter = resolver.resolve_completion(_ES, date(2026, 3, 6))
    summer = resolver.resolve_completion(_ES, date(2026, 3, 9))

    assert winter is not None and summer is not None
    assert winter.value.endswith("T23:00:00Z")
    assert summer.value.endswith("T22:00:00Z")


# ---------------------------------------------------------------------------
# The load-bearing arithmetic regression
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "trading_date", [date(2026, 7, 3), date(2026, 11, 27)], ids=["jul3", "nov27"]
)
def test_an_early_close_is_not_the_session_open_plus_twenty_four_hours(
    resolver: ExchangeCalendarFuturesDailyBarCompletionResolver, trading_date: date
) -> None:
    """Proves the resolver reads the close instead of computing it.

    This test fails if the implementation is ever mutated to fixed 24-hour
    arithmetic on the session open.
    """
    calendar = xcals.get_calendar("CME")
    session_open = calendar.session_open(trading_date)
    session_close = calendar.session_close(trading_date)
    naive_arithmetic = session_open + dt.timedelta(hours=24)

    # The shortcut and the truth genuinely disagree on this session.
    assert naive_arithmetic != session_close
    assert (naive_arithmetic - session_close) == dt.timedelta(hours=5)

    resolved = resolver.resolve_completion(_ES, trading_date)

    assert resolved == PointInTime(session_close.strftime("%Y-%m-%dT%H:%M:%SZ"))
    assert resolved != PointInTime(naive_arithmetic.strftime("%Y-%m-%dT%H:%M:%SZ"))


def test_a_normal_session_is_where_the_shortcut_would_have_passed(
    resolver: ExchangeCalendarFuturesDailyBarCompletionResolver,
) -> None:
    """Documents why DST-only testing is not enough.

    On a full session the 24-hour shortcut agrees with the calendar, including
    across a daylight-saving transition, so a suite that tested only these
    would never catch the early-close defect.
    """
    calendar = xcals.get_calendar("CME")

    for trading_date in (date(2026, 9, 15), date(2026, 3, 6), date(2026, 3, 9)):
        session_open = calendar.session_open(trading_date)
        session_close = calendar.session_close(trading_date)

        assert session_open + dt.timedelta(hours=24) == session_close
        assert resolver.resolve_completion(_ES, trading_date) == PointInTime(
            session_close.strftime("%Y-%m-%dT%H:%M:%SZ")
        )


# ---------------------------------------------------------------------------
# Session-label semantics
# ---------------------------------------------------------------------------


def test_a_session_labelled_2026_09_15_opens_on_2026_09_14(
    resolver: ExchangeCalendarFuturesDailyBarCompletionResolver,
) -> None:
    """The label names the session, not the civil day it begins on."""
    calendar = xcals.get_calendar("CME")
    label = date(2026, 9, 15)

    session_open = calendar.session_open(label)
    session_close = calendar.session_close(label)

    assert session_open.date() == date(2026, 9, 14)
    assert session_close.date() == date(2026, 9, 15)

    completion = resolver.resolve_completion(_ES, label)

    assert completion == PointInTime("2026-09-15T22:00:00Z")
    assert completion is not None
    assert completion.value.startswith("2026-09-15T")


def test_every_sampled_session_opens_on_the_previous_civil_date() -> None:
    """Not an edge case: this is the ordinary shape of a CME session."""
    calendar = xcals.get_calendar("CME")

    for label in (
        date(2026, 3, 6),
        date(2026, 3, 9),
        date(2026, 7, 3),
        date(2026, 9, 15),
        date(2026, 11, 27),
    ):
        assert calendar.session_open(label).date() == label - dt.timedelta(days=1)


def test_the_label_is_resolved_directly_not_via_utc_date_arithmetic(
    resolver: ExchangeCalendarFuturesDailyBarCompletionResolver,
) -> None:
    """Deriving the label from the open's UTC date would be off by one session."""
    calendar = xcals.get_calendar("CME")
    label = date(2026, 9, 15)
    open_utc_date = calendar.session_open(label).date()

    assert open_utc_date != label

    assert resolver.resolve_completion(_ES, label) == PointInTime("2026-09-15T22:00:00Z")
    assert resolver.resolve_completion(_ES, open_utc_date) == PointInTime("2026-09-14T22:00:00Z")


# ---------------------------------------------------------------------------
# Non-sessions return None
# ---------------------------------------------------------------------------


def test_a_holiday_returns_none(
    resolver: ExchangeCalendarFuturesDailyBarCompletionResolver,
) -> None:
    assert resolver.resolve_completion(_ES, date(2026, 12, 25)) is None


@pytest.mark.parametrize(
    "trading_date", [date(2026, 9, 19), date(2026, 9, 20)], ids=["saturday", "sunday"]
)
def test_a_weekend_returns_none(
    resolver: ExchangeCalendarFuturesDailyBarCompletionResolver, trading_date: date
) -> None:
    assert resolver.resolve_completion(_ES, trading_date) is None


@pytest.mark.parametrize("product", _SUPPORTED, ids=lambda p: p.exchange_code.value)
def test_a_holiday_returns_none_on_every_supported_venue(
    resolver: ExchangeCalendarFuturesDailyBarCompletionResolver,
    product: FuturesProductReference,
) -> None:
    assert resolver.resolve_completion(product, date(2026, 12, 25)) is None


# ---------------------------------------------------------------------------
# None versus error
# ---------------------------------------------------------------------------


def test_an_unsupported_venue_raises_rather_than_returning_none(
    resolver: ExchangeCalendarFuturesDailyBarCompletionResolver,
) -> None:
    """ "Cannot answer" must never be reported as "no session that day"."""
    unknown = FuturesProductReference(Symbol("ES"), ExchangeCode("XXXX"))

    with pytest.raises(FuturesSessionResolutionError, match="Unsupported futures venue"):
        resolver.resolve_completion(unknown, date(2026, 9, 15))


@pytest.mark.parametrize("venue", ["NASDAQ", "NYSE", "LSE", "BSE"])
def test_equity_venues_are_rejected_by_the_futures_resolver(
    resolver: ExchangeCalendarFuturesDailyBarCompletionResolver, venue: str
) -> None:
    """An equity venue has no futures session schedule here, even if it exists."""
    product = FuturesProductReference(Symbol("ES"), ExchangeCode(venue))

    with pytest.raises(FuturesSessionResolutionError, match="Unsupported futures venue"):
        resolver.resolve_completion(product, date(2026, 9, 15))


def test_the_error_names_the_supported_venues(
    resolver: ExchangeCalendarFuturesDailyBarCompletionResolver,
) -> None:
    product = FuturesProductReference(Symbol("ES"), ExchangeCode("XXXX"))

    with pytest.raises(FuturesSessionResolutionError) as raised:
        resolver.resolve_completion(product, date(2026, 9, 15))

    for venue in ("CME", "CBOT", "NYMEX", "COMEX"):
        assert venue in str(raised.value)


@pytest.mark.parametrize(
    "trading_date", [date(1800, 1, 1), date(2200, 1, 1)], ids=["before", "after"]
)
def test_a_date_outside_the_calendar_bounds_raises(
    resolver: ExchangeCalendarFuturesDailyBarCompletionResolver, trading_date: date
) -> None:
    """Out of bounds means "cannot answer", not "not a session"."""
    with pytest.raises(FuturesSessionResolutionError, match="resolution failed"):
        resolver.resolve_completion(_ES, trading_date)


def test_a_supported_venue_on_a_non_session_returns_none_not_an_error(
    resolver: ExchangeCalendarFuturesDailyBarCompletionResolver,
) -> None:
    """None is reserved strictly for this case."""
    assert resolver.resolve_completion(_ES, date(2026, 12, 25)) is None
    assert resolver.resolve_completion(_ES, date(2026, 9, 19)) is None


# ---------------------------------------------------------------------------
# Venue map
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("product", _SUPPORTED, ids=lambda p: p.exchange_code.value)
def test_every_supported_venue_resolves(
    resolver: ExchangeCalendarFuturesDailyBarCompletionResolver,
    product: FuturesProductReference,
) -> None:
    assert resolver.resolve_completion(product, date(2026, 9, 15)) == PointInTime(
        "2026-09-15T22:00:00Z"
    )


def test_the_four_venues_are_mapped_separately() -> None:
    """Four decisions, not one generic calendar, even though they alias today."""
    mapping = ExchangeCalendarFuturesDailyBarCompletionResolver._VENUE_CALENDARS

    assert set(mapping) == {"CME", "CBOT", "NYMEX", "COMEX"}
    assert len(set(mapping.values())) == 4
    assert "CMES" not in mapping.values()


def test_the_venues_currently_alias_to_one_library_calendar() -> None:
    """Records why the separate mapping looks redundant right now.

    If a future library version gives a venue its own schedule, this test fails
    and the separate mapping starts doing visible work.
    """
    names = {xcals.get_calendar(name).name for name in ("CME", "CBOT", "NYMEX", "COMEX")}

    assert names == {"CMES"}


# ---------------------------------------------------------------------------
# Input handling
# ---------------------------------------------------------------------------


def test_two_products_on_one_venue_resolve_consistently(
    resolver: ExchangeCalendarFuturesDailyBarCompletionResolver,
) -> None:
    assert resolver.resolve_completion(_ES, date(2026, 9, 15)) == resolver.resolve_completion(
        _MES, date(2026, 9, 15)
    )


def test_the_product_reaches_the_adapter_as_a_product_reference(
    resolver: ExchangeCalendarFuturesDailyBarCompletionResolver,
) -> None:
    """A bare ExchangeCode is not accepted in its place."""
    with pytest.raises(TypeError, match="FuturesProductReference"):
        resolver.resolve_completion(ExchangeCode("CME"), date(2026, 9, 15))  # type: ignore[arg-type]


def test_a_futures_contract_is_not_accepted(
    resolver: ExchangeCalendarFuturesDailyBarCompletionResolver,
) -> None:
    from northstar_core.derivatives import ExpirationDate

    contract = FuturesContract(_ES, ExpirationDate("2026-12-18"))

    with pytest.raises(TypeError, match="FuturesProductReference"):
        resolver.resolve_completion(contract, date(2026, 9, 15))  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "trading_date",
    ["2026-09-15", PointInTime("2026-09-15T22:00:00Z"), dt.datetime(2026, 9, 15, 22, 0)],
    ids=["str", "point_in_time", "datetime"],
)
def test_the_trading_date_must_be_a_plain_date(
    resolver: ExchangeCalendarFuturesDailyBarCompletionResolver, trading_date: object
) -> None:
    """A datetime smuggles a clock into what must be a session label."""
    with pytest.raises(TypeError, match="datetime.date"):
        resolver.resolve_completion(_ES, trading_date)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Result shape
# ---------------------------------------------------------------------------


def test_the_result_is_a_canonical_point_in_time(
    resolver: ExchangeCalendarFuturesDailyBarCompletionResolver,
) -> None:
    completion = resolver.resolve_completion(_ES, date(2026, 9, 15))

    assert isinstance(completion, PointInTime)
    assert completion.value == "2026-09-15T22:00:00Z"
    assert completion == PointInTime("2026-09-16T03:30:00+05:30")


def test_the_result_is_utc_and_not_merely_labelled_utc(
    resolver: ExchangeCalendarFuturesDailyBarCompletionResolver,
) -> None:
    """The instant must equal the calendar's own UTC-converted timestamp."""
    calendar = xcals.get_calendar("CME")
    expected = calendar.session_close(date(2026, 9, 15)).tz_convert("UTC")

    completion = resolver.resolve_completion(_ES, date(2026, 9, 15))

    assert completion is not None
    assert completion.value == expected.strftime("%Y-%m-%dT%H:%M:%SZ")


def test_the_resolver_implements_the_application_port() -> None:
    assert issubclass(
        ExchangeCalendarFuturesDailyBarCompletionResolver, FuturesDailyBarCompletionResolver
    )
    assert isinstance(
        ExchangeCalendarFuturesDailyBarCompletionResolver(), FuturesDailyBarCompletionResolver
    )


def test_repeated_calls_are_stable(
    resolver: ExchangeCalendarFuturesDailyBarCompletionResolver,
) -> None:
    """Calendars are cached; caching must not change the answer."""
    first = [resolver.resolve_completion(product, date(2026, 9, 15)) for product in _SUPPORTED]
    second = [resolver.resolve_completion(product, date(2026, 9, 15)) for product in _SUPPORTED]

    assert first == second
    assert len(set(first)) == 1
