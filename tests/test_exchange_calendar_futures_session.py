"""Integration tests for the exchange-calendar futures trading-session resolver.

These run against the real exchange_calendars library rather than a double, so
they exercise the actual CME schedule: its holidays, its early closes, its
daylight-saving transitions and its weekend gaps. A double would only agree
with whatever the adapter already does.

The expected instants are pinned as literals. They were established
independently from the calendar during the Epic 9.6a probe and confirmed
against live Databento data, so a library upgrade that silently moved a session
boundary fails here rather than quietly re-stamping every aggregated bar.
"""

from __future__ import annotations

import ast
import datetime as dt
from datetime import date
from pathlib import Path

import exchange_calendars as xcals
import pytest
from northstar_application.ports import (
    FuturesSessionResolutionError,
    FuturesTradingSession,
    FuturesTradingSessionResolver,
)
from northstar_core.foundation.value_objects import ExchangeCode, PointInTime, Symbol
from northstar_core.futures import FuturesContract, FuturesProductReference

from northstar_infrastructure.market_data import (
    ExchangeCalendarFuturesTradingSessionResolver,
)

_ES = FuturesProductReference(Symbol("ES"), ExchangeCode("CME"))
_MES = FuturesProductReference(Symbol("MES"), ExchangeCode("CME"))
_ZB = FuturesProductReference(Symbol("ZB"), ExchangeCode("CBOT"))
_CL = FuturesProductReference(Symbol("CL"), ExchangeCode("NYMEX"))
_GC = FuturesProductReference(Symbol("GC"), ExchangeCode("COMEX"))

_SUPPORTED = (_ES, _ZB, _CL, _GC)


@pytest.fixture
def resolver() -> ExchangeCalendarFuturesTradingSessionResolver:
    return ExchangeCalendarFuturesTradingSessionResolver()


# ---------------------------------------------------------------------------
# Pinned session windows
# ---------------------------------------------------------------------------


def test_a_normal_session_window_is_pinned(
    resolver: ExchangeCalendarFuturesTradingSessionResolver,
) -> None:
    session = resolver.resolve(_ES, date(2026, 9, 15))

    assert session == FuturesTradingSession(
        date(2026, 9, 15),
        PointInTime("2026-09-14T22:00:00Z"),
        PointInTime("2026-09-15T22:00:00Z"),
    )


@pytest.mark.parametrize(
    ("trading_date", "expected_close"),
    [
        (date(2026, 3, 6), "2026-03-06T23:00:00Z"),
        (date(2026, 3, 9), "2026-03-09T22:00:00Z"),
        (date(2026, 9, 15), "2026-09-15T22:00:00Z"),
    ],
    ids=["dst_winter", "dst_summer", "normal"],
)
def test_normal_and_dst_closes_are_pinned(
    resolver: ExchangeCalendarFuturesTradingSessionResolver,
    trading_date: date,
    expected_close: str,
) -> None:
    session = resolver.resolve(_ES, trading_date)

    assert session is not None
    assert session.closes_at == PointInTime(expected_close)


@pytest.mark.parametrize(
    ("trading_date", "expected_close"),
    [
        (date(2026, 7, 3), "2026-07-03T17:00:00Z"),
        (date(2026, 11, 27), "2026-11-27T18:00:00Z"),
    ],
    ids=["independence_day_observed", "day_after_thanksgiving"],
)
def test_early_close_sessions_are_pinned(
    resolver: ExchangeCalendarFuturesTradingSessionResolver,
    trading_date: date,
    expected_close: str,
) -> None:
    session = resolver.resolve(_ES, trading_date)

    assert session is not None
    assert session.closes_at == PointInTime(expected_close)


def test_daylight_saving_shifts_the_utc_instants_by_one_hour(
    resolver: ExchangeCalendarFuturesTradingSessionResolver,
) -> None:
    """Both are 17:00 Chicago; only the UTC offset moves."""
    winter = resolver.resolve(_ES, date(2026, 3, 6))
    summer = resolver.resolve(_ES, date(2026, 3, 9))

    assert winter is not None and summer is not None
    assert winter.closes_at.value.endswith("T23:00:00Z")
    assert summer.closes_at.value.endswith("T22:00:00Z")
    assert winter.opens_at.value.endswith("T23:00:00Z")
    assert summer.opens_at.value.endswith("T22:00:00Z")


def test_a_holiday_returns_none(
    resolver: ExchangeCalendarFuturesTradingSessionResolver,
) -> None:
    assert resolver.resolve(_ES, date(2026, 12, 25)) is None


@pytest.mark.parametrize(
    "trading_date", [date(2026, 9, 19), date(2026, 9, 20)], ids=["saturday", "sunday"]
)
def test_a_weekend_returns_none(
    resolver: ExchangeCalendarFuturesTradingSessionResolver, trading_date: date
) -> None:
    assert resolver.resolve(_ES, trading_date) is None


# ---------------------------------------------------------------------------
# Load-bearing: boundaries are read, never computed
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "trading_date", [date(2026, 7, 3), date(2026, 11, 27)], ids=["jul3", "nov27"]
)
def test_an_early_close_is_not_the_open_plus_twenty_four_hours(
    resolver: ExchangeCalendarFuturesTradingSessionResolver, trading_date: date
) -> None:
    """Fails if the close is ever derived by fixed-duration arithmetic."""
    calendar = xcals.get_calendar("CME")
    session_open = calendar.session_open(trading_date)
    session_close = calendar.session_close(trading_date)
    naive = session_open + dt.timedelta(hours=24)

    assert naive != session_close
    assert (naive - session_close) == dt.timedelta(hours=5)

    session = resolver.resolve(_ES, trading_date)

    assert session is not None
    assert session.closes_at == PointInTime(session_close.strftime("%Y-%m-%dT%H:%M:%SZ"))
    assert session.closes_at != PointInTime(naive.strftime("%Y-%m-%dT%H:%M:%SZ"))


def test_a_normal_session_is_where_the_shortcut_would_have_passed(
    resolver: ExchangeCalendarFuturesTradingSessionResolver,
) -> None:
    """Documents why DST-only testing is not enough.

    On a full session the 24-hour shortcut agrees with the calendar, including
    across both daylight-saving transitions, so a suite testing only these
    would never catch the early-close defect.
    """
    calendar = xcals.get_calendar("CME")

    for trading_date in (date(2026, 9, 15), date(2026, 3, 6), date(2026, 3, 9)):
        assert calendar.session_open(trading_date) + dt.timedelta(
            hours=24
        ) == calendar.session_close(trading_date)


def test_the_holiday_gap_is_real(
    resolver: ExchangeCalendarFuturesTradingSessionResolver,
) -> None:
    """Load-bearing: fails if a session open is ever derived from the previous close.

    Friday 2026-07-03 is an early close. The next session, Monday 2026-07-06,
    opens on the Sunday evening -- more than two days later. Chaining closes
    would make Monday's window start on Friday lunchtime and swallow the whole
    weekend.
    """
    friday = resolver.resolve(_ES, date(2026, 7, 3))
    monday = resolver.resolve(_ES, date(2026, 7, 6))

    assert friday is not None and monday is not None
    assert friday.closes_at == PointInTime("2026-07-03T17:00:00Z")
    assert monday.opens_at == PointInTime("2026-07-05T22:00:00Z")

    assert monday.opens_at != friday.closes_at
    assert friday.closes_at.compare(monday.opens_at) < 0

    gap = dt.datetime.fromisoformat(
        monday.opens_at.value.replace("Z", "+00:00")
    ) - dt.datetime.fromisoformat(friday.closes_at.value.replace("Z", "+00:00"))
    assert gap == dt.timedelta(days=2, hours=5)


def test_consecutive_weekday_sessions_do_abut(
    resolver: ExchangeCalendarFuturesTradingSessionResolver,
) -> None:
    """Within a week they meet, which is exactly why the gap is easy to miss."""
    first = resolver.resolve(_ES, date(2026, 9, 15))
    second = resolver.resolve(_ES, date(2026, 9, 16))

    assert first is not None and second is not None
    assert first.closes_at == second.opens_at


def test_a_weekend_gap_is_also_real(
    resolver: ExchangeCalendarFuturesTradingSessionResolver,
) -> None:
    friday = resolver.resolve(_ES, date(2026, 9, 18))
    monday = resolver.resolve(_ES, date(2026, 9, 21))

    assert friday is not None and monday is not None
    assert monday.opens_at != friday.closes_at


# ---------------------------------------------------------------------------
# Session-label semantics
# ---------------------------------------------------------------------------


def test_a_session_labelled_2026_09_15_opens_on_2026_09_14(
    resolver: ExchangeCalendarFuturesTradingSessionResolver,
) -> None:
    """The label names the session, not the civil day it begins on."""
    session = resolver.resolve(_ES, date(2026, 9, 15))

    assert session is not None
    assert session.trading_date == date(2026, 9, 15)
    assert session.opens_at.value.startswith("2026-09-14")
    assert session.closes_at.value.startswith("2026-09-15")


def test_every_sampled_session_opens_on_the_previous_civil_date(
    resolver: ExchangeCalendarFuturesTradingSessionResolver,
) -> None:
    """Not an edge case: this is the ordinary shape of a CME session."""
    for label in (
        date(2026, 3, 6),
        date(2026, 3, 9),
        date(2026, 7, 3),
        date(2026, 9, 15),
        date(2026, 11, 27),
    ):
        session = resolver.resolve(_ES, label)
        assert session is not None
        opens_on = dt.date.fromisoformat(session.opens_at.value[:10])
        assert opens_on == label - dt.timedelta(days=1)


def test_the_label_is_resolved_directly_not_via_utc_date_arithmetic(
    resolver: ExchangeCalendarFuturesTradingSessionResolver,
) -> None:
    """Deriving the label from the open's UTC date would be off by one session."""
    session = resolver.resolve(_ES, date(2026, 9, 15))
    assert session is not None
    open_utc_date = dt.date.fromisoformat(session.opens_at.value[:10])

    assert open_utc_date != date(2026, 9, 15)

    earlier = resolver.resolve(_ES, open_utc_date)
    assert earlier is not None
    assert earlier.closes_at == PointInTime("2026-09-14T22:00:00Z")


# ---------------------------------------------------------------------------
# sessions_in_range
# ---------------------------------------------------------------------------


def test_a_single_session_inclusive_range(
    resolver: ExchangeCalendarFuturesTradingSessionResolver,
) -> None:
    sessions = resolver.sessions_in_range(_ES, date(2026, 9, 15), date(2026, 9, 15))

    assert len(sessions) == 1
    assert sessions[0].trading_date == date(2026, 9, 15)
    assert sessions[0].closes_at == PointInTime("2026-09-15T22:00:00Z")


def test_a_multi_session_range_is_inclusive_at_both_ends(
    resolver: ExchangeCalendarFuturesTradingSessionResolver,
) -> None:
    sessions = resolver.sessions_in_range(_ES, date(2026, 9, 15), date(2026, 9, 17))

    assert [s.trading_date for s in sessions] == [
        date(2026, 9, 15),
        date(2026, 9, 16),
        date(2026, 9, 17),
    ]


def test_weekends_are_omitted(
    resolver: ExchangeCalendarFuturesTradingSessionResolver,
) -> None:
    sessions = resolver.sessions_in_range(_ES, date(2026, 9, 18), date(2026, 9, 21))

    assert [s.trading_date for s in sessions] == [date(2026, 9, 18), date(2026, 9, 21)]


def test_holidays_are_omitted(
    resolver: ExchangeCalendarFuturesTradingSessionResolver,
) -> None:
    sessions = resolver.sessions_in_range(_ES, date(2026, 12, 24), date(2026, 12, 28))
    labels = [s.trading_date for s in sessions]

    assert date(2026, 12, 25) not in labels
    assert date(2026, 12, 24) in labels


def test_a_range_spanning_the_july_gap_includes_the_early_close(
    resolver: ExchangeCalendarFuturesTradingSessionResolver,
) -> None:
    sessions = resolver.sessions_in_range(_ES, date(2026, 7, 1), date(2026, 7, 8))
    by_label = {s.trading_date: s for s in sessions}

    assert [s.trading_date for s in sessions] == [
        date(2026, 7, 1),
        date(2026, 7, 2),
        date(2026, 7, 3),
        date(2026, 7, 6),
        date(2026, 7, 7),
        date(2026, 7, 8),
    ]
    assert by_label[date(2026, 7, 3)].closes_at == PointInTime("2026-07-03T17:00:00Z")
    assert by_label[date(2026, 7, 6)].opens_at == PointInTime("2026-07-05T22:00:00Z")


def test_range_results_are_strictly_ascending(
    resolver: ExchangeCalendarFuturesTradingSessionResolver,
) -> None:
    sessions = resolver.sessions_in_range(_ES, date(2026, 6, 1), date(2026, 9, 30))
    labels = [s.trading_date for s in sessions]

    assert labels == sorted(labels)
    assert len(labels) == len(set(labels))
    assert len(labels) > 50


def test_every_range_result_uses_real_calendar_boundaries(
    resolver: ExchangeCalendarFuturesTradingSessionResolver,
) -> None:
    calendar = xcals.get_calendar("CME")

    for session in resolver.sessions_in_range(_ES, date(2026, 7, 1), date(2026, 7, 8)):
        expected_open = calendar.session_open(session.trading_date)
        expected_close = calendar.session_close(session.trading_date)
        assert session.opens_at == PointInTime(expected_open.strftime("%Y-%m-%dT%H:%M:%SZ"))
        assert session.closes_at == PointInTime(expected_close.strftime("%Y-%m-%dT%H:%M:%SZ"))


def test_a_range_of_only_non_sessions_is_empty(
    resolver: ExchangeCalendarFuturesTradingSessionResolver,
) -> None:
    assert resolver.sessions_in_range(_ES, date(2026, 9, 19), date(2026, 9, 20)) == ()


def test_a_range_matches_resolve_for_each_label(
    resolver: ExchangeCalendarFuturesTradingSessionResolver,
) -> None:
    """One authority: enumeration and single lookup must never disagree."""
    sessions = resolver.sessions_in_range(_ES, date(2026, 7, 1), date(2026, 7, 8))

    for session in sessions:
        assert resolver.resolve(_ES, session.trading_date) == session


def test_an_inverted_range_raises(
    resolver: ExchangeCalendarFuturesTradingSessionResolver,
) -> None:
    """The calendar returns an empty index for this; the contract refuses it."""
    with pytest.raises(FuturesSessionResolutionError, match="is after end"):
        resolver.sessions_in_range(_ES, date(2026, 9, 17), date(2026, 9, 15))


def test_the_underlying_calendar_would_have_silently_allowed_the_inversion() -> None:
    """Pins why the adapter validates the range itself."""
    calendar = xcals.get_calendar("CME")

    assert len(calendar.sessions_in_range(date(2026, 9, 17), date(2026, 9, 15))) == 0


# ---------------------------------------------------------------------------
# None versus error
# ---------------------------------------------------------------------------


def test_an_unsupported_venue_raises_rather_than_returning_none(
    resolver: ExchangeCalendarFuturesTradingSessionResolver,
) -> None:
    unknown = FuturesProductReference(Symbol("ES"), ExchangeCode("XXXX"))

    with pytest.raises(FuturesSessionResolutionError, match="Unsupported futures venue"):
        resolver.resolve(unknown, date(2026, 9, 15))


@pytest.mark.parametrize("venue", ["NASDAQ", "NYSE", "LSE", "BSE"])
def test_equity_venues_are_rejected(
    resolver: ExchangeCalendarFuturesTradingSessionResolver, venue: str
) -> None:
    product = FuturesProductReference(Symbol("ES"), ExchangeCode(venue))

    with pytest.raises(FuturesSessionResolutionError, match="Unsupported futures venue"):
        resolver.resolve(product, date(2026, 9, 15))
    with pytest.raises(FuturesSessionResolutionError, match="Unsupported futures venue"):
        resolver.sessions_in_range(product, date(2026, 9, 15), date(2026, 9, 16))


def test_the_error_names_the_supported_venues(
    resolver: ExchangeCalendarFuturesTradingSessionResolver,
) -> None:
    product = FuturesProductReference(Symbol("ES"), ExchangeCode("XXXX"))

    with pytest.raises(FuturesSessionResolutionError) as raised:
        resolver.resolve(product, date(2026, 9, 15))

    for venue in ("CME", "CBOT", "NYMEX", "COMEX"):
        assert venue in str(raised.value)


@pytest.mark.parametrize(
    "trading_date", [date(1800, 1, 1), date(2200, 1, 1)], ids=["before", "after"]
)
def test_a_date_outside_calendar_bounds_raises(
    resolver: ExchangeCalendarFuturesTradingSessionResolver, trading_date: date
) -> None:
    with pytest.raises(FuturesSessionResolutionError, match="resolution failed"):
        resolver.resolve(_ES, trading_date)


@pytest.mark.parametrize(
    ("start", "end"),
    [(date(1800, 1, 1), date(1800, 1, 5)), (date(2200, 1, 1), date(2200, 1, 5))],
    ids=["before", "after"],
)
def test_a_range_outside_calendar_bounds_raises(
    resolver: ExchangeCalendarFuturesTradingSessionResolver, start: date, end: date
) -> None:
    with pytest.raises(FuturesSessionResolutionError, match="resolution failed"):
        resolver.sessions_in_range(_ES, start, end)


def test_a_supported_venue_on_a_non_session_returns_none_not_an_error(
    resolver: ExchangeCalendarFuturesTradingSessionResolver,
) -> None:
    assert resolver.resolve(_ES, date(2026, 12, 25)) is None
    assert resolver.resolve(_ES, date(2026, 9, 19)) is None


# ---------------------------------------------------------------------------
# Venue map
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("product", _SUPPORTED, ids=lambda p: p.exchange_code.value)
def test_every_supported_venue_resolves(
    resolver: ExchangeCalendarFuturesTradingSessionResolver,
    product: FuturesProductReference,
) -> None:
    session = resolver.resolve(product, date(2026, 9, 15))

    assert session is not None
    assert session.closes_at == PointInTime("2026-09-15T22:00:00Z")


@pytest.mark.parametrize("product", _SUPPORTED, ids=lambda p: p.exchange_code.value)
def test_every_supported_venue_enumerates(
    resolver: ExchangeCalendarFuturesTradingSessionResolver,
    product: FuturesProductReference,
) -> None:
    sessions = resolver.sessions_in_range(product, date(2026, 9, 15), date(2026, 9, 17))

    assert len(sessions) == 3


def test_the_four_venues_are_mapped_separately() -> None:
    """Four decisions, not one generic calendar, even though they alias today."""
    mapping = ExchangeCalendarFuturesTradingSessionResolver._VENUE_CALENDARS

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
    resolver: ExchangeCalendarFuturesTradingSessionResolver,
) -> None:
    assert resolver.resolve(_ES, date(2026, 9, 15)) == resolver.resolve(_MES, date(2026, 9, 15))


def test_a_bare_exchange_code_is_not_accepted(
    resolver: ExchangeCalendarFuturesTradingSessionResolver,
) -> None:
    with pytest.raises(TypeError, match="FuturesProductReference"):
        resolver.resolve(ExchangeCode("CME"), date(2026, 9, 15))  # type: ignore[arg-type]


def test_a_futures_contract_is_not_accepted(
    resolver: ExchangeCalendarFuturesTradingSessionResolver,
) -> None:
    from northstar_core.derivatives import ExpirationDate

    contract = FuturesContract(_ES, ExpirationDate("2026-12-18"))

    with pytest.raises(TypeError, match="FuturesProductReference"):
        resolver.resolve(contract, date(2026, 9, 15))  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "trading_date",
    ["2026-09-15", PointInTime("2026-09-15T22:00:00Z"), dt.datetime(2026, 9, 15, 22, 0)],
    ids=["str", "point_in_time", "datetime"],
)
def test_resolve_requires_a_plain_date(
    resolver: ExchangeCalendarFuturesTradingSessionResolver, trading_date: object
) -> None:
    """A datetime smuggles a clock into what must be a session label."""
    with pytest.raises(TypeError, match="datetime.date"):
        resolver.resolve(_ES, trading_date)  # type: ignore[arg-type]


@pytest.mark.parametrize("position", ["start_date", "end_date"])
def test_sessions_in_range_requires_plain_dates(
    resolver: ExchangeCalendarFuturesTradingSessionResolver, position: str
) -> None:
    bounds: dict[str, object] = {
        "start_date": date(2026, 9, 15),
        "end_date": date(2026, 9, 17),
    }
    bounds[position] = dt.datetime(2026, 9, 15, 22, 0)

    with pytest.raises(TypeError, match="datetime.date"):
        resolver.sessions_in_range(_ES, **bounds)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Result shape and migration
# ---------------------------------------------------------------------------


def test_the_result_is_utc_and_not_merely_labelled_utc(
    resolver: ExchangeCalendarFuturesTradingSessionResolver,
) -> None:
    calendar = xcals.get_calendar("CME")
    expected = calendar.session_close(date(2026, 9, 15)).tz_convert("UTC")

    session = resolver.resolve(_ES, date(2026, 9, 15))

    assert session is not None
    assert session.closes_at.value == expected.strftime("%Y-%m-%dT%H:%M:%SZ")


def test_the_result_is_a_canonical_point_in_time(
    resolver: ExchangeCalendarFuturesTradingSessionResolver,
) -> None:
    session = resolver.resolve(_ES, date(2026, 9, 15))

    assert session is not None
    assert session.closes_at == PointInTime("2026-09-16T03:30:00+05:30")


def test_the_resolver_implements_the_application_port() -> None:
    assert issubclass(ExchangeCalendarFuturesTradingSessionResolver, FuturesTradingSessionResolver)
    assert isinstance(
        ExchangeCalendarFuturesTradingSessionResolver(), FuturesTradingSessionResolver
    )


def test_repeated_calls_are_stable(
    resolver: ExchangeCalendarFuturesTradingSessionResolver,
) -> None:
    """Calendars are cached; caching must not change the answer."""
    first = [resolver.resolve(p, date(2026, 9, 15)) for p in _SUPPORTED]
    second = [resolver.resolve(p, date(2026, 9, 15)) for p in _SUPPORTED]

    assert first == second
    assert len(set(first)) == 1


def test_infrastructure_no_longer_references_the_superseded_port() -> None:
    """Checked through the AST across all Infrastructure sources."""
    import northstar_infrastructure

    root = Path(northstar_infrastructure.__file__).parent
    offenders = []
    for path in root.rglob("*.py"):
        if "__pycache__" in path.parts:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                for alias in node.names:
                    if alias.name == "FuturesDailyBarCompletionResolver":
                        offenders.append(path.name)
                if node.module and "futures_daily_bar_completion" in node.module:
                    offenders.append(path.name)

    assert not offenders, f"still importing the superseded port: {sorted(set(offenders))}"


def test_the_superseded_adapter_is_gone() -> None:
    import northstar_infrastructure.market_data as market_data

    assert not hasattr(market_data, "ExchangeCalendarFuturesDailyBarCompletionResolver")
    assert "ExchangeCalendarFuturesDailyBarCompletionResolver" not in market_data.__all__
    assert "ExchangeCalendarFuturesTradingSessionResolver" in market_data.__all__
