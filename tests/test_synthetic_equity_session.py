"""EI-1A acceptance: synthetic equity session resolution and the 5-minute grid.

Everything here is invented. The reference is labelled synthetic and uses a
2031 calendar with deliberately non-exchange hours -- 10:00-12:00 and
13:00-14:30 IST with a break, and a one-hour special session at 18:00 IST.
None of these dates, closures or timings is an NSE fact, and no real exchange
calendar is loaded or consulted.

The chain exercised is: synthetic reference -> SyntheticEquityTradingSessionResolver
(Infrastructure) -> EquityTradingSession (Application port) ->
EquitySessionIntervalGrid (Application). A grid interval that has ended is a
time fact; it is not a candle and says nothing about candle finality.
"""

from __future__ import annotations

import ast
import inspect
import re
import socket
from datetime import date, datetime, time
from zoneinfo import ZoneInfo

import pytest
from northstar_application.application_services import (
    EquitySessionIntervalGrid,
    InvalidEquitySessionIntervalGridError,
)
from northstar_application.ports import (
    EquitySessionKind,
    EquitySessionResolutionError,
    EquityTradingSession,
    EquityTradingSessionResolver,
)
from northstar_core.foundation.value_objects import ExchangeCode, PointInTime, Timeframe

from northstar_infrastructure import market_data
from northstar_infrastructure.market_data import (
    InvalidSyntheticEquitySessionReferenceError,
    SyntheticEquityPhaseHours,
    SyntheticEquitySessionReference,
    SyntheticEquitySpecialSession,
    SyntheticEquityTradingSessionResolver,
)
from northstar_infrastructure.market_data import synthetic_equity_session as adapter_module

_NSE = ExchangeCode("NSE")
_IST = ZoneInfo("Asia/Kolkata")
_FIVE_MINUTES = Timeframe("5m")
_VERSION = "ei1a-acceptance-v1"

_MORNING = SyntheticEquityPhaseHours(opens=time(10, 0), closes=time(12, 0))
_AFTERNOON = SyntheticEquityPhaseHours(opens=time(13, 0), closes=time(14, 30))
_EVENING = SyntheticEquityPhaseHours(opens=time(18, 0), closes=time(19, 0))

# Invented 2031 fixture calendar.
_NORMAL_DAY = date(2031, 3, 3)
_SECOND_NORMAL_DAY = date(2031, 3, 4)
_CLOSED_DAY = date(2031, 3, 5)
_SPECIAL_ON_CLOSED_DAY = date(2031, 3, 6)
_SPECIAL_ON_NORMAL_DAY = date(2031, 3, 7)
_SPECIAL_WEEKEND_DAY = date(2031, 3, 8)
_UNTIMED_SPECIAL_DAY = date(2031, 3, 9)
_UNSTATED_WEEKDAY = date(2031, 3, 10)
_UNSTATED_WEEKEND_DAY = date(2031, 3, 15)


def _reference(**overrides: object) -> SyntheticEquitySessionReference:
    values: dict[str, object] = {
        "exchange_code": _NSE,
        "version": _VERSION,
        "timezone": "Asia/Kolkata",
        "loaded_years": frozenset({2031}),
        "normal_phases": (_MORNING, _AFTERNOON),
        "normal_sessions": frozenset(
            {_NORMAL_DAY, _SECOND_NORMAL_DAY, _SPECIAL_ON_NORMAL_DAY, _UNTIMED_SPECIAL_DAY}
        ),
        "closed_dates": {
            _CLOSED_DAY: "synthetic closure",
            _SPECIAL_ON_CLOSED_DAY: "synthetic closure overridden by a special session",
        },
        "special_sessions": {
            _SPECIAL_ON_CLOSED_DAY: SyntheticEquitySpecialSession("synthetic evening", (_EVENING,)),
            _SPECIAL_ON_NORMAL_DAY: SyntheticEquitySpecialSession(
                "synthetic shortened day", (_MORNING,)
            ),
            _SPECIAL_WEEKEND_DAY: SyntheticEquitySpecialSession("synthetic weekend", (_MORNING,)),
            _UNTIMED_SPECIAL_DAY: SyntheticEquitySpecialSession("synthetic untimed", None),
        },
    }
    values.update(overrides)
    return SyntheticEquitySessionReference(**values)  # type: ignore[arg-type]


def _resolver(**overrides: object) -> SyntheticEquityTradingSessionResolver:
    return SyntheticEquityTradingSessionResolver(_reference(**overrides))


def _resolve(trading_date: date) -> EquityTradingSession:
    session = _resolver().resolve(_NSE, trading_date)
    assert session is not None
    return session


def _phase_values(session: EquityTradingSession) -> list[tuple[str, str]]:
    return [(phase.opens_at.value, phase.closes_at.value) for phase in session.phases]


def _ist(instant: PointInTime) -> str:
    return datetime.fromisoformat(instant.value.replace("Z", "+00:00")).astimezone(_IST).isoformat()


@pytest.fixture(autouse=True)
def _refuse_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("EI-1A tests must not open network connections.")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)


# ----------------------------------------------------------------------
# A. Session lookup
# ----------------------------------------------------------------------


def test_resolver_implements_the_application_port() -> None:
    assert isinstance(_resolver(), EquityTradingSessionResolver)


def test_stated_normal_session_resolves_with_normal_phases_in_utc() -> None:
    session = _resolve(_NORMAL_DAY)

    assert session.exchange_code == _NSE
    assert session.trading_date == _NORMAL_DAY
    assert session.kind is EquitySessionKind.NORMAL
    assert _phase_values(session) == [
        ("2031-03-03T04:30:00Z", "2031-03-03T06:30:00Z"),
        ("2031-03-03T07:30:00Z", "2031-03-03T09:00:00Z"),
    ]
    assert session.reference == f"synthetic-reference {_VERSION}"


def test_phases_convert_back_to_ist_wall_clock_through_the_named_zone() -> None:
    session = _resolve(_NORMAL_DAY)

    assert [(_ist(phase.opens_at), _ist(phase.closes_at)) for phase in session.phases] == [
        ("2031-03-03T10:00:00+05:30", "2031-03-03T12:00:00+05:30"),
        ("2031-03-03T13:00:00+05:30", "2031-03-03T14:30:00+05:30"),
    ]


def test_stated_closed_date_returns_none() -> None:
    assert _resolver().resolve(_NSE, _CLOSED_DAY) is None


def test_special_session_uses_its_own_phases_not_normal_hours() -> None:
    session = _resolve(_SPECIAL_WEEKEND_DAY)

    assert session.kind is EquitySessionKind.SPECIAL
    assert _phase_values(session) == [("2031-03-08T04:30:00Z", "2031-03-08T06:30:00Z")]


def test_special_session_overrides_a_stated_closure() -> None:
    session = _resolve(_SPECIAL_ON_CLOSED_DAY)

    assert session.kind is EquitySessionKind.SPECIAL
    assert _phase_values(session) == [("2031-03-06T12:30:00Z", "2031-03-06T13:30:00Z")]


def test_special_session_overrides_a_stated_normal_session() -> None:
    session = _resolve(_SPECIAL_ON_NORMAL_DAY)

    assert session.kind is EquitySessionKind.SPECIAL
    assert len(session.phases) == 1


def test_special_session_without_timings_raises_instead_of_using_normal_hours() -> None:
    # The date is also stated as a normal session; the untimed special still wins.
    with pytest.raises(EquitySessionResolutionError, match="no established timings"):
        _resolver().resolve(_NSE, _UNTIMED_SPECIAL_DAY)


@pytest.mark.parametrize("trading_date", [_UNSTATED_WEEKDAY, _UNSTATED_WEEKEND_DAY])
def test_unstated_date_in_a_loaded_year_raises_whatever_its_weekday(trading_date: date) -> None:
    with pytest.raises(EquitySessionResolutionError, match="not stated"):
        _resolver().resolve(_NSE, trading_date)


@pytest.mark.parametrize("trading_date", [date(2030, 12, 31), date(2032, 1, 1)])
def test_unloaded_year_raises(trading_date: date) -> None:
    with pytest.raises(EquitySessionResolutionError, match="is not loaded"):
        _resolver().resolve(_NSE, trading_date)


def test_unloaded_year_raises_even_for_a_date_shape_the_reference_knows() -> None:
    with pytest.raises(EquitySessionResolutionError, match="is not loaded"):
        _resolver().resolve(_NSE, date(2032, 3, 3))


def test_other_venue_raises() -> None:
    with pytest.raises(EquitySessionResolutionError, match="Unsupported equity venue: BSE"):
        _resolver().resolve(ExchangeCode("BSE"), _NORMAL_DAY)


def test_resolver_refuses_wrong_input_types() -> None:
    resolver = _resolver()

    with pytest.raises(TypeError, match="exchange_code"):
        resolver.resolve("NSE", _NORMAL_DAY)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="trading_date"):
        resolver.resolve(_NSE, datetime(2031, 3, 3, 10, 0))  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="trading_date"):
        resolver.resolve(_NSE, "2031-03-03")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="SyntheticEquitySessionReference"):
        SyntheticEquityTradingSessionResolver(object())  # type: ignore[arg-type]


# ----------------------------------------------------------------------
# A. Invalid or inconsistent reference evidence
# ----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"normal_sessions": frozenset({date(2032, 1, 5)})}, "outside the loaded years"),
        ({"closed_dates": {date(2030, 1, 1): "x"}}, "outside the loaded years"),
        (
            {"special_sessions": {date(2032, 1, 1): SyntheticEquitySpecialSession("x", None)}},
            "outside the loaded years",
        ),
        ({"closed_dates": {_NORMAL_DAY: "contradiction"}}, "both normal sessions and closed"),
        ({"closed_dates": {_CLOSED_DAY: "  "}}, "must state a reason"),
        ({"closed_dates": {_CLOSED_DAY: None}}, "must state a reason"),
        ({"normal_sessions": frozenset({datetime(2031, 3, 3)})}, "plain date"),
        ({"special_sessions": {_SPECIAL_WEEKEND_DAY: (_MORNING,)}}, "SyntheticEquitySpecial"),
        ({"version": ""}, "version"),
        ({"timezone": "Not/AZone"}, "is unknown"),
        ({"timezone": ""}, "timezone"),
        ({"loaded_years": frozenset()}, "loaded_years"),
        ({"loaded_years": {2031}}, "loaded_years"),
        ({"loaded_years": frozenset({True})}, "loaded_years"),
        ({"normal_phases": ()}, "non-empty tuple"),
        ({"normal_phases": (_AFTERNOON, _MORNING)}, "separated by a break"),
        (
            {
                "normal_phases": (
                    _MORNING,
                    SyntheticEquityPhaseHours(opens=time(12, 0), closes=time(13, 0)),
                )
            },
            "separated by a break",
        ),
        ({"normal_sessions": {_NORMAL_DAY}}, "frozenset"),
        ({"closed_dates": [(_CLOSED_DAY, "x")]}, "mapping"),
        ({"exchange_code": "NSE"}, "ExchangeCode"),
    ],
)
def test_reference_refuses_incomplete_or_contradictory_evidence(
    overrides: dict[str, object], message: str
) -> None:
    with pytest.raises(InvalidSyntheticEquitySessionReferenceError, match=message):
        _reference(**overrides)


@pytest.mark.parametrize(
    ("opens", "closes", "message"),
    [
        (time(12, 0), time(10, 0), "before closes"),
        (time(10, 0), time(10, 0), "before closes"),
        (time(10, 0, tzinfo=_IST), time(12, 0), "naive"),
        (time(10, 0, 0, 500), time(12, 0), "fractional seconds"),
        ("10:00", time(12, 0), "must be a time"),
    ],
)
def test_phase_hours_refuse_invalid_boundaries(opens: object, closes: object, message: str) -> None:
    with pytest.raises(InvalidSyntheticEquitySessionReferenceError, match=message):
        SyntheticEquityPhaseHours(opens=opens, closes=closes)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("name", "phases", "message"),
    [
        ("", (_MORNING,), "name"),
        ("synthetic", (), "non-empty tuple"),
        ("synthetic", (_AFTERNOON, _MORNING), "separated by a break"),
    ],
)
def test_special_session_refuses_invalid_evidence(name: str, phases: object, message: str) -> None:
    with pytest.raises(InvalidSyntheticEquitySessionReferenceError, match=message):
        SyntheticEquitySpecialSession(name, phases)  # type: ignore[arg-type]


def test_reference_errors_are_session_resolution_errors() -> None:
    assert issubclass(InvalidSyntheticEquitySessionReferenceError, EquitySessionResolutionError)


def test_reference_is_frozen_against_later_mutation_of_its_inputs() -> None:
    closed = {_CLOSED_DAY: "synthetic closure"}
    reference = _reference(closed_dates=closed)
    closed[_NORMAL_DAY] = "added after construction"

    resolver = SyntheticEquityTradingSessionResolver(reference)

    assert resolver.resolve(_NSE, _NORMAL_DAY) is not None
    with pytest.raises(TypeError):
        reference.closed_dates[_CLOSED_DAY] = "mutated"  # type: ignore[index]


# ----------------------------------------------------------------------
# B. Five-minute grid of resolved sessions
# ----------------------------------------------------------------------


def test_grid_of_a_resolved_session_ends_on_ist_five_minute_boundaries() -> None:
    grid = EquitySessionIntervalGrid(session=_resolve(_NORMAL_DAY), timeframe=_FIVE_MINUTES)

    assert len(grid.intervals) == 42
    assert _ist(grid.intervals[0].point_in_time) == "2031-03-03T10:05:00+05:30"
    assert _ist(grid.intervals[23].point_in_time) == "2031-03-03T12:00:00+05:30"
    assert _ist(grid.intervals[24].opens_at) == "2031-03-03T13:00:00+05:30"
    assert _ist(grid.intervals[-1].point_in_time) == "2031-03-03T14:30:00+05:30"
    assert all(interval.point_in_time.value.endswith("Z") for interval in grid.intervals)


def test_grid_of_a_special_session_covers_only_its_phase() -> None:
    grid = EquitySessionIntervalGrid(
        session=_resolve(_SPECIAL_ON_CLOSED_DAY), timeframe=_FIVE_MINUTES
    )

    assert len(grid.intervals) == 12
    assert _ist(grid.intervals[0].opens_at) == "2031-03-06T18:00:00+05:30"
    assert _ist(grid.intervals[-1].point_in_time) == "2031-03-06T19:00:00+05:30"


def test_grid_completion_follows_the_explicit_as_of_instant() -> None:
    grid = EquitySessionIntervalGrid(session=_resolve(_NORMAL_DAY), timeframe=_FIVE_MINUTES)

    before_open = grid.ended_as_of(PointInTime("2031-03-03T09:59:59+05:30"))
    at_first_end = grid.ended_as_of(PointInTime("2031-03-03T10:05:00+05:30"))
    forming = grid.ended_as_of(PointInTime("2031-03-03T10:09:59+05:30"))
    in_break = grid.ended_as_of(PointInTime("2031-03-03T12:30:00+05:30"))
    after_close = grid.ended_as_of(PointInTime("2031-03-03T15:30:00+05:30"))

    assert before_open == ()
    assert len(at_first_end) == 1
    assert len(forming) == 1
    assert len(in_break) == 24
    assert after_close == grid.intervals


def test_a_misaligned_synthetic_phase_is_resolved_but_refused_by_the_grid() -> None:
    resolver = _resolver(
        normal_phases=(SyntheticEquityPhaseHours(opens=time(10, 2), closes=time(12, 0)),)
    )
    session = resolver.resolve(_NSE, _NORMAL_DAY)
    assert session is not None

    with pytest.raises(InvalidEquitySessionIntervalGridError, match="not aligned"):
        EquitySessionIntervalGrid(session=session, timeframe=_FIVE_MINUTES)


def test_consecutive_labels_are_independent_sessions_not_a_continuous_grid() -> None:
    first = EquitySessionIntervalGrid(session=_resolve(_NORMAL_DAY), timeframe=_FIVE_MINUTES)
    second = EquitySessionIntervalGrid(
        session=_resolve(_SECOND_NORMAL_DAY), timeframe=_FIVE_MINUTES
    )

    assert first.intervals[-1].closes_at.compare(second.intervals[0].opens_at) < 0
    assert second.intervals[0].opens_at != first.intervals[-1].closes_at
    # Nothing after the first session's close belongs to its grid.
    assert first.interval_ending_at(second.intervals[0].closes_at) is None


# ----------------------------------------------------------------------
# C. Point-in-time safety and determinism
# ----------------------------------------------------------------------


def test_identical_references_resolve_identically() -> None:
    dates = (_NORMAL_DAY, _CLOSED_DAY, _SPECIAL_ON_CLOSED_DAY, _SPECIAL_WEEKEND_DAY)

    first = [_resolver().resolve(_NSE, value) for value in dates]
    second = [_resolver().resolve(_NSE, value) for value in dates]

    assert first == second


def test_network_is_refused_in_this_module() -> None:
    with pytest.raises(AssertionError, match="network"):
        socket.create_connection(("example.invalid", 443))


def test_adapter_reads_no_clock_and_no_host_timezone() -> None:
    tree = ast.parse(inspect.getsource(adapter_module))
    called = {
        node.func.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
    }

    assert not called & {"now", "today", "utcnow", "time", "monotonic", "localtime", "tzset"}


# ----------------------------------------------------------------------
# D. Scope
# ----------------------------------------------------------------------


def test_adapter_embeds_no_exchange_dates_or_hours() -> None:
    source = inspect.getsource(adapter_module)
    tree = ast.parse(source)
    constructed = {
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }

    assert "date" not in constructed
    assert "time" not in constructed
    assert not re.search(r"\b(09|9):(00|15)\b|\b15:(30|40)\b", source)


def test_adapter_has_no_futures_session_expiry_short_execution_or_storage_coupling() -> None:
    imported: set[str] = set()
    for node in ast.walk(ast.parse(inspect.getsource(adapter_module))):
        if isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module)
        elif isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)

    for module in imported:
        assert "sqlite" not in module
        assert "broker" not in module
        assert "nse_futures" not in module
        assert "recommendations" not in module
    source = inspect.getsource(adapter_module)
    for word in ("expiry", "expiration", "SHORT", "broker"):
        assert word not in source


def test_market_data_package_exports_the_synthetic_resolver() -> None:
    for name in (
        "InvalidSyntheticEquitySessionReferenceError",
        "SyntheticEquityPhaseHours",
        "SyntheticEquitySessionReference",
        "SyntheticEquitySpecialSession",
        "SyntheticEquityTradingSessionResolver",
    ):
        assert name in market_data.__all__
