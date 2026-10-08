"""Tests for NSE option expiration resolution.

Real-date cases run against the production NSE F&O calendar reference and the
encoded NIFTY@NSE rule epoch. Fail-closed paths that the real data cannot reach
use clearly synthetic, constructor-injected calendar facts; no real holiday or
special session is invented.
"""

from __future__ import annotations

import ast
import socket
from dataclasses import replace
from datetime import date
from pathlib import Path

import pytest
from northstar_application.ports import (
    OptionExpirationResolutionError,
    OptionExpirationResolver,
    ResolvedOptionExpiration,
)
from northstar_core.derivatives import ExpirationDate
from northstar_core.foundation.value_objects import ExchangeCode, Symbol
from northstar_core.futures import FuturesProductReference
from northstar_core.options import OptionProductReference

import northstar_infrastructure.market_data as market_data
import northstar_infrastructure.market_data.nse_option_expiration as module
from northstar_infrastructure.market_data import NSEOptionExpirationResolver
from northstar_infrastructure.market_data import nse_futures_calendar_reference as calendar
from northstar_infrastructure.market_data import nse_option_expiry_reference as reference

_NIFTY = OptionProductReference(Symbol("NIFTY"), ExchangeCode("NSE"))
_SOURCE = reference.EPOCHS[0].source


@pytest.fixture
def resolver() -> NSEOptionExpirationResolver:
    return NSEOptionExpirationResolver()


def _synthetic(**overrides) -> NSEOptionExpirationResolver:
    """A resolver over the real calendar with some facts replaced by synthetic ones."""
    facts = {
        "holidays": calendar.HOLIDAYS,
        "special_sessions": calendar.SPECIAL_SESSIONS,
        "loaded_years": calendar.LOADED_YEARS,
        "epochs": reference.EPOCHS,
    }
    facts.update(overrides)
    return NSEOptionExpirationResolver(**facts)


def _assert_resolved(
    resolved: ResolvedOptionExpiration, nominal: str, expiration: str, adjusted: bool
) -> None:
    assert resolved == ResolvedOptionExpiration(
        _NIFTY, date.fromisoformat(nominal), ExpirationDate(expiration), _SOURCE
    )
    assert resolved.is_adjusted is adjusted


# ---------------------------------------------------------------------------
# Real-date matrix: monthly
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("year", "month", "nominal"),
    [
        (2026, 1, "2026-01-27"),
        (2026, 10, "2026-10-27"),
        (2026, 2, "2026-02-24"),
        (2025, 9, "2025-09-30"),
        (2025, 11, "2025-11-25"),
        (2025, 12, "2025-12-30"),
        (2026, 12, "2026-12-29"),
    ],
)
def test_a_monthly_expiry_on_a_trading_tuesday_is_not_adjusted(
    resolver: NSEOptionExpirationResolver, year: int, month: int, nominal: str
) -> None:
    _assert_resolved(resolver.monthly_expiration(_NIFTY, year, month), nominal, nominal, False)


@pytest.mark.parametrize(
    ("month", "nominal", "expiration"),
    [
        (3, "2026-03-31", "2026-03-30"),  # Shri Mahavir Jayanti, NSE/FAOP/71777
        (11, "2026-11-24", "2026-11-23"),  # Prakash Gurpurb Sri Guru Nanak Dev, NSE/FAOP/71777
    ],
)
def test_a_monthly_expiry_on_a_holiday_moves_to_the_previous_trading_day(
    resolver: NSEOptionExpirationResolver, month: int, nominal: str, expiration: str
) -> None:
    _assert_resolved(resolver.monthly_expiration(_NIFTY, 2026, month), nominal, expiration, True)


# ---------------------------------------------------------------------------
# Real-date matrix: weekly
# ---------------------------------------------------------------------------


def test_a_weekly_expiry_on_a_trading_tuesday_is_not_adjusted(
    resolver: NSEOptionExpirationResolver,
) -> None:
    _assert_resolved(
        resolver.weekly_expiration(_NIFTY, 2026, 42), "2026-10-13", "2026-10-13", False
    )


@pytest.mark.parametrize(
    ("week", "nominal", "expiration"),
    [
        (10, "2026-03-03", "2026-03-02"),  # Holi
        (16, "2026-04-14", "2026-04-13"),  # Dr. Baba Saheb Ambedkar Jayanti
        (43, "2026-10-20", "2026-10-19"),  # Dussehra
        (46, "2026-11-10", "2026-11-09"),  # Diwali-Balipratipada
    ],
)
def test_a_weekly_expiry_on_a_holiday_moves_to_the_previous_trading_day(
    resolver: NSEOptionExpirationResolver, week: int, nominal: str, expiration: str
) -> None:
    _assert_resolved(resolver.weekly_expiration(_NIFTY, 2026, week), nominal, expiration, True)


def test_the_first_weekly_expiry_under_the_rule(resolver: NSEOptionExpirationResolver) -> None:
    _assert_resolved(
        resolver.weekly_expiration(_NIFTY, 2025, 36), "2025-09-02", "2025-09-02", False
    )


def test_a_weekly_and_a_monthly_expiry_can_resolve_to_one_date(
    resolver: NSEOptionExpirationResolver,
) -> None:
    weekly = resolver.weekly_expiration(_NIFTY, 2026, 44)
    monthly = resolver.monthly_expiration(_NIFTY, 2026, 10)

    assert weekly.expiration_date == monthly.expiration_date == ExpirationDate("2026-10-27")
    assert weekly == monthly


def test_resolution_is_deterministic(resolver: NSEOptionExpirationResolver) -> None:
    first = [resolver.weekly_expiration(_NIFTY, 2026, week) for week in range(1, 54)]
    second = [
        NSEOptionExpirationResolver().weekly_expiration(_NIFTY, 2026, w) for w in range(1, 54)
    ]

    assert first == second
    assert len({resolved.expiration_date for resolved in first}) == 53


# ---------------------------------------------------------------------------
# ISO week and year boundaries
# ---------------------------------------------------------------------------


def test_iso_week_one_of_2026_is_in_december_2025(resolver: NSEOptionExpirationResolver) -> None:
    resolved = resolver.weekly_expiration(_NIFTY, 2026, 1)

    _assert_resolved(resolved, "2025-12-30", "2025-12-30", False)
    assert resolved == resolver.monthly_expiration(_NIFTY, 2025, 12)


def test_iso_week_53_of_2026_exists(resolver: NSEOptionExpirationResolver) -> None:
    _assert_resolved(
        resolver.weekly_expiration(_NIFTY, 2026, 53), "2026-12-29", "2026-12-29", False
    )


@pytest.mark.parametrize(("year", "week"), [(2025, 53), (2026, 0), (2026, 54), (2026, -1)])
def test_an_iso_week_the_year_does_not_have_is_rejected(
    resolver: NSEOptionExpirationResolver, year: int, week: int
) -> None:
    with pytest.raises(OptionExpirationResolutionError, match=f"ISO week {week} of {year}"):
        resolver.weekly_expiration(_NIFTY, year, week)


@pytest.mark.parametrize("month", [0, 13, -1])
def test_a_month_outside_the_year_is_rejected(
    resolver: NSEOptionExpirationResolver, month: int
) -> None:
    with pytest.raises(OptionExpirationResolutionError, match=f"Month {month} of 2026"):
        resolver.monthly_expiration(_NIFTY, 2026, month)


@pytest.mark.parametrize("value", [True, False, "42", 42.0, None])
def test_non_integer_coordinates_are_rejected(
    resolver: NSEOptionExpirationResolver, value: object
) -> None:
    with pytest.raises(OptionExpirationResolutionError, match="must be an integer"):
        resolver.weekly_expiration(_NIFTY, 2026, value)  # type: ignore[arg-type]
    with pytest.raises(OptionExpirationResolutionError, match="must be an integer"):
        resolver.weekly_expiration(_NIFTY, value, 42)  # type: ignore[arg-type]
    with pytest.raises(OptionExpirationResolutionError, match="must be an integer"):
        resolver.monthly_expiration(_NIFTY, 2026, value)  # type: ignore[arg-type]
    with pytest.raises(OptionExpirationResolutionError, match="must be an integer"):
        resolver.monthly_expiration(_NIFTY, value, 10)  # type: ignore[arg-type]


def test_leap_year_february_names_its_last_tuesday() -> None:
    """2028-02-29 is a Tuesday; 2028 is synthetically loaded here, holiday-free."""
    resolved = _synthetic(loaded_years={2028}).monthly_expiration(_NIFTY, 2028, 2)

    _assert_resolved(resolved, "2028-02-29", "2028-02-29", False)


# ---------------------------------------------------------------------------
# Coverage and rule epochs
# ---------------------------------------------------------------------------


def test_an_unloaded_year_fails_closed(resolver: NSEOptionExpirationResolver) -> None:
    with pytest.raises(OptionExpirationResolutionError, match="year 2027 is not loaded"):
        resolver.weekly_expiration(_NIFTY, 2027, 1)
    with pytest.raises(OptionExpirationResolutionError, match="year 2027 is not loaded"):
        resolver.monthly_expiration(_NIFTY, 2027, 1)


def test_a_walk_into_an_unloaded_year_fails_closed() -> None:
    """Synthetic: 2026-01-01 .. 2026-01-06 holidays push the walk into unloaded 2025."""
    holidays = calendar.HOLIDAYS | {date(2026, 1, day) for day in (1, 2, 5, 6)}
    resolver = _synthetic(holidays=holidays, loaded_years={2026})

    with pytest.raises(OptionExpirationResolutionError, match="year 2025 is not loaded"):
        resolver.weekly_expiration(_NIFTY, 2026, 2)


@pytest.mark.parametrize(
    ("operation", "coordinates"),
    [
        ("monthly", (2025, 8)),
        ("monthly", (2024, 12)),
        ("weekly", (2025, 35)),
        ("weekly", (2024, 1)),
    ],
)
def test_a_period_before_the_rule_epoch_fails_closed(
    resolver: NSEOptionExpirationResolver, operation: str, coordinates: tuple[int, int]
) -> None:
    resolve = getattr(resolver, f"{operation}_expiration")

    with pytest.raises(OptionExpirationResolutionError, match="effective from 2025-09-01"):
        resolve(_NIFTY, *coordinates)


def test_a_period_straddling_two_epochs_fails_closed() -> None:
    """Synthetic second epoch starting mid-week and mid-month."""
    later = replace(reference.EPOCHS[0], effective_from=date(2026, 10, 14), source="synthetic")
    resolver = _synthetic(epochs=(*reference.EPOCHS, later))

    with pytest.raises(OptionExpirationResolutionError, match="spans two"):
        resolver.weekly_expiration(_NIFTY, 2026, 42)
    with pytest.raises(OptionExpirationResolutionError, match="spans two"):
        resolver.monthly_expiration(_NIFTY, 2026, 10)
    assert resolver.weekly_expiration(_NIFTY, 2026, 41).rule_source == _SOURCE
    assert resolver.weekly_expiration(_NIFTY, 2026, 43).rule_source == "synthetic"


def test_an_adjustment_before_the_rule_epoch_fails_closed() -> None:
    """Synthetic: an epoch from Monday 2026-03-02, and that Monday made a holiday."""
    epoch = replace(reference.EPOCHS[0], effective_from=date(2026, 3, 2))
    resolver = _synthetic(holidays=calendar.HOLIDAYS | {date(2026, 3, 2)}, epochs=(epoch,))

    with pytest.raises(OptionExpirationResolutionError, match="before its rule's effective date"):
        resolver.weekly_expiration(_NIFTY, 2026, 10)


def test_the_weekday_comes_from_the_epoch_not_a_constant() -> None:
    """Synthetic Thursday epoch: the resolver applies whatever the epoch states."""
    thursday = replace(reference.EPOCHS[0], weekly_weekday=3, monthly_weekday=3)
    resolver = _synthetic(epochs=(thursday,))

    assert resolver.weekly_expiration(_NIFTY, 2026, 42).nominal_date == date(2026, 10, 15)
    assert resolver.monthly_expiration(_NIFTY, 2026, 10).nominal_date == date(2026, 10, 29)


# ---------------------------------------------------------------------------
# Special sessions fail closed
# ---------------------------------------------------------------------------


def test_the_2025_10_21_muhurat_holiday_fails_closed(resolver: NSEOptionExpirationResolver) -> None:
    """A trading holiday with a Muhurat session (NSE/FAOP/70320): not resolved to 2025-10-20."""
    assert date(2025, 10, 21) in calendar.HOLIDAYS
    assert date(2025, 10, 21) in calendar.SPECIAL_SESSIONS

    with pytest.raises(OptionExpirationResolutionError, match="2025-10-21, an NSE special session"):
        resolver.weekly_expiration(_NIFTY, 2025, 43)


def test_an_unresolved_special_session_not_reached_has_no_effect(
    resolver: NSEOptionExpirationResolver,
) -> None:
    """2026-11-08 (Muhurat, timings unpublished) lies two days before 2026-11-10."""
    assert calendar.SPECIAL_SESSIONS[date(2026, 11, 8)].opens is None
    without_it = _synthetic(
        special_sessions={day for day in calendar.SPECIAL_SESSIONS if day != date(2026, 11, 8)}
    )

    for operation, coordinates in (("weekly", (2026, 46)), ("monthly", (2026, 11))):
        with_record = getattr(resolver, f"{operation}_expiration")(_NIFTY, *coordinates)
        without_record = getattr(without_it, f"{operation}_expiration")(_NIFTY, *coordinates)
        assert with_record == without_record

    _assert_resolved(resolver.weekly_expiration(_NIFTY, 2026, 46), "2026-11-10", "2026-11-09", True)


def test_the_same_unresolved_special_session_fails_closed_once_reached() -> None:
    """Synthetic: making 2026-11-09 a holiday walks the 2026-11-10 expiry onto 2026-11-08."""
    resolver = _synthetic(holidays=calendar.HOLIDAYS | {date(2026, 11, 9)})

    with pytest.raises(OptionExpirationResolutionError, match="2026-11-08, an NSE special session"):
        resolver.weekly_expiration(_NIFTY, 2026, 46)


@pytest.mark.parametrize(
    ("special", "holidays", "week"),
    [
        (date(2026, 10, 13), set(), 42),  # on a regular nominal Tuesday: not returned
        (date(2026, 3, 2), set(), 10),  # on the day the walk would stop: not returned
        (date(2026, 3, 1), {date(2026, 3, 2)}, 10),  # on a Sunday mid-walk: not stepped through
        (date(2026, 10, 19), set(), 43),  # on the Monday after a holiday Tuesday
    ],
    ids=["nominal-tuesday", "walk-stop", "mid-walk-sunday", "after-holiday"],
)
def test_any_special_session_the_walk_reaches_fails_closed(
    special: date, holidays: set[date], week: int
) -> None:
    resolver = _synthetic(
        holidays=calendar.HOLIDAYS | holidays,
        special_sessions={*calendar.SPECIAL_SESSIONS, special},
    )

    with pytest.raises(OptionExpirationResolutionError, match=f"{special.isoformat()}, an NSE"):
        resolver.weekly_expiration(_NIFTY, 2026, week)


# ---------------------------------------------------------------------------
# The defensive walk bound
# ---------------------------------------------------------------------------


def test_the_walk_bound_is_a_safety_limit_not_an_nse_rule() -> None:
    assert module.MAX_WALK_BACK_DAYS == 21
    assert "not an NSE rule" in (module.__doc__ or "")
    assert not hasattr(reference.EPOCHS[0], "max_walk_back_days")


def test_an_exhausted_walk_fails_closed_rather_than_guessing() -> None:
    """Synthetic: every weekday for five weeks before the nominal date is a holiday."""
    nominal = date(2026, 10, 27)
    holidays = {date.fromordinal(nominal.toordinal() - offset) for offset in range(0, 36)}
    resolver = _synthetic(holidays={day for day in holidays if day.weekday() < 5})

    with pytest.raises(OptionExpirationResolutionError, match="within 21 days"):
        resolver.monthly_expiration(_NIFTY, 2026, 10)


# ---------------------------------------------------------------------------
# Products
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "product",
    [
        OptionProductReference(Symbol("BANKNIFTY"), ExchangeCode("NSE")),
        OptionProductReference(Symbol("NIFTY"), ExchangeCode("BSE")),
        OptionProductReference(Symbol("SENSEX"), ExchangeCode("BSE")),
    ],
    ids=["banknifty-nse", "nifty-bse", "sensex-bse"],
)
def test_unsupported_products_fail_closed(
    resolver: NSEOptionExpirationResolver, product: OptionProductReference
) -> None:
    with pytest.raises(OptionExpirationResolutionError, match="Unsupported option product"):
        resolver.weekly_expiration(product, 2026, 42)
    with pytest.raises(OptionExpirationResolutionError, match="Unsupported option product"):
        resolver.monthly_expiration(product, 2026, 10)


@pytest.mark.parametrize(
    "product",
    [FuturesProductReference(Symbol("NIFTY"), ExchangeCode("NSE")), "NIFTY@NSE", None],
    ids=["futures-product", "text", "none"],
)
def test_a_non_option_product_is_rejected(
    resolver: NSEOptionExpirationResolver, product: object
) -> None:
    with pytest.raises(TypeError, match="must be an OptionProductReference"):
        resolver.weekly_expiration(product, 2026, 42)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="must be an OptionProductReference"):
        resolver.monthly_expiration(product, 2026, 10)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Construction, port and exports
# ---------------------------------------------------------------------------


def test_the_resolver_implements_the_application_port(
    resolver: NSEOptionExpirationResolver,
) -> None:
    assert isinstance(resolver, OptionExpirationResolver)
    assert "NSEOptionExpirationResolver" in market_data.__all__


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("holidays", {"2026-10-20"}),
        ("special_sessions", {20261108}),
        ("loaded_years", {"2026"}),
        ("loaded_years", {True}),
    ],
)
def test_injected_calendar_facts_are_type_checked(name: str, value: object) -> None:
    with pytest.raises(TypeError):
        _synthetic(**{name: value})


def test_injected_epochs_are_validated() -> None:
    with pytest.raises(OptionExpirationResolutionError, match="no expiration rule epoch"):
        _synthetic(epochs=())


def test_a_resolution_never_generates_a_contract(resolver: NSEOptionExpirationResolver) -> None:
    resolved = resolver.weekly_expiration(_NIFTY, 2026, 42)

    for absent in ("contract", "contracts", "strike", "strikes", "right", "lot_size", "listed"):
        assert not hasattr(resolved, absent)


# ---------------------------------------------------------------------------
# Determinism and boundaries
# ---------------------------------------------------------------------------


def _tree() -> ast.Module:
    return ast.parse(Path(module.__file__).read_text(encoding="utf-8"))


def test_the_resolver_reads_no_clock() -> None:
    for node in ast.walk(_tree()):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            assert node.func.attr not in {"now", "utcnow", "today", "time", "monotonic"}


def test_the_resolver_imports_no_network_provider_timezone_or_futures_type() -> None:
    tree = _tree()
    modules = {
        node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.module
    } | {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    names = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
        for alias in node.names
    }

    for name in modules:
        assert name.split(".")[0] not in {
            "time",
            "zoneinfo",
            "socket",
            "requests",
            "httpx",
            "urllib",
            "sqlite3",
            "exchange_calendars",
            "databento",
        }
        assert "upstox" not in name
        assert not name.startswith("northstar_core.futures")
    assert not [name for name in names if name.startswith("Futures")]
    assert "nse_futures_session" not in " ".join(modules)


def test_only_day_level_calendar_facts_are_read() -> None:
    """HOLIDAYS, SPECIAL_SESSIONS and LOADED_YEARS only: never REGIMES or session hours."""
    read = {
        node.attr
        for node in ast.walk(_tree())
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "calendar_reference"
    }

    assert read == {"HOLIDAYS", "SPECIAL_SESSIONS", "LOADED_YEARS"}


def test_resolution_uses_no_network(
    resolver: NSEOptionExpirationResolver, monkeypatch: pytest.MonkeyPatch
) -> None:
    def refuse(*args, **kwargs):
        raise AssertionError("network access attempted")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)

    assert resolver.monthly_expiration(_NIFTY, 2026, 11).expiration_date == ExpirationDate(
        "2026-11-23"
    )


@pytest.mark.parametrize(
    "zone", ["UTC", "Asia/Kolkata", "America/Los_Angeles", "Pacific/Kiritimati"]
)
def test_resolution_is_independent_of_the_host_timezone(
    monkeypatch: pytest.MonkeyPatch, zone: str
) -> None:
    monkeypatch.setenv("TZ", zone)

    resolver = NSEOptionExpirationResolver()
    assert resolver.weekly_expiration(_NIFTY, 2026, 43).expiration_date == ExpirationDate(
        "2026-10-19"
    )
    assert resolver.monthly_expiration(_NIFTY, 2026, 3).expiration_date == ExpirationDate(
        "2026-03-30"
    )
