"""NSE option trading-session resolution from Northstar-owned reference data.

Option sessions are resolved from two cited sources and nothing else: the option
normal-market regimes in ``nse_option_session_reference``, and three day-level
facts of the NSE F&O calendar reused unchanged -- its trading holidays, its
special sessions and its loaded years. The futures session regimes and the
futures session resolver are not used: futures open earlier, with a pre-open
session NSE applies to futures only. Nothing is read from a calendar library, a
provider, the network or a clock.

How a range resolves
--------------------
Every date in the range is checked before any session is returned, so a range is
never partly answered:

1. A calendar year that has not been loaded fails closed.
2. A date before the product's first option regime fails closed; no earlier
   hours are inferred.
3. A published special session fails closed, whatever timings the calendar
   lists: they were sourced for futures, and option timings for them are not
   established.
4. A Saturday, a Sunday or a published trading holiday is not a session.
5. Every other date is a session with the regime's normal-market open and close,
   built in Asia/Kolkata and converted to UTC.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from northstar_application.ports import (
    OptionTradingSession,
    OptionTradingSessionResolutionError,
    OptionTradingSessionResolver,
)
from northstar_core.foundation.value_objects import PointInTime
from northstar_core.options import OptionProductReference

from northstar_infrastructure.market_data import (
    nse_futures_calendar_reference as calendar_reference,
)
from northstar_infrastructure.market_data import nse_option_session_reference as session_reference
from northstar_infrastructure.market_data.nse_option_session_reference import OptionSessionRegime

_IST = ZoneInfo("Asia/Kolkata")
_SATURDAY = 5


class NSEOptionTradingSessionResolver(OptionTradingSessionResolver):
    """Resolve NSE option trading sessions from cited regimes and the F&O calendar.

    The day-level calendar facts and the regimes are constructor-injected, with
    the production reference modules as defaults, so tests can use synthetic
    data. ``special_sessions`` may be a mapping keyed by date or any collection
    of dates.
    """

    def __init__(
        self,
        *,
        holidays: Iterable[date] = calendar_reference.HOLIDAYS,
        special_sessions: Iterable[date] = calendar_reference.SPECIAL_SESSIONS,
        loaded_years: Iterable[int] = calendar_reference.LOADED_YEARS,
        regimes: Iterable[OptionSessionRegime] = session_reference.REGIMES,
    ) -> None:
        self._holidays = _dates(holidays, "holidays")
        self._special_sessions = _dates(special_sessions, "special sessions")
        self._loaded_years = _years(loaded_years)
        self._regimes = session_reference.validate(regimes)

    def sessions_in_range(
        self, product: OptionProductReference, start_date: date, end_date: date
    ) -> tuple[OptionTradingSession, ...]:
        """Return every option session labelled within the inclusive range, ascending."""
        regimes = self._product_regimes(product)
        _require_date(start_date, "start_date")
        _require_date(end_date, "end_date")
        if start_date > end_date:
            raise OptionTradingSessionResolutionError(
                f"Option session range start {start_date.isoformat()} is after "
                f"end {end_date.isoformat()}."
            )
        for year in range(start_date.year, end_date.year + 1):
            if year not in self._loaded_years:
                raise OptionTradingSessionResolutionError(
                    f"NSE F&O calendar year {year} is not loaded. "
                    f"Loaded years are {sorted(self._loaded_years)}."
                )

        sessions: list[OptionTradingSession] = []
        day = start_date
        while day <= end_date:
            regime = _in_force(regimes, day)
            if regime is None:
                raise OptionTradingSessionResolutionError(
                    f"No option session regime for {product} is in force on {day.isoformat()}; "
                    f"the first encoded regime is effective from "
                    f"{regimes[0].effective_from.isoformat()}."
                )
            if day in self._special_sessions:
                raise OptionTradingSessionResolutionError(
                    f"{day.isoformat()} is an NSE special session whose option timings are not "
                    "established; option session resolution fails closed."
                )
            if day.weekday() < _SATURDAY and day not in self._holidays:
                sessions.append(_session(day, regime.opens, regime.closes))
            day += timedelta(days=1)
        return tuple(sessions)

    def _product_regimes(self, product: object) -> list[OptionSessionRegime]:
        if not isinstance(product, OptionProductReference):
            raise TypeError(
                "NSEOptionTradingSessionResolver product must be an OptionProductReference."
            )
        regimes = [
            regime
            for regime in self._regimes
            if regime.product_code == product.product_code.value
            and regime.exchange_code == product.exchange_code.value
        ]
        if not regimes:
            supported = sorted({f"{r.product_code}@{r.exchange_code}" for r in self._regimes})
            raise OptionTradingSessionResolutionError(
                f"Unsupported option product for NSE sessions: {product}. "
                f"Supported products are {supported}."
            )
        return regimes


def _in_force(regimes: list[OptionSessionRegime], day: date) -> OptionSessionRegime | None:
    in_force = [regime for regime in regimes if regime.effective_from <= day]
    return in_force[-1] if in_force else None


def _session(day: date, opens: time, closes: time) -> OptionTradingSession:
    return OptionTradingSession(
        trading_date=day, opens_at=_utc(day, opens), closes_at=_utc(day, closes)
    )


def _utc(day: date, clock: time) -> PointInTime:
    """Convert an IST wall-clock time on ``day`` to a canonical UTC PointInTime."""
    instant = datetime.combine(day, clock, tzinfo=_IST).astimezone(UTC)
    return PointInTime(instant.strftime("%Y-%m-%dT%H:%M:%SZ"))


def _require_date(value: object, name: str) -> None:
    if isinstance(value, datetime) or not isinstance(value, date):
        raise TypeError(f"NSEOptionTradingSessionResolver {name} must be a plain date.")


def _dates(values: Iterable[date], name: str) -> frozenset[date]:
    dates = frozenset(values)
    for value in dates:
        if isinstance(value, datetime) or not isinstance(value, date):
            raise TypeError(f"NSEOptionTradingSessionResolver {name} must be plain dates.")
    return dates


def _years(values: Iterable[int]) -> frozenset[int]:
    years = frozenset(values)
    for value in years:
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError("NSEOptionTradingSessionResolver loaded years must be integers.")
    return years
