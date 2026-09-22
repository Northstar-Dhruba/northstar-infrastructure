"""Futures daily-session completion resolution backed by market calendars.

A daily FuturesOHLCVBar is stamped at the instant its trading session
completes, and that instant comes from the venue's calendar rather than from
any provider payload. This adapter is the only place that knows how to obtain
it.

Why the completion instant is read rather than computed
-------------------------------------------------------
A CME session runs from 17:00 Chicago to 17:00 Chicago, so it opens on the
previous civil date and normally spans twenty-four hours. It is tempting to
treat that as a rule and derive the close by adding a fixed duration to the
open. That is wrong on early-close sessions, which end roughly five hours
sooner while the sessions around them are unaffected:

    2026-09-15   open 2026-09-14T22:00Z   close 2026-09-15T22:00Z   (24h)
    2026-07-03   open 2026-07-02T22:00Z   close 2026-07-03T17:00Z   (19h)

Note which case the shortcut survives: a daylight-saving transition shifts the
open and the close together, so the UTC span stays exactly twenty-four hours
and the arithmetic still agrees. Only early closes break it. An implementation
validated against DST alone would therefore look correct and still be wrong on
roughly a dozen sessions a year, stamping bars hours after the data was
actually available. The close is read from the calendar, never computed.

The session label
-----------------
``trading_date`` is the session's own name, which is what the calendar is
keyed by, and it is passed straight through. It is never derived by converting
an instant to a civil date: at 22:00Z the UTC date and the Chicago date
disagree, so that conversion silently produces off-by-one-session bars.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import ClassVar

import exchange_calendars as xcals
from exchange_calendars import errors as xcals_errors
from northstar_application.ports import (
    FuturesDailyBarCompletionResolver,
    FuturesSessionResolutionError,
)
from northstar_core.foundation.value_objects import PointInTime
from northstar_core.futures import FuturesProductReference

# Errors the calendar library raises for conditions we cannot answer. Some
# derive from CalendarError and some directly from ValueError, so both are
# named explicitly rather than catching ValueError or Exception broadly, which
# would swallow programming errors along with them.
_CALENDAR_ERRORS = (
    xcals_errors.CalendarError,
    xcals_errors.DateOutOfBounds,
    xcals_errors.NotSessionError,
)


class ExchangeCalendarFuturesDailyBarCompletionResolver(FuturesDailyBarCompletionResolver):
    """Resolve futures daily session completion instants using exchange calendars.

    Each supported Northstar venue is mapped to its own calendar identifier:

    - CME   -> CME
    - CBOT  -> CBOT
    - NYMEX -> NYMEX
    - COMEX -> COMEX

    Those four are aliases of one CMES calendar in the current library version,
    so they return identical schedules today. They are still mapped separately
    and deliberately. Collapsing them to a single hard-coded calendar name
    would record one decision where the domain makes four, and would have to be
    unpicked if the library ever gives a venue its own schedule -- whereas this
    map would then need no change at all.

    Only ``product.exchange_code`` is read. The product code is deliberately
    not inspected, because no venue here has a per-product session schedule;
    the port takes the product so that one could be added later without
    changing the Application boundary.
    """

    _VENUE_CALENDARS: ClassVar[dict[str, str]] = {
        "CME": "CME",
        "CBOT": "CBOT",
        "NYMEX": "NYMEX",
        "COMEX": "COMEX",
    }

    def __init__(self) -> None:
        self._calendars: dict[str, xcals.ExchangeCalendar] = {}

    def resolve_completion(
        self, product: FuturesProductReference, trading_date: date
    ) -> PointInTime | None:
        """Return the session completion PointInTime, or None if not a session."""
        if not isinstance(product, FuturesProductReference):
            raise TypeError(
                "ExchangeCalendarFuturesDailyBarCompletionResolver product "
                "must be a FuturesProductReference."
            )
        if isinstance(trading_date, datetime) or not isinstance(trading_date, date):
            raise TypeError(
                "ExchangeCalendarFuturesDailyBarCompletionResolver trading_date "
                "must be a datetime.date."
            )

        venue = product.exchange_code.value
        calendar_name = self._VENUE_CALENDARS.get(venue)
        if calendar_name is None:
            raise FuturesSessionResolutionError(
                f"Unsupported futures venue: {venue}. "
                f"Supported venues are {sorted(self._VENUE_CALENDARS)}."
            )

        calendar = self._calendar(calendar_name, venue, trading_date)

        try:
            if not calendar.is_session(trading_date):
                return None
            session_close = calendar.session_close(trading_date)
        except _CALENDAR_ERRORS as exc:
            raise FuturesSessionResolutionError(
                f"Futures session resolution failed for venue {venue} "
                f"on {trading_date.isoformat()}."
            ) from exc

        return self._completion_instant(session_close, venue, trading_date)

    def _calendar(
        self, calendar_name: str, venue: str, trading_date: date
    ) -> xcals.ExchangeCalendar:
        if calendar_name not in self._calendars:
            try:
                self._calendars[calendar_name] = xcals.get_calendar(calendar_name)
            except _CALENDAR_ERRORS as exc:
                raise FuturesSessionResolutionError(
                    f"Futures session calendar {calendar_name} is unavailable for venue "
                    f"{venue} on {trading_date.isoformat()}."
                ) from exc
        return self._calendars[calendar_name]

    @staticmethod
    def _completion_instant(session_close: object, venue: str, trading_date: date) -> PointInTime:
        """Convert a calendar timestamp to a canonical UTC PointInTime.

        The timestamp is converted to UTC explicitly rather than having "Z"
        appended to whatever it happened to carry. A naive timestamp is refused
        instead of being assumed to be UTC: guessing a timezone here would
        misplace every bar by the venue's offset.
        """
        tzinfo = getattr(session_close, "tzinfo", None)
        if tzinfo is None:
            raise FuturesSessionResolutionError(
                f"Futures session calendar returned a timezone-naive close for venue "
                f"{venue} on {trading_date.isoformat()}."
            )

        try:
            in_utc = session_close.tz_convert("UTC")
        except (AttributeError, TypeError, ValueError) as exc:
            raise FuturesSessionResolutionError(
                f"Futures session close could not be converted to UTC for venue "
                f"{venue} on {trading_date.isoformat()}."
            ) from exc

        # PointInTime carries at most microsecond precision. Nanoseconds are
        # refused rather than truncated, so a session close can never be
        # silently moved.
        if getattr(in_utc, "nanosecond", 0) % 1000:
            raise FuturesSessionResolutionError(
                f"Futures session close carries sub-microsecond precision for venue "
                f"{venue} on {trading_date.isoformat()}."
            )

        microsecond = getattr(in_utc, "microsecond", 0)
        fraction = f".{microsecond:06d}" if microsecond else ""
        return PointInTime(
            f"{in_utc.year:04d}-{in_utc.month:02d}-{in_utc.day:02d}"
            f"T{in_utc.hour:02d}:{in_utc.minute:02d}:{in_utc.second:02d}{fraction}Z"
        )
