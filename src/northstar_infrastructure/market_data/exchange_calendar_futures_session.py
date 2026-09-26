"""Futures trading-session resolution backed by market calendars.

Daily futures bars are aggregated from sub-daily data, and that aggregation
needs both ends of a session. This adapter is the only place that knows where
those boundaries come from.

Why the boundaries are read rather than computed
------------------------------------------------
Two shortcuts look reasonable and are both wrong.

Deriving the close by adding a fixed duration to the open fails on early-close
sessions, which end roughly five hours sooner while the sessions around them
are unaffected:

    2026-09-15   open 2026-09-14T22:00Z   close 2026-09-15T22:00Z   (24h)
    2026-07-03   open 2026-07-02T22:00Z   close 2026-07-03T17:00Z   (19h)

Note which case the shortcut survives: a daylight-saving transition shifts open
and close together, so the UTC span stays exactly twenty-four hours. Only early
closes break it, so an implementation validated against DST alone would look
correct and still be wrong roughly a dozen sessions a year.

Deriving the open from the previous session's close fails across weekends and
holidays. Within a trading week the two coincide, which is what makes the
mistake tempting:

    2026-07-03   closes 2026-07-03T17:00Z
    2026-07-06   opens  2026-07-05T22:00Z   (two days and five hours later)

Chaining would make Monday's window start at Friday lunchtime and swallow the
whole weekend, including the Friday afternoon when the market had already shut
early. Trades falling in that gap would be attributed to the wrong session and
nothing downstream could detect it.

Both boundaries are therefore read from the calendar, for every session.

The session label
-----------------
``trading_date`` is the session's own name, which is what the calendar is keyed
by, and it is passed straight through. It is never derived by converting an
instant to a civil date: at 22:00Z the UTC date and the Chicago date disagree,
so that conversion silently produces off-by-one-session windows.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import ClassVar

import exchange_calendars as xcals
from exchange_calendars import errors as xcals_errors
from northstar_application.ports import (
    FuturesSessionResolutionError,
    FuturesTradingSession,
    FuturesTradingSessionResolver,
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


class ExchangeCalendarFuturesTradingSessionResolver(FuturesTradingSessionResolver):
    """Resolve futures trading-session windows using exchange calendars.

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

    # ------------------------------------------------------------------
    # Port
    # ------------------------------------------------------------------

    def resolve(
        self, product: FuturesProductReference, trading_date: date
    ) -> FuturesTradingSession | None:
        """Return the session labelled ``trading_date``, or None if not a session."""
        venue = self._venue(product)
        self._require_date(trading_date, "trading_date")
        calendar = self._calendar(venue, trading_date)

        try:
            if not calendar.is_session(trading_date):
                return None
            return self._session(calendar, trading_date, venue)
        except _CALENDAR_ERRORS as exc:
            raise FuturesSessionResolutionError(
                f"Futures session resolution failed for venue {venue} "
                f"on {trading_date.isoformat()}."
            ) from exc

    def sessions_in_range(
        self, product: FuturesProductReference, start_date: date, end_date: date
    ) -> tuple[FuturesTradingSession, ...]:
        """Return every session labelled within the inclusive range, ascending."""
        venue = self._venue(product)
        self._require_date(start_date, "start_date")
        self._require_date(end_date, "end_date")

        # The calendar returns an empty index for an inverted range rather than
        # complaining, which would quietly turn a caller's mistake into "no
        # sessions". The contract requires it to be refused.
        if start_date > end_date:
            raise FuturesSessionResolutionError(
                f"Futures session range start {start_date.isoformat()} is after "
                f"end {end_date.isoformat()}."
            )

        calendar = self._calendar(venue, start_date)

        try:
            # Enumeration is the calendar's own, so weekends, holidays and
            # closures are omitted by the authority that defines them rather
            # than by day-by-day arithmetic here.
            labels = calendar.sessions_in_range(start_date, end_date)
            return tuple(self._session(calendar, label.date(), venue) for label in labels)
        except _CALENDAR_ERRORS as exc:
            raise FuturesSessionResolutionError(
                f"Futures session resolution failed for venue {venue} between "
                f"{start_date.isoformat()} and {end_date.isoformat()}."
            ) from exc

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _venue(self, product: FuturesProductReference) -> str:
        if not isinstance(product, FuturesProductReference):
            raise TypeError(
                "ExchangeCalendarFuturesTradingSessionResolver product "
                "must be a FuturesProductReference."
            )
        venue = product.exchange_code.value
        if venue not in self._VENUE_CALENDARS:
            raise FuturesSessionResolutionError(
                f"Unsupported futures venue: {venue}. "
                f"Supported venues are {sorted(self._VENUE_CALENDARS)}."
            )
        return venue

    @staticmethod
    def _require_date(value: object, field_name: str) -> None:
        if isinstance(value, datetime) or not isinstance(value, date):
            raise TypeError(
                f"ExchangeCalendarFuturesTradingSessionResolver {field_name} "
                "must be a datetime.date."
            )

    def _calendar(self, venue: str, trading_date: date) -> xcals.ExchangeCalendar:
        calendar_name = self._VENUE_CALENDARS[venue]
        if calendar_name not in self._calendars:
            try:
                self._calendars[calendar_name] = xcals.get_calendar(calendar_name)
            except _CALENDAR_ERRORS as exc:
                raise FuturesSessionResolutionError(
                    f"Futures session calendar {calendar_name} is unavailable for venue "
                    f"{venue} on {trading_date.isoformat()}."
                ) from exc
        return self._calendars[calendar_name]

    def _session(
        self, calendar: xcals.ExchangeCalendar, trading_date: date, venue: str
    ) -> FuturesTradingSession:
        """Build one session window, reading both boundaries from the calendar."""
        return FuturesTradingSession(
            trading_date=trading_date,
            opens_at=self._instant(calendar.session_open(trading_date), venue, trading_date),
            closes_at=self._instant(calendar.session_close(trading_date), venue, trading_date),
        )

    @staticmethod
    def _instant(timestamp: object, venue: str, trading_date: date) -> PointInTime:
        """Convert a calendar timestamp to a canonical UTC PointInTime.

        The timestamp is converted to UTC explicitly rather than having "Z"
        appended to whatever it happened to carry. A naive timestamp is refused
        instead of being assumed to be UTC: guessing a timezone here would
        misplace every session by the venue's offset.
        """
        if getattr(timestamp, "tzinfo", None) is None:
            raise FuturesSessionResolutionError(
                f"Futures session calendar returned a timezone-naive boundary for venue "
                f"{venue} on {trading_date.isoformat()}."
            )

        try:
            in_utc = timestamp.tz_convert("UTC")
        except (AttributeError, TypeError, ValueError) as exc:
            raise FuturesSessionResolutionError(
                f"Futures session boundary could not be converted to UTC for venue "
                f"{venue} on {trading_date.isoformat()}."
            ) from exc

        # PointInTime carries at most microsecond precision. Nanoseconds are
        # refused rather than truncated, so a boundary can never be silently
        # moved.
        if getattr(in_utc, "nanosecond", 0) % 1000:
            raise FuturesSessionResolutionError(
                f"Futures session boundary carries sub-microsecond precision for venue "
                f"{venue} on {trading_date.isoformat()}."
            )

        microsecond = getattr(in_utc, "microsecond", 0)
        fraction = f".{microsecond:06d}" if microsecond else ""
        return PointInTime(
            f"{in_utc.year:04d}-{in_utc.month:02d}-{in_utc.day:02d}"
            f"T{in_utc.hour:02d}:{in_utc.minute:02d}:{in_utc.second:02d}{fraction}Z"
        )
