"""NSE futures trading-session resolution from Northstar-owned reference data.

NSE sessions are read from a versioned, hand-verified reference module --
``nse_futures_calendar_reference`` -- never from a calendar library and never
from the network. The calendar library Northstar uses for CME has no NSE
calendar, and its nearest Indian calendar, XBOM, is BSE's: BSE and NSE holidays
and special sessions are separate exchange decisions, so substituting one for
the other would be wrong on exactly the days that matter.

How one date resolves
---------------------
In this order, so that each rule can only be overridden by a more specific one:

1. A calendar year that has not been loaded raises. Its holidays, special
   sessions and hours are unknown, so an ordinary weekday there is not assumed
   to be a session.
2. A published special session is a session, even on a weekend or a holiday
   (the Sunday Budget session, a Muhurat session on a Diwali holiday). A special
   session whose timings NSE has not yet notified raises rather than falling
   back to normal hours.
3. A Saturday or Sunday is not a session.
4. A published trading holiday is not a session.
5. Every other date is a session with the normal-market hours of the regime in
   force on that date.

The session label and the IST day
---------------------------------
An NSE session opens and closes on the same Indian civil date, so the label is
that date and both boundaries are built from it in Asia/Kolkata and converted
to UTC explicitly. India observes no daylight saving, so the UTC offset is
always +05:30; the conversion still goes through the named zone rather than a
hard-coded offset, so the rule is stated once, where the zone database owns it.
Nothing here reads the host's local timezone or clock.

What a window means
-------------------
``opens_at`` is the start of the day's trading on NSE for futures: 09:00 IST
once the futures pre-open is live, 09:15 IST before it. The pre-open's internal
phases -- order entry, the random close, matching and the buffer before normal
market -- are not modelled; the equilibrium price it produces is the day's open.
``closes_at`` is the normal-market close, which is also the daily bar's point in
time. Expiry does not change the window: every NSE futures contract on one date
shares one session.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, time
from zoneinfo import ZoneInfo

from northstar_application.ports import (
    FuturesSessionResolutionError,
    FuturesTradingSession,
    FuturesTradingSessionResolver,
)
from northstar_core.foundation.value_objects import PointInTime
from northstar_core.futures import FuturesProductReference

from northstar_infrastructure.market_data import nse_futures_calendar_reference as reference

_VENUE = "NSE"
_IST = ZoneInfo("Asia/Kolkata")
_SATURDAY = 5


class NSEFuturesTradingSessionResolver(FuturesTradingSessionResolver):
    """Resolve NSE futures trading-session windows from versioned reference data.

    Only the NSE venue is supported, and only ``product.exchange_code`` is read:
    no NSE futures product has its own session schedule, so every expiry and
    every product on one date share one window.
    """

    # ------------------------------------------------------------------
    # Port
    # ------------------------------------------------------------------

    def resolve(
        self, product: FuturesProductReference, trading_date: date
    ) -> FuturesTradingSession | None:
        """Return the session labelled ``trading_date``, or None if not a session."""
        self._require_venue(product)
        self._require_date(trading_date, "trading_date")
        self._require_loaded(trading_date.year)
        return self._session(trading_date)

    def sessions_in_range(
        self, product: FuturesProductReference, start_date: date, end_date: date
    ) -> tuple[FuturesTradingSession, ...]:
        """Return every session labelled within the inclusive range, ascending."""
        self._require_venue(product)
        self._require_date(start_date, "start_date")
        self._require_date(end_date, "end_date")
        if start_date > end_date:
            raise FuturesSessionResolutionError(
                f"Futures session range start {start_date.isoformat()} is after "
                f"end {end_date.isoformat()}."
            )
        # Every year the range touches must be loaded before any date is
        # answered, so a range can never be partly resolved.
        for year in range(start_date.year, end_date.year + 1):
            self._require_loaded(year)

        sessions: list[FuturesTradingSession] = []
        for ordinal in range(start_date.toordinal(), end_date.toordinal() + 1):
            session = self._session(date.fromordinal(ordinal))
            if session is not None:
                sessions.append(session)
        return tuple(sessions)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    @staticmethod
    def _require_venue(product: FuturesProductReference) -> None:
        if not isinstance(product, FuturesProductReference):
            raise TypeError(
                "NSEFuturesTradingSessionResolver product must be a FuturesProductReference."
            )
        venue = product.exchange_code.value
        if venue != _VENUE:
            raise FuturesSessionResolutionError(
                f"Unsupported futures venue: {venue}. Supported venues are ['{_VENUE}']."
            )

    @staticmethod
    def _require_date(value: object, field_name: str) -> None:
        if isinstance(value, datetime) or not isinstance(value, date):
            raise TypeError(
                f"NSEFuturesTradingSessionResolver {field_name} must be a datetime.date."
            )

    @staticmethod
    def _require_loaded(year: int) -> None:
        if year not in reference.LOADED_YEARS:
            raise FuturesSessionResolutionError(
                f"NSE futures calendar year {year} is not loaded. "
                f"Loaded years are {sorted(reference.LOADED_YEARS)}."
            )

    def _session(self, trading_date: date) -> FuturesTradingSession | None:
        special = reference.SPECIAL_SESSIONS.get(trading_date)
        if special is not None:
            if special.opens is None or special.closes is None:
                raise FuturesSessionResolutionError(
                    f"NSE futures special session on {trading_date.isoformat()} "
                    f"({special.name}) has no notified timings."
                )
            return self._window(trading_date, special.opens, special.closes)
        if trading_date.weekday() >= _SATURDAY:
            return None
        if trading_date in reference.HOLIDAYS:
            return None
        regime = reference.regime_for(trading_date)
        return self._window(trading_date, regime.opens, regime.closes)

    @staticmethod
    def _window(trading_date: date, opens: time, closes: time) -> FuturesTradingSession:
        return FuturesTradingSession(
            trading_date=trading_date,
            opens_at=_utc(trading_date, opens),
            closes_at=_utc(trading_date, closes),
        )


def _utc(trading_date: date, clock: time) -> PointInTime:
    """Convert an IST wall-clock time on ``trading_date`` to a canonical UTC PointInTime."""
    in_utc = datetime.combine(trading_date, clock, tzinfo=_IST).astimezone(UTC)
    return PointInTime(in_utc.strftime("%Y-%m-%dT%H:%M:%SZ"))
