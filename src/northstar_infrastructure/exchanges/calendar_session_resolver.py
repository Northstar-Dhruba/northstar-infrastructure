"""Trading session resolution adapter backed by market calendars."""

from __future__ import annotations

from datetime import date
from typing import ClassVar

import exchange_calendars as xcals
from northstar_application.ports import (
    TradingSessionResolutionError,
    TradingSessionResolver,
)
from northstar_core.foundation.value_objects import ExchangeCode, PointInTime


class ExchangeCalendarTradingSessionResolver(TradingSessionResolver):
    """Resolve regular market trading session completion instants using exchange calendars.

    Supported Northstar ExchangeCode venues are mapped to canonical ISO MIC
    calendar identifiers from the `exchange_calendars` library:
    - NASDAQ / XNAS -> XNAS
    - NYSE / XNYS   -> XNYS
    - BSE / XBOM    -> XBOM
    - LSE / XLON    -> XLON

    Note: NSE (National Stock Exchange of India) is currently unsupported by
    exchange_calendars and will raise TradingSessionResolutionError.
    """

    _EXCHANGE_MAP: ClassVar[dict[str, str]] = {
        "NASDAQ": "XNAS",
        "XNAS": "XNAS",
        "NYSE": "XNYS",
        "XNYS": "XNYS",
        "BSE": "XBOM",
        "XBOM": "XBOM",
        "LSE": "XLON",
        "XLON": "XLON",
    }

    def __init__(self) -> None:
        self._calendars: dict[str, xcals.ExchangeCalendar] = {}

    def resolve_session_close(
        self, exchange_code: ExchangeCode, trading_date: date
    ) -> PointInTime | None:
        """Return the regular session close PointInTime, or None if not a trading day."""
        if not isinstance(exchange_code, ExchangeCode):
            raise TypeError("exchange_code must be an ExchangeCode instance.")
        if not isinstance(trading_date, date):
            raise TypeError("trading_date must be a datetime.date instance.")

        calendar_name = self._EXCHANGE_MAP.get(exchange_code.value)
        if calendar_name is None:
            raise TradingSessionResolutionError(
                f"Unsupported exchange venue: {exchange_code.value}."
            )

        try:
            calendar = self._get_calendar(calendar_name, exchange_code, trading_date)
            date_str = trading_date.isoformat()
            if not calendar.is_session(date_str):
                return None

            close_ts = calendar.session_close(date_str)
            iso_utc = close_ts.strftime("%Y-%m-%dT%H:%M:%SZ")
            return PointInTime(iso_utc)
        except TradingSessionResolutionError:
            raise
        except Exception as exc:
            raise TradingSessionResolutionError(
                f"Trading session resolution failed for exchange {exchange_code.value} "
                f"on {trading_date.isoformat()}."
            ) from exc

    def _get_calendar(
        self, name: str, exchange_code: ExchangeCode, trading_date: date
    ) -> xcals.ExchangeCalendar:
        if name not in self._calendars:
            try:
                self._calendars[name] = xcals.get_calendar(name)
            except Exception as exc:
                raise TradingSessionResolutionError(
                    f"Trading session resolution failed for exchange {exchange_code.value} "
                    f"on {trading_date.isoformat()}."
                ) from exc
        return self._calendars[name]
