"""Versioned NSE futures (F&O segment) calendar reference data.

Every fact here was read from a primary NSE or NSE Clearing circular and is
cited beside it. Nothing is fetched at runtime and nothing is inferred for a
year that is not listed in LOADED_YEARS: to extend the calendar, add the
year's annual holiday circular, every later amendment and every special
session, then add the year.

Provenance (all at nsearchives.nseindia.com/content/circulars/)
--------------------------------------------------------------

Normal-market hours for futures:

    NSE/FAOP/65730   23 Dec 2024   09:15-15:30 "standard market timings", no F&O pre-open
    NSE/FAOP/69898   28 Aug 2025   futures pre-open "from Monday, December 08, 2025"
    NSE/FAOP/71092   03 Nov 2025   pre-open modalities (09:00 order entry)
    NSE/FAOP/71640   05 Dec 2025   pre-open LIVE on Monday, December 08, 2025
    NSE/FAOP/74467   29 May 2026   close 15:40, "effective from August 03, 2026"
    NSE/FAOP/75472   30 Jul 2026   CAS modalities LIVE from August 03, 2026
    NSE/FAOP/74970   01 Jul 2026   pre-open phases revised from 07 Sep 2026;
                                   the 09:00 start and 09:15 open are unchanged,
                                   so no new regime is needed

Trading holidays:

    2024  NSE/FAOP/59723   12 Dec 2023   annual list
          NSE/FAOP/60337   19 Jan 2024   adds 2024-01-22
          NSE/FAOP/61517   08 Apr 2024   adds 2024-05-20
          NSE/FAOP/64959   08 Nov 2024   adds 2024-11-20
    2025  NSE/FAOP/65588   13 Dec 2024   annual list; no F&O amendment was issued
    2026  NSE/FAOP/71777   12 Dec 2025   annual list
          NSE/FAOP/72262   12 Jan 2026   adds 2026-01-15

The NSE holiday-master API disagrees with the circulars for 2024 (it omits
2024-11-01 and lists the 2024-03-02 trading session as a holiday); the
circulars govern. Currency-derivative-only changes (NSE/CD/63952, NSE/CD/70033)
do not apply to F&O. Holidays falling on a Saturday or Sunday are not listed:
weekends are already non-sessions unless a special session says otherwise.

Special live sessions, all with settlement obligations:

    2024-01-20  Sat  NSE/MSD/60340   19 Jan 2024   full session from the primary site;
                                                   withdraws the split plan of NSE/MSD/59999
    2024-03-02  Sat  NSE/MSD/60677   14 Feb 2024   09:15-10:00 primary, 11:30-12:30 DR
    2024-05-18  Sat  NSE/MSD/61893   07 May 2024   09:15-10:00 primary, 11:30-12:30 DR
    2024-11-01  Fri  NSE/FAOP/64630  19 Oct 2024   Muhurat 18:00-19:00, on a holiday
    2025-02-01  Sat  NSE/FAOP/65730  23 Dec 2024   Union Budget, standard timings
    2025-10-21  Tue  NSE/FAOP/70320  22 Sep 2025   Muhurat 13:45-14:45, on a holiday
    2026-02-01  Sun  NSE/FAOP/72352  16 Jan 2026   Union Budget, pre-open 09:00, close 15:30
    2026-11-08  Sun  NSE/FAOP/71777  12 Dec 2025   Muhurat; timings "shall be notified
                                                   subsequently" and not yet notified

A session split for a disaster-recovery switchover is one window from the
first segment's open to the last segment's close. Nothing trades in the gap,
so no bar can fall there; the window's close is the day's last trading instant,
which is what a daily bar's point in time must be.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, time
from types import MappingProxyType

from northstar_application.ports import FuturesSessionResolutionError

_SATURDAY = 5


@dataclass(frozen=True, slots=True)
class SessionRegime:
    """Normal-market futures hours in force from ``effective_from`` onwards, in IST."""

    effective_from: date
    opens: time
    closes: time
    source: str


@dataclass(frozen=True, slots=True)
class TradingHoliday:
    """One weekday on which NSE F&O does not trade."""

    day: date
    name: str
    source: str


@dataclass(frozen=True, slots=True)
class SpecialSession:
    """One published live session outside the ordinary weekday calendar, in IST.

    ``opens`` and ``closes`` are None while NSE has announced the session but
    not yet notified its timings.
    """

    day: date
    name: str
    opens: time | None
    closes: time | None
    source: str


REGIMES: tuple[SessionRegime, ...] = (
    SessionRegime(date(2024, 1, 1), time(9, 15), time(15, 30), "NSE/FAOP/65730"),
    SessionRegime(date(2025, 12, 8), time(9, 0), time(15, 30), "NSE/FAOP/69898, 71640"),
    SessionRegime(date(2026, 8, 3), time(9, 0), time(15, 40), "NSE/FAOP/74467, 75472"),
)

_HOLIDAYS: tuple[TradingHoliday, ...] = (
    # 2024
    TradingHoliday(
        date(2024, 1, 22), "Special holiday (Negotiable Instruments Act)", "NSE/FAOP/60337"
    ),
    TradingHoliday(date(2024, 1, 26), "Republic Day", "NSE/FAOP/59723"),
    TradingHoliday(date(2024, 3, 8), "Mahashivratri", "NSE/FAOP/59723"),
    TradingHoliday(date(2024, 3, 25), "Holi", "NSE/FAOP/59723"),
    TradingHoliday(date(2024, 3, 29), "Good Friday", "NSE/FAOP/59723"),
    TradingHoliday(date(2024, 4, 11), "Id-Ul-Fitr (Ramadan Eid)", "NSE/FAOP/59723"),
    TradingHoliday(date(2024, 4, 17), "Shri Ram Navmi", "NSE/FAOP/59723"),
    TradingHoliday(date(2024, 5, 1), "Maharashtra Day", "NSE/FAOP/59723"),
    TradingHoliday(date(2024, 5, 20), "Parliamentary Elections in Mumbai", "NSE/FAOP/61517"),
    TradingHoliday(date(2024, 6, 17), "Bakri Id", "NSE/FAOP/59723"),
    TradingHoliday(date(2024, 7, 17), "Moharram", "NSE/FAOP/59723"),
    TradingHoliday(date(2024, 8, 15), "Independence Day", "NSE/FAOP/59723"),
    TradingHoliday(date(2024, 10, 2), "Mahatma Gandhi Jayanti", "NSE/FAOP/59723"),
    TradingHoliday(date(2024, 11, 1), "Diwali Laxmi Pujan", "NSE/FAOP/59723"),
    TradingHoliday(date(2024, 11, 15), "Gurunanak Jayanti", "NSE/FAOP/59723"),
    TradingHoliday(date(2024, 11, 20), "Assembly Elections in Maharashtra", "NSE/FAOP/64959"),
    TradingHoliday(date(2024, 12, 25), "Christmas", "NSE/FAOP/59723"),
    # 2025
    TradingHoliday(date(2025, 2, 26), "Mahashivratri", "NSE/FAOP/65588"),
    TradingHoliday(date(2025, 3, 14), "Holi", "NSE/FAOP/65588"),
    TradingHoliday(date(2025, 3, 31), "Id-Ul-Fitr (Ramadan Eid)", "NSE/FAOP/65588"),
    TradingHoliday(date(2025, 4, 10), "Shri Mahavir Jayanti", "NSE/FAOP/65588"),
    TradingHoliday(date(2025, 4, 14), "Dr. Baba Saheb Ambedkar Jayanti", "NSE/FAOP/65588"),
    TradingHoliday(date(2025, 4, 18), "Good Friday", "NSE/FAOP/65588"),
    TradingHoliday(date(2025, 5, 1), "Maharashtra Day", "NSE/FAOP/65588"),
    TradingHoliday(date(2025, 8, 15), "Independence Day", "NSE/FAOP/65588"),
    TradingHoliday(date(2025, 8, 27), "Ganesh Chaturthi", "NSE/FAOP/65588"),
    TradingHoliday(date(2025, 10, 2), "Mahatma Gandhi Jayanti/Dussehra", "NSE/FAOP/65588"),
    TradingHoliday(date(2025, 10, 21), "Diwali Laxmi Pujan", "NSE/FAOP/65588"),
    TradingHoliday(date(2025, 10, 22), "Diwali-Balipratipada", "NSE/FAOP/65588"),
    TradingHoliday(date(2025, 11, 5), "Prakash Gurpurb Sri Guru Nanak Dev", "NSE/FAOP/65588"),
    TradingHoliday(date(2025, 12, 25), "Christmas", "NSE/FAOP/65588"),
    # 2026
    TradingHoliday(
        date(2026, 1, 15), "Municipal Corporation Election, Maharashtra", "NSE/FAOP/72262"
    ),
    TradingHoliday(date(2026, 1, 26), "Republic Day", "NSE/FAOP/71777"),
    TradingHoliday(date(2026, 3, 3), "Holi", "NSE/FAOP/71777"),
    TradingHoliday(date(2026, 3, 26), "Shri Ram Navami", "NSE/FAOP/71777"),
    TradingHoliday(date(2026, 3, 31), "Shri Mahavir Jayanti", "NSE/FAOP/71777"),
    TradingHoliday(date(2026, 4, 3), "Good Friday", "NSE/FAOP/71777"),
    TradingHoliday(date(2026, 4, 14), "Dr. Baba Saheb Ambedkar Jayanti", "NSE/FAOP/71777"),
    TradingHoliday(date(2026, 5, 1), "Maharashtra Day", "NSE/FAOP/71777"),
    TradingHoliday(date(2026, 5, 28), "Bakri Id", "NSE/FAOP/71777"),
    TradingHoliday(date(2026, 6, 26), "Muharram", "NSE/FAOP/71777"),
    TradingHoliday(date(2026, 9, 14), "Ganesh Chaturthi", "NSE/FAOP/71777"),
    TradingHoliday(date(2026, 10, 2), "Mahatma Gandhi Jayanti", "NSE/FAOP/71777"),
    TradingHoliday(date(2026, 10, 20), "Dussehra", "NSE/FAOP/71777"),
    TradingHoliday(date(2026, 11, 10), "Diwali-Balipratipada", "NSE/FAOP/71777"),
    TradingHoliday(date(2026, 11, 24), "Prakash Gurpurb Sri Guru Nanak Dev", "NSE/FAOP/71777"),
    TradingHoliday(date(2026, 12, 25), "Christmas", "NSE/FAOP/71777"),
)

_SPECIAL_SESSIONS: tuple[SpecialSession, ...] = (
    SpecialSession(date(2024, 1, 20), "Live session", time(9, 15), time(15, 30), "NSE/MSD/60340"),
    SpecialSession(
        date(2024, 3, 2), "Live session, DR switchover", time(9, 15), time(12, 30), "NSE/MSD/60677"
    ),
    SpecialSession(
        date(2024, 5, 18), "Live session, DR switchover", time(9, 15), time(12, 30), "NSE/MSD/61893"
    ),
    SpecialSession(date(2024, 11, 1), "Muhurat", time(18, 0), time(19, 0), "NSE/FAOP/64630"),
    SpecialSession(date(2025, 2, 1), "Union Budget", time(9, 15), time(15, 30), "NSE/FAOP/65730"),
    SpecialSession(date(2025, 10, 21), "Muhurat", time(13, 45), time(14, 45), "NSE/FAOP/70320"),
    SpecialSession(date(2026, 2, 1), "Union Budget", time(9, 0), time(15, 30), "NSE/FAOP/72352"),
    SpecialSession(date(2026, 11, 8), "Muhurat", None, None, "NSE/FAOP/71777"),
)

LOADED_YEARS: frozenset[int] = frozenset({2024, 2025, 2026})


def validate(
    regimes: tuple[SessionRegime, ...],
    holidays: Iterable[TradingHoliday],
    special_sessions: Iterable[SpecialSession],
    loaded_years: frozenset[int],
) -> None:
    """Refuse reference data that could answer a date wrongly."""

    def fail(message: str) -> None:
        raise FuturesSessionResolutionError(f"Invalid NSE futures calendar reference: {message}")

    if not loaded_years:
        fail("no calendar year is loaded.")
    if not regimes:
        fail("no session regime is defined.")
    if regimes[0].effective_from > date(min(loaded_years), 1, 1):
        fail("the first loaded year starts before the first session regime.")
    for previous, regime in zip(regimes, regimes[1:], strict=False):
        if previous.effective_from >= regime.effective_from:
            fail("session regimes must be strictly ascending.")
    for regime in regimes:
        if regime.opens >= regime.closes:
            fail(f"regime from {regime.effective_from} does not open before it closes.")

    seen: set[date] = set()
    for holiday in holidays:
        if holiday.day in seen:
            fail(f"holiday {holiday.day} is listed twice.")
        seen.add(holiday.day)
        if holiday.day.year not in loaded_years:
            fail(f"holiday {holiday.day} is outside the loaded years.")
        if holiday.day.weekday() >= _SATURDAY:
            fail(f"holiday {holiday.day} falls on a weekend.")

    seen = set()
    for special in special_sessions:
        if special.day in seen:
            fail(f"special session {special.day} is listed twice.")
        seen.add(special.day)
        if special.day.year not in loaded_years:
            fail(f"special session {special.day} is outside the loaded years.")
        if (special.opens is None) != (special.closes is None):
            fail(f"special session {special.day} has only one of its timings.")
        if special.opens is not None and special.opens >= special.closes:
            fail(f"special session {special.day} does not open before it closes.")


validate(REGIMES, _HOLIDAYS, _SPECIAL_SESSIONS, LOADED_YEARS)

HOLIDAYS: frozenset[date] = frozenset(holiday.day for holiday in _HOLIDAYS)
SPECIAL_SESSIONS: Mapping[date, SpecialSession] = MappingProxyType(
    {special.day: special for special in _SPECIAL_SESSIONS}
)


def holidays() -> tuple[TradingHoliday, ...]:
    """Return every loaded trading holiday with its name and source."""
    return _HOLIDAYS


def regime_for(trading_date: date) -> SessionRegime:
    """Return the normal-market regime in force on ``trading_date``."""
    in_force = [regime for regime in REGIMES if regime.effective_from <= trading_date]
    if not in_force:
        raise FuturesSessionResolutionError(
            f"No NSE futures session regime is defined for {trading_date.isoformat()}."
        )
    return in_force[-1]
