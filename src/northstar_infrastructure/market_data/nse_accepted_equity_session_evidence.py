"""The production trust anchor for NSE cash-equity session reference data (EI-1C-2).

This module is the only place accepted NSE cash-equity session evidence and
the production session catalog may live. It is reviewed by hand.

Source of truth
---------------
``northstar-docs`` ``research/NSE-Cash-Equity-Session-Evidence-Register.md``,
section 18 (owner approval record, 2026-11-04). The owner accepted exactly the
21 facts below, for the NSE cash-equity session reference only. Their
summaries are original paraphrases of the register, not source text. Each
source fingerprint is copied from the register's inspection tables
(section 18.6). Nothing here was fetched, and no source document is stored.

The eight DISCOVERED facts (CE-F-11, 14, 22, 32, 40, 41, 42, 53) are not
accepted and must never be added here. Pending owner decisions CE-D8, CE-D9,
CE-D10 and CE-D15 are not assumed: the 2026-11-08 special session is recorded
but not resolvable, and the non-CAS close from 2026-08-03 to 2026-08-11 is
explicitly unsupported.

Coverage
--------
``coverage_through`` is 2026-10-19, the date of the newest inspected Capital
Market circular (NSE/CMTR/76941). It is a conservative evidence horizon chosen
in review. It is not proof that every amendment issued on or before that date
was inspected, nor that no later amendment exists: completeness of 2026
holiday amendments (CE-F-22) is not accepted, and CE-F-22 is named in every
reference string. Every later date fails closed. The date is a reviewed
constant and is never extended automatically; it moves only when the owner
accepts newly inspected evidence and this module is changed and reviewed.

Normal sessions
---------------
The weekday holiday list in NSE/CMTR/71775, as amended by NSE/CMTR/72260
(CE-F-20, CE-F-21), names the weekdays on which the CM segment does not trade.
Within the session coverage, every other weekday is therefore stated here as a
normal session, and each one is listed explicitly in the catalog. No weekend
is stated as a session or as a closure unless an accepted fact says so.

Changing this module
--------------------
Only after the owner has accepted or changed a fact in the register: add or
change exactly that fact, keep its boundaries and limitations, bump both
versions, and update the regression tests that pin this module to the
register. Never add a guessed value, a test fixture or a DISCOVERED fact.
This is a code-review boundary, not a cryptographic one.
"""

from __future__ import annotations

from datetime import date, time, timedelta

from northstar_core.foundation.value_objects import ExchangeCode

from northstar_infrastructure.market_data.nse_equity_session_reference import (
    NSEEquityAcceptedSessionEvidence,
    NSEEquityAcceptedSessionFact,
    NSEEquityClosedDate,
    NSEEquityClosureKind,
    NSEEquityContinuousCloseRule,
    NSEEquityExceptionReference,
    NSEEquityOpeningRule,
    NSEEquityOwnerAcceptance,
    NSEEquitySecurityGroup,
    NSEEquitySessionCatalog,
    NSEEquitySessionEvidenceError,
    NSEEquitySourceIdentity,
    NSEEquitySourceRole,
    NSEEquitySpecialSessionReference,
    NSEEquityUnsupportedClose,
)

_EST = NSEEquitySourceRole.ESTABLISHES
_COR = NSEEquitySourceRole.CORROBORATES
_ALL = NSEEquitySecurityGroup.ALL_NORMAL_MARKET
_CAS = NSEEquitySecurityGroup.CAS_ELIGIBLE
_NON_CAS = NSEEquitySecurityGroup.NON_CAS

_REGIME_A_FROM = date(2026, 4, 28)
_CAS_LIVE = date(2026, 8, 3)
_PRE_OPEN_REALIGNED = date(2026, 9, 7)
_NON_CAS_ACCEPTED_FROM = date(2026, 8, 12)
_COVERAGE_THROUGH = date(2026, 10, 19)

_ACCEPTANCE = NSEEquityOwnerAcceptance(
    accepted_on=date(2026, 11, 4),
    purpose="NSE cash-equity session reference only",
    record="NSE-Cash-Equity-Session-Evidence-Register.md section 18.3",
)

ACCEPTED_FACT_IDS: frozenset[str] = frozenset(
    {
        "CE-F-01",
        "CE-F-02",
        "CE-F-03",
        "CE-F-04",
        "CE-F-10",
        "CE-F-12",
        "CE-F-13",
        "CE-F-15",
        "CE-F-16",
        "CE-F-17",
        "CE-F-18",
        "CE-F-19",
        "CE-F-20",
        "CE-F-21",
        "CE-F-30",
        "CE-F-31",
        "CE-F-50",
        "CE-F-51",
        "CE-F-52",
        "CE-F-54",
        "CE-F-55",
    }
)
DISCOVERED_FACT_IDS: frozenset[str] = frozenset(
    {"CE-F-11", "CE-F-14", "CE-F-22", "CE-F-32", "CE-F-40", "CE-F-41", "CE-F-42", "CE-F-53"}
)


def _source(
    source_id: str, document: str, sha256: str, role: NSEEquitySourceRole
) -> NSEEquitySourceIdentity:
    return NSEEquitySourceIdentity(source_id, document, sha256, role)


_S03 = "CE-S-03", "NSE Closing Auction Session page, print of 2026-11-02, updated 2026-08-12"
_S03_SHA = "11bae310d99b7659a823aa6fae0f5d8cfc838f2f2ca337091e44e4077ba39eb5"
_S11 = "CE-S-11", "NSE/CMTR/73362"
_S11_SHA = "c7da4fb3a74954903d9ac2106e16f22679654405774df7abe602f369fd1e8377"
_S12 = "CE-S-12", "NSE/CMTR/73845"
_S12_SHA = "aef6e11c636ce079ec105307a55772a79d3c1be3218147f8b3fd97970312e65d"
_S12A = "CE-S-12A", "Annexure A to NSE/CMTR/73845 and NSE/CMTR/74466 (security-master layout)"
_S12A_SHA = "da1dad1077b0fe70c14a068a1c4f6a665a80f35af81b6d454ba7271d2969efae"
_S13 = "CE-S-13", "NSE/CMTR/74466"
_S13_SHA = "50778523b491d582a7b367c7c7d64aabaefe0bc43eefba16b89364295c71fdbe"
_S13B = "CE-S-13B", "Annexure B to NSE/CMTR/74466 (CAS FAQ, non-binding)"
_S13B_SHA = "06648b68cfe39f0ec2348e9124833963a7970dbd7000e6a0347edac34308b45f"
_S14 = "CE-S-14", "NSE/CMTR/74969"
_S14_SHA = "b6bfea1a5acad4a33019ff722ae04d280cf8af11dfa98e423f75d870969ffebc"
_S14A = "CE-S-14A", "Annexure to NSE/CMTR/74969 (pre-open FAQ, non-binding)"
_S14A_SHA = "c3750bc4e69cd5436803982351afb90a8428afe8c0e1df21829e2b6b2a7b97c6"
_S15 = "CE-S-15", "NSE/CMTR/75479"
_S15_SHA = "17e415f619be6a132f86a4edbc127412f44e90f87e838151d86484e2204022c1"
_S17 = "CE-S-17", "NSE/CMTR/71775"
_S17_SHA = "aa97e0afc0ce394097f2fc62631c68e3c2e4c7c23541ed35f21d9fc06b0dcacb"
_S18 = "CE-S-18", "NSE/CMTR/72260"
_S18_SHA = "c5d32a838b46d5044717b830a4793479a51a40f8e4e19d3896c65682d15b6ebc"
_S19 = "CE-S-19", "NSE/CMTR/76941"
_S19_SHA = "ce94ee1c54d98eccdf3c30fa0673ae2b6a49ea4ed4a72fb83d826f4359f71d58"
_S20 = "CE-S-20", "NSE/CMTR/73927"
_S20_SHA = "f5e04e3cc7fa33a995ddc2603b62fce3b2e1e4f230848d211567b855db5ff89e"
_S20D = "CE-S-20D", "Part D of NSE/CMTR/73927 (report and file formats)"
_S20D_SHA = "30a4e37956ba5ab1acdda177411beca0146ebab0bcb155f1cd4f12576d195798"


def _fact(
    fact_id: str,
    summary: str,
    groups: frozenset[NSEEquitySecurityGroup],
    effective_from: date | None,
    effective_through: date | None,
    sources: tuple[NSEEquitySourceIdentity, ...],
    limitations: tuple[str, ...],
) -> NSEEquityAcceptedSessionFact:
    return NSEEquityAcceptedSessionFact(
        fact_id=fact_id,
        summary=summary,
        groups=groups,
        effective_from=effective_from,
        effective_through=effective_through,
        sources=sources,
        limitations=limitations,
        acceptance=_ACCEPTANCE,
    )


_NONE: frozenset[NSEEquitySecurityGroup] = frozenset()

_FACTS: tuple[NSEEquityAcceptedSessionFact, ...] = (
    _fact(
        "CE-F-01",
        "The CM normal market opens at 09:15 IST; continuous trading begins when the regular "
        "pre-open buffer ends.",
        frozenset({_ALL}),
        _REGIME_A_FROM,
        None,
        (_source(*_S20, _S20_SHA, _EST), _source(*_S14, _S14_SHA, _EST)),
        (
            "Not established before 2026-04-28.",
            "Excludes special pre-open securities on their special pre-open day (CE-F-54).",
            "Inspected amendment history runs to NSE/CMTR/76941 (2026-10-19).",
        ),
    ),
    _fact(
        "CE-F-02",
        "The CM normal market closes at 15:30 IST for all normal-market securities.",
        frozenset({_ALL}),
        _REGIME_A_FROM,
        _CAS_LIVE - timedelta(days=1),
        (_source(*_S20, _S20_SHA, _EST),),
        (
            "Superseded for CAS securities from 2026-08-03 by CE-F-12.",
            "Its continuation for non-CAS securities is not part of this fact (CE-D15).",
        ),
    ),
    _fact(
        "CE-F-03",
        "Regular pre-open 09:00 to 09:15 IST: order entry to 09:08 with a random close, "
        "matching to 09:12, buffer to 09:15. It is not continuous trading.",
        _NONE,
        _REGIME_A_FROM,
        _PRE_OPEN_REALIGNED - timedelta(days=1),
        (_source(*_S20, _S20_SHA, _EST),),
        ("Pre-open is never a continuous-trading phase.",),
    ),
    _fact(
        "CE-F-04",
        "Regular pre-open 09:00 to 09:15 IST: limit and market orders to 09:05, limit orders "
        "only to 09:10 with a random close, matching about 09:10 to 09:12, buffer 09:12 to "
        "09:15 unchanged.",
        _NONE,
        _PRE_OPEN_REALIGNED,
        None,
        (_source(*_S14, _S14_SHA, _EST), _source(*_S14A, _S14A_SHA, _COR)),
        ("Pre-open is never a continuous-trading phase.", "The FAQ corroborates only."),
    ),
    _fact(
        "CE-F-10",
        "CAS trading-modality changes are live in the CM segment from Monday 2026-08-03.",
        frozenset({_CAS}),
        _CAS_LIVE,
        None,
        (_source(*_S15, _S15_SHA, _EST),),
        ("No CAS before 2026-08-03.",),
    ),
    _fact(
        "CE-F-12",
        "Continuous trading for CAS-applicable securities ends at 15:15 IST; CAS is a separate "
        "session from 15:15 to 15:35.",
        frozenset({_CAS}),
        _CAS_LIVE,
        None,
        (
            _source(*_S13, _S13_SHA, _EST),
            _source(*_S11, _S11_SHA, _COR),
            _source(*_S13B, _S13B_SHA, _COR),
        ),
        (
            "Normal live days only; no mock clocks.",
            "Names no securities; it is not a membership list.",
            "Does not apply on CE-F-18 days.",
        ),
    ),
    _fact(
        "CE-F-13",
        "Continuous trading for non-CAS securities runs from 09:15 to 15:30 IST.",
        frozenset({_NON_CAS}),
        _NON_CAS_ACCEPTED_FROM,
        None,
        (_source(*_S03, _S03_SHA, _EST), _source(*_S19, _S19_SHA, _COR)),
        (
            "Accepted as the current rule at the page update of 2026-08-12; the page states no "
            "effective-from date.",
            "Whether it applies from 2026-08-03 awaits owner decision CE-D15.",
        ),
    ),
    _fact(
        "CE-F-15",
        "CAS phase 1 covers cash-segment stocks on which derivative contracts are available on "
        "any exchange.",
        _NONE,
        _CAS_LIVE,
        None,
        (
            _source(*_S13, _S13_SHA, _EST),
            _source(*_S11, _S11_SHA, _EST),
            _source(*_S03, _S03_SHA, _EST),
        ),
        ("A criterion, not a membership list and not historical membership.",),
    ),
    _fact(
        "CE-F-16",
        "CAS timetable: 15:15 to 15:20 reference price and transition; 15:20 to 15:25 limit and "
        "market entry; 15:25 to 15:30 limit only with a random close; 15:30 to 15:35 matching; "
        "15:35 to 15:50 transition; 15:50 to 16:00 post-close.",
        _NONE,
        _CAS_LIVE,
        None,
        (_source(*_S13, _S13_SHA, _EST), _source(*_S03, _S03_SHA, _EST)),
        ("Not continuous trading; never gridded.", "Subject to CE-F-18."),
    ),
    _fact(
        "CE-F-17",
        "Pre-open and CAS are sessions distinct from continuous trading.",
        _NONE,
        None,
        None,
        (_source(*_S19, _S19_SHA, _EST), _source(*_S03, _S03_SHA, _EST)),
        ("Stated for 2026-11-08 and as current at 2026-08-12; it backs no dated rule.",),
    ),
    _fact(
        "CE-F-18",
        "No CAS for a derivative-eligible stock on a scheme-of-arrangement ex-date without price "
        "discovery in the special pre-open, or for any security when an index circuit breaker "
        "halts the market for the rest of the day.",
        _NONE,
        _CAS_LIVE,
        None,
        (_source(*_S13, _S13_SHA, _EST),),
        ("Day-of events; never inferred from absent data or classified automatically.",),
    ),
    _fact(
        "CE-F-19",
        "Every timetable in NSE/CMTR/75479 is for the mock session of 2026-08-01, which is not a "
        "live session.",
        _NONE,
        date(2026, 8, 1),
        date(2026, 8, 1),
        (_source(*_S15, _S15_SHA, _EST),),
        ("Mock clocks never enter live reference data.",),
    ),
    _fact(
        "CE-F-20",
        "2026 CM trading holidays: 15 weekday holidays and 4 weekend holidays; Muhurat trading "
        "on 2026-11-08 announced.",
        _NONE,
        date(2026, 1, 1),
        date(2026, 12, 31),
        (_source(*_S17, _S17_SHA, _EST),),
        ("Completeness of later amendments (CE-F-22) is not accepted.",),
    ),
    _fact(
        "CE-F-21",
        "Thursday 2026-01-15 is an added CM trading holiday, partially modifying NSE/CMTR/71775.",
        _NONE,
        date(2026, 1, 15),
        date(2026, 1, 15),
        (_source(*_S18, _S18_SHA, _EST),),
        ("CM segment only.",),
    ),
    _fact(
        "CE-F-30",
        "A special live CM session is held on Sunday 2026-11-08 for Muhurat trading.",
        _NONE,
        date(2026, 11, 8),
        date(2026, 11, 8),
        (_source(*_S19, _S19_SHA, _EST), _source(*_S17, _S17_SHA, _COR)),
        ("A special session, never a normal day or a holiday.", "V1 does not trade it."),
    ),
    _fact(
        "CE-F-31",
        "Muhurat 2026-11-08 timings: continuous trading opens 14:20 IST and closes 15:15 for CAS "
        "securities and 15:30 for non-CAS securities; CAS 15:15 to 15:35.",
        _NONE,
        date(2026, 11, 8),
        date(2026, 11, 8),
        (_source(*_S19, _S19_SHA, _EST),),
        ("Not a normal-day rule.", "Its loading awaits owner decisions CE-D9 and CE-D10."),
    ),
    _fact(
        "CE-F-50",
        "The CAS flag is field 13 of security.txt and nnf_security.dat and field 24 of the MII "
        "security file; 0 is not eligible and 1 is eligible.",
        _NONE,
        _CAS_LIVE,
        None,
        (
            _source(*_S12, _S12_SHA, _EST),
            _source(*_S12A, _S12A_SHA, _EST),
            _source(*_S13, _S13_SHA, _EST),
            _source(*_S13B, _S13B_SHA, _COR),
        ),
        ("A specification only; no file inspected; it establishes no membership.",),
    ),
    _fact(
        "CE-F-51",
        "Before 2026-08-03 the same security-master field positions carried RETDEBT eligibility "
        "and status.",
        _NONE,
        None,
        _CAS_LIVE - timedelta(days=1),
        (_source(*_S12A, _S12A_SHA, _EST), _source(*_S20D, _S20D_SHA, _COR)),
        ("A value dated before 2026-08-03 is never CAS eligibility.",),
    ),
    _fact(
        "CE-F-52",
        "Members load the updated security files from the extranet; the MII file name carries a "
        "date.",
        _NONE,
        _CAS_LIVE,
        None,
        (_source(*_S13, _S13_SHA, _EST),),
        ("Frequency, retention and back-dated availability are not stated.",),
    ),
    _fact(
        "CE-F-54",
        "Special pre-open session 09:00 to 10:00 IST for IPOs, relistings, SME IPOs, scheme "
        "ex-dates of derivative stocks and other listed cases; they join the normal market only "
        "after 10:00.",
        _NONE,
        _REGIME_A_FROM,
        None,
        (_source(*_S20, _S20_SHA, _EST),),
        (
            "Affected listing-days need an instrument-event reference (CE-D18); unknown fails "
            "closed.",
        ),
    ),
    _fact(
        "CE-F-55",
        "After a market-wide circuit breaker breach or an outage, the market reopens with a "
        "pre-open whose timings are notified that day.",
        _NONE,
        _PRE_OPEN_REALIGNED,
        None,
        (_source(*_S14, _S14_SHA, _EST), _source(*_S14A, _S14A_SHA, _COR)),
        ("A day-of event; never inferred from absent data or classified automatically.",),
    ),
)

PRODUCTION_ACCEPTED_EQUITY_SESSION_EVIDENCE = NSEEquityAcceptedSessionEvidence(
    version="nse-accepted-equity-session-evidence/1",
    facts=_FACTS,
)

_HOLIDAY_REASON = "CM trading holiday listed in NSE/CMTR/71775"
_WEEKDAY_HOLIDAYS = (
    date(2026, 1, 26),
    date(2026, 3, 3),
    date(2026, 3, 26),
    date(2026, 3, 31),
    date(2026, 4, 3),
    date(2026, 4, 14),
    date(2026, 5, 1),
    date(2026, 5, 28),
    date(2026, 6, 26),
    date(2026, 9, 14),
    date(2026, 10, 2),
    date(2026, 10, 20),
    date(2026, 11, 10),
    date(2026, 11, 24),
    date(2026, 12, 25),
)
_WEEKEND_HOLIDAYS = (
    (date(2026, 2, 15), _HOLIDAY_REASON + " as a weekend holiday"),
    (date(2026, 3, 21), _HOLIDAY_REASON + " as a weekend holiday"),
    (date(2026, 8, 15), _HOLIDAY_REASON + " as a weekend holiday"),
    (date(2026, 11, 8), "Diwali Laxmi Pujan, listed in NSE/CMTR/71775 as a weekend holiday"),
)

_CLOSED_DATES: tuple[NSEEquityClosedDate, ...] = (
    NSEEquityClosedDate(
        date(2026, 1, 15),
        NSEEquityClosureKind.WEEKDAY_TRADING_HOLIDAY,
        "CM trading holiday added by NSE/CMTR/72260 (Maharashtra municipal corporation elections)",
        ("CE-F-21",),
    ),
    *(
        NSEEquityClosedDate(
            day, NSEEquityClosureKind.WEEKDAY_TRADING_HOLIDAY, _HOLIDAY_REASON, ("CE-F-20",)
        )
        for day in _WEEKDAY_HOLIDAYS
    ),
    *(
        NSEEquityClosedDate(day, NSEEquityClosureKind.WEEKEND_TRADING_HOLIDAY, reason, ("CE-F-20",))
        for day, reason in _WEEKEND_HOLIDAYS
    ),
)


def _stated_weekday_sessions(first: date, last: date, closed: frozenset[date]) -> frozenset[date]:
    """Return the weekdays from ``first`` to ``last`` that the weekday holiday list leaves open."""
    days: set[date] = set()
    day = first
    while day <= last:
        if day.weekday() < 5 and day not in closed:
            days.add(day)
        day += timedelta(days=1)
    return frozenset(days)


PRODUCTION_NSE_EQUITY_SESSION_CATALOG = NSEEquitySessionCatalog(
    exchange_code=ExchangeCode("NSE"),
    version="nse-equity-session/1",
    timezone="Asia/Kolkata",
    evidence=PRODUCTION_ACCEPTED_EQUITY_SESSION_EVIDENCE,
    calendar_from=date(2026, 1, 1),
    session_rules_from=_REGIME_A_FROM,
    coverage_through=_COVERAGE_THROUGH,
    coverage_basis=(
        "conservative evidence horizon at the newest inspected CM circular, NSE/CMTR/76941 of "
        "2026-10-19; not proof that every amendment on or before it was inspected, nor that no "
        "later amendment exists (CE-F-22 is not accepted); never extended automatically"
    ),
    normal_sessions=_stated_weekday_sessions(
        _REGIME_A_FROM,
        _COVERAGE_THROUGH,
        frozenset(closed.day for closed in _CLOSED_DATES),
    ),
    normal_session_fact_ids=("CE-F-20", "CE-F-21"),
    opening_rules=(
        NSEEquityOpeningRule(
            _REGIME_A_FROM,
            _PRE_OPEN_REALIGNED - timedelta(days=1),
            time(9, 15),
            ("CE-F-01", "CE-F-03"),
        ),
        NSEEquityOpeningRule(
            _PRE_OPEN_REALIGNED, _COVERAGE_THROUGH, time(9, 15), ("CE-F-01", "CE-F-04")
        ),
    ),
    close_rules=(
        NSEEquityContinuousCloseRule(
            _ALL, _REGIME_A_FROM, _CAS_LIVE - timedelta(days=1), time(15, 30), ("CE-F-02",)
        ),
        NSEEquityContinuousCloseRule(
            _CAS, _CAS_LIVE, _COVERAGE_THROUGH, time(15, 15), ("CE-F-10", "CE-F-12")
        ),
        NSEEquityContinuousCloseRule(
            _NON_CAS, _NON_CAS_ACCEPTED_FROM, _COVERAGE_THROUGH, time(15, 30), ("CE-F-13",)
        ),
    ),
    unsupported_closes=(
        NSEEquityUnsupportedClose(
            _NON_CAS,
            _CAS_LIVE,
            _NON_CAS_ACCEPTED_FROM - timedelta(days=1),
            "CE-F-02 is accepted only through 2026-08-02 and CE-F-13 only from 2026-08-12; the "
            "non-CAS continuous close on these dates awaits owner decision CE-D15.",
            ("CE-D15",),
        ),
    ),
    closed_dates=_CLOSED_DATES,
    special_sessions=(
        NSEEquitySpecialSessionReference(
            date(2026, 11, 8),
            "Diwali Muhurat trading",
            ("CE-F-30", "CE-F-31"),
            ("CE-D9", "CE-D10"),
        ),
    ),
    exceptions=(
        NSEEquityExceptionReference(
            "CE-F-54",
            _REGIME_A_FROM,
            "Special pre-open securities join the normal market only after 10:00 on their "
            "special pre-open day; which listings are affected is not determined here (CE-D18).",
        ),
        NSEEquityExceptionReference(
            "CE-F-18",
            _CAS_LIVE,
            "CAS may not apply to a security on a scheme ex-date or to any security after an "
            "index circuit-breaker halt; such days are not determined here.",
        ),
        NSEEquityExceptionReference(
            "CE-F-55",
            _PRE_OPEN_REALIGNED,
            "After a circuit-breaker breach or an outage the market reopens with a pre-open "
            "notified that day; such events are not determined here.",
        ),
    ),
    limitations=(
        "Coverage through 2026-10-19 is a conservative evidence horizon, not proof that every "
        "holiday amendment on or before it was inspected; CE-F-22 is not accepted.",
        "NORMAL_SESSION means a scheduled normal venue session under the accepted rules; it does "
        "not prove actual trading, uninterrupted market availability or complete OHLCV evidence.",
        "A venue session is not evidence that any security traded continuously: special "
        "pre-open days, CAS exceptions, reopenings, halts, suspensions and call auctions are "
        "not determined here.",
        "CAS eligibility is not resolved for any security on any date.",
        "A session window does not show that any candle is complete or final, or that any trade "
        "was executed.",
    ),
    unresolved_fact_ids=("CE-F-22",),
)


def _validate_production() -> None:
    registry = PRODUCTION_ACCEPTED_EQUITY_SESSION_EVIDENCE
    if registry.fact_ids != ACCEPTED_FACT_IDS:
        raise NSEEquitySessionEvidenceError(
            "The production registry does not hold exactly the owner-accepted facts."
        )
    if registry.fact_ids & DISCOVERED_FACT_IDS:
        raise NSEEquitySessionEvidenceError("The production registry holds a DISCOVERED fact.")
    if PRODUCTION_NSE_EQUITY_SESSION_CATALOG.evidence is not registry:
        raise NSEEquitySessionEvidenceError(
            "The production catalog does not read the production evidence registry."
        )


_validate_production()
