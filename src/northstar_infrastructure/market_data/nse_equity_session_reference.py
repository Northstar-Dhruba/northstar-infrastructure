"""Evidence-gated, versioned NSE cash-equity session reference (EI-1C-2).

The reference keeps five things apart:

A. Exchange/date availability: whether a date is a stated session, a stated
   closure, a special session, or outside what the evidence covers.
B. Venue-level normal-session rules: the dated continuous-market opening and
   the dated continuous-market closes.
C. The conditional Closing Auction Session (CAS) cutoff: from 2026-08-03 the
   continuous close depends on whether a security is CAS-eligible, so the
   reference states one close per security group and never one universal close.
D. Instrument applicability: which group a security belongs to on a date. It is
   NOT resolved here. No method takes an instrument, and nothing is inferred
   about any security.
E. The Northstar V1 research window (owner decision CE-D17): NOT implemented
   here. It is a separate Application policy, never a venue session.

Evidence
--------
Every dated rule, closure, special session and exception names the register
facts it rests on (``northstar-docs`` ``research/NSE-Cash-Equity-Session-
Evidence-Register.md``). A catalog is refused unless every cited fact is in its
accepted-evidence registry, every fact carries at least one establishing source
with a SHA-256 fingerprint, and every rule lies inside the effective boundaries
of every fact it cites. Acceptance is membership of the reviewed production
registry (``nse_accepted_equity_session_evidence``); a catalog built over any
other registry is labelled test evidence.

Coverage
--------
A catalog states three dates. ``calendar_from`` is the first date its closures
answer for. ``session_rules_from`` is the first date its session rules answer
for. ``coverage_through`` is the last date anything answers for. It is a
conservative evidence horizon chosen in review. It is not proof that every
amendment issued on or before it was inspected, nor that no later amendment
exists. It is a reviewed constant: nothing here extends it, and it moves only
when the owner accepts newly inspected evidence. Every date outside the
coverage fails closed.

``unresolved_fact_ids`` names register facts the answers depend on but that are
not accepted (for the production catalog, CE-F-22: completeness of holiday
amendments). They are appended to every reference string, including the
``reference`` of a resolved EI-1A session, so the limitation stays visible to
consumers of the port.

Two capabilities
----------------
``NSEEquityVenueSessionReference.lookup`` answers every date with an explicit
``NSEEquityVenueDay``, including unavailable and unsupported states. It never
raises for a date it cannot support; it says so.

``NSEEquityTradingSessionResolver`` implements the unchanged EI-1A
``EquityTradingSessionResolver``. It returns a session only when one venue-wide
continuous phase is stated for every normal-market security, which is the
pre-CAS regime. A date whose close depends on CAS eligibility raises
``NSEEquitySessionRequiresInstrumentApplicabilityError`` rather than returning
a misleading session. A stated closure returns None. Everything else raises.

A resolved session is a venue fact. It is not evidence that any security
traded continuously that day, that any candle is complete or final, or that
any trade was executed.

Wall-clock times are converted to UTC through the catalog's named IANA zone.
Nothing reads a clock, a provider or the network.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from enum import StrEnum
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from northstar_application.ports import (
    EquitySessionKind,
    EquitySessionResolutionError,
    EquityTradingPhase,
    EquityTradingSession,
    EquityTradingSessionResolver,
)
from northstar_core.foundation.value_objects import ExchangeCode, PointInTime

PRODUCTION_EQUITY_SESSION_AUTHORITY_PREFIX = "nse-accepted-equity-session-evidence"
TEST_EQUITY_SESSION_AUTHORITY_PREFIX = "test-accepted-equity-session-evidence"

_FACT_ID = re.compile(r"CE-F-[0-9]{2,3}")
_SOURCE_ID = re.compile(r"CE-S-[0-9]{2,3}[A-Z]?")
_DECISION_ID = re.compile(r"CE-D[0-9]{1,3}")
_SHA256 = re.compile(r"[0-9a-f]{64}")
_VERSION = re.compile(r"[A-Za-z0-9][A-Za-z0-9._/-]{0,127}")
_TEXT = re.compile(r"[\x21-\x7e](?:[\x20-\x7e]{0,598}[\x21-\x7e])?")
_SATURDAY = 5


class NSEEquitySessionEvidenceError(EquitySessionResolutionError):
    """Raised when accepted evidence or a session catalog is malformed or contradictory."""


class NSEEquitySessionUnavailableError(EquitySessionResolutionError):
    """Raised when a date cannot be resolved as an EI-1A session from the catalog."""

    def __init__(self, message: str, venue_day: NSEEquityVenueDay | None = None) -> None:
        super().__init__(message)
        self.venue_day = venue_day


class NSEEquitySessionRequiresInstrumentApplicabilityError(NSEEquitySessionUnavailableError):
    """Raised when a session's continuous close depends on a security's CAS eligibility."""


class NSEEquitySecurityGroup(StrEnum):
    """Which normal-market securities a dated continuous close applies to."""

    ALL_NORMAL_MARKET = "ALL_NORMAL_MARKET"
    CAS_ELIGIBLE = "CAS_ELIGIBLE"
    NON_CAS = "NON_CAS"


_GROUP_DEPENDENT = frozenset({NSEEquitySecurityGroup.CAS_ELIGIBLE, NSEEquitySecurityGroup.NON_CAS})


class NSEEquitySourceRole(StrEnum):
    """Whether a source establishes a fact or only corroborates it (CE-D4, CE-D11)."""

    ESTABLISHES = "ESTABLISHES"
    CORROBORATES = "CORROBORATES"


class NSEEquityClosureKind(StrEnum):
    """How a stated closure falls in the week, as its source lists it."""

    WEEKDAY_TRADING_HOLIDAY = "WEEKDAY_TRADING_HOLIDAY"
    WEEKEND_TRADING_HOLIDAY = "WEEKEND_TRADING_HOLIDAY"


class NSEEquityVenueDayStatus(StrEnum):
    """What the catalog can say about one date.

    NORMAL_SESSION means a scheduled normal venue session under the accepted
    reference rules. It does not prove that trading took place, that the
    market was available without interruption, that any security traded, or
    that OHLCV evidence for the day is complete.
    """

    NORMAL_SESSION = "NORMAL_SESSION"
    CLOSED = "CLOSED"
    SPECIAL_SESSION_UNSUPPORTED = "SPECIAL_SESSION_UNSUPPORTED"
    SESSION_RULES_NOT_ESTABLISHED = "SESSION_RULES_NOT_ESTABLISHED"
    WEEKEND_NOT_STATED = "WEEKEND_NOT_STATED"
    NOT_STATED = "NOT_STATED"
    OUTSIDE_COVERAGE = "OUTSIDE_COVERAGE"


def _fail(message: str) -> None:
    raise NSEEquitySessionEvidenceError(message)


def _is_plain_date(value: object) -> bool:
    return isinstance(value, date) and not isinstance(value, datetime)


def _require_date(value: object, name: str) -> None:
    if not _is_plain_date(value):
        _fail(f"{name} must be a plain date.")


def _require_text(value: object, pattern: re.Pattern[str], name: str) -> None:
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        _fail(f"{name} is missing or malformed.")


def _require_clock(value: object, name: str) -> None:
    if not isinstance(value, time) or value.tzinfo is not None or value.microsecond:
        _fail(f"{name} must be a naive wall-clock time without fractional seconds.")


def _require_ids(values: object, pattern: re.Pattern[str], name: str) -> None:
    if not isinstance(values, tuple) or not values:
        _fail(f"{name} must be a non-empty tuple.")
    for value in values:  # type: ignore[union-attr]
        _require_text(value, pattern, f"{name} entry")
    if len(set(values)) != len(values):  # type: ignore[arg-type]
        _fail(f"{name} must not repeat an identifier.")


def _require_period(effective_from: object, effective_through: object, subject: str) -> None:
    _require_date(effective_from, f"{subject} effective-from")
    _require_date(effective_through, f"{subject} effective-through")
    if effective_through < effective_from:  # type: ignore[operator]
        _fail(f"{subject} period is inverted.")


def _overlaps(first: _Dated, second: _Dated) -> bool:
    return first.effective_from <= second.effective_through and (
        second.effective_from <= first.effective_through
    )


def _covers(rule: _Dated, day: date) -> bool:
    return rule.effective_from <= day <= rule.effective_through


# ----------------------------------------------------------------------
# Accepted evidence
# ----------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class NSEEquitySourceIdentity:
    """One inspected source of a fact, with its register ID and SHA-256 fingerprint."""

    source_id: str
    document: str
    sha256: str
    role: NSEEquitySourceRole

    def __post_init__(self) -> None:
        _require_text(self.source_id, _SOURCE_ID, "Source ID")
        _require_text(self.document, _TEXT, f"Source {self.source_id} document")
        _require_text(self.sha256, _SHA256, f"Source {self.source_id} SHA-256")
        if not isinstance(self.role, NSEEquitySourceRole):
            _fail(f"Source {self.source_id} role must be an NSEEquitySourceRole.")


@dataclass(frozen=True, slots=True)
class NSEEquityOwnerAcceptance:
    """The owner's acceptance of one fact, for one stated purpose."""

    accepted_on: date
    purpose: str
    record: str

    def __post_init__(self) -> None:
        _require_date(self.accepted_on, "Acceptance date")
        _require_text(self.purpose, _TEXT, "Acceptance purpose")
        _require_text(self.record, _TEXT, "Acceptance record")


@dataclass(frozen=True, slots=True)
class NSEEquityAcceptedSessionFact:
    """One register fact with its sources, boundaries, applicability and limitations.

    Constructing one accepts nothing; only membership of the production
    registry does. ``groups`` names the security groups whose continuous-market
    boundary the fact states, and is empty for facts that state none.
    ``effective_from`` and ``effective_through`` are the fact's accepted
    boundaries; None means the register gives no such bound, and a fact
    without an ``effective_from`` can back no dated rule.
    """

    fact_id: str
    summary: str
    groups: frozenset[NSEEquitySecurityGroup]
    effective_from: date | None
    effective_through: date | None
    sources: tuple[NSEEquitySourceIdentity, ...]
    limitations: tuple[str, ...]
    acceptance: NSEEquityOwnerAcceptance

    def __post_init__(self) -> None:
        _require_text(self.fact_id, _FACT_ID, "Fact ID")
        subject = f"Fact {self.fact_id}"
        _require_text(self.summary, _TEXT, f"{subject} summary")
        if not isinstance(self.groups, frozenset) or not all(
            isinstance(group, NSEEquitySecurityGroup) for group in self.groups
        ):
            _fail(f"{subject} groups must be a frozenset of NSEEquitySecurityGroup values.")
        if self.effective_from is not None:
            _require_date(self.effective_from, f"{subject} effective-from")
        if self.effective_through is not None:
            _require_date(self.effective_through, f"{subject} effective-through")
        if (
            self.effective_from is not None
            and self.effective_through is not None
            and self.effective_through < self.effective_from
        ):
            _fail(f"{subject} period is inverted.")
        if not isinstance(self.sources, tuple) or not self.sources:
            _fail(f"{subject} has no source identity.")
        for source in self.sources:
            if not isinstance(source, NSEEquitySourceIdentity):
                _fail(f"{subject} sources must be NSEEquitySourceIdentity values.")
        if len({source.source_id for source in self.sources}) != len(self.sources):
            _fail(f"{subject} cites a source twice.")
        if not any(source.role is NSEEquitySourceRole.ESTABLISHES for source in self.sources):
            _fail(f"{subject} has no establishing source; corroboration alone is not evidence.")
        if not isinstance(self.limitations, tuple):
            _fail(f"{subject} limitations must be a tuple.")
        for note in self.limitations:
            _require_text(note, _TEXT, f"{subject} limitation")
        if not isinstance(self.acceptance, NSEEquityOwnerAcceptance):
            _fail(f"{subject} acceptance must be an NSEEquityOwnerAcceptance.")

    def covers_period(self, effective_from: date, effective_through: date) -> bool:
        """Return whether the whole period lies inside this fact's accepted boundaries."""
        if self.effective_from is None or effective_from < self.effective_from:
            return False
        return self.effective_through is None or effective_through <= self.effective_through


@dataclass(frozen=True, slots=True)
class NSEEquityAcceptedSessionEvidence:
    """One version of a set of accepted facts, unique by fact ID, in fact-ID order.

    A source ID must name the same document and fingerprint wherever it is cited.
    """

    version: str
    facts: tuple[NSEEquityAcceptedSessionFact, ...]

    def __post_init__(self) -> None:
        _require_text(self.version, _VERSION, "Evidence registry version")
        if not isinstance(self.facts, tuple) or not all(
            isinstance(fact, NSEEquityAcceptedSessionFact) for fact in self.facts
        ):
            _fail("Evidence registry facts must be a tuple of NSEEquityAcceptedSessionFact.")
        by_fact: dict[str, NSEEquityAcceptedSessionFact] = {}
        by_source: dict[str, tuple[str, str]] = {}
        for fact in self.facts:
            earlier = by_fact.get(fact.fact_id)
            if earlier is not None:
                detail = "repeated" if earlier == fact else "recorded with conflicting content"
                _fail(f"Fact {fact.fact_id} is {detail}.")
            by_fact[fact.fact_id] = fact
            for source in fact.sources:
                identity = (source.document, source.sha256)
                if by_source.setdefault(source.source_id, identity) != identity:
                    _fail(f"Source {source.source_id} names two different documents.")
        object.__setattr__(self, "facts", tuple(sorted(self.facts, key=lambda f: f.fact_id)))

    @property
    def fact_ids(self) -> frozenset[str]:
        """Return the IDs of every fact in this registry."""
        return frozenset(fact.fact_id for fact in self.facts)

    def fact(self, fact_id: str) -> NSEEquityAcceptedSessionFact | None:
        """Return the fact recorded under exactly ``fact_id``, or None."""
        for fact in self.facts:
            if fact.fact_id == fact_id:
                return fact
        return None


# ----------------------------------------------------------------------
# Catalog records
# ----------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class NSEEquityOpeningRule:
    """The continuous-market opening, in local wall-clock time, over a closed period."""

    effective_from: date
    effective_through: date
    opens: time
    fact_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        _require_period(self.effective_from, self.effective_through, "Opening rule")
        _require_clock(self.opens, "Opening rule time")
        _require_ids(self.fact_ids, _FACT_ID, "Opening rule facts")


@dataclass(frozen=True, slots=True)
class NSEEquityContinuousCloseRule:
    """The continuous-market close for one security group over a closed period."""

    group: NSEEquitySecurityGroup
    effective_from: date
    effective_through: date
    closes: time
    fact_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.group, NSEEquitySecurityGroup):
            _fail("Close rule group must be an NSEEquitySecurityGroup.")
        _require_period(self.effective_from, self.effective_through, f"{self.group} close rule")
        _require_clock(self.closes, f"{self.group} close rule time")
        _require_ids(self.fact_ids, _FACT_ID, f"{self.group} close rule facts")


@dataclass(frozen=True, slots=True)
class NSEEquityUnsupportedClose:
    """A period in which one group's continuous close is explicitly not supported."""

    group: NSEEquitySecurityGroup
    effective_from: date
    effective_through: date
    reason: str
    pending_decisions: tuple[str, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.group, NSEEquitySecurityGroup):
            _fail("Unsupported close group must be an NSEEquitySecurityGroup.")
        _require_period(self.effective_from, self.effective_through, "Unsupported close")
        _require_text(self.reason, _TEXT, "Unsupported close reason")
        _require_ids(self.pending_decisions, _DECISION_ID, "Unsupported close decisions")


@dataclass(frozen=True, slots=True)
class NSEEquityClosedDate:
    """One stated closure with its kind, reason and evidence."""

    day: date
    kind: NSEEquityClosureKind
    reason: str
    fact_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        _require_date(self.day, "Closed date")
        if not isinstance(self.kind, NSEEquityClosureKind):
            _fail(f"Closed date {self.day} kind must be an NSEEquityClosureKind.")
        _require_text(self.reason, _TEXT, f"Closed date {self.day} reason")
        _require_ids(self.fact_ids, _FACT_ID, f"Closed date {self.day} facts")
        is_weekend = self.day.weekday() >= _SATURDAY
        if is_weekend != (self.kind is NSEEquityClosureKind.WEEKEND_TRADING_HOLIDAY):
            _fail(f"Closed date {self.day} kind {self.kind} does not match its weekday.")


@dataclass(frozen=True, slots=True)
class NSEEquitySpecialSessionReference:
    """A stated special session, recorded with its evidence but not resolved.

    It is never resolved as a session until its operational handling is
    approved; ``pending_decisions`` names what that waits for.
    """

    day: date
    name: str
    fact_ids: tuple[str, ...]
    pending_decisions: tuple[str, ...]

    def __post_init__(self) -> None:
        _require_date(self.day, "Special session date")
        _require_text(self.name, _TEXT, f"Special session {self.day} name")
        _require_ids(self.fact_ids, _FACT_ID, f"Special session {self.day} facts")
        _require_ids(self.pending_decisions, _DECISION_ID, f"Special session {self.day} decisions")


@dataclass(frozen=True, slots=True)
class NSEEquityExceptionReference:
    """An accepted instrument- or day-level exception that the catalog cannot determine.

    It is reported on every normal-session date it may apply to, so no
    consumer mistakes a venue session for a security's actual trading day.
    """

    fact_id: str
    effective_from: date
    description: str

    def __post_init__(self) -> None:
        _require_text(self.fact_id, _FACT_ID, "Exception fact")
        _require_date(self.effective_from, f"Exception {self.fact_id} effective-from")
        _require_text(self.description, _TEXT, f"Exception {self.fact_id} description")


_Dated = NSEEquityOpeningRule | NSEEquityContinuousCloseRule | NSEEquityUnsupportedClose


# ----------------------------------------------------------------------
# Catalog
# ----------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class NSEEquitySessionCatalog:
    """One explicit, versioned, evidence-checked NSE cash-equity session catalog.

    It is checked whole at construction. A cited fact missing from
    ``evidence``, a rule outside a cited fact's boundaries or outside the
    coverage, overlapping rules, a normal session without a complete opening
    and close, or a date stated twice in contradictory ways is refused before
    any date can be answered.
    """

    exchange_code: ExchangeCode
    version: str
    timezone: str
    evidence: NSEEquityAcceptedSessionEvidence
    calendar_from: date
    session_rules_from: date
    coverage_through: date
    coverage_basis: str
    normal_sessions: frozenset[date]
    normal_session_fact_ids: tuple[str, ...]
    opening_rules: tuple[NSEEquityOpeningRule, ...]
    close_rules: tuple[NSEEquityContinuousCloseRule, ...]
    unsupported_closes: tuple[NSEEquityUnsupportedClose, ...]
    closed_dates: tuple[NSEEquityClosedDate, ...]
    special_sessions: tuple[NSEEquitySpecialSessionReference, ...]
    exceptions: tuple[NSEEquityExceptionReference, ...]
    limitations: tuple[str, ...]
    unresolved_fact_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        self._validate_identity()
        self._validate_records()
        self._validate_citations()
        self._validate_rules()
        self._validate_dates()

    @property
    def is_production(self) -> bool:
        """Return whether this is the reviewed production catalog object itself."""
        from northstar_infrastructure.market_data.nse_accepted_equity_session_evidence import (
            PRODUCTION_NSE_EQUITY_SESSION_CATALOG,
        )

        return self is PRODUCTION_NSE_EQUITY_SESSION_CATALOG

    @property
    def authority(self) -> str:
        """Return the reference identity carried by every answer from this catalog."""
        prefix = (
            PRODUCTION_EQUITY_SESSION_AUTHORITY_PREFIX
            if self.is_production
            else TEST_EQUITY_SESSION_AUTHORITY_PREFIX
        )
        return (
            f"{prefix} {self.version} (evidence {self.evidence.version}, coverage through "
            f"{self.coverage_through.isoformat()})"
        )

    def _validate_identity(self) -> None:
        if not isinstance(self.exchange_code, ExchangeCode):
            _fail("Catalog exchange_code must be an ExchangeCode.")
        _require_text(self.version, _VERSION, "Catalog version")
        _require_text(self.timezone, _TEXT, "Catalog timezone")
        try:
            ZoneInfo(self.timezone)
        except (ZoneInfoNotFoundError, ValueError) as exc:
            raise NSEEquitySessionEvidenceError(
                f"Catalog timezone {self.timezone!r} is unknown."
            ) from exc
        if not isinstance(self.evidence, NSEEquityAcceptedSessionEvidence):
            _fail("Catalog evidence must be an NSEEquityAcceptedSessionEvidence registry.")
        for name in ("calendar_from", "session_rules_from", "coverage_through"):
            _require_date(getattr(self, name), f"Catalog {name}")
        if not self.calendar_from <= self.session_rules_from <= self.coverage_through:
            _fail(
                "Catalog coverage must satisfy calendar_from <= session_rules_from <= "
                "coverage_through."
            )
        _require_text(self.coverage_basis, _TEXT, "Catalog coverage basis")
        if not isinstance(self.unresolved_fact_ids, tuple):
            _fail("Catalog unresolved_fact_ids must be a tuple.")
        for fact_id in self.unresolved_fact_ids:
            _require_text(fact_id, _FACT_ID, "Catalog unresolved fact")
            if fact_id in self.evidence.fact_ids:
                _fail(f"Unresolved fact {fact_id} is accepted; it cannot also be unresolved.")
        if not isinstance(self.limitations, tuple):
            _fail("Catalog limitations must be a tuple.")
        for note in self.limitations:
            _require_text(note, _TEXT, "Catalog limitation")

    def _validate_records(self) -> None:
        expected: tuple[tuple[str, type], ...] = (
            ("opening_rules", NSEEquityOpeningRule),
            ("close_rules", NSEEquityContinuousCloseRule),
            ("unsupported_closes", NSEEquityUnsupportedClose),
            ("closed_dates", NSEEquityClosedDate),
            ("special_sessions", NSEEquitySpecialSessionReference),
            ("exceptions", NSEEquityExceptionReference),
        )
        for name, kind in expected:
            records = getattr(self, name)
            if not isinstance(records, tuple) or not all(isinstance(r, kind) for r in records):
                _fail(f"Catalog {name} must be a tuple of {kind.__name__} values.")
        if not isinstance(self.normal_sessions, frozenset) or not all(
            _is_plain_date(day) for day in self.normal_sessions
        ):
            _fail("Catalog normal_sessions must be a frozenset of plain dates.")
        _require_ids(self.normal_session_fact_ids, _FACT_ID, "Catalog normal-session facts")

    def _accepted(self, fact_id: str, subject: str) -> NSEEquityAcceptedSessionFact:
        fact = self.evidence.fact(fact_id)
        if fact is None:
            _fail(
                f"{subject} cites {fact_id}, which is not accepted in evidence registry "
                f"{self.evidence.version}."
            )
        return fact  # type: ignore[return-value]

    def _validate_citations(self) -> None:
        for fact_id in self.normal_session_fact_ids:
            self._accepted(fact_id, "Normal-session basis")
        for rule in self.opening_rules:
            facts = [self._accepted(f, "Opening rule") for f in rule.fact_ids]
            self._require_backing(facts, rule.effective_from, rule.effective_through, "Opening")
            if not any(NSEEquitySecurityGroup.ALL_NORMAL_MARKET in f.groups for f in facts):
                _fail(
                    f"Opening rule from {rule.effective_from} cites no fact stating a boundary "
                    "for all normal-market securities."
                )
        for rule in self.close_rules:
            subject = f"{rule.group} close rule from {rule.effective_from}"
            facts = [self._accepted(f, subject) for f in rule.fact_ids]
            self._require_backing(facts, rule.effective_from, rule.effective_through, subject)
            if not any(rule.group in fact.groups for fact in facts):
                _fail(f"{subject} cites no fact stating a boundary for {rule.group}.")
        for closed in self.closed_dates:
            facts = [self._accepted(f, f"Closed date {closed.day}") for f in closed.fact_ids]
            self._require_backing(facts, closed.day, closed.day, f"Closed date {closed.day}")
        for special in self.special_sessions:
            subject = f"Special session {special.day}"
            facts = [self._accepted(f, subject) for f in special.fact_ids]
            self._require_backing(facts, special.day, special.day, subject)
        seen: set[str] = set()
        for exception in self.exceptions:
            if exception.fact_id in seen:
                _fail(f"Exception {exception.fact_id} is listed twice.")
            seen.add(exception.fact_id)
            fact = self._accepted(exception.fact_id, "Exception")
            if fact.effective_from is None or exception.effective_from < fact.effective_from:
                _fail(f"Exception {exception.fact_id} starts before its accepted boundary.")

    @staticmethod
    def _require_backing(
        facts: Iterable[NSEEquityAcceptedSessionFact],
        effective_from: date,
        effective_through: date,
        subject: str,
    ) -> None:
        for fact in facts:
            if not fact.covers_period(effective_from, effective_through):
                _fail(
                    f"{subject} ({effective_from} to {effective_through}) extends beyond the "
                    f"accepted boundaries of {fact.fact_id}."
                )

    def _validate_rules(self) -> None:
        dated: list[tuple[str, _Dated]] = [("Opening rule", rule) for rule in self.opening_rules]
        dated += [(f"{rule.group} close rule", rule) for rule in self.close_rules]
        dated += [(f"{rule.group} unsupported close", rule) for rule in self.unsupported_closes]
        for subject, rule in dated:
            if (
                rule.effective_from < self.session_rules_from
                or rule.effective_through > self.coverage_through
            ):
                _fail(
                    f"{subject} from {rule.effective_from} lies outside the session coverage "
                    f"{self.session_rules_from} to {self.coverage_through}."
                )
        self._refuse_overlap("Opening rules", self.opening_rules)
        for group in NSEEquitySecurityGroup:
            same: list[_Dated] = [r for r in self.close_rules if r.group is group]
            same += [r for r in self.unsupported_closes if r.group is group]
            self._refuse_overlap(f"{group} closes", same)
        universal = [
            r for r in self.close_rules if r.group is NSEEquitySecurityGroup.ALL_NORMAL_MARKET
        ]
        universal += [
            r
            for r in self.unsupported_closes
            if r.group is NSEEquitySecurityGroup.ALL_NORMAL_MARKET
        ]
        dependent = [r for r in self.close_rules if r.group in _GROUP_DEPENDENT]
        dependent += [r for r in self.unsupported_closes if r.group in _GROUP_DEPENDENT]
        for first in universal:
            for second in dependent:
                if _overlaps(first, second):
                    _fail(
                        "A close for all normal-market securities overlaps a group-dependent "
                        f"close from {second.effective_from}; the records contradict each other."
                    )

    @staticmethod
    def _refuse_overlap(subject: str, rules: Iterable[_Dated]) -> None:
        ordered = sorted(rules, key=lambda rule: rule.effective_from)
        for earlier, later in zip(ordered, ordered[1:], strict=False):
            if _overlaps(earlier, later):
                _fail(
                    f"{subject} overlap from {later.effective_from}; contradictory records are "
                    "not resolved by order."
                )

    def _validate_dates(self) -> None:
        closed_days = [closed.day for closed in self.closed_dates]
        if len(set(closed_days)) != len(closed_days):
            _fail("A closed date is listed twice.")
        special_days = [special.day for special in self.special_sessions]
        if len(set(special_days)) != len(special_days):
            _fail("A special session is listed twice.")
        for day in (*closed_days, *special_days):
            if day < self.calendar_from:
                _fail(f"Date {day} lies before the catalog calendar_from {self.calendar_from}.")
        normal_basis = [
            self._accepted(f, "Normal-session basis") for f in self.normal_session_fact_ids
        ]
        for day in sorted(self.normal_sessions):
            subject = f"Normal session {day}"
            if not self.session_rules_from <= day <= self.coverage_through:
                _fail(f"{subject} lies outside the session coverage.")
            if day.weekday() >= _SATURDAY:
                _fail(f"{subject} falls on a weekend; a weekend session must be a special session.")
            if day in closed_days:
                _fail(f"{subject} is also stated as closed.")
            if day in special_days:
                _fail(f"{subject} is also stated as a special session.")
            if not any(fact.covers_period(day, day) for fact in normal_basis):
                _fail(f"{subject} lies outside every accepted normal-session basis fact.")
            openings = [rule for rule in self.opening_rules if _covers(rule, day)]
            if len(openings) != 1:
                _fail(f"{subject} has no stated opening rule.")
            groups = {
                rule.group
                for rule in (*self.close_rules, *self.unsupported_closes)
                if _covers(rule, day)
            }
            if groups != {NSEEquitySecurityGroup.ALL_NORMAL_MARKET} and groups != set(
                _GROUP_DEPENDENT
            ):
                _fail(f"{subject} has an incomplete continuous-close statement.")


# ----------------------------------------------------------------------
# Answers
# ----------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class NSEEquityContinuousClose:
    """The continuous close for one security group on one date, or why it is unsupported.

    Exactly one of ``closes_at`` and ``unsupported_reason`` is set.
    """

    group: NSEEquitySecurityGroup
    closes_at: PointInTime | None
    fact_ids: tuple[str, ...]
    unsupported_reason: str | None
    pending_decisions: tuple[str, ...]

    @property
    def is_supported(self) -> bool:
        """Return whether the close is stated by accepted evidence."""
        return self.closes_at is not None


@dataclass(frozen=True, slots=True)
class NSEEquityVenueDay:
    """The catalog's answer for one venue and date.

    It is a venue-level answer only. It names no security and does not say
    which group any security belongs to, or whether any security traded.
    ``possible_exceptions`` lists accepted exceptions the catalog cannot
    determine for the date.
    """

    exchange_code: ExchangeCode
    trading_date: date
    status: NSEEquityVenueDayStatus
    reason: str
    reference: str
    opens_at: PointInTime | None
    continuous_closes: tuple[NSEEquityContinuousClose, ...]
    fact_ids: tuple[str, ...]
    possible_exceptions: tuple[NSEEquityExceptionReference, ...]
    pending_decisions: tuple[str, ...]
    limitations: tuple[str, ...]

    @property
    def resolves_instrument_applicability(self) -> bool:
        """Return whether this answer determines any security's group; never."""
        return False

    @property
    def is_close_group_dependent(self) -> bool:
        """Return whether the continuous close differs by CAS eligibility on this date."""
        return any(close.group in _GROUP_DEPENDENT for close in self.continuous_closes)

    def continuous_close_for(self, group: NSEEquitySecurityGroup) -> NSEEquityContinuousClose:
        """Return the stated or explicitly unsupported close of ``group``, or raise."""
        for close in self.continuous_closes:
            if close.group is group:
                return close
        raise NSEEquitySessionUnavailableError(
            f"No continuous close for {group} is stated on {self.trading_date.isoformat()} "
            f"({self.status}).",
            self,
        )


class NSEEquityVenueSessionReference:
    """Answers venue-level questions about dates from one catalog, without raising for dates."""

    def __init__(self, catalog: NSEEquitySessionCatalog) -> None:
        if not isinstance(catalog, NSEEquitySessionCatalog):
            raise TypeError(
                "NSEEquityVenueSessionReference catalog must be an NSEEquitySessionCatalog."
            )
        self._catalog = catalog
        self._zone = ZoneInfo(catalog.timezone)
        self._closed = {closed.day: closed for closed in catalog.closed_dates}
        self._special = {special.day: special for special in catalog.special_sessions}

    @classmethod
    def production(cls) -> NSEEquityVenueSessionReference:
        """Return the reference over the reviewed production catalog."""
        from northstar_infrastructure.market_data.nse_accepted_equity_session_evidence import (
            PRODUCTION_NSE_EQUITY_SESSION_CATALOG,
        )

        return cls(PRODUCTION_NSE_EQUITY_SESSION_CATALOG)

    @property
    def catalog(self) -> NSEEquitySessionCatalog:
        """Return the catalog this reference reads."""
        return self._catalog

    def lookup(self, exchange_code: ExchangeCode, trading_date: date) -> NSEEquityVenueDay:
        """Return what the catalog states about ``trading_date`` on ``exchange_code``."""
        if not isinstance(exchange_code, ExchangeCode):
            raise TypeError("NSEEquityVenueSessionReference exchange_code must be an ExchangeCode.")
        if not _is_plain_date(trading_date):
            raise TypeError("NSEEquityVenueSessionReference trading_date must be a datetime.date.")
        catalog = self._catalog
        if exchange_code != catalog.exchange_code:
            raise EquitySessionResolutionError(
                f"Unsupported equity venue: {exchange_code}. "
                f"Supported venues are ['{catalog.exchange_code}']."
            )
        day = trading_date.isoformat()
        # A recorded special session is reported as one even beyond the coverage
        # horizon: it is never resolvable here either way, and saying why is safer.
        special = self._special.get(trading_date)
        if special is not None:
            return self._answer(
                trading_date,
                NSEEquityVenueDayStatus.SPECIAL_SESSION_UNSUPPORTED,
                f"{day} is a special session ({special.name}); its operational handling awaits "
                f"owner decisions {', '.join(special.pending_decisions)}.",
                fact_ids=special.fact_ids,
                pending_decisions=special.pending_decisions,
            )
        if trading_date < catalog.calendar_from or trading_date > catalog.coverage_through:
            return self._answer(
                trading_date,
                NSEEquityVenueDayStatus.OUTSIDE_COVERAGE,
                f"{day} lies outside the catalog coverage {catalog.calendar_from.isoformat()} "
                f"to {catalog.coverage_through.isoformat()} ({catalog.coverage_basis}).",
            )
        closed = self._closed.get(trading_date)
        if closed is not None:
            return self._answer(
                trading_date,
                NSEEquityVenueDayStatus.CLOSED,
                f"{day}: {closed.kind}, {closed.reason}",
                fact_ids=closed.fact_ids,
            )
        if trading_date < catalog.session_rules_from:
            return self._answer(
                trading_date,
                NSEEquityVenueDayStatus.SESSION_RULES_NOT_ESTABLISHED,
                f"No accepted session rule applies before "
                f"{catalog.session_rules_from.isoformat()}; {day} is not a stated closure.",
            )
        if trading_date in catalog.normal_sessions:
            return self._normal(trading_date)
        if trading_date.weekday() >= _SATURDAY:
            return self._answer(
                trading_date,
                NSEEquityVenueDayStatus.WEEKEND_NOT_STATED,
                f"{day} falls on a weekend that no accepted fact states as closed or as a "
                "session; it is not inferred either way.",
            )
        return self._answer(
            trading_date,
            NSEEquityVenueDayStatus.NOT_STATED,
            f"{day} is not stated in {catalog.authority}.",
        )

    def _normal(self, trading_date: date) -> NSEEquityVenueDay:
        catalog = self._catalog
        opening = next(rule for rule in catalog.opening_rules if _covers(rule, trading_date))
        closes: list[NSEEquityContinuousClose] = []
        for rule in catalog.close_rules:
            if _covers(rule, trading_date):
                closes.append(
                    NSEEquityContinuousClose(
                        group=rule.group,
                        closes_at=self._utc(trading_date, rule.closes),
                        fact_ids=rule.fact_ids,
                        unsupported_reason=None,
                        pending_decisions=(),
                    )
                )
        pending: list[str] = []
        for gap in catalog.unsupported_closes:
            if _covers(gap, trading_date):
                closes.append(
                    NSEEquityContinuousClose(
                        group=gap.group,
                        closes_at=None,
                        fact_ids=(),
                        unsupported_reason=gap.reason,
                        pending_decisions=gap.pending_decisions,
                    )
                )
                pending.extend(gap.pending_decisions)
        closes.sort(key=lambda close: list(NSEEquitySecurityGroup).index(close.group))
        facts = [*catalog.normal_session_fact_ids, *opening.fact_ids]
        for close in closes:
            facts.extend(close.fact_ids)
        exceptions = tuple(
            exception
            for exception in catalog.exceptions
            if exception.effective_from <= trading_date
        )
        return self._answer(
            trading_date,
            NSEEquityVenueDayStatus.NORMAL_SESSION,
            f"{trading_date.isoformat()} is a normal session of the venue.",
            fact_ids=tuple(facts),
            opens_at=self._utc(trading_date, opening.opens),
            continuous_closes=tuple(closes),
            possible_exceptions=exceptions,
            pending_decisions=tuple(pending),
        )

    def _answer(
        self,
        trading_date: date,
        status: NSEEquityVenueDayStatus,
        reason: str,
        *,
        fact_ids: tuple[str, ...] = (),
        opens_at: PointInTime | None = None,
        continuous_closes: tuple[NSEEquityContinuousClose, ...] = (),
        possible_exceptions: tuple[NSEEquityExceptionReference, ...] = (),
        pending_decisions: tuple[str, ...] = (),
    ) -> NSEEquityVenueDay:
        ordered = tuple(sorted(dict.fromkeys(fact_ids)))
        reference = self._catalog.authority
        if ordered:
            reference = f"{reference}: {' '.join(ordered)}"
        if self._catalog.unresolved_fact_ids:
            reference = f"{reference}; not accepted: {' '.join(self._catalog.unresolved_fact_ids)}"
        return NSEEquityVenueDay(
            exchange_code=self._catalog.exchange_code,
            trading_date=trading_date,
            status=status,
            reason=reason,
            reference=reference,
            opens_at=opens_at,
            continuous_closes=continuous_closes,
            fact_ids=ordered,
            possible_exceptions=possible_exceptions,
            pending_decisions=tuple(dict.fromkeys(pending_decisions)),
            limitations=self._catalog.limitations,
        )

    def _utc(self, trading_date: date, clock: time) -> PointInTime:
        """Convert a local wall-clock time on ``trading_date`` to a canonical UTC PointInTime."""
        in_utc = datetime.combine(trading_date, clock, tzinfo=self._zone).astimezone(UTC)
        return PointInTime(in_utc.strftime("%Y-%m-%dT%H:%M:%SZ"))


class NSEEquityTradingSessionResolver(EquityTradingSessionResolver):
    """EI-1A session resolution from an evidence-gated catalog, where it is expressible.

    Returns a NORMAL session only when one continuous close is stated for all
    normal-market securities. Returns None for a stated closure. Raises
    ``NSEEquitySessionRequiresInstrumentApplicabilityError`` when the close
    depends on CAS eligibility, and ``NSEEquitySessionUnavailableError`` for a
    special session, an unstated date or a date outside coverage.
    """

    def __init__(self, catalog: NSEEquitySessionCatalog) -> None:
        self._reference = NSEEquityVenueSessionReference(catalog)

    @classmethod
    def production(cls) -> NSEEquityTradingSessionResolver:
        """Return the resolver over the reviewed production catalog."""
        from northstar_infrastructure.market_data.nse_accepted_equity_session_evidence import (
            PRODUCTION_NSE_EQUITY_SESSION_CATALOG,
        )

        return cls(PRODUCTION_NSE_EQUITY_SESSION_CATALOG)

    @property
    def venue_reference(self) -> NSEEquityVenueSessionReference:
        """Return the venue-level reference this resolver answers from."""
        return self._reference

    def resolve(
        self, exchange_code: ExchangeCode, trading_date: date
    ) -> EquityTradingSession | None:
        """Return the session labelled ``trading_date``, or None if the venue is closed."""
        venue_day = self._reference.lookup(exchange_code, trading_date)
        status = venue_day.status
        if status is NSEEquityVenueDayStatus.CLOSED:
            return None
        if status is not NSEEquityVenueDayStatus.NORMAL_SESSION:
            raise NSEEquitySessionUnavailableError(
                f"Equity session on {trading_date.isoformat()} is unavailable ({status}): "
                f"{venue_day.reason}",
                venue_day,
            )
        if venue_day.is_close_group_dependent:
            raise NSEEquitySessionRequiresInstrumentApplicabilityError(
                f"Equity session on {trading_date.isoformat()} has a continuous close that "
                "depends on each security's CAS eligibility, which this reference does not "
                "resolve; no venue-wide session is returned.",
                venue_day,
            )
        close = venue_day.continuous_close_for(NSEEquitySecurityGroup.ALL_NORMAL_MARKET)
        if venue_day.opens_at is None or close.closes_at is None:
            raise NSEEquitySessionUnavailableError(
                f"Equity session on {trading_date.isoformat()} has no supported continuous phase.",
                venue_day,
            )
        return EquityTradingSession(
            exchange_code=venue_day.exchange_code,
            trading_date=trading_date,
            kind=EquitySessionKind.NORMAL,
            phases=(EquityTradingPhase(opens_at=venue_day.opens_at, closes_at=close.closes_at),),
            reference=venue_day.reference,
        )
