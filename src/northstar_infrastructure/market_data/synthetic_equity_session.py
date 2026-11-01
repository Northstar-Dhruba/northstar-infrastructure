"""Equity trading-session resolution from explicitly synthetic reference data.

This adapter exercises the Application equity-session port without asserting
any real exchange calendar. Its reference is supplied by the caller, is always
labelled synthetic, and states every fact it knows explicitly:

- the years it has loaded;
- the dates that are normal sessions, each trading the reference's normal
  continuous-trading phases;
- the dates that are closed, each with a stated reason;
- the dates that are special sessions, each with its own phases, or with no
  phases when the timings have not been established.

No NSE hours, holidays or special sessions are embedded here. Real reference
data is a separate, evidence-backed concern and is not accepted by this
resolver.

How one date resolves
---------------------
In this order, so that each rule can only be overridden by a more specific one:

1. A different venue raises.
2. A year that has not been loaded raises, whatever the reference says about
   the date.
3. A stated special session is a SPECIAL session, even on a stated closed
   date. A special session without established timings raises; it never falls
   back to the normal phases.
4. A stated closed date returns None.
5. A stated normal session is a NORMAL session with the normal phases.
6. Any other date raises. No session is inferred from the day of the week.

Wall-clock phases are converted to UTC through the reference's named IANA zone
(``Asia/Kolkata`` for an Indian venue), never through a hard-coded offset, and
never through the host's local timezone or clock.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from types import MappingProxyType
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from northstar_application.ports import (
    EquitySessionKind,
    EquitySessionResolutionError,
    EquityTradingPhase,
    EquityTradingSession,
    EquityTradingSessionResolver,
)
from northstar_core.foundation.value_objects import ExchangeCode, PointInTime

from northstar_infrastructure.market_data.historical_futures_contract_reference import (
    SYNTHETIC_REFERENCE_AUTHORITY_PREFIX,
)


class InvalidSyntheticEquitySessionReferenceError(EquitySessionResolutionError):
    """Raised when synthetic session reference data is incomplete or contradictory."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise InvalidSyntheticEquitySessionReferenceError(message)


def _is_plain_date(value: object) -> bool:
    return isinstance(value, date) and not isinstance(value, datetime)


@dataclass(frozen=True, slots=True)
class SyntheticEquityPhaseHours:
    """One continuous-trading phase as local wall-clock times on the session date."""

    opens: time
    closes: time

    def __post_init__(self) -> None:
        for name in ("opens", "closes"):
            value = getattr(self, name)
            _require(isinstance(value, time), f"Synthetic equity phase {name} must be a time.")
            _require(value.tzinfo is None, f"Synthetic equity phase {name} must be naive.")
            _require(
                value.microsecond == 0,
                f"Synthetic equity phase {name} cannot carry fractional seconds.",
            )
        _require(self.opens < self.closes, "Synthetic equity phase opens must be before closes.")


def _validate_phases(phases: object, subject: str) -> tuple[SyntheticEquityPhaseHours, ...]:
    _require(
        isinstance(phases, tuple) and len(phases) > 0,
        f"{subject} phases must be a non-empty tuple.",
    )
    for phase in phases:
        _require(
            isinstance(phase, SyntheticEquityPhaseHours),
            f"{subject} phases must be SyntheticEquityPhaseHours values.",
        )
    for earlier, later in zip(phases, phases[1:], strict=False):
        _require(
            earlier.closes < later.opens,
            f"{subject} phases must be ascending and separated by a break.",
        )
    return phases


@dataclass(frozen=True, slots=True)
class SyntheticEquitySpecialSession:
    """A stated special session. ``phases`` is None when its timings are not established."""

    name: str
    phases: tuple[SyntheticEquityPhaseHours, ...] | None

    def __post_init__(self) -> None:
        _require(
            isinstance(self.name, str) and bool(self.name.strip()),
            "Synthetic equity special session name must be a non-empty string.",
        )
        if self.phases is not None:
            _validate_phases(self.phases, f"Synthetic equity special session {self.name}")


@dataclass(frozen=True, slots=True)
class SyntheticEquitySessionReference:
    """Explicit, versioned, synthetic session facts for one venue.

    The reference is checked whole at construction: a date outside the loaded
    years, a date stated both as a normal session and as closed, or a missing
    reason or phase is refused before any date can be resolved.
    """

    exchange_code: ExchangeCode
    version: str
    timezone: str
    loaded_years: frozenset[int]
    normal_phases: tuple[SyntheticEquityPhaseHours, ...]
    normal_sessions: frozenset[date]
    closed_dates: Mapping[date, str]
    special_sessions: Mapping[date, SyntheticEquitySpecialSession]

    def __post_init__(self) -> None:
        _require(
            isinstance(self.exchange_code, ExchangeCode),
            "Synthetic equity session reference exchange_code must be an ExchangeCode.",
        )
        _require(
            isinstance(self.version, str) and bool(self.version.strip()),
            "Synthetic equity session reference version must be a non-empty string.",
        )
        _require(
            isinstance(self.timezone, str) and bool(self.timezone.strip()),
            "Synthetic equity session reference timezone must be a non-empty string.",
        )
        try:
            ZoneInfo(self.timezone)
        except (ZoneInfoNotFoundError, ValueError) as exc:
            raise InvalidSyntheticEquitySessionReferenceError(
                f"Synthetic equity session reference timezone {self.timezone!r} is unknown."
            ) from exc
        _require(
            isinstance(self.loaded_years, frozenset)
            and len(self.loaded_years) > 0
            and all(
                isinstance(year, int) and not isinstance(year, bool) for year in self.loaded_years
            ),
            "Synthetic equity session reference loaded_years must be a non-empty frozenset "
            "of years.",
        )
        _validate_phases(self.normal_phases, "Synthetic equity session reference normal")
        _require(
            isinstance(self.normal_sessions, frozenset),
            "Synthetic equity session reference normal_sessions must be a frozenset.",
        )
        _require(
            isinstance(self.closed_dates, Mapping),
            "Synthetic equity session reference closed_dates must be a mapping.",
        )
        _require(
            isinstance(self.special_sessions, Mapping),
            "Synthetic equity session reference special_sessions must be a mapping.",
        )

        for value in self.normal_sessions:
            self._require_loaded_date(value, "normal session")
        for value, reason in self.closed_dates.items():
            self._require_loaded_date(value, "closed date")
            _require(
                isinstance(reason, str) and bool(reason.strip()),
                f"Synthetic equity closed date {value} must state a reason.",
            )
        for value, special in self.special_sessions.items():
            self._require_loaded_date(value, "special session")
            _require(
                isinstance(special, SyntheticEquitySpecialSession),
                f"Synthetic equity special session {value} must be a "
                "SyntheticEquitySpecialSession.",
            )
        contradictory = sorted(self.normal_sessions & self.closed_dates.keys())
        _require(
            not contradictory,
            "Synthetic equity session reference states dates as both normal sessions and "
            f"closed: {[value.isoformat() for value in contradictory]}.",
        )

        object.__setattr__(self, "closed_dates", MappingProxyType(dict(self.closed_dates)))
        object.__setattr__(self, "special_sessions", MappingProxyType(dict(self.special_sessions)))

    @property
    def authority(self) -> str:
        """Return the reference identity carried by every session resolved from it."""
        return f"{SYNTHETIC_REFERENCE_AUTHORITY_PREFIX} {self.version}"

    def _require_loaded_date(self, value: object, subject: str) -> None:
        _require(_is_plain_date(value), f"Synthetic equity {subject} must be a plain date.")
        _require(
            value.year in self.loaded_years,
            f"Synthetic equity {subject} {value} lies outside the loaded years.",
        )


class SyntheticEquityTradingSessionResolver(EquityTradingSessionResolver):
    """Resolve equity sessions from one injected synthetic reference."""

    def __init__(self, reference: SyntheticEquitySessionReference) -> None:
        if not isinstance(reference, SyntheticEquitySessionReference):
            raise TypeError(
                "SyntheticEquityTradingSessionResolver reference must be a "
                "SyntheticEquitySessionReference."
            )
        self._reference = reference
        self._zone = ZoneInfo(reference.timezone)

    def resolve(
        self, exchange_code: ExchangeCode, trading_date: date
    ) -> EquityTradingSession | None:
        """Return the session labelled ``trading_date``, or None if the venue is closed."""
        if not isinstance(exchange_code, ExchangeCode):
            raise TypeError(
                "SyntheticEquityTradingSessionResolver exchange_code must be an ExchangeCode."
            )
        if not _is_plain_date(trading_date):
            raise TypeError(
                "SyntheticEquityTradingSessionResolver trading_date must be a datetime.date."
            )
        reference = self._reference
        if exchange_code != reference.exchange_code:
            raise EquitySessionResolutionError(
                f"Unsupported equity venue: {exchange_code}. "
                f"Supported venues are ['{reference.exchange_code}']."
            )
        if trading_date.year not in reference.loaded_years:
            raise EquitySessionResolutionError(
                f"Equity session year {trading_date.year} is not loaded in "
                f"{reference.authority}. Loaded years are {sorted(reference.loaded_years)}."
            )

        special = reference.special_sessions.get(trading_date)
        if special is not None:
            if special.phases is None:
                raise EquitySessionResolutionError(
                    f"Equity special session on {trading_date.isoformat()} ({special.name}) "
                    "has no established timings."
                )
            return self._session(trading_date, EquitySessionKind.SPECIAL, special.phases)
        if trading_date in reference.closed_dates:
            return None
        if trading_date in reference.normal_sessions:
            return self._session(trading_date, EquitySessionKind.NORMAL, reference.normal_phases)
        raise EquitySessionResolutionError(
            f"Equity session on {trading_date.isoformat()} is not stated in {reference.authority}."
        )

    def _session(
        self,
        trading_date: date,
        kind: EquitySessionKind,
        phases: tuple[SyntheticEquityPhaseHours, ...],
    ) -> EquityTradingSession:
        return EquityTradingSession(
            exchange_code=self._reference.exchange_code,
            trading_date=trading_date,
            kind=kind,
            phases=tuple(
                EquityTradingPhase(
                    opens_at=self._utc(trading_date, phase.opens),
                    closes_at=self._utc(trading_date, phase.closes),
                )
                for phase in phases
            ),
            reference=self._reference.authority,
        )

    def _utc(self, trading_date: date, clock: time) -> PointInTime:
        """Convert a local wall-clock time on ``trading_date`` to a canonical UTC PointInTime."""
        in_utc = datetime.combine(trading_date, clock, tzinfo=self._zone).astimezone(UTC)
        return PointInTime(in_utc.strftime("%Y-%m-%dT%H:%M:%SZ"))
