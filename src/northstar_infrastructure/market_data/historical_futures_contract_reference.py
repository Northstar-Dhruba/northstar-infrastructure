"""Historical futures contract reference facts and the cutoff source built on them (R-1B-1).

A contract reference records what an authoritative source says about one
individual contract: when it was listed, its actual last tradable session, and
its tick and lot size over time. Each fact carries its own source. Nothing here
is derived from an expiration date, a first candle, today's exchange rules or
weekday arithmetic, and nothing is fetched.

This slice provides the mechanism only. No real NSE contract fact is recorded
anywhere in Northstar yet: populating verified references is R-1B-2.

Synthetic and verified references
---------------------------------
``HistoricalFuturesReferenceKind`` has one member, SYNTHETIC. A reference
cannot be made authoritative by its citation text -- "NSE/FAOP/12345" in a
synthetic reference is still synthetic -- and there is no flag that upgrades
it. The Application cutoff port carries only an ``authority`` string, so the
distinction is enforced here: every cutoff this module returns has an
authority beginning ``synthetic-reference``. A verified kind will be added in
R-1B-2 together with a reviewed, version-controlled reference module, in the
same way the NSE calendar reference is maintained.

Effective-dated values
----------------------
A tick or lot size applies from ``effective_from`` through ``effective_through``,
both inclusive. ``effective_through=None`` means "for the rest of this
contract's life", which ends at its last tradable session; it never reaches
past it. Periods of one attribute may not overlap; adjacent periods (one ends
the day before the next starts) are fine. A date outside the contract's life,
or inside it but covered by no period, is unavailable: the lookup raises, and
never falls back to the nearest or latest value.

Calendar checks
---------------
Dates are checked with the existing NSE calendar resolver, never a weekday
rule. A date in an unloaded year, or on a special session without notified
timings, is UNRESOLVED -- not assumed to be a session and not assumed not to
be one. The cutoff source refuses to answer for a last session that is not
verified as a session.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from enum import StrEnum

from northstar_application.ports import (
    FuturesContractCutoff,
    FuturesContractCutoffSource,
    FuturesSessionResolutionError,
    FuturesTradingSession,
    FuturesTradingSessionResolver,
)
from northstar_core.futures import FuturesContract, FuturesProductReference

SYNTHETIC_REFERENCE_AUTHORITY_PREFIX = "synthetic-reference"

# Printable ASCII, no leading or trailing space.
_TEXT = re.compile(r"[\x21-\x7e](?:[\x20-\x7e]{0,254}[\x21-\x7e])?")
_VERSION = re.compile(r"[A-Za-z0-9][A-Za-z0-9._/-]{0,127}")
_ONE_DAY = timedelta(days=1)


class HistoricalFuturesContractReferenceError(ValueError):
    """Raised when contract reference data is invalid, contradictory or unverifiable."""


class HistoricalFuturesReferenceUnavailableError(LookupError):
    """Raised when a reference value is not established for the requested date."""


class HistoricalFuturesReferenceKind(StrEnum):
    """How a reference fact is known. Only SYNTHETIC exists until R-1B-2."""

    SYNTHETIC = "SYNTHETIC"


class HistoricalFuturesCalendarStatus(StrEnum):
    """What the exchange calendar says about one reference date."""

    SESSION = "SESSION"
    NOT_A_SESSION = "NOT_A_SESSION"
    UNRESOLVED = "UNRESOLVED"


def _error(message: str) -> HistoricalFuturesContractReferenceError:
    return HistoricalFuturesContractReferenceError(message)


def _require_date(value: object, name: str) -> None:
    # A datetime carries a clock; a reference date has none.
    if isinstance(value, datetime) or not isinstance(value, date):
        raise _error(f"Contract reference {name} must be a plain date.")


@dataclass(frozen=True, slots=True)
class HistoricalFuturesReferenceSource:
    """Where one reference fact comes from: its kind and the cited document."""

    kind: HistoricalFuturesReferenceKind
    citation: str

    def __post_init__(self) -> None:
        if not isinstance(self.kind, HistoricalFuturesReferenceKind):
            raise _error("Reference source kind must be a HistoricalFuturesReferenceKind.")
        if not isinstance(self.citation, str) or _TEXT.fullmatch(self.citation) is None:
            raise _error("Reference source must cite its document as printable text.")

    def __str__(self) -> str:
        return f"{self.kind.value}:{self.citation}"


def _validate_period(
    effective_from: object, effective_through: object, source: object, name: str
) -> None:
    _require_date(effective_from, f"{name} effective_from")
    if effective_through is not None:
        _require_date(effective_through, f"{name} effective_through")
        if effective_through < effective_from:  # type: ignore[operator]
            raise _error(f"{name} period ends before it starts.")
    if not isinstance(source, HistoricalFuturesReferenceSource):
        raise _error(f"{name} must name its HistoricalFuturesReferenceSource.")


@dataclass(frozen=True, slots=True)
class HistoricalFuturesTickSize:
    """A contract's minimum price increment over an inclusive period."""

    effective_from: date
    effective_through: date | None
    value: Decimal
    source: HistoricalFuturesReferenceSource

    def __post_init__(self) -> None:
        _validate_period(self.effective_from, self.effective_through, self.source, "Tick size")
        if not isinstance(self.value, Decimal) or not self.value.is_finite() or self.value <= 0:
            raise _error("Tick size must be a positive, finite Decimal.")


@dataclass(frozen=True, slots=True)
class HistoricalFuturesLotSize:
    """A contract's lot size, in underlying units, over an inclusive period."""

    effective_from: date
    effective_through: date | None
    value: int
    source: HistoricalFuturesReferenceSource

    def __post_init__(self) -> None:
        _validate_period(self.effective_from, self.effective_through, self.source, "Lot size")
        if isinstance(self.value, bool) or not isinstance(self.value, int) or self.value <= 0:
            raise _error("Lot size must be a positive integer.")


type _Period = HistoricalFuturesTickSize | HistoricalFuturesLotSize


@dataclass(frozen=True, slots=True)
class HistoricalFuturesContractReference:
    """The sourced listing, last tradable session and specifications of one contract.

    The expiration date is the contract's identity, not evidence of its last
    session: ``last_tradable_session`` is stated separately, with its own
    source, and may be earlier. It may never be after the expiration date or
    before the listing date. Periods are kept in date order.
    """

    contract: FuturesContract
    listing_date: date
    listing_source: HistoricalFuturesReferenceSource
    last_tradable_session: date
    last_session_source: HistoricalFuturesReferenceSource
    tick_sizes: tuple[HistoricalFuturesTickSize, ...]
    lot_sizes: tuple[HistoricalFuturesLotSize, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.contract, FuturesContract):
            raise _error("Contract reference contract must be a FuturesContract.")
        _require_date(self.listing_date, "listing date")
        _require_date(self.last_tradable_session, "last tradable session")
        for source, name in (
            (self.listing_source, "listing"),
            (self.last_session_source, "last tradable session"),
        ):
            if not isinstance(source, HistoricalFuturesReferenceSource):
                raise _error(f"Contract reference {name} must name its source.")
        expiration = date.fromisoformat(self.contract.expiration_date.value)
        if self.listing_date > expiration:
            raise _error(f"{self.contract} cannot be listed after its expiration date.")
        if self.last_tradable_session < self.listing_date:
            raise _error(f"{self.contract} last tradable session precedes its listing.")
        if self.last_tradable_session > expiration:
            raise _error(f"{self.contract} last tradable session is after its expiration date.")
        object.__setattr__(
            self, "tick_sizes", self._periods(self.tick_sizes, HistoricalFuturesTickSize, "tick")
        )
        object.__setattr__(
            self, "lot_sizes", self._periods(self.lot_sizes, HistoricalFuturesLotSize, "lot")
        )

    def _periods(self, periods: object, kind: type, name: str) -> tuple:
        if not isinstance(periods, tuple) or not all(isinstance(p, kind) for p in periods):
            raise _error(f"{self.contract} {name} sizes must be a tuple of {kind.__name__}.")
        ordered = tuple(sorted(periods, key=lambda p: p.effective_from))
        for period in ordered:
            if period.effective_from > self.last_tradable_session or (
                period.effective_through is not None
                and period.effective_through < self.listing_date
            ):
                raise _error(f"{self.contract} {name} size period lies outside its life.")
        for earlier, later in zip(ordered, ordered[1:], strict=False):
            if (
                earlier.effective_through is None
                or earlier.effective_through >= later.effective_from
            ):
                raise _error(
                    f"{self.contract} {name} size periods overlap on or after "
                    f"{later.effective_from.isoformat()}."
                )
        return ordered

    # -- as-of lookup -------------------------------------------------------

    def tick_size_on(self, day: date) -> Decimal:
        """Return the tick size in force on ``day``, or raise when it is not established."""
        return self._value_on(self.tick_sizes, day, "tick size").value  # type: ignore[return-value]

    def lot_size_on(self, day: date) -> int:
        """Return the lot size in force on ``day``, or raise when it is not established."""
        return self._value_on(self.lot_sizes, day, "lot size").value  # type: ignore[return-value]

    def _value_on(self, periods: tuple[_Period, ...], day: date, name: str) -> _Period:
        _require_date(day, f"{name} lookup date")
        if not self.listing_date <= day <= self.last_tradable_session:
            raise HistoricalFuturesReferenceUnavailableError(
                f"{self.contract} {name} on {day.isoformat()}: outside the contract's life "
                f"({self.listing_date.isoformat()} to {self.last_tradable_session.isoformat()})."
            )
        for period in periods:
            if period.effective_from <= day and (
                period.effective_through is None or day <= period.effective_through
            ):
                return period
        raise HistoricalFuturesReferenceUnavailableError(
            f"{self.contract} {name} on {day.isoformat()} is not established by any reference."
        )

    # -- coverage -----------------------------------------------------------

    @property
    def tick_size_gaps(self) -> tuple[tuple[date, date], ...]:
        """Return the inclusive ranges of the contract's life with no tick size."""
        return self._gaps(self.tick_sizes)

    @property
    def lot_size_gaps(self) -> tuple[tuple[date, date], ...]:
        """Return the inclusive ranges of the contract's life with no lot size."""
        return self._gaps(self.lot_sizes)

    def _gaps(self, periods: tuple[_Period, ...]) -> tuple[tuple[date, date], ...]:
        gaps: list[tuple[date, date]] = []
        cursor = self.listing_date
        for period in periods:
            if period.effective_from > cursor:
                gaps.append((cursor, period.effective_from - _ONE_DAY))
            if period.effective_through is None:
                return tuple(gaps)
            cursor = max(cursor, period.effective_through + _ONE_DAY)
        if cursor <= self.last_tradable_session:
            gaps.append((cursor, self.last_tradable_session))
        return tuple(gaps)

    @property
    def kinds(self) -> frozenset[HistoricalFuturesReferenceKind]:
        """Return every source kind this reference's facts rest on."""
        sources = [self.listing_source, self.last_session_source]
        sources += [p.source for p in (*self.tick_sizes, *self.lot_sizes)]
        return frozenset(source.kind for source in sources)


@dataclass(frozen=True, slots=True)
class HistoricalFuturesContractReferenceCatalog:
    """One product's contract references under one version, in expiration order.

    ``version`` names this exact collection, so a dataset manifest can cite it
    as its contract reference. A contract appears at most once; a second entry,
    identical or conflicting, is refused rather than resolved by order.
    """

    version: str
    product: FuturesProductReference
    references: tuple[HistoricalFuturesContractReference, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.version, str) or _VERSION.fullmatch(self.version) is None:
            raise _error("Contract reference catalog must have a version name.")
        if not isinstance(self.product, FuturesProductReference):
            raise _error("Contract reference catalog product must be a FuturesProductReference.")
        if not isinstance(self.references, tuple) or not all(
            isinstance(r, HistoricalFuturesContractReference) for r in self.references
        ):
            raise _error("Contract reference catalog entries must be a tuple of references.")
        seen: dict[FuturesContract, HistoricalFuturesContractReference] = {}
        for reference in self.references:
            if reference.contract.product != self.product:
                raise _error(f"{reference.contract} is not a contract of {self.product}.")
            earlier = seen.setdefault(reference.contract, reference)
            if earlier is not reference:
                detail = "repeated" if earlier == reference else "given conflicting facts"
                raise _error(f"{reference.contract} is {detail} in catalog {self.version}.")
        ordered = tuple(sorted(self.references, key=lambda r: r.contract.expiration_date.value))
        object.__setattr__(self, "references", ordered)

    def reference_for(self, contract: FuturesContract) -> HistoricalFuturesContractReference | None:
        """Return the reference of exactly ``contract``, or None; never a neighbour."""
        for reference in self.references:
            if reference.contract == contract:
                return reference
        return None

    @property
    def is_synthetic(self) -> bool:
        """Return whether any fact in the catalog is synthetic."""
        return any(
            HistoricalFuturesReferenceKind.SYNTHETIC in reference.kinds
            for reference in self.references
        )


# ---------------------------------------------------------------------------
# Calendar checks
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class HistoricalFuturesCalendarFinding:
    """What the calendar says about one reference date, with any diagnostic."""

    day: date
    status: HistoricalFuturesCalendarStatus
    diagnostic: str | None


@dataclass(frozen=True, slots=True)
class HistoricalFuturesReferenceCalendarCheck:
    """The calendar findings for one contract's listing date and last tradable session."""

    contract: FuturesContract
    listing: HistoricalFuturesCalendarFinding
    last_session: HistoricalFuturesCalendarFinding

    @property
    def consistent(self) -> bool:
        """Return whether both dates are verified sessions."""
        return (
            self.listing.status is HistoricalFuturesCalendarStatus.SESSION
            and self.last_session.status is HistoricalFuturesCalendarStatus.SESSION
        )


def check_historical_futures_reference_calendar(
    reference: HistoricalFuturesContractReference, sessions: FuturesTradingSessionResolver
) -> HistoricalFuturesReferenceCalendarCheck:
    """Classify the listing date and last tradable session against the exchange calendar."""
    if not isinstance(reference, HistoricalFuturesContractReference):
        raise _error("Calendar check needs a HistoricalFuturesContractReference.")
    if not isinstance(sessions, FuturesTradingSessionResolver):
        raise _error("Calendar check needs a FuturesTradingSessionResolver.")
    product = reference.contract.product
    return HistoricalFuturesReferenceCalendarCheck(
        reference.contract,
        _finding(sessions, product, reference.listing_date),
        _finding(sessions, product, reference.last_tradable_session),
    )


def _finding(
    sessions: FuturesTradingSessionResolver, product: FuturesProductReference, day: date
) -> HistoricalFuturesCalendarFinding:
    try:
        session = sessions.resolve(product, day)
    except FuturesSessionResolutionError as exc:
        return HistoricalFuturesCalendarFinding(
            day, HistoricalFuturesCalendarStatus.UNRESOLVED, str(exc)
        )
    if session is None:
        return HistoricalFuturesCalendarFinding(
            day, HistoricalFuturesCalendarStatus.NOT_A_SESSION, None
        )
    return HistoricalFuturesCalendarFinding(day, HistoricalFuturesCalendarStatus.SESSION, None)


# ---------------------------------------------------------------------------
# Cutoff source
# ---------------------------------------------------------------------------


class HistoricalFuturesContractCutoffSource(FuturesContractCutoffSource):
    """Resolves a contract's cutoff from a reference catalog, verified against the calendar.

    - The answer is for exactly the contract asked about; any other contract,
      including a neighbouring expiry, gets None.
    - The cutoff is the referenced last tradable session, resolved by the
      calendar. The expiration date is never used.
    - A last session the calendar cannot resolve, or does not treat as a
      session, raises: a reference that cannot be verified is not answered.
    - The authority names the catalog version and the cited source, prefixed
      ``synthetic-reference`` -- the only kind that exists -- so no cutoff from
      this source can be read as exchange-verified.
    """

    def __init__(
        self,
        catalog: HistoricalFuturesContractReferenceCatalog,
        sessions: FuturesTradingSessionResolver,
    ) -> None:
        if not isinstance(catalog, HistoricalFuturesContractReferenceCatalog):
            raise _error("Cutoff source needs a HistoricalFuturesContractReferenceCatalog.")
        if not isinstance(sessions, FuturesTradingSessionResolver):
            raise _error("Cutoff source needs a FuturesTradingSessionResolver.")
        self._catalog = catalog
        self._sessions = sessions

    def resolve(self, contract: FuturesContract) -> FuturesContractCutoff | None:
        """Return the calendar-verified cutoff of ``contract``, or None if it has no reference."""
        if not isinstance(contract, FuturesContract):
            raise TypeError(
                "HistoricalFuturesContractCutoffSource contract must be a FuturesContract."
            )
        reference = self._catalog.reference_for(contract)
        if reference is None:
            return None
        session = self._verified_session(reference)
        return FuturesContractCutoff(contract, session, self._authority(reference))

    def _verified_session(
        self, reference: HistoricalFuturesContractReference
    ) -> FuturesTradingSession:
        day = reference.last_tradable_session
        try:
            session = self._sessions.resolve(reference.contract.product, day)
        except FuturesSessionResolutionError as exc:
            raise _error(
                f"{reference.contract} last tradable session {day.isoformat()} cannot be "
                f"verified against the calendar: {exc}"
            ) from exc
        if session is None:
            raise _error(
                f"{reference.contract} last tradable session {day.isoformat()} is not a "
                f"trading session of {reference.contract.product.exchange_code}."
            )
        if session.trading_date != day:
            raise _error(
                f"The calendar answered {session.trading_date.isoformat()} for {day.isoformat()}."
            )
        return session

    def _authority(self, reference: HistoricalFuturesContractReference) -> str:
        # Only SYNTHETIC exists; the prefix is fixed, never taken from the caller.
        if reference.last_session_source.kind is not HistoricalFuturesReferenceKind.SYNTHETIC:
            raise _error(f"No authority is defined for {reference.last_session_source.kind}.")
        return (
            f"{SYNTHETIC_REFERENCE_AUTHORITY_PREFIX} {self._catalog.version}: "
            f"{reference.last_session_source.citation}"
        )
