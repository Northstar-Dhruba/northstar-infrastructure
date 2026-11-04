"""NSE adapters for the Application research-window reference ports (EI-1C-3).

``NSEEquityVenueDayReferenceSource`` answers ``EquityVenueDayReferenceSource``
from the EI-1C-2 evidence-gated venue lookup. It reports a NORMAL_SESSION only
when every normal-market security group has an accepted continuous close on the
date, and gives the common boundary: the accepted opening and the earliest of
those closes. It never calls ``NSEEquityTradingSessionResolver.resolve`` and
never builds a full session: after the CAS go-live that would need each
security's group. If any group's close is unsupported (for example the non-CAS
close from 2026-08-03 to 2026-08-11, pending CE-D15), there is no common
boundary and the date is reported as such. Coverage, closures, special sessions
and unstated dates pass through from the catalog unchanged, and the catalog's
reference string -- with its coverage horizon and unaccepted dependencies -- is
carried on every answer.

``NoApprovedEquityListingApplicabilitySource`` is the production listing
applicability source. Northstar holds no approved stock/date applicability
evidence (special pre-open days, suspensions, call auctions), so it answers
UNKNOWN for every listing and date, and every real request fails closed
(CE-D18). Synthetic fixtures live in ``synthetic_equity_listing_applicability``
and are never used here.

Nothing reads a clock, provider or network.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from zoneinfo import ZoneInfo

from northstar_application.ports import (
    EquityListingApplicability,
    EquityListingApplicabilityClassification,
    EquityListingApplicabilitySource,
    EquityVenueDayClassification,
    EquityVenueDayReference,
    EquityVenueDayReferenceSource,
)
from northstar_core.domain.value_objects import ListingReference
from northstar_core.foundation.value_objects import ExchangeCode, PointInTime

from northstar_infrastructure.market_data.nse_equity_session_reference import (
    NSEEquityVenueDayStatus,
    NSEEquityVenueSessionReference,
)

_NSE_DAY = NSEEquityVenueDayStatus
_DAY = EquityVenueDayClassification
_STATUS: dict[NSEEquityVenueDayStatus, EquityVenueDayClassification] = {
    _NSE_DAY.CLOSED: _DAY.CLOSED,
    _NSE_DAY.SPECIAL_SESSION_UNSUPPORTED: _DAY.SPECIAL_SESSION,
    _NSE_DAY.SESSION_RULES_NOT_ESTABLISHED: _DAY.OUTSIDE_COVERAGE,
    _NSE_DAY.OUTSIDE_COVERAGE: _DAY.OUTSIDE_COVERAGE,
    _NSE_DAY.WEEKEND_NOT_STATED: _DAY.NOT_STATED,
    _NSE_DAY.NOT_STATED: _DAY.NOT_STATED,
}

NO_APPROVED_APPLICABILITY_SOURCE = "no-approved-nse-equity-listing-applicability-source"


class NSEEquityVenueDayReferenceSource(EquityVenueDayReferenceSource):
    """Common continuous boundaries of NSE normal sessions, from the accepted venue reference."""

    def __init__(self, reference: NSEEquityVenueSessionReference) -> None:
        if not isinstance(reference, NSEEquityVenueSessionReference):
            raise TypeError(
                "NSEEquityVenueDayReferenceSource reference must be an "
                "NSEEquityVenueSessionReference."
            )
        self._reference = reference
        self._zone = ZoneInfo(reference.catalog.timezone)

    @classmethod
    def production(cls) -> NSEEquityVenueDayReferenceSource:
        """Return the source over the reviewed production catalog."""
        return cls(NSEEquityVenueSessionReference.production())

    @property
    def is_production(self) -> bool:
        """Return whether this source answers from the reviewed production catalog object."""
        return self._reference.catalog.is_production

    def venue_day(self, exchange_code: ExchangeCode, trading_date: date) -> EquityVenueDayReference:
        """Return the common continuous boundary of ``trading_date``, or why there is none."""
        day = self._reference.lookup(exchange_code, trading_date)

        def answer(status: EquityVenueDayClassification, reason: str) -> EquityVenueDayReference:
            return EquityVenueDayReference(
                exchange_code=day.exchange_code,
                trading_date=trading_date,
                classification=status,
                common_opens_at=None,
                common_closes_at=None,
                utc_offset=None,
                reference=day.reference,
                reason=reason,
            )

        if day.status is not NSEEquityVenueDayStatus.NORMAL_SESSION:
            return answer(_STATUS[day.status], day.reason)
        unsupported = [close for close in day.continuous_closes if not close.is_supported]
        if day.opens_at is None or not day.continuous_closes or unsupported:
            reasons = "; ".join(
                f"{close.group}: {close.unsupported_reason}" for close in unsupported
            )
            return answer(
                EquityVenueDayClassification.NORMAL_SESSION_COMMON_BOUNDARY_UNSUPPORTED,
                f"{trading_date.isoformat()} is a scheduled normal session, but no common "
                f"continuous close is established for every security group. {reasons}".strip(),
            )
        earliest: PointInTime = day.continuous_closes[0].closes_at  # type: ignore[assignment]
        for close in day.continuous_closes[1:]:
            if close.closes_at.compare(earliest) < 0:  # type: ignore[union-attr]
                earliest = close.closes_at  # type: ignore[assignment]
        opens_utc = datetime.fromisoformat(day.opens_at.value.replace("Z", "+00:00"))
        return EquityVenueDayReference(
            exchange_code=day.exchange_code,
            trading_date=trading_date,
            classification=EquityVenueDayClassification.NORMAL_SESSION,
            common_opens_at=day.opens_at,
            common_closes_at=earliest,
            utc_offset=opens_utc.astimezone(UTC).astimezone(self._zone).utcoffset(),
            reference=day.reference,
            reason=(
                f"{trading_date.isoformat()} is a scheduled normal session; the common "
                "continuous boundary applies to every normal-market security group. It is not "
                "evidence that any security traded or that trading was uninterrupted."
            ),
        )


class NoApprovedEquityListingApplicabilitySource(EquityListingApplicabilitySource):
    """The production applicability source: no approved evidence exists, so always UNKNOWN."""

    def applicability(
        self, listing: ListingReference, trading_date: date
    ) -> EquityListingApplicability:
        """Return UNKNOWN for ``listing`` on ``trading_date``."""
        if not isinstance(listing, ListingReference):
            raise TypeError("listing must be a ListingReference.")
        if isinstance(trading_date, datetime) or not isinstance(trading_date, date):
            raise TypeError("trading_date must be a plain date.")
        return EquityListingApplicability(
            listing=listing,
            trading_date=trading_date,
            classification=EquityListingApplicabilityClassification.UNKNOWN,
            evidence_reference=NO_APPROVED_APPLICABILITY_SOURCE,
            reason=(
                "No approved stock/date applicability evidence exists for special pre-open "
                "days, suspensions or other exceptions (CE-D18); unknown fails closed."
            ),
        )
