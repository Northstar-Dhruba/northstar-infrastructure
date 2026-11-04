"""Provenance of NSE research-window references, from the sources that produced them (EI-2B).

``NSEEquityResearchReferenceProvenance`` is built over the exact venue and
applicability source objects a research window was evaluated with. It never
reads a reference's spelling. For a window it:

1. classifies each source by its exact type and, for the venue, by catalog
   identity:

   - ``NSEEquityVenueDayReferenceSource`` over the reviewed production catalog
     object -> ACCEPTED_PRODUCTION; over any other catalog -> SYNTHETIC;
   - ``SyntheticListingApplicabilitySource`` -> SYNTHETIC;
   - ``NoApprovedEquityListingApplicabilitySource`` -> ACCEPTED_PRODUCTION. It
     answers UNKNOWN for every listing, so no window evaluated with it is
     eligible;
   - any other type, subclasses included -> unknown provenance;

2. asks each source again for the window's exchange, listing and date, and
   requires the window's reference to be exactly the one it answers. A window
   evaluated with other sources, or with a reference renamed after the fact,
   is unknown provenance.

Unknown provenance raises UnknownEquityReferenceProvenanceError. Nothing reads
a clock, provider or network.
"""

from __future__ import annotations

from northstar_application.application_services import EquityResearchWindowEvaluation
from northstar_application.ports import (
    EquityListingApplicabilitySource,
    EquityReferenceProvenanceKind,
    EquityResearchReferenceProvenanceSource,
    EquityResearchWindowProvenance,
    EquityVenueDayReferenceSource,
    UnknownEquityReferenceProvenanceError,
)

from northstar_infrastructure.market_data.nse_equity_research_window_reference import (
    NoApprovedEquityListingApplicabilitySource,
    NSEEquityVenueDayReferenceSource,
)
from northstar_infrastructure.market_data.synthetic_equity_listing_applicability import (
    SyntheticListingApplicabilitySource,
)

_APPLICABILITY_KINDS: dict[type, EquityReferenceProvenanceKind] = {
    SyntheticListingApplicabilitySource: EquityReferenceProvenanceKind.SYNTHETIC,
    NoApprovedEquityListingApplicabilitySource: EquityReferenceProvenanceKind.ACCEPTED_PRODUCTION,
}


class NSEEquityResearchReferenceProvenance(EquityResearchReferenceProvenanceSource):
    """Classifies a window's references by the exact source objects that produced them."""

    def __init__(
        self,
        venue_source: EquityVenueDayReferenceSource,
        applicability_source: EquityListingApplicabilitySource,
    ) -> None:
        if type(venue_source) is not NSEEquityVenueDayReferenceSource:
            raise UnknownEquityReferenceProvenanceError(
                f"Venue reference source {type(venue_source).__name__} has no known provenance."
            )
        applicability_kind = _APPLICABILITY_KINDS.get(type(applicability_source))
        if applicability_kind is None:
            raise UnknownEquityReferenceProvenanceError(
                f"Applicability source {type(applicability_source).__name__} has no known "
                "provenance."
            )
        self._venue_source = venue_source
        self._applicability_source = applicability_source
        self._venue_kind = (
            EquityReferenceProvenanceKind.ACCEPTED_PRODUCTION
            if venue_source.is_production
            else EquityReferenceProvenanceKind.SYNTHETIC
        )
        self._applicability_kind = applicability_kind

    def classify(self, window: EquityResearchWindowEvaluation) -> EquityResearchWindowProvenance:
        """Return the provenance of ``window``'s references, or raise if any is not ours."""
        if not isinstance(window, EquityResearchWindowEvaluation):
            raise TypeError("window must be an EquityResearchWindowEvaluation.")
        venue = self._venue_source.venue_day(window.listing.exchange_code, window.trading_date)
        applicability = self._applicability_source.applicability(
            window.listing, window.trading_date
        )
        if window.venue_reference is None or window.venue_reference != venue.reference:
            raise UnknownEquityReferenceProvenanceError(
                "The window's venue reference was not produced by this venue source."
            )
        if (
            window.applicability_reference is None
            or window.applicability_reference != applicability.evidence_reference
        ):
            raise UnknownEquityReferenceProvenanceError(
                "The window's applicability reference was not produced by this applicability "
                "source."
            )
        return EquityResearchWindowProvenance(
            window.venue_reference,
            self._venue_kind,
            window.applicability_reference,
            self._applicability_kind,
        )
