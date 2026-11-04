"""Listing applicability from explicitly synthetic stock/date fixtures (EI-1C-3 tests).

This adapter exercises ``EquityListingApplicabilitySource`` without asserting
any real security-master, special pre-open or suspension record. Its records
are supplied by the caller, are always labelled synthetic, and state each
listing and date explicitly. A listing and date without a record is UNKNOWN,
never eligible. Nothing is inferred from a symbol existing, from market data,
from derivatives membership or from the current day.

It is a test double for the port. The production source is
``NoApprovedEquityListingApplicabilitySource``, which answers UNKNOWN.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

from northstar_application.ports import (
    EquityListingApplicability,
    EquityListingApplicabilityClassification,
    EquityListingApplicabilitySource,
)
from northstar_core.domain.value_objects import ListingReference

from northstar_infrastructure.market_data.historical_futures_contract_reference import (
    SYNTHETIC_REFERENCE_AUTHORITY_PREFIX,
)


class InvalidSyntheticListingApplicabilityError(ValueError):
    """Raised when synthetic applicability fixtures are malformed or contradictory."""


def _is_plain_date(value: object) -> bool:
    return isinstance(value, date) and not isinstance(value, datetime)


@dataclass(frozen=True, slots=True)
class SyntheticListingApplicabilityRecord:
    """One synthetic statement about one listing on one date."""

    listing: ListingReference
    trading_date: date
    classification: EquityListingApplicabilityClassification
    reason: str

    def __post_init__(self) -> None:
        if not isinstance(self.listing, ListingReference):
            raise InvalidSyntheticListingApplicabilityError("listing must be a ListingReference.")
        if not _is_plain_date(self.trading_date):
            raise InvalidSyntheticListingApplicabilityError("trading_date must be a plain date.")
        if not isinstance(self.classification, EquityListingApplicabilityClassification):
            raise InvalidSyntheticListingApplicabilityError(
                "classification must be an EquityListingApplicabilityClassification."
            )
        if self.classification is EquityListingApplicabilityClassification.UNKNOWN:
            raise InvalidSyntheticListingApplicabilityError(
                "UNKNOWN is the absence of a record; it is not stated as a fixture."
            )
        if not isinstance(self.reason, str) or not self.reason.strip():
            raise InvalidSyntheticListingApplicabilityError("reason must be a non-empty string.")


class SyntheticListingApplicabilitySource(EquityListingApplicabilitySource):
    """Answers from one versioned set of synthetic records; anything unstated is UNKNOWN."""

    def __init__(self, version: str, records: tuple[SyntheticListingApplicabilityRecord, ...]):
        if not isinstance(version, str) or not version.strip():
            raise InvalidSyntheticListingApplicabilityError("version must be a non-empty string.")
        if not isinstance(records, tuple) or not all(
            isinstance(record, SyntheticListingApplicabilityRecord) for record in records
        ):
            raise InvalidSyntheticListingApplicabilityError(
                "records must be a tuple of SyntheticListingApplicabilityRecord."
            )
        by_key: dict[tuple[ListingReference, date], SyntheticListingApplicabilityRecord] = {}
        for record in records:
            key = (record.listing, record.trading_date)
            earlier = by_key.get(key)
            if earlier is not None:
                detail = "repeated" if earlier == record else "stated with conflicting evidence"
                raise InvalidSyntheticListingApplicabilityError(
                    f"{record.listing} on {record.trading_date.isoformat()} is {detail}."
                )
            by_key[key] = record
        self._records = by_key
        self._authority = f"{SYNTHETIC_REFERENCE_AUTHORITY_PREFIX} {version}"

    @property
    def authority(self) -> str:
        """Return the synthetic identity carried by every answer."""
        return self._authority

    def applicability(
        self, listing: ListingReference, trading_date: date
    ) -> EquityListingApplicability:
        """Return the stated record for ``listing`` on ``trading_date``, or UNKNOWN."""
        if not isinstance(listing, ListingReference):
            raise TypeError("listing must be a ListingReference.")
        if not _is_plain_date(trading_date):
            raise TypeError("trading_date must be a plain date.")
        record = self._records.get((listing, trading_date))
        if record is None:
            return EquityListingApplicability(
                listing=listing,
                trading_date=trading_date,
                classification=EquityListingApplicabilityClassification.UNKNOWN,
                evidence_reference=self._authority,
                reason=f"No synthetic record states {listing} on {trading_date.isoformat()}.",
            )
        return EquityListingApplicability(
            listing=listing,
            trading_date=trading_date,
            classification=record.classification,
            evidence_reference=self._authority,
            reason=record.reason,
        )
