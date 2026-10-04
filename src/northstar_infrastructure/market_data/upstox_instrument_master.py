"""Upstox JSON instrument master: resolving a FuturesContract to one provider instrument.

Northstar identity is never parsed out of a provider symbol. A contract is
matched on the structured fields that carry meaning:

- ``segment`` is the derivatives segment of the venue (``NSE_FO``)
- ``exchange`` is the venue itself (``NSE``)
- ``instrument_type`` is ``FUT``, which excludes options, indices and equities
- ``underlying_symbol`` is the product code directly (``NIFTY``)
- ``expiry`` dates the contract

``trading_symbol`` is display text and is not consulted. ``exchange_token`` is
a venue-assigned number that can be reissued, and is not identity either. The
resolved ``instrument_key`` is used to request candles and never leaves
Infrastructure.

Expiry
------
``expiry`` is a Unix epoch in milliseconds, published as the last instant of
the expiry day in the venue's own timezone (23:59:59 IST for NSE). A Northstar
ExpirationDate is a civil date, so the instant is converted in the venue's zone
before comparison. Converting in UTC would happen to agree for that particular
time of day, but that is a property of the timestamps Upstox chose to publish,
not a guarantee.

What the master cannot say
--------------------------
The master lists only currently tradable instruments. An expired contract is
absent, so it fails resolution explicitly rather than mapping to a stale key.
It also carries no listing date, so nothing here can bound a contract's
available history. That bound belongs to whoever composes acquisition.

The master is downloaded on every resolution. Callers that resolve repeatedly
hold the resolved instrument themselves; no cache lifetime is decided here.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

from northstar_core.futures import FuturesContract

from northstar_infrastructure.market_data.upstox_http import (
    UpstoxFetch,
    UpstoxInstrumentResolutionError,
    UpstoxMarketDataSourceError,
    default_fetch,
    get_json,
)

NSE_INSTRUMENT_MASTER_URL = (
    "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
)

_FUTURE = "FUT"
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


@dataclass(frozen=True, slots=True)
class UpstoxVenue:
    """How one Northstar venue appears in the Upstox instrument master."""

    exchange: str
    segment: str
    timezone: str


# Each venue is mapped deliberately rather than through one generic rule.
UPSTOX_VENUES: dict[str, UpstoxVenue] = {
    "NSE": UpstoxVenue(exchange="NSE", segment="NSE_FO", timezone="Asia/Kolkata"),
}


def upstox_venue(contract: FuturesContract) -> UpstoxVenue:
    """Return the Upstox venue mapping for a contract, or refuse an unsupported venue."""
    venue = UPSTOX_VENUES.get(contract.product.exchange_code.value)
    if venue is None:
        raise UpstoxInstrumentResolutionError(
            f"Unsupported futures venue for Upstox: {contract.product.exchange_code.value}. "
            f"Supported venues are {sorted(UPSTOX_VENUES)}."
        )
    return venue


@dataclass(frozen=True, slots=True)
class UpstoxFuturesInstrument:
    """One resolved Upstox futures instrument. Infrastructure-only.

    ``lot_size`` is the number of underlying units in one contract, read from
    the exact matched record. It is what converts Upstox volume, which is
    reported in underlying units, into a contract count.
    """

    instrument_key: str
    lot_size: int


class UpstoxInstrumentMaster:
    """Downloads the Upstox NSE JSON instrument master and resolves futures in it.

    The master is a public file and is fetched without credentials.
    """

    def __init__(
        self,
        *,
        fetch: UpstoxFetch = default_fetch,
        timeout: float = 60.0,
        url: str = NSE_INSTRUMENT_MASTER_URL,
    ) -> None:
        if not callable(fetch):
            raise TypeError("UpstoxInstrumentMaster fetch must be callable.")
        self._fetch = fetch
        self._timeout = timeout
        self._url = url

    def __repr__(self) -> str:
        return f"UpstoxInstrumentMaster(url={self._url!r})"

    def resolve_future(self, contract: FuturesContract) -> UpstoxFuturesInstrument:
        """Return the single current Upstox future for ``contract``, or raise."""
        if not isinstance(contract, FuturesContract):
            raise TypeError("UpstoxInstrumentMaster contract must be a FuturesContract.")
        venue = upstox_venue(contract)

        records = get_json(
            self._fetch,
            self._url,
            {"Accept": "application/json"},
            self._timeout,
            context=f"the instrument master while resolving {contract}",
        )
        return resolve_future_in_master(records, contract, venue)


def resolve_future_in_master(
    records: object, contract: FuturesContract, venue: UpstoxVenue
) -> UpstoxFuturesInstrument:
    """Resolve one contract against already-decoded master records.

    A record failing the segment, exchange, instrument type or product filter
    is someone else's instrument and is skipped whatever else it contains. A
    record passing all four is a candidate, and a candidate whose expiry cannot
    be read is refused rather than skipped: silently dropping it could turn an
    ambiguous master into an apparently unique match.
    """
    if not isinstance(records, list):
        raise UpstoxMarketDataSourceError(
            "Upstox instrument master must be a JSON array of instrument records."
        )

    product_code = contract.product.product_code.value
    expiration = date.fromisoformat(contract.expiration_date.value)
    zone = ZoneInfo(venue.timezone)

    candidates: list[dict[str, Any]] = []
    for index, record in enumerate(records):
        if not isinstance(record, dict):
            raise UpstoxMarketDataSourceError(
                f"Upstox instrument master record {index} is not a JSON object."
            )
        if record.get("segment") != venue.segment:
            continue
        if record.get("exchange") != venue.exchange:
            continue
        if record.get("instrument_type") != _FUTURE:
            continue
        if record.get("underlying_symbol") != product_code:
            continue
        if _expiry_date(record, zone, contract) != expiration:
            continue
        candidates.append(record)

    if not candidates:
        raise UpstoxInstrumentResolutionError(
            f"Upstox instrument master has no {venue.segment} future matching {contract}. "
            "The master lists only currently tradable contracts, so an expired "
            "contract cannot be resolved."
        )

    keys = {_instrument_key(record, venue, contract) for record in candidates}
    if len(keys) > 1:
        raise UpstoxInstrumentResolutionError(
            f"Upstox instrument master resolved {contract} to {len(keys)} distinct "
            f"instruments ({sorted(keys)}); the contract is ambiguous."
        )

    lot_sizes = {_lot_size(record, contract) for record in candidates}
    if len(lot_sizes) > 1:
        raise UpstoxInstrumentResolutionError(
            f"Upstox instrument master lists {contract} with conflicting lot sizes "
            f"{sorted(lot_sizes)}."
        )

    return UpstoxFuturesInstrument(instrument_key=keys.pop(), lot_size=lot_sizes.pop())


def _expiry_date(record: dict[str, Any], zone: ZoneInfo, contract: FuturesContract) -> date:
    """Return a candidate's expiry as a civil date in the venue's timezone."""
    milliseconds = record.get("expiry")
    if isinstance(milliseconds, bool) or not isinstance(milliseconds, int) or milliseconds < 0:
        raise UpstoxInstrumentResolutionError(
            f"Upstox instrument master has a {contract.product} future with an unreadable expiry."
        )
    instant = _EPOCH + timedelta(milliseconds=milliseconds)
    return instant.astimezone(zone).date()


def _instrument_key(record: dict[str, Any], venue: UpstoxVenue, contract: FuturesContract) -> str:
    key = record.get("instrument_key")
    prefix = f"{venue.segment}|"
    if not isinstance(key, str) or not key.startswith(prefix) or len(key) == len(prefix):
        raise UpstoxInstrumentResolutionError(
            f"Upstox instrument master lists {contract} with an invalid instrument key."
        )
    return key


def _lot_size(record: dict[str, Any], contract: FuturesContract) -> int:
    """Return a strictly positive integral lot size, refusing anything else.

    A lot size of zero would make volume normalisation divide by zero, a
    negative one would invert volume, and a fractional one has no meaning for a
    count of underlying units per contract.
    """
    value = record.get("lot_size")
    if isinstance(value, bool):
        lot = None
    elif isinstance(value, int):
        lot = value
    elif isinstance(value, Decimal) and value.is_finite() and value == value.to_integral_value():
        lot = int(value)
    else:
        lot = None

    if lot is None or lot <= 0:
        raise UpstoxInstrumentResolutionError(
            f"Upstox instrument master lists {contract} with an invalid lot size {value!r}; "
            "a lot size must be a positive integer."
        )
    return lot
