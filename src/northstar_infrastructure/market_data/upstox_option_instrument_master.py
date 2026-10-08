"""Upstox JSON instrument master: every current NIFTY option listing in one snapshot.

The master lists only currently tradable instruments, and an expired contract
disappears from it. This module reads one downloaded master as a snapshot and
returns every NIFTY@NSE option listing in it, so the provider mapping of each
exact contract can be persisted while the contract is still listed.

Northstar identity is never parsed out of a provider symbol. A record is a NIFTY
option candidate when its structured fields say so, as verified against the live
public master:

- ``segment`` is the venue's derivatives segment (``NSE_FO``)
- ``exchange`` is the venue itself (``NSE``)
- ``underlying_symbol`` is the product code (``NIFTY``)
- ``instrument_type`` is ``CE`` or ``PE``

Every other record -- futures (``FUT``), other underlyings, other segments and
any other instrument type -- is someone else's and is skipped. ``CE`` maps to
OptionRight.CALL and ``PE`` to OptionRight.PUT; the provider spellings never
leave this module. ``trading_symbol``, ``exchange_token``, ``tick_size`` and the
provider's ``weekly`` flag are not consulted.

A candidate is converted strictly, and a candidate that cannot be converted
fails the whole snapshot rather than being skipped: silently dropping it could
make an ambiguous or damaged master look complete.

- ``expiry`` is a Unix epoch in milliseconds for the last instant of the expiry
  day in the venue's timezone; it is converted to that civil date there, as for
  futures, and becomes an ExpirationDate.
- ``strike_price`` is a JSON number, already decoded to a Decimal (or an int),
  and becomes an OptionStrike. A bool, text, zero, negative or non-finite strike
  is refused.
- ``instrument_key`` must carry the venue's ``NSE_FO|`` prefix.
- ``lot_size`` must be a positive integer; a bool is refused.

Within one snapshot one exact contract has one key and one lot, and one key
names one contract. Identical duplicate records collapse; any other repetition
is an ambiguity that fails the snapshot.

Expiration dates are taken from the master as observed. No expiration rule or
calendar is consulted, so a listing whose expiry lies beyond Northstar's loaded
calendar is still a valid observation.

Provenance
----------
The snapshot identity is the lowercase hexadecimal SHA-256 of the exact bytes
the fetch layer returned, before decompression or decoding. The fetch instant
comes from a clock injected by the caller, read exactly once, after the body has
been received and decoded; nothing here reads a wall clock.
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

from northstar_core.derivatives import ExpirationDate
from northstar_core.foundation.exceptions.validation import ValidationError
from northstar_core.foundation.value_objects import ExchangeCode, PointInTime, Symbol
from northstar_core.options import (
    OptionContract,
    OptionProductReference,
    OptionRight,
    OptionStrike,
)

from northstar_infrastructure.market_data.upstox_http import (
    UpstoxFetch,
    UpstoxMarketDataSourceError,
    default_fetch,
    get_json,
)
from northstar_infrastructure.market_data.upstox_instrument_master import (
    NSE_INSTRUMENT_MASTER_URL,
    UPSTOX_VENUES,
)

UPSTOX_PROVIDER = "upstox"

# The only product this reference supports.
NIFTY_NSE = OptionProductReference(Symbol("NIFTY"), ExchangeCode("NSE"))

_RIGHTS: Mapping[str, OptionRight] = {"CE": OptionRight.CALL, "PE": OptionRight.PUT}
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


class UpstoxOptionMasterError(UpstoxMarketDataSourceError):
    """Raised when the Upstox master cannot be read as a sound NIFTY option snapshot."""


@dataclass(frozen=True, slots=True)
class UpstoxOptionListing:
    """One exact option contract and its Upstox mapping. Infrastructure-only.

    ``lot_size`` is the exchange lot Upstox reports for the contract: the number
    of underlying units in one contract. It is reference evidence, not
    Northstar economics.
    """

    contract: OptionContract
    instrument_key: str
    lot_size: int

    def __post_init__(self) -> None:
        if not isinstance(self.contract, OptionContract):
            raise TypeError("UpstoxOptionListing contract must be an OptionContract.")
        if not isinstance(self.instrument_key, str) or not self.instrument_key.strip():
            raise TypeError("UpstoxOptionListing instrument key must be a non-empty string.")
        if isinstance(self.lot_size, bool) or not isinstance(self.lot_size, int):
            raise TypeError("UpstoxOptionListing lot size must be an integer.")
        if self.lot_size <= 0:
            raise ValueError("UpstoxOptionListing lot size must be positive.")


@dataclass(frozen=True, slots=True)
class UpstoxOptionMasterSnapshot:
    """Every NIFTY option listing in one downloaded Upstox master.

    ``record_count`` is the number of records in the decoded master.
    ``option_record_count`` is the number of NIFTY option candidate records
    before identical duplicates were collapsed, so it can exceed
    ``len(listings)``. ``listings`` hold one entry per exact contract, in
    canonical natural-key order.
    """

    snapshot_sha256: str
    source_url: str
    fetched_at: PointInTime
    record_count: int
    option_record_count: int
    listings: tuple[UpstoxOptionListing, ...]

    @property
    def provider(self) -> str:
        """Return the provider this snapshot was read from."""
        return UPSTOX_PROVIDER


class UpstoxOptionInstrumentMaster:
    """Downloads the public Upstox NSE master and reads its NIFTY option listings.

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
            raise TypeError("UpstoxOptionInstrumentMaster fetch must be callable.")
        self._fetch = fetch
        self._timeout = timeout
        self._url = url

    def __repr__(self) -> str:
        return f"UpstoxOptionInstrumentMaster(url={self._url!r})"

    def fetch_snapshot(
        self, product: OptionProductReference, *, clock: Callable[[], datetime]
    ) -> UpstoxOptionMasterSnapshot:
        """Download the master once and return its NIFTY option snapshot.

        ``clock`` is read exactly once, after the body has been received and
        decoded, and must return a timezone-aware instant.
        """
        _require_supported(product)
        if not callable(clock):
            raise TypeError("UpstoxOptionInstrumentMaster clock must be callable.")

        received: list[bytes] = []

        def capture(url: str, headers: Mapping[str, str], timeout: float) -> bytes:
            body = self._fetch(url, headers, timeout)
            received.append(body)
            return body

        records = get_json(
            capture,
            self._url,
            {"Accept": "application/json"},
            self._timeout,
            context=f"the instrument master while reading {product} option listings",
        )
        fetched_at = observation_instant(clock())
        return parse_option_master(
            bytes(received[0]), records, product, source_url=self._url, fetched_at=fetched_at
        )


def observation_instant(value: object) -> PointInTime:
    """Return an aware datetime as a canonical UTC PointInTime, refusing a naive one."""
    if not isinstance(value, datetime):
        raise TypeError("An observation instant must be a datetime.")
    if value.utcoffset() is None:
        raise ValueError("An observation instant must be timezone-aware.")
    return PointInTime(value.astimezone(UTC).isoformat())


def parse_option_master(
    body: bytes,
    records: object,
    product: OptionProductReference,
    *,
    source_url: str,
    fetched_at: PointInTime,
) -> UpstoxOptionMasterSnapshot:
    """Read already-decoded master ``records`` downloaded as ``body``."""
    _require_supported(product)
    if not isinstance(body, bytes):
        raise TypeError("The master body must be bytes.")
    if not isinstance(fetched_at, PointInTime):
        raise TypeError("The snapshot fetch instant must be a PointInTime.")
    if not isinstance(records, list):
        raise UpstoxOptionMasterError(
            "Upstox instrument master must be a JSON array of instrument records."
        )

    venue = UPSTOX_VENUES[product.exchange_code.value]
    zone = ZoneInfo(venue.timezone)
    product_code = product.product_code.value

    candidates = 0
    by_contract: dict[OptionContract, set[tuple[str, int]]] = {}
    by_key: dict[str, set[OptionContract]] = {}
    for index, record in enumerate(records):
        if not isinstance(record, dict):
            raise UpstoxOptionMasterError(
                f"Upstox instrument master record {index} is not a JSON object."
            )
        if record.get("segment") != venue.segment:
            continue
        if record.get("exchange") != venue.exchange:
            continue
        if record.get("underlying_symbol") != product_code:
            continue
        right = _RIGHTS.get(record.get("instrument_type"))
        if right is None:
            continue

        candidates += 1
        contract = _contract(record, index, product, right, zone)
        key = _instrument_key(record, index, venue.segment)
        lot = _lot_size(record, index)
        by_contract.setdefault(contract, set()).add((key, lot))
        by_key.setdefault(key, set()).add(contract)

    for contract, mappings in by_contract.items():
        if len({key for key, _ in mappings}) > 1:
            raise UpstoxOptionMasterError(
                f"Upstox instrument master maps {contract} to more than one instrument key; "
                "the snapshot is ambiguous."
            )
        if len({lot for _, lot in mappings}) > 1:
            raise UpstoxOptionMasterError(
                f"Upstox instrument master lists {contract} with conflicting lot sizes "
                f"{sorted(lot for _, lot in mappings)}."
            )
    for contracts in by_key.values():
        if len(contracts) > 1:
            raise UpstoxOptionMasterError(
                f"Upstox instrument master maps one instrument key to {len(contracts)} "
                "different option contracts; the snapshot is ambiguous."
            )

    listings = tuple(
        UpstoxOptionListing(contract, *next(iter(by_contract[contract])))
        for contract in sorted(by_contract, key=lambda contract: contract.natural_key)
    )
    return UpstoxOptionMasterSnapshot(
        snapshot_sha256=hashlib.sha256(body).hexdigest(),
        source_url=source_url,
        fetched_at=fetched_at,
        record_count=len(records),
        option_record_count=candidates,
        listings=listings,
    )


def _require_supported(product: object) -> None:
    if not isinstance(product, OptionProductReference):
        raise TypeError("The option product must be an OptionProductReference.")
    if product != NIFTY_NSE:
        raise UpstoxOptionMasterError(
            f"Unsupported option product for Upstox listings: {product}. "
            f"The supported product is {NIFTY_NSE}."
        )


def _contract(
    record: dict[str, Any],
    index: int,
    product: OptionProductReference,
    right: OptionRight,
    zone: ZoneInfo,
) -> OptionContract:
    expiry = _expiry_date(record, index, zone)
    strike = _strike(record, index)
    try:
        return OptionContract(product, ExpirationDate(expiry.isoformat()), strike, right)
    except ValidationError as exc:
        raise UpstoxOptionMasterError(
            f"Upstox instrument master record {index} is not a valid option contract."
        ) from exc


def _expiry_date(record: dict[str, Any], index: int, zone: ZoneInfo) -> date:
    milliseconds = record.get("expiry")
    if isinstance(milliseconds, bool) or not isinstance(milliseconds, int) or milliseconds < 0:
        raise UpstoxOptionMasterError(
            f"Upstox instrument master option record {index} has an unreadable expiry."
        )
    try:
        instant = _EPOCH + timedelta(milliseconds=milliseconds)
    except OverflowError as exc:
        raise UpstoxOptionMasterError(
            f"Upstox instrument master option record {index} has an unreadable expiry."
        ) from exc
    return instant.astimezone(zone).date()


def _strike(record: dict[str, Any], index: int) -> OptionStrike:
    value = record.get("strike_price")
    if isinstance(value, bool) or not isinstance(value, int | Decimal):
        raise UpstoxOptionMasterError(
            f"Upstox instrument master option record {index} has a non-numeric strike."
        )
    try:
        return OptionStrike(Decimal(value))
    except ValidationError as exc:
        raise UpstoxOptionMasterError(
            f"Upstox instrument master option record {index} has an invalid strike: {exc}"
        ) from exc


def _instrument_key(record: dict[str, Any], index: int, segment: str) -> str:
    key = record.get("instrument_key")
    prefix = f"{segment}|"
    if (
        not isinstance(key, str)
        or not key.startswith(prefix)
        or len(key) == len(prefix)
        or key != key.strip()
    ):
        raise UpstoxOptionMasterError(
            f"Upstox instrument master option record {index} has an invalid instrument key."
        )
    return key


def _lot_size(record: dict[str, Any], index: int) -> int:
    value = record.get("lot_size")
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise UpstoxOptionMasterError(
            f"Upstox instrument master option record {index} has an invalid lot size "
            f"{value!r}; a lot size must be a positive integer."
        )
    return value
