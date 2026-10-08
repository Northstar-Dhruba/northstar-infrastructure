"""Upstox adapter for provider-native daily option candles.

The flow is: look up the exact OptionContract's persisted Upstox listing,
request the v3 ``days/1`` historical candles for the explicit label range under
that listing's instrument key, and translate each candle into a provider-neutral
OptionNativeDailyObservation.

The persisted listing is the only identity
------------------------------------------
The instrument key and the exchange lot come from the listing that ``options
instruments sync`` stored while the contract was listed. The live instrument
master is never consulted: no strike or expiry is selected, no trading symbol is
parsed and no nearest match is tried. A contract without a stored listing fails
with OptionProviderListingNotStoredError, which is deliberately not an Upstox
provider error -- Northstar lacks the mapping; Upstox was never asked. A stored
listing whose key Upstox no longer accepts (an expired contract) fails with the
provider's UpstoxInvalidInstrumentKeyError instead.

Historical only
---------------
Every request goes to the historical endpoint through the shared
``fetch_daily_candles`` with no current date. This adapter has no current
instant, reads no clock and never asks the current-day endpoint, so a venue
date the historical endpoint does not serve yet simply has no candle. Nothing
here decides whether a candle is final.

Exact numbers
-------------
Premiums are the exact Decimals the provider's JSON decoded to, refused when
negative, non-finite or not numbers. Upstox reports option volume in underlying
units; Northstar volume is a count of option contracts, so each candle's volume
is divided by the listing's exchange lot in integer arithmetic. A volume that is
negative, fractional or not a whole number of lots is refused, never rounded.

Open interest is captured, not returned
---------------------------------------
Each candle's seventh element is the provider's open interest. It is not part of
the observation. It is held, as the exact Decimal decoded, in
ProviderOptionOpenInterest records for exactly one purpose: so the composite
acquisition store can persist it atomically with the bars built from the same
fetch. The capture is cleared before every fetch attempt, filled only once the
whole response has been converted, and handed out at most once by
``take_open_interest``; a failed fetch leaves nothing to take. A candle whose
open interest is missing, null, not a number or non-finite fails the whole
fetch.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

from northstar_application.ports import (
    OptionNativeDailyMarketDataSource,
    OptionNativeDailyObservation,
)
from northstar_core.foundation.exceptions.validation import ValidationError
from northstar_core.foundation.value_objects import Quantity
from northstar_core.options import OptionContract, OptionPremium

from northstar_infrastructure.market_data.sqlite_option_daily_acquisition_store import (
    OptionOpenInterestCaptureError,
)
from northstar_infrastructure.market_data.sqlite_option_provider_listings import (
    SQLiteOptionListingRepository,
    StoredOptionProviderListing,
)
from northstar_infrastructure.market_data.sqlite_option_provider_open_interest import (
    ProviderOptionOpenInterest,
)
from northstar_infrastructure.market_data.upstox_futures_native_daily_market_data import (
    UpstoxLabelledDailyCandle,
    fetch_daily_candles,
)
from northstar_infrastructure.market_data.upstox_http import (
    UpstoxFetch,
    UpstoxMarketDataSourceError,
    default_fetch,
)
from northstar_infrastructure.market_data.upstox_instrument_master import UPSTOX_VENUES
from northstar_infrastructure.market_data.upstox_option_instrument_master import (
    UPSTOX_PROVIDER,
)

_SUBJECT = "UpstoxOptionNativeDailyMarketDataSource"


class OptionProviderListingNotStoredError(LookupError):
    """Raised when Northstar has no persisted provider listing for an exact option contract.

    This is a gap in Northstar's own reference data, not a provider failure: no
    request was made. The listing is stored by ``northstar options instruments
    sync`` while the contract is listed.
    """

    def __init__(self, provider: str, contract: OptionContract) -> None:
        super().__init__(
            f"Option instrument mapping not stored for {contract} (provider {provider}). "
            "Use 'northstar options instruments sync' while the contract is listed."
        )
        self.provider = provider
        self.contract = contract


class UpstoxOptionNativeDailyMarketDataSource(OptionNativeDailyMarketDataSource):
    """Acquire one exact option contract's native daily candles from Upstox.

    The access token is supplied by the caller and travels only in a request
    header. It is never placed in ``repr``, in an error message or in any
    returned value.
    """

    def __init__(
        self,
        access_token: str,
        *,
        listings: SQLiteOptionListingRepository,
        fetch: UpstoxFetch = default_fetch,
        timeout: float = 10.0,
    ) -> None:
        if not isinstance(access_token, str) or not access_token.strip():
            raise UpstoxMarketDataSourceError("Upstox access token must be a non-empty string.")
        if any(character in access_token for character in "\r\n"):
            raise UpstoxMarketDataSourceError("Upstox access token must be a single line.")
        if not isinstance(listings, SQLiteOptionListingRepository):
            raise TypeError(f"{_SUBJECT} listings must be an SQLiteOptionListingRepository.")
        if not callable(fetch):
            raise TypeError(f"{_SUBJECT} fetch must be callable.")

        self._headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {access_token.strip()}",
        }
        self._listings = listings
        self._fetch = fetch
        self._timeout = timeout
        self._captured: tuple[ProviderOptionOpenInterest, ...] | None = None

    def __repr__(self) -> str:
        """Deliberately carries no credential."""
        return f"{_SUBJECT}(interval='days/1')"

    # ------------------------------------------------------------------
    # Port
    # ------------------------------------------------------------------

    def fetch_daily_observations(
        self, contract: OptionContract, start_trading_date: date, end_trading_date: date
    ) -> tuple[OptionNativeDailyObservation, ...]:
        """Return the contract's historical daily candles labelled within the range."""
        # Nothing from an earlier fetch may survive this attempt, whatever happens.
        self._captured = None

        if not isinstance(contract, OptionContract):
            raise TypeError(f"{_SUBJECT} contract must be an OptionContract.")
        _require_label(start_trading_date, "start_trading_date")
        _require_label(end_trading_date, "end_trading_date")
        if start_trading_date > end_trading_date:
            raise ValueError(f"{_SUBJECT} start_trading_date must not be after end_trading_date.")

        listing = self._listing(contract)
        venue = UPSTOX_VENUES.get(contract.product.exchange_code.value)
        if venue is None:
            raise UpstoxMarketDataSourceError(
                f"Unsupported option venue for Upstox: {contract.product.exchange_code.value}."
            )
        context = (
            f"{contract} daily candles "
            f"{start_trading_date.isoformat()}..{end_trading_date.isoformat()}"
        )
        candles = fetch_daily_candles(
            self._fetch,
            self._headers,
            self._timeout,
            listing.instrument_key,
            ZoneInfo(venue.timezone),
            start_trading_date,
            end_trading_date,
            context,
            current_date=None,
        )

        observations: list[OptionNativeDailyObservation] = []
        interest: list[ProviderOptionOpenInterest] = []
        for labelled in candles:
            observation, open_interest = _convert(labelled, contract, listing.exchange_lot_size)
            observations.append(observation)
            interest.append(open_interest)

        self._captured = tuple(interest)
        return tuple(observations)

    # ------------------------------------------------------------------
    # Open-interest side capture
    # ------------------------------------------------------------------

    def take_open_interest(self) -> tuple[ProviderOptionOpenInterest, ...]:
        """Return, and forget, the open interest of the most recent successful fetch.

        Each successful fetch can be taken exactly once. Taking with nothing
        captured -- no fetch yet, a failed fetch, or a second take -- is a
        wiring defect.
        """
        captured, self._captured = self._captured, None
        if captured is None:
            raise OptionOpenInterestCaptureError(
                f"{_SUBJECT} holds no open interest from a successful fetch to take."
            )
        return captured

    def _listing(self, contract: OptionContract) -> StoredOptionProviderListing:
        listing = self._listings.get_listing(UPSTOX_PROVIDER, contract)
        if listing is None:
            raise OptionProviderListingNotStoredError(UPSTOX_PROVIDER, contract)
        return listing


# ---------------------------------------------------------------------------
# Candle translation
# ---------------------------------------------------------------------------


def _convert(
    labelled: UpstoxLabelledDailyCandle, contract: OptionContract, lot_size: int
) -> tuple[OptionNativeDailyObservation, ProviderOptionOpenInterest]:
    index, context = labelled.index, labelled.context
    _label, open_raw, high_raw, low_raw, close_raw, volume_raw, interest_raw = labelled.candle

    open_interest = _number(interest_raw, "open interest", index, context)
    try:
        observation = OptionNativeDailyObservation(
            contract=contract,
            trading_date=labelled.trading_date,
            open=_premium(open_raw, "open", index, context),
            high=_premium(high_raw, "high", index, context),
            low=_premium(low_raw, "low", index, context),
            close=_premium(close_raw, "close", index, context),
            volume=_contracts(volume_raw, lot_size, index, context),
        )
        record = ProviderOptionOpenInterest(
            UPSTOX_PROVIDER, contract, labelled.trading_date, open_interest
        )
    except UpstoxMarketDataSourceError:
        raise
    except (ValidationError, TypeError, ValueError) as exc:
        raise UpstoxMarketDataSourceError(
            f"Upstox candle {index} for {context} cannot be represented: {exc}"
        ) from exc
    return observation, record


def _number(value: Any, field_name: str, index: int, context: str) -> Decimal:
    """Return an exact finite Decimal; JSON decoding never produced a float."""
    if isinstance(value, bool) or not isinstance(value, int | Decimal):
        raise UpstoxMarketDataSourceError(
            f"Upstox candle {index} for {context} has a missing or non-numeric {field_name}."
        )
    number = Decimal(value)
    if not number.is_finite():
        raise UpstoxMarketDataSourceError(
            f"Upstox candle {index} for {context} has a non-finite {field_name}."
        )
    return number


def _premium(value: Any, field_name: str, index: int, context: str) -> OptionPremium:
    number = _number(value, field_name, index, context)
    if number < 0:
        raise UpstoxMarketDataSourceError(
            f"Upstox candle {index} for {context} has a negative {field_name} premium {number}."
        )
    return OptionPremium(number)


def _contracts(value: Any, lot_size: int, index: int, context: str) -> Quantity:
    """Convert underlying-unit volume to an exact count of option contracts.

    ``contracts = provider_volume / lot_size`` in Python's unbounded int, so no
    decimal context can round it. Any remainder is refused.
    """
    if isinstance(lot_size, bool) or not isinstance(lot_size, int) or lot_size <= 0:
        raise UpstoxMarketDataSourceError(
            f"The exchange lot for {context} must be a positive integer, not {lot_size!r}."
        )
    number = _number(value, "volume", index, context)
    if number < 0 or number != number.to_integral_value():
        raise UpstoxMarketDataSourceError(
            f"Upstox candle {index} for {context} has an invalid volume {number}; "
            "volume must be a non-negative whole number of underlying units."
        )
    contracts, remainder = divmod(int(number), lot_size)
    if remainder:
        raise UpstoxMarketDataSourceError(
            f"Upstox candle {index} for {context} has volume {int(number)}, which is not a "
            f"whole number of option contracts at exchange lot {lot_size}."
        )
    return Quantity(Decimal(contracts))


def _require_label(value: Any, field_name: str) -> None:
    if isinstance(value, datetime) or not isinstance(value, date):
        raise TypeError(f"{_SUBJECT} {field_name} must be a plain date.")
