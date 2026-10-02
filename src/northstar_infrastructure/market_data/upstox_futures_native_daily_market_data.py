"""Upstox adapter for provider-native daily futures candles.

Upstox publishes a daily candle per dated futures contract, so this adapter
implements FuturesNativeDailyMarketDataSource rather than the minute-bar port.
The Databento minute path is untouched.

The flow is: resolve the exact FuturesContract against the current JSON
instrument master, request the v3 ``days/1`` candles for the explicit label
range, and translate each candle into a FuturesNativeDailyObservation.

Provider identity stays here
----------------------------
The resolved ``instrument_key`` appears only in the request URL. Every
observation carries the requested FuturesContract itself, never anything
derived from a provider record.

Timestamps are labels, not instants
-----------------------------------
A daily candle is stamped at local midnight -- ``2026-10-01T00:00:00+05:30`` --
which names the session and says nothing about when it completed. The adapter
keeps only the civil date of that label in the venue's timezone. Taking the UTC
date instead would move every candle one day back, because local midnight is
the previous evening in UTC. The canonical bar instant is the resolved session
close, and it is stamped by Application, not here.

Volume is normalised to contracts
---------------------------------
Upstox reports futures volume in underlying units. Northstar volume is a count
of contracts, so each candle's volume is divided by the exact matched
instrument's lot size. A volume that does not divide exactly is refused: a
fractional contract count is not evidence, and rounding it would invent some.

Open interest
-------------
Each candle's seventh element is open interest. The canonical bar has no field
for it, so it is neither interpreted nor returned.

What this adapter does not decide
---------------------------------
It reads no clock and makes no judgement about whether a candle is final. It
fetches exactly the range it is asked for. It also does not know when a
contract was listed: a range starting before listing simply returns fewer
candles, and it is the caller's job to bound acquisition to the contract's
available history. Nothing is fabricated to fill the gap.

The v3 ``days`` interval serves at most one decade before ``to_date``. A dated
contract's life is far shorter, so no request is split.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any
from urllib.parse import quote
from zoneinfo import ZoneInfo

from northstar_application.ports import (
    FuturesNativeDailyMarketDataSource,
    FuturesNativeDailyObservation,
)
from northstar_core.derivatives import QuoteValue
from northstar_core.foundation.exceptions.validation import ValidationError
from northstar_core.foundation.value_objects import Quantity
from northstar_core.futures import FuturesContract

from northstar_infrastructure.market_data.upstox_http import (
    UpstoxFetch,
    UpstoxMarketDataSourceError,
    default_fetch,
    get_json,
    provider_error_codes,
)
from northstar_infrastructure.market_data.upstox_instrument_master import (
    UpstoxFuturesInstrument,
    UpstoxInstrumentMaster,
    upstox_venue,
)

_HISTORICAL_CANDLE_URL = "https://api.upstox.com/v3/historical-candle/{key}/days/1/{to}/{start}"

# [timestamp, open, high, low, close, volume, open_interest]
_CANDLE_LENGTH = 7


class UpstoxFuturesNativeDailyMarketDataSource(FuturesNativeDailyMarketDataSource):
    """Acquire one dated futures contract's native daily candles from Upstox.

    The access token is supplied by the caller. This adapter reads no
    environment variable and loads no dotenv file: choosing where a secret
    comes from is a composition concern. The token is sent only as a request
    header and is never placed in ``repr``, in an error message, or in any
    returned value.
    """

    def __init__(
        self,
        access_token: str,
        *,
        instrument_master: UpstoxInstrumentMaster | None = None,
        fetch: UpstoxFetch = default_fetch,
        timeout: float = 10.0,
    ) -> None:
        if not isinstance(access_token, str) or not access_token.strip():
            raise UpstoxMarketDataSourceError("Upstox access token must be a non-empty string.")
        if any(character in access_token for character in "\r\n"):
            raise UpstoxMarketDataSourceError("Upstox access token must be a single line.")
        if not callable(fetch):
            raise TypeError("UpstoxFuturesNativeDailyMarketDataSource fetch must be callable.")
        if instrument_master is not None and not isinstance(
            instrument_master, UpstoxInstrumentMaster
        ):
            raise TypeError(
                "UpstoxFuturesNativeDailyMarketDataSource instrument_master "
                "must be an UpstoxInstrumentMaster."
            )

        self._headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {access_token.strip()}",
        }
        self._fetch = fetch
        self._timeout = timeout
        self._master = instrument_master or UpstoxInstrumentMaster(fetch=fetch)
        self._resolved: dict[FuturesContract, UpstoxFuturesInstrument] = {}

    def __repr__(self) -> str:
        """Deliberately carries no credential."""
        return "UpstoxFuturesNativeDailyMarketDataSource(interval='days/1')"

    # ------------------------------------------------------------------
    # Port
    # ------------------------------------------------------------------

    def fetch_daily_observations(
        self, contract: FuturesContract, start_trading_date: date, end_trading_date: date
    ) -> tuple[FuturesNativeDailyObservation, ...]:
        """Return the contract's native daily candles labelled within the range."""
        if not isinstance(contract, FuturesContract):
            raise TypeError(
                "UpstoxFuturesNativeDailyMarketDataSource contract must be a FuturesContract."
            )
        _require_label(start_trading_date, "start_trading_date")
        _require_label(end_trading_date, "end_trading_date")
        if start_trading_date > end_trading_date:
            raise ValueError(
                "UpstoxFuturesNativeDailyMarketDataSource start_trading_date "
                "must not be after end_trading_date."
            )

        zone = ZoneInfo(upstox_venue(contract).timezone)
        instrument = self._instrument(contract)
        context = (
            f"{contract} daily candles "
            f"{start_trading_date.isoformat()}..{end_trading_date.isoformat()}"
        )

        url = _HISTORICAL_CANDLE_URL.format(
            key=quote(instrument.instrument_key, safe=""),
            to=end_trading_date.isoformat(),
            start=start_trading_date.isoformat(),
        )
        payload = get_json(self._fetch, url, self._headers, self._timeout, context=context)
        candles = _candles(payload, context)

        by_date: dict[date, FuturesNativeDailyObservation] = {}
        for index, candle in enumerate(candles):
            observation = _observation(candle, index, contract, instrument, zone, context)
            label = observation.trading_date
            if not start_trading_date <= label <= end_trading_date:
                raise UpstoxMarketDataSourceError(
                    f"Upstox returned a candle labelled {label.isoformat()} outside the "
                    f"requested range for {context}."
                )
            if label in by_date:
                raise UpstoxMarketDataSourceError(
                    f"Upstox returned more than one candle labelled {label.isoformat()} "
                    f"for {context}."
                )
            by_date[label] = observation

        # Upstox returns newest first. The port does not require an order, but
        # ascending labels make the adapter's own output deterministic.
        return tuple(by_date[label] for label in sorted(by_date))

    # ------------------------------------------------------------------
    # Identity resolution
    # ------------------------------------------------------------------

    def _instrument(self, contract: FuturesContract) -> UpstoxFuturesInstrument:
        """Resolve a contract once per adapter instance.

        A live contract's instrument key does not change during its life, so
        holding the resolution for this adapter's lifetime avoids re-reading
        the master for every range without deciding any cache policy.
        """
        resolved = self._resolved.get(contract)
        if resolved is None:
            resolved = self._master.resolve_future(contract)
            self._resolved[contract] = resolved
        return resolved


# ---------------------------------------------------------------------------
# Response translation
# ---------------------------------------------------------------------------


def _candles(payload: Any, context: str) -> list[Any]:
    if not isinstance(payload, dict):
        raise UpstoxMarketDataSourceError(
            f"Upstox returned an invalid payload structure for {context}."
        )
    if payload.get("status") != "success":
        codes = provider_error_codes(payload)
        described = f" (provider codes {', '.join(codes)})" if codes else ""
        raise UpstoxMarketDataSourceError(
            f"Upstox did not report success for {context}{described}."
        )
    data = payload.get("data")
    candles = data.get("candles") if isinstance(data, dict) else None
    if not isinstance(candles, list):
        raise UpstoxMarketDataSourceError(f"Upstox returned no candle list for {context}.")
    return candles


def _observation(
    candle: Any,
    index: int,
    contract: FuturesContract,
    instrument: UpstoxFuturesInstrument,
    zone: ZoneInfo,
    context: str,
) -> FuturesNativeDailyObservation:
    if not isinstance(candle, list) or len(candle) != _CANDLE_LENGTH:
        raise UpstoxMarketDataSourceError(
            f"Upstox candle {index} for {context} is malformed; expected "
            f"[timestamp, open, high, low, close, volume, open_interest]."
        )
    label, open_raw, high_raw, low_raw, close_raw, volume_raw, _open_interest = candle

    try:
        return FuturesNativeDailyObservation(
            contract=contract,
            trading_date=_trading_date(label, index, zone, context),
            open=QuoteValue(_number(open_raw, "open", index, context)),
            high=QuoteValue(_number(high_raw, "high", index, context)),
            low=QuoteValue(_number(low_raw, "low", index, context)),
            close=QuoteValue(_number(close_raw, "close", index, context)),
            volume=_contract_volume(volume_raw, instrument.lot_size, index, context),
        )
    except UpstoxMarketDataSourceError:
        raise
    except (ValidationError, TypeError, ValueError) as exc:
        raise UpstoxMarketDataSourceError(
            f"Upstox candle {index} for {context} cannot be represented: {exc}"
        ) from exc


def _trading_date(label: Any, index: int, zone: ZoneInfo, context: str) -> date:
    """Return the civil date a candle label names, in the venue's timezone."""
    if not isinstance(label, str):
        raise UpstoxMarketDataSourceError(
            f"Upstox candle {index} for {context} has a non-text timestamp."
        )
    try:
        stamped = datetime.fromisoformat(label)
    except ValueError as exc:
        raise UpstoxMarketDataSourceError(
            f"Upstox candle {index} for {context} has an unreadable timestamp."
        ) from exc
    if stamped.utcoffset() is None:
        raise UpstoxMarketDataSourceError(
            f"Upstox candle {index} for {context} has a timestamp without a UTC offset."
        )
    return stamped.astimezone(zone).date()


def _number(value: Any, field_name: str, index: int, context: str) -> Decimal:
    """Return an exact finite Decimal. JSON decoding never produced a float."""
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


def _contract_volume(value: Any, lot_size: int, index: int, context: str) -> Quantity:
    """Convert underlying-unit volume to an exact contract count.

    ``contracts = provider_volume / lot_size``, computed in Python's unbounded
    int so no decimal context can round it. Any remainder is refused.
    """
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
            f"whole number of contracts at lot size {lot_size}."
        )
    return Quantity(Decimal(contracts))


def _require_label(value: Any, field_name: str) -> None:
    if isinstance(value, datetime) or not isinstance(value, date):
        raise TypeError(
            f"UpstoxFuturesNativeDailyMarketDataSource {field_name} must be a plain date."
        )
