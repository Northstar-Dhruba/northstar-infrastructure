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

The current venue date
----------------------
Upstox's historical endpoint does not serve the current trading day; its
current-day endpoint (``historical-candle/intraday/{key}/days/1``) does. An
adapter built with ``current_instant`` -- an aware instant captured once by the
composition root -- routes by date: the venue civil date T of that instant (in
the venue's timezone, as candle labels are read) is served by the current-day
endpoint, and only when T lies inside the requested range; dates before T come
from the historical endpoint; dates after T are not requested and are never
answered by today's candle. A current-day candle is used only when its own
label is exactly T. Without ``current_instant`` every date is historical, as
before. Routing never depends on what a response contains, so a past date with
no historical candle never reaches the current-day endpoint, and its errors
reach only a request that needs it.

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

from dataclasses import dataclass
from datetime import date, datetime, timedelta
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
_CURRENT_DAY_CANDLE_URL = "https://api.upstox.com/v3/historical-candle/intraday/{key}/days/1"

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
        current_instant: datetime | None = None,
    ) -> None:
        if not isinstance(access_token, str) or not access_token.strip():
            raise UpstoxMarketDataSourceError("Upstox access token must be a non-empty string.")
        if any(character in access_token for character in "\r\n"):
            raise UpstoxMarketDataSourceError("Upstox access token must be a single line.")
        if not callable(fetch):
            raise TypeError("UpstoxFuturesNativeDailyMarketDataSource fetch must be callable.")
        if current_instant is not None:
            if not isinstance(current_instant, datetime):
                raise TypeError(
                    "UpstoxFuturesNativeDailyMarketDataSource current_instant must be a datetime "
                    "or None."
                )
            if current_instant.utcoffset() is None:
                raise ValueError(
                    "UpstoxFuturesNativeDailyMarketDataSource current_instant must be "
                    "timezone-aware."
                )
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
        self._current_instant = current_instant
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

        current_date = (
            None if self._current_instant is None else self._current_instant.astimezone(zone).date()
        )
        candles = fetch_daily_candles(
            self._fetch,
            self._headers,
            self._timeout,
            instrument.instrument_key,
            zone,
            start_trading_date,
            end_trading_date,
            context,
            current_date=current_date,
        )
        # Upstox returns newest first. The port does not require an order, but
        # ascending labels make the adapter's own output deterministic.
        return tuple(_observation(candle, contract, instrument) for candle in candles)

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


@dataclass(frozen=True, slots=True)
class UpstoxLabelledDailyCandle:
    """One shape-checked provider candle with its venue trading date.

    ``candle`` is the raw ``[timestamp, open, high, low, close, volume,
    open_interest]`` list exactly as decoded; ``index`` and ``context`` locate
    it in its response, for messages.
    """

    trading_date: date
    candle: list[Any]
    index: int
    context: str


def fetch_daily_candles(
    fetch: UpstoxFetch,
    headers: dict[str, str],
    timeout: float,
    instrument_key: str,
    zone: ZoneInfo,
    start_trading_date: date,
    end_trading_date: date,
    context: str,
    *,
    current_date: date | None,
    error: type[UpstoxMarketDataSourceError] = UpstoxMarketDataSourceError,
) -> tuple[UpstoxLabelledDailyCandle, ...]:
    """Return the provider candles answering the range, routed by date, ascending.

    ``current_date`` is the current venue date T, or None. Dates before T, or
    every date when T is None or outside the range, come from the historical
    endpoint; T, when inside the range, comes only from the current-day
    endpoint; dates after T inside the range are not requested. A historical
    candle outside its requested range, two candles for one date, or a
    current-day candle not labelled T raises ``error``. No clock is read.
    """
    key = quote(instrument_key, safe="")
    uses_current_day = (
        current_date is not None and start_trading_date <= current_date <= end_trading_date
    )
    historical_end = current_date - timedelta(days=1) if uses_current_day else end_trading_date

    candles: list[UpstoxLabelledDailyCandle] = []
    if start_trading_date <= historical_end:
        url = _HISTORICAL_CANDLE_URL.format(
            key=key, to=historical_end.isoformat(), start=start_trading_date.isoformat()
        )
        payload = get_json(fetch, url, headers, timeout, context=context)
        for labelled in _labelled(_candles(payload, context), zone, context, error):
            if not start_trading_date <= labelled.trading_date <= historical_end:
                raise error(
                    f"Upstox returned a candle labelled {labelled.trading_date.isoformat()} "
                    f"outside the requested range for {context}."
                )
            candles.append(labelled)

    if uses_current_day:
        current_context = f"{context} (current-day candle {current_date.isoformat()})"
        payload = get_json(
            fetch,
            _CURRENT_DAY_CANDLE_URL.format(key=key),
            headers,
            timeout,
            context=current_context,
        )
        for labelled in _labelled(_candles(payload, current_context), zone, current_context, error):
            if labelled.trading_date != current_date:
                raise error(
                    f"Upstox returned a current-day candle labelled "
                    f"{labelled.trading_date.isoformat()} for {current_context}; it must be "
                    f"labelled {current_date.isoformat()}."
                )
            candles.append(labelled)

    return tuple(sorted(candles, key=lambda labelled: labelled.trading_date))


def _labelled(
    candles: list[Any],
    zone: ZoneInfo,
    context: str,
    error: type[UpstoxMarketDataSourceError],
) -> list[UpstoxLabelledDailyCandle]:
    """Shape-check each candle and read its venue trading date; one per date."""
    labelled: list[UpstoxLabelledDailyCandle] = []
    seen: set[date] = set()
    for index, candle in enumerate(candles):
        if not isinstance(candle, list) or len(candle) != _CANDLE_LENGTH:
            raise error(
                f"Upstox candle {index} for {context} is malformed; expected "
                f"[timestamp, open, high, low, close, volume, open_interest]."
            )
        label = _trading_date(candle[0], index, zone, context)
        if label in seen:
            raise error(
                f"Upstox returned more than one candle labelled {label.isoformat()} for {context}."
            )
        seen.add(label)
        labelled.append(UpstoxLabelledDailyCandle(label, candle, index, context))
    return labelled


def _observation(
    labelled: UpstoxLabelledDailyCandle,
    contract: FuturesContract,
    instrument: UpstoxFuturesInstrument,
) -> FuturesNativeDailyObservation:
    index, context = labelled.index, labelled.context
    _label, open_raw, high_raw, low_raw, close_raw, volume_raw, _open_interest = labelled.candle

    try:
        return FuturesNativeDailyObservation(
            contract=contract,
            trading_date=labelled.trading_date,
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
