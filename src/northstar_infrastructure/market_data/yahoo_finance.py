"""Yahoo Finance adapter for one-asset market observation acquisition."""

from __future__ import annotations

import json
from collections.abc import Callable
from decimal import Decimal
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

from northstar_application.ports import MarketObservationSource
from northstar_core.domain.exchange import Exchange
from northstar_core.domain.instrument import Instrument
from northstar_core.domain.listing import Listing
from northstar_core.domain.value_objects import ListingStatus, Tradability
from northstar_core.foundation.value_objects import (
    Currency,
    ExchangeCode,
    PointInTime,
    Price,
    Quantity,
    Symbol,
)
from northstar_core.strategy import MarketObservationContext

_YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
_LOOKBACK_RANGE = "1mo"
_INTERVAL = "1d"
_MINIMUM_OBSERVATIONS = 20


def _default_fetch(url: str, timeout: float) -> bytes:
    request = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(request, timeout=timeout) as response:
        return response.read()


def _provider_value(meta: dict[str, Any], name: str, fallback: Any = None) -> Any:
    value = meta.get(name, fallback)
    if isinstance(value, dict):
        return value.get("raw", fallback)
    return value


def _required_number(value: Any, field_name: str) -> str:
    if value is None or isinstance(value, bool):
        raise RuntimeError(f"Yahoo Finance returned incomplete {field_name} data.")
    try:
        number = Decimal(str(value))
    except Exception as exc:
        raise RuntimeError(f"Yahoo Finance returned invalid {field_name} data.") from exc
    if not number.is_finite() or number < 0:
        raise RuntimeError(f"Yahoo Finance returned invalid {field_name} data.")
    return str(number)


def _canonical_exchange_code(value: Any) -> str:
    candidate = str(value or "OTHER").strip().upper()
    if not 2 <= len(candidate) <= 16 or not all(
        char.isalnum() or char == "_" for char in candidate
    ):
        return "OTHER"
    return candidate


def _canonical_currency(value: Any) -> str:
    candidate = str(value or "USD").strip().upper()
    if not 3 <= len(candidate) <= 5 or not candidate.isalpha():
        return "USD"
    return candidate


class YahooFinanceMarketObservationSource(MarketObservationSource):
    """Acquire one bounded daily observation context from Yahoo Finance."""

    def __init__(
        self,
        *,
        timeout: float = 10.0,
        fetch: Callable[[str, float], bytes] = _default_fetch,
    ) -> None:
        self._timeout = timeout
        self._fetch = fetch

    def get_observation_context(self, symbol: Symbol) -> MarketObservationContext:
        if not isinstance(symbol, Symbol):
            raise TypeError("YahooFinanceMarketObservationSource symbol must be a Symbol value.")

        url = (
            f"{_YAHOO_CHART_URL.format(symbol=quote(symbol.value))}"
            f"?range={_LOOKBACK_RANGE}&interval={_INTERVAL}&events=history"
        )
        try:
            payload = json.loads(self._fetch(url, self._timeout))
        except (HTTPError, URLError, TimeoutError, OSError, json.JSONDecodeError) as exc:
            raise RuntimeError("Yahoo Finance provider is unavailable.") from exc

        chart = payload.get("chart", {})
        result = chart.get("result")
        if not result:
            raise LookupError(f"Unknown symbol: {symbol.value}")

        chart_result = result[0]
        meta = chart_result.get("meta") or {}
        timestamps = chart_result.get("timestamp") or []
        quote_data = ((chart_result.get("indicators") or {}).get("quote") or [{}])[0]
        rows = [
            (timestamp, close, volume, high, low)
            for timestamp, close, volume, high, low in zip(
                timestamps,
                quote_data.get("close") or [],
                quote_data.get("volume") or [],
                quote_data.get("high") or [],
                quote_data.get("low") or [],
                strict=False,
            )
            if all(value is not None for value in (timestamp, close, volume, high, low))
        ]
        if len(rows) < _MINIMUM_OBSERVATIONS:
            raise RuntimeError("Yahoo Finance returned incomplete market observations.")

        latest_timestamp, latest_close, latest_volume, latest_high, latest_low = rows[-1]
        previous_close = _provider_value(meta, "previousClose")
        if previous_close is None:
            previous_close = rows[-2][1]

        currency = Currency(_canonical_currency(_provider_value(meta, "currency")))
        listing = Listing(
            instrument=Instrument(
                symbol=symbol,
                name=str(_provider_value(meta, "longName", symbol.value)),
                asset_class=str(_provider_value(meta, "quoteType", "Unknown")),
            ),
            exchange=Exchange(
                exchange_code=ExchangeCode(
                    _canonical_exchange_code(_provider_value(meta, "exchangeName"))
                ),
                name=str(_provider_value(meta, "exchangeName", "Unknown exchange")),
            ),
            currency=currency,
            listing_status=ListingStatus("Active"),
            tradability=Tradability("Permitted"),
        )
        return MarketObservationContext(
            listing=listing,
            observed_at=PointInTime(f"{_iso_timestamp(latest_timestamp)}Z"),
            latest_price=Price(_required_number(latest_close, "latest price"), currency),
            previous_close=Price(_required_number(previous_close, "previous close"), currency),
            latest_volume=Quantity(_required_number(latest_volume, "latest volume")),
            daily_high=Price(_required_number(latest_high, "daily high"), currency),
            daily_low=Price(_required_number(latest_low, "daily low"), currency),
            recent_closes=tuple(
                Price(_required_number(row[1], "recent close"), currency) for row in rows
            ),
            recent_volumes=tuple(
                Quantity(_required_number(row[2], "recent volume")) for row in rows
            ),
        )


def _iso_timestamp(timestamp: Any) -> str:
    try:
        from datetime import UTC, datetime

        return datetime.fromtimestamp(int(timestamp), UTC).strftime("%Y-%m-%dT%H:%M:%S")
    except (TypeError, ValueError, OverflowError, OSError) as exc:
        raise RuntimeError("Yahoo Finance returned an invalid observation timestamp.") from exc
