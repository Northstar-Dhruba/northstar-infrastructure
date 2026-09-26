"""Yahoo Finance adapter for historical market data acquisition."""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from functools import cmp_to_key
from typing import Any, ClassVar
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from zoneinfo import ZoneInfo

from northstar_application.ports import (
    HistoricalMarketDataQuery,
    HistoricalMarketDataSource,
    TradingSessionResolutionError,
    TradingSessionResolver,
)
from northstar_core.foundation.exceptions.validation import (
    InvalidPriceError,
    InvalidQuantityError,
    ValidationError,
)
from northstar_core.foundation.value_objects import (
    Currency,
    Price,
    Quantity,
    Timeframe,
)
from northstar_core.market_data import HistoricalOHLCVBar, InvalidHistoricalOHLCVBarError

from northstar_infrastructure.market_data.yahoo_finance import _default_fetch

_YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
_SUPPORTED_TIMEFRAME = Timeframe("1d")


class HistoricalMarketDataSourceError(RuntimeError):
    """Raised when an external historical market data source fails or returns invalid data."""


class YahooHistoricalMarketDataSource(HistoricalMarketDataSource):
    """Acquire factual historical OHLCV observations from Yahoo Finance."""

    _VENUE_YAHOO_EXCHANGE_MAP: ClassVar[dict[str, set[str]]] = {
        "NASDAQ": {"NASDAQ"},
    }

    def __init__(
        self,
        session_resolver: TradingSessionResolver,
        *,
        timeout: float = 10.0,
        fetch: Callable[[str, float], bytes] = _default_fetch,
    ) -> None:
        if session_resolver is None:
            raise TypeError("session_resolver cannot be None.")
        if not isinstance(session_resolver, TradingSessionResolver):
            raise TypeError("session_resolver must be a TradingSessionResolver instance.")

        self._session_resolver = session_resolver
        self._timeout = timeout
        self._fetch = fetch

    def fetch_history(self, query: HistoricalMarketDataQuery) -> tuple[HistoricalOHLCVBar, ...]:
        """Fetch historical observations from start through end, oldest to newest."""
        if not isinstance(query, HistoricalMarketDataQuery):
            raise TypeError(
                "YahooHistoricalMarketDataSource query must be a HistoricalMarketDataQuery."
            )

        if query.timeframe != _SUPPORTED_TIMEFRAME:
            raise HistoricalMarketDataSourceError(
                f"Unsupported timeframe: {query.timeframe.value}. Only '1d' is currently supported."
            )

        valid_exchanges = self._VENUE_YAHOO_EXCHANGE_MAP.get(query.exchange_code.value.upper())
        if valid_exchanges is None:
            raise HistoricalMarketDataSourceError(
                f"Unsupported exchange venue for Yahoo Finance historical data: "
                f"{query.exchange_code.value}."
            )

        start_dt = datetime.fromisoformat(query.start.value.replace("Z", "+00:00"))
        end_dt = datetime.fromisoformat(query.end.value.replace("Z", "+00:00"))
        period1 = int((start_dt - timedelta(days=2)).timestamp())
        period2 = int((end_dt + timedelta(days=2)).timestamp())

        url = (
            f"{_YAHOO_CHART_URL.format(symbol=quote(query.symbol.value))}"
            f"?period1={period1}&period2={period2}&interval=1d&events=history"
        )

        try:
            raw_payload = self._fetch(url, self._timeout)
            payload = json.loads(raw_payload)
        except (HTTPError, URLError, TimeoutError, OSError, json.JSONDecodeError) as exc:
            raise HistoricalMarketDataSourceError("Yahoo Finance provider is unavailable.") from exc

        if not isinstance(payload, dict):
            raise HistoricalMarketDataSourceError(
                "Yahoo Finance returned an invalid payload structure."
            )

        chart = payload.get("chart", {})
        result = chart.get("result")
        if not result:
            error = chart.get("error")
            if error:
                raise HistoricalMarketDataSourceError(
                    f"Yahoo Finance provider returned error: {error}"
                )
            return ()

        chart_result = result[0]
        meta = chart_result.get("meta") or {}
        meta_symbol = meta.get("symbol")
        if meta_symbol and str(meta_symbol).strip().upper() != query.symbol.value.upper():
            raise HistoricalMarketDataSourceError(
                f"Yahoo Finance returned data for symbol {meta_symbol}, "
                f"expected {query.symbol.value}."
            )

        meta_exchange = meta.get("exchangeName")
        if not meta_exchange or not str(meta_exchange).strip():
            raise HistoricalMarketDataSourceError(
                "Yahoo Finance returned incomplete exchange metadata."
            )
        if str(meta_exchange).strip().upper() not in valid_exchanges:
            raise HistoricalMarketDataSourceError(
                f"Yahoo Finance returned exchange {meta_exchange}, "
                f"expected venue matching {query.exchange_code.value}."
            )

        currency = self._parse_currency(meta.get("currency"))

        timestamps = chart_result.get("timestamp") or []
        indicators = chart_result.get("indicators") or {}
        quote_list = indicators.get("quote") or [{}]
        quote_data = quote_list[0] if quote_list else {}
        adjclose_list = indicators.get("adjclose") or [{}]
        adjclose_data = adjclose_list[0] if adjclose_list else {}

        opens = quote_data.get("open") or []
        highs = quote_data.get("high") or []
        lows = quote_data.get("low") or []
        closes = quote_data.get("close") or []
        volumes = quote_data.get("volume") or []
        adjcloses = adjclose_data.get("adjclose") or []

        bars: list[HistoricalOHLCVBar] = []
        for i in range(len(timestamps)):
            ts = timestamps[i]
            open_val = opens[i] if i < len(opens) else None
            high_val = highs[i] if i < len(highs) else None
            low_val = lows[i] if i < len(lows) else None
            close_val = closes[i] if i < len(closes) else None
            volume_val = volumes[i] if i < len(volumes) else None
            adjclose_val = adjcloses[i] if i < len(adjcloses) else None

            if (
                open_val is None
                and high_val is None
                and low_val is None
                and close_val is None
                and volume_val is None
            ):
                continue

            if any(val is None for val in (open_val, high_val, low_val, close_val, volume_val)):
                raise HistoricalMarketDataSourceError(
                    "Yahoo Finance returned incomplete bar observations."
                )

            try:
                int_ts = int(ts)
            except (TypeError, ValueError) as exc:
                raise HistoricalMarketDataSourceError(
                    "Yahoo Finance returned an invalid observation timestamp."
                ) from exc

            trading_date = self._extract_trading_date(int_ts, meta)

            try:
                session_close = self._session_resolver.resolve_session_close(
                    query.exchange_code, trading_date
                )
            except (TradingSessionResolutionError, Exception) as exc:
                raise HistoricalMarketDataSourceError(
                    f"Failed to resolve trading session for exchange {query.exchange_code.value} "
                    f"on {trading_date.isoformat()}."
                ) from exc

            if session_close is None:
                raise HistoricalMarketDataSourceError(
                    f"Trading session data is inconsistent for exchange "
                    f"{query.exchange_code.value} on {trading_date.isoformat()}."
                )

            point_in_time = session_close

            try:
                open_price = Price(self._parse_number(open_val, "open"), currency)
                high_price = Price(self._parse_number(high_val, "high"), currency)
                low_price = Price(self._parse_number(low_val, "low"), currency)
                close_price = Price(self._parse_number(close_val, "close"), currency)
                volume = Quantity(self._parse_number(volume_val, "volume"))
                adjusted_close = (
                    Price(self._parse_number(adjclose_val, "adjusted close"), currency)
                    if adjclose_val is not None
                    else None
                )
            except (TypeError, ValueError, InvalidPriceError, InvalidQuantityError) as exc:
                raise HistoricalMarketDataSourceError(
                    f"Yahoo Finance returned invalid numeric observation data: {exc}"
                ) from exc

            try:
                bar = HistoricalOHLCVBar(
                    symbol=query.symbol,
                    exchange_code=query.exchange_code,
                    point_in_time=point_in_time,
                    timeframe=query.timeframe,
                    open=open_price,
                    high=high_price,
                    low=low_price,
                    close=close_price,
                    volume=volume,
                    adjusted_close=adjusted_close,
                )
            except (ValidationError, InvalidHistoricalOHLCVBarError) as exc:
                raise HistoricalMarketDataSourceError(
                    f"Invalid historical bar constructed from Yahoo Finance: {exc}"
                ) from exc

            bars.append(bar)

        bounded_bars = [
            bar
            for bar in bars
            if query.start.compare(bar.point_in_time) <= 0
            and query.end.compare(bar.point_in_time) >= 0
        ]

        return tuple(sorted(bounded_bars, key=cmp_to_key(_compare_bars)))

    @staticmethod
    def _extract_trading_date(ts: int, meta: dict[str, Any]) -> date:
        tz_name = meta.get("exchangeTimezoneName")
        if isinstance(tz_name, str) and tz_name.strip():
            try:
                tz = ZoneInfo(tz_name.strip())
                return datetime.fromtimestamp(ts, tz).date()
            except Exception:
                pass

        gmt_offset = meta.get("gmtoffset")
        if isinstance(gmt_offset, (int, float)) and not isinstance(gmt_offset, bool):
            try:
                tz = timezone(timedelta(seconds=int(gmt_offset)))
                return datetime.fromtimestamp(ts, tz).date()
            except Exception:
                pass

        raise HistoricalMarketDataSourceError(
            "Yahoo Finance returned missing or invalid exchange timezone metadata."
        )

    @staticmethod
    def _parse_currency(value: Any) -> Currency:
        if not isinstance(value, str) or not value.strip():
            raise HistoricalMarketDataSourceError(
                "Yahoo Finance returned incomplete currency metadata."
            )
        candidate = value.strip().upper()
        if not 3 <= len(candidate) <= 5 or not candidate.isalpha():
            raise HistoricalMarketDataSourceError(
                "Yahoo Finance returned invalid currency metadata."
            )
        return Currency(candidate)

    @staticmethod
    def _parse_number(value: Any, field_name: str) -> str:
        if value is None or isinstance(value, bool):
            raise HistoricalMarketDataSourceError(
                f"Yahoo Finance returned incomplete {field_name} data."
            )
        try:
            number = Decimal(str(value))
        except Exception as exc:
            raise HistoricalMarketDataSourceError(
                f"Yahoo Finance returned invalid {field_name} data."
            ) from exc
        if not number.is_finite() or number < 0:
            raise HistoricalMarketDataSourceError(
                f"Yahoo Finance returned invalid {field_name} data."
            )
        return str(number)


def _compare_bars(left: HistoricalOHLCVBar, right: HistoricalOHLCVBar) -> int:
    return left.point_in_time.compare(right.point_in_time)
