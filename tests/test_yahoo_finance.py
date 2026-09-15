"""Contract tests for the Yahoo Finance market observation adapter."""

import json

import pytest
from northstar_core.foundation.value_objects import Symbol

from northstar_infrastructure.market_data import YahooFinanceMarketObservationSource


def _payload() -> bytes:
    closes = [100 + index for index in range(20)]
    volumes = [1000] * 20
    return json.dumps(
        {
            "chart": {
                "result": [
                    {
                        "meta": {
                            "currency": "USD",
                            "symbol": "AAPL",
                            "exchangeName": "NASDAQ",
                            "quoteType": "EQUITY",
                            "longName": "Apple Inc.",
                            "previousClose": 118,
                        },
                        "timestamp": list(range(1_700_000_000, 1_700_000_020)),
                        "indicators": {
                            "quote": [
                                {
                                    "close": closes,
                                    "volume": volumes,
                                    "high": [value + 2 for value in closes],
                                    "low": [value - 2 for value in closes],
                                }
                            ]
                        },
                    }
                ]
            }
        }
    ).encode()


def test_get_observation_context_maps_yahoo_response_to_application_contract() -> None:
    source = YahooFinanceMarketObservationSource(fetch=lambda url, timeout: _payload())

    context = source.get_observation_context(Symbol("AAPL"))

    assert context.listing.instrument.symbol == Symbol("AAPL")
    assert context.listing.exchange.exchange_code.value == "NASDAQ"
    assert str(context.latest_price) == "119 USD"
    assert context.previous_close.amount == 118
    assert context.latest_volume.value == 1000
    assert len(context.recent_closes) == 20
    assert len(context.recent_volumes) == 20


def test_get_observation_context_rejects_unknown_symbol() -> None:
    source = YahooFinanceMarketObservationSource(
        fetch=lambda url, timeout: json.dumps({"chart": {"result": []}}).encode()
    )

    try:
        source.get_observation_context(Symbol("AAPL"))
    except LookupError as exc:
        assert str(exc) == "Unknown symbol: AAPL"
    else:
        raise AssertionError("Expected unknown symbol failure")


def _payload_with_chart_result(chart_result: dict) -> bytes:
    return json.dumps({"chart": {"result": [chart_result]}}).encode()


def test_provider_429_is_mapped_to_provider_unavailable() -> None:
    from urllib.error import HTTPError

    def fetch(url: str, timeout: float) -> bytes:
        raise HTTPError(url, 429, "Too Many Requests", {}, None)

    with pytest.raises(RuntimeError, match="provider is unavailable"):
        YahooFinanceMarketObservationSource(fetch=fetch).get_observation_context(Symbol("AAPL"))


def test_provider_5xx_is_mapped_to_provider_unavailable() -> None:
    from urllib.error import HTTPError

    def fetch(url: str, timeout: float) -> bytes:
        raise HTTPError(url, 503, "Service Unavailable", {}, None)

    with pytest.raises(RuntimeError, match="provider is unavailable"):
        YahooFinanceMarketObservationSource(fetch=fetch).get_observation_context(Symbol("AAPL"))


def test_malformed_json_is_mapped_to_provider_unavailable() -> None:
    with pytest.raises(RuntimeError, match="provider is unavailable"):
        YahooFinanceMarketObservationSource(
            fetch=lambda url, timeout: b"not-json"
        ).get_observation_context(Symbol("AAPL"))


def test_incomplete_history_is_rejected() -> None:
    result = json.loads(_payload())["chart"]["result"][0]
    result["timestamp"] = result["timestamp"][:19]

    with pytest.raises(RuntimeError, match="incomplete market observations"):
        YahooFinanceMarketObservationSource(
            fetch=lambda url, timeout: _payload_with_chart_result(result)
        ).get_observation_context(Symbol("AAPL"))


def test_invalid_timestamp_is_rejected() -> None:
    result = json.loads(_payload())["chart"]["result"][0]
    result["timestamp"][-1] = "invalid"

    with pytest.raises(RuntimeError, match="invalid observation timestamp"):
        YahooFinanceMarketObservationSource(
            fetch=lambda url, timeout: _payload_with_chart_result(result)
        ).get_observation_context(Symbol("AAPL"))


def test_invalid_numeric_value_is_rejected() -> None:
    result = json.loads(_payload())["chart"]["result"][0]
    result["indicators"]["quote"][0]["close"][-1] = "invalid"

    with pytest.raises(RuntimeError, match="invalid latest price"):
        YahooFinanceMarketObservationSource(
            fetch=lambda url, timeout: _payload_with_chart_result(result)
        ).get_observation_context(Symbol("AAPL"))


@pytest.mark.parametrize("metadata_field, value", [("currency", ""), ("exchangeName", "!!!")])
def test_invalid_required_metadata_is_rejected(metadata_field: str, value: str) -> None:
    result = json.loads(_payload())["chart"]["result"][0]
    result["meta"][metadata_field] = value

    with pytest.raises(RuntimeError, match="metadata"):
        YahooFinanceMarketObservationSource(
            fetch=lambda url, timeout: _payload_with_chart_result(result)
        ).get_observation_context(Symbol("AAPL"))
