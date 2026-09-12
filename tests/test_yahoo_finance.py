"""Contract tests for the Yahoo Finance market observation adapter."""

import json

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
