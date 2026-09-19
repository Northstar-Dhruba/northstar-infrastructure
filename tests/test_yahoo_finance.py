"""Contract tests for the Yahoo Finance market observation adapter."""

import inspect
import json
from pathlib import Path

import pytest
from northstar_core.domain.value_objects import ListingReference
from northstar_core.foundation.value_objects import Currency, ExchangeCode, Symbol
from northstar_core.strategy import MarketObservationContext

from northstar_infrastructure.market_data import YahooFinanceMarketObservationSource, yahoo_finance


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


def _context() -> MarketObservationContext:
    source = YahooFinanceMarketObservationSource(fetch=lambda url, timeout: _payload())
    return source.get_observation_context(Symbol("AAPL"))


def test_get_observation_context_maps_yahoo_response_to_application_contract() -> None:
    context = _context()

    assert isinstance(context, MarketObservationContext)
    assert context.listing_reference.symbol == Symbol("AAPL")
    assert context.listing_reference.exchange_code.value == "NASDAQ"
    assert str(context.latest_price) == "119 USD"
    assert context.previous_close.amount == 118
    assert context.latest_volume.value == 1000
    assert len(context.recent_closes) == 20
    assert len(context.recent_volumes) == 20


# ---------------------------------------------------------------------------
# Canonical listing reference
# ---------------------------------------------------------------------------


def test_context_carries_a_canonical_listing_reference() -> None:
    context = _context()

    assert isinstance(context.listing_reference, ListingReference)
    assert context.listing_reference == ListingReference(Symbol("AAPL"), ExchangeCode("NASDAQ"))


def test_listing_reference_symbol_is_the_requested_canonical_symbol() -> None:
    context = _context()

    assert context.listing_reference.symbol is not None
    assert context.listing_reference.symbol == Symbol("AAPL")


def test_listing_reference_exchange_code_is_the_validated_provider_exchange() -> None:
    context = _context()

    assert context.listing_reference.exchange_code == ExchangeCode("NASDAQ")


def test_context_no_longer_exposes_a_listing_entity() -> None:
    context = _context()

    assert not hasattr(context, "listing")


def test_listing_reference_carries_no_currency_or_lifecycle_state() -> None:
    reference = _context().listing_reference

    assert not hasattr(reference, "currency")
    assert not hasattr(reference, "listing_status")
    assert not hasattr(reference, "tradability")


# ---------------------------------------------------------------------------
# Fabricated reference state is no longer constructed
# ---------------------------------------------------------------------------


def test_adapter_no_longer_constructs_listing_or_fabricated_lifecycle_state() -> None:
    source_text = Path(inspect.getfile(yahoo_finance)).read_text(encoding="utf-8")

    assert "Listing(" not in source_text
    assert "ListingStatus(" not in source_text
    assert "Tradability(" not in source_text
    assert "Instrument(" not in source_text
    assert "Exchange(" not in source_text
    assert '"Active"' not in source_text
    assert '"Permitted"' not in source_text


# ---------------------------------------------------------------------------
# Factual mapping is unchanged
# ---------------------------------------------------------------------------


def test_currency_remains_carried_by_price_values() -> None:
    context = _context()
    usd = Currency("USD")

    assert context.latest_price.currency == usd
    assert context.previous_close.currency == usd
    assert context.daily_high.currency == usd
    assert context.daily_low.currency == usd
    assert all(price.currency == usd for price in context.recent_closes)


def test_latest_and_previous_price_mapping_is_unchanged() -> None:
    context = _context()

    assert context.latest_price.amount == 119
    assert context.previous_close.amount == 118


def test_daily_high_and_low_mapping_is_unchanged() -> None:
    context = _context()

    assert context.daily_high.amount == 121
    assert context.daily_low.amount == 117


def test_latest_volume_mapping_is_unchanged() -> None:
    context = _context()

    assert context.latest_volume.value == 1000


def test_recent_closes_and_volumes_mapping_is_unchanged() -> None:
    context = _context()

    assert [price.amount for price in context.recent_closes] == [100 + index for index in range(20)]
    assert [volume.value for volume in context.recent_volumes] == [1000] * 20


def test_observed_at_mapping_is_unchanged() -> None:
    context = _context()

    assert context.observed_at.value == "2023-11-14T22:13:39Z"


# ---------------------------------------------------------------------------
# Provider error handling is unchanged
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Provider venue validation is not weakened
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("value", ["", "   ", "!!!", "X", "A" * 17])
def test_invalid_provider_exchange_metadata_is_still_rejected(value: str) -> None:
    result = json.loads(_payload())["chart"]["result"][0]
    result["meta"]["exchangeName"] = value

    with pytest.raises(RuntimeError, match="exchange metadata"):
        YahooFinanceMarketObservationSource(
            fetch=lambda url, timeout: _payload_with_chart_result(result)
        ).get_observation_context(Symbol("AAPL"))


def test_missing_provider_exchange_metadata_is_still_rejected() -> None:
    result = json.loads(_payload())["chart"]["result"][0]
    del result["meta"]["exchangeName"]

    with pytest.raises(RuntimeError, match="incomplete exchange metadata"):
        YahooFinanceMarketObservationSource(
            fetch=lambda url, timeout: _payload_with_chart_result(result)
        ).get_observation_context(Symbol("AAPL"))


def test_provider_exchange_metadata_drives_the_reference_exchange_code() -> None:
    result = json.loads(_payload())["chart"]["result"][0]
    result["meta"]["exchangeName"] = "nasdaq"

    context = YahooFinanceMarketObservationSource(
        fetch=lambda url, timeout: _payload_with_chart_result(result)
    ).get_observation_context(Symbol("AAPL"))

    assert context.listing_reference.exchange_code == ExchangeCode("NASDAQ")


# ---------------------------------------------------------------------------
# Blank display/reference metadata must not block factual acquisition
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("metadata_field", ["longName", "quoteType"])
def test_blank_display_metadata_still_produces_market_observations(metadata_field: str) -> None:
    result = json.loads(_payload())["chart"]["result"][0]
    result["meta"][metadata_field] = ""

    context = YahooFinanceMarketObservationSource(
        fetch=lambda url, timeout: _payload_with_chart_result(result)
    ).get_observation_context(Symbol("AAPL"))

    assert isinstance(context, MarketObservationContext)
    assert context.listing_reference == ListingReference(Symbol("AAPL"), ExchangeCode("NASDAQ"))
    assert context.latest_price.amount == 119
    assert context.previous_close.amount == 118
    assert len(context.recent_closes) == 20
    assert len(context.recent_volumes) == 20


def test_blank_display_metadata_on_every_field_still_produces_market_observations() -> None:
    result = json.loads(_payload())["chart"]["result"][0]
    result["meta"]["longName"] = ""
    result["meta"]["quoteType"] = ""

    context = YahooFinanceMarketObservationSource(
        fetch=lambda url, timeout: _payload_with_chart_result(result)
    ).get_observation_context(Symbol("AAPL"))

    assert isinstance(context, MarketObservationContext)
    assert context.listing_reference == ListingReference(Symbol("AAPL"), ExchangeCode("NASDAQ"))
    assert context.observed_at.value == "2023-11-14T22:13:39Z"


@pytest.mark.parametrize("metadata_field", ["longName", "quoteType"])
def test_missing_display_metadata_still_produces_market_observations(metadata_field: str) -> None:
    result = json.loads(_payload())["chart"]["result"][0]
    del result["meta"][metadata_field]

    context = YahooFinanceMarketObservationSource(
        fetch=lambda url, timeout: _payload_with_chart_result(result)
    ).get_observation_context(Symbol("AAPL"))

    assert isinstance(context, MarketObservationContext)
    assert context.listing_reference == ListingReference(Symbol("AAPL"), ExchangeCode("NASDAQ"))


def test_display_metadata_is_absent_from_the_analysis_contract() -> None:
    context = _context()

    assert not hasattr(context, "instrument")
    assert not hasattr(context.listing_reference, "name")
    assert not hasattr(context.listing_reference, "asset_class")
