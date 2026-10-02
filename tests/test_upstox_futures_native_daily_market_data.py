"""Tests for the Upstox native daily futures adapter and its instrument master.

Every HTTP exchange is faked. The fake routes by URL: the instrument master is
served gzip-compressed, exactly as Upstox publishes it, and candle responses
follow the documented v3 shape ``[timestamp, open, high, low, close, volume,
open_interest]`` with midnight ``+05:30`` labels and newest-first ordering.

The instrument keys and lot size are the ones observed on a live account for
NIFTY futures. The token below is a placeholder, not a credential.
"""

from __future__ import annotations

import ast
import gzip
import io
import json
import random
from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError

import pytest
from northstar_application.application_services import (
    AcquireFuturesNativeDailyHistoryUseCase,
)
from northstar_application.ports import (
    FuturesDailyHistoricalAcquisitionQuery,
    FuturesHistoricalMarketDataQuery,
    FuturesNativeDailyMarketDataSource,
    FuturesNativeDailyObservation,
)
from northstar_core.derivatives import ExpirationDate, QuoteValue
from northstar_core.foundation.value_objects import (
    ExchangeCode,
    PointInTime,
    Quantity,
    Symbol,
    Timeframe,
)
from northstar_core.futures import FuturesContract, FuturesProductReference

from northstar_infrastructure.market_data import (
    NSEFuturesTradingSessionResolver,
    SQLiteFuturesHistoricalMarketDataRepository,
    SQLiteFuturesHistoricalMarketDataStore,
    UpstoxAccessBlockedError,
    UpstoxAuthenticationError,
    UpstoxFuturesNativeDailyMarketDataSource,
    UpstoxInstrumentMaster,
    UpstoxInstrumentResolutionError,
    UpstoxInvalidInstrumentKeyError,
    UpstoxMarketDataSourceError,
    UpstoxProviderUnavailableError,
)
from northstar_infrastructure.market_data import (
    upstox_futures_native_daily_market_data as adapter_module,
)
from northstar_infrastructure.market_data import upstox_http as http_module
from northstar_infrastructure.market_data import upstox_instrument_master as master_module
from northstar_infrastructure.market_data.upstox_instrument_master import (
    NSE_INSTRUMENT_MASTER_URL,
    UPSTOX_VENUES,
    resolve_future_in_master,
)

_TOKEN = "placeholder-analytics-token-0123456789"

_NSE = ExchangeCode("NSE")
_NIFTY = FuturesProductReference(Symbol("NIFTY"), _NSE)
_BANKNIFTY = FuturesProductReference(Symbol("BANKNIFTY"), _NSE)

_NIFTY_OCT = FuturesContract(_NIFTY, ExpirationDate("2026-10-27"))
_NIFTY_NOV = FuturesContract(_NIFTY, ExpirationDate("2026-11-24"))
_NIFTY_DEC = FuturesContract(_NIFTY, ExpirationDate("2026-12-29"))
_NIFTY_JAN = FuturesContract(_NIFTY, ExpirationDate("2027-01-26"))

_OCT_KEY = "NSE_FO|48704"
_NOV_KEY = "NSE_FO|61471"
_DEC_KEY = "NSE_FO|58875"
_LOT = 65

_IST = timezone(timedelta(hours=5, minutes=30))

_TUE = date(2026, 9, 29)
_WED = date(2026, 9, 30)
_THU = date(2026, 10, 1)
_MON = date(2026, 10, 5)


def _expiry_ms(day: date) -> int:
    """Upstox publishes expiry as 23:59:59 IST on the expiry date, in epoch ms."""
    instant = datetime(day.year, day.month, day.day, 23, 59, 59, tzinfo=_IST)
    return int((instant - datetime(1970, 1, 1, tzinfo=UTC)).total_seconds()) * 1000


def _future(
    key: str,
    expiry: date,
    *,
    underlying: str = "NIFTY",
    lot_size: object = _LOT,
    segment: str = "NSE_FO",
    exchange: str = "NSE",
    instrument_type: str = "FUT",
    trading_symbol: str | None = None,
) -> dict[str, Any]:
    return {
        "weekly": False,
        "segment": segment,
        "name": underlying,
        "exchange": exchange,
        "expiry": _expiry_ms(expiry),
        "instrument_type": instrument_type,
        "asset_symbol": underlying,
        "underlying_symbol": underlying,
        "instrument_key": key,
        "lot_size": lot_size,
        "freeze_quantity": 1800.0,
        "exchange_token": key.split("|")[-1],
        "minimum_lot": lot_size,
        "underlying_key": "NSE_INDEX|Nifty 50",
        "tick_size": 10.0,
        "underlying_type": "INDEX",
        "trading_symbol": trading_symbol
        or f"{underlying} {instrument_type} {expiry.strftime('%d %b %y').upper()}",
        "strike_price": 0.0,
    }


def _master_records() -> list[dict[str, Any]]:
    """A representative slice of NSE.json: the target among its look-alikes."""
    oct_expiry = date(2026, 10, 27)
    return [
        {
            "segment": "NSE_INDEX",
            "name": "Nifty 50",
            "exchange": "NSE",
            "instrument_type": "INDEX",
            "underlying_symbol": "NIFTY",
            "instrument_key": "NSE_INDEX|Nifty 50",
            "exchange_token": "26000",
            "trading_symbol": "NIFTY",
        },
        {
            "segment": "NSE_EQ",
            "name": "NIPPON INDIA ETF NIFTY BEES",
            "exchange": "NSE",
            "instrument_type": "EQ",
            "instrument_key": "NSE_EQ|INF204KB14I2",
            "lot_size": 1,
            "trading_symbol": "NIFTYBEES",
        },
        {**_future("NSE_FO|40001", oct_expiry, instrument_type="CE"), "strike_price": 25000.0},
        {**_future("NSE_FO|40002", oct_expiry, instrument_type="PE"), "strike_price": 25000.0},
        _future("NSE_FO|48700", oct_expiry, underlying="BANKNIFTY", lot_size=30),
        _future("BSE_FO|1170001", oct_expiry, segment="BSE_FO", exchange="BSE"),
        _future("NSE_FO|99999", oct_expiry, exchange="BSE"),
        _future(_DEC_KEY, date(2026, 12, 29)),
        _future(_OCT_KEY, oct_expiry),
        _future(_NOV_KEY, date(2026, 11, 24)),
    ]


def _gzip_json(value: object) -> bytes:
    return gzip.compress(json.dumps(value).encode("utf-8"))


def _candle(
    day: date,
    *,
    open_: object = 25100,
    high: object = 25250,
    low: object = 25020,
    close: object = 25180.5,
    volume: object = 4200 * _LOT,
    open_interest: object = 13_500_000,
) -> list[object]:
    return [f"{day.isoformat()}T00:00:00+05:30", open_, high, low, close, volume, open_interest]


def _week() -> list[list[object]]:
    """Newest first, as Upstox returns them."""
    return [
        _candle(_MON, close=25230, volume=4700 * _LOT),
        _candle(_THU, close=25150, volume=5100 * _LOT),
        _candle(_WED, close=25200, volume=3900 * _LOT),
        _candle(_TUE, close=25180.5, volume=4200 * _LOT),
    ]


def _success(candles: list[list[object]]) -> bytes:
    return json.dumps({"status": "success", "data": {"candles": candles}}).encode("utf-8")


def _error_body(code: str, message: str) -> bytes:
    return json.dumps(
        {
            "status": "error",
            "errors": [
                {
                    "errorCode": code,
                    "message": message,
                    "propertyPath": None,
                    "invalidValue": None,
                    "error_code": code,
                    "property_path": None,
                    "invalid_value": None,
                }
            ],
        }
    ).encode("utf-8")


def _http_error(url: str, status: int, body: bytes = b"") -> HTTPError:
    return HTTPError(url, status, "provider error", {}, io.BytesIO(body))


class FakeFetch:
    """Routes the master URL and candle URLs; records every request."""

    def __init__(
        self,
        *,
        master: object | None = None,
        candles: object = None,
        candle_error: Exception | None = None,
        master_error: Exception | None = None,
    ) -> None:
        self.master = _gzip_json(_master_records()) if master is None else master
        self.candles = _success(_week()) if candles is None else candles
        self.candle_error = candle_error
        self.master_error = master_error
        self.calls: list[tuple[str, dict[str, str], float]] = []

    def __call__(self, url: str, headers: dict[str, str], timeout: float) -> bytes:
        self.calls.append((url, dict(headers), timeout))
        if url == NSE_INSTRUMENT_MASTER_URL:
            if self.master_error is not None:
                raise self.master_error
            return self.master
        if url.startswith("https://api.upstox.com/v3/historical-candle/"):
            if self.candle_error is not None:
                raise self.candle_error
            return self.candles
        raise AssertionError(f"unexpected URL requested: {url}")

    def candle_calls(self) -> list[tuple[str, dict[str, str], float]]:
        return [call for call in self.calls if call[0] != NSE_INSTRUMENT_MASTER_URL]


def _source(fetch: FakeFetch | None = None) -> UpstoxFuturesNativeDailyMarketDataSource:
    return UpstoxFuturesNativeDailyMarketDataSource(_TOKEN, fetch=fetch or FakeFetch())


def _fetch_week(
    fetch: FakeFetch | None = None, contract: FuturesContract = _NIFTY_OCT
) -> tuple[FuturesNativeDailyObservation, ...]:
    return _source(fetch).fetch_daily_observations(contract, _TUE, _MON)


def _resolve(records: object, contract: FuturesContract = _NIFTY_OCT):
    return resolve_future_in_master(records, contract, UPSTOX_VENUES["NSE"])


# ---------------------------------------------------------------------------
# Instrument master resolution
# ---------------------------------------------------------------------------


def test_the_exact_nifty_expiry_resolves_exactly_one_future() -> None:
    instrument = _resolve(_master_records())

    assert instrument.instrument_key == _OCT_KEY
    assert instrument.lot_size == _LOT


@pytest.mark.parametrize(
    ("contract", "key"),
    [(_NIFTY_OCT, _OCT_KEY), (_NIFTY_NOV, _NOV_KEY), (_NIFTY_DEC, _DEC_KEY)],
)
def test_each_dated_contract_resolves_its_own_instrument(
    contract: FuturesContract, key: str
) -> None:
    assert _resolve(_master_records(), contract).instrument_key == key


def test_another_expiry_is_not_selected() -> None:
    records = [r for r in _master_records() if r.get("instrument_key") != _OCT_KEY]

    with pytest.raises(UpstoxInstrumentResolutionError, match="no NSE_FO future"):
        _resolve(records)


def test_option_records_are_not_selected() -> None:
    records = [r for r in _master_records() if r.get("instrument_type") in ("CE", "PE")]

    with pytest.raises(UpstoxInstrumentResolutionError, match="no NSE_FO future"):
        _resolve(records)


def test_index_and_equity_records_are_not_selected() -> None:
    records = [r for r in _master_records() if r["segment"] in ("NSE_INDEX", "NSE_EQ")]

    with pytest.raises(UpstoxInstrumentResolutionError, match="no NSE_FO future"):
        _resolve(records)


@pytest.mark.parametrize(
    "record",
    [
        _future("BSE_FO|1170001", date(2026, 10, 27), segment="BSE_FO", exchange="BSE"),
        _future("NSE_FO|99999", date(2026, 10, 27), exchange="BSE"),
        _future("MCX_FO|1", date(2026, 10, 27), segment="MCX_FO", exchange="MCX"),
        _future("NSE_COM|1", date(2026, 10, 27), segment="NSE_COM"),
    ],
    ids=["bse-segment", "nse-segment-bse-exchange", "mcx", "nse-commodity"],
)
def test_a_wrong_exchange_or_segment_is_not_selected(record: dict[str, Any]) -> None:
    with pytest.raises(UpstoxInstrumentResolutionError, match="no NSE_FO future"):
        _resolve([record])


def test_another_product_on_the_same_expiry_is_not_selected() -> None:
    banknifty_oct = FuturesContract(_BANKNIFTY, ExpirationDate("2026-10-27"))

    instrument = _resolve(_master_records(), banknifty_oct)

    assert instrument.instrument_key == "NSE_FO|48700"
    assert instrument.lot_size == 30


def test_no_match_fails_explicitly() -> None:
    with pytest.raises(UpstoxInstrumentResolutionError, match="expired") as excinfo:
        _resolve(_master_records(), _NIFTY_JAN)

    assert "NIFTY@NSE 2027-01-26" in str(excinfo.value)


def test_an_ambiguous_match_fails_explicitly() -> None:
    records = [*_master_records(), _future("NSE_FO|77777", date(2026, 10, 27))]

    with pytest.raises(UpstoxInstrumentResolutionError, match="ambiguous"):
        _resolve(records)


def test_a_repeated_identical_record_is_not_ambiguous() -> None:
    records = [*_master_records(), _future(_OCT_KEY, date(2026, 10, 27))]

    assert _resolve(records).instrument_key == _OCT_KEY


def test_one_key_with_conflicting_lot_sizes_fails() -> None:
    records = [*_master_records(), _future(_OCT_KEY, date(2026, 10, 27), lot_size=75)]

    with pytest.raises(UpstoxInstrumentResolutionError, match="conflicting lot sizes"):
        _resolve(records)


def test_trading_symbol_text_is_not_identity() -> None:
    """A FUT whose display text claims October but whose expiry says November."""
    misleading = _future(_NOV_KEY, date(2026, 11, 24), trading_symbol="NIFTY FUT 27 OCT 26")

    with pytest.raises(UpstoxInstrumentResolutionError):
        _resolve([misleading])


def test_expiry_is_compared_as_an_ist_civil_date() -> None:
    """00:00 IST on 27 Oct is 18:30 UTC on 26 Oct, and must still match 27 Oct."""
    record = _future(_OCT_KEY, date(2026, 10, 27))
    record["expiry"] = int(datetime(2026, 10, 26, 18, 30, tzinfo=UTC).timestamp()) * 1000

    assert _resolve([record]).instrument_key == _OCT_KEY


@pytest.mark.parametrize("expiry", [None, "1793116799000", -1, True, Decimal("1793116799000")])
def test_a_candidate_with_an_unreadable_expiry_fails(expiry: object) -> None:
    record = _future(_OCT_KEY, date(2026, 10, 27))
    record["expiry"] = expiry

    with pytest.raises(UpstoxInstrumentResolutionError, match="unreadable expiry"):
        _resolve([record])


@pytest.mark.parametrize("key", [None, "", "NSE_FO|", "NSE_EQ|48704", 48704])
def test_a_candidate_with_an_invalid_instrument_key_fails(key: object) -> None:
    record = _future(_OCT_KEY, date(2026, 10, 27))
    record["instrument_key"] = key

    with pytest.raises(UpstoxInstrumentResolutionError, match="invalid instrument key"):
        _resolve([record])


def test_lot_size_is_read_from_the_exact_matched_contract() -> None:
    records = [
        _future(_OCT_KEY, date(2026, 10, 27), lot_size=65),
        _future(_NOV_KEY, date(2026, 11, 24), lot_size=75),
        _future("NSE_FO|48700", date(2026, 10, 27), underlying="BANKNIFTY", lot_size=30),
    ]

    assert _resolve(records, _NIFTY_OCT).lot_size == 65
    assert _resolve(records, _NIFTY_NOV).lot_size == 75


@pytest.mark.parametrize(
    "lot_size",
    [0, -65, Decimal("65.5"), Decimal("0"), "65", None, True, Decimal("NaN")],
    ids=["zero", "negative", "fractional", "decimal-zero", "text", "missing", "bool", "nan"],
)
def test_an_invalid_lot_size_fails(lot_size: object) -> None:
    record = _future(_OCT_KEY, date(2026, 10, 27), lot_size=lot_size)

    with pytest.raises(UpstoxInstrumentResolutionError, match="invalid lot size"):
        _resolve([record])


def test_an_integral_decimal_lot_size_is_accepted() -> None:
    record = _future(_OCT_KEY, date(2026, 10, 27), lot_size=Decimal("65.0"))

    assert _resolve([record]).lot_size == 65


@pytest.mark.parametrize("records", [{"data": []}, "NSE", None])
def test_a_master_that_is_not_an_array_fails(records: object) -> None:
    with pytest.raises(UpstoxMarketDataSourceError, match="JSON array"):
        _resolve(records)


def test_a_non_object_master_record_fails() -> None:
    with pytest.raises(UpstoxMarketDataSourceError, match="not a JSON object"):
        _resolve([*_master_records(), "garbage"])


def test_an_unsupported_venue_fails_before_any_request() -> None:
    fetch = FakeFetch()
    cme = FuturesContract(
        FuturesProductReference(Symbol("ES"), ExchangeCode("CME")), ExpirationDate("2026-12-18")
    )

    with pytest.raises(UpstoxInstrumentResolutionError, match="Unsupported futures venue"):
        _source(fetch).fetch_daily_observations(cme, _TUE, _MON)

    assert fetch.calls == []


def test_the_master_is_fetched_gzipped_without_credentials() -> None:
    fetch = FakeFetch()

    UpstoxInstrumentMaster(fetch=fetch).resolve_future(_NIFTY_OCT)

    url, headers, _ = fetch.calls[0]
    assert url == "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
    assert "Authorization" not in headers


def test_an_uncompressed_master_is_also_accepted() -> None:
    fetch = FakeFetch(master=json.dumps(_master_records()).encode("utf-8"))

    assert UpstoxInstrumentMaster(fetch=fetch).resolve_future(_NIFTY_OCT).lot_size == _LOT


@pytest.mark.parametrize(
    "error", [_http_error(NSE_INSTRUMENT_MASTER_URL, 503), URLError("offline"), TimeoutError()]
)
def test_a_master_download_failure_is_provider_unavailable(error: Exception) -> None:
    with pytest.raises(UpstoxProviderUnavailableError, match="instrument master"):
        _fetch_week(FakeFetch(master_error=error))


def test_a_corrupt_master_is_provider_unavailable() -> None:
    with pytest.raises(UpstoxProviderUnavailableError, match="undecodable"):
        _fetch_week(FakeFetch(master=b"\x1f\x8bnot really gzip"))


def test_resolution_is_held_for_the_adapter_lifetime() -> None:
    fetch = FakeFetch()
    source = _source(fetch)

    source.fetch_daily_observations(_NIFTY_OCT, _TUE, _MON)
    source.fetch_daily_observations(_NIFTY_OCT, _TUE, _MON)

    master_calls = [call for call in fetch.calls if call[0] == NSE_INSTRUMENT_MASTER_URL]
    assert len(master_calls) == 1
    assert len(fetch.candle_calls()) == 2


# ---------------------------------------------------------------------------
# Candle request
# ---------------------------------------------------------------------------


def test_the_resolved_key_is_used_only_in_the_v3_daily_request() -> None:
    fetch = FakeFetch()

    _fetch_week(fetch)

    [(url, headers, _)] = fetch.candle_calls()
    assert url == (
        "https://api.upstox.com/v3/historical-candle/NSE_FO%7C48704/days/1/2026-10-05/2026-09-29"
    )
    assert headers["Authorization"] == f"Bearer {_TOKEN}"
    assert headers["Accept"] == "application/json"
    assert _TOKEN not in url


def test_the_instrument_key_never_reaches_an_observation() -> None:
    observations = _fetch_week()

    assert set(FuturesNativeDailyObservation.__slots__) == {
        "contract",
        "trading_date",
        "open",
        "high",
        "low",
        "close",
        "volume",
    }
    for observation in observations:
        for text in (str(observation), repr(observation)):
            assert "48704" not in text
            assert "NSE_FO" not in text


def test_the_adapter_satisfies_the_application_port() -> None:
    assert isinstance(_source(), FuturesNativeDailyMarketDataSource)


@pytest.mark.parametrize(
    ("start", "end"),
    [(datetime(2026, 9, 29), _MON), (_TUE, "2026-10-05"), (_MON, _TUE)],
    ids=["datetime-start", "text-end", "inverted"],
)
def test_invalid_ranges_are_refused_before_any_request(start: object, end: object) -> None:
    fetch = FakeFetch()

    with pytest.raises((TypeError, ValueError)):
        _source(fetch).fetch_daily_observations(_NIFTY_OCT, start, end)  # type: ignore[arg-type]

    assert fetch.calls == []


def test_a_non_contract_is_refused() -> None:
    with pytest.raises(TypeError, match="FuturesContract"):
        _source().fetch_daily_observations(_NIFTY, _TUE, _MON)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Candle translation
# ---------------------------------------------------------------------------


def test_provider_volume_is_normalised_to_contracts() -> None:
    observations = _fetch_week()

    assert [o.volume for o in observations] == [
        Quantity(Decimal(4200)),
        Quantity(Decimal(3900)),
        Quantity(Decimal(5100)),
        Quantity(Decimal(4700)),
    ]


def test_volume_uses_the_matched_contracts_lot_size() -> None:
    records = [_future(_OCT_KEY, date(2026, 10, 27), lot_size=75)]
    fetch = FakeFetch(master=_gzip_json(records), candles=_success([_candle(_TUE, volume=7500)]))

    [observation] = _source(fetch).fetch_daily_observations(_NIFTY_OCT, _TUE, _TUE)

    assert observation.volume == Quantity(Decimal(100))


def test_zero_volume_is_zero_contracts() -> None:
    fetch = FakeFetch(candles=_success([_candle(_TUE, volume=0)]))

    [observation] = _source(fetch).fetch_daily_observations(_NIFTY_OCT, _TUE, _TUE)

    assert observation.volume == Quantity(Decimal(0))


@pytest.mark.parametrize("volume", [4200 * _LOT + 1, 64, 130.5], ids=["remainder", "below", "frac"])
def test_non_integral_volume_normalisation_fails(volume: object) -> None:
    fetch = FakeFetch(candles=_success([_candle(_TUE, volume=volume)]))

    with pytest.raises(UpstoxMarketDataSourceError, match="volume"):
        _source(fetch).fetch_daily_observations(_NIFTY_OCT, _TUE, _TUE)


@pytest.mark.parametrize("volume", [-65, None, "273000", True])
def test_invalid_volume_fails(volume: object) -> None:
    fetch = FakeFetch(candles=_success([_candle(_TUE, volume=volume)]))

    with pytest.raises(UpstoxMarketDataSourceError, match="volume"):
        _source(fetch).fetch_daily_observations(_NIFTY_OCT, _TUE, _TUE)


def test_ohlc_values_map_exactly() -> None:
    fetch = FakeFetch(
        candles=_success([_candle(_TUE, open_=25100.05, high=25250.9, low=25020.1, close=25180.45)])
    )

    [observation] = _source(fetch).fetch_daily_observations(_NIFTY_OCT, _TUE, _TUE)

    assert observation.open == QuoteValue(Decimal("25100.05"))
    assert observation.high == QuoteValue(Decimal("25250.9"))
    assert observation.low == QuoteValue(Decimal("25020.1"))
    assert observation.close == QuoteValue(Decimal("25180.45"))


def test_prices_never_pass_through_a_float() -> None:
    """A float holds ~17 significant digits; these prices carry more and must survive."""
    body = (
        b'{"status":"success","data":{"candles":'
        b'[["2026-09-29T00:00:00+05:30",25100.1000000000000001,25250.3,'
        b"25020.2,25180.3000000000000007,273000,0]]}}"
    )

    [observation] = _source(FakeFetch(candles=body)).fetch_daily_observations(
        _NIFTY_OCT, _TUE, _TUE
    )

    assert observation.open.value == Decimal("25100.1000000000000001")
    assert observation.close.value == Decimal("25180.3000000000000007")
    assert Decimal(float("25180.3000000000000007")) != observation.close.value


def test_the_provider_timestamp_maps_only_to_the_ist_trading_date() -> None:
    """Midnight IST is 18:30 UTC the previous day; the label must stay on its own day."""
    [observation] = _source(FakeFetch(candles=_success([_candle(_THU)]))).fetch_daily_observations(
        _NIFTY_OCT, _THU, _THU
    )

    assert observation.trading_date == date(2026, 10, 1)
    assert type(observation.trading_date) is date
    for absent in ("point_in_time", "timestamp", "opens_at", "closes_at"):
        assert not hasattr(observation, absent)
    assert "00:00:00" not in repr(observation)
    assert "+05:30" not in repr(observation)


def test_an_equivalent_utc_label_maps_to_the_same_trading_date() -> None:
    candle = _candle(_THU)
    candle[0] = "2026-09-30T18:30:00+00:00"

    [observation] = _source(FakeFetch(candles=_success([candle]))).fetch_daily_observations(
        _NIFTY_OCT, _THU, _THU
    )

    assert observation.trading_date == _THU


def test_observations_retain_the_exact_requested_contract() -> None:
    records = [_future(_NOV_KEY, date(2026, 11, 24))]
    fetch = FakeFetch(master=_gzip_json(records))

    observations = _fetch_week(fetch, _NIFTY_NOV)

    assert all(o.contract == _NIFTY_NOV for o in observations)
    assert all(o.contract != _NIFTY_OCT for o in observations)


def test_observations_are_returned_oldest_first() -> None:
    assert [o.trading_date for o in _fetch_week()] == [_TUE, _WED, _THU, _MON]


@pytest.mark.parametrize("seed", [0, 1, 2, 3])
def test_a_shuffled_provider_response_yields_the_same_observations(seed: int) -> None:
    shuffled = _week()
    random.Random(seed).shuffle(shuffled)

    assert _fetch_week(FakeFetch(candles=_success(shuffled))) == _fetch_week()


def test_open_interest_is_not_returned() -> None:
    low_oi = _fetch_week(FakeFetch(candles=_success([_candle(_TUE, open_interest=1)])))
    high_oi = _fetch_week(FakeFetch(candles=_success([_candle(_TUE, open_interest=99_999_999)])))

    assert low_oi == high_oi


@pytest.mark.parametrize("differ", [False, True], ids=["identical", "differing"])
def test_duplicate_trading_dates_fail(differ: bool) -> None:
    second = _candle(_TUE, close=25190 if differ else 25180.5)
    fetch = FakeFetch(candles=_success([_candle(_TUE), second]))

    with pytest.raises(UpstoxMarketDataSourceError, match="more than one candle"):
        _source(fetch).fetch_daily_observations(_NIFTY_OCT, _TUE, _TUE)


def test_duplicate_dates_spelled_differently_fail() -> None:
    utc_spelling = _candle(_TUE)
    utc_spelling[0] = "2026-09-28T18:30:00+00:00"
    fetch = FakeFetch(candles=_success([_candle(_TUE), utc_spelling]))

    with pytest.raises(UpstoxMarketDataSourceError, match="more than one candle"):
        _source(fetch).fetch_daily_observations(_NIFTY_OCT, _TUE, _TUE)


def test_a_candle_outside_the_requested_range_fails() -> None:
    fetch = FakeFetch(candles=_success([_candle(date(2026, 10, 6))]))

    with pytest.raises(UpstoxMarketDataSourceError, match="outside the requested range"):
        _source(fetch).fetch_daily_observations(_NIFTY_OCT, _TUE, _MON)


def _malformed_candles() -> list[tuple[str, object]]:
    good = _candle(_TUE)
    return [
        ("not-a-list", {"timestamp": good[0]}),
        ("six-fields", good[:6]),
        ("eight-fields", [*good, 0]),
        ("missing-open", [good[0], None, *good[2:]]),
        ("text-close", [*good[:4], "25180.5", *good[5:]]),
        ("bool-high", [good[0], good[1], True, *good[3:]]),
        ("numeric-timestamp", [1790620200, *good[1:]]),
        ("unparseable-timestamp", ["29/09/2026", *good[1:]]),
        ("naive-timestamp", ["2026-09-29T00:00:00", *good[1:]]),
        ("date-only-timestamp", ["2026-09-29", *good[1:]]),
    ]


@pytest.mark.parametrize(
    "candle", [c for _, c in _malformed_candles()], ids=[i for i, _ in _malformed_candles()]
)
def test_malformed_candles_fail(candle: object) -> None:
    fetch = FakeFetch(candles=_success([candle]))  # type: ignore[list-item]

    with pytest.raises(UpstoxMarketDataSourceError, match="candle 0"):
        _source(fetch).fetch_daily_observations(_NIFTY_OCT, _TUE, _TUE)


def test_a_non_finite_price_literal_is_refused() -> None:
    body = b'{"status":"success","data":{"candles":[["2026-09-29T00:00:00+05:30",NaN,1,1,1,0,0]]}}'

    with pytest.raises(UpstoxProviderUnavailableError, match="undecodable"):
        _source(FakeFetch(candles=body)).fetch_daily_observations(_NIFTY_OCT, _TUE, _TUE)


@pytest.mark.parametrize(
    "body",
    [
        b"[]",
        b'{"status":"success"}',
        b'{"status":"success","data":{"candles":{}}}',
        b'{"status":"partial","data":{"candles":[]}}',
    ],
    ids=["array", "no-data", "candles-not-list", "status-not-success"],
)
def test_a_malformed_payload_fails(body: bytes) -> None:
    with pytest.raises(UpstoxMarketDataSourceError):
        _source(FakeFetch(candles=body)).fetch_daily_observations(_NIFTY_OCT, _TUE, _TUE)


def test_undecodable_json_is_provider_unavailable() -> None:
    with pytest.raises(UpstoxProviderUnavailableError, match="undecodable"):
        _source(FakeFetch(candles=b"<html>gateway</html>")).fetch_daily_observations(
            _NIFTY_OCT, _TUE, _TUE
        )


def test_an_empty_provider_result_is_returned_honestly() -> None:
    """A range before listing or with nothing published yields nothing, not filler."""
    fetch = FakeFetch(candles=_success([]))

    assert _source(fetch).fetch_daily_observations(_NIFTY_OCT, _TUE, _MON) == ()


def test_a_partial_range_is_returned_without_filling_gaps() -> None:
    fetch = FakeFetch(candles=_success([_candle(_MON), _candle(_THU)]))

    observations = _source(fetch).fetch_daily_observations(_NIFTY_OCT, _TUE, _MON)

    assert [o.trading_date for o in observations] == [_THU, _MON]


# ---------------------------------------------------------------------------
# HTTP and provider failure translation
# ---------------------------------------------------------------------------


def _candle_failure(error: Exception) -> UpstoxMarketDataSourceError:
    with pytest.raises(UpstoxMarketDataSourceError) as excinfo:
        _fetch_week(FakeFetch(candle_error=error))
    return excinfo.value


def test_http_400_invalid_instrument_key_is_translated_explicitly() -> None:
    error = _candle_failure(
        _http_error("u", 400, _error_body("UDAPI100011", "Invalid Instrument key"))
    )

    assert type(error) is UpstoxInvalidInstrumentKeyError
    assert "UDAPI100011" in str(error)
    assert "NIFTY@NSE 2026-10-27" in str(error)


def test_another_http_400_is_not_reported_as_an_invalid_key() -> None:
    error = _candle_failure(_http_error("u", 400, _error_body("UDAPI1022", "to_date is required")))

    assert type(error) is UpstoxMarketDataSourceError
    assert "UDAPI1022" in str(error)


def test_http_400_without_a_body_is_a_generic_rejection() -> None:
    error = _candle_failure(HTTPError("u", 400, "Bad Request", {}, None))

    assert type(error) is UpstoxMarketDataSourceError
    assert "HTTP 400" in str(error)


@pytest.mark.parametrize("status", [401, 403])
def test_authentication_failures_are_translated_explicitly(status: int) -> None:
    error = _candle_failure(_http_error("u", status, _error_body("UDAPI100050", "Invalid token")))

    assert type(error) is UpstoxAuthenticationError
    assert error.status == status


def test_rate_limiting_is_provider_unavailable_with_its_status() -> None:
    error = _candle_failure(_http_error("u", 429, _error_body("UDAPI10005", "Too many requests")))

    assert type(error) is UpstoxProviderUnavailableError
    assert error.status == 429
    assert "rate limited" in str(error)


@pytest.mark.parametrize("status", [500, 502, 503, 504])
def test_server_failures_are_provider_unavailable(status: int) -> None:
    error = _candle_failure(_http_error("u", status, b"<html>"))

    assert type(error) is UpstoxProviderUnavailableError
    assert error.status == status


@pytest.mark.parametrize("error", [URLError("dns"), TimeoutError("slow"), ConnectionResetError()])
def test_transport_failures_are_provider_unavailable(error: Exception) -> None:
    translated = _candle_failure(error)

    assert type(translated) is UpstoxProviderUnavailableError
    assert translated.status is None


def test_every_translated_error_is_a_runtime_error() -> None:
    for error_type in (
        UpstoxInstrumentResolutionError,
        UpstoxInvalidInstrumentKeyError,
        UpstoxAuthenticationError,
        UpstoxAccessBlockedError,
        UpstoxProviderUnavailableError,
    ):
        assert issubclass(error_type, UpstoxMarketDataSourceError)
    assert issubclass(UpstoxMarketDataSourceError, RuntimeError)


# ---------------------------------------------------------------------------
# Cloudflare edge: access blocks are not authentication failures
# ---------------------------------------------------------------------------

# Verbatim shape of the 403 Cloudflare returned live for urllib's default
# User-Agent when JSON was accepted (ray and timestamp values replaced).
_CLOUDFLARE_1010_JSON = json.dumps(
    {
        "type": "https://developers.cloudflare.com/support/troubleshooting/"
        "http-status-codes/cloudflare-1xxx-errors/error-1010/",
        "title": "Error 1010: Access denied",
        "status": 403,
        "detail": "The site owner has blocked access based on your browser's signature.",
        "instance": "0000000000000000",
        "error_code": 1010,
        "error_name": "browser_signature_banned",
        "error_category": "access_denied",
        "ray_id": "0000000000000000",
        "timestamp": "2026-10-02T00:00:00Z",
        "zone": "api.upstox.com",
        "cloudflare_error": True,
        "retryable": False,
        "owner_action_required": True,
    }
).encode("utf-8")


@pytest.mark.parametrize(
    "body",
    [_CLOUDFLARE_1010_JSON, b"error code: 1010", b"error code: 1010\n"],
    ids=["json", "plain-text", "plain-text-newline"],
)
def test_a_cloudflare_403_is_an_access_block_not_an_auth_failure(body: bytes) -> None:
    error = _candle_failure(_http_error("u", 403, body))

    assert type(error) is UpstoxAccessBlockedError
    assert not isinstance(error, UpstoxAuthenticationError)
    assert error.status == 403
    assert error.edge_error_code == 1010
    assert "Cloudflare error 1010" in str(error)
    assert "did not evaluate the credentials" in str(error)


def test_a_cloudflare_json_block_without_a_code_is_still_a_block() -> None:
    body = json.dumps({"cloudflare_error": True, "title": "Access denied"}).encode("utf-8")

    error = _candle_failure(_http_error("u", 403, body))

    assert type(error) is UpstoxAccessBlockedError
    assert error.edge_error_code is None


def test_an_upstox_403_with_provider_codes_remains_an_auth_failure() -> None:
    error = _candle_failure(_http_error("u", 403, _error_body("UDAPI100050", "Invalid token")))

    assert type(error) is UpstoxAuthenticationError


@pytest.mark.parametrize(
    "body",
    [b"", b"<html>Forbidden</html>", b"error code: abc", b'{"error_code": 1010}'],
    ids=["empty", "html", "non-numeric-code", "json-without-cloudflare-marker"],
)
def test_an_unrecognised_403_remains_an_auth_failure(body: bytes) -> None:
    assert type(_candle_failure(_http_error("u", 403, body))) is UpstoxAuthenticationError


def test_a_cloudflare_body_on_another_status_is_not_reclassified() -> None:
    error = _candle_failure(_http_error("u", 401, b"error code: 1010"))

    assert type(error) is UpstoxAuthenticationError


def test_a_blocked_master_download_is_reported_as_an_access_block() -> None:
    error = _http_error(NSE_INSTRUMENT_MASTER_URL, 403, _CLOUDFLARE_1010_JSON)

    with pytest.raises(UpstoxAccessBlockedError, match="instrument master"):
        _fetch_week(FakeFetch(master_error=error))


def test_the_token_never_appears_in_an_access_block_error() -> None:
    translated = _candle_failure(_http_error("u", 403, _CLOUDFLARE_1010_JSON))

    for link in _chain(translated):
        assert _TOKEN not in str(link)
        assert _TOKEN not in repr(link)


# ---------------------------------------------------------------------------
# default_fetch: what actually goes on the wire
# ---------------------------------------------------------------------------


class _Response:
    def __init__(self, body: bytes) -> None:
        self._body = body

    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def read(self) -> bytes:
        return self._body


def _sent_request(monkeypatch: pytest.MonkeyPatch, headers: dict[str, str]) -> Any:
    sent: dict[str, Any] = {}

    def fake_urlopen(request: Any, timeout: float) -> _Response:
        sent["request"] = request
        sent["timeout"] = timeout
        return _Response(b"{}")

    monkeypatch.setattr(http_module, "urlopen", fake_urlopen)
    assert http_module.default_fetch("https://api.upstox.com/x", headers, 7.0) == b"{}"
    return sent["request"]


def test_default_fetch_names_this_client_instead_of_urllib(monkeypatch: pytest.MonkeyPatch) -> None:
    request = _sent_request(monkeypatch, {"Accept": "application/json"})

    assert request.get_header("User-agent") == "northstar-infrastructure/0.1"
    assert "Python-urllib" not in request.get_header("User-agent")


def test_default_fetch_keeps_the_callers_headers_and_method(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _sent_request(
        monkeypatch, {"Accept": "application/json", "Authorization": f"Bearer {_TOKEN}"}
    )

    assert request.get_method() == "GET"
    assert request.data is None
    assert request.get_header("Accept") == "application/json"
    assert request.get_header("Authorization") == f"Bearer {_TOKEN}"
    assert {name for name, _ in request.header_items()} == {
        "User-agent",
        "Accept",
        "Authorization",
    }


def test_a_caller_supplied_user_agent_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    request = _sent_request(monkeypatch, {"User-Agent": "custom/1.0"})

    assert request.get_header("User-agent") == "custom/1.0"


def test_the_adapter_request_carries_the_explicit_user_agent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """End to end through the real default_fetch, with urlopen faked."""
    seen: list[Any] = []
    master = _gzip_json(_master_records())

    def fake_urlopen(request: Any, timeout: float) -> _Response:
        seen.append(request)
        if request.full_url == NSE_INSTRUMENT_MASTER_URL:
            return _Response(master)
        return _Response(_success(_week()))

    monkeypatch.setattr(http_module, "urlopen", fake_urlopen)

    observations = UpstoxFuturesNativeDailyMarketDataSource(_TOKEN).fetch_daily_observations(
        _NIFTY_OCT, _TUE, _MON
    )

    assert len(observations) == 4
    assert len(seen) == 2
    for request in seen:
        assert request.get_header("User-agent") == "northstar-infrastructure/0.1"
        assert request.get_method() == "GET"


# ---------------------------------------------------------------------------
# Credentials
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("token", ["", "   ", None, 123, "line\nbreak", "carriage\rreturn"])
def test_an_invalid_token_is_refused(token: object) -> None:
    with pytest.raises(UpstoxMarketDataSourceError, match="token") as excinfo:
        UpstoxFuturesNativeDailyMarketDataSource(token)  # type: ignore[arg-type]

    if isinstance(token, str) and token.strip():
        assert token not in str(excinfo.value)


def _chain(error: BaseException) -> list[BaseException]:
    chain: list[BaseException] = []
    current: BaseException | None = error
    while current is not None and current not in chain:
        chain.append(current)
        current = current.__cause__ or current.__context__
    return chain


@pytest.mark.parametrize(
    "error",
    [
        _http_error("u", 400, _error_body("UDAPI100011", "Invalid Instrument key")),
        _http_error("u", 401, _error_body("UDAPI100050", "Invalid token")),
        _http_error("u", 403, b""),
        _http_error("u", 429, b""),
        _http_error("u", 503, b""),
        URLError("offline"),
    ],
)
def test_the_token_never_appears_in_errors(error: Exception) -> None:
    translated = _candle_failure(error)

    for link in _chain(translated):
        assert _TOKEN not in str(link)
        assert _TOKEN not in repr(link)
        assert _TOKEN not in repr(getattr(link, "args", ()))


def test_the_token_never_appears_in_data_errors() -> None:
    fetch = FakeFetch(candles=_success([_candle(_TUE, volume=1)]))

    with pytest.raises(UpstoxMarketDataSourceError) as excinfo:
        _source(fetch).fetch_daily_observations(_NIFTY_OCT, _TUE, _TUE)

    assert all(_TOKEN not in str(link) for link in _chain(excinfo.value))


def test_the_token_never_appears_in_repr_or_output() -> None:
    source = _source()
    observations = source.fetch_daily_observations(_NIFTY_OCT, _TUE, _MON)

    assert _TOKEN not in repr(source)
    assert _TOKEN not in str(source)
    assert _TOKEN not in repr(observations)
    assert _TOKEN not in repr(UpstoxInstrumentMaster())


def test_no_token_or_credential_source_is_in_the_adapter_modules() -> None:
    for module in (adapter_module, http_module, master_module):
        text = Path(module.__file__).read_text(encoding="utf-8")
        assert ".env" not in text
        assert "os.environ" not in text
        assert "getenv" not in text
        assert "print(" not in text
        assert "logging" not in text


# ---------------------------------------------------------------------------
# Scope: no clock, no finality, no trading endpoints
# ---------------------------------------------------------------------------


def _tree(module: Any) -> ast.Module:
    return ast.parse(Path(module.__file__).read_text(encoding="utf-8"))


@pytest.mark.parametrize("module", [adapter_module, http_module, master_module])
def test_no_wall_clock_or_finality_logic_exists(module: Any) -> None:
    tree = _tree(module)
    imported = {
        node.module
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module is not None
    } | {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    assert not any(name.split(".")[0] in {"time", "os", "random"} for name in imported)

    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            assert node.func.attr not in {"now", "utcnow", "today", "time", "monotonic"}
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            for cutoff in ("16:00", "19:00", "21:00", "15:30"):
                assert cutoff not in node.value


@pytest.mark.parametrize("module", [adapter_module, http_module, master_module])
def test_no_broker_or_order_endpoint_is_present(module: Any) -> None:
    urls = {
        node.value
        for node in ast.walk(_tree(module))
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and "upstox.com" in node.value
    }

    allowed = {
        "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz",
        "https://api.upstox.com/v3/historical-candle/{key}/days/1/{to}/{start}",
    }
    assert urls <= allowed
    for url in urls:
        for forbidden in ("order", "portfolio", "position", "funds", "trade", "margin"):
            assert forbidden not in url


def test_every_request_is_a_get_without_a_body() -> None:
    source = Path(http_module.__file__).read_text(encoding="utf-8")

    assert 'method="GET"' in source
    for verb in ('"POST"', '"PUT"', '"DELETE"', '"PATCH"', "data="):
        assert verb not in source


def test_only_the_master_and_one_candle_request_are_made() -> None:
    fetch = FakeFetch()

    _fetch_week(fetch)

    assert [call[0] for call in fetch.calls] == [
        NSE_INSTRUMENT_MASTER_URL,
        "https://api.upstox.com/v3/historical-candle/NSE_FO%7C48704/days/1/2026-10-05/2026-09-29",
    ]


# ---------------------------------------------------------------------------
# Composition with the Application use case
# ---------------------------------------------------------------------------


def test_acquisition_stamps_upstox_candles_at_the_nse_session_close(tmp_path: Path) -> None:
    """End to end: Upstox labels in, canonical session-close bars persisted."""
    database = str(tmp_path / "futures.sqlite3")
    use_case = AcquireFuturesNativeDailyHistoryUseCase(
        NSEFuturesTradingSessionResolver(),
        _source(),
        SQLiteFuturesHistoricalMarketDataStore(database),
    )

    result = use_case.execute(FuturesDailyHistoricalAcquisitionQuery(_NIFTY_OCT, _TUE, _MON))

    assert result.session_count == result.daily_bar_count == 4
    bars = SQLiteFuturesHistoricalMarketDataRepository(database).get_bars(
        FuturesHistoricalMarketDataQuery(_NIFTY_OCT, Timeframe("1d"))
    )
    sessions = NSEFuturesTradingSessionResolver().sessions_in_range(_NIFTY, _TUE, _MON)
    assert [bar.point_in_time for bar in bars] == [s.closes_at for s in sessions]
    assert bars[0].point_in_time == PointInTime("2026-09-29T10:10:00Z")
    assert [bar.volume for bar in bars] == [
        Quantity(Decimal(4200)),
        Quantity(Decimal(3900)),
        Quantity(Decimal(5100)),
        Quantity(Decimal(4700)),
    ]
    assert all(bar.contract == _NIFTY_OCT for bar in bars)
