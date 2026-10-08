"""Tests for the Upstox native daily option candle adapter."""

from __future__ import annotations

import ast
import inspect
import io
import json
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from urllib.error import HTTPError

import pytest
from northstar_application.ports import (
    OptionNativeDailyMarketDataSource,
    OptionNativeDailyObservation,
)
from northstar_core.derivatives import ExpirationDate
from northstar_core.foundation.value_objects import (
    ExchangeCode,
    PointInTime,
    Quantity,
    Symbol,
)
from northstar_core.options import (
    OptionContract,
    OptionPremium,
    OptionProductReference,
    OptionRight,
    OptionStrike,
)

import northstar_infrastructure.market_data.upstox_option_native_daily_market_data as module
from northstar_infrastructure.market_data import (
    OptionOpenInterestCaptureError,
    OptionProviderListingNotStoredError,
    ProviderOptionOpenInterest,
    SQLiteOptionListingRepository,
    SQLiteOptionListingStore,
    UpstoxAuthenticationError,
    UpstoxInvalidInstrumentKeyError,
    UpstoxMarketDataSourceError,
    UpstoxOptionListing,
    UpstoxOptionMasterSnapshot,
    UpstoxOptionNativeDailyMarketDataSource,
    UpstoxProviderUnavailableError,
)
from northstar_infrastructure.market_data.upstox_instrument_master import NSE_INSTRUMENT_MASTER_URL

_TOKEN = "test-token-not-a-secret"
_NIFTY = OptionProductReference(Symbol("NIFTY"), ExchangeCode("NSE"))
_HISTORICAL = "https://api.upstox.com/v3/historical-candle/"
_INTRADAY = "https://api.upstox.com/v3/historical-candle/intraday/"
_START, _END = date(2026, 10, 5), date(2026, 10, 7)


def _contract(
    expiration: str = "2026-10-27", strike: str = "22600", right: OptionRight = OptionRight.PUT
) -> OptionContract:
    return OptionContract(_NIFTY, ExpirationDate(expiration), OptionStrike(Decimal(strike)), right)


_PUT = _contract()
_CALL = _contract(right=OptionRight.CALL)
_FAR_CALL = _contract(expiration="2027-03-30", strike="25000", right=OptionRight.CALL)
_WEEKLY_PUT = _contract(expiration="2026-10-13", strike="21100")
_UNLISTED = _contract(strike="22650")

# Synthetic keys: tests never depend on real provider instrument keys.
_KEYS = {_PUT: "NSE_FO|1001", _CALL: "NSE_FO|1002", _FAR_CALL: "NSE_FO|1003",
         _WEEKLY_PUT: "NSE_FO|1004"}  # fmt: skip


@pytest.fixture
def database(tmp_path: Path) -> Path:
    path = tmp_path / "northstar.sqlite3"
    listings = tuple(UpstoxOptionListing(contract, key, 65) for contract, key in _KEYS.items())
    SQLiteOptionListingStore(path).store(
        UpstoxOptionMasterSnapshot(
            snapshot_sha256="a" * 64,
            source_url=NSE_INSTRUMENT_MASTER_URL,
            fetched_at=PointInTime("2026-10-04T04:30:00Z"),
            record_count=1000,
            option_record_count=len(listings),
            listings=listings,
        )
    )
    return path


def _row(day: str, o="191.8", h="226.05", lo="122.45", c="132.6", v="2600", oi="6686095"):
    return [f"{day}T00:00:00+05:30", o, h, lo, c, v, oi]


def _body(*rows: list, status: str = "success") -> bytes:
    """Encode raw JSON text so numbers keep their exact provider spelling."""

    def value(item: object) -> str:
        return (
            json.dumps(item) if item is None or isinstance(item, str) and "T" in item else str(item)
        )

    candles = ", ".join("[" + ", ".join(value(item) for item in row) + "]" for row in rows)
    return f'{{"status": "{status}", "data": {{"candles": [{candles}]}}}}'.encode()


# Upstox answers newest first.
_WEEK = (_row("2026-10-07"), _row("2026-10-06", c="140.1"), _row("2026-10-05", c="150"))


def _error_body(code: str) -> bytes:
    return json.dumps({"status": "error", "errors": [{"errorCode": code}]}).encode()


class FakeFetch:
    """Answers historical candle URLs only; records every request."""

    def __init__(self, body: bytes | None = None, error: Exception | None = None) -> None:
        self.body = _body(*_WEEK) if body is None else body
        self.error = error
        self.calls: list[tuple[str, dict[str, str], float]] = []

    def __call__(self, url: str, headers: dict[str, str], timeout: float) -> bytes:
        self.calls.append((url, dict(headers), timeout))
        if url.startswith(_INTRADAY) or not url.startswith(_HISTORICAL):
            raise AssertionError(f"unexpected URL requested: {url}")
        if self.error is not None:
            raise self.error
        return self.body


def _source(
    database: Path, fetch: FakeFetch | None = None
) -> UpstoxOptionNativeDailyMarketDataSource:
    return UpstoxOptionNativeDailyMarketDataSource(
        _TOKEN, listings=SQLiteOptionListingRepository(database), fetch=fetch or FakeFetch()
    )


def _fetch(source, contract: OptionContract = _PUT, start: date = _START, end: date = _END):
    return source.fetch_daily_observations(contract, start, end)


def _http_error(status: int, body: bytes = b"") -> HTTPError:
    return HTTPError("u", status, "provider error", {}, io.BytesIO(body))


# ---------------------------------------------------------------------------
# Persisted listing
# ---------------------------------------------------------------------------


def test_the_source_implements_the_option_port(database: Path) -> None:
    assert isinstance(_source(database), OptionNativeDailyMarketDataSource)


@pytest.mark.parametrize("contract", [_PUT, _CALL, _FAR_CALL, _WEEKLY_PUT], ids=str)
def test_the_persisted_instrument_key_is_used_exactly(database: Path, contract) -> None:
    fetch = FakeFetch()

    _fetch(_source(database, fetch), contract)

    key = _KEYS[contract].replace("|", "%7C")
    assert [call[0] for call in fetch.calls] == [f"{_HISTORICAL}{key}/days/1/2026-10-07/2026-10-05"]


def test_a_contract_without_a_stored_listing_fails_before_any_request(database: Path) -> None:
    fetch = FakeFetch()

    with pytest.raises(OptionProviderListingNotStoredError) as raised:
        _fetch(_source(database, fetch), _UNLISTED)

    assert fetch.calls == []
    assert raised.value.contract == _UNLISTED
    assert raised.value.provider == "upstox"
    assert "northstar options instruments sync" in str(raised.value)


def test_a_missing_listing_is_not_a_provider_error() -> None:
    assert not issubclass(OptionProviderListingNotStoredError, UpstoxMarketDataSourceError)
    assert issubclass(OptionProviderListingNotStoredError, LookupError)


def test_a_missing_database_is_a_missing_listing_and_creates_nothing(tmp_path: Path) -> None:
    path = tmp_path / "absent.sqlite3"

    with pytest.raises(OptionProviderListingNotStoredError):
        _fetch(_source(path))
    assert not path.exists()


def test_an_expired_key_is_the_provider_error_not_a_missing_listing(database: Path) -> None:
    fetch = FakeFetch(error=_http_error(400, _error_body("UDAPI100011")))

    with pytest.raises(UpstoxInvalidInstrumentKeyError):
        _fetch(_source(database, fetch))


def test_the_live_master_is_never_requested(database: Path) -> None:
    fetch = FakeFetch()

    _fetch(_source(database, fetch), _CALL)

    assert all(NSE_INSTRUMENT_MASTER_URL != url for url, _, _ in fetch.calls)
    tree = ast.parse(Path(module.__file__).read_text(encoding="utf-8"))
    names = {a.name for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) for a in n.names}
    assert not names & {
        "UpstoxInstrumentMaster",
        "UpstoxOptionInstrumentMaster",
        "parse_option_master",
        "NSE_INSTRUMENT_MASTER_URL",
    }
    assert "trading_symbol" not in Path(module.__file__).read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Historical-only routing and no clock
# ---------------------------------------------------------------------------


def test_only_the_historical_endpoint_is_ever_requested(database: Path) -> None:
    fetch = FakeFetch(body=_body())

    # Whatever the range -- even one ending on any venue "today" -- one historical request.
    for end in (date(2026, 10, 8), date(2026, 10, 9), date(2030, 1, 1)):
        _fetch(_source(database, fetch), end=end)

    assert len(fetch.calls) == 3
    assert all(url.startswith(_HISTORICAL) and "/intraday/" not in url for url, _, _ in fetch.calls)


def test_the_source_has_no_current_instant_and_reads_no_clock() -> None:
    parameters = inspect.signature(UpstoxOptionNativeDailyMarketDataSource).parameters
    assert set(parameters) == {"access_token", "listings", "fetch", "timeout"}

    tree = ast.parse(Path(module.__file__).read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute):
            assert node.attr not in {"now", "utcnow", "today", "time", "monotonic"}
        if isinstance(node, ast.keyword) and node.arg == "current_date":
            assert isinstance(node.value, ast.Constant) and node.value.value is None


def test_futures_private_helpers_are_not_imported() -> None:
    tree = ast.parse(Path(module.__file__).read_text(encoding="utf-8"))
    imported = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module and "futures" in node.module
        for alias in node.names
    }
    assert imported == {"UpstoxLabelledDailyCandle", "fetch_daily_candles"}


# ---------------------------------------------------------------------------
# Observations
# ---------------------------------------------------------------------------


def test_candles_become_ascending_provider_neutral_observations(database: Path) -> None:
    observations = _fetch(_source(database))

    assert [o.trading_date for o in observations] == [
        date(2026, 10, 5), date(2026, 10, 6), date(2026, 10, 7)
    ]  # fmt: skip
    assert all(isinstance(o, OptionNativeDailyObservation) for o in observations)
    assert {o.contract for o in observations} == {_PUT}
    last = observations[-1]
    assert (last.open, last.high, last.low, last.close) == (
        OptionPremium(Decimal("191.8")),
        OptionPremium(Decimal("226.05")),
        OptionPremium(Decimal("122.45")),
        OptionPremium(Decimal("132.6")),
    )
    assert not hasattr(last, "open_interest")


def test_premiums_are_exact_provider_decimals(database: Path) -> None:
    fetch = FakeFetch(_body(_row("2026-10-05", o="400.0", h="470.55", lo="0.05", c="413.30")))

    (observation,) = _fetch(_source(database, fetch))

    assert observation.open.value == Decimal("400")
    assert observation.high.value == Decimal("470.55")
    assert observation.low.value == Decimal("0.05")
    assert observation.close.value == Decimal("413.3")


@pytest.mark.parametrize(
    ("volume", "contracts"), [("203645585", "3133009"), ("65", "1"), ("0", "0")]
)
def test_volume_is_divided_exactly_by_the_persisted_lot(database: Path, volume, contracts) -> None:
    fetch = FakeFetch(_body(_row("2026-10-05", v=volume)))

    (observation,) = _fetch(_source(database, fetch))

    assert observation.volume == Quantity(Decimal(contracts))


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("v", "130.5", "invalid volume"),
        ("v", "-65", "invalid volume"),
        ("v", "100", "not a whole number of option contracts"),
        ("v", '"650"', "non-numeric volume"),
        ("v", "null", "non-numeric volume"),
        ("o", "-0.05", "negative open premium"),
        ("c", '"132.6"', "non-numeric close"),
        ("h", "true", "non-numeric high"),
        ("lo", "null", "non-numeric low"),
    ],
)
def test_malformed_numbers_fail_the_whole_fetch(database: Path, field, value, message) -> None:
    rows = (_row("2026-10-06"), _row("2026-10-05", **{field: value}))
    source = _source(database, FakeFetch(_body(*rows)))

    with pytest.raises(UpstoxMarketDataSourceError, match=message):
        _fetch(source)
    with pytest.raises(OptionOpenInterestCaptureError):
        source.take_open_interest()


def test_incoherent_ohlc_is_a_provider_error(database: Path) -> None:
    fetch = FakeFetch(_body(_row("2026-10-05", h="100", lo="122.45")))

    with pytest.raises(UpstoxMarketDataSourceError, match="cannot be represented"):
        _fetch(_source(database, fetch))


def test_a_non_finite_number_is_refused() -> None:
    for raw in (Decimal("NaN"), Decimal("Infinity"), Decimal("-Infinity")):
        with pytest.raises(UpstoxMarketDataSourceError, match="non-finite"):
            module._number(raw, "close", 0, "ctx")


def test_a_non_finite_json_literal_is_refused_at_decode(database: Path) -> None:
    body = (
        b'{"status": "success", "data": {"candles": '
        b'[["2026-10-05T00:00:00+05:30", NaN, 1, 1, 1, 65, 65]]}}'
    )

    with pytest.raises(UpstoxProviderUnavailableError):
        _fetch(_source(database, FakeFetch(body)))


@pytest.mark.parametrize("lot", [0, -65, True, 65.0])
def test_a_non_positive_lot_is_refused(lot) -> None:
    with pytest.raises(UpstoxMarketDataSourceError, match="positive integer"):
        module._contracts(650, lot, 0, "ctx")


def test_a_malformed_candle_shape_fails(database: Path) -> None:
    body = _body(_row("2026-10-05")[:6])

    with pytest.raises(UpstoxMarketDataSourceError, match="malformed"):
        _fetch(_source(database, FakeFetch(body)))


def test_a_duplicate_provider_date_fails(database: Path) -> None:
    body = _body(_row("2026-10-06"), _row("2026-10-06", c="140"))

    with pytest.raises(UpstoxMarketDataSourceError, match="more than one candle"):
        _fetch(_source(database, FakeFetch(body)))


def test_a_provider_date_outside_the_range_fails(database: Path) -> None:
    body = _body(_row("2026-10-08"), _row("2026-10-07"))

    with pytest.raises(UpstoxMarketDataSourceError, match="outside the requested range"):
        _fetch(_source(database, FakeFetch(body)))


def test_a_non_success_payload_fails(database: Path) -> None:
    with pytest.raises(UpstoxMarketDataSourceError, match="did not report success"):
        _fetch(_source(database, FakeFetch(_body(status="error"))))


def test_an_empty_answer_is_no_observation(database: Path) -> None:
    source = _source(database, FakeFetch(_body()))

    assert _fetch(source) == ()
    assert source.take_open_interest() == ()


@pytest.mark.parametrize(
    "arguments",
    [
        ("NIFTY", _START, _END),
        (_PUT, datetime(2026, 10, 5), _END),
        (_PUT, _START, "2026-10-07"),
        (_PUT, _END, _START),
    ],
    ids=["contract", "start-datetime", "end-text", "reversed"],
)
def test_invalid_arguments_fail_before_any_request(database: Path, arguments) -> None:
    fetch = FakeFetch()

    with pytest.raises((TypeError, ValueError)):
        _source(database, fetch).fetch_daily_observations(*arguments)
    assert fetch.calls == []


# ---------------------------------------------------------------------------
# Open-interest capture
# ---------------------------------------------------------------------------


def test_open_interest_is_captured_exactly_and_separately(database: Path) -> None:
    rows = (_row("2026-10-06", oi="975.25"), _row("2026-10-05", oi="1234500"))
    source = _source(database, FakeFetch(_body(*rows)))

    _fetch(source)

    captured = source.take_open_interest()
    assert captured == (
        ProviderOptionOpenInterest("upstox", _PUT, date(2026, 10, 5), Decimal("1234500")),
        ProviderOptionOpenInterest("upstox", _PUT, date(2026, 10, 6), Decimal("975.25")),
    )
    assert [str(record.open_interest_raw) for record in captured] == ["1234500", "975.25"]


def test_open_interest_is_not_divided_by_the_lot(database: Path) -> None:
    source = _source(database, FakeFetch(_body(_row("2026-10-05", v="65", oi="130"))))

    (observation,) = _fetch(source)

    assert observation.volume == Quantity(Decimal("1"))
    assert source.take_open_interest()[0].open_interest_raw == Decimal("130")


@pytest.mark.parametrize(
    ("value", "message"),
    [
        ("null", "non-numeric open interest"),
        ('"6686095"', "non-numeric open interest"),
        ("false", "non-numeric open interest"),
    ],  # fmt: skip
)
def test_missing_or_malformed_open_interest_fails_the_fetch(database: Path, value, message) -> None:
    source = _source(database, FakeFetch(_body(_row("2026-10-06"), _row("2026-10-05", oi=value))))

    with pytest.raises(UpstoxMarketDataSourceError, match=message):
        _fetch(source)
    with pytest.raises(OptionOpenInterestCaptureError):
        source.take_open_interest()


def test_nothing_can_be_taken_before_a_fetch(database: Path) -> None:
    with pytest.raises(OptionOpenInterestCaptureError, match="no open interest"):
        _source(database).take_open_interest()


def test_a_capture_can_be_taken_only_once(database: Path) -> None:
    source = _source(database)
    _fetch(source)

    assert len(source.take_open_interest()) == 3
    with pytest.raises(OptionOpenInterestCaptureError):
        source.take_open_interest()


@pytest.mark.parametrize(
    "failure",
    [
        lambda database: (FakeFetch(error=_http_error(503)), _PUT),
        lambda database: (FakeFetch(_body(_row("2026-10-05", oi="null"))), _PUT),
        lambda database: (FakeFetch(), _UNLISTED),
    ],
    ids=["provider", "open-interest", "listing"],
)
def test_a_failed_fetch_leaves_no_stale_capture(database: Path, failure) -> None:
    fetch, contract = failure(database)
    source = _source(database, fetch)
    source._fetch = FakeFetch()
    _fetch(source)  # a successful fetch whose capture is never taken
    source._fetch = fetch

    with pytest.raises((UpstoxMarketDataSourceError, OptionProviderListingNotStoredError)):
        _fetch(source, contract)
    with pytest.raises(OptionOpenInterestCaptureError):
        source.take_open_interest()


def test_invalid_arguments_also_clear_an_untaken_capture(database: Path) -> None:
    source = _source(database)
    _fetch(source)

    with pytest.raises(ValueError):
        _fetch(source, start=_END, end=_START)
    with pytest.raises(OptionOpenInterestCaptureError):
        source.take_open_interest()


def test_consecutive_fetches_never_leak_open_interest(database: Path) -> None:
    fetch = FakeFetch()
    source = _source(database, fetch)

    _fetch(source, _PUT)
    fetch.body = _body(_row("2026-10-06", oi="777"))
    _fetch(source, _FAR_CALL, date(2026, 10, 6), date(2026, 10, 6))

    assert source.take_open_interest() == (
        ProviderOptionOpenInterest("upstox", _FAR_CALL, date(2026, 10, 6), Decimal("777")),
    )


# ---------------------------------------------------------------------------
# Credentials
# ---------------------------------------------------------------------------


def test_the_token_travels_only_in_the_authorization_header(database: Path) -> None:
    fetch = FakeFetch()
    source = _source(database, fetch)

    _fetch(source)

    url, headers, _ = fetch.calls[0]
    assert headers["Authorization"] == f"Bearer {_TOKEN}"
    assert _TOKEN not in url
    assert _TOKEN not in repr(source)


def test_the_token_never_appears_in_a_provider_error(database: Path) -> None:
    fetch = FakeFetch(error=_http_error(401, _error_body("UDAPI100050")))

    with pytest.raises(UpstoxAuthenticationError) as raised:
        _fetch(_source(database, fetch))
    assert _TOKEN not in str(raised.value)


@pytest.mark.parametrize("token", ["", "   ", "a\nb", "a\rb", None])
def test_an_unusable_token_is_refused(database: Path, token) -> None:
    with pytest.raises(UpstoxMarketDataSourceError, match="token"):
        UpstoxOptionNativeDailyMarketDataSource(
            token, listings=SQLiteOptionListingRepository(database)
        )


def test_the_listings_must_be_the_persisted_repository(database: Path) -> None:
    with pytest.raises(TypeError, match="SQLiteOptionListingRepository"):
        UpstoxOptionNativeDailyMarketDataSource(_TOKEN, listings=object())  # type: ignore[arg-type]
