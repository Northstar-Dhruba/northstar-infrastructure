"""Tests for reading NIFTY option listings out of the Upstox JSON instrument master.

Fixtures are synthetic and deterministic, shaped like the live NSE.json option
rows (verified field set: segment, exchange, underlying_symbol, instrument_type
CE/PE, expiry epoch milliseconds, Decimal strike_price, NSE_FO| instrument_key,
integer lot_size, plus display fields this module ignores). No real instrument
key is used.
"""

from __future__ import annotations

import ast
import gzip
import hashlib
import json
from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError

import pytest
from northstar_core.derivatives import ExpirationDate
from northstar_core.foundation.value_objects import ExchangeCode, PointInTime, Symbol
from northstar_core.options import (
    OptionContract,
    OptionProductReference,
    OptionRight,
    OptionStrike,
)

import northstar_infrastructure.market_data.upstox_option_instrument_master as module
from northstar_infrastructure.market_data import (
    UPSTOX_PROVIDER,
    UpstoxOptionInstrumentMaster,
    UpstoxOptionListing,
    UpstoxOptionMasterError,
)
from northstar_infrastructure.market_data.upstox_http import (
    UpstoxMarketDataSourceError,
    UpstoxProviderUnavailableError,
    decode_json,
)
from northstar_infrastructure.market_data.upstox_instrument_master import (
    NSE_INSTRUMENT_MASTER_URL,
)
from northstar_infrastructure.market_data.upstox_option_instrument_master import (
    NIFTY_NSE,
    observation_instant,
    parse_option_master,
)

_IST = timezone(timedelta(hours=5, minutes=30))
_OCT_27 = date(2026, 10, 27)
_NOV_24 = date(2026, 11, 24)
_FETCHED = PointInTime("2026-10-08T04:30:00Z")


def _expiry_ms(day: date, hour: int = 23, minute: int = 59, second: int = 59) -> int:
    instant = datetime(day.year, day.month, day.day, hour, minute, second, tzinfo=_IST)
    return int((instant - datetime(1970, 1, 1, tzinfo=UTC)).total_seconds()) * 1000


def _option(
    key: str,
    expiry: date,
    strike: float,
    instrument_type: str = "CE",
    *,
    underlying: str = "NIFTY",
    lot_size: object = 65,
    segment: str = "NSE_FO",
    exchange: str = "NSE",
    trading_symbol: str | None = None,
    weekly: bool = True,
) -> dict[str, Any]:
    return {
        "weekly": weekly,
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
        "asset_key": "NSE_INDEX|Nifty 50",
        "underlying_key": "NSE_INDEX|Nifty 50",
        "tick_size": 5.0,
        "asset_type": "INDEX",
        "underlying_type": "INDEX",
        "trading_symbol": trading_symbol
        or f"{underlying} {int(strike)} {instrument_type} {expiry:%d %b %y}".upper(),
        "strike_price": strike,
        "qty_multiplier": 1.0,
    }


def _master() -> list[dict[str, Any]]:
    """A representative slice: NIFTY options among futures, other underlyings and venues."""
    return [
        {"segment": "NSE_INDEX", "name": "Nifty 50", "exchange": "NSE", "instrument_type": "INDEX",
         "instrument_key": "NSE_INDEX|Nifty 50", "trading_symbol": "NIFTY"},
        {"segment": "NSE_EQ", "name": "NIFTYBEES", "exchange": "NSE", "instrument_type": "EQ",
         "instrument_key": "NSE_EQ|INF204KB14I2", "lot_size": 1, "trading_symbol": "NIFTYBEES"},
        {**_option("NSE_FO|90001", _OCT_27, 0.0, "FUT"), "strike_price": 0.0},
        _option("NSE_FO|90101", _OCT_27, 25000.0, "CE"),
        _option("NSE_FO|90102", _OCT_27, 25000.0, "PE"),
        _option("NSE_FO|90103", _OCT_27, 25050.0, "CE"),
        _option("NSE_FO|90104", _NOV_24, 25000.0, "CE", weekly=False),
        _option("NSE_FO|90201", _OCT_27, 56000.0, "CE", underlying="BANKNIFTY", lot_size=30),
        _option("BSE_FO|90301", _OCT_27, 80000.0, "CE", segment="BSE_FO", exchange="BSE"),
        _option("NSE_FO|90401", _OCT_27, 25000.0, "CE", exchange="BSE"),
        {**_option("NSE_FO|90501", _OCT_27, 25000.0, "XX")},
    ]  # fmt: skip


def _body(records: list[dict[str, Any]]) -> bytes:
    return gzip.compress(json.dumps(records).encode("utf-8"))


def _parse(records: list[dict[str, Any]], product: OptionProductReference = NIFTY_NSE):
    body = _body(records)
    return parse_option_master(
        body,
        decode_json(body, context="test"),
        product,
        source_url=NSE_INSTRUMENT_MASTER_URL,
        fetched_at=_FETCHED,
    )


def _decoded_option(**override: object) -> dict[str, Any]:
    """One good NIFTY call decoded exactly as the master decodes it, then overridden."""
    record = decode_json(
        json.dumps([_option("NSE_FO|1", _OCT_27, 25000.0)]).encode("utf-8"), context="t"
    )[0]
    record.update(override)
    return record


def _parse_raw(record: dict[str, Any]):
    return parse_option_master(b"{}", [record], NIFTY_NSE, source_url="u", fetched_at=_FETCHED)


def _contract(expiry: date, strike: str, right: OptionRight) -> OptionContract:
    return OptionContract(
        NIFTY_NSE, ExpirationDate(expiry.isoformat()), OptionStrike(Decimal(strike)), right
    )


# ---------------------------------------------------------------------------
# Mapping
# ---------------------------------------------------------------------------


def test_the_master_yields_exactly_its_nifty_option_listings() -> None:
    snapshot = _parse(_master())

    assert snapshot.listings == (
        UpstoxOptionListing(_contract(_OCT_27, "25000", OptionRight.CALL), "NSE_FO|90101", 65),
        UpstoxOptionListing(_contract(_OCT_27, "25000", OptionRight.PUT), "NSE_FO|90102", 65),
        UpstoxOptionListing(_contract(_OCT_27, "25050", OptionRight.CALL), "NSE_FO|90103", 65),
        UpstoxOptionListing(_contract(_NOV_24, "25000", OptionRight.CALL), "NSE_FO|90104", 65),
    )


def test_ce_maps_to_call_and_pe_to_put() -> None:
    snapshot = _parse([_option("NSE_FO|1", _OCT_27, 25000.0, "CE"),
                       _option("NSE_FO|2", _OCT_27, 25000.0, "PE")])  # fmt: skip

    assert [listing.contract.right for listing in snapshot.listings] == [
        OptionRight.CALL,
        OptionRight.PUT,
    ]


def test_several_strikes_of_one_expiry_coexist() -> None:
    strikes = (24900.0, 24950.0, 25000.0, 25050.0)
    snapshot = _parse([_option(f"NSE_FO|{i}", _OCT_27, s) for i, s in enumerate(strikes, 1)])

    assert [str(listing.contract.strike) for listing in snapshot.listings] == [
        "24900",
        "24950",
        "25000",
        "25050",
    ]


def test_one_strike_and_right_across_several_expiries_coexist() -> None:
    expiries = (date(2026, 10, 13), _OCT_27, _NOV_24, date(2031, 6, 24))
    snapshot = _parse([_option(f"NSE_FO|{i}", e, 25000.0) for i, e in enumerate(expiries, 1)])

    assert [listing.contract.expiration_date.value for listing in snapshot.listings] == [
        "2026-10-13",
        "2026-10-27",
        "2026-11-24",
        "2031-06-24",
    ]


def test_listings_are_in_canonical_natural_key_order() -> None:
    records = [
        _option("NSE_FO|1", _NOV_24, 25000.0, "PE"),
        _option("NSE_FO|2", _OCT_27, 25050.0, "CE"),
        _option("NSE_FO|3", _OCT_27, 9950.0, "PE"),
        _option("NSE_FO|4", _OCT_27, 25050.0, "PE"),
    ]
    keys = [listing.contract.natural_key for listing in _parse(records).listings]

    assert keys == sorted(keys)
    assert [listing.instrument_key for listing in _parse(records).listings] == [
        "NSE_FO|3",
        "NSE_FO|2",
        "NSE_FO|4",
        "NSE_FO|1",
    ]


@pytest.mark.parametrize(
    ("raw", "canonical"),
    [(25000.0, "25000"), (24950.5, "24950.5"), (25000, "25000"), (12000.00, "12000")],
)
def test_the_decoded_strike_becomes_an_exact_option_strike(raw: object, canonical: str) -> None:
    strike = _parse([_option("NSE_FO|1", _OCT_27, raw)]).listings[0].contract.strike

    assert strike == OptionStrike(Decimal(canonical))
    assert str(strike) == canonical


def test_expiry_is_the_ist_civil_date() -> None:
    """01:00 IST on 2026-10-27 is 19:30 UTC on 2026-10-26; the IST date governs."""
    record = {**_option("NSE_FO|1", _OCT_27, 25000.0), "expiry": _expiry_ms(_OCT_27, 1, 0, 0)}

    assert _parse([record]).listings[0].contract.expiration_date == ExpirationDate("2026-10-27")


def test_an_expiry_beyond_the_loaded_calendar_is_a_valid_listing() -> None:
    """No expiration rule or calendar is consulted during ingestion."""
    listing = _parse([_option("NSE_FO|1", date(2031, 6, 24), 25000.0)]).listings[0]

    assert listing.contract.expiration_date == ExpirationDate("2031-06-24")


def test_the_lot_is_the_reported_positive_integer() -> None:
    assert _parse([_option("NSE_FO|1", _OCT_27, 25000.0, lot_size=75)]).listings[0].lot_size == 75


# ---------------------------------------------------------------------------
# Skipped records
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "record",
    [
        _option("NSE_FO|1", _OCT_27, 0.0, "FUT"),
        _option("NSE_FO|1", _OCT_27, 56000.0, underlying="BANKNIFTY"),
        _option("NSE_FO|1", _OCT_27, 25000.0, underlying="FINNIFTY"),
        _option("BSE_FO|1", _OCT_27, 25000.0, segment="BSE_FO"),
        _option("NSE_FO|1", _OCT_27, 25000.0, exchange="BSE"),
        _option("NSE_FO|1", _OCT_27, 25000.0, "XX"),
        _option("NSE_FO|1", _OCT_27, 25000.0, "OPTIDX"),
        _option("NSE_FO|1", _OCT_27, 25000.0, "ce"),
        {"segment": "NSE_INDEX", "instrument_type": "INDEX",
         "instrument_key": "NSE_INDEX|Nifty 50"},
    ],
    ids=["fut", "banknifty", "finnifty", "bse-segment", "bse-exchange", "xx", "optidx", "lower",
         "index"],
)  # fmt: skip
def test_other_instruments_are_skipped_whatever_they_contain(record: dict[str, Any]) -> None:
    snapshot = _parse([record])

    assert snapshot.listings == ()
    assert snapshot.option_record_count == 0
    assert snapshot.record_count == 1


def test_a_skipped_record_is_skipped_even_when_malformed() -> None:
    record = {**_option("NSE_FO|1", _OCT_27, 25000.0, underlying="BANKNIFTY"), "expiry": "soon"}

    assert _parse([record]).listings == ()


def test_identity_never_comes_from_the_trading_symbol() -> None:
    misleading = _option("NSE_FO|1", _OCT_27, 25000.0, "CE", trading_symbol="BANKNIFTY 99999 PE")
    disguised = _option(
        "NSE_FO|2", _OCT_27, 25000.0, "CE", underlying="BANKNIFTY", trading_symbol="NIFTY 25000 CE"
    )

    snapshot = _parse([misleading, disguised])

    assert snapshot.listings == (
        UpstoxOptionListing(_contract(_OCT_27, "25000", OptionRight.CALL), "NSE_FO|1", 65),
    )


# ---------------------------------------------------------------------------
# Malformed candidates fail the snapshot
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "strike",
    [None, "25000", True, False, 0, 0.0, -25000.0, Decimal("NaN"), Decimal("Infinity")],
    ids=["missing", "text", "true", "false", "zero-int", "zero", "negative", "nan", "infinity"],
)
def test_a_malformed_strike_fails_the_snapshot(strike: object) -> None:
    record = _decoded_option(strike_price=strike)

    with pytest.raises(UpstoxOptionMasterError, match="strike"):
        _parse_raw(record)


@pytest.mark.parametrize(
    "expiry",
    [None, "2026-10-27", -1, True, Decimal("1792000000000.5"), 10**30],
    ids=["missing", "text", "negative", "bool", "fractional", "overflow"],
)
def test_a_malformed_expiry_fails_the_snapshot(expiry: object) -> None:
    record = _decoded_option(expiry=expiry)

    with pytest.raises(UpstoxOptionMasterError, match="expiry"):
        _parse_raw(record)


@pytest.mark.parametrize(
    "key",
    [None, 90101, "BSE_FO|90101", "NSE_FO|", "NSE_EQ|90101", " NSE_FO|90101", "NSE_FO|90101 "],
    ids=["missing", "number", "other-segment", "bare-prefix", "equity", "leading", "trailing"],
)
def test_a_malformed_instrument_key_fails_the_snapshot(key: object) -> None:
    record = _decoded_option(instrument_key=key)

    with pytest.raises(UpstoxOptionMasterError, match="instrument key"):
        _parse_raw(record)


@pytest.mark.parametrize(
    "lot",
    [None, 0, -65, True, Decimal("65"), Decimal("65.5"), "65", 65.0],
    ids=["missing", "zero", "negative", "bool", "decimal", "fraction", "text", "float"],
)
def test_a_malformed_lot_fails_the_snapshot(lot: object) -> None:
    record = _decoded_option(lot_size=lot)

    with pytest.raises(UpstoxOptionMasterError, match="lot size"):
        _parse_raw(record)


def test_one_malformed_candidate_fails_an_otherwise_good_master() -> None:
    records = [*_master(), {**_option("NSE_FO|9", _OCT_27, 25100.0), "lot_size": 0}]

    with pytest.raises(UpstoxOptionMasterError):
        _parse(records)


@pytest.mark.parametrize("records", [{}, "[]", None])
def test_a_master_that_is_not_an_array_fails(records: object) -> None:
    with pytest.raises(UpstoxOptionMasterError, match="JSON array"):
        parse_option_master(b"{}", records, NIFTY_NSE, source_url="u", fetched_at=_FETCHED)


def test_a_record_that_is_not_an_object_fails() -> None:
    with pytest.raises(UpstoxOptionMasterError, match="record 1 is not a JSON object"):
        _parse([_option("NSE_FO|1", _OCT_27, 25000.0), ["NSE_FO|2"]])  # type: ignore[list-item]


# ---------------------------------------------------------------------------
# Duplicates and ambiguity
# ---------------------------------------------------------------------------


def test_identical_duplicate_rows_collapse_and_are_still_counted() -> None:
    row = _option("NSE_FO|1", _OCT_27, 25000.0)
    snapshot = _parse([row, dict(row), dict(row)])

    assert len(snapshot.listings) == 1
    assert snapshot.option_record_count == 3
    assert snapshot.record_count == 3


def test_duplicates_differing_only_in_display_fields_collapse() -> None:
    first = _option("NSE_FO|1", _OCT_27, 25000.0, trading_symbol="A", weekly=True)
    second = {**_option("NSE_FO|1", _OCT_27, 25000.0, trading_symbol="B", weekly=False),
              "tick_size": 10.0, "exchange_token": "other"}  # fmt: skip

    assert len(_parse([first, second]).listings) == 1


def test_one_contract_with_two_keys_fails() -> None:
    with pytest.raises(UpstoxOptionMasterError, match="more than one instrument key"):
        _parse([_option("NSE_FO|1", _OCT_27, 25000.0), _option("NSE_FO|2", _OCT_27, 25000.0)])


def test_one_contract_with_two_lots_fails() -> None:
    with pytest.raises(UpstoxOptionMasterError, match="conflicting lot sizes"):
        _parse([_option("NSE_FO|1", _OCT_27, 25000.0, lot_size=65),
                _option("NSE_FO|1", _OCT_27, 25000.0, lot_size=75)])  # fmt: skip


@pytest.mark.parametrize(
    "other",
    [
        _option("NSE_FO|1", _OCT_27, 25050.0),
        _option("NSE_FO|1", _OCT_27, 25000.0, "PE"),
        _option("NSE_FO|1", _NOV_24, 25000.0),
    ],
    ids=["strike", "right", "expiry"],
)
def test_one_key_for_two_contracts_fails(other: dict[str, Any]) -> None:
    with pytest.raises(UpstoxOptionMasterError, match="one instrument key to 2"):
        _parse([_option("NSE_FO|1", _OCT_27, 25000.0), other])


def test_the_errors_are_provider_errors() -> None:
    assert issubclass(UpstoxOptionMasterError, UpstoxMarketDataSourceError)


# ---------------------------------------------------------------------------
# Snapshot identity and counts
# ---------------------------------------------------------------------------


def test_the_snapshot_hash_is_the_lowercase_sha256_of_the_exact_body() -> None:
    records = _master()
    body = _body(records)
    snapshot = parse_option_master(
        body, decode_json(body, context="t"), NIFTY_NSE, source_url="u", fetched_at=_FETCHED
    )

    assert snapshot.snapshot_sha256 == hashlib.sha256(body).hexdigest()
    assert snapshot.snapshot_sha256 == snapshot.snapshot_sha256.lower()
    assert len(snapshot.snapshot_sha256) == 64


def test_the_counts_are_records_and_raw_option_candidates() -> None:
    snapshot = _parse(_master())

    assert snapshot.record_count == len(_master()) == 11
    assert snapshot.option_record_count == 4
    assert snapshot.provider == UPSTOX_PROVIDER == "upstox"
    assert snapshot.source_url == NSE_INSTRUMENT_MASTER_URL
    assert snapshot.fetched_at == _FETCHED


@pytest.mark.parametrize(
    "product",
    [
        OptionProductReference(Symbol("BANKNIFTY"), ExchangeCode("NSE")),
        OptionProductReference(Symbol("NIFTY"), ExchangeCode("BSE")),
    ],
)
def test_only_nifty_at_nse_is_supported(product: OptionProductReference) -> None:
    with pytest.raises(UpstoxOptionMasterError, match="Unsupported option product"):
        _parse(_master(), product)


# ---------------------------------------------------------------------------
# Fetching and the clock
# ---------------------------------------------------------------------------


class _Fetch:
    def __init__(self, body: bytes | None = None, error: Exception | None = None) -> None:
        self.body, self.error, self.calls = body, error, []

    def __call__(self, url, headers, timeout):
        self.calls.append((url, dict(headers), timeout))
        if self.error is not None:
            raise self.error
        return self.body


class _Clock:
    def __init__(self, value: datetime, fetch: _Fetch | None = None) -> None:
        self.value, self.fetch, self.reads, self.calls_seen = value, fetch, 0, []

    def __call__(self) -> datetime:
        self.reads += 1
        self.calls_seen.append(len(self.fetch.calls) if self.fetch else None)
        return self.value


def test_a_fetch_reads_the_master_once_and_the_clock_once_after_it() -> None:
    body = _body(_master())
    fetch = _Fetch(body)
    clock = _Clock(datetime(2026, 10, 8, 10, 0, 0, tzinfo=_IST), fetch)

    snapshot = UpstoxOptionInstrumentMaster(fetch=fetch).fetch_snapshot(NIFTY_NSE, clock=clock)

    assert len(fetch.calls) == 1
    url, headers, _ = fetch.calls[0]
    assert url == NSE_INSTRUMENT_MASTER_URL
    assert headers == {"Accept": "application/json"}
    assert clock.reads == 1
    assert clock.calls_seen == [1]
    assert snapshot.fetched_at == PointInTime("2026-10-08T04:30:00Z")
    assert snapshot.snapshot_sha256 == hashlib.sha256(body).hexdigest()
    assert len(snapshot.listings) == 4


@pytest.mark.parametrize(
    ("error", "translated"),
    [
        (URLError("down"), UpstoxProviderUnavailableError),
        (TimeoutError(), UpstoxProviderUnavailableError),
        (HTTPError(NSE_INSTRUMENT_MASTER_URL, 503, "unavailable", {}, None),
         UpstoxMarketDataSourceError),
    ],
)  # fmt: skip
def test_a_failed_fetch_is_a_provider_error_and_reads_no_clock(
    error: Exception, translated: type[Exception]
) -> None:
    clock = _Clock(datetime(2026, 10, 8, tzinfo=UTC))

    with pytest.raises(translated):
        UpstoxOptionInstrumentMaster(fetch=_Fetch(error=error)).fetch_snapshot(
            NIFTY_NSE, clock=clock
        )
    assert clock.reads == 0


def test_an_undecodable_body_is_a_provider_error_and_reads_no_clock() -> None:
    clock = _Clock(datetime(2026, 10, 8, tzinfo=UTC))

    with pytest.raises(UpstoxProviderUnavailableError, match="undecodable"):
        UpstoxOptionInstrumentMaster(fetch=_Fetch(b"\x1f\x8bnot gzip")).fetch_snapshot(
            NIFTY_NSE, clock=clock
        )
    assert clock.reads == 0


def test_a_naive_clock_instant_is_refused() -> None:
    master = UpstoxOptionInstrumentMaster(fetch=_Fetch(_body(_master())))

    with pytest.raises(ValueError, match="timezone-aware"):
        master.fetch_snapshot(NIFTY_NSE, clock=lambda: datetime(2026, 10, 8, 10, 0))


def test_an_unsupported_product_is_refused_before_any_fetch() -> None:
    fetch = _Fetch(_body(_master()))
    banknifty = OptionProductReference(Symbol("BANKNIFTY"), ExchangeCode("NSE"))

    with pytest.raises(UpstoxOptionMasterError):
        UpstoxOptionInstrumentMaster(fetch=fetch).fetch_snapshot(
            banknifty, clock=lambda: datetime.now(UTC)
        )
    assert fetch.calls == []


def test_observation_instants_are_canonical_utc() -> None:
    assert observation_instant(datetime(2026, 10, 8, 10, 0, tzinfo=_IST)).value == (
        "2026-10-08T04:30:00Z"
    )
    assert observation_instant(datetime(2026, 10, 8, 4, 30, 0, 250000, tzinfo=UTC)).value == (
        "2026-10-08T04:30:00.25Z"
    )
    with pytest.raises(TypeError):
        observation_instant("2026-10-08T04:30:00Z")


# ---------------------------------------------------------------------------
# Boundaries
# ---------------------------------------------------------------------------


def _tree() -> ast.Module:
    return ast.parse(Path(module.__file__).read_text(encoding="utf-8"))


def test_the_module_reads_no_wall_clock() -> None:
    for node in ast.walk(_tree()):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            assert node.func.attr not in {"now", "utcnow", "today", "time", "monotonic"}


def test_the_module_consults_no_expiration_rule_calendar_or_futures_contract() -> None:
    tree = _tree()
    modules = {n.module for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module}
    names = {a.name for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) for a in n.names}

    for forbidden in ("nse_option_expiration", "nse_option_expiry_reference", "calendar_reference"):
        assert not [m for m in modules if forbidden in m]
    assert "northstar_core.futures" not in modules
    assert not [name for name in names if name.startswith("Futures")]
    assert "OptionExpirationResolver" not in names


def test_display_fields_are_never_looked_up() -> None:
    """No string constant names a display field, so no record lookup can read one."""
    constants = {
        node.value
        for node in ast.walk(_tree())
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    }

    for field in ("trading_symbol", "exchange_token", "tick_size", "weekly", "option_type"):
        assert field not in constants
