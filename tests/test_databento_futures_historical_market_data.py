"""Tests for the Databento futures historical market data adapter.

No network is used. The fakes are shaped like real DBN records -- raw integer
fixed-point prices, integer nanosecond timestamps, provider enums -- because
the whole point of this adapter is converting those exactly. A fake that
handed over Decimals would agree with any implementation.

Field values and semantics were confirmed against the live GLBX.MDP3 API during
the Epic 9.6a probe: asset is the product code, instrument_class 'F' is an
outright, exchange is XCME/XCBT/XNYM/XCEC, and ohlcv-1m ts_event is the
interval open.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, getcontext, localcontext
from pathlib import Path

import pytest
from databento_dbn import UNDEF_PRICE, InstrumentClass
from northstar_application.ports import (
    FuturesHistoricalMarketDataSource,
    FuturesTradingSession,
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
    DatabentoFuturesHistoricalMarketDataSource,
    DatabentoFuturesHistoricalMarketDataSourceError,
)

_API_KEY = "db-SECRET-NEVER-LOGGED-0123456789"

_ES = FuturesProductReference(Symbol("ES"), ExchangeCode("CME"))
_MES = FuturesProductReference(Symbol("MES"), ExchangeCode("CME"))
_ZB = FuturesProductReference(Symbol("ZB"), ExchangeCode("CBOT"))
_CL = FuturesProductReference(Symbol("CL"), ExchangeCode("NYMEX"))
_GC = FuturesProductReference(Symbol("GC"), ExchangeCode("COMEX"))

_DEC_EXPIRY = ExpirationDate("2026-12-18")
_ES_DEC = FuturesContract(_ES, _DEC_EXPIRY)
_MES_DEC = FuturesContract(_MES, _DEC_EXPIRY)

# Real CME session: opens on the previous civil day.
_SESSION = FuturesTradingSession(
    date(2026, 9, 15),
    PointInTime("2026-09-14T22:00:00Z"),
    PointInTime("2026-09-15T22:00:00Z"),
)

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_MINUTE_NS = 60_000_000_000

# Databento's expiration for ESZ6-style contracts: 08:30 America/Chicago.
_DEC_EXPIRY_NS = (
    int((datetime(2026, 12, 18, 14, 30, tzinfo=UTC) - _EPOCH).total_seconds()) * 1_000_000_000
)


def _ns(instant: str) -> int:
    moment = datetime.fromisoformat(instant.replace("Z", "+00:00"))
    return int((moment - _EPOCH).total_seconds()) * 1_000_000_000


# ---------------------------------------------------------------------------
# Record-shaped fakes
# ---------------------------------------------------------------------------


@dataclass
class FakeDefinition:
    raw_symbol: str = "ESZ6"
    asset: str = "ES"
    exchange: str = "XCME"
    expiration: int = _DEC_EXPIRY_NS
    instrument_class: object = InstrumentClass.FUTURE
    security_type: str = "FUT"
    instrument_id: int = 17740


@dataclass
class FakeOhlcv:
    ts_event: int
    open: int = 7_660_000_000_000
    high: int = 7_700_000_000_000
    low: int = 7_500_000_000_000
    close: int = 7_663_000_000_000
    volume: int = 100


@dataclass
class FakeTimeseries:
    definitions: list[FakeDefinition] = field(default_factory=list)
    bars: list[FakeOhlcv] = field(default_factory=list)
    calls: list[dict] = field(default_factory=list)
    failure: Exception | None = None

    def get_range(self, **parameters):
        self.calls.append(parameters)
        if self.failure is not None:
            raise self.failure
        if parameters["schema"] == "definition":
            return list(self.definitions)
        return list(self.bars)


# The provider's own availability bound, as GLBX.MDP3 reported it during the
# Epic 9.6a probe. Definitions run from 2010-06-06 to the dataset's edge.
_AVAILABLE_START = "2010-06-06T00:00:00.000000000Z"
_AVAILABLE_END = "2026-09-22T23:12:02.079075000Z"


@dataclass
class FakeMetadata:
    start: str = _AVAILABLE_START
    end: str = _AVAILABLE_END
    calls: list[str] = field(default_factory=list)
    payload_override: object | None = None

    def get_dataset_range(self, dataset):
        self.calls.append(dataset)
        if self.payload_override is not None:
            return self.payload_override
        return {
            "start": self.start,
            "end": self.end,
            "schema": {"definition": {"start": self.start, "end": self.end}},
        }

    def get_cost(self, **_):  # pragma: no cover - production must never call this
        raise AssertionError("production must not call metadata.get_cost")


@dataclass
class FakeClient:
    timeseries: FakeTimeseries = field(default_factory=FakeTimeseries)
    metadata: FakeMetadata = field(default_factory=FakeMetadata)


# Long after every session these tests fetch, so none depends on the wall clock.
_LONG_AFTER = datetime(2030, 1, 1, tzinfo=UTC)


def _completed_clock() -> datetime:
    return _LONG_AFTER


def _source(
    definitions: list[FakeDefinition] | None = None,
    bars: list[FakeOhlcv] | None = None,
) -> tuple[DatabentoFuturesHistoricalMarketDataSource, FakeClient]:
    client = FakeClient(
        FakeTimeseries(
            definitions=definitions if definitions is not None else [FakeDefinition()],
            bars=bars or [],
        )
    )
    return (
        DatabentoFuturesHistoricalMarketDataSource(_API_KEY, client=client, clock=_completed_clock),
        client,
    )


# ---------------------------------------------------------------------------
# Identity resolution
# ---------------------------------------------------------------------------


def test_es_resolves_through_the_es_parent_on_xcme() -> None:
    source, client = _source()

    source.fetch_session_bars(_ES_DEC, _SESSION)

    definition_call = client.timeseries.calls[0]
    assert definition_call["symbols"] == ["ES.FUT"]
    assert definition_call["stype_in"] == "parent"
    assert definition_call["schema"] == "definition"
    assert client.timeseries.calls[1]["symbols"] == ["ESZ6"]


@pytest.mark.parametrize(
    ("product", "asset", "exchange", "symbol"),
    [
        (_ES, "ES", "XCME", "ESZ6"),
        (_MES, "MES", "XCME", "MESZ6"),
        (_ZB, "ZB", "XCBT", "ZBZ6"),
        (_CL, "CL", "XNYM", "CLZ6"),
        (_GC, "GC", "XCEC", "GCZ6"),
    ],
)
def test_every_supported_venue_maps_to_its_provider_exchange(
    product: FuturesProductReference, asset: str, exchange: str, symbol: str
) -> None:
    contract = FuturesContract(product, _DEC_EXPIRY)
    source, client = _source([FakeDefinition(raw_symbol=symbol, asset=asset, exchange=exchange)])

    source.fetch_session_bars(contract, _SESSION)

    assert client.timeseries.calls[0]["symbols"] == [f"{asset}.FUT"]
    assert client.timeseries.calls[1]["symbols"] == [symbol]


def test_mes_is_not_matched_by_the_es_definition() -> None:
    """One venue, one expiry, two instruments."""
    source, _ = _source([FakeDefinition(raw_symbol="ESZ6", asset="ES")])

    with pytest.raises(
        DatabentoFuturesHistoricalMarketDataSourceError, match="no outright future matching"
    ):
        source.fetch_session_bars(_MES_DEC, _SESSION)


def test_spreads_are_ignored() -> None:
    source, _ = _source(
        [
            FakeDefinition(raw_symbol="ESZ6-ESH7", instrument_class=InstrumentClass.FUTURE_SPREAD),
            FakeDefinition(raw_symbol="ESZ6"),
        ]
    )

    source.fetch_session_bars(_ES_DEC, _SESSION)

    assert True  # resolved unambiguously despite the spread


def test_a_wrong_exchange_definition_is_ignored() -> None:
    source, _ = _source([FakeDefinition(raw_symbol="ESZ6", exchange="XCBT")])

    with pytest.raises(
        DatabentoFuturesHistoricalMarketDataSourceError, match="no outright future matching"
    ):
        source.fetch_session_bars(_ES_DEC, _SESSION)


def test_a_wrong_expiration_definition_is_ignored() -> None:
    other = int((datetime(2027, 3, 19, 14, 30, tzinfo=UTC) - _EPOCH).total_seconds()) * 10**9
    source, _ = _source([FakeDefinition(raw_symbol="ESH7", expiration=other)])

    with pytest.raises(
        DatabentoFuturesHistoricalMarketDataSourceError, match="no outright future matching"
    ):
        source.fetch_session_bars(_ES_DEC, _SESSION)


def test_expiration_is_compared_in_the_venue_timezone_not_utc() -> None:
    """A late-evening Chicago expiry falls on the next UTC date.

    23:00 America/Chicago on 2026-12-18 is 05:00 UTC on 2026-12-19. Comparing
    in UTC would reject the correct contract.
    """
    late_local = datetime(2026, 12, 19, 5, 0, tzinfo=UTC)
    expiration = int((late_local - _EPOCH).total_seconds()) * 10**9

    assert late_local.date() == date(2026, 12, 19)  # the UTC date disagrees

    source, client = _source([FakeDefinition(raw_symbol="ESZ6", expiration=expiration)])
    source.fetch_session_bars(_ES_DEC, _SESSION)

    assert client.timeseries.calls[1]["symbols"] == ["ESZ6"]


def test_ambiguous_definitions_raise() -> None:
    source, _ = _source([FakeDefinition(raw_symbol="ESZ6"), FakeDefinition(raw_symbol="ESZ6-ALT")])

    with pytest.raises(DatabentoFuturesHistoricalMarketDataSourceError, match="ambiguous"):
        source.fetch_session_bars(_ES_DEC, _SESSION)


def test_duplicate_identical_definitions_are_not_ambiguous() -> None:
    """Definitions repeat across a session; one distinct symbol is unambiguous."""
    source, _ = _source([FakeDefinition(), FakeDefinition(), FakeDefinition()])

    assert source.fetch_session_bars(_ES_DEC, _SESSION) == ()


def test_no_matching_definition_raises_rather_than_returning_empty() -> None:
    """An unresolvable contract is not the same fact as a session with no trades."""
    source, _ = _source([])

    with pytest.raises(
        DatabentoFuturesHistoricalMarketDataSourceError, match="no outright future matching"
    ):
        source.fetch_session_bars(_ES_DEC, _SESSION)


def test_an_unsupported_venue_raises() -> None:
    contract = FuturesContract(
        FuturesProductReference(Symbol("ES"), ExchangeCode("NASDAQ")), _DEC_EXPIRY
    )
    source, _ = _source()

    with pytest.raises(
        DatabentoFuturesHistoricalMarketDataSourceError, match="Unsupported futures venue"
    ):
        source.fetch_session_bars(contract, _SESSION)


def test_a_resolved_contract_is_cached() -> None:
    source, client = _source(bars=[])

    source.fetch_session_bars(_ES_DEC, _SESSION)
    source.fetch_session_bars(_ES_DEC, _SESSION)

    schemas = [call["schema"] for call in client.timeseries.calls]
    assert schemas == ["definition", "ohlcv-1m", "ohlcv-1m"]


def test_the_adapter_never_parses_raw_symbol_for_identity() -> None:
    """Identity comes from asset/exchange/expiration, never from the symbol text."""
    source, client = _source(
        [FakeDefinition(raw_symbol="TOTALLY-OPAQUE-42", asset="ES", exchange="XCME")]
    )

    source.fetch_session_bars(_ES_DEC, _SESSION)

    assert client.timeseries.calls[1]["symbols"] == ["TOTALLY-OPAQUE-42"]


def test_instrument_id_never_becomes_domain_identity() -> None:
    source, client = _source([FakeDefinition(instrument_id=999_999)])

    source.fetch_session_bars(_ES_DEC, _SESSION)

    for call in client.timeseries.calls:
        assert "999999" not in str(call)
    assert "instrument_id" not in str(client.timeseries.calls[1])


def test_a_definition_with_a_foreign_security_type_is_ignored() -> None:
    source, _ = _source([FakeDefinition(security_type="OOF")])

    with pytest.raises(
        DatabentoFuturesHistoricalMarketDataSourceError, match="no outright future matching"
    ):
        source.fetch_session_bars(_ES_DEC, _SESSION)


# ---------------------------------------------------------------------------
# Request shape
# ---------------------------------------------------------------------------


def test_the_bar_request_is_exactly_the_session_window() -> None:
    source, client = _source()

    source.fetch_session_bars(_ES_DEC, _SESSION)

    bar_call = client.timeseries.calls[1]
    assert bar_call["dataset"] == "GLBX.MDP3"
    assert bar_call["schema"] == "ohlcv-1m"
    assert bar_call["stype_in"] == "raw_symbol"
    assert bar_call["start"] == datetime(2026, 9, 14, 22, 0, tzinfo=UTC)
    assert bar_call["end"] == datetime(2026, 9, 15, 22, 0, tzinfo=UTC)


def test_the_request_bounds_are_neither_padded_nor_trimmed() -> None:
    source, client = _source()

    source.fetch_session_bars(_ES_DEC, _SESSION)

    bar_call = client.timeseries.calls[1]
    assert bar_call["start"].isoformat() == "2026-09-14T22:00:00+00:00"
    assert bar_call["end"].isoformat() == "2026-09-15T22:00:00+00:00"
    assert (bar_call["end"] - bar_call["start"]) == timedelta(hours=24)


def test_an_early_close_session_requests_its_shorter_window() -> None:
    early = FuturesTradingSession(
        date(2026, 7, 3), PointInTime("2026-07-02T22:00:00Z"), PointInTime("2026-07-03T17:00:00Z")
    )
    source, client = _source()

    source.fetch_session_bars(_ES_DEC, early)

    bar_call = client.timeseries.calls[1]
    assert (bar_call["end"] - bar_call["start"]) == timedelta(hours=19)


def test_the_bar_request_uses_utc_aware_datetimes() -> None:
    source, client = _source()

    source.fetch_session_bars(_ES_DEC, _SESSION)

    bar_call = client.timeseries.calls[1]
    assert bar_call["start"].tzinfo is UTC
    assert bar_call["end"].tzinfo is UTC


def test_the_definition_request_uses_plain_dates() -> None:
    """A definition window is a span of days, not an instant range."""
    source, client = _source()

    source.fetch_session_bars(_ES_DEC, _SESSION)

    definition_call = client.timeseries.calls[0]
    assert definition_call["start"] == "2026-09-19"
    assert definition_call["end"] == "2026-09-22"


def test_no_daily_or_settlement_schema_is_ever_requested() -> None:
    source, client = _source(bars=[FakeOhlcv(_ns("2026-09-15T14:30:00Z"))])

    source.fetch_session_bars(_ES_DEC, _SESSION)

    schemas = {call["schema"] for call in client.timeseries.calls}
    assert schemas == {"definition", "ohlcv-1m"}
    for forbidden in ("ohlcv-1d", "ohlcv-eod", "statistics", "trades"):
        assert forbidden not in schemas


# ---------------------------------------------------------------------------
# Timestamp translation
# ---------------------------------------------------------------------------


def test_a_bar_at_the_session_open_completes_one_minute_later() -> None:
    source, _ = _source(bars=[FakeOhlcv(_ns("2026-09-14T22:00:00Z"))])

    (bar,) = source.fetch_session_bars(_ES_DEC, _SESSION)

    assert bar.point_in_time == PointInTime("2026-09-14T22:01:00Z")
    assert _SESSION.opens_at.compare(bar.point_in_time) < 0


def test_the_final_interval_completes_exactly_at_the_session_close() -> None:
    source, _ = _source(bars=[FakeOhlcv(_ns("2026-09-15T21:59:00Z"))])

    (bar,) = source.fetch_session_bars(_ES_DEC, _SESSION)

    assert bar.point_in_time == _SESSION.closes_at
    assert bar.point_in_time.compare(_SESSION.closes_at) == 0


def test_a_raw_timestamp_at_the_session_close_is_rejected() -> None:
    """End-exclusive: an interval opening at the close belongs to the next session."""
    source, _ = _source(bars=[FakeOhlcv(_ns("2026-09-15T22:00:00Z"))])

    with pytest.raises(
        DatabentoFuturesHistoricalMarketDataSourceError, match="outside the requested session"
    ):
        source.fetch_session_bars(_ES_DEC, _SESSION)


def test_a_raw_timestamp_before_the_session_open_is_rejected() -> None:
    source, _ = _source(bars=[FakeOhlcv(_ns("2026-09-14T21:59:00Z"))])

    with pytest.raises(
        DatabentoFuturesHistoricalMarketDataSourceError, match="outside the requested session"
    ):
        source.fetch_session_bars(_ES_DEC, _SESSION)


def test_a_sub_microsecond_timestamp_is_rejected_not_truncated() -> None:
    source, _ = _source(bars=[FakeOhlcv(_ns("2026-09-15T14:30:00Z") + 500)])

    with pytest.raises(DatabentoFuturesHistoricalMarketDataSourceError, match="sub-microsecond"):
        source.fetch_session_bars(_ES_DEC, _SESSION)


def test_a_microsecond_timestamp_is_preserved() -> None:
    source, _ = _source(bars=[FakeOhlcv(_ns("2026-09-15T14:30:00Z") + 250_000)])

    (bar,) = source.fetch_session_bars(_ES_DEC, _SESSION)

    assert bar.point_in_time == PointInTime("2026-09-15T14:31:00.00025Z")


def test_the_translation_uses_no_float_epoch_conversion() -> None:
    """A float epoch loses precision long before nanoseconds."""
    import northstar_infrastructure.market_data.databento_futures_historical_market_data as module

    tree = ast.parse(Path(module.__file__).read_text(encoding="utf-8"))

    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            assert node.func.attr not in {"fromtimestamp", "timestamp"}
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            assert node.func.id != "float"


# ---------------------------------------------------------------------------
# Fixed-point price conversion
# ---------------------------------------------------------------------------


def test_positive_prices_convert_exactly() -> None:
    source, _ = _source(
        bars=[
            FakeOhlcv(
                _ns("2026-09-15T14:30:00Z"),
                open=7_660_250_000_000,
                high=7_700_750_000_000,
                low=7_500_000_000_000,
                close=7_663_125_000_000,
            )
        ]
    )

    (bar,) = source.fetch_session_bars(_ES_DEC, _SESSION)

    assert bar.open == QuoteValue(Decimal("7660.25"))
    assert bar.high == QuoteValue(Decimal("7700.75"))
    assert bar.low == QuoteValue(Decimal("7500"))
    assert bar.close == QuoteValue(Decimal("7663.125"))


def test_negative_prices_convert_exactly() -> None:
    """The 2020-04-20 crude session, in raw provider units."""
    source, _ = _source(
        bars=[
            FakeOhlcv(
                _ns("2026-09-15T14:30:00Z"),
                open=-14_000_000_000,
                high=-10_500_000_000,
                low=-40_320_000_000,
                close=-37_630_000_000,
            )
        ]
    )

    (bar,) = source.fetch_session_bars(_ES_DEC, _SESSION)

    assert bar.open == QuoteValue(Decimal("-14"))
    assert bar.close == QuoteValue(Decimal("-37.63"))
    assert bar.low == QuoteValue(Decimal("-40.32"))


def test_a_session_crossing_zero_converts_exactly() -> None:
    source, _ = _source(
        bars=[
            FakeOhlcv(
                _ns("2026-09-15T14:30:00Z"),
                open=5_000_000_000,
                high=6_000_000_000,
                low=-5_250_000_000,
                close=0,
            )
        ]
    )

    (bar,) = source.fetch_session_bars(_ES_DEC, _SESSION)

    assert bar.low == QuoteValue(Decimal("-5.25"))
    assert bar.close == QuoteValue(Decimal("0"))
    assert not bar.close.value.is_signed()


def test_zero_converts_to_unsigned_canonical_zero() -> None:
    source, _ = _source(
        bars=[FakeOhlcv(_ns("2026-09-15T14:30:00Z"), open=0, high=0, low=0, close=0)]
    )

    (bar,) = source.fetch_session_bars(_ES_DEC, _SESSION)

    assert str(bar.open.value) == "0"
    assert not bar.open.value.is_signed()


def test_a_high_precision_raw_integer_is_preserved() -> None:
    source, _ = _source(
        bars=[
            FakeOhlcv(
                _ns("2026-09-15T14:30:00Z"),
                open=7_660_123_456_789,
                high=9_999_999_999_999,
                low=1,
                close=7_660_123_456_789,
            )
        ]
    )

    (bar,) = source.fetch_session_bars(_ES_DEC, _SESSION)

    assert bar.open == QuoteValue(Decimal("7660.123456789"))
    assert bar.low == QuoteValue(Decimal("0.000000001"))


@pytest.mark.parametrize("precision", [6, 28, 50])
def test_price_conversion_is_identical_under_any_ambient_precision(precision: int) -> None:
    source, _ = _source(
        bars=[
            FakeOhlcv(
                _ns("2026-09-15T14:30:00Z"),
                open=7_660_123_456_789,
                high=9_999_999_999_999,
                low=1,
                close=7_660_123_456_789,
            )
        ]
    )

    with localcontext() as context:
        context.prec = precision
        (bar,) = source.fetch_session_bars(_ES_DEC, _SESSION)

    assert str(bar.open.value) == "7660.123456789"
    assert str(bar.low.value) == "1E-9"


def test_conversion_leaves_the_callers_context_untouched() -> None:
    source, _ = _source(bars=[FakeOhlcv(_ns("2026-09-15T14:30:00Z"))])
    before = getcontext()
    precision, rounding, flags = before.prec, before.rounding, dict(before.flags)

    source.fetch_session_bars(_ES_DEC, _SESSION)

    after = getcontext()
    assert (after.prec, after.rounding) == (precision, rounding)
    assert dict(after.flags) == flags


def test_no_decimal_context_flag_is_raised() -> None:
    source, _ = _source(
        bars=[
            FakeOhlcv(
                _ns("2026-09-15T14:30:00Z"), open=7_660_123_456_789, high=9_999_999_999_999, low=1
            )
        ]
    )

    with localcontext() as context:
        context.prec = 6
        context.clear_flags()

        source.fetch_session_bars(_ES_DEC, _SESSION)

        assert not any(context.flags.values())


@pytest.mark.parametrize("field_name", ["open", "high", "low", "close"])
def test_an_undefined_price_sentinel_is_rejected(field_name: str) -> None:
    """A sentinel must never become a colossal quotation."""
    bar = FakeOhlcv(_ns("2026-09-15T14:30:00Z"))
    setattr(bar, field_name, UNDEF_PRICE)
    source, _ = _source(bars=[bar])

    with pytest.raises(DatabentoFuturesHistoricalMarketDataSourceError, match="undefined"):
        source.fetch_session_bars(_ES_DEC, _SESSION)


def test_the_adapter_uses_no_pretty_price_accessor() -> None:
    """pretty_* fields are float conveniences and must not build a bar."""
    import northstar_infrastructure.market_data.databento_futures_historical_market_data as module

    source_text = Path(module.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source_text)

    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute):
            assert not node.attr.startswith("pretty_")
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            assert node.func.attr != "to_df"


# ---------------------------------------------------------------------------
# Volume
# ---------------------------------------------------------------------------


def test_volume_converts_exactly() -> None:
    source, _ = _source(bars=[FakeOhlcv(_ns("2026-09-15T14:30:00Z"), volume=1_442_686)])

    (bar,) = source.fetch_session_bars(_ES_DEC, _SESSION)

    assert bar.volume == Quantity(Decimal("1442686"))


def test_zero_volume_is_valid() -> None:
    source, _ = _source(bars=[FakeOhlcv(_ns("2026-09-15T14:30:00Z"), volume=0)])

    (bar,) = source.fetch_session_bars(_ES_DEC, _SESSION)

    assert bar.volume == Quantity(Decimal("0"))


def test_a_large_unsigned_volume_is_exact() -> None:
    large = 18_446_744_073_709_551_615
    source, _ = _source(bars=[FakeOhlcv(_ns("2026-09-15T14:30:00Z"), volume=large)])

    (bar,) = source.fetch_session_bars(_ES_DEC, _SESSION)

    assert str(bar.volume.value) == str(large)


def test_a_negative_volume_is_rejected() -> None:
    source, _ = _source(bars=[FakeOhlcv(_ns("2026-09-15T14:30:00Z"), volume=-1)])

    with pytest.raises(DatabentoFuturesHistoricalMarketDataSourceError, match="negative volume"):
        source.fetch_session_bars(_ES_DEC, _SESSION)


# ---------------------------------------------------------------------------
# Shape, order and sparseness
# ---------------------------------------------------------------------------


def test_a_session_with_no_records_returns_an_empty_tuple() -> None:
    source, _ = _source(bars=[])

    assert source.fetch_session_bars(_ES_DEC, _SESSION) == ()


def test_sparse_minutes_are_accepted() -> None:
    """Databento emits no record for a minute with no trades."""
    source, _ = _source(
        bars=[
            FakeOhlcv(_ns("2026-09-14T23:05:00Z")),
            FakeOhlcv(_ns("2026-09-15T08:17:00Z")),
            FakeOhlcv(_ns("2026-09-15T20:59:00Z")),
        ]
    )

    bars = source.fetch_session_bars(_ES_DEC, _SESSION)

    assert len(bars) == 3
    assert [b.point_in_time.value for b in bars] == [
        "2026-09-14T23:06:00Z",
        "2026-09-15T08:18:00Z",
        "2026-09-15T21:00:00Z",
    ]


def test_provider_order_is_preserved_and_never_sorted() -> None:
    """Malformed order is the aggregator's to reject, not this adapter's to hide."""
    source, _ = _source(
        bars=[
            FakeOhlcv(_ns("2026-09-15T14:31:00Z")),
            FakeOhlcv(_ns("2026-09-15T14:30:00Z")),
        ]
    )

    bars = source.fetch_session_bars(_ES_DEC, _SESSION)

    assert [b.point_in_time.value for b in bars] == [
        "2026-09-15T14:32:00Z",
        "2026-09-15T14:31:00Z",
    ]


def test_the_bar_path_contains_no_sort_call() -> None:
    """Scoped to fetch_session_bars: sorting venue names for an error message is fine."""
    import northstar_infrastructure.market_data.databento_futures_historical_market_data as module

    tree = ast.parse(Path(module.__file__).read_text(encoding="utf-8"))
    fetch = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == "fetch_session_bars"
    )

    for node in ast.walk(fetch):
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name):
                assert node.func.id not in {"sorted", "reversed"}
            if isinstance(node.func, ast.Attribute):
                assert node.func.attr != "sort"


def test_every_emitted_bar_is_a_one_minute_bar_for_the_requested_contract() -> None:
    source, _ = _source(bars=[FakeOhlcv(_ns("2026-09-15T14:30:00Z"))])

    (bar,) = source.fetch_session_bars(_ES_DEC, _SESSION)

    assert bar.timeframe == Timeframe("1m")
    assert bar.contract == _ES_DEC


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


def test_a_provider_failure_is_wrapped_with_its_cause() -> None:
    source, client = _source()
    client.timeseries.failure = ConnectionError("socket closed")

    with pytest.raises(DatabentoFuturesHistoricalMarketDataSourceError) as raised:
        source.fetch_session_bars(_ES_DEC, _SESSION)

    assert isinstance(raised.value.__cause__, ConnectionError)


def test_the_error_is_a_runtime_error() -> None:
    assert issubclass(DatabentoFuturesHistoricalMarketDataSourceError, RuntimeError)


def test_an_empty_session_is_not_an_operational_error() -> None:
    source, _ = _source(bars=[])

    assert source.fetch_session_bars(_ES_DEC, _SESSION) == ()


def test_foreign_argument_types_are_rejected() -> None:
    source, _ = _source()

    with pytest.raises(TypeError, match="FuturesContract"):
        source.fetch_session_bars(_ES, _SESSION)
    with pytest.raises(TypeError, match="FuturesTradingSession"):
        source.fetch_session_bars(_ES_DEC, "2026-09-15")


# ---------------------------------------------------------------------------
# Security
# ---------------------------------------------------------------------------


def test_the_api_key_never_appears_in_repr() -> None:
    source, _ = _source()

    assert _API_KEY not in repr(source)
    assert "SECRET" not in repr(source)


def test_the_api_key_never_appears_in_an_error_message() -> None:
    source, client = _source()
    client.timeseries.failure = ConnectionError("socket closed")

    with pytest.raises(DatabentoFuturesHistoricalMarketDataSourceError) as raised:
        source.fetch_session_bars(_ES_DEC, _SESSION)

    assert _API_KEY not in str(raised.value)
    assert _API_KEY not in repr(raised.value)


def test_an_empty_api_key_is_rejected() -> None:
    with pytest.raises(DatabentoFuturesHistoricalMarketDataSourceError, match="non-empty string"):
        DatabentoFuturesHistoricalMarketDataSource("   ")


def test_the_adapter_reads_no_environment_or_dotenv() -> None:
    import northstar_infrastructure.market_data.databento_futures_historical_market_data as module

    tree = ast.parse(Path(module.__file__).read_text(encoding="utf-8"))

    modules = {
        node.module
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module is not None
    } | {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    for forbidden in ("os", "dotenv", "sqlite3"):
        assert not any(m.split(".")[0] == forbidden for m in modules)

    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute):
            assert node.attr not in {"environ", "getenv", "load_dotenv"}


# ---------------------------------------------------------------------------
# Scope guards
# ---------------------------------------------------------------------------


def test_the_adapter_mentions_no_deferred_futures_concept() -> None:
    import northstar_infrastructure.market_data.databento_futures_historical_market_data as module

    tree = ast.parse(Path(module.__file__).read_text(encoding="utf-8"))
    literals = [
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    ]
    executed = [text for text in literals if text.startswith(("ohlcv", "statistics", "trades"))]

    assert executed == ["ohlcv-1m"]
    for forbidden in ("continuous", ".c.0", "rollover"):
        assert not any(forbidden in text for text in literals)


def test_the_adapter_implements_the_application_port() -> None:
    source, _ = _source()

    assert isinstance(source, FuturesHistoricalMarketDataSource)


# ---------------------------------------------------------------------------
# Session-independent contract resolution
# ---------------------------------------------------------------------------

_LATER_SESSION = FuturesTradingSession(
    date(2026, 9, 16),
    PointInTime("2026-09-15T22:00:00Z"),
    PointInTime("2026-09-16T22:00:00Z"),
)
_PRE_LISTING_SESSION = FuturesTradingSession(
    date(2015, 6, 2),
    PointInTime("2015-06-01T22:00:00Z"),
    PointInTime("2015-06-02T22:00:00Z"),
)


def _expiry_ns(moment: datetime) -> int:
    return int((moment - _EPOCH).total_seconds()) * 1_000_000_000


def _definition_call(client: FakeClient) -> dict:
    return next(call for call in client.timeseries.calls if call["schema"] == "definition")


def test_the_definition_window_ignores_the_requested_session() -> None:
    """The window is derived from the contract, never from the session."""
    source, client = _source()

    source.fetch_session_bars(_ES_DEC, _PRE_LISTING_SESSION)

    call = _definition_call(client)
    assert call["start"] == "2026-09-19"
    assert call["end"] == "2026-09-22"
    assert "2015" not in str(call["start"]) + str(call["end"])


def test_two_sessions_resolve_the_same_provider_symbol() -> None:
    source, client = _source()

    source.fetch_session_bars(_ES_DEC, _SESSION)
    source.fetch_session_bars(_ES_DEC, _LATER_SESSION)

    bar_calls = [call for call in client.timeseries.calls if call["schema"] == "ohlcv-1m"]
    assert [call["symbols"] for call in bar_calls] == [["ESZ6"], ["ESZ6"]]


def test_an_expired_contract_anchors_near_its_expiration() -> None:
    expired = FuturesContract(_ES, ExpirationDate("2024-12-20"))
    source, client = _source(
        [
            FakeDefinition(
                raw_symbol="ESZ4",
                expiration=_expiry_ns(datetime(2024, 12, 20, 14, 30, tzinfo=UTC)),
            )
        ]
    )

    source.fetch_session_bars(expired, _SESSION)

    call = _definition_call(client)
    assert call["start"] == "2024-12-18"
    assert call["end"] == "2024-12-21"


def test_a_future_expiry_contract_clamps_to_the_dataset_end() -> None:
    """ESZ6 expires after the dataset ends; the future day must not be queried."""
    source, client = _source()

    source.fetch_session_bars(_ES_DEC, _SESSION)

    call = _definition_call(client)
    assert call["end"] == "2026-09-22"
    assert call["start"] == "2026-09-19"
    assert "2026-12-18" not in (call["start"], call["end"])


def test_the_lookback_is_three_days() -> None:
    source, client = _source()

    source.fetch_session_bars(_ES_DEC, _SESSION)

    call = _definition_call(client)
    span = date.fromisoformat(call["end"]) - date.fromisoformat(call["start"])
    assert span == timedelta(days=3)


def test_the_window_start_clamps_to_provider_availability() -> None:
    """A contract expiring at the very start of the dataset cannot look back."""
    early = FuturesContract(_ES, ExpirationDate("2010-06-07"))
    source, client = _source(
        [
            FakeDefinition(
                raw_symbol="ESM0",
                expiration=_expiry_ns(datetime(2010, 6, 7, 14, 30, tzinfo=UTC)),
            )
        ]
    )

    source.fetch_session_bars(early, _SESSION)

    call = _definition_call(client)
    assert call["start"] == "2010-06-06"
    assert call["end"] == "2010-06-08"


def test_a_contract_outside_the_available_range_is_unresolvable() -> None:
    ancient = FuturesContract(_ES, ExpirationDate("2001-03-16"))
    source, _ = _source()

    with pytest.raises(
        DatabentoFuturesHistoricalMarketDataSourceError,
        match="falls outside the available range",
    ):
        source.fetch_session_bars(ancient, _SESSION)


# ---------------------------------------------------------------------------
# Metadata and caching
# ---------------------------------------------------------------------------


def test_the_dataset_range_is_fetched_once_per_adapter() -> None:
    source, client = _source([FakeDefinition(), FakeDefinition(raw_symbol="MESZ6", asset="MES")])

    source.fetch_session_bars(_ES_DEC, _SESSION)
    source.fetch_session_bars(_ES_DEC, _LATER_SESSION)
    source.fetch_session_bars(_MES_DEC, _SESSION)

    assert client.metadata.calls == ["GLBX.MDP3"]


def test_a_second_session_does_not_repeat_the_definition_lookup() -> None:
    source, client = _source()

    source.fetch_session_bars(_ES_DEC, _SESSION)
    source.fetch_session_bars(_ES_DEC, _LATER_SESSION)

    assert [call["schema"] for call in client.timeseries.calls] == [
        "definition",
        "ohlcv-1m",
        "ohlcv-1m",
    ]


def test_each_contract_is_resolved_separately() -> None:
    source, client = _source([FakeDefinition(), FakeDefinition(raw_symbol="MESZ6", asset="MES")])

    source.fetch_session_bars(_ES_DEC, _SESSION)
    source.fetch_session_bars(_MES_DEC, _SESSION)

    assert [call["schema"] for call in client.timeseries.calls] == [
        "definition",
        "ohlcv-1m",
        "definition",
        "ohlcv-1m",
    ]


def test_an_unusable_dataset_range_is_reported() -> None:
    source, client = _source()
    client.metadata.payload_override = {"start": "2010-06-06", "end": "2026-09-22"}

    with pytest.raises(
        DatabentoFuturesHistoricalMarketDataSourceError,
        match="usable GLBX.MDP3 definition range",
    ):
        source.fetch_session_bars(_ES_DEC, _SESSION)


def test_production_never_calls_get_cost() -> None:
    """The fake raises if it is called; cost estimation belongs to probes."""
    source, _ = _source(bars=[FakeOhlcv(_ns("2026-09-15T14:30:00Z"))])

    assert len(source.fetch_session_bars(_ES_DEC, _SESSION)) == 1


# ---------------------------------------------------------------------------
# Out-of-life sessions
# ---------------------------------------------------------------------------


def test_a_pre_listing_session_returns_an_empty_tuple() -> None:
    """Resolution succeeds from the contract window; the session has no data."""
    source, client = _source(bars=[])

    assert source.fetch_session_bars(_ES_DEC, _PRE_LISTING_SESSION) == ()
    assert _definition_call(client)["start"] == "2026-09-19"


def test_a_post_life_session_returns_an_empty_tuple() -> None:
    expired = FuturesContract(_ES, ExpirationDate("2024-12-20"))
    source, _ = _source(
        [
            FakeDefinition(
                raw_symbol="ESZ4",
                expiration=_expiry_ns(datetime(2024, 12, 20, 14, 30, tzinfo=UTC)),
            )
        ],
        bars=[],
    )

    assert source.fetch_session_bars(expired, _SESSION) == ()


def test_an_unresolvable_contract_still_raises_rather_than_returning_empty() -> None:
    """The distinction the whole change exists to preserve."""
    source, _ = _source([], bars=[])

    with pytest.raises(
        DatabentoFuturesHistoricalMarketDataSourceError, match="no outright future matching"
    ):
        source.fetch_session_bars(_ES_DEC, _PRE_LISTING_SESSION)


def test_an_ambiguous_contract_still_raises() -> None:
    source, _ = _source([FakeDefinition(raw_symbol="ESZ6"), FakeDefinition(raw_symbol="ESZ6-ALT")])

    with pytest.raises(DatabentoFuturesHistoricalMarketDataSourceError, match="ambiguous"):
        source.fetch_session_bars(_ES_DEC, _PRE_LISTING_SESSION)


# ---------------------------------------------------------------------------
# Why symbol text must stay opaque
# ---------------------------------------------------------------------------


def test_a_symbol_whose_text_reads_as_a_month_year_code_is_still_opaque() -> None:
    """Live evidence: ES expiring 2031-12-19 carries raw_symbol ESZ1.

    The venue's single-digit year code repeats every decade, so the identical
    string meant December 2021 five years earlier. Identity therefore comes
    from asset, exchange and expiration; the symbol is carried, never read.
    """
    far_future = FuturesContract(_ES, ExpirationDate("2031-12-19"))
    source, client = _source(
        [
            FakeDefinition(
                raw_symbol="ESZ1",
                expiration=_expiry_ns(datetime(2031, 12, 19, 14, 30, tzinfo=UTC)),
            )
        ],
        bars=[],
    )

    source.fetch_session_bars(far_future, _SESSION)

    bar_call = next(c for c in client.timeseries.calls if c["schema"] == "ohlcv-1m")
    assert bar_call["symbols"] == ["ESZ1"]


def test_a_decade_older_symbol_with_the_same_text_is_not_matched() -> None:
    """ESZ1 carrying a 2021 expiration must not satisfy a 2031 contract."""
    far_future = FuturesContract(_ES, ExpirationDate("2031-12-19"))
    source, _ = _source(
        [
            FakeDefinition(
                raw_symbol="ESZ1",
                expiration=_expiry_ns(datetime(2021, 12, 17, 14, 30, tzinfo=UTC)),
            )
        ]
    )

    with pytest.raises(
        DatabentoFuturesHistoricalMarketDataSourceError, match="no outright future matching"
    ):
        source.fetch_session_bars(far_future, _SESSION)


# ---------------------------------------------------------------------------
# Definition lookup across a long market closure
# ---------------------------------------------------------------------------


@dataclass
class ClosureTimeseries(FakeTimeseries):
    """Returns definitions only once the window is wide enough to hold a session.

    Mirrors a weekend followed by a Monday holiday: the three-day window spans
    no trading session at all, so the provider has nothing to report, while a
    wider window reaches back to the last session before the closure.
    """

    min_span_days: int = 7

    def get_range(self, **parameters):
        self.calls.append(parameters)
        if self.failure is not None:
            raise self.failure
        if parameters["schema"] != "definition":
            return list(self.bars)
        span = date.fromisoformat(parameters["end"]) - date.fromisoformat(parameters["start"])
        if span < timedelta(days=self.min_span_days):
            return []
        return list(self.definitions)


def _closure_source(
    definitions: list[FakeDefinition] | None = None,
    *,
    min_span_days: int = 7,
) -> tuple[DatabentoFuturesHistoricalMarketDataSource, FakeClient]:
    client = FakeClient(
        ClosureTimeseries(
            definitions=definitions if definitions is not None else [FakeDefinition()],
            bars=[],
            min_span_days=min_span_days,
        )
    )
    return (
        DatabentoFuturesHistoricalMarketDataSource(_API_KEY, client=client, clock=_completed_clock),
        client,
    )


def _definition_calls(client: FakeClient) -> list[dict]:
    return [call for call in client.timeseries.calls if call["schema"] == "definition"]


def test_a_populated_three_day_window_makes_exactly_one_definition_request() -> None:
    """Case A: the normal path must not pay for a second request."""
    source, client = _source()

    source.fetch_session_bars(_ES_DEC, _SESSION)

    calls = _definition_calls(client)
    assert len(calls) == 1
    span = date.fromisoformat(calls[0]["end"]) - date.fromisoformat(calls[0]["start"])
    assert span == timedelta(days=3)


def test_a_long_closure_widens_the_window_once_and_resolves() -> None:
    """Case B: no session in three days, so widen to seven and succeed."""
    source, client = _closure_source()

    source.fetch_session_bars(_ES_DEC, _SESSION)

    calls = _definition_calls(client)
    assert len(calls) == 2
    spans = [date.fromisoformat(call["end"]) - date.fromisoformat(call["start"]) for call in calls]
    assert spans == [timedelta(days=3), timedelta(days=7)]
    assert client.timeseries.calls[-1]["symbols"] == ["ESZ6"]


def test_the_widened_window_keeps_the_same_end_bound() -> None:
    """Only the lookback grows; the contract-anchored end is unchanged."""
    source, client = _closure_source()

    source.fetch_session_bars(_ES_DEC, _SESSION)

    calls = _definition_calls(client)
    assert {call["end"] for call in calls} == {"2026-09-22"}
    assert [call["start"] for call in calls] == ["2026-09-19", "2026-09-15"]


def test_a_non_matching_definition_set_raises_without_widening() -> None:
    """Case C: evidence disagreed with the request, so fail closed immediately."""
    wrong_expiry = _expiry_ns(datetime(2027, 3, 19, 14, 30, tzinfo=UTC))
    source, client = _source([FakeDefinition(raw_symbol="ESH7", expiration=wrong_expiry)])

    with pytest.raises(
        DatabentoFuturesHistoricalMarketDataSourceError, match="no outright future matching"
    ):
        source.fetch_session_bars(_ES_DEC, _SESSION)

    assert len(_definition_calls(client)) == 1


@pytest.mark.parametrize(
    ("label", "definition"),
    [
        ("wrong_venue", FakeDefinition(exchange="XCBT")),
        ("wrong_asset", FakeDefinition(asset="MES")),
        ("spread", FakeDefinition(instrument_class=InstrumentClass.FUTURE_SPREAD)),
        ("wrong_security_type", FakeDefinition(security_type="OOF")),
    ],
)
def test_no_filter_rejection_ever_triggers_a_retry(label: str, definition: FakeDefinition) -> None:
    """Every fail-closed path stays a single request."""
    source, client = _source([definition])

    with pytest.raises(DatabentoFuturesHistoricalMarketDataSourceError):
        source.fetch_session_bars(_ES_DEC, _SESSION)

    assert len(_definition_calls(client)) == 1


def test_an_empty_fallback_window_is_unresolvable() -> None:
    """Case D: widening once is the whole budget."""
    source, client = _closure_source(min_span_days=30)

    with pytest.raises(
        DatabentoFuturesHistoricalMarketDataSourceError, match="no outright future matching"
    ):
        source.fetch_session_bars(_ES_DEC, _SESSION)

    assert len(_definition_calls(client)) == 2


def test_both_attempts_clamp_to_provider_availability_start() -> None:
    """Case E: neither window may reach before the dataset begins."""
    early = FuturesContract(_ES, ExpirationDate("2010-06-07"))
    source, client = _closure_source(
        [
            FakeDefinition(
                raw_symbol="ESM0",
                expiration=_expiry_ns(datetime(2010, 6, 7, 14, 30, tzinfo=UTC)),
            )
        ],
        min_span_days=2,
    )

    source.fetch_session_bars(early, _SESSION)

    for call in _definition_calls(client):
        assert call["start"] == "2010-06-06"


def test_the_fallback_window_remains_independent_of_the_session() -> None:
    """Case F: widening changes the lookback, never the anchor's source."""
    source, client = _closure_source()

    source.fetch_session_bars(_ES_DEC, _PRE_LISTING_SESSION)

    for call in _definition_calls(client):
        assert call["start"].startswith("2026-09")
        assert call["end"] == "2026-09-22"
        assert "2015" not in call["start"] + call["end"]


def test_a_widened_resolution_is_cached_like_any_other() -> None:
    source, client = _closure_source()

    source.fetch_session_bars(_ES_DEC, _SESSION)
    source.fetch_session_bars(_ES_DEC, _LATER_SESSION)

    assert len(_definition_calls(client)) == 2
    assert client.metadata.calls == ["GLBX.MDP3"]


def test_a_failed_resolution_is_not_cached() -> None:
    """A later attempt must be free to succeed."""
    source, client = _closure_source(min_span_days=30)

    with pytest.raises(DatabentoFuturesHistoricalMarketDataSourceError):
        source.fetch_session_bars(_ES_DEC, _SESSION)

    client.timeseries.min_span_days = 7
    source.fetch_session_bars(_ES_DEC, _SESSION)

    assert client.timeseries.calls[-1]["symbols"] == ["ESZ6"]
