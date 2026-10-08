"""Tests for atomically persisting option daily bars with their raw provider open interest."""

from __future__ import annotations

import ast
import json
import sqlite3
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from northstar_application.application_services import AcquireOptionNativeDailyHistoryUseCase
from northstar_application.ports import (
    OptionDailyAcquisitionQuery,
    OptionHistoricalMarketDataConflictError,
    OptionHistoricalMarketDataQuery,
    OptionHistoricalMarketDataStore,
)
from northstar_core.derivatives import ExpirationDate
from northstar_core.foundation.value_objects import (
    ExchangeCode,
    PointInTime,
    Quantity,
    Symbol,
    Timeframe,
)
from northstar_core.options import (
    OptionContract,
    OptionOHLCVBar,
    OptionPremium,
    OptionProductReference,
    OptionRight,
    OptionStrike,
)

import northstar_infrastructure.market_data.sqlite_option_daily_acquisition_store as module
from northstar_infrastructure.market_data import (
    NSEOptionTradingSessionResolver,
    OptionHistoricalStorageError,
    OptionOpenInterestCaptureError,
    OptionOpenInterestConflictError,
    ProviderOptionOpenInterest,
    SQLiteOptionDailyAcquisitionStore,
    SQLiteOptionHistoricalMarketDataRepository,
    SQLiteOptionHistoricalMarketDataStore,
    SQLiteOptionListingRepository,
    SQLiteOptionListingStore,
    SQLiteOptionProviderOpenInterestRepository,
    SQLiteOptionProviderOpenInterestStore,
    UpstoxOptionListing,
    UpstoxOptionMasterSnapshot,
    UpstoxOptionNativeDailyMarketDataSource,
)
from northstar_infrastructure.market_data.sqlite_option_provider_open_interest import (
    SQLiteOptionProviderOpenInterestStore as OpenInterestStore,
)

_NIFTY = OptionProductReference(Symbol("NIFTY"), ExchangeCode("NSE"))
_DAILY = Timeframe("1d")
_DAYS = (date(2026, 10, 5), date(2026, 10, 6), date(2026, 10, 7))


def _contract(strike: str = "22600", right: OptionRight = OptionRight.PUT) -> OptionContract:
    return OptionContract(
        _NIFTY, ExpirationDate("2026-10-27"), OptionStrike(Decimal(strike)), right
    )


_PUT = _contract()
_CALL = _contract(right=OptionRight.CALL)


def _bar(day: date, contract: OptionContract = _PUT, close: str = "132.6") -> OptionOHLCVBar:
    # 15:40 IST is 10:10 UTC: the resolved NIFTY option session close.
    return OptionOHLCVBar(
        contract=contract,
        point_in_time=PointInTime(f"{day.isoformat()}T10:10:00Z"),
        timeframe=_DAILY,
        open=OptionPremium(Decimal("191.8")),
        high=OptionPremium(Decimal("226.05")),
        low=OptionPremium(Decimal("122.45")),
        close=OptionPremium(Decimal(close)),
        volume=Quantity(Decimal("40")),
    )


def _oi(day: date, value: str = "6686095", contract: OptionContract = _PUT, provider="upstox"):
    return ProviderOptionOpenInterest(provider, contract, day, Decimal(value))


_BARS = tuple(_bar(day) for day in _DAYS)
_OI = tuple(_oi(day) for day in _DAYS)


class Capture:
    """Hands out one prepared capture per take, like the Upstox source."""

    def __init__(self, *captures: object) -> None:
        self.captures = list(captures)
        self.takes = 0

    def take_open_interest(self):
        self.takes += 1
        if not self.captures:
            raise OptionOpenInterestCaptureError("nothing captured")
        return self.captures.pop(0)


@pytest.fixture
def database(tmp_path: Path) -> Path:
    return tmp_path / "northstar.sqlite3"


def _store(database: Path, bars=_BARS, captured=_OI) -> int:
    capture = Capture(captured)
    return SQLiteOptionDailyAcquisitionStore(
        database, open_interest=capture, provider="upstox"
    ).store(bars)


def _query(database: Path, sql: str) -> list[tuple]:
    with sqlite3.connect(database) as connection:
        return connection.execute(sql).fetchall()


def _tables(database: Path) -> set[str]:
    if not database.exists():
        return set()
    return {r[0] for r in _query(database, "SELECT name FROM sqlite_master WHERE type='table'")}


def _bar_rows(database: Path) -> list[tuple]:
    if "option_ohlcv" not in _tables(database):
        return []
    return _query(database, "SELECT point_in_time, close_premium FROM option_ohlcv ORDER BY 1")


def _oi_rows(database: Path) -> list[tuple]:
    if "option_daily_provider_open_interest" not in _tables(database):
        return []
    return _query(
        database,
        "SELECT trading_date, open_interest_raw FROM option_daily_provider_open_interest "
        "ORDER BY 1",
    )


# ---------------------------------------------------------------------------
# Writing together
# ---------------------------------------------------------------------------


def test_the_store_implements_the_option_port(database: Path) -> None:
    store = SQLiteOptionDailyAcquisitionStore(database, open_interest=Capture(), provider="upstox")
    assert isinstance(store, OptionHistoricalMarketDataStore)


def test_bars_and_open_interest_are_written_together(database: Path) -> None:
    assert _store(database) == 3

    assert _tables(database) == {"option_ohlcv", "option_daily_provider_open_interest"}
    assert _bar_rows(database) == [
        ("2026-10-05T10:10:00Z", "132.6"),
        ("2026-10-06T10:10:00Z", "132.6"),
        ("2026-10-07T10:10:00Z", "132.6"),
    ]
    assert _oi_rows(database) == [
        ("2026-10-05", "6686095"),
        ("2026-10-06", "6686095"),
        ("2026-10-07", "6686095"),
    ]


def test_capture_order_does_not_matter(database: Path) -> None:
    assert _store(database, captured=tuple(reversed(_OI))) == 3
    assert len(_oi_rows(database)) == 3


def test_the_bar_instant_maps_back_to_its_ist_trading_date(database: Path) -> None:
    # 2026-10-05T20:00Z is already 2026-10-06 in IST, so it needs that date's record.
    late = _replace_instant(_BARS[0], "2026-10-05T20:00:00Z")

    with pytest.raises(OptionOpenInterestCaptureError):
        _store(database, (late,), (_oi(date(2026, 10, 5)),))
    assert _store(database, (late,), (_oi(date(2026, 10, 6)),)) == 1


def _replace_instant(bar: OptionOHLCVBar, instant: str) -> OptionOHLCVBar:
    return OptionOHLCVBar(
        bar.contract, PointInTime(instant), bar.timeframe, bar.open, bar.high, bar.low, bar.close,
        bar.volume,
    )  # fmt: skip


def test_records_survive_reopen(database: Path) -> None:
    _store(database)

    bars = SQLiteOptionHistoricalMarketDataRepository(database).get_bars(
        OptionHistoricalMarketDataQuery(_PUT, _DAILY)
    )
    assert bars == _BARS
    repository = SQLiteOptionProviderOpenInterestRepository(database)
    assert [repository.get("upstox", _PUT, day) for day in _DAYS] == list(_OI)


def test_an_identical_replay_is_idempotent(database: Path) -> None:
    _store(database)
    before = (_bar_rows(database), _oi_rows(database))

    assert _store(database) == 3
    assert (_bar_rows(database), _oi_rows(database)) == before


def test_a_replay_can_add_a_later_candle(database: Path) -> None:
    _store(database, (_BARS[0], _BARS[2]), (_OI[0], _OI[2]))

    assert _store(database) == 3
    assert len(_bar_rows(database)) == 3
    assert len(_oi_rows(database)) == 3


def test_an_empty_batch_with_an_empty_capture_writes_nothing(database: Path) -> None:
    assert _store(database, (), ()) == 0
    assert not database.exists()


def test_an_empty_batch_still_consumes_the_capture(database: Path) -> None:
    capture = Capture((), ())
    store = SQLiteOptionDailyAcquisitionStore(database, open_interest=capture, provider="upstox")

    store.store(())

    assert capture.takes == 1
    assert capture.captures == [()]


def test_the_schema_matches_the_standalone_option_stores(tmp_path: Path) -> None:
    composite, standalone = tmp_path / "composite.sqlite3", tmp_path / "standalone.sqlite3"
    _store(composite)
    SQLiteOptionHistoricalMarketDataStore(standalone).store(_BARS)
    SQLiteOptionProviderOpenInterestStore(standalone).store(_OI)

    def schema(path: Path) -> list[tuple]:
        return sorted(_query(path, "SELECT type, name, sql FROM sqlite_master"))

    assert schema(composite) == schema(standalone)
    assert _bar_rows(composite) == _bar_rows(standalone)
    assert _oi_rows(composite) == _oi_rows(standalone)


# ---------------------------------------------------------------------------
# Exact one-to-one correspondence
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("captured", "message"),
    [
        ((_OI[0], _OI[1]), "No open interest was captured"),
        ((*_OI, _oi(date(2026, 10, 8))), "with no bar to store beside it"),
        ((_OI[0], _OI[1], _oi(_DAYS[2], contract=_CALL)), "No open interest was captured"),
        ((*_OI, _OI[0]), "two records"),
        ((_OI[0], _OI[1], _oi(_DAYS[2], provider="other")), "from other, not upstox"),
        (list(_OI), "must be a tuple"),
        ((*_OI[:2], object()), "must be a tuple"),
    ],
    ids=["missing", "extra", "wrong-contract", "duplicate", "wrong-provider", "list", "foreign"],
)
def test_a_capture_that_does_not_match_writes_nothing(database: Path, captured, message) -> None:
    with pytest.raises(OptionOpenInterestCaptureError, match=message):
        _store(database, captured=captured)
    assert not database.exists()


def test_an_empty_batch_with_captured_open_interest_is_a_defect(database: Path) -> None:
    with pytest.raises(OptionOpenInterestCaptureError, match="no bar"):
        _store(database, (), (_OI[0],))
    assert not database.exists()


def test_no_capture_at_all_is_a_defect(database: Path) -> None:
    store = SQLiteOptionDailyAcquisitionStore(database, open_interest=Capture(), provider="upstox")

    with pytest.raises(OptionOpenInterestCaptureError):
        store.store(_BARS)
    assert not database.exists()


def test_the_capture_is_consumed_even_when_the_bars_are_refused(database: Path) -> None:
    capture = Capture(_OI)
    store = SQLiteOptionDailyAcquisitionStore(database, open_interest=capture, provider="upstox")

    with pytest.raises(TypeError):
        store.store(list(_BARS))  # type: ignore[arg-type]
    assert capture.takes == 1
    with pytest.raises(OptionOpenInterestCaptureError):
        store.store(_BARS)


def test_a_capture_mismatch_is_an_internal_defect_not_a_data_or_storage_error() -> None:
    assert issubclass(OptionOpenInterestCaptureError, RuntimeError)
    assert not issubclass(OptionOpenInterestCaptureError, OptionHistoricalStorageError)


# ---------------------------------------------------------------------------
# All or nothing
# ---------------------------------------------------------------------------


def test_an_open_interest_conflict_rolls_back_the_new_bars(database: Path) -> None:
    OpenInterestStore(database).store((_oi(_DAYS[2], "1"),))

    with pytest.raises(OptionOpenInterestConflictError):
        _store(database)

    assert _bar_rows(database) == []
    assert "option_ohlcv" not in _tables(database)
    assert _oi_rows(database) == [("2026-10-07", "1")]


def test_a_bar_conflict_rolls_back_the_new_open_interest(database: Path) -> None:
    SQLiteOptionHistoricalMarketDataStore(database).store((_bar(_DAYS[2], close="199"),))

    with pytest.raises(OptionHistoricalMarketDataConflictError):
        _store(database)

    assert _bar_rows(database) == [("2026-10-07T10:10:00Z", "199")]
    assert "option_daily_provider_open_interest" not in _tables(database)


def test_a_conflict_after_earlier_stores_changes_nothing(database: Path) -> None:
    _store(database, (_BARS[0],), (_OI[0],))
    before = (_bar_rows(database), _oi_rows(database))

    with pytest.raises(OptionOpenInterestConflictError):
        _store(database, _BARS, (_oi(_DAYS[0], "5"), _OI[1], _OI[2]))

    assert (_bar_rows(database), _oi_rows(database)) == before


def test_a_failed_first_store_leaves_no_table(database: Path, monkeypatch) -> None:
    def broken(connection, record) -> None:
        raise sqlite3.OperationalError("disk I/O error")

    monkeypatch.setattr(OpenInterestStore, "_store_one", staticmethod(broken))

    with pytest.raises(OptionHistoricalStorageError, match="unavailable"):
        _store(database)

    assert _tables(database) == set()


def test_an_unexpected_failure_also_rolls_back(database: Path, monkeypatch) -> None:
    def broken(connection, record) -> None:
        raise KeyError("boom")

    monkeypatch.setattr(OpenInterestStore, "_store_one", staticmethod(broken))

    with pytest.raises(KeyError):
        _store(database)
    assert _tables(database) == set()


def test_a_database_that_cannot_be_opened_is_a_storage_error(tmp_path: Path) -> None:
    with pytest.raises(OptionHistoricalStorageError):
        _store(tmp_path / "missing-directory" / "northstar.sqlite3")


def test_the_standalone_option_stores_still_create_only_their_own_table(tmp_path: Path) -> None:
    bars_only, oi_only = tmp_path / "bars.sqlite3", tmp_path / "oi.sqlite3"
    SQLiteOptionHistoricalMarketDataStore(bars_only).store(_BARS)
    OpenInterestStore(oi_only).store(_OI)

    assert _tables(bars_only) == {"option_ohlcv"}
    assert _tables(oi_only) == {"option_daily_provider_open_interest"}


def test_the_module_issues_no_overwrite_and_names_no_futures_table() -> None:
    source = Path(module.__file__).read_text(encoding="utf-8")
    for forbidden in ("UPDATE ", "DELETE ", "REPLACE", "ON CONFLICT", "DROP ", "futures_"):
        assert forbidden not in source
    tree = ast.parse(source)
    names = {a.name for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) for a in n.names}
    assert not [name for name in names if "Upstox" in name or name.startswith("Futures")]
    assert "initialize_option_market_data_schema" not in names


# ---------------------------------------------------------------------------
# One acquisition end to end (Application use case, real resolver, Upstox source)
# ---------------------------------------------------------------------------


def _listed(database: Path) -> SQLiteOptionListingRepository:
    SQLiteOptionListingStore(database).store(
        UpstoxOptionMasterSnapshot(
            snapshot_sha256="a" * 64,
            source_url="https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz",
            fetched_at=PointInTime("2026-10-04T04:30:00Z"),
            record_count=10,
            option_record_count=1,
            listings=(UpstoxOptionListing(_PUT, "NSE_FO|1001", 65),),
        )
    )
    return SQLiteOptionListingRepository(database)


def _candles(*days: str) -> bytes:
    rows = [[f"{day}T00:00:00+05:30", 191.8, 226.05, 122.45, 132.6, 2600, 6686095] for day in days]
    return json.dumps({"status": "success", "data": {"candles": rows}}).encode()


def _acquire(database: Path, body: bytes):
    source = UpstoxOptionNativeDailyMarketDataSource(
        "token", listings=_listed(database), fetch=lambda url, headers, timeout: body
    )
    store = SQLiteOptionDailyAcquisitionStore(database, open_interest=source, provider="upstox")
    use_case = AcquireOptionNativeDailyHistoryUseCase(
        NSEOptionTradingSessionResolver(), source, store
    )
    return use_case.execute(OptionDailyAcquisitionQuery(_PUT, _DAYS[0], _DAYS[-1]))


def test_a_session_without_a_provider_candle_gets_no_bar_and_no_open_interest(
    database: Path,
) -> None:
    result = _acquire(database, _candles("2026-10-07", "2026-10-05"))

    assert (result.session_count, result.daily_bar_count) == (3, 2)
    assert result.missing_trading_dates == (date(2026, 10, 6),)
    assert [row[0] for row in _bar_rows(database)] == [
        "2026-10-05T10:10:00Z",
        "2026-10-07T10:10:00Z",
    ]
    assert [row[0] for row in _oi_rows(database)] == ["2026-10-05", "2026-10-07"]


def test_a_range_without_any_candle_succeeds_and_writes_no_market_data(database: Path) -> None:
    result = _acquire(database, _candles())

    assert (result.daily_bar_count, result.missing_trading_dates) == (0, _DAYS)
    assert _tables(database) == {"option_listing_snapshots", "option_provider_listings"}
