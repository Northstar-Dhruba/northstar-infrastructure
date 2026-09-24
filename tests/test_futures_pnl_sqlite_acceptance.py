"""Acceptance: futures product economics and gross simulated P&L on real SQLite.

Market bars, frozen forward decisions, paper orders, paper fills and product
economics live in real temporary SQLite files and are written and read only
through the production adapters. Every adapter is constructed afresh for every
call, so nothing depends on process memory. There is no provider, no network,
no calendar, no credential and no clock.

Everything valued here is gross simulated P&L: gross simulated realized P&L
reconstructed from immutable fills, and gross simulated unrealized P&L of open
positions marked at the latest persisted daily CLOSE observable at the cutoff.
No commission, fee, slippage, tax, financing or margin is modelled, and the
mark is not a settlement price.

Product economics are test fixtures, not exchange metadata:

    ES@CME        50 USD / point / contract
    FESX@EUREX    25 EUR / point / contract
    ES@EUREX       7 EUR / point / contract   (same code, other exchange)

Most paper facts are stored directly through the SQLite order and fill stores
so fill quotes are exact; one compact case runs the 9.9 execution pipeline to
show P&L derives from the fill the pipeline persisted.
"""

from __future__ import annotations

import shutil
import sqlite3
from datetime import date, timedelta
from decimal import (
    ROUND_CEILING,
    ROUND_DOWN,
    ROUND_HALF_EVEN,
    ROUND_HALF_UP,
    Context,
    Decimal,
    localcontext,
)
from pathlib import Path

import pytest
from northstar_application.application_services import (
    BuildFuturesPaperPortfolioUseCase,
    BuildFuturesPaperTradingValuationUseCase,
    CalculateFuturesRealizedPnlUseCase,
    FreezeFuturesForwardResearchDecisionUseCase,
    FuturesContractPnl,
    FuturesPaperTradingValuation,
    FuturesProductEconomicsNotFoundError,
    RunFuturesPaperTradingDecisionUseCase,
    ValueFuturesPaperPortfolioUseCase,
)
from northstar_application.ports import (
    FuturesPaperFillConflictError,
    FuturesPaperFillQuery,
)
from northstar_core.derivatives import ExpirationDate, QuoteValue
from northstar_core.foundation.value_objects import (
    Currency,
    ExchangeCode,
    Money,
    PointInTime,
    Quantity,
    Symbol,
    Timeframe,
)
from northstar_core.futures import (
    FuturesContract,
    FuturesOHLCVBar,
    FuturesPointValue,
    FuturesProductEconomics,
    FuturesProductReference,
)
from northstar_core.paper_trading import (
    FuturesContractCount,
    FuturesExecutionIntent,
    FuturesPaperFill,
    FuturesPaperOrder,
    FuturesPosition,
    OrderSide,
    PaperFillIdentity,
    PaperOrderIdentity,
    PaperPortfolioIdentity,
)
from northstar_core.strategy import FuturesAssetAnalysisGenerator, Strategy, StrategyIdentity

from northstar_infrastructure.market_data import (
    SQLiteFuturesHistoricalMarketDataRepository,
    SQLiteFuturesHistoricalMarketDataStore,
    initialize_futures_market_data_schema,
)
from northstar_infrastructure.persistence import (
    FuturesProductEconomicsConflictError,
    SQLiteFuturesForwardResearchRecordRepository,
    SQLiteFuturesForwardResearchRecordStore,
    SQLiteFuturesPaperFillRepository,
    SQLiteFuturesPaperFillStore,
    SQLiteFuturesPaperOrderRepository,
    SQLiteFuturesPaperOrderStore,
    SQLiteFuturesProductEconomicsRepository,
    SQLiteFuturesProductEconomicsStore,
    initialize_futures_forward_research_record_schema,
    initialize_futures_paper_trading_schema,
    initialize_futures_product_economics_schema,
)

_USD = Currency("USD")
_EUR = Currency("EUR")
_DAILY = Timeframe("1d")
_ES = FuturesProductReference(Symbol("ES"), ExchangeCode("CME"))
_FESX = FuturesProductReference(Symbol("FESX"), ExchangeCode("EUREX"))
_ES_EUREX = FuturesProductReference(Symbol("ES"), ExchangeCode("EUREX"))
_ES_DEC = FuturesContract(_ES, ExpirationDate("2026-12-18"))
_ES_MAR = FuturesContract(_ES, ExpirationDate("2027-03-19"))
_FESX_DEC = FuturesContract(_FESX, ExpirationDate("2026-12-18"))
_ES_ECONOMICS = FuturesProductEconomics(_ES, FuturesPointValue(Decimal("50"), _USD))
_FESX_ECONOMICS = FuturesProductEconomics(_FESX, FuturesPointValue(Decimal("25"), _EUR))
_ES_EUREX_ECONOMICS = FuturesProductEconomics(_ES_EUREX, FuturesPointValue(Decimal("7"), _EUR))
_ALPHA = StrategyIdentity("alpha")
_BETA = StrategyIdentity("beta")
_PORTFOLIO = PaperPortfolioIdentity("futures-pnl-alpha")
_DECIDED = PointInTime("2026-09-01T21:00:00Z")
_CONTEXT = Context(prec=28, rounding=ROUND_HALF_EVEN)
_BASIS = "100.6666666666666666666666667"

BUY = OrderSide.BUY
SELL = OrderSide.SELL


def _day(day: int, clock: str = "21:00:00") -> PointInTime:
    return PointInTime(f"2026-09-{day:02d}T{clock}Z")


_T = _day(20)


def _usd(amount: str) -> Money:
    return Money(Decimal(amount), _USD)


def _eur(amount: str) -> Money:
    return Money(Decimal(amount), _EUR)


def _quote(value: str) -> QuoteValue:
    return QuoteValue(Decimal(value))


def _position(net: int, entry: str, contract: FuturesContract = _ES_DEC) -> FuturesPosition:
    return FuturesPosition(contract, net, _quote(entry))


# ---------------------------------------------------------------------------
# Production adapters and use cases, rebuilt for every call
# ---------------------------------------------------------------------------


def _initialize(path: Path) -> None:
    with sqlite3.connect(path) as connection:
        initialize_futures_market_data_schema(connection)
        initialize_futures_forward_research_record_schema(connection)
        initialize_futures_paper_trading_schema(connection)
        initialize_futures_product_economics_schema(connection)


def _database(
    tmp_path: Path,
    name: str = "pnl",
    economics: tuple[FuturesProductEconomics, ...] = (_ES_ECONOMICS, _FESX_ECONOMICS),
) -> Path:
    path = tmp_path / f"{name}.sqlite3"
    _initialize(path)
    if economics:
        assert SQLiteFuturesProductEconomicsStore(path).store(economics) == len(economics)
    return path


def _query(path: Path, sql: str, parameters: tuple = ()) -> list[tuple]:
    with sqlite3.connect(path) as connection:
        return connection.execute(sql, parameters).fetchall()


def _count(path: Path, table: str) -> int:
    return _query(path, f"SELECT COUNT(*) FROM {table}")[0][0]  # noqa: S608


def _dump(path: Path) -> dict[str, list[tuple]]:
    tables = [row[0] for row in _query(path, "SELECT name FROM sqlite_master WHERE type='table'")]
    return {
        "schema": sorted(_query(path, "SELECT type, name, sql FROM sqlite_master")),
        **{table: sorted(_query(path, f"SELECT * FROM {table}")) for table in tables},  # noqa: S608
    }


def _trade(
    path: Path,
    side: OrderSide,
    contracts: int,
    quote: str | None,
    filled_at: PointInTime | None,
    *,
    contract: FuturesContract = _ES_DEC,
    strategy: StrategyIdentity = _ALPHA,
) -> FuturesPaperOrder:
    """Persist one order, and its fill unless ``quote`` is None, through the real stores."""
    order = FuturesPaperOrder(
        PaperOrderIdentity(f"order-{_count(path, 'futures_paper_orders'):03d}"),
        FuturesExecutionIntent(
            _PORTFOLIO, contract, side, FuturesContractCount(contracts), strategy, _DECIDED
        ),
    )
    assert SQLiteFuturesPaperOrderStore(path).store((order,)) == 1
    if quote is not None:
        assert SQLiteFuturesPaperFillStore(path).store((_fill_of(order, quote, filled_at),)) == 1
    return order


def _fill_of(
    order: FuturesPaperOrder, quote: str, filled_at: PointInTime, fill_id: str | None = None
) -> FuturesPaperFill:
    return FuturesPaperFill(
        PaperFillIdentity(fill_id or f"fill-{order.identity.identity}"),
        order.identity,
        order.intent,
        order.intent.contracts,
        _quote(quote),
        filled_at,
    )


def _trades(
    path: Path, *trades: tuple[OrderSide, int, str], contract: FuturesContract = _ES_DEC
) -> None:
    """One filled trade per day from day 2, in order."""
    for offset, (side, contracts, quote) in enumerate(trades):
        _trade(path, side, contracts, quote, _day(2 + offset), contract=contract)


def _bar(
    at: PointInTime,
    close: str,
    contract: FuturesContract = _ES_DEC,
    *,
    open_: str | None = None,
    high: str | None = None,
    low: str | None = None,
) -> FuturesOHLCVBar:
    closing = Decimal(close)
    opening = Decimal(open_) if open_ is not None else closing + 3
    return FuturesOHLCVBar(
        contract=contract,
        point_in_time=at,
        timeframe=_DAILY,
        open=QuoteValue(opening),
        high=QuoteValue(Decimal(high) if high is not None else max(opening, closing) + 10),
        low=QuoteValue(Decimal(low) if low is not None else min(opening, closing) - 10),
        close=QuoteValue(closing),
        volume=Quantity(Decimal("1000")),
    )


def _store_bars(path: Path, *bars: FuturesOHLCVBar) -> None:
    assert SQLiteFuturesHistoricalMarketDataStore(path).store(bars) == len(bars)


def _valuation(
    path: Path,
    through: PointInTime = _T,
    strategy: StrategyIdentity = _ALPHA,
    **overrides: object,
) -> FuturesPaperTradingValuation:
    ports: dict[str, object] = {
        "order_repository": SQLiteFuturesPaperOrderRepository(path),
        "fill_repository": SQLiteFuturesPaperFillRepository(path),
        "market_repository": SQLiteFuturesHistoricalMarketDataRepository(path),
        "economics_repository": SQLiteFuturesProductEconomicsRepository(path),
        **overrides,
    }
    return BuildFuturesPaperTradingValuationUseCase(**ports).execute(_PORTFOLIO, strategy, through)


def _row(valuation: FuturesPaperTradingValuation, contract: FuturesContract = _ES_DEC):
    (row,) = [row for row in valuation.contracts if row.contract == contract]
    return row


def _is_flat(row: FuturesContractPnl, currency: Currency = _USD) -> bool:
    return (
        row.position is None
        and row.mark_quote is None
        and row.mark_instant is None
        and row.unrealized_pnl == Money(Decimal(0), currency)
    )


def _is_unmarked(row: FuturesContractPnl) -> bool:
    return row.position is not None and (
        row.mark_quote,
        row.mark_instant,
        row.unrealized_pnl,
    ) == (None, None, None)


# ---------------------------------------------------------------------------
# Schema and economics persistence
# ---------------------------------------------------------------------------

_REQUIRED_TABLES = {
    "futures_ohlcv",
    "futures_forward_research_records",
    "futures_paper_orders",
    "futures_paper_fills",
    "futures_product_economics",
}


def test_all_five_tables_coexist_without_changing_each_other(tmp_path: Path) -> None:
    path = tmp_path / "schema.sqlite3"
    with sqlite3.connect(path) as connection:
        initialize_futures_market_data_schema(connection)
        initialize_futures_forward_research_record_schema(connection)
        initialize_futures_paper_trading_schema(connection)
    before = set(_query(path, "SELECT type, name, sql FROM sqlite_master"))

    _initialize(path)
    _initialize(path)
    after = set(_query(path, "SELECT type, name, sql FROM sqlite_master"))

    assert before <= after
    assert {name for _, name, _ in after - before} == {
        "futures_product_economics",
        "sqlite_autoindex_futures_product_economics_1",
    }
    assert {name for kind, name, _ in after if kind == "table"} == _REQUIRED_TABLES


def test_pnl_stays_derived_and_valuation_writes_nothing(tmp_path: Path) -> None:
    path = _database(tmp_path)
    _trades(path, (BUY, 2, "100"), (SELL, 1, "110"))
    _trade(path, BUY, 1, "5000", _day(5), contract=_FESX_DEC)
    _store_bars(path, _bar(_day(9), "120"), _bar(_day(9), "5010", _FESX_DEC))
    before = _dump(path)

    _valuation(path)
    _valuation(path, _day(30))

    assert _dump(path) == before
    tables = {row[0] for row in _query(path, "SELECT name FROM sqlite_master WHERE type='table'")}
    assert tables == _REQUIRED_TABLES
    for forbidden in ("pnl", "valuation", "mark", "position", "portfolio", "total", "realized"):
        assert not [table for table in tables if forbidden in table]


def test_economics_persist_as_exact_text_and_survive_restart(tmp_path: Path) -> None:
    precise = FuturesProductEconomics(
        FuturesProductReference(Symbol("HP"), ExchangeCode("CME")),
        FuturesPointValue(Decimal("12.345678901234567890123456789012345678901234567890123"), _USD),
    )
    stored = (_ES_ECONOMICS, _FESX_ECONOMICS, _ES_EUREX_ECONOMICS, precise)
    path = _database(tmp_path, economics=stored)

    for economics in stored:
        assert SQLiteFuturesProductEconomicsRepository(path).get_economics(economics.reference) == (
            economics
        )
    assert SQLiteFuturesProductEconomicsRepository(path).get_economics(_ES_EUREX) != _ES_ECONOMICS
    assert sorted(_query(path, "SELECT * FROM futures_product_economics")) == [
        ("ES", "CME", "50", "USD"),
        ("ES", "EUREX", "7", "EUR"),
        ("FESX", "EUREX", "25", "EUR"),
        ("HP", "CME", "12.345678901234567890123456789012345678901234567890123", "USD"),
    ]
    assert _query(
        path,
        "SELECT DISTINCT typeof(point_value_amount), typeof(settlement_currency) "
        "FROM futures_product_economics",
    ) == [("text", "text")]


def test_economics_are_insert_only(tmp_path: Path) -> None:
    path = _database(tmp_path)
    rows = sorted(_query(path, "SELECT * FROM futures_product_economics"))

    assert SQLiteFuturesProductEconomicsStore(path).store((_ES_ECONOMICS,)) == 1
    for changed in (
        FuturesProductEconomics(_ES, FuturesPointValue(Decimal("5"), _USD)),
        FuturesProductEconomics(_ES, FuturesPointValue(Decimal("50"), _EUR)),
    ):
        with pytest.raises(FuturesProductEconomicsConflictError):
            SQLiteFuturesProductEconomicsStore(path).store((changed,))

    assert sorted(_query(path, "SELECT * FROM futures_product_economics")) == rows


# ---------------------------------------------------------------------------
# Gross simulated realized P&L
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("trades", "realized", "position"),
    [
        ([(BUY, 2, "100"), (SELL, 1, "110")], "500", (1, "100")),
        ([(BUY, 2, "100"), (SELL, 1, "90")], "-500", (1, "100")),
        ([(SELL, 2, "100"), (BUY, 1, "90")], "500", (-1, "100")),
        ([(SELL, 2, "100"), (BUY, 1, "110")], "-500", (-1, "100")),
        ([(BUY, 1, "-20"), (SELL, 1, "-10")], "500", None),
        ([(SELL, 1, "-20"), (BUY, 1, "-30")], "500", None),
        ([(BUY, 1, "-10"), (SELL, 1, "5")], "750", None),
        ([(SELL, 1, "-10"), (BUY, 1, "5")], "-750", None),
        ([(BUY, 1, "10"), (SELL, 1, "0")], "-500", None),
        ([(SELL, 1, "10"), (BUY, 1, "0")], "500", None),
        ([(BUY, 2, "100"), (SELL, 2, "110")], "1000", None),
        ([(BUY, 3, "100"), (SELL, 1, "110"), (SELL, 1, "90"), (SELL, 1, "120")], "1000", None),
    ],
    ids=[
        "long-profit",
        "long-loss",
        "short-profit",
        "short-loss",
        "negative-long-profit",
        "negative-short-profit",
        "cross-long-profit",
        "cross-short-loss",
        "long-to-zero-loss",
        "short-to-zero-profit",
        "full-close",
        "multiple-reductions",
    ],
)
def test_realized_pnl_from_real_fills(tmp_path: Path, trades, realized, position) -> None:
    path = _database(tmp_path)
    _trades(path, *trades)

    valuation = _valuation(path)
    row = _row(valuation)

    assert row.realized_pnl == _usd(realized)
    if position is None:
        assert valuation.portfolio.positions == ()
        assert _is_flat(row)
    else:
        assert row.position == _position(*position)
        assert valuation.portfolio.positions == (row.position,)


def test_a_long_to_short_reversal_realizes_only_the_closed_long(tmp_path: Path) -> None:
    path = _database(tmp_path)
    _trades(path, (BUY, 2, "100"), (SELL, 3, "110"))
    _store_bars(path, _bar(_day(9), "90"))

    row = _row(_valuation(path))

    assert row.realized_pnl == _usd("1000")
    assert row.position == _position(-1, "110")
    assert (row.mark_quote, row.unrealized_pnl) == (_quote("90"), _usd("1000"))
    assert not hasattr(row, "total_pnl")


def test_a_short_to_long_reversal_marks_only_the_new_long(tmp_path: Path) -> None:
    path = _database(tmp_path)
    _trades(path, (SELL, 2, "100"), (BUY, 3, "90"))
    _store_bars(path, _bar(_day(9), "95"))

    row = _row(_valuation(path))

    assert row.realized_pnl == _usd("1000")
    assert row.position == _position(1, "90")
    assert row.unrealized_pnl == _usd("250")


def test_a_partial_reduction_marks_the_remaining_size(tmp_path: Path) -> None:
    path = _database(tmp_path)
    _trades(path, (BUY, 3, "100"), (SELL, 1, "110"))
    _store_bars(path, _bar(_day(9), "120"))

    row = _row(_valuation(path))

    assert row.realized_pnl == _usd("500")
    assert row.position == _position(2, "100")
    assert row.unrealized_pnl == _usd("2000")


def test_the_rounded_weighted_average_is_the_only_basis(tmp_path: Path) -> None:
    path = _database(tmp_path)
    _trades(path, (BUY, 2, "100"), (BUY, 1, "102"))
    assert _row(_valuation(path, _day(3))).position == _position(3, _BASIS)

    _trade(path, SELL, 1, "110", _day(4))
    _store_bars(path, _bar(_day(9), "105"))
    row = _row(_valuation(path))

    assert row.realized_pnl == _usd("466.666666666666666666666665")
    assert row.realized_pnl not in (_usd("500"), _usd("466.6666666666666666666666667"))
    assert row.position == _position(2, _BASIS)
    basis = Decimal(_BASIS)
    expected = _CONTEXT.multiply(
        _CONTEXT.multiply(_CONTEXT.subtract(Decimal("105"), basis), 2), Decimal("50")
    )
    assert row.unrealized_pnl == Money(expected, _USD)


def test_point_value_is_applied_once_to_accumulated_points(tmp_path: Path) -> None:
    point_value = Decimal("3.333333333333333333333333337")
    first, second = "0.1428571428571428571428571429", "3.666666666666666666666666667"
    path = _database(
        tmp_path, economics=(FuturesProductEconomics(_ES, FuturesPointValue(point_value, _USD)),)
    )
    _trades(path, (BUY, 1, "0"), (SELL, 1, first), (BUY, 1, "0"), (SELL, 1, second))

    once = _CONTEXT.multiply(_CONTEXT.add(Decimal(first), Decimal(second)), point_value)
    per_close = _CONTEXT.add(
        _CONTEXT.multiply(Decimal(first), point_value),
        _CONTEXT.multiply(Decimal(second), point_value),
    )

    assert once != per_close
    assert _row(_valuation(path)).realized_pnl == Money(once, _USD)


def test_a_closing_fill_after_the_cutoff_changes_nothing_until_visible(tmp_path: Path) -> None:
    path = _database(tmp_path)
    _trade(path, BUY, 2, "100", _day(3))
    _trade(path, SELL, 2, "110", _day(25))
    _store_bars(path, _bar(_day(9), "105"))

    early = _valuation(path)
    late = _valuation(path, _day(26))

    assert _row(early).realized_pnl == _usd("0")
    assert _row(early).position == _position(2, "100")
    assert _row(early).unrealized_pnl == _usd("500")
    assert _row(late).realized_pnl == _usd("1000") and _is_flat(_row(late))
    assert _valuation(path) == early


def test_realized_and_unrealized_components_match_the_valuation(tmp_path: Path) -> None:
    path = _database(tmp_path)
    _trades(path, (BUY, 3, "100"), (SELL, 1, "110"))
    _trade(path, BUY, 2, "5000", _day(6), contract=_FESX_DEC)
    _store_bars(path, _bar(_day(9), "120"), _bar(_day(9), "5010", _FESX_DEC))
    fills = SQLiteFuturesPaperFillRepository(path).get_fills(FuturesPaperFillQuery(_PORTFOLIO))
    economics = SQLiteFuturesProductEconomicsRepository(path)

    realized = CalculateFuturesRealizedPnlUseCase().execute(
        _PORTFOLIO,
        _ALPHA,
        fills,
        (economics.get_economics(_ES), economics.get_economics(_FESX)),
        _T,
    )
    portfolio = BuildFuturesPaperPortfolioUseCase().execute(_PORTFOLIO, _ALPHA, fills, _T)
    unrealized = ValueFuturesPaperPortfolioUseCase(
        SQLiteFuturesHistoricalMarketDataRepository(path), economics
    ).execute(portfolio, _T)
    valuation = _valuation(path)

    assert [(r.contract, r.realized_pnl) for r in realized] == [
        (row.contract, row.realized_pnl) for row in valuation.contracts
    ]
    assert [(u.position, u.unrealized_pnl) for u in unrealized] == [
        (row.position, row.unrealized_pnl) for row in valuation.contracts
    ]
    assert valuation.portfolio == portfolio
    assert [row.unrealized_pnl for row in valuation.contracts] == [_usd("2000"), _eur("500")]


# ---------------------------------------------------------------------------
# Gross simulated unrealized P&L and marks
# ---------------------------------------------------------------------------


def test_an_opening_fill_is_marked_at_the_close_not_the_open(tmp_path: Path) -> None:
    path = _database(tmp_path)
    _trade(path, BUY, 1, "100", _day(3))
    _store_bars(path, _bar(_day(9), "110", open_="101", high="120", low="90"))

    row = _row(_valuation(path))

    assert row.realized_pnl == _usd("0")
    assert row.position == _position(1, "100")
    assert (row.mark_quote, row.mark_instant) == (_quote("110"), _day(9))
    assert row.unrealized_pnl == _usd("500")


def test_a_bar_at_the_cutoff_is_visible_and_later_bars_never_leak(tmp_path: Path) -> None:
    path = _database(tmp_path)
    _trade(path, BUY, 1, "100", _day(3))
    _store_bars(path, _bar(_day(9), "104"), _bar(_T, "106"))
    at_t = _valuation(path)

    _store_bars(path, _bar(_day(21), "999"), _bar(_day(22), "-999"))

    assert (_row(at_t).mark_instant, _row(at_t).unrealized_pnl) == (_T, _usd("300"))
    assert _valuation(path) == at_t
    later = _row(_valuation(path, _day(22)))
    assert (later.mark_quote, later.unrealized_pnl) == (_quote("-999"), _usd("-54950"))


def test_a_stale_mark_keeps_its_own_instant(tmp_path: Path) -> None:
    monday, thursday = _day(14), _day(17)
    path = _database(tmp_path)
    _trade(path, BUY, 1, "100", _day(10))
    _store_bars(path, _bar(monday, "103"))

    row = _row(_valuation(path, thursday))

    assert (row.mark_quote, row.mark_instant) == (_quote("103"), monday)
    assert row.unrealized_pnl == _usd("150")


def test_no_visible_mark_is_unavailable_while_realized_is_known(tmp_path: Path) -> None:
    path = _database(tmp_path)
    _trades(path, (BUY, 2, "100"), (SELL, 1, "110"))
    _store_bars(path, _bar(_day(21), "120"))

    valuation = _valuation(path)
    row = _row(valuation)

    assert row.realized_pnl == _usd("500")
    assert row.position == _position(1, "100")
    assert _is_unmarked(row)


def test_semantic_instants_govern_fills_marks_and_the_cutoff(tmp_path: Path) -> None:
    whole, quarter, half = _day(14), _day(14, "21:00:00.25"), _day(14, "21:00:00.5")
    offset_quarter = PointInTime("2026-09-15T02:30:00.25+05:30")
    path = _database(tmp_path)
    _trade(path, BUY, 1, "100", _day(3))
    _trade(path, SELL, 1, "130", half)
    _store_bars(path, _bar(whole, "105"), _bar(quarter, "107"))

    at_quarter = _valuation(path, offset_quarter)
    after_half = _valuation(path, _day(15))

    assert quarter.value < whole.value and half.value < whole.value  # the text trap
    assert offset_quarter == quarter
    assert (_row(at_quarter).mark_instant, _row(at_quarter).mark_quote) == (quarter, _quote("107"))
    assert _row(at_quarter).unrealized_pnl == _usd("350")
    assert _row(after_half).realized_pnl == _usd("1500") and _is_flat(_row(after_half))


# ---------------------------------------------------------------------------
# Several contracts, products and currencies
# ---------------------------------------------------------------------------


def test_flat_and_unavailable_contracts_stay_distinct_in_one_valuation(tmp_path: Path) -> None:
    path = _database(tmp_path)
    _trade(path, BUY, 1, "100", _day(2), contract=_ES_DEC)
    _trade(path, SELL, 1, "104", _day(3), contract=_ES_DEC)
    _trade(path, SELL, 2, "200", _day(4), contract=_ES_MAR)
    _trade(path, BUY, 3, "5000", _day(5), contract=_FESX_DEC)
    _store_bars(path, _bar(_day(9), "5010", _FESX_DEC), _bar(_day(21), "190", _ES_MAR))

    valuation = _valuation(path)
    flat, unmarked, fesx = (_row(valuation, c) for c in (_ES_DEC, _ES_MAR, _FESX_DEC))

    assert _is_flat(flat) and flat.realized_pnl == _usd("200")
    assert _is_unmarked(unmarked) and unmarked.realized_pnl == _usd("0")
    assert unmarked.position == _position(-2, "200", _ES_MAR)
    assert flat.unrealized_pnl != unmarked.unrealized_pnl
    assert (fesx.realized_pnl, fesx.unrealized_pnl) == (_eur("0"), _eur("750"))
    assert [row.contract for row in valuation.contracts] == sorted(
        [_ES_DEC, _ES_MAR, _FESX_DEC], key=lambda contract: contract.natural_key
    )
    assert tuple(r.position for r in valuation.contracts if r.is_open) == (
        valuation.portfolio.positions
    )
    assert (valuation.contract_count, valuation.open_position_count) == (3, 2)
    for name in ("total_pnl", "gross_pnl", "total_realized", "total_unrealized"):
        assert not hasattr(valuation, name)


def test_expiries_share_one_economics_row_but_not_positions_or_marks(tmp_path: Path) -> None:
    path = _database(tmp_path, economics=(_ES_ECONOMICS,))
    _trade(path, BUY, 1, "100", _day(2), contract=_ES_DEC)
    _trade(path, BUY, 1, "100", _day(3), contract=_ES_MAR)
    _store_bars(path, _bar(_day(9), "110", _ES_DEC), _bar(_day(9), "90", _ES_MAR))

    valuation = _valuation(path)

    assert valuation.portfolio.positions == (
        _position(1, "100", _ES_DEC),
        _position(1, "100", _ES_MAR),
    )
    assert [(row.contract, row.unrealized_pnl) for row in valuation.contracts] == [
        (_ES_DEC, _usd("500")),
        (_ES_MAR, _usd("-500")),
    ]
    assert _query(path, "SELECT * FROM futures_product_economics") == [("ES", "CME", "50", "USD")]


def test_missing_economics_for_a_visible_product_fails_the_valuation(tmp_path: Path) -> None:
    path = _database(tmp_path, economics=(_ES_ECONOMICS,))
    _trade(path, BUY, 1, "100", _day(2))
    _trade(path, BUY, 1, "5000", _day(3), contract=_FESX_DEC)

    with pytest.raises(FuturesProductEconomicsNotFoundError, match="FESX@EUREX"):
        _valuation(path)


def test_a_future_only_product_needs_no_economics_until_visible(tmp_path: Path) -> None:
    path = _database(tmp_path, economics=(_ES_ECONOMICS,))
    _trade(path, BUY, 1, "100", _day(2))
    _trade(path, BUY, 1, "5000", _day(25), contract=_FESX_DEC)

    valuation = _valuation(path)

    assert [row.contract for row in valuation.contracts] == [_ES_DEC]
    assert valuation.portfolio.get_position(_FESX_DEC) is None
    with pytest.raises(FuturesProductEconomicsNotFoundError):
        _valuation(path, _day(26))


# ---------------------------------------------------------------------------
# Determinism and restart
# ---------------------------------------------------------------------------


def _precise_database(tmp_path: Path) -> Path:
    economics = FuturesProductEconomics(
        _ES, FuturesPointValue(Decimal("12.34567890123456789012345678"), _USD)
    )
    path = _database(tmp_path, "precise", economics=(economics,))
    _trades(
        path,
        (BUY, 3, "100.1234567890123456789012345"),
        (BUY, 4, "101.9876543210987654321098765"),
        (SELL, 2, "102.5555555555555555555555555"),
    )
    _store_bars(path, _bar(_day(9), "99.87654321098765432109876543"))
    return path


@pytest.mark.parametrize("precision", [6, 28, 50])
@pytest.mark.parametrize("rounding", [ROUND_DOWN, ROUND_CEILING, ROUND_HALF_UP])
def test_the_valuation_ignores_the_callers_decimal_context(
    tmp_path: Path, precision: int, rounding: str
) -> None:
    path = _precise_database(tmp_path)
    baseline = _valuation(path)

    with localcontext() as ambient:
        ambient.prec = precision
        ambient.rounding = rounding
        assert _valuation(path) == baseline
        assert (ambient.prec, ambient.rounding) == (precision, rounding)
    assert _row(baseline).realized_pnl != _usd("0")
    assert _row(baseline).unrealized_pnl is not None


def test_a_copied_database_rebuilds_an_equal_valuation(tmp_path: Path) -> None:
    path = _precise_database(tmp_path)
    _trade(path, SELL, 2, "5000", _day(6), contract=_FESX_DEC)
    SQLiteFuturesProductEconomicsStore(path).store((_FESX_ECONOMICS,))
    original = _valuation(path)

    copy = tmp_path / "restarted.sqlite3"
    shutil.copyfile(path, copy)

    assert _valuation(copy) == original


# ---------------------------------------------------------------------------
# Frozen facts
# ---------------------------------------------------------------------------


def test_changed_economics_conflict_and_the_valuation_is_unchanged(tmp_path: Path) -> None:
    path = _database(tmp_path)
    _trades(path, (BUY, 2, "100"), (SELL, 1, "110"))
    before = _valuation(path)

    with pytest.raises(FuturesProductEconomicsConflictError):
        SQLiteFuturesProductEconomicsStore(path).store(
            (FuturesProductEconomics(_ES, FuturesPointValue(Decimal("20"), _USD)),)
        )

    assert _valuation(path) == before
    assert _row(before).realized_pnl == _usd("500")


def test_fills_are_frozen_execution_facts(tmp_path: Path) -> None:
    path = _database(tmp_path)
    order = _trade(path, BUY, 1, "100", _day(2))
    _trade(path, SELL, 1, "110", _day(3))
    before = _valuation(path)
    fills = SQLiteFuturesPaperFillStore(path)

    with pytest.raises(FuturesPaperFillConflictError):
        fills.store((_fill_of(order, "50", _day(2)),))
    with pytest.raises(FuturesPaperFillConflictError):
        fills.store((_fill_of(order, "50", _day(2), fill_id="second-fill"),))
    orphan = FuturesPaperOrder(PaperOrderIdentity("never-stored"), order.intent)
    with pytest.raises(FuturesPaperFillConflictError):
        fills.store((_fill_of(orphan, "1", _day(4)),))

    assert _valuation(path) == before
    assert _row(before).realized_pnl == _usd("500")


def test_a_pending_order_has_no_pnl(tmp_path: Path) -> None:
    path = _database(tmp_path)
    _trade(path, BUY, 1, None, None, contract=_ES_MAR)
    assert _valuation(path).contracts == ()

    _trade(path, BUY, 1, "100", _day(2))
    valuation = _valuation(path)

    assert [row.contract for row in valuation.contracts] == [_ES_DEC]
    assert _count(path, "futures_paper_orders") == 2


class _CountingEconomics(SQLiteFuturesProductEconomicsRepository):
    def __init__(self, path: Path) -> None:
        super().__init__(path)
        self.calls: list[FuturesProductReference] = []

    def get_economics(self, reference):
        self.calls.append(reference)
        return super().get_economics(reference)


class _CountingMarket(SQLiteFuturesHistoricalMarketDataRepository):
    def __init__(self, path: Path) -> None:
        super().__init__(path)
        self.queries: list = []

    def get_bars(self, query):
        self.queries.append(query)
        return super().get_bars(query)


@pytest.mark.parametrize("pending", [False, True], ids=["empty", "pending-only"])
def test_a_portfolio_without_fills_values_to_nothing(tmp_path: Path, pending: bool) -> None:
    path = _database(tmp_path)
    if pending:
        _trade(path, SELL, 2, None, None)
    economics, market = _CountingEconomics(path), _CountingMarket(path)

    valuation = _valuation(path, economics_repository=economics, market_repository=market)

    assert valuation.contracts == () and valuation.portfolio.positions == ()
    assert (economics.calls, market.queries) == ([], [])


def test_another_strategy_cannot_value_the_portfolio(tmp_path: Path) -> None:
    path = _database(tmp_path)
    _trade(path, BUY, 1, "100", _day(2))

    with pytest.raises(ValueError, match="one paper portfolio belongs to one strategy"):
        _valuation(path, strategy=_BETA)

    _trade(path, BUY, 1, None, None, contract=_ES_MAR, strategy=_BETA)
    with pytest.raises(ValueError, match="one paper portfolio belongs to one strategy"):
        _valuation(path)


# ---------------------------------------------------------------------------
# Cross-epic: P&L follows the fill the 9.9 pipeline persisted
# ---------------------------------------------------------------------------


def _session_dates(count: int) -> list[date]:
    dates: list[date] = []
    day = date(2026, 6, 1)
    while len(dates) < count:
        if day.weekday() < 5 and day != date(2026, 6, 19):
            dates.append(day)
        day += timedelta(days=1)
    return dates


_SESSIONS = _session_dates(30)


def _session(number: int) -> PointInTime:
    return PointInTime(f"{_SESSIONS[number - 1].isoformat()}T21:00:00Z")


def test_a_back_filled_next_bar_never_repairs_the_filled_pnl(tmp_path: Path) -> None:
    path = _database(tmp_path, economics=(_ES_ECONOMICS,))
    closes = ["7600"] * 15 + [str(7601 + index) for index in range(10)]
    _store_bars(
        path,
        *(_bar(_session(n), close) for n, close in enumerate(closes, start=1)),
        _bar(_session(27), "7612", open_="7777", high="7800", low="7600"),
    )
    record = FreezeFuturesForwardResearchDecisionUseCase(
        SQLiteFuturesHistoricalMarketDataRepository(path),
        FuturesAssetAnalysisGenerator(),
        SQLiteFuturesForwardResearchRecordStore(path),
    ).execute(_ES_DEC, _DAILY, Strategy(_ALPHA), _session(25))

    def decide():
        return RunFuturesPaperTradingDecisionUseCase(
            forward_repository=SQLiteFuturesForwardResearchRecordRepository(path),
            market_repository=SQLiteFuturesHistoricalMarketDataRepository(path),
            order_store=SQLiteFuturesPaperOrderStore(path),
            order_repository=SQLiteFuturesPaperOrderRepository(path),
            fill_store=SQLiteFuturesPaperFillStore(path),
            fill_repository=SQLiteFuturesPaperFillRepository(path),
        ).execute(record, _PORTFOLIO, FuturesContractCount(1), _session(27))

    original = decide().fill
    assert original.fill_quote == _quote("7777")
    before = _valuation(path, _session(27))

    _store_bars(path, _bar(_session(26), "7611", open_="7650", high="7700", low="7600"))
    with pytest.raises(FuturesPaperFillConflictError):
        decide()

    row = _row(_valuation(path, _session(27)))
    assert _valuation(path, _session(27)) == before
    assert row.position == _position(1, "7777")
    assert (row.realized_pnl, row.mark_quote, row.unrealized_pnl) == (
        _usd("0"),
        _quote("7612"),
        _usd("-8250"),
    )
