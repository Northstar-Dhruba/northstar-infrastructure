"""Tests for the SQLite futures paper order and fill stores and repositories.

Every test runs against a real temporary SQLite file. Raw SQL is used only to
inspect tables or to plant corrupt rows the adapters must refuse; every
legitimate write goes through a store.
"""

from __future__ import annotations

import ast
import sqlite3
from decimal import Decimal
from pathlib import Path

import pytest
from northstar_application.ports import (
    FuturesPaperFillConflictError,
    FuturesPaperFillQuery,
    FuturesPaperFillRepository,
    FuturesPaperFillStore,
    FuturesPaperOrderConflictError,
    FuturesPaperOrderQuery,
    FuturesPaperOrderRepository,
    FuturesPaperOrderStore,
)
from northstar_core.derivatives import ExpirationDate, QuoteValue
from northstar_core.foundation.value_objects import ExchangeCode, PointInTime, Symbol
from northstar_core.futures import FuturesContract, FuturesProductReference
from northstar_core.paper_trading import (
    FuturesContractCount,
    FuturesExecutionIntent,
    FuturesPaperFill,
    FuturesPaperOrder,
    OrderSide,
    PaperFillIdentity,
    PaperOrderIdentity,
    PaperPortfolioIdentity,
)
from northstar_core.strategy import StrategyIdentity

import northstar_infrastructure.persistence as persistence
import northstar_infrastructure.persistence.sqlite_futures_paper_trading as module
from northstar_infrastructure.market_data.sqlite_futures_schema import (
    initialize_futures_market_data_schema,
)
from northstar_infrastructure.persistence import (
    FuturesPaperTradingStorageError,
    SQLiteFuturesPaperFillRepository,
    SQLiteFuturesPaperFillStore,
    SQLiteFuturesPaperOrderRepository,
    SQLiteFuturesPaperOrderStore,
    initialize_futures_forward_research_record_schema,
    initialize_futures_paper_trading_schema,
    initialize_paper_fill_schema,
)


def _contract(product: str = "ES", exchange: str = "CME", expiry: str = "2026-12-18"):
    return FuturesContract(
        FuturesProductReference(Symbol(product), ExchangeCode(exchange)), ExpirationDate(expiry)
    )


_ES_DEC = _contract()
_ES_MAR = _contract(expiry="2027-03-19")
_MES_DEC = _contract("MES")
_PORTFOLIO = "futures-paper-1"
_OTHER = "futures-paper-2"
_D = "2026-09-15T21:00:00Z"
_NEXT = "2026-09-16T21:00:00Z"
_BIG = 9_223_372_036_854_775_807 * 1000 + 7

BUY = OrderSide.BUY
SELL = OrderSide.SELL


def _order(
    order_id: str = "order-1",
    *,
    portfolio: str = _PORTFOLIO,
    contract: FuturesContract = _ES_DEC,
    side: OrderSide = BUY,
    contracts: int = 2,
    strategy: str = "futures-forward",
    decided_at: str = _D,
) -> FuturesPaperOrder:
    return FuturesPaperOrder(
        identity=PaperOrderIdentity(order_id),
        intent=FuturesExecutionIntent(
            portfolio_identity=PaperPortfolioIdentity(portfolio),
            contract=contract,
            side=side,
            contracts=FuturesContractCount(contracts),
            strategy_identity=StrategyIdentity(strategy),
            decided_at=PointInTime(decided_at),
        ),
    )


def _fill(
    order: FuturesPaperOrder | None = None,
    *,
    fill_id: str | None = None,
    quote: str = "7663.25",
    filled_at: str = _NEXT,
    intent: FuturesExecutionIntent | None = None,
) -> FuturesPaperFill:
    order = order if order is not None else _order()
    intent = intent if intent is not None else order.intent
    return FuturesPaperFill(
        identity=PaperFillIdentity(fill_id or f"fill-{order.identity.identity}"),
        order_identity=order.identity,
        intent=intent,
        contracts=intent.contracts,
        fill_quote=QuoteValue(Decimal(quote)),
        filled_at=PointInTime(filled_at),
    )


@pytest.fixture
def database(tmp_path: Path) -> Path:
    return tmp_path / "futures_paper.sqlite3"


def _orders(database: Path, portfolio: str = _PORTFOLIO) -> tuple[FuturesPaperOrder, ...]:
    return SQLiteFuturesPaperOrderRepository(database).get_orders(
        FuturesPaperOrderQuery(PaperPortfolioIdentity(portfolio))
    )


def _fills(database: Path, portfolio: str = _PORTFOLIO) -> tuple[FuturesPaperFill, ...]:
    return SQLiteFuturesPaperFillRepository(database).get_fills(
        FuturesPaperFillQuery(PaperPortfolioIdentity(portfolio))
    )


def _store_orders(database: Path, *orders: FuturesPaperOrder) -> int:
    return SQLiteFuturesPaperOrderStore(database).store(orders)


def _store_fills(database: Path, *fills: FuturesPaperFill) -> int:
    return SQLiteFuturesPaperFillStore(database).store(fills)


def _query(database: Path, sql: str, parameters: tuple = ()) -> list[tuple]:
    with sqlite3.connect(database) as connection:
        return connection.execute(sql, parameters).fetchall()


def _execute(database: Path, sql: str, parameters: tuple = ()) -> None:
    with sqlite3.connect(database) as connection:
        connection.execute(sql, parameters)


def _count(database: Path, table: str) -> int:
    return _query(database, f"SELECT COUNT(*) FROM {table}")[0][0]  # noqa: S608


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


def _initialize(database: Path, *initializers) -> None:
    with sqlite3.connect(database) as connection:
        for initializer in initializers:
            initializer(connection)


def test_a_fresh_database_gets_exactly_the_two_paper_tables(database: Path) -> None:
    _initialize(database, initialize_futures_paper_trading_schema)

    tables = {
        row[0] for row in _query(database, "SELECT name FROM sqlite_master WHERE type='table'")
    }

    assert tables == {"futures_paper_orders", "futures_paper_fills"}


def test_the_order_table_columns_are_exactly_the_order_value(database: Path) -> None:
    _initialize(database, initialize_futures_paper_trading_schema)

    columns = _query(database, "PRAGMA table_info(futures_paper_orders)")

    assert [(name, kind, bool(not_null), pk) for _, name, kind, not_null, _, pk in columns] == [
        ("order_identity", "TEXT", True, 1),
        ("portfolio_identity", "TEXT", True, 0),
        ("product_code", "TEXT", True, 0),
        ("exchange_code", "TEXT", True, 0),
        ("expiration_date", "TEXT", True, 0),
        ("side", "TEXT", True, 0),
        ("contracts", "TEXT", True, 0),
        ("strategy_identity", "TEXT", True, 0),
        ("decided_at", "TEXT", True, 0),
    ]


def test_the_fill_table_holds_only_what_the_parent_order_cannot_supply(database: Path) -> None:
    _initialize(database, initialize_futures_paper_trading_schema)

    columns = _query(database, "PRAGMA table_info(futures_paper_fills)")
    unique = [
        _query(database, f"PRAGMA index_info('{name}')")[0][2]
        for _, name, is_unique, *_ in _query(database, "PRAGMA index_list(futures_paper_fills)")
        if is_unique
    ]

    assert [(name, kind, bool(not_null), pk) for _, name, kind, not_null, _, pk in columns] == [
        ("fill_identity", "TEXT", True, 1),
        ("order_identity", "TEXT", True, 0),
        ("fill_quote", "TEXT", True, 0),
        ("filled_at", "TEXT", True, 0),
    ]
    assert "order_identity" in unique


def test_existing_tables_are_left_untouched(database: Path) -> None:
    _initialize(
        database,
        initialize_futures_market_data_schema,
        initialize_futures_forward_research_record_schema,
        initialize_paper_fill_schema,
    )
    before = set(_query(database, "SELECT type, name, sql FROM sqlite_master"))

    _initialize(database, initialize_futures_paper_trading_schema)
    after = set(_query(database, "SELECT type, name, sql FROM sqlite_master"))

    assert before <= after
    assert {name for _, name, _ in after - before} == {
        "futures_paper_orders",
        "futures_paper_fills",
        "futures_paper_orders_portfolio_identity",
        "sqlite_autoindex_futures_paper_orders_1",
        "sqlite_autoindex_futures_paper_fills_1",
        "sqlite_autoindex_futures_paper_fills_2",
    }


def test_initialization_is_repeatable(database: Path) -> None:
    _initialize(database, initialize_futures_paper_trading_schema)
    _store_orders(database, _order())

    _initialize(database, initialize_futures_paper_trading_schema)

    assert _orders(database) == (_order(),)


def test_normal_use_creates_no_status_position_portfolio_or_pnl_table(database: Path) -> None:
    order = _order()
    _store_orders(database, order, _order("order-2", decided_at=_NEXT))
    _store_fills(database, _fill(order))
    _orders(database)
    _fills(database)

    tables = {
        row[0] for row in _query(database, "SELECT name FROM sqlite_master WHERE type='table'")
    }
    columns = {
        row[1]
        for table in tables
        for row in _query(database, f"PRAGMA table_info({table})")  # noqa: S608
    }

    assert tables == {"futures_paper_orders", "futures_paper_fills"}
    for forbidden in ("status", "state", "position", "pnl", "timeframe", "currency", "margin"):
        assert not [column for column in columns if forbidden in column]


# ---------------------------------------------------------------------------
# Orders: round trip and serialization
# ---------------------------------------------------------------------------


def test_an_order_round_trips_exactly(database: Path) -> None:
    order = _order()

    assert _store_orders(database, order) == 1
    assert _orders(database) == (order,)
    assert type(_orders(database)[0].identity) is PaperOrderIdentity


def test_an_order_is_stored_as_canonical_text(database: Path) -> None:
    _store_orders(database, _order(decided_at="2026-09-16T02:30:00+05:30"))

    assert _query(database, "SELECT * FROM futures_paper_orders") == [
        (
            "order-1",
            "futures-paper-1",
            "ES",
            "CME",
            "2026-12-18",
            "BUY",
            "2",
            "futures-forward",
            "2026-09-15T21:00:00Z",
        )
    ]
    assert _query(database, "SELECT typeof(contracts) FROM futures_paper_orders") == [("text",)]


def test_a_count_beyond_sqlite_integer_range_round_trips(database: Path) -> None:
    order = _order(contracts=_BIG)

    _store_orders(database, order)

    assert _BIG > 9_223_372_036_854_775_807
    assert _orders(database) == (order,)
    assert _query(database, "SELECT contracts, typeof(contracts) FROM futures_paper_orders") == [
        (str(_BIG), "text")
    ]


@pytest.mark.parametrize(
    "order",
    [
        pytest.param(_order(contract=_ES_DEC), id="es-dec"),
        pytest.param(_order(contract=_ES_MAR), id="es-mar"),
        pytest.param(_order(contract=_MES_DEC), id="mes-dec"),
        pytest.param(_order(contract=_contract(exchange="CBOT")), id="cbot"),
        pytest.param(_order(side=SELL, contracts=7), id="sell"),
        pytest.param(_order(strategy="another-strategy"), id="strategy"),
        pytest.param(_order(decided_at="2026-09-15T21:00:00.123456Z"), id="sub-second"),
    ],
)
def test_every_order_field_round_trips(database: Path, order: FuturesPaperOrder) -> None:
    _store_orders(database, order)

    assert _orders(database) == (order,)


def test_contracts_expiries_and_strategies_stay_independent(database: Path) -> None:
    orders = (
        _order("o-1", contract=_ES_DEC),
        _order("o-2", contract=_ES_MAR, side=SELL),
        _order("o-3", contract=_MES_DEC, strategy="other"),
    )

    _store_orders(database, *orders)

    assert _orders(database) == orders


# ---------------------------------------------------------------------------
# Orders: idempotency and conflicts
# ---------------------------------------------------------------------------


def test_an_equal_retry_is_idempotent(database: Path) -> None:
    order = _order()
    _store_orders(database, order)

    assert _store_orders(database, order) == 1
    assert _count(database, "futures_paper_orders") == 1


def test_a_separately_rebuilt_equal_retry_is_idempotent(database: Path) -> None:
    _store_orders(database, _order())

    assert _store_orders(database, _order(decided_at="2026-09-16T02:30:00+05:30")) == 1
    assert _count(database, "futures_paper_orders") == 1


@pytest.mark.parametrize(
    "overrides",
    [
        {"side": SELL},
        {"contracts": 3},
        {"contract": _MES_DEC},
        {"contract": _ES_MAR},
        {"strategy": "other"},
        {"portfolio": _OTHER},
        {"decided_at": _NEXT},
    ],
    ids=["side", "contracts", "product", "expiry", "strategy", "portfolio", "decided-at"],
)
def test_a_different_order_under_one_identity_conflicts(database: Path, overrides: dict) -> None:
    original = _order()
    _store_orders(database, original)

    with pytest.raises(FuturesPaperOrderConflictError, match="already stored"):
        _store_orders(database, _order(**overrides))

    assert _orders(database) == (original,)


def test_an_empty_batch_returns_zero(database: Path) -> None:
    assert _store_orders(database) == 0


def test_a_batch_repeating_one_identity_is_rejected_even_when_equal(database: Path) -> None:
    with pytest.raises(FuturesPaperOrderConflictError, match="two orders sharing"):
        _store_orders(database, _order(), _order())

    assert _orders(database) == ()


def test_one_conflict_leaves_the_whole_order_batch_uncommitted(database: Path) -> None:
    _store_orders(database, _order("o-1"))

    with pytest.raises(FuturesPaperOrderConflictError):
        _store_orders(database, _order("o-2"), _order("o-1", contracts=9))

    assert [order.identity.identity for order in _orders(database)] == ["o-1"]


def test_idempotent_and_new_orders_both_count(database: Path) -> None:
    _store_orders(database, _order("o-1"))

    assert _store_orders(database, _order("o-1"), _order("o-2")) == 2


@pytest.mark.parametrize(
    "orders",
    [[_order()], (_order(), "order"), (_fill(),)],
    ids=["list", "foreign-member", "fill"],
)
def test_foreign_order_batches_are_rejected_before_any_write(database: Path, orders) -> None:
    with pytest.raises(TypeError):
        SQLiteFuturesPaperOrderStore(database).store(orders)

    assert _orders(database) == ()


# ---------------------------------------------------------------------------
# Orders: repository
# ---------------------------------------------------------------------------


def test_orders_are_isolated_by_portfolio(database: Path) -> None:
    mine = _order("o-1")
    theirs = _order("o-2", portfolio=_OTHER)
    _store_orders(database, mine, theirs)

    assert _orders(database) == (mine,)
    assert _orders(database, _OTHER) == (theirs,)
    assert _orders(database, "nobody") == ()


def test_orders_are_ordered_semantically_then_by_identity(database: Path) -> None:
    whole_b = _order("b", decided_at="2026-09-15T21:00:00Z")
    whole_a = _order("a", decided_at="2026-09-15T21:00:00Z")
    half = _order("0", decided_at="2026-09-15T21:00:00.5Z")
    next_day = _order("z-earliest-id", decided_at="2026-09-16T16:00:00-05:00")
    _store_orders(database, next_day, half, whole_b, whole_a)

    by_text = [
        row[0]
        for row in _query(
            database, "SELECT decided_at FROM futures_paper_orders ORDER BY decided_at"
        )
    ]

    assert by_text[0] == "2026-09-15T21:00:00.5Z"  # the text trap
    assert _orders(database) == (whole_a, whole_b, half, next_day)


def test_the_order_repository_rejects_a_non_query(database: Path) -> None:
    with pytest.raises(TypeError, match="FuturesPaperOrderQuery"):
        SQLiteFuturesPaperOrderRepository(database).get_orders(PaperPortfolioIdentity(_PORTFOLIO))


# ---------------------------------------------------------------------------
# Orders: restart and races
# ---------------------------------------------------------------------------


def test_orders_survive_restart_and_retries_stay_idempotent(tmp_path: Path) -> None:
    path = tmp_path / "restart.sqlite3"
    orders = (_order("o-1"), _order("o-2", contracts=_BIG, side=SELL))
    SQLiteFuturesPaperOrderStore(path).store(orders)

    assert (
        SQLiteFuturesPaperOrderRepository(path).get_orders(
            FuturesPaperOrderQuery(PaperPortfolioIdentity(_PORTFOLIO))
        )
        == orders
    )
    assert SQLiteFuturesPaperOrderStore(path).store(orders) == 2
    assert _count(path, "futures_paper_orders") == 2


def _race_orders(monkeypatch: pytest.MonkeyPatch) -> None:
    real = SQLiteFuturesPaperOrderStore._existing
    calls = {"count": 0}

    def racing(connection, order_identity):
        calls["count"] += 1
        return None if calls["count"] == 1 else real(connection, order_identity)

    monkeypatch.setattr(SQLiteFuturesPaperOrderStore, "_existing", staticmethod(racing))


def test_an_order_race_with_an_equal_order_is_idempotent(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _store_orders(database, _order())
    _race_orders(monkeypatch)

    assert _store_orders(database, _order()) == 1
    assert _count(database, "futures_paper_orders") == 1


def test_an_order_race_with_a_different_order_conflicts(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _store_orders(database, _order())
    _race_orders(monkeypatch)

    with pytest.raises(FuturesPaperOrderConflictError):
        _store_orders(database, _order(contracts=5))

    monkeypatch.undo()
    assert _orders(database) == (_order(),)


# ---------------------------------------------------------------------------
# Orders: corruption
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("column", "value"),
    [
        ("side", "HOLD"),
        ("side", "buy"),
        ("contracts", "0"),
        ("contracts", "-1"),
        ("contracts", "1.0"),
        ("contracts", "1e3"),
        ("contracts", "+1"),
        ("contracts", "01"),
        ("contracts", " 2"),
        ("contracts", ""),
        ("contracts", "true"),
        ("contracts", b"2"),
        ("expiration_date", "2026-02-30"),
        ("decided_at", "not-an-instant"),
        ("decided_at", "2026-09-15T21:00:00+00:00"),
        ("order_identity", " order-1 "),
        ("strategy_identity", " futures-forward "),
        ("product_code", ""),
    ],
)
def test_a_corrupt_order_row_fails_loudly(database: Path, column: str, value: object) -> None:
    _store_orders(database, _order())
    _execute(database, f"UPDATE futures_paper_orders SET {column} = ?", (value,))  # noqa: S608

    with pytest.raises(FuturesPaperTradingStorageError):
        _orders(database)


def test_a_store_over_a_corrupt_existing_order_fails(database: Path) -> None:
    _store_orders(database, _order())
    _execute(database, "UPDATE futures_paper_orders SET side = 'HOLD'")

    with pytest.raises(FuturesPaperTradingStorageError):
        _store_orders(database, _order())


# ---------------------------------------------------------------------------
# Fills: parent-order integrity
# ---------------------------------------------------------------------------


def test_a_fill_without_a_stored_order_is_refused(database: Path) -> None:
    with pytest.raises(FuturesPaperFillConflictError, match="before its order is stored"):
        _store_fills(database, _fill())

    assert _count(database, "futures_paper_fills") == 0


@pytest.mark.parametrize(
    "overrides",
    [
        {"side": SELL},
        {"contracts": 3},
        {"contract": _ES_MAR},
        {"strategy": "other"},
        {"portfolio": _OTHER},
        {"decided_at": "2026-09-14T21:00:00Z"},
    ],
    ids=["side", "contracts", "expiry", "strategy", "portfolio", "decided-at"],
)
def test_a_fill_whose_intent_differs_from_its_stored_order_is_refused(
    database: Path, overrides: dict
) -> None:
    stored = _order()
    _store_orders(database, stored)
    impostor = _order(**overrides)

    with pytest.raises(FuturesPaperFillConflictError, match="does not match its stored order"):
        _store_fills(database, _fill(stored, intent=impostor.intent))

    assert _count(database, "futures_paper_fills") == 0


def test_a_missing_parent_rolls_back_the_whole_fill_batch(database: Path) -> None:
    stored = _order("o-1")
    _store_orders(database, stored)

    with pytest.raises(FuturesPaperFillConflictError):
        _store_fills(database, _fill(stored), _fill(_order("o-missing")))

    assert _fills(database) == ()


def test_a_mismatched_parent_rolls_back_the_whole_fill_batch(database: Path) -> None:
    first, second = _order("o-1"), _order("o-2")
    _store_orders(database, first, second)

    with pytest.raises(FuturesPaperFillConflictError):
        _store_fills(database, _fill(first), _fill(second, intent=_order(side=SELL).intent))

    assert _fills(database) == ()


# ---------------------------------------------------------------------------
# Fills: round trip
# ---------------------------------------------------------------------------


def test_a_fill_round_trips_exactly_on_its_stored_order(database: Path) -> None:
    order = _order(contracts=_BIG, side=SELL)
    fill = _fill(order)
    _store_orders(database, order)

    assert _store_fills(database, fill) == 1
    assert _fills(database) == (fill,)
    assert _fills(database)[0].intent == order.intent
    assert _query(database, "SELECT * FROM futures_paper_fills") == [
        ("fill-order-1", "order-1", "7663.25", _NEXT)
    ]


@pytest.mark.parametrize(
    "quote",
    ["0", "-37.63", "-0.000000000000000000000000000001", "123456789012345678901234567890.5"],
    ids=["zero", "negative", "tiny-negative", "high-precision"],
)
def test_zero_negative_and_precise_quotes_round_trip(database: Path, quote: str) -> None:
    order = _order()
    fill = _fill(order, quote=quote)
    _store_orders(database, order)
    _store_fills(database, fill)

    assert _fills(database) == (fill,)
    assert _fills(database)[0].fill_quote.value == Decimal(quote)


def test_a_pending_order_has_no_fill(database: Path) -> None:
    filled, pending = _order("o-1"), _order("o-2")
    _store_orders(database, filled, pending)
    _store_fills(database, _fill(filled))

    assert [fill.order_identity.identity for fill in _fills(database)] == ["o-1"]
    assert len(_orders(database)) == 2


# ---------------------------------------------------------------------------
# Fills: idempotency and conflicts
# ---------------------------------------------------------------------------


def test_an_equal_fill_retry_is_idempotent(database: Path) -> None:
    order = _order()
    _store_orders(database, order)
    _store_fills(database, _fill(order))

    assert _store_fills(database, _fill(order)) == 1
    assert _store_fills(database, _fill(order, filled_at="2026-09-17T02:30:00+05:30")) == 1
    assert _count(database, "futures_paper_fills") == 1


@pytest.mark.parametrize(
    "overrides",
    [{"quote": "7663.5"}, {"quote": "-37.63"}, {"filled_at": "2026-09-17T21:00:00Z"}],
    ids=["quote", "negative-quote", "filled-at"],
)
def test_a_different_fill_under_one_identity_conflicts(database: Path, overrides: dict) -> None:
    order = _order()
    original = _fill(order)
    _store_orders(database, order)
    _store_fills(database, original)

    with pytest.raises(FuturesPaperFillConflictError, match="already stored under this fill"):
        _store_fills(database, _fill(order, **overrides))

    assert _fills(database) == (original,)


def test_a_second_fill_for_one_order_conflicts(database: Path) -> None:
    order = _order()
    original = _fill(order)
    _store_orders(database, order)
    _store_fills(database, original)

    with pytest.raises(FuturesPaperFillConflictError, match="already attached"):
        _store_fills(database, _fill(order, fill_id="fill-other", quote="1"))

    assert _fills(database) == (original,)


def test_an_empty_fill_batch_returns_zero(database: Path) -> None:
    assert _store_fills(database) == 0


def test_a_fill_batch_repeating_one_fill_identity_is_rejected(database: Path) -> None:
    first, second = _order("o-1"), _order("o-2")
    _store_orders(database, first, second)

    with pytest.raises(FuturesPaperFillConflictError, match="sharing one fill identity"):
        _store_fills(database, _fill(first, fill_id="f"), _fill(second, fill_id="f"))
    with pytest.raises(FuturesPaperFillConflictError, match="sharing one fill identity"):
        _store_fills(database, _fill(first), _fill(first))

    assert _fills(database) == ()


def test_a_fill_batch_repeating_one_order_identity_is_rejected(database: Path) -> None:
    order = _order()
    _store_orders(database, order)

    with pytest.raises(FuturesPaperFillConflictError, match="two fills for one order"):
        _store_fills(database, _fill(order, fill_id="f-1"), _fill(order, fill_id="f-2"))

    assert _fills(database) == ()


def test_one_conflict_leaves_the_whole_fill_batch_uncommitted(database: Path) -> None:
    first, second = _order("o-1"), _order("o-2")
    _store_orders(database, first, second)
    _store_fills(database, _fill(first))

    with pytest.raises(FuturesPaperFillConflictError):
        _store_fills(database, _fill(second), _fill(first, quote="1"))

    assert [fill.order_identity.identity for fill in _fills(database)] == ["o-1"]


@pytest.mark.parametrize(
    "fills",
    [[_fill()], (_fill(), "fill"), (_order(),)],
    ids=["list", "foreign-member", "order"],
)
def test_foreign_fill_batches_are_rejected_before_any_write(database: Path, fills) -> None:
    _store_orders(database, _order())

    with pytest.raises(TypeError):
        SQLiteFuturesPaperFillStore(database).store(fills)

    assert _fills(database) == ()


# ---------------------------------------------------------------------------
# Fills: repository
# ---------------------------------------------------------------------------


def test_fills_are_isolated_by_their_parent_orders_portfolio(database: Path) -> None:
    mine, theirs = _order("o-1"), _order("o-2", portfolio=_OTHER)
    _store_orders(database, mine, theirs)
    _store_fills(database, _fill(mine), _fill(theirs))

    assert _fills(database) == (_fill(mine),)
    assert _fills(database, _OTHER) == (_fill(theirs),)
    assert _fills(database, "nobody") == ()


def test_fills_are_ordered_semantically_then_by_order_identity(database: Path) -> None:
    orders = {name: _order(name) for name in ("a", "b", "c", "d")}
    _store_orders(database, *orders.values())
    whole_b = _fill(orders["b"], fill_id="f-1", filled_at="2026-09-16T21:00:00Z")
    whole_a = _fill(orders["a"], fill_id="f-2", filled_at="2026-09-16T21:00:00Z")
    half = _fill(orders["c"], fill_id="f-0", filled_at="2026-09-16T21:00:00.5Z")
    later = _fill(orders["d"], fill_id="f-00", filled_at="2026-09-17T16:00:00-05:00")
    _store_fills(database, later, half, whole_b, whole_a)

    by_text = [
        row[0]
        for row in _query(database, "SELECT filled_at FROM futures_paper_fills ORDER BY filled_at")
    ]

    assert by_text[0] == "2026-09-16T21:00:00.5Z"  # the text trap
    assert _fills(database) == (whole_a, whole_b, half, later)


def test_fills_survive_restart_and_retries_stay_idempotent(tmp_path: Path) -> None:
    path = tmp_path / "restart.sqlite3"
    order = _order(contracts=_BIG)
    fill = _fill(order, quote="-37.63")
    SQLiteFuturesPaperOrderStore(path).store((order,))
    SQLiteFuturesPaperFillStore(path).store((fill,))

    reopened = SQLiteFuturesPaperFillRepository(path).get_fills(
        FuturesPaperFillQuery(PaperPortfolioIdentity(_PORTFOLIO))
    )

    assert reopened == (fill,)
    assert SQLiteFuturesPaperFillStore(path).store((fill,)) == 1
    assert _count(path, "futures_paper_fills") == 1


def test_the_fill_repository_rejects_a_non_query(database: Path) -> None:
    with pytest.raises(TypeError, match="FuturesPaperFillQuery"):
        SQLiteFuturesPaperFillRepository(database).get_fills(
            FuturesPaperOrderQuery(PaperPortfolioIdentity(_PORTFOLIO))  # type: ignore[arg-type]
        )


# ---------------------------------------------------------------------------
# Fills: races
# ---------------------------------------------------------------------------


def _race_fills(monkeypatch: pytest.MonkeyPatch) -> None:
    real_existing = SQLiteFuturesPaperFillStore._existing
    real_attached = SQLiteFuturesPaperFillStore._attached
    calls = {"existing": 0, "attached": 0}

    def existing(connection, fill_identity):
        calls["existing"] += 1
        return None if calls["existing"] == 1 else real_existing(connection, fill_identity)

    def attached(connection, order_identity):
        calls["attached"] += 1
        return None if calls["attached"] == 1 else real_attached(connection, order_identity)

    monkeypatch.setattr(SQLiteFuturesPaperFillStore, "_existing", staticmethod(existing))
    monkeypatch.setattr(SQLiteFuturesPaperFillStore, "_attached", staticmethod(attached))


def test_a_fill_race_with_an_equal_fill_is_idempotent(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    order = _order()
    _store_orders(database, order)
    _store_fills(database, _fill(order))
    _race_fills(monkeypatch)

    assert _store_fills(database, _fill(order)) == 1
    assert _count(database, "futures_paper_fills") == 1


def test_a_fill_race_with_a_different_fill_conflicts(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    order = _order()
    _store_orders(database, order)
    _store_fills(database, _fill(order))
    _race_fills(monkeypatch)

    with pytest.raises(FuturesPaperFillConflictError):
        _store_fills(database, _fill(order, quote="1"))


def test_a_fill_race_on_the_same_order_conflicts(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    order = _order()
    _store_orders(database, order)
    _store_fills(database, _fill(order))
    _race_fills(monkeypatch)

    with pytest.raises(FuturesPaperFillConflictError, match="already attached"):
        _store_fills(database, _fill(order, fill_id="fill-other"))

    monkeypatch.undo()
    assert _fills(database) == (_fill(order),)


# ---------------------------------------------------------------------------
# Fills: corruption
# ---------------------------------------------------------------------------


def _stored_fill(database: Path) -> None:
    order = _order()
    _store_orders(database, order)
    _store_fills(database, _fill(order))


def test_an_orphan_fill_row_fails_every_read(database: Path) -> None:
    _stored_fill(database)
    _execute(
        database,
        "INSERT INTO futures_paper_fills VALUES (?, ?, ?, ?)",
        ("fill-orphan", "order-missing", "1", _NEXT),
    )

    with pytest.raises(FuturesPaperTradingStorageError, match="no stored parent order"):
        _fills(database)
    with pytest.raises(FuturesPaperTradingStorageError, match="no stored parent order"):
        _fills(database, _OTHER)


@pytest.mark.parametrize(
    ("column", "value"),
    [
        ("filled_at", _D),
        ("filled_at", "2026-09-14T21:00:00Z"),
        ("filled_at", "garbage"),
        ("filled_at", "2026-09-16T21:00:00+00:00"),
        ("fill_quote", "abc"),
        ("fill_quote", "NaN"),
        ("fill_quote", "Infinity"),
        ("fill_quote", ""),
        ("fill_quote", "7663.250"),
        ("fill_quote", b"7663.25"),
    ],
)
def test_a_corrupt_fill_row_fails_loudly(database: Path, column: str, value: object) -> None:
    _stored_fill(database)
    _execute(database, f"UPDATE futures_paper_fills SET {column} = ?", (value,))  # noqa: S608

    with pytest.raises(FuturesPaperTradingStorageError):
        _fills(database)


def test_a_corrupt_parent_order_fails_its_fill_read(database: Path) -> None:
    _stored_fill(database)
    _execute(database, "UPDATE futures_paper_orders SET contracts = '0'")

    with pytest.raises(FuturesPaperTradingStorageError):
        _fills(database)


def test_a_parent_moved_past_its_fill_fails_the_read(database: Path) -> None:
    """The Core fill reasserts strictly-after timing on the stored parent."""
    _stored_fill(database)
    _execute(database, "UPDATE futures_paper_orders SET decided_at = ?", (_NEXT,))

    with pytest.raises(FuturesPaperTradingStorageError):
        _fills(database)


def test_a_store_over_a_corrupt_existing_fill_fails(database: Path) -> None:
    _stored_fill(database)
    _execute(database, "UPDATE futures_paper_fills SET fill_quote = 'abc'")

    with pytest.raises(FuturesPaperTradingStorageError):
        _store_fills(database, _fill())


# ---------------------------------------------------------------------------
# Structure, transactions, boundaries and exports
# ---------------------------------------------------------------------------


def _non_docstring_strings(tree: ast.Module) -> list[str]:
    docstrings = {
        id(node.body[0].value)
        for node in ast.walk(tree)
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef))
        and node.body
        and isinstance(node.body[0], ast.Expr)
        and isinstance(node.body[0].value, ast.Constant)
    }
    return [
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and id(node) not in docstrings
    ]


def _tree() -> ast.Module:
    return ast.parse(Path(module.__file__).read_text(encoding="utf-8"))


def test_the_adapters_issue_insert_only_sql() -> None:
    sql = " ".join(_non_docstring_strings(_tree())).upper()

    assert "INSERT INTO FUTURES_PAPER_ORDERS" in sql
    assert "INSERT INTO FUTURES_PAPER_FILLS" in sql
    for forbidden in ("UPDATE ", "DELETE ", "REPLACE", "ON CONFLICT", "UPSERT", "ORDER BY"):
        assert forbidden not in sql


def _trace(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    statements: list[str] = []
    real_connect = sqlite3.connect

    def connect(*args, **kwargs):
        connection = real_connect(*args, **kwargs)
        connection.set_trace_callback(statements.append)
        return connection

    monkeypatch.setattr(module.sqlite3, "connect", connect)
    return statements


def test_the_order_store_writes_inside_begin_immediate(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    statements = _trace(monkeypatch)

    _store_orders(database, _order())

    begin = statements.index("BEGIN IMMEDIATE")
    insert = next(
        i for i, s in enumerate(statements) if s.startswith("INSERT INTO futures_paper_orders")
    )
    assert begin < insert
    assert statements[-1] == "COMMIT"


def test_the_fill_store_writes_inside_begin_immediate(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _store_orders(database, _order())
    statements = _trace(monkeypatch)

    _store_fills(database, _fill())

    begin = statements.index("BEGIN IMMEDIATE")
    parent = next(i for i, s in enumerate(statements) if "FROM futures_paper_orders" in s)
    insert = next(
        i for i, s in enumerate(statements) if s.startswith("INSERT INTO futures_paper_fills")
    )
    assert begin < parent < insert
    assert statements[-1] == "COMMIT"


def test_the_adapters_depend_on_no_provider_clock_or_economics() -> None:
    tree = _tree()
    modules = {
        node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.module
    } | {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    names = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
        for alias in node.names
    }

    for module_path in modules:
        assert module_path.split(".")[0] not in {
            "databento",
            "exchange_calendars",
            "requests",
            "urllib",
            "socket",
            "time",
            "datetime",
            "random",
            "uuid",
            "pickle",
            "json",
        }
    for forbidden in (
        "FuturesHistoricalMarketDataSource",
        "FuturesTradingSessionResolver",
        "Price",
        "Money",
        "Currency",
        "PaperOrderStatus",
        "FuturesPosition",
        "FuturesPaperPortfolio",
    ):
        assert forbidden not in names


def test_the_adapters_implement_the_application_ports() -> None:
    assert issubclass(SQLiteFuturesPaperOrderStore, FuturesPaperOrderStore)
    assert issubclass(SQLiteFuturesPaperOrderRepository, FuturesPaperOrderRepository)
    assert issubclass(SQLiteFuturesPaperFillStore, FuturesPaperFillStore)
    assert issubclass(SQLiteFuturesPaperFillRepository, FuturesPaperFillRepository)


def test_the_adapters_are_exported_and_the_helpers_are_not() -> None:
    for name in (
        "SQLiteFuturesPaperOrderStore",
        "SQLiteFuturesPaperOrderRepository",
        "SQLiteFuturesPaperFillStore",
        "SQLiteFuturesPaperFillRepository",
        "FuturesPaperTradingStorageError",
        "initialize_futures_paper_trading_schema",
        "FUTURES_PAPER_ORDER_SCHEMA",
        "FUTURES_PAPER_FILL_SCHEMA",
    ):
        assert name in persistence.__all__
    for private in ("_order_row", "_fill_row", "_decode_order", "_decode_fill", "_count"):
        assert not hasattr(persistence, private)
