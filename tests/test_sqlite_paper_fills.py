"""Contract tests for the SQLite paper fill adapters."""

from __future__ import annotations

import ast
import sqlite3
from decimal import Decimal
from pathlib import Path

import pytest
from northstar_application.application_services import BuildPaperPortfolioUseCase
from northstar_application.ports import PaperFillConflictError, PaperFillQuery
from northstar_core.domain.value_objects import ListingReference
from northstar_core.foundation.value_objects import (
    Currency,
    ExchangeCode,
    PointInTime,
    Price,
    Quantity,
    Symbol,
)
from northstar_core.paper_trading import (
    ExecutionIntent,
    OrderSide,
    PaperFill,
    PaperFillIdentity,
    PaperOrderIdentity,
    PaperPortfolioIdentity,
)
from northstar_core.strategy import StrategyIdentity

from northstar_infrastructure.persistence import (
    SQLitePaperFillRepository,
    SQLitePaperFillStore,
)

_USD = Currency("USD")
_EUR = Currency("EUR")
_NASDAQ = ExchangeCode("NASDAQ")
_LSE = ExchangeCode("LSE")
_AAPL = ListingReference(Symbol("AAPL"), _NASDAQ)
_AAPL_LSE = ListingReference(Symbol("AAPL"), _LSE)
_MSFT = ListingReference(Symbol("MSFT"), _NASDAQ)
_STRATEGY = StrategyIdentity("alpha")
_PORTFOLIO = PaperPortfolioIdentity("paper-1")
_OTHER_PORTFOLIO = PaperPortfolioIdentity("paper-2")

_WHOLE_SECOND = PointInTime("2026-01-20T16:00:00Z")
_SUB_SECOND = PointInTime("2026-01-20T16:00:00.1Z")
_NEXT_DAY = PointInTime("2026-01-21T16:00:00Z")


@pytest.fixture
def database_path(tmp_path: Path) -> Path:
    return tmp_path / "paper_fills.sqlite3"


def _fill(
    fill_id: str = "fill-1",
    order_id: str = "order-1",
    *,
    filled_at: PointInTime = _WHOLE_SECOND,
    decided_at: PointInTime | None = None,
    portfolio: PaperPortfolioIdentity = _PORTFOLIO,
    listing: ListingReference = _AAPL,
    side: OrderSide = OrderSide.BUY,
    strategy: StrategyIdentity = _STRATEGY,
    quantity: str = "10",
    price: str = "100",
    currency: Currency = _USD,
) -> PaperFill:
    intent = ExecutionIntent(
        portfolio_identity=portfolio,
        listing_reference=listing,
        side=side,
        quantity=Quantity(quantity),
        strategy_identity=strategy,
        decided_at=decided_at or filled_at,
    )
    return PaperFill(
        identity=PaperFillIdentity(fill_id),
        order_identity=PaperOrderIdentity(order_id),
        intent=intent,
        quantity=Quantity(quantity),
        price=Price(price, currency),
        filled_at=filled_at,
    )


def _query(portfolio: PaperPortfolioIdentity = _PORTFOLIO) -> PaperFillQuery:
    return PaperFillQuery(portfolio_identity=portfolio)


# ---------------------------------------------------------------------------
# Insert and idempotency
# ---------------------------------------------------------------------------


def test_an_empty_batch_is_a_safe_no_op(database_path: Path) -> None:
    assert SQLitePaperFillStore(database_path).store(()) == 0


def test_a_new_fill_is_inserted(database_path: Path) -> None:
    fill = _fill()

    assert SQLitePaperFillStore(database_path).store((fill,)) == 1
    assert SQLitePaperFillRepository(database_path).get_fills(_query()) == (fill,)


def test_a_batch_of_new_fills_is_inserted(database_path: Path) -> None:
    fills = (_fill("f-1", "o-1"), _fill("f-2", "o-2", filled_at=_NEXT_DAY))

    assert SQLitePaperFillStore(database_path).store(fills) == 2
    assert len(SQLitePaperFillRepository(database_path).get_fills(_query())) == 2


def test_identical_retry_is_idempotent(database_path: Path) -> None:
    store = SQLitePaperFillStore(database_path)
    fill = _fill()
    store.store((fill,))

    assert store.store((fill,)) == 1
    assert len(SQLitePaperFillRepository(database_path).get_fills(_query())) == 1


def test_a_structurally_equal_fresh_object_is_idempotent(database_path: Path) -> None:
    store = SQLitePaperFillStore(database_path)
    store.store((_fill(),))

    assert store.store((_fill(),)) == 1
    assert len(SQLitePaperFillRepository(database_path).get_fills(_query())) == 1


def test_accepted_and_idempotent_entries_both_count(database_path: Path) -> None:
    store = SQLitePaperFillStore(database_path)
    store.store((_fill("f-1", "o-1"),))

    assert store.store((_fill("f-1", "o-1"), _fill("f-2", "o-2", filled_at=_NEXT_DAY))) == 2


# ---------------------------------------------------------------------------
# Conflicts
# ---------------------------------------------------------------------------


def test_a_different_fill_under_one_identity_is_rejected(database_path: Path) -> None:
    store = SQLitePaperFillStore(database_path)
    original = _fill("f-1", "o-1", quantity="10")
    store.store((original,))

    with pytest.raises(PaperFillConflictError, match="already stored under this fill identity"):
        store.store((_fill("f-1", "o-1", quantity="99"),))

    assert SQLitePaperFillRepository(database_path).get_fills(_query()) == (original,)


@pytest.mark.parametrize(
    "difference",
    [
        {"quantity": "11"},
        {"price": "101"},
        {"currency": _EUR},
        {"filled_at": _NEXT_DAY},
        {"side": OrderSide.SELL},
        {"listing": _MSFT},
        {"strategy": StrategyIdentity("zeta")},
    ],
)
def test_any_domain_difference_is_a_conflict(database_path: Path, difference: dict) -> None:
    store = SQLitePaperFillStore(database_path)
    store.store((_fill("f-1", "o-1"),))

    with pytest.raises(PaperFillConflictError):
        store.store((_fill("f-1", "o-1", **difference),))


def test_one_order_cannot_produce_a_second_fill(database_path: Path) -> None:
    store = SQLitePaperFillStore(database_path)
    original = _fill("f-1", "o-1")
    store.store((original,))

    with pytest.raises(PaperFillConflictError, match="already attached to a different paper fill"):
        store.store((_fill("f-2", "o-1", filled_at=_NEXT_DAY),))

    assert SQLitePaperFillRepository(database_path).get_fills(_query()) == (original,)


def test_in_batch_duplicate_fill_identity_is_rejected(database_path: Path) -> None:
    store = SQLitePaperFillStore(database_path)

    with pytest.raises(PaperFillConflictError, match="two fills sharing one fill identity"):
        store.store((_fill("f-1", "o-1"), _fill("f-1", "o-2")))

    assert SQLitePaperFillRepository(database_path).get_fills(_query()) == ()


def test_in_batch_duplicate_order_identity_is_rejected(database_path: Path) -> None:
    store = SQLitePaperFillStore(database_path)

    with pytest.raises(PaperFillConflictError, match="two fills for one order"):
        store.store((_fill("f-1", "o-1"), _fill("f-2", "o-1")))

    assert SQLitePaperFillRepository(database_path).get_fills(_query()) == ()


def test_a_conflicting_batch_rolls_back_earlier_writes(database_path: Path) -> None:
    """The conflict is last, so a non-atomic store would leave the first fill behind."""
    store = SQLitePaperFillStore(database_path)
    store.store((_fill("f-1", "o-1"),))

    with pytest.raises(PaperFillConflictError):
        store.store(
            (
                _fill("f-2", "o-2", filled_at=_NEXT_DAY),
                _fill("f-1", "o-1", quantity="999"),
            )
        )

    stored = SQLitePaperFillRepository(database_path).get_fills(_query())
    assert [fill.identity.identity for fill in stored] == ["f-1"]
    assert stored[0].quantity == Quantity("10")


def test_an_order_conflict_mid_batch_rolls_everything_back(database_path: Path) -> None:
    store = SQLitePaperFillStore(database_path)
    store.store((_fill("f-1", "o-1"),))

    with pytest.raises(PaperFillConflictError):
        store.store(
            (
                _fill("f-2", "o-2", filled_at=_NEXT_DAY),
                _fill("f-3", "o-1", filled_at=_NEXT_DAY),
            )
        )

    assert len(SQLitePaperFillRepository(database_path).get_fills(_query())) == 1


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


def test_the_order_identity_is_unique_in_the_schema(database_path: Path) -> None:
    """Storage enforces one fill per order, not only Python validation."""
    SQLitePaperFillStore(database_path).store((_fill(),))

    with sqlite3.connect(database_path) as connection:
        indexes = connection.execute(
            "SELECT name, \"unique\" FROM pragma_index_list('paper_fills')"
        ).fetchall()
        unique_columns = set()
        for name, is_unique in indexes:
            if is_unique:
                for row in connection.execute(f"PRAGMA index_info('{name}')").fetchall():
                    unique_columns.add(row[2])

    assert "order_identity" in unique_columns


def test_the_database_itself_refuses_a_duplicate_order(database_path: Path) -> None:
    """Bypassing the adapter must still hit the storage constraint."""
    SQLitePaperFillStore(database_path).store((_fill("f-1", "o-1"),))

    with pytest.raises(sqlite3.IntegrityError), sqlite3.connect(database_path) as connection:
        connection.execute(
            "INSERT INTO paper_fills (fill_identity, order_identity, portfolio_identity, "
            "symbol, exchange_code, side, strategy_identity, decided_at, quantity, "
            "price_amount, price_currency, filled_at) "
            "VALUES ('f-2', 'o-1', 'paper-1', 'AAPL', 'NASDAQ', 'BUY', 'alpha', "
            "'2026-01-20T16:00:00Z', '1', '1', 'USD', '2026-01-20T16:00:00Z')"
        )


def test_the_fill_identity_is_the_primary_key(database_path: Path) -> None:
    SQLitePaperFillStore(database_path).store((_fill(),))

    with sqlite3.connect(database_path) as connection:
        columns = connection.execute("PRAGMA table_info(paper_fills)").fetchall()

    primary_key = [column[1] for column in columns if column[5]]
    assert primary_key == ["fill_identity"]


def test_every_field_has_its_own_column(database_path: Path) -> None:
    SQLitePaperFillStore(database_path).store((_fill(),))

    with sqlite3.connect(database_path) as connection:
        columns = [row[1] for row in connection.execute("PRAGMA table_info(paper_fills)")]

    assert columns == [
        "fill_identity",
        "order_identity",
        "portfolio_identity",
        "symbol",
        "exchange_code",
        "side",
        "strategy_identity",
        "decided_at",
        "quantity",
        "price_amount",
        "price_currency",
        "filled_at",
    ]


def test_no_holdings_or_order_table_exists(database_path: Path) -> None:
    SQLitePaperFillStore(database_path).store((_fill(),))

    with sqlite3.connect(database_path) as connection:
        tables = {
            row[0]
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }

    assert tables == {"paper_fills"}


# ---------------------------------------------------------------------------
# Fidelity
# ---------------------------------------------------------------------------


def test_decimal_values_round_trip_exactly(database_path: Path) -> None:
    fill = _fill(quantity="0.000000000000000000000001", price="123.456789012345678901234567")
    SQLitePaperFillStore(database_path).store((fill,))

    restored = SQLitePaperFillRepository(database_path).get_fills(_query())[0]

    assert restored.quantity.value == Decimal("0.000000000000000000000001")
    assert restored.price.amount == Decimal("123.456789012345678901234567")
    assert restored == fill


def test_decimal_values_are_stored_as_text_not_float(database_path: Path) -> None:
    SQLitePaperFillStore(database_path).store((_fill(price="123.456789012345678901234567"),))

    with sqlite3.connect(database_path) as connection:
        amount = connection.execute("SELECT price_amount FROM paper_fills").fetchone()[0]

    assert isinstance(amount, str)
    assert amount == "123.456789012345678901234567"


def test_currency_is_preserved(database_path: Path) -> None:
    SQLitePaperFillStore(database_path).store((_fill(currency=_EUR),))

    restored = SQLitePaperFillRepository(database_path).get_fills(_query())[0]

    assert restored.price.currency == _EUR


def test_point_in_time_semantics_are_preserved(database_path: Path) -> None:
    offset = PointInTime("2026-01-20T21:30:00+05:30")
    fill = _fill(filled_at=offset, decided_at=offset)
    SQLitePaperFillStore(database_path).store((fill,))

    restored = SQLitePaperFillRepository(database_path).get_fills(_query())[0]

    assert restored.filled_at == _WHOLE_SECOND
    assert restored.filled_at.compare(offset) == 0
    assert restored.decided_at.compare(offset) == 0


def test_every_derived_value_survives_the_round_trip(database_path: Path) -> None:
    fill = _fill(
        listing=_AAPL_LSE, side=OrderSide.SELL, strategy=StrategyIdentity("zeta"), quantity="2.5"
    )
    SQLitePaperFillStore(database_path).store((fill,))

    restored = SQLitePaperFillRepository(database_path).get_fills(_query())[0]

    assert restored.portfolio_identity == _PORTFOLIO
    assert restored.listing_reference == _AAPL_LSE
    assert restored.side is OrderSide.SELL
    assert restored.strategy_identity == StrategyIdentity("zeta")
    assert restored.decided_at == fill.decided_at
    assert restored.intent == fill.intent
    assert restored == fill


def test_a_sub_second_decision_instant_is_preserved(database_path: Path) -> None:
    fill = _fill(filled_at=_SUB_SECOND, decided_at=_SUB_SECOND)
    SQLitePaperFillStore(database_path).store((fill,))

    restored = SQLitePaperFillRepository(database_path).get_fills(_query())[0]

    assert restored.filled_at == _SUB_SECOND
    assert restored.decided_at == _SUB_SECOND


# ---------------------------------------------------------------------------
# Query isolation and persistence
# ---------------------------------------------------------------------------


def test_an_empty_store_returns_an_empty_tuple(database_path: Path) -> None:
    assert SQLitePaperFillRepository(database_path).get_fills(_query()) == ()


def test_a_query_matching_no_portfolio_returns_an_empty_tuple(database_path: Path) -> None:
    SQLitePaperFillStore(database_path).store((_fill(),))

    assert SQLitePaperFillRepository(database_path).get_fills(_query(_OTHER_PORTFOLIO)) == ()


def test_fills_are_isolated_by_portfolio(database_path: Path) -> None:
    fills = (
        _fill("f-1", "o-1"),
        _fill("f-2", "o-2", portfolio=_OTHER_PORTFOLIO),
    )
    SQLitePaperFillStore(database_path).store(fills)

    repository = SQLitePaperFillRepository(database_path)
    assert [f.identity.identity for f in repository.get_fills(_query())] == ["f-1"]
    assert [
        f.identity.identity
        for f in SQLitePaperFillRepository(database_path).get_fills(_query(_OTHER_PORTFOLIO))
    ] == ["f-2"]


def test_the_complete_portfolio_history_is_returned(database_path: Path) -> None:
    fills = tuple(
        _fill(
            f"f-{index}",
            f"o-{index}",
            filled_at=PointInTime(f"2026-01-{20 + index:02d}T16:00:00Z"),
        )
        for index in range(5)
    )
    SQLitePaperFillStore(database_path).store(fills)

    assert len(SQLitePaperFillRepository(database_path).get_fills(_query())) == 5


def test_fills_survive_closing_and_reopening_the_database(database_path: Path) -> None:
    fill = _fill(quantity="3.25", price="987.654321", currency=_EUR)
    SQLitePaperFillStore(database_path).store((fill,))

    reopened = SQLitePaperFillRepository(database_path).get_fills(_query())

    assert reopened == (fill,)
    assert database_path.exists()


def test_independent_adapter_instances_share_state(database_path: Path) -> None:
    SQLitePaperFillStore(database_path).store((_fill("f-1", "o-1"),))
    SQLitePaperFillStore(database_path).store((_fill("f-2", "o-2", filled_at=_NEXT_DAY),))

    assert len(SQLitePaperFillRepository(database_path).get_fills(_query())) == 2
    assert len(SQLitePaperFillRepository(database_path).get_fills(_query())) == 2


def test_repository_rejects_a_wrong_query_type(database_path: Path) -> None:
    with pytest.raises(TypeError, match="query must be a PaperFillQuery"):
        SQLitePaperFillRepository(database_path).get_fills("paper-1")


# ---------------------------------------------------------------------------
# Ordering
# ---------------------------------------------------------------------------


def test_fills_are_ordered_oldest_to_newest(database_path: Path) -> None:
    store = SQLitePaperFillStore(database_path)
    store.store((_fill("f-2", "o-2", filled_at=_NEXT_DAY),))
    store.store((_fill("f-1", "o-1", filled_at=_WHOLE_SECOND),))

    retrieved = SQLitePaperFillRepository(database_path).get_fills(_query())

    assert [f.identity.identity for f in retrieved] == ["f-1", "f-2"]


def test_equal_instants_tie_break_by_fill_identity(database_path: Path) -> None:
    store = SQLitePaperFillStore(database_path)
    store.store((_fill("zeta", "o-z", filled_at=_WHOLE_SECOND),))
    store.store((_fill("alpha", "o-a", filled_at=_WHOLE_SECOND),))

    retrieved = SQLitePaperFillRepository(database_path).get_fills(_query())

    assert [f.identity.identity for f in retrieved] == ["alpha", "zeta"]


def test_sub_second_instants_sort_chronologically(database_path: Path) -> None:
    store = SQLitePaperFillStore(database_path)
    store.store((_fill("f-2", "o-2", filled_at=_SUB_SECOND),))
    store.store((_fill("f-1", "o-1", filled_at=_WHOLE_SECOND),))

    retrieved = SQLitePaperFillRepository(database_path).get_fills(_query())

    assert [f.filled_at for f in retrieved] == [_WHOLE_SECOND, _SUB_SECOND]


def test_ordering_does_not_follow_sqlite_text_ordering(database_path: Path) -> None:
    """The regression case: '.'(0x2E) sorts before 'Z'(0x5A) as text."""
    store = SQLitePaperFillStore(database_path)
    store.store((_fill("f-1", "o-1", filled_at=_WHOLE_SECOND),))
    store.store((_fill("f-2", "o-2", filled_at=_SUB_SECOND),))

    with sqlite3.connect(database_path) as connection:
        text_ordered = [
            row[0]
            for row in connection.execute(
                "SELECT filled_at FROM paper_fills ORDER BY filled_at ASC"
            )
        ]

    retrieved = SQLitePaperFillRepository(database_path).get_fills(_query())

    assert text_ordered == [_SUB_SECOND.value, _WHOLE_SECOND.value]
    assert [f.filled_at.value for f in retrieved] == [_WHOLE_SECOND.value, _SUB_SECOND.value]


@pytest.mark.parametrize(
    "instants",
    [
        ("2026-01-20T16:00:00Z", "2026-01-20T16:00:00.1Z", "2026-01-20T16:00:00.25Z"),
        ("2026-01-20T16:00:00.000001Z", "2026-01-20T16:00:01Z", "2026-01-20T16:00:59.9Z"),
    ],
)
def test_chronological_ordering_across_fractions(
    database_path: Path, instants: tuple[str, ...]
) -> None:
    store = SQLitePaperFillStore(database_path)
    for index, value in enumerate(reversed(instants)):
        store.store((_fill(f"f-{index}", f"o-{index}", filled_at=PointInTime(value)),))

    retrieved = SQLitePaperFillRepository(database_path).get_fills(_query())

    assert [f.filled_at.value for f in retrieved] == [PointInTime(v).value for v in instants]


# ---------------------------------------------------------------------------
# End to end with the fold
# ---------------------------------------------------------------------------


def test_repository_output_feeds_the_portfolio_fold(database_path: Path) -> None:
    """The fold refuses to reorder, so the adapter must deliver a valid order."""
    store = SQLitePaperFillStore(database_path)
    store.store((_fill("f-3", "o-3", filled_at=_NEXT_DAY, quantity="5", price="300"),))
    store.store((_fill("f-2", "o-2", filled_at=_SUB_SECOND, quantity="10", price="200"),))
    store.store((_fill("f-1", "o-1", filled_at=_WHOLE_SECOND, quantity="10", price="100"),))

    fills = SQLitePaperFillRepository(database_path).get_fills(_query())
    portfolio = BuildPaperPortfolioUseCase().execute(_PORTFOLIO, fills, _NEXT_DAY)

    position = portfolio.get_position(_AAPL)
    assert position.quantity == Quantity("25")
    # (10*100 + 10*200 + 5*300) / 25 = 4500 / 25
    assert position.average_price == Price("180", _USD)


def test_a_sell_history_folds_to_a_closed_position(database_path: Path) -> None:
    store = SQLitePaperFillStore(database_path)
    store.store((_fill("f-1", "o-1", filled_at=_WHOLE_SECOND, quantity="10"),))
    store.store((_fill("f-2", "o-2", filled_at=_NEXT_DAY, quantity="10", side=OrderSide.SELL),))

    fills = SQLitePaperFillRepository(database_path).get_fills(_query())
    portfolio = BuildPaperPortfolioUseCase().execute(_PORTFOLIO, fills, _NEXT_DAY)

    assert portfolio.positions == ()


def test_an_earlier_as_of_rebuilds_from_the_same_stored_history(database_path: Path) -> None:
    store = SQLitePaperFillStore(database_path)
    store.store((_fill("f-1", "o-1", filled_at=_WHOLE_SECOND, quantity="10"),))
    store.store((_fill("f-2", "o-2", filled_at=_NEXT_DAY, quantity="10"),))

    fills = SQLitePaperFillRepository(database_path).get_fills(_query())
    use_case = BuildPaperPortfolioUseCase()

    assert use_case.execute(_PORTFOLIO, fills, _WHOLE_SECOND).get_position(
        _AAPL
    ).quantity == Quantity("10")
    assert use_case.execute(_PORTFOLIO, fills, _NEXT_DAY).get_position(_AAPL).quantity == Quantity(
        "20"
    )


# ---------------------------------------------------------------------------
# Append-only guarantee
# ---------------------------------------------------------------------------


def _adapter_tree() -> ast.Module:
    from northstar_infrastructure.persistence import sqlite_paper_fills

    return ast.parse(Path(sqlite_paper_fills.__file__).read_text(encoding="utf-8"))


def _executed_sql() -> list[str]:
    """Return the SQL actually handed to sqlite3, ignoring prose in docstrings."""
    statements: list[str] = []
    for node in ast.walk(_adapter_tree()):
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)):
            continue
        if node.func.attr != "execute" or not node.args:
            continue
        argument = node.args[0]
        parts = [argument] if not isinstance(argument, ast.JoinedStr) else argument.values
        statements.append(" ".join(part.value for part in parts if isinstance(part, ast.Constant)))
    return statements


def test_the_adapter_issues_no_mutating_sql() -> None:
    executed = " ".join(_executed_sql()).upper()

    assert executed, "expected to find SQL statements to inspect"
    for forbidden in ("UPDATE ", "REPLACE ", "DELETE ", "ON CONFLICT", "INSERT OR"):
        assert forbidden not in executed
    assert "INSERT INTO PAPER_FILLS" in executed


def test_the_adapter_generates_no_identities() -> None:
    """Identity generation belongs to the producer, never to storage."""
    modules: set[str] = set()
    for node in ast.walk(_adapter_tree()):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            modules.add(node.module)

    for forbidden in ("uuid", "random", "secrets", "datetime", "time", "os"):
        assert forbidden not in {module.split(".")[0] for module in modules}
