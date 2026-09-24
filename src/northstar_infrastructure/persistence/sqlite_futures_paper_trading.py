"""SQLite-backed futures paper order and fill stores and repositories.

Orders are written when a decision is taken; fills are written later, once the
next stored daily bar exists. Both are immutable history: this module issues
INSERT only -- no UPDATE, no REPLACE, no overwriting upsert and no DELETE.
Order status is never stored; it is derived from whether a fill exists.

Every column is TEXT holding the canonical form of one Core value, and every
read re-serializes the decoded value and requires it to reproduce the stored
text exactly. A non-canonical or corrupt row is therefore reported, never
normalized into something the writer did not write. Quotes use the futures
OHLCV exact-Decimal text and never pass through a float; zero and negative
quotes are ordinary values. Contract counts are exact base-10 integer text, so
counts beyond SQLite's 64-bit INTEGER survive.

A fill row holds only its identity, its order identity, its quote and its
instant. Everything else is read from its stored parent order, so a decoded
fill is always built on the exact order it filled. Before a fill is inserted
its parent order must exist and carry exactly the fill's intent; that is
checked inside the write transaction, not left to SQLite foreign keys.

Ordering is applied in Python with PointInTime.compare(): ``...T21:00:00.5Z``
sorts before ``...T21:00:00Z`` as text while being the later instant.
"""

from __future__ import annotations

import re
import sqlite3
from contextlib import closing
from decimal import Decimal
from functools import cmp_to_key
from pathlib import Path

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

from northstar_infrastructure.persistence.sqlite_futures_paper_trading_schema import (
    initialize_futures_paper_trading_schema,
)

_ORDER_COLUMNS = (
    "order_identity",
    "portfolio_identity",
    "product_code",
    "exchange_code",
    "expiration_date",
    "side",
    "contracts",
    "strategy_identity",
    "decided_at",
)
_FILL_COLUMNS = ("fill_identity", "order_identity", "fill_quote", "filled_at")

_SELECT_ORDERS = f"SELECT {', '.join(_ORDER_COLUMNS)} FROM futures_paper_orders"  # noqa: S608
_INSERT_ORDER = (
    f"INSERT INTO futures_paper_orders ({', '.join(_ORDER_COLUMNS)}) "  # noqa: S608
    f"VALUES ({', '.join('?' for _ in _ORDER_COLUMNS)})"
)
_INSERT_FILL = (
    f"INSERT INTO futures_paper_fills ({', '.join(_FILL_COLUMNS)}) "  # noqa: S608
    f"VALUES ({', '.join('?' for _ in _FILL_COLUMNS)})"
)
# LEFT JOIN so a fill whose parent order is missing is reported, not hidden.
_SELECT_FILLS = (
    f"SELECT {', '.join(f'f.{column}' for column in _FILL_COLUMNS)}, "  # noqa: S608
    f"{', '.join(f'o.{column}' for column in _ORDER_COLUMNS)} "
    "FROM futures_paper_fills AS f "
    "LEFT JOIN futures_paper_orders AS o ON o.order_identity = f.order_identity"
)

_COUNT_TEXT = re.compile(r"[1-9][0-9]*")

_DECODE_ERRORS = (TypeError, ValueError, ArithmeticError)


class FuturesPaperTradingStorageError(RuntimeError):
    """Raised when the local futures paper trading store is unavailable or malformed."""


# ---------------------------------------------------------------------------
# Serialization
# ---------------------------------------------------------------------------


def _order_row(order: FuturesPaperOrder) -> tuple[str, ...]:
    intent = order.intent
    product = intent.contract.product
    return (
        order.identity.identity,
        intent.portfolio_identity.identity,
        product.product_code.value,
        product.exchange_code.value,
        intent.contract.expiration_date.value,
        intent.side.value,
        str(intent.contracts.value),
        intent.strategy_identity.identity,
        intent.decided_at.value,
    )


def _fill_row(fill: FuturesPaperFill) -> tuple[str, ...]:
    return (
        fill.identity.identity,
        fill.order_identity.identity,
        str(fill.fill_quote.value),
        fill.filled_at.value,
    )


def _count(text: str) -> int:
    """Read an exact positive base-10 integer, refusing any other spelling."""
    if _COUNT_TEXT.fullmatch(text) is None:
        raise ValueError("Futures paper order contracts must be positive integer text.")
    return int(text)


def _decimal(text: str) -> Decimal:
    """Rebuild an exact Decimal from stored text, never through a float."""
    value = Decimal(text)
    if not value.is_finite():
        raise ValueError("Futures paper fill quote must be finite.")
    return value


def _order(row: tuple[object, ...]) -> FuturesPaperOrder:
    if len(row) != len(_ORDER_COLUMNS) or not all(isinstance(value, str) for value in row):
        raise TypeError("Futures paper order columns must all be text.")
    (
        order_identity,
        portfolio_identity,
        product_code,
        exchange_code,
        expiration_date,
        side,
        contracts,
        strategy_identity,
        decided_at,
    ) = row
    return FuturesPaperOrder(
        identity=PaperOrderIdentity(order_identity),
        intent=FuturesExecutionIntent(
            portfolio_identity=PaperPortfolioIdentity(portfolio_identity),
            contract=FuturesContract(
                FuturesProductReference(Symbol(product_code), ExchangeCode(exchange_code)),
                ExpirationDate(expiration_date),
            ),
            side=OrderSide(side),
            contracts=FuturesContractCount(_count(contracts)),
            strategy_identity=StrategyIdentity(strategy_identity),
            decided_at=PointInTime(decided_at),
        ),
    )


def _decode_order(row: tuple[object, ...]) -> FuturesPaperOrder:
    """Rebuild a stored order and require it to reproduce its stored text."""
    try:
        order = _order(row)
    except _DECODE_ERRORS as exc:
        raise FuturesPaperTradingStorageError(
            "Futures paper order storage contains invalid data."
        ) from exc
    if _order_row(order) != row:
        raise FuturesPaperTradingStorageError(
            "Futures paper order storage contains non-canonical data."
        )
    return order


def _decode_fill(row: tuple[object, ...]) -> FuturesPaperFill:
    """Rebuild a stored fill on its stored parent order."""
    fill_part = row[: len(_FILL_COLUMNS)]
    order_part = row[len(_FILL_COLUMNS) :]
    if order_part[0] is None:
        raise FuturesPaperTradingStorageError(
            f"Futures paper fill {fill_part[0]!r} has no stored parent order."
        )
    parent = _decode_order(order_part)
    try:
        if not all(isinstance(value, str) for value in fill_part):
            raise TypeError("Futures paper fill columns must all be text.")
        fill_identity, _, fill_quote, filled_at = fill_part
        fill = FuturesPaperFill(
            identity=PaperFillIdentity(fill_identity),
            order_identity=parent.identity,
            intent=parent.intent,
            contracts=parent.intent.contracts,
            fill_quote=QuoteValue(_decimal(fill_quote)),
            filled_at=PointInTime(filled_at),
        )
    except _DECODE_ERRORS as exc:
        raise FuturesPaperTradingStorageError(
            "Futures paper fill storage contains invalid data."
        ) from exc
    if _fill_row(fill) != fill_part:
        raise FuturesPaperTradingStorageError(
            "Futures paper fill storage contains non-canonical data."
        )
    return fill


def _compare_text(left: str, right: str) -> int:
    return (left > right) - (left < right)


def _compare_orders(left: FuturesPaperOrder, right: FuturesPaperOrder) -> int:
    instant = left.intent.decided_at.compare(right.intent.decided_at)
    return instant or _compare_text(left.identity.identity, right.identity.identity)


def _compare_fills(left: FuturesPaperFill, right: FuturesPaperFill) -> int:
    instant = left.filled_at.compare(right.filled_at)
    return instant or _compare_text(left.order_identity.identity, right.order_identity.identity)


def _connect_for_write(database_path: str) -> sqlite3.Connection:
    try:
        connection = sqlite3.connect(database_path, isolation_level=None)
        initialize_futures_paper_trading_schema(connection)
    except sqlite3.Error as exc:
        raise FuturesPaperTradingStorageError(
            "Futures paper trading storage is unavailable."
        ) from exc
    return connection


def _read(database_path: str, sql: str, parameters: tuple[str, ...]) -> list[tuple[object, ...]]:
    try:
        with closing(sqlite3.connect(database_path)) as connection:
            initialize_futures_paper_trading_schema(connection)
            return [tuple(row) for row in connection.execute(sql, parameters).fetchall()]
    except sqlite3.Error as exc:
        raise FuturesPaperTradingStorageError(
            "Futures paper trading storage is unavailable."
        ) from exc


def _select_order(connection: sqlite3.Connection, order_identity: str) -> tuple | None:
    row = connection.execute(
        f"{_SELECT_ORDERS} WHERE order_identity = ?", (order_identity,)
    ).fetchone()
    return None if row is None else tuple(row)


# ---------------------------------------------------------------------------
# Orders
# ---------------------------------------------------------------------------


class SQLiteFuturesPaperOrderStore(FuturesPaperOrderStore):
    """Persist futures paper orders into a local SQLite store, insert only."""

    def __init__(self, database_path: str | Path) -> None:
        self._database_path = str(database_path)

    def store(self, orders: tuple[FuturesPaperOrder, ...]) -> int:
        """Persist a batch atomically, never overwriting a stored order."""
        self._prepare(orders)
        if not orders:
            return 0

        connection = _connect_for_write(self._database_path)
        try:
            connection.execute("BEGIN IMMEDIATE")
            for order in orders:
                self._store_one(connection, order)
            connection.commit()
        except (FuturesPaperOrderConflictError, FuturesPaperTradingStorageError):
            connection.rollback()
            raise
        except sqlite3.Error as exc:
            connection.rollback()
            raise FuturesPaperTradingStorageError(
                "Futures paper trading storage is unavailable."
            ) from exc
        finally:
            connection.close()

        return len(orders)

    @staticmethod
    def _prepare(orders: tuple[FuturesPaperOrder, ...]) -> None:
        """Reject foreign values and in-batch duplicate identities before any write."""
        if not isinstance(orders, tuple):
            raise TypeError("SQLiteFuturesPaperOrderStore orders must be a tuple.")
        seen: set[str] = set()
        for order in orders:
            if not isinstance(order, FuturesPaperOrder):
                raise TypeError(
                    "SQLiteFuturesPaperOrderStore orders must be FuturesPaperOrder values."
                )
            if order.identity.identity in seen:
                raise FuturesPaperOrderConflictError(
                    "Futures paper order batch contains two orders sharing one order identity."
                )
            seen.add(order.identity.identity)

    def _store_one(self, connection: sqlite3.Connection, order: FuturesPaperOrder) -> None:
        existing = self._existing(connection, order.identity.identity)
        if existing is None:
            try:
                connection.execute(_INSERT_ORDER, _order_row(order))
                return
            except sqlite3.IntegrityError:
                # Another writer inserted the identity first; resolve as if seen.
                existing = self._existing(connection, order.identity.identity)
                if existing is None:
                    raise
        if _decode_order(existing) != order:
            raise FuturesPaperOrderConflictError(
                "A different futures paper order is already stored under this order identity."
            )

    @staticmethod
    def _existing(connection: sqlite3.Connection, order_identity: str) -> tuple | None:
        return _select_order(connection, order_identity)


class SQLiteFuturesPaperOrderRepository(FuturesPaperOrderRepository):
    """Retrieve one portfolio's futures paper orders from a local SQLite store.

    SQL filters only on the canonical portfolio identity text. Ordering is by
    decision instant with PointInTime.compare(), then order identity.
    """

    def __init__(self, database_path: str | Path) -> None:
        self._database_path = str(database_path)

    def get_orders(self, query: FuturesPaperOrderQuery) -> tuple[FuturesPaperOrder, ...]:
        """Return one portfolio's futures orders, oldest decision first."""
        if not isinstance(query, FuturesPaperOrderQuery):
            raise TypeError(
                "SQLiteFuturesPaperOrderRepository query must be a FuturesPaperOrderQuery."
            )
        rows = _read(
            self._database_path,
            f"{_SELECT_ORDERS} WHERE portfolio_identity = ?",
            (query.portfolio_identity.identity,),
        )
        orders = [_decode_order(row) for row in rows]
        return tuple(sorted(orders, key=cmp_to_key(_compare_orders)))


# ---------------------------------------------------------------------------
# Fills
# ---------------------------------------------------------------------------


class SQLiteFuturesPaperFillStore(FuturesPaperFillStore):
    """Persist futures paper fills into a local SQLite store, insert only.

    A fill is accepted only when its parent order is stored and carries exactly
    the fill's intent. A missing parent or a differing intent is a conflict: the
    fill would attach to an order that does not exist, or re-attribute one that
    does.
    """

    def __init__(self, database_path: str | Path) -> None:
        self._database_path = str(database_path)

    def store(self, fills: tuple[FuturesPaperFill, ...]) -> int:
        """Persist a batch atomically, never overwriting a stored fill."""
        self._prepare(fills)
        if not fills:
            return 0

        connection = _connect_for_write(self._database_path)
        try:
            connection.execute("BEGIN IMMEDIATE")
            for fill in fills:
                self._store_one(connection, fill)
            connection.commit()
        except (FuturesPaperFillConflictError, FuturesPaperTradingStorageError):
            connection.rollback()
            raise
        except sqlite3.Error as exc:
            connection.rollback()
            raise FuturesPaperTradingStorageError(
                "Futures paper trading storage is unavailable."
            ) from exc
        finally:
            connection.close()

        return len(fills)

    @staticmethod
    def _prepare(fills: tuple[FuturesPaperFill, ...]) -> None:
        """Reject foreign values and in-batch duplicate identities before any write."""
        if not isinstance(fills, tuple):
            raise TypeError("SQLiteFuturesPaperFillStore fills must be a tuple.")
        seen_fills: set[str] = set()
        seen_orders: set[str] = set()
        for fill in fills:
            if not isinstance(fill, FuturesPaperFill):
                raise TypeError(
                    "SQLiteFuturesPaperFillStore fills must be FuturesPaperFill values."
                )
            if fill.identity.identity in seen_fills:
                raise FuturesPaperFillConflictError(
                    "Futures paper fill batch contains two fills sharing one fill identity."
                )
            seen_fills.add(fill.identity.identity)
            if fill.order_identity.identity in seen_orders:
                raise FuturesPaperFillConflictError(
                    "Futures paper fill batch contains two fills for one order."
                )
            seen_orders.add(fill.order_identity.identity)

    def _store_one(self, connection: sqlite3.Connection, fill: FuturesPaperFill) -> None:
        parent_row = _select_order(connection, fill.order_identity.identity)
        if parent_row is None:
            raise FuturesPaperFillConflictError(
                "A futures paper fill cannot be stored before its order is stored."
            )
        if _decode_order(parent_row).intent != fill.intent:
            raise FuturesPaperFillConflictError(
                "A futures paper fill's intent does not match its stored order's intent."
            )

        if self._resolve_existing(connection, fill):
            return
        try:
            connection.execute(_INSERT_FILL, _fill_row(fill))
        except sqlite3.IntegrityError:
            # Another writer inserted first; resolve as if the reads had seen it.
            if not self._resolve_existing(connection, fill):
                raise

    def _resolve_existing(self, connection: sqlite3.Connection, fill: FuturesPaperFill) -> bool:
        """Return True for an idempotent retry, raise on conflict, False when absent."""
        existing = self._existing(connection, fill.identity.identity)
        if existing is not None:
            if _decode_fill(existing) != fill:
                raise FuturesPaperFillConflictError(
                    "A different futures paper fill is already stored under this fill identity."
                )
            return True
        attached = self._attached(connection, fill.order_identity.identity)
        if attached is not None:
            raise FuturesPaperFillConflictError(
                "This futures paper order is already attached to a different fill."
            )
        return False

    @staticmethod
    def _existing(connection: sqlite3.Connection, fill_identity: str) -> tuple | None:
        row = connection.execute(
            f"{_SELECT_FILLS} WHERE f.fill_identity = ?", (fill_identity,)
        ).fetchone()
        return None if row is None else tuple(row)

    @staticmethod
    def _attached(connection: sqlite3.Connection, order_identity: str) -> str | None:
        row = connection.execute(
            "SELECT fill_identity FROM futures_paper_fills WHERE order_identity = ?",
            (order_identity,),
        ).fetchone()
        return None if row is None else row[0]


class SQLiteFuturesPaperFillRepository(FuturesPaperFillRepository):
    """Retrieve one portfolio's futures paper fills from a local SQLite store.

    The portfolio is read from each fill's stored parent order. A fill with no
    parent cannot be attributed to any portfolio, so its presence fails every
    read rather than silently vanishing. Ordering is by fill instant with
    PointInTime.compare(), then order identity. No as-of filtering is applied.
    """

    def __init__(self, database_path: str | Path) -> None:
        self._database_path = str(database_path)

    def get_fills(self, query: FuturesPaperFillQuery) -> tuple[FuturesPaperFill, ...]:
        """Return one portfolio's complete futures fill history, oldest to newest."""
        if not isinstance(query, FuturesPaperFillQuery):
            raise TypeError(
                "SQLiteFuturesPaperFillRepository query must be a FuturesPaperFillQuery."
            )
        rows = _read(
            self._database_path,
            f"{_SELECT_FILLS} WHERE o.order_identity IS NULL OR o.portfolio_identity = ?",
            (query.portfolio_identity.identity,),
        )
        fills = [_decode_fill(row) for row in rows]
        return tuple(sorted(fills, key=cmp_to_key(_compare_fills)))
