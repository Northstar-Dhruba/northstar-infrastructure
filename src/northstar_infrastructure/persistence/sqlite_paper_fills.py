"""SQLite-backed paper fill store and repository.

Completed fills are append-only. A fill records something that already
happened, and holdings are folded from the history on demand, so a stored fill
is never rewritten: this module issues INSERT only, and contains no UPDATE,
REPLACE or upsert of any kind.

Every field is stored in its own TEXT column. Decimal amounts are persisted as
text and never as floats, so a quantity or price round-trips exactly, and no
pickled state is used.

Identities are never generated here. A fill arrives with the identity its
producer assigned; Infrastructure only records it.
"""

from __future__ import annotations

import sqlite3
from decimal import Decimal
from functools import cmp_to_key
from pathlib import Path

from northstar_application.ports import (
    PaperFillConflictError,
    PaperFillQuery,
    PaperFillRepository,
    PaperFillStore,
)
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

from northstar_infrastructure.persistence.sqlite_paper_fill_schema import (
    initialize_paper_fill_schema,
)

_COLUMNS = (
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
)

_SELECT_ALL = f"SELECT {', '.join(_COLUMNS)} FROM paper_fills"  # noqa: S608 - fixed identifiers


class PaperFillStorageError(RuntimeError):
    """Raised when the local paper fill store is unavailable or malformed."""


# ---------------------------------------------------------------------------
# Serialization
# ---------------------------------------------------------------------------


def _row(fill: PaperFill) -> tuple[str, ...]:
    """Return one fill as its stored column values, all text."""
    intent = fill.intent
    return (
        fill.identity.identity,
        fill.order_identity.identity,
        intent.portfolio_identity.identity,
        intent.listing_reference.symbol.value,
        intent.listing_reference.exchange_code.value,
        intent.side.value,
        intent.strategy_identity.identity,
        intent.decided_at.value,
        str(fill.quantity.value),
        str(fill.price.amount),
        fill.price.currency.value,
        fill.filled_at.value,
    )


def _fill(row: tuple[str, ...]) -> PaperFill:
    """Rebuild one fill from its stored column values.

    PaperFill requires its own quantity to equal the intended quantity, so one
    stored quantity reconstructs both without risk of them disagreeing.
    """
    (
        fill_identity,
        order_identity,
        portfolio_identity,
        symbol,
        exchange_code,
        side,
        strategy_identity,
        decided_at,
        quantity,
        price_amount,
        price_currency,
        filled_at,
    ) = row

    intent = ExecutionIntent(
        portfolio_identity=PaperPortfolioIdentity(portfolio_identity),
        listing_reference=ListingReference(Symbol(symbol), ExchangeCode(exchange_code)),
        side=OrderSide(side),
        quantity=Quantity(Decimal(quantity)),
        strategy_identity=StrategyIdentity(strategy_identity),
        decided_at=PointInTime(decided_at),
    )
    return PaperFill(
        identity=PaperFillIdentity(fill_identity),
        order_identity=PaperOrderIdentity(order_identity),
        intent=intent,
        quantity=Quantity(Decimal(quantity)),
        price=Price(Decimal(price_amount), Currency(price_currency)),
        filled_at=PointInTime(filled_at),
    )


def _compare_fills(left: PaperFill, right: PaperFill) -> int:
    """Order fills chronologically, then by fill identity.

    Fill instants are compared with PointInTime.compare() rather than by stored
    text. A canonical instant omits fractional seconds when they are zero, so
    ``...T16:00:00Z`` sorts after ``...T16:00:00.1Z`` lexicographically while
    being the earlier instant.
    """
    instant = left.filled_at.compare(right.filled_at)
    if instant:
        return instant
    left_identity = left.identity.identity
    right_identity = right.identity.identity
    return (left_identity > right_identity) - (left_identity < right_identity)


# ---------------------------------------------------------------------------
# Adapters
# ---------------------------------------------------------------------------


class SQLitePaperFillStore(PaperFillStore):
    """Persist completed paper fills into a local SQLite store."""

    def __init__(self, database_path: str | Path) -> None:
        self._database_path = str(database_path)

    def store(self, fills: tuple[PaperFill, ...]) -> int:
        """Persist a batch atomically, never overwriting a stored fill."""
        if not fills:
            return 0

        self._validate_batch(fills)

        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            for fill in fills:
                self._store_one(connection, fill)
            connection.commit()
        except PaperFillConflictError:
            connection.rollback()
            raise
        except sqlite3.IntegrityError as exc:
            connection.rollback()
            raise PaperFillConflictError(
                "A paper fill violating a storage uniqueness constraint was rejected."
            ) from exc
        except sqlite3.Error as exc:
            connection.rollback()
            raise PaperFillStorageError("Paper fill storage is unavailable.") from exc
        finally:
            connection.close()

        return len(fills)

    @staticmethod
    def _validate_batch(fills: tuple[PaperFill, ...]) -> None:
        seen_fills: set[str] = set()
        seen_orders: set[str] = set()
        for fill in fills:
            if fill.identity.identity in seen_fills:
                raise PaperFillConflictError(
                    "Paper fill batch contains two fills sharing one fill identity."
                )
            seen_fills.add(fill.identity.identity)
            if fill.order_identity.identity in seen_orders:
                raise PaperFillConflictError("Paper fill batch contains two fills for one order.")
            seen_orders.add(fill.order_identity.identity)

    def _store_one(self, connection: sqlite3.Connection, fill: PaperFill) -> None:
        existing = connection.execute(
            f"{_SELECT_ALL} WHERE fill_identity = ?", (fill.identity.identity,)
        ).fetchone()
        if existing is not None:
            if self._decode(tuple(existing)) != fill:
                raise PaperFillConflictError(
                    "A different paper fill is already stored under this fill identity."
                )
            return

        attached = connection.execute(
            "SELECT fill_identity FROM paper_fills WHERE order_identity = ?",
            (fill.order_identity.identity,),
        ).fetchone()
        if attached is not None:
            raise PaperFillConflictError(
                "This order is already attached to a different paper fill."
            )

        placeholders = ", ".join("?" for _ in _COLUMNS)
        connection.execute(
            f"INSERT INTO paper_fills ({', '.join(_COLUMNS)}) VALUES ({placeholders})",  # noqa: S608
            _row(fill),
        )

    def _connect(self) -> sqlite3.Connection:
        try:
            connection = sqlite3.connect(self._database_path, isolation_level=None)
            initialize_paper_fill_schema(connection)
        except sqlite3.Error as exc:
            raise PaperFillStorageError("Paper fill storage is unavailable.") from exc
        return connection

    @staticmethod
    def _decode(row: tuple[str, ...]) -> PaperFill:
        try:
            return _fill(row)
        except (TypeError, ValueError) as exc:
            raise PaperFillStorageError("Paper fill storage contains invalid data.") from exc


class SQLitePaperFillRepository(PaperFillRepository):
    """Retrieve one portfolio's paper fill history from a local SQLite store.

    Ordering is applied in Python with PointInTime.compare() semantics rather
    than by stored text, because a canonical instant omits fractional seconds
    when they are zero and is therefore not lexicographically chronological.
    """

    def __init__(self, database_path: str | Path) -> None:
        self._database_path = str(database_path)

    def get_fills(self, query: PaperFillQuery) -> tuple[PaperFill, ...]:
        """Return one portfolio's complete fill history, oldest to newest."""
        if not isinstance(query, PaperFillQuery):
            raise TypeError("SQLitePaperFillRepository query must be a PaperFillQuery.")

        connection = None
        try:
            connection = sqlite3.connect(self._database_path)
            initialize_paper_fill_schema(connection)
            rows = connection.execute(
                f"{_SELECT_ALL} WHERE portfolio_identity = ?",
                (query.portfolio_identity.identity,),
            ).fetchall()
        except sqlite3.Error as exc:
            raise PaperFillStorageError("Paper fill storage is unavailable.") from exc
        finally:
            if connection is not None:
                connection.close()

        try:
            fills = [_fill(tuple(row)) for row in rows]
        except (TypeError, ValueError) as exc:
            raise PaperFillStorageError("Paper fill storage contains invalid data.") from exc

        return tuple(sorted(fills, key=cmp_to_key(_compare_fills)))
