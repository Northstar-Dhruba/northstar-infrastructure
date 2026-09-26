"""SQLite-backed futures product economics store and repository.

Stored economics are frozen reference facts. This module issues INSERT only --
no UPDATE, no REPLACE, no overwriting upsert and no DELETE. Storing economics
equal to a stored row is an idempotent success; storing different economics
under a stored product reference is a conflict.

Every column is TEXT holding the canonical form of one Core value, and every
read re-serializes the decoded value and requires it to reproduce the stored
text exactly. A non-canonical or corrupt row is therefore reported, never
normalized into something the writer did not write. The point-value amount is
the Core canonical Decimal spelling and never passes through a float.

Callers supply the economics; nothing here loads them from a provider or holds
a default catalog. A product with no row is simply absent.
"""

from __future__ import annotations

import sqlite3
from contextlib import closing
from decimal import Decimal
from pathlib import Path

from northstar_application.ports import (
    FuturesProductEconomicsConflictError,
    FuturesProductEconomicsRepository,
    FuturesProductEconomicsStore,
)
from northstar_core.foundation.value_objects import Currency, ExchangeCode, Symbol
from northstar_core.futures import (
    FuturesPointValue,
    FuturesProductEconomics,
    FuturesProductReference,
)

from northstar_infrastructure.persistence.sqlite_futures_product_economics_schema import (
    initialize_futures_product_economics_schema,
)

_COLUMNS = ("product_code", "exchange_code", "point_value_amount", "settlement_currency")

_SELECT = (
    f"SELECT {', '.join(_COLUMNS)} FROM futures_product_economics "  # noqa: S608
    "WHERE product_code = ? AND exchange_code = ?"
)
_INSERT = (
    f"INSERT INTO futures_product_economics ({', '.join(_COLUMNS)}) "  # noqa: S608
    f"VALUES ({', '.join('?' for _ in _COLUMNS)})"
)

_DECODE_ERRORS = (TypeError, ValueError, ArithmeticError)


class FuturesProductEconomicsStorageError(RuntimeError):
    """Raised when the local futures product economics store is unavailable or malformed."""


def _key(reference: FuturesProductReference) -> tuple[str, str]:
    return (reference.product_code.value, reference.exchange_code.value)


def _row(economics: FuturesProductEconomics) -> tuple[str, ...]:
    point_value = economics.point_value
    return (*_key(economics.reference), str(point_value.amount), point_value.currency.value)


def _economics(row: tuple[object, ...]) -> FuturesProductEconomics:
    if len(row) != len(_COLUMNS) or not all(isinstance(value, str) for value in row):
        raise TypeError("Futures product economics columns must all be text.")
    product_code, exchange_code, amount, currency = row
    return FuturesProductEconomics(
        FuturesProductReference(Symbol(product_code), ExchangeCode(exchange_code)),
        FuturesPointValue(Decimal(amount), Currency(currency)),
    )


def _decode(row: tuple[object, ...]) -> FuturesProductEconomics:
    """Rebuild stored economics and require them to reproduce their stored text."""
    try:
        economics = _economics(row)
    except _DECODE_ERRORS as exc:
        raise FuturesProductEconomicsStorageError(
            "Futures product economics storage contains invalid data."
        ) from exc
    if _row(economics) != row:
        raise FuturesProductEconomicsStorageError(
            "Futures product economics storage contains non-canonical data."
        )
    return economics


def _select(connection: sqlite3.Connection, reference: FuturesProductReference) -> tuple | None:
    row = connection.execute(_SELECT, _key(reference)).fetchone()
    return None if row is None else tuple(row)


class SQLiteFuturesProductEconomicsStore(FuturesProductEconomicsStore):
    """Persist futures product economics into a local SQLite store, insert only."""

    def __init__(self, database_path: str | Path) -> None:
        self._database_path = str(database_path)

    def store(self, economics: tuple[FuturesProductEconomics, ...]) -> int:
        """Persist a batch atomically, never overwriting stored economics."""
        self._prepare(economics)
        if not economics:
            return 0

        try:
            connection = sqlite3.connect(self._database_path, isolation_level=None)
            initialize_futures_product_economics_schema(connection)
        except sqlite3.Error as exc:
            raise FuturesProductEconomicsStorageError(
                "Futures product economics storage is unavailable."
            ) from exc
        try:
            connection.execute("BEGIN IMMEDIATE")
            for entry in economics:
                self._store_one(connection, entry)
            connection.commit()
        except (FuturesProductEconomicsConflictError, FuturesProductEconomicsStorageError):
            connection.rollback()
            raise
        except sqlite3.Error as exc:
            connection.rollback()
            raise FuturesProductEconomicsStorageError(
                "Futures product economics storage is unavailable."
            ) from exc
        finally:
            connection.close()

        return len(economics)

    @staticmethod
    def _prepare(economics: tuple[FuturesProductEconomics, ...]) -> None:
        """Reject foreign values and in-batch duplicate references before any write."""
        if not isinstance(economics, tuple):
            raise TypeError("SQLiteFuturesProductEconomicsStore economics must be a tuple.")
        seen: set[FuturesProductReference] = set()
        for entry in economics:
            if not isinstance(entry, FuturesProductEconomics):
                raise TypeError(
                    "SQLiteFuturesProductEconomicsStore economics must be "
                    "FuturesProductEconomics values."
                )
            if entry.reference in seen:
                raise FuturesProductEconomicsConflictError(
                    "Futures product economics batch contains two entries for one product."
                )
            seen.add(entry.reference)

    def _store_one(
        self, connection: sqlite3.Connection, economics: FuturesProductEconomics
    ) -> None:
        existing = self._existing(connection, economics.reference)
        if existing is None:
            try:
                connection.execute(_INSERT, _row(economics))
                return
            except sqlite3.IntegrityError:
                # Another writer inserted the product first; resolve as if seen.
                existing = self._existing(connection, economics.reference)
                if existing is None:
                    raise
        if _decode(existing) != economics:
            raise FuturesProductEconomicsConflictError(
                f"Different futures product economics are already stored for {economics.reference}."
            )

    @staticmethod
    def _existing(
        connection: sqlite3.Connection, reference: FuturesProductReference
    ) -> tuple | None:
        return _select(connection, reference)


class SQLiteFuturesProductEconomicsRepository(FuturesProductEconomicsRepository):
    """Retrieve one product's economics from a local SQLite store.

    The lookup is exact on product code and exchange code; there is no
    expiry, fuzzy match or fallback. A missing row is None, never a default.
    """

    def __init__(self, database_path: str | Path) -> None:
        self._database_path = str(database_path)

    def get_economics(self, reference: FuturesProductReference) -> FuturesProductEconomics | None:
        """Return the stored economics for one product, or None when none are stored."""
        if not isinstance(reference, FuturesProductReference):
            raise TypeError(
                "SQLiteFuturesProductEconomicsRepository reference must be a "
                "FuturesProductReference."
            )
        try:
            with closing(sqlite3.connect(self._database_path)) as connection:
                initialize_futures_product_economics_schema(connection)
                row = _select(connection, reference)
        except sqlite3.Error as exc:
            raise FuturesProductEconomicsStorageError(
                "Futures product economics storage is unavailable."
            ) from exc
        if row is None:
            return None
        economics = _decode(row)
        if economics.reference != reference:
            raise FuturesProductEconomicsStorageError(
                f"Futures product economics storage returned {economics.reference} for {reference}."
            )
        return economics
