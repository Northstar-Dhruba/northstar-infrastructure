"""SQLite-backed futures contract economics store and repository.

Stored economics are frozen reference facts. This module issues INSERT only --
no UPDATE, no REPLACE, no overwriting upsert and no DELETE. Storing economics
equal to a stored row is an idempotent success; storing different economics
under a stored contract is a conflict.

Every column is TEXT holding the canonical form of one Core value, and every
read re-serializes the decoded value and requires it to reproduce the stored
text exactly. A non-canonical or corrupt row is therefore reported, never
normalized into something the writer did not write. The point-value amount is
the Core canonical Decimal spelling and never passes through a float.

The lookup is exact on the complete contract. There is no fallback to another
expiration, to the latest expiration of the product or to product-level
economics: the ``futures_product_economics`` table is never read here.

Callers supply the economics; nothing here loads them from a provider or holds
a default catalog. A contract with no row is simply absent.
"""

from __future__ import annotations

import sqlite3
from contextlib import closing
from decimal import Decimal
from pathlib import Path

from northstar_application.ports import (
    FuturesContractEconomicsConflictError,
    FuturesContractEconomicsRepository,
    FuturesContractEconomicsStore,
)
from northstar_core.derivatives import ExpirationDate
from northstar_core.foundation.value_objects import Currency, ExchangeCode, Symbol
from northstar_core.futures import (
    FuturesContract,
    FuturesContractEconomics,
    FuturesPointValue,
    FuturesProductReference,
)

from northstar_infrastructure.persistence.sqlite_futures_contract_economics_schema import (
    initialize_futures_contract_economics_schema,
)

_COLUMNS = (
    "product_code",
    "exchange_code",
    "expiration_date",
    "point_value_amount",
    "settlement_currency",
)

_SELECT = (
    f"SELECT {', '.join(_COLUMNS)} FROM futures_contract_economics "  # noqa: S608
    "WHERE product_code = ? AND exchange_code = ? AND expiration_date = ?"
)
_INSERT = (
    f"INSERT INTO futures_contract_economics ({', '.join(_COLUMNS)}) "  # noqa: S608
    f"VALUES ({', '.join('?' for _ in _COLUMNS)})"
)

_DECODE_ERRORS = (TypeError, ValueError, ArithmeticError)


class FuturesContractEconomicsStorageError(RuntimeError):
    """Raised when the local futures contract economics store is unavailable or malformed."""


def _key(contract: FuturesContract) -> tuple[str, str, str]:
    product = contract.product
    return (
        product.product_code.value,
        product.exchange_code.value,
        contract.expiration_date.value,
    )


def _row(economics: FuturesContractEconomics) -> tuple[str, ...]:
    point_value = economics.point_value
    return (*_key(economics.contract), str(point_value.amount), point_value.currency.value)


def _economics(row: tuple[object, ...]) -> FuturesContractEconomics:
    if len(row) != len(_COLUMNS) or not all(isinstance(value, str) for value in row):
        raise TypeError("Futures contract economics columns must all be text.")
    product_code, exchange_code, expiration_date, amount, currency = row
    return FuturesContractEconomics(
        FuturesContract(
            FuturesProductReference(Symbol(product_code), ExchangeCode(exchange_code)),
            ExpirationDate(expiration_date),
        ),
        FuturesPointValue(Decimal(amount), Currency(currency)),
    )


def _decode(row: tuple[object, ...]) -> FuturesContractEconomics:
    """Rebuild stored economics and require them to reproduce their stored text."""
    try:
        economics = _economics(row)
    except _DECODE_ERRORS as exc:
        raise FuturesContractEconomicsStorageError(
            "Futures contract economics storage contains invalid data."
        ) from exc
    if _row(economics) != row:
        raise FuturesContractEconomicsStorageError(
            "Futures contract economics storage contains non-canonical data."
        )
    return economics


def _select(connection: sqlite3.Connection, contract: FuturesContract) -> tuple | None:
    row = connection.execute(_SELECT, _key(contract)).fetchone()
    return None if row is None else tuple(row)


class SQLiteFuturesContractEconomicsStore(FuturesContractEconomicsStore):
    """Persist futures contract economics into a local SQLite store, insert only."""

    def __init__(self, database_path: str | Path) -> None:
        self._database_path = str(database_path)

    def store(self, economics: tuple[FuturesContractEconomics, ...]) -> int:
        """Persist a batch atomically, never overwriting stored economics."""
        self._prepare(economics)
        if not economics:
            return 0

        try:
            connection = sqlite3.connect(self._database_path, isolation_level=None)
            initialize_futures_contract_economics_schema(connection)
        except sqlite3.Error as exc:
            raise FuturesContractEconomicsStorageError(
                "Futures contract economics storage is unavailable."
            ) from exc
        try:
            connection.execute("BEGIN IMMEDIATE")
            for entry in economics:
                self._store_one(connection, entry)
            connection.commit()
        except (FuturesContractEconomicsConflictError, FuturesContractEconomicsStorageError):
            connection.rollback()
            raise
        except sqlite3.Error as exc:
            connection.rollback()
            raise FuturesContractEconomicsStorageError(
                "Futures contract economics storage is unavailable."
            ) from exc
        finally:
            connection.close()

        return len(economics)

    @staticmethod
    def _prepare(economics: tuple[FuturesContractEconomics, ...]) -> None:
        """Reject foreign values and in-batch duplicate contracts before any write."""
        if not isinstance(economics, tuple):
            raise TypeError("SQLiteFuturesContractEconomicsStore economics must be a tuple.")
        seen: set[FuturesContract] = set()
        for entry in economics:
            if not isinstance(entry, FuturesContractEconomics):
                raise TypeError(
                    "SQLiteFuturesContractEconomicsStore economics must be "
                    "FuturesContractEconomics values."
                )
            if entry.contract in seen:
                raise FuturesContractEconomicsConflictError(
                    "Futures contract economics batch contains two entries for one contract."
                )
            seen.add(entry.contract)

    def _store_one(
        self, connection: sqlite3.Connection, economics: FuturesContractEconomics
    ) -> None:
        existing = self._existing(connection, economics.contract)
        if existing is None:
            try:
                connection.execute(_INSERT, _row(economics))
                return
            except sqlite3.IntegrityError:
                # Another writer inserted the contract first; resolve as if seen.
                existing = self._existing(connection, economics.contract)
                if existing is None:
                    raise
        if _decode(existing) != economics:
            raise FuturesContractEconomicsConflictError(
                f"Different futures contract economics are already stored for {economics.contract}."
            )

    @staticmethod
    def _existing(connection: sqlite3.Connection, contract: FuturesContract) -> tuple | None:
        return _select(connection, contract)


class SQLiteFuturesContractEconomicsRepository(FuturesContractEconomicsRepository):
    """Retrieve one contract's economics from a local SQLite store.

    The lookup is exact on product code, exchange code and expiration date;
    there is no fuzzy match and no fallback to another expiration or to product
    economics. A missing row is None, never a default.
    """

    def __init__(self, database_path: str | Path) -> None:
        self._database_path = str(database_path)

    def get_economics(self, contract: FuturesContract) -> FuturesContractEconomics | None:
        """Return the stored economics for one contract, or None when none are stored."""
        if not isinstance(contract, FuturesContract):
            raise TypeError(
                "SQLiteFuturesContractEconomicsRepository contract must be a FuturesContract."
            )
        try:
            with closing(sqlite3.connect(self._database_path)) as connection:
                initialize_futures_contract_economics_schema(connection)
                row = _select(connection, contract)
        except sqlite3.Error as exc:
            raise FuturesContractEconomicsStorageError(
                "Futures contract economics storage is unavailable."
            ) from exc
        if row is None:
            return None
        economics = _decode(row)
        if economics.contract != contract:
            raise FuturesContractEconomicsStorageError(
                f"Futures contract economics storage returned {economics.contract} for {contract}."
            )
        return economics
