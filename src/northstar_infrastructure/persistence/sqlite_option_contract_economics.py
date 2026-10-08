"""SQLite-backed option contract economics store and repository.

Stored economics are frozen reference facts. This module issues INSERT only --
no UPDATE, no REPLACE, no overwriting upsert and no DELETE. Storing economics
equal to a stored row is an idempotent success; storing different economics
under a stored contract is a conflict.

Every column is TEXT holding the canonical form of one Core value, and every
read re-serializes the decoded value and requires it to reproduce the stored
text exactly. A non-canonical or corrupt row is therefore reported, never
normalized into something the writer did not write. The strike and the
point-value amount are Core canonical Decimal spellings and never pass through
a float; the right is exactly ``CALL`` or ``PUT``.

The lookup is exact on the complete option contract: product code, exchange
code, expiration date, strike and right. There is no fallback to a neighbouring
strike, the other right, another expiration or another product, and no futures
table is ever read or written here.

Only the store creates anything. It creates ``option_contract_economics``, when
absent, immediately before its first write. The repository is genuinely
read-only: it opens the database in SQLite read-only mode, never runs the
schema initializer, and treats a database file that does not exist, or one
without the option table, as holding no economics. A read therefore never
creates a file, a table or any other schema object.

Callers supply the economics; nothing here loads them from a provider or holds
a default catalog. A contract with no row is simply absent.
"""

from __future__ import annotations

import sqlite3
from contextlib import closing
from decimal import Decimal
from pathlib import Path

from northstar_application.ports import (
    OptionContractEconomicsConflictError,
    OptionContractEconomicsRepository,
    OptionContractEconomicsStore,
)
from northstar_core.derivatives import ExpirationDate
from northstar_core.foundation.value_objects import Currency, ExchangeCode, Symbol
from northstar_core.options import (
    OptionContract,
    OptionContractEconomics,
    OptionPointValue,
    OptionProductReference,
    OptionRight,
    OptionStrike,
)

from northstar_infrastructure.persistence.sqlite_option_contract_economics_schema import (
    initialize_option_contract_economics_schema,
)

_COLUMNS = (
    "product_code",
    "exchange_code",
    "expiration_date",
    "strike",
    "option_right",
    "point_value_amount",
    "settlement_currency",
)

_SELECT = (
    f"SELECT {', '.join(_COLUMNS)} FROM option_contract_economics "  # noqa: S608
    "WHERE product_code = ? AND exchange_code = ? AND expiration_date = ? "
    "AND strike = ? AND option_right = ?"
)
_TABLE_EXISTS = (
    "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'option_contract_economics'"
)
_INSERT = (
    f"INSERT INTO option_contract_economics ({', '.join(_COLUMNS)}) "  # noqa: S608
    f"VALUES ({', '.join('?' for _ in _COLUMNS)})"
)

_DECODE_ERRORS = (TypeError, ValueError, ArithmeticError)


class OptionContractEconomicsStorageError(RuntimeError):
    """Raised when the local option contract economics store is unavailable or malformed."""


def _key(contract: OptionContract) -> tuple[str, str, str, str, str]:
    product = contract.product
    return (
        product.product_code.value,
        product.exchange_code.value,
        contract.expiration_date.value,
        str(contract.strike.value),
        contract.right.value,
    )


def _row(economics: OptionContractEconomics) -> tuple[str, ...]:
    point_value = economics.point_value
    return (*_key(economics.contract), str(point_value.amount), point_value.currency.value)


def _economics(row: tuple[object, ...]) -> OptionContractEconomics:
    if len(row) != len(_COLUMNS) or not all(isinstance(value, str) for value in row):
        raise TypeError("Option contract economics columns must all be text.")
    product_code, exchange_code, expiration_date, strike, right, amount, currency = row
    return OptionContractEconomics(
        OptionContract(
            OptionProductReference(Symbol(product_code), ExchangeCode(exchange_code)),
            ExpirationDate(expiration_date),
            OptionStrike(Decimal(strike)),
            OptionRight(right),
        ),
        OptionPointValue(Decimal(amount), Currency(currency)),
    )


def _decode(row: tuple[object, ...]) -> OptionContractEconomics:
    """Rebuild stored economics and require them to reproduce their stored text."""
    try:
        economics = _economics(row)
    except _DECODE_ERRORS as exc:
        raise OptionContractEconomicsStorageError(
            "Option contract economics storage contains invalid data."
        ) from exc
    if _row(economics) != row:
        raise OptionContractEconomicsStorageError(
            "Option contract economics storage contains non-canonical data."
        )
    return economics


def _select(connection: sqlite3.Connection, contract: OptionContract) -> tuple | None:
    row = connection.execute(_SELECT, _key(contract)).fetchone()
    return None if row is None else tuple(row)


class SQLiteOptionContractEconomicsStore(OptionContractEconomicsStore):
    """Persist option contract economics into a local SQLite store, insert only."""

    def __init__(self, database_path: str | Path) -> None:
        self._database_path = str(database_path)

    def store(self, economics: tuple[OptionContractEconomics, ...]) -> int:
        """Persist a batch atomically, never overwriting stored economics."""
        self._prepare(economics)
        if not economics:
            return 0

        try:
            connection = sqlite3.connect(self._database_path, isolation_level=None)
            initialize_option_contract_economics_schema(connection)
        except sqlite3.Error as exc:
            raise OptionContractEconomicsStorageError(
                "Option contract economics storage is unavailable."
            ) from exc
        try:
            connection.execute("BEGIN IMMEDIATE")
            for entry in economics:
                self._store_one(connection, entry)
            connection.commit()
        except (OptionContractEconomicsConflictError, OptionContractEconomicsStorageError):
            connection.rollback()
            raise
        except sqlite3.Error as exc:
            connection.rollback()
            raise OptionContractEconomicsStorageError(
                "Option contract economics storage is unavailable."
            ) from exc
        finally:
            connection.close()

        return len(economics)

    @staticmethod
    def _prepare(economics: tuple[OptionContractEconomics, ...]) -> None:
        """Reject foreign values and in-batch duplicate contracts before any write."""
        if not isinstance(economics, tuple):
            raise TypeError("SQLiteOptionContractEconomicsStore economics must be a tuple.")
        seen: set[OptionContract] = set()
        for entry in economics:
            if not isinstance(entry, OptionContractEconomics):
                raise TypeError(
                    "SQLiteOptionContractEconomicsStore economics must be "
                    "OptionContractEconomics values."
                )
            if entry.contract in seen:
                raise OptionContractEconomicsConflictError(
                    "Option contract economics batch contains two entries for one contract."
                )
            seen.add(entry.contract)

    def _store_one(
        self, connection: sqlite3.Connection, economics: OptionContractEconomics
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
            raise OptionContractEconomicsConflictError(
                f"Different option contract economics are already stored for {economics.contract}."
            )

    @staticmethod
    def _existing(connection: sqlite3.Connection, contract: OptionContract) -> tuple | None:
        return _select(connection, contract)


class SQLiteOptionContractEconomicsRepository(OptionContractEconomicsRepository):
    """Retrieve one option contract's economics from a local SQLite store.

    The lookup is exact on product code, exchange code, expiration date, strike
    and right; there is no fuzzy match and no fallback to a neighbouring
    contract. A missing row is None, never a default.

    The lookup is read-only. A database file that does not exist, or one that
    has no ``option_contract_economics`` table yet, holds no economics and
    answers None; neither the file nor the table is ever created here.
    """

    def __init__(self, database_path: str | Path) -> None:
        self._database_path = str(database_path)

    def get_economics(self, contract: OptionContract) -> OptionContractEconomics | None:
        """Return the stored economics for one contract, or None when none are stored."""
        if not isinstance(contract, OptionContract):
            raise TypeError(
                "SQLiteOptionContractEconomicsRepository contract must be an OptionContract."
            )
        path = Path(self._database_path)
        if not path.exists():
            return None
        try:
            # mode=ro can neither create the file nor change its schema.
            read_only = f"{path.absolute().as_uri()}?mode=ro"
            with closing(sqlite3.connect(read_only, uri=True)) as connection:
                if connection.execute(_TABLE_EXISTS).fetchone() is None:
                    return None
                row = _select(connection, contract)
        except sqlite3.Error as exc:
            raise OptionContractEconomicsStorageError(
                "Option contract economics storage is unavailable."
            ) from exc
        if row is None:
            return None
        economics = _decode(row)
        if economics.contract != contract:
            raise OptionContractEconomicsStorageError(
                f"Option contract economics storage returned {economics.contract} for {contract}."
            )
        return economics
