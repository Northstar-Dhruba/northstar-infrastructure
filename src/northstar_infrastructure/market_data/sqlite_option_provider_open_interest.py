"""SQLite-backed preservation of provider-reported option open interest.

Open interest is Infrastructure-only provider evidence, not canonical market
data. Its units -- underlying units or contracts -- the instant it describes and
how a provider revises it are not established, so it is kept apart from
OptionOHLCVBar and is never exposed through Core or Application. It is preserved
now because, once a contract expires, the provider may no longer serve it.

Each record is one provider's reported open interest for one exact option
contract on one trading date. The value is held as the exact Decimal the
provider's number decoded to and stored as that Decimal's exact text: it is not
rounded, normalized, converted to contracts or given a unit.

Records are immutable. This module issues INSERT only. Re-storing a record whose
value is numerically equal is idempotent and keeps the text first stored; a
different value for the same provider, contract and date is a conflict, and the
stored value is left exactly as it was. A batch is stored completely or not at
all, inside ``BEGIN IMMEDIATE``.

The store creates its table immediately before its first write. The repository
is read-only: it opens the database in SQLite read-only mode, never runs the
schema initializer, and answers None when the file, the table or the record does
not exist. No futures table is read or written here.
"""

from __future__ import annotations

import re
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from northstar_core.derivatives import ExpirationDate
from northstar_core.foundation.value_objects import ExchangeCode, Symbol
from northstar_core.options import (
    OptionContract,
    OptionProductReference,
    OptionRight,
    OptionStrike,
)

from northstar_infrastructure.market_data.sqlite_option_market_data_schema import (
    initialize_option_provider_open_interest_schema,
)

_PROVIDER = re.compile(r"[a-z][a-z0-9_]*")

_KEY_COLUMNS = (
    "provider",
    "product_code",
    "exchange_code",
    "expiration_date",
    "strike",
    "option_right",
    "trading_date",
)
_COLUMNS = (*_KEY_COLUMNS, "open_interest_raw")

_SELECT_KEY = (
    f"SELECT {', '.join(_COLUMNS)} FROM option_daily_provider_open_interest "  # noqa: S608
    f"WHERE {' AND '.join(f'{c} = ?' for c in _KEY_COLUMNS)}"
)
_INSERT = (
    f"INSERT INTO option_daily_provider_open_interest ({', '.join(_COLUMNS)}) "  # noqa: S608
    f"VALUES ({', '.join('?' for _ in _COLUMNS)})"
)
_TABLE_EXISTS = (
    "SELECT 1 FROM sqlite_master WHERE type = 'table' "
    "AND name = 'option_daily_provider_open_interest'"
)

_DECODE_ERRORS = (TypeError, ValueError, ArithmeticError)


class OptionOpenInterestStorageError(RuntimeError):
    """Raised when the local provider open-interest evidence is unavailable or malformed."""


class OptionOpenInterestConflictError(ValueError):
    """Raised when a provider reports a different open interest for a stored record."""


@dataclass(frozen=True, slots=True)
class ProviderOptionOpenInterest:
    """One provider's reported open interest for one exact option contract and date.

    Infrastructure-only provider evidence. ``open_interest_raw`` is the exact
    Decimal the provider's number decoded to; no unit is claimed for it.
    """

    provider: str
    contract: OptionContract
    trading_date: date
    open_interest_raw: Decimal

    def __post_init__(self) -> None:
        if not isinstance(self.provider, str) or _PROVIDER.fullmatch(self.provider) is None:
            raise TypeError("ProviderOptionOpenInterest provider must be a canonical name.")
        if not isinstance(self.contract, OptionContract):
            raise TypeError("ProviderOptionOpenInterest contract must be an OptionContract.")
        if isinstance(self.trading_date, datetime) or not isinstance(self.trading_date, date):
            raise TypeError("ProviderOptionOpenInterest trading date must be a plain date.")
        if not isinstance(self.open_interest_raw, Decimal):
            raise TypeError("ProviderOptionOpenInterest value must be a Decimal.")
        if not self.open_interest_raw.is_finite():
            raise ValueError("ProviderOptionOpenInterest value must be finite.")


def _key(provider: str, contract: OptionContract, trading_date: date) -> tuple[str, ...]:
    product = contract.product
    return (
        provider,
        product.product_code.value,
        product.exchange_code.value,
        contract.expiration_date.value,
        str(contract.strike.value),
        contract.right.value,
        trading_date.isoformat(),
    )


def _row(record: ProviderOptionOpenInterest) -> tuple[str, ...]:
    return (
        *_key(record.provider, record.contract, record.trading_date),
        str(record.open_interest_raw),
    )


def _record(row: tuple[object, ...]) -> ProviderOptionOpenInterest:
    if len(row) != len(_COLUMNS) or not all(isinstance(value, str) for value in row):
        raise TypeError("Option open-interest columns must all be text.")
    provider, product, exchange, expiration, strike, right, trading_date, raw = row
    return ProviderOptionOpenInterest(
        provider=provider,
        contract=OptionContract(
            OptionProductReference(Symbol(product), ExchangeCode(exchange)),
            ExpirationDate(expiration),
            OptionStrike(Decimal(strike)),
            OptionRight(right),
        ),
        trading_date=date.fromisoformat(trading_date),
        open_interest_raw=Decimal(raw),
    )


def _decode(row: tuple[object, ...]) -> ProviderOptionOpenInterest:
    """Rebuild a stored record and require it to reproduce its stored text."""
    try:
        record = _record(row)
    except _DECODE_ERRORS as exc:
        raise OptionOpenInterestStorageError(
            "Option open-interest storage contains invalid data."
        ) from exc
    if _row(record) != row:
        raise OptionOpenInterestStorageError(
            "Option open-interest storage contains non-canonical data."
        )
    return record


class SQLiteOptionProviderOpenInterestStore:
    """Preserve provider-reported option open interest, insert only."""

    def __init__(self, database_path: str | Path) -> None:
        self._database_path = str(database_path)

    def store(self, records: tuple[ProviderOptionOpenInterest, ...]) -> int:
        """Persist a batch atomically and return its length; never overwrite."""
        if not isinstance(records, tuple):
            raise TypeError("SQLiteOptionProviderOpenInterestStore records must be a tuple.")
        seen: set[tuple[str, ...]] = set()
        for record in records:
            if not isinstance(record, ProviderOptionOpenInterest):
                raise TypeError(
                    "SQLiteOptionProviderOpenInterestStore records must be "
                    "ProviderOptionOpenInterest values."
                )
            key = _key(record.provider, record.contract, record.trading_date)
            if key in seen:
                raise OptionOpenInterestConflictError(
                    "Option open-interest batch contains two records for one key."
                )
            seen.add(key)
        if not records:
            return 0

        try:
            connection = sqlite3.connect(self._database_path, isolation_level=None)
            initialize_option_provider_open_interest_schema(connection)
        except sqlite3.Error as exc:
            raise OptionOpenInterestStorageError(
                "Option open-interest storage is unavailable."
            ) from exc
        try:
            connection.execute("BEGIN IMMEDIATE")
            for record in records:
                self._store_one(connection, record)
            connection.commit()
        except (OptionOpenInterestConflictError, OptionOpenInterestStorageError):
            connection.rollback()
            raise
        except sqlite3.Error as exc:
            connection.rollback()
            raise OptionOpenInterestStorageError(
                "Option open-interest storage is unavailable."
            ) from exc
        finally:
            connection.close()
        return len(records)

    @staticmethod
    def _store_one(connection: sqlite3.Connection, record: ProviderOptionOpenInterest) -> None:
        key = _key(record.provider, record.contract, record.trading_date)
        existing = connection.execute(_SELECT_KEY, key).fetchone()
        if existing is None:
            try:
                connection.execute(_INSERT, _row(record))
                return
            except sqlite3.IntegrityError:
                existing = connection.execute(_SELECT_KEY, key).fetchone()
                if existing is None:
                    raise
        stored = _decode(tuple(existing))
        if stored.open_interest_raw != record.open_interest_raw:
            raise OptionOpenInterestConflictError(
                f"{record.provider} already reported open interest {stored.open_interest_raw} "
                f"for {record.contract} on {record.trading_date.isoformat()}, not "
                f"{record.open_interest_raw}."
            )


class SQLiteOptionProviderOpenInterestRepository:
    """Look up one preserved provider open-interest record, read-only."""

    def __init__(self, database_path: str | Path) -> None:
        self._database_path = str(database_path)

    def get(
        self, provider: str, contract: OptionContract, trading_date: date
    ) -> ProviderOptionOpenInterest | None:
        """Return the preserved record, or None when none is stored."""
        if not isinstance(provider, str) or _PROVIDER.fullmatch(provider) is None:
            raise TypeError("SQLiteOptionProviderOpenInterestRepository provider is not canonical.")
        if not isinstance(contract, OptionContract):
            raise TypeError(
                "SQLiteOptionProviderOpenInterestRepository contract must be an OptionContract."
            )
        if isinstance(trading_date, datetime) or not isinstance(trading_date, date):
            raise TypeError(
                "SQLiteOptionProviderOpenInterestRepository trading date must be a plain date."
            )
        path = Path(self._database_path)
        if not path.exists():
            return None
        try:
            read_only = f"{path.absolute().as_uri()}?mode=ro"
            with closing(sqlite3.connect(read_only, uri=True)) as connection:
                if connection.execute(_TABLE_EXISTS).fetchone() is None:
                    return None
                row = connection.execute(_SELECT_KEY, _key(provider, contract, trading_date))
                row = row.fetchone()
        except sqlite3.Error as exc:
            raise OptionOpenInterestStorageError(
                "Option open-interest storage is unavailable."
            ) from exc
        return None if row is None else _decode(tuple(row))
