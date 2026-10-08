"""SQLite-backed option provider-listing reference: one atomic store, one read-only repository.

Stored snapshots and listings are frozen reference facts. This module issues
INSERT only -- no UPDATE, no REPLACE, no overwriting upsert and no DELETE.

What one sync stores
--------------------
One downloaded master and every listing read from it are stored in a single
``BEGIN IMMEDIATE`` transaction, so a sync either persists the snapshot with
its whole listing batch or nothing at all.

- A snapshot already stored under the same provider and body hash is the same
  body fetched again. Its first fetch instant is kept; only the counts, which
  the body determines, must agree.
- A listing already stored with the same instrument key and lot is the same
  mapping observed again: a no-op that keeps the snapshot and instant which
  first established it.
- A stored contract reported with a different key or lot, or a stored key
  reported for a different contract, is a conflict. The whole sync is rolled
  back and the stored mapping is left exactly as it was.

Every column is canonical TEXT, and every read re-serializes the decoded values
and requires them to reproduce the stored text exactly. A corrupt or
non-canonical row is reported, never normalized.

The repository is read-only
---------------------------
It opens the database in SQLite read-only mode and never runs the schema
initializer. A database file that does not exist, or one without the listing
table, holds no listings and answers None; a lookup never creates a file, a
table or any other schema object. No futures table is read or written here.
"""

from __future__ import annotations

import re
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

from northstar_core.derivatives import ExpirationDate
from northstar_core.foundation.value_objects import ExchangeCode, PointInTime, Symbol
from northstar_core.options import (
    OptionContract,
    OptionProductReference,
    OptionRight,
    OptionStrike,
)

from northstar_infrastructure.market_data.sqlite_option_listing_schema import (
    initialize_option_listing_schema,
)
from northstar_infrastructure.market_data.upstox_option_instrument_master import (
    UpstoxOptionListing,
    UpstoxOptionMasterSnapshot,
)

_PROVIDER = re.compile(r"[a-z][a-z0-9_]*")
_SHA256 = re.compile(r"[0-9a-f]{64}")
_COUNT = re.compile(r"0|[1-9][0-9]*")
_POSITIVE = re.compile(r"[1-9][0-9]*")

_SNAPSHOT_COLUMNS = (
    "provider",
    "snapshot_sha256",
    "source_url",
    "first_fetched_at",
    "record_count",
    "option_record_count",
)
_LISTING_COLUMNS = (
    "provider",
    "product_code",
    "exchange_code",
    "expiration_date",
    "strike",
    "option_right",
    "instrument_key",
    "exchange_lot_size",
    "established_snapshot_sha256",
    "established_at",
)
_CONTRACT_WHERE = (
    "provider = ? AND product_code = ? AND exchange_code = ? AND expiration_date = ? "
    "AND strike = ? AND option_right = ?"
)

_SELECT_SNAPSHOT = (
    f"SELECT {', '.join(_SNAPSHOT_COLUMNS)} FROM option_listing_snapshots "  # noqa: S608
    "WHERE provider = ? AND snapshot_sha256 = ?"
)
_INSERT_SNAPSHOT = (
    f"INSERT INTO option_listing_snapshots ({', '.join(_SNAPSHOT_COLUMNS)}) "  # noqa: S608
    f"VALUES ({', '.join('?' for _ in _SNAPSHOT_COLUMNS)})"
)
_SELECT_LISTING = (
    f"SELECT {', '.join(_LISTING_COLUMNS)} FROM option_provider_listings "  # noqa: S608
    f"WHERE {_CONTRACT_WHERE}"
)
_SELECT_LISTING_BY_KEY = (
    f"SELECT {', '.join(_LISTING_COLUMNS)} FROM option_provider_listings "  # noqa: S608
    "WHERE provider = ? AND instrument_key = ?"
)
_INSERT_LISTING = (
    f"INSERT INTO option_provider_listings ({', '.join(_LISTING_COLUMNS)}) "  # noqa: S608
    f"VALUES ({', '.join('?' for _ in _LISTING_COLUMNS)})"
)
_TABLE_EXISTS = (
    "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'option_provider_listings'"
)

_DECODE_ERRORS = (TypeError, ValueError, ArithmeticError)


class OptionListingStorageError(RuntimeError):
    """Raised when the local option listing reference is unavailable or malformed."""


class OptionListingConflictError(ValueError):
    """Raised when a provider observation contradicts a stored snapshot or listing."""


@dataclass(frozen=True, slots=True)
class StoredOptionProviderListing:
    """One stored provider mapping of an exact option contract, with its provenance."""

    provider: str
    contract: OptionContract
    instrument_key: str
    exchange_lot_size: int
    established_snapshot_sha256: str
    established_at: PointInTime


# ---------------------------------------------------------------------------
# Canonical text
# ---------------------------------------------------------------------------


def _contract_key(provider: str, contract: OptionContract) -> tuple[str, ...]:
    product = contract.product
    return (
        provider,
        product.product_code.value,
        product.exchange_code.value,
        contract.expiration_date.value,
        str(contract.strike.value),
        contract.right.value,
    )


def _listing_row(listing: StoredOptionProviderListing) -> tuple[str, ...]:
    return (
        *_contract_key(listing.provider, listing.contract),
        listing.instrument_key,
        str(listing.exchange_lot_size),
        listing.established_snapshot_sha256,
        listing.established_at.value,
    )


def _require(pattern: re.Pattern[str], value: object, name: str) -> str:
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        raise ValueError(f"{name} is not canonical.")
    return value


def _instrument_key(value: object) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError("instrument key is not canonical.")
    return value


def _listing(row: tuple[object, ...]) -> StoredOptionProviderListing:
    if len(row) != len(_LISTING_COLUMNS) or not all(isinstance(value, str) for value in row):
        raise TypeError("Option listing columns must all be text.")
    provider, product, exchange, expiration, strike, right, key, lot, sha, at = row
    return StoredOptionProviderListing(
        provider=_require(_PROVIDER, provider, "provider"),
        contract=OptionContract(
            OptionProductReference(Symbol(product), ExchangeCode(exchange)),
            ExpirationDate(expiration),
            OptionStrike(Decimal(strike)),
            OptionRight(right),
        ),
        instrument_key=_instrument_key(key),
        exchange_lot_size=int(_require(_POSITIVE, lot, "exchange lot size")),
        established_snapshot_sha256=_require(_SHA256, sha, "snapshot hash"),
        established_at=PointInTime(at),
    )


def _decode_listing(row: tuple[object, ...]) -> StoredOptionProviderListing:
    """Rebuild a stored listing and require it to reproduce its stored text."""
    try:
        listing = _listing(row)
    except _DECODE_ERRORS as exc:
        raise OptionListingStorageError("Option listing storage contains invalid data.") from exc
    if _listing_row(listing) != row:
        raise OptionListingStorageError("Option listing storage contains non-canonical data.")
    return listing


def _decode_snapshot_counts(row: tuple[object, ...]) -> tuple[int, int]:
    """Validate a stored snapshot row and return its two counts."""
    try:
        if len(row) != len(_SNAPSHOT_COLUMNS) or not all(isinstance(v, str) for v in row):
            raise TypeError("Option listing snapshot columns must all be text.")
        provider, sha, url, fetched, records, options = row
        _require(_PROVIDER, provider, "provider")
        _require(_SHA256, sha, "snapshot hash")
        if not url.strip():
            raise ValueError("source URL is empty.")
        if PointInTime(fetched).value != fetched:
            raise ValueError("first fetch instant is not canonical.")
        return (
            int(_require(_COUNT, records, "record count")),
            int(_require(_COUNT, options, "option record count")),
        )
    except _DECODE_ERRORS as exc:
        raise OptionListingStorageError(
            "Option listing snapshot storage contains invalid data."
        ) from exc


def _validate_snapshot(snapshot: UpstoxOptionMasterSnapshot) -> None:
    """Refuse a snapshot that could never be stored canonically, before any write."""
    if not isinstance(snapshot, UpstoxOptionMasterSnapshot):
        raise TypeError("SQLiteOptionListingStore snapshot must be an UpstoxOptionMasterSnapshot.")
    try:
        _require(_PROVIDER, snapshot.provider, "provider")
        _require(_SHA256, snapshot.snapshot_sha256, "snapshot hash")
        if not isinstance(snapshot.source_url, str) or not snapshot.source_url.strip():
            raise ValueError("source URL is empty.")
        if not isinstance(snapshot.fetched_at, PointInTime):
            raise TypeError("fetch instant must be a PointInTime.")
        for name in ("record_count", "option_record_count"):
            value = getattr(snapshot, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name.replace('_', ' ')} must be a non-negative integer.")
        if not isinstance(snapshot.listings, tuple):
            raise TypeError("listings must be a tuple.")
        if snapshot.option_record_count < len(snapshot.listings):
            raise ValueError("there are more listings than option records.")
        contracts: set[OptionContract] = set()
        keys: set[str] = set()
        for listing in snapshot.listings:
            if not isinstance(listing, UpstoxOptionListing):
                raise TypeError("listings must be UpstoxOptionListing values.")
            _instrument_key(listing.instrument_key)
            if listing.contract in contracts or listing.instrument_key in keys:
                raise ValueError("listings repeat a contract or an instrument key.")
            contracts.add(listing.contract)
            keys.add(listing.instrument_key)
    except (TypeError, ValueError) as exc:
        raise TypeError(f"SQLiteOptionListingStore refused the snapshot: {exc}") from exc


# ---------------------------------------------------------------------------
# Store
# ---------------------------------------------------------------------------


class SQLiteOptionListingStore:
    """Persist one provider master snapshot and its listings atomically, insert only."""

    def __init__(self, database_path: str | Path) -> None:
        self._database_path = str(database_path)

    def store(self, snapshot: UpstoxOptionMasterSnapshot) -> int:
        """Persist the snapshot and all its listings in one transaction.

        Returns the number of listings in the snapshot, counting those already
        stored as accepted. Creates the listing tables first if they are absent.
        """
        _validate_snapshot(snapshot)
        try:
            connection = sqlite3.connect(self._database_path, isolation_level=None)
            initialize_option_listing_schema(connection)
        except sqlite3.Error as exc:
            raise OptionListingStorageError("Option listing storage is unavailable.") from exc
        try:
            connection.execute("BEGIN IMMEDIATE")
            self._store_snapshot(connection, snapshot)
            for listing in snapshot.listings:
                self._store_listing(connection, snapshot, listing)
            connection.commit()
        except (OptionListingConflictError, OptionListingStorageError):
            connection.rollback()
            raise
        except sqlite3.Error as exc:
            connection.rollback()
            raise OptionListingStorageError("Option listing storage is unavailable.") from exc
        finally:
            connection.close()
        return len(snapshot.listings)

    @staticmethod
    def _store_snapshot(
        connection: sqlite3.Connection, snapshot: UpstoxOptionMasterSnapshot
    ) -> None:
        key = (snapshot.provider, snapshot.snapshot_sha256)
        existing = connection.execute(_SELECT_SNAPSHOT, key).fetchone()
        if existing is None:
            connection.execute(
                _INSERT_SNAPSHOT,
                (
                    *key,
                    snapshot.source_url,
                    snapshot.fetched_at.value,
                    str(snapshot.record_count),
                    str(snapshot.option_record_count),
                ),
            )
            return
        # The same body again: its first fetch instant stands, and the counts it determines
        # must agree.
        counts = _decode_snapshot_counts(tuple(existing))
        if counts != (snapshot.record_count, snapshot.option_record_count):
            raise OptionListingConflictError(
                f"Snapshot {snapshot.snapshot_sha256} is already stored with different counts."
            )

    def _store_listing(
        self,
        connection: sqlite3.Connection,
        snapshot: UpstoxOptionMasterSnapshot,
        listing: UpstoxOptionListing,
    ) -> None:
        provider = snapshot.provider
        if self._resolve_existing(connection, provider, listing):
            return
        candidate = StoredOptionProviderListing(
            provider=provider,
            contract=listing.contract,
            instrument_key=listing.instrument_key,
            exchange_lot_size=listing.lot_size,
            established_snapshot_sha256=snapshot.snapshot_sha256,
            established_at=snapshot.fetched_at,
        )
        try:
            connection.execute(_INSERT_LISTING, _listing_row(candidate))
        except sqlite3.IntegrityError:
            # Another writer stored the contract or the key first; resolve as if seen.
            if not self._resolve_existing(connection, provider, listing):
                raise

    @staticmethod
    def _resolve_existing(
        connection: sqlite3.Connection, provider: str, listing: UpstoxOptionListing
    ) -> bool:
        """Return True when the mapping is already stored; raise when it is contradicted."""
        row = connection.execute(_SELECT_LISTING, _contract_key(provider, listing.contract))
        existing = row.fetchone()
        if existing is not None:
            stored = _decode_listing(tuple(existing))
            if stored.instrument_key != listing.instrument_key:
                raise OptionListingConflictError(
                    f"{listing.contract} is already mapped by {provider} to a different "
                    "instrument key."
                )
            if stored.exchange_lot_size != listing.lot_size:
                raise OptionListingConflictError(
                    f"{listing.contract} is already stored with exchange lot size "
                    f"{stored.exchange_lot_size}, not {listing.lot_size}."
                )
            return True
        owner = connection.execute(
            _SELECT_LISTING_BY_KEY, (provider, listing.instrument_key)
        ).fetchone()
        if owner is not None:
            stored = _decode_listing(tuple(owner))
            raise OptionListingConflictError(
                f"The {provider} instrument key for {listing.contract} is already stored "
                f"for {stored.contract}."
            )
        return False


# ---------------------------------------------------------------------------
# Repository
# ---------------------------------------------------------------------------


class SQLiteOptionListingRepository:
    """Look up one stored provider mapping of an exact option contract, read-only."""

    def __init__(self, database_path: str | Path) -> None:
        self._database_path = str(database_path)

    def get_listing(
        self, provider: str, contract: OptionContract
    ) -> StoredOptionProviderListing | None:
        """Return the stored mapping, or None when none is stored."""
        if not isinstance(provider, str) or _PROVIDER.fullmatch(provider) is None:
            raise TypeError("SQLiteOptionListingRepository provider must be a canonical name.")
        if not isinstance(contract, OptionContract):
            raise TypeError("SQLiteOptionListingRepository contract must be an OptionContract.")
        path = Path(self._database_path)
        if not path.exists():
            return None
        try:
            # mode=ro can neither create the file nor change its schema.
            read_only = f"{path.absolute().as_uri()}?mode=ro"
            with closing(sqlite3.connect(read_only, uri=True)) as connection:
                if connection.execute(_TABLE_EXISTS).fetchone() is None:
                    return None
                row = connection.execute(
                    _SELECT_LISTING, _contract_key(provider, contract)
                ).fetchone()
        except sqlite3.Error as exc:
            raise OptionListingStorageError("Option listing storage is unavailable.") from exc
        if row is None:
            return None
        listing = _decode_listing(tuple(row))
        if listing.provider != provider or listing.contract != contract:
            raise OptionListingStorageError(
                f"Option listing storage returned {listing.contract} for {contract}."
            )
        return listing
