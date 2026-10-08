"""Read-only answer to which option contracts Northstar knew were listed by an instant.

The answer is drawn from one provider's stored listings (INDIA-OPT-4A). A stored
listing records when Northstar first observed it -- ``established_at`` -- and
nothing later, so a contract is known by an instant exactly when that first
observation is at or before it. Instants are compared as PointInTime values,
never as text: stored instants may carry fractional seconds, so their text does
not sort chronologically. A contract first observed after the instant is never
returned, whatever market data is stored for it.

Once observed, a contract is treated as listed through its expiration. That is
Northstar's reconstruction assumption, made because listing membership is not
recorded after the first observation; it is not an exchange guarantee.

The repository is read-only. It opens SQLite in read-only mode, runs no schema
initializer and no DDL, takes no lock, and reads no instrument master, network
or clock. A database file or listing table that does not exist holds no
listings. Every row is decoded by the listing store's own canonical decoder, so
a corrupt or non-canonical row fails loudly with OptionListingStorageError.
Only contracts are returned: the instrument key, lot, snapshot hash and
established instant stay here.
"""

from __future__ import annotations

import sqlite3
from contextlib import closing
from pathlib import Path

from northstar_application.ports import OptionListedContractQuery, OptionListedContractRepository
from northstar_core.options import OptionContract, OptionRight

from northstar_infrastructure.market_data.sqlite_option_provider_listings import (
    _LISTING_COLUMNS,
    _PROVIDER,
    _TABLE_EXISTS,
    OptionListingStorageError,
    _decode_listing,
)

_SELECT_EXPIRATION = (
    f"SELECT {', '.join(_LISTING_COLUMNS)} FROM option_provider_listings "  # noqa: S608
    "WHERE provider = ? AND product_code = ? AND exchange_code = ? AND expiration_date = ?"
)
_RIGHT_RANK = {OptionRight.CALL: 0, OptionRight.PUT: 1}


def _chain_order(contract: OptionContract) -> tuple:
    return (contract.strike.value, _RIGHT_RANK[contract.right])


class SQLiteOptionListedContractRepository(OptionListedContractRepository):
    """List one expiration's contracts one provider's listings made known by an instant."""

    def __init__(self, database_path: str | Path, *, provider: str) -> None:
        if not isinstance(provider, str) or _PROVIDER.fullmatch(provider) is None:
            raise TypeError(
                "SQLiteOptionListedContractRepository provider must be a canonical name."
            )
        self._database_path = str(database_path)
        self._provider = provider

    def listed_contracts(self, query: OptionListedContractQuery) -> tuple[OptionContract, ...]:
        """Return the expiration's contracts first observed at or before ``known_by``."""
        if not isinstance(query, OptionListedContractQuery):
            raise TypeError(
                "SQLiteOptionListedContractRepository query must be an OptionListedContractQuery."
            )
        path = Path(self._database_path)
        if not path.exists():
            return ()
        product = query.product
        try:
            # mode=ro can neither create the file nor change its schema.
            read_only = f"{path.absolute().as_uri()}?mode=ro"
            with closing(sqlite3.connect(read_only, uri=True)) as connection:
                if connection.execute(_TABLE_EXISTS).fetchone() is None:
                    return ()
                rows = connection.execute(
                    _SELECT_EXPIRATION,
                    (
                        self._provider,
                        product.product_code.value,
                        product.exchange_code.value,
                        query.expiration_date.value,
                    ),
                ).fetchall()
        except sqlite3.Error as exc:
            raise OptionListingStorageError("Option listing storage is unavailable.") from exc

        contracts: list[OptionContract] = []
        for row in rows:
            listing = _decode_listing(tuple(row))
            contract = listing.contract
            if (
                listing.provider != self._provider
                or contract.product != product
                or contract.expiration_date != query.expiration_date
            ):
                raise OptionListingStorageError(
                    f"Option listing storage returned {contract} for "
                    f"{product} {query.expiration_date}."
                )
            if listing.established_at.compare(query.known_by) <= 0:
                contracts.append(contract)
        return tuple(sorted(contracts, key=_chain_order))
