"""Tests for the SQLite option provider-listing reference store and repository."""

from __future__ import annotations

import ast
import sqlite3
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest
from northstar_core.derivatives import ExpirationDate
from northstar_core.foundation.value_objects import ExchangeCode, PointInTime, Symbol
from northstar_core.options import (
    OptionContract,
    OptionProductReference,
    OptionRight,
    OptionStrike,
)

import northstar_infrastructure.market_data.sqlite_option_listing_schema as schema
import northstar_infrastructure.market_data.sqlite_option_provider_listings as module
from northstar_infrastructure.market_data import (
    OptionListingConflictError,
    OptionListingStorageError,
    SQLiteOptionListingRepository,
    SQLiteOptionListingStore,
    StoredOptionProviderListing,
    UpstoxOptionListing,
    UpstoxOptionMasterSnapshot,
    initialize_futures_market_data_schema,
    initialize_option_listing_schema,
)
from northstar_infrastructure.persistence import (
    initialize_futures_contract_economics_schema,
    initialize_futures_paper_trading_schema,
)

_NIFTY = OptionProductReference(Symbol("NIFTY"), ExchangeCode("NSE"))
_SHA_A = "a" * 64
_SHA_B = "b" * 64
_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
_AT_A = PointInTime("2026-10-08T04:30:00Z")
_AT_B = PointInTime("2026-10-09T04:30:00Z")


def _contract(
    expiration: str = "2026-10-27", strike: str = "25000", right: OptionRight = OptionRight.CALL
) -> OptionContract:
    return OptionContract(_NIFTY, ExpirationDate(expiration), OptionStrike(Decimal(strike)), right)


def _listing(key: str = "NSE_FO|1", lot: int = 65, **contract) -> UpstoxOptionListing:
    return UpstoxOptionListing(_contract(**contract), key, lot)


_CALL = _listing("NSE_FO|1")
_PUT = _listing("NSE_FO|2", right=OptionRight.PUT)
_NEXT_STRIKE = _listing("NSE_FO|3", strike="25050")
_EXPIRED_WEEKLY = _listing("NSE_FO|4", expiration="2026-10-13", strike="22600")


def _snapshot(
    *listings: UpstoxOptionListing,
    sha: str = _SHA_A,
    at: PointInTime = _AT_A,
    records: int = 1000,
    options: int | None = None,
) -> UpstoxOptionMasterSnapshot:
    return UpstoxOptionMasterSnapshot(
        snapshot_sha256=sha,
        source_url=_URL,
        fetched_at=at,
        record_count=records,
        option_record_count=len(listings) if options is None else options,
        listings=tuple(listings),
    )


@pytest.fixture
def database(tmp_path: Path) -> Path:
    return tmp_path / "northstar.sqlite3"


def _store(database: Path, snapshot: UpstoxOptionMasterSnapshot) -> int:
    return SQLiteOptionListingStore(database).store(snapshot)


def _get(database: Path, contract: OptionContract = _CALL.contract, provider: str = "upstox"):
    return SQLiteOptionListingRepository(database).get_listing(provider, contract)


def _query(database: Path, sql: str, parameters: tuple = ()) -> list[tuple]:
    with sqlite3.connect(database) as connection:
        return connection.execute(sql, parameters).fetchall()


def _execute(database: Path, sql: str, parameters: tuple = ()) -> None:
    with sqlite3.connect(database) as connection:
        connection.execute(sql, parameters)


def _tables(database: Path) -> set[str]:
    return {row[0] for row in _query(database, "SELECT name FROM sqlite_master WHERE type='table'")}


def _schema(database: Path) -> list[tuple]:
    return sorted(_query(database, "SELECT type, name, sql FROM sqlite_master"))


def _snapshots(database: Path) -> list[tuple]:
    return _query(database, "SELECT * FROM option_listing_snapshots ORDER BY 1, 2")


def _listings(database: Path) -> list[tuple]:
    return _query(database, "SELECT * FROM option_provider_listings ORDER BY 4, 5, 6")


def _initialize(database: Path, *initializers) -> None:
    with sqlite3.connect(database) as connection:
        for initializer in initializers:
            initializer(connection)


def _call_row(sha: str = _SHA_A, at: str = "2026-10-08T04:30:00Z") -> tuple:
    return ("upstox", "NIFTY", "NSE", "2026-10-27", "25000", "CALL", "NSE_FO|1", "65", sha, at)


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


def test_a_store_creates_exactly_the_two_listing_tables(database: Path) -> None:
    _store(database, _snapshot(_CALL))

    assert _tables(database) == {"option_listing_snapshots", "option_provider_listings"}


def _columns(database: Path, table: str) -> list[tuple]:
    return [
        (name, kind, notnull, pk)
        for _, name, kind, notnull, _, pk in _query(database, f"PRAGMA table_info({table})")
    ]


def test_the_snapshot_table_shape_is_exact(database: Path) -> None:
    _initialize(database, initialize_option_listing_schema)

    assert _columns(database, "option_listing_snapshots") == [
        ("provider", "TEXT", 1, 1),
        ("snapshot_sha256", "TEXT", 1, 2),
        ("source_url", "TEXT", 1, 0),
        ("first_fetched_at", "TEXT", 1, 0),
        ("record_count", "TEXT", 1, 0),
        ("option_record_count", "TEXT", 1, 0),
    ]


def test_the_listing_table_shape_is_exact(database: Path) -> None:
    _initialize(database, initialize_option_listing_schema)

    assert _columns(database, "option_provider_listings") == [
        ("provider", "TEXT", 1, 1),
        ("product_code", "TEXT", 1, 2),
        ("exchange_code", "TEXT", 1, 3),
        ("expiration_date", "TEXT", 1, 4),
        ("strike", "TEXT", 1, 5),
        ("option_right", "TEXT", 1, 6),
        ("instrument_key", "TEXT", 1, 0),
        ("exchange_lot_size", "TEXT", 1, 0),
        ("established_snapshot_sha256", "TEXT", 1, 0),
        ("established_at", "TEXT", 1, 0),
    ]


def test_provider_and_instrument_key_are_unique(database: Path) -> None:
    _initialize(database, initialize_option_listing_schema)
    unique = [
        [column for _, _, column in _query(database, f"PRAGMA index_info('{name}')")]
        for _, name, is_unique, origin, _ in _query(
            database, "PRAGMA index_list('option_provider_listings')"
        )
        if is_unique and origin == "u"
    ]

    assert unique == [["provider", "instrument_key"]]


def test_the_initializer_is_repeatable(database: Path) -> None:
    _store(database, _snapshot(_CALL))
    before = _schema(database)

    _initialize(database, initialize_option_listing_schema, initialize_option_listing_schema)

    assert _schema(database) == before
    assert _get(database).instrument_key == "NSE_FO|1"


def test_existing_futures_tables_are_left_untouched(database: Path) -> None:
    _initialize(
        database,
        initialize_futures_market_data_schema,
        initialize_futures_paper_trading_schema,
        initialize_futures_contract_economics_schema,
    )
    before = set(_query(database, "SELECT type, name, sql FROM sqlite_master"))

    _store(database, _snapshot(_CALL))

    after = set(_query(database, "SELECT type, name, sql FROM sqlite_master"))
    assert before <= after
    assert {name for _, name, _ in after - before} == {
        "option_listing_snapshots",
        "option_provider_listings",
        "sqlite_autoindex_option_listing_snapshots_1",
        "sqlite_autoindex_option_provider_listings_1",
        "sqlite_autoindex_option_provider_listings_2",
    }


# ---------------------------------------------------------------------------
# Canonical storage
# ---------------------------------------------------------------------------


def test_a_snapshot_and_its_listings_are_stored_as_canonical_text(database: Path) -> None:
    fractional = _listing("NSE_FO|9", strike="24950.50", right=OptionRight.PUT)
    snapshot = _snapshot(_CALL, fractional, at=PointInTime("2026-10-08T10:00:00.250+05:30"))

    assert _store(database, snapshot) == 2

    assert _snapshots(database) == [
        ("upstox", _SHA_A, _URL, "2026-10-08T04:30:00.25Z", "1000", "2"),
    ]
    assert _listings(database) == [
        ("upstox", "NIFTY", "NSE", "2026-10-27", "24950.5", "PUT", "NSE_FO|9", "65", _SHA_A,
         "2026-10-08T04:30:00.25Z"),
        _call_row(at="2026-10-08T04:30:00.25Z"),
    ]  # fmt: skip
    for table in ("option_listing_snapshots", "option_provider_listings"):
        types = _query(database, f"SELECT DISTINCT typeof(provider) FROM {table}")  # noqa: S608
        assert types == [("text",)]


def test_every_listing_column_is_text(database: Path) -> None:
    _store(database, _snapshot(_CALL))
    columns = ", ".join(f"typeof({column})" for column in module._LISTING_COLUMNS)

    assert _query(database, f"SELECT {columns} FROM option_provider_listings") == [  # noqa: S608
        ("text",) * 10
    ]


def test_one_listing_round_trips_with_its_provenance(database: Path) -> None:
    _store(database, _snapshot(_CALL, _PUT))

    assert _get(database, _PUT.contract) == StoredOptionProviderListing(
        provider="upstox",
        contract=_PUT.contract,
        instrument_key="NSE_FO|2",
        exchange_lot_size=65,
        established_snapshot_sha256=_SHA_A,
        established_at=_AT_A,
    )


@pytest.mark.parametrize(
    "field_value",
    [
        {"snapshot_sha256": "A" * 64},
        {"snapshot_sha256": "a" * 63},
        {"snapshot_sha256": "g" * 64},
        {"source_url": ""},
        {"record_count": -1},
        {"record_count": True},
        {"option_record_count": 0},
        {"fetched_at": "2026-10-08T04:30:00Z"},
        {"listings": [_CALL]},
    ],
    ids=["upper-hash", "short-hash", "non-hex", "empty-url", "negative", "bool", "fewer-options",
         "text-instant", "list"],
)  # fmt: skip
def test_a_snapshot_that_cannot_be_stored_canonically_is_refused_before_any_write(
    database: Path, field_value: dict
) -> None:
    with pytest.raises(TypeError, match="refused the snapshot"):
        _store(database, replace(_snapshot(_CALL), **field_value))
    assert not database.exists()


def test_a_snapshot_repeating_a_contract_or_key_is_refused(database: Path) -> None:
    with pytest.raises(TypeError, match="repeat"):
        _store(database, _snapshot(_CALL, _listing("NSE_FO|9")))
    with pytest.raises(TypeError, match="repeat"):
        _store(database, _snapshot(_CALL, _listing("NSE_FO|1", right=OptionRight.PUT)))
    assert not database.exists()


def test_a_foreign_value_is_refused(database: Path) -> None:
    with pytest.raises(TypeError):
        SQLiteOptionListingStore(database).store((_CALL,))  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Idempotency, provenance and conflicts
# ---------------------------------------------------------------------------


def test_the_same_body_fetched_later_is_idempotent_and_keeps_its_first_fetch(
    database: Path,
) -> None:
    _store(database, _snapshot(_CALL, _PUT))
    stored = (_snapshots(database), _listings(database))

    assert _store(database, _snapshot(_CALL, _PUT, at=_AT_B)) == 2

    assert (_snapshots(database), _listings(database)) == stored
    assert _snapshots(database)[0][3] == "2026-10-08T04:30:00Z"


def test_a_new_snapshot_adds_new_listings_and_keeps_old_provenance(database: Path) -> None:
    _store(database, _snapshot(_CALL, _PUT))

    assert _store(database, _snapshot(_CALL, _PUT, _NEXT_STRIKE, sha=_SHA_B, at=_AT_B)) == 3

    assert [row[1] for row in _snapshots(database)] == [_SHA_A, _SHA_B]
    assert _get(database, _CALL.contract).established_snapshot_sha256 == _SHA_A
    assert _get(database, _CALL.contract).established_at == _AT_A
    assert _get(database, _PUT.contract).established_snapshot_sha256 == _SHA_A
    assert _get(database, _NEXT_STRIKE.contract).established_snapshot_sha256 == _SHA_B
    assert _get(database, _NEXT_STRIKE.contract).established_at == _AT_B


@pytest.mark.parametrize(
    ("changed", "message"),
    [
        (_listing("NSE_FO|99"), "different instrument key"),
        (_listing("NSE_FO|1", lot=75), "exchange lot size 65, not 75"),
        (_listing("NSE_FO|1", strike="25050"), "already stored for"),
        (_listing("NSE_FO|2", expiration="2026-11-24"), "already stored for"),
    ],
    ids=["changed-key", "changed-lot", "key-to-other-strike", "key-to-other-expiry"],
)  # fmt: skip
def test_a_contradicting_observation_rolls_back_the_whole_sync(
    database: Path, changed: UpstoxOptionListing, message: str
) -> None:
    """A genuinely new listing in the same sync is rolled back with the conflicting one."""
    _store(database, _snapshot(_CALL, _PUT))
    stored = (_snapshots(database), _listings(database))

    with pytest.raises(OptionListingConflictError, match=message):
        _store(database, _snapshot(_EXPIRED_WEEKLY, changed, sha=_SHA_B, at=_AT_B))

    assert (_snapshots(database), _listings(database)) == stored
    assert _get(database, _EXPIRED_WEEKLY.contract) is None


def test_a_stored_body_with_different_counts_conflicts(database: Path) -> None:
    _store(database, _snapshot(_CALL, records=1000))

    with pytest.raises(OptionListingConflictError, match="different counts"):
        _store(database, _snapshot(_CALL, records=1001, at=_AT_B))
    assert _snapshots(database)[0][4] == "1000"


def test_listings_survive_reopen(tmp_path: Path) -> None:
    path = tmp_path / "reopen.sqlite3"
    SQLiteOptionListingStore(path).store(_snapshot(_CALL, _PUT, _NEXT_STRIKE))

    for listing in (_CALL, _PUT, _NEXT_STRIKE):
        assert SQLiteOptionListingRepository(path).get_listing("upstox", listing.contract) == (
            StoredOptionProviderListing(
                "upstox", listing.contract, listing.instrument_key, 65, _SHA_A, _AT_A
            )
        )
    assert SQLiteOptionListingStore(path).store(_snapshot(_CALL, sha=_SHA_B, at=_AT_B)) == 1
    assert SQLiteOptionListingRepository(path).get_listing(
        "upstox", _CALL.contract
    ).established_at == (_AT_A)


def _race(monkeypatch: pytest.MonkeyPatch) -> None:
    real = SQLiteOptionListingStore._resolve_existing
    calls = {"count": 0}

    def racing(connection, provider, listing):
        calls["count"] += 1
        return False if calls["count"] == 1 else real(connection, provider, listing)

    monkeypatch.setattr(SQLiteOptionListingStore, "_resolve_existing", staticmethod(racing))


def test_a_race_with_an_equal_listing_is_idempotent(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _store(database, _snapshot(_CALL))
    _race(monkeypatch)

    assert _store(database, _snapshot(_CALL, sha=_SHA_B, at=_AT_B)) == 1
    assert _listings(database) == [_call_row()]


def test_a_race_with_a_different_listing_conflicts(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _store(database, _snapshot(_CALL))
    _race(monkeypatch)

    with pytest.raises(OptionListingConflictError):
        _store(database, _snapshot(_listing("NSE_FO|99"), sha=_SHA_B, at=_AT_B))
    assert _listings(database) == [_call_row()]
    assert [row[1] for row in _snapshots(database)] == [_SHA_A]


# ---------------------------------------------------------------------------
# Corruption
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("column", "value"),
    [
        ("exchange_lot_size", "0"),
        ("exchange_lot_size", "065"),
        ("exchange_lot_size", "65.0"),
        ("exchange_lot_size", "-65"),
        ("instrument_key", ""),
        ("instrument_key", " NSE_FO|1"),
        ("established_snapshot_sha256", "A" * 64),
        ("established_snapshot_sha256", "a" * 63),
        ("established_at", "2026-10-08T10:00:00+05:30"),
        ("established_at", "yesterday"),
        ("provider", "Upstox"),
        ("exchange_lot_size", b"65"),
    ],
)
def test_a_corrupt_listing_row_fails_loudly(database: Path, column: str, value: object) -> None:
    _store(database, _snapshot(_CALL))
    _execute(database, f"UPDATE option_provider_listings SET {column} = ?", (value,))  # noqa: S608

    with pytest.raises(OptionListingStorageError):
        module._decode_listing(_query(database, "SELECT * FROM option_provider_listings")[0])
    if column != "provider":
        with pytest.raises(OptionListingStorageError):
            _get(database)


@pytest.mark.parametrize(
    "row",
    [
        ("upstox", "NIFTY", "NSE", "2026-10-27", "25000.0", "CALL", "NSE_FO|1", "65", _SHA_A,
         "2026-10-08T04:30:00Z"),
        ("upstox", "NIFTY", "NSE", "2026-10-27", "25000", "CE", "NSE_FO|1", "65", _SHA_A,
         "2026-10-08T04:30:00Z"),
        ("upstox", "nifty", "NSE", "2026-10-27", "25000", "CALL", "NSE_FO|1", "65", _SHA_A,
         "2026-10-08T04:30:00Z"),
        ("upstox", "NIFTY", "NSE", "20261027", "25000", "CALL", "NSE_FO|1", "65", _SHA_A,
         "2026-10-08T04:30:00Z"),
        ("upstox", "NIFTY", "NSE", "2026-10-27", "0", "CALL", "NSE_FO|1", "65", _SHA_A,
         "2026-10-08T04:30:00Z"),
        ("upstox", "NIFTY", "NSE", "2026-10-27", "25000", "CALL", "NSE_FO|1", "65", _SHA_A),
    ],
    ids=["padded-strike", "provider-right", "lower-product", "compact-expiry", "zero-strike",
         "short"],
)  # fmt: skip
def test_a_corrupt_listing_key_is_refused_by_the_decoder(row: tuple) -> None:
    with pytest.raises(OptionListingStorageError):
        module._decode_listing(row)


def test_a_store_over_a_corrupt_listing_fails_and_repairs_nothing(database: Path) -> None:
    _store(database, _snapshot(_CALL))
    _execute(database, "UPDATE option_provider_listings SET exchange_lot_size = '0'")

    with pytest.raises(OptionListingStorageError):
        _store(database, _snapshot(_CALL, sha=_SHA_B, at=_AT_B))

    assert _listings(database)[0][7] == "0"
    assert [row[1] for row in _snapshots(database)] == [_SHA_A]


@pytest.mark.parametrize(
    ("column", "value"),
    [("record_count", "01000"), ("record_count", "-1"), ("first_fetched_at", "today")],
)
def test_a_corrupt_snapshot_row_fails_loudly_on_restore(
    database: Path, column: str, value: str
) -> None:
    _store(database, _snapshot(_CALL))
    _execute(database, f"UPDATE option_listing_snapshots SET {column} = ?", (value,))  # noqa: S608

    with pytest.raises(OptionListingStorageError, match="snapshot storage"):
        _store(database, _snapshot(_CALL, at=_AT_B))


# ---------------------------------------------------------------------------
# Read-only repository
# ---------------------------------------------------------------------------


def test_a_missing_database_is_none_and_creates_no_file(database: Path) -> None:
    assert _get(database) is None
    assert not database.exists()


def test_a_database_without_listing_tables_is_none_and_unchanged(database: Path) -> None:
    _initialize(database, initialize_futures_market_data_schema)
    before = _schema(database)

    assert _get(database) is None

    assert _schema(database) == before


def test_a_missing_listing_is_none_not_a_neighbour(database: Path) -> None:
    _store(database, _snapshot(_CALL))

    for neighbour in (
        _contract(strike="25050"),
        _contract(right=OptionRight.PUT),
        _contract(expiration="2026-11-24"),
    ):
        assert _get(database, neighbour) is None
    assert _get(database, _CALL.contract, provider="zerodha") is None


def test_an_expired_listing_stays_resolvable_after_it_leaves_the_master(database: Path) -> None:
    """The later master no longer lists the 2026-10-13 weekly; the stored mapping remains."""
    _store(database, _snapshot(_CALL, _EXPIRED_WEEKLY))
    _store(database, _snapshot(_CALL, _NEXT_STRIKE, sha=_SHA_B, at=_AT_B))

    stored = _get(database, _EXPIRED_WEEKLY.contract)

    assert stored == StoredOptionProviderListing(
        "upstox", _EXPIRED_WEEKLY.contract, "NSE_FO|4", 65, _SHA_A, _AT_A
    )
    assert str(stored.contract) == "NIFTY@NSE 2026-10-13 22600 CALL"


def _trace(monkeypatch: pytest.MonkeyPatch) -> list[tuple[tuple, dict, list[str]]]:
    connections: list[tuple[tuple, dict, list[str]]] = []
    real_connect = sqlite3.connect

    def connect(*args, **kwargs):
        statements: list[str] = []
        connections.append((args, kwargs, statements))
        connection = real_connect(*args, **kwargs)
        connection.set_trace_callback(statements.append)
        return connection

    monkeypatch.setattr(module.sqlite3, "connect", connect)
    return connections


def test_a_lookup_is_read_only_and_runs_no_ddl(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _store(database, _snapshot(_CALL))

    def forbidden(connection) -> None:
        raise AssertionError("a lookup must never initialize the schema")

    monkeypatch.setattr(module, "initialize_option_listing_schema", forbidden)
    connections = _trace(monkeypatch)

    assert _get(database) is not None
    assert _get(database, _contract(strike="25050")) is None

    for args, kwargs, statements in connections:
        assert args[0].startswith("file:") and args[0].endswith("?mode=ro")
        assert kwargs == {"uri": True}
        for statement in statements:
            assert (
                not statement.upper()
                .lstrip()
                .startswith(
                    ("CREATE", "ALTER", "DROP", "INSERT", "UPDATE", "DELETE", "REPLACE", "BEGIN")
                )
            ), statement


def test_a_directory_is_a_storage_error(tmp_path: Path) -> None:
    with pytest.raises(OptionListingStorageError, match="unavailable"):
        _get(tmp_path)


@pytest.mark.parametrize(
    ("provider", "contract"),
    [("Upstox", _CALL.contract), ("", _CALL.contract), ("upstox", "NIFTY@NSE")],
)
def test_the_repository_rejects_bad_arguments(database: Path, provider, contract) -> None:
    with pytest.raises(TypeError):
        SQLiteOptionListingRepository(database).get_listing(provider, contract)


# ---------------------------------------------------------------------------
# Transactions and boundaries
# ---------------------------------------------------------------------------


def test_one_sync_is_one_begin_immediate_transaction(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    connections = _trace(monkeypatch)

    _store(database, _snapshot(_CALL, _PUT))

    ((_, _, statements),) = connections
    begin = statements.index("BEGIN IMMEDIATE")
    inserts = [i for i, s in enumerate(statements) if s.startswith("INSERT INTO")]
    assert [statements[i].split()[2] for i in inserts] == [
        "option_listing_snapshots",
        "option_provider_listings",
        "option_provider_listings",
    ]
    assert begin < min(inserts)
    assert statements[-1] == "COMMIT"
    assert statements.count("COMMIT") == 1


_DECLARED_SQL = (
    module._SELECT_SNAPSHOT,
    module._INSERT_SNAPSHOT,
    module._SELECT_LISTING,
    module._SELECT_LISTING_BY_KEY,
    module._INSERT_LISTING,
    module._TABLE_EXISTS,
    schema.OPTION_LISTING_SNAPSHOTS_SCHEMA,
    schema.OPTION_PROVIDER_LISTINGS_SCHEMA,
)


def _assert_insert_only_text(sql: str) -> None:
    upper = sql.upper()
    for forbidden in ("UPDATE ", "DELETE ", "REPLACE", "ON CONFLICT", "UPSERT", "DROP ", "ALTER "):
        assert forbidden not in upper, sql
    for forbidden in (" REAL", " FLOAT", " NUMERIC", " INTEGER", " BLOB"):
        assert forbidden not in upper, sql
    assert "FUTURES_" not in upper, sql


def test_every_declared_statement_is_insert_only_text_and_names_no_futures_table() -> None:
    for sql in _DECLARED_SQL:
        _assert_insert_only_text(sql)


def test_every_executed_statement_is_insert_only_and_names_no_futures_table(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    connections = _trace(monkeypatch)

    _store(database, _snapshot(_CALL, _PUT))
    _store(database, _snapshot(_CALL, _PUT, _NEXT_STRIKE, sha=_SHA_B, at=_AT_B))
    _get(database)

    statements = [statement for _, _, executed in connections for statement in executed]
    assert statements
    for statement in statements:
        _assert_insert_only_text(statement)


def test_the_adapters_read_no_clock_and_reach_no_network() -> None:
    tree = ast.parse(Path(module.__file__).read_text(encoding="utf-8"))
    modules = {n.module for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module} | {
        a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names
    }

    for name in modules:
        assert name.split(".")[0] not in {"time", "socket", "urllib", "requests", "httpx"}
        assert "upstox_http" not in name
        assert not name.startswith("northstar_core.futures")
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            assert node.func.attr not in {"now", "utcnow", "today", "time", "monotonic"}
