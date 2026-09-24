"""Tests for the SQLite futures product economics store and repository."""

from __future__ import annotations

import ast
import sqlite3
from decimal import ROUND_DOWN, Decimal, localcontext
from pathlib import Path

import pytest
from northstar_application.ports import FuturesProductEconomicsRepository
from northstar_core.derivatives import ExpirationDate
from northstar_core.foundation.value_objects import Currency, ExchangeCode, Symbol
from northstar_core.futures import (
    FuturesContract,
    FuturesPointValue,
    FuturesProductEconomics,
    FuturesProductReference,
)

import northstar_infrastructure.persistence as persistence
import northstar_infrastructure.persistence.sqlite_futures_product_economics as module
import northstar_infrastructure.persistence.sqlite_futures_product_economics_schema as schema
from northstar_infrastructure.market_data.sqlite_futures_schema import (
    initialize_futures_market_data_schema,
)
from northstar_infrastructure.persistence import (
    FuturesProductEconomicsConflictError,
    FuturesProductEconomicsStorageError,
    SQLiteFuturesProductEconomicsRepository,
    SQLiteFuturesProductEconomicsStore,
    initialize_futures_forward_research_record_schema,
    initialize_futures_paper_trading_schema,
    initialize_futures_product_economics_schema,
)


def _reference(product: str = "ES", exchange: str = "CME") -> FuturesProductReference:
    return FuturesProductReference(Symbol(product), ExchangeCode(exchange))


def _economics(
    product: str = "ES", exchange: str = "CME", amount: str = "50", currency: str = "USD"
) -> FuturesProductEconomics:
    return FuturesProductEconomics(
        _reference(product, exchange), FuturesPointValue(Decimal(amount), Currency(currency))
    )


_ES = _reference()
_ES_ECONOMICS = _economics()
_HIGH_PRECISION = "12.345678901234567890123456789012345678901234567890123"


@pytest.fixture
def database(tmp_path: Path) -> Path:
    return tmp_path / "futures_economics.sqlite3"


def _store(database: Path, *economics: FuturesProductEconomics) -> int:
    return SQLiteFuturesProductEconomicsStore(database).store(economics)


def _get(
    database: Path, reference: FuturesProductReference = _ES
) -> FuturesProductEconomics | None:
    return SQLiteFuturesProductEconomicsRepository(database).get_economics(reference)


def _query(database: Path, sql: str, parameters: tuple = ()) -> list[tuple]:
    with sqlite3.connect(database) as connection:
        return connection.execute(sql, parameters).fetchall()


def _execute(database: Path, sql: str, parameters: tuple = ()) -> None:
    with sqlite3.connect(database) as connection:
        connection.execute(sql, parameters)


def _rows(database: Path) -> list[tuple]:
    return _query(database, "SELECT * FROM futures_product_economics ORDER BY 1, 2")


def _initialize(database: Path, *initializers) -> None:
    with sqlite3.connect(database) as connection:
        for initializer in initializers:
            initializer(connection)


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


def test_a_fresh_database_gets_exactly_the_economics_table(database: Path) -> None:
    _initialize(database, initialize_futures_product_economics_schema)

    tables = {
        row[0] for row in _query(database, "SELECT name FROM sqlite_master WHERE type='table'")
    }

    assert tables == {"futures_product_economics"}


def test_the_columns_are_exactly_the_product_key_and_point_value(database: Path) -> None:
    _initialize(database, initialize_futures_product_economics_schema)

    columns = _query(database, "PRAGMA table_info(futures_product_economics)")

    assert [(name, kind, bool(not_null), pk) for _, name, kind, not_null, _, pk in columns] == [
        ("product_code", "TEXT", True, 1),
        ("exchange_code", "TEXT", True, 2),
        ("point_value_amount", "TEXT", True, 0),
        ("settlement_currency", "TEXT", True, 0),
    ]


def test_the_schema_has_no_expiry_or_extra_economics(database: Path) -> None:
    _store(database, _ES_ECONOMICS)
    _get(database)

    tables = {
        row[0] for row in _query(database, "SELECT name FROM sqlite_master WHERE type='table'")
    }
    columns = {row[1] for row in _query(database, "PRAGMA table_info(futures_product_economics)")}

    assert tables == {"futures_product_economics"}
    for forbidden in (
        "expir",
        "underlying",
        "tick",
        "margin",
        "notional",
        "multiplier",
        "effective",
        "provider",
        "created",
        "updated",
        "pnl",
        "mark",
    ):
        assert not [column for column in columns if forbidden in column]


def test_existing_futures_tables_are_left_untouched(database: Path) -> None:
    _initialize(
        database,
        initialize_futures_market_data_schema,
        initialize_futures_forward_research_record_schema,
        initialize_futures_paper_trading_schema,
    )
    before = set(_query(database, "SELECT type, name, sql FROM sqlite_master"))

    _initialize(database, initialize_futures_product_economics_schema)
    _store(database, _ES_ECONOMICS)
    after = set(_query(database, "SELECT type, name, sql FROM sqlite_master"))

    assert {name for _, name, _ in before} >= {
        "futures_ohlcv",
        "futures_forward_research_records",
        "futures_paper_orders",
        "futures_paper_fills",
    }
    assert before <= after
    assert {name for _, name, _ in after - before} == {
        "futures_product_economics",
        "sqlite_autoindex_futures_product_economics_1",
    }


def test_initialization_is_repeatable(database: Path) -> None:
    _initialize(database, initialize_futures_product_economics_schema)
    _store(database, _ES_ECONOMICS)

    _initialize(database, initialize_futures_product_economics_schema)

    assert _get(database) == _ES_ECONOMICS


# ---------------------------------------------------------------------------
# Store: round trip and serialization
# ---------------------------------------------------------------------------


def test_one_economics_value_round_trips_exactly(database: Path) -> None:
    assert _store(database, _ES_ECONOMICS) == 1

    assert _get(database) == _ES_ECONOMICS


def test_every_column_is_stored_as_canonical_text(database: Path) -> None:
    _store(database, _economics(amount="12.500"), _economics("MES", amount="1E+1"))

    assert _rows(database) == [("ES", "CME", "12.5", "USD"), ("MES", "CME", "10", "USD")]
    assert _query(
        database,
        "SELECT DISTINCT typeof(product_code), typeof(exchange_code), "
        "typeof(point_value_amount), typeof(settlement_currency) FROM futures_product_economics",
    ) == [("text", "text", "text", "text")]


def test_a_high_precision_amount_round_trips_under_any_caller_context(database: Path) -> None:
    economics = _economics(amount=_HIGH_PRECISION)

    with localcontext() as ambient:
        ambient.prec = 6
        ambient.rounding = ROUND_DOWN
        _store(database, economics)
        read = _get(database)

    assert read == economics
    assert read.point_value.amount == Decimal(_HIGH_PRECISION)
    assert _rows(database)[0][2] == _HIGH_PRECISION


# ---------------------------------------------------------------------------
# Store: idempotency, conflicts and batches
# ---------------------------------------------------------------------------


def test_an_equal_retry_is_idempotent(database: Path) -> None:
    _store(database, _ES_ECONOMICS)

    assert _store(database, _ES_ECONOMICS) == 1
    assert _store(database, _economics(amount="50.000")) == 1
    assert _rows(database) == [("ES", "CME", "50", "USD")]


@pytest.mark.parametrize(
    "changed",
    [_economics(amount="50.01"), _economics(amount="5"), _economics(currency="EUR")],
    ids=["amount", "smaller-amount", "currency"],
)
def test_different_economics_for_a_stored_product_conflict(
    database: Path, changed: FuturesProductEconomics
) -> None:
    _store(database, _ES_ECONOMICS)

    with pytest.raises(FuturesProductEconomicsConflictError, match="already stored for ES@CME"):
        _store(database, changed)

    assert _get(database) == _ES_ECONOMICS


def test_an_empty_batch_returns_zero_and_touches_nothing(database: Path) -> None:
    assert _store(database) == 0
    assert not database.exists()


@pytest.mark.parametrize(
    "batch",
    [(_ES_ECONOMICS, _ES_ECONOMICS), (_ES_ECONOMICS, _economics(amount="5"))],
    ids=["equal", "different"],
)
def test_a_batch_repeating_one_product_is_rejected(database: Path, batch: tuple) -> None:
    with pytest.raises(FuturesProductEconomicsConflictError, match="two entries for one product"):
        SQLiteFuturesProductEconomicsStore(database).store(batch)

    assert not database.exists()


@pytest.mark.parametrize(
    "batch",
    [[_ES_ECONOMICS], (_ES_ECONOMICS, "ES@CME"), (_ES_ECONOMICS.point_value,)],
    ids=["list", "string-member", "point-value-member"],
)
def test_foreign_batches_are_rejected_before_any_write(database: Path, batch) -> None:
    with pytest.raises(TypeError):
        SQLiteFuturesProductEconomicsStore(database).store(batch)

    assert not database.exists()


def test_one_conflict_leaves_the_whole_batch_uncommitted(database: Path) -> None:
    _store(database, _ES_ECONOMICS)

    with pytest.raises(FuturesProductEconomicsConflictError):
        _store(database, _economics("MES", amount="5"), _economics(amount="51"))

    assert _get(database, _reference("MES")) is None
    assert _rows(database) == [("ES", "CME", "50", "USD")]


def test_idempotent_and_new_entries_both_count(database: Path) -> None:
    _store(database, _ES_ECONOMICS)

    assert _store(database, _ES_ECONOMICS, _economics("MES", amount="5")) == 2
    assert len(_rows(database)) == 2


def test_economics_survive_restart_and_retries_stay_idempotent(tmp_path: Path) -> None:
    path = tmp_path / "restart.sqlite3"
    economics = _economics(amount=_HIGH_PRECISION, currency="EUR")
    SQLiteFuturesProductEconomicsStore(path).store((economics,))

    assert SQLiteFuturesProductEconomicsRepository(path).get_economics(_ES) == economics
    assert SQLiteFuturesProductEconomicsStore(path).store((economics,)) == 1
    with pytest.raises(FuturesProductEconomicsConflictError):
        SQLiteFuturesProductEconomicsStore(path).store((_ES_ECONOMICS,))
    assert len(_rows(path)) == 1


# ---------------------------------------------------------------------------
# Store: races
# ---------------------------------------------------------------------------


def _race(monkeypatch: pytest.MonkeyPatch) -> None:
    real = SQLiteFuturesProductEconomicsStore._existing
    calls = {"count": 0}

    def racing(connection, reference):
        calls["count"] += 1
        return None if calls["count"] == 1 else real(connection, reference)

    monkeypatch.setattr(SQLiteFuturesProductEconomicsStore, "_existing", staticmethod(racing))


def test_a_race_with_equal_economics_is_idempotent(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _store(database, _ES_ECONOMICS)
    _race(monkeypatch)

    assert _store(database, _ES_ECONOMICS) == 1
    assert len(_rows(database)) == 1


def test_a_race_with_different_economics_conflicts(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _store(database, _ES_ECONOMICS)
    _race(monkeypatch)

    with pytest.raises(FuturesProductEconomicsConflictError):
        _store(database, _economics(currency="EUR"))

    monkeypatch.undo()
    assert _get(database) == _ES_ECONOMICS


# ---------------------------------------------------------------------------
# Repository
# ---------------------------------------------------------------------------


def test_a_missing_product_is_none_not_a_default(database: Path) -> None:
    assert _get(database) is None
    assert _rows(database) == []


def test_the_lookup_is_exact_on_product_and_exchange(database: Path) -> None:
    _store(database, _ES_ECONOMICS)

    assert _get(database, _reference("ES", "EUREX")) is None
    assert _get(database, _reference("MES", "CME")) is None
    assert _get(database, _ES) == _ES_ECONOMICS


def test_products_exchanges_and_currencies_resolve_independently(database: Path) -> None:
    stored = (
        _ES_ECONOMICS,
        _economics("MES", amount="5"),
        _economics("FESX", "EUREX", amount="10", currency="EUR"),
        _economics("ES", "EUREX", amount="7.25", currency="EUR"),
    )
    _store(database, *stored)

    for economics in stored:
        assert _get(database, economics.reference) == economics
    assert [_get(database, e.reference).point_value.currency for e in stored] == [
        Currency("USD"),
        Currency("USD"),
        Currency("EUR"),
        Currency("EUR"),
    ]


def test_every_expiry_of_a_product_shares_one_row(database: Path) -> None:
    _store(database, _ES_ECONOMICS)
    december = FuturesContract(_ES, ExpirationDate("2026-12-18"))
    march = FuturesContract(_ES, ExpirationDate("2027-03-19"))

    assert _get(database, december.product) == _get(database, march.product) == _ES_ECONOMICS
    assert len(_rows(database)) == 1


@pytest.mark.parametrize(
    "reference",
    [("ES", "CME"), "ES@CME", FuturesContract(_ES, ExpirationDate("2026-12-18")), None],
    ids=["tuple", "string", "contract", "none"],
)
def test_the_repository_rejects_a_non_reference(database: Path, reference) -> None:
    with pytest.raises(TypeError, match="FuturesProductReference"):
        _get(database, reference)


def test_the_repository_writes_no_rows(database: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _store(database, _ES_ECONOMICS)
    statements = _trace(monkeypatch)

    _get(database)
    _get(database, _reference("MES"))

    assert statements
    for statement in statements:
        assert statement.lstrip().startswith(("SELECT", "CREATE TABLE IF NOT EXISTS"))
    assert len(_rows(database)) == 1


def test_the_repository_implements_the_application_port() -> None:
    assert issubclass(SQLiteFuturesProductEconomicsRepository, FuturesProductEconomicsRepository)


# ---------------------------------------------------------------------------
# Corruption
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("column", "value"),
    [
        ("point_value_amount", "0"),
        ("point_value_amount", "-50"),
        ("point_value_amount", "-0"),
        ("point_value_amount", "abc"),
        ("point_value_amount", ""),
        ("point_value_amount", "NaN"),
        ("point_value_amount", "Infinity"),
        ("point_value_amount", "50.0"),
        ("point_value_amount", "5E+1"),
        ("point_value_amount", " 50"),
        ("point_value_amount", b"50"),
        ("settlement_currency", "US"),
        ("settlement_currency", "usd"),
        ("settlement_currency", ""),
        ("settlement_currency", b"USD"),
    ],
)
def test_a_corrupt_row_fails_loudly(database: Path, column: str, value: object) -> None:
    _store(database, _ES_ECONOMICS)
    _execute(database, f"UPDATE futures_product_economics SET {column} = ?", (value,))  # noqa: S608

    with pytest.raises(FuturesProductEconomicsStorageError):
        _get(database)


@pytest.mark.parametrize(
    "row",
    [
        ("", "CME", "50", "USD"),
        ("es", "CME", "50", "USD"),
        ("ES", "", "50", "USD"),
        ("ES", "cme", "50", "USD"),
        (b"ES", "CME", "50", "USD"),
        ("ES", "CME", "50"),
    ],
    ids=["empty-product", "lower-product", "empty-exchange", "lower-exchange", "blob", "short"],
)
def test_a_corrupt_key_is_refused_by_the_decoder(row: tuple) -> None:
    with pytest.raises(FuturesProductEconomicsStorageError):
        module._decode(row)


def test_a_corrupt_key_is_never_matched_to_a_valid_reference(database: Path) -> None:
    _initialize(database, initialize_futures_product_economics_schema)
    _execute(
        database,
        "INSERT INTO futures_product_economics VALUES (?, ?, ?, ?)",
        ("es", "cme", "50", "USD"),
    )

    assert _get(database) is None


def test_a_store_over_a_corrupt_existing_row_fails_and_repairs_nothing(database: Path) -> None:
    _store(database, _ES_ECONOMICS)
    _execute(database, "UPDATE futures_product_economics SET point_value_amount = '0'")

    with pytest.raises(FuturesProductEconomicsStorageError):
        _store(database, _ES_ECONOMICS)

    assert _rows(database) == [("ES", "CME", "0", "USD")]


# ---------------------------------------------------------------------------
# Structure, transactions, boundaries and exports
# ---------------------------------------------------------------------------


def _non_docstring_strings(tree: ast.Module) -> list[str]:
    docstrings = {
        id(node.body[0].value)
        for node in ast.walk(tree)
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef))
        and node.body
        and isinstance(node.body[0], ast.Expr)
        and isinstance(node.body[0].value, ast.Constant)
    }
    return [
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and id(node) not in docstrings
    ]


def _tree(source) -> ast.Module:
    return ast.parse(Path(source.__file__).read_text(encoding="utf-8"))


@pytest.mark.parametrize("source", [module, schema], ids=["adapters", "schema"])
def test_the_economics_persistence_issues_insert_only_sql(source) -> None:
    sql = " ".join(_non_docstring_strings(_tree(source))).upper()

    for forbidden in ("UPDATE ", "DELETE ", "REPLACE", "ON CONFLICT", "UPSERT", "DROP ", "ALTER "):
        assert forbidden not in sql
    for forbidden in ("REAL", "FLOAT", "NUMERIC", "INTEGER"):
        assert forbidden not in sql


def test_the_adapter_inserts_into_the_economics_table() -> None:
    sql = " ".join(_non_docstring_strings(_tree(module))).upper()

    assert "INSERT INTO FUTURES_PRODUCT_ECONOMICS" in sql


def _trace(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    statements: list[str] = []
    real_connect = sqlite3.connect

    def connect(*args, **kwargs):
        connection = real_connect(*args, **kwargs)
        connection.set_trace_callback(statements.append)
        return connection

    monkeypatch.setattr(module.sqlite3, "connect", connect)
    return statements


def test_the_store_writes_inside_begin_immediate(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    statements = _trace(monkeypatch)

    _store(database, _ES_ECONOMICS)

    begin = statements.index("BEGIN IMMEDIATE")
    select = next(i for i, s in enumerate(statements) if s.startswith("SELECT"))
    insert = next(
        i for i, s in enumerate(statements) if s.startswith("INSERT INTO futures_product_economics")
    )
    assert begin < select < insert
    assert statements[-1] == "COMMIT"


def test_the_adapters_depend_on_no_provider_clock_or_calculation() -> None:
    tree = _tree(module)
    modules = {
        node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.module
    } | {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }

    for module_path in modules:
        assert module_path.split(".")[0] not in {
            "databento",
            "exchange_calendars",
            "requests",
            "urllib",
            "socket",
            "time",
            "datetime",
            "os",
            "dotenv",
            "json",
            "pickle",
        }
        assert "application_services" not in module_path
        assert "market_data" not in module_path
    assert "northstar_application.ports" in modules


def test_the_surface_is_exported_and_the_helpers_are_not() -> None:
    for name in (
        "SQLiteFuturesProductEconomicsStore",
        "SQLiteFuturesProductEconomicsRepository",
        "FuturesProductEconomicsStorageError",
        "FuturesProductEconomicsConflictError",
        "initialize_futures_product_economics_schema",
        "FUTURES_PRODUCT_ECONOMICS_SCHEMA",
    ):
        assert name in persistence.__all__
        assert hasattr(persistence, name)
    for private in ("_row", "_decode", "_economics", "_key", "_select"):
        assert not hasattr(persistence, private)
