"""Tests for the SQLite futures contract economics store and repository."""

from __future__ import annotations

import ast
import sqlite3
from decimal import ROUND_DOWN, Decimal, localcontext
from pathlib import Path

import pytest
from northstar_application.ports import (
    FuturesContractEconomicsConflictError as ApplicationConflictError,
)
from northstar_application.ports import (
    FuturesContractEconomicsRepository,
    FuturesContractEconomicsStore,
)
from northstar_core.derivatives import ExpirationDate
from northstar_core.foundation.value_objects import Currency, ExchangeCode, Symbol
from northstar_core.futures import (
    FuturesContract,
    FuturesContractEconomics,
    FuturesPointValue,
    FuturesProductEconomics,
    FuturesProductReference,
)

import northstar_infrastructure.persistence as persistence
import northstar_infrastructure.persistence.sqlite_futures_contract_economics as module
import northstar_infrastructure.persistence.sqlite_futures_contract_economics_schema as schema
from northstar_infrastructure.market_data.sqlite_futures_schema import (
    initialize_futures_market_data_schema,
)
from northstar_infrastructure.persistence import (
    FuturesContractEconomicsConflictError,
    FuturesContractEconomicsStorageError,
    SQLiteFuturesContractEconomicsRepository,
    SQLiteFuturesContractEconomicsStore,
    SQLiteFuturesProductEconomicsStore,
    initialize_futures_contract_economics_schema,
    initialize_futures_forward_research_record_schema,
    initialize_futures_paper_trading_schema,
    initialize_futures_product_economics_schema,
)


def _contract(
    product: str = "ES", exchange: str = "CME", expiration: str = "2026-12-18"
) -> FuturesContract:
    return FuturesContract(
        FuturesProductReference(Symbol(product), ExchangeCode(exchange)),
        ExpirationDate(expiration),
    )


def _economics(
    product: str = "ES",
    exchange: str = "CME",
    expiration: str = "2026-12-18",
    amount: str = "50",
    currency: str = "USD",
) -> FuturesContractEconomics:
    return FuturesContractEconomics(
        _contract(product, exchange, expiration),
        FuturesPointValue(Decimal(amount), Currency(currency)),
    )


_ES_DEC = _contract()
_ES_MAR = _contract(expiration="2027-03-19")
_ES_ECONOMICS = _economics()
# Historically plausible fixtures only: concurrent NIFTY expiries at lots of 75 and 65.
_NIFTY_NOV = _contract("NIFTY", "NSE", "2025-11-25")
_NIFTY_JAN = _contract("NIFTY", "NSE", "2026-01-27")
_NIFTY_NOV_ECONOMICS = _economics("NIFTY", "NSE", "2025-11-25", "75", "INR")
_NIFTY_JAN_ECONOMICS = _economics("NIFTY", "NSE", "2026-01-27", "65", "INR")
_HIGH_PRECISION = "12.345678901234567890123456789012345678901234567890123"


@pytest.fixture
def database(tmp_path: Path) -> Path:
    return tmp_path / "futures_contract_economics.sqlite3"


def _store(database: Path, *economics: FuturesContractEconomics) -> int:
    return SQLiteFuturesContractEconomicsStore(database).store(economics)


def _get(database: Path, contract: FuturesContract = _ES_DEC) -> FuturesContractEconomics | None:
    return SQLiteFuturesContractEconomicsRepository(database).get_economics(contract)


def _query(database: Path, sql: str, parameters: tuple = ()) -> list[tuple]:
    with sqlite3.connect(database) as connection:
        return connection.execute(sql, parameters).fetchall()


def _execute(database: Path, sql: str, parameters: tuple = ()) -> None:
    with sqlite3.connect(database) as connection:
        connection.execute(sql, parameters)


def _rows(database: Path) -> list[tuple]:
    return _query(database, "SELECT * FROM futures_contract_economics ORDER BY 1, 2, 3")


def _tables(database: Path) -> set[str]:
    return {row[0] for row in _query(database, "SELECT name FROM sqlite_master WHERE type='table'")}


def _initialize(database: Path, *initializers) -> None:
    with sqlite3.connect(database) as connection:
        for initializer in initializers:
            initializer(connection)


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


def test_a_fresh_database_gets_exactly_the_contract_economics_table(database: Path) -> None:
    _initialize(database, initialize_futures_contract_economics_schema)

    assert _tables(database) == {"futures_contract_economics"}


def test_the_columns_are_exactly_the_contract_key_and_point_value(database: Path) -> None:
    _initialize(database, initialize_futures_contract_economics_schema)

    columns = _query(database, "PRAGMA table_info(futures_contract_economics)")

    assert [(name, kind, bool(not_null), pk) for _, name, kind, not_null, _, pk in columns] == [
        ("product_code", "TEXT", True, 1),
        ("exchange_code", "TEXT", True, 2),
        ("expiration_date", "TEXT", True, 3),
        ("point_value_amount", "TEXT", True, 0),
        ("settlement_currency", "TEXT", True, 0),
    ]


def test_the_schema_has_no_lot_size_or_extra_economics(database: Path) -> None:
    _store(database, _ES_ECONOMICS)
    _get(database)

    columns = {row[1] for row in _query(database, "PRAGMA table_info(futures_contract_economics)")}

    assert _tables(database) == {"futures_contract_economics"}
    for forbidden in (
        "lot",
        "multiplier",
        "underlying",
        "tick",
        "margin",
        "notional",
        "effective",
        "provider",
        "created",
        "updated",
        "pnl",
        "mark",
    ):
        assert not [column for column in columns if forbidden in column]


def test_existing_futures_tables_and_product_economics_are_left_untouched(database: Path) -> None:
    _initialize(
        database,
        initialize_futures_market_data_schema,
        initialize_futures_forward_research_record_schema,
        initialize_futures_paper_trading_schema,
        initialize_futures_product_economics_schema,
    )
    SQLiteFuturesProductEconomicsStore(database).store(
        (
            FuturesProductEconomics(
                _ES_DEC.product, FuturesPointValue(Decimal("50"), Currency("USD"))
            ),
        )
    )
    before = set(_query(database, "SELECT type, name, sql FROM sqlite_master"))
    product_rows = _query(database, "SELECT * FROM futures_product_economics")

    _initialize(database, initialize_futures_contract_economics_schema)
    _store(database, _ES_ECONOMICS)
    after = set(_query(database, "SELECT type, name, sql FROM sqlite_master"))

    assert {name for _, name, _ in before} >= {
        "futures_ohlcv",
        "futures_forward_research_records",
        "futures_paper_orders",
        "futures_paper_fills",
        "futures_product_economics",
    }
    assert before <= after
    assert {name for _, name, _ in after - before} == {
        "futures_contract_economics",
        "sqlite_autoindex_futures_contract_economics_1",
    }
    assert _query(database, "SELECT * FROM futures_product_economics") == product_rows


def test_initialization_is_repeatable(database: Path) -> None:
    _initialize(database, initialize_futures_contract_economics_schema)
    _store(database, _ES_ECONOMICS)

    _initialize(database, initialize_futures_contract_economics_schema)

    assert _get(database) == _ES_ECONOMICS


# ---------------------------------------------------------------------------
# Store: round trip and serialization
# ---------------------------------------------------------------------------


def test_one_economics_value_round_trips_exactly(database: Path) -> None:
    assert _store(database, _ES_ECONOMICS) == 1

    assert _get(database) == _ES_ECONOMICS


def test_every_column_is_stored_as_canonical_text(database: Path) -> None:
    _store(
        database,
        _economics(amount="12.500"),
        _economics("NIFTY", "NSE", "2026-10-27", "6.5E+1", "INR"),
    )

    assert _rows(database) == [
        ("ES", "CME", "2026-12-18", "12.5", "USD"),
        ("NIFTY", "NSE", "2026-10-27", "65", "INR"),
    ]
    assert _query(
        database,
        "SELECT DISTINCT typeof(product_code), typeof(exchange_code), typeof(expiration_date), "
        "typeof(point_value_amount), typeof(settlement_currency) FROM futures_contract_economics",
    ) == [("text", "text", "text", "text", "text")]


def test_a_high_precision_amount_round_trips_under_any_caller_context(database: Path) -> None:
    economics = _economics(amount=_HIGH_PRECISION)

    with localcontext() as ambient:
        ambient.prec = 6
        ambient.rounding = ROUND_DOWN
        _store(database, economics)
        read = _get(database)

    assert read == economics
    assert read.point_value.amount == Decimal(_HIGH_PRECISION)
    assert _rows(database)[0][3] == _HIGH_PRECISION


# ---------------------------------------------------------------------------
# Store: coexistence of expiries, products and exchanges
# ---------------------------------------------------------------------------


def test_expiries_of_one_product_with_different_point_values_coexist(database: Path) -> None:
    """INDIA-1: NIFTY@NSE NOV at 75 INR and JAN at 65 INR, beside ES@CME at 50 USD."""
    assert _store(database, _NIFTY_NOV_ECONOMICS, _NIFTY_JAN_ECONOMICS, _ES_ECONOMICS) == 3

    assert _get(database, _NIFTY_NOV) == _NIFTY_NOV_ECONOMICS
    assert _get(database, _NIFTY_JAN) == _NIFTY_JAN_ECONOMICS
    assert _get(database, _ES_DEC) == _ES_ECONOMICS
    assert _rows(database) == [
        ("ES", "CME", "2026-12-18", "50", "USD"),
        ("NIFTY", "NSE", "2025-11-25", "75", "INR"),
        ("NIFTY", "NSE", "2026-01-27", "65", "INR"),
    ]


def test_a_second_expiry_is_stored_separately_not_as_a_conflict(database: Path) -> None:
    _store(database, _NIFTY_NOV_ECONOMICS)

    assert _store(database, _NIFTY_JAN_ECONOMICS) == 1
    assert _get(database, _NIFTY_NOV).point_value.amount == Decimal("75")
    assert _get(database, _NIFTY_JAN).point_value.amount == Decimal("65")


def test_one_expiry_date_on_two_exchanges_and_products_coexist(database: Path) -> None:
    stored = (
        _economics("ES", "CME", "2026-12-18", "50", "USD"),
        _economics("MES", "CME", "2026-12-18", "5", "USD"),
        _economics("ES", "EUREX", "2026-12-18", "7.25", "EUR"),
    )
    _store(database, *stored)

    for economics in stored:
        assert _get(database, economics.contract) == economics


# ---------------------------------------------------------------------------
# Store: idempotency, conflicts and batches
# ---------------------------------------------------------------------------


def test_an_equal_retry_is_idempotent(database: Path) -> None:
    _store(database, _ES_ECONOMICS)

    assert _store(database, _ES_ECONOMICS) == 1
    assert _store(database, _economics(amount="50.000")) == 1
    assert _rows(database) == [("ES", "CME", "2026-12-18", "50", "USD")]


@pytest.mark.parametrize(
    "changed",
    [
        _economics("NIFTY", "NSE", "2025-11-25", "65", "INR"),
        _economics("NIFTY", "NSE", "2025-11-25", "75.01", "INR"),
        _economics("NIFTY", "NSE", "2025-11-25", "75", "USD"),
    ],
    ids=["amount", "fractional-amount", "currency"],
)
def test_different_economics_for_a_stored_contract_conflict(
    database: Path, changed: FuturesContractEconomics
) -> None:
    _store(database, _NIFTY_NOV_ECONOMICS)

    with pytest.raises(
        FuturesContractEconomicsConflictError, match="already stored for NIFTY@NSE 2025-11-25"
    ):
        _store(database, changed)

    assert _get(database, _NIFTY_NOV) == _NIFTY_NOV_ECONOMICS
    assert _rows(database) == [("NIFTY", "NSE", "2025-11-25", "75", "INR")]


def test_an_empty_batch_returns_zero_and_touches_nothing(database: Path) -> None:
    assert _store(database) == 0
    assert not database.exists()


@pytest.mark.parametrize(
    "batch",
    [(_ES_ECONOMICS, _ES_ECONOMICS), (_ES_ECONOMICS, _economics(amount="5"))],
    ids=["equal", "different"],
)
def test_a_batch_repeating_one_contract_is_rejected(database: Path, batch: tuple) -> None:
    with pytest.raises(FuturesContractEconomicsConflictError, match="two entries for one contract"):
        SQLiteFuturesContractEconomicsStore(database).store(batch)

    assert not database.exists()


def test_a_batch_of_two_expiries_of_one_product_is_not_a_repeat(database: Path) -> None:
    assert _store(database, _NIFTY_NOV_ECONOMICS, _NIFTY_JAN_ECONOMICS) == 2


@pytest.mark.parametrize(
    "batch",
    [
        [_ES_ECONOMICS],
        (_ES_ECONOMICS, "ES@CME 2026-12-18"),
        (_ES_ECONOMICS.point_value,),
        (FuturesProductEconomics(_ES_DEC.product, _ES_ECONOMICS.point_value),),
    ],
    ids=["list", "string-member", "point-value-member", "product-economics-member"],
)
def test_foreign_batches_are_rejected_before_any_write(database: Path, batch) -> None:
    with pytest.raises(TypeError):
        SQLiteFuturesContractEconomicsStore(database).store(batch)

    assert not database.exists()


def test_one_conflict_leaves_the_whole_batch_uncommitted(database: Path) -> None:
    _store(database, _ES_ECONOMICS)

    with pytest.raises(FuturesContractEconomicsConflictError):
        _store(database, _NIFTY_NOV_ECONOMICS, _economics(amount="51"))

    assert _get(database, _NIFTY_NOV) is None
    assert _rows(database) == [("ES", "CME", "2026-12-18", "50", "USD")]


def test_idempotent_and_new_entries_both_count(database: Path) -> None:
    _store(database, _ES_ECONOMICS)

    assert _store(database, _ES_ECONOMICS, _economics(expiration="2027-03-19")) == 2
    assert len(_rows(database)) == 2


def test_economics_survive_restart_and_retries_stay_idempotent(tmp_path: Path) -> None:
    path = tmp_path / "restart.sqlite3"
    economics = _economics(amount=_HIGH_PRECISION, currency="EUR")
    SQLiteFuturesContractEconomicsStore(path).store((economics,))

    assert SQLiteFuturesContractEconomicsRepository(path).get_economics(_ES_DEC) == economics
    assert SQLiteFuturesContractEconomicsStore(path).store((economics,)) == 1
    with pytest.raises(FuturesContractEconomicsConflictError):
        SQLiteFuturesContractEconomicsStore(path).store((_ES_ECONOMICS,))
    assert len(_rows(path)) == 1


# ---------------------------------------------------------------------------
# Store: races
# ---------------------------------------------------------------------------


def _race(monkeypatch: pytest.MonkeyPatch) -> None:
    real = SQLiteFuturesContractEconomicsStore._existing
    calls = {"count": 0}

    def racing(connection, contract):
        calls["count"] += 1
        return None if calls["count"] == 1 else real(connection, contract)

    monkeypatch.setattr(SQLiteFuturesContractEconomicsStore, "_existing", staticmethod(racing))


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

    with pytest.raises(FuturesContractEconomicsConflictError):
        _store(database, _economics(currency="EUR"))

    monkeypatch.undo()
    assert _get(database) == _ES_ECONOMICS


# ---------------------------------------------------------------------------
# Repository: exact lookup, never a fallback
# ---------------------------------------------------------------------------


def test_a_missing_contract_is_none_not_a_default(database: Path) -> None:
    assert _get(database) is None
    assert _rows(database) == []


def test_the_lookup_is_exact_on_product_exchange_and_expiration(database: Path) -> None:
    _store(database, _ES_ECONOMICS)

    assert _get(database, _contract("ES", "EUREX")) is None
    assert _get(database, _contract("MES", "CME")) is None
    assert _get(database, _ES_MAR) is None
    assert _get(database, _ES_DEC) == _ES_ECONOMICS


def test_an_unconfigured_expiry_never_falls_back_to_another(database: Path) -> None:
    _store(database, _NIFTY_NOV_ECONOMICS)

    assert _get(database, _NIFTY_JAN) is None
    assert _get(database, _contract("NIFTY", "NSE", "2025-12-30")) is None
    assert _get(database, _contract("NIFTY", "NSE", "2025-10-28")) is None


def test_another_markets_economics_never_answer_for_nifty(database: Path) -> None:
    _store(database, _ES_ECONOMICS, _economics(expiration="2025-11-25"))

    assert _get(database, _NIFTY_NOV) is None


def test_product_economics_are_never_read_as_contract_economics(database: Path) -> None:
    SQLiteFuturesProductEconomicsStore(database).store(
        (
            FuturesProductEconomics(
                _NIFTY_NOV.product, FuturesPointValue(Decimal("75"), Currency("INR"))
            ),
        )
    )

    assert _get(database, _NIFTY_NOV) is None
    assert _get(database, _NIFTY_JAN) is None
    assert _rows(database) == []


@pytest.mark.parametrize(
    "contract",
    [
        ("ES", "CME", "2026-12-18"),
        "ES@CME 2026-12-18",
        FuturesProductReference(Symbol("ES"), ExchangeCode("CME")),
        None,
    ],
    ids=["tuple", "string", "product-reference", "none"],
)
def test_the_repository_rejects_a_non_contract(database: Path, contract) -> None:
    with pytest.raises(TypeError, match="FuturesContract"):
        _get(database, contract)


def test_the_repository_writes_no_rows(database: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _store(database, _ES_ECONOMICS)
    statements = _trace(monkeypatch)

    _get(database)
    _get(database, _ES_MAR)

    assert statements
    for statement in statements:
        assert statement.lstrip().startswith(("SELECT", "CREATE TABLE IF NOT EXISTS"))
        assert "futures_product_economics" not in statement
    assert len(_rows(database)) == 1


def test_the_repository_implements_the_application_port() -> None:
    assert issubclass(SQLiteFuturesContractEconomicsRepository, FuturesContractEconomicsRepository)


def test_the_store_implements_the_application_port(database: Path) -> None:
    assert issubclass(SQLiteFuturesContractEconomicsStore, FuturesContractEconomicsStore)
    assert isinstance(SQLiteFuturesContractEconomicsStore(database), FuturesContractEconomicsStore)


def test_the_conflict_error_is_the_applications_own_class() -> None:
    assert FuturesContractEconomicsConflictError is ApplicationConflictError
    assert persistence.FuturesContractEconomicsConflictError is ApplicationConflictError
    assert ApplicationConflictError.__module__.startswith("northstar_application.ports")
    assert "FuturesContractEconomicsConflictError" not in {
        node.name for node in ast.walk(_tree(module)) if isinstance(node, ast.ClassDef)
    }


@pytest.mark.parametrize(
    "changed",
    [_economics(amount="51"), _economics(currency="EUR")],
    ids=["amount", "currency"],
)
def test_a_conflict_raises_the_application_error(
    database: Path, changed: FuturesContractEconomics
) -> None:
    _store(database, _ES_ECONOMICS)

    with pytest.raises(ApplicationConflictError):
        _store(database, changed)

    assert _store(database, _ES_ECONOMICS) == 1
    assert _get(database) == _ES_ECONOMICS


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
    _execute(database, f"UPDATE futures_contract_economics SET {column} = ?", (value,))  # noqa: S608

    with pytest.raises(FuturesContractEconomicsStorageError):
        _get(database)


@pytest.mark.parametrize(
    "row",
    [
        ("", "CME", "2026-12-18", "50", "USD"),
        ("es", "CME", "2026-12-18", "50", "USD"),
        ("ES", "", "2026-12-18", "50", "USD"),
        ("ES", "cme", "2026-12-18", "50", "USD"),
        ("ES", "CME", "", "50", "USD"),
        ("ES", "CME", "2026-02-30", "50", "USD"),
        ("ES", "CME", " 2026-12-18", "50", "USD"),
        ("ES", "CME", "20261218", "50", "USD"),
        (b"ES", "CME", "2026-12-18", "50", "USD"),
        ("ES", "CME", "50", "USD"),
    ],
    ids=[
        "empty-product",
        "lower-product",
        "empty-exchange",
        "lower-exchange",
        "empty-expiration",
        "impossible-expiration",
        "padded-expiration",
        "compact-expiration",
        "blob",
        "short",
    ],
)
def test_a_corrupt_key_is_refused_by_the_decoder(row: tuple) -> None:
    with pytest.raises(FuturesContractEconomicsStorageError):
        module._decode(row)


def test_a_corrupt_key_is_never_matched_to_a_valid_contract(database: Path) -> None:
    _initialize(database, initialize_futures_contract_economics_schema)
    _execute(
        database,
        "INSERT INTO futures_contract_economics VALUES (?, ?, ?, ?, ?)",
        ("es", "cme", "2026-12-18", "50", "USD"),
    )

    assert _get(database) is None


def test_a_store_over_a_corrupt_existing_row_fails_and_repairs_nothing(database: Path) -> None:
    _store(database, _ES_ECONOMICS)
    _execute(database, "UPDATE futures_contract_economics SET point_value_amount = '0'")

    with pytest.raises(FuturesContractEconomicsStorageError):
        _store(database, _ES_ECONOMICS)

    assert _rows(database) == [("ES", "CME", "2026-12-18", "0", "USD")]


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


@pytest.mark.parametrize("source", [module, schema], ids=["adapters", "schema"])
def test_the_contract_persistence_never_names_the_product_table(source) -> None:
    sql = " ".join(_non_docstring_strings(_tree(source))).lower()

    assert "futures_product_economics" not in sql


def test_the_adapter_inserts_into_the_contract_economics_table() -> None:
    sql = " ".join(_non_docstring_strings(_tree(module))).upper()

    assert "INSERT INTO FUTURES_CONTRACT_ECONOMICS" in sql
    assert "EXPIRATION_DATE = ?" in sql


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
        i
        for i, s in enumerate(statements)
        if s.startswith("INSERT INTO futures_contract_economics")
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
        assert "product_economics" not in module_path
    assert "northstar_application.ports" in modules


def test_the_surface_is_exported_and_the_helpers_are_not() -> None:
    for name in (
        "SQLiteFuturesContractEconomicsStore",
        "SQLiteFuturesContractEconomicsRepository",
        "FuturesContractEconomicsStorageError",
        "FuturesContractEconomicsConflictError",
        "initialize_futures_contract_economics_schema",
        "FUTURES_CONTRACT_ECONOMICS_SCHEMA",
    ):
        assert name in persistence.__all__
        assert hasattr(persistence, name)
    for private in ("_row", "_decode", "_economics", "_key", "_select"):
        assert not hasattr(persistence, private)
