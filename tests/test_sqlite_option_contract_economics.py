"""Tests for the SQLite option contract economics store and repository."""

from __future__ import annotations

import ast
import sqlite3
from decimal import ROUND_DOWN, Decimal, localcontext
from pathlib import Path

import pytest
from northstar_application.ports import (
    OptionContractEconomicsConflictError as ApplicationConflictError,
)
from northstar_application.ports import (
    OptionContractEconomicsRepository,
    OptionContractEconomicsStore,
)
from northstar_core.derivatives import ExpirationDate
from northstar_core.foundation.value_objects import Currency, ExchangeCode, Symbol
from northstar_core.futures import (
    FuturesContract,
    FuturesContractEconomics,
    FuturesPointValue,
    FuturesProductReference,
)
from northstar_core.options import (
    OptionContract,
    OptionContractEconomics,
    OptionPointValue,
    OptionProductReference,
    OptionRight,
    OptionStrike,
)

import northstar_infrastructure.persistence as persistence
import northstar_infrastructure.persistence.sqlite_option_contract_economics as module
import northstar_infrastructure.persistence.sqlite_option_contract_economics_schema as schema
from northstar_infrastructure.market_data.sqlite_futures_schema import (
    initialize_futures_market_data_schema,
)
from northstar_infrastructure.persistence import (
    OptionContractEconomicsConflictError,
    OptionContractEconomicsStorageError,
    SQLiteFuturesContractEconomicsRepository,
    SQLiteFuturesContractEconomicsStore,
    SQLiteOptionContractEconomicsRepository,
    SQLiteOptionContractEconomicsStore,
    initialize_futures_contract_economics_schema,
    initialize_futures_forward_research_record_schema,
    initialize_futures_paper_trading_schema,
    initialize_option_contract_economics_schema,
)

_CALL = OptionRight.CALL
_PUT = OptionRight.PUT


def _contract(
    product: str = "NIFTY",
    exchange: str = "NSE",
    expiration: str = "2026-10-27",
    strike: str = "25000",
    right: OptionRight = _CALL,
) -> OptionContract:
    return OptionContract(
        OptionProductReference(Symbol(product), ExchangeCode(exchange)),
        ExpirationDate(expiration),
        OptionStrike(Decimal(strike)),
        right,
    )


def _economics(
    contract: OptionContract | None = None, amount: str = "65", currency: str = "INR"
) -> OptionContractEconomics:
    return OptionContractEconomics(
        _contract() if contract is None else contract,
        OptionPointValue(Decimal(amount), Currency(currency)),
    )


_NIFTY_CALL = _contract()
_NIFTY_ECONOMICS = _economics()
_ROW = ("NIFTY", "NSE", "2026-10-27", "25000", "CALL", "65", "INR")
_HIGH_PRECISION = "65.345678901234567890123456789012345678901234567890123"


@pytest.fixture
def database(tmp_path: Path) -> Path:
    return tmp_path / "option_contract_economics.sqlite3"


def _store(database: Path, *economics: OptionContractEconomics) -> int:
    return SQLiteOptionContractEconomicsStore(database).store(economics)


def _get(database: Path, contract: OptionContract = _NIFTY_CALL) -> OptionContractEconomics | None:
    return SQLiteOptionContractEconomicsRepository(database).get_economics(contract)


def _query(database: Path, sql: str, parameters: tuple = ()) -> list[tuple]:
    with sqlite3.connect(database) as connection:
        return connection.execute(sql, parameters).fetchall()


def _execute(database: Path, sql: str, parameters: tuple = ()) -> None:
    with sqlite3.connect(database) as connection:
        connection.execute(sql, parameters)


def _rows(database: Path) -> list[tuple]:
    return _query(database, "SELECT * FROM option_contract_economics ORDER BY 1, 2, 3, 4, 5")


def _tables(database: Path) -> set[str]:
    return {row[0] for row in _query(database, "SELECT name FROM sqlite_master WHERE type='table'")}


def _initialize(database: Path, *initializers) -> None:
    with sqlite3.connect(database) as connection:
        for initializer in initializers:
            initializer(connection)


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


def test_a_fresh_database_gets_exactly_the_option_economics_table(database: Path) -> None:
    _initialize(database, initialize_option_contract_economics_schema)

    assert _tables(database) == {"option_contract_economics"}


def test_the_columns_are_exactly_the_contract_key_and_point_value_in_order(
    database: Path,
) -> None:
    _initialize(database, initialize_option_contract_economics_schema)
    columns = _query(database, "PRAGMA table_info(option_contract_economics)")

    assert [(name, kind, notnull) for _, name, kind, notnull, _, _ in columns] == [
        ("product_code", "TEXT", 1),
        ("exchange_code", "TEXT", 1),
        ("expiration_date", "TEXT", 1),
        ("strike", "TEXT", 1),
        ("option_right", "TEXT", 1),
        ("point_value_amount", "TEXT", 1),
        ("settlement_currency", "TEXT", 1),
    ]


def test_the_primary_key_is_exactly_the_five_part_contract(database: Path) -> None:
    _initialize(database, initialize_option_contract_economics_schema)
    columns = _query(database, "PRAGMA table_info(option_contract_economics)")

    key = sorted((position, name) for _, name, _, _, _, position in columns if position)
    assert [name for _, name in key] == [
        "product_code",
        "exchange_code",
        "expiration_date",
        "strike",
        "option_right",
    ]


def test_the_schema_has_no_lot_size_or_extra_economics(database: Path) -> None:
    _initialize(database, initialize_option_contract_economics_schema)
    columns = {row[1] for row in _query(database, "PRAGMA table_info(option_contract_economics)")}

    for forbidden in (
        "lot",
        "multiplier",
        "underlying",
        "premium",
        "count",
        "tick",
        "margin",
        "fee",
        "instrument_key",
        "provider",
        "pnl",
        "created",
        "updated",
    ):
        assert not [column for column in columns if forbidden in column]


def test_existing_futures_tables_are_left_untouched(database: Path) -> None:
    _initialize(
        database,
        initialize_futures_market_data_schema,
        initialize_futures_forward_research_record_schema,
        initialize_futures_paper_trading_schema,
        initialize_futures_contract_economics_schema,
    )
    futures = FuturesContractEconomics(
        FuturesContract(
            FuturesProductReference(Symbol("NIFTY"), ExchangeCode("NSE")),
            ExpirationDate("2026-10-27"),
        ),
        FuturesPointValue(Decimal("65"), Currency("INR")),
    )
    SQLiteFuturesContractEconomicsStore(database).store((futures,))
    before = set(_query(database, "SELECT type, name, sql FROM sqlite_master"))
    futures_rows = _query(database, "SELECT * FROM futures_contract_economics")

    _initialize(database, initialize_option_contract_economics_schema)
    _store(database, _NIFTY_ECONOMICS)
    after = set(_query(database, "SELECT type, name, sql FROM sqlite_master"))

    assert before <= after
    assert {name for _, name, _ in after - before} == {
        "option_contract_economics",
        "sqlite_autoindex_option_contract_economics_1",
    }
    assert _query(database, "SELECT * FROM futures_contract_economics") == futures_rows
    assert SQLiteFuturesContractEconomicsRepository(database).get_economics(futures.contract) == (
        futures
    )


def test_initialization_is_repeatable(database: Path) -> None:
    _initialize(database, initialize_option_contract_economics_schema)
    _store(database, _NIFTY_ECONOMICS)

    _initialize(database, initialize_option_contract_economics_schema)

    assert _get(database) == _NIFTY_ECONOMICS
    assert _tables(database) == {"option_contract_economics"}


def test_only_a_store_creates_the_option_table(database: Path) -> None:
    """A lookup on a fresh path creates nothing; the first write creates only the option table."""
    assert _get(database) is None
    assert not database.exists()

    _store(database, _NIFTY_ECONOMICS)
    assert _tables(database) == {"option_contract_economics"}


# ---------------------------------------------------------------------------
# Repository: genuinely read-only
# ---------------------------------------------------------------------------


def _schema(database: Path) -> list[tuple]:
    return sorted(_query(database, "SELECT type, name, sql FROM sqlite_master"))


def test_a_lookup_on_a_missing_file_is_none_and_creates_no_file(database: Path) -> None:
    assert _get(database) is None
    assert _get(database, _contract(right=_PUT)) is None
    assert not database.exists()


def test_a_lookup_without_the_option_table_is_none_and_creates_nothing(database: Path) -> None:
    _initialize(
        database,
        initialize_futures_market_data_schema,
        initialize_futures_forward_research_record_schema,
        initialize_futures_paper_trading_schema,
        initialize_futures_contract_economics_schema,
    )
    before = _schema(database)

    assert _get(database) is None

    assert _schema(database) == before
    assert "option_contract_economics" not in _tables(database)


def test_a_lookup_in_an_empty_database_file_creates_nothing(database: Path) -> None:
    sqlite3.connect(database).close()
    assert database.exists()

    assert _get(database) is None
    assert _tables(database) == set()


def test_a_lookup_never_runs_ddl_or_the_initializer(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _store(database, _NIFTY_ECONOMICS)
    _initialize(database, initialize_futures_contract_economics_schema)

    def forbidden(connection) -> None:
        raise AssertionError("a lookup must never initialize the schema")

    monkeypatch.setattr(module, "initialize_option_contract_economics_schema", forbidden)
    statements = _trace(monkeypatch)

    assert _get(database) == _NIFTY_ECONOMICS
    assert _get(database, _contract(strike="25050")) is None

    for statement in statements:
        assert (
            not statement.upper()
            .lstrip()
            .startswith(
                ("CREATE", "ALTER", "DROP", "INSERT", "UPDATE", "DELETE", "REPLACE", "BEGIN")
            )
        ), statement


def test_a_lookup_opens_the_database_read_only(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _store(database, _NIFTY_ECONOMICS)
    opened: list[tuple[tuple, dict]] = []
    real_connect = sqlite3.connect

    def connect(*args, **kwargs):
        opened.append((args, kwargs))
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(module.sqlite3, "connect", connect)

    assert _get(database) == _NIFTY_ECONOMICS
    assert len(opened) == 1
    (target,), options = opened[0]
    assert target.startswith("file:") and target.endswith("?mode=ro")
    assert options == {"uri": True}


def test_a_lookup_against_a_directory_is_a_storage_error(tmp_path: Path) -> None:
    with pytest.raises(OptionContractEconomicsStorageError, match="unavailable"):
        _get(tmp_path)


# ---------------------------------------------------------------------------
# Store: round trip and serialization
# ---------------------------------------------------------------------------


def test_one_economics_value_round_trips_exactly(database: Path) -> None:
    assert _store(database, _NIFTY_ECONOMICS) == 1
    assert _get(database) == _NIFTY_ECONOMICS


def test_every_column_is_stored_as_canonical_text(database: Path) -> None:
    respelled = _economics(_contract(strike="2.5E+4"), amount="65.000", currency="inr")

    _store(database, respelled)

    assert _rows(database) == [_ROW]
    types = _query(
        database,
        "SELECT "
        + ", ".join(f"typeof({column})" for column in module._COLUMNS)  # noqa: S608
        + " FROM option_contract_economics",
    )
    assert types == [("text",) * 7]


def test_a_fractional_strike_round_trips_in_canonical_spelling(database: Path) -> None:
    economics = _economics(_contract(strike="24950.50"))

    _store(database, economics)

    assert _rows(database)[0][3] == "24950.5"
    assert _get(database, _contract(strike="24950.5")) == economics


def test_a_high_precision_amount_round_trips_under_any_caller_context(database: Path) -> None:
    economics = _economics(amount=_HIGH_PRECISION)

    with localcontext() as context:
        context.prec = 6
        context.rounding = ROUND_DOWN
        _store(database, economics)
        found = _get(database)

    assert found == economics
    assert str(found.point_value.amount) == _HIGH_PRECISION
    assert _rows(database)[0][5] == _HIGH_PRECISION


# ---------------------------------------------------------------------------
# Store: every contract is its own key
# ---------------------------------------------------------------------------


def test_call_and_put_at_one_strike_coexist(database: Path) -> None:
    call = _economics(_contract(right=_CALL), amount="65")
    put = _economics(_contract(right=_PUT), amount="75")

    assert _store(database, call, put) == 2
    assert _get(database, call.contract) == call
    assert _get(database, put.contract) == put


def test_several_strikes_of_one_expiry_coexist(database: Path) -> None:
    economics = [_economics(_contract(strike=strike)) for strike in ("24950", "25000", "25050")]

    assert _store(database, *economics) == 3
    for entry in economics:
        assert _get(database, entry.contract) == entry


def test_expiries_of_one_product_with_different_point_values_coexist(database: Path) -> None:
    december = _economics(_contract(expiration="2025-12-30"), amount="75")
    january = _economics(_contract(expiration="2026-01-27"), amount="65")

    assert _store(database, december) == 1
    assert _store(database, january) == 1
    assert _get(database, december.contract) == december
    assert _get(database, january.contract) == january


# ---------------------------------------------------------------------------
# Store: immutability, idempotency and batches
# ---------------------------------------------------------------------------


def test_an_equal_retry_is_idempotent(database: Path) -> None:
    _store(database, _NIFTY_ECONOMICS)

    assert _store(database, _economics(amount="65.0", currency="inr")) == 1
    assert _rows(database) == [_ROW]


@pytest.mark.parametrize(
    "changed",
    [_economics(amount="75"), _economics(currency="USD")],
    ids=["amount", "currency"],
)
def test_different_economics_for_a_stored_contract_conflict(
    database: Path, changed: OptionContractEconomics
) -> None:
    _store(database, _NIFTY_ECONOMICS)

    with pytest.raises(OptionContractEconomicsConflictError, match="NIFTY@NSE 2026-10-27 25000"):
        _store(database, changed)

    assert _rows(database) == [_ROW]


def test_an_empty_batch_returns_zero_and_touches_nothing(database: Path) -> None:
    assert _store(database) == 0
    assert not database.exists()


@pytest.mark.parametrize(
    "batch",
    [(_NIFTY_ECONOMICS, _NIFTY_ECONOMICS), (_NIFTY_ECONOMICS, _economics(amount="75"))],
    ids=["equal", "different"],
)
def test_a_batch_repeating_one_contract_is_rejected(database: Path, batch: tuple) -> None:
    with pytest.raises(OptionContractEconomicsConflictError, match="two entries"):
        SQLiteOptionContractEconomicsStore(database).store(batch)
    assert not database.exists()


@pytest.mark.parametrize(
    "batch",
    [
        [_NIFTY_ECONOMICS],
        (_NIFTY_ECONOMICS, _NIFTY_ECONOMICS.point_value),
        (
            FuturesContractEconomics(
                FuturesContract(
                    FuturesProductReference(Symbol("NIFTY"), ExchangeCode("NSE")),
                    ExpirationDate("2026-10-27"),
                ),
                FuturesPointValue(Decimal("65"), Currency("INR")),
            ),
        ),
        None,
    ],
    ids=["list", "point-value", "futures-economics", "none"],
)
def test_foreign_batches_are_rejected_before_any_write(database: Path, batch) -> None:
    with pytest.raises(TypeError):
        SQLiteOptionContractEconomicsStore(database).store(batch)
    assert not database.exists()


def test_one_conflict_leaves_the_whole_batch_uncommitted(database: Path) -> None:
    _store(database, _NIFTY_ECONOMICS)
    fresh = _economics(_contract(strike="25050"))

    with pytest.raises(OptionContractEconomicsConflictError):
        _store(database, fresh, _economics(amount="75"))

    assert _rows(database) == [_ROW]
    assert _get(database, fresh.contract) is None


def test_idempotent_and_new_entries_both_count(database: Path) -> None:
    _store(database, _NIFTY_ECONOMICS)

    assert _store(database, _NIFTY_ECONOMICS, _economics(_contract(right=_PUT))) == 2
    assert len(_rows(database)) == 2


def test_economics_survive_reopen_and_retries_stay_idempotent(tmp_path: Path) -> None:
    path = tmp_path / "reopen.sqlite3"
    economics = _economics(amount=_HIGH_PRECISION)
    SQLiteOptionContractEconomicsStore(path).store((economics,))

    assert SQLiteOptionContractEconomicsRepository(path).get_economics(_NIFTY_CALL) == economics
    assert SQLiteOptionContractEconomicsStore(path).store((economics,)) == 1
    with pytest.raises(OptionContractEconomicsConflictError):
        SQLiteOptionContractEconomicsStore(path).store((_NIFTY_ECONOMICS,))
    assert len(_rows(path)) == 1


# ---------------------------------------------------------------------------
# Store: races
# ---------------------------------------------------------------------------


def _race(monkeypatch: pytest.MonkeyPatch) -> None:
    real = SQLiteOptionContractEconomicsStore._existing
    calls = {"count": 0}

    def racing(connection, contract):
        calls["count"] += 1
        return None if calls["count"] == 1 else real(connection, contract)

    monkeypatch.setattr(SQLiteOptionContractEconomicsStore, "_existing", staticmethod(racing))


def test_a_race_with_equal_economics_is_idempotent(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _store(database, _NIFTY_ECONOMICS)
    _race(monkeypatch)

    assert _store(database, _NIFTY_ECONOMICS) == 1
    assert len(_rows(database)) == 1


def test_a_race_with_different_economics_conflicts(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _store(database, _NIFTY_ECONOMICS)
    _race(monkeypatch)

    with pytest.raises(OptionContractEconomicsConflictError):
        _store(database, _economics(amount="75"))

    monkeypatch.undo()
    assert _get(database) == _NIFTY_ECONOMICS


# ---------------------------------------------------------------------------
# Repository: exact lookup, never a fallback
# ---------------------------------------------------------------------------


def test_a_missing_contract_is_none_not_a_default(database: Path) -> None:
    _store(database, _economics(_contract(strike="25050")))

    assert _get(database) is None
    assert len(_rows(database)) == 1


@pytest.mark.parametrize(
    "neighbour",
    [
        _contract(strike="25050"),
        _contract(strike="24950"),
        _contract(right=_PUT),
        _contract(expiration="2026-10-20"),
        _contract(product="BANKNIFTY"),
        _contract(exchange="BSE"),
    ],
    ids=["higher-strike", "lower-strike", "right", "expiry", "product", "exchange"],
)
def test_a_neighbouring_contract_never_answers(database: Path, neighbour: OptionContract) -> None:
    _store(database, _NIFTY_ECONOMICS)

    assert _get(database, neighbour) is None


def test_futures_economics_never_answer_for_an_option(database: Path) -> None:
    futures = FuturesContractEconomics(
        FuturesContract(
            FuturesProductReference(Symbol("NIFTY"), ExchangeCode("NSE")),
            ExpirationDate("2026-10-27"),
        ),
        FuturesPointValue(Decimal("65"), Currency("INR")),
    )
    SQLiteFuturesContractEconomicsStore(database).store((futures,))

    assert _get(database) is None
    assert _tables(database) == {"futures_contract_economics"}


@pytest.mark.parametrize(
    "contract",
    [
        None,
        "NIFTY@NSE 2026-10-27 25000 CALL",
        FuturesContract(
            FuturesProductReference(Symbol("NIFTY"), ExchangeCode("NSE")),
            ExpirationDate("2026-10-27"),
        ),
    ],
    ids=["none", "text", "futures-contract"],
)
def test_the_repository_rejects_a_non_option_contract(database: Path, contract) -> None:
    with pytest.raises(TypeError, match="must be an OptionContract"):
        SQLiteOptionContractEconomicsRepository(database).get_economics(contract)


def test_the_repository_writes_no_rows(database: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    statements = _trace(monkeypatch)

    _get(database)

    assert not [s for s in statements if s.upper().startswith(("INSERT", "UPDATE", "DELETE"))]


# ---------------------------------------------------------------------------
# Port conformance
# ---------------------------------------------------------------------------


def test_the_repository_implements_the_application_port(database: Path) -> None:
    assert isinstance(
        SQLiteOptionContractEconomicsRepository(database), OptionContractEconomicsRepository
    )


def test_the_store_implements_the_application_port(database: Path) -> None:
    assert isinstance(SQLiteOptionContractEconomicsStore(database), OptionContractEconomicsStore)


def test_the_conflict_error_is_the_applications_own_class() -> None:
    assert OptionContractEconomicsConflictError is ApplicationConflictError


def test_the_storage_error_is_a_runtime_error_of_its_own() -> None:
    assert issubclass(OptionContractEconomicsStorageError, RuntimeError)
    assert (
        OptionContractEconomicsStorageError is not persistence.FuturesContractEconomicsStorageError
    )


# ---------------------------------------------------------------------------
# Corruption
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("column", "value"),
    [
        ("strike", "25000.0"),
        ("strike", "2.5E+4"),
        ("strike", " 25000"),
        ("strike", "0"),
        ("strike", "-25000"),
        ("strike", "abc"),
        ("strike", "NaN"),
        ("strike", b"25000"),
        ("option_right", "call"),
        ("option_right", "CE"),
        ("option_right", "PE"),
        ("option_right", ""),
        ("option_right", b"CALL"),
        ("point_value_amount", "0"),
        ("point_value_amount", "-65"),
        ("point_value_amount", "abc"),
        ("point_value_amount", "NaN"),
        ("point_value_amount", "Infinity"),
        ("point_value_amount", "65.0"),
        ("point_value_amount", "6.5E+1"),
        ("point_value_amount", b"65"),
        ("settlement_currency", "IN"),
        ("settlement_currency", "inr"),
        ("settlement_currency", ""),
    ],
)
def test_a_corrupt_row_fails_loudly(database: Path, column: str, value: object) -> None:
    _store(database, _NIFTY_ECONOMICS)
    _execute(database, f"UPDATE option_contract_economics SET {column} = ?", (value,))  # noqa: S608
    stored_key = _query(
        database,
        "SELECT product_code, exchange_code, expiration_date, strike, option_right "
        "FROM option_contract_economics",
    )[0]

    with pytest.raises(OptionContractEconomicsStorageError):
        module._decode(_query(database, "SELECT * FROM option_contract_economics")[0])
    if stored_key == _ROW[:5]:
        with pytest.raises(OptionContractEconomicsStorageError):
            _get(database)


@pytest.mark.parametrize(
    "row",
    [
        ("", "NSE", "2026-10-27", "25000", "CALL", "65", "INR"),
        ("nifty", "NSE", "2026-10-27", "25000", "CALL", "65", "INR"),
        ("NIFTY", "nse", "2026-10-27", "25000", "CALL", "65", "INR"),
        ("NIFTY", "NSE", "2026-02-30", "25000", "CALL", "65", "INR"),
        ("NIFTY", "NSE", "20261027", "25000", "CALL", "65", "INR"),
        ("NIFTY", "NSE", "2026-10-27", "25000.00", "CALL", "65", "INR"),
        ("NIFTY", "NSE", "2026-10-27", "25000", "Call", "65", "INR"),
        ("NIFTY", "NSE", "2026-10-27", "25000", "CALL", "65"),
        (b"NIFTY", "NSE", "2026-10-27", "25000", "CALL", "65", "INR"),
    ],
    ids=[
        "empty-product",
        "lower-product",
        "lower-exchange",
        "impossible-expiration",
        "compact-expiration",
        "padded-strike",
        "mixed-case-right",
        "short",
        "blob",
    ],
)
def test_a_corrupt_key_is_refused_by_the_decoder(row: tuple) -> None:
    with pytest.raises(OptionContractEconomicsStorageError):
        module._decode(row)


def test_a_non_canonical_key_is_never_matched_to_a_valid_contract(database: Path) -> None:
    _initialize(database, initialize_option_contract_economics_schema)
    _execute(
        database,
        "INSERT INTO option_contract_economics VALUES (?, ?, ?, ?, ?, ?, ?)",
        ("NIFTY", "NSE", "2026-10-27", "25000.0", "call", "65", "INR"),
    )

    assert _get(database) is None


def test_a_store_over_a_corrupt_existing_row_fails_and_repairs_nothing(database: Path) -> None:
    _store(database, _NIFTY_ECONOMICS)
    _execute(database, "UPDATE option_contract_economics SET point_value_amount = '0'")

    with pytest.raises(OptionContractEconomicsStorageError):
        _store(database, _NIFTY_ECONOMICS)

    assert _rows(database) == [("NIFTY", "NSE", "2026-10-27", "25000", "CALL", "0", "INR")]


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
def test_the_option_persistence_never_names_a_futures_table(source) -> None:
    sql = " ".join(_non_docstring_strings(_tree(source))).lower()

    assert "futures_" not in sql


def test_the_adapter_inserts_into_the_option_economics_table_with_a_five_part_lookup() -> None:
    sql = " ".join(_non_docstring_strings(_tree(module))).upper()

    assert "INSERT INTO OPTION_CONTRACT_ECONOMICS" in sql
    for clause in (
        "PRODUCT_CODE = ?",
        "EXCHANGE_CODE = ?",
        "EXPIRATION_DATE = ?",
        "STRIKE = ?",
        "OPTION_RIGHT = ?",
    ):
        assert clause in sql


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

    _store(database, _NIFTY_ECONOMICS)

    begin = statements.index("BEGIN IMMEDIATE")
    select = next(i for i, s in enumerate(statements) if s.startswith("SELECT"))
    insert = next(
        i for i, s in enumerate(statements) if s.startswith("INSERT INTO option_contract_economics")
    )
    assert begin < select < insert
    assert statements[-1] == "COMMIT"


def test_the_adapters_depend_on_no_provider_clock_futures_or_calculation() -> None:
    for source in (module, schema):
        tree = _tree(source)
        modules = {
            node.module
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom) and node.module
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
                "httpx",
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
            assert "futures" not in module_path
            assert "upstox" not in module_path
    assert "northstar_application.ports" in {
        node.module
        for node in ast.walk(_tree(module))
        if isinstance(node, ast.ImportFrom) and node.module
    }


def test_the_surface_is_exported_and_the_helpers_are_not() -> None:
    for name in (
        "SQLiteOptionContractEconomicsStore",
        "SQLiteOptionContractEconomicsRepository",
        "OptionContractEconomicsStorageError",
        "OptionContractEconomicsConflictError",
        "initialize_option_contract_economics_schema",
        "OPTION_CONTRACT_ECONOMICS_SCHEMA",
    ):
        assert name in persistence.__all__
        assert hasattr(persistence, name)
    for private in ("_row", "_decode", "_economics", "_key", "_select"):
        assert not hasattr(persistence, private)
