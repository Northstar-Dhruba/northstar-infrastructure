"""Tests for preserving provider-reported option open interest as Infrastructure evidence."""

from __future__ import annotations

import ast
import sqlite3
from dataclasses import fields
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pytest
from northstar_core.derivatives import ExpirationDate
from northstar_core.foundation.value_objects import ExchangeCode, Symbol
from northstar_core.options import (
    OptionContract,
    OptionProductReference,
    OptionRight,
    OptionStrike,
)

import northstar_infrastructure.market_data.sqlite_option_provider_open_interest as module
from northstar_infrastructure.market_data import (
    OptionOpenInterestConflictError,
    OptionOpenInterestStorageError,
    ProviderOptionOpenInterest,
    SQLiteOptionProviderOpenInterestRepository,
    SQLiteOptionProviderOpenInterestStore,
    initialize_futures_market_data_schema,
    initialize_option_provider_open_interest_schema,
)

_NIFTY = OptionProductReference(Symbol("NIFTY"), ExchangeCode("NSE"))
_DAY = date(2026, 10, 8)


def _contract(
    expiration: str = "2026-10-27", strike: str = "25000", right: OptionRight = OptionRight.CALL
) -> OptionContract:
    return OptionContract(_NIFTY, ExpirationDate(expiration), OptionStrike(Decimal(strike)), right)


def _oi(
    value: str = "1234500",
    contract: OptionContract | None = None,
    day: date = _DAY,
    provider: str = "upstox",
) -> ProviderOptionOpenInterest:
    return ProviderOptionOpenInterest(
        provider, _contract() if contract is None else contract, day, Decimal(value)
    )


_ROW = ("upstox", "NIFTY", "NSE", "2026-10-27", "25000", "CALL", "2026-10-08", "1234500")


@pytest.fixture
def database(tmp_path: Path) -> Path:
    return tmp_path / "northstar.sqlite3"


def _store(database: Path, *records: ProviderOptionOpenInterest) -> int:
    return SQLiteOptionProviderOpenInterestStore(database).store(records)


def _get(database: Path, contract=None, day: date = _DAY, provider: str = "upstox"):
    return SQLiteOptionProviderOpenInterestRepository(database).get(
        provider, _contract() if contract is None else contract, day
    )


def _query(database: Path, sql: str) -> list[tuple]:
    with sqlite3.connect(database) as connection:
        return connection.execute(sql).fetchall()


def _rows(database: Path) -> list[tuple]:
    return _query(
        database, "SELECT * FROM option_daily_provider_open_interest ORDER BY 1, 4, 5, 6, 7"
    )


def _schema(database: Path) -> list[tuple]:
    return sorted(_query(database, "SELECT type, name, sql FROM sqlite_master"))


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


def test_a_store_creates_exactly_the_open_interest_table(database: Path) -> None:
    _store(database, _oi())

    assert {
        r[0] for r in _query(database, "SELECT name FROM sqlite_master WHERE type='table'")
    } == {"option_daily_provider_open_interest"}


def test_the_table_shape_and_key_are_exact(database: Path) -> None:
    with sqlite3.connect(database) as connection:
        initialize_option_provider_open_interest_schema(connection)
    columns = [
        (name, kind, notnull, pk)
        for _, name, kind, notnull, _, pk in _query(
            database, "PRAGMA table_info(option_daily_provider_open_interest)"
        )
    ]

    assert columns == [
        ("provider", "TEXT", 1, 1),
        ("product_code", "TEXT", 1, 2),
        ("exchange_code", "TEXT", 1, 3),
        ("expiration_date", "TEXT", 1, 4),
        ("strike", "TEXT", 1, 5),
        ("option_right", "TEXT", 1, 6),
        ("trading_date", "TEXT", 1, 7),
        ("open_interest_raw", "TEXT", 1, 0),
    ]


def test_futures_tables_are_left_untouched(database: Path) -> None:
    with sqlite3.connect(database) as connection:
        initialize_futures_market_data_schema(connection)
    before = set(_query(database, "SELECT type, name, sql FROM sqlite_master"))

    _store(database, _oi())

    after = set(_query(database, "SELECT type, name, sql FROM sqlite_master"))
    assert before <= after
    assert {name for _, name, _ in after - before} == {
        "option_daily_provider_open_interest",
        "sqlite_autoindex_option_daily_provider_open_interest_1",
    }


# ---------------------------------------------------------------------------
# Raw preservation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("raw", ["1234500", "1234500.0", "0", "975.25", "1E+6"])
def test_the_parsed_provider_number_is_preserved_exactly(database: Path, raw: str) -> None:
    _store(database, _oi(raw))

    assert _rows(database)[0][7] == raw
    stored = _get(database)
    assert str(stored.open_interest_raw) == raw
    assert stored.open_interest_raw == Decimal(raw)


def test_no_unit_conversion_is_applied(database: Path) -> None:
    """A lot-sized number is not divided into contracts or otherwise reinterpreted."""
    _store(database, _oi("130"))

    assert _get(database).open_interest_raw == Decimal("130")


def test_a_record_round_trips(database: Path) -> None:
    _store(database, _oi())

    assert _get(database) == _oi()
    assert _rows(database) == [_ROW]


# ---------------------------------------------------------------------------
# Identity
# ---------------------------------------------------------------------------


def test_contracts_dates_and_providers_are_separate_records(database: Path) -> None:
    records = (
        _oi("1"),
        _oi("2", _contract(right=OptionRight.PUT)),
        _oi("3", _contract(strike="25050")),
        _oi("4", _contract(expiration="2026-11-24")),
        _oi("5", day=date(2026, 10, 9)),
        _oi("6", provider="other_provider"),
    )

    assert _store(database, *records) == 6

    for record in records:
        assert _get(database, record.contract, record.trading_date, record.provider) == record


def test_a_missing_record_is_none(database: Path) -> None:
    _store(database, _oi())

    assert _get(database, _contract(strike="25050")) is None
    assert _get(database, day=date(2026, 10, 9)) is None
    assert _get(database, provider="other_provider") is None


# ---------------------------------------------------------------------------
# Immutability
# ---------------------------------------------------------------------------


def test_an_identical_repeat_is_idempotent(database: Path) -> None:
    _store(database, _oi())

    assert _store(database, _oi()) == 1
    assert _rows(database) == [_ROW]


def test_a_numerically_equal_repeat_keeps_the_first_text(database: Path) -> None:
    _store(database, _oi("1234500"))

    assert _store(database, _oi("1234500.00")) == 1
    assert _rows(database)[0][7] == "1234500"


def test_a_changed_value_conflicts_and_changes_nothing(database: Path) -> None:
    _store(database, _oi())

    with pytest.raises(OptionOpenInterestConflictError, match="already reported"):
        _store(database, _oi("1234565"))
    assert _rows(database) == [_ROW]


def test_a_conflict_rolls_back_the_whole_batch(database: Path) -> None:
    _store(database, _oi())

    with pytest.raises(OptionOpenInterestConflictError):
        _store(database, _oi("9", day=date(2026, 10, 9)), _oi("1234565"))
    assert _rows(database) == [_ROW]


def test_a_batch_repeating_one_key_is_rejected_before_any_write(database: Path) -> None:
    with pytest.raises(OptionOpenInterestConflictError, match="two records"):
        _store(database, _oi(), _oi())
    assert not database.exists()


def test_records_survive_reopen(tmp_path: Path) -> None:
    path = tmp_path / "reopen.sqlite3"
    SQLiteOptionProviderOpenInterestStore(path).store((_oi(), _oi("7", day=date(2026, 10, 9))))

    repository = SQLiteOptionProviderOpenInterestRepository(path)
    assert repository.get("upstox", _contract(), _DAY) == _oi()
    assert repository.get("upstox", _contract(), date(2026, 10, 9)) == _oi(
        "7", day=date(2026, 10, 9)
    )


# ---------------------------------------------------------------------------
# Validation and corruption
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("arguments", "error"),
    [
        (("Upstox", _contract(), _DAY, Decimal("1")), TypeError),
        (("upstox", "NIFTY", _DAY, Decimal("1")), TypeError),
        (("upstox", _contract(), datetime(2026, 10, 8), Decimal("1")), TypeError),
        (("upstox", _contract(), _DAY, 1), TypeError),
        (("upstox", _contract(), _DAY, 1.0), TypeError),
        (("upstox", _contract(), _DAY, Decimal("NaN")), ValueError),
    ],
)
def test_invalid_records_are_refused(arguments: tuple, error: type[Exception]) -> None:
    with pytest.raises(error):
        ProviderOptionOpenInterest(*arguments)


@pytest.mark.parametrize(
    ("column", "value"),
    [
        ("open_interest_raw", "abc"),
        ("open_interest_raw", "NaN"),
        ("open_interest_raw", " 1234500"),
        ("trading_date", "20261008"),
        ("strike", "25000.0"),
        ("option_right", "CE"),
        ("provider", "Upstox"),
    ],
)
def test_a_corrupt_row_fails_loudly(database: Path, column: str, value: str) -> None:
    _store(database, _oi())
    with sqlite3.connect(database) as connection:
        connection.execute(
            f"UPDATE option_daily_provider_open_interest SET {column} = ?",  # noqa: S608
            (value,),
        )

    with pytest.raises(OptionOpenInterestStorageError):
        module._decode(
            tuple(_query(database, "SELECT * FROM option_daily_provider_open_interest")[0])
        )


def test_a_store_over_a_corrupt_row_fails_and_repairs_nothing(database: Path) -> None:
    _store(database, _oi())
    with sqlite3.connect(database) as connection:
        connection.execute("UPDATE option_daily_provider_open_interest SET open_interest_raw = 'x'")

    with pytest.raises(OptionOpenInterestStorageError):
        _store(database, _oi())
    assert _rows(database)[0][7] == "x"


# ---------------------------------------------------------------------------
# Read-only repository and boundaries
# ---------------------------------------------------------------------------


def test_a_missing_database_is_none_and_creates_no_file(database: Path) -> None:
    assert _get(database) is None
    assert not database.exists()


def test_a_database_without_the_table_is_none_and_unchanged(database: Path) -> None:
    with sqlite3.connect(database) as connection:
        initialize_futures_market_data_schema(connection)
    before = _schema(database)

    assert _get(database) is None
    assert _schema(database) == before


def test_a_lookup_never_initializes_the_schema(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _store(database, _oi())

    def forbidden(connection) -> None:
        raise AssertionError("a lookup must never initialize the schema")

    monkeypatch.setattr(module, "initialize_option_provider_open_interest_schema", forbidden)

    assert _get(database) == _oi()


def test_the_record_carries_only_raw_evidence() -> None:
    assert [field.name for field in fields(ProviderOptionOpenInterest)] == [
        "provider",
        "contract",
        "trading_date",
        "open_interest_raw",
    ]


def test_open_interest_is_never_exposed_through_core_or_application() -> None:
    import northstar_application.application_services as services
    import northstar_application.ports as ports
    import northstar_core.options as options

    for package in (options, ports, services):
        assert not [name for name in package.__all__ if "OpenInterest" in name]


def test_the_module_converts_no_units_and_names_no_futures_table() -> None:
    tree = ast.parse(Path(module.__file__).read_text(encoding="utf-8"))
    names = {a.name for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) for a in n.names}

    assert "UPSTOX_PROVIDER" not in names
    assert not [n for n in names if n.startswith("Futures")]
    for statement in (module._INSERT, module._SELECT_KEY, module._TABLE_EXISTS):
        assert "FUTURES_" not in statement.upper()
        for forbidden in ("UPDATE ", "DELETE ", "REPLACE", "ON CONFLICT", "DROP "):
            assert forbidden not in statement.upper()
    for node in ast.walk(tree):
        assert not (isinstance(node, ast.Name) and node.id in {"lot_size", "lot"})
