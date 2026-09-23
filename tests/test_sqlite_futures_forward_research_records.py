"""Tests for the SQLite futures forward research record store and repository.

Every test runs against a real temporary SQLite file. Raw SQL is used only to
inspect the table or to plant corrupt rows the adapters must refuse; every
legitimate write goes through the store.
"""

from __future__ import annotations

import ast
import json
import sqlite3
from decimal import Decimal
from pathlib import Path

import pytest
from northstar_application.application_services import (
    FuturesAnalysisResult,
    FuturesForwardResearchRecord,
)
from northstar_application.ports import (
    FuturesForwardResearchRecordConflictError,
    FuturesForwardResearchRecordQuery,
    FuturesForwardResearchRecordRepository,
    FuturesForwardResearchRecordStore,
)
from northstar_core.derivatives import ExpirationDate, QuoteValue
from northstar_core.foundation.value_objects import (
    ExchangeCode,
    PointInTime,
    Quantity,
    Symbol,
    Timeframe,
)
from northstar_core.futures import FuturesContract, FuturesOHLCVBar, FuturesProductReference
from northstar_core.strategy import (
    FuturesAssetAnalysis,
    FuturesMarketObservationContext,
    Strategy,
    StrategyIdentity,
)

import northstar_infrastructure.persistence as persistence
import northstar_infrastructure.persistence.sqlite_futures_forward_research_records as module
from northstar_infrastructure.market_data import SQLiteFuturesHistoricalMarketDataStore
from northstar_infrastructure.persistence import (
    FuturesForwardResearchStorageError,
    SQLiteFuturesForwardResearchRecordRepository,
    SQLiteFuturesForwardResearchRecordStore,
)


def _contract(product: str = "ES", exchange: str = "CME", expiry: str = "2026-12-18"):
    return FuturesContract(
        FuturesProductReference(Symbol(product), ExchangeCode(exchange)), ExpirationDate(expiry)
    )


_ES_DEC = _contract()
_DAILY = Timeframe("1d")
_INSTANT = "2026-09-15T21:00:00Z"
_CLOSES = tuple(str(7600 + index) for index in range(20))
_VOLUMES = tuple(str(1000 + index) for index in range(20))


def _quote(value: str) -> QuoteValue:
    return QuoteValue(Decimal(value))


def _record(
    *,
    contract: FuturesContract = _ES_DEC,
    observed_at: str = _INSTANT,
    strategy: str = "futures-forward",
    signal: str = "strong bullish",
    latest: str = "7663.25",
    previous: str = "7650",
    high: str = "7700",
    low: str = "7500",
    closes: tuple[str, ...] = _CLOSES,
    volumes: tuple[str, ...] = _VOLUMES,
) -> FuturesForwardResearchRecord:
    instant = PointInTime(observed_at)
    context = FuturesMarketObservationContext(
        contract=contract,
        timeframe=_DAILY,
        observed_at=instant,
        latest_quote=_quote(latest),
        previous_close=_quote(previous),
        latest_volume=Quantity(Decimal("1250")),
        session_high=_quote(high),
        session_low=_quote(low),
        recent_closes=tuple(_quote(close) for close in closes),
        recent_volumes=tuple(Quantity(Decimal(volume)) for volume in volumes),
    )
    recommendation = Strategy(StrategyIdentity(strategy)).evaluate_futures(
        FuturesAssetAnalysis(contract, instant, (signal,))
    )
    return FuturesForwardResearchRecord(FuturesAnalysisResult(recommendation, context))


@pytest.fixture
def database(tmp_path: Path) -> Path:
    return tmp_path / "futures_forward.sqlite3"


def _store(database: Path) -> SQLiteFuturesForwardResearchRecordStore:
    return SQLiteFuturesForwardResearchRecordStore(database)


def _records(
    database: Path, contract: FuturesContract = _ES_DEC
) -> tuple[FuturesForwardResearchRecord, ...]:
    return SQLiteFuturesForwardResearchRecordRepository(database).get_records(
        FuturesForwardResearchRecordQuery(contract, _DAILY)
    )


def _rows(database: Path) -> list[tuple]:
    with sqlite3.connect(database) as connection:
        return connection.execute(
            "SELECT product_code, exchange_code, expiration_date, timeframe, "
            "decision_instant, strategy_identity, evidence FROM futures_forward_research_records"
        ).fetchall()


def _evidence(database: Path) -> dict:
    (row,) = _rows(database)
    return json.loads(row[6])


def _execute(database: Path, sql: str, parameters: tuple = ()) -> None:
    with sqlite3.connect(database) as connection:
        connection.execute(sql, parameters)


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


def test_the_table_has_six_not_null_key_columns_and_not_null_evidence(database: Path) -> None:
    _store(database).store((_record(),))

    with sqlite3.connect(database) as connection:
        columns = connection.execute(
            "PRAGMA table_info(futures_forward_research_records)"
        ).fetchall()

    assert [(name, kind, not_null) for _, name, kind, not_null, _, _ in columns] == [
        ("product_code", "TEXT", 1),
        ("exchange_code", "TEXT", 1),
        ("expiration_date", "TEXT", 1),
        ("timeframe", "TEXT", 1),
        ("decision_instant", "TEXT", 1),
        ("strategy_identity", "TEXT", 1),
        ("evidence", "TEXT", 1),
    ]
    primary_key = sorted((pk, name) for _, name, _, _, _, pk in columns if pk)
    assert [name for _, name in primary_key] == [
        "product_code",
        "exchange_code",
        "expiration_date",
        "timeframe",
        "decision_instant",
        "strategy_identity",
    ]


def test_the_futures_ohlcv_table_is_left_untouched(database: Path) -> None:
    SQLiteFuturesHistoricalMarketDataStore(database).store(
        (
            FuturesOHLCVBar(
                contract=_ES_DEC,
                point_in_time=PointInTime(_INSTANT),
                timeframe=_DAILY,
                open=_quote("7660"),
                high=_quote("7700"),
                low=_quote("7500"),
                close=_quote("7663.25"),
                volume=Quantity(Decimal("1000")),
            ),
        )
    )
    with sqlite3.connect(database) as connection:
        before = connection.execute("PRAGMA table_info(futures_ohlcv)").fetchall()
        bars_before = connection.execute("SELECT * FROM futures_ohlcv").fetchall()

    _store(database).store((_record(),))

    with sqlite3.connect(database) as connection:
        assert connection.execute("PRAGMA table_info(futures_ohlcv)").fetchall() == before
        assert connection.execute("SELECT * FROM futures_ohlcv").fetchall() == bars_before


def test_a_fresh_database_gets_only_the_forward_table(database: Path) -> None:
    _store(database).store((_record(),))

    with sqlite3.connect(database) as connection:
        tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master")}
    assert "futures_forward_research_records" in tables
    assert "futures_ohlcv" not in tables


# ---------------------------------------------------------------------------
# Key columns
# ---------------------------------------------------------------------------


def test_the_key_columns_are_exactly_the_records_natural_key(database: Path) -> None:
    record = _record()
    _store(database).store((record,))

    (row,) = _rows(database)
    contract, timeframe, instant, strategy = record.natural_key
    assert row[:6] == (
        contract.product.product_code.value,
        contract.product.exchange_code.value,
        contract.expiration_date.value,
        timeframe.value,
        instant.value,
        strategy.identity,
    )
    assert row[:6] == ("ES", "CME", "2026-12-18", "1d", _INSTANT, "futures-forward")


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"contract": _contract("MES")}, id="es-vs-mes"),
        pytest.param({"contract": _contract(exchange="CBOT")}, id="cme-vs-cbot"),
        pytest.param({"contract": _contract(expiry="2027-03-19")}, id="dec-vs-mar"),
        pytest.param({"strategy": "another-strategy"}, id="another-strategy"),
        pytest.param({"observed_at": "2026-09-16T21:00:00Z"}, id="another-instant"),
    ],
)
def test_each_identity_part_is_its_own_row(database: Path, overrides: dict) -> None:
    store = _store(database)

    assert store.store((_record(),)) == 1
    assert store.store((_record(**overrides),)) == 1
    assert len(_rows(database)) == 2


# ---------------------------------------------------------------------------
# Evidence JSON v1 and numeric round trip
# ---------------------------------------------------------------------------


def test_the_evidence_is_deterministic_versioned_json(database: Path) -> None:
    _store(database).store((_record(),))
    (row,) = _rows(database)

    payload = json.loads(row[6])
    assert row[6] == json.dumps(payload, sort_keys=True, separators=(",", ":"))
    assert payload["version"] == 1
    assert set(payload) == {
        "version",
        "contract",
        "timeframe",
        "context",
        "analysis",
        "recommendation",
    }
    assert payload["contract"] == {
        "product_code": "ES",
        "exchange_code": "CME",
        "expiration_date": "2026-12-18",
    }
    assert payload["timeframe"] == "1d"
    assert set(payload["context"]) == {
        "observed_at",
        "latest_quote",
        "previous_close",
        "latest_volume",
        "session_high",
        "session_low",
        "recent_closes",
        "recent_volumes",
    }
    assert payload["analysis"] == {
        "point_in_time": _INSTANT,
        "summarized_signals": ["strong bullish"],
    }
    assert payload["recommendation"] == {
        "action": "BUY",
        "strategy_identity": "futures-forward",
        "point_in_time": _INSTANT,
    }


def test_the_evidence_holds_no_float_currency_provider_or_explanation(database: Path) -> None:
    _store(database).store((_record(),))
    (row,) = _rows(database)

    def reject_float(text: str) -> float:
        raise AssertionError(f"float in evidence: {text}")

    json.loads(row[6], parse_float=reject_float, parse_int=lambda text: int(text))
    for absent in ("currency", "price", "explanation", "raw_symbol", "instrument_id", "multiplier"):
        assert absent not in row[6]


@pytest.mark.parametrize(
    ("latest", "text"),
    [
        pytest.param("7663.25", "7663.25", id="positive"),
        pytest.param("0", "0", id="zero"),
        pytest.param("-37.63", "-37.63", id="negative"),
        pytest.param("-0", "0", id="negative-zero-canonicalised-by-core"),
        pytest.param("65.123456789", "65.123456789", id="nanosecond-precision"),
        pytest.param("-0.000000001", "-1E-9", id="tiny-negative"),
    ],
)
def test_quotes_round_trip_exactly_as_text(database: Path, latest: str, text: str) -> None:
    record = _record(latest=latest, previous=latest, high="10000", low="-10000")

    _store(database).store((record,))

    assert _evidence(database)["context"]["latest_quote"] == text
    (reloaded,) = _records(database)
    assert reloaded == record
    assert reloaded.result.market_observation_context.latest_quote.value == Decimal(latest)


def test_the_text_representation_is_the_futures_ohlcv_one(database: Path) -> None:
    record = _record(latest="-0.000000001", previous="-0.000000001", high="1", low="-1")
    _store(database).store((record,))

    quote = record.result.market_observation_context.latest_quote.value
    assert _evidence(database)["context"]["latest_quote"] == str(quote)


def test_negative_and_zero_quotes_persist_in_a_complete_valid_record(database: Path) -> None:
    closes = ("-40", "-35.5", "-30", "-20", "0", "-10", "5", "0", "-1", "-2") * 2
    record = _record(
        latest="-37.63",
        previous="-40",
        high="5",
        low="-40",
        closes=closes,
        signal="strong bearish",
    )

    _store(database).store((record,))

    (reloaded,) = _records(database)
    assert reloaded == record
    context = reloaded.result.market_observation_context
    assert context.latest_quote == _quote("-37.63")
    assert _quote("0") in context.recent_closes
    assert all(quote.value <= 5 for quote in context.recent_closes)


# ---------------------------------------------------------------------------
# Decoder equality and restart
# ---------------------------------------------------------------------------


def test_a_decoded_record_is_exactly_the_stored_record(database: Path) -> None:
    record = _record()
    _store(database).store((record,))

    (decoded,) = _records(database)

    assert decoded == record
    assert decoded is not record
    assert decoded.natural_key == record.natural_key
    assert type(decoded) is FuturesForwardResearchRecord
    assert decoded.result.recommendation.action == record.result.recommendation.action


def test_records_survive_closing_and_reopening_the_database(database: Path) -> None:
    records = (_record(strategy="alpha"), _record(strategy="beta"))
    SQLiteFuturesForwardResearchRecordStore(database).store(records)

    reopened = SQLiteFuturesForwardResearchRecordRepository(database)

    assert reopened.get_records(FuturesForwardResearchRecordQuery(_ES_DEC, _DAILY)) == records
    assert SQLiteFuturesForwardResearchRecordStore(database).store(records) == 2
    assert len(_rows(database)) == 2


# ---------------------------------------------------------------------------
# Idempotency and conflict
# ---------------------------------------------------------------------------


def test_a_first_store_inserts_one_row(database: Path) -> None:
    assert _store(database).store((_record(),)) == 1
    assert len(_rows(database)) == 1


def test_an_identical_retry_is_idempotent(database: Path) -> None:
    record = _record()
    store = _store(database)
    store.store((record,))

    assert store.store((record,)) == 1
    assert _store(database).store((_record(),)) == 1  # separately rebuilt, fresh adapter
    assert len(_rows(database)) == 1


def test_an_offset_spelled_retry_is_the_same_decision(database: Path) -> None:
    _store(database).store((_record(),))

    assert _store(database).store((_record(observed_at="2026-09-15T16:00:00-05:00"),)) == 1
    assert len(_rows(database)) == 1
    assert _rows(database)[0][4] == _INSTANT


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"latest": "7663.50"}, id="latest-quote"),
        pytest.param({"previous": "7649.75"}, id="previous-close"),
        pytest.param({"closes": (*_CLOSES[:19], "7699")}, id="recent-close"),
        pytest.param({"volumes": (*_VOLUMES[:19], "1")}, id="recent-volume"),
        pytest.param({"signal": "neutral trend"}, id="signal-and-action"),
        pytest.param({"signal": "strong bearish"}, id="recommendation-action"),
    ],
)
def test_changed_evidence_under_the_same_key_conflicts(database: Path, overrides: dict) -> None:
    original = _record()
    _store(database).store((original,))
    rows_before = _rows(database)
    changed = _record(**overrides)
    assert changed.natural_key == original.natural_key

    with pytest.raises(FuturesForwardResearchRecordConflictError):
        _store(database).store((changed,))

    assert _rows(database) == rows_before
    assert _records(database) == (original,)


# ---------------------------------------------------------------------------
# Batches
# ---------------------------------------------------------------------------


def test_an_empty_batch_writes_nothing_and_returns_zero(database: Path) -> None:
    assert _store(database).store(()) == 0
    assert not database.exists() or _rows(database) == []


def test_distinct_records_and_strategies_at_one_instant_are_stored_together(
    database: Path,
) -> None:
    records = (
        _record(strategy="alpha"),
        _record(strategy="beta"),
        _record(observed_at="2026-09-16T21:00:00Z"),
        _record(contract=_contract("MES")),
    )

    assert _store(database).store(records) == 4
    assert len(_rows(database)) == 4


@pytest.mark.parametrize(
    "duplicate",
    [
        pytest.param(_record(), id="identical"),
        pytest.param(_record(latest="7663.50"), id="differing"),
        pytest.param(_record(observed_at="2026-09-15T16:00:00-05:00"), id="offset-spelled"),
    ],
)
def test_a_batch_repeating_one_key_is_rejected_before_any_write(
    database: Path, duplicate: FuturesForwardResearchRecord
) -> None:
    with pytest.raises(FuturesForwardResearchRecordConflictError, match="batch"):
        _store(database).store((_record(strategy="alpha"), _record(), duplicate))

    assert not database.exists() or _rows(database) == []


def test_one_conflict_leaves_the_whole_batch_uncommitted(database: Path) -> None:
    original = _record()
    _store(database).store((original,))

    with pytest.raises(FuturesForwardResearchRecordConflictError):
        _store(database).store(
            (
                _record(strategy="alpha"),
                _record(strategy="beta"),
                _record(latest="7663.50"),  # conflicts with the stored original
                _record(strategy="gamma"),
            )
        )

    assert _records(database) == (original,)


def test_idempotent_records_mixed_with_new_ones_count_every_record(database: Path) -> None:
    existing = _record(strategy="alpha")
    _store(database).store((existing,))

    assert _store(database).store((existing, _record(strategy="beta"), _record())) == 3
    assert len(_rows(database)) == 3


def test_a_foreign_batch_member_is_rejected_before_any_write(database: Path) -> None:
    with pytest.raises(TypeError, match="FuturesForwardResearchRecord"):
        _store(database).store((_record(), "record"))  # type: ignore[arg-type]

    assert not database.exists() or _rows(database) == []


# ---------------------------------------------------------------------------
# IntegrityError resolution
# ---------------------------------------------------------------------------


def _race(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make the first existence check miss, as if another writer won the race."""
    real = SQLiteFuturesForwardResearchRecordStore._existing
    calls = {"count": 0}

    def racing(connection, key):
        calls["count"] += 1
        return None if calls["count"] == 1 else real(connection, key)

    monkeypatch.setattr(SQLiteFuturesForwardResearchRecordStore, "_existing", staticmethod(racing))


def test_a_primary_key_race_with_an_equal_record_is_idempotent(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _store(database).store((_record(),))
    _race(monkeypatch)

    assert _store(database).store((_record(),)) == 1
    assert len(_rows(database)) == 1


def test_a_primary_key_race_with_a_different_record_is_a_conflict(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = _record()
    _store(database).store((original,))
    _race(monkeypatch)

    with pytest.raises(FuturesForwardResearchRecordConflictError):
        _store(database).store((_record(latest="7663.50"),))

    monkeypatch.undo()
    assert _records(database) == (original,)


# ---------------------------------------------------------------------------
# Repository filtering and ordering
# ---------------------------------------------------------------------------


def test_a_query_matching_nothing_returns_an_empty_tuple(database: Path) -> None:
    assert _records(database) == ()
    assert type(_records(database)) is tuple

    _store(database).store((_record(),))
    assert _records(database, _contract("MES")) == ()


@pytest.mark.parametrize(
    "other",
    [
        pytest.param(_contract("MES"), id="mes"),
        pytest.param(_contract(exchange="CBOT"), id="cbot"),
        pytest.param(_contract(expiry="2027-03-19"), id="march"),
    ],
)
def test_each_contract_is_queried_in_isolation(database: Path, other: FuturesContract) -> None:
    es = _record()
    foreign = _record(contract=other, latest="7601", previous="7600")
    _store(database).store((es, foreign))

    assert _records(database) == (es,)
    assert _records(database, other) == (foreign,)


def test_a_manual_row_at_another_timeframe_never_enters_a_daily_query(database: Path) -> None:
    record = _record()
    _store(database).store((record,))
    _execute(
        database,
        "INSERT INTO futures_forward_research_records VALUES (?, ?, ?, ?, ?, ?, ?)",
        ("ES", "CME", "2026-12-18", "1m", _INSTANT, "futures-forward", "not evidence"),
    )

    assert _records(database) == (record,)


def test_records_are_ordered_by_semantic_instant_then_strategy(database: Path) -> None:
    whole_beta = _record(observed_at="2026-09-15T21:00:00Z", strategy="beta")
    whole_alpha = _record(observed_at="2026-09-15T21:00:00Z", strategy="alpha")
    half = _record(observed_at="2026-09-15T21:00:00.5Z", strategy="alpha")
    next_day = _record(observed_at="2026-09-16T16:00:00-05:00", strategy="alpha")
    assert half.decision_instant.value < whole_alpha.decision_instant.value  # the text trap

    _store(database).store((next_day, half, whole_beta, whole_alpha))

    assert _records(database) == (whole_alpha, whole_beta, half, next_day)


def test_text_ordering_would_have_been_wrong(database: Path) -> None:
    """Guard: the ordering test is load-bearing only if SQL text order disagrees."""
    _store(database).store(
        (
            _record(observed_at="2026-09-15T21:00:00Z"),
            _record(observed_at="2026-09-15T21:00:00.5Z"),
        )
    )
    with sqlite3.connect(database) as connection:
        by_text = [
            row[0]
            for row in connection.execute(
                "SELECT decision_instant FROM futures_forward_research_records "
                "ORDER BY decision_instant"
            )
        ]

    assert by_text == ["2026-09-15T21:00:00.5Z", "2026-09-15T21:00:00Z"]
    assert [record.decision_instant.value for record in _records(database)] == list(
        reversed(by_text)
    )


def test_a_non_query_is_rejected(database: Path) -> None:
    with pytest.raises(TypeError, match="FuturesForwardResearchRecordQuery"):
        SQLiteFuturesForwardResearchRecordRepository(database).get_records(_ES_DEC)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Corrupt rows: key/evidence coherence and versions
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("column", "value"),
    [
        ("strategy_identity", "impostor"),
        ("decision_instant", "2026-09-16T21:00:00Z"),
    ],
)
def test_a_row_whose_key_disagrees_with_its_evidence_is_refused(
    database: Path, column: str, value: str
) -> None:
    _store(database).store((_record(),))
    _execute(database, f"UPDATE futures_forward_research_records SET {column} = ?", (value,))  # noqa: S608

    with pytest.raises(FuturesForwardResearchStorageError, match="does not match"):
        _records(database)


def _replace_evidence(database: Path, mutate) -> None:
    payload = _evidence(database)
    mutate(payload)
    _execute(
        database,
        "UPDATE futures_forward_research_records SET evidence = ?",
        (json.dumps(payload, sort_keys=True),),
    )


@pytest.mark.parametrize(
    "mutate",
    [
        pytest.param(lambda p: p["contract"].update(product_code="MES"), id="product"),
        pytest.param(lambda p: p["contract"].update(exchange_code="CBOT"), id="exchange"),
        pytest.param(lambda p: p["contract"].update(expiration_date="2027-03-19"), id="expiry"),
    ],
)
def test_evidence_naming_another_contract_than_its_key_is_refused(database: Path, mutate) -> None:
    _store(database).store((_record(),))
    _replace_evidence(database, mutate)

    with pytest.raises(FuturesForwardResearchStorageError, match="does not match"):
        _records(database)


def test_a_store_over_a_corrupt_existing_row_fails_rather_than_accepting(
    database: Path,
) -> None:
    _store(database).store((_record(),))
    _replace_evidence(database, lambda p: p["recommendation"].update(strategy_identity="x"))

    with pytest.raises(FuturesForwardResearchStorageError):
        _store(database).store((_record(),))


@pytest.mark.parametrize(
    "mutate",
    [
        pytest.param(lambda p: p.update(version=2), id="version-2"),
        pytest.param(lambda p: p.pop("version"), id="missing-version"),
        pytest.param(lambda p: p.update(version="1"), id="text-version"),
        pytest.param(lambda p: p.update(version=True), id="boolean-version"),
        pytest.param(lambda p: p["context"].update(latest_quote=7663.25), id="float-quote"),
        pytest.param(lambda p: p["context"].update(latest_quote="NaN"), id="nan-quote"),
        pytest.param(lambda p: p["context"].update(latest_quote="abc"), id="garbage-quote"),
        pytest.param(lambda p: p["context"].pop("session_low"), id="missing-field"),
        pytest.param(lambda p: p.update(timeframe="1m"), id="non-daily-evidence"),
    ],
)
def test_unsupported_or_malformed_evidence_is_refused(database: Path, mutate) -> None:
    _store(database).store((_record(),))
    _replace_evidence(database, mutate)

    with pytest.raises(FuturesForwardResearchStorageError):
        _records(database)


def test_evidence_that_is_not_json_is_refused(database: Path) -> None:
    _store(database).store((_record(),))
    _execute(database, "UPDATE futures_forward_research_records SET evidence = ?", ("{not json",))

    with pytest.raises(FuturesForwardResearchStorageError, match="invalid data"):
        _records(database)


# ---------------------------------------------------------------------------
# Structure, boundaries and exports
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


def test_the_adapter_issues_no_update_replace_or_delete() -> None:
    tree = ast.parse(Path(module.__file__).read_text(encoding="utf-8"))
    sql = " ".join(_non_docstring_strings(tree)).upper()

    assert "INSERT INTO FUTURES_FORWARD_RESEARCH_RECORDS" in sql
    for forbidden in ("UPDATE ", "DELETE ", "REPLACE", "ON CONFLICT", "UPSERT"):
        assert forbidden not in sql


def test_the_adapter_depends_on_no_provider_clock_or_execution_concept() -> None:
    tree = ast.parse(Path(module.__file__).read_text(encoding="utf-8"))
    modules = {
        node.module
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module is not None
    } | {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    names = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
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
            "random",
            "uuid",
            "pickle",
        }
        assert "paper_trading" not in module_path
        assert "execution" not in module_path
    for forbidden in (
        "FuturesHistoricalMarketDataSource",
        "AcquireFuturesDailyHistoryUseCase",
        "FuturesTradingSessionResolver",
        "Price",
        "Money",
        "Currency",
        "RecommendationExplanation",
    ):
        assert forbidden not in names


def test_the_adapters_implement_the_application_ports() -> None:
    assert issubclass(SQLiteFuturesForwardResearchRecordStore, FuturesForwardResearchRecordStore)
    assert issubclass(
        SQLiteFuturesForwardResearchRecordRepository, FuturesForwardResearchRecordRepository
    )


def test_the_adapters_are_exported_and_the_helpers_are_not() -> None:
    for name in (
        "SQLiteFuturesForwardResearchRecordStore",
        "SQLiteFuturesForwardResearchRecordRepository",
        "FuturesForwardResearchStorageError",
    ):
        assert name in persistence.__all__
    for private in ("_record", "_evidence_payload", "_EVIDENCE_VERSION", "_decode", "_key_columns"):
        assert not hasattr(persistence, private)
