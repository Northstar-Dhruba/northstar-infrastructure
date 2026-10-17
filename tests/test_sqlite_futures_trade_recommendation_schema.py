"""Tests for the futures trade recommendation schema (F-5B-1B definitions, F-5B-2A steps)."""

from __future__ import annotations

import sqlite3
from contextlib import closing
from pathlib import Path

import pytest

from northstar_infrastructure.persistence import (
    FUTURES_RECOMMENDATION_CONFIGURATION_SCHEMA,
    FUTURES_TRADE_RECOMMENDATION_SCHEMA,
    FUTURES_TRADE_RECOMMENDATION_STEP_SCHEMA,
    initialize_futures_trade_recommendation_schema,
)


@pytest.fixture
def database(tmp_path: Path) -> Path:
    path = tmp_path / "recommendations.sqlite3"
    with closing(sqlite3.connect(path)) as connection:
        initialize_futures_trade_recommendation_schema(connection)
    return path


def _columns(connection: sqlite3.Connection, table: str) -> list[tuple[str, str, int, int]]:
    return [
        (row[1], row[2], row[3], row[5])
        for row in connection.execute(f"PRAGMA table_info({table})")  # noqa: S608
    ]


def _unique_index_columns(connection: sqlite3.Connection, table: str) -> set[tuple[str, ...]]:
    found = set()
    for index in connection.execute(f"PRAGMA index_list({table})"):  # noqa: S608
        if index[2]:
            found.add(
                tuple(row[2] for row in connection.execute(f"PRAGMA index_info({index[1]})"))  # noqa: S608
            )
    return found


def test_exactly_the_definition_and_step_tables_exist(database: Path) -> None:
    with closing(sqlite3.connect(database)) as connection:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
            )
        }

    assert tables == {
        "futures_recommendation_configurations",
        "futures_trade_recommendations",
        "futures_trade_recommendation_steps",
    }
    assert not any("audit" in table for table in tables)


def test_the_configuration_table(database: Path) -> None:
    with closing(sqlite3.connect(database)) as connection:
        columns = _columns(connection, "futures_recommendation_configurations")
        unique = _unique_index_columns(connection, "futures_recommendation_configurations")

    assert columns == [
        ("profile_version", "TEXT", 1, 1),
        ("policy_version", "TEXT", 1, 0),
        ("validity_sessions", "TEXT", 1, 0),
        ("policy_parameters", "TEXT", 1, 0),
    ]
    assert ("profile_version", "policy_version") in unique


def test_the_definition_table(database: Path) -> None:
    with closing(sqlite3.connect(database)) as connection:
        columns = _columns(connection, "futures_trade_recommendations")
        unique = _unique_index_columns(connection, "futures_trade_recommendations")
        foreign = connection.execute(
            "PRAGMA foreign_key_list(futures_trade_recommendations)"
        ).fetchall()
        indexes = {
            row[1] for row in connection.execute("PRAGMA index_list(futures_trade_recommendations)")
        }

    assert [column[0] for column in columns] == [
        "recommendation_identity",
        "product_code",
        "exchange_code",
        "expiration_date",
        "timeframe",
        "decision_instant",
        "strategy_identity",
        "policy_version",
        "profile_version",
        "issued_at",
        "definition",
    ]
    assert all(column[1] == "TEXT" and column[2] == 1 for column in columns)
    assert columns[0][3] == 1  # recommendation_identity is the primary key
    assert (
        "product_code",
        "exchange_code",
        "expiration_date",
        "timeframe",
        "decision_instant",
        "strategy_identity",
        "policy_version",
        "profile_version",
    ) in unique
    assert {(row[2], row[3], row[4]) for row in foreign} == {
        ("futures_recommendation_configurations", "profile_version", "profile_version"),
        ("futures_recommendation_configurations", "policy_version", "policy_version"),
    }
    assert "futures_trade_recommendations_product" in indexes


def test_there_is_no_product_uniqueness_that_would_block_a_released_product(database: Path) -> None:
    with closing(sqlite3.connect(database)) as connection:
        unique = _unique_index_columns(connection, "futures_trade_recommendations")

    assert ("product_code", "exchange_code") not in unique


def test_initialization_is_idempotent(database: Path) -> None:
    with closing(sqlite3.connect(database)) as connection:
        before = connection.execute("SELECT sql FROM sqlite_master ORDER BY name").fetchall()
        initialize_futures_trade_recommendation_schema(connection)
        initialize_futures_trade_recommendation_schema(connection)
        after = connection.execute("SELECT sql FROM sqlite_master ORDER BY name").fetchall()

    assert before == after


def test_the_database_refuses_a_definition_without_its_registered_configuration(
    database: Path,
) -> None:
    with closing(sqlite3.connect(database)) as connection:
        connection.execute("PRAGMA foreign_keys = ON")
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO futures_trade_recommendations VALUES "
                "('id', 'NIFTY', 'NSE', '2026-10-27', '1d', 't', 's', 'p', "
                "'unregistered', 't', '{}')"
            )


def test_the_database_refuses_a_policy_version_its_profile_was_not_registered_with(
    database: Path,
) -> None:
    with closing(sqlite3.connect(database)) as connection:
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute(
            "INSERT INTO futures_recommendation_configurations VALUES ('v1', 'p1', '5', '{}')"
        )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO futures_trade_recommendations VALUES "
                "('id', 'NIFTY', 'NSE', '2026-10-27', '1d', 't', 's', 'p2', 'v1', 't', '{}')"
            )


def test_the_database_refuses_a_second_identity_for_one_source_and_configuration(
    database: Path,
) -> None:
    row = "'NIFTY', 'NSE', '2026-10-27', '1d', 't', 's', 'p1', 'v1', 't', '{}'"
    with closing(sqlite3.connect(database)) as connection:
        connection.execute(
            "INSERT INTO futures_recommendation_configurations VALUES ('v1', 'p1', '5', '{}')"
        )
        connection.execute(f"INSERT INTO futures_trade_recommendations VALUES ('a', {row})")  # noqa: S608
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(f"INSERT INTO futures_trade_recommendations VALUES ('b', {row})")  # noqa: S608


def test_the_schema_text_names_no_lifecycle_or_audit_concept() -> None:
    text = (
        FUTURES_RECOMMENDATION_CONFIGURATION_SCHEMA + FUTURES_TRADE_RECOMMENDATION_SCHEMA
    ).lower()

    for fragment in ("step", "event", "cursor", "audit", "terminal", "status"):
        assert fragment not in text


# ---------------------------------------------------------------------------
# Lifecycle steps (F-5B-2A)
# ---------------------------------------------------------------------------

_DEFINITION = "'NIFTY', 'NSE', '2026-10-27', '1d', 't', 's', 'p1', 'v1', 't', '{}'"
_CANDLE = "'NIFTY', 'NSE', '2026-10-27', '1d', '2026-10-08T10:00:00Z', '2026-10-08', 'fdc1'"
_NO_CANDLE = "NULL, NULL, NULL, NULL, NULL, NULL, NULL"


def _with_definition(database: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(database)
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute(
        "INSERT INTO futures_recommendation_configurations VALUES ('v1', 'p1', '5', '{}')"
    )
    connection.execute(f"INSERT INTO futures_trade_recommendations VALUES ('a', {_DEFINITION})")  # noqa: S608
    return connection


def _insert_step(
    connection: sqlite3.Connection,
    sequence: object = 1,
    kind: str = "OBSERVATION",
    candle: str = _CANDLE,
    terminal: object = 0,
    identity: str = "a",
) -> None:
    connection.execute(
        "INSERT INTO futures_trade_recommendation_steps VALUES "  # noqa: S608
        f"(?, ?, ?, {candle}, '{{}}', 'WAITING_FOR_ENTRY', ?)",
        (identity, sequence, kind, terminal),
    )


def test_the_step_table(database: Path) -> None:
    with closing(sqlite3.connect(database)) as connection:
        columns = _columns(connection, "futures_trade_recommendation_steps")
        foreign = connection.execute(
            "PRAGMA foreign_key_list(futures_trade_recommendation_steps)"
        ).fetchall()

    assert columns == [
        ("recommendation_identity", "TEXT", 1, 1),
        ("sequence", "INTEGER", 1, 2),
        ("step_kind", "TEXT", 1, 0),
        ("product_code", "TEXT", 0, 0),
        ("exchange_code", "TEXT", 0, 0),
        ("expiration_date", "TEXT", 0, 0),
        ("timeframe", "TEXT", 0, 0),
        ("point_in_time", "TEXT", 0, 0),
        ("trading_date", "TEXT", 0, 0),
        ("candle_fingerprint", "TEXT", 0, 0),
        ("evaluation", "TEXT", 1, 0),
        ("resulting_status", "TEXT", 1, 0),
        ("is_terminal", "INTEGER", 1, 0),
    ]
    assert {(row[2], row[3], row[4]) for row in foreign} == {
        ("futures_trade_recommendations", "recommendation_identity", "recommendation_identity"),
    }


def test_the_step_indexes(database: Path) -> None:
    with closing(sqlite3.connect(database)) as connection:
        indexes = {
            row[1]: (row[2], row[4])
            for row in connection.execute("PRAGMA index_list(futures_trade_recommendation_steps)")
        }
        columns = {
            name: tuple(row[2] for row in connection.execute(f"PRAGMA index_info({name})"))  # noqa: S608
            for name in indexes
        }
        unique = _unique_index_columns(connection, "futures_trade_recommendation_steps")

    assert indexes["futures_trade_recommendation_steps_observation"] == (1, 1)
    assert columns["futures_trade_recommendation_steps_observation"] == (
        "recommendation_identity",
        "point_in_time",
    )
    assert indexes["futures_trade_recommendation_steps_terminal"] == (1, 1)
    assert columns["futures_trade_recommendation_steps_terminal"] == ("recommendation_identity",)
    assert columns["futures_trade_recommendation_steps_candle"] == (
        "product_code",
        "exchange_code",
        "expiration_date",
        "timeframe",
        "point_in_time",
    )
    assert ("recommendation_identity", "sequence") in unique


def test_the_step_table_stores_no_prices_and_the_definitions_no_lifecycle(database: Path) -> None:
    with closing(sqlite3.connect(database)) as connection:
        steps = {column[0] for column in _columns(connection, "futures_trade_recommendation_steps")}
        definitions = {
            column[0] for column in _columns(connection, "futures_trade_recommendations")
        }

    for fragment in ("open", "high", "low", "close", "volume", "payload", "provider"):
        assert not any(fragment in column for column in steps), fragment
    for fragment in ("status", "terminal", "cursor", "observation", "step"):
        assert not any(fragment in column for column in definitions), fragment


def test_the_database_refuses_a_step_of_an_unrecorded_recommendation(database: Path) -> None:
    with closing(_with_definition(database)) as connection, pytest.raises(sqlite3.IntegrityError):
        _insert_step(connection, identity="unrecorded")


def test_the_database_refuses_a_duplicated_sequence(database: Path) -> None:
    with closing(_with_definition(database)) as connection:
        _insert_step(connection, 1)
        with pytest.raises(sqlite3.IntegrityError):
            _insert_step(connection, 1, "TIME_EXPIRY", _NO_CANDLE)


def test_the_database_refuses_a_second_evaluation_of_one_candle(database: Path) -> None:
    with closing(_with_definition(database)) as connection:
        _insert_step(connection, 1)
        with pytest.raises(sqlite3.IntegrityError):
            _insert_step(connection, 2)


def test_the_database_refuses_a_second_terminal_step(database: Path) -> None:
    with closing(_with_definition(database)) as connection:
        _insert_step(connection, 1, terminal=1)
        with pytest.raises(sqlite3.IntegrityError):
            _insert_step(connection, 2, "TIME_EXPIRY", _NO_CANDLE, terminal=1)


@pytest.mark.parametrize(
    "sequence, kind, candle, terminal",
    [
        (0, "OBSERVATION", _CANDLE, 0),
        ("one", "OBSERVATION", _CANDLE, 0),
        (1, "OTHER", _CANDLE, 0),
        (1, "OBSERVATION", _NO_CANDLE, 0),
        (1, "OBSERVATION", _CANDLE.replace("'fdc1'", "NULL"), 0),
        (1, "TIME_EXPIRY", _CANDLE, 0),
        (1, "OBSERVATION", _CANDLE, 2),
        (1, "OBSERVATION", _CANDLE, "yes"),
    ],
    ids=[
        "sequence-zero",
        "sequence-text",
        "unknown-kind",
        "observation-without-candle",
        "observation-without-fingerprint",
        "time-expiry-with-candle",
        "terminal-two",
        "terminal-text",
    ],
)
def test_the_database_refuses_a_malformed_step(
    database: Path, sequence: object, kind: str, candle: str, terminal: object
) -> None:
    with closing(_with_definition(database)) as connection, pytest.raises(sqlite3.IntegrityError):
        _insert_step(connection, sequence, kind, candle, terminal)


def test_the_step_schema_has_no_audit_or_update_concept() -> None:
    text = FUTURES_TRADE_RECOMMENDATION_STEP_SCHEMA.lower()

    for fragment in ("audit", "updated_at", "trigger", "open_value", "close_value"):
        assert fragment not in text
