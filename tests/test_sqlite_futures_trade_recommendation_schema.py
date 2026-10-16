"""Tests for the futures trade recommendation definition schema (F-5B-1B)."""

from __future__ import annotations

import sqlite3
from contextlib import closing
from pathlib import Path

import pytest

from northstar_infrastructure.persistence import (
    FUTURES_RECOMMENDATION_CONFIGURATION_SCHEMA,
    FUTURES_TRADE_RECOMMENDATION_SCHEMA,
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


def test_exactly_the_two_definition_tables_exist(database: Path) -> None:
    with closing(sqlite3.connect(database)) as connection:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
            )
        }

    assert tables == {"futures_recommendation_configurations", "futures_trade_recommendations"}


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
