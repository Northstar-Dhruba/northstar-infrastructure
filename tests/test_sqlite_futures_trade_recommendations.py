"""Tests for the SQLite futures trade recommendation definition store (F-5B-1B).

Synthetic data only: the contracts, instants, levels, thesis and every policy
and profile value are non-production fixtures. Nothing here reads a provider.
"""

from __future__ import annotations

import ast
import dataclasses
import sqlite3
import threading
import time
from contextlib import closing
from decimal import Decimal
from pathlib import Path

import pytest
from northstar_application.application_services import (
    FuturesTradeRecommendationGenerationProfile,
    FuturesTradeRecommendationIdentityService,
)
from northstar_application.ports import (
    FuturesTradeRecommendationConfigurationConflictError,
    FuturesTradeRecommendationConflictError,
    FuturesTradeRecommendationExclusivityError,
    FuturesTradeRecommendationQuery,
    FuturesTradeRecommendationRepository,
    FuturesTradeRecommendationStore,
    InvalidFuturesTradeRecommendationRecordingError,
)
from northstar_core.derivatives import ExpirationDate, QuoteValue
from northstar_core.foundation.value_objects import ExchangeCode, PointInTime, Symbol, Timeframe
from northstar_core.futures import FuturesContract, FuturesProductReference
from northstar_core.recommendations.futures import (
    EntryZone,
    FuturesRecommendationGenerationPolicy,
    FuturesResearchDecisionReference,
    FuturesTradeRecommendation,
    FuturesTradeRecommendationIdentity,
    PriceTarget,
    RecommendationPolicyVersion,
    RecommendationStatus,
    RecommendationValidity,
    StopLoss,
    TradeDirection,
)
from northstar_core.strategy import ExplanationReason, StrategyIdentity

import northstar_infrastructure.persistence.sqlite_futures_trade_recommendations as module
from northstar_infrastructure.persistence import (
    FuturesTradeRecommendationStorageError,
    SQLiteFuturesTradeRecommendationRepository,
    SQLiteFuturesTradeRecommendationStore,
)
from tests.futures_recommendation_audit_fixtures import recording_audit

LONG, SHORT = TradeDirection.LONG, TradeDirection.SHORT

_NIFTY = FuturesProductReference(Symbol("NIFTY"), ExchangeCode("NSE"))
_BANKNIFTY = FuturesProductReference(Symbol("BANKNIFTY"), ExchangeCode("NSE"))
_NIFTY_SGX = FuturesProductReference(Symbol("NIFTY"), ExchangeCode("SGX"))
_OCT = FuturesContract(_NIFTY, ExpirationDate("2026-10-27"))
_NOV = FuturesContract(_NIFTY, ExpirationDate("2026-11-24"))
_BANK_OCT = FuturesContract(_BANKNIFTY, ExpirationDate("2026-10-27"))
_SGX_OCT = FuturesContract(_NIFTY_SGX, ExpirationDate("2026-10-27"))
_STRATEGY = StrategyIdentity("synthetic-directional-v1")


def _policy(version: str = "f4-synthetic-fixture-v1", **overrides: object):
    values: dict[str, object] = {
        "version": RecommendationPolicyVersion(version),
        "approved_strategies": (_STRATEGY,),
        "lookback_changes": 19,
        "entry_multiplier": Decimal("0.25"),
        "stop_multiplier": Decimal("1.5"),
        "target_multiplier": Decimal("3"),
        "price_increment": Decimal("0.05"),
        "min_reward_to_risk": Decimal("1.5"),
        "short_enabled": True,
    }
    values.update(overrides)
    return FuturesRecommendationGenerationPolicy(**values)  # type: ignore[arg-type]


_POLICY = _policy()
_PROFILE = FuturesTradeRecommendationGenerationProfile(
    "f5a-synthetic-profile-v1", 5, _POLICY.version
)
_PROFILE_V2 = FuturesTradeRecommendationGenerationProfile(
    "f5a-synthetic-profile-v2", 5, _POLICY.version
)


def _q(value: str) -> QuoteValue:
    return QuoteValue(Decimal(value))


def _recommendation(
    contract: FuturesContract = _OCT,
    direction: TradeDirection = LONG,
    *,
    issued_at: str = "2026-10-07T12:00:00Z",
    decided: str = "2026-10-07T10:00:00Z",
    profile: FuturesTradeRecommendationGenerationProfile = _PROFILE,
    policy: FuturesRecommendationGenerationPolicy = _POLICY,
) -> FuturesTradeRecommendation:
    source = FuturesResearchDecisionReference(
        contract, Timeframe("1d"), PointInTime(decided), _STRATEGY
    )
    long = direction is LONG
    return FuturesTradeRecommendation(
        identity=FuturesTradeRecommendationIdentityService().identity(source, profile),
        contract=contract,
        direction=direction,
        entry=EntryZone(_q("24378") if long else _q("23995"), _q("24388") if long else _q("24005")),
        stop=StopLoss(_q("24347.75") if long else _q("24035.25")),
        target=PriceTarget(_q("24448.45") if long else _q("23934.55")),
        validity=RecommendationValidity(
            PointInTime("2026-10-08T10:00:00Z"),
            PointInTime("2026-10-14T10:00:00Z"),
            PointInTime("2026-10-27T10:00:00Z"),
        ),
        source=source,
        policy_version=policy.version,
        issued_at=PointInTime(issued_at),
        thesis=(
            ExplanationReason("Research BUY", ("strong bullish",)),
            ExplanationReason(
                "LONG setup under policy f4-synthetic-fixture-v1",
                ("reference close 24383", "reward 60.45 against risk 40.25"),
            ),
        ),
    )


@pytest.fixture
def database(tmp_path: Path) -> Path:
    return tmp_path / "recommendations.sqlite3"


def _store(database: Path) -> SQLiteFuturesTradeRecommendationStore:
    return SQLiteFuturesTradeRecommendationStore(database)


def _repository(database: Path) -> SQLiteFuturesTradeRecommendationRepository:
    return SQLiteFuturesTradeRecommendationRepository(database)


def _record(database: Path, *recommendations, policy=_POLICY, profile=_PROFILE) -> int:
    return _store(database).record(
        tuple(recommendations),
        policy=policy,
        profile=profile,
        audit=recording_audit(*recommendations, profile=profile),
    )


def _rows(database: Path, table: str) -> list[tuple]:
    with closing(sqlite3.connect(database)) as connection:
        return connection.execute(f"SELECT * FROM {table} ORDER BY 1").fetchall()  # noqa: S608


def _execute(database: Path, sql: str, parameters: tuple = ()) -> None:
    with closing(sqlite3.connect(database)) as connection:
        connection.execute(sql, parameters)
        connection.commit()


# ---------------------------------------------------------------------------
# Configuration registry
# ---------------------------------------------------------------------------


def test_the_first_recording_registers_its_configuration(database: Path) -> None:
    assert _record(database, _recommendation()) == 1

    ((profile, policy, sessions, parameters),) = _rows(
        database, "futures_recommendation_configurations"
    )
    assert (profile, policy, sessions) == (
        "f5a-synthetic-profile-v1",
        "f4-synthetic-fixture-v1",
        "5",
    )
    payload = module.json.loads(parameters)
    assert payload == {
        "version": 1,
        "policy_version": "f4-synthetic-fixture-v1",
        "approved_strategies": ["synthetic-directional-v1"],
        "lookback_changes": 19,
        "entry_multiplier": "0.25",
        "stop_multiplier": "1.5",
        "target_multiplier": "3",
        "price_increment": "0.05",
        "min_reward_to_risk": "1.5",
        "short_enabled": True,
    }


def test_every_material_policy_field_is_registered() -> None:
    names = {field.name for field in dataclasses.fields(FuturesRecommendationGenerationPolicy)}
    payload = module.json.loads(module._policy_parameters(_POLICY))

    assert {"approved_strategies", "lookback_changes", "short_enabled"} <= set(payload)
    assert names - {"version"} <= set(payload)
    assert payload["policy_version"] == _POLICY.version.version


def test_an_identical_configuration_registers_once(database: Path) -> None:
    _record(database, _recommendation())
    _record(database, _recommendation(_BANK_OCT))

    assert len(_rows(database, "futures_recommendation_configurations")) == 1


def test_equal_decimals_are_one_configuration(database: Path) -> None:
    _record(database, _recommendation())
    same = _policy(entry_multiplier=Decimal("0.250"), target_multiplier=Decimal("3.00"))

    _record(database, _recommendation(_BANK_OCT), policy=same, profile=_PROFILE)

    assert len(_rows(database, "futures_recommendation_configurations")) == 1


def test_a_profile_version_reused_with_another_horizon_is_a_configuration_conflict(
    database: Path,
) -> None:
    _record(database, _recommendation())
    reused = FuturesTradeRecommendationGenerationProfile(_PROFILE.version, 7, _POLICY.version)

    with pytest.raises(FuturesTradeRecommendationConfigurationConflictError):
        _record(database, _recommendation(_BANK_OCT, profile=reused), profile=reused)


def test_a_policy_version_reused_with_other_parameters_conflicts_even_under_a_new_profile(
    database: Path,
) -> None:
    _record(database, _recommendation())
    changed = _policy(min_reward_to_risk=Decimal("1.6"))

    with pytest.raises(FuturesTradeRecommendationConfigurationConflictError):
        _record(
            database,
            _recommendation(_BANK_OCT, profile=_PROFILE_V2),
            policy=changed,
            profile=_PROFILE_V2,
        )
    assert len(_rows(database, "futures_recommendation_configurations")) == 1


def test_two_profiles_may_share_one_policy(database: Path) -> None:
    _record(database, _recommendation())
    _record(database, _recommendation(_BANK_OCT, profile=_PROFILE_V2), profile=_PROFILE_V2)

    assert [row[0] for row in _rows(database, "futures_recommendation_configurations")] == [
        "f5a-synthetic-profile-v1",
        "f5a-synthetic-profile-v2",
    ]


def test_a_failed_batch_does_not_register_its_configuration(database: Path) -> None:
    _record(database, _recommendation(_OCT))
    with pytest.raises(FuturesTradeRecommendationExclusivityError):
        _record(database, _recommendation(_NOV, profile=_PROFILE_V2), profile=_PROFILE_V2)

    assert [row[0] for row in _rows(database, "futures_recommendation_configurations")] == [
        "f5a-synthetic-profile-v1"
    ]


# ---------------------------------------------------------------------------
# Definitions
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("direction", [LONG, SHORT])
def test_a_definition_round_trips_exactly(database: Path, direction: TradeDirection) -> None:
    recommendation = _recommendation(direction=direction)
    _record(database, recommendation)

    stored = _repository(database).get_recommendation(recommendation.identity)

    assert stored == recommendation
    assert stored.issued_at == recommendation.issued_at
    assert stored.thesis == recommendation.thesis
    assert (stored.entry, stored.stop, stored.target) == (
        recommendation.entry,
        recommendation.stop,
        recommendation.target,
    )


def test_the_columns_carry_the_identity_inputs(database: Path) -> None:
    recommendation = _recommendation()
    _record(database, recommendation)

    ((row),) = _rows(database, "futures_trade_recommendations")
    assert row[:10] == (
        recommendation.identity.identity,
        "NIFTY",
        "NSE",
        "2026-10-27",
        "1d",
        "2026-10-07T10:00:00Z",
        "synthetic-directional-v1",
        "f4-synthetic-fixture-v1",
        "f5a-synthetic-profile-v1",
        "2026-10-07T12:00:00Z",
    )
    assert "24383" in row[10] and "OHLC" not in row[10].upper()


def test_recording_the_identical_definition_again_is_a_no_op(database: Path) -> None:
    recommendation = _recommendation()
    _record(database, recommendation)

    assert _record(database, recommendation) == 1
    assert len(_rows(database, "futures_trade_recommendations")) == 1


def test_a_different_definition_under_one_identity_is_a_conflict(database: Path) -> None:
    first = _recommendation(issued_at="2026-10-07T11:00:00Z")
    later = _recommendation(issued_at="2026-10-07T12:00:00Z")
    assert first.identity == later.identity
    _record(database, first)

    with pytest.raises(FuturesTradeRecommendationConflictError):
        _record(database, later)
    assert _repository(database).get_recommendation(first.identity) == first


def test_an_identity_not_derived_from_its_source_is_refused(database: Path) -> None:
    forged = dataclasses.replace(
        _recommendation(), identity=FuturesTradeRecommendationIdentity("forged")
    )

    with pytest.raises(InvalidFuturesTradeRecommendationRecordingError):
        _record(database, forged)
    assert not database.exists() or _rows(database, "futures_trade_recommendations") == []


def test_a_profile_for_another_policy_is_refused_before_anything_is_written(database: Path) -> None:
    other = FuturesTradeRecommendationGenerationProfile(
        "p", 5, RecommendationPolicyVersion("f4-synthetic-fixture-v9")
    )

    with pytest.raises(InvalidFuturesTradeRecommendationRecordingError):
        _record(database, _recommendation(), profile=other)
    assert not database.exists()


def test_a_batch_naming_one_identity_twice_is_refused(database: Path) -> None:
    recommendation = _recommendation()

    with pytest.raises(InvalidFuturesTradeRecommendationRecordingError):
        _store(database).record(
            (recommendation, recommendation),
            policy=_POLICY,
            profile=_PROFILE,
            audit=recording_audit(recommendation, profile=_PROFILE),
        )


def test_different_products_are_recorded_independently(database: Path) -> None:
    _record(database, _recommendation(_OCT))
    _record(database, _recommendation(_BANK_OCT, SHORT))
    _record(database, _recommendation(_SGX_OCT))

    assert len(_rows(database, "futures_trade_recommendations")) == 3


# ---------------------------------------------------------------------------
# Product exclusivity across expiries
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "second",
    [
        _recommendation(_NOV, LONG),
        _recommendation(_NOV, SHORT),
        _recommendation(
            _OCT, LONG, decided="2026-10-06T10:00:00Z", issued_at="2026-10-06T12:00:00Z"
        ),
        _recommendation(_NOV, LONG, profile=_PROFILE_V2),
    ],
    ids=["same-direction-other-expiry", "opposite-other-expiry", "same-contract", "new-profile"],
)
def test_an_open_recommendation_blocks_its_product(
    database: Path, second: FuturesTradeRecommendation
) -> None:
    _record(database, _recommendation(_OCT))
    profile = (
        _PROFILE_V2
        if second.identity == _recommendation(_NOV, profile=_PROFILE_V2).identity
        else _PROFILE
    )

    with pytest.raises(FuturesTradeRecommendationExclusivityError):
        _record(database, second, profile=profile)
    assert len(_rows(database, "futures_trade_recommendations")) == 1


def test_a_new_policy_version_cannot_bypass_exclusivity(database: Path) -> None:
    _record(database, _recommendation(_OCT))
    policy = _policy("f4-synthetic-fixture-v2")
    profile = FuturesTradeRecommendationGenerationProfile(
        "f5a-synthetic-profile-v3", 5, policy.version
    )

    with pytest.raises(FuturesTradeRecommendationExclusivityError):
        _record(
            database,
            _recommendation(_NOV, policy=policy, profile=profile),
            policy=policy,
            profile=profile,
        )


def test_one_batch_cannot_hold_two_recommendations_for_one_product(database: Path) -> None:
    sibling = _recommendation(_OCT, decided="2026-10-06T10:00:00Z")

    with pytest.raises(FuturesTradeRecommendationExclusivityError):
        _record(database, _recommendation(_OCT), sibling)

    assert _rows(database, "futures_trade_recommendations") == []
    assert _rows(database, "futures_recommendation_configurations") == []


# ---------------------------------------------------------------------------
# Concurrency: separate connections, serialised by BEGIN IMMEDIATE
# ---------------------------------------------------------------------------


def _race(database: Path, attempts: list) -> list[object]:
    """Run each attempt on its own thread and connection while a writer lock is held.

    Every attempt starts behind the held lock, so all of them meet SQLite's
    writer serialisation; the outcomes are then classified, never ordered.
    """
    holder = sqlite3.connect(database, isolation_level=None)
    holder.execute("BEGIN IMMEDIATE")
    barrier = threading.Barrier(len(attempts) + 1)
    outcomes: list[object] = [None] * len(attempts)

    def run(index: int, attempt) -> None:
        barrier.wait()
        try:
            outcomes[index] = attempt()
        except Exception as exc:  # noqa: BLE001 -- the outcome is what is classified
            outcomes[index] = exc

    threads = [threading.Thread(target=run, args=(i, a)) for i, a in enumerate(attempts)]
    for thread in threads:
        thread.start()
    barrier.wait()
    holder.rollback()
    holder.close()
    for thread in threads:
        thread.join(timeout=30)
    return outcomes


def _initialised(database: Path) -> Path:
    with closing(sqlite3.connect(database)) as connection:
        module.initialize_futures_trade_recommendation_schema(connection)
    return database


def test_concurrent_attempts_on_one_identity_leave_exactly_one_winner(database: Path) -> None:
    _initialised(database)
    first = _recommendation(issued_at="2026-10-07T11:00:00Z")
    second = _recommendation(issued_at="2026-10-07T12:00:00Z")

    outcomes = _race(
        database, [lambda: _record(database, first), lambda: _record(database, second)]
    )

    assert sorted(type(o).__name__ for o in outcomes) == [
        "FuturesTradeRecommendationConflictError",
        "int",
    ]
    stored = _repository(database).get_recommendation(first.identity)
    assert stored == (first if outcomes[0] == 1 else second)


def test_concurrent_attempts_on_two_expiries_leave_one_open_recommendation(database: Path) -> None:
    _initialised(database)

    outcomes = _race(
        database,
        [
            lambda: _record(database, _recommendation(_OCT)),
            lambda: _record(database, _recommendation(_NOV)),
        ],
    )

    assert sorted(type(o).__name__ for o in outcomes) == [
        "FuturesTradeRecommendationExclusivityError",
        "int",
    ]
    assert len(_rows(database, "futures_trade_recommendations")) == 1


def test_concurrent_profiles_of_one_product_leave_one_open_recommendation(database: Path) -> None:
    _initialised(database)
    v2 = _recommendation(_OCT, profile=_PROFILE_V2)

    outcomes = _race(
        database,
        [
            lambda: _record(database, _recommendation(_OCT)),
            lambda: _record(database, v2, profile=_PROFILE_V2),
        ],
    )

    assert sorted(type(o).__name__ for o in outcomes) == [
        "FuturesTradeRecommendationExclusivityError",
        "int",
    ]


def test_concurrent_configuration_collision_registers_only_one(database: Path) -> None:
    reused = FuturesTradeRecommendationGenerationProfile(_PROFILE.version, 7, _POLICY.version)

    outcomes = _race(
        database,
        [
            lambda: _record(database, _recommendation(_OCT)),
            lambda: _record(database, _recommendation(_BANK_OCT, profile=reused), profile=reused),
        ],
    )

    assert sorted(type(o).__name__ for o in outcomes) == [
        "FuturesTradeRecommendationConfigurationConflictError",
        "int",
    ]
    assert len(_rows(database, "futures_recommendation_configurations")) == 1


def test_concurrent_attempts_on_different_products_both_succeed(database: Path) -> None:
    _initialised(database)

    outcomes = _race(
        database,
        [
            lambda: _record(database, _recommendation(_OCT)),
            lambda: _record(database, _recommendation(_BANK_OCT)),
        ],
    )

    assert outcomes == [1, 1]
    assert len(_rows(database, "futures_trade_recommendations")) == 2


def test_the_busy_timeout_is_five_seconds() -> None:
    assert module._BUSY_TIMEOUT_SECONDS == 5.0


def test_a_writer_that_cannot_get_the_lock_fails_and_writes_nothing(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _initialised(database)
    monkeypatch.setattr(module, "_BUSY_TIMEOUT_SECONDS", 0.2)
    holder = sqlite3.connect(database, isolation_level=None)
    holder.execute("BEGIN IMMEDIATE")
    try:
        started = time.monotonic()
        with pytest.raises(FuturesTradeRecommendationStorageError):
            _record(database, _recommendation())
        assert time.monotonic() - started >= 0.15
    finally:
        holder.rollback()
        holder.close()

    assert _rows(database, "futures_trade_recommendations") == []


# ---------------------------------------------------------------------------
# Repository reconstruction
# ---------------------------------------------------------------------------


def test_an_unknown_identity_is_none(database: Path) -> None:
    _record(database, _recommendation())

    assert _repository(database).get_recommendation(FuturesTradeRecommendationIdentity("x")) is None


def test_every_expiry_of_the_product_is_returned_and_others_are_not(database: Path) -> None:
    october, november = _recommendation(_OCT), _recommendation(_NOV)
    _record(database, october)
    # A second open NIFTY recommendation cannot be recorded until the first is
    # released (F-5B-2); insert its canonical row directly to prove that the
    # read spans every expiry of the product.
    configuration = _rows(database, "futures_recommendation_configurations")[0]
    row = module._definition_row(
        november, _PROFILE.version, module._configuration_digest(configuration)
    )
    _execute(database, module._INSERT_DEFINITION, row)
    _record(database, _recommendation(_BANK_OCT))

    lifecycles = _repository(database).get_lifecycles(FuturesTradeRecommendationQuery(_NIFTY))

    assert {lc.recommendation for lc in lifecycles} == {october, november}


def test_lifecycles_start_waiting_and_ignore_the_clock(database: Path) -> None:
    long_expired = _recommendation(decided="2025-01-06T10:00:00Z", issued_at="2025-01-06T12:00:00Z")
    _record(database, long_expired)

    (lifecycle,) = _repository(database).get_lifecycles(FuturesTradeRecommendationQuery(_NIFTY))

    assert lifecycle.status is RecommendationStatus.WAITING_FOR_ENTRY
    assert lifecycle.events == () and lifecycle.last_observation is None


@pytest.mark.parametrize(
    "sql",
    [
        "UPDATE futures_trade_recommendations SET definition = 'not json'",
        "UPDATE futures_trade_recommendations "
        "SET definition = replace(definition, '\"version\":1', '\"version\":2')",
        "UPDATE futures_trade_recommendations SET definition = replace(definition, ',', ', ')",
        "UPDATE futures_trade_recommendations "
        "SET definition = replace(definition, '24448.45', '24448.50')",
        "UPDATE futures_trade_recommendations SET issued_at = '2026-10-07T12:30:00Z'",
        "UPDATE futures_trade_recommendations SET strategy_identity = 'other'",
        "UPDATE futures_recommendation_configurations SET validity_sessions = '6'",
        "UPDATE futures_recommendation_configurations "
        "SET policy_parameters = replace(policy_parameters, '0.05', '0.1')",
    ],
    ids=[
        "corrupt-json",
        "unknown-version",
        "non-canonical-json",
        "tampered-level",
        "column-disagrees",
        "source-column-disagrees",
        "profile-horizon-changed",
        "policy-parameters-changed",
    ],
)
def test_tampered_storage_fails_closed(database: Path, sql: str) -> None:
    recommendation = _recommendation()
    _record(database, recommendation)
    _execute(database, sql)

    with pytest.raises(FuturesTradeRecommendationStorageError):
        _repository(database).get_recommendation(recommendation.identity)
    with pytest.raises(FuturesTradeRecommendationStorageError):
        _repository(database).get_lifecycles(FuturesTradeRecommendationQuery(_NIFTY))


def test_a_tampered_identity_fails_closed(database: Path) -> None:
    recommendation = _recommendation()
    _record(database, recommendation)
    _execute(
        database,
        "UPDATE futures_trade_recommendations SET recommendation_identity = 'tampered', "
        "definition = replace(definition, ?, 'tampered')",
        (recommendation.identity.identity,),
    )

    with pytest.raises(FuturesTradeRecommendationStorageError):
        _repository(database).get_recommendation(FuturesTradeRecommendationIdentity("tampered"))


def test_a_missing_configuration_fails_closed(database: Path) -> None:
    recommendation = _recommendation()
    _record(database, recommendation)
    _execute(database, "DELETE FROM futures_recommendation_configurations")

    with pytest.raises(FuturesTradeRecommendationStorageError, match="no registered"):
        _repository(database).get_recommendation(recommendation.identity)


def test_reads_never_repair_storage(database: Path) -> None:
    recommendation = _recommendation()
    _record(database, recommendation)
    _execute(database, "UPDATE futures_trade_recommendations SET definition = 'not json'")
    before = _rows(database, "futures_trade_recommendations")

    with pytest.raises(FuturesTradeRecommendationStorageError):
        _repository(database).get_recommendation(recommendation.identity)
    assert _rows(database, "futures_trade_recommendations") == before


# ---------------------------------------------------------------------------
# Architecture and hygiene
# ---------------------------------------------------------------------------


def _non_docstring_strings(tree: ast.AST) -> list[str]:
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


def test_the_adapter_issues_insert_only() -> None:
    tree = ast.parse(Path(module.__file__).read_text(encoding="utf-8"))
    sql = " ".join(_non_docstring_strings(tree)).upper()

    assert "INSERT INTO FUTURES_TRADE_RECOMMENDATIONS" in sql
    assert "INSERT INTO FUTURES_RECOMMENDATION_CONFIGURATIONS" in sql
    for forbidden in ("UPDATE ", "DELETE ", "REPLACE", "ON CONFLICT", "UPSERT", "JOURNAL_MODE"):
        assert forbidden not in sql


def test_the_adapter_depends_on_no_provider_clock_execution_or_lifecycle_store() -> None:
    tree = ast.parse(Path(module.__file__).read_text(encoding="utf-8"))
    modules = {
        node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.module
    } | {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }

    for name in modules:
        assert name.split(".")[0] not in {
            "requests",
            "urllib",
            "socket",
            "time",
            "datetime",
            "random",
            "uuid",
            "pickle",
            "databento",
        }, name
        for fragment in ("upstox", "paper_trading", "execution", "market_data"):
            assert fragment not in name, name


def test_the_adapters_implement_the_application_ports() -> None:
    assert issubclass(SQLiteFuturesTradeRecommendationStore, FuturesTradeRecommendationStore)
    assert issubclass(
        SQLiteFuturesTradeRecommendationRepository, FuturesTradeRecommendationRepository
    )
