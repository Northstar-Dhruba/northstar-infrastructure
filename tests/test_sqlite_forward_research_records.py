"""Contract tests for the SQLite forward research record adapters."""

from __future__ import annotations

import json
import sqlite3
from decimal import Decimal
from pathlib import Path

import pytest
from northstar_application.application_services import (
    AnalyzeAssetResult,
    AnalyzeMarketObservationContextService,
    ForwardResearchRecord,
)
from northstar_application.ports import (
    ForwardResearchRecordConflictError,
    ForwardResearchRecordQuery,
)
from northstar_core.domain.value_objects import ListingReference
from northstar_core.foundation.value_objects import (
    Currency,
    ExchangeCode,
    PointInTime,
    Price,
    Quantity,
    Symbol,
    Timeframe,
)
from northstar_core.strategy import (
    AssetAnalysisGenerator,
    ExplanationReason,
    MarketObservationContext,
    RecommendationExplanation,
    Strategy,
    StrategyIdentity,
)

from northstar_infrastructure.persistence import (
    SQLiteForwardResearchRecordRepository,
    SQLiteForwardResearchRecordStore,
)

_USD = Currency("USD")
_DAILY = Timeframe("1d")
_SYMBOL = Symbol("AAPL")
_EXCHANGE = ExchangeCode("NASDAQ")
_DECISION_INSTANT = PointInTime("2026-01-20T16:00:00Z")


@pytest.fixture
def database_path(tmp_path: Path) -> Path:
    return tmp_path / "forward_research.sqlite3"


def _context(
    *,
    observed_at: PointInTime = _DECISION_INSTANT,
    symbol: Symbol = _SYMBOL,
    exchange_code: ExchangeCode = _EXCHANGE,
    latest_price: Price | None = None,
) -> MarketObservationContext:
    price = latest_price or Price("100", _USD)
    return MarketObservationContext(
        ListingReference(symbol, exchange_code),
        observed_at,
        price,
        Price("99", price.currency),
        Quantity("1000"),
        Price("900", price.currency),
        Price("1", price.currency),
        tuple(Price(str(100 + index), price.currency) for index in range(20)),
        tuple(Quantity(str(1000 + index)) for index in range(20)),
    )


def _result(
    *,
    observed_at: PointInTime = _DECISION_INSTANT,
    symbol: Symbol = _SYMBOL,
    exchange_code: ExchangeCode = _EXCHANGE,
    strategy: str = "mvp",
    latest_price: Price | None = None,
) -> AnalyzeAssetResult:
    return AnalyzeMarketObservationContextService(
        strategy=Strategy(StrategyIdentity(strategy)),
        analysis_generator=AssetAnalysisGenerator(),
    ).execute(
        _context(
            observed_at=observed_at,
            symbol=symbol,
            exchange_code=exchange_code,
            latest_price=latest_price,
        )
    )


def _record(timeframe: Timeframe = _DAILY, **kwargs: object) -> ForwardResearchRecord:
    return ForwardResearchRecord(result=_result(**kwargs), timeframe=timeframe)


def _query(
    symbol: Symbol = _SYMBOL,
    exchange_code: ExchangeCode = _EXCHANGE,
    timeframe: Timeframe = _DAILY,
) -> ForwardResearchRecordQuery:
    return ForwardResearchRecordQuery(symbol, exchange_code, timeframe)


# ---------------------------------------------------------------------------
# Round trip
# ---------------------------------------------------------------------------


def test_record_round_trips_exactly(database_path: Path) -> None:
    record = _record()
    SQLiteForwardResearchRecordStore(database_path).store((record,))

    retrieved = SQLiteForwardResearchRecordRepository(database_path).get_records(_query())

    assert retrieved == (record,)
    assert retrieved[0].result == record.result
    assert retrieved[0].timeframe == record.timeframe


def test_round_trip_preserves_every_evidence_component(database_path: Path) -> None:
    record = _record()
    SQLiteForwardResearchRecordStore(database_path).store((record,))

    restored = SQLiteForwardResearchRecordRepository(database_path).get_records(_query())[0]

    assert restored.result.recommendation == record.result.recommendation
    assert restored.result.explanation == record.result.explanation
    assert restored.result.market_observation_context == (record.result.market_observation_context)
    assert restored.result.recommendation.asset_analysis == (
        record.result.recommendation.asset_analysis
    )


def test_round_trip_preserves_explanation_recommendation_linkage(database_path: Path) -> None:
    record = _record()
    SQLiteForwardResearchRecordStore(database_path).store((record,))

    restored = SQLiteForwardResearchRecordRepository(database_path).get_records(_query())[0]

    assert restored.result.explanation.recommendation == restored.result.recommendation


def test_round_trip_preserves_derived_identity(database_path: Path) -> None:
    record = _record()
    SQLiteForwardResearchRecordStore(database_path).store((record,))

    restored = SQLiteForwardResearchRecordRepository(database_path).get_records(_query())[0]

    assert restored.natural_key == record.natural_key
    assert restored.listing_reference == record.listing_reference
    assert restored.strategy_identity == record.strategy_identity
    assert restored.decision_instant == record.decision_instant


# ---------------------------------------------------------------------------
# Decimal and PointInTime fidelity
# ---------------------------------------------------------------------------


def test_decimal_values_are_preserved_losslessly(database_path: Path) -> None:
    precise = Price("123.456789012345678901234567", _USD)
    record = _record(latest_price=precise)
    SQLiteForwardResearchRecordStore(database_path).store((record,))

    restored = SQLiteForwardResearchRecordRepository(database_path).get_records(_query())[0]
    context = restored.result.market_observation_context

    assert context.latest_price.amount == Decimal("123.456789012345678901234567")
    assert isinstance(context.latest_price.amount, Decimal)
    assert context.recent_closes[0].amount == Decimal("100")
    assert context.latest_volume.value == Decimal("1000")


def test_decimal_values_are_stored_as_text_not_float(database_path: Path) -> None:
    SQLiteForwardResearchRecordStore(database_path).store(
        (_record(latest_price=Price("123.456789012345678901234567", _USD)),)
    )

    with sqlite3.connect(database_path) as connection:
        evidence = connection.execute("SELECT evidence FROM forward_research_records").fetchone()[0]

    assert isinstance(evidence, str)
    assert '"123.456789012345678901234567"' in evidence
    assert "123.45678901234568" not in evidence


def test_point_in_time_semantics_are_preserved(database_path: Path) -> None:
    offset_instant = PointInTime("2026-01-20T21:30:00+05:30")
    record = _record(observed_at=offset_instant)
    SQLiteForwardResearchRecordStore(database_path).store((record,))

    restored = SQLiteForwardResearchRecordRepository(database_path).get_records(_query())[0]

    assert restored.decision_instant == _DECISION_INSTANT
    assert restored.decision_instant.compare(offset_instant) == 0


def test_tuple_ordering_is_preserved(database_path: Path) -> None:
    record = _record()
    SQLiteForwardResearchRecordStore(database_path).store((record,))

    restored = SQLiteForwardResearchRecordRepository(database_path).get_records(_query())[0]
    context = restored.result.market_observation_context

    assert context.recent_closes == record.result.market_observation_context.recent_closes
    assert context.recent_volumes == record.result.market_observation_context.recent_volumes
    assert [price.amount for price in context.recent_closes] == [
        Decimal(100 + index) for index in range(20)
    ]


def test_evidence_is_json_and_not_pickled(database_path: Path) -> None:
    SQLiteForwardResearchRecordStore(database_path).store((_record(),))

    with sqlite3.connect(database_path) as connection:
        evidence = connection.execute("SELECT evidence FROM forward_research_records").fetchone()[0]

    payload = json.loads(evidence)
    assert payload["version"] == 1
    assert payload["timeframe"] == "1d"
    assert set(payload) == {
        "version",
        "timeframe",
        "context",
        "analysis",
        "recommendation",
        "explanation",
    }


# ---------------------------------------------------------------------------
# Freeze semantics
# ---------------------------------------------------------------------------


def test_new_key_is_inserted(database_path: Path) -> None:
    store = SQLiteForwardResearchRecordStore(database_path)

    assert store.store((_record(),)) == 1
    assert len(SQLiteForwardResearchRecordRepository(database_path).get_records(_query())) == 1


def test_identical_retry_is_idempotent(database_path: Path) -> None:
    store = SQLiteForwardResearchRecordStore(database_path)
    record = _record()

    assert store.store((record,)) == 1
    assert store.store((record,)) == 1

    retrieved = SQLiteForwardResearchRecordRepository(database_path).get_records(_query())
    assert retrieved == (record,)


def test_structurally_identical_record_from_a_new_object_is_idempotent(
    database_path: Path,
) -> None:
    store = SQLiteForwardResearchRecordStore(database_path)
    store.store((_record(),))

    assert store.store((_record(),)) == 1
    assert len(SQLiteForwardResearchRecordRepository(database_path).get_records(_query())) == 1


def test_conflicting_same_key_record_is_rejected_and_original_preserved(
    database_path: Path,
) -> None:
    store = SQLiteForwardResearchRecordStore(database_path)
    original = _record()
    store.store((original,))

    divergent_result = AnalyzeAssetResult(
        recommendation=original.result.recommendation,
        explanation=RecommendationExplanation(
            recommendation=original.result.recommendation,
            reasons=(ExplanationReason(rationale="A different explanation."),),
        ),
        market_observation_context=original.result.market_observation_context,
    )
    divergent = ForwardResearchRecord(result=divergent_result, timeframe=_DAILY)
    assert divergent.natural_key == original.natural_key

    with pytest.raises(ForwardResearchRecordConflictError, match="already frozen"):
        store.store((divergent,))

    retrieved = SQLiteForwardResearchRecordRepository(database_path).get_records(_query())
    assert retrieved == (original,)


def test_batch_conflict_writes_nothing(database_path: Path) -> None:
    store = SQLiteForwardResearchRecordStore(database_path)
    existing = _record()
    store.store((existing,))

    divergent_result = AnalyzeAssetResult(
        recommendation=existing.result.recommendation,
        explanation=RecommendationExplanation(
            recommendation=existing.result.recommendation,
            reasons=(ExplanationReason(rationale="Conflicting."),),
        ),
        market_observation_context=existing.result.market_observation_context,
    )
    fresh = _record(observed_at=PointInTime("2026-01-21T16:00:00Z"))
    conflicting = ForwardResearchRecord(result=divergent_result, timeframe=_DAILY)

    with pytest.raises(ForwardResearchRecordConflictError):
        store.store((fresh, conflicting))

    retrieved = SQLiteForwardResearchRecordRepository(database_path).get_records(_query())
    assert retrieved == (existing,)


def test_batch_with_duplicate_natural_keys_is_rejected(database_path: Path) -> None:
    store = SQLiteForwardResearchRecordStore(database_path)
    record = _record()

    with pytest.raises(ForwardResearchRecordConflictError, match="two records sharing"):
        store.store((record, record))

    assert SQLiteForwardResearchRecordRepository(database_path).get_records(_query()) == ()


def test_empty_batch_is_a_safe_no_op(database_path: Path) -> None:
    assert SQLiteForwardResearchRecordStore(database_path).store(()) == 0


def test_accepted_and_idempotent_records_both_count(database_path: Path) -> None:
    store = SQLiteForwardResearchRecordStore(database_path)
    first = _record()
    store.store((first,))
    second = _record(observed_at=PointInTime("2026-01-21T16:00:00Z"))

    assert store.store((first, second)) == 2
    assert len(SQLiteForwardResearchRecordRepository(database_path).get_records(_query())) == 2


# ---------------------------------------------------------------------------
# Natural key distinctions
# ---------------------------------------------------------------------------


def test_multiple_strategies_coexist_at_one_instant(database_path: Path) -> None:
    store = SQLiteForwardResearchRecordStore(database_path)
    store.store((_record(strategy="mvp"),))
    store.store((_record(strategy="momentum"),))

    retrieved = SQLiteForwardResearchRecordRepository(database_path).get_records(_query())

    assert len(retrieved) == 2
    assert {record.strategy_identity.identity for record in retrieved} == {"mvp", "momentum"}


def test_timeframe_distinguishes_records(database_path: Path) -> None:
    store = SQLiteForwardResearchRecordStore(database_path)
    store.store((_record(timeframe=_DAILY),))
    store.store((_record(timeframe=Timeframe("1h")),))

    repository = SQLiteForwardResearchRecordRepository(database_path)

    assert len(repository.get_records(_query(timeframe=_DAILY))) == 1
    assert len(repository.get_records(_query(timeframe=Timeframe("1h")))) == 1


def test_listing_distinguishes_records(database_path: Path) -> None:
    store = SQLiteForwardResearchRecordStore(database_path)
    store.store((_record(),))
    store.store((_record(symbol=Symbol("MSFT")),))
    store.store((_record(exchange_code=ExchangeCode("NYSE")),))

    repository = SQLiteForwardResearchRecordRepository(database_path)

    assert len(repository.get_records(_query())) == 1
    assert len(repository.get_records(_query(symbol=Symbol("MSFT")))) == 1
    assert len(repository.get_records(_query(exchange_code=ExchangeCode("NYSE")))) == 1


def test_decision_instant_distinguishes_records(database_path: Path) -> None:
    store = SQLiteForwardResearchRecordStore(database_path)
    store.store((_record(),))
    store.store((_record(observed_at=PointInTime("2026-01-21T16:00:00Z")),))

    assert len(SQLiteForwardResearchRecordRepository(database_path).get_records(_query())) == 2


# ---------------------------------------------------------------------------
# Ordering and isolation
# ---------------------------------------------------------------------------


def test_records_are_ordered_by_decision_instant_ascending(database_path: Path) -> None:
    store = SQLiteForwardResearchRecordStore(database_path)
    store.store((_record(observed_at=PointInTime("2026-01-23T16:00:00Z")),))
    store.store((_record(observed_at=PointInTime("2026-01-21T16:00:00Z")),))
    store.store((_record(observed_at=_DECISION_INSTANT),))

    retrieved = SQLiteForwardResearchRecordRepository(database_path).get_records(_query())

    assert [record.decision_instant.value for record in retrieved] == [
        "2026-01-20T16:00:00Z",
        "2026-01-21T16:00:00Z",
        "2026-01-23T16:00:00Z",
    ]


def test_same_instant_records_are_ordered_by_strategy_identity(database_path: Path) -> None:
    store = SQLiteForwardResearchRecordStore(database_path)
    for strategy in ("zeta", "alpha", "mvp"):
        store.store((_record(strategy=strategy),))

    retrieved = SQLiteForwardResearchRecordRepository(database_path).get_records(_query())

    assert [record.strategy_identity.identity for record in retrieved] == ["alpha", "mvp", "zeta"]


def test_ordering_applies_across_instants_and_strategies(database_path: Path) -> None:
    store = SQLiteForwardResearchRecordStore(database_path)
    later = PointInTime("2026-01-21T16:00:00Z")
    store.store((_record(observed_at=later, strategy="alpha"),))
    store.store((_record(strategy="zeta"),))
    store.store((_record(strategy="alpha"),))

    retrieved = SQLiteForwardResearchRecordRepository(database_path).get_records(_query())

    assert [
        (record.decision_instant.value, record.strategy_identity.identity) for record in retrieved
    ] == [
        ("2026-01-20T16:00:00Z", "alpha"),
        ("2026-01-20T16:00:00Z", "zeta"),
        ("2026-01-21T16:00:00Z", "alpha"),
    ]


def test_fractional_second_instants_are_ordered_chronologically(database_path: Path) -> None:
    whole_second = PointInTime("2026-01-20T16:00:00Z")
    fractional = PointInTime("2026-01-20T16:00:00.1Z")
    store = SQLiteForwardResearchRecordStore(database_path)
    store.store((_record(observed_at=fractional),))
    store.store((_record(observed_at=whole_second),))

    retrieved = SQLiteForwardResearchRecordRepository(database_path).get_records(_query())

    assert whole_second.compare(fractional) < 0
    assert [record.decision_instant for record in retrieved] == [whole_second, fractional]


def test_ordering_does_not_follow_sqlite_text_ordering(database_path: Path) -> None:
    whole_second = PointInTime("2026-01-20T16:00:00Z")
    fractional = PointInTime("2026-01-20T16:00:00.1Z")
    store = SQLiteForwardResearchRecordStore(database_path)
    store.store((_record(observed_at=whole_second),))
    store.store((_record(observed_at=fractional),))

    with sqlite3.connect(database_path) as connection:
        text_ordered = [
            row[0]
            for row in connection.execute(
                "SELECT decision_instant FROM forward_research_records "
                "ORDER BY decision_instant ASC"
            ).fetchall()
        ]

    retrieved = SQLiteForwardResearchRecordRepository(database_path).get_records(_query())

    assert text_ordered == [fractional.value, whole_second.value]
    assert [record.decision_instant.value for record in retrieved] == [
        whole_second.value,
        fractional.value,
    ]


@pytest.mark.parametrize(
    "instants",
    [
        ("2026-01-20T16:00:00Z", "2026-01-20T16:00:00.1Z", "2026-01-20T16:00:00.25Z"),
        ("2026-01-20T16:00:00.000001Z", "2026-01-20T16:00:01Z", "2026-01-20T16:00:59.9Z"),
    ],
)
def test_sub_second_instants_sort_chronologically(
    database_path: Path, instants: tuple[str, ...]
) -> None:
    store = SQLiteForwardResearchRecordStore(database_path)
    for value in reversed(instants):
        store.store((_record(observed_at=PointInTime(value)),))

    retrieved = SQLiteForwardResearchRecordRepository(database_path).get_records(_query())

    assert [record.decision_instant.value for record in retrieved] == [
        PointInTime(value).value for value in instants
    ]


def test_strategy_tie_break_applies_at_a_fractional_instant(database_path: Path) -> None:
    fractional = PointInTime("2026-01-20T16:00:00.1Z")
    store = SQLiteForwardResearchRecordStore(database_path)
    for strategy in ("zeta", "alpha", "mvp"):
        store.store((_record(observed_at=fractional, strategy=strategy),))

    retrieved = SQLiteForwardResearchRecordRepository(database_path).get_records(_query())

    assert [record.strategy_identity.identity for record in retrieved] == ["alpha", "mvp", "zeta"]


def test_strategy_tie_break_applies_at_semantically_equal_instants(database_path: Path) -> None:
    utc = PointInTime("2026-01-20T16:00:00Z")
    offset = PointInTime("2026-01-20T21:30:00+05:30")
    store = SQLiteForwardResearchRecordStore(database_path)
    store.store((_record(observed_at=offset, strategy="zeta"),))
    store.store((_record(observed_at=utc, strategy="alpha"),))

    retrieved = SQLiteForwardResearchRecordRepository(database_path).get_records(_query())

    assert utc.compare(offset) == 0
    assert [record.strategy_identity.identity for record in retrieved] == ["alpha", "zeta"]


def test_chronological_and_strategy_ordering_combine_with_fractions(
    database_path: Path,
) -> None:
    whole_second = PointInTime("2026-01-20T16:00:00Z")
    fractional = PointInTime("2026-01-20T16:00:00.5Z")
    store = SQLiteForwardResearchRecordStore(database_path)
    store.store((_record(observed_at=fractional, strategy="alpha"),))
    store.store((_record(observed_at=whole_second, strategy="zeta"),))
    store.store((_record(observed_at=whole_second, strategy="alpha"),))

    retrieved = SQLiteForwardResearchRecordRepository(database_path).get_records(_query())

    assert [
        (record.decision_instant.value, record.strategy_identity.identity) for record in retrieved
    ] == [
        ("2026-01-20T16:00:00Z", "alpha"),
        ("2026-01-20T16:00:00Z", "zeta"),
        ("2026-01-20T16:00:00.5Z", "alpha"),
    ]


def test_query_isolates_by_series(database_path: Path) -> None:
    store = SQLiteForwardResearchRecordStore(database_path)
    store.store((_record(),))
    store.store((_record(symbol=Symbol("MSFT")),))

    retrieved = SQLiteForwardResearchRecordRepository(database_path).get_records(_query())

    assert len(retrieved) == 1
    assert retrieved[0].listing_reference.symbol == _SYMBOL


def test_empty_query_result_is_an_empty_tuple(database_path: Path) -> None:
    repository = SQLiteForwardResearchRecordRepository(database_path)

    assert repository.get_records(_query()) == ()


def test_repository_rejects_a_wrong_query_type(database_path: Path) -> None:
    with pytest.raises(TypeError, match="must be a ForwardResearchRecordQuery"):
        SQLiteForwardResearchRecordRepository(database_path).get_records("query")


# ---------------------------------------------------------------------------
# Durability
# ---------------------------------------------------------------------------


def test_record_survives_reopening_the_database(database_path: Path) -> None:
    record = _record()
    SQLiteForwardResearchRecordStore(database_path).store((record,))

    reopened = SQLiteForwardResearchRecordRepository(database_path).get_records(_query())

    assert reopened == (record,)
    assert database_path.exists()


def test_independent_store_instances_share_frozen_state(database_path: Path) -> None:
    SQLiteForwardResearchRecordStore(database_path).store((_record(),))

    second_store = SQLiteForwardResearchRecordStore(database_path)
    assert second_store.store((_record(),)) == 1

    assert len(SQLiteForwardResearchRecordRepository(database_path).get_records(_query())) == 1


def test_natural_key_columns_are_queryable_and_constrained(database_path: Path) -> None:
    SQLiteForwardResearchRecordStore(database_path).store((_record(),))

    with sqlite3.connect(database_path) as connection:
        row = connection.execute(
            """
            SELECT symbol, exchange_code, timeframe, decision_instant, strategy_identity
            FROM forward_research_records
            """
        ).fetchone()
        indexes = connection.execute(
            "SELECT name FROM pragma_index_list('forward_research_records')"
        ).fetchall()

    assert row == ("AAPL", "NASDAQ", "1d", "2026-01-20T16:00:00Z", "mvp")
    assert indexes, "expected the natural key to be enforced by a primary key index"
