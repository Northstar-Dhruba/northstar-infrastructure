"""End-to-end acceptance tests for the forward research path.

These tests exercise the seam the unit suites cannot reach: real SQLite
persistence driving the Application forward-research use cases. Record
ordering is implemented independently on both sides of that seam --
``_compare_records`` in the adapter and ``_validate_records`` in
RunForwardResearchUseCase -- so agreement between them is only provable by
running one against the other.

The fixture deliberately mixes whole-second and sub-second decision instants,
because a canonical instant omits fractional seconds when they are zero and is
therefore not lexicographically chronological. Stored as text,
``...T16:00:00.1Z`` sorts before ``...T16:00:00Z`` while being the later
instant, so an adapter that regressed to SQL text ordering would hand the run
records in an order its contract rejects.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from northstar_application.application_services import (
    AnalyzeAssetResult,
    AnalyzeMarketObservationContextService,
    BuildForwardResearchReportUseCase,
    CalculateForwardResearchMetricsUseCase,
    ForwardResearchMeasurementState,
    ForwardResearchRecord,
    ForwardResearchReport,
    ForwardResearchRun,
    HistoricalDataContractViolationError,
    MeasureForwardResearchRecordUseCase,
    MeasureRecommendationOutcomeUseCase,
    RecordForwardResearchDecisionUseCase,
    RunForwardResearchUseCase,
)
from northstar_application.ports import (
    ForwardResearchRecordQuery,
    ForwardResearchRecordRepository,
    HistoricalMarketDataQuery,
    HistoricalMarketDataRepository,
)
from northstar_core.domain.value_objects import ListingReference
from northstar_core.foundation.value_objects import (
    Currency,
    ExchangeCode,
    Percentage,
    PointInTime,
    Price,
    Quantity,
    Symbol,
    Timeframe,
)
from northstar_core.market_data import HistoricalOHLCVBar
from northstar_core.strategy import (
    AssetAnalysisGenerator,
    MarketObservationContext,
    ResearchHorizon,
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
_ALPHA = StrategyIdentity("alpha")
_ZETA = StrategyIdentity("zeta")
_HORIZONS = (ResearchHorizon(1), ResearchHorizon(2))

# Whole-second and sub-second instants on the same second, plus a later day.
# Text ordering would place _SUB_SECOND before _WHOLE_SECOND: '.' < 'Z'.
_WHOLE_SECOND = PointInTime("2026-01-20T16:00:00Z")
_SUB_SECOND = PointInTime("2026-01-20T16:00:00.1Z")
_NEXT_DAY = PointInTime("2026-01-21T16:00:00Z")

_FIRST_OBSERVATION = PointInTime("2026-01-22T16:00:00Z")
_SECOND_OBSERVATION = PointInTime("2026-01-23T16:00:00Z")

# The frozen decisions: alpha acts three times, zeta twice, sharing the two
# instants on 20 January so the strategy tie-break is exercised as well.
_FROZEN_DECISIONS = (
    (_WHOLE_SECOND, "alpha"),
    (_WHOLE_SECOND, "zeta"),
    (_SUB_SECOND, "alpha"),
    (_SUB_SECOND, "zeta"),
    (_NEXT_DAY, "alpha"),
)

# Deliberately scrambled, so a correct retrieval order cannot come from the
# order rows happened to be inserted in.
_INSERTION_ORDER = (
    (_NEXT_DAY, "alpha"),
    (_SUB_SECOND, "zeta"),
    (_WHOLE_SECOND, "zeta"),
    (_SUB_SECOND, "alpha"),
    (_WHOLE_SECOND, "alpha"),
)


@pytest.fixture
def database_path(tmp_path: Path) -> Path:
    return tmp_path / "forward_research.sqlite3"


class StubMarketData(HistoricalMarketDataRepository):
    """Returns bounded observations exactly as a conforming repository would."""

    def __init__(self, observations: tuple[HistoricalOHLCVBar, ...] = ()) -> None:
        self.observations = observations

    def get_history(self, query: HistoricalMarketDataQuery) -> tuple[HistoricalOHLCVBar, ...]:
        return tuple(
            bar
            for bar in self.observations
            if bar.symbol == query.symbol
            and bar.exchange_code == query.exchange_code
            and bar.timeframe == query.timeframe
            and query.start.compare(bar.point_in_time) <= 0
            and query.end.compare(bar.point_in_time) >= 0
        )


def _bar(point_in_time: PointInTime, close: str) -> HistoricalOHLCVBar:
    return HistoricalOHLCVBar(
        _SYMBOL,
        _EXCHANGE,
        point_in_time,
        _DAILY,
        Price("50", _USD),
        Price("900", _USD),
        Price("1", _USD),
        Price(close, _USD),
        Quantity("1000"),
    )


def _observations() -> tuple[HistoricalOHLCVBar, ...]:
    """Two observations after every frozen decision: +10% then +30%."""
    return (
        _bar(_FIRST_OBSERVATION, "110"),
        _bar(_SECOND_OBSERVATION, "130"),
    )


def _result(observed_at: PointInTime, strategy: str) -> AnalyzeAssetResult:
    context = MarketObservationContext(
        ListingReference(_SYMBOL, _EXCHANGE),
        observed_at,
        Price("100", _USD),
        Price("99", _USD),
        Quantity("1000"),
        Price("900", _USD),
        Price("1", _USD),
        tuple(Price(str(100 + index), _USD) for index in range(20)),
        tuple(Quantity(str(1000 + index)) for index in range(20)),
    )
    return AnalyzeMarketObservationContextService(
        strategy=Strategy(StrategyIdentity(strategy)),
        analysis_generator=AssetAnalysisGenerator(),
    ).execute(context)


def _query() -> ForwardResearchRecordQuery:
    return ForwardResearchRecordQuery(_SYMBOL, _EXCHANGE, _DAILY)


def _freeze_decisions(database_path: Path) -> tuple[ForwardResearchRecord, ...]:
    """Freeze every decision through the real store, one decision at a time."""
    recorder = RecordForwardResearchDecisionUseCase(SQLiteForwardResearchRecordStore(database_path))
    return tuple(
        recorder.execute(_result(observed_at, strategy), _DAILY)
        for observed_at, strategy in _INSERTION_ORDER
    )


def _run(
    database_path: Path,
    available_through: PointInTime,
    observations: tuple[HistoricalOHLCVBar, ...] = (),
) -> ForwardResearchRun:
    """Reopen the database from scratch and measure every frozen decision."""
    return RunForwardResearchUseCase(
        SQLiteForwardResearchRecordRepository(database_path),
        MeasureForwardResearchRecordUseCase(
            MeasureRecommendationOutcomeUseCase(StubMarketData(observations))
        ),
    ).execute(_query(), _HORIZONS, available_through)


def _report(
    database_path: Path,
    available_through: PointInTime,
    observations: tuple[HistoricalOHLCVBar, ...] = (),
) -> ForwardResearchReport:
    return BuildForwardResearchReportUseCase().execute(
        _run(database_path, available_through, observations)
    )


def _identities(records: tuple[ForwardResearchRecord, ...]) -> list[tuple[str, str]]:
    return [
        (record.decision_instant.value, record.strategy_identity.identity) for record in records
    ]


# ---------------------------------------------------------------------------
# The persistence seam: adapter output must satisfy the run's ordering contract
# ---------------------------------------------------------------------------


def test_real_repository_output_satisfies_the_run_ordering_contract(
    database_path: Path,
) -> None:
    """The run validates repository output defensively; it must not reject it."""
    _freeze_decisions(database_path)

    run = _run(database_path, _NEXT_DAY)

    assert _identities(run.records) == [
        (_WHOLE_SECOND.value, "alpha"),
        (_WHOLE_SECOND.value, "zeta"),
        (_SUB_SECOND.value, "alpha"),
        (_SUB_SECOND.value, "zeta"),
        (_NEXT_DAY.value, "alpha"),
    ]


def test_sql_text_ordering_would_violate_the_run_contract(database_path: Path) -> None:
    """Proves the ordering seam is load-bearing, not incidentally satisfied."""
    _freeze_decisions(database_path)

    with sqlite3.connect(database_path) as connection:
        text_ordered = connection.execute(
            "SELECT decision_instant, strategy_identity FROM forward_research_records "
            "ORDER BY decision_instant ASC, strategy_identity ASC"
        ).fetchall()

    run = _run(database_path, _NEXT_DAY)

    # Text ordering puts the sub-second instant first; chronological order does not.
    assert text_ordered[0] == (_SUB_SECOND.value, "alpha")
    assert _identities(run.records)[0] == (_WHOLE_SECOND.value, "alpha")
    assert [tuple(row) for row in text_ordered] != _identities(run.records)


def test_text_ordered_records_would_be_rejected_by_the_run(database_path: Path) -> None:
    """The seam is load-bearing: text order is not merely different, it is invalid.

    Feeding the run exactly what a text-ordering adapter would return makes the
    run raise, so the passing test above proves the real adapter's ordering,
    not an accident of this fixture.
    """
    _freeze_decisions(database_path)
    chronological = _run(database_path, _NEXT_DAY).records
    text_ordered = tuple(
        sorted(
            chronological,
            key=lambda record: (
                record.decision_instant.value,
                record.strategy_identity.identity,
            ),
        )
    )
    assert _identities(text_ordered) != _identities(chronological)

    class TextOrderedRepository(ForwardResearchRecordRepository):
        def get_records(
            self, query: ForwardResearchRecordQuery
        ) -> tuple[ForwardResearchRecord, ...]:
            return text_ordered

    use_case = RunForwardResearchUseCase(
        TextOrderedRepository(),
        MeasureForwardResearchRecordUseCase(MeasureRecommendationOutcomeUseCase(StubMarketData())),
    )

    with pytest.raises(HistoricalDataContractViolationError, match="ordered by decision instant"):
        use_case.execute(_query(), _HORIZONS, _NEXT_DAY)


def test_every_frozen_decision_survives_close_and_reopen(database_path: Path) -> None:
    frozen = _freeze_decisions(database_path)

    run = _run(database_path, _NEXT_DAY)

    assert len(run.records) == len(_FROZEN_DECISIONS)
    assert set(run.records) == set(frozen)


def test_both_strategies_are_retrieved_across_shared_instants(
    database_path: Path,
) -> None:
    _freeze_decisions(database_path)

    run = _run(database_path, _NEXT_DAY)

    assert {record.strategy_identity for record in run.records} == {_ALPHA, _ZETA}
    assert sum(1 for record in run.records if record.strategy_identity == _ALPHA) == 3
    assert sum(1 for record in run.records if record.strategy_identity == _ZETA) == 2


# ---------------------------------------------------------------------------
# Earlier as-of boundary: nothing has happened yet
# ---------------------------------------------------------------------------


def test_earlier_available_through_leaves_every_measurement_pending(
    database_path: Path,
) -> None:
    _freeze_decisions(database_path)

    run = _run(database_path, _NEXT_DAY)

    assert len(run.measurements) == len(_FROZEN_DECISIONS) * len(_HORIZONS)
    assert all(
        measurement.state is ForwardResearchMeasurementState.PENDING
        for measurement in run.measurements
    )


def test_earlier_available_through_reports_pending_counts(database_path: Path) -> None:
    _freeze_decisions(database_path)

    report = _report(database_path, _NEXT_DAY)

    assert report.record_count == 5
    assert report.pending_count == 10
    assert report.strategy_identities == (_ALPHA, _ZETA)
    assert [
        (entry.strategy_identity, entry.horizon.observations, entry.measured_count)
        for entry in report.metrics
    ] == [(_ALPHA, 1, 0), (_ALPHA, 2, 0), (_ZETA, 1, 0), (_ZETA, 2, 0)]
    assert all(entry.average_forward_return is None for entry in report.metrics)
    assert all(entry.unavailable_count == 0 for entry in report.metrics)


def test_pending_counts_partition_the_totals(database_path: Path) -> None:
    _freeze_decisions(database_path)

    report = _report(database_path, _NEXT_DAY)

    alpha_first, alpha_second, zeta_first, zeta_second = report.metrics
    assert alpha_first.total_count == alpha_first.pending_count == 3
    assert alpha_second.total_count == alpha_second.pending_count == 3
    assert zeta_first.total_count == zeta_first.pending_count == 2
    assert zeta_second.total_count == zeta_second.pending_count == 2


# ---------------------------------------------------------------------------
# Later as-of boundary: the future arrived
# ---------------------------------------------------------------------------


def test_later_available_through_measures_every_decision(database_path: Path) -> None:
    _freeze_decisions(database_path)

    run = _run(database_path, _SECOND_OBSERVATION, _observations())

    assert all(
        measurement.state is ForwardResearchMeasurementState.MEASURED
        for measurement in run.measurements
    )


def test_later_available_through_reports_measured_counts(database_path: Path) -> None:
    _freeze_decisions(database_path)

    report = _report(database_path, _SECOND_OBSERVATION, _observations())

    assert report.record_count == 5
    assert report.pending_count == 0
    assert [
        (entry.strategy_identity, entry.horizon.observations, entry.measured_count)
        for entry in report.metrics
    ] == [(_ALPHA, 1, 3), (_ALPHA, 2, 3), (_ZETA, 1, 2), (_ZETA, 2, 2)]


def test_measured_returns_follow_the_observed_movement(database_path: Path) -> None:
    _freeze_decisions(database_path)

    report = _report(database_path, _SECOND_OBSERVATION, _observations())

    alpha_first, alpha_second, zeta_first, zeta_second = report.metrics
    assert alpha_first.average_forward_return == Percentage(10)
    assert alpha_second.average_forward_return == Percentage(30)
    assert zeta_first.average_forward_return == Percentage(10)
    assert zeta_second.average_forward_return == Percentage(30)


def test_report_metrics_stay_strategy_major_and_horizon_minor(
    database_path: Path,
) -> None:
    _freeze_decisions(database_path)

    report = _report(database_path, _SECOND_OBSERVATION, _observations())

    assert [
        (entry.strategy_identity.identity, entry.horizon.observations) for entry in report.metrics
    ] == [("alpha", 1), ("alpha", 2), ("zeta", 1), ("zeta", 2)]


def test_measurement_state_transitions_pending_to_measured(
    database_path: Path,
) -> None:
    """The headline forward-testing promise: freeze now, measure later."""
    _freeze_decisions(database_path)

    before = _report(database_path, _NEXT_DAY)
    after = _report(database_path, _SECOND_OBSERVATION, _observations())

    assert before.pending_count == 10
    assert after.pending_count == 0
    assert sum(entry.measured_count for entry in before.metrics) == 0
    assert sum(entry.measured_count for entry in after.metrics) == 10


def test_no_measurement_state_is_persisted_by_the_transition(
    database_path: Path,
) -> None:
    _freeze_decisions(database_path)
    _report(database_path, _NEXT_DAY)
    _report(database_path, _SECOND_OBSERVATION, _observations())

    with sqlite3.connect(database_path) as connection:
        columns = [
            row[1]
            for row in connection.execute("PRAGMA table_info(forward_research_records)").fetchall()
        ]

    assert columns == [
        "symbol",
        "exchange_code",
        "timeframe",
        "decision_instant",
        "strategy_identity",
        "evidence",
    ]


# ---------------------------------------------------------------------------
# Frozen evidence is unaffected by measurement
# ---------------------------------------------------------------------------


def test_frozen_records_are_identical_before_and_after_measurement(
    database_path: Path,
) -> None:
    _freeze_decisions(database_path)

    before = _run(database_path, _NEXT_DAY)
    after = _run(database_path, _SECOND_OBSERVATION, _observations())

    assert before.records == after.records


def test_decision_time_evidence_is_unchanged_by_later_observations(
    database_path: Path,
) -> None:
    _freeze_decisions(database_path)

    before = _run(database_path, _NEXT_DAY)
    after = _run(database_path, _SECOND_OBSERVATION, _observations())

    for original, measured in zip(before.records, after.records, strict=True):
        original_context = original.result.market_observation_context
        measured_context = measured.result.market_observation_context
        assert original_context == measured_context
        assert original_context.latest_price == Price("100", _USD)
        assert measured_context.latest_price == Price("100", _USD)
        assert original.result.recommendation == measured.result.recommendation
        assert original.result.explanation == measured.result.explanation
        assert original.decision_instant.compare(measured.decision_instant) == 0


def test_measured_outcomes_price_from_the_frozen_decision_not_the_observation(
    database_path: Path,
) -> None:
    _freeze_decisions(database_path)

    run = _run(database_path, _SECOND_OBSERVATION, _observations())

    for measurement in run.measurements:
        outcome = measurement.measurement.outcome
        assert outcome is not None
        assert outcome.decision_price == Price("100", _USD)
        assert outcome.evaluation_instant.compare(measurement.record.decision_instant) > 0


# ---------------------------------------------------------------------------
# Reproducibility
# ---------------------------------------------------------------------------


def test_rerunning_at_the_same_available_through_produces_an_equal_report(
    database_path: Path,
) -> None:
    _freeze_decisions(database_path)

    first = _report(database_path, _SECOND_OBSERVATION, _observations())
    second = _report(database_path, _SECOND_OBSERVATION, _observations())

    assert first == second
    assert first.metrics == second.metrics
    assert first.run.records == second.run.records


def test_rerunning_a_pending_report_is_equally_reproducible(
    database_path: Path,
) -> None:
    _freeze_decisions(database_path)

    assert _report(database_path, _NEXT_DAY) == _report(database_path, _NEXT_DAY)


def test_metrics_are_reproducible_from_a_separately_built_run(
    database_path: Path,
) -> None:
    _freeze_decisions(database_path)

    run = _run(database_path, _SECOND_OBSERVATION, _observations())
    metrics = CalculateForwardResearchMetricsUseCase().execute(run)

    assert metrics == _report(database_path, _SECOND_OBSERVATION, _observations()).metrics


def test_freezing_the_same_decisions_again_changes_nothing(
    database_path: Path,
) -> None:
    """Idempotent re-freezing must not alter a reproduced report."""
    _freeze_decisions(database_path)
    before = _report(database_path, _SECOND_OBSERVATION, _observations())

    _freeze_decisions(database_path)
    after = _report(database_path, _SECOND_OBSERVATION, _observations())

    assert before == after
