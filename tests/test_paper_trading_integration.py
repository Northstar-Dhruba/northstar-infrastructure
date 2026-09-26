"""End-to-end acceptance tests for the paper trading path.

These tests exercise the seam the unit suites cannot reach: real SQLite
persistence driving the real orchestration use case, against recommendations
produced by the real strategy pipeline. Nothing here is fabricated -- every
fill was decided on by evidence the strategy actually interpreted.

The seam matters because orchestration depends on persistence in two ways the
Application suites can only simulate. Retries converge only if the store
accepts an identical fill idempotently, and the portfolio fold refuses to
reorder, so the repository must deliver chronological order that SQL text
ordering would not produce.
"""

from __future__ import annotations

import sqlite3
from decimal import Decimal
from pathlib import Path

import pytest
from northstar_application.application_services import (
    AnalyzeAssetResult,
    AnalyzeMarketObservationContextService,
    BuildPaperPortfolioUseCase,
    BuildPaperTradingReportUseCase,
    CreatePaperExecutionIdentitiesUseCase,
    ExecutionIntentNoIntentReason,
    PaperTradingResult,
    PaperTradingRun,
    RunPaperTradingDecisionUseCase,
)
from northstar_application.ports import PaperFillConflictError, PaperFillQuery
from northstar_core.domain.value_objects import ListingReference
from northstar_core.foundation.value_objects import (
    Currency,
    ExchangeCode,
    PointInTime,
    Price,
    Quantity,
    Symbol,
)
from northstar_core.paper_trading import (
    ExecutionIntent,
    OrderSide,
    PaperPortfolioIdentity,
)
from northstar_core.strategy import (
    AssetAnalysisGenerator,
    MarketObservationContext,
    Strategy,
    StrategyIdentity,
)

from northstar_infrastructure.persistence import (
    SQLitePaperFillRepository,
    SQLitePaperFillStore,
)

_USD = Currency("USD")
_NASDAQ = ExchangeCode("NASDAQ")
_AAPL = ListingReference(Symbol("AAPL"), _NASDAQ)
_MSFT = ListingReference(Symbol("MSFT"), _NASDAQ)
_ALPHA = StrategyIdentity("alpha")
_PORTFOLIO = PaperPortfolioIdentity("paper-1")


@pytest.fixture
def database_path(tmp_path: Path) -> Path:
    return tmp_path / "paper_trading.sqlite3"


def _instant(day: int, fraction: str = "") -> PointInTime:
    return PointInTime(f"2026-01-{day:02d}T16:00:00{fraction}Z")


# ---------------------------------------------------------------------------
# Real strategy evidence at controlled prices
# ---------------------------------------------------------------------------


def _analyse(context: MarketObservationContext) -> AnalyzeAssetResult:
    return AnalyzeMarketObservationContextService(
        strategy=Strategy(_ALPHA),
        analysis_generator=AssetAnalysisGenerator(),
    ).execute(context)


def _context(
    closes: list[int],
    latest: str,
    previous: str,
    instant: PointInTime,
    listing: ListingReference,
    volume: str,
) -> MarketObservationContext:
    return MarketObservationContext(
        listing,
        instant,
        Price(latest, _USD),
        Price(previous, _USD),
        Quantity(volume),
        Price("9000", _USD),
        Price("1", _USD),
        tuple(Price(str(close), _USD) for close in closes),
        tuple(Quantity("1000") for _ in range(20)),
    )


def _buy(
    instant: PointInTime, price: str = "250", listing: ListingReference = _AAPL
) -> AnalyzeAssetResult:
    """A rising series closing at ``price``: the real strategy answers BUY."""
    base = int(price) - 20
    result = _analyse(
        _context(list(range(base, base + 20)), price, str(int(price) - 1), instant, listing, "5000")
    )
    assert result.recommendation.action.value == "BUY"
    return result


def _sell(
    instant: PointInTime, price: str = "150", listing: ListingReference = _AAPL
) -> AnalyzeAssetResult:
    """A declining series closing at ``price``: the real strategy answers SELL."""
    result = _analyse(_context(list(range(200, 180, -1)), price, "181", instant, listing, "5000"))
    assert result.recommendation.action.value == "SELL"
    return result


def _hold(instant: PointInTime, listing: ListingReference = _AAPL) -> AnalyzeAssetResult:
    """A flat series on thin volume: the real strategy answers HOLD."""
    result = _analyse(_context([100] * 20, "100", "100", instant, listing, "100"))
    assert result.recommendation.action.value == "HOLD"
    return result


# ---------------------------------------------------------------------------
# Real pipeline
# ---------------------------------------------------------------------------


def _pipeline(database_path: Path) -> RunPaperTradingDecisionUseCase:
    """Orchestration wired to the real SQLite adapters."""
    return RunPaperTradingDecisionUseCase(
        SQLitePaperFillRepository(database_path),
        SQLitePaperFillStore(database_path),
    )


def _run(database_path: Path, result: AnalyzeAssetResult, quantity: str) -> PaperTradingResult:
    return _pipeline(database_path).execute(result, _PORTFOLIO, Quantity(quantity))


def _reopen(database_path: Path):
    """Read the stored history back through a brand new repository."""
    return SQLitePaperFillRepository(database_path).get_fills(PaperFillQuery(_PORTFOLIO))


def _rebuild(database_path: Path, as_of: PointInTime):
    """Fold the persisted history independently of any orchestration run."""
    return BuildPaperPortfolioUseCase().execute(_PORTFOLIO, _reopen(database_path), as_of)


def _rows(database_path: Path) -> list[tuple]:
    with sqlite3.connect(database_path) as connection:
        return connection.execute(
            "SELECT fill_identity, order_identity, quantity, price_amount, filled_at "
            "FROM paper_fills ORDER BY fill_identity"
        ).fetchall()


# ---------------------------------------------------------------------------
# 1. BUY from an empty portfolio
# ---------------------------------------------------------------------------


def test_a_buy_from_an_empty_portfolio_persists_and_reopens(database_path: Path) -> None:
    outcome = _run(database_path, _buy(_instant(20), price="250"), "10")

    assert outcome.execution is not None
    assert outcome.execution.fill.price == Price("250", _USD)
    assert outcome.execution.fill.filled_at == _instant(20)

    reopened = _reopen(database_path)
    assert reopened == (outcome.execution.fill,)

    rebuilt = _rebuild(database_path, _instant(20))
    assert rebuilt.get_position(_AAPL) == outcome.portfolio.get_position(_AAPL)
    assert rebuilt.get_position(_AAPL).quantity == Quantity("10")
    assert rebuilt.get_position(_AAPL).average_price == Price("250", _USD)


def test_a_hold_persists_nothing(database_path: Path) -> None:
    outcome = _run(database_path, _hold(_instant(20)), "10")

    assert outcome.execution is None
    assert _reopen(database_path) == ()
    assert _rebuild(database_path, _instant(20)).positions == ()


def test_an_insufficient_sell_persists_nothing(database_path: Path) -> None:
    outcome = _run(database_path, _sell(_instant(20)), "10")

    assert outcome.decision.no_intent_reason is ExecutionIntentNoIntentReason.INSUFFICIENT_POSITION
    assert _reopen(database_path) == ()


# ---------------------------------------------------------------------------
# 2. Identical BUY retry
# ---------------------------------------------------------------------------


def test_an_identical_buy_retry_is_idempotent(database_path: Path) -> None:
    source = _buy(_instant(20), price="250")

    first = _run(database_path, source, "10")
    second = _run(database_path, source, "10")

    assert first.execution.order.identity == second.execution.order.identity
    assert first.execution.fill.identity == second.execution.fill.identity
    assert first == second
    assert len(_reopen(database_path)) == 1
    assert _rebuild(database_path, _instant(20)).get_position(_AAPL).quantity == Quantity("10")


def test_a_buy_retry_never_overwrites_the_stored_evidence(database_path: Path) -> None:
    source = _buy(_instant(20), price="250")
    _run(database_path, source, "10")
    before = _rows(database_path)

    _run(database_path, source, "10")

    assert _rows(database_path) == before


# ---------------------------------------------------------------------------
# 3. Partial SELL retry
# ---------------------------------------------------------------------------


def test_a_partial_sell_reduces_the_position_and_retries_idempotently(
    database_path: Path,
) -> None:
    _run(database_path, _buy(_instant(20), price="250"), "10")
    source = _sell(_instant(21))

    first = _run(database_path, source, "4")
    assert first.execution.fill.side is OrderSide.SELL
    assert first.portfolio.get_position(_AAPL).quantity == Quantity("6")

    second = _run(database_path, source, "4")

    assert first == second
    assert len(_reopen(database_path)) == 2
    rebuilt = _rebuild(database_path, _instant(21)).get_position(_AAPL)
    assert rebuilt.quantity == Quantity("6")
    assert rebuilt.average_price == Price("250", _USD)


# ---------------------------------------------------------------------------
# 4. Full SELL retry
# ---------------------------------------------------------------------------


def test_a_full_sell_removes_the_position_and_retries_idempotently(
    database_path: Path,
) -> None:
    """The decisive retry: the persisted SELL must not block its own repeat."""
    _run(database_path, _buy(_instant(20), price="250"), "10")
    source = _sell(_instant(21))

    first = _run(database_path, source, "10")
    assert first.portfolio.positions == ()

    second = _run(database_path, source, "10")

    assert second.decision.has_intent
    assert second.execution is not None
    assert first == second
    assert len(_reopen(database_path)) == 2
    assert _rebuild(database_path, _instant(21)).positions == ()


def test_a_full_sell_retry_never_duplicates_the_fill(database_path: Path) -> None:
    _run(database_path, _buy(_instant(20), price="250"), "10")
    source = _sell(_instant(21))
    _run(database_path, source, "10")
    before = _rows(database_path)

    _run(database_path, source, "10")

    assert _rows(database_path) == before
    assert len(before) == 2


# ---------------------------------------------------------------------------
# 5. Changed quantity at one decision boundary
# ---------------------------------------------------------------------------


def test_a_changed_quantity_conflicts_and_preserves_the_original(
    database_path: Path,
) -> None:
    source = _buy(_instant(20), price="250")
    original = _run(database_path, source, "10")
    before = _rows(database_path)

    with pytest.raises(PaperFillConflictError):
        _run(database_path, source, "11")

    assert _rows(database_path) == before
    assert _reopen(database_path) == (original.execution.fill,)
    assert _rebuild(database_path, _instant(20)).get_position(_AAPL).quantity == Quantity("10")


def test_a_conflicting_retry_derives_the_same_identities(database_path: Path) -> None:
    """Identity is a function of the decision, which is why the retry collides."""
    source = _buy(_instant(20), price="250")
    original = _run(database_path, source, "10")
    intent = original.decision.intent
    resized = CreatePaperExecutionIdentitiesUseCase().execute(
        ExecutionIntent(
            portfolio_identity=intent.portfolio_identity,
            listing_reference=intent.listing_reference,
            side=intent.side,
            quantity=Quantity("11"),
            strategy_identity=intent.strategy_identity,
            decided_at=intent.decided_at,
        )
    )

    assert resized.fill_identity == original.execution.fill.identity
    assert resized.order_identity == original.execution.order.identity


def test_a_changed_action_conflicts_and_preserves_the_original(
    database_path: Path,
) -> None:
    buy_source = _buy(_instant(20), price="250")
    _run(database_path, buy_source, "10")
    before = _rows(database_path)

    with pytest.raises(PaperFillConflictError):
        _run(database_path, _sell(_instant(20)), "5")

    assert _rows(database_path) == before


# ---------------------------------------------------------------------------
# 6. Weighted average across differing prices
# ---------------------------------------------------------------------------


def test_two_buys_at_different_prices_fold_to_the_weighted_average(
    database_path: Path,
) -> None:
    """30 @ 100 then 10 @ 300 must fold to 40 @ 150 exactly."""
    _run(database_path, _buy(_instant(20), price="100"), "30")
    outcome = _run(database_path, _buy(_instant(21), price="300"), "10")

    position = outcome.portfolio.get_position(_AAPL)
    assert position.quantity == Quantity("40")
    assert position.average_price == Price("150", _USD)

    rebuilt = _rebuild(database_path, _instant(21)).get_position(_AAPL)
    assert rebuilt.quantity == Quantity("40")
    assert rebuilt.average_price == Price("150", _USD)


def test_a_recurring_weighted_average_keeps_full_precision(database_path: Path) -> None:
    _run(database_path, _buy(_instant(20), price="100"), "1")
    _run(database_path, _buy(_instant(21), price="200"), "1")
    outcome = _run(database_path, _buy(_instant(22), price="700"), "1")

    assert outcome.portfolio.get_position(_AAPL).average_price.amount == Decimal(
        "333.3333333333333333333333333"
    )
    assert _rebuild(database_path, _instant(22)).get_position(
        _AAPL
    ).average_price.amount == Decimal("333.3333333333333333333333333")


def test_a_partial_sell_after_two_buys_preserves_the_weighted_cost(
    database_path: Path,
) -> None:
    _run(database_path, _buy(_instant(20), price="100"), "30")
    _run(database_path, _buy(_instant(21), price="300"), "10")

    outcome = _run(database_path, _sell(_instant(22)), "15")

    position = outcome.portfolio.get_position(_AAPL)
    assert position.quantity == Quantity("25")
    assert position.average_price == Price("150", _USD)


# ---------------------------------------------------------------------------
# 7. Temporal correctness
# ---------------------------------------------------------------------------


def test_sub_second_history_feeds_orchestration_without_an_ordering_violation(
    database_path: Path,
) -> None:
    """A third decision loads two fills whose text order is not chronological."""
    whole = _instant(20)
    fractional = _instant(20, ".1")
    _run(database_path, _buy(whole, price="100"), "10")
    _run(database_path, _buy(fractional, price="200"), "10")

    outcome = _run(database_path, _buy(_instant(21), price="300"), "10")

    assert fractional.value < whole.value
    assert fractional.compare(whole) > 0
    assert outcome.portfolio.get_position(_AAPL).quantity == Quantity("30")
    assert outcome.portfolio.get_position(_AAPL).average_price == Price("200", _USD)


def test_repository_order_is_chronological_not_textual(database_path: Path) -> None:
    _run(database_path, _buy(_instant(20), price="100"), "10")
    _run(database_path, _buy(_instant(20, ".1"), price="200"), "10")

    with sqlite3.connect(database_path) as connection:
        text_ordered = [
            row[0]
            for row in connection.execute(
                "SELECT filled_at FROM paper_fills ORDER BY filled_at ASC"
            )
        ]

    retrieved = [fill.filled_at.value for fill in _reopen(database_path)]

    assert text_ordered == [_instant(20, ".1").value, _instant(20).value]
    assert retrieved == [_instant(20).value, _instant(20, ".1").value]


def test_an_earlier_as_of_rebuilds_an_earlier_portfolio(database_path: Path) -> None:
    _run(database_path, _buy(_instant(20), price="100"), "10")
    _run(database_path, _buy(_instant(21), price="300"), "10")

    assert _rebuild(database_path, _instant(20)).get_position(_AAPL).quantity == Quantity("10")
    assert _rebuild(database_path, _instant(21)).get_position(_AAPL).quantity == Quantity("20")


def test_an_offset_equivalent_retry_converges(database_path: Path) -> None:
    _run(database_path, _buy(_instant(20), price="250"), "10")
    offset = PointInTime("2026-01-20T21:30:00+05:30")

    retry = _run(database_path, _buy(offset, price="250"), "10")

    assert offset.compare(_instant(20)) == 0
    assert retry.execution is not None
    assert len(_reopen(database_path)) == 1


# ---------------------------------------------------------------------------
# 8. Reporting seam
# ---------------------------------------------------------------------------


def test_a_report_counts_sqlite_backed_decisions_correctly(database_path: Path) -> None:
    results = (
        _run(database_path, _buy(_instant(20), price="100"), "10"),
        _run(database_path, _hold(_instant(21)), "10"),
        _run(database_path, _sell(_instant(22)), "4"),
        _run(database_path, _sell(_instant(23)), "999"),
    )

    report = BuildPaperTradingReportUseCase().execute(PaperTradingRun(_PORTFOLIO, results))

    assert report.decision_count == 4
    assert report.execution_count == 2
    only = report.metrics[0]
    assert (only.buy_recommendations, only.hold_recommendations, only.sell_recommendations) == (
        1,
        1,
        2,
    )
    assert (only.execution_count, only.buy_execution_count, only.sell_execution_count) == (
        2,
        1,
        1,
    )
    assert only.hold_no_intent_count == 1
    assert only.insufficient_position_count == 1


def test_a_report_keeps_listings_separated_across_sqlite_backed_results(
    database_path: Path,
) -> None:
    results = (
        _run(database_path, _buy(_instant(20), price="100"), "10"),
        _run(database_path, _buy(_instant(20), price="300", listing=_MSFT), "5"),
    )

    report = BuildPaperTradingReportUseCase().execute(PaperTradingRun(_PORTFOLIO, results))

    assert [entry.listing_reference for entry in report.metrics] == [_AAPL, _MSFT]
    assert report.execution_count == 2
    assert report.final_portfolio.position_count == 2


def test_the_report_final_portfolio_matches_the_refolded_history(
    database_path: Path,
) -> None:
    results = (
        _run(database_path, _buy(_instant(20), price="100"), "30"),
        _run(database_path, _buy(_instant(21), price="300"), "10"),
    )

    report = BuildPaperTradingReportUseCase().execute(PaperTradingRun(_PORTFOLIO, results))

    assert report.final_portfolio == _rebuild(database_path, _instant(21))


# ---------------------------------------------------------------------------
# Persistence guarantees
# ---------------------------------------------------------------------------


def test_only_fills_are_persisted(database_path: Path) -> None:
    _run(database_path, _buy(_instant(20), price="250"), "10")
    _run(database_path, _sell(_instant(21)), "4")

    with sqlite3.connect(database_path) as connection:
        tables = {
            row[0]
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }

    assert tables == {"paper_fills"}


def test_reopening_preserves_exact_fill_evidence(database_path: Path) -> None:
    first = _run(database_path, _buy(_instant(20), price="100"), "3.25")
    second = _run(database_path, _sell(_instant(21)), "1.5")

    reopened = _reopen(database_path)

    assert reopened == (first.execution.fill, second.execution.fill)
    assert reopened[0].quantity == Quantity("3.25")
    assert reopened[0].intent == first.decision.intent
    assert reopened[1].quantity == Quantity("1.5")
    assert reopened[1].side is OrderSide.SELL
