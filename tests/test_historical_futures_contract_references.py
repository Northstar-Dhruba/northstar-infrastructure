"""R-1B-1: historical futures contract references, effective-dated sizes and cutoffs.

Every contract fact here is SYNTHETIC: listing dates, last sessions, tick and
lot sizes and citations are test labels, not statements about real NIFTY
contracts. Only the NSE calendar itself is the existing loaded reference.
"""

from __future__ import annotations

import ast
import dataclasses
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from northstar_application.application_services import (
    EvaluateFuturesRecommendationPolicyUseCase,
    FuturesEvaluationEvidence,
    FuturesEvaluationEvidenceClassification,
    FuturesHistoricalDatasetBarReference,
    FuturesHistoricalDatasetCompleteness,
    FuturesHistoricalDatasetCoverage,
    FuturesHistoricalDatasetManifest,
    FuturesHistoricalDatasetSource,
    FuturesHistoricalDatasetSourceCategory,
    FuturesHistoricalDatasetVolumeUnit,
    FuturesRecommendationEvaluationEvidenceError,
    FuturesRecommendationEvaluationRequest,
    FuturesTradeRecommendationGenerationProfile,
)
from northstar_application.ports import (
    FuturesContractCutoff,
    FuturesContractCutoffSource,
    FuturesHistoricalMarketDataRepository,
    FuturesSessionResolutionError,
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
from northstar_core.recommendations.futures import (
    FuturesRecommendationGenerationPolicy,
    RecommendationPolicyVersion,
)
from northstar_core.strategy import (
    FuturesAssetAnalysis,
    FuturesAssetAnalysisGenerator,
    Strategy,
    StrategyIdentity,
)

import northstar_infrastructure.market_data as market_data
from northstar_infrastructure.market_data import (
    SYNTHETIC_REFERENCE_AUTHORITY_PREFIX,
    HistoricalFuturesCalendarStatus,
    HistoricalFuturesContractCutoffSource,
    HistoricalFuturesContractReference,
    HistoricalFuturesContractReferenceCatalog,
    HistoricalFuturesContractReferenceError,
    HistoricalFuturesLotSize,
    HistoricalFuturesReferenceKind,
    HistoricalFuturesReferenceSource,
    HistoricalFuturesReferenceUnavailableError,
    HistoricalFuturesTickSize,
    NSEFuturesTradingSessionResolver,
    check_historical_futures_reference_calendar,
)
from northstar_infrastructure.market_data import nse_futures_calendar_reference as calendar

_NIFTY = FuturesProductReference(Symbol("NIFTY"), ExchangeCode("NSE"))
_BANKNIFTY = FuturesProductReference(Symbol("BANKNIFTY"), ExchangeCode("NSE"))
_OCT = FuturesContract(_NIFTY, ExpirationDate("2026-10-27"))
_NOV = FuturesContract(_NIFTY, ExpirationDate("2026-11-24"))
_JAN27 = FuturesContract(_NIFTY, ExpirationDate("2027-01-26"))
_LISTED = date(2026, 9, 1)
_SYNTHETIC = HistoricalFuturesReferenceKind.SYNTHETIC
_NSE = NSEFuturesTradingSessionResolver()
Ref = HistoricalFuturesContractReference
Catalog = HistoricalFuturesContractReferenceCatalog
Tick = HistoricalFuturesTickSize
Lot = HistoricalFuturesLotSize


def _src(citation: str = "synthetic-fixture/1") -> HistoricalFuturesReferenceSource:
    return HistoricalFuturesReferenceSource(_SYNTHETIC, citation)


def _tick(start: date = _LISTED, end: date | None = None, value: str = "0.05") -> Tick:
    return Tick(start, end, Decimal(value), _src())


def _lot(start: date = _LISTED, end: date | None = None, value: int = 50) -> Lot:
    return Lot(start, end, value, _src())


def _ref(contract: FuturesContract = _OCT, **changes: object) -> Ref:
    fields: dict[str, object] = {
        "contract": contract,
        "listing_date": _LISTED,
        "listing_source": _src(),
        "last_tradable_session": date(2026, 10, 23),
        "last_session_source": _src("synthetic-last-session/1"),
        "tick_sizes": (_tick(),),
        "lot_sizes": (_lot(),),
    }
    fields.update(changes)
    return Ref(**fields)  # type: ignore[arg-type]


def _catalog(*references: Ref, version: str = "synthetic-nifty-contracts/1") -> Catalog:
    return Catalog(version, _NIFTY, references or (_ref(),))


def _source(*references: Ref) -> HistoricalFuturesContractCutoffSource:
    return HistoricalFuturesContractCutoffSource(_catalog(*references), _NSE)


# ---------------------------------------------------------------------------
# Reference models
# ---------------------------------------------------------------------------


def test_a_valid_synthetic_reference() -> None:
    reference = _ref()
    assert reference.contract == _OCT
    assert reference.listing_date == _LISTED
    assert reference.last_tradable_session == date(2026, 10, 23)
    assert reference.kinds == frozenset({_SYNTHETIC})
    assert reference.tick_size_gaps == ()
    assert reference.lot_size_gaps == ()


def test_the_last_session_may_be_the_expiration_date_itself() -> None:
    assert _ref(last_tradable_session=date(2026, 10, 27)).last_tradable_session == date(
        2026, 10, 27
    )


def test_listing_after_expiration_is_refused() -> None:
    with pytest.raises(HistoricalFuturesContractReferenceError, match="listed after"):
        _ref(listing_date=date(2026, 10, 28), last_tradable_session=date(2026, 10, 28))


def test_last_session_before_listing_is_refused() -> None:
    with pytest.raises(HistoricalFuturesContractReferenceError, match="precedes its listing"):
        _ref(listing_date=date(2026, 10, 1), last_tradable_session=date(2026, 9, 30))


def test_last_session_after_expiration_is_refused_without_exception() -> None:
    with pytest.raises(HistoricalFuturesContractReferenceError, match="after its expiration"):
        _ref(last_tradable_session=date(2026, 10, 28))


@pytest.mark.parametrize(
    "changes",
    [
        {"listing_date": "2026-09-01"},
        {"last_tradable_session": None},
        {"listing_source": "NSE circular"},
        {"last_session_source": None},
        {"contract": "NIFTY-2026-10-27"},
        {"tick_sizes": [_tick()]},
        {"lot_sizes": (_tick(),)},
    ],
)
def test_invalid_reference_fields_are_refused(changes: dict[str, object]) -> None:
    with pytest.raises(HistoricalFuturesContractReferenceError):
        _ref(**changes)


@pytest.mark.parametrize(
    "citation", ["", " ", " padded", "trailing ", "tab\tinside", "caf" + chr(0xE9), 5]
)
def test_invalid_source_citations_are_refused(citation: object) -> None:
    with pytest.raises(HistoricalFuturesContractReferenceError, match="cite"):
        HistoricalFuturesReferenceSource(_SYNTHETIC, citation)  # type: ignore[arg-type]


def test_source_kind_must_be_the_enum() -> None:
    with pytest.raises(HistoricalFuturesContractReferenceError, match="kind"):
        HistoricalFuturesReferenceSource("SYNTHETIC", "x")  # type: ignore[arg-type]


def test_cross_product_contract_is_refused_by_the_catalog() -> None:
    bank = FuturesContract(_BANKNIFTY, ExpirationDate("2026-10-27"))
    with pytest.raises(HistoricalFuturesContractReferenceError, match="not a contract of"):
        _catalog(_ref(), _ref(bank))


def test_duplicate_contract_reference_is_refused() -> None:
    with pytest.raises(HistoricalFuturesContractReferenceError, match="repeated"):
        _catalog(_ref(), _ref())


@pytest.mark.parametrize(
    "conflict",
    [
        {"listing_date": date(2026, 9, 2)},
        {"last_tradable_session": date(2026, 10, 26)},
        {"tick_sizes": (_tick(value="0.10"),)},
        {"lot_sizes": (_lot(value=75),)},
        {"last_session_source": _src("another-citation")},
    ],
)
def test_conflicting_contract_facts_are_refused_not_ordered_away(
    conflict: dict[str, object],
) -> None:
    for pair in ((_ref(), _ref(**conflict)), (_ref(**conflict), _ref())):
        with pytest.raises(HistoricalFuturesContractReferenceError, match="conflicting"):
            _catalog(*pair)


def test_catalog_is_ordered_and_exact() -> None:
    nov = _ref(_NOV, last_tradable_session=date(2026, 11, 23))
    catalog = _catalog(nov, _ref())
    assert [r.contract for r in catalog.references] == [_OCT, _NOV]
    assert catalog == _catalog(_ref(), nov)
    assert catalog.reference_for(_NOV) is nov
    assert catalog.reference_for(_JAN27) is None
    assert catalog.is_synthetic is True


@pytest.mark.parametrize("version", ["", " v1", "has space", "../escape", 7])
def test_catalog_needs_a_version_name(version: object) -> None:
    with pytest.raises(HistoricalFuturesContractReferenceError, match="version"):
        Catalog(version, _NIFTY, (_ref(),))  # type: ignore[arg-type]


def test_catalog_entries_must_be_a_tuple_of_references() -> None:
    with pytest.raises(HistoricalFuturesContractReferenceError):
        Catalog("v1", _NIFTY, [_ref()])  # type: ignore[arg-type]
    with pytest.raises(HistoricalFuturesContractReferenceError):
        Catalog("v1", "NIFTY", (_ref(),))  # type: ignore[arg-type]


def test_references_are_immutable() -> None:
    reference = _ref()
    with pytest.raises(dataclasses.FrozenInstanceError):
        reference.last_tradable_session = date(2026, 10, 27)  # type: ignore[misc]
    with pytest.raises(dataclasses.FrozenInstanceError):
        reference.last_session_source.kind = "VERIFIED"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Effective-dated values
# ---------------------------------------------------------------------------


def test_tick_and_lot_lookup_inside_a_period() -> None:
    reference = _ref()
    assert reference.tick_size_on(date(2026, 10, 1)) == Decimal("0.05")
    assert reference.lot_size_on(date(2026, 10, 1)) == 50


def test_exact_decimal_is_preserved() -> None:
    value = Decimal("0.0500")
    reference = _ref(tick_sizes=(Tick(_LISTED, None, value, _src()),))
    looked_up = reference.tick_size_on(_LISTED)
    assert looked_up == value
    assert str(looked_up) == "0.0500"
    assert isinstance(looked_up, Decimal)


@pytest.mark.parametrize(
    "value", [Decimal("0"), Decimal("-0.05"), Decimal("NaN"), Decimal("Infinity"), 0.05, "0.05", 1]
)
def test_invalid_tick_sizes_are_refused(value: object) -> None:
    with pytest.raises(HistoricalFuturesContractReferenceError, match="Tick size"):
        Tick(_LISTED, None, value, _src())  # type: ignore[arg-type]


@pytest.mark.parametrize("value", [0, -50, 50.0, Decimal("50"), True, "50"])
def test_invalid_lot_sizes_are_refused(value: object) -> None:
    with pytest.raises(HistoricalFuturesContractReferenceError, match="Lot size"):
        Lot(_LISTED, None, value, _src())  # type: ignore[arg-type]


def test_period_end_before_start_and_missing_source_are_refused() -> None:
    with pytest.raises(HistoricalFuturesContractReferenceError, match="ends before"):
        _lot(date(2026, 10, 2), date(2026, 10, 1))
    with pytest.raises(HistoricalFuturesContractReferenceError, match="must name its"):
        Lot(_LISTED, None, 50, "synthetic")  # type: ignore[arg-type]


def test_adjacent_periods_are_valid_and_resolve_at_the_boundary() -> None:
    reference = _ref(
        lot_sizes=(_lot(date(2026, 10, 1), None, 65), _lot(_LISTED, date(2026, 9, 30), 75))
    )
    assert [p.value for p in reference.lot_sizes] == [75, 65]
    assert reference.lot_size_on(date(2026, 9, 30)) == 75
    assert reference.lot_size_on(date(2026, 10, 1)) == 65
    assert reference.lot_size_gaps == ()


@pytest.mark.parametrize(
    "periods",
    [
        (_lot(_LISTED, date(2026, 10, 1), 75), _lot(date(2026, 10, 1), None, 65)),
        (_lot(_LISTED, None, 75), _lot(date(2026, 10, 5), None, 65)),
        (_lot(_LISTED, None, 50), _lot(_LISTED, None, 50)),
        (_lot(_LISTED, date(2026, 10, 9), 75), _lot(date(2026, 9, 15), date(2026, 9, 20), 75)),
    ],
)
def test_overlapping_or_contradictory_periods_are_refused(periods: tuple[Lot, ...]) -> None:
    with pytest.raises(HistoricalFuturesContractReferenceError, match="overlap"):
        _ref(lot_sizes=periods)


def test_a_period_outside_the_contract_life_is_refused() -> None:
    with pytest.raises(HistoricalFuturesContractReferenceError, match="outside its life"):
        _ref(tick_sizes=(_tick(), _tick(date(2026, 10, 24), None)))


def test_uncovered_dates_are_unavailable_and_reported() -> None:
    reference = _ref(
        tick_sizes=(
            _tick(date(2026, 9, 10), date(2026, 9, 30)),
            _tick(date(2026, 10, 5), date(2026, 10, 20), "0.10"),
        ),
        lot_sizes=(),
    )
    assert reference.tick_size_gaps == (
        (date(2026, 9, 1), date(2026, 9, 9)),
        (date(2026, 10, 1), date(2026, 10, 4)),
        (date(2026, 10, 21), date(2026, 10, 23)),
    )
    assert reference.lot_size_gaps == ((_LISTED, date(2026, 10, 23)),)
    for day in (date(2026, 9, 9), date(2026, 10, 2), date(2026, 10, 21)):
        with pytest.raises(HistoricalFuturesReferenceUnavailableError, match="not established"):
            reference.tick_size_on(day)
    with pytest.raises(HistoricalFuturesReferenceUnavailableError):
        reference.lot_size_on(date(2026, 10, 1))


def test_no_value_before_listing_or_after_the_last_session() -> None:
    reference = _ref(tick_sizes=(_tick(date(2026, 1, 1), None),))
    with pytest.raises(HistoricalFuturesReferenceUnavailableError, match="outside the contract"):
        reference.tick_size_on(date(2026, 8, 31))
    # An open-ended period never reaches past the last tradable session.
    with pytest.raises(HistoricalFuturesReferenceUnavailableError, match="outside the contract"):
        reference.tick_size_on(date(2026, 10, 26))


def test_no_latest_value_fallback() -> None:
    reference = _ref(lot_sizes=(_lot(_LISTED, date(2026, 9, 30), 75),))
    assert reference.lot_size_on(date(2026, 9, 30)) == 75
    with pytest.raises(HistoricalFuturesReferenceUnavailableError):
        reference.lot_size_on(date(2026, 10, 1))


def test_as_of_lookup_is_deterministic_and_order_independent() -> None:
    periods = (_lot(date(2026, 10, 1), None, 65), _lot(_LISTED, date(2026, 9, 30), 75))
    first, second = _ref(lot_sizes=periods), _ref(lot_sizes=tuple(reversed(periods)))
    assert first == second
    days = [_LISTED + timedelta(days=n) for n in range(53)]
    assert [first.lot_size_on(d) for d in days] == [second.lot_size_on(d) for d in days]


def test_lookup_needs_a_plain_date() -> None:
    from datetime import datetime

    with pytest.raises(HistoricalFuturesContractReferenceError):
        _ref().tick_size_on(datetime(2026, 10, 1))  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Cutoff source
# ---------------------------------------------------------------------------


def test_the_cutoff_source_implements_the_application_port() -> None:
    assert issubclass(HistoricalFuturesContractCutoffSource, FuturesContractCutoffSource)


def test_exact_contract_cutoff_is_returned() -> None:
    cutoff = _source().resolve(_OCT)
    assert isinstance(cutoff, FuturesContractCutoff)
    assert cutoff.contract == _OCT
    assert cutoff.last_session == _NSE.resolve(_NIFTY, date(2026, 10, 23))
    assert cutoff.last_session.closes_at == PointInTime("2026-10-23T10:10:00Z")


def test_the_expiration_date_is_never_used_as_the_cutoff() -> None:
    cutoff = _source().resolve(_OCT)
    assert cutoff is not None
    assert cutoff.last_session.trading_date == date(2026, 10, 23)
    assert cutoff.last_session.trading_date.isoformat() != _OCT.expiration_date.value


def test_a_missing_contract_returns_none_and_no_neighbour() -> None:
    source = _source(_ref())
    assert source.resolve(_NOV) is None
    assert source.resolve(FuturesContract(_BANKNIFTY, ExpirationDate("2026-10-27"))) is None


def test_each_contract_gets_its_own_cutoff() -> None:
    source = _source(_ref(), _ref(_NOV, last_tradable_session=date(2026, 11, 23)))
    assert source.resolve(_OCT).last_session.trading_date == date(2026, 10, 23)  # type: ignore[union-attr]
    assert source.resolve(_NOV).last_session.trading_date == date(2026, 11, 23)  # type: ignore[union-attr]


def test_a_last_session_on_a_holiday_is_refused() -> None:
    assert date(2026, 11, 24) in calendar.HOLIDAYS
    source = _source(_ref(_NOV, last_tradable_session=date(2026, 11, 24)))
    with pytest.raises(HistoricalFuturesContractReferenceError, match="is not a trading session"):
        source.resolve(_NOV)


def test_a_last_session_on_a_weekend_is_refused() -> None:
    source = _source(_ref(last_tradable_session=date(2026, 10, 25)))
    with pytest.raises(HistoricalFuturesContractReferenceError, match="is not a trading session"):
        source.resolve(_OCT)


def test_an_unloaded_calendar_year_fails_closed_with_a_diagnostic() -> None:
    source = _source(
        _ref(_JAN27, listing_date=date(2026, 11, 2), last_tradable_session=date(2027, 1, 25))
    )
    with pytest.raises(HistoricalFuturesContractReferenceError, match="2027 is not loaded"):
        source.resolve(_JAN27)


def test_an_unnotified_special_session_fails_closed() -> None:
    nov8 = FuturesContract(_NIFTY, ExpirationDate("2026-11-08"))
    source = _source(_ref(nov8, last_tradable_session=date(2026, 11, 8)))
    with pytest.raises(HistoricalFuturesContractReferenceError, match="no notified timings"):
        source.resolve(nov8)


def test_synthetic_provenance_is_carried_and_distinguishable() -> None:
    cutoff = _source().resolve(_OCT)
    assert cutoff is not None
    assert cutoff.authority == (
        "synthetic-reference synthetic-nifty-contracts/1: synthetic-last-session/1"
    )
    assert cutoff.authority.startswith(SYNTHETIC_REFERENCE_AUTHORITY_PREFIX)


def test_an_official_looking_citation_does_not_make_a_source_authoritative() -> None:
    reference = _ref(last_session_source=_src("NSE/FAOP/99999 exchange-verified"))
    cutoff = _source(reference).resolve(_OCT)
    assert cutoff is not None
    assert cutoff.authority.startswith("synthetic-reference ")
    assert not cutoff.authority.startswith("NSE")
    assert [kind.value for kind in HistoricalFuturesReferenceKind] == ["SYNTHETIC"]
    with pytest.raises(ValueError):
        HistoricalFuturesReferenceKind("VERIFIED")


def test_the_cutoff_source_refuses_wrong_inputs() -> None:
    with pytest.raises(HistoricalFuturesContractReferenceError):
        HistoricalFuturesContractCutoffSource((_ref(),), _NSE)  # type: ignore[arg-type]
    with pytest.raises(HistoricalFuturesContractReferenceError):
        HistoricalFuturesContractCutoffSource(_catalog(), object())  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        _source().resolve("NIFTY 2026-10-27")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Calendar
# ---------------------------------------------------------------------------


def test_a_reference_on_loaded_sessions_is_consistent() -> None:
    check = check_historical_futures_reference_calendar(_ref(), _NSE)
    assert check.listing.status is HistoricalFuturesCalendarStatus.SESSION
    assert check.last_session.status is HistoricalFuturesCalendarStatus.SESSION
    assert check.consistent is True


def test_a_holiday_listing_is_reported_not_a_session() -> None:
    reference = _ref(listing_date=date(2026, 10, 2))
    assert date(2026, 10, 2) in calendar.HOLIDAYS
    check = check_historical_futures_reference_calendar(reference, _NSE)
    assert check.listing.status is HistoricalFuturesCalendarStatus.NOT_A_SESSION
    assert check.consistent is False


def test_an_unloaded_year_listing_stays_unresolved_with_a_diagnostic() -> None:
    reference = _ref(
        _JAN27, listing_date=date(2023, 10, 27), last_tradable_session=date(2027, 1, 25)
    )
    check = check_historical_futures_reference_calendar(reference, _NSE)
    assert check.listing.status is HistoricalFuturesCalendarStatus.UNRESOLVED
    assert "2023 is not loaded" in (check.listing.diagnostic or "")
    assert check.last_session.status is HistoricalFuturesCalendarStatus.UNRESOLVED
    assert check.consistent is False


def test_an_unloaded_years_weekday_is_never_assumed_a_session() -> None:
    weekday = date(2023, 10, 25)
    assert weekday.weekday() < 5
    reference = _ref(_JAN27, listing_date=weekday, last_tradable_session=date(2026, 12, 29))
    check = check_historical_futures_reference_calendar(reference, _NSE)
    assert check.listing.status is HistoricalFuturesCalendarStatus.UNRESOLVED
    with pytest.raises(FuturesSessionResolutionError):
        _NSE.resolve(_NIFTY, weekday)


def test_existing_special_sessions_are_unchanged() -> None:
    muhurat = FuturesContract(_NIFTY, ExpirationDate("2024-11-28"))
    reference = _ref(
        muhurat,
        listing_date=date(2024, 11, 1),
        last_tradable_session=date(2024, 11, 28),
        tick_sizes=(),
        lot_sizes=(),
    )
    check = check_historical_futures_reference_calendar(reference, _NSE)
    assert check.listing.status is HistoricalFuturesCalendarStatus.SESSION  # Muhurat 2024
    unnotified = _ref(
        FuturesContract(_NIFTY, ExpirationDate("2026-11-30")),
        listing_date=date(2026, 11, 8),
        last_tradable_session=date(2026, 11, 30),
        tick_sizes=(),
        lot_sizes=(),
    )
    finding = check_historical_futures_reference_calendar(unnotified, _NSE).listing
    assert finding.status is HistoricalFuturesCalendarStatus.UNRESOLVED
    assert calendar.LOADED_YEARS == frozenset({2024, 2025, 2026})


def test_calendar_check_refuses_wrong_inputs() -> None:
    with pytest.raises(HistoricalFuturesContractReferenceError):
        check_historical_futures_reference_calendar("ref", _NSE)  # type: ignore[arg-type]
    with pytest.raises(HistoricalFuturesContractReferenceError):
        check_historical_futures_reference_calendar(_ref(), "calendar")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Integration with R-0 and R-1A
# ---------------------------------------------------------------------------

_STRATEGY = StrategyIdentity("synthetic-directional-v1")
_DAILY = Timeframe("1d")
_DECISION = date(2026, 10, 7)
_CLOSES = (Decimal("23900"), Decimal("23950"), Decimal("23925"), Decimal("23975"),
           Decimal("23990")) + tuple(Decimal(24000 + 20 * s) for s in range(19)) + (
           Decimal("24383"),)  # fmt: skip


class _Market(FuturesHistoricalMarketDataRepository):
    def __init__(self, bars: list[FuturesOHLCVBar]) -> None:
        self.bars = bars

    def get_bars(self, query):  # type: ignore[no-untyped-def]
        return tuple(
            b for b in self.bars if b.contract == query.contract and query.covers(b.point_in_time)
        )


class _Buy(FuturesAssetAnalysisGenerator):
    def generate(self, context):  # type: ignore[no-untyped-def]
        decision = context.observed_at.value.startswith(_DECISION.isoformat())
        return FuturesAssetAnalysis(
            context.contract,
            context.observed_at,
            ("strong bullish" if decision else "neutral trend",),
        )


def _history() -> list[FuturesOHLCVBar]:
    sessions = _NSE.sessions_in_range(_NIFTY, date(2026, 8, 10), _DECISION)[-len(_CLOSES) :]
    return [
        FuturesOHLCVBar(
            contract=_OCT,
            point_in_time=s.closes_at,
            timeframe=_DAILY,
            open=QuoteValue(c),
            high=QuoteValue(c + 5),
            low=QuoteValue(c - 5),
            close=QuoteValue(c),
            volume=Quantity(Decimal("1000")),
        )
        for s, c in zip(sessions, _CLOSES, strict=True)
    ]


def _evaluate(cutoffs: FuturesContractCutoffSource):  # type: ignore[no-untyped-def]
    policy = FuturesRecommendationGenerationPolicy(
        version=RecommendationPolicyVersion("f4-synthetic-evaluation-v1"),
        approved_strategies=(_STRATEGY,),
        lookback_changes=19,
        entry_multiplier=Decimal("0.25"),
        stop_multiplier=Decimal("1.5"),
        target_multiplier=Decimal("3"),
        price_increment=Decimal("0.05"),
        min_reward_to_risk=Decimal("1.5"),
        short_enabled=True,
    )
    close = _NSE.resolve(_NIFTY, _DECISION).closes_at  # type: ignore[union-attr]
    return EvaluateFuturesRecommendationPolicyUseCase(
        _Market(_history()), _NSE, cutoffs, _Buy()
    ).execute(
        FuturesRecommendationEvaluationRequest(
            product=_NIFTY,
            contracts=(_OCT,),
            strategy=Strategy(_STRATEGY),
            policy=policy,
            profile=FuturesTradeRecommendationGenerationProfile(
                "r1b1-synthetic-v1", 5, policy.version
            ),
            start=close,
            end=close,
            evidence=FuturesEvaluationEvidence(
                FuturesEvaluationEvidenceClassification.SYNTHETIC, "r1b1-synthetic-fixture"
            ),
        )
    )


def test_r0_uses_the_adapter_in_a_synthetic_context() -> None:
    result = _evaluate(_source())
    (record,) = result.recommendations
    assert record.recommendation.contract == _OCT
    assert record.recommendation.validity.contract_cutoff == PointInTime("2026-10-23T10:10:00Z")


def test_r0_fails_closed_without_a_reference() -> None:
    nov_only = _source(_ref(_NOV, last_tradable_session=date(2026, 11, 23)))
    with pytest.raises(FuturesRecommendationEvaluationEvidenceError):
        _evaluate(nov_only)


def test_r0_fails_closed_on_an_unverifiable_last_session() -> None:
    with pytest.raises(HistoricalFuturesContractReferenceError, match="not a trading session"):
        _evaluate(_source(_ref(last_tradable_session=date(2026, 10, 25))))


def test_r0_evidence_classification_is_unchanged() -> None:
    assert [c.value for c in FuturesEvaluationEvidenceClassification] == ["SYNTHETIC"]


def test_a_manifest_citing_the_catalog_is_still_not_market_evidence() -> None:
    catalog = _catalog()
    bar = _history()[-1]
    manifest = FuturesHistoricalDatasetManifest(
        dataset_id="r1b1-synthetic",
        source=FuturesHistoricalDatasetSource(
            FuturesHistoricalDatasetSourceCategory.SYNTHETIC,
            "fixture",
            None,
            PointInTime("2026-10-16T00:00:00Z"),
            None,
        ),
        product=_NIFTY,
        contracts=(_OCT,),
        timeframe=_DAILY,
        source_volume_unit=FuturesHistoricalDatasetVolumeUnit.CONTRACTS,
        normalization_version="n/1",
        coverage=FuturesHistoricalDatasetCoverage(
            _DECISION, _DECISION, "nse_futures_calendar_reference", catalog.version, None
        ),
        bars=(FuturesHistoricalDatasetBarReference.from_bar(_OCT, _DECISION, bar),),
    )
    assert manifest.completeness is FuturesHistoricalDatasetCompleteness.UNVERIFIED
    assert manifest.is_market_evidence is False
    assert manifest.source.rights_verified is False


def test_the_module_has_no_persistence_provider_or_network_dependency() -> None:
    path = Path(market_data.__file__).parent / "historical_futures_contract_reference.py"
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported = {
        node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.module
    } | {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    for name in imported:
        assert not name.startswith(
            ("sqlite3", "databento", "exchange_calendars", "requests", "httpx", "urllib", "socket",
             "northstar_api", "northstar_infrastructure.persistence", "time", "random", "os")
        ), name  # fmt: skip
    assert "upstox" not in source.lower()
    assert "CREATE TABLE" not in source
    called = {
        node.func.attr if isinstance(node.func, ast.Attribute) else getattr(node.func, "id", "")
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
    }
    assert not called & {"now", "today", "utcnow", "open", "connect", "weekday"}


def test_public_exports() -> None:
    for name in (
        "HistoricalFuturesContractReference",
        "HistoricalFuturesContractReferenceCatalog",
        "HistoricalFuturesContractCutoffSource",
        "HistoricalFuturesTickSize",
        "HistoricalFuturesLotSize",
        "HistoricalFuturesReferenceSource",
        "HistoricalFuturesReferenceKind",
        "HistoricalFuturesCalendarStatus",
        "HistoricalFuturesReferenceUnavailableError",
        "HistoricalFuturesContractReferenceError",
        "check_historical_futures_reference_calendar",
        "SYNTHETIC_REFERENCE_AUTHORITY_PREFIX",
    ):
        assert name in market_data.__all__
        assert getattr(market_data, name) is not None
