"""EI-1C-3 acceptance: the V1 research window over the accepted NSE venue reference.

The chain is: production EI-1C-2 catalog -> NSEEquityVenueDayReferenceSource
(Infrastructure) -> EvaluateEquityResearchWindowUseCase (Application) with a
listing applicability source.

Listing applicability here comes only from explicitly synthetic records, or
from the production ``NoApprovedEquityListingApplicabilitySource``, which
answers UNKNOWN. The synthetic records are invented test statements: they are
not NSE security-master, special pre-open or suspension facts. RELIANCE, TCS,
M&M and MM are used only as identities the Core Symbol contract accepts.

An ended interval is a time fact only; nothing here says any candle exists or
is final, or that the market ran without interruption.
"""

from __future__ import annotations

import ast
import dataclasses
import inspect
import socket
from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo

import pytest
from northstar_application.application_services import (
    EquityResearchWindowRejection,
    EvaluateEquityResearchWindowUseCase,
)
from northstar_application.ports import (
    EquityListingApplicabilityClassification,
    EquityVenueDayClassification,
)
from northstar_core.domain.value_objects import ListingReference
from northstar_core.foundation.value_objects import ExchangeCode, PointInTime, Symbol

from northstar_infrastructure import market_data
from northstar_infrastructure.market_data import (
    NO_APPROVED_APPLICABILITY_SOURCE,
    PRODUCTION_NSE_EQUITY_SESSION_CATALOG,
    InvalidSyntheticListingApplicabilityError,
    NoApprovedEquityListingApplicabilitySource,
    NSEEquityContinuousCloseRule,
    NSEEquitySecurityGroup,
    NSEEquitySessionEvidenceError,
    NSEEquitySessionRequiresInstrumentApplicabilityError,
    NSEEquityTradingSessionResolver,
    NSEEquityVenueDayReferenceSource,
    NSEEquityVenueSessionReference,
    SyntheticListingApplicabilityRecord,
    SyntheticListingApplicabilitySource,
)
from northstar_infrastructure.market_data import (
    nse_equity_research_window_reference as venue_adapter_module,
)
from northstar_infrastructure.market_data import (
    synthetic_equity_listing_applicability as synthetic_module,
)

_NSE = ExchangeCode("NSE")
_IST = ZoneInfo("Asia/Kolkata")
_REJECT = EquityResearchWindowRejection
_STATUS = EquityListingApplicabilityClassification
_RELIANCE = ListingReference(Symbol("RELIANCE"), _NSE)
_TCS = ListingReference(Symbol("TCS"), _NSE)
_M_AND_M = ListingReference(Symbol("M&M"), _NSE)
_MM = ListingReference(Symbol("MM"), _NSE)
_RELIANCE_BSE = ListingReference(Symbol("RELIANCE"), ExchangeCode("BSE"))

_PRE_CAS = date(2026, 4, 28)
_POST_CAS = date(2026, 8, 12)
_REALIGNED = date(2026, 9, 7)
_LAST_COVERED = date(2026, 10, 19)
_GAP_DAY = date(2026, 8, 5)
_LATE = PointInTime("2027-01-01T00:00:00Z")


def _eligible(listing: ListingReference, day: date) -> SyntheticListingApplicabilityRecord:
    return SyntheticListingApplicabilityRecord(
        listing, day, _STATUS.ELIGIBLE_FOR_NORMAL_RESEARCH_WINDOW, "synthetic test statement"
    )


_RECORDS = (
    _eligible(_RELIANCE, _PRE_CAS),
    _eligible(_RELIANCE, _POST_CAS),
    _eligible(_RELIANCE, _REALIGNED),
    _eligible(_RELIANCE, _LAST_COVERED),
    _eligible(_RELIANCE, _GAP_DAY),
    _eligible(_RELIANCE, date(2026, 5, 1)),
    _eligible(_RELIANCE, date(2026, 5, 2)),
    _eligible(_RELIANCE, date(2026, 11, 8)),
    _eligible(_RELIANCE, date(2026, 4, 27)),
    _eligible(_RELIANCE, date(2026, 10, 21)),
    _eligible(_M_AND_M, _POST_CAS),
    SyntheticListingApplicabilityRecord(
        _TCS, _POST_CAS, _STATUS.SPECIAL_PRE_OPEN, "synthetic special pre-open day"
    ),
    SyntheticListingApplicabilityRecord(
        _TCS, _PRE_CAS, _STATUS.SUSPENDED_OR_NONCONTINUOUS, "synthetic suspension"
    ),
    SyntheticListingApplicabilityRecord(
        _TCS, _REALIGNED, _STATUS.OTHER_UNSUPPORTED_EXCEPTION, "synthetic other exception"
    ),
)


@pytest.fixture(autouse=True)
def _refuse_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("EI-1C-3 tests must not open network connections.")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)


def _synthetic() -> SyntheticListingApplicabilitySource:
    return SyntheticListingApplicabilitySource("ei1c3-acceptance/1", _RECORDS)


def _use_case(applicability: object = None) -> EvaluateEquityResearchWindowUseCase:
    return EvaluateEquityResearchWindowUseCase(
        NSEEquityVenueDayReferenceSource.production(),
        applicability or _synthetic(),  # type: ignore[arg-type]
    )


def _evaluate(
    day: date, listing: ListingReference = _RELIANCE, as_of: PointInTime = _LATE
) -> object:
    return _use_case().execute(listing, day, as_of)


def _ist(instant: PointInTime) -> time:
    return datetime.fromisoformat(instant.value.replace("Z", "+00:00")).astimezone(_IST).time()


def _at_ist(day: date, clock: time, microseconds: int = 0) -> PointInTime:
    utc = datetime.combine(day, clock, tzinfo=_IST).astimezone(UTC)
    utc -= timedelta(microseconds=microseconds)
    return PointInTime(utc.isoformat().replace("+00:00", "Z"))


# ----------------------------------------------------------------------
# A. Eligible windows from accepted reference facts
# ----------------------------------------------------------------------


@pytest.mark.parametrize("day", [_PRE_CAS, _POST_CAS, _REALIGNED, _LAST_COVERED])
def test_eligible_window_has_69_intervals_from_0920_to_1500_ist(day: date) -> None:
    result = _evaluate(day)
    assert result.is_eligible, result.detail
    assert len(result.intervals) == 69
    assert _ist(result.intervals[0].closes_at) == time(9, 20)
    assert _ist(result.intervals[-1].closes_at) == time(15, 0)
    assert _ist(result.window_opens_at) == time(9, 15)
    assert result.intervals[0].closes_at == _at_ist(day, time(9, 20))
    for interval in result.intervals:
        assert interval.closes_at.value.endswith("Z")
        assert time(9, 15) <= _ist(interval.opens_at) < _ist(interval.closes_at) <= time(15, 0)


def test_pre_cas_window_rests_on_the_universal_accepted_close() -> None:
    venue = NSEEquityVenueDayReferenceSource.production().venue_day(_NSE, _PRE_CAS)
    assert venue.classification is EquityVenueDayClassification.NORMAL_SESSION
    assert (_ist(venue.common_opens_at), _ist(venue.common_closes_at)) == (
        time(9, 15),
        time(15, 30),
    )
    assert venue.utc_offset == timedelta(hours=5, minutes=30)
    assert "CE-F-02" in venue.reference


def test_post_cas_window_uses_the_conservative_15_15_common_close() -> None:
    venue = NSEEquityVenueDayReferenceSource.production().venue_day(_NSE, _POST_CAS)
    assert venue.classification is EquityVenueDayClassification.NORMAL_SESSION
    assert (_ist(venue.common_opens_at), _ist(venue.common_closes_at)) == (
        time(9, 15),
        time(15, 15),
    )
    assert "CE-F-12" in venue.reference and "CE-F-13" in venue.reference


def test_post_cas_full_session_resolution_still_fails_closed() -> None:
    assert _evaluate(_POST_CAS).is_eligible
    with pytest.raises(NSEEquitySessionRequiresInstrumentApplicabilityError):
        NSEEquityTradingSessionResolver.production().resolve(_NSE, _POST_CAS)


def test_cas_status_is_unknown_but_unnecessary_for_the_common_window() -> None:
    # No fixture states any CAS status; the window is still eligible on explicit evidence.
    result = _evaluate(_POST_CAS, _M_AND_M)
    assert result.is_eligible
    assert result.applicability_reference == "synthetic-reference ei1c3-acceptance/1"


def test_eligible_answers_carry_the_evidence_horizon_and_unaccepted_dependency() -> None:
    result = _evaluate(_PRE_CAS)
    assert "coverage through 2026-10-19" in result.venue_reference
    assert result.venue_reference.endswith("not accepted: CE-F-22")
    assert result.unscheduled_interruptions_assessed is False
    assert result.candles_assessed is False


# ----------------------------------------------------------------------
# B. Venue reference rejections
# ----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("day", "rejection"),
    [
        (date(2026, 5, 1), _REJECT.VENUE_CLOSED),
        (date(2026, 5, 2), _REJECT.VENUE_DAY_NOT_STATED),
        (date(2026, 11, 8), _REJECT.SPECIAL_SESSION_UNSUPPORTED),
        (date(2026, 4, 27), _REJECT.OUTSIDE_REFERENCE_COVERAGE),
        (date(2026, 10, 21), _REJECT.OUTSIDE_REFERENCE_COVERAGE),
        (_GAP_DAY, _REJECT.COMMON_BOUNDARY_UNSUPPORTED),
    ],
)
def test_unsupported_venue_days_fail_closed_even_for_an_eligible_listing(
    day: date, rejection: EquityResearchWindowRejection
) -> None:
    result = _evaluate(day)
    assert result.rejection is rejection
    assert result.intervals == ()


@pytest.mark.parametrize("day", [date(2026, 8, d) for d in (3, 4, 5, 6, 7, 10, 11)])
def test_august_3_to_11_has_no_common_boundary_pending_ce_d15(day: date) -> None:
    venue = NSEEquityVenueDayReferenceSource.production().venue_day(_NSE, day)
    assert (
        venue.classification
        is EquityVenueDayClassification.NORMAL_SESSION_COMMON_BOUNDARY_UNSUPPORTED
    )
    assert "CE-D15" in venue.reason
    assert venue.common_closes_at is None


def test_every_date_beyond_the_horizon_is_outside_coverage() -> None:
    source = NSEEquityVenueDayReferenceSource.production()
    for offset in range(1, 120):
        day = _LAST_COVERED + timedelta(days=offset)
        status = source.venue_day(_NSE, day).classification
        assert status in {
            EquityVenueDayClassification.OUTSIDE_COVERAGE,
            EquityVenueDayClassification.SPECIAL_SESSION,
        }


def test_missing_accepted_opening_rule_is_refused_by_the_catalog() -> None:
    with pytest.raises(NSEEquitySessionEvidenceError, match="no stated opening rule"):
        dataclasses.replace(
            PRODUCTION_NSE_EQUITY_SESSION_CATALOG,
            opening_rules=PRODUCTION_NSE_EQUITY_SESSION_CATALOG.opening_rules[:1],
        )


def test_missing_common_close_is_refused_by_the_catalog() -> None:
    catalog = PRODUCTION_NSE_EQUITY_SESSION_CATALOG
    with pytest.raises(NSEEquitySessionEvidenceError, match="incomplete continuous-close"):
        dataclasses.replace(catalog, close_rules=catalog.close_rules[:2])


def test_unaccepted_evidence_cannot_back_a_venue_rule() -> None:
    catalog = PRODUCTION_NSE_EQUITY_SESSION_CATALOG
    rules = (
        catalog.close_rules[0],
        NSEEquityContinuousCloseRule(
            NSEEquitySecurityGroup.CAS_ELIGIBLE,
            date(2026, 8, 3),
            date(2026, 10, 19),
            time(15, 15),
            ("CE-F-12", "CE-F-14"),
        ),
        catalog.close_rules[2],
    )
    with pytest.raises(NSEEquitySessionEvidenceError, match="CE-F-14, which is not accepted"):
        dataclasses.replace(catalog, close_rules=rules)


def test_venue_adapter_never_resolves_a_full_session() -> None:
    tree = ast.parse(inspect.getsource(venue_adapter_module))
    called = {
        node.func.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
    }
    assert "resolve" not in called
    names = {node.id for node in ast.walk(tree) if isinstance(node, ast.Name)}
    imported = {
        alias.name for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)
        for alias in node.names
    }  # fmt: skip
    assert "NSEEquityTradingSessionResolver" not in names | imported


def test_venue_adapter_refuses_a_non_reference() -> None:
    with pytest.raises(TypeError):
        NSEEquityVenueDayReferenceSource(object())  # type: ignore[arg-type]
    assert isinstance(
        NSEEquityVenueDayReferenceSource(NSEEquityVenueSessionReference.production()),
        NSEEquityVenueDayReferenceSource,
    )


# ----------------------------------------------------------------------
# C. Listing applicability
# ----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("listing", "day", "rejection"),
    [
        (_TCS, _POST_CAS, _REJECT.LISTING_SPECIAL_PRE_OPEN),
        (_TCS, _PRE_CAS, _REJECT.LISTING_SUSPENDED_OR_NONCONTINUOUS),
        (_TCS, _REALIGNED, _REJECT.LISTING_OTHER_EXCEPTION),
        (_TCS, _LAST_COVERED, _REJECT.LISTING_APPLICABILITY_UNKNOWN),
        (_MM, _POST_CAS, _REJECT.LISTING_APPLICABILITY_UNKNOWN),
        (_M_AND_M, _PRE_CAS, _REJECT.LISTING_APPLICABILITY_UNKNOWN),
    ],
)
def test_listing_exceptions_and_unknown_status_fail_closed(
    listing: ListingReference, day: date, rejection: EquityResearchWindowRejection
) -> None:
    result = _evaluate(day, listing)
    assert result.rejection is rejection
    assert result.intervals == ()


def test_identities_are_exact_per_symbol_and_exchange() -> None:
    source = _synthetic()
    assert (
        source.applicability(_RELIANCE, _POST_CAS).classification
        is _STATUS.ELIGIBLE_FOR_NORMAL_RESEARCH_WINDOW
    )
    assert source.applicability(_RELIANCE_BSE, _POST_CAS).classification is _STATUS.UNKNOWN
    assert (
        source.applicability(_M_AND_M, _POST_CAS).classification
        is _STATUS.ELIGIBLE_FOR_NORMAL_RESEARCH_WINDOW
    )
    assert source.applicability(_MM, _POST_CAS).classification is _STATUS.UNKNOWN
    assert _evaluate(_POST_CAS, _RELIANCE_BSE).rejection is _REJECT.POLICY_NOT_APPLICABLE


def test_absent_applicability_source_fails_every_real_request_closed() -> None:
    use_case = _use_case(NoApprovedEquityListingApplicabilitySource())
    venue = NSEEquityVenueDayReferenceSource.production()
    for offset in range(0, 200):
        day = date(2026, 4, 1) + timedelta(days=offset)
        result = use_case.execute(_RELIANCE, day, _LATE)
        assert not result.is_eligible
        if venue.venue_day(_NSE, day).classification is EquityVenueDayClassification.NORMAL_SESSION:
            assert result.rejection is _REJECT.LISTING_APPLICABILITY_UNKNOWN
            assert result.applicability_reference == NO_APPROVED_APPLICABILITY_SOURCE


def test_conflicting_or_repeated_synthetic_evidence_is_refused() -> None:
    conflicting = SyntheticListingApplicabilityRecord(
        _RELIANCE, _PRE_CAS, _STATUS.SPECIAL_PRE_OPEN, "synthetic conflict"
    )
    with pytest.raises(InvalidSyntheticListingApplicabilityError, match="conflicting"):
        SyntheticListingApplicabilitySource("t/1", (_RECORDS[0], conflicting))
    with pytest.raises(InvalidSyntheticListingApplicabilityError, match="repeated"):
        SyntheticListingApplicabilitySource("t/1", (_RECORDS[0], _RECORDS[0]))


def test_unknown_is_never_a_synthetic_fixture_and_records_are_validated() -> None:
    with pytest.raises(InvalidSyntheticListingApplicabilityError, match="absence"):
        SyntheticListingApplicabilityRecord(_RELIANCE, _PRE_CAS, _STATUS.UNKNOWN, "x")
    with pytest.raises(InvalidSyntheticListingApplicabilityError):
        SyntheticListingApplicabilityRecord(_RELIANCE, _PRE_CAS, _STATUS.SPECIAL_PRE_OPEN, " ")
    with pytest.raises(InvalidSyntheticListingApplicabilityError):
        SyntheticListingApplicabilitySource("", ())


def test_synthetic_answers_are_labelled_synthetic() -> None:
    answer = _synthetic().applicability(_RELIANCE, _PRE_CAS)
    assert answer.evidence_reference.startswith("synthetic-reference ")


# ----------------------------------------------------------------------
# D. Point in time
# ----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("clock", "microseconds", "ended"),
    [
        (time(9, 0), 0, 0),
        (time(9, 20), 1, 0),
        (time(9, 20), 0, 1),
        (time(12, 0), 0, 33),
        (time(15, 0), 1, 68),
        (time(15, 0), 0, 69),
        (time(15, 30), 0, 69),
    ],
)
def test_ended_intervals_follow_the_supplied_as_of(
    clock: time, microseconds: int, ended: int
) -> None:
    as_of = _at_ist(_POST_CAS, clock, microseconds)
    result = _evaluate(_POST_CAS, as_of=as_of)
    assert len(result.ended_intervals) == ended
    assert result.ended_intervals == result.intervals[:ended]


def test_identical_inputs_produce_identical_results() -> None:
    as_of = _at_ist(_POST_CAS, time(11, 7))
    assert _evaluate(_POST_CAS, as_of=as_of) == _evaluate(_POST_CAS, as_of=as_of)


def test_network_is_refused_in_this_module() -> None:
    with pytest.raises(AssertionError, match="network"):
        socket.create_connection(("example.invalid", 443))


@pytest.mark.parametrize("module", [venue_adapter_module, synthetic_module])
def test_adapters_read_no_clock_provider_or_storage(module: object) -> None:
    tree = ast.parse(inspect.getsource(module))  # type: ignore[arg-type]
    called = {
        node.func.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
    }
    assert not called & {"now", "today", "utcnow", "monotonic", "localtime", "tzset"}
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module)
    for name in imported:
        for forbidden in ("sqlite", "http", "upstox", "yahoo", "databento", "broker"):
            assert forbidden not in name


def test_production_venue_source_does_not_use_synthetic_applicability() -> None:
    tree = ast.parse(inspect.getsource(venue_adapter_module))
    modules = {node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)}
    names = {node.id for node in ast.walk(tree) if isinstance(node, ast.Name)}
    assert not any("synthetic" in (module or "") for module in modules)
    assert not any("Synthetic" in name for name in names)


def test_market_data_package_exports_the_adapters() -> None:
    for name in (
        "NSEEquityVenueDayReferenceSource",
        "NoApprovedEquityListingApplicabilitySource",
        "SyntheticListingApplicabilitySource",
        "SyntheticListingApplicabilityRecord",
    ):
        assert name in market_data.__all__
