"""EI-1C-2: evidence-gated NSE cash-equity session reference.

The production catalog is checked against the owner-approved register
(``northstar-docs`` ``research/NSE-Cash-Equity-Session-Evidence-Register.md``,
section 18). Mechanism tests derive variants of it with
``dataclasses.replace``; any catalog that is not the production object is
labelled test evidence. Facts with IDs from CE-F-900 upwards are invented test
evidence and are never NSE facts.
"""

from __future__ import annotations

import ast
import dataclasses
import inspect
import re
import socket
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
from northstar_application.application_services import EquitySessionIntervalGrid
from northstar_application.ports import (
    EquitySessionKind,
    EquitySessionResolutionError,
    EquityTradingSession,
    EquityTradingSessionResolver,
)
from northstar_core.foundation.value_objects import ExchangeCode, PointInTime, Timeframe

from northstar_infrastructure import market_data
from northstar_infrastructure.market_data import (
    PRODUCTION_ACCEPTED_EQUITY_SESSION_EVIDENCE,
    PRODUCTION_EQUITY_SESSION_AUTHORITY_PREFIX,
    PRODUCTION_NSE_EQUITY_SESSION_CATALOG,
    TEST_EQUITY_SESSION_AUTHORITY_PREFIX,
    NSEEquityAcceptedSessionEvidence,
    NSEEquityAcceptedSessionFact,
    NSEEquityClosedDate,
    NSEEquityClosureKind,
    NSEEquityContinuousCloseRule,
    NSEEquityOpeningRule,
    NSEEquityOwnerAcceptance,
    NSEEquitySecurityGroup,
    NSEEquitySessionCatalog,
    NSEEquitySessionEvidenceError,
    NSEEquitySessionRequiresInstrumentApplicabilityError,
    NSEEquitySessionUnavailableError,
    NSEEquitySourceIdentity,
    NSEEquitySourceRole,
    NSEEquityTradingSessionResolver,
    NSEEquityUnsupportedClose,
    NSEEquityVenueDay,
    NSEEquityVenueDayStatus,
    NSEEquityVenueSessionReference,
)
from northstar_infrastructure.market_data import (
    nse_accepted_equity_session_evidence as production_module,
)
from northstar_infrastructure.market_data import nse_equity_session_reference as reference_module

_NSE = ExchangeCode("NSE")
_IST = ZoneInfo("Asia/Kolkata")
_ALL = NSEEquitySecurityGroup.ALL_NORMAL_MARKET
_CAS = NSEEquitySecurityGroup.CAS_ELIGIBLE
_NON_CAS = NSEEquitySecurityGroup.NON_CAS
_STATUS = NSEEquityVenueDayStatus

_ACCEPTED = frozenset(
    {
        "CE-F-01", "CE-F-02", "CE-F-03", "CE-F-04", "CE-F-10", "CE-F-12", "CE-F-13",
        "CE-F-15", "CE-F-16", "CE-F-17", "CE-F-18", "CE-F-19", "CE-F-20", "CE-F-21",
        "CE-F-30", "CE-F-31", "CE-F-50", "CE-F-51", "CE-F-52", "CE-F-54", "CE-F-55",
    }
)  # fmt: skip
_DISCOVERED = frozenset(
    {"CE-F-11", "CE-F-14", "CE-F-22", "CE-F-32", "CE-F-40", "CE-F-41", "CE-F-42", "CE-F-53"}
)
_REGISTER = (
    Path(__file__).resolve().parents[2]
    / "northstar-docs"
    / "research"
    / "NSE-Cash-Equity-Session-Evidence-Register.md"
)

_PROD = PRODUCTION_NSE_EQUITY_SESSION_CATALOG
_REGIME_A_DAY = date(2026, 4, 28)
_LAST_PRE_CAS_DAY = date(2026, 7, 31)
_CAS_LIVE = date(2026, 8, 3)
_NON_CAS_FROM = date(2026, 8, 12)
_PRE_OPEN_REALIGNED = date(2026, 9, 7)
_COVERAGE_THROUGH = date(2026, 10, 19)


@pytest.fixture(autouse=True)
def _refuse_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("EI-1C-2 tests must not open network connections.")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)


def _lookup(trading_date: date, catalog: NSEEquitySessionCatalog = _PROD) -> NSEEquityVenueDay:
    return NSEEquityVenueSessionReference(catalog).lookup(_NSE, trading_date)


def _resolve(
    trading_date: date, catalog: NSEEquitySessionCatalog = _PROD
) -> EquityTradingSession | None:
    return NSEEquityTradingSessionResolver(catalog).resolve(_NSE, trading_date)


def _ist(instant: PointInTime) -> time:
    utc = datetime.fromisoformat(instant.value.replace("Z", "+00:00"))
    return utc.astimezone(_IST).time()


def _days(first: date, last: date) -> list[date]:
    return [first + timedelta(days=offset) for offset in range((last - first).days + 1)]


def _test_fact(fact_id: str = "CE-F-901", **overrides: object) -> NSEEquityAcceptedSessionFact:
    values: dict[str, object] = {
        "fact_id": fact_id,
        "summary": "Invented test evidence; not an NSE fact.",
        "groups": frozenset({_ALL}),
        "effective_from": date(2031, 1, 1),
        "effective_through": None,
        "sources": (
            NSEEquitySourceIdentity(
                "CE-S-901", "TEST ONLY document", "0" * 64, NSEEquitySourceRole.ESTABLISHES
            ),
        ),
        "limitations": (),
        "acceptance": NSEEquityOwnerAcceptance(date(2031, 1, 1), "test only", "test only"),
    }
    values.update(overrides)
    return NSEEquityAcceptedSessionFact(**values)  # type: ignore[arg-type]


# ----------------------------------------------------------------------
# A. Accepted evidence
# ----------------------------------------------------------------------


def test_production_registry_holds_exactly_the_21_accepted_facts() -> None:
    assert PRODUCTION_ACCEPTED_EQUITY_SESSION_EVIDENCE.fact_ids == _ACCEPTED
    assert production_module.ACCEPTED_FACT_IDS == _ACCEPTED
    assert production_module.DISCOVERED_FACT_IDS == _DISCOVERED


def test_no_discovered_fact_is_loaded_or_cited() -> None:
    assert not PRODUCTION_ACCEPTED_EQUITY_SESSION_EVIDENCE.fact_ids & _DISCOVERED
    cited = set(_PROD.normal_session_fact_ids)
    for record in (*_PROD.opening_rules, *_PROD.close_rules, *_PROD.closed_dates):
        cited.update(record.fact_ids)
    for special in _PROD.special_sessions:
        cited.update(special.fact_ids)
    cited.update(exception.fact_id for exception in _PROD.exceptions)
    assert cited <= _ACCEPTED
    assert not cited & _DISCOVERED


def test_every_accepted_fact_has_an_establishing_fingerprinted_source_and_owner_acceptance() -> (
    None
):
    for fact in PRODUCTION_ACCEPTED_EQUITY_SESSION_EVIDENCE.facts:
        assert any(source.role is NSEEquitySourceRole.ESTABLISHES for source in fact.sources)
        for source in fact.sources:
            assert re.fullmatch(r"[0-9a-f]{64}", source.sha256)
        assert fact.acceptance.accepted_on == date(2026, 11, 4)
        assert "section 18.3" in fact.acceptance.record


def test_faq_annexures_only_ever_corroborate() -> None:
    for fact in PRODUCTION_ACCEPTED_EQUITY_SESSION_EVIDENCE.facts:
        for source in fact.sources:
            if source.source_id in {"CE-S-13B", "CE-S-14A", "CE-S-20D"}:
                assert source.role is NSEEquitySourceRole.CORROBORATES


def test_accepted_boundaries_are_preserved() -> None:
    fact = PRODUCTION_ACCEPTED_EQUITY_SESSION_EVIDENCE.fact
    assert (fact("CE-F-01").effective_from, fact("CE-F-01").effective_through) == (
        date(2026, 4, 28),
        None,
    )
    assert fact("CE-F-02").effective_through == date(2026, 8, 2)
    assert fact("CE-F-03").effective_through == date(2026, 9, 6)
    assert fact("CE-F-04").effective_from == date(2026, 9, 7)
    assert fact("CE-F-12").effective_from == date(2026, 8, 3)
    assert fact("CE-F-13").effective_from == date(2026, 8, 12)
    assert fact("CE-F-51").effective_through == date(2026, 8, 2)
    assert fact("CE-F-55").effective_from == date(2026, 9, 7)


# Copied from register section 18.6. Pinned here so CI, which has no
# northstar-docs checkout, still checks every source identity and fingerprint.
_SOURCE_FINGERPRINTS = {
    "CE-S-03": "11bae310d99b7659a823aa6fae0f5d8cfc838f2f2ca337091e44e4077ba39eb5",
    "CE-S-11": "c7da4fb3a74954903d9ac2106e16f22679654405774df7abe602f369fd1e8377",
    "CE-S-12": "aef6e11c636ce079ec105307a55772a79d3c1be3218147f8b3fd97970312e65d",
    "CE-S-12A": "da1dad1077b0fe70c14a068a1c4f6a665a80f35af81b6d454ba7271d2969efae",
    "CE-S-13": "50778523b491d582a7b367c7c7d64aabaefe0bc43eefba16b89364295c71fdbe",
    "CE-S-13B": "06648b68cfe39f0ec2348e9124833963a7970dbd7000e6a0347edac34308b45f",
    "CE-S-14": "b6bfea1a5acad4a33019ff722ae04d280cf8af11dfa98e423f75d870969ffebc",
    "CE-S-14A": "c3750bc4e69cd5436803982351afb90a8428afe8c0e1df21829e2b6b2a7b97c6",
    "CE-S-15": "17e415f619be6a132f86a4edbc127412f44e90f87e838151d86484e2204022c1",
    "CE-S-17": "aa97e0afc0ce394097f2fc62631c68e3c2e4c7c23541ed35f21d9fc06b0dcacb",
    "CE-S-18": "c5d32a838b46d5044717b830a4793479a51a40f8e4e19d3896c65682d15b6ebc",
    "CE-S-19": "ce94ee1c54d98eccdf3c30fa0673ae2b6a49ea4ed4a72fb83d826f4359f71d58",
    "CE-S-20": "f5e04e3cc7fa33a995ddc2603b62fce3b2e1e4f230848d211567b855db5ff89e",
    "CE-S-20D": "30a4e37956ba5ab1acdda177411beca0146ebab0bcb155f1cd4f12576d195798",
}


def test_source_identities_and_fingerprints_are_pinned_without_the_docs_repository() -> None:
    used = {
        source.source_id: source.sha256
        for fact in PRODUCTION_ACCEPTED_EQUITY_SESSION_EVIDENCE.facts
        for source in fact.sources
    }
    assert used == _SOURCE_FINGERPRINTS


def test_each_accepted_fact_cites_its_pinned_establishing_sources() -> None:
    establishing = {
        fact.fact_id: sorted(
            s.source_id for s in fact.sources if s.role is NSEEquitySourceRole.ESTABLISHES
        )
        for fact in PRODUCTION_ACCEPTED_EQUITY_SESSION_EVIDENCE.facts
    }
    assert establishing == {
        "CE-F-01": ["CE-S-14", "CE-S-20"],
        "CE-F-02": ["CE-S-20"],
        "CE-F-03": ["CE-S-20"],
        "CE-F-04": ["CE-S-14"],
        "CE-F-10": ["CE-S-15"],
        "CE-F-12": ["CE-S-13"],
        "CE-F-13": ["CE-S-03"],
        "CE-F-15": ["CE-S-03", "CE-S-11", "CE-S-13"],
        "CE-F-16": ["CE-S-03", "CE-S-13"],
        "CE-F-17": ["CE-S-03", "CE-S-19"],
        "CE-F-18": ["CE-S-13"],
        "CE-F-19": ["CE-S-15"],
        "CE-F-20": ["CE-S-17"],
        "CE-F-21": ["CE-S-18"],
        "CE-F-30": ["CE-S-19"],
        "CE-F-31": ["CE-S-19"],
        "CE-F-50": ["CE-S-12", "CE-S-12A", "CE-S-13"],
        "CE-F-51": ["CE-S-12A"],
        "CE-F-52": ["CE-S-13"],
        "CE-F-54": ["CE-S-20"],
        "CE-F-55": ["CE-S-14"],
    }


def test_production_module_refuses_a_registry_that_drifts_from_the_accepted_set() -> None:
    registry = production_module.PRODUCTION_ACCEPTED_EQUITY_SESSION_EVIDENCE
    assert registry.fact_ids == production_module.ACCEPTED_FACT_IDS
    assert _PROD.unresolved_fact_ids == ("CE-F-22",)
    assert set(_PROD.unresolved_fact_ids) <= _DISCOVERED


@pytest.mark.skipif(not _REGISTER.is_file(), reason="northstar-docs is not checked out beside")
def test_production_evidence_matches_the_owner_approval_record_in_the_register() -> None:
    text = _REGISTER.read_text(encoding="utf-8")
    approval = text.split("## 18. Owner approval record", 1)[1]
    accepted_table = approval.split("### 18.3", 1)[1].split("### 18.4", 1)[0]
    accepted = set(re.findall(r"^\| (CE-F-\d+) \|", accepted_table, flags=re.MULTILINE))
    assert accepted == _ACCEPTED

    fingerprints_table = approval.split("### 18.6", 1)[1].split("### 18.7", 1)[0]
    register_hashes = dict(
        re.findall(r"^\| (CE-S-\w+) \|[^|]*\| `([0-9a-f]{64})`", fingerprints_table, re.MULTILINE)
    )
    used = {
        source.source_id: source.sha256
        for fact in PRODUCTION_ACCEPTED_EQUITY_SESSION_EVIDENCE.facts
        for source in fact.sources
    }
    assert used == register_hashes


def test_a_catalog_citing_an_unaccepted_fact_is_refused() -> None:
    rules = (
        NSEEquityContinuousCloseRule(_ALL, _REGIME_A_DAY, date(2026, 8, 2), time(15, 30),
                                     ("CE-F-02", "CE-F-14")),
        *_PROD.close_rules[1:],
    )  # fmt: skip
    with pytest.raises(NSEEquitySessionEvidenceError, match="CE-F-14, which is not accepted"):
        dataclasses.replace(_PROD, close_rules=rules)


def test_a_registry_without_the_cited_fact_rejects_the_catalog() -> None:
    reduced = NSEEquityAcceptedSessionEvidence(
        "test-registry/1",
        tuple(
            f for f in PRODUCTION_ACCEPTED_EQUITY_SESSION_EVIDENCE.facts if f.fact_id != "CE-F-13"
        ),
    )
    with pytest.raises(NSEEquitySessionEvidenceError, match="CE-F-13"):
        dataclasses.replace(_PROD, evidence=reduced)


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"sources": ()}, "no source identity"),
        (
            {
                "sources": (
                    NSEEquitySourceIdentity(
                        "CE-S-902", "TEST ONLY FAQ", "1" * 64, NSEEquitySourceRole.CORROBORATES
                    ),
                )
            },
            "no establishing source",
        ),
        ({"fact_id": "NSE-F-01"}, "Fact ID"),
        ({"effective_through": date(2030, 12, 31)}, "inverted"),
    ],
)
def test_facts_without_valid_source_identity_are_refused(
    overrides: dict[str, object], message: str
) -> None:
    with pytest.raises(NSEEquitySessionEvidenceError, match=message):
        _test_fact(**overrides)


@pytest.mark.parametrize(
    ("args", "message"),
    [
        (("CE-S-901", "TEST ONLY", "not-a-hash", NSEEquitySourceRole.ESTABLISHES), "SHA-256"),
        (("", "TEST ONLY", "0" * 64, NSEEquitySourceRole.ESTABLISHES), "Source ID"),
        (("CE-S-901", "", "0" * 64, NSEEquitySourceRole.ESTABLISHES), "document"),
        (("CE-S-901", "TEST ONLY", "0" * 64, "ESTABLISHES"), "role"),
    ],
)
def test_missing_or_malformed_source_identities_are_refused(
    args: tuple[object, ...], message: str
) -> None:
    with pytest.raises(NSEEquitySessionEvidenceError, match=message):
        NSEEquitySourceIdentity(*args)  # type: ignore[arg-type]


def test_registry_refuses_repeated_facts_and_conflicting_sources() -> None:
    fact = _test_fact()
    with pytest.raises(NSEEquitySessionEvidenceError, match="repeated"):
        NSEEquityAcceptedSessionEvidence("test-registry/1", (fact, fact))
    conflicting = _test_fact(
        "CE-F-902",
        sources=(
            NSEEquitySourceIdentity(
                "CE-S-901", "TEST ONLY other document", "0" * 64, NSEEquitySourceRole.ESTABLISHES
            ),
        ),
    )
    with pytest.raises(NSEEquitySessionEvidenceError, match="two different documents"):
        NSEEquityAcceptedSessionEvidence("test-registry/1", (fact, conflicting))


# ----------------------------------------------------------------------
# B. Versioning and authority
# ----------------------------------------------------------------------


def test_production_answers_carry_the_production_authority_and_versions() -> None:
    session = _resolve(_REGIME_A_DAY)
    assert session is not None
    assert session.reference.startswith(PRODUCTION_EQUITY_SESSION_AUTHORITY_PREFIX + " ")
    assert "nse-equity-session/1" in session.reference
    assert "nse-accepted-equity-session-evidence/1" in session.reference
    assert NSEEquityTradingSessionResolver.production().venue_reference.catalog is _PROD
    assert NSEEquityVenueSessionReference.production().catalog is _PROD


def test_any_other_catalog_is_labelled_test_evidence_even_with_production_facts() -> None:
    copy = dataclasses.replace(_PROD, version="ei1c2-test/2")
    assert not copy.is_production
    session = _resolve(_REGIME_A_DAY, copy)
    assert session is not None
    assert session.reference.startswith(TEST_EQUITY_SESSION_AUTHORITY_PREFIX + " ei1c2-test/2")
    assert session.reference != _resolve(_REGIME_A_DAY).reference  # type: ignore[union-attr]


def test_session_reference_names_the_facts_it_rests_on() -> None:
    assert _lookup(_REGIME_A_DAY).fact_ids == ("CE-F-01", "CE-F-02", "CE-F-03", "CE-F-20",
                                                "CE-F-21")  # fmt: skip
    assert "CE-F-04" in _lookup(_PRE_OPEN_REALIGNED).fact_ids
    assert "CE-F-03" not in _lookup(_PRE_OPEN_REALIGNED).fact_ids


# ----------------------------------------------------------------------
# C. Regime A: pre-CAS venue sessions
# ----------------------------------------------------------------------


def test_resolver_implements_the_unchanged_ei1a_port() -> None:
    assert isinstance(NSEEquityTradingSessionResolver.production(), EquityTradingSessionResolver)


def test_pre_cas_normal_session_resolves_0915_to_1530_ist_in_utc() -> None:
    session = _resolve(_REGIME_A_DAY)
    assert session is not None
    assert session.kind is EquitySessionKind.NORMAL
    assert session.exchange_code == _NSE
    assert session.trading_date == _REGIME_A_DAY
    assert [(p.opens_at.value, p.closes_at.value) for p in session.phases] == [
        ("2026-04-28T03:45:00Z", "2026-04-28T10:00:00Z")
    ]
    assert (_ist(session.opens_at), _ist(session.closes_at)) == (time(9, 15), time(15, 30))


def test_utc_conversion_uses_the_named_zone() -> None:
    session = _resolve(_LAST_PRE_CAS_DAY)
    assert session is not None
    expected = datetime.combine(_LAST_PRE_CAS_DAY, time(9, 15), tzinfo=_IST).astimezone(UTC)
    assert session.opens_at.value == expected.strftime("%Y-%m-%dT%H:%M:%SZ")
    assert _PROD.timezone == "Asia/Kolkata"


def test_every_pre_cas_weekday_is_a_resolved_session_or_a_stated_holiday() -> None:
    for day in _days(_REGIME_A_DAY, date(2026, 8, 2)):
        if day.weekday() >= 5:
            continue
        session = _resolve(day)
        if session is None:
            assert _lookup(day).status is _STATUS.CLOSED
        else:
            assert (_ist(session.opens_at), _ist(session.closes_at)) == (time(9, 15), time(15, 30))


def test_resolved_session_is_compatible_with_the_ei1a_interval_grid() -> None:
    session = _resolve(_REGIME_A_DAY)
    assert session is not None
    grid = EquitySessionIntervalGrid(session, Timeframe("5m"))
    assert len(grid.intervals) == 75
    assert grid.intervals[0].closes_at == PointInTime("2026-04-28T03:50:00Z")
    assert grid.intervals[-1].closes_at == PointInTime("2026-04-28T10:00:00Z")


def test_normal_session_count_in_coverage_is_pinned() -> None:
    assert len(_PROD.normal_sessions) == 120
    assert min(_PROD.normal_sessions) == _REGIME_A_DAY
    assert max(_PROD.normal_sessions) == _COVERAGE_THROUGH


# ----------------------------------------------------------------------
# D. Regime B: conditional CAS cutoff
# ----------------------------------------------------------------------


def test_no_universal_session_is_resolved_on_any_date_after_cas_introduction() -> None:
    for day in _days(_CAS_LIVE, _COVERAGE_THROUGH):
        try:
            session = _resolve(day)
        except EquitySessionResolutionError:
            continue
        assert session is None, f"{day} resolved a venue-wide session after CAS go-live"


def test_post_cas_normal_day_raises_for_instrument_applicability_with_the_venue_day() -> None:
    with pytest.raises(NSEEquitySessionRequiresInstrumentApplicabilityError) as caught:
        _resolve(_NON_CAS_FROM)
    venue_day = caught.value.venue_day
    assert venue_day is not None
    assert venue_day.status is _STATUS.NORMAL_SESSION
    assert venue_day.is_close_group_dependent


def test_post_cas_lookup_states_a_close_per_group() -> None:
    day = _lookup(_NON_CAS_FROM)
    assert _ist(day.opens_at) == time(9, 15)  # type: ignore[arg-type]
    cas = day.continuous_close_for(_CAS)
    non_cas = day.continuous_close_for(_NON_CAS)
    assert cas.closes_at == PointInTime("2026-08-12T09:45:00Z")
    assert _ist(cas.closes_at) == time(15, 15)
    assert cas.fact_ids == ("CE-F-10", "CE-F-12")
    assert non_cas.closes_at == PointInTime("2026-08-12T10:00:00Z")
    assert non_cas.fact_ids == ("CE-F-13",)
    with pytest.raises(NSEEquitySessionUnavailableError):
        day.continuous_close_for(_ALL)


def test_pre_cas_lookup_has_one_universal_close_and_no_cas_close() -> None:
    day = _lookup(_LAST_PRE_CAS_DAY)
    assert [close.group for close in day.continuous_closes] == [_ALL]
    assert not day.is_close_group_dependent
    with pytest.raises(NSEEquitySessionUnavailableError):
        day.continuous_close_for(_CAS)


@pytest.mark.parametrize("day", [d for d in _days(_CAS_LIVE, date(2026, 8, 11)) if d.weekday() < 5])
def test_august_3_to_11_non_cas_close_is_explicitly_unsupported(day: date) -> None:
    venue_day = _lookup(day)
    assert venue_day.status is _STATUS.NORMAL_SESSION
    non_cas = venue_day.continuous_close_for(_NON_CAS)
    assert not non_cas.is_supported
    assert non_cas.closes_at is None
    assert non_cas.pending_decisions == ("CE-D15",)
    assert "CE-D15" in non_cas.unsupported_reason  # type: ignore[operator]
    assert venue_day.continuous_close_for(_CAS).is_supported
    assert "CE-F-13" not in venue_day.fact_ids
    assert venue_day.pending_decisions == ("CE-D15",)


def test_the_non_cas_gap_ends_exactly_at_ce_f_13_accepted_coverage() -> None:
    assert not _lookup(date(2026, 8, 11)).continuous_close_for(_NON_CAS).is_supported
    assert _lookup(date(2026, 8, 12)).continuous_close_for(_NON_CAS).is_supported


def test_filling_the_august_gap_with_ce_f_13_is_refused() -> None:
    rules = (
        _PROD.close_rules[0],
        _PROD.close_rules[1],
        NSEEquityContinuousCloseRule(_NON_CAS, _CAS_LIVE, _COVERAGE_THROUGH, time(15, 30),
                                     ("CE-F-13",)),
    )  # fmt: skip
    with pytest.raises(NSEEquitySessionEvidenceError, match="beyond the accepted boundaries"):
        dataclasses.replace(_PROD, close_rules=rules, unsupported_closes=())


def test_extending_the_universal_close_past_cas_go_live_is_refused() -> None:
    rules = (
        NSEEquityContinuousCloseRule(_ALL, _REGIME_A_DAY, date(2026, 8, 11), time(15, 30),
                                     ("CE-F-02",)),
        *_PROD.close_rules[1:],
    )  # fmt: skip
    with pytest.raises(NSEEquitySessionEvidenceError):
        dataclasses.replace(_PROD, close_rules=rules, unsupported_closes=())


def test_a_close_rule_citing_a_fact_for_another_group_is_refused() -> None:
    rules = (
        _PROD.close_rules[0],
        NSEEquityContinuousCloseRule(_CAS, _NON_CAS_FROM, _COVERAGE_THROUGH, time(15, 15),
                                     ("CE-F-13",)),
        _PROD.close_rules[2],
    )  # fmt: skip
    with pytest.raises(NSEEquitySessionEvidenceError, match="no fact stating a boundary"):
        dataclasses.replace(_PROD, close_rules=rules)


def test_pre_open_phases_are_never_continuous_phases() -> None:
    for day in (_REGIME_A_DAY, _PRE_OPEN_REALIGNED):
        venue_day = _lookup(day)
        assert _ist(venue_day.opens_at) == time(9, 15)  # type: ignore[arg-type]
    session = _resolve(_REGIME_A_DAY)
    assert session is not None
    assert len(session.phases) == 1
    assert _ist(session.opens_at) == time(9, 15)


# ----------------------------------------------------------------------
# E. No instrument applicability is inferred
# ----------------------------------------------------------------------


def test_no_query_accepts_an_instrument() -> None:
    for method in (
        NSEEquityVenueSessionReference.lookup,
        NSEEquityTradingSessionResolver.resolve,
    ):
        assert list(inspect.signature(method).parameters) == [
            "self",
            "exchange_code",
            "trading_date",
        ]


def test_venue_day_names_no_security_and_resolves_no_applicability() -> None:
    venue_day = _lookup(_NON_CAS_FROM)
    fields = {field.name for field in dataclasses.fields(NSEEquityVenueDay)}
    assert not fields & {"instrument", "symbol", "listing", "security", "isin"}
    assert venue_day.resolves_instrument_applicability is False


def test_reference_modules_hold_no_security_identities_or_membership() -> None:
    for module in (reference_module, production_module):
        source = inspect.getsource(module)
        assert "Symbol" not in source
        assert "InstrumentIdentity" not in source
        assert not re.search(r"\b(RELIANCE|TCS|INFY|HDFCBANK)\b", source)


def test_possible_exceptions_are_reported_not_determined() -> None:
    assert [e.fact_id for e in _lookup(_REGIME_A_DAY).possible_exceptions] == ["CE-F-54"]
    assert [e.fact_id for e in _lookup(_NON_CAS_FROM).possible_exceptions] == [
        "CE-F-54",
        "CE-F-18",
    ]
    assert [e.fact_id for e in _lookup(_PRE_OPEN_REALIGNED).possible_exceptions] == [
        "CE-F-54",
        "CE-F-18",
        "CE-F-55",
    ]
    limitations = " ".join(_lookup(_REGIME_A_DAY).limitations)
    assert "not evidence that any security traded continuously" in limitations
    assert "CAS eligibility is not resolved" in limitations


# ----------------------------------------------------------------------
# F. Holidays, weekends and special sessions
# ----------------------------------------------------------------------


@pytest.mark.parametrize(
    "day",
    [date(2026, 5, 1), date(2026, 5, 28), date(2026, 6, 26), date(2026, 9, 14), date(2026, 10, 2)],
)
def test_weekday_holidays_inside_session_coverage_are_closed(day: date) -> None:
    assert _resolve(day) is None
    venue_day = _lookup(day)
    assert venue_day.status is _STATUS.CLOSED
    assert venue_day.fact_ids == ("CE-F-20",)
    assert "WEEKDAY_TRADING_HOLIDAY" in venue_day.reason


def test_the_january_15_amendment_is_a_closure_from_its_own_fact() -> None:
    assert _resolve(date(2026, 1, 15)) is None
    venue_day = _lookup(date(2026, 1, 15))
    assert venue_day.fact_ids == ("CE-F-21",)
    assert "NSE/CMTR/72260" in venue_day.reason


@pytest.mark.parametrize("day", [date(2026, 1, 26), date(2026, 3, 3), date(2026, 4, 14)])
def test_holidays_before_the_session_rules_are_still_stated_closures(day: date) -> None:
    assert _resolve(day) is None


@pytest.mark.parametrize("day", [date(2026, 2, 15), date(2026, 3, 21), date(2026, 8, 15)])
def test_weekend_holidays_are_closures_of_their_own_kind(day: date) -> None:
    assert _resolve(day) is None
    assert "WEEKEND_TRADING_HOLIDAY" in _lookup(day).reason


def test_a_holiday_list_has_twenty_entries_including_the_amendment() -> None:
    kinds = [closed.kind for closed in _PROD.closed_dates]
    assert kinds.count(NSEEquityClosureKind.WEEKDAY_TRADING_HOLIDAY) == 16
    assert kinds.count(NSEEquityClosureKind.WEEKEND_TRADING_HOLIDAY) == 4


@pytest.mark.parametrize("day", [date(2026, 5, 2), date(2026, 5, 3), date(2026, 8, 1)])
def test_unstated_weekends_fail_closed_and_are_never_inferred(day: date) -> None:
    assert _lookup(day).status is _STATUS.WEEKEND_NOT_STATED
    with pytest.raises(NSEEquitySessionUnavailableError, match="WEEKEND_NOT_STATED"):
        _resolve(day)


def test_muhurat_special_session_is_recorded_with_evidence_but_not_resolved() -> None:
    venue_day = _lookup(date(2026, 11, 8))
    assert venue_day.status is _STATUS.SPECIAL_SESSION_UNSUPPORTED
    assert venue_day.fact_ids == ("CE-F-30", "CE-F-31")
    assert venue_day.pending_decisions == ("CE-D9", "CE-D10")
    assert venue_day.opens_at is None and venue_day.continuous_closes == ()
    with pytest.raises(NSEEquitySessionUnavailableError, match="SPECIAL_SESSION_UNSUPPORTED"):
        _resolve(date(2026, 11, 8))


def test_a_special_session_inside_coverage_is_also_refused() -> None:
    fact = _test_fact(
        "CE-F-903", groups=frozenset(), effective_from=date(2026, 6, 6),
        effective_through=date(2026, 6, 6),
    )  # fmt: skip
    evidence = NSEEquityAcceptedSessionEvidence(
        "test-registry/1", (*PRODUCTION_ACCEPTED_EQUITY_SESSION_EVIDENCE.facts, fact)
    )
    special = dataclasses.replace(
        _PROD.special_sessions[0], day=date(2026, 6, 6), name="TEST ONLY", fact_ids=("CE-F-903",)
    )
    catalog = dataclasses.replace(
        _PROD, evidence=evidence, special_sessions=(*_PROD.special_sessions, special)
    )
    assert _lookup(date(2026, 6, 6), catalog).status is _STATUS.SPECIAL_SESSION_UNSUPPORTED
    with pytest.raises(NSEEquitySessionUnavailableError):
        _resolve(date(2026, 6, 6), catalog)


# ----------------------------------------------------------------------
# G. Coverage and unknown dates
# ----------------------------------------------------------------------


@pytest.mark.parametrize("day", [date(2026, 4, 27), date(2026, 4, 1), date(2026, 1, 2)])
def test_weekdays_before_april_28_fail_closed(day: date) -> None:
    assert _lookup(day).status is _STATUS.SESSION_RULES_NOT_ESTABLISHED
    with pytest.raises(NSEEquitySessionUnavailableError):
        _resolve(day)


@pytest.mark.parametrize(
    "day", [date(2025, 12, 31), date(2026, 10, 20), date(2026, 10, 21), date(2026, 12, 25),
            date(2027, 1, 4)],
)  # fmt: skip
def test_dates_beyond_explicit_coverage_fail_closed_even_if_a_holiday(day: date) -> None:
    assert _lookup(day).status is _STATUS.OUTSIDE_COVERAGE
    with pytest.raises(NSEEquitySessionUnavailableError, match="OUTSIDE_COVERAGE"):
        _resolve(day)


def test_coverage_through_is_explicit_and_not_a_completeness_claim() -> None:
    assert _PROD.coverage_through == _COVERAGE_THROUGH
    assert "not proof that every amendment on or before it was inspected" in _PROD.coverage_basis
    assert "never extended automatically" in _PROD.coverage_basis
    assert _lookup(_COVERAGE_THROUGH).status is _STATUS.NORMAL_SESSION
    assert _lookup(_COVERAGE_THROUGH + timedelta(days=1)).status is _STATUS.OUTSIDE_COVERAGE


def test_coverage_through_is_a_literal_reviewed_constant() -> None:
    tree = ast.parse(inspect.getsource(production_module))
    assigned = [
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Assign)
        and any(isinstance(t, ast.Name) and t.id == "_COVERAGE_THROUGH" for t in node.targets)
    ]
    assert len(assigned) == 1
    assert ast.unparse(assigned[0]) == "date(2026, 10, 19)"


def test_holiday_amendment_limitation_is_visible_in_every_answer_and_session() -> None:
    session = _resolve(_REGIME_A_DAY)
    assert session is not None
    assert session.reference.endswith("; not accepted: CE-F-22")
    assert "coverage through 2026-10-19" in session.reference
    for day in (_NON_CAS_FROM, date(2026, 5, 1), date(2026, 5, 2), date(2026, 11, 8)):
        venue_day = _lookup(day)
        assert venue_day.reference.endswith("; not accepted: CE-F-22")
        assert any("CE-F-22" in note for note in venue_day.limitations)
        assert any("does not prove actual trading" in note for note in venue_day.limitations)


def test_an_accepted_fact_cannot_be_declared_unresolved() -> None:
    with pytest.raises(NSEEquitySessionEvidenceError, match="cannot also be unresolved"):
        dataclasses.replace(_PROD, unresolved_fact_ids=("CE-F-20",))


def test_an_unstated_weekday_inside_coverage_fails_closed() -> None:
    day = date(2026, 6, 10)
    catalog = dataclasses.replace(_PROD, normal_sessions=_PROD.normal_sessions - {day})
    assert _lookup(day, catalog).status is _STATUS.NOT_STATED
    with pytest.raises(NSEEquitySessionUnavailableError, match="NOT_STATED"):
        _resolve(day, catalog)


def test_other_venues_and_wrong_types_are_refused() -> None:
    with pytest.raises(EquitySessionResolutionError, match="Unsupported equity venue"):
        NSEEquityTradingSessionResolver.production().resolve(ExchangeCode("BSE"), _REGIME_A_DAY)
    with pytest.raises(TypeError):
        NSEEquityVenueSessionReference.production().lookup("NSE", _REGIME_A_DAY)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        NSEEquityVenueSessionReference.production().lookup(_NSE, datetime(2026, 4, 28, tzinfo=UTC))
    with pytest.raises(TypeError):
        NSEEquityVenueSessionReference(object())  # type: ignore[arg-type]


# ----------------------------------------------------------------------
# H. Contradictory and incomplete catalogs
# ----------------------------------------------------------------------


def test_a_universal_close_overlapping_a_group_close_is_contradictory() -> None:
    fact = _test_fact("CE-F-904", effective_from=_CAS_LIVE, effective_through=date(2026, 8, 20))
    evidence = NSEEquityAcceptedSessionEvidence(
        "test-registry/1", (*PRODUCTION_ACCEPTED_EQUITY_SESSION_EVIDENCE.facts, fact)
    )
    rules = (
        *_PROD.close_rules,
        NSEEquityContinuousCloseRule(_ALL, _CAS_LIVE, date(2026, 8, 20), time(15, 30),
                                     ("CE-F-904",)),
    )  # fmt: skip
    with pytest.raises(NSEEquitySessionEvidenceError, match="contradict"):
        dataclasses.replace(_PROD, evidence=evidence, close_rules=rules)


def test_overlapping_opening_rules_are_contradictory() -> None:
    rules = (
        *_PROD.opening_rules,
        NSEEquityOpeningRule(date(2026, 9, 1), date(2026, 9, 6), time(9, 15),
                             ("CE-F-01", "CE-F-03")),
    )  # fmt: skip
    with pytest.raises(NSEEquitySessionEvidenceError, match="overlap"):
        dataclasses.replace(_PROD, opening_rules=rules)


def test_a_rule_beyond_coverage_through_is_refused() -> None:
    rules = (
        _PROD.close_rules[0],
        _PROD.close_rules[1],
        dataclasses.replace(_PROD.close_rules[2], effective_through=date(2026, 12, 31)),
    )
    with pytest.raises(NSEEquitySessionEvidenceError, match="outside the session coverage"):
        dataclasses.replace(_PROD, close_rules=rules)


def test_a_normal_session_also_stated_closed_is_contradictory() -> None:
    with pytest.raises(NSEEquitySessionEvidenceError, match="also stated as closed"):
        dataclasses.replace(_PROD, normal_sessions=_PROD.normal_sessions | {date(2026, 5, 1)})


def test_a_weekend_normal_session_is_refused() -> None:
    with pytest.raises(NSEEquitySessionEvidenceError, match="weekend"):
        dataclasses.replace(_PROD, normal_sessions=_PROD.normal_sessions | {date(2026, 5, 2)})


def test_a_normal_session_without_a_complete_close_statement_is_refused() -> None:
    with pytest.raises(NSEEquitySessionEvidenceError, match="incomplete continuous-close"):
        dataclasses.replace(_PROD, unsupported_closes=())


def test_a_normal_session_before_the_session_rules_is_refused() -> None:
    with pytest.raises(NSEEquitySessionEvidenceError, match="outside the session coverage"):
        dataclasses.replace(_PROD, normal_sessions=_PROD.normal_sessions | {date(2026, 4, 27)})


def test_a_closure_whose_kind_contradicts_its_weekday_is_refused() -> None:
    with pytest.raises(NSEEquitySessionEvidenceError, match="does not match its weekday"):
        NSEEquityClosedDate(
            date(2026, 5, 2), NSEEquityClosureKind.WEEKDAY_TRADING_HOLIDAY, "TEST ONLY",
            ("CE-F-20",),
        )  # fmt: skip


def test_a_repeated_closure_is_refused() -> None:
    with pytest.raises(NSEEquitySessionEvidenceError, match="listed twice"):
        dataclasses.replace(_PROD, closed_dates=(*_PROD.closed_dates, _PROD.closed_dates[0]))


def test_an_unsupported_close_needs_a_pending_decision() -> None:
    with pytest.raises(NSEEquitySessionEvidenceError):
        NSEEquityUnsupportedClose(_NON_CAS, _CAS_LIVE, date(2026, 8, 11), "TEST ONLY", ())


def test_catalog_inputs_cannot_be_mutated_after_construction() -> None:
    with pytest.raises(dataclasses.FrozenInstanceError):
        _PROD.coverage_through = date(2027, 1, 1)  # type: ignore[misc]
    assert isinstance(_PROD.normal_sessions, frozenset)
    assert isinstance(_PROD.closed_dates, tuple)


# ----------------------------------------------------------------------
# I. Determinism, no clock, no provider, scope
# ----------------------------------------------------------------------


def test_identical_catalogs_answer_identically() -> None:
    days = _days(date(2026, 1, 1), date(2026, 12, 31))
    first = [_lookup(day) for day in days]
    second = [_lookup(day, dataclasses.replace(_PROD)) for day in days]
    assert [(d.status, d.opens_at, d.continuous_closes) for d in first] == [
        (d.status, d.opens_at, d.continuous_closes) for d in second
    ]


def test_network_is_refused_in_this_module() -> None:
    with pytest.raises(AssertionError, match="network"):
        socket.create_connection(("example.invalid", 443))


@pytest.mark.parametrize("module", [reference_module, production_module])
def test_modules_read_no_clock_provider_storage_or_network(module: object) -> None:
    source = inspect.getsource(module)  # type: ignore[arg-type]
    tree = ast.parse(source)
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
        elif isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
    for name in imported:
        for forbidden in (
            "sqlite",
            "http",
            "socket",
            "upstox",
            "yahoo",
            "databento",
            "futures",
            "option",
            "broker",
            "recommendation",
            "exchange_calendars",
        ):
            assert forbidden not in name, f"{module} imports {name}"  # fmt: skip
    assert "+05:30" not in source and "timedelta(hours=5" not in source


def test_research_window_and_candle_finality_are_not_implemented_here() -> None:
    for module in (reference_module, production_module):
        tree = ast.parse(inspect.getsource(module))
        names = {
            node.name for node in ast.walk(tree) if isinstance(node, ast.ClassDef | ast.FunctionDef)
        }
        assert not any("window" in name.lower() or "candle" in name.lower() for name in names)
        assert "time(15, 0)" not in inspect.getsource(module)


def test_market_data_package_exports_the_reference() -> None:
    for name in (
        "NSEEquityTradingSessionResolver",
        "NSEEquityVenueSessionReference",
        "NSEEquitySessionCatalog",
        "PRODUCTION_NSE_EQUITY_SESSION_CATALOG",
        "PRODUCTION_ACCEPTED_EQUITY_SESSION_EVIDENCE",
    ):
        assert name in market_data.__all__
