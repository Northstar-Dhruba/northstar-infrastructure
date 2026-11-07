"""EI-4E acceptance: synthetic candles to verified generation records, on real SQLite.

Every test drives the merged chain over one disposable SQLite file, through
public use cases and adapters only (tests/equity_recommendation_acceptance_
fixtures.py): synthetic candles journaled by EI-2C ingestion, EI-2B snapshots
issued under EI-1C-3 eligibility and the accepted NSE venue reference, EI-3A
evaluation and the EI-3C timeline, EI-4C generation with EI-4B Core, and EI-4D-A
recording and verification over the EI-4D-B SQLite store. Only the external
reference and issued-history sources are injected, as labelled synthetic test
fixtures. No test here alters a stored row; fault injection, tampering and
concurrency are in test_equity_recommendation_generation_acceptance_faults.py.

Test names carry the EI-4E prompt section they evidence (e5 to e16, e22).
Nothing here is an issued recommendation, an order or a fill, and nothing
opens a network connection.
"""

from __future__ import annotations

import gc
import json
import shutil
import socket
import sqlite3
from collections.abc import Callable
from contextlib import closing
from dataclasses import replace
from datetime import time, timedelta
from pathlib import Path
from typing import Any

import pytest
from northstar_application.application_services import (
    EQUITY_RECOMMENDATION_EVALUATOR_VERSION,
    EQUITY_RECOMMENDATION_GENERATION_KEY_PREFIX,
    EquityCandleEvidenceClass,
    EquityCandleEvidenceSnapshot,
    EquityIntradayResearchOutcome,
    EquityIntradayResearchRejection,
    EquityIntradayResearchTimelineIdentity,
    EquityIntradayResearchTimelineRole,
    EquityOpeningRangeBreakoutConfiguration,
    EquityRecommendationCheck,
    EquityRecommendationCheckStatus,
    EquityRecommendationDispositionKind,
    EquityRecommendationEligibilityAudit,
    EquityRecommendationEligibilityReason,
    EquityRecommendationGenerationStatus,
    EquityRecommendationRecordingOutcome,
    EquityTradeRecommendationRequest,
    EvaluateEquityIntradayResearchUseCase,
    GenerateEquityTradeRecommendationUseCase,
    InvalidEquityTradeRecommendationRequestError,
    assess_equity_recommendation_inputs,
    equity_modeled_issuance,
    equity_recommendation_attempt,
    equity_recommendation_disposition,
    equity_recommendation_generation_key,
    establish_equity_recommendation_candidate,
    rehydrate_equity_intraday_research_timeline,
    verify_equity_recommendation_document,
)
from northstar_application.ports import (
    EquityRecommendationGenerationStoreUnavailableError,
    EquitySessionRecommendationHistory,
    EquitySyntheticEligibilityCriterion,
    EquitySyntheticEligibilityVerdict,
    EquityVenueDayClassification,
    InvalidEquityRecommendationReferenceError,
)

from northstar_infrastructure.persistence import (
    SQLiteEquityCandleEvidenceSnapshotStore,
    SQLiteEquityRecommendationGenerationStore,
)
from tests.equity_recommendation_acceptance_fixtures import (
    ATTEMPTS,
    CONFIG,
    DAY,
    DISPOSITIONS,
    ENTRIES,
    INFY,
    NSE,
    QUIET_SESSION,
    SNAPSHOTS,
    TCS,
    TIMELINES,
    VENUE,
    WIPRO,
    Evidence,
    History,
    Pipeline,
    Research,
    check_database,
    counts,
    establish_research,
    ist,
    prior,
    raw,
    request,
    rows,
    schema_objects,
    timing_policy,
)

_O = EquityRecommendationRecordingOutcome
_S = EquityRecommendationGenerationStatus
_R = EquityRecommendationEligibilityReason
_K = EquityRecommendationDispositionKind
_ROLE = EquityIntradayResearchTimelineRole
_CHECK = EquityRecommendationCheck
_IDENTITY = EquityIntradayResearchTimelineIdentity(TCS, DAY, CONFIG)
_GENERATION = frozenset({DISPOSITIONS, ATTEMPTS})


@pytest.fixture(autouse=True)
def _refuse_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("EI-4E acceptance must not open network connections.")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)


@pytest.fixture
def pipeline(tmp_path: Path) -> Pipeline:
    return Pipeline(tmp_path / "ei4e.sqlite3")


@pytest.fixture
def research(pipeline: Pipeline) -> Research:
    return establish_research(pipeline)


def _check(audit: EquityRecommendationEligibilityAudit, check: EquityRecommendationCheck) -> Any:
    return next(result for result in audit.checks if result.check is check)


def _document(text: str) -> dict[str, Any]:
    return json.loads(text)  # type: ignore[no-any-return]


def _generation_objects(database: Path) -> dict[str, tuple[str, str]]:
    return {
        name: value
        for name, value in schema_objects(database).items()
        if any(name.startswith(table) for table in _GENERATION)
    }


def _evidence_objects(database: Path) -> dict[str, tuple[str, str]]:
    generation = _generation_objects(database)
    return {n: v for n, v in schema_objects(database).items() if n not in generation}


# ----------------------------------------------------------------------
# e5. Synthetic evidence: journal, finalized immutable snapshot, provenance
# ----------------------------------------------------------------------


def test_e5_synthetic_candles_are_journaled_and_snapshotted_through_ei2(pipeline: Pipeline) -> None:
    venue = VENUE.venue_day(NSE, DAY)
    assert venue.classification is EquityVenueDayClassification.NORMAL_SESSION  # confirmed
    evidence = pipeline.evidence()
    evidence.candles()
    journal = rows(pipeline.database, "equity_candle_journal")
    assert len(journal) == 12  # six synthetic observations, each with its finality
    snapshot = evidence.issue(ist(9, 41))
    assert snapshot.evidence_class is EquityCandleEvidenceClass.SYNTHETIC
    assert (snapshot.listing, snapshot.trading_date, snapshot.as_of) == (TCS, DAY, ist(9, 41))
    assert snapshot.snapshot_id.startswith("ecs1-sha256:")
    assert snapshot.manifest_fingerprint.startswith("ecm1-sha256:")
    assert snapshot.venue_reference == venue.reference  # the accepted venue evidence
    assert (snapshot.window_opens_at, snapshot.window_closes_at) == (ist(9, 15), ist(15, 0))
    assert len(snapshot.intervals) == 69  # the full five-minute research-window grid
    outcomes = [interval.outcome.value for interval in snapshot.intervals]
    assert outcomes == ["ACCEPTABLE"] * 5 + ["NOT_ENDED"] * 64  # bars 1-5 final by 09:41
    assert not any(i.acceptable_sequences for i in snapshot.intervals[5:])  # no look-ahead
    assert snapshot.coverage.value == "COMPLETE_AS_OF"
    # Independently reloaded by a fresh adapter: the same immutable snapshot.
    reloaded = SQLiteEquityCandleEvidenceSnapshotStore(pipeline.database).get_snapshot(
        snapshot.snapshot_id
    )
    assert reloaded == snapshot
    assert len(rows(pipeline.database, SNAPSHOTS)) == 1
    with closing(raw(pipeline.database)) as connection, pytest.raises(sqlite3.IntegrityError):
        connection.execute(f"DELETE FROM {SNAPSHOTS}")  # noqa: S608 -- immutable at the database
    check_database(pipeline.database)


# ----------------------------------------------------------------------
# e6. The authoritative research CANDIDATE
# ----------------------------------------------------------------------


def test_e6_generation_never_uses_an_uncommitted_candidate(pipeline: Pipeline) -> None:
    evidence = pipeline.evidence()
    evidence.candles()
    s41 = evidence.issue(ist(9, 41))
    in_memory = EvaluateEquityIntradayResearchUseCase(evidence.snapshots, CONFIG).execute(
        TCS, DAY, s41.snapshot_id
    )
    assert in_memory.outcome is EquityIntradayResearchOutcome.LONG_CANDIDATE  # evaluated only
    audit = pipeline.generate()
    assert audit.status is _S.NO_CANDIDATE and audit.reasons == (_R.NO_AUTHORITATIVE_CANDIDATE,)
    assert audit.generation_key is None and audit.candidate is None
    result = pipeline.recorder().execute(audit)
    assert (result.outcome, result.attempt_recorded, result.disposition) == (
        _O.COMMITTED,
        True,
        None,
    )
    assert counts(pipeline.database) == (0, 1)


def test_e6_the_first_committed_candidate_is_the_authoritative_source(
    pipeline: Pipeline, research: Research
) -> None:
    roles = [entry.role for entry in research.advance.appended]
    assert roles == [_ROLE.NO_SIGNAL, _ROLE.CANDIDATE, _ROLE.REPEAT_QUALIFYING]
    timeline = research.advance.timeline
    assert timeline is not None and timeline.candidate is not None
    candidate = timeline.candidate
    assert candidate.sequence == 2 and candidate.snapshot_id == research.s41.snapshot_id
    assert candidate.fingerprint.startswith("eirt1-sha256:")
    fresh = pipeline.evidence()
    rerun = EvaluateEquityIntradayResearchUseCase(fresh.snapshots, CONFIG).execute(
        TCS, DAY, research.s41.snapshot_id
    )
    assert candidate.decision == rerun and candidate.decision.fingerprint == rerun.fingerprint
    stored = fresh.timelines.load(CONFIG.strategy, TCS, DAY)
    assert stored is not None
    rehydrated = rehydrate_equity_intraday_research_timeline(_IDENTITY, stored, fresh.snapshots)
    assert rehydrated.document() == timeline.document()
    assert rehydrated.candidate == candidate
    audit = pipeline.generate()
    assert audit.candidate is not None
    source = audit.candidate.source
    assert source.candidate_entry_fingerprint == candidate.fingerprint  # never entry 3
    assert source.evidence_snapshot_id == research.s41.snapshot_id
    assert source.research_decision_fingerprint == candidate.decision.fingerprint
    assert source.decision_as_of == research.s41.as_of
    assert audit.candidate.candidate_sequence == 2
    repeat = timeline.entries[2]
    assert repeat.role is _ROLE.REPEAT_QUALIFYING and repeat.fingerprint != candidate.fingerprint


def test_e6_a_session_without_a_breakout_has_no_candidate_to_generate_from(
    pipeline: Pipeline,
) -> None:
    evidence = pipeline.evidence()
    evidence.candles(QUIET_SESSION, INFY)
    quiet = evidence.issue(ist(9, 46), INFY)
    advanced = evidence.advance([quiet], INFY)
    assert [e.role for e in advanced.appended] == [_ROLE.NO_SIGNAL]
    audit = pipeline.generate(listing=INFY)
    assert audit.status is _S.NO_CANDIDATE and audit.generation_key is None
    assert pipeline.recorder().execute(audit).outcome is _O.COMMITTED
    assert counts(pipeline.database) == (0, 1)


# ----------------------------------------------------------------------
# e8. The PROPOSED path, step by step
# ----------------------------------------------------------------------


def _proposed_path(database: Path) -> dict[str, Any]:
    pipeline = Pipeline(database)
    # 1-4. Candles, immutable snapshots, research evaluation, committed first CANDIDATE.
    research = establish_research(pipeline)
    # 5. A fresh 'process' rehydrates the authoritative research evidence.
    fresh = pipeline.evidence()
    stored = fresh.timelines.load(CONFIG.strategy, TCS, DAY)
    assert stored is not None
    candidate = rehydrate_equity_intraday_research_timeline(
        _IDENTITY, stored, fresh.snapshots
    ).candidate
    assert candidate is not None
    # 6-10. References resolved by version, modeled issuance, entry bar, Core, proposal.
    asked = len(pipeline.history.asked)
    audit = pipeline.generate()
    assert audit.status is _S.PROPOSED and audit.reasons == ()
    inputs, proposal, window = audit.inputs, audit.proposal, audit.entry_window
    assert inputs is not None and proposal is not None and window is not None
    assert pipeline.history.asked[asked:] == [ist(9, 43, 30)]  # as of the modeled issuance
    latency = pipeline.references.timing["timing/1"].modeled_issuance_latency
    assert inputs.modeled_issued_at == equity_modeled_issuance(research.s41.as_of, latency)
    assert window.timing.eligible_from == ist(9, 45)
    assert inputs.policy_set.reference == request().policy_set
    assert inputs.policy_set.fingerprint.startswith("eirs1-sha256:")
    key = audit.generation_key
    assert key is not None and key.startswith(EQUITY_RECOMMENDATION_GENERATION_KEY_PREFIX)
    assert key == equity_recommendation_generation_key(audit.candidate.source, inputs.policy_set)  # type: ignore[union-attr]
    assert (proposal.is_synthetic, proposal.is_issued, proposal.is_actionable) == (
        True,
        False,
        False,
    )
    assert not proposal.is_authoritative and not audit.is_authoritative
    assert _check(audit, _CHECK.CORE_FORMATION).status is EquityRecommendationCheckStatus.PASSED
    # 11. The EI-4D-A records, verified on construction.
    disposition = equity_recommendation_disposition(audit)
    attempt = equity_recommendation_attempt(audit)
    assert disposition is not None and disposition.kind is _K.PROPOSED
    assert disposition.proposal_document is not None
    assert disposition.proposal_fingerprint == proposal.fingerprint
    # 12. Recorded through the EI-4D-B SQLite store, after source and replay verification.
    result = pipeline.recorder().execute(audit)
    assert (result.outcome, result.attempt_recorded, result.writes) == (_O.COMMITTED, True, 1)
    assert result.is_stored and result.authoritative == disposition
    assert len(pipeline.history.asked) == asked + 1  # recording never consults history
    # 13. A fresh adapter reloads it; the verifier re-establishes and replays it.
    reloaded = SQLiteEquityRecommendationGenerationStore(database).load_disposition(key)
    assert reloaded == disposition
    verified = pipeline.verify_stored().execute(key)
    assert verified is not None
    assert verified.source_integrity_verified and verified.evaluation_reproduced
    assert not verified.issuance_established
    assert verified.replayed.assessment_fingerprint == disposition.assessment_fingerprint
    (row,) = rows(database, DISPOSITIONS)
    (attempt_row,) = rows(database, ATTEMPTS)
    assert row["disposition_kind"] == "PROPOSED"
    assert row["candidate_entry_fingerprint"] == candidate.fingerprint
    assert row["evidence_snapshot_id"] == research.s41.snapshot_id
    assert row["policy_set_fingerprint"] == inputs.policy_set.fingerprint
    assert row["evaluator_version"] == EQUITY_RECOMMENDATION_EVALUATOR_VERSION
    assert attempt_row["attempt_identity"] == attempt.attempt_identity
    assert attempt_row["disposition_key"] == key
    stored_proposal = _document(row["proposal_document"])
    assert (stored_proposal["issued"], stored_proposal["actionable"]) == (False, False)
    check_database(database)
    return {
        "rows": {
            "journal": len(rows(database, "equity_candle_journal")),
            "snapshots": len(rows(database, SNAPSHOTS)),
            "timelines": len(rows(database, TIMELINES)),
            "timeline_entries": len(rows(database, ENTRIES)),
            "dispositions": counts(database)[0],
            "attempts": counts(database)[1],
        },
        "snapshot_id": research.s41.snapshot_id,
        "manifest_fingerprint": research.s41.manifest_fingerprint,
        "decision_fingerprint": candidate.decision.fingerprint,
        "candidate_entry_fingerprint": candidate.fingerprint,
        "policy_set_fingerprint": inputs.policy_set.fingerprint,
        "inputs_fingerprint": inputs.fingerprint,
        "generation_key": key,
        "assessment_fingerprint": disposition.assessment_fingerprint,
        "proposal_fingerprint": disposition.proposal_fingerprint,
        "attempt_identity": attempt.attempt_identity,
        "modeled_issued_at": inputs.modeled_issued_at.value,
        "eligible_from": window.timing.eligible_from.value,
        "quantity": proposal.definition.sizing.quantity,
    }


def test_e8_the_proposed_path_from_candles_to_a_verified_stored_disposition(
    tmp_path: Path, record_property: Callable[[str, object], None]
) -> None:
    evidence = _proposed_path(tmp_path / "ei4e.sqlite3")
    assert evidence["rows"] == {
        "journal": 12,
        "snapshots": 3,
        "timelines": 1,
        "timeline_entries": 3,
        "dispositions": 1,
        "attempts": 1,
    }
    for name, value in evidence.items():
        record_property(name, value)


def test_e8_the_same_synthetic_inputs_reproduce_every_fingerprint_on_a_new_database(
    tmp_path: Path,
) -> None:
    assert _proposed_path(tmp_path / "first.sqlite3") == _proposed_path(tmp_path / "second.sqlite3")


# ----------------------------------------------------------------------
# e9. NOT_ELIGIBLE after a generation identity is established
# ----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("changes", "reason"),
    [
        (
            {"generation": "generation/strict", "set_version": "set/strict"},
            _R.CORE_FORMATION_REJECTED,
        ),
        ({"timing": "timing/late", "set_version": "set/late"}, _R.NO_PERMISSIBLE_ENTRY_BAR),
        ({}, _R.LISTING_NOT_ELIGIBLE),
    ],
    ids=["core rejection", "no permissible entry bar", "listing ineligible"],
)
def test_e9_a_confirmed_ineligibility_is_recorded_without_a_proposal(
    pipeline: Pipeline, research: Research, changes: dict[str, str], reason: Any
) -> None:
    if reason is _R.LISTING_NOT_ELIGIBLE:
        criterion = next(iter(EquitySyntheticEligibilityCriterion))
        pipeline.eligibility.verdicts = {criterion: EquitySyntheticEligibilityVerdict.INELIGIBLE}
    audit = pipeline.generate(**changes)
    assert audit.status is _S.NOT_ELIGIBLE and audit.reasons == (reason,)
    assert audit.generation_key is not None and audit.proposal is None
    disposition = equity_recommendation_disposition(audit)
    assert disposition is not None and disposition.kind is _K.NOT_ELIGIBLE
    assert (disposition.proposal_document, disposition.proposal_fingerprint) == (None, None)
    assert disposition.reasons == (reason,)
    assessment = _document(disposition.assessment_document)
    assert assessment["status"] == "NOT_ELIGIBLE" and assessment["reasons"] == [reason.value]
    timeline = research.advance.timeline
    assert timeline is not None and timeline.candidate is not None
    assert (
        disposition.candidate.source.candidate_entry_fingerprint == timeline.candidate.fingerprint
    )
    result = pipeline.recorder().execute(audit)
    assert (result.outcome, result.attempt_recorded) == (_O.COMMITTED, True)
    again = pipeline.generate(**changes)  # deterministic: the same canonical assessment
    assert again.assessment_fingerprint == audit.assessment_fingerprint
    assert pipeline.recorder().execute(again).outcome is _O.EQUIVALENT_COMMITTED
    (row,) = rows(pipeline.database, DISPOSITIONS)
    assert (row["disposition_kind"], row["proposal_document"]) == ("NOT_ELIGIBLE", None)
    with (
        closing(raw(pipeline.database)) as connection,
        pytest.raises(sqlite3.IntegrityError, match="insert-only"),
    ):
        connection.execute(f"UPDATE {DISPOSITIONS} SET disposition_kind = 'PROPOSED'")  # noqa: S608
    assert counts(pipeline.database) == (1, 1)


# ----------------------------------------------------------------------
# e10. UNRESOLVED: open-recommendation policy is not approved
# ----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("listing", "reason"),
    [(TCS, _R.EXCLUSIVITY_POLICY_UNRESOLVED), (WIPRO, _R.EXPOSURE_POLICY_UNRESOLVED)],
    ids=["same-listing exclusivity", "other-listing exposure"],
)
def test_e10_an_unresolved_disposition_is_immutable_and_never_becomes_proposed(
    pipeline: Pipeline, research: Research, listing: Any, reason: Any
) -> None:
    pipeline.history = History(records=(prior("earlier", listing, ist(9, 30), 1),))
    audit = pipeline.generate()
    assert audit.status is _S.EVALUATION_UNRESOLVED and audit.reasons == (reason,)
    open_check = _check(audit, _CHECK.OPEN_RECOMMENDATIONS)
    assert open_check.status is EquityRecommendationCheckStatus.FAILED
    assert open_check.reasons == (reason,)
    assert _check(audit, _CHECK.SAME_LISTING_EXCLUSIVITY).status.value == "NOT_ASSESSED"
    assert _check(audit, _CHECK.PORTFOLIO_EXPOSURE).status.value == "NOT_ASSESSED"
    unresolved = equity_recommendation_disposition(audit)
    assert unresolved is not None and unresolved.kind is _K.UNRESOLVED
    assert unresolved.proposal_document is None
    assert pipeline.recorder().execute(audit).outcome is _O.COMMITTED
    before = rows(pipeline.database, DISPOSITIONS)

    pipeline.history = History()  # the explicit synthetic empty history
    later = pipeline.generate()
    assert later.status is _S.PROPOSED and later.generation_key == audit.generation_key
    refused = pipeline.recorder().execute(later)
    assert refused.outcome is _O.CONTENT_CONFLICT and not refused.is_stored
    assert refused.authoritative == unresolved and refused.attempt_recorded is True
    assert rows(pipeline.database, DISPOSITIONS) == before  # the UNRESOLVED row, unchanged
    assert before[0]["disposition_kind"] == "UNRESOLVED" and before[0]["proposal_document"] is None
    assert counts(pipeline.database) == (1, 2)  # the refused attempt is audited
    verified = pipeline.verify_stored().execute(unresolved.generation_key)
    assert verified is not None and verified.disposition.kind is _K.UNRESOLVED


# ----------------------------------------------------------------------
# e11. Attempt-only results: no candidate, integrity, outages, early refusals
# ----------------------------------------------------------------------


def _wrong_snapshot_store(pipeline: Pipeline, tmp: Path) -> None:
    """FAULT INJECTION, labelled: the generator reads EI-2B snapshots from another database.

    The other database holds the same synthetic candles but never the snapshots
    the committed timeline names, so the timeline cannot be re-verified.
    """
    other = Evidence(tmp / "other-evidence.sqlite3")
    other.candles()

    def miswired() -> GenerateEquityTradeRecommendationUseCase:
        return GenerateEquityTradeRecommendationUseCase(
            pipeline.evidence().timelines,
            other.snapshots,
            VENUE,
            pipeline.references,
            pipeline.eligibility,
            pipeline.history,
        )

    pipeline.generator = miswired  # type: ignore[method-assign]


@pytest.mark.parametrize(
    "scenario",
    [
        "no committed research",
        "missing issued history",
        "unavailable issued history",
        "incomplete issued history",
        "reference outage",
        "unknown policy version",
        "configuration mismatch",
        "research integrity failure",
    ],
)
def test_e11_an_attempt_only_result_is_audited_without_a_disposition(
    pipeline: Pipeline, tmp_path: Path, scenario: str
) -> None:
    changes: dict[str, Any] = {}
    if scenario != "no committed research":
        establish_research(pipeline)
    else:
        evidence = pipeline.evidence()
        evidence.candles()
        evidence.issue(ist(9, 41))
    expected = {
        "no committed research": (_S.NO_CANDIDATE, (_R.NO_AUTHORITATIVE_CANDIDATE,)),
        "missing issued history": (
            _S.DEPENDENCY_UNAVAILABLE,
            (_R.OPEN_RECOMMENDATION_STATE_UNAVAILABLE,),
        ),
        "unavailable issued history": (
            _S.DEPENDENCY_UNAVAILABLE,
            (_R.OPEN_RECOMMENDATION_STATE_UNAVAILABLE,),
        ),
        "incomplete issued history": (
            _S.DEPENDENCY_UNAVAILABLE,
            (_R.RECOMMENDATION_HISTORY_INCOMPLETE,),
        ),
        "reference outage": (_S.DEPENDENCY_UNAVAILABLE, None),
        "unknown policy version": (
            _S.DEPENDENCY_UNAVAILABLE,
            (_R.GENERATION_POLICY_UNAVAILABLE,),
        ),
        "configuration mismatch": (_S.NOT_ELIGIBLE, (_R.RESEARCH_CONFIGURATION_MISMATCH,)),
        "research integrity failure": (_S.RESEARCH_INTEGRITY_FAILURE, None),
    }[scenario]
    if scenario == "missing issued history":
        pipeline.history = History(mode="missing")
    elif scenario == "unavailable issued history":
        pipeline.history = History(mode="unavailable")
    elif scenario == "incomplete issued history":
        pipeline.history = History(complete_through=ist(9, 43))  # before 09:43:30
    elif scenario == "reference outage":
        pipeline.references.available = False
    elif scenario == "unknown policy version":
        changes = {"generation": "generation/absent"}
    elif scenario == "configuration mismatch":
        changes = {
            "configuration": EquityOpeningRangeBreakoutConfiguration(
                strategy=CONFIG.strategy,
                version="synthetic-ei4e-research/other",
                opening_range_bars=CONFIG.opening_range_bars,
                volume_multiplier=CONFIG.volume_multiplier,
                minimum_baseline_bars=CONFIG.minimum_baseline_bars,
            )
        }
    elif scenario == "research integrity failure":
        _wrong_snapshot_store(pipeline, tmp_path)
    audit = pipeline.generate(**changes)
    status, reasons = expected
    assert audit.status is status, audit.reasons
    if reasons is not None:
        assert audit.reasons == reasons
    assert audit.generation_key is None and equity_recommendation_disposition(audit) is None
    attempt = equity_recommendation_attempt(audit)
    document = _document(attempt.attempt_document)
    assert document["generation_key"] is None and document["disposition_kind"] is None
    assert "candidate" not in document and "inputs" not in document  # nothing fabricated
    first = pipeline.recorder().execute(audit)
    assert (first.outcome, first.attempt_recorded, first.disposition) == (_O.COMMITTED, True, None)
    again = pipeline.recorder().execute(pipeline.generate(**changes))  # an identical attempt
    assert (again.outcome, again.attempt) == (_O.EQUIVALENT_COMMITTED, attempt)
    assert counts(pipeline.database) == (0, 1)
    (row,) = rows(pipeline.database, ATTEMPTS)
    assert (row["status"], row["generation_key"], row["disposition_key"]) == (
        status.value,
        None,
        None,
    )


def test_e11_identical_attempts_deduplicate_and_distinct_ones_stay_auditable(
    pipeline: Pipeline, research: Research
) -> None:
    pipeline.history = History(mode="unavailable")
    unavailable = equity_recommendation_attempt(pipeline.generate())
    pipeline.history = History(mode="missing")  # another outage, the same structured result
    missing = equity_recommendation_attempt(pipeline.generate())
    assert unavailable.attempt_identity == missing.attempt_identity  # prose is not identity
    pipeline.history = History(complete_through=ist(9, 43))
    incomplete = pipeline.generate()
    for attempt_of in (unavailable, missing):
        assert pipeline.recorder().record_attempt(attempt_of).is_stored
    assert pipeline.recorder().execute(incomplete).outcome is _O.COMMITTED
    identities = {r["attempt_identity"] for r in rows(pipeline.database, ATTEMPTS)}
    assert identities == {
        unavailable.attempt_identity,
        equity_recommendation_attempt(incomplete).attempt_identity,
    }
    assert counts(pipeline.database) == (0, 2)


def test_e11_an_invalid_request_is_refused_before_any_identity_or_record(
    pipeline: Pipeline, research: Research
) -> None:
    with pytest.raises(InvalidEquityTradeRecommendationRequestError):
        EquityTradeRecommendationRequest(
            configuration=CONFIG,
            listing=TCS,
            trading_date=DAY.isoformat(),  # type: ignore[arg-type] -- not a date
            policy_set=request().policy_set,
            listing_eligibility_version=request().listing_eligibility_version,
        )
    assert schema_objects(pipeline.database).keys().isdisjoint(_GENERATION)  # nothing created


# ----------------------------------------------------------------------
# e12. The composite policy-set identity and canonical-content conflicts
# ----------------------------------------------------------------------


def test_e12_a_b_f_one_key_per_candidate_and_approved_policy_set(
    pipeline: Pipeline, research: Research
) -> None:
    first, second = pipeline.generate(), pipeline.generate()
    assert first.generation_key == second.generation_key  # A
    assert first.assessment_fingerprint == second.assessment_fingerprint
    keys = {
        name: pipeline.generate(**changes).generation_key
        for name, changes in {
            "set/1": {},
            "another set version": {"set_version": "set/2"},
            "another generation policy": {"generation": "generation/strict"},
            "another timing policy": {"timing": "timing/on-opening"},
        }.items()
    }
    assert len(set(keys.values())) == 4  # B: every component change is another identity
    assert pipeline.recorder().execute(first).outcome is _O.COMMITTED
    again = pipeline.recorder().execute(second)
    assert (again.outcome, again.writes) == (_O.EQUIVALENT_COMMITTED, 1)  # F
    assert counts(pipeline.database) == (1, 1)


def test_e12_c_a_reused_version_label_with_changed_content_is_another_identity(
    pipeline: Pipeline, research: Research
) -> None:
    original = pipeline.generate()
    assert pipeline.recorder().execute(original).outcome is _O.COMMITTED
    before = rows(pipeline.database, DISPOSITIONS)
    # The same label "timing/1" now names other content: a 240 s latency, not 150 s.
    pipeline.references.timing["timing/1"] = timing_policy("timing/1", timedelta(minutes=4))
    relabelled = pipeline.generate()
    assert relabelled.request == original.request  # every version label is identical
    assert relabelled.inputs is not None and original.inputs is not None
    assert relabelled.inputs.policy_set.fingerprint != original.inputs.policy_set.fingerprint
    assert relabelled.generation_key != original.generation_key  # never the identical set
    result = pipeline.recorder().execute(relabelled)
    assert result.outcome is _O.COMMITTED
    stored = {r["generation_key"]: r for r in rows(pipeline.database, DISPOSITIONS)}
    assert stored[original.generation_key] == before[0]  # the first record is untouched
    policies = {
        key: _document(row["assessment_document"])["inputs"]["policy_set"]["timing_policy"]
        for key, row in stored.items()
    }
    assert policies[original.generation_key] != policies[relabelled.generation_key]


@pytest.mark.parametrize("change", ["eligibility universe", "eligibility verdict", "history"])
def test_e12_d_e_g_h_changed_evidence_under_one_key_is_a_conflict_never_a_replacement(
    pipeline: Pipeline, research: Research, change: str
) -> None:
    original = pipeline.generate()
    first = pipeline.recorder().execute(original)
    assert first.outcome is _O.COMMITTED
    before = (rows(pipeline.database, DISPOSITIONS), rows(pipeline.database, ATTEMPTS))
    if change == "eligibility universe":
        pipeline.eligibility.universe = "universe/2"  # still ELIGIBLE everywhere
    elif change == "eligibility verdict":
        criterion = next(iter(EquitySyntheticEligibilityCriterion))
        pipeline.eligibility.verdicts = {criterion: EquitySyntheticEligibilityVerdict.INELIGIBLE}
    else:  # as-of history: a later issuance raises the recording watermark only
        pipeline.history = History(records=(prior("later", WIPRO, ist(11, 0), 1),))
    changed = pipeline.generate()
    assert changed.generation_key == original.generation_key  # D, E: never a new key
    assert changed.assessment_fingerprint != original.assessment_fingerprint
    result = pipeline.recorder().execute(changed)
    assert result.outcome is _O.CONTENT_CONFLICT and not result.is_stored  # G
    assert result.authoritative == first.authoritative and result.attempt_recorded is True
    after = (rows(pipeline.database, DISPOSITIONS), rows(pipeline.database, ATTEMPTS))
    assert after[0] == before[0]  # H: the first committed disposition is unchanged
    assert after[1][: len(before[1])] == before[1] and len(after[1]) == len(before[1]) + 1


# ----------------------------------------------------------------------
# e13. Timing and look-ahead, end to end
# ----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("timing", "issued", "eligible"),
    [
        ("timing/1", ist(9, 43, 30), ist(9, 45)),  # between bar openings
        ("timing/on-opening", ist(9, 45), ist(9, 45)),  # exactly on a bar opening
        ("timing/late", ist(10, 46), None),  # after the latest entry-bar opening
    ],
    ids=["between openings", "on an opening", "late"],
)
def test_e13_entry_bars_open_only_at_or_after_the_modeled_issuance(
    pipeline: Pipeline, research: Research, timing: str, issued: Any, eligible: Any
) -> None:
    audit = pipeline.generate(timing=timing, set_version=f"set/{timing}")
    inputs = audit.inputs
    assert inputs is not None and audit.candidate is not None
    decided = audit.candidate.source.decision_as_of
    assert decided == research.s41.as_of == ist(9, 41)
    assert audit.candidate.signal_interval_end == ist(9, 40)  # the decision's bar 5
    latency = pipeline.references.timing[timing].modeled_issuance_latency
    assert inputs.modeled_issued_at == issued == equity_modeled_issuance(decided, latency)
    assert decided.compare(issued) <= 0
    openings = audit.candidate.entry_bar_openings()
    assert openings[0] == ist(9, 15) and openings[-1] == ist(14, 55)  # the research window
    if eligible is None:
        assert audit.status is _S.NOT_ELIGIBLE
        assert audit.reasons == (_R.NO_PERMISSIBLE_ENTRY_BAR,)
        assert audit.entry_window is None and audit.proposal is None
        return
    assert audit.status is _S.PROPOSED
    timing_ = audit.entry_window.timing  # type: ignore[union-attr]
    assert (timing_.decision_as_of, timing_.issued_at, timing_.eligible_from) == (
        decided,
        issued,
        eligible,
    )
    assert timing_.issuance_kind.value == "MODELED"
    assert eligible in openings and eligible.compare(issued) >= 0
    earlier = [o for o in openings if o.compare(eligible) < 0]
    assert all(o.compare(issued) < 0 for o in earlier)  # no full bar began after issuance
    assert eligible.compare(ist(10, 45)) <= 0  # by the latest entry-bar opening
    window = audit.entry_window
    assert window is not None and window.modeled_exit_at == ist(14, 0)
    proposal = _document(equity_recommendation_disposition(audit).proposal_document)  # type: ignore[arg-type,union-attr]
    assert json.dumps(proposal).count(eligible.value) >= 1


def test_e13_a_signal_after_the_policy_cutoff_is_refused(
    pipeline: Pipeline, research: Research
) -> None:
    # Synthetic: the latest signal bar must end by 09:35; the candidate's ended 09:40.
    pipeline.references.timing["timing/early-cutoff"] = replace(
        timing_policy("timing/early-cutoff", timedelta(seconds=150)),
        latest_signal_bar_end=time(9, 35),
    )
    audit = pipeline.generate(timing="timing/early-cutoff", set_version="set/early-cutoff")
    assert audit.status is _S.NOT_ELIGIBLE and audit.reasons == (_R.AFTER_SIGNAL_CUTOFF,)
    assert audit.generation_key is not None and audit.proposal is None


# ----------------------------------------------------------------------
# e14. Point-in-time issued history
# ----------------------------------------------------------------------


def test_e14_an_explicit_synthetic_empty_history_is_frozen_with_its_coverage(
    pipeline: Pipeline, research: Research
) -> None:
    audit = pipeline.generate()
    assert audit.status is _S.PROPOSED and audit.inputs is not None
    history = audit.inputs.issued_history
    assert (history.reference, history.as_of, history.complete_through) == (
        "synthetic-ei4e-issued-history/1",
        ist(9, 43, 30),
        ist(15, 30),
    )
    assert (history.recording_watermark, history.recommendations) == (0, ())
    assert pipeline.history.asked == [ist(9, 43, 30)]


def test_e14_only_records_issued_by_the_modeled_issuance_are_evidence(
    pipeline: Pipeline, research: Research
) -> None:
    # Recording order differs from event order: WIPRO was recorded second but issued first.
    pipeline.history = History(
        records=(
            prior("wipro", WIPRO, ist(9, 25), 2),
            prior("tcs", TCS, ist(9, 30), 1),
            prior("after", TCS, ist(11, 0), 3),  # issued after the modeled issuance
        )
    )
    audit = pipeline.generate()
    assert audit.inputs is not None
    history = audit.inputs.issued_history
    assert [r.recommendation_key for r in history.recommendations] == [
        "synthetic-ei4e-issued-wipro",
        "synthetic-ei4e-issued-tcs",
    ]
    assert history.recording_watermark == 3
    assert audit.status is _S.EVALUATION_UNRESOLVED
    assert audit.reasons == (_R.EXCLUSIVITY_POLICY_UNRESOLVED, _R.EXPOSURE_POLICY_UNRESOLVED)
    pipeline.history = History(records=(prior("after", TCS, ist(11, 0), 1),))
    future_only = pipeline.generate()
    assert future_only.status is _S.PROPOSED  # a later issuance never reaches back


def test_e14_missing_incomplete_and_contradictory_history_fail_closed(
    pipeline: Pipeline, research: Research
) -> None:
    for history, reason in (
        (History(mode="missing"), _R.OPEN_RECOMMENDATION_STATE_UNAVAILABLE),  # not empty
        (History(complete_through=ist(9, 43)), _R.RECOMMENDATION_HISTORY_INCOMPLETE),
        (History(mode="other-session"), _R.OPEN_RECOMMENDATION_STATE_UNAVAILABLE),
    ):
        pipeline.history = history
        audit = pipeline.generate()
        assert audit.status is _S.DEPENDENCY_UNAVAILABLE and audit.reasons == (reason,)
        assert audit.generation_key is None and audit.proposal is None
    with pytest.raises(InvalidEquityRecommendationReferenceError, match="after it is complete"):
        EquitySessionRecommendationHistory(
            NSE, DAY, (prior("x", TCS, ist(11, 0), 1),), ist(10, 0), 1, "synthetic-ei4e"
        )
    with pytest.raises(InvalidEquityRecommendationReferenceError, match="recording watermark"):
        EquitySessionRecommendationHistory(
            NSE, DAY, (prior("x", TCS, ist(9, 30), 2),), ist(15, 30), 1, "synthetic-ei4e"
        )


def test_e14_recording_never_writes_history_and_never_blocks_its_own_retry(
    pipeline: Pipeline, research: Research
) -> None:
    before = _evidence_objects(pipeline.database)
    assert pipeline.recorder().execute(pipeline.generate()).outcome is _O.COMMITTED
    asked = list(pipeline.history.asked)
    assert pipeline.history.records == ()  # the synthetic history was never written
    retry = pipeline.generate()
    assert (
        _check(retry, _CHECK.OPEN_RECOMMENDATIONS).status is EquityRecommendationCheckStatus.PASSED
    )
    assert pipeline.recorder().execute(retry).outcome is _O.EQUIVALENT_COMMITTED
    assert pipeline.history.asked == [*asked, ist(9, 43, 30)]  # generation asked; recording never
    added = set(schema_objects(pipeline.database)) - set(before)
    tokens = {token for name in added for token in name.split("_")}
    assert not tokens & {"issued", "issuance", "history", "lifecycle", "order", "broker"}


# ----------------------------------------------------------------------
# e15. Restart and historical replay, for every durable kind
# ----------------------------------------------------------------------


def test_e15_every_disposition_kind_replays_after_restart_from_frozen_evidence(
    tmp_path: Path,
) -> None:
    database = tmp_path / "before-restart.sqlite3"
    pipeline = Pipeline(database)
    research = establish_research(pipeline)
    committed = {}
    for kind, changes, history in (
        (_K.PROPOSED, {}, History()),
        (
            _K.NOT_ELIGIBLE,
            {"generation": "generation/strict", "set_version": "set/strict"},
            History(),
        ),
        (
            _K.UNRESOLVED,
            {"set_version": "set/2"},
            History(records=(prior("p", TCS, ist(9, 30), 1),)),
        ),
    ):
        pipeline.history = history
        audit = pipeline.generate(**changes)
        disposition = equity_recommendation_disposition(audit)
        assert disposition is not None and disposition.kind is kind
        assert pipeline.recorder().execute(audit).outcome is _O.COMMITTED
        committed[kind] = (disposition, equity_recommendation_attempt(audit))
    assert counts(database) == (3, 3)

    # 1. Close everything: no object of the first 'process' survives, and on Windows the
    # file can be moved only when no connection holds it open.
    del pipeline, research
    gc.collect()
    moved = tmp_path / "after-restart.sqlite3"
    database.rename(moved)
    copy = tmp_path / "copied.sqlite3"
    shutil.copyfile(moved, copy)  # rollback journal: the file alone is the database

    # 2-3. Reopen the copy with fresh adapters, and make every live reference unavailable.
    restarted = Pipeline(copy)
    restarted.references.available = False
    restarted.eligibility.available = False
    restarted.history = History(mode="unavailable")
    down = restarted.generate()
    assert down.status is _S.DEPENDENCY_UNAVAILABLE  # the references really are down
    asked = (restarted.references.asked, restarted.eligibility.asked, len(restarted.history.asked))
    store = SQLiteEquityRecommendationGenerationStore(copy)
    fresh = restarted.evidence()
    for kind, (original, attempt) in committed.items():
        # 4-6. Load, decode canonical documents, verify fingerprints and the evaluator.
        loaded = store.load_disposition(original.generation_key)
        assert loaded == original and loaded is not None and loaded.kind is kind
        assert loaded.evaluator_version == EQUITY_RECOMMENDATION_EVALUATOR_VERSION
        assert verify_equity_recommendation_document(
            loaded.assessment_document, loaded.assessment_fingerprint
        ) == _document(original.assessment_document)
        if loaded.proposal_document is not None:
            verify_equity_recommendation_document(
                loaded.proposal_document,
                loaded.proposal_fingerprint,  # type: ignore[arg-type]
            )
        assert store.load_attempt(attempt.attempt_identity) == attempt
        # 7. Rehydrate the original EI-3C candidate.
        found = establish_equity_recommendation_candidate(
            loaded.request, fresh.timelines, fresh.snapshots
        )
        assert found.provenance == loaded.candidate  # type: ignore[union-attr]
        # 8. The original EI-2B snapshot and manifest.
        snapshot = fresh.snapshots.get_snapshot(loaded.candidate.source.evidence_snapshot_id)
        assert isinstance(snapshot, EquityCandleEvidenceSnapshot)
        assert snapshot.manifest_fingerprint == loaded.candidate.manifest_fingerprint
        # 9-10. Replay from the frozen references alone, and compare.
        replayed = assess_equity_recommendation_inputs(loaded.request, loaded.inputs)
        assert replayed.assessment_document() == loaded.assessment.document
        assert replayed.assessment_fingerprint == loaded.assessment_fingerprint
        verified = restarted.verify_stored().execute(loaded.generation_key)
        assert verified is not None
        assert (
            verified.document_integrity_verified,
            verified.source_integrity_verified,
            verified.evaluation_reproduced,
            verified.issuance_established,
        ) == (True, True, True, False)
    assert asked == (
        restarted.references.asked,
        restarted.eligibility.asked,
        len(restarted.history.asked),
    )  # replay consulted no live reference, eligibility or history source
    check_database(copy)


# ----------------------------------------------------------------------
# e16. A later candle correction never changes a stored generation
# ----------------------------------------------------------------------


def test_e16_a_later_correction_leaves_the_original_candidate_and_assessment(
    pipeline: Pipeline, research: Research
) -> None:
    audit = pipeline.generate()
    disposition = equity_recommendation_disposition(audit)
    assert disposition is not None
    assert pipeline.recorder().execute(audit).outcome is _O.COMMITTED
    entries_before = rows(pipeline.database, ENTRIES)
    stored_before = rows(pipeline.database, DISPOSITIONS)

    evidence = pipeline.evidence()
    evidence.correct_bar_2()  # a valid later source revision, journaled through EI-2C
    evidence.candles({7: ("254.5", "255.5", "254", "255")})  # bar 7, so only the revision differs
    s51 = evidence.issue(ist(9, 51))
    advanced = evidence.advance([research.s36, research.s41, research.s46, s51])
    assert advanced.status.value == "COMMITTED"
    (rejected,) = advanced.appended
    assert rejected.role is _ROLE.REJECTED
    assert rejected.decision.reasons == (
        EquityIntradayResearchRejection.UNRESOLVED_SOURCE_REVISION,
    )
    assert advanced.timeline is not None and advanced.timeline.candidate is not None
    assert advanced.timeline.candidate.snapshot_id == research.s41.snapshot_id  # preserved
    assert rows(pipeline.database, ENTRIES)[:3] == entries_before  # append-only

    verified = pipeline.verify_stored().execute(disposition.generation_key)
    assert verified is not None and verified.evaluation_reproduced
    assert verified.disposition.candidate.source.evidence_snapshot_id == research.s41.snapshot_id
    assert verified.disposition.candidate.source.evidence_snapshot_id != s51.snapshot_id
    assert verified.replayed.assessment_fingerprint == disposition.assessment_fingerprint
    assert rows(pipeline.database, DISPOSITIONS) == stored_before  # never rewritten
    regenerated = pipeline.generate()
    assert regenerated.assessment_fingerprint == disposition.assessment_fingerprint
    assert pipeline.recorder().execute(regenerated).outcome is _O.EQUIVALENT_COMMITTED
    assert counts(pipeline.database) == (1, 1)


# ----------------------------------------------------------------------
# e22. Compatibility with the EI-2B and EI-3C evidence in one database
# ----------------------------------------------------------------------


def test_e22_recording_adds_its_schema_and_never_alters_earlier_evidence(
    pipeline: Pipeline, research: Research
) -> None:
    objects = _evidence_objects(pipeline.database)
    evidence_rows = {
        table: rows(pipeline.database, table)
        for table in ("equity_candle_journal", SNAPSHOTS, TIMELINES, ENTRIES)
    }
    assert _generation_objects(pipeline.database) == {}  # an EI-3C database, not yet EI-4D
    for changes in ({}, {"generation": "generation/strict", "set_version": "set/strict"}):
        assert pipeline.recorder().execute(pipeline.generate(**changes)).outcome is _O.COMMITTED
    assert _evidence_objects(pipeline.database) == objects  # indexes and triggers intact
    for table, before in evidence_rows.items():
        assert rows(pipeline.database, table) == before, table
    generation = _generation_objects(pipeline.database)
    assert {n for n, (kind, _) in generation.items() if kind == "table"} == _GENERATION
    assert sum(kind == "trigger" for kind, _ in generation.values()) >= 8
    # A previously initialized database: reopened, nothing is recreated or changed.
    snapshot_of_schema = schema_objects(pipeline.database)
    assert pipeline.store().load_disposition(pipeline.generate().generation_key) is not None  # type: ignore[arg-type]
    assert schema_objects(pipeline.database) == snapshot_of_schema
    check_database(pipeline.database)


def test_e22_the_store_never_creates_or_adopts_a_database_without_the_evidence(
    tmp_path: Path,
) -> None:
    absent = tmp_path / "absent.sqlite3"
    with pytest.raises(EquityRecommendationGenerationStoreUnavailableError):
        SQLiteEquityRecommendationGenerationStore(absent).load_disposition(
            "eirk1-sha256:" + "0" * 64
        )
    assert not absent.exists()
    empty = tmp_path / "empty.sqlite3"
    sqlite3.connect(empty).close()
    with pytest.raises(EquityRecommendationGenerationStoreUnavailableError):
        SQLiteEquityRecommendationGenerationStore(empty).load_disposition(
            "eirk1-sha256:" + "0" * 64
        )
    assert schema_objects(empty) == {}  # no generation tables beside missing evidence
