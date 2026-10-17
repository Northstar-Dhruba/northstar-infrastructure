"""Tests for atomic, insert-only futures recommendation generation audits (F-5B-2B).

Synthetic data only: the contracts, sessions, instants, levels, identities,
policy and profile are non-production fixtures. Nothing here reads a provider.
"""

from __future__ import annotations

import ast
import dataclasses
import json
import sqlite3
import threading
from contextlib import closing
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from northstar_application.application_services import (
    FuturesCandidateRejectionAudit,
    FuturesGenerationRejectionAudit,
    FuturesRecommendationGenerationAudit,
    FuturesResearchHistoryCoverage,
    FuturesResearchHistoryDeficiency,
    FuturesTradeRecommendationBlockReason,
    FuturesTradeRecommendationGenerationProfile,
    FuturesTradeRecommendationGenerationStatus,
    FuturesTradeRecommendationIdentityService,
    InvalidFuturesRecommendationGenerationAuditError,
)
from northstar_application.ports import (
    FuturesDailyBarFinality,
    FuturesDailyBarFinalityOutcome,
    FuturesRecommendationGenerationAuditConflictError,
    FuturesRecommendationGenerationAuditRepository,
    FuturesRecommendationGenerationAuditStore,
    FuturesTradeRecommendationConfigurationConflictError,
    FuturesTradeRecommendationConflictError,
    FuturesTradeRecommendationExclusivityError,
    FuturesTradeRecommendationTimeExpiryStep,
    InvalidFuturesRecommendationGenerationAuditRecordingError,
)
from northstar_core.derivatives import ExpirationDate, QuoteValue
from northstar_core.foundation.value_objects import ExchangeCode, PointInTime, Symbol, Timeframe
from northstar_core.futures import FuturesContract, FuturesProductReference
from northstar_core.recommendations.futures import (
    CandidateRejectionReason,
    CoverageDeficiency,
    EntryZone,
    FuturesRecommendationGenerationPolicy,
    FuturesResearchDecisionReference,
    FuturesTradeRecommendation,
    FuturesTradeRecommendationLifecycle,
    GenerationRejectionReason,
    ObservationCoverage,
    PriceTarget,
    RecommendationPolicyVersion,
    RecommendationValidity,
    StopLoss,
    TimeExpiryEvidence,
    TradeDirection,
)
from northstar_core.strategy import ExplanationReason, RecommendationAction, StrategyIdentity

import northstar_infrastructure.persistence.sqlite_futures_recommendation_generation_audits as codec
import northstar_infrastructure.persistence.sqlite_futures_trade_recommendations as module
from northstar_infrastructure.persistence import (
    FuturesTradeRecommendationStorageError,
    SQLiteFuturesRecommendationGenerationAuditRepository,
    SQLiteFuturesRecommendationGenerationAuditStore,
    SQLiteFuturesTradeRecommendationLifecycleStore,
    SQLiteFuturesTradeRecommendationRepository,
    SQLiteFuturesTradeRecommendationStore,
)
from tests.futures_recommendation_audit_fixtures import SYNTHETIC_SESSION, recording_audit

Status = FuturesTradeRecommendationGenerationStatus
Block = FuturesTradeRecommendationBlockReason

_NIFTY = FuturesProductReference(Symbol("NIFTY"), ExchangeCode("NSE"))
_OCT = FuturesContract(_NIFTY, ExpirationDate("2026-10-27"))
_NOV = FuturesContract(_NIFTY, ExpirationDate("2026-11-24"))
_STRATEGY = StrategyIdentity("synthetic-directional-v1")
_POLICY = FuturesRecommendationGenerationPolicy(
    version=RecommendationPolicyVersion("f4-synthetic-fixture-v1"),
    approved_strategies=(_STRATEGY,),
    lookback_changes=19,
    entry_multiplier=Decimal("0.25"),
    stop_multiplier=Decimal("1.5"),
    target_multiplier=Decimal("3"),
    price_increment=Decimal("0.05"),
    min_reward_to_risk=Decimal("1.5"),
    short_enabled=True,
)
_PROFILE = FuturesTradeRecommendationGenerationProfile(
    "f5a-synthetic-profile-v1", 5, _POLICY.version
)
_T1 = "2026-10-07T12:00:00Z"
_T2 = "2026-10-07T12:30:00Z"
_AUDITS = "futures_recommendation_generation_audits"
_REFERENCES = "futures_recommendation_generation_audit_recommendations"
_DEFINITIONS = "futures_trade_recommendations"
_CONFIGURATIONS = "futures_recommendation_configurations"


def _q(value: str) -> QuoteValue:
    return QuoteValue(Decimal(value))


def _recommendation(
    contract: FuturesContract = _OCT,
    *,
    issued_at: str = _T1,
    decided: str = "2026-10-07T10:00:00Z",
    profile: FuturesTradeRecommendationGenerationProfile = _PROFILE,
) -> FuturesTradeRecommendation:
    source = FuturesResearchDecisionReference(
        contract, Timeframe("1d"), PointInTime(decided), _STRATEGY
    )
    return FuturesTradeRecommendation(
        identity=FuturesTradeRecommendationIdentityService().identity(source, profile),
        contract=contract,
        direction=TradeDirection.LONG,
        entry=EntryZone(_q("24378"), _q("24388")),
        stop=StopLoss(_q("24347.75")),
        target=PriceTarget(_q("24448.45")),
        validity=RecommendationValidity(
            PointInTime("2026-10-08T10:00:00Z"),
            PointInTime("2026-10-14T10:00:00Z"),
            PointInTime("2026-10-27T10:00:00Z"),
        ),
        source=source,
        policy_version=_POLICY.version,
        issued_at=PointInTime(issued_at),
        thesis=(ExplanationReason("Synthetic thesis", ("synthetic signal",)),),
    )


def _finality(contract: FuturesContract = _OCT, final: bool = True) -> FuturesDailyBarFinality:
    outcome = (
        FuturesDailyBarFinalityOutcome.FINAL if final else FuturesDailyBarFinalityOutcome.UNKNOWN
    )
    return FuturesDailyBarFinality(contract, SYNTHETIC_SESSION, outcome, "synthetic finality")


def _standalone(**overrides: object) -> FuturesRecommendationGenerationAudit:
    """A BLOCKED audit: no research for the session, at ``_T1`` unless overridden."""
    values: dict[str, object] = {
        "contract": _OCT,
        "strategy": _STRATEGY,
        "policy_version": _POLICY.version,
        "profile_version": _PROFILE.version,
        "attempted_at": PointInTime(_T1),
        "status": Status.BLOCKED,
        "block_reason": Block.NO_RESEARCH_FOR_SESSION,
        "session": SYNTHETIC_SESSION,
        "finality": _finality(),
    }
    values.update(overrides)
    return FuturesRecommendationGenerationAudit(**values)  # type: ignore[arg-type]


def _existing(recommendation: FuturesTradeRecommendation, attempted_at: str = _T2):
    return _standalone(
        status=Status.COMPLETED,
        block_reason=None,
        attempted_at=PointInTime(attempted_at),
        candidates=(recommendation.source,),
        existing=(recommendation.identity,),
    )


@pytest.fixture
def database(tmp_path: Path) -> Path:
    return tmp_path / "northstar.sqlite3"


def _record(
    database: Path,
    *recommendations: FuturesTradeRecommendation,
    audit: FuturesRecommendationGenerationAudit | None = None,
    profile: FuturesTradeRecommendationGenerationProfile = _PROFILE,
) -> int:
    return SQLiteFuturesTradeRecommendationStore(database).record(
        recommendations,
        policy=_POLICY,
        profile=profile,
        audit=audit or recording_audit(*recommendations, profile=profile),
    )


def _audits(database: Path) -> SQLiteFuturesRecommendationGenerationAuditStore:
    return SQLiteFuturesRecommendationGenerationAuditStore(database)


def _read(database: Path, identity: str) -> FuturesRecommendationGenerationAudit | None:
    return SQLiteFuturesRecommendationGenerationAuditRepository(database).get_audit(identity)


def _rows(database: Path, table: str) -> list[tuple]:
    with closing(sqlite3.connect(database)) as connection:
        return connection.execute(f"SELECT * FROM {table} ORDER BY 1, 2").fetchall()  # noqa: S608


def _execute(database: Path, sql: str, parameters: tuple = ()) -> None:
    with closing(sqlite3.connect(database)) as connection:
        connection.execute(sql, parameters)
        connection.commit()


def _everything(database: Path) -> tuple:
    return tuple(
        _rows(database, table)
        for table in (
            _CONFIGURATIONS,
            _DEFINITIONS,
            "futures_trade_recommendation_steps",
            _AUDITS,
            _REFERENCES,
        )
    )


def _expire(database: Path, recommendation: FuturesTradeRecommendation) -> None:
    """Make ``recommendation`` terminal with an incomplete-coverage time expiry."""
    step = FuturesTradeRecommendationTimeExpiryStep(
        TimeExpiryEvidence(
            as_of=PointInTime("2026-10-14T12:00:00Z"),
            evaluated_through=None,
            coverage=ObservationCoverage.INCOMPLETE_OR_UNVERIFIED_COVERAGE,
            attested_by="synthetic coverage fixture",
            deficiency=CoverageDeficiency.MISSING_OBSERVATIONS,
        )
    )
    initial = FuturesTradeRecommendationLifecycle(recommendation)
    SQLiteFuturesTradeRecommendationLifecycleStore(database).record_progress(
        initial, initial.expire(step.evidence), steps=(step,)
    )


# ---------------------------------------------------------------------------
# Atomic recording: definitions and their RECORDED audit (I-2)
# ---------------------------------------------------------------------------


def test_a_new_recommendation_commits_with_its_audit(database: Path) -> None:
    recommendation = _recommendation()
    audit = recording_audit(recommendation, profile=_PROFILE)

    assert _record(database, recommendation, audit=audit) == 1

    assert len(_rows(database, _DEFINITIONS)) == 1
    ((row),) = _rows(database, _AUDITS)
    assert row[:13] == (
        audit.identity,
        1,
        "NIFTY",
        "NSE",
        "2026-10-27",
        "synthetic-directional-v1",
        "f4-synthetic-fixture-v1",
        "f5a-synthetic-profile-v1",
        _T1,
        "2026-10-07",
        "COMPLETED",
        None,
        "RECORDED",
    )
    assert _rows(database, _REFERENCES) == [
        (audit.identity, recommendation.identity.identity, "RECORDED")
    ]
    assert _read(database, audit.identity) == audit


def test_the_audit_names_exactly_the_committed_definition(database: Path) -> None:
    recommendation = _recommendation()
    _record(database, recommendation)

    ((audit_row),) = _rows(database, _AUDITS)
    stored = _read(database, audit_row[0])
    definition = SQLiteFuturesTradeRecommendationRepository(database).get_recommendation(
        recommendation.identity
    )
    assert stored is not None and stored.recorded == (definition.identity,)
    assert stored.attempted_at == definition.issued_at


def test_a_failing_audit_insert_commits_no_definition(database: Path) -> None:
    _record(database, _recommendation(_NOV))  # create the schema and one committed recording
    _expire(database, _recommendation(_NOV))
    before = _everything(database)
    _execute(
        database,
        f"CREATE TRIGGER fail_audit BEFORE INSERT ON {_AUDITS} "
        "BEGIN SELECT RAISE(ABORT, 'synthetic audit failure'); END",
    )

    with pytest.raises(FuturesTradeRecommendationStorageError):
        _record(database, _recommendation(_OCT))
    assert _everything(database) == before


def test_a_failing_definition_insert_commits_no_audit(database: Path) -> None:
    _record(database, _recommendation(_NOV))
    _expire(database, _recommendation(_NOV))
    before = _everything(database)
    _execute(
        database,
        f"CREATE TRIGGER fail_definition BEFORE INSERT ON {_DEFINITIONS} "
        "BEGIN SELECT RAISE(ABORT, 'synthetic definition failure'); END",
    )

    with pytest.raises(FuturesTradeRecommendationStorageError):
        _record(database, _recommendation(_OCT))
    assert _everything(database) == before


def test_a_failing_reference_insert_commits_nothing(database: Path) -> None:
    with closing(sqlite3.connect(database)) as connection:
        module.initialize_futures_trade_recommendation_schema(connection)
    _execute(
        database,
        f"CREATE TRIGGER fail_reference BEFORE INSERT ON {_REFERENCES} "
        "BEGIN SELECT RAISE(ABORT, 'synthetic reference failure'); END",
    )

    with pytest.raises(FuturesTradeRecommendationStorageError):
        _record(database, _recommendation())
    assert _rows(database, _DEFINITIONS) == _rows(database, _AUDITS) == []
    assert _rows(database, _CONFIGURATIONS) == []


def test_a_configuration_conflict_registers_and_audits_nothing(database: Path) -> None:
    _record(database, _recommendation(_NOV))
    _expire(database, _recommendation(_NOV))
    before = _everything(database)
    reused = FuturesTradeRecommendationGenerationProfile(_PROFILE.version, 7, _POLICY.version)

    with pytest.raises(FuturesTradeRecommendationConfigurationConflictError):
        _record(database, _recommendation(_OCT, profile=reused), profile=reused)
    assert _everything(database) == before


def test_a_cross_expiry_exclusivity_conflict_audits_nothing(database: Path) -> None:
    _record(database, _recommendation(_OCT))
    before = _everything(database)

    with pytest.raises(FuturesTradeRecommendationExclusivityError):
        _record(database, _recommendation(_NOV, issued_at=_T2))
    assert _everything(database) == before


def test_replaying_the_same_committed_recording_is_a_no_op(database: Path) -> None:
    recommendation = _recommendation()
    _record(database, recommendation)
    before = _everything(database)

    assert _record(database, recommendation) == 1
    assert _everything(database) == before


def test_another_attempt_cannot_claim_an_equal_recorded_definition(database: Path) -> None:
    recommendation = _recommendation()
    _record(database, recommendation)
    before = _everything(database)
    other = recording_audit(recommendation, profile=_PROFILE, candidates=())

    with pytest.raises(FuturesTradeRecommendationConflictError):
        _record(database, recommendation, audit=other)
    assert _everything(database) == before


def test_a_losing_attempt_claims_nothing_and_audits_the_winner_as_existing(
    database: Path,
) -> None:
    winner, loser = _recommendation(issued_at=_T1), _recommendation(issued_at=_T2)
    assert winner.identity == loser.identity
    _record(database, winner)
    before = _everything(database)

    with pytest.raises(FuturesTradeRecommendationConflictError):
        _record(database, loser)
    assert _everything(database) == before

    lost = _standalone(
        status=Status.COMPLETED,
        block_reason=None,
        attempted_at=PointInTime(_T2),
        history_coverage=FuturesResearchHistoryCoverage.VERIFIED,
        candidates=(loser.source,),
        proposed=(loser.identity,),
        existing=(winner.identity,),
    )
    assert _audits(database).record_audit(lost) == 1
    assert sorted(row[12] for row in _rows(database, _AUDITS)) == [
        "EXISTING_RETURNED",
        "RECORDED",
    ]
    assert _read(database, lost.identity) == lost
    references = {(row[1], row[2]) for row in _rows(database, _REFERENCES)}
    assert references == {
        (winner.identity.identity, "RECORDED"),
        (winner.identity.identity, "EXISTING"),
    }


def test_a_recorded_audit_identity_already_stored_differently_is_a_conflict(
    database: Path,
) -> None:
    first = _recommendation(decided="2026-10-07T10:00:00Z")
    _record(database, first)
    _expire(database, first)
    before = _everything(database)
    # Another recording by the same attempt key: same contract, configuration,
    # instant and session, so the same audit identity with different content.
    second = _recommendation(decided="2026-10-06T10:00:00Z")
    assert (
        recording_audit(second, profile=_PROFILE).identity
        == recording_audit(first, profile=_PROFILE).identity
    )

    with pytest.raises(FuturesRecommendationGenerationAuditConflictError):
        _record(database, second)
    assert _everything(database) == before


def test_a_recorded_audit_naming_an_unrecorded_existing_identity_is_refused(
    database: Path,
) -> None:
    recommendation = _recommendation()
    phantom = _recommendation(_OCT, decided="2026-10-06T10:00:00Z")
    audit = recording_audit(recommendation, profile=_PROFILE, existing=(phantom.identity,))

    with pytest.raises(InvalidFuturesRecommendationGenerationAuditRecordingError):
        _record(database, recommendation, audit=audit)
    assert _rows(database, _DEFINITIONS) == _rows(database, _AUDITS) == []


# ---------------------------------------------------------------------------
# Standalone audits: attempts that recorded nothing new
# ---------------------------------------------------------------------------


def test_a_blocked_audit_is_recorded_once_without_registering_anything(database: Path) -> None:
    audit = _standalone()

    assert _audits(database).record_audit(audit) == 1
    assert _audits(database).record_audit(audit) == 0

    assert len(_rows(database, _AUDITS)) == 1
    assert _rows(database, _CONFIGURATIONS) == _rows(database, _DEFINITIONS) == []
    assert _rows(database, _REFERENCES) == []
    assert _read(database, audit.identity) == audit


def test_the_same_standalone_identity_with_other_content_is_a_conflict(database: Path) -> None:
    _audits(database).record_audit(_standalone())
    before = _everything(database)
    changed = _standalone(finality=dataclasses.replace(_finality(), reason="another reason"))

    with pytest.raises(FuturesRecommendationGenerationAuditConflictError):
        _audits(database).record_audit(changed)
    assert _everything(database) == before


def test_independent_blocked_attempts_remain_distinguishable(database: Path) -> None:
    audits = (
        _standalone(),
        _standalone(attempted_at=PointInTime(_T2)),
        _standalone(block_reason=Block.SESSION_NOT_OPERATOR_REVIEWED),
        _standalone(block_reason=Block.SESSION_NOT_FINAL, finality=_finality(final=False)),
        _standalone(block_reason=Block.NO_PERSISTED_SESSION, session=None, finality=None),
        _standalone(profile_version="f5a-synthetic-profile-v2"),
    )

    for audit in audits:
        assert _audits(database).record_audit(audit) == 1

    assert len({audit.identity for audit in audits}) == len(_rows(database, _AUDITS)) == 6
    assert all(_read(database, audit.identity) == audit for audit in audits)


def test_a_recorded_audit_cannot_be_recorded_on_its_own(database: Path) -> None:
    with pytest.raises(InvalidFuturesRecommendationGenerationAuditRecordingError):
        _audits(database).record_audit(recording_audit(_recommendation(), profile=_PROFILE))
    assert not database.exists() or _rows(database, _AUDITS) == []


@pytest.mark.parametrize(
    "recorded_under", ["nothing", "another-profile"], ids=["unrecorded", "other-profile"]
)
def test_an_existing_reference_must_be_recorded_for_the_audited_attempt(
    database: Path, recorded_under: str
) -> None:
    recommendation = _recommendation()
    if recorded_under == "another-profile":
        v2 = FuturesTradeRecommendationGenerationProfile(
            "f5a-synthetic-profile-v2", 5, _POLICY.version
        )
        recommendation = _recommendation(profile=v2)
        _record(database, recommendation, profile=v2)
    before = _everything(database) if database.exists() else None

    with pytest.raises(InvalidFuturesRecommendationGenerationAuditRecordingError):
        _audits(database).record_audit(_existing(recommendation))
    if before is not None:
        assert _everything(database) == before


def test_a_retry_returning_an_existing_recommendation_is_audited_truthfully(
    database: Path,
) -> None:
    recommendation = _recommendation()
    _record(database, recommendation)
    definitions = _rows(database, _DEFINITIONS)

    assert _audits(database).record_audit(_existing(recommendation)) == 1

    assert _rows(database, _DEFINITIONS) == definitions
    outcomes = sorted(row[12] for row in _rows(database, _AUDITS))
    assert outcomes == ["EXISTING_RETURNED", "RECORDED"]
    recorded_refs = [row for row in _rows(database, _REFERENCES) if row[2] == "RECORDED"]
    assert len(recorded_refs) == 1


# ---------------------------------------------------------------------------
# Reconstruction and fail-closed reads
# ---------------------------------------------------------------------------


def _rich_rejected_audit() -> FuturesRecommendationGenerationAudit:
    source = _recommendation().source
    hold = dataclasses.replace(source, strategy_identity=StrategyIdentity("synthetic-other-v1"))
    return _standalone(
        status=Status.COMPLETED,
        block_reason=None,
        history_coverage=FuturesResearchHistoryCoverage.VERIFIED,
        candidates=(source,),
        candidate_rejections=(
            FuturesCandidateRejectionAudit(
                hold, RecommendationAction("HOLD"), CandidateRejectionReason.UNAPPROVED_STRATEGY
            ),
        ),
        generation_rejections=(
            FuturesGenerationRejectionAudit(
                source, GenerationRejectionReason.CONTRADICTS_OPEN_RECOMMENDATION
            ),
        ),
        exclusivity_breaches=(_NIFTY,),
    )


def _unverified_audit() -> FuturesRecommendationGenerationAudit:
    return _standalone(
        block_reason=Block.RESEARCH_HISTORY_NOT_VERIFIED,
        history_coverage=FuturesResearchHistoryCoverage.UNVERIFIED,
        history_deficiency=FuturesResearchHistoryDeficiency.UNAPPROVED_CANDLE,
        history_deficient_sessions=(date(2026, 9, 30), date(2026, 10, 1)),
        candidates=(_recommendation().source,),
    )


@pytest.mark.parametrize("build", [_standalone, _rich_rejected_audit, _unverified_audit])
def test_a_standalone_audit_reconstructs_exactly_after_restart(database: Path, build) -> None:
    audit = build()
    _audits(database).record_audit(audit)

    restarted = SQLiteFuturesRecommendationGenerationAuditRepository(str(database))
    stored = restarted.get_audit(audit.identity)

    assert stored == audit
    assert stored.outcome is audit.outcome and stored.operator_review is audit.operator_review
    ((row),) = _rows(database, _AUDITS)
    assert row[-1] == codec.audit_json(audit)
    assert json.loads(row[-1])["identity"] == audit.identity


def test_an_unknown_audit_is_none(database: Path) -> None:
    assert _read(database, "f" * 64) is None


def test_an_audit_identity_must_be_text(database: Path) -> None:
    with pytest.raises(TypeError):
        SQLiteFuturesRecommendationGenerationAuditRepository(database).get_audit(1)  # type: ignore[arg-type]


def test_the_canonical_json_is_byte_stable() -> None:
    audit = _rich_rejected_audit()
    text = codec.audit_json(audit)

    assert text == json.dumps(json.loads(text), sort_keys=True, separators=(",", ":"))
    assert codec.audit_json(codec._decode(text)) == text

    def numbers(value: object) -> list[object]:
        if isinstance(value, dict):
            return [n for item in value.values() for n in numbers(item)]
        if isinstance(value, list):
            return [n for item in value for n in numbers(item)]
        return [value] if isinstance(value, (int, float)) and not isinstance(value, bool) else []

    assert numbers(json.loads(text)) == [1]  # only the evidence version; no float, no price


def _recorded_with_existing_retry(database: Path) -> tuple[str, str]:
    recommendation = _recommendation()
    audit = recording_audit(recommendation, profile=_PROFILE)
    _record(database, recommendation, audit=audit)
    retry = _existing(recommendation)
    _audits(database).record_audit(retry)
    return audit.identity, retry.identity


_SET = f"UPDATE {_AUDITS} SET "


@pytest.mark.parametrize(
    "sql",
    [
        _SET + "audit = 'not json'",
        _SET + "audit = replace(audit, ',', ', ')",
        _SET + "audit = replace(audit, '\"version\":1', '\"version\":2'), evidence_version = 2",
        _SET + 'audit = replace(audit, \'"outcome":"RECORDED"\', \'"outcome":"REJECTED"\')',
        _SET + 'audit = replace(audit, \'"operator_review":"REVIEWED"\', '
        '\'"operator_review":"NOT_REVIEWED"\')',
        _SET + "attempted_at = '2026-10-07T12:00:01Z'",
        _SET + "session_date = '2026-10-06'",
        _SET + "profile_version = 'f5a-synthetic-profile-v2'",
        _SET + "audit = replace(audit, '2026-10-07T12:00:00Z', '2026-10-07T17:30:00+05:30')",
        _SET + "audit = replace(audit, '\"2026-10-07\"', '\"20261007\"')",
        f"DELETE FROM {_REFERENCES} WHERE role = 'RECORDED'",
        f"UPDATE {_REFERENCES} SET role = 'EXISTING' WHERE role = 'RECORDED'",
        f"DELETE FROM {_DEFINITIONS}",
    ],
    ids=[
        "corrupt-json",
        "non-canonical-json",
        "unknown-version",
        "tampered-outcome",
        "tampered-review",
        "column-attempt-instant",
        "column-session",
        "column-profile",
        "non-canonical-instant",
        "non-canonical-date",
        "missing-reference",
        "tampered-reference-role",
        "referenced-definition-gone",
    ],
)
def test_tampered_audit_evidence_fails_closed(database: Path, sql: str) -> None:
    recorded, retry = _recorded_with_existing_retry(database)
    _execute(database, sql)
    before = _everything(database)

    with pytest.raises(FuturesTradeRecommendationStorageError):
        _read(database, recorded)
    if "DELETE FROM futures_trade_recommendations" in sql:
        with pytest.raises(FuturesTradeRecommendationStorageError):
            _read(database, retry)
    assert _everything(database) == before


def test_an_audit_stored_under_another_identity_fails_closed(database: Path) -> None:
    audit = _standalone()
    _audits(database).record_audit(audit)
    _execute(database, _SET + "audit_identity = ?", ("e" * 64,))

    with pytest.raises(FuturesTradeRecommendationStorageError):
        _read(database, "e" * 64)


def test_a_stored_audit_that_violates_its_invariants_fails_closed(database: Path) -> None:
    audit = _standalone()
    _audits(database).record_audit(audit)
    _execute(database, _SET + 'audit = replace(audit, \'"session":{\', \'"session":null,"x":{\')')

    with pytest.raises(FuturesTradeRecommendationStorageError):
        _read(database, audit.identity)


def test_a_stored_audit_breaking_an_audit_invariant_fails_closed(database: Path) -> None:
    audit = _standalone()
    _audits(database).record_audit(audit)
    _execute(
        database,
        _SET + "audit = replace(audit, '\"proposed\":[]', ?)",
        (f'"proposed":["{"a" * 64}"]',),
    )

    with pytest.raises(FuturesTradeRecommendationStorageError) as raised:
        _read(database, audit.identity)
    assert isinstance(raised.value.__cause__, InvalidFuturesRecommendationGenerationAuditError)


def test_a_corrupt_stored_audit_blocks_writes_under_its_identity(database: Path) -> None:
    audit = _standalone()
    _audits(database).record_audit(audit)
    _execute(database, _SET + "audit = 'not json'")

    with pytest.raises(FuturesTradeRecommendationStorageError):
        _audits(database).record_audit(audit)


def test_no_market_data_or_price_is_stored_in_audits(database: Path) -> None:
    _recorded_with_existing_retry(database)
    _audits(database).record_audit(_rich_rejected_audit())

    text = " ".join(str(value) for row in _rows(database, _AUDITS) for value in row)
    for fragment in (
        "24378",
        "24388",
        "24347",
        "24448",
        "open_value",
        "high_value",
        "low_value",
        "close_value",
        '"volume',
        '"high"',
        '"low"',
    ):
        assert fragment not in text, fragment
    assert "futures_ohlcv" not in text


def test_audits_never_touch_lifecycle_steps(database: Path) -> None:
    recommendation = _recommendation(_NOV)
    _record(database, recommendation)
    _expire(database, recommendation)
    steps = _rows(database, "futures_trade_recommendation_steps")

    _record(database, _recommendation(_OCT))
    _audits(database).record_audit(_standalone(attempted_at=PointInTime(_T2)))

    assert _rows(database, "futures_trade_recommendation_steps") == steps


# ---------------------------------------------------------------------------
# Concurrency: separate connections, serialised by BEGIN IMMEDIATE
# ---------------------------------------------------------------------------


def _race(database: Path, attempts: list) -> list[object]:
    """Run each attempt on its own thread and connection while a writer lock is held."""
    with closing(sqlite3.connect(database)) as connection:
        module.initialize_futures_trade_recommendation_schema(connection)
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


def test_concurrent_same_identity_recordings_leave_one_definition_and_its_audit(
    database: Path,
) -> None:
    first, second = _recommendation(issued_at=_T1), _recommendation(issued_at=_T2)

    outcomes = _race(
        database, [lambda: _record(database, first), lambda: _record(database, second)]
    )

    assert sorted(type(o).__name__ for o in outcomes) == [
        "FuturesTradeRecommendationConflictError",
        "int",
    ]
    winner = first if outcomes[0] == 1 else second
    stored = SQLiteFuturesTradeRecommendationRepository(database).get_recommendation(
        winner.identity
    )
    assert stored == winner
    ((row),) = _rows(database, _AUDITS)
    audit = _read(database, row[0])
    assert audit == recording_audit(winner, profile=_PROFILE)
    assert audit.attempted_at == stored.issued_at


def test_concurrent_replays_of_one_recording_commit_it_once(database: Path) -> None:
    recommendation = _recommendation()

    outcomes = _race(
        database,
        [lambda: _record(database, recommendation), lambda: _record(database, recommendation)],
    )

    assert outcomes == [1, 1]
    assert len(_rows(database, _DEFINITIONS)) == len(_rows(database, _AUDITS)) == 1


def test_concurrent_identical_standalone_audits_store_one(database: Path) -> None:
    audit = _standalone()

    outcomes = _race(
        database,
        [
            lambda: _audits(database).record_audit(audit),
            lambda: _audits(database).record_audit(audit),
        ],
    )

    assert sorted(outcomes) == [0, 1]  # type: ignore[type-var]
    assert len(_rows(database, _AUDITS)) == 1


def test_concurrent_cross_expiry_recordings_leave_one_audited_recommendation(
    database: Path,
) -> None:
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
    assert len(_rows(database, _DEFINITIONS)) == len(_rows(database, _AUDITS)) == 1


@pytest.mark.parametrize("write", ["record", "record_audit"])
def test_a_writer_that_cannot_get_the_lock_writes_nothing(
    database: Path, monkeypatch: pytest.MonkeyPatch, write: str
) -> None:
    with closing(sqlite3.connect(database)) as connection:
        module.initialize_futures_trade_recommendation_schema(connection)
    monkeypatch.setattr(module, "_BUSY_TIMEOUT_SECONDS", 0.2)
    holder = sqlite3.connect(database, isolation_level=None)
    holder.execute("BEGIN IMMEDIATE")
    try:
        with pytest.raises(FuturesTradeRecommendationStorageError):
            if write == "record":
                _record(database, _recommendation())
            else:
                _audits(database).record_audit(_standalone())
    finally:
        holder.rollback()
        holder.close()

    assert _everything(database) == ([], [], [], [], [])


# ---------------------------------------------------------------------------
# Architecture and hygiene
# ---------------------------------------------------------------------------


def _strings_and_imports(path: str) -> tuple[str, set[str]]:
    tree = ast.parse(Path(path).read_text(encoding="utf-8"))
    docstrings = {
        id(node.body[0].value)
        for node in ast.walk(tree)
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef))
        and node.body
        and isinstance(node.body[0], ast.Expr)
        and isinstance(node.body[0].value, ast.Constant)
    }
    strings = " ".join(
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and id(node) not in docstrings
    ).upper()
    imports = {
        node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.module
    } | {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    return strings, imports


def test_the_audit_codec_is_insert_only_and_provider_free() -> None:
    sql, imports = _strings_and_imports(codec.__file__)

    assert "INSERT INTO FUTURES_RECOMMENDATION_GENERATION_AUDITS" in sql
    assert "INSERT INTO FUTURES_RECOMMENDATION_GENERATION_AUDIT_RECOMMENDATIONS" in sql
    for forbidden in ("UPDATE ", "DELETE ", "REPLACE", "ON CONFLICT", "UPSERT", "JOURNAL_MODE"):
        assert forbidden not in sql
    for name in imports:
        assert name.split(".")[0] not in {"requests", "urllib", "socket", "time", "random", "uuid"}
        for fragment in ("upstox", "market_data", "paper_trading", "execution", "databento"):
            assert fragment not in name, name


def test_the_audit_adapters_implement_the_application_ports() -> None:
    assert issubclass(
        SQLiteFuturesRecommendationGenerationAuditStore, FuturesRecommendationGenerationAuditStore
    )
    assert issubclass(
        SQLiteFuturesRecommendationGenerationAuditRepository,
        FuturesRecommendationGenerationAuditRepository,
    )
