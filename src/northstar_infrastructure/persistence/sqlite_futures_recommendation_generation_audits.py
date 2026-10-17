"""Canonical encoding and verified reading of futures recommendation generation audits.

One row of ``futures_recommendation_generation_audits`` is one
FuturesRecommendationGenerationAudit, plus one row of
``futures_recommendation_generation_audit_recommendations`` per identity it
names as RECORDED or EXISTING. The recommendation store owns the transactions;
this module turns an audit into those rows, inserts them, and reads them back.

Evidence
--------
``audit`` is versioned canonical JSON -- sorted keys, no whitespace -- of every
field of the audit, including its derived outcome and operator-review
assessment. Instants are canonical UTC PointInTime text and dates are
YYYY-MM-DD; there are no prices and no floats. Research decisions are named by
their F-2 source reference, recommendations by identity. The constrained
columns repeat what queries and the database's checks need.

Every field of a row is a function of the audit, so a row is checked by
regenerating it: a read rebuilds the audit from its JSON -- which re-runs every
audit invariant -- re-encodes it, and requires the stored JSON, the columns,
the stored identity and the reference rows to be exactly what it produces.
Referenced recommendations must exist. Anything else raises ValueError,
TypeError or KeyError, which the store reports as a storage error; nothing is
repaired.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import date
from typing import Any

from northstar_application.application_services import (
    FuturesCandidateRejectionAudit,
    FuturesGenerationRejectionAudit,
    FuturesRecommendationGenerationAudit,
    FuturesResearchHistoryCoverage,
    FuturesResearchHistoryDeficiency,
    FuturesTradeRecommendationBlockReason,
    FuturesTradeRecommendationGenerationStatus,
)
from northstar_application.ports import (
    FuturesDailyBarFinality,
    FuturesDailyBarFinalityOutcome,
    FuturesTradingSession,
    InvalidFuturesRecommendationGenerationAuditRecordingError,
)
from northstar_core.derivatives import ExpirationDate
from northstar_core.foundation.value_objects import ExchangeCode, PointInTime, Symbol, Timeframe
from northstar_core.futures import FuturesContract, FuturesProductReference
from northstar_core.recommendations.futures import (
    CandidateRejectionReason,
    FuturesResearchDecisionReference,
    FuturesTradeRecommendationIdentity,
    GenerationRejectionReason,
    RecommendationPolicyVersion,
)
from northstar_core.strategy import RecommendationAction, StrategyIdentity

AUDIT_COLUMNS = (
    "audit_identity",
    "evidence_version",
    "product_code",
    "exchange_code",
    "expiration_date",
    "strategy_identity",
    "policy_version",
    "profile_version",
    "attempted_at",
    "session_date",
    "generation_status",
    "block_reason",
    "outcome",
    "audit",
)
_REFERENCE_COLUMNS = ("audit_identity", "recommendation_identity", "role")
_RECORDED, _EXISTING = "RECORDED", "EXISTING"

_SELECT_AUDIT = (
    f"SELECT {', '.join(AUDIT_COLUMNS)} FROM futures_recommendation_generation_audits "  # noqa: S608
    "WHERE audit_identity = ?"
)
_SELECT_REFERENCES = (
    "SELECT audit_identity, recommendation_identity, role "
    "FROM futures_recommendation_generation_audit_recommendations "
    "WHERE audit_identity = ? ORDER BY recommendation_identity"
)
_SELECT_RECOMMENDATION_SOURCE = (
    "SELECT product_code, exchange_code, expiration_date, strategy_identity, policy_version, "
    "profile_version FROM futures_trade_recommendations WHERE recommendation_identity = ?"
)
_INSERT_AUDIT = (
    "INSERT INTO futures_recommendation_generation_audits "
    f"({', '.join(AUDIT_COLUMNS)}) VALUES ({', '.join('?' * len(AUDIT_COLUMNS))})"
)
_INSERT_REFERENCE = (
    "INSERT INTO futures_recommendation_generation_audit_recommendations "
    f"({', '.join(_REFERENCE_COLUMNS)}) VALUES (?, ?, ?)"
)


# ---------------------------------------------------------------------------
# Encoding
# ---------------------------------------------------------------------------


def _canonical_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _contract(contract: FuturesContract) -> dict[str, str]:
    return {
        "product_code": contract.product.product_code.value,
        "exchange_code": contract.product.exchange_code.value,
        "expiration_date": contract.expiration_date.value,
    }


def _source(source: FuturesResearchDecisionReference) -> dict[str, Any]:
    return {
        "contract": _contract(source.contract),
        "timeframe": source.timeframe.value,
        "decision_instant": source.decision_instant.value,
        "strategy_identity": source.strategy_identity.identity,
    }


def _optional(value: Any) -> Any:
    return None if value is None else value.value


def audit_json(audit: FuturesRecommendationGenerationAudit) -> str:
    """Return ``audit`` as versioned canonical JSON."""
    session, finality = audit.session, audit.finality
    return _canonical_json(
        {
            "version": audit.version,
            "identity": audit.identity,
            "contract": _contract(audit.contract),
            "strategy_identity": audit.strategy.identity,
            "policy_version": audit.policy_version.version,
            "profile_version": audit.profile_version,
            "attempted_at": audit.attempted_at.value,
            "status": audit.status.value,
            "block_reason": _optional(audit.block_reason),
            "outcome": audit.outcome.value,
            "operator_review": audit.operator_review.value,
            "session": (
                None
                if session is None
                else {
                    "trading_date": session.trading_date.isoformat(),
                    "opens_at": session.opens_at.value,
                    "closes_at": session.closes_at.value,
                }
            ),
            "finality": (
                None
                if finality is None
                else {"outcome": finality.outcome.value, "reason": finality.reason}
            ),
            "history": {
                "coverage": _optional(audit.history_coverage),
                "deficiency": _optional(audit.history_deficiency),
                "deficient_sessions": [day.isoformat() for day in audit.history_deficient_sessions],
            },
            "candidates": [_source(source) for source in audit.candidates],
            "candidate_rejections": [
                {
                    "source": _source(rejection.source),
                    "research_action": rejection.research_action.value,
                    "reason": rejection.reason.value,
                }
                for rejection in audit.candidate_rejections
            ],
            "generation_rejections": [
                {"source": _source(rejection.source), "reason": rejection.reason.value}
                for rejection in audit.generation_rejections
            ],
            "exclusivity_breaches": [
                {
                    "product_code": product.product_code.value,
                    "exchange_code": product.exchange_code.value,
                }
                for product in audit.exclusivity_breaches
            ],
            "proposed": [identity.identity for identity in audit.proposed],
            "recorded": [identity.identity for identity in audit.recorded],
            "existing": [identity.identity for identity in audit.existing],
        }
    )


def audit_row(audit: FuturesRecommendationGenerationAudit) -> tuple[object, ...]:
    """Return the audit table row of ``audit``."""
    product = audit.contract.product
    return (
        audit.identity,
        audit.version,
        product.product_code.value,
        product.exchange_code.value,
        audit.contract.expiration_date.value,
        audit.strategy.identity,
        audit.policy_version.version,
        audit.profile_version,
        audit.attempted_at.value,
        None if audit.session is None else audit.session.trading_date.isoformat(),
        audit.status.value,
        _optional(audit.block_reason),
        audit.outcome.value,
        audit_json(audit),
    )


def _reference_rows(audit: FuturesRecommendationGenerationAudit) -> list[tuple[str, str, str]]:
    rows = [(audit.identity, identity.identity, _RECORDED) for identity in audit.recorded]
    rows += [(audit.identity, identity.identity, _EXISTING) for identity in audit.existing]
    return sorted(rows, key=lambda row: row[1])


# ---------------------------------------------------------------------------
# Writing
# ---------------------------------------------------------------------------


def require_existing_references(
    connection: sqlite3.Connection, audit: FuturesRecommendationGenerationAudit
) -> None:
    """Refuse an audit naming as existing a recommendation not recorded for its attempt.

    The recommendation must be recorded, for the audit's contract and strategy,
    under its policy and profile versions.
    """
    product = audit.contract.product
    expected = (
        product.product_code.value,
        product.exchange_code.value,
        audit.contract.expiration_date.value,
        audit.strategy.identity,
        audit.policy_version.version,
        audit.profile_version,
    )
    for identity in audit.existing:
        row = connection.execute(_SELECT_RECOMMENDATION_SOURCE, (identity.identity,)).fetchone()
        if row is None or tuple(row) != expected:
            raise InvalidFuturesRecommendationGenerationAuditRecordingError(
                f"Audit {audit.identity} names {identity} as existing, but it is not recorded "
                f"for {audit.contract} by {audit.strategy} under {audit.policy_version} and "
                f"profile {audit.profile_version}."
            )


def insert_audit(
    connection: sqlite3.Connection, audit: FuturesRecommendationGenerationAudit
) -> None:
    """Insert ``audit`` and its recommendation references; the caller owns the transaction."""
    connection.execute(_INSERT_AUDIT, audit_row(audit))
    for row in _reference_rows(audit):
        connection.execute(_INSERT_REFERENCE, row)


# ---------------------------------------------------------------------------
# Reading
# ---------------------------------------------------------------------------


def _text(value: Any) -> str:
    if not isinstance(value, str):
        raise TypeError("Stored audit values must be text.")
    return value


def _decode_contract(payload: dict[str, Any]) -> FuturesContract:
    return FuturesContract(
        FuturesProductReference(
            Symbol(_text(payload["product_code"])), ExchangeCode(_text(payload["exchange_code"]))
        ),
        ExpirationDate(_text(payload["expiration_date"])),
    )


def _decode_source(payload: dict[str, Any]) -> FuturesResearchDecisionReference:
    return FuturesResearchDecisionReference(
        _decode_contract(payload["contract"]),
        Timeframe(_text(payload["timeframe"])),
        PointInTime(_text(payload["decision_instant"])),
        StrategyIdentity(_text(payload["strategy_identity"])),
    )


def _identities(values: Any) -> tuple[FuturesTradeRecommendationIdentity, ...]:
    if not isinstance(values, list):
        raise TypeError("Stored audit identities must be a JSON list.")
    return tuple(FuturesTradeRecommendationIdentity(_text(value)) for value in values)


def _decode(text: str) -> FuturesRecommendationGenerationAudit:
    payload = json.loads(text)
    if not isinstance(payload, dict):
        raise TypeError("Stored audit evidence must be a JSON object.")
    version = payload["version"]
    if not isinstance(version, int) or isinstance(version, bool):
        raise TypeError("Stored audit evidence version must be an integer.")
    contract = _decode_contract(payload["contract"])
    session_payload = payload["session"]
    session = (
        None
        if session_payload is None
        else FuturesTradingSession(
            date.fromisoformat(_text(session_payload["trading_date"])),
            PointInTime(_text(session_payload["opens_at"])),
            PointInTime(_text(session_payload["closes_at"])),
        )
    )
    finality_payload = payload["finality"]
    finality = (
        None
        if finality_payload is None or session is None
        else FuturesDailyBarFinality(
            contract,
            session,
            FuturesDailyBarFinalityOutcome(_text(finality_payload["outcome"])),
            _text(finality_payload["reason"]),
        )
    )
    if finality_payload is not None and session is None:
        raise ValueError("Stored audit evidence names a finality without a session.")
    history = payload["history"]
    coverage, deficiency, block_reason = (
        history["coverage"],
        history["deficiency"],
        payload["block_reason"],
    )
    return FuturesRecommendationGenerationAudit(
        contract=contract,
        strategy=StrategyIdentity(_text(payload["strategy_identity"])),
        policy_version=RecommendationPolicyVersion(_text(payload["policy_version"])),
        profile_version=_text(payload["profile_version"]),
        attempted_at=PointInTime(_text(payload["attempted_at"])),
        status=FuturesTradeRecommendationGenerationStatus(_text(payload["status"])),
        block_reason=(
            None
            if block_reason is None
            else FuturesTradeRecommendationBlockReason(_text(block_reason))
        ),
        session=session,
        finality=finality,
        history_coverage=(
            None if coverage is None else FuturesResearchHistoryCoverage(_text(coverage))
        ),
        history_deficiency=(
            None if deficiency is None else FuturesResearchHistoryDeficiency(_text(deficiency))
        ),
        history_deficient_sessions=tuple(
            date.fromisoformat(_text(day)) for day in history["deficient_sessions"]
        ),
        candidates=tuple(_decode_source(source) for source in payload["candidates"]),
        candidate_rejections=tuple(
            FuturesCandidateRejectionAudit(
                _decode_source(item["source"]),
                RecommendationAction(_text(item["research_action"])),
                CandidateRejectionReason(_text(item["reason"])),
            )
            for item in payload["candidate_rejections"]
        ),
        generation_rejections=tuple(
            FuturesGenerationRejectionAudit(
                _decode_source(item["source"]), GenerationRejectionReason(_text(item["reason"]))
            )
            for item in payload["generation_rejections"]
        ),
        exclusivity_breaches=tuple(
            FuturesProductReference(
                Symbol(_text(item["product_code"])), ExchangeCode(_text(item["exchange_code"]))
            )
            for item in payload["exclusivity_breaches"]
        ),
        proposed=_identities(payload["proposed"]),
        recorded=_identities(payload["recorded"]),
        existing=_identities(payload["existing"]),
        version=version,
    )


def stored_audit(
    connection: sqlite3.Connection, identity: str
) -> FuturesRecommendationGenerationAudit | None:
    """Return the audit stored under ``identity``, requiring every stored fact to agree."""
    row = connection.execute(_SELECT_AUDIT, (identity,)).fetchone()
    if row is None:
        return None
    row = tuple(row)
    audit = _decode(_text(row[-1]))
    if audit_row(audit) != row:
        raise ValueError(
            f"Audit {identity!r} is stored non-canonically, its columns disagree with its "
            "evidence, or it is stored under another identity."
        )
    references = [tuple(item) for item in connection.execute(_SELECT_REFERENCES, (identity,))]
    if references != _reference_rows(audit):
        raise ValueError(f"Audit {identity!r} has recommendation references that disagree.")
    for _, recommendation, _ in references:
        if connection.execute(_SELECT_RECOMMENDATION_SOURCE, (recommendation,)).fetchone() is None:
            raise ValueError(f"Audit {identity!r} references unrecorded {recommendation!r}.")
    return audit
