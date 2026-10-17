"""Synthetic generation audits for the SQLite futures recommendation tests (F-5B-2B).

Every value is a fixture: the session, its instants and the finality reason are
not exchange reference data. Not a test module; imported by the tests.
"""

from __future__ import annotations

from datetime import date

from northstar_application.application_services import (
    FuturesRecommendationGenerationAudit,
    FuturesResearchHistoryCoverage,
    FuturesTradeRecommendationGenerationProfile,
    FuturesTradeRecommendationGenerationStatus,
)
from northstar_application.ports import (
    FuturesDailyBarFinality,
    FuturesDailyBarFinalityOutcome,
    FuturesTradingSession,
)
from northstar_core.foundation.value_objects import PointInTime
from northstar_core.recommendations.futures import FuturesTradeRecommendation

SYNTHETIC_SESSION = FuturesTradingSession(
    date(2026, 10, 7), PointInTime("2026-10-07T03:45:00Z"), PointInTime("2026-10-07T10:00:00Z")
)


def recording_audit(
    *recommendations: FuturesTradeRecommendation,
    profile: FuturesTradeRecommendationGenerationProfile,
    **overrides: object,
) -> FuturesRecommendationGenerationAudit:
    """The RECORDED audit of an attempt that recorded exactly ``recommendations``."""
    first = recommendations[0]
    identities = tuple(recommendation.identity for recommendation in recommendations)
    values: dict[str, object] = {
        "contract": first.contract,
        "strategy": first.source.strategy_identity,
        "policy_version": first.policy_version,
        "profile_version": profile.version,
        "attempted_at": first.issued_at,
        "status": FuturesTradeRecommendationGenerationStatus.COMPLETED,
        "session": SYNTHETIC_SESSION,
        "finality": FuturesDailyBarFinality(
            first.contract,
            SYNTHETIC_SESSION,
            FuturesDailyBarFinalityOutcome.FINAL,
            "synthetic operator finality",
        ),
        "history_coverage": FuturesResearchHistoryCoverage.VERIFIED,
        "candidates": tuple(recommendation.source for recommendation in recommendations),
        "proposed": identities,
        "recorded": identities,
    }
    values.update(overrides)
    return FuturesRecommendationGenerationAudit(**values)  # type: ignore[arg-type]
