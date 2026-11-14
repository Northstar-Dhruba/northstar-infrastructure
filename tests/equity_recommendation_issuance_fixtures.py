"""Synthetic fixtures for the EI-6A durable issuance authority, on real SQLite.

EI-6A acceptance runs the real chain over one disposable SQLite file, through
merged use cases and adapters only:

    EI-4E research fixtures (EI-2 snapshots, EI-3C timeline, policy catalog)
    -> DeclareEquitySessionIssuanceCompletenessUseCase (declare through b)
    -> EI-4C version-2 generation against the AUTHORITATIVE history source
    -> EI-4D-A recording into the EI-4D-B store
    -> IssueEquityTradeRecommendationUseCase
    -> SQLiteEquityRecommendationIssuanceAuthority

Every issuance and declaration is written by the real use case through the
adapter's authorized entries; the tests never construct, copy or register an
authorization. With ``t`` the modeled issuance (09:43:30 IST, ``timing/1``)
and ``b = t - 1 us``, version-2 generation needs the session declared complete
exactly through ``b`` (N10), and the issuer needs the frozen history to name
the authority and reproduce at its watermark (D5D-3).

Everything is SYNTHETIC TEST DATA, reusing the EI-4E acceptance fixtures
(TCS and INFY on 2026-08-19). Nothing here opens a network connection, reads
a clock, or touches a production database.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from northstar_application.application_services import (
    AdvanceEquityIntradayResearchTimelineUseCase,
    AuthoritativeEquitySessionRecommendationHistorySource,
    DeclareEquitySessionIssuanceCompletenessUseCase,
    EquityIssuanceDeclarationResult,
    EquityRecommendationEligibilityAudit,
    EquityRecommendationGenerationStatus,
    EquityRecommendationGenerationVerifier,
    EquityRecommendationIssuanceResult,
    EquityRecommendationRecordingOutcome,
    GenerateEquityTradeRecommendationUseCase,
    IssueEquityTradeRecommendationUseCase,
    VerifyStoredEquityRecommendationGenerationUseCase,
)
from northstar_core.foundation.value_objects import PointInTime

from northstar_infrastructure.persistence import (
    SQLiteEquityCandleEvidenceSnapshotStore,
    SQLiteEquityIntradayResearchTimelineStore,
    SQLiteEquityRecommendationGenerationStore,
    SQLiteEquityRecommendationIssuanceAuthority,
)
from tests.equity_recommendation_acceptance_fixtures import (
    CONFIG,
    DAY,
    INFY,
    NSE,
    SESSION,
    VENUE,
    Pipeline,
    establish_research,
    ist,
    request,
)

V2 = "northstar.equity-recommendation-evaluator/2"
T = ist(9, 43, 30)  # timing/1: the 09:41 decision + 150 s
B = PointInTime("2026-08-19T04:13:29.999999Z")  # b = t - 1 us
LEDGER = "equity_recommendation_issuance_ledger"
ISSUANCES = "equity_recommendation_issuances"
DECLARATIONS = "equity_recommendation_issuance_declarations"
UNKNOWN_KEY = "eirk1-sha256:" + "0" * 64

_S = EquityRecommendationGenerationStatus


def authority(database: Path) -> SQLiteEquityRecommendationIssuanceAuthority:
    return SQLiteEquityRecommendationIssuanceAuthority(database)


def verify_stored(database: Path) -> VerifyStoredEquityRecommendationGenerationUseCase:
    """The read-only stored-generation verifier over ``database``, from its path alone."""
    return VerifyStoredEquityRecommendationGenerationUseCase(
        SQLiteEquityRecommendationGenerationStore(database),
        EquityRecommendationGenerationVerifier(
            SQLiteEquityIntradayResearchTimelineStore(database),
            SQLiteEquityCandleEvidenceSnapshotStore(database),
        ),
    )


def declarer(
    database: Path, max_writes: int = 3
) -> DeclareEquitySessionIssuanceCompletenessUseCase:
    store = authority(database)
    return DeclareEquitySessionIssuanceCompletenessUseCase(VENUE, store, store, max_writes)


def issuer(database: Path, max_writes: int = 3) -> IssueEquityTradeRecommendationUseCase:
    store = authority(database)
    return IssueEquityTradeRecommendationUseCase(verify_stored(database), store, store, max_writes)


def declare(database: Path, through: PointInTime) -> EquityIssuanceDeclarationResult:
    return declarer(database).execute(NSE, DAY, through)


def issue(
    database: Path, generation_key: str, **options: Any
) -> EquityRecommendationIssuanceResult:
    return issuer(database, **options).execute(generation_key)


@dataclass
class IssuanceWorld:
    """The EI-4E research pipeline over one file, with the SQLite issuance authority."""

    pipeline: Pipeline

    @property
    def database(self) -> Path:
        return self.pipeline.database

    def generate(self, **changes: Any) -> EquityRecommendationEligibilityAudit:
        """Version-2 generation against the authoritative history source over this file."""
        fresh = self.pipeline.evidence()
        generator = GenerateEquityTradeRecommendationUseCase(
            fresh.timelines,
            fresh.snapshots,
            VENUE,
            self.pipeline.references,
            self.pipeline.eligibility,
            AuthoritativeEquitySessionRecommendationHistorySource(authority(self.database)),
            self.pipeline.catalog(),
            evaluator_version=V2,
        )
        return generator.execute(request(**changes))

    def proposed(self, **changes: Any) -> str:
        """Generate and record a version-2 PROPOSED disposition; return its generation key."""
        audit = self.generate(**changes)
        assert audit.status is _S.PROPOSED, audit.reasons
        recorded = self.pipeline.recorder().execute(audit)
        assert recorded.outcome is EquityRecommendationRecordingOutcome.COMMITTED, recorded.detail
        assert audit.generation_key is not None
        return audit.generation_key

    def declare(self, through: PointInTime) -> EquityIssuanceDeclarationResult:
        return declare(self.database, through)

    def issue(self, generation_key: str, **options: Any) -> EquityRecommendationIssuanceResult:
        return issue(self.database, generation_key, **options)


def issuance_world(database: Path) -> IssuanceWorld:
    """Research established and published; nothing declared or issued yet.

    The EI-6A schema is kept only beside existing EI-4D generation tables, so
    the EI-4D store initializes its own schema first, through an ordinary read.
    """
    pipeline = Pipeline(database)
    establish_research(pipeline)
    assert pipeline.store().load_disposition(UNKNOWN_KEY) is None
    return IssuanceWorld(pipeline)


def establish_infy_breakout(world: IssuanceWorld) -> None:
    """SYNTHETIC: give INFY the TCS breakout prices and commit its EI-3C timeline."""
    evidence = world.pipeline.evidence()
    evidence.candles(SESSION, INFY)
    snapshots = [evidence.issue(ist(9, minute), INFY) for minute in (36, 41, 46)]
    advanced = AdvanceEquityIntradayResearchTimelineUseCase(
        evidence.timelines, evidence.snapshots, CONFIG
    ).execute(INFY, DAY, [s.snapshot_id for s in snapshots])
    assert advanced.status.value == "COMMITTED", advanced.status


def infy(**policy: str) -> dict[str, Any]:
    """Request changes for INFY with the CONFIG strategy."""
    return {"listing": INFY, **policy}


# ---------------------------------------------------------------------------
# Separate OS processes (spawned; the Windows start method, everywhere)
# ---------------------------------------------------------------------------


def issue_in_another_process(
    database: str, generation_key: str, start: Any, results: Any, name: str
) -> None:
    """Spawned worker: build the issuer from the path alone, wait at ``start``, issue.

    Its authorization is issued in this process by the real use case; nothing
    is shared with the parent but the file.
    """
    use_case = issuer(Path(database))
    start.wait(60)
    try:
        result = use_case.execute(generation_key)
        results.put((name, os.getpid(), result.outcome.value, result.recording_sequence, None))
    except BaseException as exc:  # noqa: BLE001 -- reported to the parent, never swallowed
        results.put((name, os.getpid(), None, None, repr(exc)))


def declare_in_another_process(
    database: str, through: str, start: Any, results: Any, name: str
) -> None:
    """Spawned worker: build the declarer from the path alone, wait at ``start``, declare."""
    use_case = declarer(Path(database))
    start.wait(60)
    try:
        result = use_case.execute(NSE, DAY, PointInTime(through))
        sequence = None if result.recorded is None else result.recorded.recording_sequence
        results.put((name, os.getpid(), result.outcome.value, sequence, None))
    except BaseException as exc:  # noqa: BLE001 -- reported to the parent, never swallowed
        results.put((name, os.getpid(), None, None, repr(exc)))
