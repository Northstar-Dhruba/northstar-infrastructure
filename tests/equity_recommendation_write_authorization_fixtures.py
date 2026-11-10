"""Test-only helpers for the EI4-D11 authorized write boundary (ADR-023, W2-I).

Two helpers, both used only by tests, both labelled where used:

- ``AuthorizationCapture`` obtains a GENUINE authorization: it is the store a
  real ``RecordEquityRecommendationGenerationUseCase`` writes to, so the
  recorder issues its authorization only after the EI4-D10 gate and full
  source, snapshot and replay verification pass. The capture keeps it and
  writes nothing (it reports a definite rollback), so a test can then hand
  that authorization to a real SQLite adapter -- with its fault seams --
  through the enforced ``write_generation`` and ``write_attempt``. Nothing
  here constructs, copies or registers an authorization, or touches the
  Application's private issuer.
- ``plant_generation_records`` is TEST-ONLY LEGACY-EVIDENCE SETUP: raw SQL
  INSERTs into a disposable database, for records the current verified
  recorder would never authorize -- a record from another evaluator, a forged
  contradictory record (EI-4E AA-2, e20). It stands for evidence written
  before enforcement, or by privileged access outside the EI4-D11 boundary
  (ADR-022 DQ9). The schema's CHECK constraints, foreign keys and triggers
  still apply. It is never a supported write path, and no production module
  exports anything like it.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from contextlib import closing
from pathlib import Path
from typing import Any

from northstar_application.application_services import (
    EquityRecommendationGenerationAttempt,
    EquityRecommendationGenerationDisposition,
    EquityRecommendationRecordingOutcome,
    RecordEquityRecommendationGenerationUseCase,
)
from northstar_application.ports import EquityRecommendationGenerationNotCommittedError

from northstar_infrastructure.persistence import SQLiteEquityRecommendationGenerationStore
from northstar_infrastructure.persistence import (
    sqlite_equity_recommendation_generation as adapter_module,
)


class AuthorizationCapture(SQLiteEquityRecommendationGenerationStore):
    """TEST-ONLY: keeps the authorization the verified recorder issues; writes nothing."""

    def __init__(self, database: Path) -> None:
        super().__init__(database)
        self.authorizations: list[Any] = []

    def write_generation(self, authorization):  # type: ignore[no-untyped-def]
        self.authorizations.append(authorization)
        raise EquityRecommendationGenerationNotCommittedError(
            "test: authorization captured; nothing written"
        )

    def write_attempt(self, authorization):  # type: ignore[no-untyped-def]
        self.authorizations.append(authorization)
        raise EquityRecommendationGenerationNotCommittedError(
            "test: authorization captured; nothing written"
        )


def capture_authorization(
    recorder: Callable[[AuthorizationCapture], RecordEquityRecommendationGenerationUseCase],
    database: Path,
    record: Callable[[RecordEquityRecommendationGenerationUseCase], Any],
) -> Any:
    """Run ``record`` on a real recorder writing to a capture; return its one authorization."""
    capture = AuthorizationCapture(database)
    result = record(recorder(capture))
    assert result.outcome is EquityRecommendationRecordingOutcome.NOT_COMMITTED, (
        result.outcome,
        result.detail,
    )
    (authorization,) = capture.authorizations
    return authorization


def plant_generation_records(
    database: Path,
    disposition: EquityRecommendationGenerationDisposition | None,
    attempt: EquityRecommendationGenerationAttempt | None,
) -> None:
    """TEST-ONLY LEGACY-EVIDENCE SETUP: insert rows by raw SQL, outside any write path.

    The generation schema must already exist in ``database``. Constraints,
    foreign keys and insert-only triggers still apply to these INSERTs.
    """
    with closing(sqlite3.connect(database, isolation_level=None)) as connection:
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("BEGIN IMMEDIATE")
        if disposition is not None:
            connection.execute(
                adapter_module._INSERT_DISPOSITION,  # noqa: SLF001 -- test-only setup
                adapter_module.disposition_row(disposition),
            )
        if attempt is not None:
            connection.execute(
                adapter_module._INSERT_ATTEMPT,  # noqa: SLF001 -- test-only setup
                adapter_module.attempt_row(attempt),
            )
        connection.execute("COMMIT")
