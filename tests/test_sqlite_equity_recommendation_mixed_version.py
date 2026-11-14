"""EI-4C slice 4: mixed-version persistence and replay on real SQLite (ADR-025).

Slice 4 of the EI-4C Versioned History Compatibility Design (section 13)
verifies, on the existing EI-4D-B schema and adapter, what section 11 only
expected: version-1 and version-2 generation records coexist in one file,
keep their own formats and identities, replay under their own evaluator, and
collide on one generation key as an EVALUATOR_VERSION_CONFLICT -- never as
equivalence. Test names carry the design's scenario identifiers (PS-01 to
PS-08, VC-09, VC-10, VC-13 to VC-16), and I08 names the fingerprint-only
equivalence mutation that slice 2 left without a real version-2 record.

Every success path is the real chain over one disposable SQLite file
(tests/equity_recommendation_acceptance_fixtures.py): EI-2 candles and
snapshots, the EI-3C timeline, the merged EI-4C generator with its keyword-only
``evaluator_version``, and the EI-4D-A recorder writing through the EI4-D11
authorized ``write_generation`` / ``write_attempt``. No version-2 record is
inserted by raw SQL. Raw SQL appears only where a test says so: refused
direct writes, and ``bypass`` corruption of a disposable file.

Separate synthetic history sources (SYNTHETIC TEST DATA)
--------------------------------------------------------
With ``t`` the modeled issuance (09:43:30 IST) and ``b = t - 1 us``, version 1
reads the issued history as of ``t`` and needs it complete at least through
``t``; version 2 reads it as of ``b`` and needs it complete *exactly* through
``b`` (N8, N10). No one answer satisfies both, so each version is generated
against its own labelled synthetic history -- version 1 complete through 15:30
IST, version 2 complete through ``b`` -- and each record freezes the evidence it
was actually generated from. Neither history is altered after use, and the
candidate, snapshot and policy-set evidence are the same committed rows, so
both versions derive the same ``eirk1`` key (no evaluator in the key, N5).

Nothing here issues a recommendation, writes issued history, opens a network
connection or touches a production database.
"""

from __future__ import annotations

import hashlib
import json
import multiprocessing
import os
import socket
import sqlite3
import subprocess
import sys
from collections.abc import Callable, Iterator
from contextlib import closing, contextmanager
from itertools import groupby
from pathlib import Path
from typing import Any

import pytest
from northstar_application.application_services import (
    EquityRecommendationDispositionKind,
    EquityRecommendationEligibilityAudit,
    EquityRecommendationEligibilityReason,
    EquityRecommendationGenerationAttempt,
    EquityRecommendationGenerationDisposition,
    EquityRecommendationGenerationStatus,
    EquityRecommendationRecordingOutcome,
    EquityRecommendationSourceVerificationError,
    GenerateEquityTradeRecommendationUseCase,
    IncompatibleEquityRecommendationRecordError,
    InvalidEquityRecommendationRecordError,
    equity_history_boundary,
    equity_recommendation_attempt,
    equity_recommendation_disposition,
)
from northstar_application.ports import (
    EquityRecommendationStoreOutcome,
    UnverifiedEquityRecommendationGenerationWriteError,
)
from northstar_core.foundation.value_objects import PointInTime

from northstar_infrastructure.persistence import (
    SQLiteEquityRecommendationGenerationStore,
    initialize_equity_candle_evidence_schema,
    initialize_equity_intraday_research_timeline_schema,
)
from northstar_infrastructure.persistence import (
    sqlite_equity_recommendation_generation as adapter_module,
)
from northstar_infrastructure.persistence.sqlite_equity_recommendation_generation_schema import (  # noqa: E501
    initialize_equity_recommendation_generation_schema,
)
from tests.equity_recommendation_acceptance_fixtures import (
    ATTEMPTS,
    DISPOSITIONS,
    ENTRIES,
    SNAPSHOTS,
    TCS,
    VENUE,
    History,
    Pipeline,
    Research,
    bypass,
    check_database,
    counts,
    documents,
    establish_research,
    ist,
    prior,
    raw,
    record_in_another_process,
    request,
    rows,
    schema_objects,
)
from tests.equity_recommendation_write_authorization_fixtures import plant_generation_records

_O = EquityRecommendationRecordingOutcome
_S = EquityRecommendationGenerationStatus
_R = EquityRecommendationEligibilityReason
_K = EquityRecommendationDispositionKind
_W = EquityRecommendationStoreOutcome
_Records = tuple[EquityRecommendationGenerationDisposition, EquityRecommendationGenerationAttempt]

_V1 = "northstar.equity-recommendation-evaluator/1"
_V2 = "northstar.equity-recommendation-evaluator/2"
_UNSUPPORTED = "northstar.equity-recommendation-evaluator/999"  # outside the governed set
_FORMATS = {
    _V1: (
        "northstar.equity-recommendation-generation-inputs/1",
        "northstar.equity-recommendation-assessment/1",
        "northstar.equity-recommendation-generation-attempt/1",
    ),
    _V2: (
        "northstar.equity-recommendation-generation-inputs/2",
        "northstar.equity-recommendation-assessment/2",
        "northstar.equity-recommendation-generation-attempt/2",
    ),
}
_PROPOSAL_FORMAT = "northstar.equity-recommendation-proposal/1"  # unversioned by ADR-025
_T = ist(9, 43, 30)  # 09:41 decision + 150 s (timing/1): the modeled issuance
_B = "2026-08-19T04:13:29.999999Z"  # b = t - 1 us, 09:43:29.999999 IST
_STRICTLY_BEFORE = "STRICTLY_BEFORE_MODELED_ISSUANCE"
_INFRA_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(autouse=True)
def _refuse_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("EI-4C slice 4 must not open network connections.")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)


@pytest.fixture
def pipeline(tmp_path: Path) -> Pipeline:
    return Pipeline(tmp_path / "ei4c-mixed.sqlite3")


@pytest.fixture
def research(pipeline: Pipeline) -> Research:
    return establish_research(pipeline)


# ----------------------------------------------------------------------
# The real generator, under an explicit evaluator and synthetic history
# ----------------------------------------------------------------------


def _generate(
    pipeline: Pipeline, evaluator: str, history: History, **changes: str
) -> EquityRecommendationEligibilityAudit:
    """The merged EI-4C generator over the pipeline's file, selecting ``evaluator``."""
    fresh = pipeline.evidence()
    generator = GenerateEquityTradeRecommendationUseCase(
        fresh.timelines,
        fresh.snapshots,
        VENUE,
        pipeline.references,
        pipeline.eligibility,
        history,
        pipeline.catalog(),
        evaluator_version=evaluator,
    )
    return generator.execute(request(**changes))


def _v2_history(**fields: Any) -> History:
    """Version 2's synthetic history: complete exactly through b, unless told otherwise."""
    return History(**{"complete_through": _point(_B), **fields})


def _point(value: str) -> PointInTime:
    return PointInTime(value)


def _v1_proposed(pipeline: Pipeline, **changes: str) -> EquityRecommendationEligibilityAudit:
    audit = _generate(pipeline, _V1, History(), **changes)  # complete through 15:30 IST
    assert audit.status is _S.PROPOSED, audit.reasons
    return audit


def _v2_proposed(pipeline: Pipeline, **changes: str) -> EquityRecommendationEligibilityAudit:
    audit = _generate(pipeline, _V2, _v2_history(), **changes)  # N10 case A
    assert audit.status is _S.PROPOSED, audit.reasons
    return audit


def _v2_not_eligible(pipeline: Pipeline) -> EquityRecommendationEligibilityAudit:
    audit = _generate(
        pipeline, _V2, _v2_history(), generation="generation/strict", set_version="set/strict"
    )
    assert audit.status is _S.NOT_ELIGIBLE and audit.generation_key is not None
    return audit


def _v2_unresolved(pipeline: Pipeline, **changes: str) -> EquityRecommendationEligibilityAudit:
    """A prior synthetic same-listing issuance at 09:30, before b: open-state UNRESOLVED."""
    history = _v2_history(records=(prior("earlier", TCS, ist(9, 30), 1),))
    audit = _generate(pipeline, _V2, history, **changes)
    assert audit.status is _S.EVALUATION_UNRESOLVED
    return audit


def _v2_case_c(pipeline: Pipeline) -> EquityRecommendationEligibilityAudit:
    """N10 case C: the history is declared complete through 15:30 IST, beyond b."""
    audit = _generate(pipeline, _V2, History())
    assert audit.status is _S.DEPENDENCY_UNAVAILABLE
    assert audit.reasons == (_R.RECOMMENDATION_HISTORY_BOUNDARY_EXCEEDED,)
    return audit


def _records(audit: EquityRecommendationEligibilityAudit) -> _Records:
    disposition = equity_recommendation_disposition(audit)
    assert disposition is not None
    return disposition, equity_recommendation_attempt(audit)


def _canonical(text: str) -> str:
    return json.dumps(json.loads(text), sort_keys=True, separators=(",", ":"))


def _row(database: Path, table: str, column: str, value: str) -> dict[str, Any]:
    (found,) = [r for r in rows(database, table) if r[column] == value]
    return found


def _file_state(database: Path) -> tuple[str, int]:
    data = database.read_bytes()
    return hashlib.sha256(data).hexdigest(), len(data)


# ----------------------------------------------------------------------
# PS-08, PS-01: the current schema accepts version-2 rows; nothing is altered
# ----------------------------------------------------------------------


def _schema_state(database: Path) -> tuple[Any, ...]:
    with closing(raw(database)) as connection:
        version = connection.execute("PRAGMA user_version").fetchone()
        columns = {
            table: connection.execute(f"PRAGMA table_info({table})").fetchall()
            for table in (DISPOSITIONS, ATTEMPTS)
        }
    return schema_objects(database), version, columns


def test_ps08_ps01_the_current_schema_accepts_version2_rows_with_no_migration(
    pipeline: Pipeline, research: Research
) -> None:
    assert pipeline.store().load_disposition("eirk1-sha256:" + "0" * 64) is None  # schema only
    before = _schema_state(pipeline.database)
    made = {
        _K.UNRESOLVED: _v2_unresolved(pipeline),  # set/1
        _K.NOT_ELIGIBLE: _v2_not_eligible(pipeline),  # set/strict
        _K.PROPOSED: _v2_proposed(pipeline, set_version="set/2"),
    }
    for kind, audit in made.items():
        result = pipeline.recorder().execute(audit)
        assert (result.outcome, result.attempt_recorded) == (_O.COMMITTED, True), kind
        disposition, attempt = _records(audit)
        row = _row(pipeline.database, DISPOSITIONS, "generation_key", disposition.generation_key)
        assert (row["disposition_kind"], row["evaluator_version"]) == (kind.value, _V2)
        assert row["assessment_format"] == _FORMATS[_V2][1]
        assert row["proposal_format"] == (_PROPOSAL_FORMAT if kind is _K.PROPOSED else None)
        attempt_row = _row(
            pipeline.database, ATTEMPTS, "attempt_identity", attempt.attempt_identity
        )
        assert attempt_row["attempt_format"] == _FORMATS[_V2][2]
        assert attempt_row["disposition_key"] == disposition.generation_key
    assert _schema_state(pipeline.database) == before  # no table, index or trigger altered
    names = {name for name, _ in schema_objects(pipeline.database).items()}
    assert not any("version" in name or "migration" in name for name in names)
    assert counts(pipeline.database) == (3, 3)
    check_database(pipeline.database)


# ----------------------------------------------------------------------
# Section 5: a genuine version-2 PROPOSED disposition and its keyed attempt
# ----------------------------------------------------------------------


def test_a_version2_proposed_record_persists_exactly_its_own_documents_and_provenance(
    pipeline: Pipeline, research: Research
) -> None:
    assert equity_history_boundary(_T).value == _B  # b = t - 1 microsecond
    history = _v2_history()
    audit = _generate(pipeline, _V2, history)
    assert audit.status is _S.PROPOSED and history.asked == [_point(_B)]  # read as of b
    disposition, attempt = _records(audit)
    result = pipeline.recorder().execute(audit)
    assert (result.outcome, result.attempt_recorded, result.writes) == (_O.COMMITTED, True, 1)
    assert result.authoritative == disposition

    key = disposition.generation_key
    assert key == audit.generation_key and key.startswith("eirk1-sha256:")
    row = _row(pipeline.database, DISPOSITIONS, "generation_key", key)
    assert row["evaluator_version"] == _V2
    assert row["assessment_document"] == disposition.assessment_document  # exact text
    assert _canonical(row["assessment_document"]) == row["assessment_document"]
    assert row["proposal_document"] == disposition.proposal_document
    assert _canonical(row["proposal_document"]) == row["proposal_document"]
    assert row["assessment_fingerprint"].startswith("eira1-sha256:")
    assert row["proposal_fingerprint"].startswith("eirp1-sha256:")
    assert row["inputs_fingerprint"].startswith("eiri1-sha256:")
    assert row["policy_set_fingerprint"].startswith("eirs1-sha256:")
    assessment = json.loads(row["assessment_document"])
    inputs = assessment["inputs"]
    assert (assessment["format"], inputs["format"]) == (_FORMATS[_V2][1], _FORMATS[_V2][0])
    assert inputs["modeled_issued_at"] == _T.value
    history_document = inputs["issued_history"]
    assert history_document["as_of"] == history_document["complete_through"] == _B
    assert history_document["history_boundary"] == _STRICTLY_BEFORE
    assert json.loads(row["proposal_document"])["format"] == _PROPOSAL_FORMAT

    timeline = research.advance.timeline
    assert timeline is not None and timeline.candidate is not None
    candidate = timeline.candidate
    assert row["candidate_entry_fingerprint"] == candidate.fingerprint  # the CANDIDATE entry
    assert row["research_decision_fingerprint"] == candidate.decision.fingerprint
    assert row["evidence_snapshot_id"] == research.s41.snapshot_id  # the immutable snapshot
    assert row["manifest_fingerprint"] == research.s41.manifest_fingerprint
    assert row["decision_as_of"] == research.s41.as_of.value
    assert _row(pipeline.database, SNAPSHOTS, "snapshot_id", research.s41.snapshot_id)
    assert _row(pipeline.database, ENTRIES, "entry_fingerprint", candidate.fingerprint)

    attempt_row = _row(pipeline.database, ATTEMPTS, "attempt_identity", attempt.attempt_identity)
    assert attempt.attempt_identity.startswith("eirat1-sha256:")
    assert attempt_row["attempt_document"] == attempt.attempt_document
    assert attempt_row["attempt_format"] == _FORMATS[_V2][2]
    assert (attempt_row["status"], attempt_row["disposition_kind"]) == ("PROPOSED", "PROPOSED")
    assert attempt_row["generation_key"] == attempt_row["disposition_key"] == key
    assert attempt_row["assessment_fingerprint"] == disposition.assessment_fingerprint
    reloaded = SQLiteEquityRecommendationGenerationStore(pipeline.database)
    assert reloaded.load_disposition(key) == disposition
    assert reloaded.load_attempt(attempt.attempt_identity) == attempt
    assert attempt.evaluator_version == _V2
    check_database(pipeline.database)


# ----------------------------------------------------------------------
# PS-02, VC-19 on SQLite: keyless version-2 attempts; case C consumes no key
# ----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("history", "reason"),
    [
        (_v2_history(complete_through=ist(9, 30)), _R.RECOMMENDATION_HISTORY_INCOMPLETE),
        (History(), _R.RECOMMENDATION_HISTORY_BOUNDARY_EXCEEDED),
        (History(mode="missing"), _R.OPEN_RECOMMENDATION_STATE_UNAVAILABLE),
    ],
    ids=["case B incomplete before b", "case C declared beyond b", "case D missing"],
)
def test_ps02_keyless_version2_attempts_insert_alone_and_retry_idempotently(
    pipeline: Pipeline, research: Research, history: History, reason: Any
) -> None:
    audit = _generate(pipeline, _V2, history)
    assert audit.reasons == (reason,) and audit.generation_key is None
    assert equity_recommendation_disposition(audit) is None
    attempt = equity_recommendation_attempt(audit)
    first = pipeline.recorder().execute(audit)
    assert (first.outcome, first.attempt_recorded, first.disposition) == (_O.COMMITTED, True, None)
    again = pipeline.recorder().execute(_generate(pipeline, _V2, history))
    assert (again.outcome, again.attempt_recorded) == (_O.EQUIVALENT_COMMITTED, True)
    assert counts(pipeline.database) == (0, 1)  # one logical attempt; no disposition
    (row,) = rows(pipeline.database, ATTEMPTS)
    assert row["attempt_identity"] == attempt.attempt_identity
    assert row["attempt_format"] == _FORMATS[_V2][2]
    assert (row["generation_key"], row["disposition_kind"], row["disposition_key"]) == (
        None,
        None,
        None,
    )
    document = json.loads(row["attempt_document"])
    assert document["reasons"] == [reason.value] and row["status"] == audit.status.value


def test_case_c_is_dependency_unavailable_attempt_only_and_leaves_the_key_unconsumed(
    pipeline: Pipeline, research: Research
) -> None:
    refused = _v2_case_c(pipeline)
    assert refused.proposal is None and refused.candidate is not None  # a candidate existed
    result = pipeline.recorder().execute(refused)
    assert (result.outcome, result.attempt_recorded) == (_O.COMMITTED, True)
    assert counts(pipeline.database) == (0, 1)  # no disposition, no proposal
    (row,) = rows(pipeline.database, ATTEMPTS)
    assert (row["status"], row["generation_key"]) == ("DEPENDENCY_UNAVAILABLE", None)
    # A version-1 generation over the same declared history is unaffected (VC-19).
    assert _generate(pipeline, _V1, History()).status is _S.PROPOSED
    # The key was never consumed: the version-2 case-A record commits under it.
    accepted = _v2_proposed(pipeline)
    committed = pipeline.recorder().execute(accepted)
    assert committed.outcome is _O.COMMITTED
    assert counts(pipeline.database) == (1, 2)
    assert row["attempt_identity"] != equity_recommendation_attempt(accepted).attempt_identity


# ----------------------------------------------------------------------
# VC-13, VC-14: both versions coexist; attempts are distinct per version
# ----------------------------------------------------------------------


def test_vc13_vc14_both_versions_coexist_with_distinct_attempts_each_idempotent(
    pipeline: Pipeline, research: Research
) -> None:
    keyed = {
        _V1: _v1_proposed(pipeline),  # set/1
        _V2: _v2_proposed(pipeline, set_version="set/2"),  # another approved policy set
    }
    keyless = {
        _V1: _generate(pipeline, _V1, History(mode="missing")),
        _V2: _generate(pipeline, _V2, History(mode="missing")),
    }
    for audits in (keyed, keyless):
        identities = {
            v: equity_recommendation_attempt(a).attempt_identity for v, a in audits.items()
        }
        assert identities[_V1] != identities[_V2]  # never one logical attempt across versions
        for version, audit in audits.items():
            assert pipeline.recorder().execute(audit).outcome is _O.COMMITTED, version
    assert keyed[_V1].generation_key != keyed[_V2].generation_key  # distinct keys (VC-14)
    assert counts(pipeline.database) == (2, 4)
    for audits in (keyed, keyless):  # retries are idempotent within each version
        for audit in audits.values():
            result = pipeline.recorder().execute(audit)
            assert (result.outcome, result.attempt_recorded) == (_O.EQUIVALENT_COMMITTED, True)
    assert counts(pipeline.database) == (2, 4)
    store = SQLiteEquityRecommendationGenerationStore(pipeline.database)
    for version, audit in keyed.items():
        disposition, attempt = _records(audit)
        assert store.load_disposition(disposition.generation_key) == disposition
        assert store.load_disposition(disposition.generation_key).evaluator_version == version  # type: ignore[union-attr]
        assert store.load_attempt(attempt.attempt_identity).evaluator_version == version  # type: ignore[union-attr]
    for version, audit in keyless.items():
        attempt = equity_recommendation_attempt(audit)
        loaded = store.load_attempt(attempt.attempt_identity)
        assert loaded == attempt and loaded.evaluator_version == version  # type: ignore[union-attr]
        assert json.loads(loaded.attempt_document)["format"] == _FORMATS[version][2]  # type: ignore[union-attr]


# ----------------------------------------------------------------------
# VC-09, VC-10, PS-02: one generation key, both versions, both orders
# ----------------------------------------------------------------------


def _same_key_pair(pipeline: Pipeline) -> dict[str, EquityRecommendationEligibilityAudit]:
    """Genuine version-1 and version-2 PROPOSED audits of one candidate and policy set."""
    audits = {_V1: _v1_proposed(pipeline), _V2: _v2_proposed(pipeline)}
    (v1, _), (v2, _) = _records(audits[_V1]), _records(audits[_V2])
    assert v1.generation_key == v2.generation_key  # eirk1 holds no evaluator (N5)
    assert v1.proposal_fingerprint == v2.proposal_fingerprint  # the same eirp1 (VC-10)
    assert v1.assessment_fingerprint != v2.assessment_fingerprint  # format is in the preimage
    return audits


@pytest.mark.parametrize(
    ("first", "second"), [(_V1, _V2), (_V2, _V1)], ids=["A v1 first", "B v2 first"]
)
def test_vc09_a_same_key_record_of_the_other_version_is_a_conflict_never_adopted(
    pipeline: Pipeline, research: Research, first: str, second: str
) -> None:
    audits = _same_key_pair(pipeline)
    kept, kept_attempt = _records(audits[first])
    offered, offered_attempt = _records(audits[second])
    assert pipeline.recorder().execute(audits[first]).outcome is _O.COMMITTED
    before = rows(pipeline.database, DISPOSITIONS)

    # The store itself: CONFLICT, the first record returned, the competing attempt appended.
    authorization = pipeline.authorize(audits[second])  # issued by the verified recorder
    written = pipeline.store().write_generation(authorization)
    assert (written.outcome, written.stored, written.attempt_inserted) == (_W.CONFLICT, kept, True)

    # The recorder: EVALUATOR_VERSION_CONFLICT, nothing adopted, the attempt audited.
    result = pipeline.recorder().execute(audits[second])
    assert result.outcome is _O.EVALUATOR_VERSION_CONFLICT
    assert result.authoritative is None and not result.is_stored
    assert result.attempt_recorded is True
    assert first in result.detail and second in result.detail

    assert rows(pipeline.database, DISPOSITIONS) == before  # first kept, never replaced
    assert counts(pipeline.database) == (1, 2)
    store = SQLiteEquityRecommendationGenerationStore(pipeline.database)
    assert store.load_disposition(kept.generation_key) == kept
    assert store.load_attempt(kept_attempt.attempt_identity) == kept_attempt
    competing = store.load_attempt(offered_attempt.attempt_identity)
    assert competing == offered_attempt and competing.evaluator_version == second  # type: ignore[union-attr]
    competing_row = _row(
        pipeline.database, ATTEMPTS, "attempt_identity", offered_attempt.attempt_identity
    )
    assert competing_row["attempt_format"] == _FORMATS[second][2]
    assert competing_row["disposition_key"] == kept.generation_key  # under the first record
    verified = pipeline.verify_stored().execute(kept.generation_key)
    assert verified is not None and verified.disposition == kept
    assert verified.disposition.evaluator_version == first
    assert verified.source_integrity_verified and verified.evaluation_reproduced
    check_database(pipeline.database)


def test_vc10_the_shared_proposal_identity_cannot_add_a_second_row(
    pipeline: Pipeline, research: Research
) -> None:
    audits = _same_key_pair(pipeline)
    assert pipeline.recorder().execute(audits[_V1]).outcome is _O.COMMITTED
    before = rows(pipeline.database, DISPOSITIONS)
    second = adapter_module.disposition_row(_records(audits[_V2])[0])
    columns = ", ".join(adapter_module.DISPOSITION_COLUMNS)
    marks = ", ".join("?" * len(adapter_module.DISPOSITION_COLUMNS))
    insert = f"INSERT INTO {DISPOSITIONS} ({columns}) VALUES ({marks})"  # noqa: S608
    with closing(raw(pipeline.database)) as connection:
        for statement in (insert, insert.replace("INSERT", "INSERT OR REPLACE", 1)):
            with pytest.raises(sqlite3.IntegrityError, match="insert-only"):  # no-replace trigger
                connection.execute(statement, second)
    # Independently of that trigger, the key and the UNIQUE proposal identity refuse it.
    with pytest.raises(sqlite3.IntegrityError, match="UNIQUE constraint failed"):
        bypass(pipeline.database, (insert, second), drop=(f"{DISPOSITIONS}_no_replace",))
    assert rows(pipeline.database, DISPOSITIONS) == before


# ----------------------------------------------------------------------
# I08: equal key, proposal identity or fingerprint family is not equivalence
# ----------------------------------------------------------------------


def test_i08_shared_identities_never_make_records_of_two_evaluators_equivalent(
    pipeline: Pipeline, research: Research
) -> None:
    audits = _same_key_pair(pipeline)
    v1, v1_attempt = _records(audits[_V1])
    v2, v2_attempt = _records(audits[_V2])
    shared = {
        "generation key": (v1.generation_key, v2.generation_key),
        "proposal identity": (v1.proposal_fingerprint, v2.proposal_fingerprint),
        "proposal document": (v1.proposal_document, v2.proposal_document),
    }
    assert all(left == right for left, right in shared.values())
    for one, two in (
        (v1.assessment_fingerprint, v2.assessment_fingerprint),
        (v1_attempt.attempt_identity, v2_attempt.attempt_identity),
    ):
        assert one.split(":")[0] == two.split(":")[0] and one != two  # same family only
    assert v1 != v2  # the evaluator label takes part in record equality

    # No record can carry one evaluator's documents under the other's label, so an
    # equal assessment fingerprint can never stand in for an equal evaluator.
    for documents_of, label in ((v1, _V2), (v2, _V1)):
        with pytest.raises(InvalidEquityRecommendationRecordError) as raised:
            EquityRecommendationGenerationDisposition(
                documents_of.generation_key,
                documents_of.kind,
                label,
                documents_of.assessment_fingerprint,
                documents_of.assessment_document,
                documents_of.proposal_fingerprint,
                documents_of.proposal_document,
            )
        assert not isinstance(raised.value, IncompatibleEquityRecommendationRecordError)

    # On SQLite, in both orders, the shared identities decide nothing.
    for first, second in ((_V1, _V2), (_V2, _V1)):
        database = pipeline.database.with_name(f"i08-{first[-1]}-first.sqlite3")
        other = Pipeline(database)
        establish_research(other)
        assert other.recorder().execute(audits[first]).outcome is _O.COMMITTED
        written = other.store().write_generation(other.authorize(audits[second]))
        assert written.outcome is _W.CONFLICT
        assert other.recorder().execute(audits[second]).outcome is _O.EVALUATOR_VERSION_CONFLICT
        assert counts(database) == (1, 2)


# ----------------------------------------------------------------------
# PS-03: version-2 rows are append-only
# ----------------------------------------------------------------------


def test_ps03_update_delete_and_replacement_are_refused_for_version2_rows(
    pipeline: Pipeline, research: Research
) -> None:
    for audit in (
        _v2_unresolved(pipeline),
        _v2_not_eligible(pipeline),
        _v2_proposed(pipeline, set_version="set/2"),
    ):
        assert pipeline.recorder().execute(audit).outcome is _O.COMMITTED
    assert pipeline.recorder().execute(_v2_case_c(pipeline)).outcome is _O.COMMITTED
    before = (rows(pipeline.database, DISPOSITIONS), rows(pipeline.database, ATTEMPTS))
    statements = [
        f"UPDATE {DISPOSITIONS} SET evaluator_version = '{_V1}'",  # an approved label
        f"UPDATE {DISPOSITIONS} SET disposition_kind = 'PROPOSED' "
        "WHERE disposition_kind = 'UNRESOLVED'",
        f"UPDATE {DISPOSITIONS} SET assessment_document = assessment_document",
        f"UPDATE {DISPOSITIONS} SET proposal_document = NULL",
        f"DELETE FROM {DISPOSITIONS}",
        f"INSERT OR REPLACE INTO {DISPOSITIONS} SELECT * FROM {DISPOSITIONS}",
        f"REPLACE INTO {DISPOSITIONS} SELECT * FROM {DISPOSITIONS}",
        f"UPDATE {ATTEMPTS} SET attempt_format = '{_FORMATS[_V1][2]}'",
        f"UPDATE {ATTEMPTS} SET status = 'PROPOSED'",
        f"DELETE FROM {ATTEMPTS}",
        f"INSERT OR REPLACE INTO {ATTEMPTS} SELECT * FROM {ATTEMPTS}",
    ]
    with closing(raw(pipeline.database)) as connection:
        for statement in statements:
            with pytest.raises(sqlite3.IntegrityError, match="insert-only"):
                connection.execute(statement)
    assert (rows(pipeline.database, DISPOSITIONS), rows(pipeline.database, ATTEMPTS)) == before
    assert {r["evaluator_version"] for r in before[0]} == {_V2}
    check_database(pipeline.database)


# ----------------------------------------------------------------------
# PS-04, VC-15: a mixed file reloads and verifies after restart, in a fresh process
# ----------------------------------------------------------------------


def _mixed_file(pipeline: Pipeline) -> dict[str, Any]:
    """A deterministic file holding legitimate records of both evaluators."""
    v1 = _v1_proposed(pipeline)  # set/1
    v2_competing = _v2_proposed(pipeline)  # set/1: same key as v1, conflicts
    v2 = _v2_proposed(pipeline, set_version="set/2")
    v2_not_eligible = _v2_not_eligible(pipeline)
    v2_keyless = _v2_case_c(pipeline)
    v1_keyless = _generate(pipeline, _V1, History(mode="missing"))
    outcomes = [
        pipeline.recorder().execute(a).outcome
        for a in (v1, v2_competing, v2, v2_not_eligible, v2_keyless, v1_keyless)
    ]
    assert outcomes == [
        _O.COMMITTED,
        _O.EVALUATOR_VERSION_CONFLICT,
        _O.COMMITTED,
        _O.COMMITTED,
        _O.COMMITTED,
        _O.COMMITTED,
    ]
    dispositions = {
        _records(a)[0].generation_key: (version, _records(a)[0])
        for version, a in ((_V1, v1), (_V2, v2), (_V2, v2_not_eligible))
    }
    attempts = {
        equity_recommendation_attempt(a).attempt_identity: (
            version,
            equity_recommendation_attempt(a),
        )
        for version, a in (
            (_V1, v1),
            (_V2, v2_competing),
            (_V2, v2),
            (_V2, v2_not_eligible),
            (_V2, v2_keyless),
            (_V1, v1_keyless),
        )
    }
    assert counts(pipeline.database) == (3, 6)
    return {"dispositions": dispositions, "attempts": attempts}


_FRESH_PROCESS = """
import json, sys
from pathlib import Path
from northstar_application.application_services import (
    EquityRecommendationGenerationVerifier,
    VerifyStoredEquityRecommendationGenerationUseCase,
)
from northstar_infrastructure.persistence import (
    SQLiteEquityCandleEvidenceSnapshotStore,
    SQLiteEquityIntradayResearchTimelineStore,
    SQLiteEquityRecommendationGenerationStore,
)
path = Path(sys.argv[1])
store = SQLiteEquityRecommendationGenerationStore(path)
verifier = EquityRecommendationGenerationVerifier(
    SQLiteEquityIntradayResearchTimelineStore(path), SQLiteEquityCandleEvidenceSnapshotStore(path)
)
use = VerifyStoredEquityRecommendationGenerationUseCase(store, verifier)
out = {"dispositions": {}, "attempts": {}}
for key in json.loads(sys.argv[2]):
    v = use.execute(key)
    d = v.disposition
    out["dispositions"][key] = [
        d.evaluator_version, d.kind.value, d.assessment.document["format"],
        d.assessment_fingerprint, d.assessment_document, d.proposal_fingerprint,
        d.proposal_document, v.source_integrity_verified, v.evaluation_reproduced,
        v.replayed.assessment_fingerprint,
    ]
for identity in json.loads(sys.argv[3]):
    a = store.load_attempt(identity)
    out["attempts"][identity] = [a.evaluator_version, a.attempt_document]
print(json.dumps(out, sort_keys=True))
"""


def test_ps04_vc15_a_mixed_file_reloads_and_verifies_each_version_in_a_fresh_process(
    pipeline: Pipeline, research: Research
) -> None:
    mixed = _mixed_file(pipeline)
    before = _file_state(pipeline.database)
    completed = subprocess.run(  # the test's own interpreter, running fixed code only
        [
            sys.executable,
            "-c",
            _FRESH_PROCESS,
            str(pipeline.database),
            json.dumps(sorted(mixed["dispositions"])),
            json.dumps(sorted(mixed["attempts"])),
        ],
        cwd=_INFRA_ROOT,
        env=dict(os.environ, PYTHONHASHSEED="4242"),
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    fresh = json.loads(completed.stdout)
    for key, (version, disposition) in mixed["dispositions"].items():
        assert fresh["dispositions"][key] == [
            version,
            disposition.kind.value,
            _FORMATS[version][1],
            disposition.assessment_fingerprint,
            disposition.assessment_document,  # byte-identical text
            disposition.proposal_fingerprint,
            disposition.proposal_document,
            True,  # level B: the source re-established from the stored evidence
            True,  # level C: the assessment reproduced by its own evaluator
            disposition.assessment_fingerprint,
        ]
    for identity, (version, attempt) in mixed["attempts"].items():
        assert fresh["attempts"][identity] == [version, attempt.attempt_document]
    assert _file_state(pipeline.database) == before  # nothing repaired or migrated on read
    # And in this process, through new adapter instances.
    store = SQLiteEquityRecommendationGenerationStore(pipeline.database)
    for key, (_, disposition) in mixed["dispositions"].items():
        assert store.load_disposition(key) == disposition
    for identity, (_, attempt) in mixed["attempts"].items():
        assert store.load_attempt(identity) == attempt
    check_database(pipeline.database)


# ----------------------------------------------------------------------
# PS-05: concurrent writers of both versions on one key, in separate processes
# ----------------------------------------------------------------------


def test_ps05_concurrent_version1_and_version2_processes_on_one_key(
    pipeline: Pipeline, research: Research
) -> None:
    audits = _same_key_pair(pipeline)
    offered = {version: _records(audit) for version, audit in audits.items()}
    pipeline.store().load_disposition(audits[_V1].generation_key)  # type: ignore[arg-type]
    context = multiprocessing.get_context("spawn")  # the Windows start method, everywhere
    start, results = context.Barrier(2), context.Queue()
    processes = [
        context.Process(
            target=record_in_another_process,
            args=(str(pipeline.database), documents(*records), start, results, version),
        )
        for version, records in offered.items()
    ]
    for process in processes:
        process.start()
    reported = {}
    for _ in processes:
        name, pid, outcome, attempt_recorded, error = results.get(timeout=180)
        reported[name] = (pid, outcome, attempt_recorded, error)
    for process in processes:
        process.join(timeout=60)
        assert process.exitcode == 0
    assert all(error is None for _, _, _, error in reported.values()), reported
    assert len({pid for pid, _, _, _ in reported.values()} | {os.getpid()}) == 3
    outcomes = sorted(outcome for _, outcome, _, _ in reported.values())
    assert outcomes == ["COMMITTED", "EVALUATOR_VERSION_CONFLICT"]
    winner = next(n for n, (_, o, _, _) in reported.items() if o == "COMMITTED")
    (row,) = rows(pipeline.database, DISPOSITIONS)
    assert row["evaluator_version"] == winner
    assert row["assessment_fingerprint"] == offered[winner][0].assessment_fingerprint
    assert all(recorded is True for _, _, recorded, _ in reported.values())
    assert counts(pipeline.database) == (1, 2)
    check_database(pipeline.database)


# ----------------------------------------------------------------------
# PS-06: an unconfirmed commit that reconciles to the other version's record
# ----------------------------------------------------------------------


class _CompetitorBeforeReload(SQLiteEquityRecommendationGenerationStore):
    """Test-only coordination: another writer commits before this store's first reload."""

    def __init__(self, database: Path, competitor: Callable[[], None]) -> None:
        super().__init__(database)
        self._competitor: Callable[[], None] | None = competitor

    def load_disposition(
        self, generation_key: str
    ) -> EquityRecommendationGenerationDisposition | None:
        if self._competitor is not None:
            competitor, self._competitor = self._competitor, None
            competitor()
        return super().load_disposition(generation_key)


@pytest.mark.parametrize(("ours", "theirs"), [(_V1, _V2), (_V2, _V1)], ids=["v1 ours", "v2 ours"])
def test_ps06_an_unconfirmed_commit_reconciling_to_the_other_version_is_a_conflict(
    pipeline: Pipeline,
    research: Research,
    monkeypatch: pytest.MonkeyPatch,
    ours: str,
    theirs: str,
) -> None:
    audits = _same_key_pair(pipeline)
    their_write = pipeline.authorize(audits[theirs])

    def competitor() -> None:  # an independent adapter and connection, its own verified write
        written = SQLiteEquityRecommendationGenerationStore(pipeline.database).write_generation(
            their_write
        )
        assert written.outcome is _W.COMMITTED

    real, armed = adapter_module._commit, [True]  # noqa: SLF001

    def commit(connection: sqlite3.Connection) -> None:
        if armed[0]:  # the next COMMIT fails before it takes effect, once
            armed[0] = False
            raise sqlite3.OperationalError("test: COMMIT failed before it took effect")
        real(connection)

    monkeypatch.setattr(adapter_module, "_commit", commit)
    store = _CompetitorBeforeReload(pipeline.database, competitor)
    result = pipeline.recorder(store).execute(audits[ours])
    assert result.reconciled and result.writes == 1
    assert result.outcome is _O.EVALUATOR_VERSION_CONFLICT  # never EQUIVALENT_COMMITTED
    assert result.authoritative is None and not result.is_stored
    assert result.attempt_recorded is False  # this write rolled back; never claimed
    (row,) = rows(pipeline.database, DISPOSITIONS)
    assert row["evaluator_version"] == theirs
    assert counts(pipeline.database) == (1, 1)
    check_database(pipeline.database)


# ----------------------------------------------------------------------
# PS-07, VC-16: a wrong-version write is refused before a connection opens
# ----------------------------------------------------------------------


@pytest.fixture
def connections(monkeypatch: pytest.MonkeyPatch) -> list[Path]:
    """Every call of the adapter's real connection seam, ``_connect``."""
    opened: list[Path] = []
    real = adapter_module._connect  # noqa: SLF001

    def connect(database_path: Path) -> sqlite3.Connection:
        opened.append(database_path)
        return real(database_path)

    monkeypatch.setattr(adapter_module, "_connect", connect)
    return opened


@contextmanager
def _refused_before_sqlite(database: Path, opened: list[Path]) -> Iterator[None]:
    before = (rows(database, DISPOSITIONS), rows(database, ATTEMPTS))
    calls = len(opened)
    with pytest.raises(UnverifiedEquityRecommendationGenerationWriteError):
        yield
    assert len(opened) == calls, "a refused write opened a SQLite connection"
    assert (rows(database, DISPOSITIONS), rows(database, ATTEMPTS)) == before


def _tamper(obj: object, name: str, value: object) -> None:
    """TEST-ONLY privileged tampering with a frozen record (outside the trust boundary)."""
    object.__setattr__(obj, name, value)


def test_ps07_vc16_a_wrong_version_authorized_write_is_refused_before_sqlite(
    pipeline: Pipeline, research: Research, connections: list[Path]
) -> None:
    audits = _same_key_pair(pipeline)
    v1, v1_attempt = _records(audits[_V1])
    pipeline.store().load_disposition(v1.generation_key)  # the schema exists
    store = pipeline.store()

    relabelled = pipeline.authorize(audits[_V2])
    _tamper(relabelled.disposition, "evaluator_version", _V1)  # an approved label, swapped
    with _refused_before_sqlite(pipeline.database, connections):
        store.write_generation(relabelled)

    swapped_attempt = pipeline.authorize(audits[_V2])
    _tamper(swapped_attempt, "attempt", v1_attempt)  # the same key's version-1 attempt
    with _refused_before_sqlite(pipeline.database, connections):
        store.write_generation(swapped_attempt)

    swapped_content = pipeline.authorize(audits[_V1])
    v2, _ = _records(audits[_V2])
    for field_name in ("evaluator_version", "assessment_fingerprint", "assessment_document"):
        _tamper(swapped_content.disposition, field_name, getattr(v2, field_name))
    with _refused_before_sqlite(pipeline.database, connections):
        store.write_generation(swapped_content)

    keyless = pipeline.authorize(_generate(pipeline, _V2, History(mode="missing")))
    _tamper(
        keyless,
        "attempt",
        equity_recommendation_attempt(_generate(pipeline, _V1, History(mode="missing"))),
    )
    with _refused_before_sqlite(pipeline.database, connections):
        store.write_attempt(keyless)
    assert counts(pipeline.database) == (0, 0)


# ----------------------------------------------------------------------
# Failure classification of stored version-2 rows; same-version conflicts
# ----------------------------------------------------------------------


def _assessment_with(document: str, **changes: object) -> str:
    loaded = json.loads(document)
    loaded.update(changes)
    return json.dumps(loaded, sort_keys=True, separators=(",", ":"))


@pytest.mark.parametrize(
    ("case", "error", "outcome"),
    [
        ("tampered fingerprint", InvalidEquityRecommendationRecordError, _O.STORED_RECORD_INVALID),
        (
            "non-canonical document",
            InvalidEquityRecommendationRecordError,
            _O.STORED_RECORD_INVALID,
        ),
        ("relabelled to /1", InvalidEquityRecommendationRecordError, _O.STORED_RECORD_INVALID),
        (
            "unknown evaluator",
            IncompatibleEquityRecommendationRecordError,
            _O.STORED_RECORD_INCOMPATIBLE,
        ),
        (
            "unknown format",
            IncompatibleEquityRecommendationRecordError,
            _O.STORED_RECORD_INCOMPATIBLE,
        ),
    ],
)
def test_a_corrupted_stored_version2_disposition_is_classified_and_never_repaired(
    pipeline: Pipeline, research: Research, case: str, error: type[Exception], outcome: Any
) -> None:
    audit = _v2_proposed(pipeline)
    disposition, _ = _records(audit)
    assert pipeline.recorder().execute(audit).outcome is _O.COMMITTED
    document = disposition.assessment_document
    column, value, checks = {
        "tampered fingerprint": ("assessment_fingerprint", "eira1-sha256:" + "0" * 64, True),
        "non-canonical document": (
            "assessment_document",
            json.dumps(json.loads(document), sort_keys=True, indent=1),
            True,
        ),
        "relabelled to /1": ("evaluator_version", _V1, True),  # known label, /2 formats
        "unknown evaluator": ("evaluator_version", _UNSUPPORTED, True),
        "unknown format": (
            "assessment_document",
            _assessment_with(document, format="northstar.equity-recommendation-assessment/9"),
            False,
        ),
    }[case]
    bypass(  # TEST-ONLY corruption of the disposable database
        pipeline.database,
        (f"UPDATE {DISPOSITIONS} SET {column} = ?", (value,)),  # noqa: S608
        drop=(f"{DISPOSITIONS}_no_update",),
        checks=checks,
    )
    corrupted = rows(pipeline.database, DISPOSITIONS)
    with pytest.raises(error) as raised:
        pipeline.store().load_disposition(disposition.generation_key)
    if error is InvalidEquityRecommendationRecordError:
        assert not isinstance(raised.value, IncompatibleEquityRecommendationRecordError)
    with pytest.raises(error):
        pipeline.verify_stored().execute(disposition.generation_key)
    result = pipeline.recorder().execute(_v2_proposed(pipeline))
    assert result.outcome is outcome and not result.is_stored
    assert result.attempt_recorded is None and result.authoritative is None
    assert rows(pipeline.database, DISPOSITIONS) == corrupted  # never repaired or replaced
    assert counts(pipeline.database) == (1, 1)


def test_missing_provenance_of_a_version2_record_fails_source_verification(
    pipeline: Pipeline, research: Research
) -> None:
    audit = _v2_proposed(pipeline)
    disposition, _ = _records(audit)
    assert pipeline.recorder().execute(audit).outcome is _O.COMMITTED
    snapshot = research.s41.snapshot_id
    bypass(  # TEST-ONLY: remove the CANDIDATE's snapshot with foreign keys off
        pipeline.database,
        ("DELETE FROM equity_candle_evidence_snapshot_entries WHERE snapshot_id = ?", (snapshot,)),
        ("DELETE FROM equity_candle_evidence_snapshots WHERE snapshot_id = ?", (snapshot,)),
        drop=(
            "equity_candle_evidence_snapshots_no_delete",
            "equity_candle_evidence_snapshot_entries_no_delete",
        ),
        foreign_keys=False,
    )
    assert pipeline.store().load_disposition(disposition.generation_key) == disposition
    with pytest.raises(EquityRecommendationSourceVerificationError):
        pipeline.verify_stored().execute(disposition.generation_key)


def test_same_version2_key_with_other_content_is_a_content_conflict(
    pipeline: Pipeline, research: Research
) -> None:
    proposed = _v2_proposed(pipeline)
    first, _ = _records(proposed)
    assert pipeline.recorder().execute(proposed).outcome is _O.COMMITTED
    before = rows(pipeline.database, DISPOSITIONS)
    unresolved = _v2_unresolved(pipeline)
    assert unresolved.generation_key == first.generation_key
    result = pipeline.recorder().execute(unresolved)
    assert result.outcome is _O.CONTENT_CONFLICT  # within one version: content, not evaluator
    assert result.authoritative == first and result.attempt_recorded is True
    assert rows(pipeline.database, DISPOSITIONS) == before
    assert counts(pipeline.database) == (1, 2)


# ----------------------------------------------------------------------
# Atomicity, idempotence and foreign keys with version-2 records
# ----------------------------------------------------------------------


def _verbs(statements: list[str]) -> list[str]:
    def verb(statement: str) -> str:
        words = statement.split()
        if words[0] == "INSERT":
            return f"INSERT {words[2]}"
        return " ".join(words[:2]) if words[0] == "BEGIN" else words[0]

    return [verb(s) for s, _ in groupby(statements) if not s.startswith("SELECT")]


def test_version2_writes_are_one_transaction_and_identical_retries_are_idempotent(
    pipeline: Pipeline, research: Research, monkeypatch: pytest.MonkeyPatch
) -> None:
    audits = _same_key_pair(pipeline)
    pipeline.store().load_disposition(audits[_V2].generation_key)  # type: ignore[arg-type]
    statements: list[str] = []
    real = adapter_module._connect  # noqa: SLF001

    def connect(path: Path) -> sqlite3.Connection:
        connection = real(path)
        connection.set_trace_callback(statements.append)
        return connection

    monkeypatch.setattr(adapter_module, "_connect", connect)
    authorization = pipeline.authorize(audits[_V2])
    statements.clear()
    first = pipeline.store().write_generation(authorization)
    assert (first.outcome, first.attempt_inserted) == (_W.COMMITTED, True)
    assert _verbs(statements) == [
        "BEGIN IMMEDIATE",
        f"INSERT {DISPOSITIONS}",
        f"INSERT {ATTEMPTS}",
        "COMMIT",
    ]
    statements.clear()
    retry = pipeline.store().write_generation(authorization)
    assert (retry.outcome, retry.attempt_inserted) == (_W.EQUIVALENT, False)
    assert _verbs(statements) == ["BEGIN IMMEDIATE", "COMMIT"]  # nothing written
    statements.clear()
    conflict = pipeline.recorder().execute(audits[_V1])
    assert conflict.outcome is _O.EVALUATOR_VERSION_CONFLICT
    assert [v for v in _verbs(statements) if v.startswith(("BEGIN", "INSERT", "COMMIT"))] == [
        "BEGIN IMMEDIATE",
        f"INSERT {ATTEMPTS}",  # only the competing attempt is appended
        "COMMIT",
    ]
    assert pipeline.recorder().execute(audits[_V2]).outcome is _O.EQUIVALENT_COMMITTED
    assert counts(pipeline.database) == (1, 2)


def test_foreign_keys_remain_enforced_for_version2_rows(
    pipeline: Pipeline, research: Research, tmp_path: Path
) -> None:
    disposition, attempt = _records(_v2_proposed(pipeline))
    empty = tmp_path / "no-evidence.sqlite3"
    with closing(sqlite3.connect(empty)) as connection:
        initialize_equity_candle_evidence_schema(connection)
        initialize_equity_intraday_research_timeline_schema(connection)
        initialize_equity_recommendation_generation_schema(connection)
    with pytest.raises(sqlite3.IntegrityError, match="synthetic CANDIDATE"):  # source trigger
        plant_generation_records(empty, disposition, attempt)
    columns = ", ".join(adapter_module.DISPOSITION_COLUMNS)
    marks = ", ".join("?" * len(adapter_module.DISPOSITION_COLUMNS))
    with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY"):  # without the trigger
        bypass(
            empty,
            (
                f"INSERT INTO {DISPOSITIONS} ({columns}) VALUES ({marks})",  # noqa: S608
                adapter_module.disposition_row(disposition),
            ),
            drop=(f"{DISPOSITIONS}_source_matches",),
        )
    assert counts(empty) == (0, 0)
    with closing(adapter_module._connect(pipeline.database)) as connection:  # noqa: SLF001
        assert connection.execute("PRAGMA foreign_keys").fetchone() == (1,)
