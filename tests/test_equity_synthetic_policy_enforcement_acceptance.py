"""EI4-D10-S4: policy-version identity enforcement, end to end on real SQLite.

The whole chain runs over one disposable SQLite file, through the merged use
cases and adapters only -- no Application fake replaces any of them:

    synthetic candles -> EI-2C ingestion -> EI-2B snapshots -> EI-3A evaluation
    -> EI-3C first CANDIDATE -> explicit S1 publication into the S2 catalog
    -> EI-4C generation, checked against the catalog (S3, layer D)
    -> EI-4D-A recording gate (S3, layer E) -> EI-4D-B SQLite store
    -> restart and frozen historical replay

The fixtures (tests/equity_recommendation_acceptance_fixtures.py) publish
the synthetic policy sets explicitly, through PublishEquitySyntheticPolicies-
UseCase, before anything is generated; generation and recording only read
the catalog. Market, listing, venue and issued-history inputs are the EI-4E
labelled synthetic fixtures. Faults are labelled where they are made:

- a catalog outage is a real SQLite lock (``BEGIN EXCLUSIVE`` on another
  connection) held while the catalog is read, with the adapter's busy
  timeout shortened;
- corrupt and unsupported bindings are written by the test-only ``bypass()``;
- a forged disposition is a genuine record generated against
  ``scratch_catalog`` -- a real catalog in ANOTHER file, never the authority.

Counting subclasses of the real catalog adapter count reads and writes; they
change no behaviour. Nothing here tests a direct EI-4D-B adapter write, which
EI4-D11 governs. Separate-process tests run on Windows here; nothing is
claimed for POSIX or several hosts. No network, clock or broker is used.
"""

from __future__ import annotations

import json
import socket
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from northstar_application.application_services import (
    AuditEquityRecommendationPolicyBindingsUseCase,
    EquityRecommendationEligibilityReason,
    EquityRecommendationGenerationStatus,
    EquityRecommendationPolicyBindingStatus,
    EquityRecommendationRecordingOutcome,
    EquitySyntheticPolicyBindingVerdict,
    EquitySyntheticPolicyBundle,
    EquitySyntheticPolicyPublicationOutcome,
    EquitySyntheticPolicyType,
    PublishEquitySyntheticPoliciesUseCase,
    assess_equity_recommendation_inputs,
    equity_recommendation_attempt,
    equity_recommendation_disposition,
)
from northstar_application.ports import (
    EquitySyntheticEligibilityCriterion,
    EquitySyntheticEligibilityVerdict,
)
from northstar_core.recommendations.equity import EquitySyntheticVersion

from northstar_infrastructure.persistence import SQLiteEquitySyntheticPolicyCatalogStore
from northstar_infrastructure.persistence import (
    sqlite_equity_synthetic_policy_catalog as catalog_module,
)
from tests.equity_recommendation_acceptance_fixtures import (
    ATTEMPTS,
    DISPOSITIONS,
    PUBLISHED_POLICY_SETS,
    Evidence,
    Pipeline,
    References,
    Research,
    bypass,
    check_database,
    counts,
    establish_research,
    ist,
    policy_bundle,
    pre_enforcement_catalog,
    request,
    rows,
    schema_objects,
    scratch_catalog,
    timing_policy,
)

_O = EquityRecommendationRecordingOutcome
_S = EquityRecommendationGenerationStatus
_R = EquityRecommendationEligibilityReason
_V = EquitySyntheticPolicyBindingVerdict
_T = EquitySyntheticPolicyType
_P = EquitySyntheticPolicyPublicationOutcome
_A = EquityRecommendationPolicyBindingStatus
PUBLICATIONS = "equity_synthetic_policy_publications"
DEFINITIONS = "equity_synthetic_policy_definitions"
POLICY_SETS = "equity_synthetic_policy_sets"
_CATALOG = (PUBLICATIONS, DEFINITIONS, POLICY_SETS)
_ALL_ABSENT = (
    _R.TIMING_POLICY_UNAVAILABLE,
    _R.GENERATION_POLICY_UNAVAILABLE,
    _R.RISK_POLICY_UNAVAILABLE,
    _R.COST_SCHEDULE_UNAVAILABLE,
    _R.PRICE_INCREMENT_UNAVAILABLE,
    _R.POLICY_SET_UNAVAILABLE,
)
_STATUSES = (
    "PROPOSED",
    "NO_CANDIDATE",
    "NOT_ELIGIBLE",
    "EVALUATION_UNRESOLVED",
    "DEPENDENCY_UNAVAILABLE",
    "RESEARCH_INTEGRITY_FAILURE",
)


@pytest.fixture(autouse=True)
def _refuse_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("EI4-D10-S4 acceptance must not open network connections.")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)


@pytest.fixture
def pipeline(tmp_path: Path) -> Pipeline:
    return Pipeline(tmp_path / "ei4-d10-s4.sqlite3")


@pytest.fixture
def research(pipeline: Pipeline) -> Research:
    return establish_research(pipeline)  # publishes PUBLISHED_POLICY_SETS explicitly


@pytest.fixture
def short_catalog_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test-only: a locked catalog fails fast instead of after five seconds."""
    monkeypatch.setattr(catalog_module, "_BUSY_TIMEOUT_SECONDS", 0.05)


class _Counted(SQLiteEquitySyntheticPolicyCatalogStore):
    """The real SQLite catalog adapter, counting its reads and publications only."""

    def __init__(self, database: Path) -> None:
        super().__init__(database)
        self.loads = 0
        self.publishes = 0

    def load_definition(self, identity):  # type: ignore[no-untyped-def]
        self.loads += 1
        return super().load_definition(identity)

    def load_policy_set(self, identity):  # type: ignore[no-untyped-def]
        self.loads += 1
        return super().load_policy_set(identity)

    def publish(self, bundle: EquitySyntheticPolicyBundle):  # type: ignore[no-untyped-def]
        self.publishes += 1
        return super().publish(bundle)


@contextmanager
def _locked(database: Path) -> Iterator[None]:
    """FAULT INJECTION, labelled: another connection holds the database's exclusive lock."""
    holder = sqlite3.connect(database, isolation_level=None)
    holder.execute("BEGIN EXCLUSIVE")
    try:
        yield
    finally:
        holder.rollback()
        holder.close()


class _LockAfterReferences(References):
    """FAULT INJECTION, labelled: the lock is taken once every component was answered.

    The generator reads the five components (price increment last), then the
    catalog: so the catalog -- and only the catalog -- meets the lock.
    """

    def __init__(self, database: Path) -> None:
        super().__init__()
        self.database = database
        self.holder: sqlite3.Connection | None = None

    def price_increment(self, version, listing, trading_date):  # type: ignore[no-untyped-def]
        answer = super().price_increment(version, listing, trading_date)
        self.holder = sqlite3.connect(self.database, isolation_level=None)
        self.holder.execute("BEGIN EXCLUSIVE")
        return answer

    def release(self) -> None:
        if self.holder is not None:
            self.holder.rollback()
            self.holder.close()
            self.holder = None


def _counts(database: Path) -> tuple[int, int]:
    """Dispositions and attempts; (0, 0) when the generation store was never even opened."""
    if DISPOSITIONS not in schema_objects(database):
        return 0, 0
    return counts(database)


def _catalog_rows(database: Path) -> dict[str, list[dict[str, Any]]]:
    return {table: rows(database, table) for table in _CATALOG}


def _ei4e(name: str) -> EquitySyntheticVersion:
    return EquitySyntheticVersion(f"synthetic-ei4e-{name}")


def _other_costs(references: References) -> None:
    """Other content under the published label "costs/1": a 0.0005 slippage, not 0.0003."""
    references.costs["costs/1"] = replace(
        references.costs["costs/1"], slippage_rate_per_side=Decimal("0.0005")
    )


def _forged(pipeline: Pipeline, tmp_path: Path, change: Any, **changes: Any) -> Any:
    """A genuine audit generated with ``change``d policies against a NON-authority catalog."""
    forged = References()
    change(forged)
    set_version = changes.get("set_version", "set/1")
    timing = changes.get("timing", "timing/1")
    other = scratch_catalog(
        tmp_path / "forger-catalog.sqlite3",
        policy_bundle(forged, set_version, timing=timing),
    )
    authority, pipeline.references = pipeline.references, forged
    try:
        audit = pipeline.generator(other).execute(request(**changes))
    finally:
        pipeline.references = authority
    assert audit.status is _S.PROPOSED, audit.reasons
    return audit


def _detail(audit: Any) -> str:
    return next(c.detail for c in audit.checks if c.check.value == "SYNTHETIC_REFERENCES")


# ----------------------------------------------------------------------
# The published chain: generate, record through the gate, restart, replay
# ----------------------------------------------------------------------


def test_s4_the_published_chain_generates_records_restarts_and_replays(
    pipeline: Pipeline, research: Research, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    catalog = _catalog_rows(pipeline.database)
    assert len(catalog[POLICY_SETS]) == len(PUBLISHED_POLICY_SETS)
    assert len(catalog[PUBLICATIONS]) == len(PUBLISHED_POLICY_SETS)
    counted = _Counted(pipeline.database)
    audit = pipeline.generator(counted).execute(request())
    assert audit.status is _S.PROPOSED and audit.inputs is not None
    assert counted.loads == 6 and counted.publishes == 0  # each identity read once
    bundle = policy_bundle(pipeline.references)
    assert audit.inputs.policy_set.fingerprint == bundle.policy_set.policy_set_fingerprint
    result = pipeline.recorder(catalog=counted).execute(audit)
    assert (result.outcome, result.attempt_recorded, result.writes) == (_O.COMMITTED, True, 1)
    assert result.policy_verification is not None and result.policy_verification.is_verified
    assert counted.loads == 12 and counted.publishes == 0  # the gate read it again, once
    assert _counts(pipeline.database) == (1, 1)
    disposition = equity_recommendation_disposition(audit)
    assert disposition is not None
    # Restart: every connection is closed (Windows refuses to rename an open file).
    moved = tmp_path / "after-restart.sqlite3"
    pipeline.database.rename(moved)
    restarted = Pipeline(moved)
    store = SQLiteEquitySyntheticPolicyCatalogStore(moved)
    assert store.load_policy_set(bundle.policy_set.identity) == bundle.policy_set
    for component in bundle.components:
        assert store.load_definition(component.identity) == component
    again = restarted.generate()
    assert (again.generation_key, again.assessment_fingerprint) == (
        audit.generation_key,
        audit.assessment_fingerprint,
    )
    assert restarted.recorder().execute(again).outcome is _O.EQUIVALENT_COMMITTED
    assert _counts(moved) == (1, 1) and _catalog_rows(moved) == catalog

    # Historical replay never reads the catalog: make every catalog read fail.
    def refuse(*_args: object) -> None:
        raise AssertionError("historical replay must not read the policy catalog")

    monkeypatch.setattr(SQLiteEquitySyntheticPolicyCatalogStore, "load_definition", refuse)
    monkeypatch.setattr(SQLiteEquitySyntheticPolicyCatalogStore, "load_policy_set", refuse)
    verified = restarted.verify_stored().execute(disposition.generation_key)
    assert verified is not None and verified.evaluation_reproduced
    replayed = assess_equity_recommendation_inputs(audit.request, disposition.inputs)
    assert replayed.assessment_fingerprint == disposition.assessment_fingerprint
    check_database(moved)


# ----------------------------------------------------------------------
# Generation validation: unpublished, conflicting, outage
# ----------------------------------------------------------------------


def test_s4_an_unpublished_component_or_set_fails_closed_and_is_never_published(
    pipeline: Pipeline, research: Research
) -> None:
    catalog = _catalog_rows(pipeline.database)
    pipeline.references.timing["timing/new"] = timing_policy("timing/new", timedelta(0))
    new = pipeline.generate(set_version="set/new", timing="timing/new")
    assert new.status is _S.DEPENDENCY_UNAVAILABLE
    assert new.reasons == (_R.TIMING_POLICY_UNAVAILABLE, _R.POLICY_SET_UNAVAILABLE)
    unpublished_set = pipeline.generate(set_version="set/unpublished")  # published components
    assert unpublished_set.reasons == (_R.POLICY_SET_UNAVAILABLE,)
    for audit in (new, unpublished_set):
        assert audit.generation_key is None and audit.inputs is None  # attempt-only
        assert pipeline.recorder().execute(audit).outcome is _O.COMMITTED  # the attempt
    assert _counts(pipeline.database) == (0, 2)
    assert _catalog_rows(pipeline.database) == catalog  # nothing published implicitly


def test_s4_an_empty_catalog_is_never_an_empty_authorized_registry(
    pipeline: Pipeline,
) -> None:
    establish_research(pipeline, publish=False)
    audit = pipeline.generate()
    assert audit.status is _S.DEPENDENCY_UNAVAILABLE and audit.reasons == _ALL_ABSENT
    assert {t: len(r) for t, r in _catalog_rows(pipeline.database).items()} == dict.fromkeys(
        _CATALOG, 0
    )


def test_s4_other_content_under_a_published_component_label_is_a_permanent_conflict(
    pipeline: Pipeline, research: Research
) -> None:
    catalog = _catalog_rows(pipeline.database)
    _other_costs(pipeline.references)
    refused = PublishEquitySyntheticPoliciesUseCase(_Counted(pipeline.database)).execute(
        policy_bundle(pipeline.references)
    )
    assert (refused.outcome, refused.writes) == (_P.POLICY_REFERENCE_CONFLICT, 1)  # no retry
    counted = _Counted(pipeline.database)
    first = pipeline.generator(counted).execute(request())
    assert first.status is _S.DEPENDENCY_UNAVAILABLE
    assert first.reasons == (_R.POLICY_REFERENCE_CONFLICT,)
    assert first.generation_key is None and first.inputs is None and first.proposal is None
    assert (counted.loads, counted.publishes) == (6, 0)  # each identity once; nothing retried
    second = pipeline.generator(counted).execute(request())
    assert second.reasons == first.reasons and (counted.loads, counted.publishes) == (12, 0)
    assert _catalog_rows(pipeline.database) == catalog  # never republished or overwritten


def test_s4_a_set_label_rebound_to_other_components_is_a_permanent_conflict(
    pipeline: Pipeline, research: Research
) -> None:
    catalog = _catalog_rows(pipeline.database)
    # Every component is published (timing/late under set/late); set/1 names timing/1.
    audit = pipeline.generate(set_version="set/1", timing="timing/late")
    assert audit.reasons == (_R.POLICY_REFERENCE_CONFLICT,)
    assert "POLICY_SET" in _detail(audit) and "TIMING_POLICY" not in _detail(audit)
    refused = PublishEquitySyntheticPoliciesUseCase(pipeline.catalog()).execute(
        policy_bundle(pipeline.references, "set/1", timing="timing/late")
    )
    assert refused.outcome is _P.POLICY_REFERENCE_CONFLICT
    assert _catalog_rows(pipeline.database) == catalog


@pytest.mark.usefixtures("short_catalog_timeout")
def test_s4_scenario_30_a_catalog_outage_during_generation_fails_closed(
    pipeline: Pipeline, research: Research
) -> None:
    catalog = _catalog_rows(pipeline.database)
    locking = _LockAfterReferences(pipeline.database)
    pipeline.references = locking
    try:
        audit = pipeline.generate()
    finally:
        locking.release()
    assert audit.status is _S.DEPENDENCY_UNAVAILABLE and audit.reasons == _ALL_ABSENT
    assert _R.POLICY_REFERENCE_CONFLICT not in audit.reasons
    assert audit.generation_key is None and audit.inputs is None  # attempt-only
    assert _catalog_rows(pipeline.database) == catalog  # nothing invented or published
    assert _counts(pipeline.database) == (0, 0)  # generation never records
    pipeline.references = References()  # the lock is gone; nothing re-takes it
    recovered = pipeline.generate()  # no automatic retry: a new call, after the outage
    assert recovered.status is _S.PROPOSED


@pytest.mark.usefixtures("short_catalog_timeout")
def test_s4_scenario_31_a_catalog_outage_at_the_recording_gate_writes_nothing(
    pipeline: Pipeline, research: Research
) -> None:
    audit = pipeline.generate()
    assert audit.status is _S.PROPOSED
    recorder = pipeline.recorder()
    with _locked(pipeline.database):
        result = recorder.execute(audit)
    assert result.outcome is _O.POLICY_REFERENCE_UNAVAILABLE
    assert (result.writes, result.attempt_recorded, result.authoritative) == (0, False, None)
    assert result.policy_verification is not None
    assert result.policy_verification.verdict is _V.UNAVAILABLE
    assert _counts(pipeline.database) == (0, 0)  # neither the disposition nor the attempt
    assert pipeline.recorder().execute(audit).outcome is _O.COMMITTED  # once it can be read
    assert _counts(pipeline.database) == (1, 1)


@pytest.mark.usefixtures("short_catalog_timeout")
def test_s4_scenario_32_a_permanent_conflict_and_an_outage_are_told_apart_by_value(
    pipeline: Pipeline, research: Research, tmp_path: Path
) -> None:
    # Generation, on the same candidate: an outage, then a conflicting label.
    locking = _LockAfterReferences(pipeline.database)
    pipeline.references = locking
    try:
        outage = pipeline.generate()
    finally:
        locking.release()
    pipeline.references = References()
    _other_costs(pipeline.references)
    conflict = pipeline.generate()
    assert outage.status is conflict.status is _S.DEPENDENCY_UNAVAILABLE
    assert _R.POLICY_REFERENCE_CONFLICT in conflict.reasons
    assert _R.POLICY_REFERENCE_CONFLICT not in outage.reasons
    assert (
        equity_recommendation_attempt(outage).attempt_identity
        != equity_recommendation_attempt(conflict).attempt_identity
    )
    # Recording: a forged disposition, and a valid one while the catalog is locked.
    pipeline.references = References()
    forged = _forged(pipeline, tmp_path, _other_costs)
    valid = pipeline.generate()
    refused = pipeline.recorder().execute(forged)
    recorder = pipeline.recorder()
    with _locked(pipeline.database):
        unavailable = recorder.execute(valid)
    assert refused.outcome is _O.POLICY_REFERENCE_CONFLICT
    assert unavailable.outcome is _O.POLICY_REFERENCE_UNAVAILABLE
    for audit in (outage, conflict):
        assert audit.status not in (_S.NOT_ELIGIBLE, _S.EVALUATION_UNRESOLVED)
    for result in (refused, unavailable):
        assert result.outcome not in (_O.CONTENT_CONFLICT, _O.NOT_COMMITTED)
    assert _counts(pipeline.database) == (0, 0)


def test_s4_scenario_33_a_permanent_conflict_is_never_retried_or_republished(
    pipeline: Pipeline, research: Research, tmp_path: Path
) -> None:
    catalog = _catalog_rows(pipeline.database)
    # (1) publication: one transaction, refused, never retried.
    changed = References()
    _other_costs(changed)
    counted = _Counted(pipeline.database)
    published = PublishEquitySyntheticPoliciesUseCase(counted).execute(policy_bundle(changed))
    assert (published.outcome, published.writes, counted.publishes) == (
        _P.POLICY_REFERENCE_CONFLICT,
        1,
        1,
    )
    # (2) generation: each label read once; nothing published.
    pipeline.references = changed
    generation = _Counted(pipeline.database)
    audit = pipeline.generator(generation).execute(request())
    assert audit.reasons == (_R.POLICY_REFERENCE_CONFLICT,)
    assert (generation.loads, generation.publishes) == (6, 0)
    # (3) recording: the gate refuses a conflicting disposition with zero writes.
    pipeline.references = References()
    forged = _forged(pipeline, tmp_path, _other_costs)
    gate = _Counted(pipeline.database)
    result = pipeline.recorder(catalog=gate).execute(forged)
    assert (result.outcome, result.writes) == (_O.POLICY_REFERENCE_CONFLICT, 0)
    assert (gate.loads, gate.publishes) == (6, 0)
    assert _catalog_rows(pipeline.database) == catalog  # byte-identical binding rows
    assert _counts(pipeline.database) == (0, 0)  # no disposition from contradictory policy


@pytest.mark.parametrize(
    ("corruption", "verdict"),
    [("fingerprint mismatch", _V.INVALID), ("unsupported format", _V.INCOMPATIBLE)],
)
def test_s4_corrupt_and_unsupported_catalog_bindings_fail_closed(
    pipeline: Pipeline, research: Research, corruption: str, verdict: Any
) -> None:
    audit = pipeline.generate()  # generated before the corruption
    timing = policy_bundle(pipeline.references).component(_T.TIMING_POLICY)
    document = json.loads(timing.canonical_json())
    fingerprint = timing.fingerprint
    if corruption == "fingerprint mismatch":
        fingerprint = "eipd1-sha256:" + "0" * 64
    else:
        document["format"] = "northstar.equity-synthetic-policy-definition/2"
    bypass(  # TEST-ONLY corruption of the one published timing/1 row
        pipeline.database,
        (
            f"UPDATE {DEFINITIONS} SET definition_document = ?, definition_fingerprint = ? "  # noqa: S608
            "WHERE policy_type = 'TIMING_POLICY' AND version_label = ?",
            (
                json.dumps(document, sort_keys=True, separators=(",", ":")),
                fingerprint,
                timing.version.value,
            ),
        ),
        drop=(f"{DEFINITIONS}_no_update",),
        checks=False,
    )
    corrupted = _catalog_rows(pipeline.database)
    regenerated = pipeline.generate()
    assert regenerated.reasons == (_R.POLICY_REFERENCE_CONFLICT,)  # ADR-022 section 8, case D
    assert _detail(regenerated).startswith(f"{verdict.value}:")
    result = pipeline.recorder().execute(audit)
    assert (result.outcome, result.writes) == (_O.POLICY_REFERENCE_CONFLICT, 0)
    assert result.policy_verification is not None
    assert result.policy_verification.verdict is verdict  # the typed distinction is kept
    assert _counts(pipeline.database) == (0, 0)
    assert _catalog_rows(pipeline.database) == corrupted  # nothing repaired or rewritten


# ----------------------------------------------------------------------
# The recording gate against bypass
# ----------------------------------------------------------------------


def test_s4_the_recorder_refuses_a_forged_disposition_before_any_write(
    pipeline: Pipeline, research: Research, tmp_path: Path
) -> None:
    forged = _forged(pipeline, tmp_path, _other_costs)
    disposition = equity_recommendation_disposition(forged)
    assert disposition is not None
    result = pipeline.recorder().record(disposition, equity_recommendation_attempt(forged))
    assert result.outcome is _O.POLICY_REFERENCE_CONFLICT
    assert (result.writes, result.attempt_recorded, result.authoritative) == (0, False, None)
    assert result.policy_verification is not None
    failed = {c.identity.policy_type: c.verdict for c in result.policy_verification.failed}
    assert failed == {_T.COST_SCHEDULE: _V.CONFLICT, _T.POLICY_SET: _V.CONFLICT}
    assert _counts(pipeline.database) == (0, 0)


def test_s4_the_recorder_refuses_unpublished_policy_evidence_before_any_write(
    pipeline: Pipeline, research: Research, tmp_path: Path
) -> None:
    def copy_timing(references: References) -> None:  # timing/1's values, another label
        references.timing["timing/copy"] = timing_policy("timing/copy", timedelta(seconds=150))

    forged = _forged(pipeline, tmp_path, copy_timing, set_version="set/copy", timing="timing/copy")
    result = pipeline.recorder().execute(forged)
    assert result.outcome is _O.POLICY_REFERENCE_NOT_ESTABLISHED
    assert (result.writes, result.attempt_recorded) == (0, False)
    assert _counts(pipeline.database) == (0, 0)


# ----------------------------------------------------------------------
# Attempt-only semantics and the unchanged attempt statuses
# ----------------------------------------------------------------------


def test_s4_a_policy_failure_is_an_attempt_only_record_with_a_machine_readable_reason(
    pipeline: Pipeline, research: Research
) -> None:
    _other_costs(pipeline.references)
    audit = pipeline.generate()
    assert equity_recommendation_disposition(audit) is None and audit.proposal is None
    first = pipeline.recorder().execute(audit)
    assert (first.outcome, first.attempt_recorded, first.disposition) == (_O.COMMITTED, True, None)
    assert first.policy_verification is None  # attempt-only: the gate has nothing to check
    again = pipeline.recorder().execute(pipeline.generate())  # the identical attempt
    assert (again.outcome, again.attempt_recorded) == (_O.EQUIVALENT_COMMITTED, True)
    assert _counts(pipeline.database) == (0, 1)
    (row,) = rows(pipeline.database, ATTEMPTS)
    assert (row["status"], row["generation_key"], row["disposition_kind"]) == (
        "DEPENDENCY_UNAVAILABLE",
        None,
        None,
    )
    document = json.loads(row["attempt_document"])
    assert "POLICY_REFERENCE_CONFLICT" in json.dumps(document["reasons"])
    # No new persisted attempt status: the attempts table CHECK is the six EI4-D9 values.
    (sql,) = (
        definition
        for name, (kind, definition) in schema_objects(pipeline.database).items()
        if name == ATTEMPTS
    )
    assert "POLICY_REFERENCE" not in sql
    assert all(f"'{status}'" in sql for status in _STATUSES)


def test_s4_no_candidate_never_consults_the_catalog(pipeline: Pipeline) -> None:
    evidence = Evidence(pipeline.database)
    evidence.candles()
    evidence.issue(ist(9, 41))  # evidence, but no committed research timeline
    counted = _Counted(pipeline.database)
    audit = pipeline.generator(counted).execute(request())
    assert audit.status is _S.NO_CANDIDATE and counted.loads == 0


def test_s4_an_eligibility_refusal_under_published_policies_stays_not_eligible(
    pipeline: Pipeline, research: Research
) -> None:
    criterion = next(iter(EquitySyntheticEligibilityCriterion))
    pipeline.eligibility.verdicts = {criterion: EquitySyntheticEligibilityVerdict.INELIGIBLE}
    audit = pipeline.generate()
    assert audit.status is _S.NOT_ELIGIBLE and audit.reasons == (_R.LISTING_NOT_ELIGIBLE,)
    assert pipeline.recorder().execute(audit).outcome is _O.COMMITTED  # through the gate
    assert _counts(pipeline.database) == (1, 1)


# ----------------------------------------------------------------------
# Records from before enforcement: replay, audit, no backfill
# ----------------------------------------------------------------------


def test_s4_pre_enforcement_records_replay_and_audit_without_backfill(
    pipeline: Pipeline, tmp_path: Path
) -> None:
    establish_research(pipeline, publish=False)
    before = pre_enforcement_catalog(pipeline, tmp_path / "before.sqlite3")
    reused = References()
    reused.timing["timing/1"] = timing_policy("timing/1", timedelta(minutes=4))  # AA-1 content
    reusing = scratch_catalog(tmp_path / "reusing.sqlite3", policy_bundle(reused))
    made, audits_made = {}, {}
    strict = {"generation": "generation/strict", "set_version": "set/strict"}
    for name, catalog, references, changes in (
        ("matching", before, pipeline.references, {}),
        ("unbound", before, pipeline.references, strict),
        ("reused label", reusing, reused, {}),
    ):
        authority, pipeline.references = pipeline.references, references
        try:
            audit = pipeline.generator(catalog).execute(request(**changes))
            assert pipeline.recorder(catalog=catalog).execute(audit).is_stored
        finally:
            pipeline.references = authority
        disposition = equity_recommendation_disposition(audit)
        assert disposition is not None
        made[name], audits_made[name] = disposition, audit
    # Enforcement begins: only set/1 is published in this database's catalog.
    pipeline.publish(policy_bundle(pipeline.references))
    stored = rows(pipeline.database, DISPOSITIONS)
    catalog_rows = _catalog_rows(pipeline.database)
    audits = {
        name: AuditEquityRecommendationPolicyBindingsUseCase(
            pipeline.store(), pipeline.catalog()
        ).execute([disposition.generation_key])
        for name, disposition in made.items()
    }
    statuses = {name: audit.records[0].status for name, audit in audits.items()}
    assert statuses == {
        "matching": _A.BINDING_NOT_ESTABLISHED,  # today's match is no historical proof
        "unbound": _A.BINDING_NOT_ESTABLISHED,
        "reused label": _A.HISTORICAL_POLICY_VERSION_CONFLICT,
    }
    for disposition in made.values():  # replay from frozen inputs alone
        verified = pipeline.verify_stored().execute(disposition.generation_key)
        assert verified is not None and verified.evaluation_reproduced
    assert rows(pipeline.database, DISPOSITIONS) == stored  # nothing rewritten
    assert _catalog_rows(pipeline.database) == catalog_rows  # nothing backfilled
    # Re-recording old records through the gate never adopts or certifies them.
    attempts = rows(pipeline.database, ATTEMPTS)
    conflicting = pipeline.recorder().execute(audits_made["reused label"])
    assert (conflicting.outcome, conflicting.writes) == (_O.POLICY_REFERENCE_CONFLICT, 0)
    unbound = pipeline.recorder().execute(audits_made["unbound"])
    assert (unbound.outcome, unbound.writes) == (_O.POLICY_REFERENCE_NOT_ESTABLISHED, 0)
    assert rows(pipeline.database, DISPOSITIONS) == stored
    assert rows(pipeline.database, ATTEMPTS) == attempts
    check_database(pipeline.database)


def test_s4_unchanged_identities_reproduce_under_explicit_publication(
    pipeline: Pipeline, research: Research, tmp_path: Path
) -> None:
    published = pipeline.generate()
    before = pre_enforcement_catalog(pipeline, tmp_path / "before.sqlite3")
    unenforced = pipeline.generator(before).execute(request())
    # The same canonical policies give the same eirs1, eirk1 and assessment.
    assert published.inputs is not None and unenforced.inputs is not None
    assert published.inputs.policy_set.fingerprint == unenforced.inputs.policy_set.fingerprint
    assert published.generation_key == unenforced.generation_key
    assert published.assessment_fingerprint == unenforced.assessment_fingerprint
    assert _ei4e("set/1") == request().policy_set.version
