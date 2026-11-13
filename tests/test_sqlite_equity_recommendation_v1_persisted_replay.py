"""EI-4C compatibility slice 2: the version-1 persisted-row freeze (ADR-025, GV-07).

A pinned version-1 generation record -- a PROPOSED disposition, its keyed
attempt and a keyless NO_CANDIDATE attempt -- as it is stored on real SQLite
under the existing schema, and proof that it reloads and replays unchanged.

The reference
-------------
It was derived once, outside the test suite, from the unchanged production
code (Infrastructure ``c105e94``, Application ``ba79805``, Core ``ccff26e``), by
recording the EI-4D-B synthetic world of equity_recommendation_generation_
fixtures through the real verified recorder into a temporary database. Two
processes, with different hash seeds and different database paths (one with a
space), produced identical rows. It is stored under
``golden/equity_recommendation_v1_persisted``:

- the four canonical documents, pretty-printed (sorted keys, two-space
  indent) so they can be reviewed line by line; and
- ``rows.json``: every other column of the three rows, as stored.

The stored text of a document column is its canonical form, which a
test-local encoder below restates (sorted keys, no whitespace, ASCII). The
identities are literals here. Nothing in this module writes or regenerates the
reference.

Why not the application slice-1 goldens
---------------------------------------
Those documents rest on evidence that exists only in the Application's
in-memory test doubles. A disposition row must reference its CANDIDATE entry
and snapshot in the same SQLite file (foreign keys and the source trigger), so
they cannot be stored here. This reference is the same version-1 contract over
the SQLite evidence world; it shares every format, prefix and the evaluator
label with them. Application's keyless attempt golden needs no evidence and is
stored and reloaded here unchanged (test_slice1_*).

Two paths
---------
- **Path A** records the reference through the authorized EI4-D11 path and
  compares every stored column with the pinned reference.
- **Path B** plants the pinned rows into a fresh database by raw SQL (TEST-ONLY
  HISTORICAL-REFERENCE SETUP: rows that stand for records written in the past;
  constraints, foreign keys and triggers still apply), closes that connection,
  then reopens through the real adapter and verifies levels A to C, in this
  process and in a fresh one.

The tamper table records, for each invalid condition, which layer refuses it:
SQLite (a CHECK, foreign key or trigger, at INSERT) or the Application (record
construction when the adapter reads the row, or verification).

Everything is synthetic and local: a temporary file, no clock, no network.
The version-1 history boundary is inclusive through the modeled issuance t;
no version-2 field, format or label exists here.
"""

from __future__ import annotations

import hashlib
import json
import os
import socket
import sqlite3
import subprocess
import sys
from collections.abc import Callable
from contextlib import closing
from pathlib import Path
from typing import Any

import northstar_application
import pytest
from northstar_application.application_services import (
    EQUITY_RECOMMENDATION_ASSESSMENT_FORMAT,
    EQUITY_RECOMMENDATION_ATTEMPT_FORMAT,
    EQUITY_RECOMMENDATION_DOCUMENT_PREFIXES,
    EQUITY_RECOMMENDATION_EVALUATOR_VERSION,
    EQUITY_RECOMMENDATION_INPUTS_FORMAT,
    EQUITY_RECOMMENDATION_PROPOSAL_FORMAT,
    SUPPORTED_EQUITY_RECOMMENDATION_EVALUATOR_VERSIONS,
    EquityRecommendationDispositionKind,
    EquityRecommendationGenerationAttempt,
    EquityRecommendationRecordingOutcome,
    IncompatibleEquityRecommendationRecordError,
    InvalidEquityRecommendationRecordError,
    VerifiedEquityRecommendationGenerationWrite,
    VerifyStoredEquityRecommendationGenerationUseCase,
)
from northstar_application.ports import UnverifiedEquityRecommendationGenerationWriteError

from tests.equity_intraday_research_timeline_fixtures import raw
from tests.equity_recommendation_generation_fixtures import (
    ATTEMPTS,
    DISPOSITIONS,
    UNSUPPORTED_EVALUATOR,
    World,
)

_O = EquityRecommendationRecordingOutcome
_K = EquityRecommendationDispositionKind
_GOLDEN_DIR = Path(__file__).parent / "golden" / "equity_recommendation_v1_persisted"
_INFRA_ROOT = Path(__file__).resolve().parents[1]

# ----------------------------------------------------------------------
# The frozen version-1 contract and the pinned identities
# ----------------------------------------------------------------------

_EVALUATOR_V1 = "northstar.equity-recommendation-evaluator/1"
_EVALUATOR_V2 = "northstar.equity-recommendation-evaluator/2"
_INPUTS_V1 = "northstar.equity-recommendation-generation-inputs/1"
_ASSESSMENT_V1 = "northstar.equity-recommendation-assessment/1"
_PROPOSAL_V1 = "northstar.equity-recommendation-proposal/1"
_ATTEMPT_V1 = "northstar.equity-recommendation-generation-attempt/1"
_PREFIX = {
    _ASSESSMENT_V1: "eira1-sha256:",
    _PROPOSAL_V1: "eirp1-sha256:",
    _ATTEMPT_V1: "eirat1-sha256:",
}

_KEY = "eirk1-sha256:7c31bc99f52c080d0f1678d265bedaa2b5be41b3eac3056f335e35305cf4c9f6"
_EIRA1 = "eira1-sha256:a6f076008a3de16c1791d2777805e8e36318fee9f9d3185d42b7bd4fe7f95321"
_EIRP1 = "eirp1-sha256:d8bbc301d3505b9b4a6272452b0d372bee23c3d8e2b6e8533eac8f0c5d3d9aa6"
_EIRI1 = "eiri1-sha256:0714aa9217ca7fdafe304067ead994b39286def6cf5bfd736d3ad1756faf804c"
_EIRS1 = "eirs1-sha256:942dce39f255d8068a8dd758c71c69e647988465838bccc08bb687d62f0d6771"
_KEYED = "eirat1-sha256:15fb34389d05af4ae7cc340cada2c786a72e1f0adc0bd4d114dec3bfb8bec05c"
_KEYLESS = "eirat1-sha256:a460694b437eb00b07d07bf4bcfd03654512d90213dd7f1620fdab3612aebd31"
_ENTRY = "eirt1-sha256:98b13f3e6e392e20bd04b726417d52179df27772239f3e3856b1e065cd1b2618"
_DECISION = "eir1-sha256:7d2b6a702326b2b7f2e4eb734941ed6b81ecad92c937b1def40d6370129d566e"
_SNAPSHOT = "ecs1-sha256:8a7ffc0e682dc5d94deee620633a8ad1e9e4261c14a859b3cfefbb618a735859"
_MANIFEST = "ecm1-sha256:a8cfb438f4aae2c294125c9656788ebce6115c1b0764add54cd5950e127b582f"
_T = "2026-08-12T04:06:00Z"  # the modeled issuance: 09:36 IST, zero latency

# golden document -> (format, identity)
_DOCUMENTS = {
    "proposed.assessment": (_ASSESSMENT_V1, _EIRA1),
    "proposed.proposal": (_PROPOSAL_V1, _EIRP1),
    "proposed.attempt": (_ATTEMPT_V1, _KEYED),
    "no-candidate.attempt": (_ATTEMPT_V1, _KEYLESS),
}

# The application slice-1 keyless attempt golden (Application tests, _GOLDEN).
_SLICE1_DIR = (
    Path(northstar_application.__file__).resolve().parents[2]
    / "tests"
    / "application_services"
    / "golden"
    / "equity_recommendation_v1"
)
_SLICE1_KEYLESS = "eirat1-sha256:7ac11181e0aa2e25dcac43ae90bb5be9da4ad8675dc5d6a4917200d51534a1ce"


@pytest.fixture(autouse=True)
def _refuse_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("GV-07 tests must not open network connections.")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)


# ----------------------------------------------------------------------
# Test-local canonical form and the pinned rows
# ----------------------------------------------------------------------


def _canonical(document: dict[str, Any]) -> str:
    """The EI4-D9-C encoding, restated: sorted keys, no whitespace, ASCII, no NaN."""
    return json.dumps(
        document, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
    )


def _identity(document: dict[str, Any]) -> str:
    return _PREFIX[document["format"]] + hashlib.sha256(_canonical(document).encode()).hexdigest()


def _load(directory: Path, name: str) -> dict[str, Any]:
    with (directory / f"{name}.json").open(encoding="utf-8") as handle:
        return json.load(handle)  # type: ignore[no-any-return]


def _golden(name: str) -> dict[str, Any]:
    return _load(_GOLDEN_DIR, name)


def _text(name: str) -> str:
    return _canonical(_golden(name))


def _reference() -> dict[str, dict[str, Any]]:
    """The three pinned rows, every column, as stored."""
    rows = _golden("rows")
    disposition = dict(
        rows["disposition"],
        assessment_document=_text("proposed.assessment"),
        proposal_document=_text("proposed.proposal"),
    )
    keyed = dict(rows["keyed_attempt"], attempt_document=_text("proposed.attempt"))
    keyless = dict(rows["keyless_attempt"], attempt_document=_text("no-candidate.attempt"))
    return {"disposition": disposition, "keyed_attempt": keyed, "keyless_attempt": keyless}


def _rows(database: Path, table: str, key: str) -> list[dict[str, Any]]:
    with closing(raw(database)) as connection:
        connection.row_factory = sqlite3.Row
        return [
            dict(row)
            for row in connection.execute(f"SELECT * FROM {table} ORDER BY {key}")  # noqa: S608
        ]


def _stored(database: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    return (
        _rows(database, DISPOSITIONS, "generation_key"),
        _rows(database, ATTEMPTS, "attempt_identity"),
    )


def _expected() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    reference = _reference()
    attempts = sorted(
        (reference["keyed_attempt"], reference["keyless_attempt"]),
        key=lambda row: row["attempt_identity"],
    )
    return [reference["disposition"]], attempts


def _with_schema(world: World) -> None:
    """Have an adapter connection add the generation schema to the evidence database."""
    assert world.store().load_disposition("eirk1-sha256:" + "0" * 64) is None


def _plant(database: Path, *rows: tuple[str, dict[str, Any]]) -> None:
    """TEST-ONLY HISTORICAL-REFERENCE SETUP: insert pinned rows by raw SQL.

    The rows stand for records written in the past. Columns are named from the
    pinned row itself, never from the adapter. Constraints, foreign keys and
    insert-only triggers apply; nothing goes through a write path.
    """
    with closing(raw(database)) as connection:
        connection.execute("BEGIN IMMEDIATE")
        try:
            for table, row in rows:
                columns = sorted(row)
                connection.execute(
                    f"INSERT INTO {table} ({', '.join(columns)}) "  # noqa: S608
                    f"VALUES ({', '.join('?' * len(columns))})",
                    tuple(row[c] for c in columns),
                )
        except BaseException:
            connection.execute("ROLLBACK")
            raise
        connection.execute("COMMIT")


def _plant_reference(world: World) -> None:
    reference = _reference()
    _with_schema(world)
    _plant(
        world.database,
        (DISPOSITIONS, reference["disposition"]),
        (ATTEMPTS, reference["keyed_attempt"]),
        (ATTEMPTS, reference["keyless_attempt"]),
    )


def _file_state(database: Path) -> tuple[str, tuple[str, ...]]:
    """The database file's bytes and any journal beside it: a read must change neither."""
    siblings = tuple(sorted(p.name for p in database.parent.iterdir() if p != database))
    return hashlib.sha256(database.read_bytes()).hexdigest(), siblings


@pytest.fixture
def world(tmp_path: Path) -> World:
    return World(tmp_path / "northstar.sqlite3")


@pytest.fixture
def planted(world: World) -> World:
    """A fresh evidence database holding only the planted version-1 reference rows."""
    _plant_reference(world)
    return world


# ======================================================================
# The reference itself
# ======================================================================


def test_the_reference_is_closed_canonical_and_self_consistent() -> None:
    assert sorted(p.name for p in _GOLDEN_DIR.iterdir()) == sorted(
        [f"{name}.json" for name in _DOCUMENTS] + ["rows.json"]
    )
    for name in [*_DOCUMENTS, "rows"]:
        raw_text = (_GOLDEN_DIR / f"{name}.json").read_text(encoding="utf-8")
        document = json.loads(raw_text)
        assert raw_text.replace("\r\n", "\n") == (
            json.dumps(document, sort_keys=True, indent=2, ensure_ascii=True) + "\n"
        ), name
    for name, (format_, identity) in _DOCUMENTS.items():
        document = _golden(name)
        assert document["format"] == format_ and _identity(document) == identity, name
    reference = _reference()
    disposition = reference["disposition"]
    assessment = _golden("proposed.assessment")
    # The pinned structured columns agree with the pinned documents.
    assert (
        disposition["generation_key"],
        disposition["assessment_fingerprint"],
        disposition["proposal_fingerprint"],
        disposition["inputs_fingerprint"],
        disposition["policy_set_fingerprint"],
    ) == (
        assessment["generation_key"],
        _EIRA1,
        assessment["proposal_fingerprint"],
        assessment["inputs_fingerprint"],
        assessment["inputs"]["policy_set_fingerprint"],
    )
    assert assessment["proposal"] == _golden("proposed.proposal")
    assert reference["keyed_attempt"]["attempt_identity"] == _KEYED
    assert reference["keyless_attempt"]["attempt_identity"] == _KEYLESS


def test_the_reference_is_version1_with_its_original_provenance() -> None:
    disposition = _reference()["disposition"]
    assert {k: disposition[k] for k in sorted(disposition) if not k.endswith("_document")} == {
        "assessment_fingerprint": _EIRA1,
        "assessment_format": _ASSESSMENT_V1,
        "candidate_entry_fingerprint": _ENTRY,
        "candidate_sequence": 1,
        "decision_as_of": _T,
        "disposition_kind": "PROPOSED",
        "evaluator_version": _EVALUATOR_V1,
        "evidence_snapshot_id": _SNAPSHOT,
        "exchange_code": "NSE",
        "generation_key": _KEY,
        "inputs_fingerprint": _EIRI1,
        "manifest_fingerprint": _MANIFEST,
        "policy_set_fingerprint": _EIRS1,
        "policy_set_version": "synthetic-ei4db-set/1",
        "proposal_fingerprint": _EIRP1,
        "proposal_format": _PROPOSAL_V1,
        "research_configuration_version": "synthetic-ei3c-sqlite/1",
        "research_decision_fingerprint": _DECISION,
        "strategy": "ei3c-sqlite-orb-vwap-volume",
        "symbol": "M&M",
        "trading_date": "2026-08-12",
    }
    inputs = _golden("proposed.assessment")["inputs"]
    assert inputs["format"] == _INPUTS_V1
    # Version 1: the issued history is read as of the modeled issuance t itself.
    assert inputs["modeled_issued_at"] == inputs["issued_history"]["as_of"] == _T
    assert "history_boundary" not in inputs["issued_history"]
    assert "evaluator" not in _text("proposed.assessment")  # the label is no document's
    keyless = _reference()["keyless_attempt"]
    assert (keyless["status"], keyless["generation_key"], keyless["disposition_key"]) == (
        "NO_CANDIDATE",
        None,
        None,
    )
    assert _golden("no-candidate.attempt")["reasons"] == ["NO_AUTHORITATIVE_CANDIDATE"]


def test_the_version1_contract_is_the_one_the_application_runs() -> None:
    assert EQUITY_RECOMMENDATION_EVALUATOR_VERSION == _EVALUATOR_V1
    # Exactly one of the two governed states (ADR-025): version 1 alone, before the
    # Application's version-2 upgrade, or the closed table of /1 and /2 after it.
    # Version 1 is supported in both; no other label is ever accepted.
    assert SUPPORTED_EQUITY_RECOMMENDATION_EVALUATOR_VERSIONS in (
        frozenset({_EVALUATOR_V1}),
        frozenset({_EVALUATOR_V1, _EVALUATOR_V2}),
    )
    assert UNSUPPORTED_EVALUATOR not in SUPPORTED_EQUITY_RECOMMENDATION_EVALUATOR_VERSIONS
    assert (
        EQUITY_RECOMMENDATION_INPUTS_FORMAT,
        EQUITY_RECOMMENDATION_ASSESSMENT_FORMAT,
        EQUITY_RECOMMENDATION_PROPOSAL_FORMAT,
        EQUITY_RECOMMENDATION_ATTEMPT_FORMAT,
    ) == (_INPUTS_V1, _ASSESSMENT_V1, _PROPOSAL_V1, _ATTEMPT_V1)
    for format_, prefix in _PREFIX.items():
        assert EQUITY_RECOMMENDATION_DOCUMENT_PREFIXES[format_] == prefix
    assert UNSUPPORTED_EVALUATOR not in {_EVALUATOR_V1, _EVALUATOR_V2}


# ======================================================================
# Path A -- the authorized write persists exactly the reference
# ======================================================================


def test_path_a_an_authorized_write_persists_exactly_the_frozen_reference(world: World) -> None:
    proposed = world.recorder().execute(world.proposed())
    assert (proposed.outcome, proposed.attempt_recorded) == (_O.COMMITTED, True)
    keyless = world.recorder().execute(world.attempt_only())
    assert keyless.outcome is _O.COMMITTED
    assert _stored(world.database) == _expected()  # every column, every byte of text
    assert world.history.asked[-1].value == _T  # asked at t, not t - 1 us


def test_path_a_retry_is_idempotent_and_a_conflict_never_replaces_the_reference(
    world: World,
) -> None:
    assert world.recorder().execute(world.proposed()).outcome is _O.COMMITTED
    assert world.recorder().execute(world.attempt_only()).outcome is _O.COMMITTED
    again = world.recorder().execute(world.proposed())
    assert again.outcome is _O.EQUIVALENT_COMMITTED and again.attempt_recorded is True
    assert world.recorder().execute(world.attempt_only()).outcome is _O.EQUIVALENT_COMMITTED
    assert _stored(world.database) == _expected()
    # The same key with other content (exclusivity unresolved): a version-1 content
    # conflict. The reference disposition is kept; only the new attempt is audited.
    conflict = world.recorder().execute(world.unresolved())
    assert conflict.outcome is _O.CONTENT_CONFLICT
    assert conflict.authoritative is not None
    assert conflict.authoritative.assessment_fingerprint == _EIRA1
    dispositions, attempts = _stored(world.database)
    assert dispositions == _expected()[0]
    assert len(attempts) == 3 and all(row in attempts for row in _expected()[1])


@pytest.mark.parametrize(
    "statement",
    [
        f"UPDATE {DISPOSITIONS} SET evaluator_version = '{UNSUPPORTED_EVALUATOR}'",
        f"UPDATE {DISPOSITIONS} SET assessment_document = assessment_document",
        f"DELETE FROM {DISPOSITIONS}",
        f"INSERT OR REPLACE INTO {DISPOSITIONS} SELECT * FROM {DISPOSITIONS}",
        f"UPDATE {ATTEMPTS} SET attempt_document = attempt_document",
        f"DELETE FROM {ATTEMPTS}",
        f"REPLACE INTO {ATTEMPTS} SELECT * FROM {ATTEMPTS}",
    ],
)
def test_path_a_the_database_refuses_to_change_the_reference(
    planted: World, statement: str
) -> None:
    with closing(raw(planted.database)) as connection:
        with pytest.raises(sqlite3.IntegrityError, match="insert-only"):
            connection.execute(statement)
    assert _stored(planted.database) == _expected()


def test_path_a_an_unauthorized_write_is_refused_before_sqlite(world: World) -> None:
    genuine = world.authorize(world.proposed())
    unissued = VerifiedEquityRecommendationGenerationWrite(  # public construction, not issued
        genuine.verified, genuine.policy, genuine.attempt
    )
    _with_schema(world)
    before = _file_state(world.database)
    for candidate in (unissued, genuine.disposition, None):
        with pytest.raises(UnverifiedEquityRecommendationGenerationWriteError):
            world.store().write_generation(candidate)  # type: ignore[arg-type]
    assert _file_state(world.database) == before
    assert _stored(world.database) == ([], [])


# ======================================================================
# Path B -- the frozen rows reload unchanged after reopening, and replay
# ======================================================================


def test_path_b_the_reference_loads_unchanged_through_fresh_adapters(planted: World) -> None:
    before = _file_state(planted.database)
    first = planted.store().load_disposition(_KEY)
    second = planted.store().load_disposition(_KEY)  # another adapter, another connection
    assert first is not None and first == second
    assert (
        first.generation_key,
        first.kind,
        first.evaluator_version,
        first.assessment_fingerprint,
        first.proposal_fingerprint,
        first.inputs_fingerprint,
        first.policy_set_fingerprint,
    ) == (_KEY, _K.PROPOSED, _EVALUATOR_V1, _EIRA1, _EIRP1, _EIRI1, _EIRS1)
    # Exact stored bytes: never re-encoded, rehashed, relabelled or migrated.
    assert first.assessment_document == _text("proposed.assessment")
    assert first.proposal_document == _text("proposed.proposal")
    assert first.inputs.issued_history.as_of.value == _T
    for identity, name in ((_KEYED, "proposed.attempt"), (_KEYLESS, "no-candidate.attempt")):
        attempt = planted.store().load_attempt(identity)
        assert attempt is not None and attempt.attempt_identity == identity
        assert attempt.attempt_document == _text(name)
        assert planted.store().load_attempt(identity) == attempt
    keyless = planted.store().load_attempt(_KEYLESS)
    assert keyless is not None and keyless.generation_key is None
    assert keyless.disposition_kind is None and keyless.assessment_fingerprint is None
    # No hidden write during read: the file and its rows are byte-identical.
    assert _file_state(planted.database) == before
    assert _stored(planted.database) == _expected()


def test_path_b_each_key_reads_back_only_its_own_record(planted: World) -> None:
    # Beside the reference, another version-1 record under another key (another
    # published policy set): every read is by exact identity, never substituted.
    other = planted.recorder().execute(planted.not_eligible())
    assert other.outcome is _O.COMMITTED and other.disposition is not None
    other_key = other.disposition.generation_key
    assert other_key != _KEY
    reference = planted.store().load_disposition(_KEY)
    assert reference is not None and reference.assessment_fingerprint == _EIRA1
    loaded_other = planted.store().load_disposition(other_key)
    assert loaded_other == other.disposition and loaded_other.kind is _K.NOT_ELIGIBLE
    assert planted.store().load_disposition("eirk1-sha256:" + "f" * 64) is None
    assert planted.store().load_attempt("eirat1-sha256:" + "f" * 64) is None
    dispositions = _stored(planted.database)[0]
    assert len(dispositions) == 2
    assert [r for r in dispositions if r["generation_key"] == _KEY] == _expected()[0]


def test_path_b_levels_a_to_c_verify_the_reference(planted: World) -> None:
    before = _file_state(planted.database)
    verified = VerifyStoredEquityRecommendationGenerationUseCase(
        planted.store(), planted.verifier()
    ).execute(_KEY)
    assert verified is not None
    assert (
        verified.document_integrity_verified,
        verified.source_integrity_verified,
        verified.evaluation_reproduced,
        verified.issuance_established,
    ) == (True, True, True, False)
    assert verified.disposition.evaluator_version == _EVALUATOR_V1
    replayed = verified.replayed
    assert replayed.assessment_fingerprint == _EIRA1
    assert _canonical(replayed.assessment_document()) == _text("proposed.assessment")
    assert replayed.proposal is not None and replayed.proposal.fingerprint == _EIRP1
    source = verified.disposition.candidate.source
    assert (
        source.candidate_entry_fingerprint,
        source.research_decision_fingerprint,
        source.evidence_snapshot_id,
        verified.disposition.candidate.manifest_fingerprint,
    ) == (_ENTRY, _DECISION, _SNAPSHOT, _MANIFEST)
    assert _file_state(planted.database) == before  # verification wrote nothing


def test_path_b_todays_generation_is_equivalent_to_the_historical_record(
    planted: World,
) -> None:
    # Re-running generation over the same evidence reproduces the frozen record:
    # the recorder finds it equivalent and adopts nothing new.
    result = planted.recorder().execute(planted.proposed())
    assert result.outcome is _O.EQUIVALENT_COMMITTED and result.attempt_recorded is True
    assert result.authoritative is not None
    assert result.authoritative.assessment_document == _text("proposed.assessment")
    assert _stored(planted.database) == _expected()


def test_path_b_a_fresh_process_reloads_and_verifies_the_reference(planted: World) -> None:
    code = (
        "import json, sys\n"
        "from pathlib import Path\n"
        "from northstar_application.application_services import (\n"
        "    EquityRecommendationGenerationVerifier,\n"
        "    VerifyStoredEquityRecommendationGenerationUseCase,\n"
        ")\n"
        "from northstar_infrastructure.persistence import "
        "SQLiteEquityRecommendationGenerationStore\n"
        "from tests.equity_intraday_research_timeline_fixtures import Harness\n"
        "database = Path(sys.argv[1])\n"
        "store = SQLiteEquityRecommendationGenerationStore(database)\n"
        "harness = Harness(database)\n"
        "verifier = EquityRecommendationGenerationVerifier(harness.timelines, harness.snapshots)\n"
        "verified = VerifyStoredEquityRecommendationGenerationUseCase(store, verifier)"
        ".execute(sys.argv[2])\n"
        "print(json.dumps({\n"
        "    'evaluator': verified.disposition.evaluator_version,\n"
        "    'eira1': verified.disposition.assessment_fingerprint,\n"
        "    'replayed': verified.replayed.assessment_fingerprint,\n"
        "    'eirp1': verified.replayed.proposal.fingerprint,\n"
        "    'reproduced': verified.evaluation_reproduced,\n"
        "    'keyed': store.load_attempt(sys.argv[3]).attempt_identity,\n"
        "    'keyless': store.load_attempt(sys.argv[4]).attempt_identity,\n"
        "}, sort_keys=True))\n"
    )
    before = _file_state(planted.database)
    completed = subprocess.run(  # the test's own interpreter, running fixed code only
        [sys.executable, "-c", code, str(planted.database), _KEY, _KEYED, _KEYLESS],
        cwd=_INFRA_ROOT,
        env=dict(os.environ, PYTHONHASHSEED="4242"),
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    assert json.loads(completed.stdout.strip().splitlines()[-1]) == {
        "evaluator": _EVALUATOR_V1,
        "eira1": _EIRA1,
        "replayed": _EIRA1,
        "eirp1": _EIRP1,
        "reproduced": True,
        "keyed": _KEYED,
        "keyless": _KEYLESS,
    }
    assert _file_state(planted.database) == before


def test_slice1_the_application_keyless_golden_is_stored_and_reloaded_unchanged(
    world: World,
) -> None:
    # Application slice-1's keyless attempt rests on no evidence row, so it can be
    # stored here as is: the adapter returns its exact text and pinned identity.
    golden = _load(_SLICE1_DIR, "history-incomplete.attempt")
    text = _canonical(golden)
    assert _identity(golden) == _SLICE1_KEYLESS
    request = golden["request"]
    row = {
        "attempt_identity": _SLICE1_KEYLESS,
        "attempt_format": golden["format"],
        "attempt_document": text,
        "status": golden["status"],
        "strategy": request["strategy"],
        "symbol": request["listing"]["symbol"],
        "exchange_code": request["listing"]["exchange_code"],
        "trading_date": request["trading_date"],
        "policy_set_version": request["policy_set"]["version"],
        "generation_key": None,
        "disposition_kind": None,
        "assessment_fingerprint": None,
        "disposition_key": None,
    }
    _with_schema(world)
    _plant(world.database, (ATTEMPTS, row))
    loaded = world.store().load_attempt(_SLICE1_KEYLESS)
    assert loaded == EquityRecommendationGenerationAttempt(_SLICE1_KEYLESS, text)
    assert loaded is not None and loaded.attempt_document == text
    assert [r.value for r in loaded.reasons] == ["RECOMMENDATION_HISTORY_INCOMPLETE"]
    assert _rows(world.database, ATTEMPTS, "attempt_identity") == [row]


# ======================================================================
# Historical failure and tampering: which layer refuses
# ======================================================================


def _resealed_assessment(change: Callable[[dict[str, Any]], None]) -> dict[str, Any]:
    """TEST-ONLY: the reference disposition with its assessment altered, every fingerprint
    re-derived and every structured column kept consistent, so only a semantic check fails."""
    assessment = _golden("proposed.assessment")
    change(assessment)
    assessment["inputs_fingerprint"] = (
        "eiri1-sha256:" + hashlib.sha256(_canonical(assessment["inputs"]).encode()).hexdigest()
    )
    row = dict(_reference()["disposition"])
    row["assessment_document"] = _canonical(assessment)
    row["assessment_fingerprint"] = (
        "eira1-sha256:" + hashlib.sha256(row["assessment_document"].encode()).hexdigest()
    )
    row["inputs_fingerprint"] = assessment["inputs_fingerprint"]
    row["assessment_format"] = assessment["format"]
    return row


def _disposition(**changes: Any) -> dict[str, Any]:
    return dict(_reference()["disposition"], **changes)


def _set_path(path: tuple[str, ...], value: Any) -> Callable[[dict[str, Any]], None]:
    def change(document: dict[str, Any]) -> None:
        for part in path[:-1]:
            document = document[part]
        document[path[-1]] = value

    return change


_ZEROS = "0" * 64
_SQLITE, _LOAD = "SQLite refuses the INSERT", "the Application refuses the row on read"

# name -> (build the (disposition, attempt) rows, the refusing layer, error type or SQLite pattern)
_TAMPER: dict[str, tuple[Callable[[], tuple[Any, Any]], str, Any]] = {
    "unknown evaluator label": (
        lambda: (_disposition(evaluator_version=UNSUPPORTED_EVALUATOR), None),
        _LOAD,
        IncompatibleEquityRecommendationRecordError,
    ),
    "empty evaluator label": (
        lambda: (_disposition(evaluator_version=""), None),
        _SQLITE,
        "CHECK",
    ),
    "canonical inputs changed and resealed (history at t - 1 us)": (
        lambda: (
            _resealed_assessment(
                _set_path(("inputs", "issued_history", "as_of"), "2026-08-12T04:05:59.999999Z")
            ),
            None,
        ),
        _LOAD,
        InvalidEquityRecommendationRecordError,
    ),
    "assessment document changed, fingerprint kept": (
        lambda: (
            _disposition(
                assessment_document=_text("proposed.assessment").replace(
                    '"reasons":[]', '"reasons":["CORE_FORMATION_REJECTED"]', 1
                )
            ),
            None,
        ),
        _LOAD,
        InvalidEquityRecommendationRecordError,
    ),
    "assessment status changed": (
        lambda: (
            _disposition(
                assessment_document=_text("proposed.assessment").replace(
                    '"status":"PROPOSED"', '"status":"NOT_ELIGIBLE"', 1
                )
            ),
            None,
        ),
        _SQLITE,
        "CHECK",
    ),
    "incorrect assessment fingerprint": (
        lambda: (_disposition(assessment_fingerprint="eira1-sha256:" + _ZEROS), None),
        _LOAD,
        InvalidEquityRecommendationRecordError,
    ),
    "fingerprint without its namespace": (
        lambda: (_disposition(assessment_fingerprint="eirp1-" + _EIRA1.split("-", 1)[1]), None),
        _SQLITE,
        "CHECK",
    ),
    "foreign proposal identity": (
        lambda: (_disposition(proposal_fingerprint="eirp1-sha256:" + _ZEROS), None),
        _SQLITE,
        "CHECK",
    ),
    "structured column disagrees with its document": (
        lambda: (_disposition(policy_set_version="synthetic-ei4db-set/strict"), None),
        _SQLITE,
        "CHECK",
    ),
    "candidate relinked to other provenance": (
        lambda: (
            _resealed_assessment(
                _set_path(
                    ("inputs", "candidate", "source", "research_decision_fingerprint"),
                    "eir1-sha256:" + _ZEROS,
                )
            )
            | {"research_decision_fingerprint": "eir1-sha256:" + _ZEROS},
            None,
        ),
        _SQLITE,
        "synthetic CANDIDATE",
    ),
    "unknown assessment format, resealed": (
        lambda: (
            _resealed_assessment(
                _set_path(("format",), "northstar.equity-recommendation-assessment/9")
            ),
            None,
        ),
        _LOAD,
        IncompatibleEquityRecommendationRecordError,
    ),
    "non-canonical encoding": (
        lambda: (
            _disposition(assessment_document=json.dumps(_golden("proposed.assessment"), indent=1)),
            None,
        ),
        _LOAD,
        InvalidEquityRecommendationRecordError,
    ),
    "attempt identity tampered": (
        lambda: (
            None,
            dict(_reference()["keyless_attempt"], attempt_identity="eirat1-sha256:" + _ZEROS),
        ),
        _LOAD,
        InvalidEquityRecommendationRecordError,
    ),
    "attempt status changed": (
        lambda: (
            None,
            dict(
                _reference()["keyless_attempt"],
                attempt_document=_text("no-candidate.attempt").replace(
                    '"status":"NO_CANDIDATE"', '"status":"NOT_ELIGIBLE"', 1
                ),
            ),
        ),
        _SQLITE,
        "CHECK",
    ),
}


@pytest.mark.parametrize("case", list(_TAMPER))
def test_a_tampered_historical_row_is_refused_by_the_recorded_layer(
    world: World, case: str
) -> None:
    build, layer, expected = _TAMPER[case]
    disposition, attempt = build()
    _with_schema(world)
    rows = [(DISPOSITIONS, disposition)] if disposition is not None else []
    rows += [(ATTEMPTS, attempt)] if attempt is not None else []
    if layer == _SQLITE:
        with pytest.raises(sqlite3.IntegrityError, match=expected):
            _plant(world.database, *rows)
        assert _stored(world.database) == ([], [])  # nothing reached the tables
        return
    _plant(world.database, *rows)
    before = _stored(world.database)
    with pytest.raises(expected) as raised:
        if disposition is not None:
            world.store().load_disposition(disposition["generation_key"])
        else:
            world.store().load_attempt(attempt["attempt_identity"])
    if expected is InvalidEquityRecommendationRecordError:
        assert not isinstance(raised.value, IncompatibleEquityRecommendationRecordError)
    assert _stored(world.database) == before  # never repaired, migrated or rewritten
