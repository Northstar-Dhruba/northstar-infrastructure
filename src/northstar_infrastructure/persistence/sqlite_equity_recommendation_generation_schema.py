"""Schema for immutable equity recommendation-generation records (EI-4D-B).

Two insert-only tables, under EI4-D9-A to EI4-D9-J (EI-4 Decision Register,
section 9, approved 2026-11-07) and ADR-021, in the SAME SQLite file as the
EI-2B evidence snapshots and EI-3C research timelines they reference. The
schema is additive: it creates only its own tables, index and triggers, and
refuses a database that does not already hold the EI-2B and EI-3C tables --
it never creates an empty copy of the evidence it references.

- ``equity_recommendation_generation_dispositions`` holds at most one
  disposition per ``eirk1`` generation key (its PRIMARY KEY): its kind, the
  evaluator version, the exact canonical assessment document with its
  ``eira1`` fingerprint and, for PROPOSED only, the exact canonical proposal
  document with its ``eirp1`` fingerprint, in the same row -- so no proposal
  exists without its disposition, and no PROPOSED disposition without its
  proposal. Structured columns repeat the assessment's provenance: the
  ``eiri1`` inputs and ``eirs1`` policy-set fingerprints, the policy-set
  version, the strategy, research configuration, listing and date, and the
  original candidate -- its research decision, timeline entry and sequence,
  decision instant, EI-2B snapshot and manifest.
- ``equity_recommendation_generation_attempts`` holds one row per ``eirat1``
  logical-attempt identity (its PRIMARY KEY): the exact canonical attempt
  document, its status, request scope and policy-set version and, when
  established, its generation key, disposition kind and assessment
  fingerprint. ``disposition_key`` is the generation key exactly when the
  attempt has a disposition kind, and NULL otherwise: attempt-only results
  never need a disposition, and an attempt with a disposition kind can never
  be stored without one under its key.

Constraints
-----------
- Real foreign keys: a disposition to its EI-3C entry, by the entry's unique
  ``eirt1`` fingerprint, and to its EI-2B snapshot; an attempt's
  ``disposition_key`` to its disposition.
- CHECK constraints tie the kind to the proposal columns and to the
  assessment's status, and every structured column to the document it is
  read from, so a column can never disagree with its document.
- Insert triggers require the referenced entry to be a CANDIDATE -- which the
  EI-3C schema already makes the only CANDIDATE of its timeline -- with the
  disposition's sequence, decision, decision instant, snapshot, manifest and
  research window close; its timeline to have the disposition's strategy,
  configuration version, configuration document, listing and date; and its
  snapshot to be SYNTHETIC evidence. An attempt under a stored disposition
  must share its scope and policy-set version, and the same assessment
  fingerprint means the same kind.
- Triggers refuse every UPDATE and DELETE, and an INSERT that would replace a
  row (``INSERT OR REPLACE`` deletes without firing delete triggers unless
  recursive triggers are on, so a replacing insert is refused before it can).
  A disposition therefore never changes kind, document, evaluator version or
  source, and an UNRESOLVED disposition never becomes PROPOSED.

Not expressible here, and the Application's (EI4-D9-I): the SHA-256
fingerprints, which the Application re-derives whenever it constructs a
record; whether the CANDIDATE's timeline rehydrates under EI3-D13; and
whether the assessment reproduces. A foreign key proves that a row exists,
never that it is authoritative. ``evaluator_version`` is stored and compared
but covered by no fingerprint.
"""

from __future__ import annotations

import sqlite3

DISPOSITIONS = "equity_recommendation_generation_dispositions"
ATTEMPTS = "equity_recommendation_generation_attempts"
_ENTRIES = "equity_intraday_research_timeline_entries"
_TIMELINES = "equity_intraday_research_timelines"
_SNAPSHOTS = "equity_candle_evidence_snapshots"
REQUIRED_EVIDENCE_TABLES = (_SNAPSHOTS, _TIMELINES, _ENTRIES)

_KINDS = "('PROPOSED', 'NOT_ELIGIBLE', 'UNRESOLVED')"
_STATUSES = (
    "('PROPOSED', 'NO_CANDIDATE', 'NOT_ELIGIBLE', 'EVALUATION_UNRESOLVED', "
    "'DEPENDENCY_UNAVAILABLE', 'RESEARCH_INTEGRITY_FAILURE')"
)


def _status_of(kind: str) -> str:
    """Return SQL for the assessment status a disposition kind requires."""
    return f"(CASE {kind} WHEN 'UNRESOLVED' THEN 'EVALUATION_UNRESOLVED' ELSE {kind} END)"


def _assessment(path: str) -> str:
    return f"json_extract(assessment_document, '$.{path}')"


def _attempt(path: str) -> str:
    return f"json_extract(attempt_document, '$.{path}')"


EQUITY_RECOMMENDATION_GENERATION_DISPOSITION_SCHEMA = f"""
CREATE TABLE IF NOT EXISTS {DISPOSITIONS} (
    generation_key TEXT NOT NULL PRIMARY KEY CHECK (generation_key GLOB 'eirk1-sha256:?*'),
    disposition_kind TEXT NOT NULL CHECK (disposition_kind IN {_KINDS}),
    evaluator_version TEXT NOT NULL
        CHECK (typeof(evaluator_version) = 'text' AND length(evaluator_version) > 0),
    assessment_format TEXT NOT NULL,
    assessment_fingerprint TEXT NOT NULL CHECK (assessment_fingerprint GLOB 'eira1-sha256:?*'),
    assessment_document TEXT NOT NULL CHECK (json_valid(assessment_document)),
    proposal_format TEXT,
    proposal_fingerprint TEXT UNIQUE
        CHECK (proposal_fingerprint IS NULL OR proposal_fingerprint GLOB 'eirp1-sha256:?*'),
    proposal_document TEXT CHECK (proposal_document IS NULL OR json_valid(proposal_document)),
    inputs_fingerprint TEXT NOT NULL CHECK (inputs_fingerprint GLOB 'eiri1-sha256:?*'),
    policy_set_version TEXT NOT NULL CHECK (length(policy_set_version) > 0),
    policy_set_fingerprint TEXT NOT NULL CHECK (policy_set_fingerprint GLOB 'eirs1-sha256:?*'),
    strategy TEXT NOT NULL CHECK (length(strategy) > 0),
    research_configuration_version TEXT NOT NULL,
    symbol TEXT NOT NULL CHECK (length(symbol) > 0),
    exchange_code TEXT NOT NULL CHECK (length(exchange_code) > 0),
    trading_date TEXT NOT NULL
        CHECK (trading_date GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'),
    decision_as_of TEXT NOT NULL,
    research_decision_fingerprint TEXT NOT NULL
        CHECK (research_decision_fingerprint GLOB 'eir1-sha256:?*'),
    candidate_entry_fingerprint TEXT NOT NULL
        CHECK (candidate_entry_fingerprint GLOB 'eirt1-sha256:?*'),
    candidate_sequence INTEGER NOT NULL
        CHECK (typeof(candidate_sequence) = 'integer' AND candidate_sequence >= 1),
    evidence_snapshot_id TEXT NOT NULL CHECK (evidence_snapshot_id GLOB 'ecs1-sha256:?*'),
    manifest_fingerprint TEXT NOT NULL CHECK (manifest_fingerprint GLOB 'ecm1-sha256:?*'),
    FOREIGN KEY (candidate_entry_fingerprint) REFERENCES {_ENTRIES} (entry_fingerprint),
    FOREIGN KEY (evidence_snapshot_id) REFERENCES {_SNAPSHOTS} (snapshot_id),
    CHECK ((disposition_kind = 'PROPOSED') = (proposal_document IS NOT NULL)),
    CHECK ((disposition_kind = 'PROPOSED') = (proposal_fingerprint IS NOT NULL)),
    CHECK ((disposition_kind = 'PROPOSED') = (proposal_format IS NOT NULL)),
    CHECK (
        {_assessment("format")} IS assessment_format
        AND {_assessment("generation_key")} IS generation_key
        AND {_assessment("status")} IS {_status_of("disposition_kind")}
        AND {_assessment("proposal_fingerprint")} IS proposal_fingerprint
        AND json({_assessment("proposal")}) IS json(proposal_document)
        AND {_assessment("inputs_fingerprint")} IS inputs_fingerprint
        AND {_assessment("inputs.policy_set_fingerprint")} IS policy_set_fingerprint
        AND {_assessment("inputs.policy_set.reference.version")} IS policy_set_version
        AND {_assessment("request.policy_set.version")} IS policy_set_version
        AND {_assessment("request.strategy")} IS strategy
        AND {_assessment("request.listing.symbol")} IS symbol
        AND {_assessment("request.listing.exchange_code")} IS exchange_code
        AND {_assessment("request.trading_date")} IS trading_date
        AND {_assessment("inputs.candidate.source.strategy")} IS strategy
        AND {_assessment("inputs.candidate.source.research_configuration_version")}
            IS research_configuration_version
        AND {_assessment("inputs.candidate.source.listing.symbol")} IS symbol
        AND {_assessment("inputs.candidate.source.listing.exchange_code")} IS exchange_code
        AND {_assessment("inputs.candidate.source.trading_date")} IS trading_date
        AND {_assessment("inputs.candidate.source.decision_as_of")} IS decision_as_of
        AND {_assessment("inputs.candidate.source.research_decision_fingerprint")}
            IS research_decision_fingerprint
        AND {_assessment("inputs.candidate.source.candidate_entry_fingerprint")}
            IS candidate_entry_fingerprint
        AND {_assessment("inputs.candidate.source.evidence_snapshot_id")}
            IS evidence_snapshot_id
        AND {_assessment("inputs.candidate.candidate_sequence")} IS candidate_sequence
        AND {_assessment("inputs.candidate.manifest_fingerprint")} IS manifest_fingerprint
    ),
    CHECK (
        proposal_document IS NULL OR (
            json_extract(proposal_document, '$.format') IS proposal_format
            AND json_extract(proposal_document, '$.generation_key') IS generation_key
        )
    )
)
"""

EQUITY_RECOMMENDATION_GENERATION_ATTEMPT_SCHEMA = f"""
CREATE TABLE IF NOT EXISTS {ATTEMPTS} (
    attempt_identity TEXT NOT NULL PRIMARY KEY CHECK (attempt_identity GLOB 'eirat1-sha256:?*'),
    attempt_format TEXT NOT NULL,
    attempt_document TEXT NOT NULL CHECK (json_valid(attempt_document)),
    status TEXT NOT NULL CHECK (status IN {_STATUSES}),
    strategy TEXT NOT NULL CHECK (length(strategy) > 0),
    symbol TEXT NOT NULL CHECK (length(symbol) > 0),
    exchange_code TEXT NOT NULL CHECK (length(exchange_code) > 0),
    trading_date TEXT NOT NULL
        CHECK (trading_date GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'),
    policy_set_version TEXT NOT NULL CHECK (length(policy_set_version) > 0),
    generation_key TEXT CHECK (generation_key IS NULL OR generation_key GLOB 'eirk1-sha256:?*'),
    disposition_kind TEXT CHECK (disposition_kind IS NULL OR disposition_kind IN {_KINDS}),
    assessment_fingerprint TEXT
        CHECK (assessment_fingerprint IS NULL OR assessment_fingerprint GLOB 'eira1-sha256:?*'),
    disposition_key TEXT,
    FOREIGN KEY (disposition_key) REFERENCES {DISPOSITIONS} (generation_key),
    CHECK ((generation_key IS NULL) = (assessment_fingerprint IS NULL)),
    CHECK (disposition_kind IS NULL OR generation_key IS NOT NULL),
    CHECK (disposition_kind IS NULL OR status IS {_status_of("disposition_kind")}),
    CHECK (
        disposition_key IS (CASE WHEN disposition_kind IS NULL THEN NULL ELSE generation_key END)
    ),
    CHECK (
        {_attempt("format")} IS attempt_format
        AND {_attempt("status")} IS status
        AND {_attempt("request.strategy")} IS strategy
        AND {_attempt("request.listing.symbol")} IS symbol
        AND {_attempt("request.listing.exchange_code")} IS exchange_code
        AND {_attempt("request.trading_date")} IS trading_date
        AND {_attempt("request.policy_set.version")} IS policy_set_version
        AND {_attempt("generation_key")} IS generation_key
        AND {_attempt("disposition_kind")} IS disposition_kind
        AND {_attempt("assessment_fingerprint")} IS assessment_fingerprint
    )
)
"""

EQUITY_RECOMMENDATION_GENERATION_ATTEMPT_KEY_INDEX = f"""
CREATE INDEX IF NOT EXISTS {ATTEMPTS}_by_generation_key
ON {ATTEMPTS} (generation_key)
WHERE generation_key IS NOT NULL
"""


def _refuse(name: str, event: str, table: str, message: str, when: str = "") -> str:
    condition = f"\n{when}" if when else ""
    return (
        f"CREATE TRIGGER IF NOT EXISTS {name}\n{event} ON {table}{condition}\n"
        f"BEGIN SELECT RAISE(ABORT, '{message}'); END"
    )


EQUITY_RECOMMENDATION_GENERATION_TRIGGERS = (
    *(
        _refuse(f"{table}_no_{verb.lower()}", f"BEFORE {verb}", table, f"{table} is insert-only")
        for table in (DISPOSITIONS, ATTEMPTS)
        for verb in ("UPDATE", "DELETE")
    ),
    _refuse(
        f"{DISPOSITIONS}_no_replace",
        "BEFORE INSERT",
        DISPOSITIONS,
        f"{DISPOSITIONS} is insert-only",
        f"WHEN EXISTS (SELECT 1 FROM {DISPOSITIONS} AS d "
        "WHERE d.generation_key = NEW.generation_key "
        "OR d.proposal_fingerprint = NEW.proposal_fingerprint)",
    ),
    _refuse(
        f"{ATTEMPTS}_no_replace",
        "BEFORE INSERT",
        ATTEMPTS,
        f"{ATTEMPTS} is insert-only",
        f"WHEN EXISTS (SELECT 1 FROM {ATTEMPTS} AS a "
        "WHERE a.attempt_identity = NEW.attempt_identity)",
    ),
    _refuse(
        f"{DISPOSITIONS}_source_matches",
        "BEFORE INSERT",
        DISPOSITIONS,
        "a disposition must rest on the synthetic CANDIDATE entry and snapshot it names",
        f"WHEN NOT EXISTS (SELECT 1 FROM {_ENTRIES} AS e "
        f"JOIN {_TIMELINES} AS t ON t.timeline_id = e.timeline_id "
        f"JOIN {_SNAPSHOTS} AS s ON s.snapshot_id = e.snapshot_id "
        "WHERE e.entry_fingerprint = NEW.candidate_entry_fingerprint "
        "AND e.role = 'CANDIDATE' "
        "AND e.sequence = NEW.candidate_sequence "
        "AND e.decision_fingerprint = NEW.research_decision_fingerprint "
        "AND e.decision_instant = NEW.decision_as_of "
        "AND e.snapshot_id = NEW.evidence_snapshot_id "
        "AND e.manifest_fingerprint = NEW.manifest_fingerprint "
        "AND e.research_window_closes_at IS json_extract(NEW.assessment_document, "
        "'$.inputs.candidate.research_window.closes_at') "
        "AND t.strategy = NEW.strategy "
        "AND t.configuration_version = NEW.research_configuration_version "
        "AND json(t.configuration_document) IS json(json_extract(NEW.assessment_document, "
        "'$.request.research_configuration')) "
        "AND t.symbol = NEW.symbol AND t.exchange_code = NEW.exchange_code "
        "AND t.trading_date = NEW.trading_date "
        "AND s.evidence_class = 'SYNTHETIC')",
    ),
    _refuse(
        f"{ATTEMPTS}_matches_disposition",
        "BEFORE INSERT",
        ATTEMPTS,
        "an attempt must share the scope and policy set of the disposition under its key",
        f"WHEN NEW.disposition_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM {DISPOSITIONS} "
        "AS d WHERE d.generation_key = NEW.disposition_key "
        "AND d.strategy = NEW.strategy AND d.symbol = NEW.symbol "
        "AND d.exchange_code = NEW.exchange_code AND d.trading_date = NEW.trading_date "
        "AND d.policy_set_version = NEW.policy_set_version "
        "AND (d.assessment_fingerprint <> NEW.assessment_fingerprint "
        "OR d.disposition_kind = NEW.disposition_kind))",
    ),
)


def _require_evidence_tables(connection: sqlite3.Connection) -> None:
    present = {
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name IN (?, ?, ?)",
            REQUIRED_EVIDENCE_TABLES,
        )
    }
    missing = [name for name in REQUIRED_EVIDENCE_TABLES if name not in present]
    if missing:
        raise sqlite3.OperationalError(
            "This database holds no EI-2B and EI-3C evidence ("
            + ", ".join(missing)
            + " absent); generation records are kept only beside the evidence they "
            "reference, and no evidence table is created here."
        )


def initialize_equity_recommendation_generation_schema(connection: sqlite3.Connection) -> None:
    """Create the generation tables, index and triggers when they are absent.

    The EI-2B snapshot and EI-3C timeline tables must already exist in the
    same database; otherwise sqlite3.OperationalError is raised and nothing is
    created. Existing rows, tables, indexes and triggers are never altered.
    """
    _require_evidence_tables(connection)
    for statement in (
        EQUITY_RECOMMENDATION_GENERATION_DISPOSITION_SCHEMA,
        EQUITY_RECOMMENDATION_GENERATION_ATTEMPT_SCHEMA,
        EQUITY_RECOMMENDATION_GENERATION_ATTEMPT_KEY_INDEX,
        *EQUITY_RECOMMENDATION_GENERATION_TRIGGERS,
    ):
        connection.execute(statement)
    connection.commit()
