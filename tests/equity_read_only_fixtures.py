"""Synthetic fixtures for the EI-7D read-only SQLite readers, on real SQLite.

Every persisted record a read test inspects is written by the accepted chain,
through merged use cases and the accepted write-capable adapters only, into a
disposable file (tests/equity_recommendation_lifecycle_fixtures.py):

    EI-4E research (TCS breakout, EI-2B snapshots 09:36/09:41/09:46, EI-3C
    timeline, published synthetic policies) -> declaration through b
    (sequence 1) -> EI-4C v2 generation, EI-4D recording -> the real issuer
    (TCS, sequence 2) -> EI-6B lifecycle events by the real recorder.

Additional issuances of the SAME session come from additional synthetic
strategies (``ei7d-orb-NN``): each gets its own EI-3C timeline over the same
TCS snapshots, a version-2 PROPOSED disposition and a real issuance. All are
issued at the same modeled instant, so the EI-6A declared order
``(issued_at, issuance_id)`` differs from the recording-sequence order.

``plant_production_snapshot`` issues a genuine PRODUCTION-class snapshot
through the real EI-2B store: the INFY window of a world without INFY candles,
re-validated by the real constructor with ACCEPTED_PRODUCTION provenance and a
finality policy that refuses synthetic evidence. It is SYNTHETIC TEST DATA
dressed as production evidence to exercise the EI7-EC1 refusal; nothing real
is read.

Raw SQL appears only in ``corrupt`` (TEST-ONLY corruption of a disposable
copy) and in inspection helpers. Nothing here opens a network connection,
reads a clock or touches a production database. Once a fixture is built it is
treated as sealed: read tests compare its bytes before and after.
"""

from __future__ import annotations

import hashlib
import shutil
import sqlite3
from contextlib import closing
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from northstar_application.application_services import (
    NO_APPROVED_EQUITY_CANDLE_FINALITY,
    AdvanceEquityIntradayResearchTimelineUseCase,
    EquityCandleEvidenceClass,
    EquityCandleEvidenceSnapshot,
    EquityOpeningRangeBreakoutConfiguration,
    EquityRecordedIssuance,
)
from northstar_application.ports import EquityReferenceProvenanceKind
from northstar_core.strategy import StrategyIdentity

from northstar_infrastructure.persistence import (
    SQLiteEquityCandleEvidenceSnapshotStore,
    SQLiteReadOnlyEquityCandleEvidenceSnapshotReader,
    SQLiteReadOnlyEquityIntradayResearchTimelineReader,
    SQLiteReadOnlyEquityRecommendationGenerationReader,
    SQLiteReadOnlyEquityRecommendationIssuanceReader,
    SQLiteReadOnlyEquityRecommendationLifecycleReader,
)
from tests.equity_lifecycle_monitoring_fixtures import copy_world
from tests.equity_recommendation_acceptance_fixtures import (
    CONFIG,
    DAY,
    INFY,
    SNAPSHOTS,
    TCS,
    bypass,
    ist,
    raw,
)
from tests.equity_recommendation_issuance_fixtures import establish_infy_breakout
from tests.equity_recommendation_lifecycle_fixtures import LifecycleWorld, lifecycle_world

LATER_DECLARATION = ist(9, 44)  # covers the issuances at t = 09:43:30


def base_world(database: Path) -> LifecycleWorld:
    """Declared through b (sequence 1); TCS issued (sequence 2); the lifecycle schema in place.

    The EI-6B store adds its own tables on its first ordinary read, exactly as
    in production; afterwards the file holds every equity table.
    """
    world = lifecycle_world(database)
    assert world.store().load_lifecycle(world.issuance_id) is not None
    return world


@dataclass(frozen=True)
class SessionWorld:
    """One NSE session with three issuances, two declarations and lifecycles (see below).

    - sequence 1: declared complete through b (before every issuance);
    - sequences 2, 3, 4: TCS issued at t = 09:43:30 IST under CONFIG's strategy
      (``terminal``: opened, activated, target reached -- CLOSED_TARGET),
      ``ei7d-orb-01`` (``active``: opened and activated -- ACTIVE) and
      ``ei7d-orb-02`` (``quiet``: no lifecycle event);
    - sequence 5: declared complete through 09:44 IST, covering all three;
    - INFY: a committed EI-3C timeline with a CANDIDATE, and no issuance.
    """

    world: LifecycleWorld
    issuances: tuple[EquityRecordedIssuance, ...]

    @property
    def database(self) -> Path:
        return self.world.database

    @property
    def terminal(self) -> EquityRecordedIssuance:
        return self.issuances[0]

    @property
    def active(self) -> EquityRecordedIssuance:
        return self.issuances[1]

    @property
    def quiet(self) -> EquityRecordedIssuance:
        return self.issuances[2]


def session_world(database: Path) -> SessionWorld:
    """Build the SessionWorld above through the accepted chain only."""
    world = base_world(database)
    first = EquityRecordedIssuance(world.issuance, 2)
    others = issue_all(world, propose_strategies(world, range(1, 3)))
    assert [r.recording_sequence for r in others] == [3, 4]
    assert world.world.declare(LATER_DECLARATION).recorded.recording_sequence == 5  # type: ignore[union-attr]
    opened, activated, targeted = world.opened(), world.activated(), world.targeted()
    assert world.record(opened, activated, targeted).state.value == "CLOSED_TARGET"
    active = others[0].recommendation.issuance_id
    assert world.record(activated, issuance_id=active).state.value == "ACTIVE"
    establish_infy_breakout(world.world)
    return SessionWorld(world, (first, *others))


def copy_session(template: SessionWorld, database: Path) -> SessionWorld:
    """A disposable copy of ``template``'s file (rollback journal: the file is the database)."""
    world = copy_world(template.world, database)
    return SessionWorld(world, template.issuances)


def configuration(k: int) -> EquityOpeningRangeBreakoutConfiguration:
    """The CONFIG research configuration under the synthetic strategy ``ei7d-orb-NN``."""
    return replace(CONFIG, strategy=StrategyIdentity(f"ei7d-orb-{k:02d}"))


def research_snapshot_ids(database: Path) -> list[str]:
    """The TCS research snapshots (09:36, 09:41, 09:46 IST), in that order."""
    with closing(raw(database)) as connection:
        found = dict(
            connection.execute(
                f"SELECT as_of, snapshot_id FROM {SNAPSHOTS} WHERE symbol = 'TCS'"  # noqa: S608
            ).fetchall()
        )
    return [found[ist(9, minute).value] for minute in (36, 41, 46)]


def propose_strategies(world: LifecycleWorld, numbers: range) -> list[str]:
    """For each strategy number: commit its TCS timeline, generate and record a v2 proposal.

    Nothing is issued. Returns the generation keys in ``numbers`` order.
    """
    snapshot_ids = research_snapshot_ids(world.database)
    keys = []
    for k in numbers:
        evidence = world.evidence()
        advanced = AdvanceEquityIntradayResearchTimelineUseCase(
            evidence.timelines, evidence.snapshots, configuration(k)
        ).execute(TCS, DAY, snapshot_ids)
        assert advanced.status.value == "COMMITTED", advanced.status
        keys.append(world.world.proposed(configuration=configuration(k)))
    return keys


def issue_all(world: LifecycleWorld, keys: list[str]) -> list[EquityRecordedIssuance]:
    """Issue each generation key through the real issuer, in order; every one is ISSUED."""
    recorded = []
    for key in keys:
        result = world.world.issue(key)
        assert result.outcome.value == "ISSUED", (result.outcome, result.detail)
        recorded.append(EquityRecordedIssuance(result.issuance, result.recording_sequence))
    return recorded


def plant_production_snapshot(world: LifecycleWorld) -> EquityCandleEvidenceSnapshot:
    """Issue a genuine, verifying PRODUCTION-class snapshot of the empty INFY window.

    The world must hold no INFY candle, so the journal at any watermark holds
    exactly the snapshot's evidence: none.
    """
    synthetic = world.evidence().issue(ist(9, 36), INFY)
    accepted = EquityReferenceProvenanceKind.ACCEPTED_PRODUCTION
    production = replace(
        synthetic,
        venue_provenance=accepted,
        applicability_provenance=accepted,
        finality_policy=NO_APPROVED_EQUITY_CANDLE_FINALITY,
    )
    assert production.evidence_class is EquityCandleEvidenceClass.PRODUCTION
    assert SQLiteEquityCandleEvidenceSnapshotStore(world.database).issue(production) == 1
    return production


def readers(database: Path) -> dict[str, Any]:
    """Every EI-7D read-only reader over ``database``, by short name."""
    return {
        "issuances": SQLiteReadOnlyEquityRecommendationIssuanceReader(database),
        "sessions": SQLiteReadOnlyEquityRecommendationIssuanceReader(database),
        "lifecycles": SQLiteReadOnlyEquityRecommendationLifecycleReader(database),
        "generations": SQLiteReadOnlyEquityRecommendationGenerationReader(database),
        "timelines": SQLiteReadOnlyEquityIntradayResearchTimelineReader(database),
        "snapshots": SQLiteReadOnlyEquityCandleEvidenceSnapshotReader(database),
    }


def session_answer(database: Path, watermark: int | None = None) -> list[Any]:
    """A plain summary of one discovery answer: N, W, declaration and issuance sequences."""
    from tests.equity_recommendation_acceptance_fixtures import NSE

    result = SQLiteReadOnlyEquityRecommendationIssuanceReader(database).recorded_session_issuances(
        NSE, DAY, limit=50, watermark=watermark
    )
    return [
        result.recording_position,
        result.recording_watermark,
        [r.recording_sequence for r in result.declarations],
        [r.recording_sequence for r in result.issuances],
    ]


def read_sessions_repeatedly(database: str, rounds: int, start: Any, results: Any) -> None:
    """Spawned reader: discover the session ``rounds`` times after ``start``; report all."""
    import os

    path = Path(database)
    start.wait(60)
    answers, errors = [], []
    for _ in range(rounds):
        try:
            answers.append(session_answer(path))
        except Exception as exc:  # noqa: BLE001 -- reported to the parent, never swallowed
            errors.append(repr(exc))
    results.put((os.getpid(), answers, errors))


def file_state(database: Path) -> tuple[str, int, dict[str, str]]:
    """The database bytes' SHA-256 and size, and every sibling file's SHA-256 by name."""
    data = database.read_bytes()
    siblings = {
        p.name: hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(database.parent.iterdir())
        if p.is_file() and p.name != database.name
    }
    return hashlib.sha256(data).hexdigest(), len(data), siblings


def schema_state(database: Path) -> tuple[list[tuple[Any, ...]], int, int]:
    """Every schema object's (type, name, sql), the schema version and the user version."""
    with closing(sqlite3.connect(f"{database.resolve().as_uri()}?mode=ro", uri=True)) as c:
        objects = c.execute("SELECT type, name, sql FROM sqlite_master ORDER BY type, name")
        return (
            objects.fetchall(),
            c.execute("PRAGMA schema_version").fetchone()[0],
            c.execute("PRAGMA user_version").fetchone()[0],
        )


_JOURNAL_MAGIC = bytes.fromhex("d9d505f920a163d7")  # a synced rollback-journal header


def crash_image(source: Path, target: Path) -> Path:
    """TEST-ONLY: copy ``source`` as a writer that stopped mid-transaction leaves it.

    A raw writer with a two-page cache deletes rows inside an open
    transaction, so SQLite syncs its rollback journal and spills changed pages
    into the database file. Both files are copied at that moment -- the copy
    is a crash image with a HOT journal -- and the writer then rolls back, so
    ``source`` keeps its committed state. Returns the copy's journal path.
    """
    writer = sqlite3.connect(source, isolation_level=None)
    journal = source.with_name(source.name + "-journal")
    hot = target.with_name(target.name + "-journal")
    try:
        writer.execute("PRAGMA cache_size = 2")
        writer.execute("BEGIN IMMEDIATE")
        for table in ("equity_candle_evidence_snapshot_entries", "equity_candle_journal"):
            writer.execute(f"DROP TRIGGER {table}_no_delete")
            writer.execute(f"DELETE FROM {table}")  # noqa: S608
        assert journal.read_bytes()[:8] == _JOURNAL_MAGIC, "the journal was not synced"
        shutil.copyfile(source, target)
        shutil.copyfile(journal, hot)
    finally:
        writer.execute("ROLLBACK")
        writer.close()
    return hot


def convert_to_wal(database: Path) -> None:
    """TEST-ONLY: switch a disposable copy to write-ahead logging; no sidecar is left behind."""
    with closing(sqlite3.connect(database)) as connection:
        assert connection.execute("PRAGMA journal_mode = WAL").fetchone() == ("wal",)
    assert sidecars(database) == []


def sidecars(database: Path) -> list[str]:
    """The names of SQLite's auxiliary files of ``database`` that exist now."""
    return sorted(p.name for p in database.parent.glob(database.name + "-*"))


def corrupt(
    database: Path,
    statement: str,
    parameters: tuple[object, ...] = (),
    *,
    drop: tuple[str, ...] = (),
    checks: bool = True,
    foreign_keys: bool = False,
) -> None:
    """TEST-ONLY corruption of a disposable copy: drop triggers, relax checks, write raw SQL.

    It is never evidence of a normal workflow; the accepted adapters' next
    read-write connection recreates any dropped trigger.
    """
    bypass(database, (statement, parameters), drop=drop, checks=checks, foreign_keys=foreign_keys)
