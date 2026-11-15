"""EI-6C: the read-only SQLite snapshot catalog, on real SQLite.

Acceptance of the EI-6C Lifecycle Monitoring and Snapshot Selection Design
v1.0 (sections 5.1, 5.2, 6.4 and 12) at layer S for the catalog adapter
itself: temporal eligibility on parsed instants (MS-02, MS-29, MS-35, MS-40),
scope (MS-21, MS-25), complete same-instant groups (MS-03), the governed limit
of 300 eligible candidates (MS-30), fail-closed corruption, SQLite failures,
one consistent read view, read-only enforcement and the architecture rows
(MS-24). The monitor composed over this catalog is
tests/test_equity_lifecycle_monitoring_sqlite_acceptance.py.

Every snapshot on a success path is issued by the real EI-2B use case into a
disposable file. Raw SQL appears only where a test says so: TEST-ONLY
corruption of a disposable copy, uncommitted or rolled-back writers, and
writes the read-only connection must refuse. Nothing here opens a network
connection or touches a production database.
"""

from __future__ import annotations

import ast
import hashlib
import inspect
import os
import socket
import sqlite3
import stat
import threading
from collections.abc import Iterator
from contextlib import closing, contextmanager
from itertools import pairwise
from pathlib import Path
from typing import Any

import pytest
from northstar_application.application_services import (
    EquityCandleEvidenceClass,
)
from northstar_application.ports import (
    EquityCandleEvidenceSnapshotCatalog,
    EquityCandleEvidenceSnapshotCatalogEntry,
    EquityCandleEvidenceSnapshotCatalogLimitExceededError,
    EquityCandleEvidenceSnapshotCatalogScope,
    EquityCandleEvidenceSnapshotCatalogUnavailableError,
    InvalidEquityCandleEvidenceSnapshotCatalogError,
)
from northstar_core.domain.value_objects import ListingReference
from northstar_core.foundation.value_objects import ExchangeCode, PointInTime, Symbol, Timeframe

from northstar_infrastructure.persistence import (
    SQLiteEquityCandleEvidenceSnapshotCatalog,
    SQLiteEquityCandleEvidenceSnapshotStore,
)
from northstar_infrastructure.persistence import (
    sqlite_equity_candle_evidence_snapshot_catalog as catalog_module,
)
from tests.equity_lifecycle_monitoring_fixtures import CATALOG_SCOPE, copy_world, instant, issue
from tests.equity_recommendation_acceptance_fixtures import (
    DAY,
    INFY,
    SNAPSHOTS,
    TCS,
    bypass,
    ist,
    raw,
)
from tests.equity_recommendation_issuance_fixtures import establish_infy_breakout
from tests.equity_recommendation_lifecycle_fixtures import (
    QUIET_BAR,
    LifecycleWorld,
    lifecycle_world,
)

LIMIT = 300  # EQUITY_LIFECYCLE_MONITORING_CATALOG_LIMIT, owner-approved (V3)
CROWD = 301  # extra snapshots at 09:30:00 + k seconds, k = 0..300 (before the research)
CROWD_START = ist(9, 30)
SCOPE = CATALOG_SCOPE
_Invalid = InvalidEquityCandleEvidenceSnapshotCatalogError
_Unavailable = EquityCandleEvidenceSnapshotCatalogUnavailableError
_Limit = EquityCandleEvidenceSnapshotCatalogLimitExceededError
_NO_UPDATE = f"{SNAPSHOTS}_no_update"
_FAKE_ID = "ecs1-sha256:" + "f" * 64


@pytest.fixture(autouse=True)
def _refuse_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("EI-6C tests must not open network connections.")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)


@pytest.fixture(scope="module")
def template(tmp_path_factory: pytest.TempPathFactory) -> LifecycleWorld:
    """One MODELED TCS issuance at 09:43:30 IST; research snapshots 09:36, 09:41, 09:46."""
    return lifecycle_world(tmp_path_factory.mktemp("ei6c-catalog") / "template.sqlite3")


@pytest.fixture
def world(template: LifecycleWorld, tmp_path: Path) -> LifecycleWorld:
    return copy_world(template, tmp_path / "ei6c.sqlite3")


@pytest.fixture(scope="module")
def crowded_template(
    template: LifecycleWorld, tmp_path_factory: pytest.TempPathFactory
) -> LifecycleWorld:
    """The template plus 301 real snapshots at 09:30:00 + k s (as_of = issued_at)."""
    crowded = copy_world(template, tmp_path_factory.mktemp("ei6c-crowd") / "crowd.sqlite3")
    for k in range(CROWD):
        issue(crowded, instant(CROWD_START, k))
    return crowded


@pytest.fixture
def crowded(crowded_template: LifecycleWorld, tmp_path: Path) -> LifecycleWorld:
    return copy_world(crowded_template, tmp_path / "crowd.sqlite3")


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------


def _catalog(world: LifecycleWorld) -> SQLiteEquityCandleEvidenceSnapshotCatalog:
    return SQLiteEquityCandleEvidenceSnapshotCatalog(world.database)


def _listed(
    world: LifecycleWorld,
    at: PointInTime,
    limit: int = LIMIT,
    scope: EquityCandleEvidenceSnapshotCatalogScope = SCOPE,
) -> tuple[EquityCandleEvidenceSnapshotCatalogEntry, ...]:
    return _catalog(world).list_snapshots(scope, at, limit)


def _ids(world: LifecycleWorld, at: PointInTime, **options: Any) -> list[str]:
    return [entry.snapshot_id for entry in _listed(world, at, **options)]


def _research(world: LifecycleWorld) -> dict[str, str]:
    """The research snapshots by IST minute: {'09:36': id, '09:41': id, '09:46': id}."""
    with closing(raw(world.database)) as connection:
        found = {
            as_of: snapshot_id
            for snapshot_id, as_of in connection.execute(
                f"SELECT snapshot_id, as_of FROM {SNAPSHOTS} WHERE symbol = 'TCS'"  # noqa: S608
            )
        }
    return {minute: found[ist(9, int(minute[3:])).value] for minute in ("09:36", "09:41", "09:46")}


def _file_state(database: Path) -> tuple[str, int, list[str]]:
    data = database.read_bytes()
    siblings = sorted(p.name for p in database.parent.iterdir() if p.name != database.name)
    return hashlib.sha256(data).hexdigest(), len(data), siblings


def _stored_row(world: LifecycleWorld, snapshot_id: str) -> dict[str, Any]:
    with closing(raw(world.database)) as connection:
        connection.row_factory = sqlite3.Row
        row = connection.execute(
            f"SELECT * FROM {SNAPSHOTS} WHERE snapshot_id = ?",  # noqa: S608
            (snapshot_id,),
        ).fetchone()
    return dict(row)


def _insert(world: LifecycleWorld, row: dict[str, Any]) -> None:
    """TEST-ONLY: insert a snapshot row by raw SQL into the disposable file."""
    columns = ", ".join(row)
    bypass(
        world.database,
        (
            f"INSERT INTO {SNAPSHOTS} ({columns}) VALUES ({', '.join('?' * len(row))})",  # noqa: S608
            tuple(row.values()),
        ),
    )


def _set(world: LifecycleWorld, target: str, checks: bool = True, **columns: object) -> None:
    """TEST-ONLY corruption: rewrite columns of one stored snapshot, triggers dropped."""
    assignments = ", ".join(f"{name} = ?" for name in columns)
    bypass(
        world.database,
        (
            f"UPDATE {SNAPSHOTS} SET {assignments} WHERE snapshot_id = ?",  # noqa: S608
            (*columns.values(), target),
        ),
        drop=(_NO_UPDATE,),
        checks=checks,
        foreign_keys=False,  # a rewritten identity would orphan its entry rows
    )


def _invalid(world: LifecycleWorld, at: PointInTime, category: str, **options: Any) -> None:
    with pytest.raises(_Invalid) as raised:
        _listed(world, at, **options)
    assert raised.value.category == category, raised.value


def _ordered(entries: tuple[EquityCandleEvidenceSnapshotCatalogEntry, ...]) -> bool:
    """Ordered by parsed as_of, then snapshot_id."""
    return all(
        a.as_of.compare(b.as_of) < 0
        or (a.as_of.compare(b.as_of) == 0 and a.snapshot_id < b.snapshot_id)
        for a, b in pairwise(entries)
    )


# ----------------------------------------------------------------------
# MS-01, MS-02, MS-29, MS-35, MS-40: eligibility on parsed instants
# ----------------------------------------------------------------------


def test_ms01_no_eligible_snapshot_is_an_empty_answer_and_the_file_is_untouched(
    world: LifecycleWorld,
) -> None:
    before = _file_state(world.database)
    assert _listed(world, ist(9, 35, 59)) == ()
    assert _file_state(world.database) == before  # nothing written, no journal left behind


def test_ms02_ms35_ms40_both_guards_on_parsed_instants_and_equality_is_eligible(
    world: LifecycleWorld,
) -> None:
    research = _research(world)
    on_time = issue(world, ist(9, 50))  # as_of = issued_at = 09:50
    late = issue(world, ist(9, 49), ist(9, 55))  # as_of 09:49, declared issued 09:55
    later = issue(world, ist(9, 52))
    at_0950 = _ids(world, ist(9, 50))
    assert set(at_0950) == {*research.values(), on_time}  # equality on both instants
    assert late not in at_0950  # as_of <= T, issued_at > T: excluded (MS-40)
    assert late not in _ids(world, PointInTime("2026-08-19T04:24:59.999999Z"))
    at_0955 = _ids(world, ist(9, 55))
    assert {late, later} <= set(at_0955)  # issued_at == T is eligible
    entry = next(e for e in _listed(world, ist(9, 55)) if e.snapshot_id == late)
    snapshot = SQLiteEquityCandleEvidenceSnapshotStore(world.database).get_snapshot(late)
    assert snapshot is not None
    assert (entry.as_of, entry.issued_at, entry.watermark, entry.manifest_fingerprint) == (
        snapshot.as_of,
        snapshot.issued_at,
        snapshot.watermark,
        snapshot.manifest_fingerprint,
    )


def test_ms02_a_monitoring_instant_given_with_an_offset_is_the_same_instant(
    world: LifecycleWorld,
) -> None:
    issue(world, ist(9, 50))
    offset = PointInTime("2026-08-19T09:50:00+05:30")
    assert offset.value == ist(9, 50).value == "2026-08-19T04:20:00Z"
    assert _listed(world, offset) == _listed(world, ist(9, 50))


def test_ms29_fractional_seconds_are_ordered_chronologically_not_as_text(
    world: LifecycleWorld,
) -> None:
    whole = issue(world, ist(10, 0))
    half = issue(world, instant(ist(10, 0), 0.5))
    quarter = issue(world, instant(ist(10, 0), 0.25))
    texts = {_stored_row(world, i)["as_of"]: i for i in (whole, half, quarter)}
    assert [texts[t] for t in sorted(texts)] == [quarter, half, whole]  # text order is wrong
    assert _ids(world, ist(10, 0))[-1] == whole  # 10:00:00.25 and .5 are later
    assert half not in _ids(world, instant(ist(10, 0), 0.25))
    assert _ids(world, ist(10, 1))[-3:] == [whole, quarter, half]  # chronological


def test_ms02_limitation_a_declared_issued_at_is_not_proof_of_availability(
    world: LifecycleWorld,
) -> None:
    """LIMITATION (design 5.1): a snapshot written later but declaring an early issued_at."""
    first = _ids(world, ist(9, 50))
    backdated = issue(world, ist(9, 48), ist(9, 49))  # written now, declares 09:49
    second = _ids(world, ist(9, 50))
    assert backdated not in first and backdated in second  # same T, a larger answer
    # Nothing persisted records the real creation time; the catalog cannot tell.


# ----------------------------------------------------------------------
# MS-21, MS-25: scope
# ----------------------------------------------------------------------

_OTHER_SCOPE_VALUES = {
    "symbol": "INFY",
    "exchange_code": "BSE",
    "trading_date": "2026-08-20",
    "timeframe": "1m",
    "window_policy_version": "another-research-window/1",
    "evidence_class": "PRODUCTION",
}


@pytest.mark.parametrize("column", sorted(_OTHER_SCOPE_VALUES))
def test_ms21_ms25_each_scope_column_is_filtered_in_sql(world: LifecycleWorld, column: str) -> None:
    research = _research(world)
    baseline = _listed(world, ist(10, 0))
    clone = {**_stored_row(world, research["09:46"]), "snapshot_id": _FAKE_ID}
    _insert(world, {**clone, column: _OTHER_SCOPE_VALUES[column]})  # TEST-ONLY row
    assert _listed(world, ist(10, 0)) == baseline  # another scope: never listed, never read


def test_ms21_the_same_clone_within_the_scope_is_listed_and_fails_verification(
    world: LifecycleWorld,
) -> None:
    """The control for the test above: the clone is excluded by the filter, not by chance."""
    clone = {**_stored_row(world, _research(world)["09:46"]), "snapshot_id": _FAKE_ID}
    _insert(world, clone)
    _invalid(world, ist(10, 0), _Invalid.INTEGRITY)  # its document names another identity


def test_ms21_another_listing_in_the_same_file_is_never_listed(world: LifecycleWorld) -> None:
    tcs = _ids(world, ist(10, 0))
    establish_infy_breakout(world.world)  # INFY snapshots at 09:36, 09:41, 09:46
    assert _ids(world, ist(10, 0)) == tcs
    infy = EquityCandleEvidenceSnapshotCatalogScope(
        INFY, DAY, SCOPE.timeframe, SCOPE.window_policy_version, SCOPE.evidence_class
    )
    listed = _listed(world, ist(10, 0), scope=infy)
    assert len(listed) == 3 and {e.listing for e in listed} == {INFY}
    assert not set(tcs) & {e.snapshot_id for e in listed}


@pytest.mark.parametrize(
    "scope",
    [
        EquityCandleEvidenceSnapshotCatalogScope(
            ListingReference(Symbol("TCS"), ExchangeCode("BSE")),
            DAY,
            SCOPE.timeframe,
            SCOPE.window_policy_version,
            SCOPE.evidence_class,
        ),
        EquityCandleEvidenceSnapshotCatalogScope(
            TCS,
            DAY.replace(day=20),
            SCOPE.timeframe,
            SCOPE.window_policy_version,
            SCOPE.evidence_class,
        ),
        EquityCandleEvidenceSnapshotCatalogScope(
            TCS, DAY, Timeframe("1m"), SCOPE.window_policy_version, SCOPE.evidence_class
        ),
        EquityCandleEvidenceSnapshotCatalogScope(
            TCS, DAY, SCOPE.timeframe, "another-research-window/1", SCOPE.evidence_class
        ),
        EquityCandleEvidenceSnapshotCatalogScope(
            TCS,
            DAY,
            SCOPE.timeframe,
            SCOPE.window_policy_version,
            EquityCandleEvidenceClass.PRODUCTION,
        ),
    ],
    ids=["exchange", "date", "timeframe", "window-version", "production"],
)
def test_ms21_ms25_a_scope_no_snapshot_matches_lists_nothing(
    world: LifecycleWorld, scope: EquityCandleEvidenceSnapshotCatalogScope
) -> None:
    assert _listed(world, ist(15, 30), scope=scope) == ()


# ----------------------------------------------------------------------
# MS-03: complete same-instant groups
# ----------------------------------------------------------------------


def test_ms03_equivalent_snapshots_at_one_instant_are_all_listed(world: LifecycleWorld) -> None:
    twins = [issue(world, ist(10, 0), ist(10, 0, second)) for second in (0, 30, 59)]
    listed = [e for e in _listed(world, ist(10, 1)) if e.as_of == ist(10, 0)]
    assert [e.snapshot_id for e in listed] == sorted(twins)  # none deduplicated or chosen
    assert len({e.manifest_fingerprint for e in listed}) == 1  # one manifest, three identities
    assert len({e.issued_at for e in listed}) == 3
    partial = [e.snapshot_id for e in _listed(world, ist(10, 0, 30)) if e.as_of == ist(10, 0)]
    assert sorted(partial) == sorted(twins[:2])  # the third is not yet issued at T


def test_ms03_conflicting_manifests_at_one_instant_are_all_listed(world: LifecycleWorld) -> None:
    earlier = issue(world, ist(9, 55))
    first = issue(world, ist(10, 0))
    world.bars({1: QUIET_BAR}, listing=INFY)  # the journal grows: a later watermark
    second = issue(world, ist(10, 0), ist(10, 0, 30))
    listed = {e.snapshot_id: e for e in _listed(world, ist(10, 1))}
    assert {earlier, first, second} <= set(listed)
    assert listed[first].manifest_fingerprint != listed[second].manifest_fingerprint
    assert listed[first].watermark < listed[second].watermark
    # No winner is chosen: selection and the conflict belong to the Application.


# ----------------------------------------------------------------------
# MS-30: the governed limit of 300 eligible candidates
# ----------------------------------------------------------------------


@pytest.mark.parametrize("eligible", [0, 1, 299, 300])
def test_ms30_up_to_the_limit_every_eligible_snapshot_is_listed(
    crowded: LifecycleWorld, eligible: int
) -> None:
    at = instant(CROWD_START, eligible - 1)  # 0 eligible: one second before the first
    listed = _listed(crowded, at)
    assert len(listed) == eligible
    assert [e.as_of for e in listed] == [instant(CROWD_START, k) for k in range(eligible)]


def test_ms30_one_eligible_snapshot_beyond_the_limit_is_refused_never_truncated(
    crowded: LifecycleWorld,
) -> None:
    with pytest.raises(_Limit) as raised:
        _listed(crowded, instant(CROWD_START, 300))  # 301 eligible
    assert raised.value.limit == LIMIT
    assert "301 snapshots are eligible" in str(raised.value)
    with pytest.raises(_Limit):
        _listed(crowded, ist(15, 0))  # 304 eligible
    assert len(_listed(crowded, ist(15, 0), limit=304)) == 304  # all of them, when admitted


def test_ms30_the_limit_counts_eligible_rows_not_physical_rows(crowded: LifecycleWorld) -> None:
    assert len(_listed(crowded, instant(CROWD_START, 120))) == 121  # 304 rows in the scope
    future = issue(crowded, instant(CROWD_START, 1.5), ist(9, 50))  # as_of early, issued 09:50
    at = instant(CROWD_START, 299)
    listed = _listed(crowded, at)  # 300 eligible: the future-declared one is not counted
    assert len(listed) == 300 and future not in {e.snapshot_id for e in listed}
    with pytest.raises(_Limit):
        _listed(crowded, ist(9, 50))


def test_ms30_ms29_the_limit_holds_across_mixed_textual_instants(crowded: LifecycleWorld) -> None:
    at = instant(CROWD_START, 298)  # 299 eligible
    fraction = issue(crowded, instant(CROWD_START, 0.5))
    listed = _listed(crowded, at)
    assert len(listed) == 300
    assert [e.as_of for e in listed[:3]] == [
        CROWD_START,
        instant(CROWD_START, 0.5),  # its text sorts before '...:00Z', its instant after
        instant(CROWD_START, 1),
    ]
    assert listed[1].snapshot_id == fraction
    issue(crowded, instant(CROWD_START, 1.25))
    with pytest.raises(_Limit):
        _listed(crowded, at)  # 301 eligible across whole and fractional seconds


def test_ms30_overflow_on_the_final_same_instant_group_returns_nothing(
    crowded: LifecycleWorld,
) -> None:
    group = instant(CROWD_START, 298.5)  # 299 eligible before it
    first = issue(crowded, group)
    issue(crowded, group, instant(group, 0.25))  # the same manifest, issued later
    listed = _listed(crowded, group)
    assert len(listed) == 300 and listed[-1].snapshot_id == first
    with pytest.raises(_Limit):
        _listed(crowded, instant(group, 0.25))  # the whole group would make 301


@pytest.mark.parametrize(
    ("limit", "error"), [(0, ValueError), (-1, ValueError), (True, TypeError), ("300", TypeError)]
)
def test_ms30_the_limit_must_be_a_positive_integer(
    world: LifecycleWorld, limit: object, error: type[Exception]
) -> None:
    with pytest.raises(error):
        _catalog(world).list_snapshots(SCOPE, ist(10, 0), limit)  # type: ignore[arg-type]


def test_the_arguments_are_typed(world: LifecycleWorld) -> None:
    with pytest.raises(TypeError):
        _catalog(world).list_snapshots("scope", ist(10, 0), LIMIT)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        _catalog(world).list_snapshots(SCOPE, "2026-08-19T04:30:00Z", LIMIT)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        SQLiteEquityCandleEvidenceSnapshotCatalog(42)  # type: ignore[arg-type]


# ----------------------------------------------------------------------
# Fail-closed corruption (TEST-ONLY tampering of a disposable copy)
# ----------------------------------------------------------------------


@pytest.mark.parametrize("column", ["as_of", "issued_at"])
@pytest.mark.parametrize(
    "text",
    [
        "not-an-instant",
        "",
        "2026-08-19T04:16:00",  # naive
        "2026-08-19T09:46:00+05:30",  # the same instant, not canonical
        "2026-08-19T04:16:00.000Z",  # not canonical
        " 2026-08-19T04:16:00Z",
        "2026-08-19T04:16:00.1234567Z",  # beyond microseconds
    ],
)
def test_a_malformed_stored_instant_of_the_scope_is_metadata(
    world: LifecycleWorld, column: str, text: str
) -> None:
    _set(world, _research(world)["09:46"], **{column: text})
    _invalid(world, ist(10, 0), _Invalid.METADATA)
    _invalid(world, ist(9, 40), _Invalid.METADATA)  # even when it might be ineligible: unknown


@pytest.mark.parametrize("value", ["seven", -1, 1.5, b"\x01"])
def test_a_malformed_stored_watermark_is_metadata(world: LifecycleWorld, value: object) -> None:
    _set(world, _research(world)["09:46"], checks=False, watermark=value)
    _invalid(world, ist(10, 0), _Invalid.METADATA)


@pytest.mark.parametrize("column", ["snapshot_id", "manifest_fingerprint"])
def test_an_empty_identity_or_fingerprint_is_metadata(world: LifecycleWorld, column: str) -> None:
    _set(world, _research(world)["09:46"], **{column: ""})
    _invalid(world, ist(10, 0), _Invalid.METADATA)


def test_a_stored_issued_at_before_its_as_of_is_metadata(world: LifecycleWorld) -> None:
    _set(world, _research(world)["09:46"], issued_at=ist(9, 45).value)
    _invalid(world, ist(10, 0), _Invalid.METADATA)
    _invalid(world, ist(9, 45, 30), _Invalid.METADATA)  # never silently judged ineligible


def test_a_column_disagreeing_with_the_stored_document_is_integrity(
    world: LifecycleWorld,
) -> None:
    _set(world, _research(world)["09:46"], issued_at=ist(9, 47).value)  # canonical, wrong
    _invalid(world, ist(10, 0), _Invalid.INTEGRITY)


def test_an_altered_manifest_fingerprint_is_integrity(world: LifecycleWorld) -> None:
    _set(world, _research(world)["09:46"], manifest_fingerprint="ecm1-sha256:" + "0" * 64)
    _invalid(world, ist(10, 0), _Invalid.INTEGRITY)


@pytest.mark.parametrize("document", ["{}", "not json", '{"format": "other"}'])
def test_a_stored_document_that_does_not_decode_is_metadata(
    world: LifecycleWorld, document: str
) -> None:
    _set(world, _research(world)["09:46"], document=document)
    _invalid(world, ist(10, 0), _Invalid.METADATA)


def test_missing_journal_evidence_or_entry_rows_are_integrity(
    world: LifecycleWorld, tmp_path: Path
) -> None:
    research = _research(world)["09:46"]
    entries = copy_world(world, tmp_path / "entries.sqlite3")
    bypass(
        entries.database,
        (
            "DELETE FROM equity_candle_evidence_snapshot_entries WHERE snapshot_id = ? "
            "AND sequence = (SELECT MAX(sequence) FROM equity_candle_evidence_snapshot_entries "
            "WHERE snapshot_id = ?)",
            (research, research),
        ),
        drop=("equity_candle_evidence_snapshot_entries_no_delete",),
    )
    _invalid(entries, ist(10, 0), _Invalid.INTEGRITY)
    bypass(
        world.database,
        (
            "DELETE FROM equity_candle_journal WHERE sequence = (SELECT MIN(sequence) FROM "
            "equity_candle_evidence_snapshot_entries WHERE snapshot_id = ?)",
            (research,),
        ),
        drop=("equity_candle_journal_no_delete",),
        foreign_keys=False,
    )
    _invalid(world, ist(10, 0), _Invalid.INTEGRITY)


def test_a_corrupt_row_after_the_first_300_in_scan_order_is_still_found(
    crowded: LifecycleWorld,
) -> None:
    with closing(raw(crowded.database)) as connection:
        (last,) = connection.execute(f"SELECT MAX(snapshot_id) FROM {SNAPSHOTS}").fetchone()  # noqa: S608
    _set(crowded, last, as_of="corrupt")
    _invalid(crowded, instant(CROWD_START, 10), _Invalid.METADATA)  # 11 eligible
    _invalid(crowded, ist(15, 0), _Invalid.METADATA)  # corruption is reported before the limit


def _corrupt_page(database: Path, table: str) -> None:
    with closing(sqlite3.connect(database)) as connection:
        (page_size,) = connection.execute("PRAGMA page_size").fetchone()
        (root,) = connection.execute(
            "SELECT rootpage FROM sqlite_master WHERE name = ?", (table,)
        ).fetchone()
    with database.open("r+b") as handle:  # TEST-ONLY: damage one b-tree page of a copy
        handle.seek((root - 1) * page_size)
        handle.write(b"\xa5" * page_size)


def test_a_damaged_database_page_is_permanent_incomplete(world: LifecycleWorld) -> None:
    _corrupt_page(world.database, SNAPSHOTS)
    _invalid(world, ist(10, 0), _Invalid.INCOMPLETE)


def test_a_file_that_is_not_a_database_is_permanent_incomplete(tmp_path: Path) -> None:
    path = tmp_path / "not-a-database.sqlite3"
    path.write_bytes(b"\x00garbage" * 1024)
    with pytest.raises(_Invalid) as raised:
        SQLiteEquityCandleEvidenceSnapshotCatalog(path).list_snapshots(SCOPE, ist(10, 0), LIMIT)
    assert raised.value.category == _Invalid.INCOMPLETE


# ----------------------------------------------------------------------
# SQLite failures: typed, retryable unavailability never mistaken for data
# ----------------------------------------------------------------------


def test_a_missing_database_is_unavailable_and_never_created(tmp_path: Path) -> None:
    path = tmp_path / "missing.sqlite3"
    with pytest.raises(_Unavailable):
        SQLiteEquityCandleEvidenceSnapshotCatalog(path).list_snapshots(SCOPE, ist(10, 0), LIMIT)
    assert list(tmp_path.iterdir()) == []


def test_a_database_without_the_evidence_tables_is_unavailable_and_unchanged(
    tmp_path: Path,
) -> None:
    path = tmp_path / "other.sqlite3"
    with closing(sqlite3.connect(path)) as connection:
        connection.execute("CREATE TABLE unrelated (x INTEGER)")
        connection.commit()
    before = path.read_bytes()
    with pytest.raises(_Unavailable, match="no EI-2B evidence snapshot tables"):
        SQLiteEquityCandleEvidenceSnapshotCatalog(path).list_snapshots(SCOPE, ist(10, 0), LIMIT)
    assert path.read_bytes() == before  # no schema created


def test_a_locked_database_is_unavailable_and_reads_resume_after_release(
    world: LifecycleWorld, monkeypatch: pytest.MonkeyPatch
) -> None:
    baseline = _listed(world, ist(10, 0))
    monkeypatch.setattr(catalog_module, "_BUSY_TIMEOUT_SECONDS", 0.05)
    holder = raw(world.database)
    try:
        holder.execute("BEGIN EXCLUSIVE")  # another connection holds the database
        with pytest.raises(_Unavailable, match="SQLITE_BUSY"):
            _listed(world, ist(10, 0))
    finally:
        holder.execute("ROLLBACK")
        holder.close()
    assert _listed(world, ist(10, 0)) == baseline


def test_a_read_only_file_is_listed(world: LifecycleWorld) -> None:
    baseline = _listed(world, ist(10, 0))
    mode = world.database.stat().st_mode
    os.chmod(world.database, stat.S_IREAD)
    try:
        assert _listed(world, ist(10, 0)) == baseline
    finally:
        os.chmod(world.database, mode)


class _Proxy:
    """TEST-ONLY: a connection whose cleanup can be made to fail."""

    def __init__(self, connection: sqlite3.Connection, failing: bool) -> None:
        self._connection, self._failing, self.closed = connection, failing, False

    def execute(self, *args: Any) -> Any:
        return self._connection.execute(*args)

    @property
    def in_transaction(self) -> bool:
        return self._connection.in_transaction

    def rollback(self) -> None:
        self._connection.rollback()
        if self._failing:
            raise sqlite3.OperationalError("test: rollback failed")

    def close(self) -> None:
        self._connection.close()
        self.closed = True
        if self._failing:
            raise sqlite3.OperationalError("test: close failed")


@contextmanager
def _proxied(monkeypatch: pytest.MonkeyPatch, failing: bool) -> Iterator[list[_Proxy]]:
    real, opened = catalog_module._connect, []  # noqa: SLF001

    def connect(database_path: Path) -> Any:
        proxy = _Proxy(real(database_path), failing)
        opened.append(proxy)
        return proxy

    monkeypatch.setattr(catalog_module, "_connect", connect)
    yield opened
    monkeypatch.setattr(catalog_module, "_connect", real)


def test_a_failure_while_reading_rows_is_unavailable_and_the_connection_is_closed(
    world: LifecycleWorld, monkeypatch: pytest.MonkeyPatch
) -> None:
    real, calls = catalog_module._stored_snapshot, []  # noqa: SLF001

    def failing(connection: sqlite3.Connection, snapshot_id: str) -> Any:
        calls.append(snapshot_id)
        if len(calls) == 2:
            raise sqlite3.OperationalError("test: disk I/O error")
        return real(connection, snapshot_id)

    with _proxied(monkeypatch, failing=False) as opened:
        monkeypatch.setattr(catalog_module, "_stored_snapshot", failing)
        with pytest.raises(_Unavailable) as raised:
            _listed(world, ist(10, 0))
        assert isinstance(raised.value.__cause__, sqlite3.OperationalError)
        assert opened[0].closed


def test_an_interrupt_propagates_unchanged_and_the_connection_is_closed(
    world: LifecycleWorld, monkeypatch: pytest.MonkeyPatch
) -> None:
    def interrupted(connection: sqlite3.Connection, snapshot_id: str) -> Any:
        raise KeyboardInterrupt

    with _proxied(monkeypatch, failing=False) as opened:
        monkeypatch.setattr(catalog_module, "_stored_snapshot", interrupted)
        with pytest.raises(KeyboardInterrupt):
            _listed(world, ist(10, 0))
        assert opened[0].closed


def test_a_cleanup_failure_never_replaces_the_primary_failure(
    world: LifecycleWorld, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set(world, _research(world)["09:46"], as_of="corrupt")
    with _proxied(monkeypatch, failing=True) as opened:
        _invalid(world, ist(10, 0), _Invalid.METADATA)  # not the rollback or close failure
        assert opened[0].closed


def test_a_close_failure_after_a_complete_read_is_unavailable(
    world: LifecycleWorld, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _proxied(monkeypatch, failing=True):
        with pytest.raises(_Unavailable):
            _listed(world, ist(10, 0))  # nothing is returned when the read cannot end cleanly


# ----------------------------------------------------------------------
# E1, E3, I6: one consistent read view
# ----------------------------------------------------------------------


def test_e1_the_database_runs_the_rollback_journal_the_guarantees_were_verified_under(
    world: LifecycleWorld,
) -> None:
    with closing(raw(world.database)) as connection:
        assert connection.execute("PRAGMA journal_mode").fetchone() == ("delete",)


def test_i6_an_uncommitted_or_rolled_back_snapshot_row_is_never_listed(
    world: LifecycleWorld,
) -> None:
    baseline = _listed(world, ist(10, 0))
    clone = {**_stored_row(world, _research(world)["09:46"]), "snapshot_id": _FAKE_ID}
    writer = raw(world.database)
    try:
        writer.execute("BEGIN IMMEDIATE")
        writer.execute(
            f"INSERT INTO {SNAPSHOTS} ({', '.join(clone)}) "  # noqa: S608
            f"VALUES ({', '.join('?' * len(clone))})",
            tuple(clone.values()),
        )
        assert _listed(world, ist(10, 0)) == baseline  # uncommitted: invisible (no dirty read)
        writer.execute("ROLLBACK")
    finally:
        writer.close()
    assert _listed(world, ist(10, 0)) == baseline  # rolled back: never existed


def test_e1_no_write_can_commit_inside_the_listing_view(
    world: LifecycleWorld, monkeypatch: pytest.MonkeyPatch
) -> None:
    baseline = _listed(world, ist(10, 0))
    clone = {**_stored_row(world, _research(world)["09:46"]), "snapshot_id": _FAKE_ID}
    real_claim, attempts = catalog_module._Claim, []  # noqa: SLF001

    def claim(row: tuple[Any, ...]) -> Any:
        if not attempts:
            writer = sqlite3.connect(world.database, timeout=0.05, isolation_level=None)
            try:
                writer.execute("BEGIN IMMEDIATE")
                writer.execute(
                    f"INSERT INTO {SNAPSHOTS} ({', '.join(clone)}) "  # noqa: S608
                    f"VALUES ({', '.join('?' * len(clone))})",
                    tuple(clone.values()),
                )
                writer.execute("COMMIT")
                attempts.append("committed")
            except sqlite3.OperationalError as exc:
                attempts.append(str(exc))
                writer.execute("ROLLBACK")
            finally:
                writer.close()
        return real_claim(row)

    monkeypatch.setattr(catalog_module, "_Claim", claim)
    assert _listed(world, ist(10, 0)) == baseline
    assert attempts == ["database is locked"]  # the listing view held its shared lock
    monkeypatch.setattr(catalog_module, "_Claim", real_claim)
    assert _listed(world, ist(10, 0)) == baseline  # and nothing was committed


def test_e1_a_snapshot_committed_after_the_listing_view_is_not_in_that_answer(
    world: LifecycleWorld, monkeypatch: pytest.MonkeyPatch
) -> None:
    baseline = _ids(world, ist(10, 0))
    real_verified, written = catalog_module._verified, []  # noqa: SLF001

    def verified(connection: sqlite3.Connection, claim: Any, scope: Any) -> Any:
        if not written:  # another connection commits between the view and verification
            written.append(issue(world, ist(9, 58)))
        return real_verified(connection, claim, scope)

    monkeypatch.setattr(catalog_module, "_verified", verified)
    assert _ids(world, ist(10, 0)) == baseline  # the answer is the listing view's
    monkeypatch.setattr(catalog_module, "_verified", real_verified)
    assert written[0] in _ids(world, ist(10, 0))  # the next call sees it


def test_e2_a_listed_row_that_vanishes_before_verification_is_incomplete(
    world: LifecycleWorld, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Only possible by bypassing the append-only triggers: TEST-ONLY deletion mid-call."""
    research = _research(world)["09:46"]
    real_verified, deleted = catalog_module._verified, []  # noqa: SLF001

    def verified(connection: sqlite3.Connection, claim: Any, scope: Any) -> Any:
        if not deleted:
            deleted.append(research)
            bypass(
                world.database,
                (
                    "DELETE FROM equity_candle_evidence_snapshot_entries WHERE snapshot_id = ?",
                    (research,),
                ),
                (f"DELETE FROM {SNAPSHOTS} WHERE snapshot_id = ?", (research,)),  # noqa: S608
                drop=(
                    "equity_candle_evidence_snapshot_entries_no_delete",
                    f"{SNAPSHOTS}_no_delete",
                ),
                foreign_keys=False,  # the EI-3C timeline references the research snapshot
            )
        return real_verified(connection, claim, scope)

    monkeypatch.setattr(catalog_module, "_verified", verified)
    _invalid(world, ist(10, 0), _Invalid.INCOMPLETE)  # never an answer without it


def test_e3_a_candidate_set_growing_past_the_limit_concurrently(
    crowded: LifecycleWorld, monkeypatch: pytest.MonkeyPatch
) -> None:
    at = instant(CROWD_START, 299)  # 300 eligible
    real_verified, written = catalog_module._verified, []  # noqa: SLF001

    def verified(connection: sqlite3.Connection, claim: Any, scope: Any) -> Any:
        if not written:
            written.append(issue(crowded, instant(CROWD_START, 2.5)))  # the 301st eligible
        return real_verified(connection, claim, scope)

    monkeypatch.setattr(catalog_module, "_verified", verified)
    assert len(_listed(crowded, at)) == 300  # consistent: the view had 300
    monkeypatch.setattr(catalog_module, "_verified", real_verified)
    with pytest.raises(_Limit):
        _listed(crowded, at)  # never 300 of 301


# ----------------------------------------------------------------------
# J: read-only enforcement
# ----------------------------------------------------------------------


def test_j_the_catalog_connection_cannot_write_or_change_the_schema(
    world: LifecycleWorld,
) -> None:
    before = _file_state(world.database)
    connection = catalog_module._connect(world.database)  # noqa: SLF001
    try:
        assert connection.execute("PRAGMA query_only").fetchone() == (1,)
        for statement in (
            "CREATE TABLE monitoring_checkpoint (x INTEGER)",
            f"DELETE FROM {SNAPSHOTS}",
            "PRAGMA user_version = 7",
        ):
            with pytest.raises(sqlite3.OperationalError, match="readonly|read-only|query_only"):
                connection.execute(statement)
    finally:
        connection.close()
    assert _file_state(world.database) == before


def test_j_listing_leaves_the_file_byte_identical_on_every_path(
    crowded: LifecycleWorld,
) -> None:
    before = _file_state(crowded.database)
    _listed(crowded, ist(9, 33))
    with pytest.raises(_Limit):
        _listed(crowded, ist(15, 0))
    assert _listed(crowded, ist(9, 29)) == ()
    assert _file_state(crowded.database) == before


def test_f_the_scope_query_uses_the_scope_index_with_bound_parameters(
    world: LifecycleWorld,
) -> None:
    with closing(raw(world.database)) as connection:
        plan = connection.execute(
            f"EXPLAIN QUERY PLAN {catalog_module._SCOPE_ROWS}",  # noqa: SLF001
            ("TCS", "NSE", "2026-08-19", "5m", "v", "SYNTHETIC"),
        ).fetchall()
    assert any("equity_candle_evidence_snapshots_scope" in str(row) for row in plan), plan
    assert catalog_module._SCOPE_ROWS.count("?") == 6  # noqa: SLF001
    assert "LIMIT" not in catalog_module._SCOPE_ROWS.upper()  # noqa: SLF001


def test_ms22_repeated_calls_and_new_instances_give_identical_answers(
    world: LifecycleWorld,
) -> None:
    issue(world, ist(10, 0))
    issue(world, ist(10, 0), ist(10, 0, 30))
    first = _listed(world, ist(10, 1))
    assert all(_listed(world, ist(10, 1)) == first for _ in range(3))
    assert _ordered(first)


# ----------------------------------------------------------------------
# MS-24: architecture
# ----------------------------------------------------------------------

_SOURCE = Path(inspect.getfile(catalog_module)).read_text(encoding="utf-8")


def test_ms24_the_adapter_implements_the_application_port_and_nothing_else() -> None:
    assert issubclass(
        SQLiteEquityCandleEvidenceSnapshotCatalog, EquityCandleEvidenceSnapshotCatalog
    )
    public = {
        name
        for name, _ in inspect.getmembers(SQLiteEquityCandleEvidenceSnapshotCatalog)
        if not name.startswith("_")
    }
    assert public == {"list_snapshots"}
    assert inspect.signature(
        SQLiteEquityCandleEvidenceSnapshotCatalog.list_snapshots
    ) == inspect.signature(EquityCandleEvidenceSnapshotCatalog.list_snapshots)


def test_ms24_the_catalog_module_is_read_only_by_construction() -> None:
    tree = ast.parse(_SOURCE)
    strings = [
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    ]
    sql = [s for s in strings if s.lstrip().upper().startswith(("SELECT", "BEGIN", "ROLLBACK"))]
    assert sql, "the module's SQL statements were not found"
    for keyword in ("INSERT", "UPDATE", "DELETE", "REPLACE", "CREATE", "DROP", "ALTER", "ATTACH"):
        assert not any(keyword in s.upper().split() for s in strings if s in sql), keyword
    assert "?mode=ro" in _SOURCE and "PRAGMA query_only = ON" in _SOURCE
    imported = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
        for alias in node.names
    }
    modules = {
        node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.module
    } | {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    assert not {name for name in imported if name.startswith("initialize_")}
    assert "_connect" not in imported  # the EI-2B connection path initializes a schema
    assert not any("lifecycle" in module or "issuance" in module for module in modules)
    forbidden = {"time", "datetime", "threading", "multiprocessing", "sched", "socket", "random"}
    assert not modules & forbidden, modules & forbidden
    assert "_stored_snapshot" in imported  # the accepted EI-2B verified decoding, reused


def test_ms24_construction_opens_nothing_and_starts_nothing(tmp_path: Path) -> None:
    threads = threading.active_count()
    SQLiteEquityCandleEvidenceSnapshotCatalog(tmp_path / "never-created.sqlite3")
    assert list(tmp_path.iterdir()) == [] and threading.active_count() == threads


def test_the_adapter_is_exported_from_the_persistence_package() -> None:
    from northstar_infrastructure import persistence

    assert persistence.SQLiteEquityCandleEvidenceSnapshotCatalog is (
        SQLiteEquityCandleEvidenceSnapshotCatalog
    )
    assert "SQLiteEquityCandleEvidenceSnapshotCatalog" in persistence.__all__
