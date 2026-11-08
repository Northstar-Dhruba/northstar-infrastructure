"""Synthetic fixtures for the EI4-D10-S2 SQLite policy-catalog tests.

Bundles are built from the EI-4E acceptance references
(tests/equity_recommendation_acceptance_fixtures.py): labelled SYNTHETIC TEST
DATA, approved for nothing. Databases are disposable temporary files that hold
the EI-2B and EI-3C schema, created by their own existing initializers, so the
catalog is always tested inside the single evidence database (EI4-D10-DQ4).

``publish_in_another_process`` is the spawned-process worker. It rebuilds its
bundle from a plain spec and opens its own adapter and connection from the
database path alone; nothing but the file is shared with the parent.
"""

from __future__ import annotations

import os
import sqlite3
from contextlib import closing
from datetime import timedelta
from pathlib import Path
from typing import Any

from northstar_application.application_services import (
    EquityResolvedSyntheticPolicySet,
    EquitySyntheticPolicyBundle,
)

from northstar_infrastructure.persistence import (
    SQLiteEquitySyntheticPolicyCatalogStore,
    initialize_equity_candle_evidence_schema,
    initialize_equity_intraday_research_timeline_schema,
)
from tests.equity_recommendation_acceptance_fixtures import References, policy_set, timing_policy

PUBLICATIONS = "equity_synthetic_policy_publications"
DEFINITIONS = "equity_synthetic_policy_definitions"
POLICY_SETS = "equity_synthetic_policy_sets"
CATALOG_TABLES = (PUBLICATIONS, DEFINITIONS, POLICY_SETS)


def evidence_database(path: Path) -> Path:
    """Create an empty EI-2B and EI-3C evidence database through the existing initializers."""
    with closing(sqlite3.connect(path)) as connection:
        initialize_equity_candle_evidence_schema(connection)
        initialize_equity_intraday_research_timeline_schema(connection)
    return path


def resolved(
    set_version: str = "set/1",
    *,
    generation: str = "generation/1",
    timing: str = "timing/1",
    latency_seconds: int | None = None,
) -> EquityResolvedSyntheticPolicySet:
    """A resolved synthetic policy set; ``latency_seconds`` redefines the timing content."""
    references = References()
    timing_definition = (
        references.timing[timing]
        if latency_seconds is None
        else timing_policy(timing, timedelta(seconds=latency_seconds))
    )
    return EquityResolvedSyntheticPolicySet(
        reference=policy_set(set_version, generation, timing),
        generation_policy=references.generation[generation],
        risk_policy=references.risk["risk/1"],
        cost_schedule=references.costs["costs/1"],
        price_increment=references.increment["increment/1"],
        timing_policy=timing_definition,
    )


def bundle(*args: Any, **kwargs: Any) -> EquitySyntheticPolicyBundle:
    return EquitySyntheticPolicyBundle.of(resolved(*args, **kwargs))


def publish_in_another_process(
    database: str,
    spec: tuple[str, str, int | None],
    start: Any,
    results: Any,
    name: str,
) -> None:
    """Spawned worker: rebuild the bundle, wait at ``start``, publish through the adapter."""
    set_version, timing, latency_seconds = spec
    offered = bundle(set_version, timing=timing, latency_seconds=latency_seconds)
    store = SQLiteEquitySyntheticPolicyCatalogStore(Path(database))
    start.wait(60)
    try:
        written = store.publish(offered)
        results.put((name, os.getpid(), written.outcome.value, None))
    except BaseException as exc:  # noqa: BLE001 -- reported to the parent, never swallowed
        results.put((name, os.getpid(), None, repr(exc)))
