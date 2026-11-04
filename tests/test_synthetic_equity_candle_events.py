"""EI-2C: deterministic synthetic equity candle and finality events (test inputs only)."""

from __future__ import annotations

import ast
import inspect
from datetime import timedelta

import pytest
from northstar_core.domain.value_objects import ListingReference
from northstar_core.foundation.value_objects import ExchangeCode, PointInTime, Symbol

from northstar_infrastructure.market_data import (
    InvalidSyntheticEquityCandleEventError,
    SyntheticEquityCandleEventSource,
)
from northstar_infrastructure.market_data import synthetic_equity_candle_events as module

_LISTING = ListingReference(Symbol("M&M"), ExchangeCode("NSE"))
_END = PointInTime("2026-08-12T03:50:00Z")
_SOURCE = SyntheticEquityCandleEventSource("ei2c-test/1", "synthetic-src", _LISTING)


def test_events_are_deterministic_explicitly_synthetic_and_identified() -> None:
    observation, again = _SOURCE.observation(_END), _SOURCE.observation(_END)
    finality = _SOURCE.finality(_END)
    assert observation == again
    assert observation.is_synthetic and finality.is_synthetic
    event = "ei2c-test/1:M&M@NSE:2026-08-12T03:50:00Z"
    assert observation.observation.source_event_id == event == finality.evidence.source_event_id
    assert observation.ingestion_id == f"{event}/observation/r1"
    assert finality.ingestion_id == f"{event}/finality/r1"
    bar = observation.observation.bar
    assert (str(bar.open.amount), str(bar.close.amount), str(bar.volume.value)) == (
        "1511.5",
        "1512.25",
        "10230",
    )
    assert observation.received_at == PointInTime("2026-08-12T03:50:05Z")
    assert finality.evidence.confirmed_at == PointInTime("2026-08-12T03:50:10Z")
    assert finality.received_at == PointInTime("2026-08-12T03:50:11Z")
    assert observation.observation.finality is None


def test_revisions_corrections_and_explicit_times_are_honoured() -> None:
    corrected = _SOURCE.observation(
        _END,
        revision="r2",
        corrects="r1",
        prices=("1", "2", "0.5", "1.5"),
        received_at=PointInTime("2026-08-12T05:00:00Z"),
        published_at=PointInTime("2026-08-12T04:59:00Z"),
    )
    assert corrected.observation.corrects_revision == "r1"
    assert corrected.ingestion_id.endswith("/observation/r2")
    assert corrected.source_published_at == PointInTime("2026-08-12T04:59:00Z")
    assert corrected.observation.bar.close.amount == 1.5


def test_a_session_omits_exactly_what_it_is_told_and_fabricates_nothing() -> None:
    ends = (_END, PointInTime("2026-08-12T03:55:00Z"), PointInTime("2026-08-12T04:00:00Z"))
    full = _SOURCE.session(ends)
    assert [type(e).__name__ for e in full] == [
        "EquityCandleObservationRecord",
        "EquityCandleFinalityRecord",
    ] * 3
    partial = _SOURCE.session(
        ends, without_finality=frozenset({ends[0]}), without_observation=frozenset({ends[1]})
    )
    assert [e.ingestion_id.rsplit("/", 2)[1] for e in partial] == [
        "observation",
        "observation",
        "finality",
    ]


@pytest.mark.parametrize(
    "changes",
    [{"version": " "}, {"source_id": ""}, {"listing": "M&M"}, {"receipt_lag": timedelta(-1)}],
)
def test_malformed_parameters_are_refused(changes) -> None:  # type: ignore[no-untyped-def]
    arguments = {"version": "v/1", "source_id": "s", "listing": _LISTING} | changes
    with pytest.raises(InvalidSyntheticEquityCandleEventError):
        SyntheticEquityCandleEventSource(**arguments)


def test_the_source_reads_no_clock_or_network() -> None:
    tree = ast.parse(inspect.getsource(module))
    called = {
        node.func.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
    }
    assert not called & {"now", "today", "utcnow", "time", "sleep", "urlopen", "get", "post"}
    imported = {node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)}
    assert not {m for m in imported if m and any(x in m for x in ("socket", "http", "upstox"))}
