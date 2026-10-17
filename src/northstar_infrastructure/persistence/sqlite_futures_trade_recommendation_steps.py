"""Evidence encoding and exact replay of futures trade recommendation lifecycle steps.

One row of ``futures_trade_recommendation_steps`` is one F-2 lifecycle
evaluation. This module turns an evaluation into that row and rebuilds the
F-2 lifecycle from the rows. It reads only and contains no write SQL; the
recommendation store owns the transaction and the inserts.

Row
---
An OBSERVATION step names its candle by the ``futures_ohlcv`` natural key, the
trading-session label it was matched to and the ``fdc1`` fingerprint of that
candle under that label. A TIME_EXPIRY step names no candle. Both carry:

- ``evaluation``: canonical JSON (sorted keys, no whitespace) of the step's
  events -- kind, touched boundaries, intrabar ordering and expiry reason, in
  F-2 order, an empty list for a quiet bar -- and, for a time expiry, its
  TimeExpiryEvidence: as-of, evaluated-through, coverage, attested-by and
  deficiency exactly as supplied. No price, volume or provider payload is
  stored; an event's bar is the step's candle;
- ``resulting_status`` and ``is_terminal``: the F-2 status after the step.

Every field of a row is a function of the lifecycle before the step and the
step itself, so a row can be checked by regenerating it.

Replay
------
``reconstruct_lifecycle`` starts from the initial WAITING_FOR_ENTRY lifecycle
and, for each row in sequence order:

1. requires the sequence numbers to be exactly 1, 2, 3, ...;
2. resolves an OBSERVATION step's candle from ``futures_ohlcv`` -- of the
   recommendation's own contract and timeframe, never another -- and requires
   its recomputed fingerprint to equal the stored one, so a missing or altered
   candle is refused;
3. applies the step with Core's ``advance`` or ``expire``, refusing a step
   F-2 would not evaluate;
4. rebuilds the stored events as Core events on the resolved candle or time
   evidence and requires them to be exactly the events Core produced;
5. requires the stored row to be byte-for-byte the row the replay produces:
   canonical JSON, status, terminality and every column.

Any disagreement raises; nothing is repaired or substituted. Errors are
ValueError, TypeError or KeyError, which the store reports as a storage error.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import date
from decimal import Decimal
from typing import Any

from northstar_application.application_services import futures_daily_candle_fingerprint
from northstar_application.ports import (
    FuturesTradeRecommendationLifecycleStep,
    FuturesTradeRecommendationObservationStep,
    FuturesTradeRecommendationTimeExpiryStep,
    apply_futures_trade_recommendation_lifecycle_step,
)
from northstar_core.derivatives import ExpirationDate, QuoteValue
from northstar_core.foundation.value_objects import (
    ExchangeCode,
    PointInTime,
    Quantity,
    Symbol,
    Timeframe,
)
from northstar_core.futures import FuturesContract, FuturesOHLCVBar, FuturesProductReference
from northstar_core.recommendations.futures import (
    CoverageDeficiency,
    FuturesTradeRecommendation,
    FuturesTradeRecommendationEvent,
    FuturesTradeRecommendationLifecycle,
    IntrabarOrdering,
    ObservationCoverage,
    RecommendationBoundary,
    RecommendationEventKind,
    RecommendationExpiryReason,
    TimeExpiryEvidence,
)

_STEP_EVIDENCE_VERSION = 1

OBSERVATION_STEP = "OBSERVATION"
TIME_EXPIRY_STEP = "TIME_EXPIRY"

STEP_COLUMNS = (
    "recommendation_identity",
    "sequence",
    "step_kind",
    "product_code",
    "exchange_code",
    "expiration_date",
    "timeframe",
    "point_in_time",
    "trading_date",
    "candle_fingerprint",
    "evaluation",
    "resulting_status",
    "is_terminal",
)
_CANDLE_KEY_COLUMNS = (
    "product_code",
    "exchange_code",
    "expiration_date",
    "timeframe",
    "point_in_time",
)
_CANDLE_VALUE_COLUMNS = ("open_value", "high_value", "low_value", "close_value", "volume")

_SELECT_STEPS = (
    f"SELECT {', '.join(STEP_COLUMNS)} FROM futures_trade_recommendation_steps "  # noqa: S608
    "WHERE recommendation_identity = ? ORDER BY sequence"
)
_SELECT_MARKET_DATA_TABLE = (
    "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'futures_ohlcv'"
)
_SELECT_CANDLE = (
    f"SELECT {', '.join(_CANDLE_VALUE_COLUMNS)} FROM futures_ohlcv WHERE "  # noqa: S608
    + " AND ".join(f"{column} = ?" for column in _CANDLE_KEY_COLUMNS)
)

StepRow = tuple[object, ...]


# ---------------------------------------------------------------------------
# Canonical encoding
# ---------------------------------------------------------------------------


def _canonical_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _text(value: Any) -> str:
    if not isinstance(value, str):
        raise TypeError("Stored step values must be text.")
    return value


def _optional_text(value: Any) -> str | None:
    return None if value is None else _text(value)


def _instant(value: Any) -> PointInTime:
    text = _text(value)
    instant = PointInTime(text)
    if instant.value != text:
        raise ValueError(f"Stored instant {text!r} is not canonical.")
    return instant


def _event_payload(event: FuturesTradeRecommendationEvent) -> dict[str, Any]:
    return {
        "kind": event.kind.value,
        "touched": [boundary.value for boundary in event.touched],
        "ordering": event.ordering.value,
        "expiry_reason": None if event.expiry_reason is None else event.expiry_reason.value,
    }


def _time_evidence_payload(evidence: TimeExpiryEvidence) -> dict[str, Any]:
    return {
        "as_of": evidence.as_of.value,
        "evaluated_through": (
            None if evidence.evaluated_through is None else evidence.evaluated_through.value
        ),
        "coverage": evidence.coverage.value,
        "attested_by": evidence.attested_by,
        "deficiency": None if evidence.deficiency is None else evidence.deficiency.value,
    }


def _evaluation(
    step: FuturesTradeRecommendationLifecycleStep,
    events: tuple[FuturesTradeRecommendationEvent, ...],
) -> str:
    time_evidence = (
        _time_evidence_payload(step.evidence)
        if isinstance(step, FuturesTradeRecommendationTimeExpiryStep)
        else None
    )
    return _canonical_json(
        {
            "version": _STEP_EVIDENCE_VERSION,
            "events": [_event_payload(event) for event in events],
            "time_evidence": time_evidence,
        }
    )


def candle_fingerprint(
    recommendation: FuturesTradeRecommendation, step: FuturesTradeRecommendationObservationStep
) -> str:
    """Return the ``fdc1`` fingerprint of a step's candle under its trading-session label."""
    return futures_daily_candle_fingerprint(recommendation.contract, step.trading_date, step.bar)


def step_row(
    recommendation: FuturesTradeRecommendation,
    sequence: int,
    before: FuturesTradeRecommendationLifecycle,
    step: FuturesTradeRecommendationLifecycleStep,
    after: FuturesTradeRecommendationLifecycle,
) -> StepRow:
    """Return the stored row of one evaluation taking ``before`` to ``after``."""
    events = after.events[len(before.events) :]
    if isinstance(step, FuturesTradeRecommendationObservationStep):
        bar = step.bar
        product = bar.contract.product
        candle: tuple[str | None, ...] = (
            OBSERVATION_STEP,
            product.product_code.value,
            product.exchange_code.value,
            bar.contract.expiration_date.value,
            bar.timeframe.value,
            bar.point_in_time.value,
            step.trading_date.isoformat(),
            candle_fingerprint(recommendation, step),
        )
    else:
        candle = (TIME_EXPIRY_STEP, None, None, None, None, None, None, None)
    return (
        recommendation.identity.identity,
        sequence,
        *candle,
        _evaluation(step, events),
        after.status.value,
        1 if after.is_terminal else 0,
    )


# ---------------------------------------------------------------------------
# Candles
# ---------------------------------------------------------------------------


def _decimal(value: Any) -> Decimal:
    number = Decimal(_text(value))
    if not number.is_finite():
        raise ValueError("Stored candle numbers must be finite.")
    return number


def _candle_key(
    contract: FuturesContract, timeframe: Timeframe, point_in_time: PointInTime
) -> tuple[str, ...]:
    product = contract.product
    return (
        product.product_code.value,
        product.exchange_code.value,
        contract.expiration_date.value,
        timeframe.value,
        point_in_time.value,
    )


def load_candle(
    connection: sqlite3.Connection,
    contract: FuturesContract,
    timeframe: Timeframe,
    point_in_time: PointInTime,
) -> FuturesOHLCVBar | None:
    """Return the stored ``futures_ohlcv`` candle under exactly this key, if any.

    Only the named contract, timeframe and instant is read. No neighbouring or
    other-contract candle is ever returned in its place.
    """
    if connection.execute(_SELECT_MARKET_DATA_TABLE).fetchone() is None:
        return None
    row = connection.execute(
        _SELECT_CANDLE, _candle_key(contract, timeframe, point_in_time)
    ).fetchone()
    if row is None:
        return None
    open_value, high_value, low_value, close_value, volume = row
    return FuturesOHLCVBar(
        contract=contract,
        point_in_time=point_in_time,
        timeframe=timeframe,
        open=QuoteValue(_decimal(open_value)),
        high=QuoteValue(_decimal(high_value)),
        low=QuoteValue(_decimal(low_value)),
        close=QuoteValue(_decimal(close_value)),
        volume=Quantity(_decimal(volume)),
    )


# ---------------------------------------------------------------------------
# Decoding
# ---------------------------------------------------------------------------


def _trading_date(value: Any) -> date:
    text = _text(value)
    parsed = date.fromisoformat(text)
    if parsed.isoformat() != text:
        raise ValueError(f"Stored trading date {text!r} is not canonical.")
    return parsed


def _observation_step(
    connection: sqlite3.Connection,
    recommendation: FuturesTradeRecommendation,
    row: StepRow,
) -> FuturesTradeRecommendationObservationStep:
    product_code, exchange_code, expiration_date, timeframe_text, instant = row[3:8]
    contract = FuturesContract(
        FuturesProductReference(Symbol(_text(product_code)), ExchangeCode(_text(exchange_code))),
        ExpirationDate(_text(expiration_date)),
    )
    timeframe = Timeframe(_text(timeframe_text))
    if contract != recommendation.contract or timeframe != recommendation.source.timeframe:
        raise ValueError(
            f"Step {row[1]} names a {contract} {timeframe} candle, not one of the "
            "recommendation's own contract and timeframe."
        )
    point_in_time = _instant(instant)
    candle = load_candle(connection, contract, timeframe, point_in_time)
    if candle is None:
        raise ValueError(
            f"Step {row[1]} references the {contract} {timeframe} candle at {point_in_time}, "
            "which futures_ohlcv does not hold."
        )
    step = FuturesTradeRecommendationObservationStep(candle, _trading_date(row[8]))
    if candle_fingerprint(recommendation, step) != _text(row[9]):
        raise ValueError(
            f"Step {row[1]}'s candle at {point_in_time} no longer has its recorded fingerprint."
        )
    return step


def _time_expiry_step(row: StepRow) -> FuturesTradeRecommendationTimeExpiryStep:
    if any(value is not None for value in row[3:10]):
        raise ValueError(f"Time-expiry step {row[1]} names a candle.")
    payload = _evaluation_payload(row)["time_evidence"]
    if not isinstance(payload, dict):
        raise TypeError(f"Time-expiry step {row[1]} has no time evidence.")
    evaluated_through = payload["evaluated_through"]
    deficiency = _optional_text(payload["deficiency"])
    return FuturesTradeRecommendationTimeExpiryStep(
        TimeExpiryEvidence(
            as_of=_instant(payload["as_of"]),
            evaluated_through=None if evaluated_through is None else _instant(evaluated_through),
            coverage=ObservationCoverage(_text(payload["coverage"])),
            attested_by=_text(payload["attested_by"]),
            deficiency=None if deficiency is None else CoverageDeficiency(deficiency),
        )
    )


def _evaluation_payload(row: StepRow) -> dict[str, Any]:
    payload = json.loads(_text(row[10]))
    if not isinstance(payload, dict):
        raise TypeError("Stored step evaluation must be a JSON object.")
    version = payload.get("version")
    if version != _STEP_EVIDENCE_VERSION or isinstance(version, bool):
        raise ValueError(f"Unsupported lifecycle step evidence version {version!r}.")
    return payload


def _stored_events(
    recommendation: FuturesTradeRecommendation,
    row: StepRow,
    step: FuturesTradeRecommendationLifecycleStep,
) -> tuple[FuturesTradeRecommendationEvent, ...]:
    """Rebuild a step's stored events as Core events on its own evidence."""
    events = _evaluation_payload(row)["events"]
    if not isinstance(events, list):
        raise TypeError("Stored step events must be a JSON list.")
    observation = step.bar if isinstance(step, FuturesTradeRecommendationObservationStep) else None
    time_evidence = (
        step.evidence if isinstance(step, FuturesTradeRecommendationTimeExpiryStep) else None
    )
    rebuilt = []
    for event in events:
        reason = _optional_text(event["expiry_reason"])
        rebuilt.append(
            FuturesTradeRecommendationEvent(
                recommendation=recommendation.identity,
                kind=RecommendationEventKind(_text(event["kind"])),
                observation=observation,
                touched=tuple(RecommendationBoundary(_text(item)) for item in event["touched"]),
                ordering=IntrabarOrdering(_text(event["ordering"])),
                expiry_reason=None if reason is None else RecommendationExpiryReason(reason),
                time_evidence=time_evidence,
            )
        )
    return tuple(rebuilt)


def reconstruct_lifecycle(
    connection: sqlite3.Connection, recommendation: FuturesTradeRecommendation
) -> tuple[
    FuturesTradeRecommendationLifecycle, tuple[FuturesTradeRecommendationLifecycleStep, ...]
]:
    """Replay a recommendation's stored steps through Core; return the lifecycle and steps.

    ``recommendation`` must already be the validated stored definition. A
    recommendation with no step is its initial WAITING_FOR_ENTRY lifecycle.
    """
    lifecycle = FuturesTradeRecommendationLifecycle(recommendation)
    steps: list[FuturesTradeRecommendationLifecycleStep] = []
    rows = connection.execute(_SELECT_STEPS, (recommendation.identity.identity,)).fetchall()
    for expected, stored in enumerate(rows, start=1):
        row = tuple(stored)
        sequence = row[1]
        if not isinstance(sequence, int) or isinstance(sequence, bool) or sequence != expected:
            raise ValueError(f"Lifecycle steps are not numbered contiguously at step {expected}.")
        kind = row[2]
        step: FuturesTradeRecommendationLifecycleStep
        if kind == OBSERVATION_STEP:
            step = _observation_step(connection, recommendation, row)
        elif kind == TIME_EXPIRY_STEP:
            step = _time_expiry_step(row)
        else:
            raise ValueError(f"Step {sequence} has unknown kind {kind!r}.")

        after = apply_futures_trade_recommendation_lifecycle_step(lifecycle, step)
        if _stored_events(recommendation, row, step) != after.events[len(lifecycle.events) :]:
            raise ValueError(f"Step {sequence}'s stored events are not what F-2 produces.")
        if step_row(recommendation, sequence, lifecycle, step, after) != row:
            raise ValueError(
                f"Step {sequence} is stored non-canonically or its status, terminality or "
                "candle reference disagrees with its F-2 replay."
            )
        lifecycle = after
        steps.append(step)
    return lifecycle, tuple(steps)
