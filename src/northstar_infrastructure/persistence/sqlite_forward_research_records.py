"""SQLite-backed forward research record store and repository.

Frozen decisions are append-only with respect to their natural identity. The
natural key is stored as constrained, queryable columns; the complete decision
evidence is stored as explicit JSON so a record round-trips exactly. Decimal
values are persisted as text and never as floats, and no pickled state is used.
"""

from __future__ import annotations

import json
import sqlite3
from decimal import Decimal
from functools import cmp_to_key
from pathlib import Path
from typing import Any

from northstar_application.application_services import (
    AnalyzeAssetResult,
    ForwardResearchRecord,
)
from northstar_application.ports import (
    ForwardResearchRecordConflictError,
    ForwardResearchRecordQuery,
    ForwardResearchRecordRepository,
    ForwardResearchRecordStore,
)
from northstar_core.domain.value_objects import ListingReference
from northstar_core.foundation.value_objects import (
    Currency,
    ExchangeCode,
    PointInTime,
    Price,
    Quantity,
    Symbol,
    Timeframe,
)
from northstar_core.strategy import (
    AssetAnalysis,
    ExplanationReason,
    MarketObservationContext,
    Recommendation,
    RecommendationAction,
    RecommendationExplanation,
    StrategyIdentity,
)

from northstar_infrastructure.persistence.sqlite_forward_research_schema import (
    initialize_forward_research_record_schema,
)

_EVIDENCE_VERSION = 1


class ForwardResearchStorageError(RuntimeError):
    """Raised when the local forward research store is unavailable or malformed."""


# ---------------------------------------------------------------------------
# Serialization
# ---------------------------------------------------------------------------


def _price_payload(price: Price) -> dict[str, str]:
    return {"amount": str(price.amount), "currency": price.currency.value}


def _price(payload: Any) -> Price:
    return Price(Decimal(payload["amount"]), Currency(payload["currency"]))


def _listing_payload(listing_reference: ListingReference) -> dict[str, str]:
    return {
        "symbol": listing_reference.symbol.value,
        "exchange_code": listing_reference.exchange_code.value,
    }


def _listing(payload: Any) -> ListingReference:
    return ListingReference(Symbol(payload["symbol"]), ExchangeCode(payload["exchange_code"]))


def _evidence_payload(record: ForwardResearchRecord) -> str:
    context = record.result.market_observation_context
    recommendation = record.result.recommendation
    analysis = recommendation.asset_analysis
    payload = {
        "version": _EVIDENCE_VERSION,
        "timeframe": record.timeframe.value,
        "context": {
            "listing_reference": _listing_payload(context.listing_reference),
            "observed_at": context.observed_at.value,
            "latest_price": _price_payload(context.latest_price),
            "previous_close": _price_payload(context.previous_close),
            "latest_volume": str(context.latest_volume.value),
            "daily_high": _price_payload(context.daily_high),
            "daily_low": _price_payload(context.daily_low),
            "recent_closes": [_price_payload(price) for price in context.recent_closes],
            "recent_volumes": [str(volume.value) for volume in context.recent_volumes],
        },
        "analysis": {
            "listing_reference": _listing_payload(analysis.listing_reference),
            "point_in_time": analysis.point_in_time.value,
            "summarized_signals": list(analysis.summarized_signals),
        },
        "recommendation": {
            "action": recommendation.action.value,
            "strategy_identity": recommendation.strategy_identity.identity,
            "point_in_time": recommendation.point_in_time.value,
        },
        "explanation": {
            "reasons": [
                {
                    "rationale": reason.rationale,
                    "supporting_signals": list(reason.supporting_signals),
                }
                for reason in record.result.explanation.reasons
            ]
        },
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _record(evidence: str) -> ForwardResearchRecord:
    payload = json.loads(evidence)
    context_payload = payload["context"]
    analysis_payload = payload["analysis"]
    recommendation_payload = payload["recommendation"]

    context = MarketObservationContext(
        listing_reference=_listing(context_payload["listing_reference"]),
        observed_at=PointInTime(context_payload["observed_at"]),
        latest_price=_price(context_payload["latest_price"]),
        previous_close=_price(context_payload["previous_close"]),
        latest_volume=Quantity(Decimal(context_payload["latest_volume"])),
        daily_high=_price(context_payload["daily_high"]),
        daily_low=_price(context_payload["daily_low"]),
        recent_closes=tuple(_price(item) for item in context_payload["recent_closes"]),
        recent_volumes=tuple(Quantity(Decimal(item)) for item in context_payload["recent_volumes"]),
    )
    analysis = AssetAnalysis(
        listing_reference=_listing(analysis_payload["listing_reference"]),
        point_in_time=PointInTime(analysis_payload["point_in_time"]),
        summarized_signals=tuple(analysis_payload["summarized_signals"]),
    )
    recommendation = Recommendation(
        action=RecommendationAction(recommendation_payload["action"]),
        asset_analysis=analysis,
        strategy_identity=StrategyIdentity(recommendation_payload["strategy_identity"]),
        point_in_time=PointInTime(recommendation_payload["point_in_time"]),
    )
    explanation = RecommendationExplanation(
        recommendation=recommendation,
        reasons=tuple(
            ExplanationReason(
                rationale=reason["rationale"],
                supporting_signals=tuple(reason["supporting_signals"]),
            )
            for reason in payload["explanation"]["reasons"]
        ),
    )
    return ForwardResearchRecord(
        result=AnalyzeAssetResult(
            recommendation=recommendation,
            explanation=explanation,
            market_observation_context=context,
        ),
        timeframe=Timeframe(payload["timeframe"]),
    )


def _compare_records(left: ForwardResearchRecord, right: ForwardResearchRecord) -> int:
    """Order frozen decisions chronologically, then by strategy identity.

    Decision instants are compared with PointInTime.compare() rather than by
    stored text. A canonical instant omits fractional seconds when they are
    zero, so ``...T16:00:00Z`` sorts after ``...T16:00:00.1Z`` lexicographically
    while being the earlier instant.
    """
    instant = left.decision_instant.compare(right.decision_instant)
    if instant:
        return instant
    left_identity = left.strategy_identity.identity
    right_identity = right.strategy_identity.identity
    return (left_identity > right_identity) - (left_identity < right_identity)


def _key_columns(record: ForwardResearchRecord) -> tuple[str, str, str, str, str]:
    return (
        record.listing_reference.symbol.value,
        record.listing_reference.exchange_code.value,
        record.timeframe.value,
        record.decision_instant.value,
        record.strategy_identity.identity,
    )


# ---------------------------------------------------------------------------
# Adapters
# ---------------------------------------------------------------------------


class SQLiteForwardResearchRecordStore(ForwardResearchRecordStore):
    """Freeze forward research decisions into a local SQLite store."""

    def __init__(self, database_path: str | Path) -> None:
        self._database_path = str(database_path)

    def store(self, records: tuple[ForwardResearchRecord, ...]) -> int:
        """Freeze a batch atomically, never overwriting an existing decision."""
        if not records:
            return 0

        prepared: list[tuple[tuple[str, str, str, str, str], str, ForwardResearchRecord]] = []
        seen: set[tuple[str, str, str, str, str]] = set()
        for record in records:
            key = _key_columns(record)
            if key in seen:
                raise ForwardResearchRecordConflictError(
                    "Forward research batch contains two records sharing one natural key."
                )
            seen.add(key)
            prepared.append((key, _evidence_payload(record), record))

        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            for key, evidence, record in prepared:
                existing = connection.execute(
                    """
                    SELECT evidence FROM forward_research_records
                    WHERE symbol = ? AND exchange_code = ? AND timeframe = ?
                      AND decision_instant = ? AND strategy_identity = ?
                    """,
                    key,
                ).fetchone()
                if existing is None:
                    connection.execute(
                        """
                        INSERT INTO forward_research_records (
                            symbol, exchange_code, timeframe,
                            decision_instant, strategy_identity, evidence
                        ) VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (*key, evidence),
                    )
                    continue
                if self._decode(existing[0]) != record:
                    raise ForwardResearchRecordConflictError(
                        "A different forward research record is already frozen under "
                        "this natural key."
                    )
            connection.commit()
        except ForwardResearchRecordConflictError:
            connection.rollback()
            raise
        except sqlite3.Error as exc:
            connection.rollback()
            raise ForwardResearchStorageError(
                "Forward research record storage is unavailable."
            ) from exc
        finally:
            connection.close()

        return len(records)

    def _connect(self) -> sqlite3.Connection:
        try:
            connection = sqlite3.connect(self._database_path, isolation_level=None)
            initialize_forward_research_record_schema(connection)
        except sqlite3.Error as exc:
            raise ForwardResearchStorageError(
                "Forward research record storage is unavailable."
            ) from exc
        return connection

    @staticmethod
    def _decode(evidence: str) -> ForwardResearchRecord:
        try:
            return _record(evidence)
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ForwardResearchStorageError(
                "Forward research record storage contains invalid data."
            ) from exc


class SQLiteForwardResearchRecordRepository(ForwardResearchRecordRepository):
    """Retrieve frozen forward research decisions from a local SQLite store.

    Ordering is applied with PointInTime.compare() semantics rather than by
    stored text, because a canonical instant omits fractional seconds when they
    are zero and is therefore not lexicographically chronological.
    """

    def __init__(self, database_path: str | Path) -> None:
        self._database_path = str(database_path)

    def get_records(self, query: ForwardResearchRecordQuery) -> tuple[ForwardResearchRecord, ...]:
        """Return frozen decisions for one series, oldest to newest."""
        if not isinstance(query, ForwardResearchRecordQuery):
            raise TypeError(
                "SQLiteForwardResearchRecordRepository query must be a ForwardResearchRecordQuery."
            )

        try:
            with sqlite3.connect(self._database_path) as connection:
                initialize_forward_research_record_schema(connection)
                rows = connection.execute(
                    """
                    SELECT evidence FROM forward_research_records
                    WHERE symbol = ? AND exchange_code = ? AND timeframe = ?
                    """,
                    (
                        query.symbol.value,
                        query.exchange_code.value,
                        query.timeframe.value,
                    ),
                ).fetchall()
        except sqlite3.Error as exc:
            raise ForwardResearchStorageError(
                "Forward research record storage is unavailable."
            ) from exc

        try:
            records = [_record(row[0]) for row in rows]
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ForwardResearchStorageError(
                "Forward research record storage contains invalid data."
            ) from exc

        return tuple(sorted(records, key=cmp_to_key(_compare_records)))
