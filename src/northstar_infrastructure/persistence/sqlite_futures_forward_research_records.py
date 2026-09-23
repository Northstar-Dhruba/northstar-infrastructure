"""SQLite-backed futures forward research record store and repository.

A frozen futures decision is immutable evidence. The natural key is stored as
six constrained, queryable columns; the complete decision evidence is stored as
explicit, versioned JSON so a record round-trips exactly. This module issues
INSERT only -- no UPDATE, no REPLACE, no overwriting upsert and no DELETE -- so
later market data can never rewrite what a strategy decided.

Every Decimal-backed value is stored as exact text in the same representation
the futures OHLCV store uses and rebuilt through QuoteValue and Quantity, so a
quotation round-trips digit for digit, sign included, and never passes through
a float. There is no currency: a futures quotation has none.

Temporal handling splits equality from ordering, as in the OHLCV store.
PointInTime canonicalizes to one UTC spelling, so TEXT equality is safe for the
natural key. Ordering is not: ``...T21:00:00.5Z`` sorts before ``...T21:00:00Z``
as text while being the later instant, so records are decoded and ordered in
Python with PointInTime.compare().

The key columns and the evidence describe one record twice. Every read checks
that they agree, and a row whose columns name a different decision than its
evidence is reported as corrupt rather than returned under the wrong key.
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from decimal import Decimal
from functools import cmp_to_key
from pathlib import Path
from typing import Any

from northstar_application.application_services import (
    FuturesAnalysisResult,
    FuturesForwardResearchRecord,
)
from northstar_application.ports import (
    FuturesForwardResearchRecordConflictError,
    FuturesForwardResearchRecordQuery,
    FuturesForwardResearchRecordRepository,
    FuturesForwardResearchRecordStore,
)
from northstar_core.derivatives import ExpirationDate, QuoteValue
from northstar_core.foundation.value_objects import (
    ExchangeCode,
    PointInTime,
    Quantity,
    Symbol,
    Timeframe,
)
from northstar_core.futures import FuturesContract, FuturesProductReference
from northstar_core.strategy import (
    FuturesAssetAnalysis,
    FuturesMarketObservationContext,
    FuturesRecommendation,
    RecommendationAction,
    StrategyIdentity,
)

from northstar_infrastructure.persistence.sqlite_futures_forward_research_schema import (
    initialize_futures_forward_research_record_schema,
)

_EVIDENCE_VERSION = 1

_KEY_COLUMNS = (
    "product_code",
    "exchange_code",
    "expiration_date",
    "timeframe",
    "decision_instant",
    "strategy_identity",
)
_KEY_WHERE = " AND ".join(f"{column} = ?" for column in _KEY_COLUMNS)
_SELECT_BY_KEY = f"SELECT evidence FROM futures_forward_research_records WHERE {_KEY_WHERE}"  # noqa: S608
_INSERT = (
    "INSERT INTO futures_forward_research_records "
    f"({', '.join(_KEY_COLUMNS)}, evidence) VALUES (?, ?, ?, ?, ?, ?, ?)"
)
_SELECT_SERIES = (
    f"SELECT {', '.join(_KEY_COLUMNS)}, evidence FROM futures_forward_research_records "  # noqa: S608
    "WHERE product_code = ? AND exchange_code = ? AND expiration_date = ? AND timeframe = ?"
)

_KeyColumns = tuple[str, str, str, str, str, str]

_DECODE_ERRORS = (KeyError, TypeError, ValueError, ArithmeticError)


class FuturesForwardResearchStorageError(RuntimeError):
    """Raised when the local futures forward research store is unavailable or malformed."""


# ---------------------------------------------------------------------------
# Serialization
# ---------------------------------------------------------------------------


def _key_columns(record: FuturesForwardResearchRecord) -> _KeyColumns:
    """Return one record's natural key as its stored column values."""
    product = record.contract.product
    return (
        product.product_code.value,
        product.exchange_code.value,
        record.contract.expiration_date.value,
        record.timeframe.value,
        record.decision_instant.value,
        record.strategy_identity.identity,
    )


def _decimal_text(value: Decimal) -> str:
    """Exact text of a canonical Decimal, as the futures OHLCV store writes it."""
    return str(value)


def _decimal(value: Any) -> Decimal:
    """Rebuild an exact Decimal from stored text, never through a float."""
    if not isinstance(value, str):
        raise TypeError("Futures forward research evidence numbers must be text.")
    return Decimal(value)


def _quote(value: Any) -> QuoteValue:
    return QuoteValue(_decimal(value))


def _quantity(value: Any) -> Quantity:
    return Quantity(_decimal(value))


def _evidence_payload(record: FuturesForwardResearchRecord) -> str:
    context = record.result.market_observation_context
    recommendation = record.result.recommendation
    analysis = recommendation.asset_analysis
    product = record.contract.product
    payload = {
        "version": _EVIDENCE_VERSION,
        "contract": {
            "product_code": product.product_code.value,
            "exchange_code": product.exchange_code.value,
            "expiration_date": record.contract.expiration_date.value,
        },
        "timeframe": context.timeframe.value,
        "context": {
            "observed_at": context.observed_at.value,
            "latest_quote": _decimal_text(context.latest_quote.value),
            "previous_close": _decimal_text(context.previous_close.value),
            "latest_volume": _decimal_text(context.latest_volume.value),
            "session_high": _decimal_text(context.session_high.value),
            "session_low": _decimal_text(context.session_low.value),
            "recent_closes": [_decimal_text(quote.value) for quote in context.recent_closes],
            "recent_volumes": [_decimal_text(volume.value) for volume in context.recent_volumes],
        },
        "analysis": {
            "point_in_time": analysis.point_in_time.value,
            "summarized_signals": list(analysis.summarized_signals),
        },
        "recommendation": {
            "action": recommendation.action.value,
            "strategy_identity": recommendation.strategy_identity.identity,
            "point_in_time": recommendation.point_in_time.value,
        },
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _validate_version(payload: Any) -> None:
    """Reject evidence this reader cannot faithfully interpret.

    A payload written by another evidence version is never decoded on a
    best-effort basis. Both failures are ValueError or TypeError so they reach
    callers through the storage-error mapping.
    """
    if not isinstance(payload, dict):
        raise TypeError("Futures forward research evidence must be a JSON object.")
    version = payload.get("version")
    if version != _EVIDENCE_VERSION or isinstance(version, bool):
        raise ValueError(f"Unsupported futures forward research evidence version {version!r}.")


def _record(evidence: str) -> FuturesForwardResearchRecord:
    payload = json.loads(evidence)
    _validate_version(payload)
    contract_payload = payload["contract"]
    context_payload = payload["context"]
    analysis_payload = payload["analysis"]
    recommendation_payload = payload["recommendation"]

    contract = FuturesContract(
        product=FuturesProductReference(
            Symbol(contract_payload["product_code"]),
            ExchangeCode(contract_payload["exchange_code"]),
        ),
        expiration_date=ExpirationDate(contract_payload["expiration_date"]),
    )
    context = FuturesMarketObservationContext(
        contract=contract,
        timeframe=Timeframe(payload["timeframe"]),
        observed_at=PointInTime(context_payload["observed_at"]),
        latest_quote=_quote(context_payload["latest_quote"]),
        previous_close=_quote(context_payload["previous_close"]),
        latest_volume=_quantity(context_payload["latest_volume"]),
        session_high=_quote(context_payload["session_high"]),
        session_low=_quote(context_payload["session_low"]),
        recent_closes=tuple(_quote(item) for item in context_payload["recent_closes"]),
        recent_volumes=tuple(_quantity(item) for item in context_payload["recent_volumes"]),
    )
    analysis = FuturesAssetAnalysis(
        contract=contract,
        point_in_time=PointInTime(analysis_payload["point_in_time"]),
        summarized_signals=tuple(analysis_payload["summarized_signals"]),
    )
    recommendation = FuturesRecommendation(
        action=RecommendationAction(recommendation_payload["action"]),
        asset_analysis=analysis,
        strategy_identity=StrategyIdentity(recommendation_payload["strategy_identity"]),
        point_in_time=PointInTime(recommendation_payload["point_in_time"]),
    )
    return FuturesForwardResearchRecord(
        FuturesAnalysisResult(recommendation=recommendation, market_observation_context=context)
    )


def _decode(evidence: object, key: _KeyColumns) -> FuturesForwardResearchRecord:
    """Rebuild a stored record and require it to be the record its key names."""
    if not isinstance(evidence, str):
        raise FuturesForwardResearchStorageError(
            "Futures forward research record storage contains invalid data."
        )
    try:
        record = _record(evidence)
    except _DECODE_ERRORS as exc:
        raise FuturesForwardResearchStorageError(
            "Futures forward research record storage contains invalid data."
        ) from exc
    if _key_columns(record) != key:
        raise FuturesForwardResearchStorageError(
            "Futures forward research record storage holds evidence that does not match "
            "its natural-key columns."
        )
    return record


def _compare_records(
    left: FuturesForwardResearchRecord, right: FuturesForwardResearchRecord
) -> int:
    """Order frozen decisions chronologically, then by strategy identity."""
    instant = left.decision_instant.compare(right.decision_instant)
    if instant:
        return instant
    left_identity = left.strategy_identity.identity
    right_identity = right.strategy_identity.identity
    return (left_identity > right_identity) - (left_identity < right_identity)


# ---------------------------------------------------------------------------
# Adapters
# ---------------------------------------------------------------------------


class SQLiteFuturesForwardResearchRecordStore(FuturesForwardResearchRecordStore):
    """Freeze futures forward research decisions into a local SQLite store, insert only."""

    def __init__(self, database_path: str | Path) -> None:
        self._database_path = str(database_path)

    def store(self, records: tuple[FuturesForwardResearchRecord, ...]) -> int:
        """Freeze a batch atomically, never overwriting an existing decision."""
        if not records:
            return 0

        prepared = self._prepare(records)

        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            for key, evidence, record in prepared:
                self._freeze_one(connection, key, evidence, record)
            connection.commit()
        except (FuturesForwardResearchRecordConflictError, FuturesForwardResearchStorageError):
            connection.rollback()
            raise
        except sqlite3.Error as exc:
            connection.rollback()
            raise FuturesForwardResearchStorageError(
                "Futures forward research record storage is unavailable."
            ) from exc
        finally:
            connection.close()

        return len(records)

    @staticmethod
    def _prepare(
        records: tuple[FuturesForwardResearchRecord, ...],
    ) -> tuple[tuple[_KeyColumns, str, FuturesForwardResearchRecord], ...]:
        """Reject foreign values and in-batch duplicate keys before any write.

        Two records sharing a key conflict even when they are equal: a caller
        that submits one decision twice in a batch has a defect, and an
        idempotent retry is a separate call rather than a duplicated element.
        """
        prepared: list[tuple[_KeyColumns, str, FuturesForwardResearchRecord]] = []
        seen: set[_KeyColumns] = set()
        for record in records:
            if not isinstance(record, FuturesForwardResearchRecord):
                raise TypeError(
                    "SQLiteFuturesForwardResearchRecordStore records must be "
                    "FuturesForwardResearchRecord values."
                )
            key = _key_columns(record)
            if key in seen:
                raise FuturesForwardResearchRecordConflictError(
                    "Futures forward research batch contains two records sharing one natural key."
                )
            seen.add(key)
            prepared.append((key, _evidence_payload(record), record))
        return tuple(prepared)

    def _freeze_one(
        self,
        connection: sqlite3.Connection,
        key: _KeyColumns,
        evidence: str,
        record: FuturesForwardResearchRecord,
    ) -> None:
        existing = self._existing(connection, key)
        if existing is None:
            try:
                connection.execute(_INSERT, (*key, evidence))
                return
            except sqlite3.IntegrityError:
                # The key appeared between the read and the insert. Resolve it
                # exactly as if the read had seen it, rather than reporting a
                # storage failure for what is an ordinary freeze outcome.
                existing = self._existing(connection, key)
                if existing is None:
                    raise
        if _decode(existing, key) != record:
            raise FuturesForwardResearchRecordConflictError(
                "A different futures forward research record is already frozen under "
                "this natural key."
            )

    @staticmethod
    def _existing(connection: sqlite3.Connection, key: _KeyColumns) -> object | None:
        row = connection.execute(_SELECT_BY_KEY, key).fetchone()
        return None if row is None else row[0]

    def _connect(self) -> sqlite3.Connection:
        try:
            connection = sqlite3.connect(self._database_path, isolation_level=None)
            initialize_futures_forward_research_record_schema(connection)
        except sqlite3.Error as exc:
            raise FuturesForwardResearchStorageError(
                "Futures forward research record storage is unavailable."
            ) from exc
        return connection


class SQLiteFuturesForwardResearchRecordRepository(FuturesForwardResearchRecordRepository):
    """Retrieve frozen futures forward research decisions from a local SQLite store.

    SQL filters only on the contract and timeframe columns, whose canonical
    text is safe for equality. Ordering is applied in Python with
    PointInTime.compare(), then strategy identity.
    """

    def __init__(self, database_path: str | Path) -> None:
        self._database_path = str(database_path)

    def get_records(
        self, query: FuturesForwardResearchRecordQuery
    ) -> tuple[FuturesForwardResearchRecord, ...]:
        """Return frozen decisions for one contract and timeframe, oldest to newest."""
        if not isinstance(query, FuturesForwardResearchRecordQuery):
            raise TypeError(
                "SQLiteFuturesForwardResearchRecordRepository query must be a "
                "FuturesForwardResearchRecordQuery."
            )

        product = query.contract.product
        try:
            with closing(sqlite3.connect(self._database_path)) as connection:
                initialize_futures_forward_research_record_schema(connection)
                rows = connection.execute(
                    _SELECT_SERIES,
                    (
                        product.product_code.value,
                        product.exchange_code.value,
                        query.contract.expiration_date.value,
                        query.timeframe.value,
                    ),
                ).fetchall()
        except sqlite3.Error as exc:
            raise FuturesForwardResearchStorageError(
                "Futures forward research record storage is unavailable."
            ) from exc

        records = [_decode(row[6], tuple(str(column) for column in row[:6])) for row in rows]
        return tuple(sorted(records, key=cmp_to_key(_compare_records)))
