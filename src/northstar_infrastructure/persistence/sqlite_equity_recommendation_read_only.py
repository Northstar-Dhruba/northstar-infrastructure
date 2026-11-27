"""Strictly read-only SQLite readers of the equity recommendation authorities (EI-7D).

Read-only implementations, over the EXISTING EI-4D/EI-6A/EI-6B database, of the
Application reader ports the EI-7C query use cases depend on:

- ``SQLiteReadOnlyEquityRecommendationIssuanceReader`` -- one adapter for two
  ports (EI-7 design, section 23.7): ``EquityRecommendationIssuanceReader``
  (EI-6A: ``load_issuance``, ``load_session_issuance``, ``session_view`` and
  ``session_view_at``) and ``EquityRecordedSessionIssuanceReader`` (EI-7C,
  EI7-CF1: ``recorded_session_issuances``, bounded discovery of one session's
  recorded issuances at one watermark);
- ``SQLiteReadOnlyEquityRecommendationLifecycleReader`` --
  ``EquityRecommendationLifecycleReader`` (EI-6C): ``load_lifecycle``;
- ``SQLiteReadOnlyEquityRecommendationGenerationReader`` --
  ``EquityRecommendationGenerationRecordReader`` (EI-4D): ``load_disposition``
  and ``load_attempt`` by exact key.

None of them has, inherits or reaches a write: there is no issuance,
declaration, lifecycle, generation or attempt recording here, and none of the
write-capable adapters is constructed or called. They are not wired into any
operation. The accepted adapters stay the only write authorities, unchanged.

Connections and transactions
----------------------------
Every call opens one fresh connection through
``sqlite_equity_read_only.read_only_transaction`` -- ``mode=ro``,
``PRAGMA query_only`` confirmed, one ``BEGIN`` ... ``ROLLBACK`` read
transaction, no DDL, no schema initialization, never a commit -- so each
answer comes from one consistent database state. Separate calls are separate
transactions: nothing is shared or cached between them.

Verification, reused unchanged
------------------------------
Nothing is decoded or verified by a parallel implementation. Inside the read
transaction the accepted verified decodings run on the read-only connection:

- the EI-6A whole-ledger check ``_consistent_ledger`` before every read that
  establishes a watermark or returns an issuance; the issuance re-derivation
  ``_stored_issuance`` (EI-4D disposition decoding, PROPOSED, evaluator
  ``/2``, ``equity_issued_recommendation``, every column compared); the
  declaration decoding ``_stored_declaration``; and the Application's
  ``equity_issuance_session_view`` for the legacy views;
- the EI-6B parent re-derivation ``_parent``, window derivation ``_window`` and
  lifecycle rehydration ``_stored_lifecycle`` (strict decoding, column checks,
  Core replay), with the parent issuance and its lifecycle read in the SAME
  transaction;
- the EI-4D record decodings ``_stored_disposition`` and ``_stored_attempt``.

Recorded-session discovery (EI7-CF1, EI7-P6, EI7-P7)
---------------------------------------------------
``recorded_session_issuances(exchange_code, trading_date, *, limit,
watermark=None)``:

0. before any connection opens: typed arguments; ``limit`` an integer from 1 to
   the approved maximum of 50; a watermark a non-negative integer, and one
   above 2**63 - 1 refused as EquityIssuanceWatermarkBeyondAuthorityError
   before it can be bound into SQLite (EI7-P7) -- the same precedence the
   EI-7C query applies, which refuses it before calling any reader;

then, inside ONE read transaction:

1. the whole ledger is verified (LEDGER first) and fixes the position N;
2. W is the requested watermark, or N; a W above N is refused with
   EquityIssuanceWatermarkBeyondAuthorityError, never advanced to N;
3. the session's declarations at or below W are read in recording sequence
   and verified;
4. the session's issuances at or below W are counted through the session index
   ``equity_recommendation_issuances_by_session``;
5. above ``limit``, EquityRecordedSessionDiscoveryLimitExceededError is raised
   **before any issuance is re-derived** -- no prefix, no cursor, no partial
   answer. A LEDGER failure always precedes an overflow; an overflow may be
   reported before a corrupt individual issuance row is found (EI7-P6);
6. otherwise every listed issuance is re-derived and verified, in ascending
   recording sequence, and the immutable EquityRecordedSessionIssuances result
   is returned with the authority reference.

An undeclared session is an ordinary result with no declaration; it is never
None. The EI-6A ``session_view`` and ``session_view_at`` semantics, including
their ``(issued_at, issuance_id)`` order and their precedence (the ledger
before any watermark refusal; a watermark is never bound into SQL there), are
reproduced exactly by the read-only issuance reader.

Failures (the existing typed errors)
------------------------------------
- EquityRecommendationIssuanceUnavailableError,
  EquityRecommendationLifecycleStoreUnavailableError,
  EquityRecommendationGenerationStoreUnavailableError: the database is absent,
  cannot be opened read-only, holds no such tables, is locked beyond the busy
  timeout, has a hot journal or is not in rollback-journal mode, or any other
  SQLite error occurred. Retryable; nothing is claimed about the contents.
- InvalidStoredEquityRecommendationIssuanceAuthorityError (LEDGER,
  DECLARATION, ISSUANCE), InvalidStoredEquityRecommendationLifecycleError
  (EVENT, HISTORY, WINDOW) and InvalidEquityRecommendationRecordError (or its
  Incompatible subclass): stored authority does not verify. Permanent, raised
  by the reused decodings unchanged; nothing is repaired or skipped.
- EquityIssuanceWatermarkBeyondAuthorityError and
  EquityRecordedSessionDiscoveryLimitExceededError as above.

Nothing here reads a clock, retries, sleeps or calls out.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from datetime import date
from pathlib import Path

from northstar_application.application_services import (
    EQUITY_RECOMMENDATION_ISSUANCE_AUTHORITY_REFERENCE,
    EquityRecommendationGenerationAttempt,
    EquityRecommendationGenerationDisposition,
    EquityRecordedIssuance,
    equity_issuance_session_view,
)
from northstar_application.ports import (
    EQUITY_RECORDED_SESSION_DISCOVERY_LIMIT,
    EQUITY_RECORDING_POSITION_MAXIMUM,
    EquityIssuanceWatermarkBeyondAuthorityError,
    EquityRecommendationGenerationRecordReader,
    EquityRecommendationGenerationStoreUnavailableError,
    EquityRecommendationIssuanceReader,
    EquityRecommendationIssuanceUnavailableError,
    EquityRecommendationLifecycleReader,
    EquityRecommendationLifecycleStoreUnavailableError,
    EquityRecordedLifecycle,
    EquityRecordedSessionDiscoveryLimitExceededError,
    EquityRecordedSessionIssuanceReader,
    EquityRecordedSessionIssuances,
    EquitySessionRecommendationHistory,
)
from northstar_core.domain.value_objects import ListingReference
from northstar_core.foundation.value_objects import ExchangeCode
from northstar_core.strategy import StrategyIdentity

from northstar_infrastructure.persistence.sqlite_equity_read_only import (
    read_only_transaction,
    sqlite_failure_name,
)
from northstar_infrastructure.persistence.sqlite_equity_recommendation_generation import (
    _SELECT_ATTEMPT,
    _SELECT_DISPOSITION,
    _stored_attempt,  # the EI-4D stored-record decodings, reused unchanged
    _stored_disposition,
)
from northstar_infrastructure.persistence.sqlite_equity_recommendation_issuance import (
    _SELECT_DECLARATIONS,
    _SELECT_ISSUANCES,
    _consistent_ledger,  # the EI-6A whole-ledger check, reused unchanged
    _require_session,
    _session_declarations,
    _session_issuances,
    _stored_declaration,  # the EI-6A declaration decoding, reused unchanged
    _stored_issuance,  # the EI-6A issuance re-derivation, reused unchanged
)
from northstar_infrastructure.persistence.sqlite_equity_recommendation_issuance_schema import (
    ISSUANCES,
)
from northstar_infrastructure.persistence.sqlite_equity_recommendation_lifecycle import (
    _parent,  # the EI-6B parent re-derivation, window and rehydration, reused unchanged
    _stored_lifecycle,
    _window,
)

_AT_OR_BELOW = (
    " WHERE exchange_code = ? AND trading_date = ? AND recording_sequence <= ?"
    " ORDER BY recording_sequence"
)
_SESSION_DECLARATIONS_AT = _SELECT_DECLARATIONS + _AT_OR_BELOW
_SESSION_ISSUANCES_AT = _SELECT_ISSUANCES + _AT_OR_BELOW
_SESSION_COUNT_AT = (
    f"SELECT COUNT(*) FROM {ISSUANCES} "  # noqa: S608
    "WHERE exchange_code = ? AND trading_date = ? AND recording_sequence <= ?"
)


def _path(database_path: object) -> Path:
    if not isinstance(database_path, str | Path):
        raise TypeError("database_path must be a str or Path.")
    return Path(database_path)


def _issuance_unavailable(exc: sqlite3.Error) -> EquityRecommendationIssuanceUnavailableError:
    return EquityRecommendationIssuanceUnavailableError(
        f"The issuance authority is unavailable; nothing was read ({sqlite_failure_name(exc)})."
    )


def _lifecycle_unavailable(
    exc: sqlite3.Error,
) -> EquityRecommendationLifecycleStoreUnavailableError:
    return EquityRecommendationLifecycleStoreUnavailableError(
        f"The lifecycle store is unavailable; nothing was read ({sqlite_failure_name(exc)})."
    )


def _generation_unavailable(
    exc: sqlite3.Error,
) -> EquityRecommendationGenerationStoreUnavailableError:
    return EquityRecommendationGenerationStoreUnavailableError(
        f"Generation storage is unavailable; nothing was read ({sqlite_failure_name(exc)})."
    )


def _require_watermark(watermark: object) -> int:
    """The EI-6A watermark validation: a non-negative integer (never bound into SQL there)."""
    if isinstance(watermark, bool) or not isinstance(watermark, int):
        raise TypeError("watermark must be an integer.")
    if watermark < 0:
        raise ValueError("watermark must be a non-negative integer.")
    return watermark


def _require_bindable_watermark(watermark: object) -> int:
    """EI7-P7: above 2**63 - 1 is ahead of any authority, refused before any SQL binding."""
    selected = _require_watermark(watermark)
    if selected > EQUITY_RECORDING_POSITION_MAXIMUM:
        raise EquityIssuanceWatermarkBeyondAuthorityError(
            "The watermark is beyond any recording position the authority can hold."
        )
    return selected


def _require_limit(limit: object) -> int:
    if isinstance(limit, bool) or not isinstance(limit, int):
        raise TypeError("limit must be an integer.")
    if not 1 <= limit <= EQUITY_RECORDED_SESSION_DISCOVERY_LIMIT:
        raise ValueError(
            f"limit must be from 1 to the approved maximum of "
            f"{EQUITY_RECORDED_SESSION_DISCOVERY_LIMIT}."
        )
    return limit


def _beyond(watermark: int, position: int) -> EquityIssuanceWatermarkBeyondAuthorityError:
    return EquityIssuanceWatermarkBeyondAuthorityError(
        f"Watermark {watermark} is beyond the authority's position {position}."
    )


def _legacy_view(
    connection: sqlite3.Connection,
    exchange_code: ExchangeCode,
    trading_date: date,
    watermark: int,
    exchange: str,
    day: str,
) -> EquitySessionRecommendationHistory | None:
    """The EI-6A view, exactly as the accepted adapter builds it, on this connection."""
    declarations = _session_declarations(connection, exchange, day)
    issuances = _session_issuances(connection, exchange, day)
    return equity_issuance_session_view(
        exchange_code,
        trading_date,
        watermark,
        [recorded.history_record() for recorded in issuances],
        declarations,
    )


class _Reader:
    """One database path; every call is its own read-only transaction. Opens nothing here."""

    def __init__(self, database_path: str | Path) -> None:
        self._database_path = _path(database_path)

    def _read[T](
        self,
        work: Callable[[sqlite3.Connection], T],
        unavailable: Callable[[sqlite3.Error], Exception],
    ) -> T:
        return read_only_transaction(self._database_path, work, unavailable)


class SQLiteReadOnlyEquityRecommendationIssuanceReader(
    _Reader, EquityRecommendationIssuanceReader, EquityRecordedSessionIssuanceReader
):
    """The EI-6A issuance reads and EI-7 bounded discovery, strictly read-only (design 23.7).

    One adapter implements the existing issuance reader and the new
    recorded-session port; no write exists. The EI-6C monitor is never given
    it (EI6C-P7): it stays composed over the accepted authority.
    """

    def _read_issuance_authority[T](self, work: Callable[[sqlite3.Connection], T]) -> T:
        return self._read(work, _issuance_unavailable)

    def load_issuance(self, issuance_id: str) -> EquityRecordedIssuance | None:
        """Return the recorded issuance with ``issuance_id``, re-derived, or None."""
        if not isinstance(issuance_id, str):
            raise TypeError("issuance_id must be text.")

        def work(connection: sqlite3.Connection) -> EquityRecordedIssuance | None:
            _consistent_ledger(connection)
            row = connection.execute(
                _SELECT_ISSUANCES + " WHERE issuance_id = ?", (issuance_id,)
            ).fetchone()
            return None if row is None else _stored_issuance(connection, row)

        return self._read_issuance_authority(work)

    def load_session_issuance(
        self, strategy: StrategyIdentity, listing: ListingReference, trading_date: date
    ) -> EquityRecordedIssuance | None:
        """Return the recorded issuance for the strategy, listing and date, or None."""
        if not isinstance(strategy, StrategyIdentity):
            raise TypeError("strategy must be a StrategyIdentity.")
        if not isinstance(listing, ListingReference):
            raise TypeError("listing must be a ListingReference.")
        exchange, day = _require_session(listing.exchange_code, trading_date)
        key = (strategy.identity, listing.symbol.value, exchange, day)

        def work(connection: sqlite3.Connection) -> EquityRecordedIssuance | None:
            _consistent_ledger(connection)
            row = connection.execute(
                _SELECT_ISSUANCES + " WHERE strategy = ? AND symbol = ? AND exchange_code = ? "
                "AND trading_date = ?",
                key,
            ).fetchone()
            return None if row is None else _stored_issuance(connection, row)

        return self._read_issuance_authority(work)

    def session_view(
        self, exchange_code: ExchangeCode, trading_date: date
    ) -> EquitySessionRecommendationHistory | None:
        """Return the session's EI-6A view at the current position, or None when undeclared."""
        exchange, day = _require_session(exchange_code, trading_date)

        def work(connection: sqlite3.Connection) -> EquitySessionRecommendationHistory | None:
            position = _consistent_ledger(connection)
            return _legacy_view(connection, exchange_code, trading_date, position, exchange, day)

        return self._read_issuance_authority(work)

    def session_view_at(
        self, exchange_code: ExchangeCode, trading_date: date, watermark: int
    ) -> EquitySessionRecommendationHistory | None:
        """Return the session's EI-6A view at ``watermark``; refused beyond the position."""
        exchange, day = _require_session(exchange_code, trading_date)
        selected = _require_watermark(watermark)

        def work(connection: sqlite3.Connection) -> EquitySessionRecommendationHistory | None:
            position = _consistent_ledger(connection)
            if selected > position:
                raise _beyond(selected, position)
            return _legacy_view(connection, exchange_code, trading_date, selected, exchange, day)

        return self._read_issuance_authority(work)

    def recorded_session_issuances(
        self,
        exchange_code: ExchangeCode,
        trading_date: date,
        *,
        limit: int,
        watermark: int | None = None,
    ) -> EquityRecordedSessionIssuances:
        """Return the session's verified recorded issuances and declarations at W; see module."""
        exchange, day = _require_session(exchange_code, trading_date)
        bound = _require_limit(limit)
        requested = None if watermark is None else _require_bindable_watermark(watermark)

        def work(connection: sqlite3.Connection) -> EquityRecordedSessionIssuances:
            position = _consistent_ledger(connection)  # 1: LEDGER first
            selected = position if requested is None else requested  # 2: W, never advanced
            if selected > position:
                raise _beyond(selected, position)
            scope = (exchange, day, selected)
            declarations = tuple(  # 3
                _stored_declaration(row)
                for row in connection.execute(_SESSION_DECLARATIONS_AT, scope)
            )
            (count,) = connection.execute(_SESSION_COUNT_AT, scope).fetchone()  # 4
            if count > bound:  # 5: before any re-derivation
                raise EquityRecordedSessionDiscoveryLimitExceededError(bound)
            rows = connection.execute(_SESSION_ISSUANCES_AT, scope).fetchall()
            issuances = tuple(_stored_issuance(connection, row) for row in rows)  # 6
            return EquityRecordedSessionIssuances(
                exchange_code=exchange_code,
                trading_date=trading_date,
                recording_position=position,
                recording_watermark=selected,
                declarations=declarations,
                issuances=issuances,
                reference=EQUITY_RECOMMENDATION_ISSUANCE_AUTHORITY_REFERENCE,
            )

        return self._read_issuance_authority(work)


class SQLiteReadOnlyEquityRecommendationLifecycleReader(
    _Reader, EquityRecommendationLifecycleReader
):
    """The EI-6B lifecycle read over a strictly read-only connection; no append exists."""

    def load_lifecycle(self, issuance_id: str) -> EquityRecordedLifecycle | None:
        """Return the verified, replayed lifecycle of ``issuance_id``, or None when unknown."""
        if not isinstance(issuance_id, str):
            raise TypeError("issuance_id must be text.")

        def work(connection: sqlite3.Connection) -> EquityRecordedLifecycle | None:
            parent = _parent(connection, issuance_id)  # ledger + issuance, same transaction
            if parent is None:
                return None
            recorded, disposition = parent
            return _stored_lifecycle(connection, recorded, _window(recorded, disposition))

        return self._read(work, _lifecycle_unavailable)


class SQLiteReadOnlyEquityRecommendationGenerationReader(
    _Reader, EquityRecommendationGenerationRecordReader
):
    """The EI-4D exact-key record reads over a strictly read-only connection; no write exists."""

    def load_disposition(
        self, generation_key: str
    ) -> EquityRecommendationGenerationDisposition | None:
        """Return the committed disposition for ``generation_key``, decoded, or None."""
        if not isinstance(generation_key, str):
            raise TypeError("generation_key must be text.")

        def work(
            connection: sqlite3.Connection,
        ) -> EquityRecommendationGenerationDisposition | None:
            row = connection.execute(_SELECT_DISPOSITION, (generation_key,)).fetchone()
            return None if row is None else _stored_disposition(row)

        return self._read(work, _generation_unavailable)

    def load_attempt(self, attempt_identity: str) -> EquityRecommendationGenerationAttempt | None:
        """Return the committed attempt with ``attempt_identity``, decoded, or None."""
        if not isinstance(attempt_identity, str):
            raise TypeError("attempt_identity must be text.")

        def work(connection: sqlite3.Connection) -> EquityRecommendationGenerationAttempt | None:
            row = connection.execute(_SELECT_ATTEMPT, (attempt_identity,)).fetchone()
            return None if row is None else _stored_attempt(row)

        return self._read(work, _generation_unavailable)
