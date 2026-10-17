"""SQLite-backed futures trade recommendation stores, audit stores and repositories.

Status: an internal persistence capability, not an enabled recording workflow.
Nothing here is wired into operations.

Recording, with its generation audit (I-2)
------------------------------------------
``record(recommendations, policy=..., profile=..., audit=...)`` first runs the
shared Application validator, which re-derives every identity from its source
under the profile and requires ``audit`` to be the RECORDED audit naming
exactly the batch. Then, in ONE ``BEGIN IMMEDIATE`` transaction:

1. the configuration is registered, first write wins. A profile version already
   registered with other contents, or a policy version registered with other
   parameters under any profile, raises the configuration conflict;
2. if the batch's identities are already stored: when every definition is
   identical and the same audit is already stored with equal content, this is
   the same committed recording replayed -- an idempotent no-op. Otherwise
   it is a FuturesTradeRecommendationConflictError and nothing is written:
   another recording owns those identities, and this audit must not claim them;
3. a new recommendation is refused with FuturesTradeRecommendationExclusivityError
   when its product -- product code and exchange, every expiry -- already has
   another open recommendation, or an earlier one in the same batch. "Open" is
   the F-2 status of the other recommendation's lifecycle, replayed from its
   stored steps inside this transaction;
4. the definitions are inserted; then the audit, whose identity must not be
   stored already (FuturesRecommendationGenerationAuditConflictError), with a
   RECORDED reference to each definition just inserted;
5. everything commits, or everything rolls back.

So an audit claims as recorded exactly the definitions its transaction
inserted, and no definition is committed without its audit.

Standalone audits
-----------------
``SQLiteFuturesRecommendationGenerationAuditStore.record_audit(audit)`` records
an EXISTING_RETURNED, REJECTED or BLOCKED audit in its own transaction, without
registering a configuration: an equal audit already stored is a no-op, a
different one under the same identity is an audit conflict, and every identity
named as existing must be recorded for the audited attempt. A RECORDED audit is
refused there. Audits are encoded and verified by
sqlite_futures_recommendation_generation_audits.

``BEGIN IMMEDIATE`` takes the database's write lock before anything is read, so
concurrent writers are serialised by SQLite itself, whatever lock the caller
holds. A writer waits at most five seconds for that lock; past it the write
fails as a storage error and nothing is written. The journal mode is left as
it is. Foreign keys are enforced on this module's own write connections.

Lifecycle progress
------------------
``record_progress(previous, current, steps=...)`` first runs the shared
Application validator, which replays the steps from ``previous`` through Core
and requires exactly ``current``. Then, in ONE ``BEGIN IMMEDIATE`` transaction:

1. the definition is read and fully validated, configuration seal included,
   and must be the definition ``previous`` names;
2. the stored lifecycle is rebuilt by replaying every stored step
   (sqlite_futures_trade_recommendation_steps);
3. stored lifecycle equal to ``previous``: each observation step's candle must
   already be in ``futures_ohlcv`` under the bar's own contract, timeframe and
   instant, with equal values and an equal ``fdc1`` fingerprint, or
   FuturesTradeRecommendationLifecycleEvidenceError is raised; the steps are
   then inserted, numbered on from the last stored one;
4. otherwise, if exactly these steps were the last ones recorded after
   ``previous`` -- the stored lifecycle is ``current`` -- it is an idempotent
   no-op returning 0; anything else is a FuturesTradeRecommendationLifecycleConflictError.

A product is released only by a terminal step: the definition stays as it
was, its steps stay as they were, and the next recording on that product sees
the terminal F-2 status. Nothing infers terminality from validity, an expiry
date or the clock, and a terminal lifecycle accepts no further step.

Evidence
--------
The identity inputs are constrained columns; the definition is versioned
canonical JSON (sorted keys, no whitespace, decimals as fixed-point text,
instants as canonical UTC text). The definition also seals the exact
configuration row it was recorded under with a SHA-256 digest, so a registered
configuration that is changed afterwards -- a horizon, a policy parameter --
invalidates every definition that relied on it. Every read rebuilds the F-2
recommendation, re-serializes it, and requires the stored text, the columns,
the configuration seal and the re-derived identity to agree. A lifecycle read
replays every step against its candle in ``futures_ohlcv`` and requires every
stored fact to be what F-2 produces. A row that fails any check is corrupt and
raises; nothing is repaired. No OHLCV bar is copied into recommendation storage,
and nothing here calls a market-data provider. The module issues INSERT only.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Callable
from contextlib import closing
from decimal import Decimal
from pathlib import Path
from typing import Any

from northstar_application.application_services import (
    FuturesRecommendationGenerationAudit,
    FuturesTradeRecommendationGenerationProfile,
    FuturesTradeRecommendationIdentityService,
)
from northstar_application.ports import (
    FuturesRecommendationGenerationAuditConflictError,
    FuturesRecommendationGenerationAuditRepository,
    FuturesRecommendationGenerationAuditStore,
    FuturesTradeRecommendationConfigurationConflictError,
    FuturesTradeRecommendationConflictError,
    FuturesTradeRecommendationExclusivityError,
    FuturesTradeRecommendationLifecycleConflictError,
    FuturesTradeRecommendationLifecycleEvidenceError,
    FuturesTradeRecommendationLifecycleStep,
    FuturesTradeRecommendationLifecycleStore,
    FuturesTradeRecommendationObservationStep,
    FuturesTradeRecommendationQuery,
    FuturesTradeRecommendationRepository,
    FuturesTradeRecommendationStore,
    InvalidFuturesTradeRecommendationLifecycleProgressError,
    InvalidFuturesTradeRecommendationRecordingError,
    apply_futures_trade_recommendation_lifecycle_step,
    validate_futures_trade_recommendation_lifecycle_progress,
    validate_futures_trade_recommendation_recording,
    validate_standalone_futures_recommendation_generation_audit,
)
from northstar_core.derivatives import ExpirationDate, QuoteValue
from northstar_core.foundation.value_objects import (
    ExchangeCode,
    PointInTime,
    Symbol,
    Timeframe,
)
from northstar_core.futures import FuturesContract, FuturesProductReference
from northstar_core.recommendations.futures import (
    EntryZone,
    FuturesRecommendationGenerationPolicy,
    FuturesResearchDecisionReference,
    FuturesTradeRecommendation,
    FuturesTradeRecommendationIdentity,
    FuturesTradeRecommendationLifecycle,
    PriceTarget,
    RecommendationPolicyVersion,
    RecommendationValidity,
    StopLoss,
    TradeDirection,
)
from northstar_core.strategy import ExplanationReason, StrategyIdentity

from northstar_infrastructure.persistence.sqlite_futures_recommendation_generation_audits import (  # noqa: E501
    insert_audit,
    require_existing_references,
    stored_audit,
)
from northstar_infrastructure.persistence.sqlite_futures_trade_recommendation_schema import (
    initialize_futures_trade_recommendation_schema,
)
from northstar_infrastructure.persistence.sqlite_futures_trade_recommendation_steps import (
    STEP_COLUMNS,
    StepRow,
    candle_fingerprint,
    load_candle,
    reconstruct_lifecycle,
    step_row,
)

_EVIDENCE_VERSION = 1
_BUSY_TIMEOUT_SECONDS = 5.0

_DEFINITION_COLUMNS = (
    "recommendation_identity",
    "product_code",
    "exchange_code",
    "expiration_date",
    "timeframe",
    "decision_instant",
    "strategy_identity",
    "policy_version",
    "profile_version",
    "issued_at",
    "definition",
)
_CONFIGURATION_COLUMNS = (
    "profile_version",
    "policy_version",
    "validity_sessions",
    "policy_parameters",
)
_JOINED = (
    f"SELECT {', '.join(f'r.{c}' for c in _DEFINITION_COLUMNS)}, "  # noqa: S608
    f"{', '.join(f'c.{c}' for c in _CONFIGURATION_COLUMNS)} "
    "FROM futures_trade_recommendations AS r "
    "LEFT JOIN futures_recommendation_configurations AS c "
    "ON c.profile_version = r.profile_version"
)
_SELECT_BY_IDENTITY = f"{_JOINED} WHERE r.recommendation_identity = ?"
_SELECT_BY_PRODUCT = f"{_JOINED} WHERE r.product_code = ? AND r.exchange_code = ?"
_SELECT_DEFINITION = (
    f"SELECT {', '.join(_DEFINITION_COLUMNS)} FROM futures_trade_recommendations "  # noqa: S608
    "WHERE recommendation_identity = ?"
)
_SELECT_OTHERS_ON_PRODUCT = f"{_SELECT_BY_PRODUCT} AND r.recommendation_identity <> ?"
_SELECT_PROFILE = (
    f"SELECT {', '.join(_CONFIGURATION_COLUMNS)} "  # noqa: S608
    "FROM futures_recommendation_configurations WHERE profile_version = ?"
)
_SELECT_POLICY_PARAMETERS = (
    "SELECT DISTINCT policy_parameters FROM futures_recommendation_configurations "
    "WHERE policy_version = ?"
)
_INSERT_CONFIGURATION = (
    "INSERT INTO futures_recommendation_configurations "
    f"({', '.join(_CONFIGURATION_COLUMNS)}) VALUES (?, ?, ?, ?)"
)
_INSERT_DEFINITION = (
    "INSERT INTO futures_trade_recommendations "
    f"({', '.join(_DEFINITION_COLUMNS)}) VALUES ({', '.join('?' * len(_DEFINITION_COLUMNS))})"
)
_INSERT_STEP = (
    "INSERT INTO futures_trade_recommendation_steps "
    f"({', '.join(STEP_COLUMNS)}) VALUES ({', '.join('?' * len(STEP_COLUMNS))})"
)

_DefinitionRow = tuple[str, ...]
_ConfigurationRow = tuple[str, str, str, str]

_DECODE_ERRORS = (KeyError, TypeError, ValueError, ArithmeticError)
_SESSIONS_TEXT = frozenset("0123456789")


class FuturesTradeRecommendationStorageError(RuntimeError):
    """Raised when the recommendation store is unavailable or holds invalid data."""


# ---------------------------------------------------------------------------
# Canonical encoding
# ---------------------------------------------------------------------------


def _canonical_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _decimal_text(value: Decimal) -> str:
    """Return a Decimal's canonical fixed-point text without consulting any context.

    Trailing fractional zeros are dropped and zero has one spelling, so equal
    values always encode identically: 1.5 and 1.50 are one configuration.
    """
    sign, digits, exponent = value.as_tuple()
    coefficient = list(digits)
    while exponent < 0 and len(coefficient) > 1 and coefficient[-1] == 0:
        coefficient.pop()
        exponent += 1
    if not any(coefficient):
        return "0"
    return format(Decimal((sign, tuple(coefficient), exponent)), "f")


def _decimal(value: Any) -> Decimal:
    if not isinstance(value, str):
        raise TypeError("Stored decimals must be text.")
    return Decimal(value)


def _text(value: Any) -> str:
    if not isinstance(value, str):
        raise TypeError("Stored values must be text.")
    return value


def _policy_parameters(policy: FuturesRecommendationGenerationPolicy) -> str:
    """Return every F-4 policy parameter as canonical JSON."""
    return _canonical_json(
        {
            "version": _EVIDENCE_VERSION,
            "policy_version": policy.version.version,
            "approved_strategies": [strategy.identity for strategy in policy.approved_strategies],
            "lookback_changes": policy.lookback_changes,
            "entry_multiplier": _decimal_text(policy.entry_multiplier),
            "stop_multiplier": _decimal_text(policy.stop_multiplier),
            "target_multiplier": _decimal_text(policy.target_multiplier),
            "price_increment": _decimal_text(policy.price_increment),
            "min_reward_to_risk": _decimal_text(policy.min_reward_to_risk),
            "short_enabled": policy.short_enabled,
        }
    )


def _policy(parameters: str) -> FuturesRecommendationGenerationPolicy:
    payload = json.loads(parameters)
    _require_version(payload)
    if not isinstance(payload["lookback_changes"], int) or not isinstance(
        payload["short_enabled"], bool
    ):
        raise TypeError("Stored policy parameters have the wrong JSON types.")
    return FuturesRecommendationGenerationPolicy(
        version=RecommendationPolicyVersion(_text(payload["policy_version"])),
        approved_strategies=tuple(
            StrategyIdentity(_text(item)) for item in payload["approved_strategies"]
        ),
        lookback_changes=payload["lookback_changes"],
        entry_multiplier=_decimal(payload["entry_multiplier"]),
        stop_multiplier=_decimal(payload["stop_multiplier"]),
        target_multiplier=_decimal(payload["target_multiplier"]),
        price_increment=_decimal(payload["price_increment"]),
        min_reward_to_risk=_decimal(payload["min_reward_to_risk"]),
        short_enabled=payload["short_enabled"],
    )


def _configuration_row(
    policy: FuturesRecommendationGenerationPolicy,
    profile: FuturesTradeRecommendationGenerationProfile,
) -> _ConfigurationRow:
    return (
        profile.version,
        policy.version.version,
        str(profile.validity_sessions),
        _policy_parameters(policy),
    )


def _require_version(payload: Any) -> None:
    if not isinstance(payload, dict):
        raise TypeError("Stored evidence must be a JSON object.")
    version = payload.get("version")
    if version != _EVIDENCE_VERSION or isinstance(version, bool):
        raise ValueError(f"Unsupported futures trade recommendation evidence version {version!r}.")


def _configuration_digest(configuration: tuple[object, ...]) -> str:
    """Seal one configuration row: SHA-256 of its four columns as canonical JSON."""
    encoded = json.dumps([_text(value) for value in configuration], separators=(",", ":"))
    return "sha256:" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _definition(recommendation: FuturesTradeRecommendation, configuration_digest: str) -> str:
    """Return one F-2 recommendation, sealed to its configuration, as canonical JSON."""
    product = recommendation.contract.product
    source = recommendation.source
    validity = recommendation.validity
    return _canonical_json(
        {
            "version": _EVIDENCE_VERSION,
            "configuration_digest": configuration_digest,
            "identity": recommendation.identity.identity,
            "contract": {
                "product_code": product.product_code.value,
                "exchange_code": product.exchange_code.value,
                "expiration_date": recommendation.contract.expiration_date.value,
            },
            "direction": recommendation.direction.value,
            "entry": {
                "low": _decimal_text(recommendation.entry.low.value),
                "high": _decimal_text(recommendation.entry.high.value),
            },
            "stop": _decimal_text(recommendation.stop.price.value),
            "target": _decimal_text(recommendation.target.price.value),
            "validity": {
                "eligible_from": validity.eligible_from.value,
                "valid_through": validity.valid_through.value,
                "contract_cutoff": validity.contract_cutoff.value,
            },
            "source": {
                "timeframe": source.timeframe.value,
                "decision_instant": source.decision_instant.value,
                "strategy_identity": source.strategy_identity.identity,
            },
            "policy_version": recommendation.policy_version.version,
            "issued_at": recommendation.issued_at.value,
            "thesis": [
                {
                    "rationale": reason.rationale,
                    "supporting_signals": list(reason.supporting_signals),
                }
                for reason in recommendation.thesis
            ],
        }
    )


def _recommendation(definition: str) -> FuturesTradeRecommendation:
    payload = json.loads(definition)
    _require_version(payload)
    contract_payload = payload["contract"]
    contract = FuturesContract(
        FuturesProductReference(
            Symbol(_text(contract_payload["product_code"])),
            ExchangeCode(_text(contract_payload["exchange_code"])),
        ),
        ExpirationDate(_text(contract_payload["expiration_date"])),
    )
    source = payload["source"]
    validity = payload["validity"]
    return FuturesTradeRecommendation(
        identity=FuturesTradeRecommendationIdentity(_text(payload["identity"])),
        contract=contract,
        direction=TradeDirection(_text(payload["direction"])),
        entry=EntryZone(
            QuoteValue(_decimal(payload["entry"]["low"])),
            QuoteValue(_decimal(payload["entry"]["high"])),
        ),
        stop=StopLoss(QuoteValue(_decimal(payload["stop"]))),
        target=PriceTarget(QuoteValue(_decimal(payload["target"]))),
        validity=RecommendationValidity(
            PointInTime(_text(validity["eligible_from"])),
            PointInTime(_text(validity["valid_through"])),
            PointInTime(_text(validity["contract_cutoff"])),
        ),
        source=FuturesResearchDecisionReference(
            contract,
            Timeframe(_text(source["timeframe"])),
            PointInTime(_text(source["decision_instant"])),
            StrategyIdentity(_text(source["strategy_identity"])),
        ),
        policy_version=RecommendationPolicyVersion(_text(payload["policy_version"])),
        issued_at=PointInTime(_text(payload["issued_at"])),
        thesis=tuple(
            ExplanationReason(
                _text(reason["rationale"]),
                tuple(_text(signal) for signal in reason["supporting_signals"]),
            )
            for reason in payload["thesis"]
        ),
    )


def _definition_row(
    recommendation: FuturesTradeRecommendation, profile_version: str, configuration_digest: str
) -> _DefinitionRow:
    product = recommendation.contract.product
    source = recommendation.source
    return (
        recommendation.identity.identity,
        product.product_code.value,
        product.exchange_code.value,
        recommendation.contract.expiration_date.value,
        source.timeframe.value,
        source.decision_instant.value,
        source.strategy_identity.identity,
        recommendation.policy_version.version,
        profile_version,
        recommendation.issued_at.value,
        _definition(recommendation, configuration_digest),
    )


def _profile(row: tuple[object, ...]) -> FuturesTradeRecommendationGenerationProfile:
    profile_version, policy_version, sessions, parameters = (_text(value) for value in row)
    if not sessions or not set(sessions) <= _SESSIONS_TEXT or sessions.startswith("0"):
        raise ValueError("Stored validity sessions must be a positive integer.")
    policy = _policy(parameters)
    if policy.version.version != policy_version or _policy_parameters(policy) != parameters:
        raise ValueError("Stored policy parameters are not the canonical parameters of their row.")
    return FuturesTradeRecommendationGenerationProfile(
        profile_version, int(sessions), RecommendationPolicyVersion(policy_version)
    )


def _decode(row: tuple[object, ...]) -> FuturesTradeRecommendation:
    """Rebuild a stored recommendation and require every stored fact to agree."""
    definition_row = row[: len(_DEFINITION_COLUMNS)]
    configuration_row = row[len(_DEFINITION_COLUMNS) :]
    if configuration_row[0] is None:
        raise FuturesTradeRecommendationStorageError(
            f"Recommendation {definition_row[0]!r} has no registered configuration."
        )
    try:
        recommendation = _recommendation(_text(definition_row[-1]))
        profile = _profile(configuration_row)
    except _DECODE_ERRORS as exc:
        raise FuturesTradeRecommendationStorageError(
            "Futures trade recommendation storage contains invalid data."
        ) from exc
    digest = _configuration_digest(configuration_row)
    if _definition_row(recommendation, profile.version, digest) != definition_row:
        raise FuturesTradeRecommendationStorageError(
            f"Recommendation {definition_row[0]!r} is stored non-canonically, its columns "
            "disagree with its definition, or its registered configuration has changed."
        )
    if (
        recommendation.policy_version != profile.policy_version
        or FuturesTradeRecommendationIdentityService().identity(recommendation.source, profile)
        != recommendation.identity
    ):
        raise FuturesTradeRecommendationStorageError(
            f"Recommendation {definition_row[0]!r} is not the identity its source and registered "
            "configuration derive."
        )
    return recommendation


def _connect(database_path: str) -> sqlite3.Connection:
    try:
        connection = sqlite3.connect(
            database_path, timeout=_BUSY_TIMEOUT_SECONDS, isolation_level=None
        )
        connection.execute("PRAGMA foreign_keys = ON")
        initialize_futures_trade_recommendation_schema(connection)
    except sqlite3.Error as exc:
        raise FuturesTradeRecommendationStorageError(
            "Futures trade recommendation storage is unavailable."
        ) from exc
    return connection


def _lifecycle(
    connection: sqlite3.Connection, row: tuple[object, ...]
) -> tuple[
    FuturesTradeRecommendationLifecycle, tuple[FuturesTradeRecommendationLifecycleStep, ...]
]:
    """Rebuild a stored recommendation's lifecycle; require every stored fact to agree."""
    recommendation = _decode(row)
    try:
        return reconstruct_lifecycle(connection, recommendation)
    except _DECODE_ERRORS as exc:
        raise FuturesTradeRecommendationStorageError(
            f"Recommendation {recommendation.identity} has invalid lifecycle evidence: {exc}"
        ) from exc


def _stored_audit(
    connection: sqlite3.Connection, identity: str
) -> FuturesRecommendationGenerationAudit | None:
    """Return the stored audit, requiring every stored fact to agree."""
    try:
        return stored_audit(connection, identity)
    except _DECODE_ERRORS as exc:
        raise FuturesTradeRecommendationStorageError(
            f"Generation audit {identity!r} has invalid evidence: {exc}"
        ) from exc


def _write[T](database_path: str, work: Callable[[sqlite3.Connection], T]) -> T:
    """Run ``work`` in one BEGIN IMMEDIATE transaction; commit it, or roll all of it back."""
    connection = _connect(database_path)
    try:
        connection.execute("BEGIN IMMEDIATE")
        outcome = work(connection)
        connection.commit()
        return outcome
    except sqlite3.Error as exc:
        if connection.in_transaction:
            connection.rollback()
        raise FuturesTradeRecommendationStorageError(
            "Futures trade recommendation storage is unavailable."
        ) from exc
    except BaseException:
        if connection.in_transaction:
            connection.rollback()
        raise
    finally:
        connection.close()


def _read[T](database_path: str, work: Callable[[sqlite3.Connection], T]) -> T:
    """Run ``work`` in one read transaction, so every read sees one database state."""
    try:
        with closing(
            sqlite3.connect(database_path, timeout=_BUSY_TIMEOUT_SECONDS, isolation_level=None)
        ) as connection:
            initialize_futures_trade_recommendation_schema(connection)
            connection.execute("BEGIN")
            try:
                return work(connection)
            finally:
                connection.rollback()
    except sqlite3.Error as exc:
        raise FuturesTradeRecommendationStorageError(
            "Futures trade recommendation storage is unavailable."
        ) from exc


# ---------------------------------------------------------------------------
# Adapters
# ---------------------------------------------------------------------------


class SQLiteFuturesTradeRecommendationStore(FuturesTradeRecommendationStore):
    """Record immutable futures trade recommendation definitions, insert only."""

    def __init__(self, database_path: str | Path) -> None:
        self._database_path = str(database_path)

    def record(
        self,
        recommendations: tuple[FuturesTradeRecommendation, ...],
        *,
        policy: FuturesRecommendationGenerationPolicy,
        profile: FuturesTradeRecommendationGenerationProfile,
        audit: FuturesRecommendationGenerationAudit,
    ) -> int:
        """Register the configuration and record the batch and its audit in one transaction."""
        validate_futures_trade_recommendation_recording(
            recommendations, policy=policy, profile=profile, audit=audit
        )
        identities = [recommendation.identity for recommendation in recommendations]
        if len(set(identities)) != len(identities):
            raise InvalidFuturesTradeRecommendationRecordingError(
                "A recording batch names one recommendation identity more than once."
            )
        configuration = _configuration_row(policy, profile)
        digest = _configuration_digest(configuration)
        rows = tuple(
            (recommendation, _definition_row(recommendation, profile.version, digest))
            for recommendation in recommendations
        )

        def work(connection: sqlite3.Connection) -> int:
            self._register(connection, configuration)
            if self._already_recorded(connection, rows, audit):
                return len(recommendations)
            batch_products: set[tuple[str, str]] = set()
            for recommendation, row in rows:
                self._record_one(connection, recommendation, row, batch_products)
            if _stored_audit(connection, audit.identity) is not None:
                raise FuturesRecommendationGenerationAuditConflictError(
                    f"Generation audit {audit.identity} is already recorded with other content."
                )
            require_existing_references(connection, audit)
            insert_audit(connection, audit)
            return len(recommendations)

        return _write(self._database_path, work)

    @staticmethod
    def _already_recorded(
        connection: sqlite3.Connection,
        rows: tuple[tuple[FuturesTradeRecommendation, _DefinitionRow], ...],
        audit: FuturesRecommendationGenerationAudit,
    ) -> bool:
        """Return whether this exact recording is committed; refuse identities owned elsewhere."""
        stored = [connection.execute(_SELECT_DEFINITION, (row[0],)).fetchone() for _, row in rows]
        if all(definition is None for definition in stored):
            return False
        if (
            all(
                definition is not None and tuple(definition) == row
                for definition, (_, row) in zip(stored, rows, strict=True)
            )
            and _stored_audit(connection, audit.identity) == audit
        ):
            return True
        recorded = next(
            recommendation
            for definition, (recommendation, _) in zip(stored, rows, strict=True)
            if definition is not None
        )
        raise FuturesTradeRecommendationConflictError(
            f"Recommendation {recorded.identity} is already recorded by another recording; "
            "this attempt cannot record it or claim it in its audit."
        )

    @staticmethod
    def _register(connection: sqlite3.Connection, configuration: _ConfigurationRow) -> None:
        """Register the configuration, first write wins, or refuse a reused version."""
        profile_version, policy_version, _, parameters = configuration
        stored = connection.execute(_SELECT_PROFILE, (profile_version,)).fetchone()
        if stored is not None and tuple(stored) != configuration:
            raise FuturesTradeRecommendationConfigurationConflictError(
                f"Profile version {profile_version} is already registered with other contents."
            )
        registered = {
            row[0] for row in connection.execute(_SELECT_POLICY_PARAMETERS, (policy_version,))
        }
        if registered - {parameters}:
            raise FuturesTradeRecommendationConfigurationConflictError(
                f"Policy version {policy_version} is already registered with other parameters."
            )
        if stored is None:
            connection.execute(_INSERT_CONFIGURATION, configuration)

    @staticmethod
    def _record_one(
        connection: sqlite3.Connection,
        recommendation: FuturesTradeRecommendation,
        row: _DefinitionRow,
        batch_products: set[tuple[str, str]],
    ) -> None:
        product = (row[1], row[2])
        others = connection.execute(_SELECT_OTHERS_ON_PRODUCT, (*product, row[0])).fetchall()
        if product in batch_products or any(
            not _lifecycle(connection, tuple(other))[0].is_terminal for other in others
        ):
            raise FuturesTradeRecommendationExclusivityError(
                f"{recommendation.contract.product} already has an open recommendation; "
                f"{recommendation.identity} cannot be recorded beside it."
            )
        connection.execute(_INSERT_DEFINITION, row)
        batch_products.add(product)


class SQLiteFuturesTradeRecommendationLifecycleStore(FuturesTradeRecommendationLifecycleStore):
    """Record futures trade recommendation lifecycle progress as insert-only steps.

    The steps' candles must be in ``futures_ohlcv`` of this same database.
    """

    def __init__(self, database_path: str | Path) -> None:
        self._database_path = str(database_path)

    def record_progress(
        self,
        previous: FuturesTradeRecommendationLifecycle,
        current: FuturesTradeRecommendationLifecycle,
        *,
        steps: tuple[FuturesTradeRecommendationLifecycleStep, ...],
    ) -> int:
        """Verify and append ``steps`` in one transaction; return the count added."""
        validate_futures_trade_recommendation_lifecycle_progress(previous, current, steps)
        recommendation = previous.recommendation
        for step in steps:
            if isinstance(step, FuturesTradeRecommendationObservationStep):
                try:
                    candle_fingerprint(recommendation, step)
                except (TypeError, ValueError) as exc:
                    raise InvalidFuturesTradeRecommendationLifecycleProgressError(
                        f"Observation at {step.bar.point_in_time} has no fdc1 candle identity: "
                        f"{exc}"
                    ) from exc

        connection = _connect(self._database_path)
        try:
            connection.execute("BEGIN IMMEDIATE")
            recorded = self._record(connection, previous, current, steps)
            connection.commit()
        except (
            FuturesTradeRecommendationLifecycleConflictError,
            FuturesTradeRecommendationLifecycleEvidenceError,
            FuturesTradeRecommendationStorageError,
        ):
            connection.rollback()
            raise
        except sqlite3.Error as exc:
            if connection.in_transaction:
                connection.rollback()
            raise FuturesTradeRecommendationStorageError(
                "Futures trade recommendation storage is unavailable."
            ) from exc
        finally:
            connection.close()
        return recorded

    @classmethod
    def _record(
        cls,
        connection: sqlite3.Connection,
        previous: FuturesTradeRecommendationLifecycle,
        current: FuturesTradeRecommendationLifecycle,
        steps: tuple[FuturesTradeRecommendationLifecycleStep, ...],
    ) -> int:
        recommendation = previous.recommendation
        row = connection.execute(
            _SELECT_BY_IDENTITY, (recommendation.identity.identity,)
        ).fetchone()
        if row is None:
            raise FuturesTradeRecommendationLifecycleConflictError(
                f"Recommendation {recommendation.identity} is not recorded."
            )
        stored, stored_steps = _lifecycle(connection, tuple(row))
        if stored.recommendation != recommendation:
            raise FuturesTradeRecommendationLifecycleConflictError(
                f"Recommendation {recommendation.identity} is recorded with a different definition."
            )

        if stored == previous:
            rows = cls._rows(connection, recommendation, len(stored_steps), previous, steps)
            for step_values in rows:
                connection.execute(_INSERT_STEP, step_values)
            return len(rows)

        # Identical steps applied to two different lifecycles can reach one state,
        # so an already-recorded request must also have started from ``previous``.
        start = len(stored_steps) - len(steps)
        if (
            stored == current
            and start >= 0
            and stored_steps[start:] == steps
            and cls._replay(recommendation, stored_steps[:start]) == previous
        ):
            return 0
        raise FuturesTradeRecommendationLifecycleConflictError(
            f"Recommendation {recommendation.identity} is recorded at {stored.status} after "
            f"{len(stored_steps)} steps, which the requested progress does not continue."
        )

    @staticmethod
    def _rows(
        connection: sqlite3.Connection,
        recommendation: FuturesTradeRecommendation,
        recorded: int,
        previous: FuturesTradeRecommendationLifecycle,
        steps: tuple[FuturesTradeRecommendationLifecycleStep, ...],
    ) -> tuple[StepRow, ...]:
        """Return the new rows, requiring each step's candle to be the stored one."""
        rows = []
        lifecycle = previous
        for sequence, step in enumerate(steps, start=recorded + 1):
            if isinstance(step, FuturesTradeRecommendationObservationStep):
                bar = step.bar
                try:
                    stored = load_candle(connection, bar.contract, bar.timeframe, bar.point_in_time)
                except _DECODE_ERRORS as exc:
                    raise FuturesTradeRecommendationStorageError(
                        "Futures market data storage contains invalid data."
                    ) from exc
                if stored is None:
                    raise FuturesTradeRecommendationLifecycleEvidenceError(
                        f"futures_ohlcv holds no {bar.contract} {bar.timeframe} candle at "
                        f"{bar.point_in_time}; the observation cannot be recorded."
                    )
                stored_step = FuturesTradeRecommendationObservationStep(stored, step.trading_date)
                if stored != bar or candle_fingerprint(
                    recommendation, stored_step
                ) != candle_fingerprint(recommendation, step):
                    raise FuturesTradeRecommendationLifecycleEvidenceError(
                        f"The observation at {bar.point_in_time} is not the {bar.contract} "
                        "candle futures_ohlcv holds."
                    )
            after = apply_futures_trade_recommendation_lifecycle_step(lifecycle, step)
            rows.append(step_row(recommendation, sequence, lifecycle, step, after))
            lifecycle = after
        return tuple(rows)

    @staticmethod
    def _replay(
        recommendation: FuturesTradeRecommendation,
        steps: tuple[FuturesTradeRecommendationLifecycleStep, ...],
    ) -> FuturesTradeRecommendationLifecycle:
        lifecycle = FuturesTradeRecommendationLifecycle(recommendation)
        for step in steps:
            lifecycle = apply_futures_trade_recommendation_lifecycle_step(lifecycle, step)
        return lifecycle


class SQLiteFuturesRecommendationGenerationAuditStore(FuturesRecommendationGenerationAuditStore):
    """Record audits of generation attempts that recorded no new recommendation."""

    def __init__(self, database_path: str | Path) -> None:
        self._database_path = str(database_path)

    def record_audit(self, audit: FuturesRecommendationGenerationAudit) -> int:
        """Insert a standalone audit in its own transaction; 0 when already recorded."""
        validate_standalone_futures_recommendation_generation_audit(audit)

        def work(connection: sqlite3.Connection) -> int:
            stored = _stored_audit(connection, audit.identity)
            if stored is not None:
                if stored != audit:
                    raise FuturesRecommendationGenerationAuditConflictError(
                        f"Generation audit {audit.identity} is already recorded with other content."
                    )
                return 0
            require_existing_references(connection, audit)
            insert_audit(connection, audit)
            return 1

        return _write(self._database_path, work)


class SQLiteFuturesRecommendationGenerationAuditRepository(
    FuturesRecommendationGenerationAuditRepository
):
    """Read recorded generation audits, verified against their stored evidence."""

    def __init__(self, database_path: str | Path) -> None:
        self._database_path = str(database_path)

    def get_audit(self, identity: str) -> FuturesRecommendationGenerationAudit | None:
        """Return the audit recorded under ``identity``, if any."""
        if not isinstance(identity, str):
            raise TypeError(
                "SQLiteFuturesRecommendationGenerationAuditRepository identity must be text."
            )
        return _read(self._database_path, lambda connection: _stored_audit(connection, identity))


class SQLiteFuturesTradeRecommendationRepository(FuturesTradeRecommendationRepository):
    """Read recorded futures trade recommendations and their lifecycles.

    Each lifecycle is replayed from its stored steps and their candles, inside
    one read transaction so that the whole product is read as of one instant.
    A recommendation with no step is its initial WAITING_FOR_ENTRY lifecycle.
    Nothing reflects the clock or the validity window: a recommendation whose
    validity has passed stays open until a terminal step is recorded.
    """

    def __init__(self, database_path: str | Path) -> None:
        self._database_path = str(database_path)

    def get_recommendation(
        self, identity: FuturesTradeRecommendationIdentity
    ) -> FuturesTradeRecommendation | None:
        """Return the recommendation recorded under ``identity``, if any."""
        if not isinstance(identity, FuturesTradeRecommendationIdentity):
            raise TypeError(
                "SQLiteFuturesTradeRecommendationRepository identity must be a "
                "FuturesTradeRecommendationIdentity."
            )
        rows = _read(
            self._database_path,
            lambda connection: connection.execute(
                _SELECT_BY_IDENTITY, (identity.identity,)
            ).fetchall(),
        )
        return _decode(tuple(rows[0])) if rows else None

    def get_lifecycles(
        self, query: FuturesTradeRecommendationQuery
    ) -> tuple[FuturesTradeRecommendationLifecycle, ...]:
        """Return the lifecycle of every recommendation of the product, on every expiry."""
        if not isinstance(query, FuturesTradeRecommendationQuery):
            raise TypeError(
                "SQLiteFuturesTradeRecommendationRepository query must be a "
                "FuturesTradeRecommendationQuery."
            )
        product = query.product

        def lifecycles(
            connection: sqlite3.Connection,
        ) -> list[FuturesTradeRecommendationLifecycle]:
            rows = connection.execute(
                _SELECT_BY_PRODUCT, (product.product_code.value, product.exchange_code.value)
            ).fetchall()
            return [_lifecycle(connection, tuple(row))[0] for row in rows]

        return tuple(
            sorted(
                _read(self._database_path, lifecycles),
                key=lambda item: item.recommendation.identity.identity,
            )
        )
