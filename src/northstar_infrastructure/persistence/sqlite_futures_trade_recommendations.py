"""SQLite-backed futures trade recommendation definition store and repository.

Status: an internal persistence capability, not an enabled recording workflow.
A successful generation must eventually commit its recommendation and its
generation audit in one transaction (F-5B-2); this slice stores definitions and
configurations only, and nothing wires it into operations.

Recording
---------
``record(recommendations, policy=..., profile=...)`` first runs the shared
Application validator, which re-derives every identity from its source under
the profile. Then, in ONE ``BEGIN IMMEDIATE`` transaction:

1. the configuration is registered, first write wins. A profile version already
   registered with other contents, or a policy version registered with other
   parameters under any profile, raises the configuration conflict;
2. for each recommendation: an identity already stored with the identical
   definition is an idempotent no-op; with a different definition it is a
   FuturesTradeRecommendationConflictError and nothing is overwritten;
3. a new recommendation is refused with FuturesTradeRecommendationExclusivityError
   when its product -- product code and exchange, every expiry -- already has
   another open recommendation, or an earlier one in the same batch;
4. new definitions are inserted, and the whole batch commits or rolls back.

``BEGIN IMMEDIATE`` takes the database's write lock before anything is read, so
concurrent writers are serialised by SQLite itself, whatever lock the caller
holds. A writer waits at most five seconds for that lock; past it the write
fails as a storage error and nothing is written. The journal mode is left as
it is. Foreign keys are enforced on this module's own write connections.

Openness, for now
-----------------
F-2 derives terminality from a recommendation's lifecycle events. No events
are persisted yet, so every stored definition is open, and a product with any
stored recommendation is blocked. Nothing here infers terminality from validity
or the clock, and there is no terminality column: F-5B-2 adds insert-only
lifecycle steps, and openness will become "no terminal step recorded" without
rewriting any definition. Until then a product is never released.

Evidence
--------
The identity inputs are constrained columns; the definition is versioned
canonical JSON (sorted keys, no whitespace, decimals as fixed-point text,
instants as canonical UTC text). The definition also seals the exact
configuration row it was recorded under with a SHA-256 digest, so a registered
configuration that is changed afterwards -- a horizon, a policy parameter --
invalidates every definition that relied on it. Every read rebuilds the F-2
recommendation, re-serializes it, and requires the stored text, the columns,
the configuration seal and the re-derived identity to agree. A row that fails any check
is corrupt and raises; nothing is repaired. No OHLCV bar is stored, and nothing
here calls a market-data provider. The module issues INSERT only.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import closing
from decimal import Decimal
from pathlib import Path
from typing import Any

from northstar_application.application_services import (
    FuturesTradeRecommendationGenerationProfile,
    FuturesTradeRecommendationIdentityService,
)
from northstar_application.ports import (
    FuturesTradeRecommendationConfigurationConflictError,
    FuturesTradeRecommendationConflictError,
    FuturesTradeRecommendationExclusivityError,
    FuturesTradeRecommendationQuery,
    FuturesTradeRecommendationRepository,
    FuturesTradeRecommendationStore,
    InvalidFuturesTradeRecommendationRecordingError,
    validate_futures_trade_recommendation_recording,
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

from northstar_infrastructure.persistence.sqlite_futures_trade_recommendation_schema import (
    initialize_futures_trade_recommendation_schema,
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
_SELECT_OTHER_ON_PRODUCT = (
    "SELECT recommendation_identity FROM futures_trade_recommendations "
    "WHERE product_code = ? AND exchange_code = ? AND recommendation_identity <> ? LIMIT 1"
)
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
    ) -> int:
        """Register the configuration and record the batch in one transaction."""
        validate_futures_trade_recommendation_recording(
            recommendations, policy=policy, profile=profile
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

        connection = _connect(self._database_path)
        try:
            connection.execute("BEGIN IMMEDIATE")
            self._register(connection, configuration)
            batch_products: set[tuple[str, str]] = set()
            for recommendation, row in rows:
                self._record_one(connection, recommendation, row, batch_products)
            connection.commit()
        except (
            FuturesTradeRecommendationConflictError,
            FuturesTradeRecommendationConfigurationConflictError,
            FuturesTradeRecommendationExclusivityError,
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
        return len(recommendations)

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
        stored = connection.execute(_SELECT_DEFINITION, (row[0],)).fetchone()
        if stored is not None:
            if tuple(stored) != row:
                raise FuturesTradeRecommendationConflictError(
                    f"Recommendation {recommendation.identity} is already recorded with a "
                    "different definition."
                )
            return
        product = (row[1], row[2])
        other = connection.execute(_SELECT_OTHER_ON_PRODUCT, (*product, row[0])).fetchone()
        if other is not None or product in batch_products:
            raise FuturesTradeRecommendationExclusivityError(
                f"{recommendation.contract.product} already has an open recommendation; "
                f"{recommendation.identity} cannot be recorded beside it."
            )
        connection.execute(_INSERT_DEFINITION, row)
        batch_products.add(product)


class SQLiteFuturesTradeRecommendationRepository(FuturesTradeRecommendationRepository):
    """Read recorded futures trade recommendations and their lifecycles.

    Lifecycles are rebuilt from definitions alone: each is the initial
    WAITING_FOR_ENTRY lifecycle, because no lifecycle event is persisted yet.
    That is a temporary state of this slice (F-5B-2 adds lifecycle steps); it
    never reflects the clock or the validity window, and a recommendation whose
    validity has passed is still returned open.
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
        rows = self._rows(_SELECT_BY_IDENTITY, (identity.identity,))
        return _decode(rows[0]) if rows else None

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
        rows = self._rows(
            _SELECT_BY_PRODUCT, (product.product_code.value, product.exchange_code.value)
        )
        recommendations = sorted(
            (_decode(row) for row in rows), key=lambda item: item.identity.identity
        )
        return tuple(FuturesTradeRecommendationLifecycle(item) for item in recommendations)

    def _rows(self, sql: str, parameters: tuple[str, ...]) -> list[tuple[object, ...]]:
        try:
            with closing(
                sqlite3.connect(self._database_path, timeout=_BUSY_TIMEOUT_SECONDS)
            ) as connection:
                initialize_futures_trade_recommendation_schema(connection)
                return [tuple(row) for row in connection.execute(sql, parameters).fetchall()]
        except sqlite3.Error as exc:
            raise FuturesTradeRecommendationStorageError(
                "Futures trade recommendation storage is unavailable."
            ) from exc
