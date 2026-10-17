"""Owner-accepted reference evidence and the gate that binds reference values to it (R-1B-2B-2).

Five things are kept apart:

1. a source document -- ``HistoricalFuturesDocumentIdentity``: an NSE
   document ID, its official URL and, where verified, its SHA-256;
2. a fact extracted from it -- ``HistoricalFuturesAcceptedFact``, with its own
   fact ID;
3. that fact's scope and interpretation -- category, product, the exact
   contract, value, effective boundaries and their semantics, the accepted
   interpretation, limitations and unresolved conditions;
4. the owner's decision -- ``HistoricalFuturesOwnerAcceptance``;
5. a reference value built from it -- only through
   ``HistoricalFuturesEvidenceGate``, which returns a
   ``HistoricalFuturesReferenceAuthority``.

Acceptance is membership, not a flag
------------------------------------
A fact object carries no "accepted" field. A fact is accepted for production
only when it is present in ``PRODUCTION_ACCEPTED_EVIDENCE``
(``nse_accepted_reference_evidence``), which is reviewed by hand and is
EMPTY. A citation string, a matching document hash or a fact constructed by a
caller grants nothing.

A gate may be built over any registry so that the mechanism can be tested,
but authority from a registry that is not the production object is labelled
``test-accepted-evidence`` and ``is_production`` is False. Only the production
registry object yields ``nse-accepted-evidence`` authority, and today it
yields none.

What the gate refuses
---------------------
- a fact ID not in the registry;
- a different category, product or contract than the fact states, or a
  different value;
- a period outside the fact's effective boundaries, or open-ended;
- effective-from semantics other than the fact's, and end-of-day semantics at
  all, until owner decision D-3 settles how EOD maps to sessions;
- any fact with an unresolved condition;
- rule and schedule facts (expiry convention, scheduled expiry, product lot
  or tick rules), which never authorize a contract value.

Trust boundary
--------------
Python cannot stop code from editing a module constant or importing a private
name. The controls are: review of ``nse_accepted_reference_evidence``; the
identity check on the production registry; and a private token without which
``HistoricalFuturesReferenceAuthority`` cannot be constructed through its
public constructor. None of this is cryptographic, and a fingerprint proves
byte identity with an observed document, not that NSE issued it. Accepted
reference evidence is never a permission to use market data.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum
from urllib.parse import urlsplit

from northstar_application.ports import (
    FuturesContractCutoff,
    FuturesContractCutoffSource,
    FuturesSessionResolutionError,
    FuturesTradingSessionResolver,
)
from northstar_core.futures import FuturesContract, FuturesProductReference

PRODUCTION_EVIDENCE_AUTHORITY_PREFIX = "nse-accepted-evidence"
TEST_EVIDENCE_AUTHORITY_PREFIX = "test-accepted-evidence"

_EVIDENCE_ID = re.compile(r"NSE-EV-[0-9]{2,4}")
_FACT_ID = re.compile(r"NSE-F-[0-9]{2,4}")
_DOCUMENT_ID = re.compile(r"NSE(?:/[A-Z]{2,8})?/[A-Za-z0-9-]{1,32}|NSE [A-Za-z0-9 ()&-]{3,120}")
_SHA256 = re.compile(r"[0-9a-f]{64}")
_VERSION = re.compile(r"[A-Za-z0-9][A-Za-z0-9._/-]{0,127}")
_TEXT = re.compile(r"[\x21-\x7e](?:[\x20-\x7e]{0,498}[\x21-\x7e])?")
_NSE_HOSTS = frozenset(
    {"nseindia.com", "www.nseindia.com", "nsearchives.nseindia.com", "archives.nseindia.com"}
)


class HistoricalFuturesEvidenceError(ValueError):
    """Raised when evidence records or a registry are malformed or contradictory."""


class HistoricalFuturesEvidenceRefusedError(ValueError):
    """Raised when the gate refuses to authorize a reference value."""


class HistoricalFuturesFactCategory(StrEnum):
    """What an extracted fact states."""

    LISTING_DATE = "LISTING_DATE"
    LAST_TRADABLE_SESSION = "LAST_TRADABLE_SESSION"
    LOT_SIZE = "LOT_SIZE"
    TICK_SIZE = "TICK_SIZE"
    # Rules and schedules: recorded, never authorizing a contract value.
    EXPIRY_RULE = "EXPIRY_RULE"
    SCHEDULED_EXPIRY = "SCHEDULED_EXPIRY"
    LOT_SIZE_RULE = "LOT_SIZE_RULE"
    TICK_SIZE_RULE = "TICK_SIZE_RULE"


_DATE_FACTS = frozenset(
    {
        HistoricalFuturesFactCategory.LISTING_DATE,
        HistoricalFuturesFactCategory.LAST_TRADABLE_SESSION,
    }
)
_PERIOD_FACTS = frozenset(
    {HistoricalFuturesFactCategory.LOT_SIZE, HistoricalFuturesFactCategory.TICK_SIZE}
)
_AUTHORIZING = _DATE_FACTS | _PERIOD_FACTS


class HistoricalFuturesBoundarySemantics(StrEnum):
    """How an effective-from date is to be read, as the source words it."""

    SESSION_INCLUSIVE = "SESSION_INCLUSIVE"
    AFTER_END_OF_DAY = "AFTER_END_OF_DAY"


def _require(value: object, expected: type, name: str) -> None:
    if not isinstance(value, expected):
        raise HistoricalFuturesEvidenceError(f"{name} must be a {expected.__name__}.")


def _require_date(value: object, name: str) -> None:
    if isinstance(value, datetime) or not isinstance(value, date):
        raise HistoricalFuturesEvidenceError(f"{name} must be a plain date.")


def _require_text(value: object, pattern: re.Pattern[str], name: str) -> None:
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        raise HistoricalFuturesEvidenceError(f"{name} is missing or malformed.")


@dataclass(frozen=True, slots=True)
class HistoricalFuturesDocumentIdentity:
    """One official NSE source document: its IDs, official URL and verified fingerprint."""

    evidence_id: str
    document_id: str
    url: str
    sha256: str | None

    def __post_init__(self) -> None:
        _require_text(self.evidence_id, _EVIDENCE_ID, "Evidence ID")
        _require_text(self.document_id, _DOCUMENT_ID, "NSE document ID")
        if not isinstance(self.url, str):
            raise HistoricalFuturesEvidenceError("Source URL must be text.")
        parts = urlsplit(self.url)
        if parts.scheme != "https" or parts.hostname not in _NSE_HOSTS or not parts.path:
            raise HistoricalFuturesEvidenceError(
                "Source URL must be an https URL on an official NSE host."
            )
        if self.sha256 is not None:
            _require_text(self.sha256, _SHA256, "Document SHA-256")


@dataclass(frozen=True, slots=True)
class HistoricalFuturesOwnerAcceptance:
    """The owner's explicit acceptance of one fact, for one stated purpose."""

    decision_id: str
    owner: str
    accepted_on: date
    purpose: str

    def __post_init__(self) -> None:
        _require_text(self.decision_id, _TEXT, "Decision ID")
        _require_text(self.owner, _TEXT, "Owner")
        _require_date(self.accepted_on, "Acceptance date")
        _require_text(self.purpose, _TEXT, "Acceptance purpose")


@dataclass(frozen=True, slots=True)
class HistoricalFuturesAcceptedFact:
    """One extracted fact with its exact scope and the owner's acceptance.

    Constructing one accepts nothing: only the production registry's contents
    are accepted. Contract facts name one exact contract. Date facts
    (listing, last tradable session) carry a date value and no period. Lot and
    tick facts carry a value and a closed effective period. Rule and schedule
    facts carry a rule identity or scheduled date and never authorize a
    contract value.
    """

    fact_id: str
    document: HistoricalFuturesDocumentIdentity
    category: HistoricalFuturesFactCategory
    product: FuturesProductReference
    contract: FuturesContract | None
    value: date | int | Decimal | str
    effective_from: date | None
    effective_from_semantics: HistoricalFuturesBoundarySemantics | None
    effective_through: date | None
    interpretation: str
    limitations: tuple[str, ...]
    unresolved_conditions: tuple[str, ...]
    acceptance: HistoricalFuturesOwnerAcceptance

    def __post_init__(self) -> None:
        _require_text(self.fact_id, _FACT_ID, "Fact ID")
        _require(self.document, HistoricalFuturesDocumentIdentity, "Fact document")
        _require(self.category, HistoricalFuturesFactCategory, "Fact category")
        _require(self.product, FuturesProductReference, "Fact product")
        _require(self.acceptance, HistoricalFuturesOwnerAcceptance, "Fact acceptance")
        _require_text(self.interpretation, _TEXT, "Fact interpretation")
        for name, notes in (
            ("limitations", self.limitations),
            ("unresolved conditions", self.unresolved_conditions),
        ):
            if not isinstance(notes, tuple):
                raise HistoricalFuturesEvidenceError(f"Fact {name} must be a tuple.")
            for note in notes:
                _require_text(note, _TEXT, f"Fact {name} entry")
        self._validate_contract()
        self._validate_value()
        self._validate_period()

    def _validate_contract(self) -> None:
        if self.category in _AUTHORIZING:
            _require(self.contract, FuturesContract, f"{self.category} fact contract")
        elif self.contract is not None:
            _require(self.contract, FuturesContract, "Fact contract")
        if self.contract is not None and self.contract.product != self.product:
            raise HistoricalFuturesEvidenceError(
                f"Fact {self.fact_id} contract is not a contract of {self.product}."
            )

    def _validate_value(self) -> None:
        category = self.category
        if category in _DATE_FACTS or category is HistoricalFuturesFactCategory.SCHEDULED_EXPIRY:
            _require_date(self.value, f"{category} fact value")
        elif category is HistoricalFuturesFactCategory.LOT_SIZE:
            if isinstance(self.value, bool) or not isinstance(self.value, int) or self.value <= 0:
                raise HistoricalFuturesEvidenceError("LOT_SIZE fact value must be a positive int.")
        elif category is HistoricalFuturesFactCategory.TICK_SIZE:
            if not isinstance(self.value, Decimal) or not self.value.is_finite() or self.value <= 0:
                raise HistoricalFuturesEvidenceError(
                    "TICK_SIZE fact value must be a positive, finite Decimal."
                )
        else:
            _require_text(self.value, _TEXT, f"{category} fact rule identity")
        if category in _DATE_FACTS and self.contract is not None:
            expiration = date.fromisoformat(self.contract.expiration_date.value)
            if self.value > expiration:  # type: ignore[operator]
                raise HistoricalFuturesEvidenceError(
                    f"Fact {self.fact_id} date is after {self.contract}'s expiration date."
                )

    def _validate_period(self) -> None:
        if self.category in _PERIOD_FACTS:
            _require_date(self.effective_from, "Effective-from")
            _require(
                self.effective_from_semantics,
                HistoricalFuturesBoundarySemantics,
                "Effective-from semantics",
            )
            # A closed period: supersession cannot be verified, so no open end.
            _require_date(self.effective_through, "Effective-through")
            if self.effective_through < self.effective_from:  # type: ignore[operator]
                raise HistoricalFuturesEvidenceError(f"Fact {self.fact_id} period is inverted.")
            return
        if self.category in _DATE_FACTS and (
            self.effective_from is not None
            or self.effective_from_semantics is not None
            or self.effective_through is not None
        ):
            raise HistoricalFuturesEvidenceError(
                f"A {self.category} fact is a single date and has no effective period."
            )
        if self.effective_from is not None:
            _require_date(self.effective_from, "Effective-from")
        if self.effective_through is not None:
            _require_date(self.effective_through, "Effective-through")
        if self.effective_from_semantics is not None:
            _require(
                self.effective_from_semantics,
                HistoricalFuturesBoundarySemantics,
                "Effective-from semantics",
            )


@dataclass(frozen=True, slots=True)
class HistoricalFuturesAcceptedEvidenceRegistry:
    """One version of a set of accepted facts, unique by fact ID, in fact-ID order.

    An evidence ID must name the same document in every fact that cites it.
    A repeated fact ID is refused whether identical or conflicting.
    """

    version: str
    facts: tuple[HistoricalFuturesAcceptedFact, ...]

    def __post_init__(self) -> None:
        _require_text(self.version, _VERSION, "Registry version")
        if not isinstance(self.facts, tuple) or not all(
            isinstance(fact, HistoricalFuturesAcceptedFact) for fact in self.facts
        ):
            raise HistoricalFuturesEvidenceError(
                "Registry facts must be a tuple of accepted facts."
            )
        by_fact: dict[str, HistoricalFuturesAcceptedFact] = {}
        by_evidence: dict[str, HistoricalFuturesDocumentIdentity] = {}
        for fact in self.facts:
            earlier = by_fact.setdefault(fact.fact_id, fact)
            if earlier is not fact:
                detail = "repeated" if earlier == fact else "recorded with conflicting content"
                raise HistoricalFuturesEvidenceError(f"Fact {fact.fact_id} is {detail}.")
            document = by_evidence.setdefault(fact.document.evidence_id, fact.document)
            if document != fact.document:
                raise HistoricalFuturesEvidenceError(
                    f"Evidence {fact.document.evidence_id} names two different documents."
                )
        object.__setattr__(self, "facts", tuple(sorted(self.facts, key=lambda f: f.fact_id)))

    def fact(self, fact_id: str) -> HistoricalFuturesAcceptedFact | None:
        """Return the fact recorded under exactly ``fact_id``, or None."""
        for fact in self.facts:
            if fact.fact_id == fact_id:
                return fact
        return None


_GATE_TOKEN = object()


@dataclass(frozen=True, slots=True)
class HistoricalFuturesReferenceAuthority:
    """What the gate granted: one accepted fact, from one registry version.

    Only the gate creates these. ``is_production`` is True only for the
    production registry. ``grants_market_data_rights`` is always False:
    reference evidence never permits retaining or using market data.
    """

    registry_version: str
    fact_id: str
    evidence_id: str
    document_id: str
    document_sha256: str | None
    decision_id: str
    is_production: bool
    _token: object = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        if self._token is not _GATE_TOKEN:
            raise HistoricalFuturesEvidenceRefusedError(
                "Reference authority is granted only by HistoricalFuturesEvidenceGate."
            )

    @property
    def grants_market_data_rights(self) -> bool:
        """Return whether this authority permits market-data use; never."""
        return False

    def __str__(self) -> str:
        prefix = (
            PRODUCTION_EVIDENCE_AUTHORITY_PREFIX
            if self.is_production
            else TEST_EVIDENCE_AUTHORITY_PREFIX
        )
        fingerprint = self.document_sha256 or "unverified"
        return (
            f"{prefix} {self.registry_version}: {self.fact_id} {self.evidence_id} "
            f"{self.document_id} sha256:{fingerprint} decision:{self.decision_id}"
        )


class HistoricalFuturesEvidenceGate:
    """Authorizes a reference value only when an accepted fact supports exactly it."""

    def __init__(self, registry: HistoricalFuturesAcceptedEvidenceRegistry) -> None:
        if not isinstance(registry, HistoricalFuturesAcceptedEvidenceRegistry):
            raise HistoricalFuturesEvidenceError(
                "Evidence gate needs a HistoricalFuturesAcceptedEvidenceRegistry."
            )
        self._registry = registry

    @classmethod
    def production(cls) -> HistoricalFuturesEvidenceGate:
        """Return the gate over the reviewed production registry (empty today)."""
        from northstar_infrastructure.market_data.nse_accepted_reference_evidence import (
            PRODUCTION_ACCEPTED_EVIDENCE,
        )

        return cls(PRODUCTION_ACCEPTED_EVIDENCE)

    @property
    def is_production(self) -> bool:
        """Return whether this gate reads the production registry object itself."""
        from northstar_infrastructure.market_data.nse_accepted_reference_evidence import (
            PRODUCTION_ACCEPTED_EVIDENCE,
        )

        return self._registry is PRODUCTION_ACCEPTED_EVIDENCE

    @property
    def registry(self) -> HistoricalFuturesAcceptedEvidenceRegistry:
        """Return the registry this gate reads."""
        return self._registry

    def authorize_date(
        self,
        fact_id: str,
        category: HistoricalFuturesFactCategory,
        contract: FuturesContract,
        value: date,
    ) -> HistoricalFuturesReferenceAuthority:
        """Authorize a listing date or last tradable session of exactly ``contract``."""
        if category not in _DATE_FACTS:
            raise HistoricalFuturesEvidenceRefusedError(f"{category} is not a date fact.")
        fact = self._fact(fact_id, category, contract)
        if type(value) is not date or fact.value != value:
            raise HistoricalFuturesEvidenceRefusedError(
                f"Fact {fact_id} does not state {value!r} for {contract}."
            )
        return self._grant(fact)

    def authorize_period(
        self,
        fact_id: str,
        category: HistoricalFuturesFactCategory,
        contract: FuturesContract,
        value: int | Decimal,
        effective_from: date,
        effective_from_semantics: HistoricalFuturesBoundarySemantics,
        effective_through: date,
    ) -> HistoricalFuturesReferenceAuthority:
        """Authorize a lot or tick value over a closed period within the fact's period."""
        if category not in _PERIOD_FACTS:
            raise HistoricalFuturesEvidenceRefusedError(f"{category} is not a period fact.")
        fact = self._fact(fact_id, category, contract)
        if type(value) is not type(fact.value) or value != fact.value:
            raise HistoricalFuturesEvidenceRefusedError(
                f"Fact {fact_id} does not state {value!r} for {contract}."
            )
        if effective_from_semantics is not fact.effective_from_semantics:
            raise HistoricalFuturesEvidenceRefusedError(
                f"Fact {fact_id} boundary semantics are {fact.effective_from_semantics}, "
                f"not {effective_from_semantics}."
            )
        if fact.effective_from_semantics is HistoricalFuturesBoundarySemantics.AFTER_END_OF_DAY:
            raise HistoricalFuturesEvidenceRefusedError(
                f"Fact {fact_id} takes effect after end of day; how that maps to sessions "
                "awaits owner decision D-3."
            )
        for day in (effective_from, effective_through):
            if type(day) is not date:
                raise HistoricalFuturesEvidenceRefusedError("Periods must be plain dates.")
        if not (
            fact.effective_from <= effective_from <= effective_through <= fact.effective_through  # type: ignore[operator]
        ):
            raise HistoricalFuturesEvidenceRefusedError(
                f"Period {effective_from.isoformat()} to {effective_through.isoformat()} is "
                f"outside fact {fact_id}'s accepted period."
            )
        return self._grant(fact)

    def accepted_fact_for(
        self, category: HistoricalFuturesFactCategory, contract: FuturesContract
    ) -> HistoricalFuturesAcceptedFact | None:
        """Return the one accepted fact of ``category`` for exactly ``contract``, or None."""
        found = [
            fact
            for fact in self._registry.facts
            if fact.category is category and fact.contract == contract
        ]
        if len(found) > 1:
            raise HistoricalFuturesEvidenceRefusedError(
                f"{len(found)} accepted {category} facts name {contract}; "
                "conflicting evidence is not resolved by order."
            )
        return found[0] if found else None

    def _fact(
        self, fact_id: str, category: HistoricalFuturesFactCategory, contract: FuturesContract
    ) -> HistoricalFuturesAcceptedFact:
        if not isinstance(contract, FuturesContract):
            raise HistoricalFuturesEvidenceRefusedError("Contract must be a FuturesContract.")
        fact = self._registry.fact(fact_id) if isinstance(fact_id, str) else None
        if fact is None:
            raise HistoricalFuturesEvidenceRefusedError(
                f"Fact {fact_id!r} is not accepted in registry {self._registry.version}."
            )
        if fact.category is not category:
            raise HistoricalFuturesEvidenceRefusedError(
                f"Fact {fact_id} is a {fact.category} fact, not {category}."
            )
        if fact.product != contract.product:
            raise HistoricalFuturesEvidenceRefusedError(
                f"Fact {fact_id} is for {fact.product}, not {contract.product}."
            )
        if fact.contract != contract:
            raise HistoricalFuturesEvidenceRefusedError(
                f"Fact {fact_id} is for {fact.contract}, not {contract}."
            )
        if fact.unresolved_conditions:
            raise HistoricalFuturesEvidenceRefusedError(
                f"Fact {fact_id} has unresolved conditions and cannot be used unconditionally."
            )
        return fact

    def _grant(self, fact: HistoricalFuturesAcceptedFact) -> HistoricalFuturesReferenceAuthority:
        return HistoricalFuturesReferenceAuthority(
            registry_version=self._registry.version,
            fact_id=fact.fact_id,
            evidence_id=fact.document.evidence_id,
            document_id=fact.document.document_id,
            document_sha256=fact.document.sha256,
            decision_id=fact.acceptance.decision_id,
            is_production=self.is_production,
            _token=_GATE_TOKEN,
        )


class HistoricalFuturesEvidenceCutoffSource(FuturesContractCutoffSource):
    """Resolves a cutoff only from an accepted last-tradable-session fact for that contract.

    The caller supplies no dates: the last session is the accepted fact's own
    value. No accepted fact for exactly the contract means None (fail closed).
    A scheduled expiry, an expiry rule, a lot or tick fact never stands in for
    it, and nothing is derived from the expiration date. The session must be a
    session of the calendar resolver, or resolution raises. The authority names
    the registry, fact, evidence, document, fingerprint and decision; from any
    registry but production it is labelled ``test-accepted-evidence``.
    """

    def __init__(
        self, gate: HistoricalFuturesEvidenceGate, sessions: FuturesTradingSessionResolver
    ) -> None:
        if not isinstance(gate, HistoricalFuturesEvidenceGate):
            raise HistoricalFuturesEvidenceError(
                "Cutoff source needs a HistoricalFuturesEvidenceGate."
            )
        if not isinstance(sessions, FuturesTradingSessionResolver):
            raise HistoricalFuturesEvidenceError(
                "Cutoff source needs a FuturesTradingSessionResolver."
            )
        self._gate = gate
        self._sessions = sessions

    def resolve(self, contract: FuturesContract) -> FuturesContractCutoff | None:
        """Return the evidence-backed cutoff of ``contract``, or None when none is accepted."""
        if not isinstance(contract, FuturesContract):
            raise TypeError(
                "HistoricalFuturesEvidenceCutoffSource contract must be a FuturesContract."
            )
        category = HistoricalFuturesFactCategory.LAST_TRADABLE_SESSION
        fact = self._gate.accepted_fact_for(category, contract)
        if fact is None:
            return None
        day: date = fact.value  # type: ignore[assignment]
        authority = self._gate.authorize_date(fact.fact_id, category, contract, day)
        try:
            session = self._sessions.resolve(contract.product, day)
        except FuturesSessionResolutionError as exc:
            raise HistoricalFuturesEvidenceRefusedError(
                f"{contract} accepted last session {day.isoformat()} cannot be verified "
                f"against the calendar: {exc}"
            ) from exc
        if session is None or session.trading_date != day:
            raise HistoricalFuturesEvidenceRefusedError(
                f"{contract} accepted last session {day.isoformat()} is not a trading session "
                f"of {contract.product.exchange_code}."
            )
        return FuturesContractCutoff(contract, session, str(authority))
