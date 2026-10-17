"""R-1B-2B-2: owner-accepted reference evidence gate and evidence-backed cutoffs.

Every accepted fact here is a SYNTHETIC TEST FIXTURE: fact IDs NSE-F-90 and
above, a synthetic document identity and a synthetic owner. None is an NSE
fact and none is in the production registry, which is empty. Only the NSE
calendar resolver is the existing loaded reference.
"""

from __future__ import annotations

import ast
import dataclasses
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from northstar_application.application_services import (
    FuturesEvaluationEvidenceClassification,
    FuturesRecommendationEvaluationEvidenceError,
)
from northstar_application.ports import FuturesContractCutoffSource
from northstar_core.derivatives import ExpirationDate
from northstar_core.foundation.value_objects import ExchangeCode, PointInTime, Symbol
from northstar_core.futures import FuturesContract, FuturesProductReference

import northstar_infrastructure.market_data as market_data
from northstar_infrastructure.market_data import (
    PRODUCTION_ACCEPTED_EVIDENCE,
    PRODUCTION_EVIDENCE_AUTHORITY_PREFIX,
    SYNTHETIC_REFERENCE_AUTHORITY_PREFIX,
    TEST_EVIDENCE_AUTHORITY_PREFIX,
    HistoricalFuturesAcceptedEvidenceRegistry,
    HistoricalFuturesAcceptedFact,
    HistoricalFuturesBoundarySemantics,
    HistoricalFuturesDocumentIdentity,
    HistoricalFuturesEvidenceCutoffSource,
    HistoricalFuturesEvidenceError,
    HistoricalFuturesEvidenceGate,
    HistoricalFuturesEvidenceRefusedError,
    HistoricalFuturesFactCategory,
    HistoricalFuturesOwnerAcceptance,
    HistoricalFuturesReferenceAuthority,
    HistoricalFuturesReferenceKind,
    HistoricalFuturesReferenceSource,
    NSEFuturesTradingSessionResolver,
)
from tests.test_historical_futures_contract_references import _evaluate, _source

_NIFTY = FuturesProductReference(Symbol("NIFTY"), ExchangeCode("NSE"))
_BANKNIFTY = FuturesProductReference(Symbol("BANKNIFTY"), ExchangeCode("NSE"))
_OCT = FuturesContract(_NIFTY, ExpirationDate("2026-10-27"))
_NOV = FuturesContract(_NIFTY, ExpirationDate("2026-11-24"))
_BANK_OCT = FuturesContract(_BANKNIFTY, ExpirationDate("2026-10-27"))
_NSE = NSEFuturesTradingSessionResolver()
Cat = HistoricalFuturesFactCategory
Sem = HistoricalFuturesBoundarySemantics
Fact = HistoricalFuturesAcceptedFact
Registry = HistoricalFuturesAcceptedEvidenceRegistry

_DOC = HistoricalFuturesDocumentIdentity(
    evidence_id="NSE-EV-90",
    document_id="NSE/TEST/SYNTHETIC-1",
    url="https://nsearchives.nseindia.com/synthetic/test-fixture-1.pdf",
    sha256="0" * 64,
)
_ACCEPT = HistoricalFuturesOwnerAcceptance(
    "SYNTHETIC-DECISION-1", "synthetic-test-owner", date(2026, 10, 18), "test fixture only"
)


def _fact(fact_id: str = "NSE-F-90", **changes: object) -> Fact:
    fields: dict[str, object] = {
        "fact_id": fact_id,
        "document": _DOC,
        "category": Cat.LAST_TRADABLE_SESSION,
        "product": _NIFTY,
        "contract": _OCT,
        "value": date(2026, 10, 23),
        "effective_from": None,
        "effective_from_semantics": None,
        "effective_through": None,
        "interpretation": "synthetic last tradable session",
        "limitations": (),
        "unresolved_conditions": (),
        "acceptance": _ACCEPT,
    }
    fields.update(changes)
    return Fact(**fields)  # type: ignore[arg-type]


def _lot(fact_id: str = "NSE-F-91", **changes: object) -> Fact:
    fields: dict[str, object] = {
        "category": Cat.LOT_SIZE,
        "value": 65,
        "effective_from": date(2026, 9, 1),
        "effective_from_semantics": Sem.SESSION_INCLUSIVE,
        "effective_through": date(2026, 10, 23),
        "interpretation": "synthetic lot size",
    }
    fields.update(changes)
    return _fact(fact_id, **fields)


def _tick(fact_id: str = "NSE-F-92", **changes: object) -> Fact:
    return _lot(fact_id, category=Cat.TICK_SIZE, value=Decimal("0.10"), **changes)


def _gate(
    *facts: Fact, version: str = "synthetic-test-evidence/1"
) -> HistoricalFuturesEvidenceGate:
    return HistoricalFuturesEvidenceGate(Registry(version, facts or (_fact(),)))


def _cutoffs(*facts: Fact) -> HistoricalFuturesEvidenceCutoffSource:
    return HistoricalFuturesEvidenceCutoffSource(_gate(*facts), _NSE)


# ---------------------------------------------------------------------------
# Governance
# ---------------------------------------------------------------------------


def test_the_production_registry_is_empty() -> None:
    assert PRODUCTION_ACCEPTED_EVIDENCE.facts == ()
    assert PRODUCTION_ACCEPTED_EVIDENCE.version == "nse-accepted-evidence/0"
    gate = HistoricalFuturesEvidenceGate.production()
    assert gate.is_production is True
    assert gate.registry is PRODUCTION_ACCEPTED_EVIDENCE


@pytest.mark.parametrize("number", range(1, 23))
def test_no_extracted_register_fact_is_accepted(number: int) -> None:
    fact_id = f"NSE-F-{number:02d}"
    gate = HistoricalFuturesEvidenceGate.production()
    assert PRODUCTION_ACCEPTED_EVIDENCE.fact(fact_id) is None
    with pytest.raises(HistoricalFuturesEvidenceRefusedError, match="not accepted"):
        gate.authorize_date(fact_id, Cat.LAST_TRADABLE_SESSION, _OCT, date(2026, 10, 23))


def test_the_production_cutoff_source_trusts_nothing() -> None:
    source = HistoricalFuturesEvidenceCutoffSource(HistoricalFuturesEvidenceGate.production(), _NSE)
    assert source.resolve(_OCT) is None
    assert source.resolve(_NOV) is None


def test_synthetic_references_remain_usable_and_labelled() -> None:
    cutoff = _source().resolve(_OCT)
    assert cutoff is not None
    assert cutoff.authority.startswith(SYNTHETIC_REFERENCE_AUTHORITY_PREFIX)
    assert [kind.value for kind in HistoricalFuturesReferenceKind] == ["SYNTHETIC"]


def test_test_fixtures_never_reach_production() -> None:
    gate = _gate(_fact(), _lot())
    assert gate.is_production is False
    assert PRODUCTION_ACCEPTED_EVIDENCE.facts == ()
    # A registry copying the production version is still not the production object.
    impostor = _gate(_fact(), version=PRODUCTION_ACCEPTED_EVIDENCE.version)
    assert impostor.is_production is False
    cutoff = HistoricalFuturesEvidenceCutoffSource(impostor, _NSE).resolve(_OCT)
    assert cutoff is not None
    assert cutoff.authority.startswith(TEST_EVIDENCE_AUTHORITY_PREFIX + " ")
    assert not cutoff.authority.startswith(PRODUCTION_EVIDENCE_AUTHORITY_PREFIX)


# ---------------------------------------------------------------------------
# Evidence identity
# ---------------------------------------------------------------------------


def test_exact_fact_id_matching() -> None:
    gate = _gate()
    assert gate.authorize_date("NSE-F-90", Cat.LAST_TRADABLE_SESSION, _OCT, date(2026, 10, 23))
    for wrong in ("NSE-F-9", "nse-f-90", "NSE-F-90 ", "NSE-EV-90", 90, None):
        with pytest.raises(HistoricalFuturesEvidenceRefusedError, match="not accepted"):
            gate.authorize_date(wrong, Cat.LAST_TRADABLE_SESSION, _OCT, date(2026, 10, 23))  # type: ignore[arg-type]


def test_the_authority_names_fact_document_fingerprint_and_decision() -> None:
    authority = _gate().authorize_date(
        "NSE-F-90", Cat.LAST_TRADABLE_SESSION, _OCT, date(2026, 10, 23)
    )
    assert (authority.fact_id, authority.evidence_id, authority.document_id) == (
        "NSE-F-90", "NSE-EV-90", "NSE/TEST/SYNTHETIC-1"
    )  # fmt: skip
    assert authority.decision_id == "SYNTHETIC-DECISION-1"
    assert str(authority) == (
        "test-accepted-evidence synthetic-test-evidence/1: NSE-F-90 NSE-EV-90 "
        "NSE/TEST/SYNTHETIC-1 sha256:" + "0" * 64 + " decision:SYNTHETIC-DECISION-1"
    )
    assert authority.grants_market_data_rights is False


def test_facts_from_one_document_are_independent() -> None:
    gate = _gate(_fact())
    assert gate.authorize_date("NSE-F-90", Cat.LAST_TRADABLE_SESSION, _OCT, date(2026, 10, 23))
    with pytest.raises(HistoricalFuturesEvidenceRefusedError, match="not accepted"):
        gate.authorize_period(
            "NSE-F-91", Cat.LOT_SIZE, _OCT, 65, date(2026, 9, 1), Sem.SESSION_INCLUSIVE,
            date(2026, 10, 23),
        )  # fmt: skip


def test_duplicate_and_conflicting_fact_records_are_refused() -> None:
    with pytest.raises(HistoricalFuturesEvidenceError, match="repeated"):
        Registry("v1", (_fact(), _fact()))
    with pytest.raises(HistoricalFuturesEvidenceError, match="conflicting"):
        Registry("v1", (_fact(), _fact(value=date(2026, 10, 26))))


def test_one_evidence_id_cannot_name_two_documents() -> None:
    other = dataclasses.replace(_DOC, document_id="NSE/TEST/SYNTHETIC-2")
    with pytest.raises(HistoricalFuturesEvidenceError, match="two different documents"):
        Registry("v1", (_fact(), _lot(document=other)))


def test_conflicting_cutoff_facts_for_one_contract_fail_closed() -> None:
    source = _cutoffs(_fact(), _fact("NSE-F-93", value=date(2026, 10, 26)))
    with pytest.raises(HistoricalFuturesEvidenceRefusedError, match="conflicting evidence"):
        source.resolve(_OCT)


@pytest.mark.parametrize(
    "changes",
    [
        {"evidence_id": "EV-90"},
        {"evidence_id": "NSE-EV-9"},
        {"document_id": ""},
        {"document_id": "FAOP71876.pdf"},
        {"url": "http://nsearchives.nseindia.com/content/circulars/X.pdf"},
        {"url": "https://nseindia.example.com/content/circulars/X.pdf"},
        {"url": "https://nsearchives.nseindia.com"},
        {"url": "NSE/FAOP/71876"},
        {"sha256": "0" * 63},
        {"sha256": "A" * 64},
    ],
)
def test_malformed_source_identities_are_refused(changes: dict[str, object]) -> None:
    with pytest.raises(HistoricalFuturesEvidenceError):
        dataclasses.replace(_DOC, **changes)


@pytest.mark.parametrize(
    "changes",
    [
        {"fact_id": "F-90"},
        {"document": "NSE/FAOP/71876"},
        {"category": "LAST_TRADABLE_SESSION"},
        {"contract": None},
        {"contract": _BANK_OCT},
        {"value": "2026-10-23"},
        {"value": date(2026, 10, 28)},
        {"effective_from": date(2026, 10, 1)},
        {"interpretation": ""},
        {"limitations": ["x"]},
        {"unresolved_conditions": ("",)},
        {"acceptance": "accepted"},
    ],
)
def test_malformed_facts_are_refused(changes: dict[str, object]) -> None:
    with pytest.raises(HistoricalFuturesEvidenceError):
        _fact(**changes)


# ---------------------------------------------------------------------------
# Scope binding
# ---------------------------------------------------------------------------


def _lot_request(gate: HistoricalFuturesEvidenceGate, **changes: object):  # type: ignore[no-untyped-def]
    request: dict[str, object] = {
        "fact_id": "NSE-F-91",
        "category": Cat.LOT_SIZE,
        "contract": _OCT,
        "value": 65,
        "effective_from": date(2026, 9, 1),
        "effective_from_semantics": Sem.SESSION_INCLUSIVE,
        "effective_through": date(2026, 10, 23),
    }
    request.update(changes)
    return gate.authorize_period(**request)  # type: ignore[arg-type]


def test_an_exact_lot_period_and_a_sub_period_are_authorized() -> None:
    gate = _gate(_lot())
    assert _lot_request(gate).fact_id == "NSE-F-91"
    assert _lot_request(gate, effective_from=date(2026, 9, 15), effective_through=date(2026, 9, 30))


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"contract": _NOV}, "not .*2026-10-27|is for"),
        ({"contract": _BANK_OCT}, "is for NIFTY"),
        ({"category": Cat.TICK_SIZE}, "is a LOT_SIZE fact"),
        ({"value": 75}, "does not state"),
        ({"value": Decimal("65")}, "does not state"),
        ({"effective_from": date(2026, 8, 31)}, "outside"),
        ({"effective_through": date(2026, 10, 26)}, "outside"),
        ({"effective_from": date(2027, 1, 1), "effective_through": date(2027, 1, 31)}, "outside"),
        ({"effective_from": date(2026, 10, 5), "effective_through": date(2026, 10, 1)}, "outside"),
        ({"effective_from_semantics": Sem.AFTER_END_OF_DAY}, "semantics"),
        ({"category": Cat.LAST_TRADABLE_SESSION}, "not a period fact"),
    ],
)
def test_out_of_scope_requests_are_refused(changes: dict[str, object], message: str) -> None:
    with pytest.raises(HistoricalFuturesEvidenceRefusedError, match=message):
        _lot_request(_gate(_lot()), **changes)


def test_a_lot_fact_cannot_serve_as_a_tick_fact() -> None:
    gate = _gate(_lot(), _tick())
    with pytest.raises(HistoricalFuturesEvidenceRefusedError, match="is a LOT_SIZE fact"):
        _lot_request(gate, category=Cat.TICK_SIZE, value=Decimal("0.10"))
    tick = _lot_request(gate, fact_id="NSE-F-92", category=Cat.TICK_SIZE, value=Decimal("0.10"))
    assert tick.fact_id == "NSE-F-92"


def test_end_of_day_semantics_await_owner_decision_d3() -> None:
    gate = _gate(_lot(effective_from_semantics=Sem.AFTER_END_OF_DAY))
    with pytest.raises(HistoricalFuturesEvidenceRefusedError, match="D-3"):
        _lot_request(gate, effective_from_semantics=Sem.AFTER_END_OF_DAY)
    with pytest.raises(HistoricalFuturesEvidenceRefusedError, match="semantics"):
        _lot_request(gate)


def test_open_ended_period_facts_are_refused() -> None:
    with pytest.raises(HistoricalFuturesEvidenceError, match="Effective-through"):
        _lot(effective_through=None)


def test_a_fact_with_an_unresolved_condition_is_never_unconditional() -> None:
    pending = _lot(unresolved_conditions=("mid-life change pending owner decision",))
    with pytest.raises(HistoricalFuturesEvidenceRefusedError, match="unresolved"):
        _lot_request(_gate(pending))
    with pytest.raises(HistoricalFuturesEvidenceRefusedError, match="unresolved"):
        _cutoffs(_fact(unresolved_conditions=("expiry equals last session?",))).resolve(_OCT)


def test_a_scheduled_expiry_cannot_masquerade_as_a_cutoff() -> None:
    scheduled = _fact(category=Cat.SCHEDULED_EXPIRY, value=date(2026, 10, 27))
    assert _cutoffs(scheduled).resolve(_OCT) is None
    gate = _gate(scheduled)
    with pytest.raises(HistoricalFuturesEvidenceRefusedError, match="is a SCHEDULED_EXPIRY"):
        gate.authorize_date("NSE-F-90", Cat.LAST_TRADABLE_SESSION, _OCT, date(2026, 10, 27))
    with pytest.raises(HistoricalFuturesEvidenceRefusedError, match="not a date fact"):
        gate.authorize_date("NSE-F-90", Cat.SCHEDULED_EXPIRY, _OCT, date(2026, 10, 27))


def test_an_expiry_rule_cannot_authorize_a_last_session() -> None:
    rule = _fact(
        category=Cat.EXPIRY_RULE, contract=None, value="last Tuesday; holiday moves earlier"
    )
    assert _cutoffs(rule).resolve(_OCT) is None
    with pytest.raises(HistoricalFuturesEvidenceRefusedError, match="is a EXPIRY_RULE"):
        _gate(rule).authorize_date("NSE-F-90", Cat.LAST_TRADABLE_SESSION, _OCT, date(2026, 10, 27))


def test_lot_and_tick_facts_cannot_authorize_a_cutoff() -> None:
    assert _cutoffs(_lot(), _tick()).resolve(_OCT) is None
    with pytest.raises(HistoricalFuturesEvidenceRefusedError, match="is a LOT_SIZE"):
        _gate(_lot()).authorize_date(
            "NSE-F-91", Cat.LAST_TRADABLE_SESSION, _OCT, date(2026, 10, 23)
        )


def test_a_date_fact_cannot_carry_a_period() -> None:
    with pytest.raises(HistoricalFuturesEvidenceError, match="no effective period"):
        _fact(effective_from=date(2026, 9, 1), effective_from_semantics=Sem.SESSION_INCLUSIVE)


def test_a_date_value_must_match_exactly() -> None:
    gate = _gate()
    with pytest.raises(HistoricalFuturesEvidenceRefusedError, match="does not state"):
        gate.authorize_date("NSE-F-90", Cat.LAST_TRADABLE_SESSION, _OCT, date(2026, 10, 26))
    with pytest.raises(HistoricalFuturesEvidenceRefusedError, match="is for"):
        gate.authorize_date("NSE-F-90", Cat.LAST_TRADABLE_SESSION, _NOV, date(2026, 10, 23))


# ---------------------------------------------------------------------------
# Authority
# ---------------------------------------------------------------------------


def test_an_arbitrary_citation_grants_no_exchange_authority() -> None:
    source = HistoricalFuturesReferenceSource(
        HistoricalFuturesReferenceKind.SYNTHETIC, "NSE-F-90 accepted by owner NSE/FAOP/71876"
    )
    assert source.kind is HistoricalFuturesReferenceKind.SYNTHETIC
    with pytest.raises(ValueError):
        HistoricalFuturesReferenceKind("EXCHANGE_PUBLISHED")


def test_there_is_no_accepted_flag_to_set() -> None:
    names = {f.name for f in dataclasses.fields(Fact)}
    assert not {n for n in names if "accepted" in n or "verified" in n or "approved" in n}
    with pytest.raises(TypeError):
        _fact(accepted=True)


def test_authority_cannot_be_constructed_outside_the_gate() -> None:
    fields = {
        "registry_version": PRODUCTION_ACCEPTED_EVIDENCE.version,
        "fact_id": "NSE-F-01",
        "evidence_id": "NSE-EV-02",
        "document_id": "NSE/FAOP/61415",
        "document_sha256": None,
        "decision_id": "forged",
        "is_production": True,
    }
    with pytest.raises(HistoricalFuturesEvidenceRefusedError, match="only by"):
        HistoricalFuturesReferenceAuthority(**fields, _token=object())  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        HistoricalFuturesReferenceAuthority(**fields)  # type: ignore[call-arg]


def test_a_matching_hash_alone_accepts_nothing() -> None:
    # The fixture document carries a fingerprint; without a registry entry nothing is granted.
    gate = HistoricalFuturesEvidenceGate(Registry("v1", ()))
    with pytest.raises(HistoricalFuturesEvidenceRefusedError, match="not accepted"):
        gate.authorize_date("NSE-F-90", Cat.LAST_TRADABLE_SESSION, _OCT, date(2026, 10, 23))
    assert HistoricalFuturesEvidenceCutoffSource(gate, _NSE).resolve(_OCT) is None


def test_the_evidence_cutoff_is_the_accepted_session_verified_by_the_calendar() -> None:
    source = _cutoffs()
    assert isinstance(source, FuturesContractCutoffSource)
    cutoff = source.resolve(_OCT)
    assert cutoff is not None
    assert cutoff.last_session == _NSE.resolve(_NIFTY, date(2026, 10, 23))
    assert cutoff.last_session.closes_at == PointInTime("2026-10-23T10:10:00Z")
    assert cutoff.last_session.trading_date.isoformat() != _OCT.expiration_date.value
    assert cutoff.authority.startswith("test-accepted-evidence synthetic-test-evidence/1: NSE-F-90")
    assert source.resolve(_NOV) is None
    assert source.resolve(_BANK_OCT) is None


@pytest.mark.parametrize(
    ("contract", "day", "message"),
    [
        (_NOV, date(2026, 11, 24), "not a trading session"),  # loaded holiday
        (_OCT, date(2026, 10, 25), "not a trading session"),  # weekend
        (
            FuturesContract(_NIFTY, ExpirationDate("2027-01-26")),
            date(2027, 1, 25),
            "2027 is not loaded",
        ),
        (
            FuturesContract(_NIFTY, ExpirationDate("2026-11-08")),
            date(2026, 11, 8),
            "no notified timings",
        ),
    ],
)
def test_an_unverifiable_accepted_session_fails_closed(
    contract: FuturesContract, day: date, message: str
) -> None:
    with pytest.raises(HistoricalFuturesEvidenceRefusedError, match=message):
        _cutoffs(_fact(contract=contract, value=day)).resolve(contract)


def test_wrong_inputs_are_refused() -> None:
    with pytest.raises(HistoricalFuturesEvidenceError):
        HistoricalFuturesEvidenceGate(())  # type: ignore[arg-type]
    with pytest.raises(HistoricalFuturesEvidenceError):
        HistoricalFuturesEvidenceCutoffSource(_gate(), object())  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        _cutoffs().resolve("NIFTY 2026-10-27")  # type: ignore[arg-type]
    with pytest.raises(HistoricalFuturesEvidenceError):
        Registry("v1", [_fact()])  # type: ignore[arg-type]


def test_records_are_immutable() -> None:
    fact = _fact()
    with pytest.raises(dataclasses.FrozenInstanceError):
        fact.value = date(2026, 10, 26)  # type: ignore[misc]
    with pytest.raises(dataclasses.FrozenInstanceError):
        PRODUCTION_ACCEPTED_EVIDENCE.facts = (fact,)  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Compatibility
# ---------------------------------------------------------------------------


def test_r0_runs_on_a_test_evidence_cutoff_and_fails_closed_in_production() -> None:
    (record,) = _evaluate(_cutoffs()).recommendations
    assert record.recommendation.validity.contract_cutoff == PointInTime("2026-10-23T10:10:00Z")
    production = HistoricalFuturesEvidenceCutoffSource(
        HistoricalFuturesEvidenceGate.production(), _NSE
    )
    with pytest.raises(FuturesRecommendationEvaluationEvidenceError):
        _evaluate(production)
    assert [c.value for c in FuturesEvaluationEvidenceClassification] == ["SYNTHETIC"]


def test_the_modules_have_no_persistence_provider_or_network_dependency() -> None:
    root = Path(market_data.__file__).parent
    for name in ("historical_futures_reference_evidence.py", "nse_accepted_reference_evidence.py"):
        source = (root / name).read_text(encoding="utf-8")
        tree = ast.parse(source)
        imported = {
            node.module
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom) and node.module
        } | {
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        }
        for module in imported:
            assert not module.startswith(
                ("sqlite3", "databento", "requests", "httpx", "socket", "urllib.request",
                 "northstar_api", "northstar_infrastructure.persistence", "os", "time")
            ), (name, module)  # fmt: skip
        assert "upstox" not in source.lower()
        assert "CREATE TABLE" not in source
        assert "environ" not in source


def test_public_exports() -> None:
    for name in (
        "PRODUCTION_ACCEPTED_EVIDENCE",
        "PRODUCTION_EVIDENCE_AUTHORITY_PREFIX",
        "TEST_EVIDENCE_AUTHORITY_PREFIX",
        "HistoricalFuturesAcceptedEvidenceRegistry",
        "HistoricalFuturesAcceptedFact",
        "HistoricalFuturesBoundarySemantics",
        "HistoricalFuturesDocumentIdentity",
        "HistoricalFuturesEvidenceCutoffSource",
        "HistoricalFuturesEvidenceError",
        "HistoricalFuturesEvidenceGate",
        "HistoricalFuturesEvidenceRefusedError",
        "HistoricalFuturesFactCategory",
        "HistoricalFuturesOwnerAcceptance",
        "HistoricalFuturesReferenceAuthority",
    ):
        assert name in market_data.__all__
        assert getattr(market_data, name) is not None
