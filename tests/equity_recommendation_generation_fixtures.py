"""Shared synthetic fixtures for the EI-4D-B generation-store tests.

The chain is the real one over one temporary SQLite file: EI-2B evidence and
EI-3C timelines from tests/equity_intraday_research_timeline_fixtures.py, the
EI-4C generator, and the EI-4D-A recorder and verifier over the EI-4D-B
SQLite store. The policy, eligibility and issued-history sources below are
labelled synthetic TEST fixtures: every version, price and parameter is
invented, none is approved, and the issued history they state complete is a
test fixture establishing a history -- never production authority, never an
empty history asserted because no issuer exists (EI4-D9-H).
"""

from __future__ import annotations

import dataclasses
from datetime import date, time, timedelta
from decimal import Decimal
from pathlib import Path

from northstar_application.application_services import (
    NSE_EQUITY_INTRADAY_V1_RESEARCH_WINDOW,
    EquityRecommendationEligibilityAudit,
    EquityRecommendationGenerationAttempt,
    EquityRecommendationGenerationDisposition,
    EquityRecommendationGenerationStatus,
    EquityRecommendationGenerationVerifier,
    EquitySyntheticPolicySetReference,
    EquityTradeRecommendationRequest,
    GenerateEquityTradeRecommendationUseCase,
    RecordEquityRecommendationGenerationUseCase,
    equity_recommendation_attempt,
    equity_recommendation_disposition,
)
from northstar_application.ports import (
    EquityPriorSessionRecommendation,
    EquitySessionRecommendationHistory,
    EquitySessionRecommendationHistorySource,
    EquitySyntheticEligibilityCriterion,
    EquitySyntheticEligibilityFinding,
    EquitySyntheticEligibilityVerdict,
    EquitySyntheticEntryTimingPolicy,
    EquitySyntheticListingEligibility,
    EquitySyntheticListingEligibilitySource,
    EquitySyntheticRecommendationReferenceSource,
)
from northstar_core.domain.value_objects import ListingReference
from northstar_core.foundation.value_objects import ExchangeCode, Money, PointInTime
from northstar_core.recommendations.equity import (
    EquityCostSide,
    EquityIssuanceKind,
    EquityPriceIncrement,
    EquitySyntheticCostComponent,
    EquitySyntheticCostSchedule,
    EquitySyntheticGenerationPolicy,
    EquitySyntheticRiskPolicy,
    EquitySyntheticVersion,
)
from northstar_core.strategy import StrategyIdentity

from northstar_infrastructure.persistence import SQLiteEquityRecommendationGenerationStore
from tests.equity_intraday_research_timeline_fixtures import (
    CONFIG,
    DAY,
    INR,
    M_AND_M,
    NSE,
    VENUE,
    Harness,
    ist,
)

DISPOSITIONS = "equity_recommendation_generation_dispositions"
ATTEMPTS = "equity_recommendation_generation_attempts"
FUTURE_EVALUATOR = "northstar.equity-recommendation-evaluator/2"
_STATUS = EquityRecommendationGenerationStatus


def v(name: str) -> EquitySyntheticVersion:
    return EquitySyntheticVersion(f"synthetic-ei4db-{name}")


class References(EquitySyntheticRecommendationReferenceSource):
    """Synthetic test policies held by version; an unknown version answers None."""

    def __init__(self) -> None:
        def side(label: str, which: EquityCostSide, rate: str) -> EquitySyntheticCostComponent:
            return EquitySyntheticCostComponent(label, which, Decimal(rate), Decimal(0), Decimal(0))

        self.timing = {
            "timing/1": EquitySyntheticEntryTimingPolicy(
                version=v("timing/1"),
                exchange_code=NSE,
                research_window_policy_version=NSE_EQUITY_INTRADAY_V1_RESEARCH_WINDOW.version,
                latest_signal_bar_end=time(10, 30),
                latest_entry_bar_opening=time(10, 45),
                modeled_exit_boundary=time(14, 0),
                minimum_remaining_session=timedelta(minutes=60),
                modeled_issuance_latency=timedelta(0),
            )
        }
        self.generation = {
            name: EquitySyntheticGenerationPolicy(
                v(name), Decimal("0.01"), Decimal("0"), Decimal("2"), Decimal(minimum)
            )
            for name, minimum in (("generation/1", "1.5"), ("generation/strict", "100"))
        }
        self.risk = {
            "risk/1": EquitySyntheticRiskPolicy(
                v("risk/1"), Money(Decimal("100000"), INR), Decimal("0.01"), Decimal("0.2")
            )
        }
        self.costs = {
            "costs/1": EquitySyntheticCostSchedule(
                v("costs/1"),
                INR,
                (
                    side("synthetic-entry", EquityCostSide.ENTRY, "0.0003"),
                    side("synthetic-exit", EquityCostSide.EXIT, "0.0005"),
                ),
                Decimal("0.0002"),
                Decimal("0.01"),
            )
        }
        self.increment = {"increment/1": EquityPriceIncrement(Decimal("0.05"), v("increment/1"))}

    @staticmethod
    def _held(held: dict, version: EquitySyntheticVersion):  # type: ignore[no-untyped-def,type-arg]
        return held.get(version.value.removeprefix("synthetic-ei4db-"))

    def timing_policy(self, version: EquitySyntheticVersion):  # type: ignore[no-untyped-def]
        return self._held(self.timing, version)

    def generation_policy(self, version: EquitySyntheticVersion):  # type: ignore[no-untyped-def]
        return self._held(self.generation, version)

    def risk_policy(self, version: EquitySyntheticVersion):  # type: ignore[no-untyped-def]
        return self._held(self.risk, version)

    def cost_schedule(self, version: EquitySyntheticVersion):  # type: ignore[no-untyped-def]
        return self._held(self.costs, version)

    def price_increment(  # type: ignore[no-untyped-def]
        self, version: EquitySyntheticVersion, listing: ListingReference, trading_date: date
    ):
        return self._held(self.increment, version)


class Eligibility(EquitySyntheticListingEligibilitySource):
    """Synthetic test eligibility: every criterion ELIGIBLE under the requested version."""

    def eligibility(
        self, version: EquitySyntheticVersion, listing: ListingReference, trading_date: date
    ) -> EquitySyntheticListingEligibility:
        return EquitySyntheticListingEligibility(
            version=version,
            universe_version=v("universe/1"),
            listing=listing,
            trading_date=trading_date,
            findings=tuple(
                EquitySyntheticEligibilityFinding(
                    criterion, EquitySyntheticEligibilityVerdict.ELIGIBLE, "synthetic test fixture"
                )
                for criterion in EquitySyntheticEligibilityCriterion
            ),
            provenance="synthetic-ei4db-test-fixture",
        )


class History(EquitySessionRecommendationHistorySource):
    """A labelled synthetic test issued history, complete through 15:30 IST.

    It answers; it is never written. ``prior`` adds one earlier synthetic
    recommendation on the same listing, which leaves exclusivity unresolved.
    """

    def __init__(self) -> None:
        self.prior = False
        self.asked: list[PointInTime] = []

    def session_history(
        self, exchange_code: ExchangeCode, trading_date: date, as_of: PointInTime
    ) -> EquitySessionRecommendationHistory:
        self.asked.append(as_of)
        records = (
            (
                EquityPriorSessionRecommendation(
                    "synthetic-ei4db-prior-1",
                    StrategyIdentity("ei4db-other"),
                    M_AND_M,
                    trading_date,
                    EquityIssuanceKind.MODELED,
                    ist(9, 20),
                    1,
                ),
            )
            if self.prior
            else ()
        )
        return EquitySessionRecommendationHistory(
            exchange_code,
            trading_date,
            records,
            ist(15, 30, day=trading_date),
            len(records),
            "synthetic-ei4db-test-history/1",
        )


def request(
    generation: str = "generation/1",
    set_version: str = "set/1",
    listing: ListingReference = M_AND_M,
    day: date = DAY,
) -> EquityTradeRecommendationRequest:
    return EquityTradeRecommendationRequest(
        configuration=CONFIG,
        listing=listing,
        trading_date=day,
        policy_set=EquitySyntheticPolicySetReference(
            v(set_version),
            generation_policy_version=v(generation),
            risk_policy_version=v("risk/1"),
            cost_schedule_version=v("costs/1"),
            price_increment_version=v("increment/1"),
            timing_policy_version=v("timing/1"),
        ),
        listing_eligibility_version=v("eligibility/1"),
    )


Records = tuple[EquityRecommendationGenerationDisposition, EquityRecommendationGenerationAttempt]


class World:
    """One disposable evidence database with a committed EI-3C timeline.

    M&M's synthetic morning is journaled; snapshots at 09:36 and 09:41 IST are
    committed as the CANDIDATE (entry 1) and a REPEAT_QUALIFYING entry (2).
    """

    def __init__(self, database: Path) -> None:
        self.database = database
        self.harness = Harness(database)
        self.harness.candles()
        self.s36, self.s41 = self.harness.at(36, 41)
        self.harness.advance([self.s36, self.s41])
        self.references = References()
        self.eligibility = Eligibility()
        self.history = History()

    def generator(self) -> GenerateEquityTradeRecommendationUseCase:
        fresh = Harness(self.database)
        return GenerateEquityTradeRecommendationUseCase(
            fresh.timelines,
            fresh.snapshots,
            VENUE,
            self.references,
            self.eligibility,
            self.history,
        )

    def run(self, **changes: object) -> EquityRecommendationEligibilityAudit:
        return self.generator().execute(request(**changes))  # type: ignore[arg-type]

    def proposed(self) -> EquityRecommendationEligibilityAudit:
        audit = self.run()
        assert audit.status is _STATUS.PROPOSED, audit.reasons
        return audit

    def not_eligible(self) -> EquityRecommendationEligibilityAudit:
        """Another approved policy set -- another key -- whose Core formation is refused."""
        audit = self.run(generation="generation/strict", set_version="set/strict")
        assert audit.status is _STATUS.NOT_ELIGIBLE, audit.reasons
        return audit

    def unresolved(self) -> EquityRecommendationEligibilityAudit:
        """The PROPOSED request's key, with other history content: exclusivity unresolved."""
        self.history.prior = True
        try:
            audit = self.run()
        finally:
            self.history.prior = False
        assert audit.status is _STATUS.EVALUATION_UNRESOLVED, audit.reasons
        return audit

    def attempt_only(self) -> EquityRecommendationEligibilityAudit:
        """A request whose listing has no timeline: no candidate, no generation key."""
        audit = self.run(day=date(2026, 9, 7))
        assert audit.generation_key is None and equity_recommendation_disposition(audit) is None
        return audit

    def store(self) -> SQLiteEquityRecommendationGenerationStore:
        return SQLiteEquityRecommendationGenerationStore(self.database)

    def verifier(self) -> EquityRecommendationGenerationVerifier:
        fresh = Harness(self.database)
        return EquityRecommendationGenerationVerifier(fresh.timelines, fresh.snapshots)

    def recorder(
        self,
        store: SQLiteEquityRecommendationGenerationStore | None = None,
        max_writes: int = 3,
    ) -> RecordEquityRecommendationGenerationUseCase:
        return RecordEquityRecommendationGenerationUseCase(
            store or self.store(), self.verifier(), max_writes
        )


def records(audit: EquityRecommendationEligibilityAudit) -> Records:
    disposition = equity_recommendation_disposition(audit)
    assert disposition is not None
    return disposition, equity_recommendation_attempt(audit)


def relabelled(
    disposition: EquityRecommendationGenerationDisposition, evaluator: str = FUTURE_EVALUATOR
) -> EquityRecommendationGenerationDisposition:
    """The same content under another evaluator label: needs that label to be supported."""
    return dataclasses.replace(disposition, evaluator_version=evaluator)
