"""Synthetic fixtures for the EI-4E end-to-end acceptance, on real SQLite.

EI-4E checks the whole chain on one disposable SQLite file, through the merged
use cases and adapters only:

    synthetic candles -> EI-2C ingestion -> EI-2B journal and snapshots
    -> EI-3A evaluation -> EI-3C timeline -> EI-4C generation (EI-4B Core)
    -> EI-4D-A recording and verification -> EI-4D-B SQLite store

The fixtures here are deliberately independent of the EI-3C and EI-4D-B test
fixtures: their own listing (TCS, with INFY as a quiet session), trading date
(2026-08-19), prices, research calibration, source, finality policy and
recommendation references. Only the accepted production NSE venue-day
reference is shared, because it is the accepted reference evidence; the test
confirms the day is a NORMAL_SESSION rather than assuming it.

Everything else is labelled SYNTHETIC TEST DATA:

- every price, volume, calibration, latency, cutoff, capital, cost rate and
  increment is invented for this test and approved for nothing;
- the listing applicability, listing eligibility and issued-recommendation
  history answers are test fixtures, never production authority. In
  particular, the "empty" issued history is a labelled synthetic history that
  is explicitly complete through 15:30 IST at recording watermark 0 -- never
  an empty history assumed because no issuer exists (EI4-D9-H, clarification
  D). Missing, unavailable and incomplete histories are separate modes.

Nothing here opens a network connection, reads a clock or writes a production
database.
"""

from __future__ import annotations

import os
import sqlite3
from contextlib import closing
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from northstar_application.application_services import (
    NSE_EQUITY_INTRADAY_V1_RESEARCH_WINDOW,
    AdvanceEquityIntradayResearchTimelineUseCase,
    EquityCandleEvidenceSnapshot,
    EquityCandleFinalityPolicy,
    EquityCandleJournalRecord,
    EquityIntradayResearchTimelineAdvance,
    EquityOpeningRangeBreakoutConfiguration,
    EquityRecommendationDispositionKind,
    EquityRecommendationEligibilityAudit,
    EquityRecommendationGenerationAttempt,
    EquityRecommendationGenerationDisposition,
    EquityRecommendationGenerationVerifier,
    EquityResearchEvidenceOutcome,
    EquityResolvedSyntheticPolicySet,
    EquitySyntheticPolicyBundle,
    EquitySyntheticPolicySetReference,
    EquityTradeRecommendationRequest,
    EvaluateEquityResearchWindowUseCase,
    GenerateEquityTradeRecommendationUseCase,
    IngestSyntheticEquityCandleEventsUseCase,
    IssueEquityCandleEvidenceSnapshotUseCase,
    IssueEquityResearchEvidenceUseCase,
    PublishEquitySyntheticPoliciesUseCase,
    RecordEquityRecommendationGenerationUseCase,
    VerifyStoredEquityRecommendationGenerationUseCase,
)
from northstar_application.ports import (
    EquityListingApplicabilityClassification,
    EquityPriorSessionRecommendation,
    EquityRecommendationHistoryUnavailableError,
    EquitySessionRecommendationHistory,
    EquitySessionRecommendationHistorySource,
    EquitySyntheticEligibilityCriterion,
    EquitySyntheticEligibilityFinding,
    EquitySyntheticEligibilityVerdict,
    EquitySyntheticEntryTimingPolicy,
    EquitySyntheticListingEligibility,
    EquitySyntheticListingEligibilitySource,
    EquitySyntheticRecommendationReferenceSource,
    EquitySyntheticReferenceUnavailableError,
)
from northstar_core.domain.value_objects import ListingReference
from northstar_core.foundation.value_objects import (
    Currency,
    ExchangeCode,
    Money,
    PointInTime,
    Symbol,
)
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

from northstar_infrastructure.market_data import (
    NSEEquityResearchReferenceProvenance,
    NSEEquityVenueDayReferenceSource,
    SyntheticEquityCandleEventSource,
    SyntheticListingApplicabilityRecord,
    SyntheticListingApplicabilitySource,
)
from northstar_infrastructure.persistence import (
    SQLiteEquityCandleEvidenceJournal,
    SQLiteEquityCandleEvidenceSnapshotStore,
    SQLiteEquityIntradayResearchTimelineStore,
    SQLiteEquityRecommendationGenerationStore,
    SQLiteEquitySyntheticPolicyCatalogStore,
    initialize_equity_candle_evidence_schema,
    initialize_equity_intraday_research_timeline_schema,
)

# ---------------------------------------------------------------------------
# The synthetic session (SYNTHETIC TEST DATA)
# ---------------------------------------------------------------------------

NSE = ExchangeCode("NSE")
INR = Currency("INR")
TCS = ListingReference(Symbol("TCS"), NSE)  # the session with a breakout
INFY = ListingReference(Symbol("INFY"), NSE)  # a quiet session: no breakout
WIPRO = ListingReference(Symbol("WIPRO"), NSE)  # appears only in synthetic issued history
DAY = date(2026, 8, 19)
IST = timezone(timedelta(hours=5, minutes=30))
SOURCE = "synthetic-ei4e-acceptance-source"
EVENTS_VERSION = "ei4e-acceptance/1"
FINALITY = EquityCandleFinalityPolicy("synthetic-ei4e-finality/1", frozenset({SOURCE}), True)
VENUE = NSEEquityVenueDayReferenceSource.production()  # accepted reference evidence
_ELIGIBLE_WINDOW = EquityListingApplicabilityClassification.ELIGIBLE_FOR_NORMAL_RESEARCH_WINDOW
APPLICABILITY = SyntheticListingApplicabilitySource(
    "synthetic-ei4e-applicability/1",
    tuple(
        SyntheticListingApplicabilityRecord(listing, DAY, _ELIGIBLE_WINDOW, "synthetic EI-4E")
        for listing in (TCS, INFY)
    ),
)
PROVENANCE = NSEEquityResearchReferenceProvenance(VENUE, APPLICABILITY)
CONFIG = EquityOpeningRangeBreakoutConfiguration(
    strategy=StrategyIdentity("ei4e-acceptance-orb-vwap-volume"),
    version="synthetic-ei4e-research/1",
    opening_range_bars=4,
    volume_multiplier=Decimal("1"),
    minimum_baseline_bars=3,
)

# Bars 1-4 (09:15-09:35 IST) form the opening range: high 252, low 249. Bar 5
# (ends 09:40) closes at 253.5, above it: the first qualifying decision, at the
# 09:41 snapshot. Bar 6 (ends 09:45) qualifies again. The source's volume rises
# every bar, so the volume condition holds whenever the price condition does.
SESSION: dict[int, tuple[str, str, str, str]] = {
    1: ("250", "251", "249", "250.5"),
    2: ("250.5", "252", "250", "251.5"),
    3: ("251.5", "252", "250.5", "251"),
    4: ("251", "251.8", "250.2", "251.2"),
    5: ("251.2", "254", "251", "253.5"),
    6: ("253.5", "255", "253", "254.5"),
}
# INFY never closes above its opening-range high (811).
QUIET_SESSION: dict[int, tuple[str, str, str, str]] = {
    index: ("810", "811", "809", "810.5") for index in range(1, 7)
}
CORRECTED_BAR_2 = ("250.5", "252.5", "250", "251.75")  # a later revision of bar 2


def pit(value: datetime) -> PointInTime:
    return PointInTime(value.astimezone(UTC).isoformat().replace("+00:00", "Z"))


def ist(hour: int, minute: int, second: int = 0, day: date = DAY) -> PointInTime:
    return pit(datetime(day.year, day.month, day.day, hour, minute, second, tzinfo=IST))


def bar_end(index: int, day: date = DAY) -> PointInTime:
    """Return the end of five-minute bar ``index``; bar 1 ends 09:20 IST."""
    return pit(
        datetime(day.year, day.month, day.day, 9, 15, tzinfo=IST) + timedelta(minutes=5 * index)
    )


def events(listing: ListingReference = TCS) -> SyntheticEquityCandleEventSource:
    return SyntheticEquityCandleEventSource(EVENTS_VERSION, SOURCE, listing)


class Clock:
    """Deterministic journal clock: one second per append, from a fixed instant."""

    def __init__(self) -> None:
        self.ticks = 0

    def __call__(self) -> datetime:
        self.ticks += 1
        return datetime(2026, 11, 8, tzinfo=UTC) + timedelta(seconds=self.ticks)


class Evidence:
    """Fresh EI-2B/EI-2C/EI-3C adapters and use cases over one database: a 'process'."""

    def __init__(self, database: Path, clock: Clock | None = None) -> None:
        self.database = database
        self.journal = SQLiteEquityCandleEvidenceJournal(database, clock=clock or Clock())
        self.snapshots = SQLiteEquityCandleEvidenceSnapshotStore(database)
        self.timelines = SQLiteEquityIntradayResearchTimelineStore(database)
        self.ingestion = IngestSyntheticEquityCandleEventsUseCase(self.journal, frozenset({SOURCE}))
        self.issuance = IssueEquityResearchEvidenceUseCase(
            EvaluateEquityResearchWindowUseCase(VENUE, APPLICABILITY),
            IssueEquityCandleEvidenceSnapshotUseCase(
                self.journal, self.snapshots, INR, FINALITY, reference_provenance=PROVENANCE
            ),
        )

    def candles(
        self,
        prices: dict[int, tuple[str, str, str, str]] = SESSION,
        listing: ListingReference = TCS,
    ) -> None:
        source = events(listing)
        records: list[EquityCandleJournalRecord] = []
        for index, values in sorted(prices.items()):
            end = bar_end(index)
            records += [source.observation(end, prices=values), source.finality(end)]
        self.ingestion.execute(records)

    def correct_bar_2(self) -> None:
        """Journal a later revision of TCS bar 2 with its finality: a source correction."""
        source, end = events(), bar_end(2)
        self.ingestion.execute(
            [
                source.observation(end, revision="r2", corrects="r1", prices=CORRECTED_BAR_2),
                source.finality(end, revision="r2"),
            ]
        )

    def issue(
        self, as_of: PointInTime, listing: ListingReference = TCS
    ) -> EquityCandleEvidenceSnapshot:
        issued = self.issuance.execute(listing, DAY, as_of, as_of)
        assert issued.outcome is EquityResearchEvidenceOutcome.ISSUED, issued.outcome
        assert issued.snapshot is not None
        return issued.snapshot

    def advance(
        self, snapshots: list[EquityCandleEvidenceSnapshot], listing: ListingReference = TCS
    ) -> EquityIntradayResearchTimelineAdvance:
        return AdvanceEquityIntradayResearchTimelineUseCase(
            self.timelines, self.snapshots, CONFIG
        ).execute(listing, DAY, [s.snapshot_id for s in snapshots])


# ---------------------------------------------------------------------------
# Synthetic recommendation references (SYNTHETIC TEST DATA, approved for nothing)
# ---------------------------------------------------------------------------


def v(name: str) -> EquitySyntheticVersion:
    return EquitySyntheticVersion(f"synthetic-ei4e-{name}")


def timing_policy(name: str, latency: timedelta) -> EquitySyntheticEntryTimingPolicy:
    """A synthetic timing policy: signal by 10:30, entry bar by 10:45, exit 14:00 IST."""
    return EquitySyntheticEntryTimingPolicy(
        version=v(name),
        exchange_code=NSE,
        research_window_policy_version=NSE_EQUITY_INTRADAY_V1_RESEARCH_WINDOW.version,
        latest_signal_bar_end=time(10, 30),
        latest_entry_bar_opening=time(10, 45),
        modeled_exit_boundary=time(14, 0),
        minimum_remaining_session=timedelta(minutes=60),
        modeled_issuance_latency=latency,
    )


def generation_policy(name: str, minimum_reward_to_risk: str) -> EquitySyntheticGenerationPolicy:
    return EquitySyntheticGenerationPolicy(
        v(name), Decimal("0.005"), Decimal("0"), Decimal("2"), Decimal(minimum_reward_to_risk)
    )


class References(EquitySyntheticRecommendationReferenceSource):
    """Synthetic policy components held by version; an unknown version answers None.

    ``available = False`` makes every answer an outage. ``asked`` counts calls,
    so a test can show that replay never consults this source.
    """

    def __init__(self) -> None:
        def side(label: str, which: EquityCostSide, rate: str) -> EquitySyntheticCostComponent:
            return EquitySyntheticCostComponent(
                label, which, Decimal(rate), Decimal(0), Decimal(20)
            )

        self.available = True
        self.asked = 0
        self.timing: dict[str, EquitySyntheticEntryTimingPolicy] = {
            # 09:41 decision + 150 s = 09:43:30, between the 09:40 and 09:45 openings.
            "timing/1": timing_policy("timing/1", timedelta(seconds=150)),
            # 09:41 + 4 min = 09:45 exactly: a modeled issuance on a bar opening.
            "timing/on-opening": timing_policy("timing/on-opening", timedelta(minutes=4)),
            # 09:41 + 65 min = 10:46, after the latest entry-bar opening 10:45.
            "timing/late": timing_policy("timing/late", timedelta(minutes=65)),
        }
        self.generation = {
            "generation/1": generation_policy("generation/1", "1.5"),
            "generation/strict": generation_policy("generation/strict", "50"),
        }
        self.risk = {
            "risk/1": EquitySyntheticRiskPolicy(
                v("risk/1"), Money(Decimal("500000"), INR), Decimal("0.005"), Decimal("0.25")
            )
        }
        self.costs = {
            "costs/1": EquitySyntheticCostSchedule(
                v("costs/1"),
                INR,
                (
                    side("synthetic-ei4e-entry", EquityCostSide.ENTRY, "0.0004"),
                    side("synthetic-ei4e-exit", EquityCostSide.EXIT, "0.0006"),
                ),
                Decimal("0.0003"),
                Decimal("0.01"),
            )
        }
        self.increment = {"increment/1": EquityPriceIncrement(Decimal("0.05"), v("increment/1"))}

    def _held(self, held: dict[str, Any], version: EquitySyntheticVersion) -> Any:
        self.asked += 1
        if not self.available:
            raise EquitySyntheticReferenceUnavailableError("synthetic EI-4E reference outage")
        return held.get(version.value.removeprefix("synthetic-ei4e-"))

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
    """Synthetic listing eligibility: ELIGIBLE on every criterion unless ``verdicts`` says so."""

    def __init__(self) -> None:
        self.available = True
        self.asked = 0
        self.verdicts: dict[
            EquitySyntheticEligibilityCriterion, EquitySyntheticEligibilityVerdict
        ] = {}
        self.universe = "universe/1"

    def eligibility(
        self, version: EquitySyntheticVersion, listing: ListingReference, trading_date: date
    ) -> EquitySyntheticListingEligibility:
        self.asked += 1
        if not self.available:
            raise EquitySyntheticReferenceUnavailableError("synthetic EI-4E eligibility outage")
        return EquitySyntheticListingEligibility(
            version=version,
            universe_version=v(self.universe),
            listing=listing,
            trading_date=trading_date,
            findings=tuple(
                EquitySyntheticEligibilityFinding(
                    criterion,
                    self.verdicts.get(criterion, EquitySyntheticEligibilityVerdict.ELIGIBLE),
                    "synthetic EI-4E test fixture",
                )
                for criterion in EquitySyntheticEligibilityCriterion
            ),
            provenance="synthetic-ei4e-eligibility-fixture",
        )


def prior(
    key: str, listing: ListingReference, issued_at: PointInTime, sequence: int
) -> EquityPriorSessionRecommendation:
    """One labelled synthetic issued-history record (never an actual issuance)."""
    return EquityPriorSessionRecommendation(
        f"synthetic-ei4e-issued-{key}",
        StrategyIdentity("ei4e-synthetic-other-strategy"),
        listing,
        DAY,
        EquityIssuanceKind.MODELED,
        issued_at,
        sequence,
    )


@dataclass
class History(EquitySessionRecommendationHistorySource):
    """A labelled synthetic issued-recommendation history (EI4-D9-H).

    ``mode`` is one of:

    - ``"established"``: the records below, complete through ``complete_through``
      at ``watermark`` (default: the highest recording sequence, or 0);
    - ``"missing"``: the source holds no record -- None, never empty;
    - ``"unavailable"``: the source cannot answer;
    - ``"other-session"``: an answer for another trading date.

    It answers; nothing here is ever written by EI-4D.
    """

    mode: str = "established"
    records: tuple[EquityPriorSessionRecommendation, ...] = ()
    complete_through: PointInTime = field(default_factory=lambda: ist(15, 30))
    watermark: int | None = None
    reference: str = "synthetic-ei4e-issued-history/1"
    asked: list[PointInTime] = field(default_factory=list)

    def session_history(
        self, exchange_code: ExchangeCode, trading_date: date, as_of: PointInTime
    ) -> EquitySessionRecommendationHistory | None:
        self.asked.append(as_of)
        if self.mode == "unavailable":
            raise EquityRecommendationHistoryUnavailableError("synthetic EI-4E history outage")
        if self.mode == "missing":
            return None
        day = trading_date if self.mode == "established" else trading_date + timedelta(days=1)
        watermark = self.watermark
        if watermark is None:
            watermark = max((r.recording_sequence for r in self.records), default=0)
        return EquitySessionRecommendationHistory(
            exchange_code,
            day,
            tuple(replace(r, trading_date=day) for r in self.records),
            self.complete_through,
            watermark,
            self.reference,
        )


def policy_set(
    set_version: str = "set/1", generation: str = "generation/1", timing: str = "timing/1"
) -> EquitySyntheticPolicySetReference:
    return EquitySyntheticPolicySetReference(
        v(set_version),
        generation_policy_version=v(generation),
        risk_policy_version=v("risk/1"),
        cost_schedule_version=v("costs/1"),
        price_increment_version=v("increment/1"),
        timing_policy_version=v(timing),
    )


# The synthetic policy sets the acceptance publishes (EI4-D10): every set label
# names exactly one set of component labels, and every component label one
# content. A set naming other components, or other content, is another label.
PUBLISHED_POLICY_SETS: tuple[tuple[str, str, str], ...] = (
    # (set label, generation label, timing label)
    ("set/1", "generation/1", "timing/1"),
    ("set/2", "generation/1", "timing/1"),
    ("set/strict", "generation/strict", "timing/1"),
    ("set/late", "generation/1", "timing/late"),
    ("set/on-opening", "generation/1", "timing/on-opening"),
    ("set/timing/1", "generation/1", "timing/1"),
    ("set/timing/on-opening", "generation/1", "timing/on-opening"),
    ("set/timing/late", "generation/1", "timing/late"),
)


def policy_bundle(
    references: References,
    set_version: str = "set/1",
    generation: str = "generation/1",
    timing: str = "timing/1",
) -> EquitySyntheticPolicyBundle:
    """The complete canonical bundle -- five components and the set -- for one set label."""
    return EquitySyntheticPolicyBundle.of(
        EquityResolvedSyntheticPolicySet(
            reference=policy_set(set_version, generation, timing),
            generation_policy=references.generation[generation],
            risk_policy=references.risk["risk/1"],
            cost_schedule=references.costs["costs/1"],
            price_increment=references.increment["increment/1"],
            timing_policy=references.timing[timing],
        )
    )


def request(
    listing: ListingReference = TCS,
    configuration: EquityOpeningRangeBreakoutConfiguration = CONFIG,
    **policy: str,
) -> EquityTradeRecommendationRequest:
    return EquityTradeRecommendationRequest(
        configuration=configuration,
        listing=listing,
        trading_date=DAY,
        policy_set=policy_set(**policy),
        listing_eligibility_version=v("eligibility/1"),
    )


# ---------------------------------------------------------------------------
# The pipeline over one database file
# ---------------------------------------------------------------------------


class Pipeline:
    """The real EI-4C/EI-4D chain over one disposable database.

    Every call builds fresh adapters, which open and close their own
    connections per operation; nothing is cached between calls. The policy
    catalog (EI4-D10) is the SQLite catalog in the SAME file: the generator
    and the recorder read it, and only ``publish`` -- the S1 use case -- writes
    it. ``catalog`` may name another file only for a labelled test that needs
    a catalog other than the authority.
    """

    def __init__(self, database: Path) -> None:
        self.database = database
        self.references = References()
        self.eligibility = Eligibility()
        self.history = History()

    def evidence(self) -> Evidence:
        return Evidence(self.database)

    def catalog(self) -> SQLiteEquitySyntheticPolicyCatalogStore:
        return SQLiteEquitySyntheticPolicyCatalogStore(self.database)

    def publish(self, *bundles: EquitySyntheticPolicyBundle) -> None:
        """Publish each complete bundle explicitly, through the S1 use case; nothing else does."""
        publisher = PublishEquitySyntheticPoliciesUseCase(self.catalog())
        for bundle in bundles:
            result = publisher.execute(bundle)
            assert result.is_published, (result.outcome, result.conflicts)

    def publish_fixture_policies(self) -> None:
        """Publish every synthetic policy set of PUBLISHED_POLICY_SETS."""
        self.publish(*(policy_bundle(self.references, *row) for row in PUBLISHED_POLICY_SETS))

    def generator(
        self, catalog: SQLiteEquitySyntheticPolicyCatalogStore | None = None
    ) -> GenerateEquityTradeRecommendationUseCase:
        fresh = self.evidence()
        return GenerateEquityTradeRecommendationUseCase(
            fresh.timelines,
            fresh.snapshots,
            VENUE,
            self.references,
            self.eligibility,
            self.history,
            catalog or self.catalog(),
        )

    def generate(self, **changes: Any) -> EquityRecommendationEligibilityAudit:
        return self.generator().execute(request(**changes))

    def store(self) -> SQLiteEquityRecommendationGenerationStore:
        return SQLiteEquityRecommendationGenerationStore(self.database)

    def verifier(self) -> EquityRecommendationGenerationVerifier:
        fresh = self.evidence()
        return EquityRecommendationGenerationVerifier(fresh.timelines, fresh.snapshots)

    def recorder(
        self,
        store: SQLiteEquityRecommendationGenerationStore | None = None,
        max_writes: int = 3,
        catalog: SQLiteEquitySyntheticPolicyCatalogStore | None = None,
    ) -> RecordEquityRecommendationGenerationUseCase:
        return RecordEquityRecommendationGenerationUseCase(
            store or self.store(), self.verifier(), catalog or self.catalog(), max_writes
        )

    def verify_stored(self) -> VerifyStoredEquityRecommendationGenerationUseCase:
        return VerifyStoredEquityRecommendationGenerationUseCase(self.store(), self.verifier())


@dataclass(frozen=True)
class Research:
    """The committed synthetic research of the TCS session, as the tests established it."""

    s36: EquityCandleEvidenceSnapshot  # NO_SIGNAL: the opening range only
    s41: EquityCandleEvidenceSnapshot  # the first CANDIDATE
    s46: EquityCandleEvidenceSnapshot  # REPEAT_QUALIFYING
    advance: EquityIntradayResearchTimelineAdvance


def establish_research(pipeline: Pipeline, *, publish: bool = True) -> Research:
    """Steps 1-4: journal the TCS session, issue snapshots, commit the EI-3C timeline.

    Then, explicitly, step 5 (EI4-D10): the synthetic policy sets are published
    into the catalog of the same database through the S1 use case, before
    anything is generated. ``publish=False`` leaves the catalog empty, for
    tests of a database from before enforcement or of unpublished policies.
    """
    evidence = pipeline.evidence()
    evidence.candles()
    s36, s41, s46 = (evidence.issue(ist(9, minute)) for minute in (36, 41, 46))
    advanced = evidence.advance([s36, s41, s46])
    assert advanced.status.value == "COMMITTED" and advanced.commit_confirmed, advanced.status
    if publish:
        pipeline.publish_fixture_policies()
    return Research(s36, s41, s46, advanced)


def pre_enforcement_catalog(
    pipeline: Pipeline, path: Path
) -> SQLiteEquitySyntheticPolicyCatalogStore:
    """TEST-ONLY HISTORICAL SIMULATION: the catalog of a record made before EI4-D10.

    Before enforcement, no catalog existed in the evidence database. The S3
    generator and recorder require one, so a record "from before" is made
    through the ordinary, gated chain against a catalog in ANOTHER disposable
    file, where the fixture policies are published. The database under test
    keeps an empty catalog, so its record carries no publication evidence.
    Never used for a record the test treats as made under enforcement.
    """
    return scratch_catalog(
        path, *(policy_bundle(pipeline.references, *row) for row in PUBLISHED_POLICY_SETS)
    )


def scratch_catalog(
    path: Path, *bundles: EquitySyntheticPolicyBundle
) -> SQLiteEquitySyntheticPolicyCatalogStore:
    """TEST-ONLY: a real SQLite catalog in ANOTHER disposable file -- never the authority.

    ``bundles`` are published into it through the S1 use case. A generator
    wired to it stands for a caller that bypasses the authoritative catalog,
    or for a time before enforcement; the tests say which.
    """
    with closing(sqlite3.connect(path)) as connection:
        initialize_equity_candle_evidence_schema(connection)
        initialize_equity_intraday_research_timeline_schema(connection)
    catalog = SQLiteEquitySyntheticPolicyCatalogStore(path)
    publisher = PublishEquitySyntheticPoliciesUseCase(catalog)
    for bundle in bundles:
        assert publisher.execute(bundle).is_published
    return catalog


# ---------------------------------------------------------------------------
# Direct inspection of the disposable database
# ---------------------------------------------------------------------------

DISPOSITIONS = "equity_recommendation_generation_dispositions"
ATTEMPTS = "equity_recommendation_generation_attempts"
ENTRIES = "equity_intraday_research_timeline_entries"
TIMELINES = "equity_intraday_research_timelines"
SNAPSHOTS = "equity_candle_evidence_snapshots"


def raw(database: Path) -> sqlite3.Connection:
    """A direct connection with foreign keys on, as the adapters use."""
    connection = sqlite3.connect(database, isolation_level=None)
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def rows(database: Path, table: str) -> list[dict[str, Any]]:
    with closing(raw(database)) as connection:
        connection.row_factory = sqlite3.Row
        return [dict(r) for r in connection.execute(f"SELECT * FROM {table} ORDER BY rowid")]  # noqa: S608


def counts(database: Path) -> tuple[int, int]:
    """(dispositions, attempts)."""
    with closing(raw(database)) as connection:
        return tuple(  # type: ignore[return-value]
            connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]  # noqa: S608
            for table in (DISPOSITIONS, ATTEMPTS)
        )


def schema_objects(database: Path) -> dict[str, tuple[str, str]]:
    with closing(raw(database)) as connection:
        return {
            name: (kind, sql)
            for kind, name, sql in connection.execute(
                "SELECT type, name, sql FROM sqlite_master WHERE sql IS NOT NULL"
            )
        }


def check_database(database: Path) -> None:
    """SQLite's own integrity and foreign-key checks."""
    with closing(raw(database)) as connection:
        assert connection.execute("PRAGMA integrity_check").fetchall() == [("ok",)]
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []


def bypass(
    database: Path,
    *statements: tuple[str, tuple[object, ...]],
    drop: tuple[str, ...] = (),
    checks: bool = True,
    foreign_keys: bool = True,
) -> None:
    """TEST-ONLY corruption of a disposable database: drop triggers, relax checks, write.

    It never counts as evidence of the normal workflow; the adapter's next
    connection recreates any dropped trigger.
    """
    with closing(sqlite3.connect(database, isolation_level=None)) as connection:
        connection.execute(f"PRAGMA foreign_keys = {'ON' if foreign_keys else 'OFF'}")
        if not checks:
            connection.execute("PRAGMA ignore_check_constraints = ON")
        for trigger in drop:
            connection.execute(f"DROP TRIGGER IF EXISTS {trigger}")
        for statement, parameters in statements:
            connection.execute(statement, parameters)


# ---------------------------------------------------------------------------
# A separate OS process (spawned; Windows-compatible)
# ---------------------------------------------------------------------------

RecordDocuments = tuple[tuple[str, str, str, str, str, str | None, str | None], tuple[str, str]]


def documents(
    disposition: EquityRecommendationGenerationDisposition,
    attempt: EquityRecommendationGenerationAttempt,
) -> RecordDocuments:
    """The plain stored text of a disposition and its attempt, to hand to another process."""
    return (
        (
            disposition.generation_key,
            disposition.kind.value,
            disposition.evaluator_version,
            disposition.assessment_fingerprint,
            disposition.assessment_document,
            disposition.proposal_fingerprint,
            disposition.proposal_document,
        ),
        (attempt.attempt_identity, attempt.attempt_document),
    )


def record_in_another_process(
    database: str, records: RecordDocuments, start: Any, results: Any, name: str
) -> None:
    """Spawned worker: rebuild the records, wait at ``start``, record through EI-4D-A.

    It constructs its own adapters, connections and Application use cases from
    the database path alone -- nothing is shared with the parent but the file.
    """
    (key, kind, evaluator, fingerprint, document, proposal_fingerprint, proposal), attempt = records
    disposition = EquityRecommendationGenerationDisposition(
        key,
        EquityRecommendationDispositionKind(kind),
        evaluator,
        fingerprint,
        document,
        proposal_fingerprint,
        proposal,
    )
    path = Path(database)
    recorder = RecordEquityRecommendationGenerationUseCase(
        SQLiteEquityRecommendationGenerationStore(path),
        EquityRecommendationGenerationVerifier(
            SQLiteEquityIntradayResearchTimelineStore(path),
            SQLiteEquityCandleEvidenceSnapshotStore(path),
        ),
        SQLiteEquitySyntheticPolicyCatalogStore(path),
    )
    built = EquityRecommendationGenerationAttempt(*attempt)
    start.wait(60)
    try:
        result = recorder.record(disposition, built)
        results.put((name, os.getpid(), result.outcome.value, result.attempt_recorded, None))
    except BaseException as exc:  # noqa: BLE001 -- reported to the parent, never swallowed
        results.put((name, os.getpid(), None, None, repr(exc)))
