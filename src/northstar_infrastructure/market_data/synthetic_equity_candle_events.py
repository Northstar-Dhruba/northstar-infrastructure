"""Deterministic synthetic 5-minute equity candle and finality events (EI-2C).

``SyntheticEquityCandleEventSource`` invents EI-2B journal records for one
listing from explicit parameters only. They are test inputs, never NSE or
provider data, and nothing here reads a clock, file or network.

Every record is marked synthetic (``is_synthetic`` on the observation and on
its finality evidence), and its identities are derived from the version, the
listing, the interval end and the revision, so the same call always produces
the same record:

- source event ``<version>:<symbol>@<exchange>:<interval end>``;
- ingestion ids ``<event>/observation/<revision>`` and
  ``<event>/finality/<revision>``;
- prices from the interval end's minute of day ``m``: open ``1500 + m/20``,
  high open+2, low open-1.5, close open+0.75, volume ``10000 + m``, unless
  explicit prices are given;
- receipt ``receipt_lag`` after the interval end; finality confirmed
  ``confirmation_lag`` after it and received ``finality_receipt_lag`` later,
  unless explicit times are given.

Generating an event does not journal, finalize or approve it.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from northstar_application.application_services import (
    EquityCandleFinalityEvidence,
    EquityCandleFinalityRecord,
    EquityCandleJournalRecord,
    EquityCandleObservation,
    EquityCandleObservationRecord,
)
from northstar_core.domain.value_objects import ListingReference
from northstar_core.foundation.value_objects import (
    Currency,
    PointInTime,
    Price,
    Quantity,
    Timeframe,
)
from northstar_core.market_data import HistoricalOHLCVBar


class InvalidSyntheticEquityCandleEventError(ValueError):
    """Raised when synthetic event parameters are malformed."""


def _instant(value: datetime) -> PointInTime:
    return PointInTime(value.astimezone(UTC).isoformat().replace("+00:00", "Z"))


def _datetime(value: PointInTime) -> datetime:
    return datetime.fromisoformat(value.value.replace("Z", "+00:00"))


@dataclass(frozen=True, slots=True)
class SyntheticEquityCandleEventSource:
    """Invents deterministic synthetic candle and finality records for one listing."""

    version: str
    source_id: str
    listing: ListingReference
    currency: Currency = Currency("INR")
    timeframe: Timeframe = Timeframe("5m")
    receipt_lag: timedelta = timedelta(seconds=5)
    confirmation_lag: timedelta = timedelta(seconds=10)
    finality_receipt_lag: timedelta = timedelta(seconds=1)
    evidence_reference: str = "synthetic finality statement"

    def __post_init__(self) -> None:
        for name in ("version", "source_id", "evidence_reference"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise InvalidSyntheticEquityCandleEventError(f"{name} must be non-empty text.")
        if not isinstance(self.listing, ListingReference):
            raise InvalidSyntheticEquityCandleEventError("listing must be a ListingReference.")
        for name in ("receipt_lag", "confirmation_lag", "finality_receipt_lag"):
            lag = getattr(self, name)
            if not isinstance(lag, timedelta) or lag < timedelta(0):
                raise InvalidSyntheticEquityCandleEventError(f"{name} must be >= 0.")

    def source_event_id(self, interval_end: PointInTime) -> str:
        """Return the synthetic source event identity of the candle ending at ``interval_end``."""
        symbol, exchange = self.listing.symbol.value, self.listing.exchange_code.value
        return f"{self.version}:{symbol}@{exchange}:{interval_end.value}"

    def _prices(self, interval_end: PointInTime) -> tuple[str, str, str, str]:
        end = _datetime(interval_end)
        opening = Decimal(1500) + Decimal(end.hour * 60 + end.minute) / Decimal(20)
        return tuple(  # type: ignore[return-value]
            format(opening + Decimal(delta), "f") for delta in ("0", "2", "-1.5", "0.75")
        )

    def observation(
        self,
        interval_end: PointInTime,
        *,
        revision: str = "r1",
        corrects: str | None = None,
        prices: tuple[str, str, str, str] | None = None,
        received_at: PointInTime | None = None,
        published_at: PointInTime | None = None,
    ) -> EquityCandleObservationRecord:
        """Return the synthetic observation record of the candle ending at ``interval_end``."""
        event = self.source_event_id(interval_end)
        o, h, low, c = (
            Price(Decimal(p), self.currency) for p in prices or self._prices(interval_end)
        )
        minute = _datetime(interval_end)
        bar = HistoricalOHLCVBar(
            symbol=self.listing.symbol,
            exchange_code=self.listing.exchange_code,
            point_in_time=interval_end,
            timeframe=self.timeframe,
            open=o,
            high=h,
            low=low,
            close=c,
            volume=Quantity(Decimal(10000 + minute.hour * 60 + minute.minute)),
        )
        return EquityCandleObservationRecord(
            ingestion_id=f"{event}/observation/{revision}",
            observation=EquityCandleObservation(
                bar=bar,
                source_id=self.source_id,
                source_event_id=event,
                source_revision=revision,
                received_at=received_at or _instant(_datetime(interval_end) + self.receipt_lag),
                finality=None,
                corrects_revision=corrects,
            ),
            is_synthetic=True,
            source_published_at=published_at,
        )

    def finality(
        self,
        interval_end: PointInTime,
        *,
        revision: str = "r1",
        confirmed_at: PointInTime | None = None,
        received_at: PointInTime | None = None,
    ) -> EquityCandleFinalityRecord:
        """Return the synthetic finality statement for one candle revision."""
        event = self.source_event_id(interval_end)
        confirmed = confirmed_at or _instant(_datetime(interval_end) + self.confirmation_lag)
        return EquityCandleFinalityRecord(
            ingestion_id=f"{event}/finality/{revision}",
            evidence=EquityCandleFinalityEvidence(
                source_id=self.source_id,
                source_event_id=event,
                source_revision=revision,
                confirmed_at=confirmed,
                evidence_reference=self.evidence_reference,
                is_synthetic=True,
            ),
            received_at=received_at or _instant(_datetime(confirmed) + self.finality_receipt_lag),
        )

    def session(
        self,
        interval_ends: Iterable[PointInTime],
        *,
        without_finality: frozenset[PointInTime] = frozenset(),
        without_observation: frozenset[PointInTime] = frozenset(),
    ) -> tuple[EquityCandleJournalRecord, ...]:
        """Return each interval's observation then finality, in the order given.

        Ends in ``without_finality`` or ``without_observation`` omit that record;
        nothing is fabricated for them.
        """
        events: list[EquityCandleJournalRecord] = []
        for end in interval_ends:
            if end not in without_observation:
                events.append(self.observation(end))
            if end not in without_finality and end not in without_observation:
                events.append(self.finality(end))
        return tuple(events)
