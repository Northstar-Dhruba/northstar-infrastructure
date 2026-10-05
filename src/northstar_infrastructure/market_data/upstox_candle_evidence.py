"""Evidence collection for Upstox futures daily-candle finality. Diagnostics only.

Whether, and when, Upstox stops revising a session's daily candle is not
known. A same-day candle has been seen to change between fetches (close and
open interest), so no time of day -- "21:00", "next morning", N hours after the
close -- is accepted as making a candle final. This module only collects
evidence: it fetches the exact candle Upstox serves for one dated contract and
one trading date, at an injected instant, and appends that response, verbatim
in its numbers, to an append-only JSON Lines file. It never declares a candle
final or stable, and nothing here feeds Northstar's canonical market data.

What it is not
--------------
It is separate from the canonical acquisition path. It builds no
FuturesOHLCVBar or Application observation, touches no Northstar database,
store or repository, and implements no Application port. Its records carry
provider evidence the canonical bar deliberately omits -- raw volume, open
interest, the instrument key and lot size in force -- because the question is
what Upstox returned, not what Northstar would store.

Reuse
-----
HTTP, the explicit User-Agent, JSON decoding with ``parse_float=Decimal`` and
error classification come from ``upstox_http``; instrument resolution from
``UpstoxInstrumentMaster``; the date-routed candle request, the success
check, the venue-date reading of a candle label and exact numbers from the
native daily adapter's ``fetch_daily_candles``. Every provider failure
therefore raises exactly the error the canonical adapter would.

Time is evidence here
---------------------
Unlike the production finality policy, an observation needs to know when it
was made. The clock is injected -- there is no default -- and read twice:
``requested_at`` immediately before the candle request and ``received_at``
immediately after the response. Both must be timezone-aware; they are recorded
as canonical UTC. Nothing here reads the wall clock itself.

``requested_at`` also routes the request. Its civil date in the venue's
timezone is the current venue date: observing that date asks Upstox's
current-day endpoint, which serves the trading day in progress or just closed;
observing any other date asks the historical endpoint. Which endpoint answered
a record therefore follows from its ``trading_date`` and ``requested_at``.

The record
----------
One JSON object per line, UTF-8, keys sorted, newline-terminated, under the
fixed schema ``northstar.upstox-daily-candle-observation/1``. Every provider
number is written as the exact text of the Decimal it decoded to
(``25010.50`` stays ``"25010.50"``); integers are decimal digit strings. The
token, request and response headers, cookies and the request URL (which embeds
the instrument key) are never recorded; the request is recorded structurally.
The instrument key is provider metadata, never Northstar identity, which stays
the contract's product, exchange and expiration.

A successful response with no candle for the date is evidence too and is
recorded with ``candle: null``. Raw volume that is not a whole number of
contracts at the recorded lot size is kept, with ``volume_contracts: null``
and a note, rather than refused as canonical acquisition refuses it. More than
one candle, or one outside the requested date, is refused and nothing is
recorded. Provider failures are never recorded.

One writer per file
-------------------
The log only ever appends: an identical repeat is a new line, because
unchanged observations over time are themselves evidence. Each record is one
write followed by flush and fsync. Append mode alone does not serialise
concurrent writers, so exactly one process may write a given evidence file.

Expiry limits what can be observed
----------------------------------
Resolution goes through the current instrument master, which lists only
tradable contracts, and an expired contract's former key is rejected
(UDAPI100011). Revisions after a contract leaves the master are therefore
unobservable, and evidence for its last sessions may be right-censored.
"""

from __future__ import annotations

import json
import os
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from northstar_core.foundation.value_objects import PointInTime
from northstar_core.futures import FuturesContract

from northstar_infrastructure.market_data.upstox_futures_native_daily_market_data import (
    UpstoxLabelledDailyCandle,
    _number,
    fetch_daily_candles,
)
from northstar_infrastructure.market_data.upstox_http import (
    UpstoxFetch,
    UpstoxMarketDataSourceError,
    default_fetch,
)
from northstar_infrastructure.market_data.upstox_instrument_master import (
    UpstoxInstrumentMaster,
    upstox_venue,
)

SCHEMA = "northstar.upstox-daily-candle-observation/1"
COLLECTOR = "northstar-infrastructure/0.1 upstox-candle-evidence/1"
PROVIDER = "upstox"
INTERVAL = "days/1"

EvidenceClock = Callable[[], datetime]
"""Returns the current instant as a timezone-aware datetime; injected, never defaulted."""


class UpstoxCandleEvidenceError(UpstoxMarketDataSourceError):
    """Raised when an Upstox response cannot be recorded as evidence for the request.

    More than one candle, or a candle outside the requested trading date: the
    response does not answer the question asked, so nothing is recorded.
    """


class UpstoxCandleEvidenceLogError(ValueError):
    """Raised when an evidence file cannot be read or safely appended to.

    ``line_number`` is the 1-based line at fault.
    """

    def __init__(self, message: str, line_number: int) -> None:
        super().__init__(message)
        self.line_number = line_number


class MalformedUpstoxCandleEvidenceError(UpstoxCandleEvidenceLogError):
    """A complete line that is not a valid observation record."""


class TruncatedUpstoxCandleEvidenceError(UpstoxCandleEvidenceLogError):
    """The final line is incomplete: it lacks its terminating newline.

    Appending after it would fuse two records, so the file is refused for
    appending too; the incomplete line is left exactly as it is.
    """


# ---------------------------------------------------------------------------
# The observation
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class UpstoxDailyCandleEvidence:
    """One Upstox daily candle exactly as served: label and exact numbers.

    ``open_interest`` is None only when Upstox sent no value for it.
    """

    provider_timestamp: str
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    open_interest: Decimal | None


@dataclass(frozen=True, slots=True)
class UpstoxDailyCandleEvidenceObservation:
    """One successful observation of one contract's candle for one trading date.

    ``candle`` is None when Upstox answered successfully with no candle for the
    date. ``volume_contracts`` is the raw volume divided by ``lot_size`` when
    that is a whole non-negative number, otherwise None with
    ``volume_contracts_note`` saying why.
    """

    requested_at: datetime
    received_at: datetime
    contract: FuturesContract
    trading_date: date
    instrument_key: str
    lot_size: int
    candle: UpstoxDailyCandleEvidence | None
    volume_contracts: int | None
    volume_contracts_note: str | None

    def __post_init__(self) -> None:
        subject = "UpstoxDailyCandleEvidenceObservation"
        for name in ("requested_at", "received_at"):
            value = getattr(self, name)
            if not isinstance(value, datetime) or value.utcoffset() is None:
                raise ValueError(f"{subject} {name} must be a timezone-aware datetime.")
        if self.received_at < self.requested_at:
            raise ValueError(f"{subject} cannot be received before it was requested.")
        if not isinstance(self.contract, FuturesContract):
            raise TypeError(f"{subject} contract must be a FuturesContract.")
        if isinstance(self.trading_date, datetime) or not isinstance(self.trading_date, date):
            raise TypeError(f"{subject} trading date must be a plain date.")
        if self.candle is None and (
            self.volume_contracts is not None or self.volume_contracts_note is not None
        ):
            raise ValueError(f"{subject} without a candle has no volume evidence.")

    def to_record(self) -> dict[str, Any]:
        """Return the JSON-ready record; every number is exact text."""
        product = self.contract.product
        candle = self.candle
        return {
            "schema": SCHEMA,
            "collector": COLLECTOR,
            "provider": PROVIDER,
            "requested_at": _utc_text(self.requested_at),
            "received_at": _utc_text(self.received_at),
            "contract": {
                "product": product.product_code.value,
                "exchange": product.exchange_code.value,
                "expiration": self.contract.expiration_date.value,
            },
            "trading_date": self.trading_date.isoformat(),
            "instrument_key": self.instrument_key,
            "lot_size": str(self.lot_size),
            "request": {
                "interval": INTERVAL,
                "from": self.trading_date.isoformat(),
                "to": self.trading_date.isoformat(),
            },
            "candle": (
                None
                if candle is None
                else {
                    "provider_timestamp": candle.provider_timestamp,
                    "open": str(candle.open),
                    "high": str(candle.high),
                    "low": str(candle.low),
                    "close": str(candle.close),
                    "volume": str(candle.volume),
                    "open_interest": (
                        None if candle.open_interest is None else str(candle.open_interest)
                    ),
                }
            ),
            "volume_contracts": (
                None if self.volume_contracts is None else str(self.volume_contracts)
            ),
            "volume_contracts_note": self.volume_contracts_note,
        }

    def to_json_line(self) -> str:
        """Return the record as one sorted-key JSON line ending in a newline."""
        return json.dumps(self.to_record(), sort_keys=True, ensure_ascii=False) + "\n"


def _utc_text(value: datetime) -> str:
    """Canonical UTC text, e.g. ``2026-10-05T10:45:00.123456Z``."""
    return PointInTime(value.astimezone(UTC).isoformat()).value


def _require_instant(value: object, name: str) -> datetime:
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise ValueError(f"The evidence clock must return a timezone-aware datetime for {name}.")
    return value.astimezone(UTC)


# ---------------------------------------------------------------------------
# The collector
# ---------------------------------------------------------------------------


class UpstoxDailyCandleEvidenceCollector:
    """Fetch the exact Upstox daily candle of one contract and trading date.

    The token is supplied by the caller and used only in the Authorization
    header; it never appears in ``repr``, an error or a record. ``clock`` is
    required. Collecting an observation records nothing: the caller appends it
    to an UpstoxCandleEvidenceLog.
    """

    def __init__(
        self,
        access_token: str,
        *,
        clock: EvidenceClock,
        instrument_master: UpstoxInstrumentMaster | None = None,
        fetch: UpstoxFetch = default_fetch,
        timeout: float = 10.0,
    ) -> None:
        if not isinstance(access_token, str) or not access_token.strip():
            raise UpstoxMarketDataSourceError("Upstox access token must be a non-empty string.")
        if any(character in access_token for character in "\r\n"):
            raise UpstoxMarketDataSourceError("Upstox access token must be a single line.")
        if not callable(clock):
            raise TypeError("UpstoxDailyCandleEvidenceCollector clock must be callable.")
        if not callable(fetch):
            raise TypeError("UpstoxDailyCandleEvidenceCollector fetch must be callable.")
        if instrument_master is not None and not isinstance(
            instrument_master, UpstoxInstrumentMaster
        ):
            raise TypeError(
                "UpstoxDailyCandleEvidenceCollector instrument_master "
                "must be an UpstoxInstrumentMaster."
            )
        self._headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {access_token.strip()}",
        }
        self._clock = clock
        self._fetch = fetch
        self._timeout = timeout
        self._master = instrument_master or UpstoxInstrumentMaster(fetch=fetch)

    def __repr__(self) -> str:
        """Deliberately carries no credential."""
        return f"UpstoxDailyCandleEvidenceCollector(interval={INTERVAL!r})"

    def observe(
        self, contract: FuturesContract, trading_date: date
    ) -> UpstoxDailyCandleEvidenceObservation:
        """Return what Upstox serves now for the contract's candle on ``trading_date``."""
        if not isinstance(contract, FuturesContract):
            raise TypeError(
                "UpstoxDailyCandleEvidenceCollector contract must be a FuturesContract."
            )
        if isinstance(trading_date, datetime) or not isinstance(trading_date, date):
            raise TypeError("UpstoxDailyCandleEvidenceCollector trading date must be a plain date.")

        zone = ZoneInfo(upstox_venue(contract).timezone)
        instrument = self._master.resolve_future(contract)
        context = f"{contract} daily candle evidence {trading_date.isoformat()}"

        requested_at = _require_instant(self._clock(), "requested_at")
        candles = fetch_daily_candles(
            self._fetch,
            self._headers,
            self._timeout,
            instrument.instrument_key,
            zone,
            trading_date,
            trading_date,
            context,
            current_date=requested_at.astimezone(zone).date(),
            error=UpstoxCandleEvidenceError,
        )
        received_at = _require_instant(self._clock(), "received_at")

        candle = _candle(candles[0]) if candles else None
        volume_contracts, note = (
            _volume_contracts(candle.volume, instrument.lot_size) if candle else (None, None)
        )
        return UpstoxDailyCandleEvidenceObservation(
            requested_at=requested_at,
            received_at=received_at,
            contract=contract,
            trading_date=trading_date,
            instrument_key=instrument.instrument_key,
            lot_size=instrument.lot_size,
            candle=candle,
            volume_contracts=volume_contracts,
            volume_contracts_note=note,
        )


def _candle(labelled: UpstoxLabelledDailyCandle) -> UpstoxDailyCandleEvidence:
    """Exact numbers of a shape-checked candle already matched to the observed date."""
    index, context = labelled.index, labelled.context
    label, open_raw, high_raw, low_raw, close_raw, volume_raw, interest_raw = labelled.candle
    return UpstoxDailyCandleEvidence(
        provider_timestamp=label,
        open=_number(open_raw, "open", index, context),
        high=_number(high_raw, "high", index, context),
        low=_number(low_raw, "low", index, context),
        close=_number(close_raw, "close", index, context),
        volume=_number(volume_raw, "volume", index, context),
        open_interest=(
            None if interest_raw is None else _number(interest_raw, "open_interest", index, context)
        ),
    )


def _volume_contracts(volume: Decimal, lot_size: int) -> tuple[int | None, str | None]:
    """Whole contracts when exact; otherwise None and a deterministic note."""
    if volume < 0:
        return None, f"raw volume {volume} is negative"
    if volume != volume.to_integral_value():
        return None, f"raw volume {volume} is not a whole number of underlying units"
    contracts, remainder = divmod(int(volume), lot_size)
    if remainder:
        return None, (
            f"raw volume {int(volume)} is not a whole number of contracts at lot size {lot_size}"
        )
    return contracts, None


# ---------------------------------------------------------------------------
# The append-only log
# ---------------------------------------------------------------------------


class UpstoxCandleEvidenceLog:
    """An append-only JSON Lines file of observations; one writer per file.

    Lines are only ever appended -- never rewritten, updated, deduplicated or
    truncated. Each append is one write, then flush and fsync. A file whose
    final line is incomplete is refused for appending, never repaired.
    """

    def __init__(self, path: Path) -> None:
        if not isinstance(path, Path):
            raise TypeError("UpstoxCandleEvidenceLog path must be a Path.")
        self._path = path

    @property
    def path(self) -> Path:
        return self._path

    def require_appendable(self) -> None:
        """Raise OSError for an unusable path, or the log error for an incomplete tail."""
        if not self._path.parent.is_dir():
            raise FileNotFoundError(f"Evidence directory does not exist: {self._path.parent}")
        if self._path.is_dir():
            raise IsADirectoryError(f"Evidence path is a directory: {self._path}")
        if not self._path.exists():
            return
        with self._path.open("rb") as handle:
            data = handle.read()
        if data and not data.endswith(b"\n"):
            raise TruncatedUpstoxCandleEvidenceError(
                f"Evidence file {self._path} ends with an incomplete line; it is left as it "
                "is and nothing more is appended to it.",
                data.count(b"\n") + 1,
            )

    def append(self, observation: UpstoxDailyCandleEvidenceObservation) -> None:
        """Append exactly one record line, durably."""
        if not isinstance(observation, UpstoxDailyCandleEvidenceObservation):
            raise TypeError("UpstoxCandleEvidenceLog can only append observations.")
        self.require_appendable()
        data = observation.to_json_line().encode("utf-8")
        with self._path.open("ab") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())

    def read(self) -> tuple[dict[str, Any], ...]:
        """Return every record in file order, refusing malformed or incomplete lines."""
        data = self._path.read_bytes() if self._path.exists() else b""
        if not data:
            return ()
        lines = data.split(b"\n")
        complete, tail = lines[:-1], lines[-1]
        if tail:
            raise TruncatedUpstoxCandleEvidenceError(
                f"Evidence file {self._path} ends with an incomplete line {len(lines)}.",
                len(lines),
            )
        records: list[dict[str, Any]] = []
        for number, line in enumerate(complete, start=1):
            try:
                record = json.loads(line.decode("utf-8"))
            except (UnicodeDecodeError, ValueError) as exc:
                raise MalformedUpstoxCandleEvidenceError(
                    f"Evidence file {self._path} line {number} is not valid JSON.", number
                ) from exc
            if not isinstance(record, dict) or record.get("schema") != SCHEMA:
                raise MalformedUpstoxCandleEvidenceError(
                    f"Evidence file {self._path} line {number} is not a {SCHEMA} record.",
                    number,
                )
            records.append(record)
        return tuple(records)
