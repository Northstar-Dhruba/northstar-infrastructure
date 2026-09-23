"""Databento adapter for futures historical market data acquisition.

Northstar acquires one trading session at a time and folds the result into a
daily bar. Databento is asked for ``ohlcv-1m`` on ``GLBX.MDP3`` and nothing
else: the session-aligned daily schema does not exist for CME data, and the
daily schema that does exist aggregates UTC calendar days, whose window
straddles two trading sessions.

Provider identity stays here
----------------------------
Northstar identity is never parsed out of a provider symbol. A contract is
resolved through Databento's own definition data, matching on the fields that
carry meaning -- ``asset`` is the product code directly, ``instrument_class``
separates outrights from spreads, ``exchange`` names the venue, ``expiration``
dates the contract. The resulting ``raw_symbol`` is used to request bars and
never leaves this module. ``instrument_id`` is not used as identity at all,
because venue instrument identifiers can be remapped.

Exactness
---------
Prices arrive as signed integer fixed-point at 1e-9 and are converted through
an explicit coefficient/exponent construction, never through a float and never
through arithmetic that could round under the caller's decimal context.
Timestamps are translated with integer nanosecond arithmetic.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any, ClassVar
from zoneinfo import ZoneInfo

import databento as dbento
from databento_dbn import UNDEF_PRICE, InstrumentClass
from northstar_application.ports import (
    FuturesHistoricalMarketDataSource,
    FuturesTradingSession,
)
from northstar_core.derivatives import QuoteValue
from northstar_core.foundation.value_objects import PointInTime, Quantity, Timeframe
from northstar_core.futures import FuturesContract, FuturesOHLCVBar

_DATASET = "GLBX.MDP3"
_BAR_SCHEMA = "ohlcv-1m"
_DEFINITION_SCHEMA = "definition"

# Three days spans a weekend or a holiday, so an expiration landing on a
# non-session day still has a session inside the window. Wider windows cost
# materially more: seven days of CL definitions is roughly three times the
# records of three days.
_DEFINITION_LOOKBACK_DAYS = 3

# A three-day window normally spans a weekend, but a weekend followed by a
# Monday holiday leaves it with no session at all, and a listed contract
# would look unresolvable. One bounded widening covers that closure.
_DEFINITION_FALLBACK_LOOKBACK_DAYS = 7

_MINUTE = Timeframe("1m")
_MINUTE_NS = 60_000_000_000
_NS_PER_SECOND = 1_000_000_000
_NS_PER_MICROSECOND = 1_000

# Fixed-point exponent: one raw unit is 1e-9.
_PRICE_EXPONENT = -9

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


class DatabentoFuturesHistoricalMarketDataSourceError(RuntimeError):
    """Raised when Databento is unavailable or returns data Northstar cannot accept."""


class DatabentoFuturesHistoricalMarketDataSource(FuturesHistoricalMarketDataSource):
    """Acquire one futures contract's minute bars for one session from Databento.

    The API key is supplied by the caller. This adapter reads no environment
    variable and loads no dotenv file: choosing where a secret comes from is a
    composition concern, and an adapter that reaches for one cannot be tested
    or deployed deterministically. The key is never placed in ``repr``, in an
    error message, or in any other output.
    """

    # Each venue is mapped deliberately rather than through one generic rule.
    _VENUE_EXCHANGES: ClassVar[dict[str, str]] = {
        "CME": "XCME",
        "CBOT": "XCBT",
        "NYMEX": "XNYM",
        "COMEX": "XCEC",
    }

    # Expiration is a calendar date in the venue's own timezone. The four
    # CME-complex venues share one zone today; they are still listed
    # separately, because "they happen to agree" is not a reason to encode one
    # implicit assumption.
    _VENUE_TIMEZONES: ClassVar[dict[str, str]] = {
        "CME": "America/Chicago",
        "CBOT": "America/Chicago",
        "NYMEX": "America/Chicago",
        "COMEX": "America/Chicago",
    }

    def __init__(self, api_key: str, *, client: Any | None = None) -> None:
        if client is None:
            if not isinstance(api_key, str) or not api_key.strip():
                raise DatabentoFuturesHistoricalMarketDataSourceError(
                    "Databento API key must be a non-empty string."
                )
            client = dbento.Historical(api_key)
        self._client = client
        self._resolved: dict[FuturesContract, str] = {}
        self._availability_range: tuple[date, date] | None = None

    def __repr__(self) -> str:
        """Deliberately carries no credential."""
        return f"DatabentoFuturesHistoricalMarketDataSource(dataset={_DATASET!r})"

    # ------------------------------------------------------------------
    # Port
    # ------------------------------------------------------------------

    def fetch_session_bars(
        self, contract: FuturesContract, session: FuturesTradingSession
    ) -> tuple[FuturesOHLCVBar, ...]:
        """Return one session's minute observations, oldest to newest."""
        if not isinstance(contract, FuturesContract):
            raise TypeError(
                "DatabentoFuturesHistoricalMarketDataSource contract must be a FuturesContract."
            )
        if not isinstance(session, FuturesTradingSession):
            raise TypeError(
                "DatabentoFuturesHistoricalMarketDataSource session "
                "must be a FuturesTradingSession."
            )

        raw_symbol = self._raw_symbol(contract)

        opens_ns = _epoch_nanoseconds(session.opens_at)
        closes_ns = _epoch_nanoseconds(session.closes_at)

        # Databento is start-inclusive and end-exclusive, so the session
        # boundaries are passed through unchanged: this is exactly
        # opens_at <= ts_event < closes_at. No minute is added or subtracted,
        # and no UTC-day boundary is involved.
        records = self._request(
            schema=_BAR_SCHEMA,
            symbols=[raw_symbol],
            stype_in="raw_symbol",
            start=_as_utc(session.opens_at),
            end=_as_utc(session.closes_at),
            context=f"{contract} session {session.trading_date.isoformat()}",
        )

        bars: list[FuturesOHLCVBar] = []
        for record in records:
            ts_event = int(record.ts_event)
            if ts_event < opens_ns or ts_event >= closes_ns:
                raise DatabentoFuturesHistoricalMarketDataSourceError(
                    f"Databento returned a bar whose interval opens outside the requested "
                    f"session for {contract} on {session.trading_date.isoformat()}."
                )
            bars.append(self._bar(record, contract, session))

        # Provider order is preserved. Malformed order is the aggregation
        # fold's to reject; silently reordering it here would hide the defect.
        return tuple(bars)

    # ------------------------------------------------------------------
    # Identity resolution
    # ------------------------------------------------------------------

    def _raw_symbol(self, contract: FuturesContract) -> str:
        """Resolve one contract to its provider symbol, independently of any session.

        Resolution deliberately does not look at the session being fetched. A
        contract is listed for only part of the dataset's history, so asking
        for its definition during a session outside that life finds nothing --
        which would make a perfectly valid contract on a pre-listing or
        post-expiry session indistinguishable from a contract that does not
        exist. The window is therefore derived from the contract itself.
        """
        cached = self._resolved.get(contract)
        if cached is not None:
            return cached

        product = contract.product
        venue = product.exchange_code.value
        expected_exchange = self._VENUE_EXCHANGES.get(venue)
        if expected_exchange is None:
            raise DatabentoFuturesHistoricalMarketDataSourceError(
                f"Unsupported futures venue for Databento: {venue}. "
                f"Supported venues are {sorted(self._VENUE_EXCHANGES)}."
            )
        venue_zone = ZoneInfo(self._VENUE_TIMEZONES[venue])

        product_code = product.product_code.value

        start, end = self._definition_window(contract, _DEFINITION_LOOKBACK_DAYS)
        definitions = self._definitions(product_code, contract, start, end)

        if not definitions:
            # No definition evidence at all in the window, which means it held
            # no session rather than that the contract is wrong. Widen once.
            # A window that *did* return records but none matching is a
            # different fact entirely, handled below without widening.
            start, end = self._definition_window(contract, _DEFINITION_FALLBACK_LOOKBACK_DAYS)
            definitions = self._definitions(product_code, contract, start, end)

        matches: set[str] = set()
        for definition in definitions:
            if not _is_outright_future(definition):
                continue
            if str(definition.asset) != product_code:
                continue
            if str(definition.exchange) != expected_exchange:
                continue
            if not _expires_on(definition, contract, venue_zone):
                continue
            matches.add(str(definition.raw_symbol))

        if not matches:
            raise DatabentoFuturesHistoricalMarketDataSourceError(
                f"Databento has no outright future matching {contract} "
                f"between {start.isoformat()} and {end.isoformat()}."
            )
        if len(matches) > 1:
            raise DatabentoFuturesHistoricalMarketDataSourceError(
                f"Databento resolved {contract} to {len(matches)} distinct instruments "
                f"({sorted(matches)}); the contract is ambiguous."
            )

        resolved = matches.pop()
        self._resolved[contract] = resolved
        return resolved

    def _definitions(
        self, product_code: str, contract: FuturesContract, start: date, end: date
    ) -> list[object]:
        """Return every parent-product definition record in one window."""
        return list(
            self._request(
                schema=_DEFINITION_SCHEMA,
                symbols=[f"{product_code}.FUT"],
                stype_in="parent",
                start=start.isoformat(),
                end=end.isoformat(),
                context=f"definitions for {product_code} expiring {contract.expiration_date}",
            )
        )

    def _definition_window(
        self, contract: FuturesContract, lookback_days: int
    ) -> tuple[date, date]:
        """Return the bounded date window in which this contract's definition lives.

        A contract's definition is broadcast every session it is listed, so any
        day inside its life resolves it. The end of that life is the reliable
        place to look, but it cannot simply be the expiration date: a contract
        still trading today expires in the future, where the provider holds no
        data at all. The window is therefore bounded by what the provider
        actually has.

        The bound comes from the provider's own availability rather than a
        clock. Reading the wall clock would make resolution depend on when it
        ran, and this adapter reads no clock.
        """
        available_start, available_end = self._availability()

        expiration = date.fromisoformat(contract.expiration_date.value)
        # Databento's end bound is exclusive, so the day after expiry includes
        # the expiration session itself. Clamping that to the provider's
        # availability is what handles a contract still trading today: its
        # expiry lies in the future, where there is no data to ask about, and
        # the window slides back to the newest sessions instead.
        end = min(expiration + timedelta(days=1), available_end)
        start = max(end - timedelta(days=lookback_days), available_start)

        if start >= end:
            raise DatabentoFuturesHistoricalMarketDataSourceError(
                f"Databento holds no definition data able to resolve {contract}; "
                f"its life falls outside the available range "
                f"{available_start.isoformat()}..{available_end.isoformat()}."
            )
        return start, end

    def _availability(self) -> tuple[date, date]:
        """Return the provider's definition-schema range, fetched once per adapter."""
        if self._availability_range is None:
            try:
                reported = self._client.metadata.get_dataset_range(_DATASET)
                definition_range = reported["schema"][_DEFINITION_SCHEMA]
                self._availability_range = (
                    date.fromisoformat(str(definition_range["start"])[:10]),
                    date.fromisoformat(str(definition_range["end"])[:10]),
                )
            except DatabentoFuturesHistoricalMarketDataSourceError:
                raise
            except Exception as exc:
                raise DatabentoFuturesHistoricalMarketDataSourceError(
                    f"Databento did not report a usable {_DATASET} definition range."
                ) from exc
        return self._availability_range

    # ------------------------------------------------------------------
    # Record translation
    # ------------------------------------------------------------------

    def _bar(
        self, record: Any, contract: FuturesContract, session: FuturesTradingSession
    ) -> FuturesOHLCVBar:
        try:
            return FuturesOHLCVBar(
                contract=contract,
                point_in_time=_completion_instant(int(record.ts_event)),
                timeframe=_MINUTE,
                open=_quote(record.open, "open"),
                high=_quote(record.high, "high"),
                low=_quote(record.low, "low"),
                close=_quote(record.close, "close"),
                volume=_volume(record.volume),
            )
        except DatabentoFuturesHistoricalMarketDataSourceError:
            raise
        except (TypeError, ValueError) as exc:
            raise DatabentoFuturesHistoricalMarketDataSourceError(
                f"Databento returned a bar Northstar cannot represent for {contract} "
                f"on {session.trading_date.isoformat()}."
            ) from exc

    # ------------------------------------------------------------------
    # Provider access
    # ------------------------------------------------------------------

    def _request(self, *, context: str, **parameters: Any) -> Any:
        """Issue one Databento request, hiding provider failures behind one error.

        The raised message never contains the request's credentials, because
        the key is held by the client and never passed through here.
        """
        try:
            return self._client.timeseries.get_range(dataset=_DATASET, **parameters)
        except DatabentoFuturesHistoricalMarketDataSourceError:
            raise
        except Exception as exc:
            raise DatabentoFuturesHistoricalMarketDataSourceError(
                f"Databento request failed for {context}."
            ) from exc


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def _is_outright_future(definition: Any) -> bool:
    """Accept only outright futures, never spreads."""
    if str(definition.instrument_class) != str(InstrumentClass.FUTURE):
        return False
    security_type = str(getattr(definition, "security_type", "") or "")
    return security_type in ("", "FUT")


def _expires_on(definition: Any, contract: FuturesContract, venue_zone: ZoneInfo) -> bool:
    """Compare expiry as a calendar date in the venue's own timezone.

    The provider's expiration is an instant -- the last eligible trade time --
    while a Northstar ExpirationDate is a civil date. Converting in UTC would
    be wrong for any product whose last trade falls after the venue's civil
    midnight, and the fact that today's sampled products happen to agree in
    both zones is a property of their expiry times rather than a guarantee.
    """
    expiration = int(definition.expiration)
    if expiration >= _UNDEF_TIMESTAMP:
        return False
    local_date = _instant(expiration).astimezone(venue_zone).date()
    return local_date.isoformat() == contract.expiration_date.value


_UNDEF_TIMESTAMP = 18446744073709551615


def _quote(raw: int, field_name: str) -> QuoteValue:
    """Convert a raw fixed-point price to an exact QuoteValue.

    The Decimal is built from an explicit sign, coefficient and exponent, which
    is exact and consults no decimal context. Dividing by a scale, or using
    scaleb, would round under a caller holding a reduced precision. Negative
    quotations and zero both survive unchanged; QuoteValue then folds negative
    zero to zero.
    """
    value = int(raw)
    if value == UNDEF_PRICE:
        raise DatabentoFuturesHistoricalMarketDataSourceError(
            f"Databento returned an undefined {field_name} price."
        )
    return QuoteValue(_fixed_point(value))


def _fixed_point(value: int) -> Decimal:
    sign = 0 if value >= 0 else 1
    digits = tuple(int(character) for character in str(abs(value)))
    return Decimal((sign, digits, _PRICE_EXPONENT))


def _volume(raw: int) -> Quantity:
    """Convert a raw contract count exactly, with no float and no rounding."""
    value = int(raw)
    if value < 0:
        raise DatabentoFuturesHistoricalMarketDataSourceError(
            "Databento returned a negative volume."
        )
    return Quantity(Decimal(value))


def _completion_instant(ts_event_ns: int) -> PointInTime:
    """Translate a provider interval-open timestamp to Northstar's completion instant.

    A one-minute timeframe *defines* a sixty-second interval, which is what
    makes this fixed addition legitimate. It must never be generalised to
    calendar-dependent timeframes, and it must never be used to derive a
    session boundary, whose length varies.

    The arithmetic is integer nanoseconds throughout; a float epoch would lose
    precision well before nanoseconds.
    """
    return _instant_as_point_in_time(ts_event_ns + _MINUTE_NS)


def _instant(nanoseconds: int) -> datetime:
    seconds, remainder = divmod(nanoseconds, _NS_PER_SECOND)
    return _EPOCH + timedelta(seconds=seconds, microseconds=remainder // _NS_PER_MICROSECOND)


def _instant_as_point_in_time(nanoseconds: int) -> PointInTime:
    seconds, remainder = divmod(nanoseconds, _NS_PER_SECOND)
    if remainder % _NS_PER_MICROSECOND:
        raise DatabentoFuturesHistoricalMarketDataSourceError(
            "Databento returned a timestamp with sub-microsecond precision, "
            "which PointInTime cannot represent."
        )
    microsecond = remainder // _NS_PER_MICROSECOND
    moment = _EPOCH + timedelta(seconds=seconds, microseconds=microsecond)
    fraction = f".{moment.microsecond:06d}" if moment.microsecond else ""
    return PointInTime(f"{moment.strftime('%Y-%m-%dT%H:%M:%S')}{fraction}Z")


def _as_utc(point_in_time: PointInTime) -> datetime:
    """Return the request boundary as an explicit UTC datetime."""
    return datetime.fromisoformat(point_in_time.value.replace("Z", "+00:00")).astimezone(UTC)


def _epoch_nanoseconds(point_in_time: PointInTime) -> int:
    delta = _as_utc(point_in_time) - _EPOCH
    return (
        delta.days * 86_400 + delta.seconds
    ) * _NS_PER_SECOND + delta.microseconds * _NS_PER_MICROSECOND
