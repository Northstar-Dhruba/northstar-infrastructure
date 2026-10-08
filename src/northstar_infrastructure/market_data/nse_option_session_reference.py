"""Versioned NSE option normal-market hours.

Every regime here is cited. Nothing is fetched at runtime, and no regime is
assumed to apply before its ``effective_from`` date: a session date before a
product's first regime is not resolvable.

Provenance
----------

    NSE Equity Derivatives market timings: Normal Market Open 09:15,
    Normal Market Close 15:40.
    NSE/FAOP/74467   29 May 2026   close 15:40, effective from August 03, 2026.

The derivatives pre-open session (NSE/FAOP/69898, live from 2025-12-08) applies
to single-stock and index futures only, so the futures 09:00 start is not an
option open. The futures regimes in ``nse_futures_calendar_reference`` are
therefore not used for options.

Northstar holds no authoritative source for option hours before 2026-08-03, and
none is inferred: neither the futures regime nor the current hours are assumed
to have applied earlier. To extend coverage backwards, add the earlier regime
from its own primary source.

Special sessions
----------------
The NSE F&O calendar lists special sessions -- Muhurat, Union Budget, disaster-
recovery live sessions -- with futures timings or, for the unresolved 2026
Muhurat, none at all. Their option timings are not established here, so an
option session request that reaches a special-session date fails closed.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime, time

from northstar_application.ports import OptionTradingSessionResolutionError


@dataclass(frozen=True, slots=True)
class OptionSessionRegime:
    """One option product's normal-market hours from ``effective_from``, in IST."""

    product_code: str
    exchange_code: str
    effective_from: date
    opens: time
    closes: time
    source: str


REGIMES: tuple[OptionSessionRegime, ...] = (
    OptionSessionRegime(
        product_code="NIFTY",
        exchange_code="NSE",
        effective_from=date(2026, 8, 3),
        opens=time(9, 15),
        closes=time(15, 40),
        source=(
            "NSE Equity Derivatives market timings (Normal Market 09:15-15:40); "
            "NSE/FAOP/74467 (2026-05-29): close 15:40 effective 2026-08-03"
        ),
    ),
)


def validate(regimes: Iterable[OptionSessionRegime]) -> tuple[OptionSessionRegime, ...]:
    """Refuse regime data that could answer a session wrongly, and return it as a tuple."""

    def fail(message: str) -> None:
        raise OptionTradingSessionResolutionError(
            f"Invalid NSE option session reference: {message}"
        )

    rows = tuple(regimes)
    if not rows:
        fail("no option session regime is defined.")
    latest: dict[tuple[str, str], date] = {}
    for regime in rows:
        if not isinstance(regime, OptionSessionRegime):
            fail("every regime must be an OptionSessionRegime.")
        for name in ("product_code", "exchange_code", "source"):
            value = getattr(regime, name)
            if not isinstance(value, str) or not value.strip():
                fail(f"a regime has no {name.replace('_', ' ')}.")
        for name in ("product_code", "exchange_code"):
            value = getattr(regime, name)
            if value != value.strip().upper():
                fail(f"{name.replace('_', ' ')} {value!r} is not canonical.")
        if isinstance(regime.effective_from, datetime) or not isinstance(
            regime.effective_from, date
        ):
            fail("a regime's effective date must be a plain date.")
        if not isinstance(regime.opens, time) or not isinstance(regime.closes, time):
            fail("a regime's open and close must be times.")
        if regime.opens.tzinfo is not None or regime.closes.tzinfo is not None:
            fail("a regime's open and close are IST wall-clock times without a zone.")
        if regime.opens >= regime.closes:
            fail(f"the regime from {regime.effective_from} does not open before it closes.")
        key = (regime.product_code, regime.exchange_code)
        previous = latest.get(key)
        if previous is not None and previous >= regime.effective_from:
            fail(
                f"regimes of {regime.product_code}@{regime.exchange_code} must be strictly "
                "ascending by effective date, with no duplicate."
            )
        latest[key] = regime.effective_from
    return rows


validate(REGIMES)
