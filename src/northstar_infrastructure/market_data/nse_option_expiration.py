"""NSE option expiration resolution from Northstar-owned reference data.

Expiration rules come from ``nse_option_expiry_reference`` and the trading days
they adjust over come from the NSE F&O calendar reference. Only three day-level
facts of that calendar are reused, unchanged: its trading holidays, its special
sessions and its loaded years. Its futures session hours and the futures session
resolver are not used -- an expiration date needs no session window. Nothing is
read from a calendar library, a provider, the network or a clock.

How one period resolves
-----------------------
1. The coordinates are validated exactly: an ISO week the ISO year does not
   have, or a month outside 1..12, raises.
2. The product's rule epoch in force for the period is selected: on the ISO
   week's Monday for a weekly expiry, on the month's first day for a monthly
   one. A period with no epoch in force raises, and so does a period whose last
   day already falls under a later epoch, so a period never straddles two
   rules.
3. The epoch names the nominal expiry date: the epoch's weekly weekday of the
   ISO week, or the last occurrence of its monthly weekday in the month.
4. Starting at the nominal date, the walk moves backward one calendar day at a
   time. A date in an unloaded calendar year raises; a date with a published
   special session raises; a weekend or a published trading holiday is passed
   over; the first remaining date is the expiration.

Special sessions fail closed. Northstar has no primary exchange source for how
an expiry reaching a special-session date is treated, so such a date is
neither taken as the expiration nor stepped through. A special session the walk
never reaches has no effect.

The walk is bounded at ``MAX_WALK_BACK_DAYS``. The bound is a defensive
implementation limit against corrupt reference data, not an NSE rule: no
genuine adjustment comes anywhere near it.

Resolving an expiration does not establish that any contract was listed.
"""

from __future__ import annotations

from calendar import monthrange
from collections.abc import Iterable
from datetime import date, datetime, timedelta

from northstar_application.ports import (
    OptionExpirationResolutionError,
    OptionExpirationResolver,
    ResolvedOptionExpiration,
)
from northstar_core.derivatives import ExpirationDate
from northstar_core.options import OptionProductReference

from northstar_infrastructure.market_data import (
    nse_futures_calendar_reference as calendar_reference,
)
from northstar_infrastructure.market_data import nse_option_expiry_reference as expiry_reference
from northstar_infrastructure.market_data.nse_option_expiry_reference import (
    ExpiryAdjustment,
    OptionExpiryRuleEpoch,
)

# Defensive limit on the backward walk; an implementation safety bound, not an NSE rule.
MAX_WALK_BACK_DAYS = 21

_SATURDAY = 5


class NSEOptionExpirationResolver(OptionExpirationResolver):
    """Resolve NSE option expirations from cited rule epochs and the NSE F&O calendar.

    The day-level calendar facts and the rule epochs are constructor-injected,
    with the production reference modules as defaults, so tests can supply
    synthetic data. Only ``holidays``, ``special_sessions`` and
    ``loaded_years`` are taken from the calendar; ``special_sessions`` may be
    a mapping keyed by date or any collection of dates.
    """

    def __init__(
        self,
        *,
        holidays: Iterable[date] = calendar_reference.HOLIDAYS,
        special_sessions: Iterable[date] = calendar_reference.SPECIAL_SESSIONS,
        loaded_years: Iterable[int] = calendar_reference.LOADED_YEARS,
        epochs: Iterable[OptionExpiryRuleEpoch] = expiry_reference.EPOCHS,
    ) -> None:
        self._holidays = _dates(holidays, "holidays")
        self._special_sessions = _dates(special_sessions, "special sessions")
        self._loaded_years = _years(loaded_years)
        self._epochs = expiry_reference.validate(epochs)

    # ------------------------------------------------------------------
    # Port
    # ------------------------------------------------------------------

    def weekly_expiration(
        self, product: OptionProductReference, iso_year: int, iso_week: int
    ) -> ResolvedOptionExpiration:
        """Return the weekly expiration of one ISO week."""
        self._require_product(product)
        _require_integer(iso_year, "ISO year")
        _require_integer(iso_week, "ISO week")
        try:
            monday = date.fromisocalendar(iso_year, iso_week, 1)
            sunday = date.fromisocalendar(iso_year, iso_week, 7)
        except ValueError as exc:
            raise OptionExpirationResolutionError(
                f"ISO week {iso_week} of {iso_year} does not exist."
            ) from exc

        epoch = self._epoch(product, monday, sunday)
        nominal = monday + timedelta(days=epoch.weekly_weekday)
        return self._resolve(product, epoch, nominal)

    def monthly_expiration(
        self, product: OptionProductReference, year: int, month: int
    ) -> ResolvedOptionExpiration:
        """Return the monthly expiration of one calendar month."""
        self._require_product(product)
        _require_integer(year, "year")
        _require_integer(month, "month")
        try:
            first = date(year, month, 1)
        except ValueError as exc:
            raise OptionExpirationResolutionError(
                f"Month {month} of {year} does not exist."
            ) from exc
        last = date(year, month, monthrange(year, month)[1])

        epoch = self._epoch(product, first, last)
        nominal = last - timedelta(days=(last.weekday() - epoch.monthly_weekday) % 7)
        return self._resolve(product, epoch, nominal)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    @staticmethod
    def _require_product(product: OptionProductReference) -> None:
        if not isinstance(product, OptionProductReference):
            raise TypeError(
                "NSEOptionExpirationResolver product must be an OptionProductReference."
            )

    def _epoch(
        self, product: OptionProductReference, first: date, last: date
    ) -> OptionExpiryRuleEpoch:
        """Return the one epoch governing the whole period ``first`` .. ``last``."""
        epochs = [
            epoch
            for epoch in self._epochs
            if epoch.product_code == product.product_code.value
            and epoch.exchange_code == product.exchange_code.value
        ]
        if not epochs:
            supported = sorted({f"{e.product_code}@{e.exchange_code}" for e in self._epochs})
            raise OptionExpirationResolutionError(
                f"Unsupported option product: {product}. Supported products are {supported}."
            )

        at_start = _in_force(epochs, first)
        if at_start is None:
            raise OptionExpirationResolutionError(
                f"No option expiration rule for {product} is in force for the period starting "
                f"{first.isoformat()}; the first encoded rule is effective from "
                f"{epochs[0].effective_from.isoformat()}."
            )
        if _in_force(epochs, last) is not at_start:
            raise OptionExpirationResolutionError(
                f"The period {first.isoformat()} .. {last.isoformat()} of {product} spans two "
                "option expiration rules and cannot be resolved."
            )
        return at_start

    def _resolve(
        self, product: OptionProductReference, epoch: OptionExpiryRuleEpoch, nominal: date
    ) -> ResolvedOptionExpiration:
        if epoch.adjustment is not ExpiryAdjustment.PREVIOUS_ELIGIBLE_TRADING_DAY:
            raise OptionExpirationResolutionError(
                f"Unsupported expiry adjustment {epoch.adjustment} for {product}."
            )
        expiration = self._previous_eligible(nominal)
        if expiration < epoch.effective_from:
            raise OptionExpirationResolutionError(
                f"Nominal expiry {nominal.isoformat()} of {product} adjusts to "
                f"{expiration.isoformat()}, before its rule's effective date "
                f"{epoch.effective_from.isoformat()}."
            )
        return ResolvedOptionExpiration(
            product=product,
            nominal_date=nominal,
            expiration_date=ExpirationDate(expiration.isoformat()),
            rule_source=epoch.source,
        )

    def _previous_eligible(self, nominal: date) -> date:
        """Walk backward from ``nominal`` to the first expiry-eligible trading day."""
        for offset in range(MAX_WALK_BACK_DAYS + 1):
            candidate = nominal - timedelta(days=offset)
            if candidate.year not in self._loaded_years:
                raise OptionExpirationResolutionError(
                    f"NSE F&O calendar year {candidate.year} is not loaded; the expiration "
                    f"for nominal date {nominal.isoformat()} cannot be resolved. "
                    f"Loaded years are {sorted(self._loaded_years)}."
                )
            if candidate in self._special_sessions:
                raise OptionExpirationResolutionError(
                    f"The expiration for nominal date {nominal.isoformat()} reaches "
                    f"{candidate.isoformat()}, an NSE special session whose expiration "
                    "treatment is not established; resolution fails closed."
                )
            if candidate.weekday() >= _SATURDAY or candidate in self._holidays:
                continue
            return candidate
        raise OptionExpirationResolutionError(
            f"No expiry-eligible trading day lies within {MAX_WALK_BACK_DAYS} days before "
            f"nominal date {nominal.isoformat()}; the calendar reference is suspect."
        )


def _in_force(epochs: list[OptionExpiryRuleEpoch], day: date) -> OptionExpiryRuleEpoch | None:
    in_force = [epoch for epoch in epochs if epoch.effective_from <= day]
    return in_force[-1] if in_force else None


def _require_integer(value: object, name: str) -> None:
    # bool is an int subclass; True must not pass as week or month 1.
    if isinstance(value, bool) or not isinstance(value, int):
        raise OptionExpirationResolutionError(f"Option expiration {name} must be an integer.")


def _dates(values: Iterable[date], name: str) -> frozenset[date]:
    dates = frozenset(values)
    for value in dates:
        if isinstance(value, datetime) or not isinstance(value, date):
            raise TypeError(f"NSEOptionExpirationResolver {name} must be plain dates.")
    return dates


def _years(values: Iterable[int]) -> frozenset[int]:
    years = frozenset(values)
    for value in years:
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError("NSEOptionExpirationResolver loaded years must be integers.")
    return years
