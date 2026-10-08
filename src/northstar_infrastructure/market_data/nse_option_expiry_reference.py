"""Versioned NSE option expiration rules.

Every rule here was read from a primary NSE circular and is cited beside it.
Nothing is fetched at runtime and no rule is assumed to apply before its
``effective_from`` date: a period before the first epoch of a product is not
resolvable, and to extend coverage backwards the earlier regime has to be added
from its own circulars.

Provenance (nsearchives.nseindia.com/content/circulars/)
--------------------------------------------------------

    NSE/FAOP/68747   25 Jun 2025   "Revision in Expiry Day of Index and Stock
                                   Derivatives Contracts - Update"

For NIFTY it moves the weekly expiry from Thursday to Tuesday, and the monthly,
quarterly and half-yearly expiry from the last Thursday to the last Tuesday of
the expiry month, for newly generated contracts expiring on or after
2025-09-01. The earlier Thursday regime is deliberately not encoded.

What an epoch states
--------------------
Only the expiration rule: the weekly and monthly expiry weekdays and the
adjustment applied when the nominal date is not an expiry-eligible trading day.
It does not state which expiries are listed (four weekly, three monthly,
quarterly, half-yearly), the strike scheme, lot sizes or provider instruments.
Those are listing and reference facts, and resolving an expiration from this
rule never establishes that a contract was listed.

The trading days the adjustment walks over come from the NSE F&O calendar
reference, which is reused unchanged.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum

from northstar_application.ports import OptionExpirationResolutionError

# Python weekday numbers, as ``date.weekday()`` returns them.
MONDAY = 0
TUESDAY = 1
FRIDAY = 4


class ExpiryAdjustment(StrEnum):
    """How a nominal expiry that is not an expiry-eligible trading day is moved."""

    PREVIOUS_ELIGIBLE_TRADING_DAY = "PREVIOUS_ELIGIBLE_TRADING_DAY"


@dataclass(frozen=True, slots=True)
class OptionExpiryRuleEpoch:
    """One option product's expiration rule, in force from ``effective_from``.

    ``weekly_weekday`` is the weekday of the ISO expiry week a weekly contract
    nominally expires on; ``monthly_weekday`` is the weekday whose last
    occurrence in the expiry month a monthly contract nominally expires on.
    Both are ``date.weekday()`` numbers. The epoch is bounded by the next
    epoch of the same product, if there is one.
    """

    product_code: str
    exchange_code: str
    effective_from: date
    weekly_weekday: int
    monthly_weekday: int
    adjustment: ExpiryAdjustment
    source: str


EPOCHS: tuple[OptionExpiryRuleEpoch, ...] = (
    OptionExpiryRuleEpoch(
        product_code="NIFTY",
        exchange_code="NSE",
        effective_from=date(2025, 9, 1),
        weekly_weekday=TUESDAY,
        monthly_weekday=TUESDAY,
        adjustment=ExpiryAdjustment.PREVIOUS_ELIGIBLE_TRADING_DAY,
        source=(
            "NSE/FAOP/68747 (2025-06-25): Revision in Expiry Day of Index and "
            "Stock Derivatives Contracts - Update"
        ),
    ),
)


def validate(epochs: Iterable[OptionExpiryRuleEpoch]) -> tuple[OptionExpiryRuleEpoch, ...]:
    """Refuse rule data that could answer an expiration wrongly, and return it as a tuple."""

    def fail(message: str) -> None:
        raise OptionExpirationResolutionError(f"Invalid NSE option expiry reference: {message}")

    rows = tuple(epochs)
    if not rows:
        fail("no expiration rule epoch is defined.")

    latest: dict[tuple[str, str], date] = {}
    for epoch in rows:
        if not isinstance(epoch, OptionExpiryRuleEpoch):
            fail("every epoch must be an OptionExpiryRuleEpoch.")
        for name in ("product_code", "exchange_code", "source"):
            value = getattr(epoch, name)
            if not isinstance(value, str) or not value.strip():
                fail(f"an epoch has no {name.replace('_', ' ')}.")
        if epoch.product_code != epoch.product_code.strip().upper():
            fail(f"product code {epoch.product_code!r} is not canonical.")
        if epoch.exchange_code != epoch.exchange_code.strip().upper():
            fail(f"exchange code {epoch.exchange_code!r} is not canonical.")
        if isinstance(epoch.effective_from, datetime) or not isinstance(epoch.effective_from, date):
            fail("an epoch's effective date must be a plain date.")
        for name in ("weekly_weekday", "monthly_weekday"):
            weekday = getattr(epoch, name)
            if isinstance(weekday, bool) or not isinstance(weekday, int):
                fail(f"{name.replace('_', ' ')} must be an integer weekday.")
            if not MONDAY <= weekday <= FRIDAY:
                fail(f"{name.replace('_', ' ')} {weekday} is not Monday to Friday.")
        if not isinstance(epoch.adjustment, ExpiryAdjustment):
            fail("an epoch's adjustment must be an ExpiryAdjustment.")

        key = (epoch.product_code, epoch.exchange_code)
        previous = latest.get(key)
        if previous is not None and previous >= epoch.effective_from:
            fail(
                f"epochs of {epoch.product_code}@{epoch.exchange_code} must be strictly "
                "ascending by effective date, with no duplicate."
            )
        latest[key] = epoch.effective_from
    return rows


validate(EPOCHS)
