"""Tests for the versioned NSE option expiration rule reference."""

from __future__ import annotations

from dataclasses import FrozenInstanceError, fields, replace
from datetime import date, datetime

import pytest
from northstar_application.ports import OptionExpirationResolutionError

from northstar_infrastructure.market_data import nse_option_expiry_reference as reference
from northstar_infrastructure.market_data.nse_option_expiry_reference import (
    ExpiryAdjustment,
    OptionExpiryRuleEpoch,
)

_NIFTY_TUESDAY = reference.EPOCHS[0]


# ---------------------------------------------------------------------------
# The encoded rule
# ---------------------------------------------------------------------------


def test_exactly_the_current_nifty_epoch_is_encoded() -> None:
    assert reference.EPOCHS == (
        OptionExpiryRuleEpoch(
            product_code="NIFTY",
            exchange_code="NSE",
            effective_from=date(2025, 9, 1),
            weekly_weekday=reference.TUESDAY,
            monthly_weekday=reference.TUESDAY,
            adjustment=ExpiryAdjustment.PREVIOUS_ELIGIBLE_TRADING_DAY,
            source=(
                "NSE/FAOP/68747 (2025-06-25): Revision in Expiry Day of Index and "
                "Stock Derivatives Contracts - Update"
            ),
        ),
    )


def test_the_rule_applies_from_2025_09_01_only() -> None:
    assert _NIFTY_TUESDAY.effective_from == date(2025, 9, 1)
    assert min(epoch.effective_from for epoch in reference.EPOCHS) == date(2025, 9, 1)


def test_weekly_and_monthly_expiries_are_on_tuesday() -> None:
    assert reference.TUESDAY == 1 == date(2025, 9, 2).weekday()
    assert _NIFTY_TUESDAY.weekly_weekday == reference.TUESDAY
    assert _NIFTY_TUESDAY.monthly_weekday == reference.TUESDAY


def test_the_adjustment_is_the_previous_eligible_trading_day() -> None:
    assert _NIFTY_TUESDAY.adjustment is ExpiryAdjustment.PREVIOUS_ELIGIBLE_TRADING_DAY
    assert [member.value for member in ExpiryAdjustment] == ["PREVIOUS_ELIGIBLE_TRADING_DAY"]


def test_the_source_cites_the_primary_circular_and_its_date() -> None:
    assert "NSE/FAOP/68747" in _NIFTY_TUESDAY.source
    assert "2025-06-25" in _NIFTY_TUESDAY.source
    for epoch in reference.EPOCHS:
        assert epoch.source.strip()


def test_the_epoch_states_only_the_expiration_rule() -> None:
    assert [field.name for field in fields(OptionExpiryRuleEpoch)] == [
        "product_code",
        "exchange_code",
        "effective_from",
        "weekly_weekday",
        "monthly_weekday",
        "adjustment",
        "source",
    ]
    names = " ".join(field.name for field in fields(OptionExpiryRuleEpoch))
    for forbidden in (
        "strike",
        "interval",
        "listing",
        "listed",
        "cycle",
        "lot",
        "provider",
        "instrument",
        "symbol",
        "series",
        "walk",
    ):
        assert forbidden not in names


def test_an_epoch_is_immutable() -> None:
    with pytest.raises(FrozenInstanceError):
        _NIFTY_TUESDAY.effective_from = date(2025, 1, 1)  # type: ignore[misc]


def test_the_module_does_not_claim_the_thursday_regime() -> None:
    assert all(epoch.weekly_weekday != 3 for epoch in reference.EPOCHS)
    assert "deliberately not encoded" in (reference.__doc__ or "")


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def test_validation_accepts_the_encoded_epochs_and_returns_a_tuple() -> None:
    assert reference.validate(iter(reference.EPOCHS)) == reference.EPOCHS


def test_ascending_epochs_of_one_product_are_accepted() -> None:
    later = replace(_NIFTY_TUESDAY, effective_from=date(2026, 9, 1))

    assert reference.validate((_NIFTY_TUESDAY, later)) == (_NIFTY_TUESDAY, later)


def test_epochs_of_different_products_are_ordered_independently() -> None:
    other = replace(_NIFTY_TUESDAY, product_code="FINNIFTY", effective_from=date(2024, 1, 1))

    assert reference.validate((_NIFTY_TUESDAY, other)) == (_NIFTY_TUESDAY, other)


@pytest.mark.parametrize(
    ("epochs", "message"),
    [
        ((), "no expiration rule epoch"),
        ((_NIFTY_TUESDAY, _NIFTY_TUESDAY), "strictly ascending"),
        (
            (_NIFTY_TUESDAY, replace(_NIFTY_TUESDAY, effective_from=date(2025, 8, 1))),
            "strictly ascending",
        ),
        ((replace(_NIFTY_TUESDAY, source=""),), "no source"),
        ((replace(_NIFTY_TUESDAY, source="   "),), "no source"),
        ((replace(_NIFTY_TUESDAY, source=None),), "no source"),
        ((replace(_NIFTY_TUESDAY, product_code=""),), "no product code"),
        ((replace(_NIFTY_TUESDAY, exchange_code=""),), "no exchange code"),
        ((replace(_NIFTY_TUESDAY, product_code="nifty"),), "not canonical"),
        ((replace(_NIFTY_TUESDAY, exchange_code="nse"),), "not canonical"),
        ((replace(_NIFTY_TUESDAY, effective_from=datetime(2025, 9, 1)),), "plain date"),
        ((replace(_NIFTY_TUESDAY, effective_from="2025-09-01"),), "plain date"),
        ((replace(_NIFTY_TUESDAY, weekly_weekday=5),), "not Monday to Friday"),
        ((replace(_NIFTY_TUESDAY, monthly_weekday=6),), "not Monday to Friday"),
        ((replace(_NIFTY_TUESDAY, weekly_weekday=-1),), "not Monday to Friday"),
        ((replace(_NIFTY_TUESDAY, weekly_weekday=True),), "integer weekday"),
        ((replace(_NIFTY_TUESDAY, monthly_weekday="TUESDAY"),), "integer weekday"),
        (
            (replace(_NIFTY_TUESDAY, adjustment="PREVIOUS_ELIGIBLE_TRADING_DAY"),),
            "ExpiryAdjustment",
        ),
        (("NIFTY@NSE Tuesday",), "OptionExpiryRuleEpoch"),
    ],
)
def test_invalid_reference_data_is_refused(epochs: tuple, message: str) -> None:
    with pytest.raises(OptionExpirationResolutionError, match=message):
        reference.validate(epochs)
