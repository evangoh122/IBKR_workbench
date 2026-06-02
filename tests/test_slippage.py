"""
tests/test_slippage.py
"""
import pytest
from etl.slippage import (
    calculate_costs, SlippageToggles,
    _ibkr_stock_commission, _ibkr_option_commission, _market_impact_stock,
)


# ── Spread tests ──────────────────────────────────────────────────────────────

def test_spread_cost_stock():
    cb = calculate_costs(
        ticker="AAPL", asset_type="stock",
        quantity=100, price=182.50,
        bid=182.45, ask=182.55,
        toggles=SlippageToggles(spread=True, commission=False, market_impact=False),
    )
    # half-spread = 0.05, round-trip = 0.05 * 100 * 2
    assert cb.spread_cost == pytest.approx(10.0)


def test_spread_cost_option():
    cb = calculate_costs(
        ticker="AAPL", asset_type="option",
        quantity=10, price=3.15,
        bid=3.10, ask=3.20, multiplier=100,
        toggles=SlippageToggles(spread=True, commission=False, market_impact=False),
    )
    # half-spread = 0.05, round-trip = 0.05 * 10 * 100 * 2
    assert cb.spread_cost == pytest.approx(100.0)


def test_spread_zero_when_no_bid_ask():
    cb = calculate_costs(
        ticker="AAPL", asset_type="stock",
        quantity=100, price=182.50,
        toggles=SlippageToggles(spread=True, commission=False, market_impact=False),
    )
    assert cb.spread_cost == 0.0


# ── Commission tests ──────────────────────────────────────────────────────────

def test_stock_commission_minimum():
    # Small order should hit minimum
    cost = _ibkr_stock_commission(shares=10, price=100.0)
    assert cost == pytest.approx(0.35 * 2)   # min $0.35 each leg


def test_stock_commission_rate():
    # 1000 shares at tier 1 rate $0.0035/share
    cost = _ibkr_stock_commission(shares=1000, price=100.0)
    per_leg = 1000 * 0.0035
    assert cost == pytest.approx(per_leg * 2)


def test_stock_commission_max_pct():
    # 100 shares at $0.01 price — commission capped at 1% of trade value
    cost = _ibkr_stock_commission(shares=100, price=0.01)
    max_per_leg = 100 * 0.01 * 0.01   # 1% of $1
    assert cost <= max_per_leg * 2 + 0.001


def test_option_commission_minimum():
    # 1 contract → min $1.00 per leg
    cost = _ibkr_option_commission(contracts=1)
    assert cost == pytest.approx(1.00 * 2)


def test_option_commission_scales():
    # 10 contracts at $0.65 + $0.02 reg = $6.70 per leg
    cost = _ibkr_option_commission(contracts=10)
    per_leg = 10 * 0.65 + 10 * 0.02
    assert cost == pytest.approx(per_leg * 2)


# ── Market impact tests ───────────────────────────────────────────────────────

def test_market_impact_zero_for_tiny_trade():
    # 10 shares in 10M ADV stock → very small impact
    impact = _market_impact_stock(shares=10, price=100.0, adv=10_000_000)
    assert impact < 0.01   # less than 1 cent


def test_market_impact_larger_for_big_trade():
    small = _market_impact_stock(shares=1_000,   price=100.0, adv=1_000_000)
    large = _market_impact_stock(shares=100_000, price=100.0, adv=1_000_000)
    assert large > small


def test_market_impact_sqrt_scaling():
    # Doubling quantity should increase impact by ~sqrt(2)
    i1 = _market_impact_stock(shares=10_000, price=100.0, adv=1_000_000)
    i2 = _market_impact_stock(shares=40_000, price=100.0, adv=1_000_000)
    ratio = i2 / i1
    assert ratio == pytest.approx(2.0, rel=0.05)   # sqrt(4) = 2


# ── Toggle tests ──────────────────────────────────────────────────────────────

def test_all_toggles_off():
    cb = calculate_costs(
        ticker="AAPL", asset_type="stock",
        quantity=1000, price=182.50,
        bid=182.45, ask=182.55, adv=50_000_000,
        toggles=SlippageToggles(spread=False, commission=False, market_impact=False),
    )
    assert cb.total_cost == 0.0
    assert cb.spread_cost == 0.0
    assert cb.commission_cost == 0.0
    assert cb.market_impact_cost == 0.0


def test_all_toggles_on_total_is_sum():
    cb = calculate_costs(
        ticker="AAPL", asset_type="stock",
        quantity=1000, price=182.50,
        bid=182.45, ask=182.55, adv=50_000_000,
        toggles=SlippageToggles(spread=True, commission=True, market_impact=True),
    )
    assert cb.total_cost == pytest.approx(
        cb.spread_cost + cb.commission_cost + cb.market_impact_cost
    )


def test_cost_bps_calculation():
    cb = calculate_costs(
        ticker="AAPL", asset_type="stock",
        quantity=1000, price=100.0,
        bid=99.95, ask=100.05, adv=50_000_000,
        toggles=SlippageToggles(spread=True, commission=False, market_impact=False),
    )
    # spread = 0.05 * 1000 * 2 = 100, notional = 100_000
    # bps = 100 / 100_000 * 10_000 = 10 bps
    assert cb.cost_bps == pytest.approx(10.0)


# ── Notional tests ────────────────────────────────────────────────────────────

def test_stock_notional():
    cb = calculate_costs("AAPL", "stock", quantity=500, price=200.0,
                          toggles=SlippageToggles(False, False, False))
    assert cb.notional == pytest.approx(100_000.0)


def test_option_notional():
    cb = calculate_costs("AAPL", "option", quantity=10, price=3.0,
                          multiplier=100,
                          toggles=SlippageToggles(False, False, False))
    assert cb.notional == pytest.approx(3_000.0)
