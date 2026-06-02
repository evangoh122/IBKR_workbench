"""
etl/slippage.py
Transaction cost model for stocks and options.

Three components (each toggleable):
  1. Bid-ask spread cost      — half-spread paid on entry + exit
  2. Commission               — IBKR tiered pricing
  3. Market impact estimate   — square-root model (simplified Almgren-Chriss)

All costs returned in dollars per trade (round-trip unless noted).
"""
from dataclasses import dataclass, field
from typing import Optional
import math


# ── IBKR Commission Schedule (as of 2024) ────────────────────────────────────

# Stocks: tiered per-share
IBKR_STOCK_TIERS = [
    (300_000,    0.0035),   # ≤ 300k shares/mo
    (3_000_000,  0.0020),
    (20_000_000, 0.0015),
    (100_000_000,0.0010),
    (float("inf"),0.0005),
]
IBKR_STOCK_MIN     = 0.35   # min per order
IBKR_STOCK_MAX_PCT = 0.01   # max 1% of trade value

# Options: per-contract
IBKR_OPT_BASE      = 0.65   # $ per contract
IBKR_OPT_MIN       = 1.00   # min per order
IBKR_OPT_MAX       = 0.65   # (no cap for standard)

# Exchange/regulatory fees (approximate)
IBKR_STOCK_REG_FEE = 0.000008   # per share (SEC + FINRA)
IBKR_OPT_REG_FEE   = 0.02       # per contract


# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class SlippageToggles:
    spread:        bool = True
    commission:    bool = True
    market_impact: bool = True


@dataclass
class CostBreakdown:
    # Inputs
    ticker:        str
    asset_type:    str          # 'stock' | 'option'
    quantity:      float        # shares or contracts
    price:         float        # mid price
    bid:           Optional[float] = None
    ask:           Optional[float] = None
    # Option-specific
    multiplier:    float        = 100.0
    # Market context
    adv:           Optional[float] = None  # average daily volume (shares)

    # Computed costs (populated by calculate())
    spread_cost:        float = 0.0
    commission_cost:    float = 0.0
    market_impact_cost: float = 0.0
    total_cost:         float = 0.0
    cost_bps:           float = 0.0   # total as basis points of notional

    # Notional
    notional: float = field(init=False, default=0.0)

    def __post_init__(self):
        if self.asset_type == "stock":
            self.notional = self.price * self.quantity
        else:
            self.notional = self.price * self.quantity * self.multiplier


# ── Main calculator ───────────────────────────────────────────────────────────

def calculate_costs(
    ticker:     str,
    asset_type: str,            # 'stock' | 'option'
    quantity:   float,
    price:      float,
    bid:        Optional[float] = None,
    ask:        Optional[float] = None,
    multiplier: float = 100.0,
    adv:        Optional[float] = None,
    toggles:    SlippageToggles = None,
    monthly_shares: float = 0,  # for IBKR tier lookup
) -> CostBreakdown:
    """
    Calculate round-trip transaction costs.

    Parameters
    ----------
    quantity        : shares (stock) or contracts (option)
    price           : mid-market price
    bid / ask       : for spread calculation
    adv             : average daily volume in shares (for market impact)
    monthly_shares  : cumulative monthly shares traded (IBKR tier)
    """
    if toggles is None:
        toggles = SlippageToggles()

    cb = CostBreakdown(
        ticker=ticker, asset_type=asset_type,
        quantity=quantity, price=price,
        bid=bid, ask=ask,
        multiplier=multiplier, adv=adv,
    )

    # 1. Spread cost ──────────────────────────────────────────────────────────
    if toggles.spread and bid is not None and ask is not None:
        half_spread = (ask - bid) / 2.0
        if asset_type == "stock":
            # Pay half-spread on entry + exit (round trip)
            cb.spread_cost = half_spread * quantity * 2
        else:
            cb.spread_cost = half_spread * quantity * multiplier * 2

    # 2. Commission ───────────────────────────────────────────────────────────
    if toggles.commission:
        if asset_type == "stock":
            cb.commission_cost = _ibkr_stock_commission(
                quantity, price, monthly_shares
            )
        else:
            cb.commission_cost = _ibkr_option_commission(quantity)

    # 3. Market impact ────────────────────────────────────────────────────────
    if toggles.market_impact and adv is not None and adv > 0:
        if asset_type == "stock":
            cb.market_impact_cost = _market_impact_stock(
                quantity, price, adv
            )
        else:
            # For options use underlying ADV with reduced impact
            cb.market_impact_cost = _market_impact_stock(
                quantity * multiplier, price, adv, scale=0.3
            )

    cb.total_cost = cb.spread_cost + cb.commission_cost + cb.market_impact_cost

    if cb.notional > 0:
        cb.cost_bps = (cb.total_cost / cb.notional) * 10_000

    return cb


# ── Commission calculators ────────────────────────────────────────────────────

def _ibkr_stock_commission(shares: float, price: float,
                            monthly_shares: float = 0) -> float:
    """IBKR tiered stock commission, round-trip."""
    rate = IBKR_STOCK_TIERS[0][1]
    cumulative = monthly_shares
    for threshold, tier_rate in IBKR_STOCK_TIERS:
        if cumulative <= threshold:
            rate = tier_rate
            break

    per_leg = max(
        IBKR_STOCK_MIN,
        min(shares * rate + shares * IBKR_STOCK_REG_FEE,
            price * shares * IBKR_STOCK_MAX_PCT)
    )
    return per_leg * 2   # round-trip


def _ibkr_option_commission(contracts: float) -> float:
    """IBKR option commission, round-trip."""
    per_leg = max(
        IBKR_OPT_MIN,
        contracts * IBKR_OPT_BASE + contracts * IBKR_OPT_REG_FEE
    )
    return per_leg * 2


# ── Market impact model ───────────────────────────────────────────────────────

def _market_impact_stock(shares: float, price: float,
                          adv: float, scale: float = 1.0) -> float:
    """
    Simplified square-root market impact model.

    impact_bps = scale * sigma * sqrt(quantity / ADV)

    We proxy daily volatility (sigma) at 1.5% (roughly S&P average).
    Multiply by notional to get dollar impact (one-way), double for round-trip.
    """
    SIGMA = 0.015   # 1.5% daily vol proxy
    participation = shares / adv
    impact_pct = scale * SIGMA * math.sqrt(participation)
    notional    = shares * price
    return impact_pct * notional * 2   # round-trip


# ── Batch helper ─────────────────────────────────────────────────────────────

def calculate_costs_batch(rows: list[dict],
                           toggles: SlippageToggles = None) -> list[CostBreakdown]:
    """
    rows: list of dicts with keys matching calculate_costs() parameters.
    Returns a list of CostBreakdown objects.
    """
    return [calculate_costs(**r, toggles=toggles) for r in rows]
