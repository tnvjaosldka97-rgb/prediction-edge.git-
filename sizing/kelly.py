"""
Kelly Criterion with:
1. Empirical calibration — bet sizes scale with how accurate your model is
2. Annualized return adjustment — prefer short-dated markets
3. Uncertainty shrinkage — shrink toward 0.5 when calibration error is high
4. Phase-in schedule — starts conservative, scales up with trade count

CRITICAL: Kelly with wrong model_prob = ruin. These adjustments prevent that.
"""
from __future__ import annotations
import math
from core import db
import config
from core.logger import log


def _get_sharpe_multiplier() -> float:
    """
    Adjust Kelly fraction based on recent portfolio Sharpe ratio.
    Uses closed-trade returns from DB to compute rolling Sharpe.

    Range: 0.60x (losing streak) → 1.30x (strong edge confirmed)
    Only kicks in after 10+ closed trades — neutral before that.
    """
    import math as _math
    returns = db.get_recent_trade_returns(limit=30)
    if len(returns) < 10:
        return 1.0

    mean = sum(returns) / len(returns)
    variance = sum((r - mean) ** 2 for r in returns) / len(returns)
    std = _math.sqrt(variance) if variance > 0 else 0.01
    sharpe = mean / std

    if sharpe > 1.5:
        return 1.30
    elif sharpe > 1.0:
        return 1.15
    elif sharpe > 0.5:
        return 1.00
    elif sharpe > 0.0:
        return 0.85
    else:
        return 0.60   # negative Sharpe → halve bets until we diagnose


def _get_kelly_fraction(trade_count: int) -> float:
    """Phase-in Kelly fraction based on number of calibrated trades."""
    phases = sorted(config.KELLY_CALIBRATION_PHASES.items())
    fraction = phases[0][1]
    for min_trades, f in phases:
        if trade_count >= min_trades:
            fraction = f
    return fraction


def _shrink_probability(model_prob: float, calibration_error: float) -> float:
    """
    Shrink model probability toward 0.5 proportional to calibration error.

    When calibration_error = 0 (perfect model): no shrinkage
    When calibration_error = 0.5 (random model): full shrinkage to 0.5

    This prevents over-betting when the model is unreliable.
    """
    shrinkage = min(calibration_error * 2, 1.0)  # 0–1
    return model_prob * (1 - shrinkage) + 0.5 * shrinkage


def compute_kelly(
    model_prob: float,
    market_price: float,
    bankroll: float,
    days_to_resolution: float,
    strategy: str,
    fee_cost_per_dollar: float = 0.0,
) -> float:
    """
    Returns optimal position size in USD.

    Args:
        model_prob: Your estimated probability of YES resolving
        market_price: Current YES token price
        bankroll: Available capital
        days_to_resolution: Days until market resolves
        strategy: Used to look up calibration stats
        fee_cost_per_dollar: Fee as fraction of position size

    Returns:
        Position size in USD (0 if no edge)
    """
    # Step 1: Get calibration stats for this strategy
    cal = db.get_calibration_stats(strategy)
    trade_count = cal["count"]
    calibration_error = cal["calibration_error"]

    # Step 2: Shrink probability toward 0.5 based on calibration error
    adjusted_prob = _shrink_probability(model_prob, calibration_error)

    # Step 3: Compute net odds (after fees)
    # On a YES buy: win (1 - market_price) per dollar, lose market_price per dollar
    # After fees: win (1 - market_price - fee) per dollar
    win_per_dollar = (1 - market_price) / market_price   # b in Kelly formula
    win_after_fees = win_per_dollar - fee_cost_per_dollar / market_price

    if win_after_fees <= 0:
        return 0.0

    lose_prob = 1 - adjusted_prob

    # Step 4: Full Kelly fraction
    full_kelly = (adjusted_prob * win_after_fees - lose_prob) / win_after_fees

    if full_kelly <= 0:
        log.debug(f"No edge after calibration: raw={model_prob:.3f} adj={adjusted_prob:.3f} market={market_price:.3f}")
        return 0.0

    # Step 5: Time-horizon adjustment
    # Prediction markets: longer horizon = more uncertainty = smaller bet
    # But we don't annualize aggressively — it over-sizes short-term bets
    days = max(1, days_to_resolution)
    # Gentle boost for short-dated markets (max 4x for same-day markets)
    time_mult = min(30.0 / days, 4.0)
    adjusted_kelly = full_kelly * time_mult

    # Step 6: Apply phase-in fraction × Sharpe multiplier
    phase_fraction  = _get_kelly_fraction(trade_count)
    sharpe_mult     = _get_sharpe_multiplier()
    final_fraction  = min(adjusted_kelly * phase_fraction * sharpe_mult, 0.08)  # hard cap 8%

    # Step 7: Hard cap per market
    max_per_market = bankroll * config.MAX_SINGLE_MARKET_PCT

    size = min(bankroll * final_fraction, max_per_market)

    log.debug(
        f"Kelly: model={model_prob:.3f} adj={adjusted_prob:.3f} "
        f"market={market_price:.3f} edge={full_kelly:.4f} "
        f"annual_mult={annualized_multiplier:.1f}x "
        f"phase={phase_fraction} "
        f"cal_trades={trade_count} cal_err={calibration_error:.3f} "
        f"size=${size:.2f}"
    )
    return size


def compute_kelly_for_arb(
    gross_profit_pct: float,
    leg_fail_prob: float,
    leg_fail_loss_pct: float,
    bankroll: float,
) -> float:
    """
    Kelly for internal YES+NO arb with explicit leg risk.

    NOT risk-free. Models the probability that the second leg fails
    and we end up with a naked directional position.

    Args:
        gross_profit_pct: (1 - YES_ask - NO_ask) as fraction
        leg_fail_prob: P(second leg doesn't fill at expected price)
        leg_fail_loss_pct: Expected loss if second leg fails
        bankroll: Available capital
    """
    # EV = p_success * gross_profit - p_fail * leg_fail_loss - fees
    # This is not Kelly directly, but a simple EV-based sizing
    ev = (1 - leg_fail_prob) * gross_profit_pct - leg_fail_prob * leg_fail_loss_pct
    if ev <= 0:
        return 0.0

    # Conservative sizing: never more than 2% bankroll on arb due to execution risk
    # Kelly on EV: f = ev / (1 + ev) approximately for small ev
    kelly_raw = ev / max(gross_profit_pct, 0.001)
    fraction = min(kelly_raw * 0.1, 0.02)  # max 2% bankroll
    return bankroll * fraction
