"""
Parameter Optimizer — grid search over historical signal outcomes.

HOW IT WORKS:
  1. Load all resolved signals from DB (was_correct is known)
  2. For each parameter combination, filter signals by those params
  3. Simulate what PnL would have been
  4. Pick combination that maximizes Sharpe ratio

WHAT GETS OPTIMIZED:
  - MIN_EDGE_AFTER_FEES: filter weak signals (global)
  - KELLY_FRACTION: bet sizing (global)
  - Per-strategy confidence threshold: only high-conviction signals
  - Convergence price threshold: when to enter closing_convergence
"""
from __future__ import annotations
import math
from core import db
from core.logger import log
import config


# ── Grid definitions ──────────────────────────────────────────────────────────

EDGE_GRID        = [0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.10]
KELLY_GRID       = [0.03, 0.05, 0.07, 0.10, 0.12, 0.15]
CONFIDENCE_GRID  = [0.0, 0.3, 0.4, 0.5, 0.6, 0.7]

STRATEGIES = [
    "oracle_convergence", "closing_convergence", "fee_arbitrage",
    "order_flow", "correlated_arb", "cross_platform", "internal_arb",
]


# ── Core math ─────────────────────────────────────────────────────────────────

def _sharpe(returns: list[float]) -> float:
    if len(returns) < 5:
        return -999.0
    mean = sum(returns) / len(returns)
    var  = sum((r - mean) ** 2 for r in returns) / len(returns)
    std  = math.sqrt(var) if var > 0 else 0.001
    return mean / std


def _simulate_trade_returns(
    signals: list[dict],
    min_edge: float       = 0.0,
    min_confidence: float = 0.0,
) -> list[float]:
    """
    Given a list of resolved signals, compute what the trade returns would be
    under the given filter params.
    """
    returns = []
    for s in signals:
        if s["net_edge"] < min_edge:
            continue
        if s["confidence"] < min_confidence:
            continue
        if s["was_correct"] is None:
            continue

        price = s["market_prob"]
        if price <= 0 or price >= 1:
            continue

        fee = config.TAKER_FEE_RATE * (1 - price)

        if s["was_correct"]:
            ret = (1.0 - price - fee) / price   # profit on winning trade
        else:
            ret = -(1.0 + fee)                   # lost the position + fee

        returns.append(ret)
    return returns


# ── Optimizers ────────────────────────────────────────────────────────────────

def optimize_global_edge_threshold(signals: list[dict]) -> float:
    """Find MIN_EDGE_AFTER_FEES that maximizes portfolio Sharpe."""
    best_edge   = config.MIN_EDGE_AFTER_FEES
    best_sharpe = _sharpe(_simulate_trade_returns(signals, min_edge=config.MIN_EDGE_AFTER_FEES))

    for edge in EDGE_GRID:
        returns = _simulate_trade_returns(signals, min_edge=edge)
        if len(returns) < 10:
            continue
        s = _sharpe(returns)
        if s > best_sharpe:
            best_sharpe = s
            best_edge   = edge

    log.info(f"[OPT] Global edge threshold: {best_edge:.2f} → Sharpe {best_sharpe:.3f}")
    return best_edge


def optimize_kelly_fraction(signals: list[dict]) -> float:
    """
    Find the Kelly fraction that would have produced the best risk-adjusted returns.
    Uses simulated bet sizing with historical win rates.
    """
    if len(signals) < 20:
        return config.KELLY_FRACTION

    accuracy = sum(1 for s in signals if s.get("was_correct")) / len(signals)
    avg_edge = sum(s["net_edge"] for s in signals) / len(signals)

    # Theoretical full Kelly = edge / avg_odds
    avg_price = sum(s["market_prob"] for s in signals) / len(signals)
    avg_odds  = (1 - avg_price) / avg_price
    full_kelly = (accuracy * avg_odds - (1 - accuracy)) / avg_odds if avg_odds > 0 else 0

    if full_kelly <= 0:
        return 0.03  # edge too low, minimum fraction

    # Apply conservative fraction (25% of full Kelly is standard practice)
    optimal = min(full_kelly * 0.25, 0.15)
    log.info(f"[OPT] Kelly fraction: {optimal:.3f} (full={full_kelly:.3f}, accuracy={accuracy:.1%})")
    return round(optimal, 3)


def optimize_strategy_confidence(strategy: str, signals: list[dict]) -> float:
    """Find the confidence threshold that maximizes Sharpe for a given strategy."""
    strategy_signals = [s for s in signals if s["strategy"] == strategy]
    if len(strategy_signals) < 10:
        return 0.0

    best_conf   = 0.0
    best_sharpe = -999.0

    for conf in CONFIDENCE_GRID:
        returns = _simulate_trade_returns(strategy_signals, min_confidence=conf)
        if len(returns) < 5:
            continue
        s = _sharpe(returns)
        if s > best_sharpe:
            best_sharpe = s
            best_conf   = conf

    log.info(f"[OPT] {strategy} confidence threshold: {best_conf:.2f} → Sharpe {best_sharpe:.3f}")
    return best_conf


# ── Main entry ─────────────────────────────────────────────────────────────────

def run_full_optimization() -> dict:
    """
    Run complete optimization across all parameters.
    Returns dict of optimized param values to be saved.
    """
    conn = db.get_conn()
    rows = conn.execute(
        """SELECT strategy, net_edge, market_prob, model_prob,
                  confidence, was_correct, created_at
           FROM signals
           WHERE was_correct IS NOT NULL
           ORDER BY created_at DESC
           LIMIT 2000"""
    ).fetchall()

    signals = [dict(r) for r in rows]
    n = len(signals)

    if n < 20:
        log.info(f"[OPT] Only {n} resolved signals — need 20+ to optimize. Skipping.")
        return {}

    log.info(f"[OPT] Optimizing on {n} resolved signals across all strategies")

    result: dict = {}

    # Global params
    result["MIN_EDGE_AFTER_FEES"] = optimize_global_edge_threshold(signals)
    result["KELLY_FRACTION"]      = optimize_kelly_fraction(signals)

    # Per-strategy confidence thresholds
    for strategy in STRATEGIES:
        conf = optimize_strategy_confidence(strategy, signals)
        if conf > 0:
            result[f"{strategy}_min_confidence"] = conf

    # Compute overall performance summary
    base_returns = _simulate_trade_returns(signals)
    opt_returns  = _simulate_trade_returns(
        signals,
        min_edge=result["MIN_EDGE_AFTER_FEES"],
    )
    if base_returns and opt_returns:
        log.info(
            f"[OPT] Baseline Sharpe: {_sharpe(base_returns):.3f}  "
            f"→  Optimized Sharpe: {_sharpe(opt_returns):.3f}  "
            f"(trades: {len(base_returns)} → {len(opt_returns)})"
        )

    return result
