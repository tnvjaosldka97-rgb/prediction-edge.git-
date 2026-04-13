"""
Hard risk limits — checked before every order submission.
If any check fails, the order is rejected regardless of edge.

These are the circuit breakers that prevent catastrophic losses.
"""
from __future__ import annotations
from core.models import Order, Signal, PortfolioState, Market
from core.logger import log
import config


def check_all(
    order: Order,
    signal: Signal | None,
    portfolio: PortfolioState,
    market: Market | None,
    store=None,
) -> tuple[bool, str]:
    """
    Run all risk checks. Returns (allowed, rejection_reason).
    All checks must pass for the order to be submitted.
    """

    # 1. Drawdown halt
    if portfolio.drawdown >= config.MAX_DRAWDOWN_HALT:
        return False, f"DRAWDOWN HALT: {portfolio.drawdown:.1%} >= {config.MAX_DRAWDOWN_HALT:.1%}"

    # 2. Single market concentration
    current_notional = sum(
        p.notional_usd for p in portfolio.positions.values()
        if p.condition_id == order.condition_id
    )
    new_notional = current_notional + order.size_usd
    max_notional = portfolio.bankroll * config.MAX_SINGLE_MARKET_PCT
    if new_notional > max_notional:
        return False, f"MARKET LIMIT: ${new_notional:.0f} > ${max_notional:.0f}"

    # 3. Oracle dispute risk
    if market and market.dispute_risk > config.ORACLE_DISPUTE_THRESHOLD_SKIP:
        return False, f"ORACLE RISK: {market.dispute_risk:.1%} > {config.ORACLE_DISPUTE_THRESHOLD_SKIP:.1%}"

    # 4. Minimum edge after fees
    if signal and signal.net_edge < config.MIN_EDGE_AFTER_FEES:
        return False, f"EDGE TOO LOW: {signal.net_edge:.3f} < {config.MIN_EDGE_AFTER_FEES}"

    # 5. Signal staleness (if current price available)
    if signal and signal.stale_threshold > 0:
        token = None
        if market:
            for t in market.tokens:
                if t.token_id == order.token_id:
                    token = t
                    break
        if token and signal.is_stale(token.price):
            return False, f"STALE SIGNAL: price moved too far from {signal.stale_price:.3f}"

    # 6. Signal expiry
    if signal and signal.is_expired():
        return False, "EXPIRED SIGNAL"

    # 7. Drawdown reduction (halve sizes but don't halt)
    if portfolio.drawdown >= config.MAX_DRAWDOWN_REDUCE:
        # Don't reject, but caller should halve size (enforced in gateway)
        pass  # handled separately in gateway.py

    # 8. Category concentration — per-category using market store lookup
    if market and market.category:
        category = market.category
        if store is not None:
            category_notional = sum(
                p.notional_usd for p in portfolio.positions.values()
                if (m := store.get_market(p.condition_id)) and m.category == category
            )
        else:
            # Fallback: count only current market's existing exposure
            category_notional = sum(
                p.notional_usd for p in portfolio.positions.values()
                if p.condition_id == order.condition_id
            )
        new_category_notional = category_notional + order.size_usd
        max_category_notional = portfolio.bankroll * config.MAX_CATEGORY_PCT
        if new_category_notional > max_category_notional:
            return False, (
                f"CATEGORY LIMIT: {category!r} "
                f"${new_category_notional:.0f} > ${max_category_notional:.0f}"
            )

    return True, "ok"


def should_halve_size(portfolio: PortfolioState) -> bool:
    """True if current drawdown triggers the 50% size reduction."""
    return portfolio.drawdown >= config.MAX_DRAWDOWN_REDUCE
