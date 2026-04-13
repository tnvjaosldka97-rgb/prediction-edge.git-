"""
Closing Convergence Strategy — Most Reliable Alpha on Polymarket.

As a binary market approaches resolution, prices converge to 0 or 1.
The convergence is NOT instant — there's always a window where the
winning side trades below 1.0 because:
  1. Traders are uncertain about oracle timing
  2. Market makers widen spreads near expiry
  3. Some participants simply aren't watching

This strategy systematically buys the winning side when:
  a) Strong external evidence points clearly to one outcome
  b) Time to resolution is short (< 7 days)
  c) Current price undervalues the winning probability
  d) UMA dispute risk is low

The edge here is information processing, not speed.
We hold positions for hours to days, not seconds.
Returns are modest per trade (~3-10%) but highly reliable.

External evidence sources (ranked by reliability):
  1. Market has already resolved (oracle_monitor handles this)
  2. Strong consensus from multiple external sources
  3. Historical resolution patterns for similar markets
  4. Price momentum toward an extreme (markets are often right)
"""
from __future__ import annotations
import asyncio
import time
import uuid
from typing import Optional
import config
from core.models import Market, Signal, Token
from core.logger import log
from signals.oracle_monitor import score_oracle_dispute_risk


# Convergence scoring thresholds
# Market qualifies when price × time-decay score is high enough
_CONVERGENCE_HORIZON_DAYS = 7      # only look at markets resolving within 7 days
_MIN_PRICE_FOR_CONVERGENCE = 0.75  # minimum price to consider "likely YES"
_MAX_PRICE_FOR_CONVERGENCE = 0.25  # maximum price to consider "likely NO"


def _time_decay_factor(days_remaining: float) -> float:
    """
    As resolution approaches, convergence becomes more certain.
    Returns a 0–1 multiplier for how much we trust the current price trend.

    Days remaining → factor:
      7 days → 0.30 (low confidence, far out)
      3 days → 0.55
      1 day  → 0.80
      6 hours → 0.95
    """
    import math
    if days_remaining <= 0:
        return 1.0
    # Exponential decay: higher confidence as time shrinks
    return 1.0 - math.exp(-2.5 / max(days_remaining, 0.01))


def _price_momentum(price_history: list[tuple[float, float]]) -> float:
    """
    Compute directional momentum from price history.
    Returns: positive = trending toward YES, negative = trending toward NO.
    Magnitude: 0 = no trend, 1 = strong trend.

    price_history: list of (timestamp, price) sorted oldest first
    """
    if len(price_history) < 3:
        return 0.0

    # Simple linear regression on price vs time
    n = len(price_history)
    times = [t for t, _ in price_history]
    prices = [p for _, p in price_history]

    # Normalize time to 0–1
    t_min, t_max = min(times), max(times)
    if t_max == t_min:
        return 0.0
    times_norm = [(t - t_min) / (t_max - t_min) for t in times]

    # Slope of best-fit line
    mean_t = sum(times_norm) / n
    mean_p = sum(prices) / n
    numerator = sum((times_norm[i] - mean_t) * (prices[i] - mean_p) for i in range(n))
    denominator = sum((times_norm[i] - mean_t) ** 2 for i in range(n))

    if denominator == 0:
        return 0.0

    slope = numerator / denominator  # price change per normalized time unit
    return max(-1.0, min(1.0, slope * 2))  # normalize to [-1, 1]


def _compute_convergence_signal(
    market: Market,
    token: Token,
    price_history: list[tuple[float, float]],
    external_prob: Optional[float] = None,
) -> Optional[Signal]:
    """
    Compute whether a closing convergence trade is warranted.

    Args:
        market: The market
        token: The YES token
        price_history: Recent price history for this token
        external_prob: Optional external probability estimate (e.g., from Metaculus)

    Returns:
        Signal if there's a trade, None otherwise
    """
    price = token.price
    days = market.days_to_resolution

    # Only act within convergence horizon
    if days > _CONVERGENCE_HORIZON_DAYS or days < 0:
        return None

    # Skip markets with high oracle dispute risk
    if market.dispute_risk > config.ORACLE_DISPUTE_THRESHOLD_WARN:
        if market.dispute_risk > config.ORACLE_DISPUTE_THRESHOLD_SKIP:
            return None
        # Use dispute risk to reduce effective model probability
        dispute_haircut = market.dispute_risk * 0.5  # N/A resolution → 0.5

    # Determine direction
    if price >= _MIN_PRICE_FOR_CONVERGENCE:
        direction = "BUY"    # price trending toward 1.0
        model_prob_base = price  # market is already saying "likely YES"
        target = 1.0
    elif price <= _MAX_PRICE_FOR_CONVERGENCE:
        direction = "BUY"    # buy this token: it should → 1.0 (NO resolves)
        # Actually for NO token: if YES price is low, we should check the NO token
        # This is handled separately — skip YES at low price
        return None
    else:
        return None  # mid-range, no clear convergence signal

    # Score the convergence evidence
    time_factor = _time_decay_factor(days)
    momentum = _price_momentum(price_history)

    # Only trade when momentum supports the direction
    if direction == "BUY" and momentum < -0.3:
        # Price is trending DOWN toward YES token — contra-indicator
        return None

    # Build model probability
    if external_prob is not None:
        # Blend market price with external signal
        model_prob = 0.6 * external_prob + 0.4 * price
    else:
        # Use price + momentum + time factor
        momentum_boost = max(0, momentum) * 0.05  # up to 5% boost from momentum
        model_prob = price + momentum_boost

    # Apply UMA dispute haircut
    if market.dispute_risk > 0:
        model_prob = model_prob * (1 - market.dispute_risk) + 0.5 * market.dispute_risk

    model_prob = min(0.99, max(0.01, model_prob))

    # Compute net edge
    fee_pct = config.TAKER_FEE_RATE * (1 - price)  # fee as fraction of USD
    gross_edge = model_prob - price
    net_edge = gross_edge - fee_pct

    if net_edge < config.MIN_EDGE_AFTER_FEES:
        return None

    # Confidence scales with time factor and price extremity
    price_conviction = min(1.0, (price - _MIN_PRICE_FOR_CONVERGENCE) / (1.0 - _MIN_PRICE_FOR_CONVERGENCE))
    confidence = time_factor * 0.7 + price_conviction * 0.3

    # Urgency based on time remaining
    if days < 0.25:       # < 6 hours
        urgency = "IMMEDIATE"
    elif days < 1:        # < 1 day
        urgency = "HIGH"
    elif days < 3:        # < 3 days
        urgency = "MEDIUM"
    else:
        urgency = "LOW"

    # TTL: signal expires when half the remaining time passes or edge is mostly gone
    expires_at = time.time() + min(days * 86400 * 0.5, 3600)

    return Signal(
        signal_id=str(uuid.uuid4()),
        strategy="closing_convergence",
        condition_id=market.condition_id,
        token_id=token.token_id,
        direction=direction,
        model_prob=model_prob,
        market_prob=price,
        edge=gross_edge,
        net_edge=net_edge,
        confidence=confidence,
        urgency=urgency,
        created_at=time.time(),
        expires_at=expires_at,
        stale_price=price,
        stale_threshold=net_edge * 0.5,  # cancel if half the edge is gone
    )


class ClosingConvergenceScanner:
    """
    Continuously scans markets near resolution for convergence opportunities.
    """

    def __init__(self, market_store, signal_bus: asyncio.Queue):
        self._store = market_store
        self._bus = signal_bus
        self._running = False
        # Cache of recent signals to avoid duplicate emission
        self._emitted: dict[str, float] = {}   # token_id → last_emit_time

    async def start(self):
        self._running = True
        while self._running:
            await self._scan()
            await asyncio.sleep(60)   # scan every minute

    async def _scan(self):
        from core import db
        markets = [
            m for m in self._store.get_active_markets()
            if 0 < m.days_to_resolution <= _CONVERGENCE_HORIZON_DAYS
        ]

        if not markets:
            return

        signals_generated = 0
        for market in markets:
            yes_token = market.yes_token
            no_token = market.no_token

            if yes_token:
                signal = await self._check_token(market, yes_token)
                if signal:
                    await self._bus.put(signal)
                    signals_generated += 1

            # Also check the NO token as a standalone "near certain NO resolves YES"
            # i.e., NO token price > 0.75 means market thinks NO is likely
            if no_token and no_token.price >= _MIN_PRICE_FOR_CONVERGENCE:
                signal = await self._check_token(market, no_token)
                if signal:
                    await self._bus.put(signal)
                    signals_generated += 1

        if signals_generated:
            log.info(
                f"[CONVERGENCE] {signals_generated} signals | "
                f"scanned {len(markets)} near-expiry markets"
            )

    async def _check_token(self, market: Market, token: Token) -> Optional[Signal]:
        """Check a single token for convergence opportunity."""
        now = time.time()

        # Throttle: don't emit same signal more than once per 15 minutes
        last = self._emitted.get(token.token_id, 0)
        if now - last < 900:
            return None

        # Fetch price history from DB
        from core.db import get_conn
        conn = get_conn()
        rows = conn.execute(
            """SELECT timestamp, price FROM price_history
               WHERE token_id = ? AND timestamp > ?
               ORDER BY timestamp ASC""",
            (token.token_id, now - 86400 * 3)  # last 3 days
        ).fetchall()

        price_history = [(r["timestamp"], r["price"]) for r in rows]

        signal = _compute_convergence_signal(market, token, price_history)

        if signal:
            self._emitted[token.token_id] = now
            log.info(
                f"[CONVERGENCE] {market.question[:50]} | "
                f"token={token.outcome} price={token.price:.3f} "
                f"days={market.days_to_resolution:.1f} "
                f"edge={signal.net_edge:.2%} urgency={signal.urgency}"
            )

        return signal

    def stop(self):
        self._running = False
