"""
UMA Oracle Resolution Monitor.

When a market resolves, prices should converge to 0 or 1.
But the convergence is NOT instant — slow traders and inattentive bots
create a window of 2-10 minutes where you can buy YES at 0.85 in a
market that just resolved YES (and will settle at 1.00).

This is the cleanest, most reliable alpha on Polymarket.
Risk: Oracle dispute (N/A resolution). Handle with position sizing.
"""
from __future__ import annotations
import asyncio
import time
import uuid
from typing import Optional
import httpx
import config
from core.models import Signal, Market
from core.logger import log


class OracleMonitor:
    """
    Polls for market resolutions and emits IMMEDIATE convergence signals.

    Signal logic:
    - Market just resolved YES → buy YES token (price should → 1.0)
    - Market just resolved NO → buy NO token (price should → 1.0)
    - Size carefully: oracle disputes can reverse the resolution
    """

    def __init__(self, market_store, signal_bus: asyncio.Queue):
        self._store = market_store
        self._bus = signal_bus
        self._seen_resolutions: set[str] = set()
        self._running = False

    async def start(self):
        self._running = True
        while self._running:
            await self._check_resolutions()
            await asyncio.sleep(15)  # poll every 15 seconds

    async def _check_resolutions(self):
        """
        Check for markets that have recently resolved.
        Polymarket reports `winner: true` on tokens once UMA settles.
        """
        markets: list[Market] = list(self._store.get_all_markets())

        for market in markets:
            if market.condition_id in self._seen_resolutions:
                continue

            winning_token = None
            for token in market.tokens:
                if token.winner is True:
                    winning_token = token
                    break

            if not winning_token:
                continue

            # Market has resolved — check if price has already converged
            current_price = winning_token.price
            remaining_gain = 1.0 - current_price

            if remaining_gain < 0.01:
                # Already fully converged — too late
                self._seen_resolutions.add(market.condition_id)
                continue

            # Calculate net edge after fees
            fee = winning_token.fee_cost(100, is_taker=True) / 100
            net_edge = remaining_gain - fee

            if net_edge < config.MIN_EDGE_AFTER_FEES:
                self._seen_resolutions.add(market.condition_id)
                continue

            # Oracle dispute risk check
            if market.dispute_risk > config.ORACLE_DISPUTE_THRESHOLD_SKIP:
                log.warning(
                    f"Skipping resolved market {market.condition_id[:8]} "
                    f"due to high dispute risk: {market.dispute_risk:.1%}"
                )
                self._seen_resolutions.add(market.condition_id)
                continue

            # Emit IMMEDIATE signal
            signal = Signal(
                signal_id=str(uuid.uuid4()),
                strategy="oracle_convergence",
                condition_id=market.condition_id,
                token_id=winning_token.token_id,
                direction="BUY",
                model_prob=1.0,          # resolved = certain (modulo dispute)
                market_prob=current_price,
                edge=remaining_gain,
                net_edge=net_edge,
                confidence=1.0 - market.dispute_risk,
                urgency="IMMEDIATE",
                created_at=time.time(),
                expires_at=time.time() + config.ORACLE_CONVERGENCE_WINDOW_SEC,
                stale_price=current_price,
                stale_threshold=remaining_gain * 0.5,  # cancel if half the edge is gone
            )

            log.info(
                f"[ORACLE] Resolution detected: {market.question[:60]} "
                f"winner={winning_token.outcome} "
                f"current_price={current_price:.3f} "
                f"net_edge={net_edge:.2%}"
            )

            await self._bus.put(signal)
            self._seen_resolutions.add(market.condition_id)

    def stop(self):
        self._running = False


# ── Oracle Dispute Risk Scorer ───────────────────────────────────────────────

_AMBIGUOUS_KEYWORDS = [
    "could", "might", "likely", "arguably", "approximately",
    "substantially", "significant", "major", "primary",
    "if applicable", "at the discretion",
]

_DISPUTE_PRONE_CATEGORIES = {
    "politics": 0.04,
    "crypto": 0.03,
    "sports": 0.01,
    "science": 0.02,
    "economics": 0.03,
}


def score_oracle_dispute_risk(market: Market) -> float:
    """
    Estimate P(UMA dispute) for a market.

    Higher risk = skip or reduce position size.
    Returns float between 0 and 1.
    """
    question = market.question.lower()
    risk = 0.01  # base rate

    # Keyword ambiguity
    ambiguity_hits = sum(1 for kw in _AMBIGUOUS_KEYWORDS if kw in question)
    risk += ambiguity_hits * 0.01

    # Category base rate
    category = market.category.lower()
    risk += _DISPUTE_PRONE_CATEGORIES.get(category, 0.02)

    # Extreme prices are more likely to be disputed (someone has strong reason to dispute)
    if market.yes_token and market.yes_token.price > 0.95:
        risk += 0.01  # people dispute near-resolved markets more

    # Long time until resolution = more things can go wrong
    if market.days_to_resolution > 180:
        risk += 0.01

    return min(risk, 0.50)  # cap at 50%
