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
            await self._check_recently_closed()
            await asyncio.sleep(15)

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

    async def _check_recently_closed(self):
        """
        Separately fetch recently-resolved markets from Gamma API.
        Active market store only has open markets — this catches the resolution window.
        """
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(
                    f"{config.GAMMA_HOST}/markets",
                    params={"closed": "true", "limit": 20, "order": "updatedAt", "ascending": "false"},
                )
                resp.raise_for_status()
                items = resp.json()
                if isinstance(items, dict):
                    items = items.get("data", [])
        except Exception:
            return

        import json as _json
        for item in items:
            try:
                condition_id  = item.get("conditionId", "")
                winner_outcome = item.get("winnerOutcome") or item.get("winner_outcome", "")
                if not condition_id or not winner_outcome:
                    continue
                if condition_id in self._seen_resolutions:
                    continue

                prices_raw = item.get("outcomePrices", [])
                outcomes   = item.get("outcomes", [])
                token_ids  = item.get("clobTokenIds", [])
                if isinstance(prices_raw, str):
                    prices_raw = _json.loads(prices_raw)
                if isinstance(outcomes, str):
                    outcomes = _json.loads(outcomes)
                if isinstance(token_ids, str):
                    token_ids = _json.loads(token_ids)

                for i, outcome in enumerate(outcomes):
                    if outcome != winner_outcome:
                        continue
                    token_id = token_ids[i] if i < len(token_ids) else ""
                    price    = float(prices_raw[i]) if i < len(prices_raw) else 0.0
                    if not token_id or price >= 0.99:
                        self._seen_resolutions.add(condition_id)
                        continue

                    remaining = 1.0 - price
                    fee       = config.TAKER_FEE_RATE * (1 - price)
                    net_edge  = remaining - fee

                    if net_edge < config.MIN_EDGE_AFTER_FEES:
                        self._seen_resolutions.add(condition_id)
                        continue

                    signal = Signal(
                        signal_id=str(uuid.uuid4()),
                        strategy="oracle_convergence",
                        condition_id=condition_id,
                        token_id=token_id,
                        direction="BUY",
                        model_prob=1.0,
                        market_prob=price,
                        edge=remaining,
                        net_edge=net_edge,
                        confidence=0.90,
                        urgency="IMMEDIATE",
                        created_at=time.time(),
                        expires_at=time.time() + config.ORACLE_CONVERGENCE_WINDOW_SEC,
                        stale_price=price,
                        stale_threshold=remaining * 0.5,
                    )
                    log.info(
                        f"[ORACLE] Closed market resolution: {item.get('question','')[:60]} "
                        f"winner={winner_outcome} price={price:.3f} net_edge={net_edge:.2%}"
                    )
                    await self._bus.put(signal)
                    self._seen_resolutions.add(condition_id)
            except Exception:
                continue

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
