"""
Correlated Market Arbitrage — Highest Value Strategy.

Prediction markets contain logical constraints between related events.
When these constraints are violated, it's a genuine arbitrage (or near-arb).

Key insight: we model this as a DIRECTED graph with lead/lag relationships.
When the lead market moves, we have a time window T before the lag market
reprices. T can be measured empirically and is typically 1-10 minutes.

Relationship types:
  1. SUBSET: "Trump wins FL" ⊆ "Republican wins FL" → P(Trump) ≤ P(Rep)
  2. EXHAUSTIVE: "A wins" + "B wins" + "Other" should sum near 1.0
  3. IMPLICATION: "X wins primary" → "X is candidate in general" → price correlation
  4. CASCADE: Market A resolves → Market B must update price

This is the strategy that the Arxiv paper found extracted $40M from Polymarket.
Even capturing 0.001% of that is significant.
"""
from __future__ import annotations
import asyncio
import time
import uuid
from dataclasses import dataclass, field
from typing import Literal, Optional
import config
from core.models import Market, Signal
from core.logger import log


RelType = Literal["subset", "superset", "exhaustive", "implication", "cascade"]


@dataclass
class MarketRelation:
    """A directed relationship between two markets."""
    from_condition_id: str      # lead market
    to_condition_id: str        # lag market
    relation_type: RelType
    # For SUBSET: P(from) ≤ P(to) must hold
    # For EXHAUSTIVE: P(from) + P(to) should sum to target
    exhaustive_target: float = 1.0
    # Empirically measured propagation delay in minutes
    # When lead market moves, we have this long before lag reprices
    propagation_delay_min: float = 5.0
    # Confidence this relationship is valid
    confidence: float = 0.8
    description: str = ""


# ── Static relationship registry ─────────────────────────────────────────────
# In production: load from a JSON file that gets updated as new markets open.
# This is the "alpha moat" — building this graph is the hard intellectual work.
#
# Seed with known structural relationships.
# Automated detection (TF-IDF similarity) adds more over time.

_RELATION_REGISTRY: list[MarketRelation] = [
    # Example election cascade relationships
    # These get populated from the JSON registry in production
    # MarketRelation(
    #   from_condition_id="0x...",
    #   to_condition_id="0x...",
    #   relation_type="subset",
    #   description="Trump wins FL ⊆ Republican wins FL",
    # )
]


def _load_relation_registry(path: str = "market_relations.json") -> list[MarketRelation]:
    """Load relationship registry from JSON file."""
    import json
    import os
    if not os.path.exists(path):
        return []
    try:
        with open(path) as f:
            data = json.load(f)
        return [MarketRelation(**r) for r in data]
    except Exception as e:
        log.warning(f"Failed to load relation registry: {e}")
        return []


# ── Auto-detection via text similarity ───────────────────────────────────────

def _extract_keywords(question: str) -> set[str]:
    """Extract meaningful keywords from a market question."""
    # Remove common stop words
    stop = {"will", "the", "a", "an", "in", "of", "by", "to", "be", "or",
            "and", "for", "on", "at", "is", "are", "was", "with", "that",
            "have", "has", "had", "this", "from", "not", "but", "as", "do"}
    words = question.lower().replace("?", "").replace(",", "").split()
    return {w for w in words if len(w) > 2 and w not in stop}


def _keyword_similarity(q1: str, q2: str) -> float:
    """Jaccard similarity between keyword sets of two questions."""
    k1 = _extract_keywords(q1)
    k2 = _extract_keywords(q2)
    if not k1 or not k2:
        return 0.0
    intersection = k1 & k2
    union = k1 | k2
    return len(intersection) / len(union)


def auto_detect_relations(markets: list[Market]) -> list[MarketRelation]:
    """
    Automatically detect potential relationships between markets via text similarity.
    Returns candidate relations for human review / automated use.

    Pairs with similarity > 0.5 are candidates for SUBSET or CASCADE relationships.
    """
    relations = []
    n = len(markets)

    for i in range(n):
        for j in range(i + 1, n):
            m1, m2 = markets[i], markets[j]

            # Skip if same market
            if m1.condition_id == m2.condition_id:
                continue

            sim = _keyword_similarity(m1.question, m2.question)

            if sim < 0.40:
                continue

            # Heuristic: if one question's keywords are a strict subset of another's,
            # it's likely a subset relationship
            k1 = _extract_keywords(m1.question)
            k2 = _extract_keywords(m2.question)

            if k1.issubset(k2) and len(k1) < len(k2):
                relations.append(MarketRelation(
                    from_condition_id=m1.condition_id,
                    to_condition_id=m2.condition_id,
                    relation_type="subset",
                    confidence=sim,
                    description=f"Auto: '{m1.question[:40]}' ⊆ '{m2.question[:40]}'",
                ))
            elif k2.issubset(k1) and len(k2) < len(k1):
                relations.append(MarketRelation(
                    from_condition_id=m2.condition_id,
                    to_condition_id=m1.condition_id,
                    relation_type="subset",
                    confidence=sim,
                    description=f"Auto: '{m2.question[:40]}' ⊆ '{m1.question[:40]}'",
                ))
            elif sim > 0.55:
                # High similarity but not strict subset — flag as cascade candidate
                relations.append(MarketRelation(
                    from_condition_id=m1.condition_id,
                    to_condition_id=m2.condition_id,
                    relation_type="cascade",
                    confidence=sim * 0.7,  # lower confidence for auto-detected
                    description=f"Auto-sim={sim:.2f}: correlated questions",
                ))

    log.info(f"Auto-detected {len(relations)} market relationships")
    return relations


# ── Violation Detection ───────────────────────────────────────────────────────

def _find_subset_violation(
    lead_market: Market,
    lag_market: Market,
    relation: MarketRelation,
) -> Optional[tuple[str, str, float]]:
    """
    For SUBSET relation: P(from) ≤ P(to) must hold.
    If P(from) > P(to), the lag market is underpriced.

    Returns (action, token_id, edge) or None.
    """
    yes_lead = lead_market.yes_token
    yes_lag = lag_market.yes_token

    if not yes_lead or not yes_lag:
        return None

    p_lead = yes_lead.price
    p_lag = yes_lag.price

    # Subset violation: specific event more likely than general event
    if p_lead > p_lag + config.MIN_EDGE_AFTER_FEES:
        # Lag market (the superset) is underpriced → BUY
        return ("BUY", yes_lag.token_id, p_lead - p_lag)

    # Reverse violation: general event less likely than specific event (rare but possible)
    if p_lag < p_lead - config.MIN_EDGE_AFTER_FEES:
        # Lead market (the subset) is overpriced → SELL (or don't hold)
        # We can't short easily, so skip sell signals for now
        pass

    return None


def _find_exhaustive_violation(
    markets: list[Market],
    target: float = 1.0,
) -> list[tuple[str, str, float]]:
    """
    For EXHAUSTIVE group: sum of YES prices should ≈ target.
    If sum < target - threshold, one or more markets are underpriced.

    Returns list of (action, token_id, edge).
    """
    violations = []
    total_price = sum(
        m.yes_token.price for m in markets if m.yes_token
    )

    gap = target - total_price
    if gap < config.MIN_EDGE_AFTER_FEES * 2:
        return []   # no significant violation

    # The cheapest markets are most underpriced (or the most likely to be wrong)
    # Simple heuristic: buy the market with highest price (most likely winner)
    # and weight by how underpriced it is relative to its "fair share"
    n = len(markets)
    fair_share = target / n

    for m in markets:
        yes = m.yes_token
        if not yes:
            continue
        # Each market should be priced at least fair_share - gap_discount
        expected_price = fair_share
        if yes.price < expected_price - config.MIN_EDGE_AFTER_FEES:
            edge = expected_price - yes.price
            violations.append(("BUY", yes.token_id, edge))

    return violations


class CorrelatedArbScanner:
    """
    Main scanner for correlated market arbitrage.
    Runs every 30 seconds to check all known relationships.
    """

    def __init__(self, market_store, signal_bus: asyncio.Queue):
        self._store = market_store
        self._bus = signal_bus
        self._running = False
        self._relations: list[MarketRelation] = []
        self._auto_detect_cache: list[MarketRelation] = []
        self._last_auto_detect = 0
        # Track last price per token for cascade detection
        self._last_prices: dict[str, float] = {}

    async def start(self):
        self._running = True
        # Load static registry
        self._relations = _load_relation_registry()
        log.info(f"Loaded {len(self._relations)} static market relations")

        while self._running:
            await self._scan()
            await asyncio.sleep(30)

    async def _scan(self):
        now = time.time()
        markets_by_id = {m.condition_id: m for m in self._store.get_active_markets()}

        if not markets_by_id:
            return

        # Refresh auto-detected relations every hour
        if now - self._last_auto_detect > 3600:
            markets_list = list(markets_by_id.values())
            # Only auto-detect on a sample to avoid O(n²) cost
            sample = sorted(markets_list, key=lambda m: m.volume_24h, reverse=True)[:100]
            self._auto_detect_cache = auto_detect_relations(sample)
            self._last_auto_detect = now

        all_relations = self._relations + self._auto_detect_cache
        signals_generated = 0

        for relation in all_relations:
            lead = markets_by_id.get(relation.from_condition_id)
            lag = markets_by_id.get(relation.to_condition_id)

            if not lead or not lag:
                continue

            # ── Cascade detection: has lead moved recently? ──────────────────
            yes_lead = lead.yes_token
            if yes_lead:
                last_price = self._last_prices.get(yes_lead.token_id, yes_lead.price)
                price_move = abs(yes_lead.price - last_price)
                self._last_prices[yes_lead.token_id] = yes_lead.price

                if price_move > 0.03:   # >3% move in lead market
                    # Check if lag market has NOT yet repriced
                    signal = await self._cascade_signal(lead, lag, relation, yes_lead.price, last_price)
                    if signal:
                        await self._bus.put(signal)
                        signals_generated += 1
                        continue   # don't double-signal same pair

            # ── Static violation check ───────────────────────────────────────
            if relation.relation_type == "subset":
                result = _find_subset_violation(lead, lag, relation)
                if result:
                    action, token_id, edge = result
                    signal = self._make_signal(lag, token_id, action, edge, relation)
                    if signal:
                        await self._bus.put(signal)
                        signals_generated += 1

        if signals_generated:
            log.info(f"[CORR ARB] {signals_generated} signals this scan")

    async def _cascade_signal(
        self,
        lead: Market,
        lag: Market,
        relation: MarketRelation,
        new_lead_price: float,
        old_lead_price: float,
    ) -> Optional[Signal]:
        """
        Lead market moved → lag market should follow.
        We have propagation_delay_min minutes before lag reprices.
        """
        yes_lag = lag.yes_token
        if not yes_lag:
            return None

        direction_up = new_lead_price > old_lead_price
        move_size = abs(new_lead_price - old_lead_price)

        # Expected lag market movement: proportional to lead move, scaled by confidence
        expected_lag_move = move_size * relation.confidence * 0.7
        lag_current = yes_lag.price

        if direction_up:
            model_prob = min(0.99, lag_current + expected_lag_move)
            direction = "BUY"
        else:
            model_prob = max(0.01, lag_current - expected_lag_move)
            direction = "SELL"
            # We can only sell what we own — signal is informational for now

        gross_edge = abs(model_prob - lag_current)
        fee_pct = config.TAKER_FEE_RATE * (1 - lag_current)
        net_edge = gross_edge - fee_pct

        if net_edge < config.MIN_EDGE_AFTER_FEES:
            return None

        # Urgency scales with propagation delay — shorter window = more urgent
        if relation.propagation_delay_min < 2:
            urgency = "IMMEDIATE"
        elif relation.propagation_delay_min < 5:
            urgency = "HIGH"
        else:
            urgency = "MEDIUM"

        # TTL = propagation delay window
        ttl = relation.propagation_delay_min * 60

        log.info(
            f"[CASCADE] {lead.question[:35]} → {lag.question[:35]} | "
            f"lead moved {new_lead_price-old_lead_price:+.3f} "
            f"→ lag should move {expected_lag_move:+.3f}"
        )

        return Signal(
            signal_id=str(uuid.uuid4()),
            strategy="correlated_arb",
            condition_id=lag.condition_id,
            token_id=yes_lag.token_id,
            direction=direction,
            model_prob=model_prob,
            market_prob=lag_current,
            edge=gross_edge,
            net_edge=net_edge,
            confidence=relation.confidence * 0.8,
            urgency=urgency,
            created_at=time.time(),
            expires_at=time.time() + ttl,
            stale_price=lag_current,
            stale_threshold=net_edge * 0.7,
        )

    def _make_signal(
        self,
        market: Market,
        token_id: str,
        direction: str,
        edge: float,
        relation: MarketRelation,
    ) -> Optional[Signal]:
        """Build a Signal from a violation result."""
        # Find the token
        token = None
        for t in market.tokens:
            if t.token_id == token_id:
                token = t
                break
        if not token:
            return None

        price = token.price
        fee_pct = config.TAKER_FEE_RATE * (1 - price)
        net_edge = edge - fee_pct

        if net_edge < config.MIN_EDGE_AFTER_FEES:
            return None

        return Signal(
            signal_id=str(uuid.uuid4()),
            strategy="correlated_arb",
            condition_id=market.condition_id,
            token_id=token_id,
            direction=direction,
            model_prob=price + edge,
            market_prob=price,
            edge=edge,
            net_edge=net_edge,
            confidence=relation.confidence,
            urgency="MEDIUM",
            created_at=time.time(),
            expires_at=time.time() + 600,
            stale_price=price,
            stale_threshold=edge * 0.5,
        )

    def stop(self):
        self._running = False
