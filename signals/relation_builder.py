"""
Automated Market Relation Graph Builder — Zero Manual Work.

Three data sources, all automatic:

1. GAMMA EVENT GROUPING (free, no API key)
   Polymarket's Gamma API returns markets grouped by "event".
   Markets in the same event are almost always an EXHAUSTIVE group.
   e.g., "2024 US Election" event contains:
     - "Will Trump win?" (p=0.55)
     - "Will Harris win?" (p=0.44)
     - "Will Other win?" (p=0.01)
   These three should sum to ~1.0. Any deviation is an arb.

2. CLAUDE API (semantic relationship detection)
   Send batches of market questions to Claude.
   Ask: "Which pairs have logical constraints? (subset, implication, exclusion)"
   Claude can detect relationships that keyword similarity misses.
   e.g., "Will Trump win FL?" ⊂ "Will Republicans win FL?"
         "Will Fed raise rates?" → "Will inflation stay above 3%?" (implication)

3. PRICE CORRELATION (from SQLite price_history)
   Markets whose prices historically move together are correlated.
   High positive correlation → likely same underlying event
   High negative correlation → likely mutually exclusive outcomes

Auto-refreshes every 6 hours. Persists to market_relations.json.
"""
from __future__ import annotations
import asyncio
import json
import math
import os
import time
from typing import Optional
import httpx
import config
from core.models import Market
from core.logger import log
from signals.correlated_arb import MarketRelation


RELATIONS_FILE = "market_relations.json"
_REFRESH_INTERVAL = 6 * 3600   # 6 hours


# ── 1. Gamma Event Grouping ──────────────────────────────────────────────────

async def fetch_events_with_markets() -> list[dict]:
    """
    Fetch all events from Gamma API.
    Each event contains multiple related markets — these form our EXHAUSTIVE groups.
    """
    url = f"{config.GAMMA_HOST}/events"
    params = {"active": "true", "limit": 200, "order": "volume_24hr", "ascending": "false"}
    async with httpx.AsyncClient(timeout=30) as client:
        try:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()
            return data if isinstance(data, list) else data.get("data", [])
        except Exception as e:
            log.warning(f"Events fetch failed: {e}")
            return []


def _extract_exhaustive_relations(events: list[dict]) -> list[MarketRelation]:
    """
    Markets in the same event form an exhaustive group.
    Their YES prices should sum to ~1.0.
    """
    relations = []
    for event in events:
        markets = event.get("markets", [])
        condition_ids = [
            m.get("conditionId", m.get("condition_id", ""))
            for m in markets
            if m.get("active", True)
        ]
        if len(condition_ids) < 2:
            continue

        event_title = event.get("title", "")

        # Create exhaustive group: all pairs in the event
        for i, cid_a in enumerate(condition_ids):
            for cid_b in condition_ids[i+1:]:
                relations.append(MarketRelation(
                    from_condition_id=cid_a,
                    to_condition_id=cid_b,
                    relation_type="exhaustive",
                    exhaustive_target=1.0,
                    propagation_delay_min=2.0,
                    confidence=0.95,   # high confidence: same event = almost certainly related
                    description=f"Gamma event group: {event_title[:50]}",
                ))

    log.info(f"Extracted {len(relations)} exhaustive relations from {len(events)} events")
    return relations


# ── 2. Claude API Semantic Relationship Detection ────────────────────────────

async def detect_relations_with_claude(
    markets: list[Market],
    batch_size: int = 20,
) -> list[MarketRelation]:
    """
    Use Claude API to detect logical relationships between markets.
    Processes markets in batches to stay within token limits.

    Returns list of MarketRelation objects.
    """
    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    if not api_key:
        log.warning("ANTHROPIC_API_KEY not set — skipping Claude-based relation detection")
        return []

    # Sort by volume and take top markets (most liquid = most worth detecting)
    top_markets = sorted(markets, key=lambda m: m.volume_24h, reverse=True)[:200]

    all_relations = []
    # Process in batches
    for i in range(0, len(top_markets), batch_size):
        batch = top_markets[i:i+batch_size]
        relations = await _claude_batch_analysis(batch, api_key)
        all_relations.extend(relations)
        await asyncio.sleep(1)  # rate limit

    log.info(f"Claude detected {len(all_relations)} semantic relations")
    return all_relations


async def _claude_batch_analysis(
    markets: list[Market],
    api_key: str,
) -> list[MarketRelation]:
    """
    Send a batch of markets to Claude and extract logical relationships.
    """
    # Build the market list for the prompt
    market_list = "\n".join(
        f"{i+1}. [{m.condition_id[:12]}] {m.question} (price={f'{m.yes_token.price:.3f}' if m.yes_token else 'N/A'})"
        for i, m in enumerate(markets)
    )

    prompt = f"""You are analyzing prediction market questions to find logical relationships.

Here are {len(markets)} active prediction markets:

{market_list}

Find ALL logical relationships between these markets. For each relationship, output JSON:
{{
  "from_id": "condition_id_prefix",
  "to_id": "condition_id_prefix",
  "type": "subset|superset|exhaustive|implication|cascade",
  "confidence": 0.0-1.0,
  "description": "brief explanation",
  "propagation_delay_min": estimated_minutes_for_lag_to_update
}}

Relationship types:
- subset: P(from) <= P(to) must hold (specific ⊆ general)
- exhaustive: from + to should sum to ~1.0 (mutually exclusive outcomes)
- implication: if from resolves YES, to is likely to also move
- cascade: from resolving will cause to to reprice within minutes

Rules:
- Only include HIGH confidence relationships (>0.7)
- Focus on ACTIONABLE relationships (price discrepancies that could be traded)
- For cascade relationships, estimate realistic propagation delay

Output ONLY a JSON array of relationships, no other text. If no relationships found, output [].
"""

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": api_key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": "claude-haiku-4-5-20251001",  # fast + cheap for bulk analysis
                    "max_tokens": 2000,
                    "messages": [{"role": "user", "content": prompt}],
                }
            )
            resp.raise_for_status()
            data = resp.json()
            text = data["content"][0]["text"].strip()

            # Parse JSON response
            if text.startswith("["):
                raw_relations = json.loads(text)
            else:
                # Sometimes Claude adds markdown code blocks
                import re
                match = re.search(r'\[.*\]', text, re.DOTALL)
                if match:
                    raw_relations = json.loads(match.group())
                else:
                    return []

    except json.JSONDecodeError as e:
        log.warning(f"Claude response parse error: {e}")
        return []
    except Exception as e:
        log.warning(f"Claude API call failed: {e}")
        return []

    # Build a lookup: condition_id_prefix → full condition_id
    id_lookup = {m.condition_id[:12]: m.condition_id for m in markets}

    relations = []
    for r in raw_relations:
        from_prefix = r.get("from_id", "")
        to_prefix = r.get("to_id", "")

        from_cid = id_lookup.get(from_prefix)
        to_cid = id_lookup.get(to_prefix)

        if not from_cid or not to_cid:
            continue

        rel_type = r.get("type", "cascade")
        if rel_type not in ("subset", "superset", "exhaustive", "implication", "cascade"):
            rel_type = "cascade"

        relations.append(MarketRelation(
            from_condition_id=from_cid,
            to_condition_id=to_cid,
            relation_type=rel_type,
            confidence=float(r.get("confidence", 0.7)),
            propagation_delay_min=float(r.get("propagation_delay_min", 5.0)),
            description=f"Claude: {r.get('description', '')}",
        ))

    return relations


# ── 3. Price Correlation Analysis ────────────────────────────────────────────

def detect_price_correlations(
    markets: list[Market],
    min_correlation: float = 0.75,
    min_data_points: int = 50,
) -> list[MarketRelation]:
    """
    Find market pairs with high price correlation from historical data.
    High correlation → likely related underlying event → cascade relationship.
    """
    from core.db import get_conn
    conn = get_conn()

    # Fetch recent price history for all tokens
    token_prices: dict[str, list[tuple[float, float]]] = {}

    for market in markets:
        for token in market.tokens:
            rows = conn.execute(
                """SELECT timestamp, price FROM price_history
                   WHERE token_id = ? ORDER BY timestamp DESC LIMIT 200""",
                (token.token_id,)
            ).fetchall()
            if len(rows) >= min_data_points:
                token_prices[token.token_id] = [(r["timestamp"], r["price"]) for r in rows]

    if not token_prices:
        return []

    # Build market → YES token mapping
    market_tokens = {}
    for m in markets:
        yes = m.yes_token
        if yes and yes.token_id in token_prices:
            market_tokens[m.condition_id] = yes.token_id

    relations = []
    market_ids = list(market_tokens.keys())

    for i, cid_a in enumerate(market_ids):
        for cid_b in market_ids[i+1:]:
            token_a = market_tokens[cid_a]
            token_b = market_tokens[cid_b]

            corr = _compute_correlation(
                token_prices[token_a],
                token_prices[token_b],
            )
            if corr is None:
                continue

            if abs(corr) >= min_correlation:
                rel_type = "cascade" if corr > 0 else "exhaustive"
                relations.append(MarketRelation(
                    from_condition_id=cid_a,
                    to_condition_id=cid_b,
                    relation_type=rel_type,
                    confidence=abs(corr) * 0.8,  # discount for autocorrelation artifacts
                    propagation_delay_min=3.0,
                    description=f"Price correlation: r={corr:.3f}",
                ))

    log.info(f"Price correlation analysis: {len(relations)} correlated pairs found")
    return relations


def _compute_correlation(
    series_a: list[tuple[float, float]],
    series_b: list[tuple[float, float]],
) -> Optional[float]:
    """
    Compute Pearson correlation between two price series.
    Aligns by timestamp (nearest neighbor within 5-minute buckets).
    """
    # Convert to {bucket → price} where bucket = timestamp // 300
    def bucketize(series):
        d = {}
        for ts, price in series:
            bucket = int(ts // 300)
            d[bucket] = price
        return d

    d_a = bucketize(series_a)
    d_b = bucketize(series_b)

    # Find common buckets
    common = set(d_a.keys()) & set(d_b.keys())
    if len(common) < 20:
        return None

    prices_a = [d_a[b] for b in sorted(common)]
    prices_b = [d_b[b] for b in sorted(common)]

    n = len(prices_a)
    mean_a = sum(prices_a) / n
    mean_b = sum(prices_b) / n

    num = sum((prices_a[i] - mean_a) * (prices_b[i] - mean_b) for i in range(n))
    den_a = math.sqrt(sum((p - mean_a) ** 2 for p in prices_a))
    den_b = math.sqrt(sum((p - mean_b) ** 2 for p in prices_b))

    if den_a == 0 or den_b == 0:
        return None

    return num / (den_a * den_b)


# ── Main Builder ──────────────────────────────────────────────────────────────

async def build_relation_graph(market_store) -> list[MarketRelation]:
    """
    Build the complete market relation graph from all three sources.
    Deduplicates and merges overlapping relations.
    Persists to market_relations.json.
    """
    log.info("Building market relation graph...")
    start = time.time()

    markets = market_store.get_active_markets()

    # Source 1: Gamma event grouping (free, instant)
    events = await fetch_events_with_markets()
    exhaustive_relations = _extract_exhaustive_relations(events)

    # Source 2: Claude API (paid but cheap with haiku)
    claude_relations = await detect_relations_with_claude(markets)

    # Source 3: Price correlation (free, from DB)
    top_markets = sorted(markets, key=lambda m: m.volume_24h, reverse=True)[:100]
    price_relations = detect_price_correlations(top_markets)

    # Merge all relations
    all_relations = exhaustive_relations + claude_relations + price_relations

    # Deduplicate: same pair + same type → keep highest confidence
    deduped: dict[tuple, MarketRelation] = {}
    for rel in all_relations:
        key = (rel.from_condition_id, rel.to_condition_id, rel.relation_type)
        if key not in deduped or rel.confidence > deduped[key].confidence:
            deduped[key] = rel

    final = list(deduped.values())

    # Persist to JSON
    _save_relations(final)

    elapsed = time.time() - start
    log.info(
        f"Relation graph built: {len(final)} relations "
        f"({len(exhaustive_relations)} event, {len(claude_relations)} claude, "
        f"{len(price_relations)} price) in {elapsed:.1f}s"
    )
    return final


def _save_relations(relations: list[MarketRelation]):
    """Persist relation graph to JSON file."""
    data = [
        {
            "from_condition_id": r.from_condition_id,
            "to_condition_id": r.to_condition_id,
            "relation_type": r.relation_type,
            "exhaustive_target": r.exhaustive_target,
            "propagation_delay_min": r.propagation_delay_min,
            "confidence": r.confidence,
            "description": r.description,
        }
        for r in relations
    ]
    with open(RELATIONS_FILE, "w") as f:
        json.dump(data, f, indent=2)
    log.info(f"Saved {len(data)} relations to {RELATIONS_FILE}")


class RelationGraphManager:
    """
    Background task that auto-refreshes the relation graph every 6 hours.
    No manual work required.
    """
    def __init__(self, market_store, correlated_arb_scanner):
        self._store = market_store
        self._scanner = correlated_arb_scanner
        self._running = False

    async def start(self):
        self._running = True
        while self._running:
            try:
                relations = await build_relation_graph(self._store)
                # Inject into the running scanner
                self._scanner._relations = relations
                log.info(f"Relation graph updated: {len(relations)} active relations")
            except Exception as e:
                log.error(f"Relation graph build failed: {e}")
            await asyncio.sleep(_REFRESH_INTERVAL)

    def stop(self):
        self._running = False
