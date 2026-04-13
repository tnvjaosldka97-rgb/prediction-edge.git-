"""
Cross-Platform Arbitrage — Kalshi vs Polymarket.

CORE INSIGHT:
  Kalshi and Polymarket trade the SAME events (elections, sports, macro)
  with different prices due to different user bases and liquidity.
  When the same binary event trades at 60¢ on Kalshi and 55¢ on Polymarket,
  buying on Polymarket is pure edge — the prices must converge by resolution.

WHY THIS IS REAL ALPHA:
  1. Platforms don't cross-hedge each other (isolated liquidity pools)
  2. Most retail bots are platform-specific
  3. Price discovery happens at different speeds on each platform
  4. Edge persists for hours to days (not milliseconds)

MATCHING ALGORITHM:
  Fuzzy keyword matching on event titles with minimum Jaccard similarity.
  Conservative threshold (0.45) to avoid false positives.
"""
from __future__ import annotations
import asyncio
import base64
import datetime
import re
import time
import uuid
from typing import Optional
import httpx
import config
from core.models import Market, Signal
from core.logger import log


_SCAN_INTERVAL_SEC  = 120
_MIN_SIMILARITY     = 0.40
_MIN_EDGE_THRESHOLD = 0.04
_KALSHI_BASE        = "https://trading-api.kalshi.com/trade-api/v2"


def _kalshi_headers(method: str, path: str) -> Optional[dict]:
    """
    Build Kalshi RSA-PSS signed request headers.
    Requires KALSHI_ACCESS_KEY and KALSHI_PRIVATE_KEY_B64 in env.
    KALSHI_PRIVATE_KEY_B64 = base64.b64encode(open('kalshi.key','rb').read()).decode()
    """
    access_key = getattr(config, "KALSHI_ACCESS_KEY", "")
    key_b64    = getattr(config, "KALSHI_PRIVATE_KEY_B64", "")
    if not access_key or not key_b64:
        return None

    try:
        from cryptography.hazmat.primitives import serialization, hashes
        from cryptography.hazmat.primitives.asymmetric import padding
        from cryptography.hazmat.backends import default_backend

        key_bytes   = base64.b64decode(key_b64)
        private_key = serialization.load_pem_private_key(
            key_bytes, password=None, backend=default_backend()
        )
        ts_ms    = str(int(datetime.datetime.now().timestamp() * 1000))
        path_clean = path.split("?")[0]
        msg      = (ts_ms + method.upper() + path_clean).encode("utf-8")
        sig      = private_key.sign(
            msg,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )
        return {
            "KALSHI-ACCESS-KEY":       access_key,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode("utf-8"),
            "KALSHI-ACCESS-TIMESTAMP": ts_ms,
        }
    except Exception as e:
        log.debug(f"[CROSS ARB] Kalshi header build failed: {e}")
        return None


def _tokenize(text: str) -> set[str]:
    """Normalize and tokenize a market title for similarity comparison."""
    text = text.lower()
    # Remove common filler words
    stopwords = {"will", "the", "a", "an", "in", "on", "by", "to", "of", "be",
                 "is", "are", "does", "do", "for", "at", "or", "and", "win",
                 "2024", "2025", "2026", "2027", "2028", "presidential", "us", "?"}
    words = set(re.findall(r"[a-z]+", text))
    return words - stopwords


def _jaccard(a: set[str], b: set[str]) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


async def _fetch_kalshi_markets() -> list[dict]:
    """
    Fetch active Kalshi markets with RSA-PSS auth.
    Returns list of {ticker, title, yes_bid, yes_ask, yes_mid, volume} dicts.
    """
    path    = "/trade-api/v2/markets"
    headers = _kalshi_headers("GET", path)
    if headers is None:
        return []   # no credentials configured

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                f"{_KALSHI_BASE}/markets",
                params={"status": "open", "limit": 200},
                headers=headers,
            )
            if resp.status_code in (401, 403):
                log.warning("[CROSS ARB] Kalshi auth failed — check KALSHI_ACCESS_KEY / KALSHI_PRIVATE_KEY_B64")
                return []
            resp.raise_for_status()
            data    = resp.json()
            markets = data.get("markets", []) if isinstance(data, dict) else data
            if not isinstance(markets, list):
                return []

            result = []
            for m in markets:
                yes_bid = float(m.get("yes_bid", 0) or 0) / 100   # Kalshi prices in cents
                yes_ask = float(m.get("yes_ask", 0) or 0) / 100
                if yes_bid <= 0 or yes_ask <= 0:
                    continue
                result.append({
                    "ticker":  m.get("ticker", ""),
                    "title":   m.get("title", "") or m.get("question", ""),
                    "yes_bid": yes_bid,
                    "yes_ask": yes_ask,
                    "yes_mid": (yes_bid + yes_ask) / 2,
                    "volume":  float(m.get("volume", 0) or 0),
                })
            return result

    except Exception as e:
        log.debug(f"[CROSS ARB] Kalshi fetch error: {e}")
        return []


def _match_markets(
    poly_market: Market,
    kalshi_markets: list[dict],
) -> Optional[dict]:
    """Find the best-matching Kalshi market for a Polymarket market."""
    poly_words = _tokenize(poly_market.question)
    if len(poly_words) < 3:
        return None

    best_score = 0.0
    best_match = None

    for k in kalshi_markets:
        k_words = _tokenize(k["title"])
        score = _jaccard(poly_words, k_words)
        if score > best_score:
            best_score = score
            best_match = k

    if best_score >= _MIN_SIMILARITY:
        return best_match
    return None


def _compute_arb_signal(
    poly_market: Market,
    poly_price: float,      # Polymarket YES ask (what we'd pay)
    kalshi_mid: float,      # Kalshi YES mid (our reference probability)
) -> Optional[Signal]:
    """
    Emit BUY signal on Polymarket when Kalshi price > Polymarket price.

    Edge = kalshi_mid - poly_ask - taker_fee
    """
    yes = poly_market.yes_token
    if not yes:
        return None

    fee_pct = config.TAKER_FEE_RATE * (1 - poly_price)
    gross_edge = kalshi_mid - poly_price
    net_edge = gross_edge - fee_pct

    if net_edge < _MIN_EDGE_THRESHOLD:
        return None

    if poly_market.dispute_risk > config.ORACLE_DISPUTE_THRESHOLD_SKIP:
        return None

    confidence = min(0.95, net_edge * 8)   # scale: 5% edge → 40% confidence, 12% → 95%

    urgency: str
    if net_edge > 0.15:
        urgency = "HIGH"
    elif net_edge > 0.08:
        urgency = "MEDIUM"
    else:
        urgency = "LOW"

    return Signal(
        signal_id=str(uuid.uuid4()),
        strategy="cross_platform",
        condition_id=poly_market.condition_id,
        token_id=yes.token_id,
        direction="BUY",
        model_prob=kalshi_mid,
        market_prob=poly_price,
        edge=gross_edge,
        net_edge=net_edge,
        confidence=confidence,
        urgency=urgency,
        created_at=time.time(),
        expires_at=time.time() + 3600,   # 1 hour TTL (prices drift slowly)
        stale_price=poly_price,
        stale_threshold=0.03,            # cancel if poly price moves 3 cents
    )


class CrossPlatformArbScanner:
    """
    Continuously scans for Kalshi vs Polymarket price divergences.
    Emits BUY signals when Polymarket is mispriced relative to Kalshi.
    """

    def __init__(self, store, signal_bus: asyncio.Queue):
        self._store    = store
        self._bus      = signal_bus
        self._running  = False
        self._enabled  = True
        self._emitted: dict[str, float] = {}   # condition_id → last_emit_time

    async def start(self):
        self._running = True
        # Try initial Kalshi fetch to confirm connectivity
        test = await _fetch_kalshi_markets()
        if not test:
            log.warning("[CROSS ARB] Kalshi unavailable — cross-platform arb disabled. "
                        "Add KALSHI_API_KEY to .env to enable.")
            self._enabled = False
        else:
            log.info(f"[CROSS ARB] Kalshi connected — {len(test)} markets available")

        while self._running:
            if self._enabled:
                try:
                    await self._scan()
                except Exception as e:
                    log.error(f"[CROSS ARB] Scan error: {e}")
            await asyncio.sleep(_SCAN_INTERVAL_SEC)

    async def _scan(self):
        kalshi_markets = await _fetch_kalshi_markets()
        if not kalshi_markets:
            return

        poly_markets = self._store.get_active_markets()
        signals_generated = 0
        now = time.time()

        for poly in poly_markets:
            if not poly.active:
                continue

            # Throttle: don't re-emit same market arb within 30 minutes
            if now - self._emitted.get(poly.condition_id, 0) < 1800:
                continue

            yes = poly.yes_token
            if not yes or yes.price <= 0:
                continue

            # Get real ask price from orderbook if available
            book = self._store.get_orderbook(yes.token_id)
            poly_ask = book.best_ask if book and book.best_ask > 0 else yes.price + 0.01

            match = _match_markets(poly, kalshi_markets)
            if not match:
                continue

            signal = _compute_arb_signal(poly, poly_ask, match["yes_mid"])
            if signal:
                await self._bus.put(signal)
                self._emitted[poly.condition_id] = now
                signals_generated += 1
                log.info(
                    f"[CROSS ARB] {poly.question[:50]}\n"
                    f"  Kalshi={match['yes_mid']:.3f}  Poly={poly_ask:.3f}  "
                    f"edge={signal.net_edge:.2%}  match={match['ticker']}"
                )

        if signals_generated:
            log.info(f"[CROSS ARB] {signals_generated} arb signals this scan")

    def stop(self):
        self._running = False
