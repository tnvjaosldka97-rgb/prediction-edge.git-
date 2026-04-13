"""
Polymarket REST API client.
Covers: Gamma API, CLOB API, Data API.
"""
from __future__ import annotations
import asyncio
import time
from typing import Optional
import httpx
import config
from core.models import Market, Token, OrderBook
from core.logger import log


# Simple token bucket rate limiter
class _RateLimiter:
    def __init__(self, per_minute: int):
        self._interval = 60.0 / per_minute
        self._last = 0.0

    async def wait(self):
        now = time.time()
        wait = self._interval - (now - self._last)
        if wait > 0:
            await asyncio.sleep(wait)
        self._last = time.time()


_gamma_rl      = _RateLimiter(config.GAMMA_REQ_PER_MIN)
_clob_rl       = _RateLimiter(config.CLOB_ORDERS_PER_MIN)
_data_rl       = _RateLimiter(config.DATA_API_REQ_PER_MIN)
_orderbook_rl  = _RateLimiter(120)   # separate limiter — reads don't count against order quota


async def fetch_active_markets(limit: int = 500) -> list[Market]:
    """
    Fetch active markets from Gamma API (supports proper active filtering + volume sort)
    then enrich with winner/price from CLOB API for each market's tokens.

    Gamma API field names (different from CLOB):
      conditionId, endDateIso, volume24hr, outcomes, outcomePrices, clobTokenIds
    """
    await _gamma_rl.wait()
    url = f"{config.GAMMA_HOST}/markets"
    offset = 0
    markets = []

    async with httpx.AsyncClient(timeout=30) as client:
        while len(markets) < limit:
            params = {
                "active": "true",
                "closed": "false",
                "limit": min(100, limit - len(markets)),
                "offset": offset,
                "order": "volume24hr",
                "ascending": "false",
            }
            try:
                resp = await client.get(url, params=params)
                resp.raise_for_status()
                items = resp.json()
                if isinstance(items, dict):
                    items = items.get("data", [])
            except Exception as e:
                log.warning(f"Gamma markets fetch error: {e}")
                break

            if not items:
                break

            for item in items:
                try:
                    import json as _json
                    # Gamma uses clobTokenIds + outcomes + outcomePrices
                    token_ids   = item.get("clobTokenIds", [])
                    outcomes    = item.get("outcomes", [])
                    prices_raw  = item.get("outcomePrices", [])
                    # Gamma sometimes returns these as JSON-encoded strings
                    if isinstance(token_ids, str):
                        token_ids = _json.loads(token_ids)
                    if isinstance(outcomes, str):
                        outcomes = _json.loads(outcomes)
                    if isinstance(prices_raw, str):
                        prices_raw = _json.loads(prices_raw)

                    tokens = []
                    for i, tid in enumerate(token_ids):
                        if not tid:
                            continue
                        outcome = outcomes[i] if i < len(outcomes) else f"outcome_{i}"
                        try:
                            price = float(prices_raw[i]) if i < len(prices_raw) else 0.5
                        except (ValueError, TypeError):
                            price = 0.5
                        tokens.append(Token(
                            token_id=tid,
                            outcome=outcome,
                            price=price,
                            winner=None,   # enriched below if resolved
                        ))

                    if not tokens:
                        continue

                    # Check if resolved: Gamma sets winner_outcome when done
                    winner_outcome = item.get("winnerOutcome") or item.get("winner_outcome") or ""
                    if winner_outcome:
                        for t in tokens:
                            if t.outcome == winner_outcome:
                                t.winner = True
                            else:
                                t.winner = False

                    condition_id = item.get("conditionId") or item.get("condition_id", "")
                    end_date = item.get("endDateIso") or item.get("endDate") or item.get("end_date_iso", "")

                    m = Market(
                        condition_id=condition_id,
                        question=item.get("question", ""),
                        end_date_iso=end_date,
                        tokens=tokens,
                        volume_24h=float(item.get("volume24hr") or item.get("volume_24hr") or 0),
                        liquidity=float(item.get("liquidityNum") or item.get("liquidity") or 0),
                        category=item.get("category", ""),
                        active=bool(item.get("active", True)),
                        tags=[t.get("label", "") for t in item.get("tags", [])],
                    )
                    markets.append(m)
                except Exception as e:
                    log.debug(f"Skipping market: {e}")

            offset += len(items)
            if len(items) < 100:
                break   # last page

    log.info(f"Fetched {len(markets)} active markets")
    return markets


async def fetch_orderbook(token_id: str) -> Optional[OrderBook]:
    """Fetch L2 order book for a token from CLOB API."""
    await _orderbook_rl.wait()
    url = f"{config.CLOB_HOST}/book"
    params = {"token_id": token_id}
    async with httpx.AsyncClient(timeout=10) as client:
        try:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            log.warning(f"Orderbook fetch failed for {token_id[:8]}: {e}")
            return None

    bids = [(float(b["price"]), float(b["size"])) for b in data.get("bids", [])]
    asks = [(float(a["price"]), float(a["size"])) for a in data.get("asks", [])]
    # Sort: bids descending, asks ascending
    bids.sort(key=lambda x: -x[0])
    asks.sort(key=lambda x: x[0])

    return OrderBook(token_id=token_id, bids=bids, asks=asks)


async def fetch_midpoint(token_id: str) -> Optional[float]:
    """Fast mid price fetch."""
    await _clob_rl.wait()
    url = f"{config.CLOB_HOST}/midpoint"
    async with httpx.AsyncClient(timeout=5) as client:
        try:
            resp = await client.get(url, params={"token_id": token_id})
            resp.raise_for_status()
            return float(resp.json().get("mid", 0))
        except Exception:
            return None


async def fetch_recent_trades(token_id: str, limit: int = 100) -> list[dict]:
    """Fetch recent trade history for a token (unauthenticated)."""
    await _clob_rl.wait()
    url = f"{config.CLOB_HOST}/trades"
    params = {"token_id": token_id, "limit": limit}
    async with httpx.AsyncClient(timeout=10) as client:
        try:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            log.warning(f"Trades fetch failed: {e}")
            return []


async def fetch_global_trades(limit: int = 500) -> list[dict]:
    """Fetch global trade stream from Data API — used for whale detection."""
    await _data_rl.wait()
    url = f"{config.DATA_API_HOST}/trades"
    params = {"limit": limit, "takerOnly": "false"}
    async with httpx.AsyncClient(timeout=15) as client:
        try:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            log.warning(f"Global trades fetch failed: {e}")
            return []


async def fetch_top_holders(condition_id: str, limit: int = 50) -> list[dict]:
    """Fetch top position holders for a market — used for wallet profiling."""
    await _data_rl.wait()
    url = f"{config.DATA_API_HOST}/holders"
    params = {"market": condition_id, "limit": limit}
    async with httpx.AsyncClient(timeout=10) as client:
        try:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            log.warning(f"Holders fetch failed: {e}")
            return []


async def fetch_wallet_positions(wallet: str) -> list[dict]:
    """Fetch all open positions for a wallet."""
    await _data_rl.wait()
    url = f"{config.DATA_API_HOST}/positions"
    params = {"user": wallet}
    async with httpx.AsyncClient(timeout=10) as client:
        try:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            log.warning(f"Wallet positions fetch failed for {wallet[:8]}: {e}")
            return []


async def fetch_wallet_trade_history(wallet: str, limit: int = 500) -> list[dict]:
    """Fetch complete trade history for a wallet — used for profitability scoring."""
    await _data_rl.wait()
    url = f"{config.DATA_API_HOST}/trades"
    params = {"maker": wallet, "limit": limit}
    async with httpx.AsyncClient(timeout=15) as client:
        try:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            log.warning(f"Wallet history fetch failed: {e}")
            return []
