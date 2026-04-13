"""
CLOB REST Orderbook Poller — real orderbook data without WebSocket.

Polls top-volume tokens every 10 seconds via CLOB REST API.
Priority: open positions > top-volume markets.

Why this matters:
  Without real orderbooks, all signal generators using bid/ask data are blind.
  This gives sub-15-second orderbook freshness even when WebSocket is unavailable.
"""
from __future__ import annotations
import asyncio
import time
from core.logger import log
from data.polymarket_rest import fetch_orderbook

_POLL_INTERVAL_SEC = 10.0
_MAX_TOKENS        = 30     # top N tokens to maintain real orderbooks for
_LOG_EVERY_SEC     = 120    # log summary every 2 minutes


class ClobOrderbookPoller:
    """
    Continuously polls CLOB REST for real orderbooks on the most important tokens.
    Designed to run permanently as an asyncio task.
    """

    def __init__(self, store, portfolio):
        self._store     = store
        self._portfolio = portfolio
        self._running   = False
        self._last_log  = 0.0
        self._ws_active = False   # set True externally if WebSocket connects

    def set_ws_active(self, active: bool):
        """WebSocket manager calls this to signal real-time data is available."""
        self._ws_active = active

    def _priority_tokens(self) -> list[str]:
        seen:   set[str]  = set()
        tokens: list[str] = []

        # Priority 1: tokens with open positions — always keep fresh
        for token_id in self._portfolio.positions:
            if token_id not in seen:
                tokens.append(token_id)
                seen.add(token_id)

        # Priority 2: YES tokens from top-volume markets
        markets = sorted(
            self._store.get_active_markets(),
            key=lambda m: m.volume_24h,
            reverse=True,
        )
        for m in markets:
            if len(tokens) >= _MAX_TOKENS:
                break
            for t in m.tokens:
                if t.token_id not in seen and t.price > 0:
                    tokens.append(t.token_id)
                    seen.add(t.token_id)

        return tokens[:_MAX_TOKENS]

    async def start(self):
        self._running = True
        log.info(f"[OB POLLER] Started — polling top {_MAX_TOKENS} tokens every {_POLL_INTERVAL_SEC}s")

        while self._running:
            cycle_start = asyncio.get_event_loop().time()

            # If WebSocket is active, only refresh open positions (not all tokens)
            tokens = self._priority_tokens()
            if self._ws_active:
                # Only keep position tokens fresh — WS handles everything else
                tokens = [t for t in tokens if t in self._portfolio.positions]

            updated = 0
            for token_id in tokens:
                if not self._running:
                    break
                try:
                    book = await fetch_orderbook(token_id)
                    if book and (book.bids or book.asks):
                        await self._store.update_orderbook(book)
                        updated += 1
                except Exception as e:
                    log.debug(f"[OB POLLER] fetch failed {token_id[:8]}: {e}")
                # Stagger to avoid burst — 120 req/min limit = 0.5s gap
                await asyncio.sleep(0.5)

            now = time.time()
            if now - self._last_log >= _LOG_EVERY_SEC and updated:
                log.info(f"[OB POLLER] Refreshed {updated}/{len(tokens)} real orderbooks")
                self._last_log = now

            elapsed = asyncio.get_event_loop().time() - cycle_start
            await asyncio.sleep(max(0, _POLL_INTERVAL_SEC - elapsed))

    def stop(self):
        self._running = False
