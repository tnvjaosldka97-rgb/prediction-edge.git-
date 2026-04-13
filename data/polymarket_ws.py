"""
Polymarket WebSocket Client — Real-time orderbook and trade stream.

WITHOUT this module, the system is blind to intra-minute price moves.
REST polling at 60s means you're always 30-60 seconds behind.
WebSocket gives you sub-second latency on every price change.

Channels:
  Market channel: book, price_change, last_trade_price, market_resolved
  User channel:  order fills, order status (requires auth)

Architecture:
  - One connection per ≤200 markets (undocumented limit)
  - Sequence tracking per token to detect dropped messages
  - Exponential backoff reconnect
  - On gap: re-fetch full snapshot from REST
"""
from __future__ import annotations
import asyncio
import json
import time
from typing import Optional
import aiohttp
import config
from core.models import OrderBook, Market
from core.logger import log


class MarketWebSocket:
    """
    Manages a single WebSocket connection for up to 200 markets.
    Publishes OrderBook objects to the shared store and event bus.
    """

    HEARTBEAT_INTERVAL = 10   # seconds between pings
    RECONNECT_BASE     = 1    # initial reconnect delay in seconds
    RECONNECT_MAX      = 60   # max reconnect delay
    SILENCE_TIMEOUT    = 45   # force reconnect if no message for this many seconds

    def __init__(
        self,
        token_ids: list[str],
        market_store,
        orderbook_bus: asyncio.Queue,
    ):
        self._token_ids = token_ids[:200]   # hard cap
        self._store = market_store
        self._bus = orderbook_bus
        self._running = False
        self._ws = None
        self._last_msg_time = time.time()
        self._msg_count = 0
        # Per-token sequence tracking for gap detection
        self._sequences: dict[str, int] = {}

    async def start(self):
        self._running = True
        reconnect_delay = self.RECONNECT_BASE
        while self._running:
            try:
                await self._connect_and_run()
                reconnect_delay = self.RECONNECT_BASE  # reset on clean disconnect
            except Exception as e:
                log.warning(f"WebSocket disconnected: {e}. Reconnecting in {reconnect_delay}s")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, self.RECONNECT_MAX)

    async def _connect_and_run(self):
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(
                config.WS_MARKET,
                heartbeat=self.HEARTBEAT_INTERVAL,
                receive_timeout=self.SILENCE_TIMEOUT,
            ) as ws:
                self._ws = ws
                self._last_msg_time = time.time()
                log.info(f"WebSocket connected. Subscribing to {len(self._token_ids)} tokens")
                await self._subscribe()
                # Run listener + silence watchdog concurrently
                await asyncio.gather(
                    self._listen(ws),
                    self._watchdog(ws),
                )

    async def _watchdog(self, ws):
        """Force reconnect if no message received for SILENCE_TIMEOUT seconds."""
        while not ws.closed:
            await asyncio.sleep(10)
            silent = time.time() - self._last_msg_time
            if silent > self.SILENCE_TIMEOUT:
                log.warning(f"WS silent {silent:.0f}s — forcing reconnect")
                await ws.close()
                return

    async def _subscribe(self):
        """Subscribe to market events for all token IDs."""
        if not self._ws:
            return
        msg = {
            "auth": {},
            "markets": [],
            "assets_ids": self._token_ids,
            "type": "Market",
        }
        await self._ws.send_json(msg)

    async def _listen(self, ws):
        async for msg in ws:
            self._last_msg_time = time.time()
            self._msg_count += 1
            if msg.type == aiohttp.WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                    await self._handle_message(data)
                except json.JSONDecodeError:
                    pass
            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                break

    async def _handle_message(self, data):
        if not isinstance(data, list):
            data = [data]

        for event in data:
            event_type = event.get("event_type", "")

            if event_type == "book":
                await self._handle_book_snapshot(event)
            elif event_type == "price_change":
                await self._handle_price_change(event)
            elif event_type == "last_trade_price":
                await self._handle_last_trade(event)
            elif event_type == "market_resolved":
                await self._handle_resolution(event)

    async def _handle_book_snapshot(self, event):
        """Full orderbook snapshot — rebuild from scratch."""
        token_id = event.get("asset_id", "")
        if not token_id:
            return

        bids = [
            (float(b["price"]), float(b["size"]))
            for b in event.get("buys", [])
        ]
        asks = [
            (float(a["price"]), float(a["size"]))
            for a in event.get("sells", [])
        ]
        bids.sort(key=lambda x: -x[0])
        asks.sort(key=lambda x: x[0])

        book = OrderBook(
            token_id=token_id,
            timestamp=time.time(),
            bids=bids,
            asks=asks,
        )

        # Track sequence for gap detection
        seq = event.get("hash", 0)
        self._sequences[token_id] = seq

        await self._store.update_orderbook(book)
        await self._bus.put(book)

    async def _handle_price_change(self, event):
        """Apply delta update to existing orderbook."""
        token_id = event.get("asset_id", "")
        if not token_id:
            return

        # Check sequence continuity
        seq = event.get("hash", 0)
        expected = self._sequences.get(token_id, 0)
        if seq and expected and seq != expected + 1:
            # Gap detected — request full snapshot from REST
            log.debug(f"Sequence gap on {token_id[:8]}, requesting snapshot")
            await self._refetch_snapshot(token_id)
            return
        if seq:
            self._sequences[token_id] = seq

        # Apply delta to existing book
        current = self._store.get_orderbook(token_id)
        if not current:
            await self._refetch_snapshot(token_id)
            return

        bids = list(current.bids)
        asks = list(current.asks)

        for change in event.get("changes", []):
            price = float(change["price"])
            size = float(change["size"])
            side = change.get("side", "").upper()

            if side == "BUY":
                bids = [(p, s) for p, s in bids if p != price]
                if size > 0:
                    bids.append((price, size))
                bids.sort(key=lambda x: -x[0])
            elif side == "SELL":
                asks = [(p, s) for p, s in asks if p != price]
                if size > 0:
                    asks.append((price, size))
                asks.sort(key=lambda x: x[0])

        book = OrderBook(
            token_id=token_id,
            timestamp=time.time(),
            bids=bids,
            asks=asks,
        )
        await self._store.update_orderbook(book)
        await self._bus.put(book)

    async def _handle_last_trade(self, event):
        """Update mid price from last trade — lightweight alternative to full book."""
        token_id = event.get("asset_id", "")
        price = float(event.get("price", 0))
        if token_id and price:
            # Just update the news risk monitor with latest price
            from mm.market_maker import news_monitor
            news_monitor.update_price(token_id, price)

    async def _handle_resolution(self, event):
        """Market has resolved. Emit immediate signal via bus."""
        token_id = event.get("asset_id", "")
        winner = event.get("winner", False)
        log.info(f"[WS] Market resolved: token={token_id[:8]} winner={winner}")
        # The oracle_monitor will pick this up on next poll
        # For WebSocket-triggered immediate signal, emit directly
        # (Future: emit Signal directly here for sub-second response)

    async def _refetch_snapshot(self, token_id: str):
        """Re-fetch full orderbook from REST when WebSocket has a gap."""
        from data.polymarket_rest import fetch_orderbook
        try:
            book = await fetch_orderbook(token_id)
            if book:
                await self._store.update_orderbook(book)
                self._sequences[token_id] = 0
        except Exception as e:
            log.warning(f"Snapshot refetch failed for {token_id[:8]}: {e}")

    def stop(self):
        self._running = False


async def start_websocket_manager(market_store, orderbook_bus: asyncio.Queue):
    """
    Launch WebSocket connections for all active markets.
    Splits into batches of 200 (connection limit).
    """
    markets: list[Market] = market_store.get_active_markets()
    token_ids = []
    for m in markets:
        for t in m.tokens:
            token_ids.append(t.token_id)

    log.info(f"Starting WebSocket manager for {len(token_ids)} tokens")

    # Split into batches
    batch_size = 190  # conservative, under 200 limit
    batches = [token_ids[i:i+batch_size] for i in range(0, len(token_ids), batch_size)]

    tasks = []
    for batch in batches:
        ws = MarketWebSocket(batch, market_store, orderbook_bus)
        tasks.append(asyncio.create_task(ws.start()))

    log.info(f"Launched {len(tasks)} WebSocket connection(s)")
    return tasks
