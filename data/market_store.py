"""
In-memory market state cache.
Hot data for O(1) signal lookups.
"""
from __future__ import annotations
import asyncio
import time
from typing import Optional
from core.models import Market, OrderBook
from core.logger import log
from core import db


class MarketStore:
    def __init__(self):
        self._markets: dict[str, Market] = {}          # condition_id → Market
        self._orderbooks: dict[str, OrderBook] = {}    # token_id → OrderBook
        self._lock = asyncio.Lock()

    async def update_markets(self, markets: list[Market]):
        async with self._lock:
            for m in markets:
                self._markets[m.condition_id] = m

    def get_market(self, condition_id: str) -> Optional[Market]:
        return self._markets.get(condition_id)

    def get_all_markets(self) -> list[Market]:
        return list(self._markets.values())

    def get_active_markets(self) -> list[Market]:
        return [m for m in self._markets.values() if m.active]

    async def update_orderbook(self, book: OrderBook):
        async with self._lock:
            self._orderbooks[book.token_id] = book
        # Record price history for correlation/volatility
        db.record_price(book.token_id, book.mid)

    def get_orderbook(self, token_id: str) -> Optional[OrderBook]:
        book = self._orderbooks.get(token_id)
        if book and book.is_stale(max_age_sec=30):
            return None  # don't return stale data
        return book

    def get_mid_price(self, token_id: str) -> Optional[float]:
        book = self.get_orderbook(token_id)
        return book.mid if book else None
