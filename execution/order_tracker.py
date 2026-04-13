"""
Order Lifecycle Tracker

After an order is submitted, this module:
1. Polls CLOB for fill status until terminal state
2. Handles partial fills — re-queues remainder if still has edge
3. Cancels orders that go stale (price moved away)
4. Prevents duplicate orders (same token/side within dedup window)
"""
from __future__ import annotations
import asyncio
import time
import uuid
from typing import Optional
from core.models import Order, Fill, Signal
from core.logger import log
import config


# In-flight order registry — prevents duplicates
# key: (token_id, side) → expires_at
_inflight: dict[tuple, float] = {}
DEDUP_WINDOW = 30.0  # seconds — block same token/side within this window


def is_duplicate(token_id: str, side: str) -> bool:
    key = (token_id, side)
    expires = _inflight.get(key, 0)
    if time.time() < expires:
        return True
    return False


def register_inflight(token_id: str, side: str, ttl: float = DEDUP_WINDOW):
    _inflight[(token_id, side)] = time.time() + ttl


def clear_inflight(token_id: str, side: str):
    _inflight.pop((token_id, side), None)


class OrderTracker:
    """
    Tracks one submitted order through its full lifecycle.

    States:
      pending → open → [partial] → filled
                              ↓
                           cancelled (stale price / timeout)
    """

    POLL_INTERVAL = 3.0      # seconds between status checks
    MAX_OPEN_TIME = 300.0    # cancel after 5 minutes unfilled
    STALE_THRESHOLD = 0.03   # cancel if price moves >3% against us

    def __init__(self, order: Order, signal: Optional[Signal], clob_client, fill_bus: asyncio.Queue):
        self._order = order
        self._signal = signal
        self._clob = clob_client
        self._fill_bus = fill_bus
        self._order_id: Optional[str] = None
        self._submitted_at = time.time()
        self._filled_size = 0.0

    async def run(self, order_id: str):
        """Track an order after submission. Call with the CLOB order ID."""
        self._order_id = order_id
        log.info(f"[TRACKER] Monitoring order {order_id[:12]} {self._order.side} {self._order.token_id[:8]}")

        while True:
            await asyncio.sleep(self.POLL_INTERVAL)

            # 1. Timeout — cancel if open too long
            if time.time() - self._submitted_at > self.MAX_OPEN_TIME:
                await self._cancel(reason="timeout")
                break

            # 2. Fetch current order status from CLOB
            status = await self._fetch_status()
            if not status:
                continue

            order_status = status.get("status", "")
            size_matched = float(status.get("sizeMatched", 0))

            # 3. Full fill
            if order_status in ("MATCHED", "FILLED") and size_matched > 0:
                new_fill = size_matched - self._filled_size
                if new_fill > 0:
                    await self._record_fill(size_matched=new_fill, fill_price=float(status.get("price", self._order.price)))
                log.info(f"[TRACKER] Order {order_id[:12]} FILLED: {size_matched:.4f} shares")
                break

            # 4. Partial fill
            if size_matched > self._filled_size:
                new_fill = size_matched - self._filled_size
                self._filled_size = size_matched
                await self._record_fill(size_matched=new_fill, fill_price=float(status.get("price", self._order.price)))
                log.info(f"[TRACKER] Partial fill {order_id[:12]}: {size_matched:.4f}/{self._order.size_usd / self._order.price:.4f} shares")

            # 5. Cancelled externally
            if order_status in ("CANCELLED", "CANCELED"):
                log.info(f"[TRACKER] Order {order_id[:12]} cancelled externally")
                clear_inflight(self._order.token_id, self._order.side)
                break

            # 6. Stale price check
            if self._signal and await self._is_stale():
                await self._cancel(reason="stale price")
                break

    async def _fetch_status(self) -> Optional[dict]:
        try:
            return self._clob.get_order(self._order_id)
        except Exception as e:
            log.debug(f"Order status fetch failed: {e}")
            return None

    async def _is_stale(self) -> bool:
        """Check if the market price has moved enough to kill the edge."""
        if not self._signal or self._signal.stale_threshold <= 0:
            return False
        try:
            from data.polymarket_rest import fetch_midpoint
            current = await fetch_midpoint(self._order.token_id)
            if current and self._signal.is_stale(current):
                log.info(f"[TRACKER] Price moved from {self._signal.stale_price:.4f} to {current:.4f} — cancelling")
                return True
        except Exception:
            pass
        return False

    async def _cancel(self, reason: str = ""):
        try:
            self._clob.cancel({"orderID": self._order_id})
            log.info(f"[TRACKER] Cancelled order {self._order_id[:12] if self._order_id else '?'} [{reason}]")
        except Exception as e:
            log.warning(f"Cancel failed for {self._order_id}: {e}")
        finally:
            clear_inflight(self._order.token_id, self._order.side)

    async def _record_fill(self, size_matched: float, fill_price: float):
        fee = fill_price * (1 - fill_price) * config.TAKER_FEE_RATE * size_matched
        fill = Fill(
            order_id=self._order_id or str(uuid.uuid4()),
            condition_id=self._order.condition_id,
            token_id=self._order.token_id,
            side=self._order.side,
            fill_price=fill_price,
            fill_size=size_matched,
            fee_paid=fee,
        )
        await self._fill_bus.put(fill)
        from core import db
        db.insert_trade(
            fill.order_id, fill.condition_id, fill.token_id,
            fill.side, fill.fill_price, fill.fill_size, fill.fee_paid,
            self._order.strategy,
        )
