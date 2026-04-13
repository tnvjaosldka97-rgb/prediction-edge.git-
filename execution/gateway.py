"""
Execution Gateway — single point for all order submission.

Responsibilities:
1. Pre-flight risk checks
2. Rate limiting (token bucket)
3. Duplicate order prevention
4. Dry-run / live routing
5. Immediate fill detection
6. Spawn OrderTracker for open orders (partial fill / stale cancel)
7. Hand off to Reconciler for balance sync

This is the ONLY module that touches py_clob_client.
"""
from __future__ import annotations
import asyncio
import time
from typing import Optional, Callable
import config
from core.models import Order, Fill, PortfolioState, Signal, Market
from core.logger import log
from risk.limits import check_all, should_halve_size
from core import db
from execution.order_tracker import is_duplicate, register_inflight, OrderTracker


class TokenBucket:
    """Token bucket rate limiter."""
    def __init__(self, per_minute: int):
        self._tokens = float(per_minute)
        self._max = float(per_minute)
        self._refill_rate = per_minute / 60.0
        self._last_refill = time.time()

    async def acquire(self):
        now = time.time()
        elapsed = now - self._last_refill
        self._tokens = min(self._max, self._tokens + elapsed * self._refill_rate)
        self._last_refill = now
        if self._tokens >= 1:
            self._tokens -= 1
        else:
            wait = (1 - self._tokens) / self._refill_rate
            await asyncio.sleep(wait)
            self._tokens = 0


class ExecutionGateway:
    def __init__(self, portfolio: PortfolioState, fill_bus: asyncio.Queue, store=None):
        self._portfolio = portfolio
        self._fill_bus = fill_bus
        self._store = store
        self._rate_limiter = TokenBucket(config.CLOB_ORDERS_PER_MIN)
        self._clob_client = None
        self._submitted_count = 0
        self._rejected_count = 0
        self._partial_count = 0
        self._reconciler = None   # set by main.py after init

    def set_reconciler(self, reconciler):
        self._reconciler = reconciler

    def get_clob(self):
        return self._clob_client

    def _init_clob_client(self):
        """Lazy init — only when we have credentials."""
        if not config.PRIVATE_KEY or not config.API_KEY:
            return
        try:
            from py_clob_client.client import ClobClient
            from py_clob_client.clob_types import ApiCreds
            self._clob_client = ClobClient(
                host=config.CLOB_HOST,
                chain_id=config.POLYGON_CHAIN_ID,
                key=config.PRIVATE_KEY,
                creds=ApiCreds(
                    api_key=config.API_KEY,
                    api_secret=config.API_SECRET,
                    api_passphrase=config.API_PASSPHRASE,
                ),
            )
            log.info("CLOB client initialized (L2 auth)")
        except Exception as e:
            log.error(f"CLOB client init failed: {e}")

    async def submit(
        self,
        order: Order,
        signal: Optional[Signal] = None,
        market: Optional[Market] = None,
    ) -> Optional[Fill]:
        """
        Submit an order with full pre-flight checks.
        Returns immediate Fill if available, None otherwise.
        Background tracking handles open/partial orders.
        """
        # 1. Duplicate prevention
        if is_duplicate(order.token_id, order.side):
            log.debug(f"Duplicate blocked: {order.side} {order.token_id[:8]}")
            return None

        # 2. Risk pre-flight
        allowed, reason = check_all(order, signal, self._portfolio, market, store=self._store)
        if not allowed:
            self._rejected_count += 1
            log.warning(f"Order REJECTED [{reason}]: {order.strategy} {order.side} {order.token_id[:8]}")
            return None

        # 3. Drawdown size reduction
        if should_halve_size(self._portfolio):
            order = order.model_copy(update={"size_usd": order.size_usd * 0.5})
            log.info(f"Drawdown reduction: halved size to ${order.size_usd:.2f}")

        # 4. Minimum size check
        min_size = getattr(config, "MIN_ORDER_SIZE_USD", 2.0)
        if order.size_usd < min_size:
            return None

        # 5. Rate limit
        await self._rate_limiter.acquire()

        # 6. Register inflight to prevent duplicates while order is live
        register_inflight(order.token_id, order.side)

        if config.DRY_RUN:
            fill = self._simulate_fill(order)
            await self._fill_bus.put(fill)
            return fill

        return await self._submit_live(order, signal)

    def _simulate_fill(self, order: Order) -> Fill:
        """Paper trading — assume immediate full fill at order price."""
        self._submitted_count += 1
        shares = order.size_usd / order.price
        fee = order.price * (1 - order.price) * config.TAKER_FEE_RATE * shares
        fill = Fill(
            order_id=f"dry_{self._submitted_count}",
            condition_id=order.condition_id,
            token_id=order.token_id,
            side=order.side,
            fill_price=order.price,
            fill_size=shares,
            fee_paid=fee,
        )
        log.info(
            f"[DRY RUN] {order.side} ${order.size_usd:.2f} @ {order.price:.4f} "
            f"| {order.strategy} | fee=${fee:.4f}"
        )
        db.insert_trade(
            fill.order_id, fill.condition_id, fill.token_id,
            fill.side, fill.fill_price, fill.fill_size,
            fill.fee_paid, order.strategy,
        )
        return fill

    async def _submit_live(self, order: Order, signal: Optional[Signal] = None) -> Optional[Fill]:
        """
        Submit real order to Polymarket CLOB.
        - Retries up to 3x with exponential backoff on 429/5xx
        - Immediate 4xx = permanent rejection, no retry
        - If order is open (no immediate fill), spawns OrderTracker
        """
        if not self._clob_client:
            self._init_clob_client()
        if not self._clob_client:
            log.error("CLOB client not available")
            return None

        for attempt in range(3):
            try:
                from py_clob_client.clob_types import OrderArgs
                args = OrderArgs(
                    token_id=order.token_id,
                    price=order.price,
                    size=order.size_usd / order.price,
                    side=order.side,
                )
                resp = self._clob_client.create_and_post_order(args)
                order_id = resp.get("orderID", "")
                fill_price = float(resp.get("price", order.price))
                size_matched = float(resp.get("sizeMatched", 0))
                order_status = resp.get("status", "")

                # Register with reconciler
                if self._reconciler and order_id:
                    self._reconciler.register_order(order_id)

                # Immediate full fill
                if size_matched > 0:
                    fee = fill_price * (1 - fill_price) * config.TAKER_FEE_RATE * size_matched
                    fill = Fill(
                        order_id=order_id,
                        condition_id=order.condition_id,
                        token_id=order.token_id,
                        side=order.side,
                        fill_price=fill_price,
                        fill_size=size_matched,
                        fee_paid=fee,
                    )
                    db.insert_trade(
                        order_id, order.condition_id, order.token_id,
                        order.side, fill_price, size_matched, fee, order.strategy,
                    )
                    await self._fill_bus.put(fill)
                    self._submitted_count += 1
                    if self._reconciler:
                        self._reconciler.confirm_fill(order_id)
                    log.info(
                        f"[FILL] {order.side} {size_matched:.4f}sh @ {fill_price:.4f} "
                        f"| ${size_matched * fill_price:.2f} | fee=${fee:.4f} | {order.strategy}"
                    )
                    return fill

                # Order is open/resting — spawn tracker for fill monitoring
                if order_id and order_status in ("LIVE", "OPEN", ""):
                    self._submitted_count += 1
                    log.info(
                        f"[OPEN] {order.side} ${order.size_usd:.2f} @ {order.price:.4f} "
                        f"order_id={order_id[:12]} — tracking for fill"
                    )
                    tracker = OrderTracker(order, signal, self._clob_client, self._fill_bus)
                    asyncio.create_task(tracker.run(order_id), name=f"tracker_{order_id[:8]}")

                return None

            except Exception as e:
                err = str(e)
                if "429" in err:
                    wait = (2 ** attempt) * 2
                    log.warning(f"Rate limited (429), backoff {wait}s (attempt {attempt+1}/3)")
                    await asyncio.sleep(wait)
                elif any(c in err for c in ["400", "401", "403", "404"]):
                    log.error(f"Order rejected by CLOB ({err[:60]})")
                    return None
                else:
                    log.error(f"Submit error attempt {attempt+1}/3: {err[:80]}")
                    if attempt < 2:
                        await asyncio.sleep(2 ** attempt)
                    else:
                        return None

        return None

    async def submit_quote(self, order: Order) -> tuple[Optional[Fill], Optional[str]]:
        """
        Submit a market making quote. Bypasses duplicate detection (MM replaces quotes intentionally).
        Returns (fill_or_none, order_id_or_none) so the MM can track open quotes for cancellation.
        """
        allowed, reason = check_all(order, None, self._portfolio, None, store=self._store)
        if not allowed:
            log.debug(f"MM quote rejected: {reason}")
            return None, None

        await self._rate_limiter.acquire()

        if config.DRY_RUN:
            fill = self._simulate_fill(order)
            await self._fill_bus.put(fill)
            return fill, fill.order_id

        if not self._clob_client:
            self._init_clob_client()
        if not self._clob_client:
            return None, None

        try:
            from py_clob_client.clob_types import OrderArgs
            args = OrderArgs(
                token_id=order.token_id,
                price=order.price,
                size=order.size_usd / order.price,
                side=order.side,
            )
            resp = self._clob_client.create_and_post_order(args)
            order_id = resp.get("orderID", "")
            fill_price = float(resp.get("price", order.price))
            size_matched = float(resp.get("sizeMatched", 0))

            if size_matched > 0:
                fee = fill_price * (1 - fill_price) * config.TAKER_FEE_RATE * size_matched
                fill = Fill(
                    order_id=order_id,
                    condition_id=order.condition_id,
                    token_id=order.token_id,
                    side=order.side,
                    fill_price=fill_price,
                    fill_size=size_matched,
                    fee_paid=fee,
                )
                db.insert_trade(
                    order_id, order.condition_id, order.token_id,
                    order.side, fill_price, size_matched, fee, order.strategy,
                )
                await self._fill_bus.put(fill)
                self._submitted_count += 1
                return fill, order_id

            if order_id:
                self._submitted_count += 1
                return None, order_id

            return None, None

        except Exception as e:
            log.error(f"MM quote submission failed: {e}")
            return None, None

    async def cancel_order(self, order_id: str) -> bool:
        """Cancel an open order by ID."""
        if config.DRY_RUN:
            log.debug(f"[DRY RUN] Cancel order {order_id[:12]}")
            return True

        if not self._clob_client:
            return False

        try:
            self._clob_client.cancel({"orderID": order_id})
            return True
        except Exception as e:
            log.debug(f"Cancel {order_id[:12]} failed: {e}")
            return False

    @property
    def stats(self) -> dict:
        return {
            "submitted": self._submitted_count,
            "rejected": self._rejected_count,
            "partial": self._partial_count,
            "dry_run": config.DRY_RUN,
        }
