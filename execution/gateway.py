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
from risk.limits import check_all, should_halve_size, record_trade_executed
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
        """
        Init CLOB client.
        1. Try stored L2 API creds (fast path)
        2. If missing or invalid, auto-derive from PRIVATE_KEY (creates new L2 session)
        """
        if not config.PRIVATE_KEY:
            return
        try:
            from py_clob_client.client import ClobClient
            from py_clob_client.clob_types import ApiCreds

            if config.API_KEY and config.API_SECRET and config.API_PASSPHRASE:
                # Use stored creds
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
                log.info("CLOB client initialized (stored L2 creds)")
            else:
                # Derive L2 creds from private key (L1 auth)
                self._clob_client = ClobClient(
                    host=config.CLOB_HOST,
                    chain_id=config.POLYGON_CHAIN_ID,
                    key=config.PRIVATE_KEY,
                )
                log.info("CLOB client initialized (L1 key only — will derive L2 on first call)")
        except Exception as e:
            log.error(f"CLOB client init failed: {e}")

    async def validate_credentials(self) -> bool:
        """
        Startup check — verify credentials are valid before first trade.
        Calls get_balance() on CLOB. Fails fast if keys are wrong.
        """
        if config.DRY_RUN:
            log.info("[Gateway] DRY RUN mode — skipping credential validation")
            return True
        if not config.PRIVATE_KEY or not config.API_KEY:
            log.error("[Gateway] LIVE mode but no credentials set! Set PRIVATE_KEY + POLY_API_KEY")
            return False
        self._init_clob_client()
        if not self._clob_client:
            return False
        try:
            # Try get_orders() — lightweight auth check
            orders = self._clob_client.get_orders()
            log.info(f"[Gateway] Credentials valid ✓ — open orders: {len(orders) if isinstance(orders, list) else 0}")
            return True
        except Exception as e:
            err = str(e)
            if "401" in err or "Unauthorized" in err or "Invalid api key" in err:
                # L2 creds rejected — try to derive fresh ones from private key
                log.warning("[Gateway] L2 creds invalid — attempting to derive fresh credentials from private key...")
                try:
                    from py_clob_client.client import ClobClient
                    fresh = ClobClient(
                        host=config.CLOB_HOST,
                        chain_id=config.POLYGON_CHAIN_ID,
                        key=config.PRIVATE_KEY,
                    )
                    creds = fresh.create_or_derive_api_creds()
                    # Re-init with fresh creds
                    from py_clob_client.clob_types import ApiCreds
                    self._clob_client = ClobClient(
                        host=config.CLOB_HOST,
                        chain_id=config.POLYGON_CHAIN_ID,
                        key=config.PRIVATE_KEY,
                        creds=ApiCreds(
                            api_key=creds.api_key,
                            api_secret=creds.api_secret,
                            api_passphrase=creds.api_passphrase,
                        ),
                    )
                    orders = self._clob_client.get_orders()
                    log.info(
                        f"[Gateway] Fresh L2 creds derived ✓ | "
                        f"key={creds.api_key[:12]}... | open orders: {len(orders) if isinstance(orders, list) else 0}"
                    )
                    return True
                except Exception as e2:
                    log.error(f"[Gateway] L2 key derivation failed: {e2}")
                    return False
            log.error(f"[Gateway] Credential validation FAILED: {e}")
            return False

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

        # Maker-first: non-IMMEDIATE signals post GTC limit (0% fee), fall back to taker after 60s
        urgency = signal.urgency if signal else "HIGH"
        if urgency not in ("IMMEDIATE", "HIGH") and order.order_type == "GTC":
            return await self._submit_maker_first(order, signal)

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

    async def _submit_maker_first(self, order: Order, signal: Optional[Signal] = None) -> Optional[Fill]:
        """
        Maker-first execution: post GTC limit at signal price (rests in book = 0% fee).
        Wait up to 60s. If unfilled, cancel and fall back to taker.

        Fee savings: taker costs 2%*(1-p) per USD. At p=0.5 that's 1% saved per trade.
        Over 300 trades this compounds materially.
        """
        if not self._clob_client:
            self._init_clob_client()
        if not self._clob_client:
            return await self._submit_live(order, signal)

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
            size_matched = float(resp.get("sizeMatched", 0))

            # Immediate fill (crossed spread) — treat as taker fill
            if size_matched > 0:
                fee = order.price * (1 - order.price) * config.TAKER_FEE_RATE * size_matched
                fill = Fill(
                    order_id=order_id,
                    condition_id=order.condition_id,
                    token_id=order.token_id,
                    side=order.side,
                    fill_price=float(resp.get("price", order.price)),
                    fill_size=size_matched,
                    fee_paid=fee,
                )
                db.insert_trade(
                    order_id, order.condition_id, order.token_id,
                    order.side, fill.fill_price, size_matched, fee, order.strategy,
                )
                await self._fill_bus.put(fill)
                self._submitted_count += 1
                if self._reconciler:
                    self._reconciler.confirm_fill(order_id)
                return fill

            if not order_id:
                return await self._submit_live(order, signal)

            # Order resting — poll for 60s then cancel + taker fallback
            log.info(
                f"[MAKER] {order.side} ${order.size_usd:.2f} @ {order.price:.4f} "
                f"order_id={order_id[:12]} — waiting up to 60s for maker fill"
            )
            self._submitted_count += 1
            if self._reconciler:
                self._reconciler.register_order(order_id)

            for _ in range(12):  # 12 × 5s = 60s
                await asyncio.sleep(5)
                try:
                    status = self._clob_client.get_order(order_id)
                    filled = float(status.get("sizeMatched", 0))
                    if filled > 0:
                        fill_price = float(status.get("price", order.price))
                        # Maker fill = 0% fee
                        fill = Fill(
                            order_id=order_id,
                            condition_id=order.condition_id,
                            token_id=order.token_id,
                            side=order.side,
                            fill_price=fill_price,
                            fill_size=filled,
                            fee_paid=0.0,
                        )
                        db.insert_trade(
                            order_id, order.condition_id, order.token_id,
                            order.side, fill_price, filled, 0.0, order.strategy + "_maker",
                        )
                        await self._fill_bus.put(fill)
                        if self._reconciler:
                            self._reconciler.confirm_fill(order_id)
                        log.info(
                            f"[MAKER FILL] {order.side} {filled:.4f}sh @ {fill_price:.4f} "
                            f"fee=$0.00 (saved ~${order.price*(1-order.price)*config.TAKER_FEE_RATE*filled:.4f})"
                        )
                        return fill
                    if status.get("status") in ("CANCELLED", "EXPIRED"):
                        break
                except Exception:
                    pass

            # Cancel resting order, fall back to taker
            await self.cancel_order(order_id)
            log.info(f"[MAKER→TAKER] No fill in 60s, falling back to taker for {order.token_id[:8]}")
            return await self._submit_live(order, signal)

        except Exception as e:
            log.warning(f"Maker-first failed ({e}), falling back to taker")
            return await self._submit_live(order, signal)

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
                    record_trade_executed()
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
