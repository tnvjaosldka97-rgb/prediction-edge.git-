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
        # 0. Hard killswitch — short-circuits everything. Survives restart.
        from risk import killswitch
        if killswitch.is_tripped():
            info = killswitch.get_trip_info() or {}
            log.warning(
                f"Order REJECTED [KILLSWITCH: {info.get('reason','?')}]: "
                f"{order.strategy} {order.side} {order.token_id[:8]}"
            )
            return None

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
            # Shadow-Live path: realistic fill via live orderbook walk,
            # persisted to virtual_trades, drift samples scheduled.
            try:
                from shadow.virtual_executor import virtual_execute
                from shadow.drift_tracker import schedule_drift_samples
                self._submitted_count += 1
                fill, trade_id = await virtual_execute(order, self._store)
                # Mirror position/bankroll bookkeeping so the existing
                # consumer sees consistent state.
                self._apply_dry_run_bookkeeping(order, fill)
                # Schedule post-fill drift samplers (adverse selection)
                if trade_id is not None:
                    asyncio.create_task(
                        schedule_drift_samples(trade_id, order.token_id, self._store)
                    )
                await self._fill_bus.put(fill)
                return fill
            except Exception as e:
                log.warning(f"[shadow] virtual_execute failed, falling back: {e}")
                fill = self._simulate_fill(order)
                await self._fill_bus.put(fill)
                return fill

        # Maker-first: non-IMMEDIATE signals post GTC limit (0% fee), fall back to taker after 60s
        urgency = signal.urgency if signal else "HIGH"
        if urgency not in ("IMMEDIATE", "HIGH") and order.order_type == "GTC":
            return await self._submit_maker_first(order, signal)

        return await self._submit_live(order, signal)

    def _apply_dry_run_bookkeeping(self, order: Order, fill: Fill) -> None:
        """
        Update portfolio bankroll + positions for a (virtual) DRY_RUN fill.
        Shared by naive _simulate_fill and shadow virtual_execute paths so
        both keep the portfolio state consistent.
        """
        from core.models import Position
        shares = fill.fill_size
        fee = fill.fee_paid
        key = order.token_id
        if order.side == "BUY":
            self._portfolio.bankroll -= fill.fill_price * shares + fee
            if key in self._portfolio.positions:
                existing = self._portfolio.positions[key]
                total_shares = existing.size_shares + shares
                avg_price = (
                    (existing.avg_entry_price * existing.size_shares + fill.fill_price * shares)
                    / total_shares
                )
                self._portfolio.positions[key] = existing.model_copy(update={
                    "size_shares": total_shares,
                    "avg_entry_price": avg_price,
                    "current_price": fill.fill_price,
                })
            else:
                from core.category import effective_category as _eff_cat
                _mkt = self._store.get_market(order.condition_id) if self._store else None
                self._portfolio.positions[key] = Position(
                    condition_id=order.condition_id,
                    token_id=order.token_id,
                    side="BUY",
                    size_shares=shares,
                    avg_entry_price=fill.fill_price,
                    current_price=fill.fill_price,
                    strategy=order.strategy or "",
                    category=_eff_cat(_mkt) if _mkt else "",
                )
        else:
            self._portfolio.bankroll += fill.fill_price * shares - fee
            if key in self._portfolio.positions:
                existing = self._portfolio.positions[key]
                new_size = existing.size_shares - shares
                realized = (fill.fill_price - existing.avg_entry_price) * shares - fee
                self._portfolio.realized_pnl += realized
                if new_size <= 0.001:
                    del self._portfolio.positions[key]
                else:
                    self._portfolio.positions[key] = existing.model_copy(update={
                        "size_shares": new_size,
                        "current_price": fill.fill_price,
                    })
        if self._portfolio.total_value > self._portfolio.peak_value:
            self._portfolio.peak_value = self._portfolio.total_value
        log.info(
            f"[SHADOW] {order.side} ${order.size_usd:.2f} @ {fill.fill_price:.4f} "
            f"(mid→fill slip={abs(fill.fill_price - order.price)*100:.2f}¢) "
            f"| {order.strategy} | fee=${fee:.4f} | bankroll=${self._portfolio.bankroll:.2f}"
        )
        db.insert_trade(
            fill.order_id, fill.condition_id, fill.token_id,
            fill.side, fill.fill_price, fill.fill_size,
            fill.fee_paid, order.strategy,
        )

    def _simulate_fill(self, order: Order) -> Fill:
        """Naive paper-trading fallback — used only if shadow path fails."""
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
            strategy=order.strategy or "",
        )
        self._apply_dry_run_bookkeeping(order, fill)
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

    def _book_snapshot_json(self, token_id: str) -> Optional[str]:
        """LIVE 주문 시 호가창 best 5단계 캡처. friction_traces 분석용."""
        if not self._store:
            return None
        try:
            import json
            book = self._store.get_orderbook(token_id)
            if not book:
                return None
            return json.dumps({
                "ts": book.timestamp,
                "bids": [(p, s) for p, s in book.bids[:5]],
                "asks": [(p, s) for p, s in book.asks[:5]],
            })
        except Exception:
            return None

    def _record_friction_trace(
        self,
        order: Order,
        submit_ts: float,
        *,
        ack_ts: Optional[float] = None,
        fill_ts: Optional[float] = None,
        order_id: Optional[str] = None,
        fill_price: Optional[float] = None,
        fill_size_shares: Optional[float] = None,
        fee_paid: Optional[float] = None,
        rejection_reason: Optional[str] = None,
        api_error_text: Optional[str] = None,
        is_partial: bool = False,
        retry_count: int = 0,
        book_snapshot: Optional[str] = None,
    ) -> None:
        """LIVE 주문 lifecycle을 friction_traces 테이블에 기록. 실패해도 거래 흐름 안 끊음."""
        try:
            fill_size_usd = (fill_size_shares * fill_price) if (fill_size_shares and fill_price) else None
            slippage_bps = None
            if fill_price is not None and order.price > 0:
                slippage_bps = abs(fill_price - order.price) / order.price * 10000
            submit_to_fill_ms = (fill_ts - submit_ts) * 1000 if (fill_ts and submit_ts) else None

            db.insert_friction_trace({
                "order_id": order_id or f"local_{int(submit_ts * 1000)}",
                "condition_id": order.condition_id,
                "token_id": order.token_id,
                "strategy": order.strategy or "",
                "side": order.side,
                "order_type": order.order_type,
                "is_maker": 0,  # _submit_live 경로는 항상 taker (maker는 _submit_maker_first에서 처리)
                "submit_ts": submit_ts,
                "ack_ts": ack_ts,
                "fill_ts": fill_ts,
                "requested_price": order.price,
                "requested_size_usd": order.size_usd,
                "normalized_price": order.price,
                "fill_price": fill_price,
                "fill_size_usd": fill_size_usd,
                "fill_size_shares": fill_size_shares,
                "fee_paid": fee_paid,
                "rejection_reason": rejection_reason,
                "submit_to_fill_ms": submit_to_fill_ms,
                "slippage_bps": slippage_bps,
                "is_partial": 1 if is_partial else 0,
                "network_blip_during": 0,
                "levels_consumed": None,
                "book_snapshot_json": book_snapshot or self._book_snapshot_json(order.token_id),
                "api_error_text": api_error_text,
                "retry_count": retry_count,
            })
        except Exception as e:
            log.warning(f"[friction_trace] record failed: {e}")

    async def _submit_live(self, order: Order, signal: Optional[Signal] = None) -> Optional[Fill]:
        """
        Submit real order to Polymarket CLOB.
        - Retries up to 3x with exponential backoff on 429/5xx
        - Immediate 4xx = permanent rejection, no retry
        - If order is open (no immediate fill), spawns OrderTracker
        - Records friction_trace for every outcome (calibration data)
        """
        if not self._clob_client:
            self._init_clob_client()
        if not self._clob_client:
            log.error("CLOB client not available")
            return None

        submit_ts = time.time()
        book_snapshot = self._book_snapshot_json(order.token_id)
        last_error_text: Optional[str] = None

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

                ack_ts = time.time()

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
                    # friction_trace 기록 — fill 케이스
                    is_partial = size_matched * fill_price < order.size_usd * 0.99
                    self._record_friction_trace(
                        order, submit_ts,
                        ack_ts=ack_ts, fill_ts=ack_ts, order_id=order_id,
                        fill_price=fill_price, fill_size_shares=size_matched, fee_paid=fee,
                        is_partial=is_partial, retry_count=attempt,
                        book_snapshot=book_snapshot,
                    )
                    log.info(
                        f"[FILL] {order.side} {size_matched:.4f}sh @ {fill_price:.4f} "
                        f"| ${size_matched * fill_price:.2f} | fee=${fee:.4f} | {order.strategy}"
                    )
                    # 텔레그램 알림 — 라이브 체결만
                    try:
                        from notifications.telegram import notify
                        notify("FILL", f"{order.side} {order.strategy}", {
                            "size_usd": size_matched * fill_price,
                            "price": fill_price,
                            "fee": fee,
                            "partial": is_partial,
                        })
                    except Exception:
                        pass
                    # WebSocket 브로드캐스트 — 대시보드 즉시 갱신
                    try:
                        from dashboard.realtime import broadcast_event
                        broadcast_event("fill", {
                            "side": order.side,
                            "size_usd": size_matched * fill_price,
                            "price": fill_price,
                            "strategy": order.strategy,
                            "is_partial": is_partial,
                        })
                    except Exception:
                        pass
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

                # friction_trace 기록 — open/no-fill 케이스 (이건 fill_ts 없음)
                self._record_friction_trace(
                    order, submit_ts,
                    ack_ts=ack_ts, order_id=order_id,
                    rejection_reason="opened_no_immediate_fill" if not size_matched else None,
                    retry_count=attempt, book_snapshot=book_snapshot,
                )
                return None

            except Exception as e:
                err = str(e)
                last_error_text = err[:200]
                if "429" in err:
                    wait = (2 ** attempt) * 2
                    log.warning(f"Rate limited (429), backoff {wait}s (attempt {attempt+1}/3)")
                    await asyncio.sleep(wait)
                elif any(c in err for c in ["400", "401", "403", "404"]):
                    log.error(f"Order rejected by CLOB ({err[:60]})")
                    self._record_friction_trace(
                        order, submit_ts,
                        rejection_reason=f"clob_4xx",
                        api_error_text=last_error_text,
                        retry_count=attempt,
                        book_snapshot=book_snapshot,
                    )
                    return None
                else:
                    log.error(f"Submit error attempt {attempt+1}/3: {err[:80]}")
                    if attempt < 2:
                        await asyncio.sleep(2 ** attempt)
                    else:
                        self._record_friction_trace(
                            order, submit_ts,
                            rejection_reason="exhausted_retries",
                            api_error_text=last_error_text,
                            retry_count=attempt,
                            book_snapshot=book_snapshot,
                        )
                        return None

        # 최종 실패 기록
        self._record_friction_trace(
            order, submit_ts,
            rejection_reason="rate_limit_exhausted",
            api_error_text=last_error_text,
            retry_count=2,
            book_snapshot=book_snapshot,
        )
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
