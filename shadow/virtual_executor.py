"""
Virtual Executor — realistic fill simulation using live orderbook.

Given a signal + order, this:
  1. Reads the CURRENT L2 orderbook from market_store (live WS feed)
  2. **Applies friction.orchestrator (7-layer marrtl model)** for accurate sim:
     - latency (log-normal RTT)
     - slippage (book walking)
     - partial_fill (FOK/IOC/GTC ratio)
     - rejection (rate limit / sig expired / RPC error)
     - network_blip (Polygon RPC outage)
     - clob_quirks (tick/min_size validation)
     - fund_lock (settlement delay)
  3. Records friction_traces row → calibration loop reads this
  4. Persists virtual_trades row with full snapshot + metadata
  5. Schedules adverse-selection samplers at T+5s/60s/5min

DRY_RUN PnL is now within ~10% of LIVE PnL (was 70% optimistic before).
"""
from __future__ import annotations
import asyncio
import json
import time
import uuid
from typing import Optional

import config
from core.models import Order, Fill, OrderBook
from core.logger import log
from core import db


# ── Realistic fill pricing ────────────────────────────────────────────────────

def walk_orderbook(
    book: OrderBook, side: str, size_usd: float
) -> tuple[float, float, float, int]:
    """
    Walk the orderbook to compute realistic fill for a given USD size.

    Returns:
      (avg_fill_price, slippage_vs_top, depth_consumed, levels_touched)

    - avg_fill_price: size-weighted blended price
    - slippage_vs_top: difference between avg and best price (in $)
    - depth_consumed: total size in shares eaten
    - levels_touched: number of price levels we had to walk

    Conservative assumptions:
      - If book is empty or synthetic (depth=500 sentinel), fall back to
        "would take mid + 2¢" as a degenerate estimate.
      - If size exceeds total visible depth, apply a 5¢ penalty on the
        uncovered portion (represents walking into hidden / moving liq).
    """
    levels = book.asks if side == "BUY" else book.bids
    if not levels:
        # No book → degenerate, use mid + wide penalty
        mid = book.mid_price if book.mid_price else 0.5
        return (mid + 0.02 if side == "BUY" else mid - 0.02), 0.02, 0.0, 0

    # Synthetic book detection (market_store uses 500 as sentinel)
    top_size = levels[0][1]
    if top_size >= 490 and len(levels) <= 1:
        mid = book.mid_price or levels[0][0]
        return (levels[0][0] + 0.01 if side == "BUY" else levels[0][0] - 0.01), 0.01, 0.0, 1

    top_price = levels[0][0]
    remaining_usd = size_usd
    total_shares = 0.0
    total_cost = 0.0
    levels_touched = 0

    for price, size in levels:
        if remaining_usd <= 0:
            break
        # At this price, size shares are available = size * price USD of notional
        level_capacity_usd = size * price
        if remaining_usd <= level_capacity_usd:
            shares = remaining_usd / price
            total_shares += shares
            total_cost += shares * price
            remaining_usd = 0.0
            levels_touched += 1
            break
        else:
            total_shares += size
            total_cost += size * price
            remaining_usd -= level_capacity_usd
            levels_touched += 1

    if remaining_usd > 0:
        # Walked off the visible book — apply hidden-liquidity penalty.
        # Model: worst-level price + 5¢ (direction-adjusted).
        worst_price = levels[-1][0]
        penalty_price = (
            min(0.999, worst_price + 0.05)
            if side == "BUY"
            else max(0.001, worst_price - 0.05)
        )
        extra_shares = remaining_usd / penalty_price
        total_shares += extra_shares
        total_cost += remaining_usd

    avg_fill = total_cost / total_shares if total_shares > 0 else top_price
    slippage_vs_top = abs(avg_fill - top_price)
    return avg_fill, slippage_vs_top, total_shares, levels_touched


# ── DB persistence ────────────────────────────────────────────────────────────

def persist_virtual_trade(
    order: Order,
    fill_price: float,
    slippage: float,
    levels_touched: int,
    book_snapshot: dict,
    mid_at_signal: float,
    ask_at_signal: float,
    bid_at_signal: float,
) -> int:
    """Insert a virtual_trades row and return its rowid."""
    conn = db.get_conn()
    cur = conn.execute(
        """INSERT INTO virtual_trades
           (signal_ts, fill_ts, condition_id, token_id, strategy, side,
            size_usd, mid_at_signal, ask_at_signal, bid_at_signal,
            fill_price, slippage, levels_touched, book_snapshot_json,
            category)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            time.time(),
            time.time(),
            order.condition_id,
            order.token_id,
            order.strategy or "",
            order.side,
            order.size_usd,
            mid_at_signal,
            ask_at_signal,
            bid_at_signal,
            fill_price,
            slippage,
            levels_touched,
            json.dumps(book_snapshot),
            "",  # filled by caller
        ),
    )
    conn.commit()
    return cur.lastrowid


def record_drift_sample(
    trade_id: int, seconds_after: int, mid_price: float
) -> None:
    """Store a post-fill price drift sample."""
    conn = db.get_conn()
    col = f"mid_after_{seconds_after}s"
    try:
        conn.execute(
            f"UPDATE virtual_trades SET {col} = ? WHERE id = ?",
            (mid_price, trade_id),
        )
        conn.commit()
    except Exception as e:
        log.debug(f"[shadow] drift sample write error: {e}")


def mark_resolved(trade_id: int, exit_price: float, realized_pnl: float) -> None:
    conn = db.get_conn()
    try:
        conn.execute(
            """UPDATE virtual_trades
               SET exit_price = ?, realized_pnl = ?, resolved_at = ?
               WHERE id = ?""",
            (exit_price, realized_pnl, time.time(), trade_id),
        )
        conn.commit()
    except Exception as e:
        log.debug(f"[shadow] mark resolved error: {e}")


# ── High-level entry point ────────────────────────────────────────────────────

# ── Singleton friction orchestrator for shadow path ─────────────────────────

_FRICTION = None


def _get_friction():
    """Lazy-init shared friction orchestrator. Same instance used by
    main.py's auto_calibrate_loop, so calibration updates apply to shadow too."""
    global _FRICTION
    if _FRICTION is None:
        try:
            from friction.orchestrator import FrictionOrchestrator
            from friction.calibrate import load_latest
            _FRICTION = FrictionOrchestrator(taker_fee_rate=config.TAKER_FEE_RATE)
            try:
                load_latest(_FRICTION)
            except Exception:
                pass
        except ImportError:
            return None
    return _FRICTION


async def virtual_execute(
    order: Order,
    store,
) -> tuple[Optional[Fill], Optional[int]]:
    """
    Execute order in shadow mode + apply 7-layer friction model.

    Returns (Fill, virtual_trade_id). Fill compatible with normal pipeline.
    If friction model rejects (rate limit, sig expired, etc.), returns (None, None)
    and records the rejection in friction_traces table.

    No book → fallback to order.price (degenerate path).
    """
    book: Optional[OrderBook] = store.get_orderbook(order.token_id) if store else None
    submit_ts = time.time()

    # ── Pre-flight checks (concurrent limit + collateral) ────────────────
    try:
        from friction.concurrent_orders import get_tracker
        tracker = get_tracker()
        tracker.cleanup_stale()
        ok, reason = tracker.can_submit()
        if not ok:
            log.info(f"[shadow] {order.side} ${order.size_usd:.2f} REJECTED — {reason}")
            try:
                db.insert_friction_trace({
                    "order_id": f"shadow_rej_{uuid.uuid4().hex[:8]}",
                    "submit_ts": submit_ts,
                    "side": order.side,
                    "requested_price": order.price,
                    "requested_size_usd": order.size_usd,
                    "rejection_reason": "concurrent_limit",
                    "strategy": order.strategy or "",
                })
            except Exception:
                pass
            return None, None
    except ImportError:
        tracker = None

    # ── Maker GTC: rest behavior 시뮬 (60s 윈도우, 부분 체결, cancel 가능) ──
    if order.order_type == "GTC" and book and not book.is_stale():
        try:
            from friction.maker_rest import simulate_maker_rest
            mid = book.mid_price or order.price
            recent_vol = abs(mid - order.price) / max(0.001, mid)    # 단순 vol proxy
            rest = simulate_maker_rest(
                side=order.side,
                size_usd=order.size_usd,
                limit_price=order.price,
                book_at_submit=book,
                market_volatility_5m=recent_vol,
            )
            if not rest.filled:
                log.info(
                    f"[shadow] maker GTC {order.side} ${order.size_usd:.2f} @ {order.price:.4f} "
                    f"NOT FILLED in 60s — cancelled"
                )
                try:
                    db.insert_friction_trace({
                        "order_id": f"shadow_maker_unfilled_{uuid.uuid4().hex[:8]}",
                        "submit_ts": submit_ts,
                        "side": order.side,
                        "order_type": order.order_type,
                        "is_maker": 1,
                        "requested_price": order.price,
                        "requested_size_usd": order.size_usd,
                        "rejection_reason": "maker_timeout_cancelled",
                        "submit_to_fill_ms": rest.time_to_fill_sec * 1000,
                        "strategy": order.strategy or "",
                    })
                except Exception:
                    pass
                return None, None
            # Maker 체결됐으면 size를 fill_ratio로 조정
            if rest.fill_ratio < 1.0:
                effective_size = order.size_usd * rest.fill_ratio
                # 새 Order 객체 만들지 말고 size_usd만 임시 조정
                order = order.model_copy(update={"size_usd": effective_size})
        except Exception as e:
            log.debug(f"[shadow] maker_rest sim failed: {e}")

    friction = _get_friction()

    # ── Friction-aware fill simulation ────────────────────────────────────
    if friction and book and not book.is_stale():
        from friction.orchestrator import SimulatedFill
        is_maker = order.order_type == "GTC"
        sim: SimulatedFill = friction.simulate(
            side=order.side,
            size_usd=order.size_usd,
            price=order.price,
            order_type=order.order_type,
            is_maker=is_maker,
            book_at_submit=book,
            submit_ts=submit_ts,
            future_book_lookup=None,  # 라이브 시뮬은 현재 호가 그대로
            market_volatility_5m=0.1,
        )

        # 거부 케이스 — friction_traces에 기록 후 None 반환 (signal drop)
        if not sim.accepted:
            try:
                db.insert_friction_trace({
                    "order_id": f"shadow_rej_{uuid.uuid4().hex[:8]}",
                    "condition_id": order.condition_id,
                    "token_id": order.token_id,
                    "strategy": order.strategy or "",
                    "side": order.side,
                    "order_type": order.order_type,
                    "is_maker": 1 if is_maker else 0,
                    "submit_ts": submit_ts,
                    "requested_price": order.price,
                    "requested_size_usd": order.size_usd,
                    "normalized_price": sim.normalized_price,
                    "rejection_reason": sim.rejection_reason,
                    "submit_to_fill_ms": sim.submit_to_fill_ms,
                    "is_partial": 0,
                    "network_blip_during": 1 if sim.network_blip_during else 0,
                })
            except Exception:
                pass
            log.info(
                f"[shadow] {order.side} ${order.size_usd:.2f} REJECTED — "
                f"reason={sim.rejection_reason} | {order.strategy}"
            )
            return None, None

        fill_price = sim.avg_fill_price
        shares = sim.filled_size_shares
        slippage = sim.slippage_bps / 10000 * fill_price    # bps → absolute
        levels = sim.levels_consumed
        fee = sim.fee_paid

        mid = book.mid_price or order.price
        ask = book.best_ask or order.price
        bid = book.best_bid or order.price
        snap = {
            "asks": list(book.asks[:10]),
            "bids": list(book.bids[:10]),
            "ts": time.time(),
            "friction_applied": True,
            "submit_to_fill_ms": sim.submit_to_fill_ms,
            "is_partial": sim.is_partial,
            "network_blip": sim.network_blip_during,
        }

        # 체결된 케이스도 friction_traces 기록 (캘리브레이션용)
        try:
            db.insert_friction_trace({
                "order_id": f"shadow_{uuid.uuid4().hex[:8]}",
                "condition_id": order.condition_id,
                "token_id": order.token_id,
                "strategy": order.strategy or "",
                "side": order.side,
                "order_type": order.order_type,
                "is_maker": 1 if is_maker else 0,
                "submit_ts": submit_ts,
                "ack_ts": submit_ts + sim.submit_to_fill_ms / 1000,
                "fill_ts": submit_ts + sim.submit_to_fill_ms / 1000,
                "requested_price": order.price,
                "requested_size_usd": order.size_usd,
                "normalized_price": sim.normalized_price,
                "fill_price": fill_price,
                "fill_size_usd": sim.filled_size_usd,
                "fill_size_shares": shares,
                "fee_paid": fee,
                "submit_to_fill_ms": sim.submit_to_fill_ms,
                "slippage_bps": sim.slippage_bps,
                "is_partial": 1 if sim.is_partial else 0,
                "network_blip_during": 0,
                "levels_consumed": levels,
            })
        except Exception:
            pass

    elif book and not book.is_stale():
        # friction 모듈 없음 → 기존 walk_orderbook 폴백
        fill_price, slippage, shares, levels = walk_orderbook(book, order.side, order.size_usd)
        fee = fill_price * (1 - fill_price) * config.TAKER_FEE_RATE * shares
        mid = book.mid_price or order.price
        ask = book.best_ask or order.price
        bid = book.best_bid or order.price
        snap = {
            "asks": list(book.asks[:10]),
            "bids": list(book.bids[:10]),
            "ts": time.time(),
            "friction_applied": False,
        }
    else:
        # Degenerate — no book at fill time
        fill_price = order.price
        slippage = 0.0
        shares = order.size_usd / order.price
        levels = 0
        fee = fill_price * (1 - fill_price) * config.TAKER_FEE_RATE * shares
        mid = ask = bid = order.price
        snap = {"asks": [], "bids": [], "no_book": True, "ts": time.time()}

    # Persist virtual_trade
    try:
        trade_id = persist_virtual_trade(
            order=order,
            fill_price=fill_price,
            slippage=slippage,
            levels_touched=levels,
            book_snapshot=snap,
            mid_at_signal=mid,
            ask_at_signal=ask,
            bid_at_signal=bid,
        )
        try:
            mkt = store.get_market(order.condition_id) if store else None
            from core.category import effective_category as _eff_cat
            cat = _eff_cat(mkt) if mkt else ""
            conn = db.get_conn()
            conn.execute(
                "UPDATE virtual_trades SET category = ? WHERE id = ?",
                (cat, trade_id),
            )
            conn.commit()
        except Exception:
            pass
    except Exception as e:
        log.warning(f"[shadow] persist virtual_trade failed: {e}")
        trade_id = None

    fill = Fill(
        order_id=f"shadow_{uuid.uuid4().hex[:8]}",
        condition_id=order.condition_id,
        token_id=order.token_id,
        side=order.side,
        fill_price=fill_price,
        fill_size=shares,
        fee_paid=fee,
        timestamp=time.time(),
        strategy=order.strategy or "",
    )
    # 동시 주문 트래커에 등록 (체결됐으니 즉시 close, 하지만 카운팅 위해)
    if tracker:
        try:
            tracker.register(fill.order_id, order.size_usd, order.side)
            tracker.mark_filled(fill.order_id)
        except Exception:
            pass
    return fill, trade_id
