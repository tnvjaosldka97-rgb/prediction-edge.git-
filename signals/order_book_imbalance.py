"""
Order Book Imbalance Momentum.

호가 depth imbalance가 급격히 5x 이상 발생하면 30초~2분 내
가격이 그 방향으로 움직이는 통계적 패턴.

원리:
- 큰 매수 주문이 호가 depth에 표시 → 매도자가 따라잡음 → 가격 ↑
- 우리는 첫 번째로 들어가서 momentum 받음

main store에서 바로 imbalance 계산. polymarket_ws가 이미 full L2 채움.
"""
from __future__ import annotations
import asyncio
import time
import uuid
from collections import deque
from dataclasses import dataclass

import config


@dataclass
class ImbalanceSnapshot:
    ts: float
    bid_depth_usd: float
    ask_depth_usd: float
    imbalance_ratio: float    # bid/ask
    mid: float


class OrderBookImbalanceScanner:
    def __init__(self, store, signal_bus):
        self._store = store
        self._bus = signal_bus
        self._history: dict[str, deque[ImbalanceSnapshot]] = {}
        self._cooldown: dict[str, float] = {}

    async def start(self):
        from core.logger import log
        from core.models import Signal
        log.info("[ob_imbalance] scanner started")

        while True:
            try:
                await asyncio.sleep(2)
                if not self._store:
                    continue

                markets = self._store.get_active_markets()
                for m in markets[:200]:
                    for token in m.tokens:
                        token_id = token.token_id
                        book = self._store.get_orderbook(token_id)
                        if not book or book.is_stale():
                            continue
                        if not book.bids or not book.asks:
                            continue

                        bid_depth = sum(p * s for p, s in book.bids[:5])
                        ask_depth = sum(p * s for p, s in book.asks[:5])
                        if bid_depth + ask_depth < 100:
                            continue

                        ratio = bid_depth / max(1, ask_depth)
                        mid_price = (book.best_bid + book.best_ask) / 2
                        snap = ImbalanceSnapshot(
                            ts=time.time(),
                            bid_depth_usd=bid_depth,
                            ask_depth_usd=ask_depth,
                            imbalance_ratio=ratio,
                            mid=mid_price,
                        )
                        hist = self._history.setdefault(token_id, deque(maxlen=30))
                        hist.append(snap)

                        if len(hist) < 5:
                            continue
                        if time.time() - self._cooldown.get(token_id, 0) < 60:
                            continue

                        prev_avg = sum(s.imbalance_ratio for s in list(hist)[:-1]) / max(1, len(hist) - 1)
                        current = snap.imbalance_ratio

                        direction = None
                        if current > 5.0 and prev_avg < 2.0:
                            direction = "BUY"
                        elif current < 0.2 and prev_avg > 0.5:
                            direction = "SELL"
                        if not direction:
                            continue

                        drift = 0.005 if direction == "BUY" else -0.005
                        implied = max(0.01, min(0.99, snap.mid + drift))
                        edge = abs(implied - snap.mid)
                        fee_pct = config.TAKER_FEE_RATE * snap.mid * (1 - snap.mid)
                        net_edge = edge - fee_pct
                        if net_edge < 0.002:
                            continue

                        sig = Signal(
                            signal_id=str(uuid.uuid4()),
                            strategy="order_flow",
                            condition_id=m.condition_id,
                            token_id=token_id,
                            direction=direction,
                            model_prob=implied,
                            market_prob=snap.mid,
                            edge=edge,
                            net_edge=net_edge,
                            confidence=0.55,
                            urgency="IMMEDIATE",
                            stale_price=snap.mid,
                            stale_threshold=0.01,
                        )
                        await self._bus.put(sig)
                        self._cooldown[token_id] = time.time()
                        log.info(
                            f"[ob_imbalance] {direction} {token_id[:8]} "
                            f"ratio={ratio:.2f} (prev_avg={prev_avg:.2f}) edge={edge:.3f}"
                        )

            except Exception as e:
                from core.logger import log
                log.warning(f"[ob_imbalance] {e}")
                await asyncio.sleep(10)
