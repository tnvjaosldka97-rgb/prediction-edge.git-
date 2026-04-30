"""
Order Book Imbalance Momentum.

호가 depth imbalance가 급격히 5x 이상 발생하면 30초~2분 내
가격이 그 방향으로 움직이는 통계적 패턴.

원리:
- 큰 매수 주문이 호가 depth에 표시 → 매도자가 따라잡음 → 가격 ↑
- 우리는 첫 번째로 들어가서 momentum 받음

L2 store에서 imbalance 계산 → 임계값 초과 시 시그널.
빠른 entry + 빠른 exit (60~120초) → 짧은 holding period.
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
    def __init__(self, l2_store, signal_bus, store=None):
        """
        l2_store: data.orderbook_l2.L2Store 인스턴스
        signal_bus: signals 전송 큐
        store: 기본 MarketStore (token_id → market 매핑)
        """
        self._l2_store = l2_store
        self._bus = signal_bus
        self._store = store
        # token_id → recent ImbalanceSnapshot deque (1분치)
        self._history: dict[str, deque[ImbalanceSnapshot]] = {}
        self._cooldown: dict[str, float] = {}     # token_id → 마지막 시그널 ts (60초 cooldown)

    async def start(self):
        from core.logger import log
        from core.models import Signal
        log.info("[ob_imbalance] scanner started")

        while True:
            try:
                await asyncio.sleep(2)  # 2초 폴링
                if not self._l2_store:
                    continue

                for token_id, book in list(self._l2_store._books.items()):
                    if not book.bids or not book.asks:
                        continue

                    # depth USD (best 5단계)
                    bid_depth = sum(l.price * l.size for l in book.bids[:5])
                    ask_depth = sum(l.price * l.size for l in book.asks[:5])
                    if bid_depth + ask_depth < 100:    # 미니멀 리퀴디티
                        continue

                    ratio = bid_depth / max(1, ask_depth)
                    snap = ImbalanceSnapshot(
                        ts=time.time(),
                        bid_depth_usd=bid_depth,
                        ask_depth_usd=ask_depth,
                        imbalance_ratio=ratio,
                        mid=book.mid,
                    )
                    hist = self._history.setdefault(token_id, deque(maxlen=30))
                    hist.append(snap)

                    # 시그널 조건
                    if len(hist) < 5:
                        continue
                    if time.time() - self._cooldown.get(token_id, 0) < 60:
                        continue

                    # 평균 imbalance 대비 급변
                    prev_avg = sum(s.imbalance_ratio for s in list(hist)[:-1]) / max(1, len(hist) - 1)
                    current = snap.imbalance_ratio

                    direction = None
                    if current > 5.0 and prev_avg < 2.0:
                        direction = "BUY"   # bid pressure → 가격 ↑
                    elif current < 0.2 and prev_avg > 0.5:
                        direction = "SELL"  # ask pressure → 가격 ↓

                    if not direction:
                        continue

                    if not self._store:
                        continue
                    market = None
                    for m in self._store.get_active_markets():
                        for t in m.tokens:
                            if t.token_id == token_id:
                                market = m
                                break
                        if market:
                            break
                    if not market:
                        continue

                    # 짧은 holding 전략 — implied price = current mid + small drift
                    drift = 0.005 if direction == "BUY" else -0.005
                    implied = max(0.01, min(0.99, snap.mid + drift))
                    edge = abs(implied - snap.mid)

                    # fee 차감 — momentum 짧은 holding이라 taker로 빠르게
                    fee_pct = config.TAKER_FEE_RATE * snap.mid * (1 - snap.mid)
                    net_edge = edge - fee_pct
                    if net_edge < 0.002:    # 0.2% 미만이면 의미 없음
                        continue

                    sig = Signal(
                        signal_id=str(uuid.uuid4()),
                        strategy="order_flow",
                        condition_id=market.condition_id,
                        token_id=token_id,
                        direction=direction,
                        model_prob=implied,
                        market_prob=snap.mid,
                        edge=edge,
                        net_edge=net_edge,
                        confidence=0.55,    # momentum 패턴 신뢰도 중간
                        urgency="IMMEDIATE",
                        stale_price=snap.mid,
                        stale_threshold=0.01,    # 1% 이상 움직이면 cancel
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
