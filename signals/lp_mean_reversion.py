"""
Liquidity Provider Mean Reversion.

원리:
- 마켓메이커가 일시적으로 호가 빠지면 → 가격 spike → 30~120초 후 평균 복귀
- 우리: spike 감지 → 반대 방향 진입 → 회귀 받음
- 짧은 holding (1~3분), 빠른 exit
"""
from __future__ import annotations
import asyncio
import time
import uuid
from collections import deque
from dataclasses import dataclass

import config


@dataclass
class MidSnapshot:
    ts: float
    mid: float
    total_depth_usd: float


class LPMeanReversionScanner:
    def __init__(self, store, signal_bus):
        self._store = store
        self._bus = signal_bus
        self._history: dict[str, deque[MidSnapshot]] = {}
        self._cooldown: dict[str, float] = {}

    async def start(self):
        from core.logger import log
        from core.models import Signal
        log.info("[lp_mean_rev] scanner started")

        while True:
            try:
                await asyncio.sleep(5)
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
                        total_depth = bid_depth + ask_depth
                        if total_depth < 100:
                            continue

                        mid = (book.best_bid + book.best_ask) / 2
                        snap = MidSnapshot(time.time(), mid, total_depth)
                        hist = self._history.setdefault(token_id, deque(maxlen=60))
                        hist.append(snap)

                        if len(hist) < 12:
                            continue
                        if time.time() - self._cooldown.get(token_id, 0) < 120:
                            continue

                        one_min_ago_snaps = [s for s in hist if s.ts >= snap.ts - 60 and s.ts < snap.ts - 5]
                        five_min_avg_depth = sum(s.total_depth_usd for s in hist) / len(hist)
                        if not one_min_ago_snaps:
                            continue

                        prev_avg_mid = sum(s.mid for s in one_min_ago_snaps) / len(one_min_ago_snaps)
                        if prev_avg_mid <= 0:
                            continue
                        deviation = (snap.mid - prev_avg_mid) / prev_avg_mid
                        depth_ratio = snap.total_depth_usd / max(1, five_min_avg_depth)

                        if abs(deviation) < 0.03:
                            continue
                        if depth_ratio > 0.50:    # depth 충분 → 정상 가격 발견
                            continue

                        direction = "SELL" if deviation > 0 else "BUY"
                        implied = prev_avg_mid
                        edge = abs(snap.mid - implied)
                        fee_pct = config.TAKER_FEE_RATE * snap.mid * (1 - snap.mid)
                        net_edge = edge - fee_pct
                        if net_edge < 0.005:
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
                            urgency="HIGH",
                            stale_price=snap.mid,
                            stale_threshold=0.015,
                        )
                        await self._bus.put(sig)
                        self._cooldown[token_id] = time.time()
                        log.info(
                            f"[lp_mean_rev] {direction} {token_id[:8]} "
                            f"dev={deviation:+.3f} depth={depth_ratio:.2f}x"
                        )
            except Exception as e:
                from core.logger import log
                log.warning(f"[lp_mean_rev] {e}")
                await asyncio.sleep(20)
