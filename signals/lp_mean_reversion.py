"""
Liquidity Provider Mean Reversion.

원리:
- 마켓메이커가 일시적으로 호가 빠지면 → 가격 spike → 30~120초 후 회귀
- 우리는 spike 감지 → 반대 방향 진입 → 회귀 받음
- 짧은 holding period (1~3분), 빠른 exit

조건:
- 1분 평균 mid 대비 현재 mid 변동 > 3%
- 호가 depth가 5분 평균 대비 < 30% (LP 빠짐)
- 변동 후 30초 안 지남 (아직 회귀 안 시작)
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
    def __init__(self, l2_store, signal_bus, store=None):
        self._l2_store = l2_store
        self._bus = signal_bus
        self._store = store
        self._history: dict[str, deque[MidSnapshot]] = {}
        self._cooldown: dict[str, float] = {}

    async def start(self):
        from core.logger import log
        from core.models import Signal
        log.info("[lp_mean_rev] scanner started")

        while True:
            try:
                await asyncio.sleep(5)    # 5초 폴링
                if not self._l2_store:
                    continue

                for token_id, book in list(self._l2_store._books.items()):
                    if not book.bids or not book.asks:
                        continue

                    total_depth = sum(l.price * l.size for l in book.bids[:5]) + \
                                  sum(l.price * l.size for l in book.asks[:5])
                    if total_depth < 100:
                        continue

                    snap = MidSnapshot(time.time(), book.mid, total_depth)
                    hist = self._history.setdefault(token_id, deque(maxlen=60))    # 5분치
                    hist.append(snap)

                    if len(hist) < 12:    # 1분 미만이면 평균 신뢰 X
                        continue
                    if time.time() - self._cooldown.get(token_id, 0) < 120:
                        continue

                    # 1분 전 평균 mid + 5분 전 평균 depth
                    one_min_ago_snaps = [s for s in hist if s.ts >= snap.ts - 60 and s.ts < snap.ts - 5]
                    five_min_avg_depth = sum(s.total_depth_usd for s in hist) / len(hist)

                    if not one_min_ago_snaps:
                        continue

                    prev_avg_mid = sum(s.mid for s in one_min_ago_snaps) / len(one_min_ago_snaps)
                    if prev_avg_mid <= 0:
                        continue

                    deviation = (snap.mid - prev_avg_mid) / prev_avg_mid
                    depth_ratio = snap.total_depth_usd / max(1, five_min_avg_depth)

                    # 조건: 변동 > 3% AND depth 빠짐 (< 30% of avg)
                    if abs(deviation) < 0.03:
                        continue
                    if depth_ratio > 0.50:    # depth 충분하면 정상 가격 발견 — skip
                        continue

                    # 회귀 방향 — 가격이 올랐으면 SELL 시그널 (회귀해서 떨어질 것)
                    direction = "SELL" if deviation > 0 else "BUY"
                    implied = prev_avg_mid    # 회귀 목표
                    edge = abs(snap.mid - implied)

                    fee_pct = config.TAKER_FEE_RATE * snap.mid * (1 - snap.mid)
                    net_edge = edge - fee_pct
                    if net_edge < 0.005:
                        continue

                    if not self._store:
                        continue
                    market = None
                    for m in self._store.get_active_markets():
                        if any(t.token_id == token_id for t in m.tokens):
                            market = m
                            break
                    if not market:
                        continue

                    sig = Signal(
                        signal_id=str(uuid.uuid4()),
                        strategy="order_flow",    # 기존 라벨 (momentum과 같은 카테고리)
                        condition_id=market.condition_id,
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
