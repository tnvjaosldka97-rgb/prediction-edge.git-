"""
UMA Dispute Premium Carry.

만기 임박 + dispute_risk 가격에 반영 안 된 마켓 → 안전한 carry.

원리:
- Polymarket UMA 분쟁 발생률은 카테고리·표현 명확성에 따라 다름
- 시장은 위험을 일률적으로 가격에 반영하지 X → 분쟁 위험 낮은 마켓에 프리미엄
- 우리는 dispute_risk 낮은 + 만기 < 3일 + 가격 0.92~0.97 마켓 매수
- 만기 도달 시 1.00 → 3~8% return per trade

조건:
- dispute_risk < 0.05 (낮은 위험)
- days_to_resolution < 3
- 0.92 <= price <= 0.97
- volume_24h > $500 (얇은 시장 회피)
- category != unknown/entertainment (지정 카테고리)
"""
from __future__ import annotations
import asyncio
import time
import uuid

import config


# 검증된 설정 (메모리 + 백테스트 기반)
DISPUTE_RISK_MAX = 0.05
DAYS_TO_RES_MAX = 3.0
PRICE_MIN = 0.92
PRICE_MAX = 0.97
VOLUME_MIN = 500.0
EXCLUDED_CATEGORIES = {"unknown", "entertainment"}

# Cooldown — 같은 마켓 30분 내 중복 X
_COOLDOWN_SEC = 1800


class DisputePremiumScanner:
    def __init__(self, store, signal_bus):
        self._store = store
        self._bus = signal_bus
        self._last_signal: dict[str, float] = {}    # condition_id → ts

    async def start(self):
        from core.logger import log
        from core.models import Signal
        from core.category import effective_category
        log.info("[dispute_premium] scanner started")

        while True:
            try:
                await asyncio.sleep(120)
                if not self._store:
                    continue

                emitted = 0
                for m in self._store.get_active_markets():
                    if effective_category(m).lower() in EXCLUDED_CATEGORIES:
                        continue
                    if m.volume_24h < VOLUME_MIN:
                        continue
                    if m.days_to_resolution > DAYS_TO_RES_MAX:
                        continue
                    if m.dispute_risk > DISPUTE_RISK_MAX:
                        continue
                    yes_token = m.yes_token
                    if not yes_token:
                        continue
                    price = yes_token.price
                    if not (PRICE_MIN <= price <= PRICE_MAX):
                        continue

                    # cooldown
                    if time.time() - self._last_signal.get(m.condition_id, 0) < _COOLDOWN_SEC:
                        continue

                    # model_prob = 만기 시 1.00 - dispute_risk × 0.5 (분쟁 시 50% 회복 가정)
                    model_prob = 1.0 - m.dispute_risk * 0.5
                    edge = model_prob - price
                    fee_pct = config.TAKER_FEE_RATE * price * (1 - price)
                    net_edge = edge - fee_pct
                    if net_edge < config.MIN_EDGE_AFTER_FEES:
                        continue

                    sig = Signal(
                        signal_id=str(uuid.uuid4()),
                        strategy="fee_arbitrage",   # 기존 라벨 재사용 (carry 패턴 동일)
                        condition_id=m.condition_id,
                        token_id=yes_token.token_id,
                        direction="BUY",
                        model_prob=model_prob,
                        market_prob=price,
                        edge=edge,
                        net_edge=net_edge,
                        confidence=0.85,    # 통계적으로 강한 패턴
                        urgency="LOW",      # GTC maker 가능
                        stale_price=price,
                        stale_threshold=0.02,
                    )
                    await self._bus.put(sig)
                    self._last_signal[m.condition_id] = time.time()
                    emitted += 1
                    log.info(
                        f"[dispute_premium] BUY {m.condition_id[:8]} "
                        f"price={price:.3f} days={m.days_to_resolution:.1f} risk={m.dispute_risk:.3f} "
                        f"edge={edge:+.3f}"
                    )

                if emitted:
                    log.info(f"[dispute_premium] {emitted} signals emitted this cycle")

            except Exception as e:
                from core.logger import log
                log.warning(f"[dispute_premium] {e}")
                await asyncio.sleep(60)
