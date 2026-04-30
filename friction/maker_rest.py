"""
Maker GTC rest behavior — maker 주문이 호가 큐에서 대기하는 시뮬.

현실:
- maker GTC limit 주문은 호가창에 들어가서 대기
- 같은 가격대 큐의 우선순위 (FIFO)
- 시장 가격이 우리 가격을 통과하면 체결
- 60초 안에 안 채워지면 우리 봇이 cancel + taker로 전환

모델:
- 큐 우선순위: 깊이 비례 (우리 사이즈만큼 앞서 있다고 가정)
- 60초 윈도우 내 체결 확률 = f(가격 거리, 변동성, 깊이)
- 부분 체결 가능

수익 영향:
- maker 0% fee (taker 2%) → 체결되면 큰 이득
- 체결 실패 → 시그널 손실 (시간 비용 + 시장 이미 움직임)
"""
from __future__ import annotations
import math
import random
from dataclasses import dataclass
from typing import Optional


@dataclass
class MakerRestResult:
    filled: bool
    fill_ratio: float            # 0~1 (부분 체결 가능)
    time_to_fill_sec: float      # 체결까지 걸린 시간
    cancelled_at_taker: bool     # 60s timeout으로 cancel 후 taker 전환했나


def simulate_maker_rest(
    side: str,
    size_usd: float,
    limit_price: float,
    book_at_submit,
    market_volatility_5m: float = 0.0,
    timeout_sec: float = 60.0,
    rng: Optional[random.Random] = None,
) -> MakerRestResult:
    """
    Maker GTC 주문이 timeout_sec 내에 체결될 확률 + 부분 체결 비율 시뮬.

    조건별 체결 확률:
      가격 거리 (mid 대비 얼마나 떨어진 limit인지)
        - mid에 close (< 0.005 차이): 60~80% 체결
        - mid에서 멈 (> 0.02): 5~20% 체결
      변동성 높음: 체결 확률 ↑ (가격이 우리 limit 통과)
      큐 깊이: 우리 앞에 사이즈 많으면 체결 확률 ↓
    """
    r = rng or random
    levels = book_at_submit.asks if side == "BUY" else book_at_submit.bids
    if not levels:
        return MakerRestResult(False, 0.0, timeout_sec, True)

    best = levels[0][0]
    # 우리 limit 가격이 best와 얼마나 떨어져있나
    if side == "BUY":
        # BUY: limit < best_ask 면 maker, ≥ best_ask면 taker (즉시 체결)
        if limit_price >= best:
            return MakerRestResult(True, 1.0, 0.5, False)
        distance_to_top = best - limit_price
    else:  # SELL
        if limit_price <= best:
            return MakerRestResult(True, 1.0, 0.5, False)
        distance_to_top = limit_price - best

    # 큐 우선순위 추정 — 같은 가격대 뒤에 깔린 사이즈
    queue_ahead_usd = 0.0
    for p, s in levels:
        if (side == "BUY" and p > limit_price) or (side == "SELL" and p < limit_price):
            queue_ahead_usd += p * s
        else:
            break

    # 체결 확률 모델
    # 1. 가격 거리 영향 (가까울수록 ↑)
    distance_factor = max(0.05, math.exp(-distance_to_top / 0.005))
    # 2. 변동성 영향 (높을수록 ↑)
    vol_factor = min(2.0, 1 + market_volatility_5m * 5)
    # 3. 큐 깊이 영향 (앞에 적을수록 ↑)
    queue_factor = 1.0 / (1 + queue_ahead_usd / 1000)

    fill_prob = min(0.95, distance_factor * vol_factor * queue_factor)

    if r.random() > fill_prob:
        # Timeout으로 cancel — 60초 후 taker 전환 (caller가 재시도)
        return MakerRestResult(False, 0.0, timeout_sec, True)

    # 체결됨 — 시간은 어디쯤?
    # 일찍 체결 = 짧은 시간, 늦게 체결 = 긴 시간
    time_to_fill = r.uniform(2.0, timeout_sec * 0.8)

    # 부분 체결 비율
    # 체결됐어도 큐 일부만 소진하고 timeout 가능성
    if r.random() < 0.15:    # 15% 부분 체결
        fill_ratio = r.uniform(0.3, 0.8)
    else:
        fill_ratio = 1.0

    return MakerRestResult(
        filled=True,
        fill_ratio=fill_ratio,
        time_to_fill_sec=time_to_fill,
        cancelled_at_taker=False,
    )
