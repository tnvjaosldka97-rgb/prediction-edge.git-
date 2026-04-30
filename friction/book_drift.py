"""
Book drift during latency — 주문이 in-flight인 동안 호가가 움직이는 시뮬.

원리:
- 우리 주문이 submit → fill 사이 latency_ms (보통 100~300ms) 시간이 흐름
- 그 사이 책이 가만히 있을 거란 가정은 비현실적
- BUY 주문 → 다른 매수자도 진입 → ask 가격 ↑ → 우리 fill 가격 ↑ (불리)
- SELL도 동일 비대칭 (불리)

모델:
- short-term realized vol (5분 mid std) 사용
- 우리 주문 방향과 같은 방향으로 ε × √(latency_sec) 만큼 mid drift 가정
  (역선택 — 우리가 들어가는 방향으로 책이 움직임)
- vol 데이터 없으면 default 0.5% per √s
"""
from __future__ import annotations
import math
import sqlite3
import time
from dataclasses import dataclass
from typing import Optional

import config


@dataclass
class DriftEstimate:
    drift_pct: float          # mid 변동 비율 (signed: BUY는 +, SELL은 -)
    annualized_vol_pct: float
    latency_sec: float


def get_recent_volatility(token_id: str, lookback_min: int = 5) -> float:
    """price_history에서 최근 N분 mid 표준편차 → annualized vol."""
    since_ts = time.time() - lookback_min * 60
    conn = sqlite3.connect(config.DB_PATH)
    rows = conn.execute(
        "SELECT price FROM price_history WHERE token_id=? AND timestamp >= ? ORDER BY timestamp ASC",
        (token_id, since_ts),
    ).fetchall()
    conn.close()
    if len(rows) < 5:
        return 0.05    # 기본값 5% (보수적)
    prices = [r[0] for r in rows]
    rets = [(prices[i] - prices[i - 1]) / prices[i - 1]
            for i in range(1, len(prices)) if prices[i - 1] > 0]
    if not rets:
        return 0.05
    mean = sum(rets) / len(rets)
    var = sum((r - mean) ** 2 for r in rets) / max(1, len(rets) - 1)
    std = math.sqrt(var)
    # 표본 간격 추정 — lookback에 표본 N개면 dt = lookback/(N-1)
    dt_sec = lookback_min * 60 / max(1, len(rets))
    n_per_year = 365 * 86400 / dt_sec
    return std * math.sqrt(n_per_year)    # 연환산


def estimate_drift(
    token_id: str,
    side: str,                    # "BUY" / "SELL"
    latency_ms: float,
    adverse_factor: float = 0.7,  # 역선택 강도 (0=중립, 1=완전 역선택)
) -> DriftEstimate:
    """
    latency_ms 동안 mid가 우리한테 불리한 방향으로 얼마나 움직일지 추정.

    BUY: drift 양수 (ask 비싸짐)
    SELL: drift 음수 (bid 싸짐)
    절대값 = adverse_factor × σ × √(latency_sec)
    """
    vol_annual = get_recent_volatility(token_id)
    latency_sec = latency_ms / 1000
    # 연환산 vol을 latency 시간 단위로 변환
    sec_per_year = 365 * 86400
    vol_in_window = vol_annual * math.sqrt(latency_sec / sec_per_year)
    drift = adverse_factor * vol_in_window
    if side == "SELL":
        drift = -drift
    return DriftEstimate(
        drift_pct=drift,
        annualized_vol_pct=vol_annual * 100,
        latency_sec=latency_sec,
    )


def apply_drift_to_book(book, drift_pct: float):
    """OrderBook 복사본에 mid drift 적용. shallow copy + 가격 시프트."""
    from core.models import OrderBook
    if drift_pct == 0:
        return book
    # 가격 시프트 — 모든 레벨에 같은 % 적용
    new_bids = [(p * (1 + drift_pct), s) for p, s in book.bids]
    new_asks = [(p * (1 + drift_pct), s) for p, s in book.asks]
    # 가격 범위 클램핑
    new_bids = [(min(0.999, max(0.001, p)), s) for p, s in new_bids]
    new_asks = [(min(0.999, max(0.001, p)), s) for p, s in new_asks]
    return OrderBook(
        token_id=book.token_id,
        timestamp=book.timestamp,
        bids=new_bids,
        asks=new_asks,
    )
