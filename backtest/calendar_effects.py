"""
Calendar Effects 마이닝 — 요일·시간대 별 수익률 패턴.

기존 trades 데이터에서 자동으로 추출:
- 요일별 평균 P&L (월~일)
- 시간대별 (UTC 0~23h)
- 시간대 × 카테고리 교차 분석
- FOMC 주, 미국 공휴일 효과

발견된 패턴 → strategy_versioning에 저장 → 다른 시그널에서 부스팅 factor로 활용.
"""
from __future__ import annotations
import json
import sqlite3
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import config


@dataclass
class CalendarPattern:
    pattern_type: str       # "day_of_week", "hour_of_day", "fomc_week", etc.
    bucket: str             # "monday", "14h_utc", "fomc=true"
    n_observations: int
    avg_pnl: float
    win_rate: float
    t_statistic: float
    p_value: float


def _conn():
    return sqlite3.connect(config.DB_PATH)


def _t_stat(values: list[float]) -> tuple[float, float]:
    import math
    n = len(values)
    if n < 3:
        return 0.0, 1.0
    mean = sum(values) / n
    var = sum((v - mean) ** 2 for v in values) / (n - 1)
    se = math.sqrt(var / n)
    t = mean / se if se > 0 else 0.0
    # 정규근사 p-value
    from math import erf, sqrt
    z = abs(t)
    p = 2 * (1 - 0.5 * (1 + erf(z / sqrt(2))))
    return t, p


def mine_day_of_week_pattern(window_days: float = 30) -> list[CalendarPattern]:
    since_ts = time.time() - window_days * 86400
    conn = _conn()
    rows = conn.execute(
        "SELECT timestamp, COALESCE(pnl, 0) FROM trades WHERE timestamp >= ?",
        (since_ts,)
    ).fetchall()
    conn.close()

    if not rows:
        return []

    by_dow: dict[int, list[float]] = defaultdict(list)
    for ts, pnl in rows:
        dow = datetime.fromtimestamp(ts, tz=timezone.utc).weekday()
        by_dow[dow].append(pnl or 0)

    names = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
    patterns = []
    for d, pnls in by_dow.items():
        if len(pnls) < 3:
            continue
        t, p = _t_stat(pnls)
        wins = sum(1 for x in pnls if x > 0)
        patterns.append(CalendarPattern(
            pattern_type="day_of_week",
            bucket=names[d],
            n_observations=len(pnls),
            avg_pnl=sum(pnls) / len(pnls),
            win_rate=wins / len(pnls),
            t_statistic=t,
            p_value=p,
        ))
    return patterns


def mine_hour_of_day_pattern(window_days: float = 30) -> list[CalendarPattern]:
    since_ts = time.time() - window_days * 86400
    conn = _conn()
    rows = conn.execute(
        "SELECT timestamp, COALESCE(pnl, 0) FROM trades WHERE timestamp >= ?",
        (since_ts,)
    ).fetchall()
    conn.close()

    by_hour: dict[int, list[float]] = defaultdict(list)
    for ts, pnl in rows:
        h = datetime.fromtimestamp(ts, tz=timezone.utc).hour
        by_hour[h].append(pnl or 0)

    patterns = []
    for h in range(24):
        pnls = by_hour.get(h, [])
        if len(pnls) < 3:
            continue
        t, p = _t_stat(pnls)
        wins = sum(1 for x in pnls if x > 0)
        patterns.append(CalendarPattern(
            pattern_type="hour_of_day",
            bucket=f"{h:02d}h_utc",
            n_observations=len(pnls),
            avg_pnl=sum(pnls) / len(pnls),
            win_rate=wins / len(pnls),
            t_statistic=t,
            p_value=p,
        ))
    return patterns


def mine_strategy_x_hour(window_days: float = 30) -> list[dict]:
    """전략별 × 시간대 — 어느 전략이 어느 시간대에 강한지."""
    since_ts = time.time() - window_days * 86400
    conn = _conn()
    rows = conn.execute(
        "SELECT strategy, timestamp, COALESCE(pnl, 0) FROM trades "
        "WHERE timestamp >= ? AND strategy IS NOT NULL",
        (since_ts,)
    ).fetchall()
    conn.close()

    bucket: dict[tuple[str, int], list[float]] = defaultdict(list)
    for strategy, ts, pnl in rows:
        h = datetime.fromtimestamp(ts, tz=timezone.utc).hour
        bucket[(strategy, h)].append(pnl or 0)

    out = []
    for (strategy, h), pnls in bucket.items():
        if len(pnls) < 3:
            continue
        out.append({
            "strategy": strategy,
            "hour_utc": h,
            "n": len(pnls),
            "avg_pnl": sum(pnls) / len(pnls),
            "win_rate": sum(1 for x in pnls if x > 0) / len(pnls),
        })
    return sorted(out, key=lambda x: -x["avg_pnl"])


def get_significant_patterns(window_days: float = 30, p_threshold: float = 0.10) -> dict:
    """모든 패턴 중 통계적 유의 (p < threshold) 만 반환."""
    dow = mine_day_of_week_pattern(window_days)
    hod = mine_hour_of_day_pattern(window_days)

    significant_dow = [p for p in dow if p.p_value < p_threshold]
    significant_hod = [p for p in hod if p.p_value < p_threshold]

    return {
        "day_of_week": [{
            "bucket": p.bucket, "n": p.n_observations,
            "avg_pnl": p.avg_pnl, "win_rate": p.win_rate,
            "t": p.t_statistic, "p": p.p_value,
        } for p in significant_dow],
        "hour_of_day": [{
            "bucket": p.bucket, "n": p.n_observations,
            "avg_pnl": p.avg_pnl, "win_rate": p.win_rate,
            "t": p.t_statistic, "p": p.p_value,
        } for p in significant_hod],
    }
