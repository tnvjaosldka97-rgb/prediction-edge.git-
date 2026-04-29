"""
포트폴리오·전략 성과 지표.

Sharpe / Sortino / max drawdown / recovery factor / 누적 분포.
portfolio_snapshots + trades 테이블에서 계산.

사용:
  from core.metrics import compute_portfolio_metrics, compute_strategy_metrics
  m = compute_portfolio_metrics(window_days=30)
  print(m["sharpe"], m["max_drawdown"])
"""
from __future__ import annotations
import math
import sqlite3
import time
from dataclasses import dataclass, asdict
from typing import Optional

import config


def _conn() -> sqlite3.Connection:
    return sqlite3.connect(config.DB_PATH)


@dataclass
class PortfolioMetrics:
    n_snapshots: int
    starting_value: float
    ending_value: float
    return_pct: float
    annualized_return_pct: float
    sharpe: float
    sortino: float
    max_drawdown_pct: float
    recovery_factor: float
    volatility_annualized_pct: float
    days_observed: float


def _returns_from_snapshots(snapshots: list[tuple]) -> list[float]:
    """[(timestamp, total_value), ...] -> 시간 가중 returns (per-snapshot)."""
    rets = []
    for i in range(1, len(snapshots)):
        prev = snapshots[i - 1][1]
        curr = snapshots[i][1]
        if prev > 0:
            rets.append(curr / prev - 1)
    return rets


def _max_drawdown(values: list[float]) -> tuple[float, float]:
    """최대 낙폭 + recovery factor (peak 대비)."""
    if not values:
        return 0.0, 0.0
    peak = values[0]
    max_dd = 0.0
    for v in values:
        if v > peak:
            peak = v
        dd = (peak - v) / peak if peak > 0 else 0
        if dd > max_dd:
            max_dd = dd
    return max_dd, peak


def compute_portfolio_metrics(window_days: float = 30, risk_free_rate: float = 0.04) -> PortfolioMetrics:
    """portfolio_snapshots 기반 성과 지표.

    Sharpe, Sortino는 연환산 (sqrt(N_snapshots_per_year) 곱).
    스냅샷은 ~10분 간격이라 연 약 52,560 표본.
    """
    since_ts = time.time() - window_days * 86400
    conn = _conn()
    rows = conn.execute(
        "SELECT timestamp, total_value FROM portfolio_snapshots "
        "WHERE timestamp >= ? ORDER BY timestamp ASC",
        (since_ts,)
    ).fetchall()
    conn.close()

    if len(rows) < 2:
        return PortfolioMetrics(
            n_snapshots=len(rows), starting_value=0, ending_value=0,
            return_pct=0, annualized_return_pct=0, sharpe=0, sortino=0,
            max_drawdown_pct=0, recovery_factor=0, volatility_annualized_pct=0,
            days_observed=0,
        )

    starting = rows[0][1]
    ending = rows[-1][1]
    days = (rows[-1][0] - rows[0][0]) / 86400 if rows else 0

    total_ret = (ending - starting) / starting if starting > 0 else 0
    annualized_ret = ((1 + total_ret) ** (365 / max(0.01, days)) - 1) if days > 0 else 0

    rets = _returns_from_snapshots(rows)
    if not rets:
        sharpe = sortino = vol = 0.0
    else:
        mean_ret = sum(rets) / len(rets)
        # 표본 수 시간 추정 — 평균 간격으로 연환산
        avg_interval_sec = (rows[-1][0] - rows[0][0]) / max(1, len(rows) - 1)
        n_per_year = 365 * 86400 / max(1, avg_interval_sec)
        # 변동성
        if len(rets) > 1:
            var = sum((r - mean_ret) ** 2 for r in rets) / (len(rets) - 1)
            std = math.sqrt(var)
        else:
            std = 0
        vol = std * math.sqrt(n_per_year) * 100
        # Sharpe — risk-free rate per snapshot
        rf_per = risk_free_rate / n_per_year
        sharpe = (mean_ret - rf_per) / std * math.sqrt(n_per_year) if std > 0 else 0
        # Sortino — downside-only deviation
        downside = [r for r in rets if r < 0]
        if downside:
            ds_var = sum(r ** 2 for r in downside) / len(downside)
            ds_std = math.sqrt(ds_var)
            sortino = (mean_ret - rf_per) / ds_std * math.sqrt(n_per_year) if ds_std > 0 else 0
        else:
            sortino = float('inf') if mean_ret > 0 else 0

    values = [r[1] for r in rows]
    max_dd, peak = _max_drawdown(values)
    recovery = total_ret / max_dd if max_dd > 0 else float('inf') if total_ret > 0 else 0

    return PortfolioMetrics(
        n_snapshots=len(rows),
        starting_value=starting,
        ending_value=ending,
        return_pct=total_ret * 100,
        annualized_return_pct=annualized_ret * 100,
        sharpe=sharpe,
        sortino=sortino if sortino != float('inf') else 999,
        max_drawdown_pct=max_dd * 100,
        recovery_factor=recovery if recovery != float('inf') else 999,
        volatility_annualized_pct=vol,
        days_observed=days,
    )


def compute_strategy_metrics(window_days: float = 7) -> dict[str, dict]:
    """전략별 P&L, 승률, 거래 수 — 자동 비활성화 판단용."""
    since_ts = time.time() - window_days * 86400
    conn = _conn()

    # 종료된 trade의 P&L (entry → exit 매칭은 어려우니 raw fill_price 차이 근사)
    # 실제 체계: closed_trades 테이블이 더 정확하지만 없으니 trades에서 strategy별 sum
    rows = conn.execute(
        """SELECT strategy,
                  COUNT(*) as n,
                  AVG(fill_price * size_shares) as avg_size,
                  SUM(fee_paid) as fees
           FROM trades
           WHERE timestamp >= ? AND strategy IS NOT NULL
           GROUP BY strategy""",
        (since_ts,),
    ).fetchall()
    conn.close()

    out = {}
    for strategy, n, avg_size, fees in rows:
        out[strategy] = {
            "n_trades": n,
            "avg_size_usd": avg_size or 0,
            "total_fees": fees or 0,
            "pnl_usd": None,  # closed_trades가 없으면 미정
            "should_disable": False,
        }
    return out


def get_metrics_summary(window_days: float = 30) -> dict:
    """대시보드용 한 번에 전체 요약."""
    pm = compute_portfolio_metrics(window_days)
    sm = compute_strategy_metrics(window_days=7)
    return {
        "portfolio": asdict(pm),
        "strategies": sm,
    }
