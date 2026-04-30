"""
Value-at-Risk + Conditional VaR + 스트레스 시나리오.

VaR(95%, 1d) = 95% 확률로 1일 손실이 X 이하
CVaR(95%, 1d) = VaR 초과 손실의 평균 (tail risk)

방법:
- Historical: 과거 returns 분포 직접 사용
- Parametric: 정규분포 가정 (꼬리 두꺼운 시장에 부정확)
- Monte Carlo: 시뮬레이션 (구현 복잡, 생략)

스트레스 시나리오:
- "Trump 당선" → 정치 카테고리 -30%
- "FED 깜짝 인상" → 경제 카테고리 -15%
- "오라클 분쟁" → dispute_risk 높은 마켓 -50%
- "거래소 다운" → 24h 입출금 정지 (개념적)
"""
from __future__ import annotations
import math
import sqlite3
import time
from dataclasses import dataclass, field
from typing import Optional

import config


@dataclass
class VaRResult:
    method: str
    confidence: float
    horizon_days: float
    var_pct: float            # 손실 % (음수)
    var_usd: float            # 손실 USD
    cvar_pct: float           # tail 평균 손실 %
    cvar_usd: float
    n_observations: int
    portfolio_value: float


@dataclass
class StressScenario:
    name: str
    description: str
    impact_per_category: dict[str, float]    # category → return shock (e.g. -0.3 = -30%)
    estimated_pnl_usd: float = 0.0


def _conn() -> sqlite3.Connection:
    return sqlite3.connect(config.DB_PATH)


def _get_returns(window_days: float = 30) -> list[float]:
    """portfolio_snapshots에서 일일 returns 추출."""
    since_ts = time.time() - window_days * 86400
    conn = _conn()
    rows = conn.execute(
        "SELECT timestamp, total_value FROM portfolio_snapshots "
        "WHERE timestamp >= ? ORDER BY timestamp ASC",
        (since_ts,)
    ).fetchall()
    conn.close()

    if len(rows) < 2:
        return []

    # 일별 그룹핑 (close-to-close)
    daily_close = {}
    for ts, val in rows:
        day = int(ts // 86400)
        daily_close[day] = val  # 마지막 값 = 그날 close

    days = sorted(daily_close.keys())
    returns = []
    for i in range(1, len(days)):
        prev = daily_close[days[i - 1]]
        curr = daily_close[days[i]]
        if prev > 0:
            returns.append(curr / prev - 1)
    return returns


def historical_var(
    confidence: float = 0.95,
    horizon_days: float = 1,
    window_days: float = 30,
    portfolio_value: float = 100,
) -> VaRResult:
    returns = _get_returns(window_days)
    if not returns:
        return VaRResult("historical", confidence, horizon_days, 0, 0, 0, 0, 0, portfolio_value)

    # 시간 horizon 조정 (sqrt(t) 룰)
    scale = math.sqrt(horizon_days)
    scaled = [r * scale for r in returns]

    sorted_r = sorted(scaled)
    var_idx = int(len(sorted_r) * (1 - confidence))
    var_pct = sorted_r[var_idx] if var_idx < len(sorted_r) else sorted_r[0]
    tail = sorted_r[: max(1, var_idx + 1)]
    cvar_pct = sum(tail) / len(tail) if tail else var_pct

    return VaRResult(
        method="historical",
        confidence=confidence,
        horizon_days=horizon_days,
        var_pct=var_pct,
        var_usd=var_pct * portfolio_value,
        cvar_pct=cvar_pct,
        cvar_usd=cvar_pct * portfolio_value,
        n_observations=len(returns),
        portfolio_value=portfolio_value,
    )


def parametric_var(
    confidence: float = 0.95,
    horizon_days: float = 1,
    window_days: float = 30,
    portfolio_value: float = 100,
) -> VaRResult:
    """정규분포 가정 — 꼬리 두꺼운 시장에 underestimate 가능."""
    returns = _get_returns(window_days)
    if len(returns) < 2:
        return VaRResult("parametric", confidence, horizon_days, 0, 0, 0, 0, 0, portfolio_value)

    mean = sum(returns) / len(returns)
    var = sum((r - mean) ** 2 for r in returns) / (len(returns) - 1)
    std = math.sqrt(var) * math.sqrt(horizon_days)

    # 정규 inverse CDF for 1-confidence
    # 95% → z = 1.645, 99% → z = 2.326
    z_table = {0.90: 1.282, 0.95: 1.645, 0.975: 1.960, 0.99: 2.326, 0.995: 2.576}
    z = z_table.get(round(confidence, 3), 1.645)

    var_pct = mean - z * std
    # parametric CVaR: -std * phi(z)/(1-c) + mean
    phi_z = math.exp(-z ** 2 / 2) / math.sqrt(2 * math.pi)
    cvar_pct = mean - std * phi_z / (1 - confidence)

    return VaRResult(
        method="parametric",
        confidence=confidence,
        horizon_days=horizon_days,
        var_pct=var_pct,
        var_usd=var_pct * portfolio_value,
        cvar_pct=cvar_pct,
        cvar_usd=cvar_pct * portfolio_value,
        n_observations=len(returns),
        portfolio_value=portfolio_value,
    )


# ── Stress scenarios ─────────────────────────────────────────────────────────

DEFAULT_SCENARIOS = [
    StressScenario(
        name="political_shock",
        description="대선·탄핵 등 정치 시장 급변동",
        impact_per_category={"politics": -0.30, "geopolitics": -0.25},
    ),
    StressScenario(
        name="fed_surprise",
        description="FED 깜짝 인상/인하",
        impact_per_category={"economy": -0.15, "finance": -0.20},
    ),
    StressScenario(
        name="oracle_dispute",
        description="UMA 대규모 dispute",
        impact_per_category={"unknown": -0.50},  # 우리 카테고리 분류 실패한 마켓
    ),
    StressScenario(
        name="crypto_crash",
        description="BTC/ETH 30% 급락",
        impact_per_category={"crypto": -0.40, "tech": -0.20},
    ),
    StressScenario(
        name="exchange_outage",
        description="Polymarket 24h 입출금 정지",
        impact_per_category={},  # 모든 카테고리 -10% (유동성 freeze)
    ),
]


def evaluate_stress_scenario(scenario: StressScenario, portfolio_state) -> StressScenario:
    """현 포트폴리오에 시나리오 충격 적용 → 예상 손실."""
    if not portfolio_state or not portfolio_state.positions:
        return scenario

    impact = 0.0
    for token_id, pos in portfolio_state.positions.items():
        cat = (pos.category or "unknown").lower()
        shock = scenario.impact_per_category.get(cat, -0.10 if not scenario.impact_per_category else 0)
        position_value = pos.size_shares * pos.current_price
        impact += position_value * shock

    return StressScenario(
        name=scenario.name,
        description=scenario.description,
        impact_per_category=scenario.impact_per_category,
        estimated_pnl_usd=impact,
    )


def run_all_stress_tests(portfolio_state) -> list[dict]:
    """모든 시나리오 평가 → dict 리스트."""
    results = []
    for sc in DEFAULT_SCENARIOS:
        evaluated = evaluate_stress_scenario(sc, portfolio_state)
        results.append({
            "name": evaluated.name,
            "description": evaluated.description,
            "estimated_pnl_usd": evaluated.estimated_pnl_usd,
            "impact_per_category": evaluated.impact_per_category,
        })
    return sorted(results, key=lambda x: x["estimated_pnl_usd"])  # 가장 큰 손실 위
