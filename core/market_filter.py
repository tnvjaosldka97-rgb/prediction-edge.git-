"""
Market Filter — 가성비 좋은 시장만 추적.

Polymarket active markets 2000+개. 대부분은 우리 전략에 안 맞음.
필터링으로 ~50~200개로 줄여서:
- WS 구독 부하 감소
- 시그널 노이즈 감소
- API rate limit 여유

선정 기준:
1. volume_24h > $1000 (얇은 시장 제외)
2. liquidity > $500
3. days_to_resolution > 0.5 (거의 결제됨)
4. category 명확 (unknown 제외, 단 일부 도메인은 OK)
5. dispute_risk < 0.3 (분쟁 위험 낮음)
6. tokens 가격이 0.05~0.95 (극단치 제외)

각 시장에 score 매겨서 top N 선택.
"""
from __future__ import annotations
import time
from dataclasses import dataclass
from typing import Optional


# 우리 전략 강한 카테고리 (closing_convergence·dispute_premium 효과)
PREFERRED_CATEGORIES = {"politics", "economy", "geopolitics", "crypto", "sports", "tech"}
EXCLUDED_CATEGORIES = {"entertainment"}


@dataclass
class MarketScore:
    condition_id: str
    score: float
    rationale: list[str]


def score_market(market) -> MarketScore:
    """0~100 점수. 50+ = 추적할 가치."""
    score = 50.0
    reasons = []

    # 거래량
    vol = market.volume_24h or 0
    if vol > 100_000:
        score += 15; reasons.append(f"vol=${vol:.0f} (>$100k)")
    elif vol > 10_000:
        score += 8; reasons.append(f"vol=${vol:.0f}")
    elif vol < 1000:
        score -= 20; reasons.append(f"vol=${vol:.0f} too thin")

    # 유동성
    liq = market.liquidity or 0
    if liq > 5000:
        score += 10
    elif liq < 500:
        score -= 15; reasons.append(f"liq=${liq:.0f} thin")

    # 만기까지 시간
    days = market.days_to_resolution
    if 0.5 <= days <= 7:
        score += 10; reasons.append(f"days={days:.1f}")
    elif days > 30:
        score -= 5    # 너무 멀면 알파 적음
    elif days < 0.1:
        score -= 30; reasons.append("expiring")

    # 카테고리
    cat = (market.category or "").lower()
    if cat in PREFERRED_CATEGORIES:
        score += 8
    elif cat in EXCLUDED_CATEGORIES:
        score -= 15; reasons.append(f"category={cat}")
    elif cat == "" or cat == "unknown":
        score -= 5    # 카테고리 모호

    # 분쟁 위험
    dispute = market.dispute_risk or 0
    if dispute > 0.3:
        score -= 20; reasons.append(f"dispute={dispute:.2f}")
    elif dispute < 0.05:
        score += 5

    # 가격 — 극단치 제외 (1.0이나 0.0 가까우면 거래 의미 X)
    yes_token = market.yes_token
    if yes_token:
        p = yes_token.price
        if p < 0.02 or p > 0.98:
            score -= 25; reasons.append(f"price={p:.3f} extreme")

    return MarketScore(
        condition_id=market.condition_id,
        score=score,
        rationale=reasons,
    )


def filter_markets(markets: list, top_n: int = 100, min_score: float = 50) -> list:
    """추적할 시장 선택. score 내림차순 top_n."""
    scored = [(score_market(m), m) for m in markets]
    scored.sort(key=lambda x: -x[0].score)
    selected = [m for s, m in scored if s.score >= min_score][:top_n]
    return selected


def get_filter_stats(markets: list) -> dict:
    """필터링 통계 — 대시보드 표시용."""
    scored = [score_market(m) for m in markets]
    if not scored:
        return {"total": 0, "median": 0, "above_50": 0}
    scores = [s.score for s in scored]
    return {
        "total": len(scored),
        "median_score": sorted(scores)[len(scores) // 2],
        "above_50": sum(1 for s in scores if s >= 50),
        "above_70": sum(1 for s in scores if s >= 70),
        "above_80": sum(1 for s in scores if s >= 80),
        "top_5": [
            {"condition_id": s.condition_id[:8], "score": s.score, "reasons": s.rationale[:3]}
            for s in sorted(scored, key=lambda x: -x.score)[:5]
        ],
    }
