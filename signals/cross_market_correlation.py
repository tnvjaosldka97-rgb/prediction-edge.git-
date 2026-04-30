"""
Cross-Market Correlation Arbitrage.

원리:
- "Trump 당선 P(A)" + "Trump 관세 부과 P(B|A)" 같은 conditional 관계
- P(B) ≤ P(A) × P(B|A) 위반 시 → arb 기회
- 또는 동일 이벤트 다른 표현 (예: "Trump 2024 당선" vs "Trump 47대 대통령") 가격 갭

구현:
1. condition_id 그래프 — 키워드/엔티티 공유 마켓 클러스터링
2. 클러스터 내 implied price 비교
3. 일관성 위반 시그널 → 모두 매수 또는 매도 (조건부 hedge)
"""
from __future__ import annotations
import asyncio
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass

import config


@dataclass
class MarketCluster:
    cluster_id: str
    member_condition_ids: list[str]
    keywords: set[str]
    primary_entity: str    # 핵심 엔티티 (예: "Trump", "Bitcoin")


def extract_entities(question: str) -> set[str]:
    """단순 entity 추출 — 대문자로 시작하는 단어 + known entities."""
    import re
    proper_nouns = set(re.findall(r'\b[A-Z][a-z]{2,}\b', question))
    # 알려진 entity 추가
    lower = question.lower()
    known = {
        "trump": "Trump", "biden": "Biden", "harris": "Harris",
        "bitcoin": "Bitcoin", "btc": "Bitcoin", "ethereum": "Ethereum",
        "ai": "AI", "openai": "OpenAI", "google": "Google",
        "fed": "FED", "iran": "Iran", "ukraine": "Ukraine",
        "fomc": "FOMC", "election": "Election",
    }
    for kw, entity in known.items():
        if kw in lower:
            proper_nouns.add(entity)
    return proper_nouns


def build_clusters(markets: list) -> list[MarketCluster]:
    """active markets을 entity 공유 기반으로 클러스터링."""
    entity_to_markets: dict[str, list] = defaultdict(list)
    for m in markets:
        entities = extract_entities(m.question)
        for e in entities:
            entity_to_markets[e].append(m)

    clusters = []
    for entity, m_list in entity_to_markets.items():
        if len(m_list) < 2:
            continue
        # 단일 엔티티만 공유하면 약한 클러스터 — 추가 키워드 매칭으로 강화
        if len(m_list) >= 3:
            clusters.append(MarketCluster(
                cluster_id=f"entity_{entity}",
                member_condition_ids=[m.condition_id for m in m_list],
                keywords={entity},
                primary_entity=entity,
            ))
    return clusters


def detect_inconsistency(cluster: MarketCluster, store) -> list[dict]:
    """클러스터 내 implied price 일관성 검사."""
    if not store:
        return []

    members = []
    for cid in cluster.member_condition_ids:
        m = store.get_market(cid)
        if m and m.yes_token:
            members.append((m, m.yes_token.price))

    if len(members) < 2:
        return []

    # 가격 분산이 매우 크면 일관성 위반 가능
    prices = [p for _, p in members]
    avg_price = sum(prices) / len(prices)
    max_dev = max(abs(p - avg_price) for p in prices)

    inconsistencies = []
    if max_dev > 0.15:    # 15% 이상 분산
        # 가장 outlier인 마켓 → 평균으로 회귀할 가능성
        for m, p in members:
            dev = p - avg_price
            if abs(dev) > 0.10:
                # 평균보다 비싸면 SELL, 싸면 BUY
                direction = "SELL" if dev > 0 else "BUY"
                implied = avg_price
                edge = abs(dev) * 0.5    # 회귀 부분만 가정
                inconsistencies.append({
                    "market": m,
                    "current_price": p,
                    "cluster_avg": avg_price,
                    "deviation": dev,
                    "direction": direction,
                    "implied_prob": implied,
                    "edge": edge,
                })
    return inconsistencies


class CrossMarketCorrelationScanner:
    def __init__(self, store, signal_bus):
        self._store = store
        self._bus = signal_bus
        self._last_cluster_build = 0.0
        self._clusters: list[MarketCluster] = []
        self._cooldown: dict[str, float] = {}

    async def start(self):
        from core.logger import log
        from core.models import Signal
        log.info("[cross_market_corr] scanner started")

        while True:
            try:
                await asyncio.sleep(180)    # 3분 간격
                if not self._store:
                    continue

                # 클러스터 6시간마다 재구성
                if time.time() - self._last_cluster_build > 21600:
                    markets = self._store.get_active_markets()
                    self._clusters = build_clusters(markets)
                    self._last_cluster_build = time.time()
                    log.info(f"[cross_market_corr] rebuilt {len(self._clusters)} clusters")

                emitted = 0
                for cluster in self._clusters:
                    inconsistencies = detect_inconsistency(cluster, self._store)
                    for inc in inconsistencies:
                        m = inc["market"]
                        if time.time() - self._cooldown.get(m.condition_id, 0) < 1800:
                            continue

                        yes_token = m.yes_token
                        if not yes_token:
                            continue

                        fee_pct = config.TAKER_FEE_RATE * yes_token.price * (1 - yes_token.price)
                        net_edge = inc["edge"] - fee_pct
                        if net_edge < config.MIN_EDGE_AFTER_FEES:
                            continue

                        sig = Signal(
                            signal_id=str(uuid.uuid4()),
                            strategy="correlated_arb",
                            condition_id=m.condition_id,
                            token_id=yes_token.token_id,
                            direction=inc["direction"],
                            model_prob=inc["implied_prob"],
                            market_prob=yes_token.price,
                            edge=inc["edge"],
                            net_edge=net_edge,
                            confidence=0.50,    # 회귀 가정 신뢰도 낮음
                            urgency="LOW",
                            stale_price=yes_token.price,
                            stale_threshold=0.03,
                        )
                        await self._bus.put(sig)
                        self._cooldown[m.condition_id] = time.time()
                        emitted += 1
                        log.info(
                            f"[cross_market_corr] {inc['direction']} {m.condition_id[:8]} "
                            f"cluster={cluster.primary_entity} dev={inc['deviation']:+.3f}"
                        )

                if emitted:
                    log.info(f"[cross_market_corr] {emitted} signals this cycle")

            except Exception as e:
                from core.logger import log
                log.warning(f"[cross_market_corr] {e}")
                await asyncio.sleep(60)
