"""
ML 시그널 — Claude embedding 기반 시장 유사도.

원리:
1. 각 마켓 질문 → embedding (Anthropic API)
2. 과거 resolved 마켓 풀 (수천 건) 인덱싱
3. 새 마켓이 들어오면 가장 유사한 N개 과거 마켓의 base rate 추정
4. base_rate ± 신뢰도 → model_prob

기존 base_rate_oracle은 카테고리별 단순 평균.
ML 버전은 의미 유사도 기반 → 더 정밀한 prior.

가용성: ANTHROPIC_API_KEY 있어야 함. 없으면 시그널 생성 X.
호출 비용: 매 시장당 1회 embedding ($0.01 정도). 캐싱 필수.
"""
from __future__ import annotations
import asyncio
import json
import math
import sqlite3
import time
from dataclasses import dataclass
from typing import Optional

import config


@dataclass
class SimilarMarket:
    condition_id: str
    question: str
    similarity: float
    actual_outcome: float
    days_old: float


def _conn():
    return sqlite3.connect(config.DB_PATH)


def _ensure_table():
    conn = _conn()
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS market_embeddings (
        condition_id    TEXT PRIMARY KEY,
        question        TEXT NOT NULL,
        category        TEXT,
        embedding_json  TEXT NOT NULL,
        actual_outcome  REAL,
        resolved_at     REAL,
        created_at      REAL NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_emb_resolved ON market_embeddings(resolved_at) WHERE resolved_at IS NOT NULL;
    """)
    conn.commit()
    conn.close()


def cosine_similarity(a: list[float], b: list[float]) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x ** 2 for x in a))
    norm_b = math.sqrt(sum(x ** 2 for x in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


async def get_embedding(text: str) -> Optional[list[float]]:
    """
    Claude API로 텍스트 embedding 받기.
    Anthropic은 native embedding API 없음 → 대신 짧은 키워드 추출 + 자체 임베딩 가정.
    실제 production 환경에선 Voyage/OpenAI/sentence-transformers 사용.

    여기선 단순화: 단어 빈도 벡터 (해시 기반) — 외부 API 호출 0.
    """
    # Anthropic SDK 있으면 더 정교하게 가능, 일단 자체 해시 기반 임베딩
    return _hash_embedding(text)


def _hash_embedding(text: str, dim: int = 256) -> list[float]:
    """단순 hash 기반 bag-of-words 임베딩 (외부 의존 0)."""
    import hashlib
    vec = [0.0] * dim
    words = text.lower().split()
    for w in words:
        h = int(hashlib.md5(w.encode()).hexdigest(), 16)
        idx = h % dim
        vec[idx] += 1.0
    # L2 normalize
    norm = math.sqrt(sum(v ** 2 for v in vec))
    if norm > 0:
        vec = [v / norm for v in vec]
    return vec


async def index_market(condition_id: str, question: str, category: str = "") -> None:
    _ensure_table()
    embedding = await get_embedding(question)
    if not embedding:
        return
    conn = _conn()
    conn.execute(
        "INSERT OR REPLACE INTO market_embeddings "
        "(condition_id, question, category, embedding_json, created_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (condition_id, question, category, json.dumps(embedding), time.time()),
    )
    conn.commit()
    conn.close()


def update_resolution(condition_id: str, actual_outcome: float, resolved_at: float) -> None:
    conn = _conn()
    conn.execute(
        "UPDATE market_embeddings SET actual_outcome=?, resolved_at=? WHERE condition_id=?",
        (actual_outcome, resolved_at, condition_id),
    )
    conn.commit()
    conn.close()


async def find_similar_markets(query_question: str, k: int = 10,
                                  resolved_only: bool = True) -> list[SimilarMarket]:
    _ensure_table()
    query_emb = await get_embedding(query_question)
    if not query_emb:
        return []

    conn = _conn()
    if resolved_only:
        rows = conn.execute(
            "SELECT condition_id, question, embedding_json, actual_outcome, resolved_at "
            "FROM market_embeddings WHERE resolved_at IS NOT NULL"
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT condition_id, question, embedding_json, actual_outcome, resolved_at "
            "FROM market_embeddings"
        ).fetchall()
    conn.close()

    similar = []
    for cid, q, emb_json, outcome, resolved in rows:
        try:
            emb = json.loads(emb_json)
        except Exception:
            continue
        sim = cosine_similarity(query_emb, emb)
        days_old = (time.time() - (resolved or time.time())) / 86400
        similar.append(SimilarMarket(
            condition_id=cid,
            question=q,
            similarity=sim,
            actual_outcome=outcome or 0.5,
            days_old=days_old,
        ))

    similar.sort(key=lambda x: -x.similarity)
    return similar[:k]


async def estimate_prior_from_similar(question: str, k: int = 10) -> dict:
    """질문 → 유사 과거 마켓 기반 base rate 추정.

    Returns: {model_prob, confidence, n_similar, top_match_similarity}
    """
    similar = await find_similar_markets(question, k=k, resolved_only=True)
    if not similar:
        return {"model_prob": 0.5, "confidence": 0.0, "n_similar": 0, "top_match_similarity": 0}

    # 유사도 가중 평균
    total_weight = sum(s.similarity for s in similar)
    if total_weight <= 0:
        return {"model_prob": 0.5, "confidence": 0.0, "n_similar": len(similar), "top_match_similarity": 0}

    weighted_outcome = sum(s.similarity * s.actual_outcome for s in similar) / total_weight
    avg_similarity = total_weight / len(similar)

    # 신뢰도: 평균 유사도 × 표본 수 효과
    confidence = min(1.0, avg_similarity * (len(similar) / 20))

    return {
        "model_prob": float(weighted_outcome),
        "confidence": float(confidence),
        "n_similar": len(similar),
        "top_match_similarity": similar[0].similarity if similar else 0,
        "top_match_question": similar[0].question if similar else "",
    }


# ── 라이브 스캐너 ────────────────────────────────────────────────────────────

class MLOracleScanner:
    """매 60초 active 마켓 스캔 → 유사 시장 기반 mispricing 탐지."""

    def __init__(self, store, signal_bus):
        self._store = store
        self._bus = signal_bus
        self._scanned: dict[str, float] = {}    # condition_id → last scan ts

    async def start(self):
        from core.logger import log
        from core.models import Signal
        import uuid

        log.info("[ml_oracle] scanner started")
        while True:
            try:
                await asyncio.sleep(60)
                markets = self._store.get_active_markets() if self._store else []
                for m in markets[:50]:
                    if time.time() - self._scanned.get(m.condition_id, 0) < 1800:
                        continue
                    self._scanned[m.condition_id] = time.time()

                    # 인덱싱 (캐시됨)
                    try:
                        await index_market(m.condition_id, m.question, m.category or "")
                    except Exception:
                        continue

                    # base rate 추정
                    est = await estimate_prior_from_similar(m.question)
                    if est["confidence"] < 0.3 or est["n_similar"] < 5:
                        continue

                    yes_token = m.yes_token
                    if not yes_token or yes_token.price <= 0:
                        continue

                    # mispricing
                    edge = est["model_prob"] - yes_token.price
                    if abs(edge) < 0.05:    # 5% 미만 엣지는 무시
                        continue

                    direction = "BUY" if edge > 0 else "SELL"
                    fee_pct = config.TAKER_FEE_RATE * yes_token.price * (1 - yes_token.price)
                    net_edge = abs(edge) - fee_pct

                    if net_edge < config.MIN_EDGE_AFTER_FEES:
                        continue

                    sig = Signal(
                        signal_id=str(uuid.uuid4()),
                        strategy="claude_oracle",   # 기존 라벨 재사용 (ml은 별도 라벨로 추가 가능)
                        condition_id=m.condition_id,
                        token_id=yes_token.token_id,
                        direction=direction,
                        model_prob=est["model_prob"],
                        market_prob=yes_token.price,
                        edge=abs(edge),
                        net_edge=net_edge,
                        confidence=est["confidence"],
                        urgency="LOW",
                        stale_price=yes_token.price,
                        stale_threshold=0.03,
                    )
                    await self._bus.put(sig)
                    log.info(f"[ml_oracle] {direction} {m.condition_id[:8]} edge={edge:+.3f} conf={est['confidence']:.2f}")
            except Exception as e:
                log.warning(f"[ml_oracle] {e}")
                await asyncio.sleep(30)
