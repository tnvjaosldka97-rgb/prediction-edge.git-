"""
News Reaction Lag — 뉴스 발생 후 시장 5~30분 시차 활용.

흐름:
1. RSS/Atom 피드 폴링 (CNN, Reuters, AP, Polymarket Research) — 60초 간격
2. 새 헤드라인 감지 → Claude API로 sentiment + 관련 시장 추출
3. 매칭된 마켓의 현재가 vs sentiment 함의 비교
4. 갭 > 3% → BUY/SELL 시그널 (urgency=IMMEDIATE)
5. stale_threshold 0.02 — 2% 이상 가격 움직이면 자동 취소

캐싱: 같은 헤드라인 24h 내 중복 처리 X.
비용: Claude API ~$0.01 per 헤드라인 × 일 50건 = ~$0.50/일.
"""
from __future__ import annotations
import asyncio
import hashlib
import json
import os
import sqlite3
import time
import uuid
from dataclasses import dataclass
from typing import Optional

import config


_RSS_FEEDS = [
    # 정치
    "https://www.reuters.com/rss/politicsNews",
    "https://feeds.npr.org/1014/rss.xml",
    # 경제
    "https://feeds.reuters.com/reuters/businessNews",
    # 일반
    "https://feeds.bbci.co.uk/news/world/rss.xml",
]


@dataclass
class NewsItem:
    headline_hash: str
    source: str
    headline: str
    url: str
    published_at: float


def _ensure_table():
    conn = sqlite3.connect(config.DB_PATH)
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS news_processed (
        headline_hash   TEXT PRIMARY KEY,
        source          TEXT,
        headline        TEXT,
        url             TEXT,
        published_at    REAL,
        processed_at    REAL,
        sentiment       REAL,         -- -1..+1
        related_markets TEXT,         -- JSON array of condition_ids
        signal_emitted  INTEGER DEFAULT 0
    );
    CREATE INDEX IF NOT EXISTS idx_news_published ON news_processed(published_at);
    """)
    conn.commit()
    conn.close()


def _hash_headline(headline: str) -> str:
    return hashlib.md5(headline.lower().strip().encode()).hexdigest()


async def fetch_rss(url: str, max_items: int = 20) -> list[NewsItem]:
    """RSS 피드에서 최신 N개 헤드라인."""
    try:
        import httpx
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(url, headers={"User-Agent": "Mozilla/5.0 prediction_edge/1.0"})
        if r.status_code != 200:
            return []
        # 단순 RSS 파싱 — 외부 의존 0
        text = r.text
        items = []
        # <item> 또는 <entry> 태그 추출
        import re
        item_pattern = re.compile(r"<(item|entry)>(.*?)</(item|entry)>", re.DOTALL | re.IGNORECASE)
        for match in item_pattern.findall(text)[:max_items]:
            content = match[1]
            title_m = re.search(r"<title[^>]*>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</title>", content, re.DOTALL)
            link_m = re.search(r"<link[^>]*>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</link>", content, re.DOTALL)
            pub_m = re.search(r"<(?:pubDate|published|updated)>(.*?)</(?:pubDate|published|updated)>", content)
            if not title_m:
                continue
            title = title_m.group(1).strip()
            link = link_m.group(1).strip() if link_m else ""
            published_at = time.time()
            if pub_m:
                try:
                    from email.utils import parsedate_to_datetime
                    dt = parsedate_to_datetime(pub_m.group(1))
                    published_at = dt.timestamp()
                except Exception:
                    pass
            items.append(NewsItem(
                headline_hash=_hash_headline(title),
                source=url,
                headline=title,
                url=link,
                published_at=published_at,
            ))
        return items
    except Exception:
        return []


def is_processed(headline_hash: str) -> bool:
    conn = sqlite3.connect(config.DB_PATH)
    row = conn.execute(
        "SELECT 1 FROM news_processed WHERE headline_hash=?",
        (headline_hash,)
    ).fetchone()
    conn.close()
    return row is not None


def save_processed(item: NewsItem, sentiment: float, related: list[str], signal_emitted: bool):
    conn = sqlite3.connect(config.DB_PATH)
    conn.execute(
        "INSERT OR REPLACE INTO news_processed "
        "(headline_hash, source, headline, url, published_at, processed_at, sentiment, related_markets, signal_emitted) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (item.headline_hash, item.source, item.headline, item.url, item.published_at,
         time.time(), sentiment, json.dumps(related), 1 if signal_emitted else 0),
    )
    conn.commit()
    conn.close()


async def analyze_with_claude(headline: str) -> dict:
    """Claude API로 sentiment + 관련 키워드 추출.

    Returns: {sentiment: float, keywords: list[str], confidence: float}
    """
    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    if not api_key:
        return _heuristic_analyze(headline)

    try:
        # anthropic SDK 우회 — 직접 HTTP (의존 최소화)
        import httpx
        prompt = f"""Analyze this news headline for prediction market signal.

Headline: {headline}

Return JSON only:
{{
  "sentiment": <number -1.0 to 1.0, where -1 is bearish, +1 is bullish for related markets>,
  "keywords": [<list of 1-5 keywords for matching to prediction markets>],
  "confidence": <number 0 to 1.0, how confident this affects markets>,
  "category": <"politics", "economy", "tech", "sports", "geopolitics", "crypto", or "other">
}}"""
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": api_key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": "claude-haiku-4-5-20251001",
                    "max_tokens": 200,
                    "messages": [{"role": "user", "content": prompt}],
                },
            )
        if r.status_code != 200:
            return _heuristic_analyze(headline)
        text = r.json()["content"][0]["text"]
        # JSON 추출
        import re
        m = re.search(r"\{.*\}", text, re.DOTALL)
        if m:
            return json.loads(m.group())
    except Exception:
        pass
    return _heuristic_analyze(headline)


def _heuristic_analyze(headline: str) -> dict:
    """API 없을 때 폴백 — 단순 키워드 기반."""
    h = headline.lower()
    bullish_words = ["surge", "rally", "win", "approve", "pass", "rise", "boost", "victory"]
    bearish_words = ["crash", "fall", "lose", "reject", "fail", "drop", "decline", "defeat"]
    pos = sum(1 for w in bullish_words if w in h)
    neg = sum(1 for w in bearish_words if w in h)
    sentiment = (pos - neg) / max(1, pos + neg)
    # 카테고리 매핑
    cat_keywords = {
        "politics": ["trump", "biden", "election", "congress", "senate", "vote", "polls"],
        "economy": ["fed", "inflation", "rate", "gdp", "unemployment", "tariff"],
        "crypto": ["bitcoin", "ethereum", "btc", "eth", "crypto"],
        "tech": ["ai", "openai", "google", "apple", "tech"],
    }
    category = "other"
    for cat, kws in cat_keywords.items():
        if any(kw in h for kw in kws):
            category = cat
            break
    keywords = [w for w in h.split() if len(w) > 4][:5]
    return {
        "sentiment": float(sentiment),
        "keywords": keywords,
        "confidence": 0.3 if sentiment != 0 else 0.0,
        "category": category,
    }


def find_matching_markets(store, keywords: list[str], category: str, max_results: int = 5) -> list:
    """키워드 + 카테고리로 매칭되는 active markets 찾기."""
    if not store:
        return []
    matches = []
    keyword_set = set(k.lower() for k in keywords)
    for m in store.get_active_markets():
        question_words = set(m.question.lower().split())
        # 카테고리 매칭
        cat_match = (m.category or "").lower() == category.lower()
        # 키워드 매칭
        kw_overlap = len(keyword_set & question_words)
        score = kw_overlap + (2 if cat_match else 0)
        if score >= 2:
            matches.append((score, m))
    matches.sort(key=lambda x: -x[0])
    return [m for _, m in matches[:max_results]]


class NewsLagScanner:
    """매 60초 RSS 폴링 + sentiment + 시그널 생성."""

    def __init__(self, store, signal_bus):
        self._store = store
        self._bus = signal_bus

    async def start(self):
        from core.logger import log
        from core.models import Signal
        _ensure_table()
        log.info("[news_lag] scanner started")

        while True:
            try:
                # 모든 피드 병렬 fetch
                results = await asyncio.gather(*[fetch_rss(u) for u in _RSS_FEEDS], return_exceptions=True)
                new_items = []
                for r in results:
                    if isinstance(r, list):
                        new_items.extend(r)

                # 중복 + 오래된 거 필터 (24h 이전 X)
                cutoff = time.time() - 86400
                fresh = [i for i in new_items
                         if i.published_at >= cutoff and not is_processed(i.headline_hash)]

                if fresh:
                    log.info(f"[news_lag] {len(fresh)} fresh headlines to process")

                for item in fresh[:10]:  # 한 cycle 당 최대 10개
                    analysis = await analyze_with_claude(item.headline)
                    if analysis.get("confidence", 0) < 0.3:
                        save_processed(item, analysis.get("sentiment", 0), [], False)
                        continue

                    matches = find_matching_markets(
                        self._store,
                        analysis.get("keywords", []),
                        analysis.get("category", "other"),
                    )
                    related_ids = [m.condition_id for m in matches]

                    signals_emitted = 0
                    for m in matches[:3]:
                        yes_token = m.yes_token
                        if not yes_token or yes_token.price <= 0.001 or yes_token.price >= 0.999:
                            continue

                        sentiment = analysis["sentiment"]
                        confidence = analysis["confidence"]
                        # sentiment → implied price shift
                        # +1 sentiment = 마켓 +5~10% YES 방향, -1 = -5~10%
                        implied_shift = sentiment * 0.05 * confidence
                        implied_prob = max(0.01, min(0.99, yes_token.price + implied_shift))

                        edge = implied_prob - yes_token.price
                        if abs(edge) < 0.03:    # 3% 미만 무시
                            continue

                        direction = "BUY" if edge > 0 else "SELL"
                        fee_pct = config.TAKER_FEE_RATE * yes_token.price * (1 - yes_token.price)
                        net_edge = abs(edge) - fee_pct
                        if net_edge < config.MIN_EDGE_AFTER_FEES:
                            continue

                        sig = Signal(
                            signal_id=str(uuid.uuid4()),
                            strategy="news_alpha",
                            condition_id=m.condition_id,
                            token_id=yes_token.token_id,
                            direction=direction,
                            model_prob=implied_prob,
                            market_prob=yes_token.price,
                            edge=abs(edge),
                            net_edge=net_edge,
                            confidence=confidence,
                            urgency="IMMEDIATE",
                            stale_price=yes_token.price,
                            stale_threshold=0.02,
                        )
                        await self._bus.put(sig)
                        signals_emitted += 1
                        log.info(
                            f"[news_lag] {direction} {m.condition_id[:8]} "
                            f"edge={edge:+.3f} conf={confidence:.2f} | {item.headline[:60]}"
                        )

                    save_processed(item, analysis["sentiment"], related_ids, signals_emitted > 0)

                await asyncio.sleep(60)
            except Exception as e:
                from core.logger import log
                log.warning(f"[news_lag] {e}")
                await asyncio.sleep(30)
