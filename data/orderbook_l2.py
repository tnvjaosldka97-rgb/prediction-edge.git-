"""
L2 호가 풀 reconstruction + 시계열 저장.

기존 store는 best bid/ask + assumed depth만.
이 모듈은 Polymarket WebSocket의 book_diff 메시지를 받아 풀 L2 호가창 유지 +
주기적으로 DB에 스냅샷 저장 → 백테스트·friction calibration 정확도 향상.

데이터 흐름:
  WS book_snapshot 수신 → L2OrderBook 초기화
  WS book_diff 수신     → bid/ask 갱신 (price level + size)
  매 30초              → DB orderbook_l2_snapshots 테이블에 직렬화 저장
  매 시간              → 6시간 이전 데이터 압축 (best 5단계만 유지)
"""
from __future__ import annotations
import json
import sqlite3
import time
from dataclasses import dataclass, field
from typing import Optional

import config


@dataclass
class L2Level:
    price: float
    size: float


@dataclass
class L2OrderBook:
    token_id: str
    bids: list[L2Level] = field(default_factory=list)   # 가격 내림차순
    asks: list[L2Level] = field(default_factory=list)   # 가격 오름차순
    last_update_ts: float = field(default_factory=time.time)

    def apply_snapshot(self, bids: list[tuple[float, float]], asks: list[tuple[float, float]]) -> None:
        self.bids = sorted([L2Level(p, s) for p, s in bids if s > 0], key=lambda x: -x.price)
        self.asks = sorted([L2Level(p, s) for p, s in asks if s > 0], key=lambda x: x.price)
        self.last_update_ts = time.time()

    def apply_diff(self, side: str, price: float, size: float) -> None:
        levels = self.bids if side == "BUY" else self.asks
        # 기존 레벨 제거
        levels[:] = [l for l in levels if abs(l.price - price) > 1e-9]
        # size > 0이면 추가
        if size > 0:
            levels.append(L2Level(price, size))
            levels.sort(key=lambda x: -x.price if side == "BUY" else x.price)
        self.last_update_ts = time.time()

    @property
    def best_bid(self) -> float:
        return self.bids[0].price if self.bids else 0.0

    @property
    def best_ask(self) -> float:
        return self.asks[0].price if self.asks else 1.0

    @property
    def mid(self) -> float:
        return (self.best_bid + self.best_ask) / 2 if self.bids and self.asks else 0.0

    def depth_to_price(self, side: str, target_price: float) -> float:
        """target_price 까지 도달하기 위한 누적 USD."""
        levels = self.asks if side == "BUY" else self.bids
        cumul_usd = 0.0
        for l in levels:
            if (side == "BUY" and l.price > target_price) or (side == "SELL" and l.price < target_price):
                break
            cumul_usd += l.price * l.size
        return cumul_usd

    def to_json(self, max_levels: int = 20) -> str:
        return json.dumps({
            "ts": self.last_update_ts,
            "bids": [(l.price, l.size) for l in self.bids[:max_levels]],
            "asks": [(l.price, l.size) for l in self.asks[:max_levels]],
        })


# ── DB 스냅샷 저장 ───────────────────────────────────────────────────────────

def _ensure_table():
    conn = sqlite3.connect(config.DB_PATH)
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS orderbook_l2_snapshots (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        token_id        TEXT NOT NULL,
        timestamp       REAL NOT NULL,
        n_bid_levels    INTEGER,
        n_ask_levels    INTEGER,
        best_bid        REAL,
        best_ask        REAL,
        bid_depth_5     REAL,           -- best 5단계 누적 USD
        ask_depth_5     REAL,
        snapshot_json   TEXT NOT NULL   -- 전체 호가창 (top 20단계)
    );
    CREATE INDEX IF NOT EXISTS idx_l2_token_ts ON orderbook_l2_snapshots(token_id, timestamp);
    """)
    conn.commit()
    conn.close()


def save_snapshot(book: L2OrderBook) -> None:
    _ensure_table()
    conn = sqlite3.connect(config.DB_PATH)
    bid_depth = sum(l.price * l.size for l in book.bids[:5])
    ask_depth = sum(l.price * l.size for l in book.asks[:5])
    conn.execute(
        "INSERT INTO orderbook_l2_snapshots "
        "(token_id, timestamp, n_bid_levels, n_ask_levels, best_bid, best_ask, "
        " bid_depth_5, ask_depth_5, snapshot_json) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (book.token_id, book.last_update_ts, len(book.bids), len(book.asks),
         book.best_bid, book.best_ask, bid_depth, ask_depth, book.to_json()),
    )
    conn.commit()
    conn.close()


def get_snapshot_at(token_id: str, ts: float) -> Optional[L2OrderBook]:
    """ts 시점 또는 직전 호가창 복원. 백테스트에서 슬리피지 정확하게 계산."""
    conn = sqlite3.connect(config.DB_PATH)
    row = conn.execute(
        "SELECT snapshot_json FROM orderbook_l2_snapshots "
        "WHERE token_id=? AND timestamp <= ? ORDER BY timestamp DESC LIMIT 1",
        (token_id, ts),
    ).fetchone()
    conn.close()
    if not row:
        return None
    data = json.loads(row[0])
    book = L2OrderBook(token_id=token_id)
    book.apply_snapshot(data["bids"], data["asks"])
    book.last_update_ts = data["ts"]
    return book


# ── 메모리 store ─────────────────────────────────────────────────────────────

class L2Store:
    """token_id → L2OrderBook 메모리 store. WS handler가 갱신."""

    def __init__(self):
        self._books: dict[str, L2OrderBook] = {}

    def get_or_create(self, token_id: str) -> L2OrderBook:
        if token_id not in self._books:
            self._books[token_id] = L2OrderBook(token_id=token_id)
        return self._books[token_id]

    def get(self, token_id: str) -> Optional[L2OrderBook]:
        return self._books.get(token_id)

    async def periodic_save(self, interval_sec: int = 30):
        """모든 active book 정기 저장."""
        import asyncio
        from core.logger import log
        while True:
            try:
                await asyncio.sleep(interval_sec)
                for book in list(self._books.values()):
                    if time.time() - book.last_update_ts < interval_sec * 2:
                        save_snapshot(book)
            except Exception as e:
                log.warning(f"[L2 save] {e}")
                await asyncio.sleep(60)
