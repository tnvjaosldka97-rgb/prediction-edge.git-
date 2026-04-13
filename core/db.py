"""
SQLite database layer.
Stores: trades, signals (with outcomes for calibration), price history, wallet stats.
"""
import asyncio
import sqlite3
import json
import time
from pathlib import Path
from typing import Optional
import config


_conn: Optional[sqlite3.Connection] = None
_lock = asyncio.Lock()


def get_conn() -> sqlite3.Connection:
    global _conn
    if _conn is None:
        _conn = sqlite3.connect(config.DB_PATH, check_same_thread=False)
        _conn.row_factory = sqlite3.Row
        _conn.execute("PRAGMA journal_mode=WAL")
        _conn.execute("PRAGMA synchronous=NORMAL")
        _run_migrations(_conn)
    return _conn


def _run_migrations(conn: sqlite3.Connection) -> None:
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS trades (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id    TEXT,
        condition_id TEXT NOT NULL,
        token_id    TEXT NOT NULL,
        side        TEXT NOT NULL,
        fill_price  REAL NOT NULL,
        size_shares REAL NOT NULL,
        fee_paid    REAL NOT NULL,
        strategy    TEXT,
        timestamp   REAL NOT NULL,
        pnl         REAL       -- filled in on close
    );

    CREATE TABLE IF NOT EXISTS signals (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        signal_id       TEXT UNIQUE,
        strategy        TEXT,
        condition_id    TEXT,
        token_id        TEXT,
        direction       TEXT,
        model_prob      REAL,
        market_prob     REAL,
        net_edge        REAL,
        confidence      REAL,
        created_at      REAL,
        resolved_at     REAL,
        actual_outcome  REAL,   -- 0 or 1, filled when market resolves
        was_correct     INTEGER -- 1/0/NULL
    );

    CREATE TABLE IF NOT EXISTS price_history (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        token_id    TEXT NOT NULL,
        price       REAL NOT NULL,
        timestamp   REAL NOT NULL
    );

    CREATE TABLE IF NOT EXISTS wallet_stats (
        address     TEXT PRIMARY KEY,
        stats_json  TEXT NOT NULL,
        updated_at  REAL NOT NULL
    );

    CREATE TABLE IF NOT EXISTS oracle_disputes (
        condition_id    TEXT PRIMARY KEY,
        dispute_risk    REAL,
        ambiguity_score REAL,
        updated_at      REAL
    );

    CREATE INDEX IF NOT EXISTS idx_price_history_token ON price_history(token_id, timestamp);
    CREATE INDEX IF NOT EXISTS idx_signals_strategy ON signals(strategy, created_at);
    CREATE INDEX IF NOT EXISTS idx_trades_condition ON trades(condition_id, timestamp);
    """)
    conn.commit()


def insert_trade(order_id: str, condition_id: str, token_id: str,
                 side: str, fill_price: float, size_shares: float,
                 fee_paid: float, strategy: str) -> int:
    conn = get_conn()
    cur = conn.execute(
        """INSERT INTO trades
           (order_id, condition_id, token_id, side, fill_price, size_shares,
            fee_paid, strategy, timestamp)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        (order_id, condition_id, token_id, side, fill_price, size_shares,
         fee_paid, strategy, time.time())
    )
    conn.commit()
    return cur.lastrowid


def insert_signal(signal) -> None:
    """Store signal for later calibration tracking."""
    conn = get_conn()
    conn.execute(
        """INSERT OR REPLACE INTO signals
           (signal_id, strategy, condition_id, token_id, direction,
            model_prob, market_prob, net_edge, confidence, created_at)
           VALUES (?,?,?,?,?,?,?,?,?,?)""",
        (signal.signal_id, signal.strategy, signal.condition_id,
         signal.token_id, signal.direction, signal.model_prob,
         signal.market_prob, signal.net_edge, signal.confidence,
         signal.created_at)
    )
    conn.commit()


def record_price(token_id: str, price: float) -> None:
    conn = get_conn()
    conn.execute(
        "INSERT INTO price_history (token_id, price, timestamp) VALUES (?,?,?)",
        (token_id, price, time.time())
    )
    conn.commit()


def get_calibration_stats(strategy: str) -> dict:
    """
    Returns calibration data for a strategy.
    Used by Kelly to determine how well-calibrated the model is.
    """
    conn = get_conn()
    rows = conn.execute(
        """SELECT model_prob, was_correct
           FROM signals
           WHERE strategy = ? AND was_correct IS NOT NULL
           ORDER BY created_at DESC LIMIT 1000""",
        (strategy,)
    ).fetchall()

    if not rows:
        return {"count": 0, "accuracy": 0.5, "calibration_error": 0.5}

    total = len(rows)
    correct = sum(1 for r in rows if r["was_correct"] == 1)
    accuracy = correct / total

    # Mean calibration error: |model_prob - empirical_hit_rate|
    # Simplified: bucket by probability and measure each bucket's accuracy
    calib_error = abs(accuracy - sum(r["model_prob"] for r in rows) / total)

    return {
        "count": total,
        "accuracy": accuracy,
        "calibration_error": calib_error,
    }


def upsert_wallet_stats(address: str, stats: dict) -> None:
    conn = get_conn()
    conn.execute(
        """INSERT OR REPLACE INTO wallet_stats (address, stats_json, updated_at)
           VALUES (?,?,?)""",
        (address, json.dumps(stats), time.time())
    )
    conn.commit()


def get_top_wallets(min_sharpe: float, limit: int) -> list[dict]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT address, stats_json FROM wallet_stats WHERE updated_at > ?",
        (time.time() - 86400 * 7,)  # only wallets updated in last 7 days
    ).fetchall()

    wallets = []
    for row in rows:
        stats = json.loads(row["stats_json"])
        if stats.get("sharpe_ratio", 0) >= min_sharpe:
            stats["address"] = row["address"]
            wallets.append(stats)

    wallets.sort(key=lambda x: x.get("sharpe_ratio", 0), reverse=True)
    return wallets[:limit]
