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
        # Ensure parent directory exists — prevents `unable to open database
        # file` crash when DB_PATH points at a volume mount that hasn't been
        # populated yet (e.g. Railway /data on first boot).
        db_path = Path(config.DB_PATH)
        parent = db_path.parent
        if parent and str(parent) not in ("", "."):
            try:
                parent.mkdir(parents=True, exist_ok=True)
            except Exception as e:
                # Can't create — fall back to cwd-relative path so the bot
                # at least starts. Log loudly so it's obvious.
                fallback = Path("./prediction_edge.db")
                print(
                    f"[db] WARNING: cannot create {parent} ({e}). "
                    f"Falling back to {fallback.resolve()}. "
                    f"Data will NOT persist across restarts!",
                    flush=True,
                )
                config.DB_PATH = str(fallback)
        _conn = sqlite3.connect(config.DB_PATH, check_same_thread=False)
        _conn.row_factory = sqlite3.Row
        _conn.execute("PRAGMA journal_mode=WAL")
        _conn.execute("PRAGMA synchronous=NORMAL")
        # Shrink WAL on first connect — recovery path for
        # "database or disk is full" without needing extra space.
        try:
            _conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except sqlite3.OperationalError:
            pass
        _run_migrations(_conn)
        # Prune stale tick data. Safe even under disk pressure because
        # prune_price_history checkpoints WAL before writing.
        try:
            n = prune_price_history(_conn)
            if n > 0:
                print(f"[db] Pruned {n:,} old price_history rows on startup")
        except Exception as _e:
            print(f"[db] price_history prune skipped: {_e}")
    return _conn


def prune_price_history(conn: sqlite3.Connection, ttl_hours: int | None = None) -> int:
    """
    Delete price_history rows older than TTL, then checkpoint WAL to reclaim disk.

    Designed to run safely even when SQLite is in "database or disk is full"
    state. Strategy:
      1. TRUNCATE-checkpoint the WAL first — shrinks .db-wal to 0 bytes without
         needing new disk space. This alone often frees enough room to continue.
      2. DELETE old rows in batches (small transactions = small journal growth).
      3. Final checkpoint to flush.

    Returns the number of rows deleted.
    """
    if ttl_hours is None:
        ttl_hours = int(getattr(config, "PRICE_HISTORY_TTL_HOURS", 48))
    cutoff = time.time() - ttl_hours * 3600
    deleted = 0

    # Step 1: shrink WAL before doing any writes. This is the critical move
    # for "disk full" recovery — checkpoint(TRUNCATE) frees the WAL file
    # back to the OS without requiring extra disk space.
    try:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    except sqlite3.OperationalError:
        pass

    # Step 2: delete in batches of 10k rows so each transaction stays tiny
    # and won't blow up the journal.
    try:
        while True:
            cur = conn.execute(
                "DELETE FROM price_history WHERE rowid IN "
                "(SELECT rowid FROM price_history WHERE timestamp < ? LIMIT 10000)",
                (cutoff,)
            )
            batch = cur.rowcount or 0
            conn.commit()
            deleted += batch
            if batch < 10000:
                break
            # Intermediate checkpoint so WAL doesn't grow mid-loop
            try:
                conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
            except sqlite3.OperationalError:
                pass
    except sqlite3.OperationalError:
        # Out of space mid-prune. Whatever we freed is better than nothing.
        pass

    # Step 3: final WAL truncate.
    try:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    except sqlite3.OperationalError:
        pass

    return deleted


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

    CREATE TABLE IF NOT EXISTS portfolio_snapshots (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp   REAL NOT NULL,
        total_value REAL NOT NULL,
        bankroll    REAL NOT NULL,
        unrealized  REAL NOT NULL,
        realized    REAL NOT NULL,
        positions   INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS cross_arb_matches (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        poly_condition_id TEXT NOT NULL,
        poly_question   TEXT,
        remote_slug     TEXT NOT NULL,
        remote_title    TEXT,
        remote_platform TEXT NOT NULL,      -- 'limitless' | 'kalshi'
        match_score     REAL NOT NULL,
        poly_price      REAL,
        remote_price    REAL,
        spread          REAL,
        signal_emitted  INTEGER DEFAULT 0,  -- 1 if signal was generated
        timestamp       REAL NOT NULL
    );

    CREATE TABLE IF NOT EXISTS cross_arb_prices (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        poly_condition_id TEXT NOT NULL,
        remote_slug     TEXT NOT NULL,
        remote_platform TEXT NOT NULL,
        poly_price      REAL NOT NULL,
        remote_bid      REAL NOT NULL,
        remote_ask      REAL NOT NULL,
        remote_mid      REAL NOT NULL,
        spread          REAL NOT NULL,
        timestamp       REAL NOT NULL
    );

    -- Shadow-Live virtual trades. Captures full execution profile of
    -- every signal fired in dry-run: realistic fill (orderbook walk),
    -- adverse selection drift, and eventual mark-to-market when the
    -- underlying market resolves. See shadow/virtual_executor.py.
    CREATE TABLE IF NOT EXISTS virtual_trades (
        id                  INTEGER PRIMARY KEY AUTOINCREMENT,
        signal_ts           REAL NOT NULL,
        fill_ts             REAL NOT NULL,
        condition_id        TEXT NOT NULL,
        token_id            TEXT NOT NULL,
        strategy            TEXT,
        category            TEXT,
        side                TEXT NOT NULL,
        size_usd            REAL NOT NULL,
        mid_at_signal       REAL,
        ask_at_signal       REAL,
        bid_at_signal       REAL,
        fill_price          REAL NOT NULL,
        slippage            REAL,              -- |fill - top_of_book|
        levels_touched      INTEGER,
        book_snapshot_json  TEXT,
        -- Adverse selection samples:
        mid_after_5s        REAL,
        mid_after_60s       REAL,
        mid_after_300s      REAL,
        -- Mark-to-market:
        unrealized_pnl      REAL,
        last_mark_ts        REAL,
        exit_price          REAL,
        realized_pnl        REAL,
        resolved_at         REAL
    );
    CREATE INDEX IF NOT EXISTS idx_virtual_trades_fill_ts ON virtual_trades(fill_ts);
    CREATE INDEX IF NOT EXISTS idx_virtual_trades_strategy ON virtual_trades(strategy, fill_ts);
    CREATE INDEX IF NOT EXISTS idx_virtual_trades_unresolved ON virtual_trades(resolved_at) WHERE resolved_at IS NULL;

    -- Friction traces — every LIVE order's full lifecycle for calibration.
    -- friction.calibrate.py reads this table to update model parameters.
    CREATE TABLE IF NOT EXISTS friction_traces (
        order_id            TEXT PRIMARY KEY,
        condition_id        TEXT,
        token_id            TEXT,
        strategy            TEXT,
        side                TEXT,
        order_type          TEXT,
        is_maker            INTEGER,
        submit_ts           REAL NOT NULL,
        ack_ts              REAL,
        fill_ts             REAL,
        requested_price     REAL,
        requested_size_usd  REAL,
        normalized_price    REAL,
        fill_price          REAL,
        fill_size_usd       REAL,
        fill_size_shares    REAL,
        fee_paid            REAL,
        rejection_reason    TEXT,                  -- NULL = filled
        submit_to_fill_ms   REAL,
        slippage_bps        REAL,
        is_partial          INTEGER,
        network_blip_during INTEGER DEFAULT 0,
        levels_consumed     INTEGER,
        book_snapshot_json  TEXT,                  -- best 5 levels at submit
        api_error_text      TEXT,                  -- raw error from CLOB if rejected
        retry_count         INTEGER DEFAULT 0
    );
    CREATE INDEX IF NOT EXISTS idx_friction_submit_ts ON friction_traces(submit_ts);
    CREATE INDEX IF NOT EXISTS idx_friction_strategy ON friction_traces(strategy, submit_ts);
    CREATE INDEX IF NOT EXISTS idx_friction_rejection ON friction_traces(rejection_reason) WHERE rejection_reason IS NOT NULL;

    -- Friction calibration snapshots — versioned model params.
    CREATE TABLE IF NOT EXISTS friction_calibration (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp       REAL NOT NULL,
        n_traces_used   INTEGER NOT NULL,
        params_json     TEXT NOT NULL,            -- friction.orchestrator.to_dict()
        notes           TEXT
    );

    -- Audit log — every dashboard control action (mode toggle, kill, etc.)
    CREATE TABLE IF NOT EXISTS audit_log (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp       REAL NOT NULL,
        actor           TEXT,                      -- 'admin' or username
        action          TEXT NOT NULL,             -- 'mode_change', 'emergency_stop', etc.
        before_state    TEXT,                      -- JSON
        after_state     TEXT,                      -- JSON
        ip_address      TEXT,
        user_agent      TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_audit_ts ON audit_log(timestamp);

    CREATE INDEX IF NOT EXISTS idx_snapshots_ts ON portfolio_snapshots(timestamp);
    CREATE INDEX IF NOT EXISTS idx_price_history_token ON price_history(token_id, timestamp);
    CREATE INDEX IF NOT EXISTS idx_signals_strategy ON signals(strategy, created_at);
    CREATE INDEX IF NOT EXISTS idx_trades_condition ON trades(condition_id, timestamp);
    CREATE INDEX IF NOT EXISTS idx_cross_matches_ts ON cross_arb_matches(timestamp);
    CREATE INDEX IF NOT EXISTS idx_cross_prices_ts ON cross_arb_prices(poly_condition_id, timestamp);
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


# ── 전략별 사전확률 (Bayesian priors) ────────────────────────────────────────
# 근거: Polymarket 실증 연구 및 전략 특성 기반
# prior_n: 사전 지식의 가중치 (트레이드 수 단위)
# higher prior_n = 실제 데이터가 더 많아야 prior 영향 감소
_STRATEGY_PRIORS: dict[str, dict] = {
    # fee_arb: p>0.95 시장은 매우 잘 calibrated. calib_error 낮게.
    "fee_arbitrage":       {"accuracy": 0.97,  "calib_error": 0.01, "n": 30},
    # oracle: 분쟁 낮은 마켓 수렴 — 높은 정확도, 약간 오차
    "oracle_convergence":  {"accuracy": 0.93,  "calib_error": 0.02, "n": 20},
    # closing_conv: 만료 임박 마켓 — 중간 정확도
    "closing_convergence": {"accuracy": 0.66,  "calib_error": 0.06, "n": 20},
    # order_flow: 고래 따라가기 — 노이즈 많음
    "order_flow":          {"accuracy": 0.55,  "calib_error": 0.08, "n": 10},
    # cross_platform: Kalshi 차익거래
    "cross_platform":      {"accuracy": 0.70,  "calib_error": 0.05, "n": 15},
    # limitless_arb: Limitless vs Polymarket — 동일 구조 CLOB 차익
    "limitless_arb":       {"accuracy": 0.68,  "calib_error": 0.06, "n": 15},
    # correlated_arb: 상관관계 기반
    "correlated_arb":      {"accuracy": 0.63,  "calib_error": 0.07, "n": 15},
    # claude_oracle: 백테스트 Brier=0.499 (랜덤 이하) → 사실상 비활성화 수준
    # calib_error 0.30 → shrinkage 60% → model_prob 거의 0.5로 수렴 → Kelly ≈ $0
    # 라이브 캘리브레이션 데이터가 이걸 개선할 때까지 관찰 모드
    "claude_oracle":       {"accuracy": 0.50,  "calib_error": 0.30, "n": 3},
    # base_rate: 백테스트 미실행, 보수적 유지
    "base_rate":           {"accuracy": 0.55,  "calib_error": 0.18, "n": 5},
    # exit_signal: 포지션 청산 — 높은 정확도 예상
    "exit_signal":         {"accuracy": 0.80,  "calib_error": 0.04, "n": 10},
}
_DEFAULT_PRIOR = {"accuracy": 0.60, "calib_error": 0.05, "n": 10}


def get_calibration_stats(strategy: str) -> dict:
    """
    전략의 캘리브레이션 데이터 반환.
    Kelly sizing에서 모델 신뢰도 측정에 사용.

    Bayesian blend: 실제 데이터가 쌓이기 전 전략별 사전확률(prior) 적용.
    - 실제 트레이드 0개: prior만 사용
    - 실제 트레이드 증가: prior 영향 감소, 실제 데이터 영향 증가
    - 100+ 트레이드: 거의 실제 데이터만 사용
    """
    conn = get_conn()
    rows = conn.execute(
        """SELECT model_prob, was_correct
           FROM signals
           WHERE strategy = ? AND was_correct IS NOT NULL
           ORDER BY created_at DESC LIMIT 1000""",
        (strategy,)
    ).fetchall()

    prior = _STRATEGY_PRIORS.get(strategy, _DEFAULT_PRIOR)
    n_prior = prior["n"]

    if not rows:
        # Cold-start: prior만 사용 (strategy 특성 반영)
        return {
            "count": 0,
            "accuracy": prior["accuracy"],
            "calibration_error": prior["calib_error"],
            "is_prior": True,
        }

    n_actual = len(rows)
    correct = sum(1 for r in rows if r["was_correct"] == 1)
    avg_model_prob = sum(r["model_prob"] for r in rows) / n_actual

    # Bayesian blend: weighted average of prior and actual
    # 실제 데이터가 많을수록 prior 영향 감소
    blend_weight = n_prior / (n_prior + n_actual)  # 0→1, 실제 많을수록 0에 가까워짐
    blended_accuracy = (
        prior["accuracy"] * blend_weight +
        (correct / n_actual) * (1 - blend_weight)
    )
    blended_calib_error = (
        prior["calib_error"] * blend_weight +
        abs((correct / n_actual) - avg_model_prob) * (1 - blend_weight)
    )
    # Cap at 0.3 — even a bad model shouldn't fully collapse bets
    blended_calib_error = min(blended_calib_error, 0.30)

    return {
        "count": n_actual,
        "accuracy": blended_accuracy,
        "calibration_error": blended_calib_error,
        "is_prior": blend_weight > 0.5,  # still mostly prior-driven
    }


def insert_cross_arb_match(
    poly_condition_id: str, poly_question: str,
    remote_slug: str, remote_title: str, remote_platform: str,
    match_score: float, poly_price: float, remote_price: float,
    spread: float, signal_emitted: bool,
) -> None:
    """Log every cross-platform match for audit trail."""
    conn = get_conn()
    conn.execute(
        """INSERT INTO cross_arb_matches
           (poly_condition_id, poly_question, remote_slug, remote_title,
            remote_platform, match_score, poly_price, remote_price,
            spread, signal_emitted, timestamp)
           VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
        (poly_condition_id, poly_question, remote_slug, remote_title,
         remote_platform, match_score, poly_price, remote_price,
         spread, 1 if signal_emitted else 0, time.time())
    )
    conn.commit()


def insert_cross_arb_price(
    poly_condition_id: str, remote_slug: str, remote_platform: str,
    poly_price: float, remote_bid: float, remote_ask: float,
    remote_mid: float, spread: float,
) -> None:
    """Log cross-platform price snapshot for convergence analysis."""
    conn = get_conn()
    conn.execute(
        """INSERT INTO cross_arb_prices
           (poly_condition_id, remote_slug, remote_platform,
            poly_price, remote_bid, remote_ask, remote_mid, spread, timestamp)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        (poly_condition_id, remote_slug, remote_platform,
         poly_price, remote_bid, remote_ask, remote_mid, spread, time.time())
    )
    conn.commit()


def get_cross_arb_convergence(poly_condition_id: str, hours: int = 24) -> list:
    """Get price convergence history for a cross-arb pair."""
    conn = get_conn()
    cutoff = time.time() - hours * 3600
    return conn.execute(
        """SELECT poly_price, remote_mid, spread, timestamp
           FROM cross_arb_prices
           WHERE poly_condition_id = ? AND timestamp > ?
           ORDER BY timestamp ASC""",
        (poly_condition_id, cutoff)
    ).fetchall()


def update_pnl_for_token(token_id: str, pnl: float) -> None:
    """Record realized PnL on the most recent open buy trade for a token."""
    conn = get_conn()
    row = conn.execute(
        "SELECT id FROM trades WHERE token_id = ? AND side = 'BUY' AND pnl IS NULL ORDER BY timestamp DESC LIMIT 1",
        (token_id,)
    ).fetchone()
    if row:
        conn.execute("UPDATE trades SET pnl = ? WHERE id = ?", (pnl, row["id"]))
        conn.commit()


def get_recent_trade_returns(limit: int = 50) -> list[float]:
    """Return recent closed-trade returns as fractions of cost basis."""
    conn = get_conn()
    rows = conn.execute(
        "SELECT pnl, fill_price, size_shares FROM trades WHERE pnl IS NOT NULL AND side = 'BUY' ORDER BY timestamp DESC LIMIT ?",
        (limit,)
    ).fetchall()
    result = []
    for r in rows:
        cost = r["fill_price"] * r["size_shares"]
        if cost > 0:
            result.append(r["pnl"] / cost)
    return result


def insert_snapshot(total_value: float, bankroll: float, unrealized: float, realized: float, positions: int) -> None:
    conn = get_conn()
    conn.execute(
        "INSERT INTO portfolio_snapshots (timestamp, total_value, bankroll, unrealized, realized, positions) VALUES (?,?,?,?,?,?)",
        (time.time(), total_value, bankroll, unrealized, realized, positions)
    )
    conn.commit()


def get_snapshots(limit: int = 2000) -> list:
    conn = get_conn()
    if limit and limit > 0:
        return conn.execute(
            "SELECT timestamp, total_value, bankroll, unrealized, realized, positions FROM portfolio_snapshots ORDER BY timestamp ASC LIMIT ?",
            (limit,)
        ).fetchall()
    return conn.execute(
        "SELECT timestamp, total_value, bankroll, unrealized, realized, positions FROM portfolio_snapshots ORDER BY timestamp ASC"
    ).fetchall()


def insert_friction_trace(trace: dict) -> None:
    """LIVE 주문의 전체 lifecycle을 friction_traces 테이블에 기록.

    trace dict 키 (필수/선택):
      order_id*, submit_ts*, condition_id, token_id, strategy, side, order_type,
      is_maker, ack_ts, fill_ts, requested_price, requested_size_usd,
      normalized_price, fill_price, fill_size_usd, fill_size_shares, fee_paid,
      rejection_reason, submit_to_fill_ms, slippage_bps, is_partial,
      network_blip_during, levels_consumed, book_snapshot_json, api_error_text, retry_count
    """
    conn = get_conn()
    cols = [
        "order_id", "condition_id", "token_id", "strategy", "side", "order_type",
        "is_maker", "submit_ts", "ack_ts", "fill_ts", "requested_price",
        "requested_size_usd", "normalized_price", "fill_price", "fill_size_usd",
        "fill_size_shares", "fee_paid", "rejection_reason", "submit_to_fill_ms",
        "slippage_bps", "is_partial", "network_blip_during", "levels_consumed",
        "book_snapshot_json", "api_error_text", "retry_count",
    ]
    placeholders = ",".join("?" * len(cols))
    values = tuple(trace.get(c) for c in cols)
    try:
        conn.execute(
            f"INSERT OR REPLACE INTO friction_traces ({','.join(cols)}) VALUES ({placeholders})",
            values,
        )
        conn.commit()
    except Exception as e:
        # trace 저장 실패가 거래 자체를 망가뜨리면 안 됨 — 로그만
        from core.logger import log
        log.warning(f"[friction_trace] insert failed: {e}")


def get_friction_traces(since_ts: float = 0, limit: int = 5000) -> list[dict]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM friction_traces WHERE submit_ts >= ? ORDER BY submit_ts DESC LIMIT ?",
        (since_ts, limit),
    ).fetchall()
    return [dict(r) for r in rows] if rows else []


def insert_friction_calibration(n_traces: int, params: dict, notes: str = "") -> None:
    conn = get_conn()
    conn.execute(
        "INSERT INTO friction_calibration (timestamp, n_traces_used, params_json, notes) "
        "VALUES (?, ?, ?, ?)",
        (time.time(), n_traces, json.dumps(params), notes),
    )
    conn.commit()


def get_latest_friction_calibration() -> dict | None:
    conn = get_conn()
    row = conn.execute(
        "SELECT timestamp, n_traces_used, params_json, notes "
        "FROM friction_calibration ORDER BY timestamp DESC LIMIT 1"
    ).fetchone()
    if not row:
        return None
    return {
        "timestamp": row[0],
        "n_traces_used": row[1],
        "params": json.loads(row[2]),
        "notes": row[3],
    }


def upsert_oracle_dispute(condition_id: str, dispute_risk: float,
                          ambiguity_score: float = 0.0) -> None:
    """oracle_disputes 테이블 upsert. 마켓별 분쟁 리스크 시계열."""
    conn = get_conn()
    conn.execute(
        """INSERT INTO oracle_disputes (condition_id, dispute_risk, ambiguity_score, updated_at)
           VALUES (?, ?, ?, ?)
           ON CONFLICT(condition_id) DO UPDATE SET
             dispute_risk = excluded.dispute_risk,
             ambiguity_score = excluded.ambiguity_score,
             updated_at = excluded.updated_at""",
        (condition_id, float(dispute_risk), float(ambiguity_score), time.time()),
    )
    conn.commit()


def insert_audit_log(actor: str, action: str, before: dict | None,
                     after: dict | None, ip: str = "", ua: str = "") -> None:
    conn = get_conn()
    conn.execute(
        "INSERT INTO audit_log (timestamp, actor, action, before_state, after_state, ip_address, user_agent) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            time.time(), actor, action,
            json.dumps(before) if before else None,
            json.dumps(after) if after else None,
            ip, ua,
        ),
    )
    conn.commit()


def get_audit_log(limit: int = 200) -> list[dict]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT timestamp, actor, action, before_state, after_state, ip_address "
        "FROM audit_log ORDER BY timestamp DESC LIMIT ?",
        (limit,),
    ).fetchall()
    return [
        {
            "timestamp": r[0], "actor": r[1], "action": r[2],
            "before": json.loads(r[3]) if r[3] else None,
            "after": json.loads(r[4]) if r[4] else None,
            "ip": r[5],
        }
        for r in rows
    ]


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
