"""
Strategy versioning + 롤백.

전략 파라미터 버전 관리. 새 파라미터 deploy 후 성과가 안 나오면
known-good 버전으로 롤백.

사용:
  ver = save_version("closing_convergence", {"momentum_min": 0.005, "days_max": 3})
  → version_id = "closing_convergence_v1_2026-04-30_15:30"

  rollback_to("closing_convergence", "closing_convergence_v1_...")
  → runtime_state.json + audit_log + 텔레그램 알림

자동 롤백:
- 새 버전 deploy 후 24시간 동안 P&L < 이전 버전 - 2σ → 자동 롤백 후보
- 사람 확인 후 적용 (자동은 위험)
"""
from __future__ import annotations
import json
import sqlite3
import time
from dataclasses import dataclass, asdict
from typing import Optional

import config


def _ensure_table():
    conn = sqlite3.connect(config.DB_PATH)
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS strategy_versions (
        version_id      TEXT PRIMARY KEY,
        strategy        TEXT NOT NULL,
        version_num     INTEGER NOT NULL,
        params_json     TEXT NOT NULL,
        deployed_at     REAL NOT NULL,
        deployed_by     TEXT,
        rollback_target INTEGER DEFAULT 0,    -- 1 if this is a known-good fallback
        notes           TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_strat_ver ON strategy_versions(strategy, deployed_at DESC);
    """)
    conn.commit()
    conn.close()


def _get_next_version(strategy: str) -> int:
    conn = sqlite3.connect(config.DB_PATH)
    row = conn.execute(
        "SELECT MAX(version_num) FROM strategy_versions WHERE strategy=?",
        (strategy,)
    ).fetchone()
    conn.close()
    return (row[0] or 0) + 1


def save_version(strategy: str, params: dict, deployed_by: str = "auto",
                  rollback_target: bool = False, notes: str = "") -> str:
    _ensure_table()
    v = _get_next_version(strategy)
    ts = time.time()
    version_id = f"{strategy}_v{v}_{int(ts)}"
    conn = sqlite3.connect(config.DB_PATH)
    conn.execute(
        "INSERT INTO strategy_versions (version_id, strategy, version_num, params_json, "
        "                               deployed_at, deployed_by, rollback_target, notes) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (version_id, strategy, v, json.dumps(params), ts, deployed_by, 1 if rollback_target else 0, notes),
    )
    conn.commit()
    conn.close()
    return version_id


def get_version(version_id: str) -> Optional[dict]:
    _ensure_table()
    conn = sqlite3.connect(config.DB_PATH)
    row = conn.execute(
        "SELECT version_id, strategy, version_num, params_json, deployed_at, deployed_by, "
        "       rollback_target, notes FROM strategy_versions WHERE version_id=?",
        (version_id,)
    ).fetchone()
    conn.close()
    if not row:
        return None
    return {
        "version_id": row[0], "strategy": row[1], "version_num": row[2],
        "params": json.loads(row[3]), "deployed_at": row[4], "deployed_by": row[5],
        "rollback_target": bool(row[6]), "notes": row[7],
    }


def list_versions(strategy: str) -> list[dict]:
    _ensure_table()
    conn = sqlite3.connect(config.DB_PATH)
    rows = conn.execute(
        "SELECT version_id, version_num, deployed_at, deployed_by, rollback_target, notes "
        "FROM strategy_versions WHERE strategy=? ORDER BY deployed_at DESC",
        (strategy,)
    ).fetchall()
    conn.close()
    return [
        {
            "version_id": r[0], "version_num": r[1], "deployed_at": r[2],
            "deployed_by": r[3], "rollback_target": bool(r[4]), "notes": r[5],
        }
        for r in rows
    ]


def get_rollback_target(strategy: str) -> Optional[dict]:
    """known-good 버전 (가장 최근 rollback_target=1)."""
    _ensure_table()
    conn = sqlite3.connect(config.DB_PATH)
    row = conn.execute(
        "SELECT version_id, params_json FROM strategy_versions "
        "WHERE strategy=? AND rollback_target=1 ORDER BY deployed_at DESC LIMIT 1",
        (strategy,)
    ).fetchone()
    conn.close()
    if not row:
        return None
    return {"version_id": row[0], "params": json.loads(row[1])}


def rollback_to(version_id: str, actor: str = "admin") -> bool:
    """특정 버전으로 롤백.
    runtime_state.json의 strategy_params 갱신 + audit_log + 텔레그램 알림."""
    v = get_version(version_id)
    if not v:
        return False

    # runtime_state.json 갱신
    from pathlib import Path
    state_file = Path(__file__).resolve().parent.parent / "runtime_state.json"
    if state_file.exists():
        state = json.loads(state_file.read_text(encoding="utf-8"))
    else:
        state = {}
    state.setdefault("strategy_params", {})[v["strategy"]] = v["params"]
    state["last_modified"] = time.time()
    state["modified_by"] = f"rollback_to:{version_id}"
    state_file.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")

    # audit log
    try:
        from core import db
        db.insert_audit_log(actor, "strategy_rollback", None,
                            {"strategy": v["strategy"], "version": version_id}, "", "")
    except Exception:
        pass

    # 텔레그램 + WebSocket
    try:
        from notifications.telegram import notify
        notify("WARN", f"전략 롤백: {v['strategy']}", {
            "version": version_id,
            "params": v["params"],
        })
    except Exception:
        pass
    try:
        from dashboard.realtime import broadcast_event
        broadcast_event("strategy_rollback", {"strategy": v["strategy"], "version": version_id})
    except Exception:
        pass

    return True
