"""
전략 자동 비활성화.

7일 롤링 윈도우에서 음수 + 통계적 유의성 있는 전략은 자동 OFF.
runtime_state.json의 strategies_enabled를 갱신.

기준:
  - 거래 수 >= 10
  - 평균 P&L per trade < -X (default: -0.5% of avg_size)
  - 통계적 유의성: t-statistic > 2 (대략 95% 유의수준)

비활성화 시 텔레그램 CRITICAL 알림.
재활성화는 사람이 대시보드에서 수동으로.
"""
from __future__ import annotations
import json
import math
import sqlite3
import time
from pathlib import Path
from typing import Optional

import config


ROOT = Path(__file__).resolve().parent.parent
STATE_FILE = ROOT / "runtime_state.json"


def _load_state() -> dict:
    if not STATE_FILE.exists():
        return {}
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_state(state: dict) -> None:
    state["last_modified"] = time.time()
    STATE_FILE.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")


def _t_stat(values: list[float]) -> float:
    """일표본 t-검정 (mean / SE)."""
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    var = sum((v - mean) ** 2 for v in values) / (len(values) - 1)
    se = math.sqrt(var / len(values))
    return mean / se if se > 0 else 0.0


def evaluate_strategy(strategy: str, window_days: float = 7) -> dict:
    """전략별 통계 + disable 권고."""
    since_ts = time.time() - window_days * 86400
    conn = sqlite3.connect(config.DB_PATH)
    rows = conn.execute(
        "SELECT pnl FROM trades WHERE strategy=? AND timestamp >= ? AND pnl IS NOT NULL",
        (strategy, since_ts),
    ).fetchall()
    conn.close()

    pnls = [r[0] for r in rows]
    n = len(pnls)
    if n < 10:
        return {"strategy": strategy, "n": n, "should_disable": False, "reason": f"n={n}<10, 샘플 부족"}

    mean = sum(pnls) / n
    t = _t_stat(pnls)
    win_rate = sum(1 for p in pnls if p > 0) / n

    should_disable = (mean < 0) and (t < -2.0)

    return {
        "strategy": strategy,
        "n": n,
        "mean_pnl": mean,
        "t_statistic": t,
        "win_rate": win_rate,
        "should_disable": should_disable,
        "reason": (
            f"mean={mean:.4f}, t={t:.2f} → 손실 유의" if should_disable
            else f"mean={mean:.4f}, t={t:.2f} → OK"
        ),
    }


def auto_disable_loop_iteration() -> list[dict]:
    """한 번 실행. 비활성화된 전략 목록 반환."""
    state = _load_state()
    enabled_dict = state.get("strategies_enabled", {})
    disabled = []

    for strategy in list(enabled_dict.keys()):
        if not enabled_dict[strategy]:
            continue  # 이미 OFF
        result = evaluate_strategy(strategy)
        if result["should_disable"]:
            enabled_dict[strategy] = False
            disabled.append(result)

    if disabled:
        state["strategies_enabled"] = enabled_dict
        state["modified_by"] = "auto_disabler"
        _save_state(state)
        # 텔레그램 알림
        try:
            from notifications.telegram import notify
            for d in disabled:
                notify("CRITICAL", f"전략 자동 비활성화: {d['strategy']}", {
                    "n_trades": d["n"],
                    "mean_pnl": f"{d['mean_pnl']:.4f}",
                    "t_stat": f"{d['t_statistic']:.2f}",
                    "win_rate": f"{d['win_rate']*100:.1f}%",
                })
        except Exception:
            pass
        # audit log
        try:
            from core import db
            db.insert_audit_log(
                "auto_disabler", "strategy_auto_disable",
                None, {"disabled": [d["strategy"] for d in disabled]},
                "", ""
            )
        except Exception:
            pass

    return disabled


async def auto_disable_loop(interval_sec: int = 3600):
    """매시간 평가."""
    import asyncio
    from core.logger import log
    while True:
        try:
            await asyncio.sleep(interval_sec)
            disabled = auto_disable_loop_iteration()
            if disabled:
                log.warning(f"[auto_disable] disabled {len(disabled)} strategies: {[d['strategy'] for d in disabled]}")
        except Exception as e:
            log.warning(f"[auto_disable] error: {e}")
            await asyncio.sleep(60)
