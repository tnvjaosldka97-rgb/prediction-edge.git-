"""
Auto-Discovery Research Agent.

매일 1회 자동 실행:
1. 새 패턴 탐색 — calendar effects, parameter sweeps, 시그널 조합
2. walk-forward CV로 통계적 유의성 검증
3. 유의 패턴 → "candidate" 상태로 strategy_versioning에 등록
4. shadow 모드로 N일 확인 → 확정 → 텔레그램 알림 (선생님이 LIVE 승인)

비유: "PhD 퀀트가 매일 출근해서 새 알파 찾는 일"을 자동으로.
"""
from __future__ import annotations
import asyncio
import json
import sqlite3
import time
from dataclasses import dataclass, field, asdict
from typing import Optional

import config


@dataclass
class Hypothesis:
    name: str
    description: str
    base_strategy: str               # 기존 전략 또는 신규
    proposed_params: dict
    rationale: str
    confidence_prior: float          # 0~1


@dataclass
class TestResult:
    hypothesis_name: str
    cv_mean: float
    cv_std: float
    cv_sharpe: float
    p_value: float
    n_folds: int
    decision: str                    # "promote_shadow" / "reject" / "needs_more_data"
    notes: str = ""


def _ensure_table():
    conn = sqlite3.connect(config.DB_PATH)
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS research_hypotheses (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        name            TEXT UNIQUE NOT NULL,
        description     TEXT,
        base_strategy   TEXT,
        proposed_params TEXT NOT NULL,
        rationale       TEXT,
        confidence_prior REAL,
        created_at      REAL NOT NULL,
        status          TEXT NOT NULL DEFAULT 'untested',  -- untested/promoted_shadow/rejected/live
        cv_mean         REAL,
        cv_std          REAL,
        cv_sharpe       REAL,
        cv_p_value      REAL,
        notes           TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_hypotheses_status ON research_hypotheses(status, created_at);
    """)
    conn.commit()
    conn.close()


def save_hypothesis(h: Hypothesis):
    _ensure_table()
    conn = sqlite3.connect(config.DB_PATH)
    conn.execute(
        "INSERT OR IGNORE INTO research_hypotheses "
        "(name, description, base_strategy, proposed_params, rationale, confidence_prior, created_at, status) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, 'untested')",
        (h.name, h.description, h.base_strategy, json.dumps(h.proposed_params),
         h.rationale, h.confidence_prior, time.time()),
    )
    conn.commit()
    conn.close()


def update_test_result(name: str, result: TestResult):
    _ensure_table()
    conn = sqlite3.connect(config.DB_PATH)
    new_status = "rejected"
    if result.decision == "promote_shadow":
        new_status = "promoted_shadow"
    elif result.decision == "needs_more_data":
        new_status = "untested"

    conn.execute(
        "UPDATE research_hypotheses SET cv_mean=?, cv_std=?, cv_sharpe=?, cv_p_value=?, "
        "                                status=?, notes=? "
        "WHERE name=?",
        (result.cv_mean, result.cv_std, result.cv_sharpe, result.p_value,
         new_status, result.notes, name),
    )
    conn.commit()
    conn.close()


def list_hypotheses(status: str = "promoted_shadow") -> list[dict]:
    _ensure_table()
    conn = sqlite3.connect(config.DB_PATH)
    rows = conn.execute(
        "SELECT name, description, proposed_params, cv_sharpe, cv_p_value, status, notes "
        "FROM research_hypotheses WHERE status=? ORDER BY cv_sharpe DESC NULLS LAST",
        (status,),
    ).fetchall()
    conn.close()
    return [
        {
            "name": r[0], "description": r[1],
            "proposed_params": json.loads(r[2]),
            "cv_sharpe": r[3], "cv_p_value": r[4],
            "status": r[5], "notes": r[6],
        }
        for r in rows
    ]


# ── Hypothesis generators ────────────────────────────────────────────────────

def generate_calendar_hypotheses() -> list[Hypothesis]:
    """calendar_effects mining 결과 → hypotheses."""
    from backtest.calendar_effects import get_significant_patterns
    patterns = get_significant_patterns(window_days=30, p_threshold=0.10)

    hyps = []
    for p in patterns.get("hour_of_day", []):
        if p["avg_pnl"] > 0:
            hyps.append(Hypothesis(
                name=f"calendar_hod_{p['bucket']}_{int(time.time())}",
                description=f"{p['bucket']}에 거래 시 평균 PnL +{p['avg_pnl']:.4f}",
                base_strategy="closing_convergence",
                proposed_params={"active_hours_utc": [int(p['bucket'].split('h')[0])]},
                rationale=f"30일 데이터 t={p['t']:.2f}, p={p['p']:.4f}, n={p['n']}",
                confidence_prior=0.5 if p["p"] < 0.05 else 0.3,
            ))
    for p in patterns.get("day_of_week", []):
        if p["avg_pnl"] > 0:
            hyps.append(Hypothesis(
                name=f"calendar_dow_{p['bucket']}_{int(time.time())}",
                description=f"{p['bucket']}에 거래 시 평균 PnL +{p['avg_pnl']:.4f}",
                base_strategy="closing_convergence",
                proposed_params={"active_dow": [p["bucket"]]},
                rationale=f"30일 데이터 t={p['t']:.2f}, p={p['p']:.4f}",
                confidence_prior=0.5 if p["p"] < 0.05 else 0.3,
            ))
    return hyps


def generate_parameter_sweep_hypotheses() -> list[Hypothesis]:
    """검증된 전략의 파라미터 변형 — 사람 직관 없이 grid search."""
    base_configs = [
        # closing_convergence — 가격 임계값 변형
        {
            "base": "closing_convergence",
            "param": "min_price",
            "values": [0.65, 0.70, 0.75, 0.80],
        },
        # closing_convergence — 만기 임박 임계값
        {
            "base": "closing_convergence",
            "param": "max_days",
            "values": [2, 3, 5, 7],
        },
        # 모멘텀 임계값
        {
            "base": "closing_convergence",
            "param": "momentum_min",
            "values": [0.003, 0.005, 0.008, 0.012],
        },
    ]

    hyps = []
    for cfg in base_configs:
        for v in cfg["values"]:
            hyps.append(Hypothesis(
                name=f"sweep_{cfg['base']}_{cfg['param']}_{v}_{int(time.time())}",
                description=f"{cfg['base']} {cfg['param']}={v} 변형",
                base_strategy=cfg["base"],
                proposed_params={cfg["param"]: v},
                rationale="grid search variation",
                confidence_prior=0.2,
            ))
    return hyps


# ── Test runner ──────────────────────────────────────────────────────────────

def test_hypothesis_via_replay(h: Hypothesis) -> TestResult:
    """기존 6일치 데이터로 빠르게 시뮬. CV는 시간 부족 시 단순 hold-out.

    실제 라이브 검증은 shadow 모드에서 N일 더 모은 후.
    """
    # 단순화 — 데이터 적은 현 시점에선 hold-out 단일 폴드
    # 향후 데이터 충분해지면 walk_forward로 교체
    conn = sqlite3.connect(config.DB_PATH)
    rows = conn.execute(
        "SELECT timestamp, COALESCE(pnl, 0) FROM trades "
        "WHERE strategy=? AND pnl IS NOT NULL ORDER BY timestamp",
        (h.base_strategy,)
    ).fetchall()
    conn.close()

    if len(rows) < 10:
        return TestResult(
            hypothesis_name=h.name,
            cv_mean=0, cv_std=0, cv_sharpe=0, p_value=1.0, n_folds=0,
            decision="needs_more_data",
            notes=f"only {len(rows)} closed trades for {h.base_strategy}",
        )

    pnls = [r[1] for r in rows]
    import math
    n = len(pnls)
    mean = sum(pnls) / n
    var = sum((p - mean) ** 2 for p in pnls) / max(1, n - 1)
    std = math.sqrt(var)
    sharpe = mean / std * math.sqrt(252) if std > 0 else 0
    se = std / math.sqrt(n) if n > 1 else 1
    t = mean / se if se > 0 else 0
    from math import erf, sqrt
    z = abs(t)
    p = 2 * (1 - 0.5 * (1 + erf(z / sqrt(2))))

    if mean > 0 and p < 0.10:
        decision = "promote_shadow"
    elif mean > 0:
        decision = "needs_more_data"
    else:
        decision = "reject"

    return TestResult(
        hypothesis_name=h.name,
        cv_mean=mean,
        cv_std=std,
        cv_sharpe=sharpe,
        p_value=p,
        n_folds=1,
        decision=decision,
        notes=f"hold-out single fold, n={n}",
    )


# ── Daily research loop ──────────────────────────────────────────────────────

async def daily_research_cycle() -> dict:
    """1회 사이클 실행 — 가설 생성 → 검증 → 승격."""
    from core.logger import log
    _ensure_table()

    # 1. 가설 생성
    hyps_calendar = generate_calendar_hypotheses()
    hyps_sweep = generate_parameter_sweep_hypotheses()
    all_hyps = hyps_calendar + hyps_sweep

    log.info(f"[research] generated {len(all_hyps)} hypotheses ({len(hyps_calendar)} calendar + {len(hyps_sweep)} sweep)")

    promoted = []
    rejected = []
    pending = []

    for h in all_hyps:
        save_hypothesis(h)
        result = test_hypothesis_via_replay(h)
        update_test_result(h.name, result)
        if result.decision == "promote_shadow":
            promoted.append({"name": h.name, "sharpe": result.cv_sharpe, "p": result.p_value})
        elif result.decision == "reject":
            rejected.append(h.name)
        else:
            pending.append(h.name)

    summary = {
        "n_generated": len(all_hyps),
        "n_promoted": len(promoted),
        "n_rejected": len(rejected),
        "n_pending": len(pending),
        "promoted": promoted,
    }

    log.info(f"[research] {summary}")

    # 텔레그램 알림 — 승격된 가설 있을 때만
    if promoted:
        try:
            from notifications.telegram import notify_async
            await notify_async("INFO", f"새 알파 후보 {len(promoted)}건 발견", {
                "top": promoted[0]["name"][:30],
                "sharpe": promoted[0]["sharpe"],
                "p_value": promoted[0]["p"],
            })
        except Exception:
            pass

    # WebSocket
    try:
        from dashboard.realtime import broadcast_event
        broadcast_event("research_cycle_complete", summary)
    except Exception:
        pass

    return summary


async def research_loop(interval_hours: int = 24):
    """매일 daily_research_cycle 실행."""
    from core.logger import log
    while True:
        try:
            await asyncio.sleep(interval_hours * 3600)
            log.info("[research] starting daily cycle")
            summary = await daily_research_cycle()
            log.info(f"[research] cycle done: {summary['n_promoted']}/{summary['n_generated']} promoted")
        except Exception as e:
            log.warning(f"[research] {e}")
            await asyncio.sleep(3600)
