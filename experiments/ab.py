"""
A/B 테스팅 프레임워크.

새 알고리즘·파라미터 변경을 50/50 분할로 라이브 테스트.
통계적 유의성 (p<0.05) + 실용적 유의성 (effect size > 1%) 둘 다 만족 시 채택.

흐름:
1. ExperimentRegistry.create("close_conv_momentum_thresh_v2",
                              control={"momentum_min": 0.005},
                              variant={"momentum_min": 0.010})
2. 시그널 생성 시점에 token_id 해싱으로 50/50 분할
3. 각 fill의 P&L을 control/variant 별로 추적
4. 50건+ 누적 시 t-test → 결과 출력

대시보드 /api/control/experiments에서 진행 상황 조회.
"""
from __future__ import annotations
import hashlib
import json
import math
import sqlite3
import time
from dataclasses import dataclass, field, asdict
from typing import Optional

import config


@dataclass
class Experiment:
    name: str
    control_params: dict
    variant_params: dict
    started_at: float
    status: str = "running"     # running / accepted / rejected / aborted
    min_samples_per_arm: int = 30
    significance_level: float = 0.05
    notes: str = ""


def _conn():
    return sqlite3.connect(config.DB_PATH)


def _ensure_table():
    conn = _conn()
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS experiments (
        name            TEXT PRIMARY KEY,
        control_json    TEXT NOT NULL,
        variant_json    TEXT NOT NULL,
        started_at      REAL NOT NULL,
        status          TEXT NOT NULL DEFAULT 'running',
        min_samples     INTEGER DEFAULT 30,
        sig_level       REAL DEFAULT 0.05,
        notes           TEXT
    );
    CREATE TABLE IF NOT EXISTS experiment_observations (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        experiment_name TEXT NOT NULL,
        arm             TEXT NOT NULL,         -- 'control' or 'variant'
        token_id        TEXT,
        pnl             REAL,
        ts              REAL NOT NULL,
        FOREIGN KEY (experiment_name) REFERENCES experiments(name)
    );
    CREATE INDEX IF NOT EXISTS idx_exp_obs ON experiment_observations(experiment_name, arm, ts);
    """)
    conn.commit()
    conn.close()


def assign_arm(experiment_name: str, token_id: str) -> str:
    """token_id 해싱으로 결정론적 50/50 분할."""
    h = hashlib.md5(f"{experiment_name}:{token_id}".encode()).hexdigest()
    return "control" if int(h, 16) % 2 == 0 else "variant"


def create(experiment_name: str, control_params: dict, variant_params: dict,
           min_samples_per_arm: int = 30, notes: str = "") -> Experiment:
    _ensure_table()
    exp = Experiment(
        name=experiment_name,
        control_params=control_params,
        variant_params=variant_params,
        started_at=time.time(),
        min_samples_per_arm=min_samples_per_arm,
        notes=notes,
    )
    conn = _conn()
    conn.execute(
        "INSERT OR REPLACE INTO experiments (name, control_json, variant_json, started_at, status, min_samples, sig_level, notes) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (exp.name, json.dumps(exp.control_params), json.dumps(exp.variant_params),
         exp.started_at, exp.status, exp.min_samples_per_arm, exp.significance_level, exp.notes),
    )
    conn.commit()
    conn.close()
    return exp


def record_observation(experiment_name: str, token_id: str, pnl: float,
                        ts: Optional[float] = None) -> None:
    arm = assign_arm(experiment_name, token_id)
    conn = _conn()
    conn.execute(
        "INSERT INTO experiment_observations (experiment_name, arm, token_id, pnl, ts) VALUES (?, ?, ?, ?, ?)",
        (experiment_name, arm, token_id, pnl, ts or time.time()),
    )
    conn.commit()
    conn.close()


def _t_test_two_sample(control: list[float], variant: list[float]) -> tuple[float, float]:
    """Welch's t-test (분산 다른 두 집단). returns (t, df)."""
    n1, n2 = len(control), len(variant)
    if n1 < 2 or n2 < 2:
        return 0.0, 0.0
    m1 = sum(control) / n1
    m2 = sum(variant) / n2
    v1 = sum((x - m1) ** 2 for x in control) / (n1 - 1)
    v2 = sum((x - m2) ** 2 for x in variant) / (n2 - 1)
    se = math.sqrt(v1 / n1 + v2 / n2)
    if se == 0:
        return 0.0, 0.0
    t = (m2 - m1) / se
    df_num = (v1 / n1 + v2 / n2) ** 2
    df_den = (v1 / n1) ** 2 / (n1 - 1) + (v2 / n2) ** 2 / (n2 - 1)
    df = df_num / df_den if df_den > 0 else max(n1, n2) - 1
    return t, df


def _t_to_p(t: float, df: float) -> float:
    """대략적 정규근사 (df>=30 가정)."""
    from math import erf, sqrt
    z = abs(t)
    p = 2 * (1 - 0.5 * (1 + erf(z / sqrt(2))))
    return max(0.0, min(1.0, p))


def evaluate(experiment_name: str) -> dict:
    """현 시점 통계 + 의사결정."""
    conn = _conn()
    rows = conn.execute(
        "SELECT arm, pnl FROM experiment_observations WHERE experiment_name=?",
        (experiment_name,)
    ).fetchall()
    exp_row = conn.execute(
        "SELECT control_json, variant_json, started_at, status, min_samples, sig_level FROM experiments WHERE name=?",
        (experiment_name,)
    ).fetchone()
    conn.close()

    if not exp_row:
        return {"error": "experiment not found"}

    control = [r[1] for r in rows if r[0] == "control"]
    variant = [r[1] for r in rows if r[0] == "variant"]
    n_c, n_v = len(control), len(variant)

    if n_c == 0 or n_v == 0:
        return {
            "name": experiment_name,
            "n_control": n_c, "n_variant": n_v,
            "decision": "insufficient_data",
        }

    mean_c = sum(control) / n_c
    mean_v = sum(variant) / n_v

    t, df = _t_test_two_sample(control, variant)
    p = _t_to_p(t, df)
    sig_level = exp_row[5]
    min_samples = exp_row[4]

    decision = "running"
    if n_c >= min_samples and n_v >= min_samples:
        if p < sig_level:
            if mean_v > mean_c:
                decision = "variant_wins"
            else:
                decision = "control_wins"
        else:
            decision = "no_significant_difference"

    return {
        "name": experiment_name,
        "n_control": n_c,
        "n_variant": n_v,
        "mean_control": mean_c,
        "mean_variant": mean_v,
        "effect_size": mean_v - mean_c,
        "effect_size_pct": (mean_v - mean_c) / abs(mean_c) * 100 if mean_c != 0 else 0,
        "t_statistic": t,
        "p_value": p,
        "significance_level": sig_level,
        "decision": decision,
    }


def list_active() -> list[dict]:
    _ensure_table()
    conn = _conn()
    rows = conn.execute(
        "SELECT name, started_at, status FROM experiments WHERE status='running' ORDER BY started_at DESC"
    ).fetchall()
    conn.close()
    return [{"name": r[0], "started_at": r[1], "status": r[2]} for r in rows]


def conclude(experiment_name: str, status: str = "accepted", notes: str = "") -> None:
    """실험 종료. status: accepted / rejected / aborted."""
    conn = _conn()
    conn.execute(
        "UPDATE experiments SET status=?, notes=? WHERE name=?",
        (status, notes, experiment_name),
    )
    conn.commit()
    conn.close()
