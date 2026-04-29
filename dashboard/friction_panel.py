"""
Friction analytics — 대시보드용 API.

friction_traces + friction_calibration 테이블에서 통계 추출.
인증 필요 (require_admin) — 거래 데이터라 노출 X.
"""
from __future__ import annotations
import json
from collections import Counter
from typing import Optional

from fastapi import APIRouter, Depends

from core import db
from dashboard import auth


router = APIRouter(prefix="/api/friction", tags=["friction"])


def _bin_data(values: list[float], bins: list[float]) -> list[dict]:
    """[(start, end, count), ...] 형태로 히스토그램 bin."""
    out = []
    for i in range(len(bins) - 1):
        lo, hi = bins[i], bins[i + 1]
        c = sum(1 for v in values if lo <= v < hi)
        out.append({"start": lo, "end": hi, "count": c})
    # 마지막 bin 이상도 포함
    overflow = sum(1 for v in values if v >= bins[-1])
    if overflow:
        out.append({"start": bins[-1], "end": float("inf"), "count": overflow})
    return out


@router.get("/summary")
async def friction_summary(_: dict = Depends(auth.require_admin), since_hours: float = 24):
    """최근 N시간 마찰 종합 — 한 화면용."""
    import time
    since_ts = time.time() - since_hours * 3600
    traces = db.get_friction_traces(since_ts=since_ts, limit=10_000)

    if not traces:
        return {
            "n_total": 0, "n_filled": 0, "n_partial": 0, "n_rejected": 0,
            "fill_rate": 0, "partial_rate": 0,
            "avg_latency_ms": 0, "avg_slippage_bps": 0,
            "rejection_breakdown": {},
            "since_hours": since_hours,
        }

    filled = [t for t in traces if t.get("fill_ts") and not t.get("rejection_reason")]
    partial = [t for t in filled if t.get("is_partial")]
    rejected = [t for t in traces if t.get("rejection_reason") and t.get("rejection_reason") != "opened_no_immediate_fill"]

    latencies = [t["submit_to_fill_ms"] for t in filled if t.get("submit_to_fill_ms")]
    slippages = [t["slippage_bps"] for t in filled if t.get("slippage_bps") is not None]

    rejection_counter = Counter(t["rejection_reason"] for t in rejected)

    return {
        "n_total": len(traces),
        "n_filled": len(filled),
        "n_partial": len(partial),
        "n_rejected": len(rejected),
        "fill_rate": len(filled) / len(traces) if traces else 0,
        "partial_rate": len(partial) / len(filled) if filled else 0,
        "avg_latency_ms": sum(latencies) / len(latencies) if latencies else 0,
        "p50_latency_ms": sorted(latencies)[len(latencies) // 2] if latencies else 0,
        "p95_latency_ms": sorted(latencies)[int(len(latencies) * 0.95)] if latencies else 0,
        "avg_slippage_bps": sum(slippages) / len(slippages) if slippages else 0,
        "rejection_breakdown": dict(rejection_counter),
        "since_hours": since_hours,
    }


@router.get("/latency_histogram")
async def latency_histogram(_: dict = Depends(auth.require_admin), since_hours: float = 24):
    import time
    since_ts = time.time() - since_hours * 3600
    traces = db.get_friction_traces(since_ts=since_ts, limit=10_000)
    latencies = [t["submit_to_fill_ms"] for t in traces if t.get("submit_to_fill_ms")]
    bins = [0, 50, 100, 200, 500, 1000, 2000, 5000, 10_000]
    return {"bins": _bin_data(latencies, bins), "n": len(latencies)}


@router.get("/slippage_histogram")
async def slippage_histogram(_: dict = Depends(auth.require_admin), since_hours: float = 24):
    import time
    since_ts = time.time() - since_hours * 3600
    traces = db.get_friction_traces(since_ts=since_ts, limit=10_000)
    slips = [abs(t["slippage_bps"]) for t in traces if t.get("slippage_bps") is not None]
    bins = [0, 5, 10, 25, 50, 100, 250, 500]
    return {"bins": _bin_data(slips, bins), "n": len(slips)}


@router.get("/rejection_breakdown")
async def rejection_breakdown(_: dict = Depends(auth.require_admin), since_hours: float = 24):
    import time
    since_ts = time.time() - since_hours * 3600
    traces = db.get_friction_traces(since_ts=since_ts, limit=10_000)
    rejected = [t for t in traces if t.get("rejection_reason")]
    counter = Counter(t["rejection_reason"] for t in rejected)
    total = len(traces)
    return {
        "total_orders": total,
        "total_rejected": len(rejected),
        "by_reason": [
            {"reason": reason, "count": count, "pct": count / total * 100 if total else 0}
            for reason, count in counter.most_common()
        ],
    }


@router.get("/calibration_history")
async def calibration_history(_: dict = Depends(auth.require_admin), limit: int = 20):
    """과거 캘리브레이션 스냅샷 목록."""
    import sqlite3
    import config
    conn = sqlite3.connect(config.DB_PATH)
    rows = conn.execute(
        "SELECT id, timestamp, n_traces_used, params_json, notes "
        "FROM friction_calibration ORDER BY timestamp DESC LIMIT ?",
        (limit,),
    ).fetchall()
    conn.close()
    out = []
    for r in rows:
        try:
            params = json.loads(r[3])
            latency = params.get("latency", {})
            out.append({
                "id": r[0],
                "timestamp": r[1],
                "n_traces": r[2],
                "notes": r[4],
                "latency_mu_ms": latency.get("mu_ms"),
                "latency_sigma_ms": latency.get("sigma_ms"),
            })
        except Exception:
            continue
    return {"snapshots": out}


@router.get("/portfolio_metrics")
async def portfolio_metrics(_: dict = Depends(auth.require_admin), window_days: float = 30):
    """Sharpe / Sortino / max drawdown 등 고급 지표."""
    from core.metrics import compute_portfolio_metrics
    from dataclasses import asdict
    pm = compute_portfolio_metrics(window_days=window_days)
    return asdict(pm)


@router.get("/strategy_eval")
async def strategy_eval(_: dict = Depends(auth.require_admin), window_days: float = 7):
    """전략별 자동 비활성화 평가."""
    from risk.strategy_disabler import evaluate_strategy
    state_path = __import__("pathlib").Path(__file__).resolve().parent.parent / "runtime_state.json"
    if state_path.exists():
        import json
        state = json.loads(state_path.read_text(encoding="utf-8"))
        strategies = list(state.get("strategies_enabled", {}).keys())
    else:
        strategies = ["closing_convergence", "claude_oracle", "fee_arbitrage", "exit_signal"]
    return {"evaluations": [evaluate_strategy(s, window_days) for s in strategies]}


@router.get("/recent_traces")
async def recent_traces(_: dict = Depends(auth.require_admin), limit: int = 50):
    """최근 trace 50건 — 상세 디버깅용."""
    traces = db.get_friction_traces(since_ts=0, limit=limit)
    # 큰 컬럼 (book_snapshot, api_error_text) 잘라서 반환
    out = []
    for t in traces:
        out.append({
            "order_id": t.get("order_id", "")[:16],
            "submit_ts": t.get("submit_ts"),
            "side": t.get("side"),
            "strategy": t.get("strategy"),
            "requested_size_usd": t.get("requested_size_usd"),
            "fill_size_usd": t.get("fill_size_usd"),
            "fill_price": t.get("fill_price"),
            "rejection_reason": t.get("rejection_reason"),
            "submit_to_fill_ms": t.get("submit_to_fill_ms"),
            "slippage_bps": t.get("slippage_bps"),
            "is_partial": bool(t.get("is_partial")),
            "api_error_text": (t.get("api_error_text") or "")[:80],
        })
    return {"traces": out}
