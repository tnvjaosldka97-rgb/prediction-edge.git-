"""
Friction model calibration — friction_traces 테이블에서 라이브 데이터로
orchestrator 파라미터 자동 갱신.

Trigger:
- 매시간 or trace 50건 이상 신규 누적 시
- 또는 대시보드에서 수동 호출

흐름:
  1. friction_traces에서 마지막 calibration 이후 trace 읽음
  2. 카테고리별 분리 (fill / partial / rejection)
  3. 각 모델의 calibrate() 호출
  4. orchestrator.to_dict() → friction_calibration 테이블에 스냅샷
  5. 다음 부팅 시 from_dict로 복원
"""
from __future__ import annotations
import json
from collections import Counter
from dataclasses import dataclass
from typing import Optional

from core import db
from core.logger import log
from friction.orchestrator import FrictionOrchestrator


@dataclass
class CalibrationReport:
    n_traces_used: int
    n_filled: int
    n_rejected: int
    n_partial: int
    rejection_breakdown: dict
    latency_mu_ms: float
    latency_sigma_ms: float
    cancel_rate: float
    saved: bool
    skip_reason: Optional[str] = None


def calibrate(
    orchestrator: FrictionOrchestrator,
    min_traces: int = 50,
    since_ts: float = 0,
) -> CalibrationReport:
    """라이브 trace 사용해 모델 파라미터 갱신."""
    traces = db.get_friction_traces(since_ts=since_ts, limit=10_000)

    if len(traces) < min_traces:
        return CalibrationReport(
            n_traces_used=len(traces), n_filled=0, n_rejected=0, n_partial=0,
            rejection_breakdown={}, latency_mu_ms=0, latency_sigma_ms=0,
            cancel_rate=0, saved=False,
            skip_reason=f"need >={min_traces} traces, have {len(traces)}",
        )

    # 분리
    filled = [t for t in traces if t.get("fill_ts") and not t.get("rejection_reason")]
    rejected = [t for t in traces if t.get("rejection_reason") and t.get("rejection_reason") != "opened_no_immediate_fill"]
    partial = [t for t in filled if t.get("is_partial")]

    # 1. Latency
    latencies = [t["submit_to_fill_ms"] for t in filled if t.get("submit_to_fill_ms")]
    if latencies:
        orchestrator.latency.calibrate(latencies)

    # 2. Partial fill (체결된 것 중 fill_ratio 분포)
    fill_ratios = []
    for t in filled:
        req = t.get("requested_size_usd") or 0
        got = t.get("fill_size_usd") or 0
        if req > 0:
            fill_ratios.append(min(1.0, got / req))
    if fill_ratios:
        orchestrator.partial_fill.calibrate(fill_ratios)

    # 3. Rejection
    rejection_reasons = Counter(t["rejection_reason"] for t in rejected)
    orchestrator.rejection.calibrate(dict(rejection_reasons), len(traces))

    # 4. Snapshot 저장
    params = orchestrator.to_dict()
    notes = f"filled={len(filled)} rejected={len(rejected)} partial={len(partial)}"
    db.insert_friction_calibration(len(traces), params, notes)

    log.info(f"[calibrate] {notes} | latency_mu={orchestrator.latency.mu_ms:.0f}ms")

    return CalibrationReport(
        n_traces_used=len(traces),
        n_filled=len(filled),
        n_rejected=len(rejected),
        n_partial=len(partial),
        rejection_breakdown=dict(rejection_reasons),
        latency_mu_ms=orchestrator.latency.mu_ms,
        latency_sigma_ms=orchestrator.latency.sigma_ms,
        cancel_rate=len(partial) / max(1, len(filled)),
        saved=True,
    )


def load_latest(orchestrator: FrictionOrchestrator) -> bool:
    """가장 최근 calibration을 orchestrator에 적용. 없으면 False."""
    snap = db.get_latest_friction_calibration()
    if not snap:
        return False

    params = snap["params"]

    if "latency" in params:
        from friction.latency import LatencyModel
        orchestrator.latency = LatencyModel.from_dict(params["latency"])

    if "partial_fill" in params:
        from friction.partial_fill import PartialFillModel
        orchestrator.partial_fill = PartialFillModel.from_dict(params["partial_fill"])

    if "network_blip" in params:
        from friction.network_blip import NetworkBlipModel
        orchestrator.network_blip = NetworkBlipModel.from_dict(params["network_blip"])

    log.info(f"[calibrate] loaded snapshot from {snap['timestamp']}, n_traces={snap['n_traces_used']}")
    return True
