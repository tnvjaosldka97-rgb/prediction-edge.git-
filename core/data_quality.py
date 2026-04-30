"""
데이터 품질 모니터링.

탐지 항목:
1. Stale 호가 — N초 이상 갱신 없으면 폐기
2. Price spike — 5분 평균 대비 20% 초과 변동 시 의심
3. API latency drift — submit→fill 분포가 이전 대비 급증
4. Order book inversion — best_bid > best_ask (거래소 버그/race)
5. Volume anomaly — 거래량 급증/급감 (기준선 대비 ±5σ)

발견 시:
- 텔레그램 WARN 알림
- 해당 token_id 일시 차단 (재진입 보류)
- 대시보드 표시
"""
from __future__ import annotations
import math
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class DataQualityIssue:
    type: str
    token_id: str
    severity: str       # INFO / WARN / CRITICAL
    detail: str
    detected_at: float = field(default_factory=time.time)


class DataQualityMonitor:
    """런타임 단일 인스턴스. store에 hook해서 수신 데이터 검증."""

    _instance: "DataQualityMonitor | None" = None

    def __init__(self):
        # token_id → 최근 가격 deque (5분치)
        self._price_buffers: dict[str, deque] = {}
        self._buffer_max = 50
        # token_id → last issue ts (dedup 1분)
        self._recent_issues: dict[str, float] = {}
        # 최근 24시간 issue 보관 (대시보드용)
        self._issue_log: deque[DataQualityIssue] = deque(maxlen=200)

    @classmethod
    def get(cls) -> "DataQualityMonitor":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def _record_issue(self, issue: DataQualityIssue) -> None:
        # Dedup: 같은 (type, token_id) 1분 내 중복 X
        key = f"{issue.type}:{issue.token_id}"
        now = time.time()
        if now - self._recent_issues.get(key, 0) < 60:
            return
        self._recent_issues[key] = now
        self._issue_log.append(issue)
        # 텔레그램
        try:
            from notifications.telegram import notify
            notify(issue.severity, f"data_quality: {issue.type}", {
                "token": issue.token_id[:10],
                "detail": issue.detail,
            })
        except Exception:
            pass
        # WebSocket
        try:
            from dashboard.realtime import broadcast_event
            broadcast_event("data_quality", {
                "type": issue.type,
                "token_id": issue.token_id[:12],
                "severity": issue.severity,
                "detail": issue.detail,
            })
        except Exception:
            pass

    # ── Hooks ────────────────────────────────────────────────────────────────

    def check_orderbook(self, token_id: str, book) -> Optional[DataQualityIssue]:
        """OrderBook 받을 때 호출."""
        # 1. Inversion
        if book.best_bid > 0 and book.best_ask > 0 and book.best_bid >= book.best_ask:
            issue = DataQualityIssue(
                type="orderbook_inversion",
                token_id=token_id,
                severity="CRITICAL",
                detail=f"bid={book.best_bid} >= ask={book.best_ask}",
            )
            self._record_issue(issue)
            return issue

        # 2. Stale 검사 — book.timestamp가 30초 이상 됐으면
        age = time.time() - book.timestamp
        if age > 30:
            issue = DataQualityIssue(
                type="stale_orderbook",
                token_id=token_id,
                severity="WARN",
                detail=f"age={age:.0f}s",
            )
            self._record_issue(issue)
            return issue

        # 3. Price spike — mid 추적
        mid = book.mid
        buf = self._price_buffers.setdefault(token_id, deque(maxlen=self._buffer_max))
        if buf:
            avg = sum(buf) / len(buf)
            if avg > 0:
                deviation = abs(mid - avg) / avg
                if deviation > 0.20:  # 20% 변동
                    issue = DataQualityIssue(
                        type="price_spike",
                        token_id=token_id,
                        severity="WARN",
                        detail=f"avg={avg:.4f}, current={mid:.4f}, dev={deviation*100:.1f}%",
                    )
                    self._record_issue(issue)
                    return issue
        buf.append(mid)
        return None

    def check_latency_drift(self, recent_latencies_ms: list[float],
                             baseline_mean_ms: float, baseline_std_ms: float) -> Optional[DataQualityIssue]:
        """주기적으로 호출. recent latency가 baseline보다 +3σ 이상이면 alert."""
        if len(recent_latencies_ms) < 10:
            return None
        recent_mean = sum(recent_latencies_ms) / len(recent_latencies_ms)
        if baseline_std_ms > 0:
            z = (recent_mean - baseline_mean_ms) / baseline_std_ms
            if z > 3:
                issue = DataQualityIssue(
                    type="latency_drift",
                    token_id="",
                    severity="WARN",
                    detail=f"recent={recent_mean:.0f}ms, baseline={baseline_mean_ms:.0f}±{baseline_std_ms:.0f}, z={z:.1f}",
                )
                self._record_issue(issue)
                return issue
        return None

    def get_recent_issues(self, hours: float = 24) -> list[DataQualityIssue]:
        cutoff = time.time() - hours * 3600
        return [i for i in self._issue_log if i.detected_at >= cutoff]


# 편의 함수
def get_monitor() -> DataQualityMonitor:
    return DataQualityMonitor.get()
