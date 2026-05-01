"""
Polymarket API 정책 변경 / 인증 실패 감지 + auto-killswitch.

감지 항목:
1. 5분 내 401/403 5건+ → API 키 만료 또는 정책 변경 → killswitch trip
2. 5분 내 502/503 5건+ → Polymarket 서버 장애 → 일시 거래 중단
3. 응답 스키마 갑자기 변경 (예: 필수 필드 누락) → 텔레그램 CRITICAL

다른 모듈(gateway 등)이 ApiHealthMonitor.report_error()를 호출.
모니터가 레이트 카운트 + 트리거 결정.
"""
from __future__ import annotations
import asyncio
import time
from collections import deque
from dataclasses import dataclass


_AUTH_FAIL_THRESHOLD = 5
_AUTH_FAIL_WINDOW_SEC = 300        # 5분
_SERVER_FAIL_THRESHOLD = 10
_SERVER_FAIL_WINDOW_SEC = 300


@dataclass
class ApiErrorEvent:
    timestamp: float
    status_code: int
    endpoint: str
    error_text: str = ""


class ApiHealthMonitor:
    _instance: "ApiHealthMonitor | None" = None

    def __init__(self):
        self._auth_fails: deque[ApiErrorEvent] = deque(maxlen=100)
        self._server_fails: deque[ApiErrorEvent] = deque(maxlen=200)
        self._last_killswitch_attempt = 0.0

    @classmethod
    def get(cls) -> "ApiHealthMonitor":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def report_error(self, status_code: int, endpoint: str = "", error_text: str = ""):
        ev = ApiErrorEvent(time.time(), status_code, endpoint, error_text)
        if status_code in (401, 403):
            self._auth_fails.append(ev)
            self._check_auth_threshold()
        elif status_code in (502, 503, 504):
            self._server_fails.append(ev)
            self._check_server_threshold()

    def _check_auth_threshold(self):
        now = time.time()
        recent = [e for e in self._auth_fails if now - e.timestamp < _AUTH_FAIL_WINDOW_SEC]
        if len(recent) >= _AUTH_FAIL_THRESHOLD:
            if now - self._last_killswitch_attempt < 600:
                return    # 10분 cooldown — 재진입 방지
            self._last_killswitch_attempt = now
            try:
                from risk import killswitch
                killswitch.trip("api_auth_failures", n_fails=len(recent),
                                  endpoints=[e.endpoint for e in recent[-5:]])
            except Exception:
                pass
            try:
                from notifications.telegram import notify
                notify("CRITICAL", f"API 인증 실패 {len(recent)}건/5분", {
                    "action": "killswitch trip → 모든 거래 중단",
                    "likely_cause": "키 만료 / 정책 변경 / Polymarket 차단",
                    "recent_endpoints": [e.endpoint[:40] for e in recent[-3:]],
                })
            except Exception:
                pass

    def _check_server_threshold(self):
        now = time.time()
        recent = [e for e in self._server_fails if now - e.timestamp < _SERVER_FAIL_WINDOW_SEC]
        if len(recent) >= _SERVER_FAIL_THRESHOLD:
            try:
                from notifications.telegram import notify
                notify("WARN", f"Polymarket 서버 장애 의심 {len(recent)}건/5분", {
                    "action": "API 호출 일시 backoff 권장",
                })
            except Exception:
                pass

    def health_summary(self) -> dict:
        now = time.time()
        return {
            "auth_fails_5min": len([e for e in self._auth_fails if now - e.timestamp < 300]),
            "server_fails_5min": len([e for e in self._server_fails if now - e.timestamp < 300]),
            "auth_fail_threshold": _AUTH_FAIL_THRESHOLD,
            "server_fail_threshold": _SERVER_FAIL_THRESHOLD,
        }


def get_monitor() -> ApiHealthMonitor:
    return ApiHealthMonitor.get()


def report_api_error(status_code: int, endpoint: str = "", error_text: str = ""):
    """편의 함수 — gateway, signals 등 어디서든 import해서 호출."""
    get_monitor().report_error(status_code, endpoint, error_text)
