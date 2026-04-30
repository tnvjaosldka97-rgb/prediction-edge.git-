"""
실시간 WebSocket 브로드캐스트.

사용:
  from dashboard.realtime import broadcast_event
  broadcast_event("fill", {"side": "BUY", "size_usd": 10.5})

이벤트 타입:
  - fill          : 체결 (gateway가 발화)
  - signal        : 새 시그널 (signal_aggregator가 발화)
  - mode_change   : 모드 전환
  - killswitch    : 비상정지
  - calibration   : 캘리브레이션 완료
  - state_update  : runtime_state.json 변경
  - portfolio     : 자산 스냅샷
  - error         : 라이브 에러

클라이언트 (control_panel.html)는 /api/ws/live 구독하고
push 받을 때마다 해당 패널만 부분 새로고침.
"""
from __future__ import annotations
import asyncio
import json
import time
from typing import Any
from collections import deque

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from dashboard import auth


router = APIRouter(tags=["realtime"])


class Broadcaster:
    """싱글턴 — 모든 연결 관리."""

    _instance: "Broadcaster | None" = None

    def __init__(self):
        self._clients: set[WebSocket] = set()
        self._history: deque = deque(maxlen=100)  # 최근 100개 이벤트 (재연결 시 backfill)
        self._lock = asyncio.Lock() if asyncio.get_event_loop_policy() else None

    @classmethod
    def get(cls) -> "Broadcaster":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    async def add(self, ws: WebSocket):
        await ws.accept()
        self._clients.add(ws)
        # 최근 이벤트 backfill
        for ev in list(self._history)[-20:]:
            try:
                await ws.send_json(ev)
            except Exception:
                pass

    def remove(self, ws: WebSocket):
        self._clients.discard(ws)

    def broadcast_sync(self, event_type: str, payload: dict | None = None) -> None:
        """동기 호출 — fire-and-forget. 새 task 생성."""
        ev = {
            "type": event_type,
            "ts": time.time(),
            "data": payload or {},
        }
        self._history.append(ev)
        if not self._clients:
            return
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.create_task(self._broadcast(ev))
        except RuntimeError:
            pass

    async def _broadcast(self, ev: dict):
        dead = set()
        for ws in self._clients:
            try:
                await ws.send_json(ev)
            except Exception:
                dead.add(ws)
        self._clients -= dead


def broadcast_event(event_type: str, payload: dict | None = None) -> None:
    """모듈 함수 — 어디서든 호출."""
    Broadcaster.get().broadcast_sync(event_type, payload)


@router.websocket("/api/ws/live")
async def ws_live(ws: WebSocket):
    """실시간 이벤트 스트림. 인증된 세션 쿠키 필요."""
    # 쿠키 검증 — WebSocket 핸드셰이크 시 쿠키는 헤더에 포함됨
    token = ws.cookies.get("admin_session", "")
    ip = ws.client.host if ws.client else ""
    if not auth.verify_session(token, ip):
        await ws.close(code=4001, reason="unauthorized")
        return

    bc = Broadcaster.get()
    await bc.add(ws)
    try:
        while True:
            # 클라이언트 메시지 대기 (heartbeat 용)
            msg = await ws.receive_text()
            if msg == "ping":
                await ws.send_text("pong")
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        bc.remove(ws)
