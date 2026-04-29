"""
Telegram 알림 봇.

구성 (.env):
  TELEGRAM_BOT_TOKEN=123456789:ABC...    # @BotFather에서 발급
  TELEGRAM_CHAT_ID=987654321              # 본인 chat_id (또는 그룹 id)
  TELEGRAM_ENABLED=true                   # false면 disable

이벤트 우선순위:
  INFO     : 캘리브레이션 완료, 일일 P&L 요약
  WARN     : 부분 체결률 급증, latency 급증, 큰 손실 발생
  CRITICAL : 비상정지, killswitch 발동, 라이브 모드 전환, $20+ 단일 손실

사용:
  from notifications.telegram import notify
  await notify("INFO", "first live fill", details={"size": 10, "price": 0.55})

채팅 ID 모르면 봇에 메시지 한 번 보내고:
  curl https://api.telegram.org/bot<TOKEN>/getUpdates
"""
from __future__ import annotations
import asyncio
import os
import time
from collections import deque
from typing import Optional

try:
    import httpx
except ImportError:
    httpx = None


_LEVEL_EMOJI = {
    "INFO": "ℹ️",
    "WARN": "⚠️",
    "CRITICAL": "🚨",
    "FILL": "✅",
    "PNL": "💰",
}


class TelegramNotifier:
    """전역 싱글턴. notify() 함수가 이 인스턴스로 위임."""

    _instance: Optional["TelegramNotifier"] = None

    def __init__(self):
        self.token = os.getenv("TELEGRAM_BOT_TOKEN", "")
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
        self.enabled = os.getenv("TELEGRAM_ENABLED", "true").lower() == "true"
        self.enabled = self.enabled and bool(self.token) and bool(self.chat_id)

        # Throttle: 같은 메시지 1분 내 중복 X
        self._recent: deque[tuple[float, str]] = deque(maxlen=50)

        # Rate limit: 30초당 최대 5건
        self._send_times: deque[float] = deque(maxlen=5)

    @classmethod
    def get(cls) -> "TelegramNotifier":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def _is_duplicate(self, msg: str) -> bool:
        now = time.time()
        # 1분 내 같은 메시지 있으면 스킵
        for ts, m in self._recent:
            if now - ts < 60 and m == msg:
                return True
        self._recent.append((now, msg))
        return False

    def _is_rate_limited(self) -> bool:
        now = time.time()
        # 5건이 모두 30초 안에 보내졌으면 throttle
        if len(self._send_times) == 5 and now - self._send_times[0] < 30:
            return True
        self._send_times.append(now)
        return False

    async def send(self, level: str, title: str, details: Optional[dict] = None) -> bool:
        if not self.enabled or httpx is None:
            return False

        emoji = _LEVEL_EMOJI.get(level, "📢")
        msg = f"{emoji} *[{level}]* {title}"
        if details:
            for k, v in details.items():
                if isinstance(v, float):
                    msg += f"\n  • {k}: `{v:.4f}`"
                else:
                    msg += f"\n  • {k}: `{v}`"

        if self._is_duplicate(msg):
            return False
        if self._is_rate_limited():
            return False

        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                r = await client.post(url, json={
                    "chat_id": self.chat_id,
                    "text": msg,
                    "parse_mode": "Markdown",
                })
                return r.status_code == 200
        except Exception:
            return False


# Helper: import해서 어디서든 fire-and-forget으로 호출
def notify(level: str, title: str, details: Optional[dict] = None) -> None:
    """동기 호출도 가능 — 새 task 생성해서 백그라운드로 전송."""
    n = TelegramNotifier.get()
    if not n.enabled:
        return
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.create_task(n.send(level, title, details))
    except RuntimeError:
        # 이벤트 루프 없으면 sync 실행
        try:
            asyncio.run(n.send(level, title, details))
        except Exception:
            pass


async def notify_async(level: str, title: str, details: Optional[dict] = None) -> bool:
    """async 컨텍스트에서 직접 await."""
    n = TelegramNotifier.get()
    return await n.send(level, title, details)
