"""
Concurrent order limit + 사전 담보 락.

Polymarket 한도:
- 계정당 동시 미체결 주문 50건 (2024 기준)
- USDC 담보는 주문 사이즈 합계만큼 lock

DRY_RUN에 정확히 반영하려면:
- 현재 미체결 주문 카운터 추적
- 새 주문 시 한도 체크
- bankroll - locked > order.size_usd 검증
"""
from __future__ import annotations
import time
from dataclasses import dataclass


MAX_CONCURRENT_ORDERS = 50


@dataclass
class OrderState:
    order_id: str
    submit_ts: float
    size_usd: float
    side: str
    is_open: bool = True


class ConcurrentOrderTracker:
    """싱글턴 — 가상 + 라이브 주문 모두 추적."""

    _instance: "ConcurrentOrderTracker | None" = None

    def __init__(self):
        self._open_orders: dict[str, OrderState] = {}

    @classmethod
    def get(cls) -> "ConcurrentOrderTracker":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def can_submit(self) -> tuple[bool, str]:
        """동시 주문 한도 체크."""
        n_open = sum(1 for o in self._open_orders.values() if o.is_open)
        if n_open >= MAX_CONCURRENT_ORDERS:
            return False, f"concurrent_limit ({n_open}/{MAX_CONCURRENT_ORDERS})"
        return True, ""

    def can_lock_collateral(self, requested_usd: float, bankroll: float) -> tuple[bool, str]:
        """사전 담보 검증. bankroll - 이미 락된 사이즈 > 요청 사이즈?"""
        locked = sum(o.size_usd for o in self._open_orders.values() if o.is_open and o.side == "BUY")
        available = bankroll - locked
        if requested_usd > available:
            return False, f"insufficient_balance (need ${requested_usd:.2f}, available ${available:.2f}, locked ${locked:.2f})"
        return True, ""

    def register(self, order_id: str, size_usd: float, side: str) -> None:
        self._open_orders[order_id] = OrderState(
            order_id=order_id,
            submit_ts=time.time(),
            size_usd=size_usd,
            side=side,
            is_open=True,
        )

    def mark_filled(self, order_id: str) -> None:
        if order_id in self._open_orders:
            self._open_orders[order_id].is_open = False

    def mark_cancelled(self, order_id: str) -> None:
        if order_id in self._open_orders:
            self._open_orders[order_id].is_open = False

    def cleanup_stale(self, max_age_sec: float = 600) -> int:
        """10분 넘은 open 주문은 정리 (실제 race condition 방지)."""
        now = time.time()
        stale = [oid for oid, o in self._open_orders.items()
                 if o.is_open and now - o.submit_ts > max_age_sec]
        for oid in stale:
            self._open_orders[oid].is_open = False
        return len(stale)

    def stats(self) -> dict:
        now = time.time()
        open_orders = [o for o in self._open_orders.values() if o.is_open]
        return {
            "n_open": len(open_orders),
            "max_concurrent": MAX_CONCURRENT_ORDERS,
            "locked_collateral_usd": sum(o.size_usd for o in open_orders if o.side == "BUY"),
            "oldest_open_age_sec": (now - min((o.submit_ts for o in open_orders), default=now))
                                    if open_orders else 0,
        }


def get_tracker() -> ConcurrentOrderTracker:
    return ConcurrentOrderTracker.get()
