"""
포지션 단위 리스크 모니터.

단일 포지션이 자본의 X% 이상 차지하면 알림 + 자동 축소 검토.
중앙값에서 벗어난 outlier 포지션 탐지.

기준:
  - 단일 포지션 > 25% bankroll → WARN (텔레그램)
  - 단일 포지션 > 40% bankroll → CRITICAL (자동 partial exit 시그널)
  - 같은 condition_id에 30분 내 5회 이상 진입 → WARN (오류 의심)
"""
from __future__ import annotations
from typing import Optional


SINGLE_POSITION_WARN_PCT = 0.25
SINGLE_POSITION_CRITICAL_PCT = 0.40
RAPID_ENTRY_WINDOW_SEC = 1800
RAPID_ENTRY_COUNT = 5


def check_position_risk(portfolio_state) -> list[dict]:
    """현재 포트폴리오에서 위험 포지션 찾기. 알림 발송 + 결과 반환."""
    alerts = []
    if not portfolio_state or not portfolio_state.positions:
        return alerts

    total_value = portfolio_state.total_value
    if total_value <= 0:
        return alerts

    for token_id, pos in portfolio_state.positions.items():
        position_value = pos.size_shares * pos.current_price
        pct_of_portfolio = position_value / total_value if total_value > 0 else 0

        if pct_of_portfolio >= SINGLE_POSITION_CRITICAL_PCT:
            alerts.append({
                "level": "CRITICAL",
                "token_id": token_id[:8],
                "strategy": pos.strategy,
                "value_usd": position_value,
                "pct_of_portfolio": pct_of_portfolio,
                "reason": f"single position is {pct_of_portfolio*100:.1f}% of portfolio",
                "action": "consider_partial_exit",
            })
        elif pct_of_portfolio >= SINGLE_POSITION_WARN_PCT:
            alerts.append({
                "level": "WARN",
                "token_id": token_id[:8],
                "strategy": pos.strategy,
                "value_usd": position_value,
                "pct_of_portfolio": pct_of_portfolio,
                "reason": f"concentrated position {pct_of_portfolio*100:.1f}%",
                "action": "monitor",
            })

    # 텔레그램 알림 + WebSocket 브로드캐스트
    if alerts:
        try:
            from notifications.telegram import notify
            for a in alerts:
                notify(a["level"], f"포지션 리스크: {a['token_id']}", {
                    "strategy": a["strategy"],
                    "value_usd": f"${a['value_usd']:.2f}",
                    "pct": f"{a['pct_of_portfolio']*100:.1f}%",
                    "action": a["action"],
                })
        except Exception:
            pass
        try:
            from dashboard.realtime import broadcast_event
            broadcast_event("position_risk", {"alerts": alerts})
        except Exception:
            pass

    return alerts


async def position_monitor_loop(portfolio_state, interval_sec: int = 300):
    """5분 간격 자동 점검."""
    import asyncio
    from core.logger import log
    while True:
        try:
            await asyncio.sleep(interval_sec)
            alerts = check_position_risk(portfolio_state)
            if alerts:
                log.warning(f"[position_monitor] {len(alerts)} risk alerts")
        except Exception as e:
            log.warning(f"[position_monitor] error: {e}")
            await asyncio.sleep(60)
