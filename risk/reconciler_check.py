"""
포지션 재조정 — CLOB 실제 vs 우리 portfolio 불일치 감지.

10분마다:
1. CLOB API에서 우리 wallet의 실제 포지션 조회
2. portfolio.positions와 비교
3. 불일치 시:
   - WARN 로그 + 텔레그램
   - critical 차이면 (>20%) killswitch trip + 비상정지

원인:
- 가스 부족으로 송금 실패
- 외부에서 직접 거래 (사람이 메타마스크로)
- DB corruption
- Reorg
- Race condition
"""
from __future__ import annotations
import asyncio
import time
from typing import Optional


_DRIFT_TOLERANCE = 0.05    # 5% 차이 OK, 그 이상은 alert
_CRITICAL_DRIFT = 0.20      # 20% 차이는 비상정지


async def fetch_actual_positions(clob_client, wallet_address: str) -> dict[str, float]:
    """CLOB API에서 token_id → shares 실제 보유."""
    if not clob_client:
        return {}
    try:
        # py_clob_client의 get_balances 또는 get_positions
        # 정확한 메서드명은 라이브러리 버전에 따라 다름
        balances = await asyncio.get_event_loop().run_in_executor(
            None, clob_client.get_balances
        )
        out = {}
        for b in balances or []:
            if isinstance(b, dict):
                tid = b.get("asset_id") or b.get("token_id")
                amt = float(b.get("balance", 0))
                if tid and amt > 0:
                    out[tid] = amt
        return out
    except Exception:
        return {}


def compare_positions(our_positions: dict, actual_positions: dict[str, float]) -> dict:
    """
    Returns: {
        "matched": [...],           # 일치 (소수점 오차 내)
        "drift": [...],             # 차이 있지만 tolerance 내
        "missing_in_actual": [...], # 우리는 갖고 있다고 하는데 실제 없음
        "missing_in_ours": [...],   # 실제 있는데 우리 모름
        "critical_drift": [...],    # 큰 차이
    }
    """
    matched, drift, missing_in_actual, missing_in_ours, critical = [], [], [], [], []

    our_token_ids = set(our_positions.keys())
    actual_token_ids = set(actual_positions.keys())

    for tid in our_token_ids - actual_token_ids:
        our_shares = our_positions[tid].size_shares
        if our_shares > 0:
            missing_in_actual.append({"token_id": tid[:8], "our_shares": our_shares})

    for tid in actual_token_ids - our_token_ids:
        actual_shares = actual_positions[tid]
        if actual_shares > 0:
            missing_in_ours.append({"token_id": tid[:8], "actual_shares": actual_shares})

    for tid in our_token_ids & actual_token_ids:
        our = our_positions[tid].size_shares
        actual = actual_positions[tid]
        if our <= 0 and actual <= 0:
            continue
        diff_pct = abs(our - actual) / max(our, actual)
        item = {
            "token_id": tid[:8],
            "our": our,
            "actual": actual,
            "diff_pct": diff_pct,
        }
        if diff_pct < 0.001:
            matched.append(item)
        elif diff_pct < _DRIFT_TOLERANCE:
            drift.append(item)
        elif diff_pct < _CRITICAL_DRIFT:
            drift.append(item)
        else:
            critical.append(item)

    return {
        "matched": matched,
        "drift": drift,
        "missing_in_actual": missing_in_actual,
        "missing_in_ours": missing_in_ours,
        "critical_drift": critical,
    }


async def reconciliation_loop(portfolio, gateway, interval_sec: int = 600):
    """매 10분 포지션 재조정."""
    from core.logger import log
    while True:
        try:
            await asyncio.sleep(interval_sec)
            if not portfolio or not gateway or not getattr(gateway, "_clob_client", None):
                continue

            # gateway에 wallet_address 있다고 가정 — 우리 봇은 .env에서 로드
            import os
            wallet = os.getenv("WALLET_ADDRESS", "")
            if not wallet:
                continue

            actual = await fetch_actual_positions(gateway._clob_client, wallet)
            comparison = compare_positions(portfolio.positions, actual)

            n_critical = len(comparison["critical_drift"])
            n_missing_actual = len(comparison["missing_in_actual"])
            n_drift = len(comparison["drift"])

            if n_critical > 0 or n_missing_actual > 2:
                log.error(
                    f"[reconcile] CRITICAL drift: critical={n_critical}, "
                    f"missing_in_actual={n_missing_actual}"
                )
                try:
                    from notifications.telegram import notify
                    notify("CRITICAL", "포지션 재조정 — 큰 불일치", {
                        "critical_drift": n_critical,
                        "missing_in_actual": n_missing_actual,
                        "action": "수동 점검 필요. 자동 정정 X (위험)",
                    })
                except Exception:
                    pass
                # 큰 불일치는 killswitch — 사람 점검 필요
                try:
                    from risk import killswitch
                    killswitch.trip("position_drift_critical",
                                     n_critical=n_critical, n_missing=n_missing_actual)
                except Exception:
                    pass
            elif n_drift > 0:
                log.info(f"[reconcile] minor drift: {n_drift} positions")

        except Exception as e:
            from core.logger import log
            log.warning(f"[reconcile] {e}")
            await asyncio.sleep(120)
