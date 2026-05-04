"""
시스템 자가관측 루프 — oracle_disputes / audit_log 테이블에 주기적으로 기록.

기존:
- oracle_disputes: 한번도 안 쌓임 (score만 메모리에서 계산하고 버림)
- audit_log: 사람이 대시보드에서 액션할 때만 기록 → 24/7 운영 흔적 무

신규:
- DisputeWriter: 30분마다 시장 분쟁 리스크 상위 N개를 DB에 적재
- SystemAuditWriter: 1시간마다 시스템 상태 스냅샷을 audit_log에 기록
"""
from __future__ import annotations
import asyncio
import time

from core import db
from core.logger import log


async def dispute_writer_loop(store, interval_sec: int = 1800, top_n: int = 50):
    """
    매 30분마다 분쟁 리스크 상위 시장들을 oracle_disputes 테이블에 upsert.
    페이퍼/라이브 모두 동작.
    """
    while True:
        try:
            await asyncio.sleep(interval_sec)
            try:
                markets = list(store.get_all_markets()) if store else []
            except Exception:
                markets = []
            if not markets:
                continue

            # dispute_risk 기준 내림차순 상위 top_n
            ranked = sorted(
                [m for m in markets if getattr(m, "dispute_risk", 0) is not None],
                key=lambda m: getattr(m, "dispute_risk", 0) or 0,
                reverse=True,
            )[:top_n]

            count = 0
            for m in ranked:
                try:
                    cid = getattr(m, "condition_id", None)
                    if not cid:
                        continue
                    risk = float(getattr(m, "dispute_risk", 0) or 0)
                    ambig = float(getattr(m, "ambiguity_score", 0) or 0)
                    if risk <= 0 and ambig <= 0:
                        continue
                    db.upsert_oracle_dispute(cid, risk, ambig)
                    count += 1
                except Exception as e:
                    log.debug(f"[dispute_writer] upsert error: {e}")

            if count:
                log.info(f"[dispute_writer] {count} markets recorded to oracle_disputes")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning(f"[dispute_writer] loop error: {type(e).__name__}: {e}")
            await asyncio.sleep(60)


async def system_audit_loop(portfolio, gateway_stats_fn=None,
                            interval_sec: int = 3600):
    """
    매 1시간마다 시스템 상태 스냅샷을 audit_log에 기록.
    24/7 운영 흔적 + 라이브 전 자체보완 로그용.
    """
    while True:
        try:
            await asyncio.sleep(interval_sec)
            stats = {}
            try:
                stats = (gateway_stats_fn() if gateway_stats_fn else {}) or {}
            except Exception:
                stats = {}
            after = {
                "ts": time.time(),
                "bankroll": getattr(portfolio, "bankroll", None),
                "total_value": getattr(portfolio, "total_value", None),
                "peak_value": getattr(portfolio, "peak_value", None),
                "drawdown_pct": (getattr(portfolio, "drawdown", 0) or 0) * 100,
                "positions": len(getattr(portfolio, "positions", {}) or {}),
                "trade_count": getattr(portfolio, "trade_count", 0),
                "gateway_stats": stats,
            }
            try:
                db.insert_audit_log(
                    "system", "hourly_snapshot",
                    before=None, after=after, ip="", ua=""
                )
                log.debug("[system_audit] snapshot recorded")
            except Exception as e:
                log.debug(f"[system_audit] insert error: {e}")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning(f"[system_audit] loop error: {type(e).__name__}: {e}")
            await asyncio.sleep(60)
