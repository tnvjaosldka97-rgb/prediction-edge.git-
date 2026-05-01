"""
Hard kill-switch — persistent, file-based trading lockout.

Why a separate file-based switch (not just in-memory):
  - Process restart should NOT reset a trip. Losing $ then auto-restarting
    into more losses is exactly the blowup pattern.
  - Reset must be a conscious, manual operation.
  - Any module can trip the switch without coordinating state.

Trips automatically on:
  1. Portfolio drawdown >= MAX_DRAWDOWN_HALT (hard halt, not reduce)
  2. Daily realized loss >= MAX_DAILY_LOSS_USD
  3. Consecutive losing trades >= MAX_CONSECUTIVE_LOSSES
  4. Manual trip via tripswitch.trip("reason")

When tripped:
  - Gateway rejects every new order
  - Existing positions are NOT auto-closed (let them run to resolution)
  - Logs loudly
  - Writes state to disk so restart preserves lockout

Reset:
  - Delete risk/KILLSWITCH_TRIPPED file OR call killswitch.reset()
  - Requires human judgment — "why did it trip and is it safe to resume"
"""
from __future__ import annotations
import json
import os
import time
from pathlib import Path

import config
from core.logger import log

# Root of the project is two levels up from this file
_STATE_FILE = Path(__file__).parent / "KILLSWITCH_TRIPPED"

# Thresholds (env-overridable, conservative defaults)
MAX_DAILY_LOSS_USD        = float(os.getenv("MAX_DAILY_LOSS_USD", "50"))
MAX_CONSECUTIVE_LOSSES    = int(os.getenv("MAX_CONSECUTIVE_LOSSES", "5"))
# Uses config.MAX_DRAWDOWN_HALT as the hard drawdown limit (already defined)


# ── In-memory rolling counters (rebuilt from DB on startup) ───────────────────
_consecutive_losses = 0
_daily_realized_loss = 0.0
_daily_reset_ts = 0.0


def _reset_daily_if_needed() -> None:
    """M5 fix: 자정(UTC) 기준 리셋 — 롤링 86400s 대신 달력 일자."""
    global _daily_realized_loss, _daily_reset_ts
    import datetime
    now = time.time()
    today_start = datetime.datetime.utcnow().replace(
        hour=0, minute=0, second=0, microsecond=0
    ).timestamp()
    if _daily_reset_ts < today_start:
        _daily_realized_loss = 0.0
        _daily_reset_ts = now


def is_tripped() -> bool:
    """True if the killswitch is currently engaged (disk-state check)."""
    return _STATE_FILE.exists()


def get_trip_info() -> dict | None:
    """Return details of the active trip, or None if not tripped."""
    if not _STATE_FILE.exists():
        return None
    try:
        with open(_STATE_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return {"reason": "unknown", "ts": _STATE_FILE.stat().st_mtime}


def _send_critical_alert(reason: str, extra: dict) -> None:
    """killswitch 발동 → 텔레그램 CRITICAL 알림. 실패해도 trip 자체는 진행."""
    try:
        from notifications.telegram import notify
        notify("CRITICAL", f"Killswitch 발동: {reason}", extra)
    except Exception:
        pass


def trip(reason: str, **extra) -> None:
    """Engage the killswitch. Writes to disk — survives restart."""
    if is_tripped():
        return
    payload = {"reason": reason, "ts": time.time(), **extra}
    try:
        with open(_STATE_FILE, "w") as f:
            json.dump(payload, f, indent=2)
    except Exception as e:
        log.error(f"[KILLSWITCH] Failed to write state file: {e}")
    log.error(
        f"[KILLSWITCH TRIPPED] reason={reason} — ALL NEW ORDERS BLOCKED. "
        f"Manual reset required (delete {_STATE_FILE.name})."
    )
    # Telegram CRITICAL alert — fire and forget
    _send_critical_alert(reason, extra)


def reset() -> None:
    """Manually clear the killswitch. Only call with human confirmation."""
    global _consecutive_losses, _daily_realized_loss, _daily_reset_ts
    if _STATE_FILE.exists():
        _STATE_FILE.unlink()
    _consecutive_losses = 0
    _daily_realized_loss = 0.0
    _daily_reset_ts = time.time()
    log.info("[KILLSWITCH] Reset. Trading resumes.")


def record_trade_result(pnl_usd: float) -> None:
    """
    Called after every closed trade. Updates counters and may trip.
    pnl_usd: realized P&L in dollars (negative = loss).
    """
    global _consecutive_losses, _daily_realized_loss
    _reset_daily_if_needed()

    if pnl_usd < 0:
        _consecutive_losses += 1
        _daily_realized_loss += -pnl_usd
    else:
        _consecutive_losses = 0
        # C4 fix: 수익도 차감 — net daily loss 기준으로 killswitch 판단
        _daily_realized_loss = max(0.0, _daily_realized_loss - pnl_usd)

    if _consecutive_losses >= MAX_CONSECUTIVE_LOSSES:
        trip(
            "consecutive_losses",
            consecutive=_consecutive_losses,
            limit=MAX_CONSECUTIVE_LOSSES,
        )
    if _daily_realized_loss >= MAX_DAILY_LOSS_USD:
        trip(
            "daily_loss_limit",
            daily_loss=round(_daily_realized_loss, 2),
            limit=MAX_DAILY_LOSS_USD,
        )


def check_drawdown(portfolio) -> None:
    """
    Called periodically (e.g. in the snapshot loop) to check drawdown.
    Trips if >= MAX_DRAWDOWN_HALT.
    """
    dd = getattr(portfolio, "drawdown", 0.0)
    if dd >= config.MAX_DRAWDOWN_HALT:
        trip(
            "drawdown_halt",
            drawdown_pct=round(dd * 100, 2),
            limit_pct=config.MAX_DRAWDOWN_HALT * 100,
        )
