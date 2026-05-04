"""
Drawdown 자동 대응 프로토콜.

drawdown 임계별 자동 액션:
- -3%: 인지만 (텔레그램 INFO)
- -5%: 사이즈 50% 자동 축소 (Kelly multiplier 절반)
- -10%: 신규 진입 정지 (기존 포지션은 자연 청산)
- -15%: SHADOW 모드 자동 전환 (실거래 X, 시뮬만)
- -20%: 비상정지 (killswitch trip)

사람 결정 X. 자동.
사람이 회복하기로 결정하면 명시적 reset 필요.

기존 killswitch는 절대 임계 ($X 손실)이지만, 이건 % drawdown 기반.
"""
from __future__ import annotations
import asyncio
import time
from dataclasses import dataclass


_PROTOCOL_THRESHOLDS = [
    (-0.03, "info", "drawdown 3% — 정상 변동"),
    (-0.05, "size_reduce_50", "drawdown 5% — 사이즈 50% 자동 축소"),
    (-0.10, "halt_new_entries", "drawdown 10% — 신규 진입 정지"),
    (-0.15, "switch_shadow", "drawdown 15% — SHADOW 모드 전환"),
    (-0.20, "killswitch", "drawdown 20% — 비상정지 발동"),
]


@dataclass
class ProtocolAction:
    threshold: float
    action: str
    reason: str


_LAST_TRIGGERED: dict[str, float] = {}    # action → ts


def get_current_drawdown(portfolio_state) -> float:
    """portfolio_state.drawdown 사용 (computed_field)."""
    if not portfolio_state:
        return 0.0
    try:
        return -portfolio_state.drawdown    # drawdown은 양수 → 음수로 변환
    except Exception:
        return 0.0


def determine_action(drawdown_pct: float) -> ProtocolAction | None:
    """현재 drawdown에 적용할 가장 강한 액션."""
    triggered = None
    for threshold, action, reason in _PROTOCOL_THRESHOLDS:
        if drawdown_pct <= threshold:
            triggered = ProtocolAction(threshold, action, reason)
    return triggered


def execute_action(action: ProtocolAction) -> bool:
    """액션 실행. 같은 액션 1시간 내 중복 X."""
    now = time.time()
    if now - _LAST_TRIGGERED.get(action.action, 0) < 3600:
        return False
    _LAST_TRIGGERED[action.action] = now

    from core.logger import log

    # DRY_RUN(paper)에서는 halt/shadow_switch/killswitch 등 진입차단 액션 스킵.
    # 데이터 누적·자체보완이 목적인데 가상 손실로 진입을 막으면 본말전도.
    # info/size_reduce는 통계 학습 위해 허용.
    try:
        import config as _cfg
        if getattr(_cfg, "DRY_RUN", True) and action.action in (
            "halt_new_entries", "switch_shadow", "killswitch"
        ):
            log.warning(
                f"[drawdown_protocol] DRY_RUN — skipping {action.action} "
                f"(would have triggered: {action.reason})"
            )
            return False
    except Exception:
        pass

    if action.action == "info":
        log.info(f"[drawdown_protocol] {action.reason}")
    elif action.action == "size_reduce_50":
        try:
            from sizing.kelly_ramp import _save_state, _load_state
            state = _load_state()
            state["current_multiplier"] = max(0.05, state.get("current_multiplier", 0.5) * 0.5)
            _save_state(state)
            log.warning(f"[drawdown_protocol] sizing reduced 50% — new mult={state['current_multiplier']}")
        except Exception:
            pass
    elif action.action == "halt_new_entries":
        try:
            # runtime_state.json에 halt 플래그
            import json
            from pathlib import Path
            _root = Path(__file__).resolve().parent.parent
            sf = (Path("/data") if Path("/data").exists() else _root) / "runtime_state.json"
            state = json.loads(sf.read_text(encoding="utf-8")) if sf.exists() else {}
            state["new_entries_halted"] = True
            state["halt_reason"] = action.reason
            sf.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")
            log.error(f"[drawdown_protocol] NEW ENTRIES HALTED — {action.reason}")
        except Exception:
            pass
    elif action.action == "switch_shadow":
        try:
            from dashboard.control import save_state as _save, load_state as _load
            state = _load()
            state["mode"] = "SHADOW"
            state["modified_by"] = "auto_drawdown_protocol"
            _save(state)
            log.error(f"[drawdown_protocol] SWITCHED TO SHADOW — {action.reason}")
        except Exception:
            pass
    elif action.action == "killswitch":
        try:
            from risk import killswitch
            killswitch.trip("drawdown_20pct_auto", reason=action.reason)
            log.error(f"[drawdown_protocol] KILLSWITCH TRIPPED — {action.reason}")
        except Exception:
            pass

    # 텔레그램
    try:
        from notifications.telegram import notify
        level = "CRITICAL" if action.action in ("switch_shadow", "killswitch") else "WARN"
        notify(level, f"Drawdown 프로토콜: {action.action}", {
            "threshold": f"{action.threshold*100:.0f}%",
            "reason": action.reason,
        })
    except Exception:
        pass
    # WebSocket
    try:
        from dashboard.realtime import broadcast_event
        broadcast_event("drawdown_protocol", {
            "action": action.action, "threshold": action.threshold, "reason": action.reason,
        })
    except Exception:
        pass
    return True


async def drawdown_monitor_loop(portfolio_state, interval_sec: int = 300):
    """5분마다 drawdown 체크."""
    from core.logger import log
    while True:
        try:
            await asyncio.sleep(interval_sec)
            if not portfolio_state:
                continue
            dd = get_current_drawdown(portfolio_state)
            action = determine_action(dd)
            if action:
                executed = execute_action(action)
                if executed:
                    log.info(f"[drawdown_monitor] dd={dd*100:.1f}% triggered {action.action}")
        except Exception as e:
            from core.logger import log
            log.warning(f"[drawdown_monitor] {e}")
            await asyncio.sleep(60)
