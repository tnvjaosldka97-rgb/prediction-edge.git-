"""
Auto-Tuner — the self-improvement engine.

LOOP:
  Every 7 days:
    1. Load resolved signal outcomes from DB
    2. Run grid search over all parameters
    3. Save best params to params_optimized.json
    4. Reload config dynamically (no restart needed)

ALSO:
  On startup: immediately load params_optimized.json if it exists.
  This means each restart benefits from all previous optimizations.
"""
from __future__ import annotations
import asyncio
import json
import time
from pathlib import Path
from backtest.optimizer import run_full_optimization
from core.logger import log
from core import db
import config

_PARAMS_FILE       = Path("params_optimized.json")
_RUN_EVERY_DAYS    = 7
_MIN_SIGNALS       = 20     # minimum resolved signals before first optimization


def load_optimized_params() -> dict:
    """Load previously saved optimized params. Called at startup."""
    if not _PARAMS_FILE.exists():
        return {}
    try:
        params = json.loads(_PARAMS_FILE.read_text())
        params.pop("optimized_at", None)
        params.pop("n_signals", None)
        return params
    except Exception:
        return {}


def apply_params(params: dict) -> None:
    """
    Apply optimized params to live config.
    Directly patches the config module so all running code sees the new values.
    Only updates scalar config values (not strategy-specific sub-params yet).
    """
    applied = []
    for key, value in params.items():
        if hasattr(config, key) and not key.startswith("_"):
            setattr(config, key, value)
            applied.append(f"{key}={value}")
    if applied:
        log.info(f"[AUTO-TUNE] Applied {len(applied)} params: {', '.join(applied)}")


def get_strategy_min_confidence(strategy: str) -> float:
    """
    Get the optimized minimum confidence threshold for a strategy.
    Call this from signal generators / aggregator to filter weak signals.
    """
    if not _PARAMS_FILE.exists():
        return 0.0
    try:
        params = json.loads(_PARAMS_FILE.read_text())
        return float(params.get(f"{strategy}_min_confidence", 0.0))
    except Exception:
        return 0.0


class AutoTuner:
    """
    Periodic parameter optimizer. Runs as an asyncio task.
    """

    def __init__(self):
        self._last_run = 0.0

    async def start(self):
        log.info("[AUTO-TUNE] Auto-tuner started")

        # Load params from previous runs immediately
        params = load_optimized_params()
        if params:
            apply_params(params)
            ts = params.get("optimized_at")
            age = f"{(time.time() - ts) / 3600:.0f}h ago" if ts else "unknown"
            log.info(f"[AUTO-TUNE] Loaded {len(params)} previously optimized params ({age})")
        else:
            log.info("[AUTO-TUNE] No previous optimization found — will run after data accumulates")

        while True:
            await asyncio.sleep(3600)   # check every hour

            # Only run every 7 days
            if time.time() - self._last_run < _RUN_EVERY_DAYS * 86400:
                continue

            # Check if we have enough data
            conn = db.get_conn()
            n = conn.execute(
                "SELECT COUNT(*) FROM signals WHERE was_correct IS NOT NULL"
            ).fetchone()[0]

            if n < _MIN_SIGNALS:
                log.info(f"[AUTO-TUNE] {n}/{_MIN_SIGNALS} resolved signals — waiting for more data")
                continue

            await self._run_optimization(n)

    async def _run_optimization(self, n_signals: int):
        log.info(f"[AUTO-TUNE] Starting optimization cycle ({n_signals} resolved signals)...")

        try:
            # Run in executor so it doesn't block the event loop
            loop  = asyncio.get_event_loop()
            params = await loop.run_in_executor(None, run_full_optimization)

            if not params:
                return

            params["optimized_at"] = time.time()
            params["n_signals"]    = n_signals

            # Save to file
            _PARAMS_FILE.write_text(json.dumps(params, indent=2))
            log.info(f"[AUTO-TUNE] Saved {len(params)} optimized params to {_PARAMS_FILE}")

            # Apply immediately to running system
            apply_params(params)

            self._last_run = time.time()

        except Exception as e:
            log.error(f"[AUTO-TUNE] Optimization failed: {e}")
