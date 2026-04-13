"""
Calibration Feedback Loop — The System That Makes Kelly Actually Work.

Without this, Kelly fraction stays at 5% forever.
With this, the system learns from every trade and improves over time.

When a market resolves:
  1. Look up all signals we generated for that market
  2. Record whether each signal was correct (model_prob vs actual_outcome)
  3. Compute running calibration error per strategy
  4. Kelly uses calibration error to shrink probabilities toward 0.5

This is the compound interest of prediction market trading:
better calibration → higher Kelly fraction → more capital per trade → more profit.

Calibration is measured per strategy type because:
  - oracle_convergence signals are very well-calibrated (near-certain resolution)
  - order_flow signals are less calibrated (noisy whale following)
  - closing_convergence depends on how good your timing model is
"""
from __future__ import annotations
import time
from core import db
from core.logger import log


def record_market_outcome(condition_id: str, winning_token_id: str):
    """
    Called when a market resolves. Updates all signals for this market
    with their actual outcome, enabling calibration tracking.

    Args:
        condition_id: The resolved market's condition ID
        winning_token_id: The token ID that resolved to 1.0
    """
    conn = db.get_conn()

    # Find all signals for this market
    signals = conn.execute(
        """SELECT signal_id, token_id, direction, model_prob
           FROM signals
           WHERE condition_id = ? AND was_correct IS NULL""",
        (condition_id,)
    ).fetchall()

    if not signals:
        return

    now = time.time()
    correct_count = 0
    total_count = len(signals)

    for sig in signals:
        # Signal was correct if:
        # BUY on the winning token → correct (market resolved YES for that token)
        # SELL on the losing token → correct (market resolved NO for that token)
        signal_token = sig["token_id"]
        direction = sig["direction"]

        if direction == "BUY":
            was_correct = 1 if signal_token == winning_token_id else 0
            actual_outcome = 1.0 if signal_token == winning_token_id else 0.0
        else:  # SELL
            was_correct = 1 if signal_token != winning_token_id else 0
            actual_outcome = 0.0 if signal_token == winning_token_id else 1.0

        conn.execute(
            """UPDATE signals
               SET resolved_at = ?, actual_outcome = ?, was_correct = ?
               WHERE signal_id = ?""",
            (now, actual_outcome, was_correct, sig["signal_id"])
        )
        if was_correct:
            correct_count += 1

    conn.commit()

    accuracy = correct_count / total_count if total_count > 0 else 0
    log.info(
        f"[CALIBRATION] Market {condition_id[:8]} resolved. "
        f"{correct_count}/{total_count} signals correct ({accuracy:.0%})"
    )


def get_strategy_calibration_report() -> dict:
    """
    Returns calibration report for all strategies.
    Used for logging and dashboard display.
    """
    conn = db.get_conn()
    strategies = conn.execute(
        "SELECT DISTINCT strategy FROM signals WHERE was_correct IS NOT NULL"
    ).fetchall()

    report = {}
    for row in strategies:
        strategy = row["strategy"]
        stats = db.get_calibration_stats(strategy)
        report[strategy] = {
            "trades": stats["count"],
            "accuracy": stats["accuracy"],
            "calibration_error": stats["calibration_error"],
            "kelly_fraction": _get_effective_kelly(stats),
        }

    return report


def _get_effective_kelly(stats: dict) -> float:
    """Current effective Kelly fraction based on calibration."""
    from config import KELLY_CALIBRATION_PHASES
    phases = sorted(KELLY_CALIBRATION_PHASES.items())
    base_fraction = phases[0][1]
    for min_trades, f in phases:
        if stats["count"] >= min_trades:
            base_fraction = f

    # Further reduce by calibration error
    # Perfect calibration (error=0): use full phase fraction
    # Poor calibration (error=0.5): use 25% of phase fraction
    error_multiplier = max(0.25, 1.0 - stats["calibration_error"] * 1.5)
    return base_fraction * error_multiplier


class CalibrationTracker:
    """
    Background task that monitors for market resolutions
    and updates signal calibration records.
    """

    def __init__(self, market_store):
        self._store = market_store
        self._running = False
        self._resolved_markets: set[str] = set()

    async def start(self):
        import asyncio
        self._running = True
        while self._running:
            await self._check_resolutions()
            await asyncio.sleep(60)

    async def _check_resolutions(self):
        for market in self._store.get_all_markets():
            if market.condition_id in self._resolved_markets:
                continue

            winning_token = None
            for token in market.tokens:
                if token.winner is True:
                    winning_token = token
                    break

            if not winning_token:
                continue

            record_market_outcome(market.condition_id, winning_token.token_id)
            self._resolved_markets.add(market.condition_id)

            log.info(
                f"[CALIBRATION] Recorded outcome for: {market.question[:50]} "
                f"winner={winning_token.outcome}"
            )

    def stop(self):
        self._running = False
