"""
Signal Aggregator — Deduplication, Conflict Resolution, Ensemble Scoring.

Without this module:
  - Two strategies signal the same market simultaneously → double position
  - Two strategies signal OPPOSITE directions → lose on both legs
  - Low-confidence signals trade alongside high-confidence signals with same size

This module is the last filter before execution.
Output: one AggregatedSignal per (market, direction) with composite confidence.

Dedup rules:
  1. Same market + same direction within 5 minutes → keep highest net_edge
  2. Same market + opposite directions → CANCEL BOTH (confused market)
  3. Multiple strategies agree → boost composite confidence

Priority weights by strategy type:
  oracle_convergence: 1.00  (certain outcome, oracle just resolved)
  internal_arb:       0.90  (structural, near risk-free)
  correlated_arb:     0.80  (logical constraint violation)
  fee_arbitrage:      0.75  (structural fee advantage)
  copy_trade:         0.70  (whale following)
  closing_convergence: 0.65 (time-based convergence)
  order_flow:         0.50  (volume/whale signal, uncertain)
  news_alpha:         0.40  (news interpretation is noisy)
"""
from __future__ import annotations
import asyncio
import time
from collections import defaultdict
from core.models import Signal, AggregatedSignal
from core.logger import log


_STRATEGY_WEIGHTS = {
    "oracle_convergence":   1.00,
    "internal_arb":         0.90,
    "correlated_arb":       0.80,
    "fee_arbitrage":        0.75,
    "copy_trade":           0.70,
    "closing_convergence":  0.65,
    "order_flow":           0.50,
    "news_alpha":           0.40,
}

_DEDUP_WINDOW_SEC  = 300   # 5 minutes
_CONFLICT_WINDOW_SEC = 120  # 2 minutes


class SignalAggregator:
    """
    Sits between signal generators and the execution engine.
    Input: raw signals from all strategies (via signal_bus_raw)
    Output: aggregated signals (via signal_bus_exec)
    """

    def __init__(self, raw_bus: asyncio.Queue, exec_bus: asyncio.Queue):
        self._raw = raw_bus
        self._exec = exec_bus
        self._running = False

        # {condition_id: {direction: [Signal]}} — recent signals per market
        self._recent: dict[str, dict[str, list[Signal]]] = defaultdict(
            lambda: {"BUY": [], "SELL": []}
        )

    async def start(self):
        self._running = True
        while self._running:
            try:
                signal = await asyncio.wait_for(self._raw.get(), timeout=1.0)
            except asyncio.TimeoutError:
                self._prune_old_signals()
                continue

            if not isinstance(signal, Signal):
                # Pass non-Signal messages through unchanged
                await self._exec.put(signal)
                continue

            await self._process(signal)

    async def _process(self, signal: Signal):
        now = time.time()
        cid = signal.condition_id

        # Prune old signals for this market
        for direction in ("BUY", "SELL"):
            self._recent[cid][direction] = [
                s for s in self._recent[cid][direction]
                if now - s.created_at < _DEDUP_WINDOW_SEC
            ]

        # ── Conflict detection ──────────────────────────────────────────────
        opposite = "SELL" if signal.direction == "BUY" else "BUY"
        conflict_signals = [
            s for s in self._recent[cid][opposite]
            if now - s.created_at < _CONFLICT_WINDOW_SEC
        ]
        if conflict_signals:
            log.debug(
                f"[AGG] Conflict on {cid[:8]}: {signal.strategy} {signal.direction} "
                f"vs {conflict_signals[0].strategy} {opposite}. Dropping both."
            )
            # Remove conflicting signals and drop current
            self._recent[cid][opposite] = [
                s for s in self._recent[cid][opposite]
                if s not in conflict_signals
            ]
            return  # don't execute in a confused market

        # ── Deduplication ───────────────────────────────────────────────────
        same_dir = self._recent[cid][signal.direction]

        # Check if existing signal for same market/direction with higher edge
        existing = [s for s in same_dir if s.token_id == signal.token_id]
        if existing:
            best_existing = max(existing, key=lambda s: s.net_edge)
            if best_existing.net_edge >= signal.net_edge:
                # Current signal is weaker — discard but update confidence boost
                log.debug(f"[AGG] Dedup: dropping weaker signal for {cid[:8]}")
                return
            else:
                # Current signal is stronger — replace existing
                self._recent[cid][signal.direction] = [
                    s for s in same_dir if s not in existing
                ]

        # Add to recent tracking
        self._recent[cid][signal.direction].append(signal)

        # ── Multi-strategy confidence boost ──────────────────────────────────
        all_same = self._recent[cid][signal.direction]
        strategy_count = len({s.strategy for s in all_same})

        # Base confidence from strategy weight
        base_weight = _STRATEGY_WEIGHTS.get(signal.strategy, 0.5)
        base_confidence = signal.confidence * base_weight

        # Boost when multiple independent strategies agree
        agreement_bonus = min(0.20, (strategy_count - 1) * 0.08)  # up to +20%
        composite_confidence = min(1.0, base_confidence + agreement_bonus)

        if strategy_count > 1:
            log.info(
                f"[AGG] Multi-strategy agreement on {cid[:8]}: "
                f"{[s.strategy for s in all_same]} → confidence boost +{agreement_bonus:.0%}"
            )

        # ── Urgency escalation ───────────────────────────────────────────────
        urgency_rank = {"LOW": 0, "MEDIUM": 1, "HIGH": 2, "IMMEDIATE": 3}
        max_urgency = max(
            all_same, key=lambda s: urgency_rank.get(s.urgency, 0)
        ).urgency

        # ── Build aggregated signal ──────────────────────────────────────────
        best_edge_signal = max(all_same, key=lambda s: s.net_edge)

        agg = AggregatedSignal(
            condition_id=signal.condition_id,
            token_id=signal.token_id,
            direction=signal.direction,
            composite_confidence=composite_confidence,
            best_net_edge=best_edge_signal.net_edge,
            urgency=max_urgency,
            contributing_signals=list(all_same),
        )

        await self._exec.put(agg)

    def _prune_old_signals(self):
        """Remove signals older than dedup window to prevent memory growth."""
        now = time.time()
        for cid in list(self._recent.keys()):
            for direction in ("BUY", "SELL"):
                self._recent[cid][direction] = [
                    s for s in self._recent[cid][direction]
                    if now - s.created_at < _DEDUP_WINDOW_SEC
                ]
            # Clean empty entries
            if not self._recent[cid]["BUY"] and not self._recent[cid]["SELL"]:
                del self._recent[cid]

    def stop(self):
        self._running = False
