"""
Exit Signal Generator — close profitable positions before they decay.

PROBLEM WITHOUT THIS:
  A winning position at 0.92 (entry 0.70) has 0.22 unrealized gain.
  If we don't sell, it sits until resolution. But near resolution:
    - Spreads widen (market makers flee)
    - Oracle dispute risk spikes
    - Any bad news causes instant snap back

SOLUTION:
  Systematically harvest profits when:
  1. Position captured >75% of potential upside (from entry to 1.0)
  2. Near resolution (< 2 days) and price > 0.88
  3. At >0.95 price (fees near zero anyway, lock in profit)
  4. Held > 14 days with < 5% appreciation (opportunity cost)
"""
from __future__ import annotations
import asyncio
import time
import uuid
from typing import Optional
import config
from core.models import PortfolioState, Position, Market, Signal
from core.logger import log


_SCAN_INTERVAL_SEC      = 120      # scan every 2 minutes
_PROFIT_CAPTURE_RATIO   = 0.75     # sell when we've captured 75% of potential upside
_HIGH_CONFIDENCE_PRICE  = 0.88     # at this price, lock in near-resolution
_NEAR_CERTAIN_PRICE     = 0.95     # above 0.95, fees are near zero — lock in
_NEAR_RESOLUTION_DAYS   = 2.0      # days threshold for early lock-in
_STALE_HOLD_DAYS        = 14.0     # max hold without significant gain
_STALE_GAIN_THRESHOLD   = 0.05     # minimum gain% to not consider position stale


def _should_exit(
    pos: Position,
    market: Optional[Market],
    current_price: float,
) -> Optional[str]:
    """
    Returns exit reason string if we should sell, None if we should hold.
    """
    entry = pos.avg_entry_price
    if entry <= 0 or current_price <= 0:
        return None

    potential_upside = 1.0 - entry       # max possible gain per share
    realized_upside  = current_price - entry
    days_held        = (time.time() - pos.entry_time) / 86400

    if potential_upside <= 0:
        return None

    # Rule 1: Captured >75% of potential upside
    capture_ratio = realized_upside / potential_upside
    if capture_ratio >= _PROFIT_CAPTURE_RATIO:
        return f"profit_capture: {capture_ratio:.0%} of upside captured"

    # Rule 2: Near resolution + high confidence
    if market:
        days_to_res = market.days_to_resolution
        if days_to_res < _NEAR_RESOLUTION_DAYS and current_price > _HIGH_CONFIDENCE_PRICE:
            return f"near_resolution: {days_to_res:.1f}d left, price={current_price:.3f}"

        # Rule 3: Near certain — lock in at >0.95 regardless
        if current_price >= _NEAR_CERTAIN_PRICE:
            return f"near_certain: price={current_price:.3f} > {_NEAR_CERTAIN_PRICE}"

        # Rule 4: Stale hold — held too long without enough gain
        if days_held > _STALE_HOLD_DAYS:
            gain_pct = realized_upside / entry
            if gain_pct < _STALE_GAIN_THRESHOLD:
                return f"stale_hold: {days_held:.0f}d held, only {gain_pct:.1%} gain"

    return None


class ExitSignalGenerator:
    """
    Monitors open positions and emits SELL signals when exit conditions are met.
    """

    def __init__(self, portfolio: PortfolioState, store, signal_bus: asyncio.Queue):
        self._portfolio = portfolio
        self._store     = store
        self._bus       = signal_bus
        self._running   = False
        self._emitted:  dict[str, float] = {}   # token_id → last_emit_time

    async def start(self):
        self._running = True
        log.info("[EXIT] Exit signal generator started")
        while self._running:
            try:
                await self._scan()
            except Exception as e:
                log.error(f"[EXIT] Scan error: {e}")
            await asyncio.sleep(_SCAN_INTERVAL_SEC)

    async def _scan(self):
        now = time.time()
        exits_signaled = 0

        for token_id, pos in list(self._portfolio.positions.items()):
            if pos.side != "BUY":
                continue

            # Throttle: don't re-emit for same position within 10 minutes
            if now - self._emitted.get(token_id, 0) < 600:
                continue

            # Get current price
            book = self._store.get_orderbook(token_id)
            if book and book.best_bid > 0:
                current_price = book.best_bid   # sell at bid
            else:
                current_price = pos.current_price

            if current_price <= 0:
                continue

            market = self._store.get_market(pos.condition_id)
            reason = _should_exit(pos, market, current_price)

            if not reason:
                continue

            # Build exit signal
            fee_pct = config.TAKER_FEE_RATE * (1 - current_price)
            gross_edge = current_price - pos.avg_entry_price
            net_edge   = gross_edge - fee_pct

            if net_edge <= 0:
                continue   # don't exit at a loss

            signal = Signal(
                signal_id=str(uuid.uuid4()),
                strategy="closing_convergence",   # reuse existing strategy label
                condition_id=pos.condition_id,
                token_id=token_id,
                direction="SELL",
                model_prob=current_price,
                market_prob=current_price,
                edge=gross_edge,
                net_edge=net_edge,
                confidence=0.90,
                urgency="HIGH",
                created_at=now,
                expires_at=now + 600,   # 10 min TTL
                stale_price=current_price,
                stale_threshold=0.02,
            )

            await self._bus.put(signal)
            self._emitted[token_id] = now
            exits_signaled += 1

            pnl = (current_price - pos.avg_entry_price) * pos.size_shares
            log.info(
                f"[EXIT] {reason}\n"
                f"  token={token_id[:8]} entry={pos.avg_entry_price:.3f} "
                f"current={current_price:.3f} unrealized=+${pnl:.2f}"
            )

        if exits_signaled:
            log.info(f"[EXIT] {exits_signaled} exit signals emitted")

    def stop(self):
        self._running = False
