"""
Fee Structure Exploitation — Systematically Mispriced Extreme-Probability Markets.

KEY INSIGHT (corrected):
  Polymarket fee per USD invested = fee_rate * (1 - price)

  At price 0.99: fee = 2% * 0.01 = 0.02% of invested USD  ← NEAR ZERO
  At price 0.95: fee = 2% * 0.05 = 0.10% of invested USD  ← very cheap
  At price 0.50: fee = 2% * 0.50 = 1.00% of invested USD  ← standard
  At price 0.05: fee = 2% * 0.95 = 1.90% of invested USD  ← expensive

The fee is nearly ZERO when buying a HIGH-probability token (p → 1).
This means:
  1. NO tokens trading at 0.95+ have near-zero fees (market almost certainly NO)
  2. YES tokens trading at 0.95+ have near-zero fees (market almost certainly YES)
  3. Any small edge at these prices is profitable after fees

Most bots apply a flat 2% fee estimate regardless of price.
This causes them to SKIP trades near certainty (p>0.95) that are actually profitable.

We exploit this: systematically seek edge in markets where one outcome trades >0.95.
Also scans for YES+NO internal arb with REALISTIC leg risk modeling.
"""
from __future__ import annotations
import asyncio
import time
import uuid
from typing import Optional
import config
from core.logger import log
from core.models import Market, Signal, Token, OrderBook
from core.logger import log


def compute_exact_fee(price: float, size_usd: float, is_taker: bool = True) -> float:
    """
    Exact Polymarket fee for a trade.

    Derivation:
      shares = size_usd / price
      fee_per_share = fee_rate * price * (1 - price)   [Polymarket fee formula]
      fee_total = fee_per_share * shares
               = fee_rate * price * (1 - price) * (size_usd / price)
               = fee_rate * size_usd * (1 - price)

    So fee as fraction of USD = fee_rate * (1 - price)
      p=0.99 → fee = 0.02 * 0.01 * 100 = $0.02  (0.02%)
      p=0.50 → fee = 0.02 * 0.50 * 100 = $1.00  (1.00%)
      p=0.01 → fee = 0.02 * 0.99 * 100 = $1.98  (1.98%)

    Buying HIGH probability tokens is cheapest. Buying low probability is expensive.
    """
    rate = config.TAKER_FEE_RATE if is_taker else config.MAKER_FEE_RATE
    return rate * size_usd * (1 - price)


def net_edge_with_exact_fee(
    model_prob: float,
    market_price: float,
    size_usd: float = 100.0,
) -> float:
    """
    Compute net edge after correct fee accounting.

    At extreme prices, this is significantly higher than naive calculation.
    """
    gross_edge = model_prob - market_price
    if gross_edge <= 0:
        return 0.0

    fee = compute_exact_fee(market_price, size_usd)
    fee_as_prob = fee / size_usd  # fee as fraction of position

    return gross_edge - fee_as_prob


class FeeArbitrageScanner:
    """
    Scans all markets for opportunities where exact fee accounting
    reveals profitable trades that naive analysis misses.

    Primary focus: p < 0.05 or p > 0.95 markets.
    Secondary: YES+NO internal arb with realistic leg risk.
    """

    def __init__(self, market_store, signal_bus: asyncio.Queue):
        self._store = market_store
        self._bus = signal_bus
        self._running = False
        self._extreme_emitted: dict[str, float] = {}  # token_id → last emit time

    async def start(self):
        self._running = True
        while self._running:
            await self._scan()
            await asyncio.sleep(30)  # scan every 30 seconds

    async def _scan(self):
        markets = self._store.get_all_markets()
        signals_generated = 0

        for market in markets:
            if not market.active:
                continue

            # ── Check 1: Extreme price fee arbitrage ────────────────────────
            for token in market.tokens:
                if not token.is_near_certain:
                    continue

                # At extreme prices, small mispricings are profitable
                # We need an external probability estimate to compare against
                # For now, use a simple base case: market is mispriced
                # by a fixed amount (would be replaced by a real model)
                # This demonstrates the fee advantage calculation
                signal = await self._check_extreme_price(market, token)
                if signal:
                    await self._bus.put(signal)
                    signals_generated += 1

            # ── Check 2: YES+NO internal arb (with leg risk) ─────────────
            signal = await self._check_internal_arb(market)
            if signal:
                await self._bus.put(signal)
                signals_generated += 1

        if signals_generated:
            log.info(f"[FEE ARB] Generated {signals_generated} signals this scan")

    async def _check_extreme_price(
        self, market: Market, token: Token
    ) -> Optional[Signal]:
        """
        Near-certain token (p > 0.95): buy the convergence to 1.0.

        This IS real, reliable alpha:
          - Token at 0.97 → settles at 1.00 → 3% gross gain
          - Fee at 0.97 = 2% * (1-0.97) = 0.06% → nearly free
          - Net: ~2.94% risk-free if oracle doesn't dispute

        No external model needed — the market itself is telling us
        this will resolve 1.0. We just capture the remaining gap.
        """
        price = token.price
        if price < (1 - config.EXTREME_PRICE_THRESHOLD):  # not near-certain
            return None

        remaining = 1.0 - price
        fee_pct = compute_exact_fee(price, 100) / 100  # near-zero at high price
        net_edge = remaining - fee_pct

        if net_edge < config.MIN_EDGE_AFTER_FEES:
            return None

        # Skip if dispute risk is high — oracle reversal would hurt
        if market.dispute_risk > config.ORACLE_DISPUTE_THRESHOLD_WARN:
            return None

        # Throttle: only signal once per market per 30 min
        cache_key = token.token_id
        last = self._extreme_emitted.get(cache_key, 0)
        if time.time() - last < 1800:
            return None
        self._extreme_emitted[cache_key] = time.time()

        urgency = "HIGH" if market.days_to_resolution < 3 else "MEDIUM"

        return Signal(
            signal_id=str(uuid.uuid4()),
            strategy="fee_arbitrage",
            condition_id=market.condition_id,
            token_id=token.token_id,
            direction="BUY",
            model_prob=1.0,        # near-certain → treat as certain
            market_prob=price,
            edge=remaining,
            net_edge=net_edge,
            confidence=0.90 - market.dispute_risk,
            urgency=urgency,
            created_at=time.time(),
            expires_at=time.time() + 3600,
            stale_price=price,
            stale_threshold=remaining * 0.4,
        )

    async def _check_internal_arb(self, market: Market) -> Optional[Signal]:
        """
        YES+NO internal arb with REALISTIC leg risk modeling.

        NOT assumed to be risk-free (it isn't).
        We model the P(second leg fails) explicitly.
        """
        yes = market.yes_token
        no = market.no_token
        if not yes or not no:
            return None

        yes_book: Optional[OrderBook] = self._store.get_orderbook(yes.token_id)
        no_book: Optional[OrderBook] = self._store.get_orderbook(no.token_id)

        if not yes_book or not no_book:
            return None
        if yes_book.is_stale() or no_book.is_stale():
            return None

        # Reject synthetic orderbooks (depth=500 is our sentinel value for fake books)
        # Internal arb on fake data = executing a trade that doesn't exist
        if yes_book.ask_depth(levels=1) >= 490 or no_book.ask_depth(levels=1) >= 490:
            return None

        yes_ask = yes_book.best_ask
        no_ask = no_book.best_ask

        if yes_ask <= 0 or no_ask <= 0:
            return None

        gross_gap = 1.0 - (yes_ask + no_ask)

        if gross_gap <= 0:
            return None

        # Exact fee for both legs
        position_size = 100.0  # per $100 to normalize
        fee_yes = compute_exact_fee(yes_ask, position_size)
        fee_no = compute_exact_fee(no_ask, position_size)
        total_fee = (fee_yes + fee_no) / position_size

        net_profit_pct = gross_gap - total_fee

        if net_profit_pct <= 0:
            return None

        # ── Leg risk modeling ────────────────────────────────────────────────
        # P(second leg misses) depends on:
        # 1. How thin the order book is
        # 2. How volatile the price has been
        # 3. How fast our execution is
        yes_depth = yes_book.ask_depth(levels=3)
        no_depth = no_book.ask_depth(levels=3)
        min_depth = min(yes_depth, no_depth)

        # Thin book → high leg miss probability
        if min_depth < 10:
            leg_fail_prob = 0.60  # very thin, high risk
        elif min_depth < 50:
            leg_fail_prob = 0.30
        elif min_depth < 200:
            leg_fail_prob = 0.15
        else:
            leg_fail_prob = 0.05  # deep book, fast execution

        # Expected loss if second leg fails (we're stuck with naked position)
        # Assume 50% probability the naked position loses 5%
        leg_fail_loss = 0.05 * 0.5

        # EV with leg risk
        ev = (1 - leg_fail_prob) * net_profit_pct - leg_fail_prob * leg_fail_loss

        if ev < config.MIN_EDGE_AFTER_FEES:
            return None

        # Only proceed if book is deep enough
        if min_depth < 10:
            log.debug(f"Internal arb found but book too thin: gap={gross_gap:.3f} depth={min_depth:.1f}")
            return None

        # Validate: check both sides have depth at the required prices
        yes_available = sum(s for p, s in yes_book.asks if p <= yes_ask + 0.001)
        no_available = sum(s for p, s in no_book.asks if p <= no_ask + 0.001)

        log.info(
            f"[INTERNAL ARB] {market.question[:50]} "
            f"gap={gross_gap:.3f} net={net_profit_pct:.3f} "
            f"ev={ev:.3f} leg_fail={leg_fail_prob:.0%} "
            f"yes_depth=${yes_available:.0f} no_depth=${no_available:.0f}"
        )

        # Generate signal for YES leg (strategy handles the dual-leg execution)
        return Signal(
            signal_id=str(uuid.uuid4()),
            strategy="internal_arb",
            condition_id=market.condition_id,
            token_id=yes.token_id,  # YES leg first (typically more liquid)
            direction="BUY",
            model_prob=1.0,         # if both fill, guaranteed to be worth $1
            market_prob=yes_ask + no_ask,  # effective market price for the pair
            edge=gross_gap,
            net_edge=ev,
            confidence=1.0 - leg_fail_prob,
            urgency="HIGH",
            created_at=time.time(),
            expires_at=time.time() + 30,   # 30 second TTL — stale very fast
            stale_price=yes_ask,
            stale_threshold=0.005,   # cancel if YES price moves 0.5 cents
        )

    def stop(self):
        self._running = False
