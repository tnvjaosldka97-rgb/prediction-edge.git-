"""
Market Making — Stoikov-inspired but adapted for prediction markets.

Key differences from equity market making:
1. Prices follow a JUMP process (not GBM) — news creates instant repricing
2. Adverse selection is CATASTROPHIC during news events (everyone becomes informed)
3. We MUST cancel all quotes when news is detected — this is the #1 risk

The news cancellation shield is not optional. It is the difference between
a profitable MM operation and losing all your spread income in one event.

Stoikov formula used for baseline spread, then multiplied by news_risk_factor.
"""
from __future__ import annotations
import asyncio
import math
import time
from typing import Optional
import config
from core.models import OrderBook, Market, Order
from core.logger import log


class NewsRiskMonitor:
    """
    Tracks news risk level per market.
    Used to widen spreads or cancel quotes entirely.

    In production: integrate with Twitter streaming API.
    For now: detect rapid price movement as a proxy for news.
    """

    def __init__(self):
        # Per-token: list of (timestamp, price) for recent prices
        self._price_history: dict[str, list[tuple[float, float]]] = {}
        self._news_risk_scores: dict[str, float] = {}

    def update_price(self, token_id: str, price: float):
        history = self._price_history.setdefault(token_id, [])
        now = time.time()
        history.append((now, price))
        # Keep last 5 minutes
        self._price_history[token_id] = [(t, p) for t, p in history if now - t < 300]
        self._news_risk_scores[token_id] = self._compute_risk(token_id)

    def _compute_risk(self, token_id: str) -> float:
        """
        Risk score based on recent price volatility.
        High score = recent price jump = likely news event.
        """
        history = self._price_history.get(token_id, [])
        if len(history) < 3:
            return 1.0  # no data = assume moderate risk

        prices = [p for _, p in history]
        mean = sum(prices) / len(prices)
        variance = sum((p - mean) ** 2 for p in prices) / len(prices)
        std = math.sqrt(variance)

        # Normalized: std of 0 = risk 1.0 (normal), std of 0.05 = risk 5.0 (pause quoting)
        base_risk = 1.0 + (std / 0.01)  # scale: every 1% std = +1x risk

        # Check for sudden large moves (last price vs 5-min-ago price)
        if len(history) >= 2:
            oldest_price = history[0][1]
            latest_price = history[-1][1]
            sudden_move = abs(latest_price - oldest_price)
            if sudden_move > 0.05:  # >5% move in 5 minutes = news detected
                base_risk *= 3.0
            if sudden_move > 0.15:  # >15% move = major news, stop quoting
                base_risk *= config.MM_NEWS_PAUSE_FACTOR

        return base_risk

    def get_risk(self, token_id: str) -> float:
        return self._news_risk_scores.get(token_id, 1.0)

    def should_pause_quoting(self, token_id: str) -> bool:
        return self.get_risk(token_id) >= config.MM_NEWS_PAUSE_FACTOR

    def get_spread_multiplier(self, token_id: str) -> float:
        """How much to multiply the base spread by."""
        risk = self.get_risk(token_id)
        return min(risk, config.MM_NEWS_PAUSE_FACTOR)


news_monitor = NewsRiskMonitor()


def compute_stoikov_spread(
    mid: float,
    inventory_shares: float,
    time_to_resolution_hours: float,
    recent_std: float,
    order_arrival_rate: float = 10.0,
    gamma: float = 0.1,
) -> tuple[float, float]:
    """
    Stoikov optimal spread adapted for prediction markets.

    Returns (bid_price, ask_price).

    Args:
        mid: current mid price
        inventory_shares: current net inventory (positive = long)
        time_to_resolution_hours: hours until market resolves
        recent_std: recent price standard deviation (volatility proxy)
        order_arrival_rate: estimated trades per hour
        gamma: risk aversion parameter (0.1 = moderate)
    """
    T = max(time_to_resolution_hours / 8760, 0.001)  # normalize to years
    sigma = max(recent_std, 0.01)  # floor at 1%

    # Reservation price (inventory-adjusted mid)
    reservation = mid - inventory_shares * gamma * sigma ** 2 * T

    # Optimal spread
    kappa = order_arrival_rate
    if kappa > 0 and gamma > 0:
        spread = (gamma * sigma ** 2 * T) + (2 / gamma) * math.log(1 + gamma / kappa)
    else:
        spread = sigma * 0.05  # fallback: 5% of volatility

    spread = max(spread, 0.01)  # minimum 1 cent spread

    half_spread = spread / 2
    bid = max(reservation - half_spread, 0.01)
    ask = min(reservation + half_spread, 0.99)

    # Ensure bid < ask
    if bid >= ask:
        bid = mid - 0.005
        ask = mid + 0.005

    return round(bid, 4), round(ask, 4)


class MarketMakerLoop:
    """
    Per-market market making loop.
    Runs as an independent asyncio task.
    """

    def __init__(
        self,
        market: Market,
        token_id: str,  # which token to make markets on (YES token)
        portfolio,       # PortfolioState — for inventory + bankroll sizing
        gateway,         # ExecutionGateway — for submit_quote + cancel_order
        market_store,
    ):
        self._market = market
        self._token_id = token_id
        self._portfolio = portfolio
        self._gateway = gateway
        self._store = market_store
        self._running = False
        self._open_bid_id: Optional[str] = None
        self._open_ask_id: Optional[str] = None

    def _should_make_market(self) -> tuple[bool, str]:
        """Pre-flight checks before placing/updating quotes."""
        market = self._market

        if market.volume_24h < config.MM_MIN_VOLUME_24H:
            return False, f"volume too low: ${market.volume_24h:,.0f}"

        if market.days_to_resolution < (config.MM_MIN_HOURS_TO_EXPIRY / 24):
            return False, f"too close to expiry: {market.days_to_resolution:.1f} days"

        if news_monitor.should_pause_quoting(self._token_id):
            return False, "news risk too high — pausing quotes"

        book: Optional[OrderBook] = self._store.get_orderbook(self._token_id)
        if not book:
            return False, "no orderbook"
        # Guard: synthetic orderbooks have exactly 500.0 depth — never quote on fake data
        if book.bids and book.bids[0][1] == 500.0:
            return False, "synthetic orderbook — real-time data not yet available"
        if book.spread > config.MM_MAX_SPREAD_TO_QUOTE:
            return False, f"spread already wide: {book.spread:.3f}"

        return True, "ok"

    async def run(self):
        self._running = True
        while self._running:
            await self._update_quotes()
            await asyncio.sleep(10)  # requote every 10 seconds

    async def _update_quotes(self):
        ok, reason = self._should_make_market()
        if not ok:
            if self._open_bid_id or self._open_ask_id:
                await self._cancel_all_quotes()
                log.debug(f"MM [{self._market.question[:30]}] cancelled: {reason}")
            return

        book: OrderBook = self._store.get_orderbook(self._token_id)
        mid = book.mid

        # Update news risk with current mid
        news_monitor.update_price(self._token_id, mid)

        # Compute spread
        inventory = self._inventory.get(self._token_id, 0)
        hours_to_expiry = self._market.days_to_resolution * 24
        recent_std = self._compute_recent_std()

        bid, ask = compute_stoikov_spread(
            mid=mid,
            inventory_shares=inventory,
            time_to_resolution_hours=hours_to_expiry,
            recent_std=recent_std,
        )

        # Apply news risk multiplier to spread
        multiplier = news_monitor.get_spread_multiplier(self._token_id)
        if multiplier > 1.0:
            spread = ask - bid
            extra_half = (spread * (multiplier - 1)) / 2
            bid = max(bid - extra_half, 0.01)
            ask = min(ask + extra_half, 0.99)

        # Check if requote needed
        if not self._needs_requote(bid, ask):
            return

        # Cancel old quotes, place new ones
        await self._cancel_all_quotes()
        self._open_bid_id = await self._place_quote("BUY", bid)
        self._open_ask_id = await self._place_quote("SELL", ask)

    def _compute_recent_std(self) -> float:
        history = news_monitor._price_history.get(self._token_id, [])
        if len(history) < 2:
            return 0.02  # default 2% vol
        prices = [p for _, p in history]
        mean = sum(prices) / len(prices)
        variance = sum((p - mean) ** 2 for p in prices) / len(prices)
        return math.sqrt(variance)

    def _needs_requote(self, new_bid: float, new_ask: float) -> bool:
        # Always requote if no open quotes
        if not self._open_bid_id and not self._open_ask_id:
            return True
        # Check if desired quotes have drifted
        book = self._store.get_orderbook(self._token_id)
        if not book:
            return True
        current_bid = book.best_bid
        current_ask = book.best_ask
        return (
            abs(new_bid - current_bid) > config.MM_REQUOTE_THRESHOLD
            or abs(new_ask - current_ask) > config.MM_REQUOTE_THRESHOLD
        )

    async def _place_quote(self, side: str, price: float) -> Optional[str]:
        """Submit quote via gateway. Returns order_id or None."""
        # Inventory-aware sizing: don't exceed MM_MAX_INVENTORY_PCT of bankroll
        inventory = 0.0
        pos = self._portfolio.positions.get(self._token_id)
        if pos:
            inventory = pos.size_shares if pos.side == "BUY" else -pos.size_shares

        max_inv_usd = self._portfolio.bankroll * config.MM_MAX_INVENTORY_PCT
        current_notional = abs(inventory) * price
        remaining = max(0.0, max_inv_usd - current_notional)
        size_usd = min(remaining, max_inv_usd)

        if size_usd < config.MIN_ORDER_SIZE_USD:
            return None

        order = Order(
            condition_id=self._market.condition_id,
            token_id=self._token_id,
            side=side,
            price=price,
            size_usd=size_usd,
            order_type="GTC",
            strategy="market_making",
        )
        _, order_id = await self._gateway.submit_quote(order)
        log.debug(f"MM quote: {side} {price:.4f} ${size_usd:.2f} on {self._market.question[:30]}")
        return order_id

    async def _cancel_all_quotes(self):
        if self._open_bid_id:
            await self._gateway.cancel_order(self._open_bid_id)
            self._open_bid_id = None
        if self._open_ask_id:
            await self._gateway.cancel_order(self._open_ask_id)
            self._open_ask_id = None

    def stop(self):
        self._running = False
