"""
Order Flow Intelligence — Whale Detection and Volume Spike Analysis.

The theory: large trades by profitable wallets contain information.
When a whale buys YES, they believe the probability is higher than the price.
We can infer from their trade that we should also go long.

The catch: by the time we see the trade, price has already moved.
Mitigation:
  1. Only follow wallets with high Sharpe (skilled, not lucky)
  2. Size our copy at 50% of whale's size (we're paying worse price)
  3. Only follow into markets that still have room to move (not at 0.95+)
  4. Verify order book hasn't already absorbed the information

Volume Spikes:
  Sudden volume > 5x baseline = someone knows something.
  Even without knowing WHO, the direction of the volume spike is informative.
  The market will re-price; we try to front-run the repricing.
"""
from __future__ import annotations
import asyncio
import time
import uuid
from typing import Optional
import config
from core.models import Signal, OnChainTrade
from core import db
from core.logger import log
from data.polymarket_rest import fetch_global_trades


class OrderFlowMonitor:
    """
    Monitors global trade stream for whale activity and volume spikes.
    Emits directional signals when informed trading is detected.
    """

    def __init__(self, market_store, signal_bus: asyncio.Queue):
        self._store = market_store
        self._bus = signal_bus
        self._running = False

        # Per-market rolling volume: {token_id: [(timestamp, usd_size)]}
        self._volume_history: dict[str, list[tuple[float, float]]] = {}

        # Known profitable wallets from DB
        self._top_wallets: set[str] = set()
        self._last_wallet_refresh = 0

    async def start(self):
        self._running = True
        await self._refresh_top_wallets()
        while self._running:
            await self._poll()
            await asyncio.sleep(30)   # poll every 30 seconds

    async def _refresh_top_wallets(self):
        """Reload top wallet list from DB periodically."""
        wallets = db.get_top_wallets(config.COPY_TRADE_MIN_SHARPE, config.COPY_TRADE_MAX_WALLETS)
        self._top_wallets = {w["address"].lower() for w in wallets}
        self._last_wallet_refresh = time.time()
        log.info(f"[FLOW] Monitoring {len(self._top_wallets)} top wallets")

    async def _poll(self):
        # Refresh wallet list every 6 hours
        if time.time() - self._last_wallet_refresh > 21600:
            await self._refresh_top_wallets()

        trades = await fetch_global_trades(limit=500)
        if not trades:
            return

        now = time.time()
        whale_signals = []
        spike_candidates: dict[str, dict] = {}   # token_id → {side, volume, count}

        for trade in trades:
            try:
                maker = trade.get("maker", "").lower()
                taker = trade.get("taker", "").lower()
                size_usd = float(trade.get("size", 0) or 0)
                price = float(trade.get("price", 0) or 0)
                token_id = trade.get("asset_id", "")
                side = trade.get("side", "").upper()
                ts = float(trade.get("timestamp", now))

                if not token_id or price <= 0:
                    continue

                # Track volume for spike detection
                hist = self._volume_history.setdefault(token_id, [])
                hist.append((ts, size_usd))
                # Keep last 2 hours
                self._volume_history[token_id] = [(t, s) for t, s in hist if now - t < 7200]

                # Whale detection: top wallet + large size
                is_whale = (maker in self._top_wallets or taker in self._top_wallets)
                if is_whale and size_usd >= config.WHALE_THRESHOLD_USD:
                    whale_signals.append({
                        "token_id": token_id,
                        "side": side if maker not in self._top_wallets else side,
                        "size_usd": size_usd,
                        "price": price,
                        "wallet": maker if maker in self._top_wallets else taker,
                    })

                # Accumulate spike candidate data
                if size_usd > 500:   # only count meaningful trades
                    c = spike_candidates.setdefault(token_id, {"buy": 0, "sell": 0, "count": 0})
                    if side in ("BUY", "YES"):
                        c["buy"] += size_usd
                    else:
                        c["sell"] += size_usd
                    c["count"] += 1

            except (ValueError, TypeError):
                continue

        # Process whale signals
        for ws in whale_signals[:5]:   # cap at 5 per poll cycle
            signal = await self._whale_signal(ws)
            if signal:
                await self._bus.put(signal)

        # Process volume spikes
        for token_id, counts in spike_candidates.items():
            signal = await self._spike_signal(token_id, counts)
            if signal:
                await self._bus.put(signal)

    async def _whale_signal(self, whale_trade: dict) -> Optional[Signal]:
        """Generate signal from detected whale trade."""
        token_id = whale_trade["token_id"]
        price = whale_trade["price"]
        direction = "BUY" if whale_trade["side"] in ("BUY", "YES") else "SELL"

        # Find the market for this token
        market = self._find_market_for_token(token_id)
        if not market:
            return None

        # Don't follow whales into near-certain markets (no room to move)
        if price > 0.92 or price < 0.08:
            return None

        # Check if price has already moved significantly (whale already front-ran us)
        book = self._store.get_orderbook(token_id)
        if book:
            current_mid = book.mid
            if direction == "BUY" and current_mid > price * 1.03:
                log.debug(f"Whale already moved price: {price:.3f} → {current_mid:.3f}")
                return None  # price moved >3%, too late
            if direction == "SELL" and current_mid < price * 0.97:
                return None

        # Model probability: assume whale has ~3-5% edge over market price
        edge_assumption = 0.04  # conservative
        model_prob = price + edge_assumption if direction == "BUY" else price - edge_assumption
        model_prob = max(0.01, min(0.99, model_prob))

        fee_pct = config.TAKER_FEE_RATE * (1 - price)
        gross_edge = model_prob - price
        net_edge = gross_edge - fee_pct

        if net_edge < config.MIN_EDGE_AFTER_FEES:
            return None

        log.info(
            f"[WHALE] {whale_trade['wallet'][:8]}... "
            f"${whale_trade['size_usd']:,.0f} {direction} @ {price:.3f} | "
            f"net_edge={net_edge:.2%}"
        )

        return Signal(
            signal_id=str(uuid.uuid4()),
            strategy="order_flow",
            condition_id=market.condition_id,
            token_id=token_id,
            direction=direction,
            model_prob=model_prob,
            market_prob=price,
            edge=gross_edge,
            net_edge=net_edge,
            confidence=0.55,   # moderate confidence — following not leading
            urgency="HIGH",
            created_at=time.time(),
            expires_at=time.time() + 600,   # 10 minute TTL
            stale_price=price,
            stale_threshold=net_edge * 0.6,
        )

    async def _spike_signal(self, token_id: str, counts: dict) -> Optional[Signal]:
        """Generate signal from volume spike."""
        now = time.time()
        hist = self._volume_history.get(token_id, [])

        # Compute 1-hour baseline vs last 5-minute volume
        last_5min = sum(s for t, s in hist if now - t < 300)
        baseline_hourly = sum(s for t, s in hist if now - t < 3600) / 12  # per 5-min

        if baseline_hourly < 100:
            return None   # too little volume to be meaningful

        spike_ratio = last_5min / baseline_hourly if baseline_hourly > 0 else 0
        if spike_ratio < config.VOLUME_SPIKE_RATIO:
            return None

        # Determine direction from buy/sell imbalance
        total = counts["buy"] + counts["sell"]
        if total < 1000:
            return None   # minimum volume threshold

        imbalance = (counts["buy"] - counts["sell"]) / total

        if abs(imbalance) < 0.3:
            return None   # not directional enough

        direction = "BUY" if imbalance > 0 else "SELL"

        market = self._find_market_for_token(token_id)
        if not market:
            return None

        book = self._store.get_orderbook(token_id)
        if not book:
            return None

        price = book.mid

        # Spike implies ~2-3% edge (conservative)
        model_prob = price + 0.03 if direction == "BUY" else price - 0.03
        model_prob = max(0.01, min(0.99, model_prob))
        fee_pct = config.TAKER_FEE_RATE * (1 - price)
        gross_edge = abs(model_prob - price)
        net_edge = gross_edge - fee_pct

        if net_edge < config.MIN_EDGE_AFTER_FEES:
            return None

        urgency = "IMMEDIATE" if spike_ratio > 20 else "HIGH" if spike_ratio > 10 else "MEDIUM"

        log.info(
            f"[SPIKE] {token_id[:8]} spike={spike_ratio:.1f}x "
            f"imbalance={imbalance:+.2f} {direction} | edge={net_edge:.2%}"
        )

        return Signal(
            signal_id=str(uuid.uuid4()),
            strategy="order_flow",
            condition_id=market.condition_id,
            token_id=token_id,
            direction=direction,
            model_prob=model_prob,
            market_prob=price,
            edge=gross_edge,
            net_edge=net_edge,
            confidence=min(0.7, 0.4 + spike_ratio / 50),
            urgency=urgency,
            created_at=time.time(),
            expires_at=time.time() + 300,
            stale_price=price,
            stale_threshold=0.02,
        )

    def _find_market_for_token(self, token_id: str):
        for market in self._store.get_all_markets():
            for token in market.tokens:
                if token.token_id == token_id:
                    return market
        return None

    def stop(self):
        self._running = False
