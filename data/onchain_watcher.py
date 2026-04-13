"""
On-Chain Copy Trading — HIGHEST ALPHA MODULE.

Polygon blockchain is 100% public. Every Polymarket trade is on-chain.
We can see every profitable wallet's positions in real-time.

Architecture:
1. Build profitability database for all wallets via Data API
2. Score each wallet by Sharpe ratio (not just returns)
3. Monitor top wallets' trades via Polygon RPC
4. Copy their trades within 1-2 blocks (~2-4 seconds)

This is NOT insider trading. This is using PUBLIC data.
This is the #1 underused alpha source on Polymarket.
"""
from __future__ import annotations
import asyncio
import json
import math
import time
import uuid
from typing import Optional
import httpx
import config
from core.models import OnChainTrade, WalletStats, Signal
from core.logger import log
from core import db
from data.polymarket_rest import fetch_wallet_trade_history, fetch_global_trades


# ── Wallet Profitability Scoring ─────────────────────────────────────────────

async def score_wallet(address: str) -> Optional[WalletStats]:
    """
    Compute profitability metrics for a wallet using historical trades.
    Returns WalletStats with Sharpe ratio, win rate, avg edge.
    """
    trades = await fetch_wallet_trade_history(address, limit=500)
    if len(trades) < 20:
        return None  # not enough data to score

    # Build PnL series per market
    # Group trades by condition_id, compute entry/exit PnL
    market_trades: dict[str, list] = {}
    for t in trades:
        cid = t.get("conditionId", "")
        if cid:
            market_trades.setdefault(cid, []).append(t)

    daily_pnl: list[float] = []
    total_pnl = 0.0
    wins = 0
    losses = 0

    for cid, ctrades in market_trades.items():
        # Sort by timestamp
        ctrades.sort(key=lambda x: x.get("timestamp", 0))
        # Estimate PnL: if market resolved, compare avg entry to resolution price
        entry_cost = sum(
            float(t.get("price", 0)) * float(t.get("size", 0))
            for t in ctrades
            if t.get("side", "").upper() in ("BUY", "LONG")
        )
        resolution_value = sum(
            float(t.get("price", 0)) * float(t.get("size", 0))
            for t in ctrades
            if t.get("side", "").upper() in ("SELL", "SHORT")
        )
        pnl = resolution_value - entry_cost
        total_pnl += pnl
        daily_pnl.append(pnl)
        if pnl > 0:
            wins += 1
        elif pnl < 0:
            losses += 1

    if not daily_pnl or (wins + losses) == 0:
        return None

    # Sharpe ratio (simplified: mean / std of per-market PnL)
    mean_pnl = sum(daily_pnl) / len(daily_pnl)
    variance = sum((p - mean_pnl) ** 2 for p in daily_pnl) / len(daily_pnl)
    std_pnl = math.sqrt(variance) if variance > 0 else 1.0
    sharpe = mean_pnl / std_pnl if std_pnl > 0 else 0.0

    win_rate = wins / (wins + losses)
    avg_edge = mean_pnl / max(
        sum(abs(float(t.get("price", 0)) * float(t.get("size", 0))) for t in trades) / len(trades),
        1.0
    )

    stats = WalletStats(
        address=address,
        total_pnl_usd=total_pnl,
        sharpe_ratio=sharpe,
        win_rate=win_rate,
        avg_edge=avg_edge,
        trade_count=len(trades),
    )
    db.upsert_wallet_stats(address, stats.model_dump())
    return stats


async def build_wallet_database() -> list[WalletStats]:
    """
    Scan recent global trades to discover active wallets.
    Score each wallet and persist to DB.
    Run once on startup, then weekly.
    """
    log.info("Building wallet profitability database...")
    trades = await fetch_global_trades(limit=500)

    # Collect unique wallet addresses
    # Data API returns 'proxyWallet' field (not maker/taker)
    wallets: set[str] = set()
    for t in trades:
        for field in ("proxyWallet", "maker", "taker", "user"):
            addr = t.get(field, "")
            if addr and len(addr) == 42 and addr.startswith("0x"):
                wallets.add(addr)
                break

    log.info(f"Discovered {len(wallets)} unique wallets. Scoring...")

    results = []
    for wallet in list(wallets)[:200]:  # score top 200 by activity
        stats = await score_wallet(wallet)
        if stats and stats.sharpe_ratio >= config.COPY_TRADE_MIN_SHARPE:
            results.append(stats)
        await asyncio.sleep(0.5)  # rate limit courtesy

    results.sort(key=lambda x: x.sharpe_ratio, reverse=True)
    top = results[:config.COPY_TRADE_MAX_WALLETS]
    log.info(f"Top {len(top)} wallets by Sharpe: {[f'{w.address[:8]}...(sharpe={w.sharpe_ratio:.2f})' for w in top[:5]]}")
    return top


# ── Real-Time Trade Monitoring via Polygon RPC ───────────────────────────────

async def _get_latest_block() -> int:
    async with httpx.AsyncClient(timeout=5) as client:
        resp = await client.post(
            config.POLYGON_RPC,
            json={"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 1},
        )
        return int(resp.json()["result"], 16)


async def _get_logs(from_block: int, to_block: int, address: str, topic: str) -> list[dict]:
    """Fetch event logs from Polygon for a contract address."""
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(
            config.POLYGON_RPC,
            json={
                "jsonrpc": "2.0",
                "method": "eth_getLogs",
                "params": [{
                    "fromBlock": hex(from_block),
                    "toBlock": hex(to_block),
                    "address": address,
                    "topics": [topic],
                }],
                "id": 1,
            }
        )
        data = resp.json()
        return data.get("result", [])


def _decode_trade_log(log_entry: dict) -> Optional[dict]:
    """
    Decode Polymarket CTF Exchange OrderFilled event.
    Event signature: OrderFilled(bytes32 orderHash, address maker, address taker,
                                 bytes32 makerAssetId, bytes32 takerAssetId,
                                 uint256 makerAmountFilled, uint256 takerAmountFilled,
                                 uint256 fee)
    Topic[0] = keccak256("OrderFilled(...)")
    """
    # OrderFilled event topic (keccak256 of signature)
    ORDER_FILLED_TOPIC = "0x30021c0a2a864e227fa5f28b5303763fe379f4cf7e9aa8986ee659fca3ee244b"

    topics = log_entry.get("topics", [])
    data = log_entry.get("data", "")

    if not topics or topics[0].lower() != ORDER_FILLED_TOPIC.lower():
        return None

    # Simplified decoding — maker address is in topics[1]
    if len(topics) < 2:
        return None

    maker = "0x" + topics[1][-40:]  # last 20 bytes = address

    # Parse data field (256-bit words)
    if len(data) < 2:
        return None
    data_bytes = bytes.fromhex(data[2:])
    if len(data_bytes) < 128:
        return None

    # makerAmountFilled (word 2), takerAmountFilled (word 3)
    # These are in USDC (6 decimals) and shares (6 decimals)
    try:
        maker_amount = int.from_bytes(data_bytes[64:96], "big") / 1e6
        taker_amount = int.from_bytes(data_bytes[96:128], "big") / 1e6
    except Exception:
        return None

    return {
        "maker": maker,
        "maker_amount": maker_amount,
        "taker_amount": taker_amount,
        "block_number": int(log_entry.get("blockNumber", "0x0"), 16),
        "tx_hash": log_entry.get("transactionHash", ""),
    }


class OnChainWatcher:
    """
    Watches Polygon blockchain for trades by top wallets.
    Uses the actual CTF Exchange ABI for proper event decoding.
    No manual work: ABI is auto-fetched from Polygonscan.
    """

    # keccak256("OrderFilled(bytes32,address,address,bytes32,bytes32,uint256,uint256,uint256)")
    # Computed via eth_hash.auto.keccak — verified correct
    ORDER_FILLED_TOPIC = "0x30021c0a2a864e227fa5f28b5303763fe379f4cf7e9aa8986ee659fca3ee244b"

    def __init__(self, market_store, signal_bus: asyncio.Queue, portfolio=None):
        self._market_store = market_store
        self._signal_bus = signal_bus
        self._portfolio = portfolio    # for exit signal generation
        self._top_wallets: set[str] = set()
        self._wallet_sharpes: dict[str, float] = {}   # address → sharpe
        self._last_block = 0
        self._running = False
        self._abi: list = []

    async def start(self):
        # Load ABI (auto-fetched from Polygonscan, cached locally)
        from data.abi_loader import load_ctf_exchange_abi
        self._abi = await load_ctf_exchange_abi() or []

        # Load top wallets + their Sharpe ratios for edge scaling
        top = db.get_top_wallets(config.COPY_TRADE_MIN_SHARPE, config.COPY_TRADE_MAX_WALLETS)
        self._top_wallets = {w["address"].lower() for w in top}
        self._wallet_sharpes = {
            w["address"].lower(): float(w.get("sharpe_ratio", config.COPY_TRADE_MIN_SHARPE))
            for w in top
        }
        log.info(f"OnChainWatcher: monitoring {len(self._top_wallets)} top wallets")

        if not self._top_wallets:
            log.warning("No top wallets in DB. Run build_wallet_database() first.")

        self._last_block = await _get_latest_block()
        self._running = True

        while self._running:
            await self._poll_new_blocks()
            await asyncio.sleep(2)  # Polygon ~2s block time

    async def _poll_new_blocks(self):
        try:
            latest = await _get_latest_block()
            if latest <= self._last_block:
                return

            from_block = self._last_block + 1
            to_block = min(latest, from_block + 10)

            logs = await _get_logs(
                from_block, to_block,
                config.CTF_EXCHANGE_ADDRESS,
                self.ORDER_FILLED_TOPIC,
            )

            for entry in logs:
                decoded = await self._decode_entry(entry)
                if not decoded:
                    continue
                if decoded["maker"].lower() not in self._top_wallets:
                    continue
                await self._handle_whale_trade(decoded)

            self._last_block = to_block
        except Exception as e:
            log.warning(f"OnChainWatcher poll error: {e}")

    async def _decode_entry(self, entry: dict) -> Optional[dict]:
        """Decode using real ABI if available, fallback to manual."""
        if self._abi:
            from data.abi_loader import decode_order_filled_event
            return decode_order_filled_event(entry, self._abi)
        return _decode_trade_log(entry)

    async def _handle_whale_trade(self, trade: dict):
        """
        Generate a copy trade signal from a decoded whale trade.
        Full token_id and price available from proper ABI decoding.
        """
        maker = trade.get("maker", "")
        size_usd = trade.get("size_usd", 0)
        token_id = trade.get("token_id", "")
        price = trade.get("price", 0)
        side = trade.get("side", "BUY")

        if not token_id or price <= 0:
            return

        # Find the market for this token
        market = None
        for m in self._market_store.get_all_markets():
            for t in m.tokens:
                if t.token_id == token_id or token_id in t.token_id:
                    market = m
                    break
            if market:
                break

        if not market:
            log.debug(f"[WHALE] Unknown token {token_id[:12]}, skipping")
            return

        # ── Whale EXIT detection ──────────────────────────────────────────────
        # If the whale is SELLING a token we hold, emit an exit signal
        if side == "SELL" and self._portfolio:
            pos = self._portfolio.positions.get(token_id)
            if pos and pos.side == "BUY":
                exit_signal = Signal(
                    signal_id=str(uuid.uuid4()),
                    strategy="copy_trade",
                    condition_id=market.condition_id,
                    token_id=token_id,
                    direction="SELL",
                    model_prob=price - 0.03,
                    market_prob=price,
                    edge=0.03,
                    net_edge=0.02,
                    confidence=0.70,
                    urgency="HIGH",
                    created_at=time.time(),
                    expires_at=time.time() + 60,
                    stale_price=price,
                    stale_threshold=0.05,
                )
                await self._signal_bus.put(exit_signal)
                log.info(f"[WHALE EXIT] {maker[:8]}... selling {token_id[:8]} — emitting exit signal")
                return

        # Only copy BUY signals with sufficient size
        if side != "BUY" or size_usd < 500:
            return

        # Check if price has already moved too much
        current_book = self._market_store.get_orderbook(token_id)
        if current_book:
            current_price = current_book.mid
            slip = abs(current_price - price) / max(price, 0.001)
            if slip > config.COPY_TRADE_MAX_PRICE_SLIP:
                log.debug(f"[WHALE] Price already moved {slip:.1%}, too late")
                return
        else:
            current_price = price

        # Don't copy near certainty (no room to move)
        if current_price > 0.93 or current_price < 0.07:
            return

        # Sharpe-scaled edge: higher Sharpe wallet → more confident edge assumption
        # Sharpe 1.5 (min threshold) → 1.5% edge
        # Sharpe 3.0 → 3.0% edge
        # Sharpe 5.0+ → 4.0% edge (capped)
        wallet_sharpe = self._wallet_sharpes.get(maker.lower(), config.COPY_TRADE_MIN_SHARPE)
        edge_assumption = min(wallet_sharpe * 0.01, 0.04)

        model_prob = current_price + edge_assumption
        model_prob = max(0.01, min(0.99, model_prob))

        fee_pct = config.TAKER_FEE_RATE * (1 - current_price)
        gross_edge = abs(model_prob - current_price)
        net_edge = gross_edge - fee_pct

        if net_edge < config.MIN_EDGE_AFTER_FEES:
            return

        # Confidence scales with wallet Sharpe and trade size
        # Sharpe 1.5, $500 trade → 0.55 confidence
        # Sharpe 4.0, $10k trade → 0.85 confidence
        size_factor = min(math.log10(max(size_usd, 500)) / math.log10(10000), 1.0)
        sharpe_factor = min(wallet_sharpe / 5.0, 1.0)
        confidence = 0.50 + 0.35 * size_factor * sharpe_factor

        log.info(
            f"[WHALE COPY] {maker[:8]}... ${size_usd:,.0f} BUY @ {price:.4f} "
            f"sharpe={wallet_sharpe:.1f} edge={net_edge:.2%} conf={confidence:.2f} "
            f"| {market.question[:40]}"
        )

        signal = Signal(
            signal_id=str(uuid.uuid4()),
            strategy="copy_trade",
            condition_id=market.condition_id,
            token_id=token_id,
            direction="BUY",
            model_prob=model_prob,
            market_prob=current_price,
            edge=gross_edge,
            net_edge=net_edge,
            confidence=confidence,
            urgency="IMMEDIATE",
            created_at=time.time(),
            expires_at=time.time() + 120,
            stale_price=current_price,
            stale_threshold=config.COPY_TRADE_MAX_PRICE_SLIP,
        )
        await self._signal_bus.put(signal)

    def stop(self):
        self._running = False
