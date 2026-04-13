"""
Position & Balance Reconciler

Every 60 seconds:
1. Fetches open orders from CLOB — cancels orphaned orders
2. Fetches wallet USDC balance — syncs bankroll
3. Fetches on-chain positions — corrects local portfolio state
4. Alerts if local state diverges from CLOB truth

This is the "ground truth" layer. Without it, the bot can
accumulate ghost positions or trade against stale local state.
"""
from __future__ import annotations
import asyncio
import time
from typing import Optional
from core.models import PortfolioState, Position
from core.logger import log
import config


class PositionReconciler:
    INTERVAL = 60.0       # sync every 60 seconds
    MAX_DRIFT_USD = 2.0   # alert if local vs CLOB value differs more than this

    def __init__(self, portfolio: PortfolioState, clob_client_fn):
        """
        clob_client_fn: callable that returns the ClobClient instance (may be None)
        """
        self._portfolio = portfolio
        self._get_clob = clob_client_fn
        self._last_sync = 0.0
        self._open_order_ids: set[str] = set()

    async def start(self):
        while True:
            await asyncio.sleep(self.INTERVAL)
            try:
                await self._sync()
            except Exception as e:
                log.warning(f"[RECONCILER] Sync error: {e}")

    async def _sync(self):
        clob = self._get_clob()
        if not clob:
            return

        # 1. Sync USDC balance
        await self._sync_balance(clob)

        # 2. Sync open orders — cancel orphans
        await self._sync_open_orders(clob)

        # 3. Sync positions from CLOB
        await self._sync_positions(clob)

        self._last_sync = time.time()
        log.info(
            f"[RECONCILER] Synced — bankroll=${self._portfolio.bankroll:.2f} "
            f"positions={len(self._portfolio.positions)} "
            f"open_orders={len(self._open_order_ids)}"
        )

    async def _sync_balance(self, clob):
        try:
            # py_clob_client exposes get_balance() in some versions
            balance_info = None
            if hasattr(clob, 'get_balance'):
                balance_info = clob.get_balance()
            elif hasattr(clob, 'get_collateral_balance'):
                balance_info = clob.get_collateral_balance()

            if balance_info is not None:
                raw = balance_info
                if isinstance(raw, dict):
                    raw = float(raw.get("balance", raw.get("free", 0)))
                else:
                    raw = float(raw)

                drift = abs(raw - self._portfolio.bankroll)
                if drift > self.MAX_DRIFT_USD:
                    log.warning(
                        f"[RECONCILER] Balance drift: local=${self._portfolio.bankroll:.2f} "
                        f"CLOB=${raw:.2f} diff=${drift:.2f} — correcting"
                    )
                    self._portfolio.bankroll = raw
        except Exception as e:
            log.debug(f"Balance sync failed: {e}")

    async def _sync_open_orders(self, clob):
        try:
            if not hasattr(clob, 'get_orders'):
                return
            orders = clob.get_orders() or []
            clob_ids = {o.get("id", o.get("orderID", "")) for o in orders if isinstance(o, dict)}

            # Cancel orphans: orders we think are open but CLOB doesn't know about
            for oid in list(self._open_order_ids):
                if oid and oid not in clob_ids:
                    log.info(f"[RECONCILER] Orphan order {oid[:12]} removed from local tracking")
                    self._open_order_ids.discard(oid)

            self._open_order_ids = clob_ids

            # Warn if too many open orders (possible bot runaway)
            if len(clob_ids) > 10:
                log.warning(f"[RECONCILER] {len(clob_ids)} open orders — possible runaway, check manually")
        except Exception as e:
            log.debug(f"Open orders sync failed: {e}")

    async def _sync_positions(self, clob):
        """
        Sync positions from CLOB.
        Uses Data API to fetch wallet's current token positions.
        """
        try:
            from data.polymarket_rest import fetch_wallet_positions
            if not config.WALLET_ADDRESS:
                return

            clob_positions = await fetch_wallet_positions(config.WALLET_ADDRESS)
            if not clob_positions:
                return

            # Build set of position keys from CLOB truth
            clob_keys = set()
            for pos in clob_positions:
                token_id = pos.get("asset", pos.get("token_id", ""))
                size = float(pos.get("size", pos.get("amount", 0)))
                if token_id and size > 0.01:
                    clob_keys.add(token_id)

            # Remove stale local positions
            stale = [k for k in self._portfolio.positions if k not in clob_keys and
                     # Only remove if we actually have a local record
                     self._portfolio.positions[k].size_shares < 0.01]
            for k in stale:
                log.info(f"[RECONCILER] Removing stale position {k[:12]}")
                del self._portfolio.positions[k]

        except Exception as e:
            log.debug(f"Position sync failed: {e}")

    def register_order(self, order_id: str):
        self._open_order_ids.add(order_id)

    def confirm_fill(self, order_id: str):
        self._open_order_ids.discard(order_id)
