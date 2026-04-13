"""
Automatic ABI Loader for Polymarket CTF Exchange Contract.

Fetches the ABI from Polygonscan API (free, no key required for basic use).
This enables proper decoding of on-chain trade events without manual work.

The CTF Exchange contract is verified on Polygonscan — ABI is public.
"""
from __future__ import annotations
import json
import os
import time
from typing import Optional
import httpx
from core.logger import log


_ABI_CACHE_FILE = "ctf_exchange_abi.json"
_ABI_CACHE_MAX_AGE = 86400 * 30  # re-fetch after 30 days


async def load_ctf_exchange_abi() -> Optional[list]:
    """
    Load the CTF Exchange contract ABI.
    1. Check local cache
    2. Fetch from Polygonscan if cache is stale
    """
    # Check cache first
    if os.path.exists(_ABI_CACHE_FILE):
        mtime = os.path.getmtime(_ABI_CACHE_FILE)
        if time.time() - mtime < _ABI_CACHE_MAX_AGE:
            with open(_ABI_CACHE_FILE) as f:
                abi = json.load(f)
            log.info(f"CTF Exchange ABI loaded from cache ({len(abi)} entries)")
            return abi

    # Fetch from Polygonscan
    import config
    abi = await _fetch_abi_from_polygonscan(config.CTF_EXCHANGE_ADDRESS)
    if abi:
        with open(_ABI_CACHE_FILE, "w") as f:
            json.dump(abi, f)
        log.info(f"CTF Exchange ABI fetched and cached ({len(abi)} entries)")
    return abi


async def _fetch_abi_from_polygonscan(address: str) -> Optional[list]:
    """Fetch verified contract ABI from Polygonscan."""
    polygonscan_key = os.getenv("POLYGONSCAN_API_KEY", "YourApiKeyToken")
    url = "https://api.polygonscan.com/api"
    params = {
        "module": "contract",
        "action": "getabi",
        "address": address,
        "apikey": polygonscan_key,
    }
    async with httpx.AsyncClient(timeout=15) as client:
        try:
            resp = await client.get(url, params=params)
            data = resp.json()
            if data.get("status") == "1":
                return json.loads(data["result"])
            else:
                log.warning(f"Polygonscan ABI fetch failed: {data.get('message')}")
                return None
        except Exception as e:
            log.warning(f"Polygonscan fetch error: {e}")
            return None


def decode_order_filled_event(log_entry: dict, abi: list) -> Optional[dict]:
    """
    Decode an OrderFilled event using the actual ABI.
    Returns decoded trade data with token IDs and amounts.
    """
    try:
        # Find OrderFilled event in ABI
        event_abi = next(
            (e for e in abi if e.get("name") == "OrderFilled" and e.get("type") == "event"),
            None
        )
        if not event_abi:
            return None

        # Use eth_abi for proper decoding
        try:
            from eth_abi import decode as eth_abi_decode
        except ImportError:
            # Fallback: manual hex decoding
            return _manual_decode_order_filled(log_entry)

        topics = log_entry.get("topics", [])
        data = log_entry.get("data", "0x")

        if len(topics) < 2:
            return None

        # Maker address is indexed (topic[1])
        maker = "0x" + topics[1][-40:]

        # Non-indexed data: makerAssetId, takerAssetId, makerAmount, takerAmount, fee
        data_bytes = bytes.fromhex(data[2:]) if data.startswith("0x") else bytes.fromhex(data)

        if len(data_bytes) < 160:
            return None

        # Decode: bytes32, bytes32, uint256, uint256, uint256
        decoded = eth_abi_decode(
            ["bytes32", "bytes32", "uint256", "uint256", "uint256"],
            data_bytes
        )
        maker_asset_id = decoded[0].hex()
        taker_asset_id = decoded[1].hex()
        maker_amount = decoded[2] / 1e6   # USDC 6 decimals
        taker_amount = decoded[3] / 1e6   # shares 6 decimals
        fee = decoded[4] / 1e6

        # Determine: is maker buying or selling?
        # maker_asset_id is what maker is giving
        # If maker gives USDC → maker is BUYING shares
        # USDC token ID on Polygon (USDC.e contract)
        USDC_TOKEN_ID = "0x2791bca1f2de4661ed88a30c99a7a9449aa84174"

        if maker_asset_id.lower() == USDC_TOKEN_ID.lower()[2:]:
            # Maker gives USDC → BUY
            side = "BUY"
            token_id = taker_asset_id   # what maker receives = the prediction token
            size_usd = maker_amount
            size_shares = taker_amount
        else:
            # Maker gives shares → SELL
            side = "SELL"
            token_id = maker_asset_id
            size_shares = maker_amount
            size_usd = taker_amount

        price = size_usd / size_shares if size_shares > 0 else 0

        return {
            "maker": maker,
            "token_id": token_id,
            "side": side,
            "price": price,
            "size_usd": size_usd,
            "size_shares": size_shares,
            "fee": fee,
            "block_number": int(log_entry.get("blockNumber", "0x0"), 16),
            "tx_hash": log_entry.get("transactionHash", ""),
        }

    except Exception as e:
        log.debug(f"Event decode error: {e}")
        return None


def _manual_decode_order_filled(log_entry: dict) -> Optional[dict]:
    """Fallback manual decoder when eth_abi is not available."""
    topics = log_entry.get("topics", [])
    data = log_entry.get("data", "0x")

    if len(topics) < 2:
        return None

    maker = "0x" + topics[1][-40:]
    data_hex = data[2:] if data.startswith("0x") else data
    data_bytes = bytes.fromhex(data_hex)

    if len(data_bytes) < 160:
        return None

    try:
        # 5 x 32-byte words
        words = [data_bytes[i*32:(i+1)*32] for i in range(5)]
        maker_asset = words[0].hex()
        taker_asset = words[1].hex()
        maker_amount = int.from_bytes(words[2], "big") / 1e6
        taker_amount = int.from_bytes(words[3], "big") / 1e6
        fee = int.from_bytes(words[4], "big") / 1e6

        price = maker_amount / taker_amount if taker_amount > 0 else 0

        return {
            "maker": maker,
            "token_id": taker_asset,
            "side": "BUY",  # simplified assumption
            "price": price,
            "size_usd": maker_amount,
            "size_shares": taker_amount,
            "fee": fee,
            "block_number": int(log_entry.get("blockNumber", "0x0"), 16),
            "tx_hash": log_entry.get("transactionHash", ""),
        }
    except Exception:
        return None
