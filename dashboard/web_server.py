"""
Real-time web dashboard — serves live market data, signals, and trades.
Access at http://localhost:8080
"""
from __future__ import annotations
import asyncio
import time
import sqlite3
from typing import Optional
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import config

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# Shared references injected by main.py
_store = None
_portfolio = None
_gateway_stats = None


def init(store, portfolio, gateway_stats_fn):
    global _store, _portfolio, _gateway_stats
    _store = store
    _portfolio = portfolio
    _gateway_stats = gateway_stats_fn


@app.get("/api/markets")
async def api_markets():
    if not _store:
        return []
    markets = _store.get_active_markets()
    result = []
    for m in sorted(markets, key=lambda x: x.volume_24h, reverse=True)[:100]:
        yes = m.yes_token
        no = m.no_token
        result.append({
            "condition_id": m.condition_id,
            "question": m.question,
            "yes_price": round(yes.price, 4) if yes else None,
            "no_price": round(no.price, 4) if no else None,
            "yes_no_sum": round(m.yes_no_sum, 4),
            "volume_24h": round(m.volume_24h, 0),
            "liquidity": round(m.liquidity, 0),
            "days_to_resolution": round(m.days_to_resolution, 1),
            "category": m.category,
            "dispute_risk": round(m.dispute_risk, 3),
        })
    return result


@app.get("/api/portfolio")
async def api_portfolio():
    if not _portfolio:
        return {}
    p = _portfolio
    positions = []
    for key, pos in p.positions.items():
        positions.append({
            "token_id": pos.token_id[:12],
            "side": pos.side,
            "size_shares": round(pos.size_shares, 4),
            "avg_entry": round(pos.avg_entry_price, 4),
            "current_price": round(pos.current_price, 4),
            "unrealized_pnl": round(pos.unrealized_pnl, 3),
            "strategy": pos.strategy,
        })
    stats = _gateway_stats() if _gateway_stats else {}
    return {
        "bankroll": round(p.bankroll, 2),
        "unrealized_pnl": round(p.unrealized_pnl, 3),
        "realized_pnl": round(p.realized_pnl, 3),
        "total_value": round(p.total_value, 2),
        "drawdown": round(p.drawdown * 100, 2),
        "trade_count": p.trade_count,
        "positions": positions,
        "submitted": stats.get("submitted", 0),
        "rejected": stats.get("rejected", 0),
        "dry_run": stats.get("dry_run", True),
    }


@app.get("/api/trades")
async def api_trades():
    try:
        conn = sqlite3.connect(config.DB_PATH)
        rows = conn.execute(
            "SELECT order_id, condition_id, token_id, side, fill_price, fill_size, fee_paid, strategy, timestamp "
            "FROM trades ORDER BY timestamp DESC LIMIT 50"
        ).fetchall()
        conn.close()
        result = []
        for r in rows:
            result.append({
                "order_id": r[0],
                "condition_id": r[1][:12] if r[1] else "",
                "token_id": r[2][:12] if r[2] else "",
                "side": r[3],
                "fill_price": round(float(r[4] or 0), 4),
                "fill_size": round(float(r[5] or 0), 4),
                "fee_paid": round(float(r[6] or 0), 4),
                "strategy": r[7],
                "time": time.strftime("%H:%M:%S", time.localtime(float(r[8] or 0))),
            })
        return result
    except Exception as e:
        return []


@app.get("/api/signals")
async def api_signals():
    try:
        conn = sqlite3.connect(config.DB_PATH)
        rows = conn.execute(
            "SELECT signal_id, strategy, condition_id, token_id, direction, model_prob, market_prob, net_edge, confidence, urgency, created_at "
            "FROM signals ORDER BY created_at DESC LIMIT 30"
        ).fetchall()
        conn.close()
        result = []
        for r in rows:
            result.append({
                "strategy": r[1],
                "condition_id": r[2][:12] if r[2] else "",
                "direction": r[4],
                "model_prob": round(float(r[5] or 0), 4),
                "market_prob": round(float(r[6] or 0), 4),
                "net_edge": round(float(r[7] or 0), 4),
                "confidence": round(float(r[8] or 0), 3),
                "urgency": r[9],
                "time": time.strftime("%H:%M:%S", time.localtime(float(r[10] or 0))),
            })
        return result
    except Exception:
        return []


@app.get("/api/opportunities")
async def api_opportunities():
    """Best current opportunities: closing soon + near-certain tokens."""
    if not _store:
        return {}
    markets = _store.get_active_markets()

    closing_soon = []
    for m in markets:
        if 0 < m.days_to_resolution <= 3:
            yes = m.yes_token
            no = m.no_token
            if yes and no:
                gap = 1.0 - (yes.price + no.price)
                closing_soon.append({
                    "question": m.question[:70],
                    "days": round(m.days_to_resolution, 1),
                    "yes": round(yes.price, 4),
                    "no": round(no.price, 4),
                    "gap": round(gap, 4),
                    "volume_24h": round(m.volume_24h, 0),
                })
    closing_soon.sort(key=lambda x: x["days"])

    near_certain = []
    for m in markets:
        for t in m.tokens:
            if t.is_near_certain:
                remaining = round(1.0 - t.price, 4)
                near_certain.append({
                    "question": m.question[:70],
                    "outcome": t.outcome,
                    "price": round(t.price, 4),
                    "remaining": remaining,
                    "days": round(m.days_to_resolution, 1),
                    "volume_24h": round(m.volume_24h, 0),
                })
    near_certain.sort(key=lambda x: -x["price"])

    return {
        "closing_soon": closing_soon[:20],
        "near_certain": near_certain[:20],
        "total_markets": len(markets),
    }


@app.get("/api/stats")
async def api_stats():
    if not _store:
        return {}
    markets = _store.get_active_markets()
    near_certain_count = sum(1 for m in markets for t in m.tokens if t.is_near_certain)
    closing_3d = sum(1 for m in markets if 0 < m.days_to_resolution <= 3)
    closing_7d = sum(1 for m in markets if 0 < m.days_to_resolution <= 7)
    arb_opps = sum(1 for m in markets if m.yes_no_sum < 0.98)
    return {
        "total_markets": len(markets),
        "near_certain": near_certain_count,
        "closing_3d": closing_3d,
        "closing_7d": closing_7d,
        "arb_opps": arb_opps,
        "timestamp": time.strftime("%H:%M:%S"),
    }


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return HTMLResponse(open("dashboard/index.html", encoding="utf-8").read())


async def start(store, portfolio, gateway_stats_fn, host="0.0.0.0", port=8080):
    init(store, portfolio, gateway_stats_fn)
    config_uvi = uvicorn.Config(app, host=host, port=port, log_level="warning")
    server = uvicorn.Server(config_uvi)
    await server.serve()
