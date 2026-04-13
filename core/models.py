"""
Pydantic v2 schemas — the contract between every module.
Change these carefully: downstream modules depend on every field.
"""
from __future__ import annotations
from typing import Literal, Optional
from pydantic import BaseModel, Field, computed_field
import time


# ── Market data ──────────────────────────────────────────────────────────────

class Token(BaseModel):
    token_id: str
    outcome: str          # "Yes" / "No"
    price: float          # current mid 0.0–1.0
    winner: Optional[bool] = None

    @computed_field
    @property
    def is_near_certain(self) -> bool:
        """
        Near-certainty zone where fees approach 0.
        fee = rate * size * (1 - price)
        At price > 0.95: fee < 0.1% of invested USD — systematically cheap to trade.
        """
        from config import EXTREME_PRICE_THRESHOLD
        return self.price > (1 - EXTREME_PRICE_THRESHOLD)

    def fee_cost(self, size_usd: float, is_taker: bool = True) -> float:
        """
        Exact fee for a given USD position.
        fee = fee_rate * size_usd * (1 - price)

        At price=0.99: fee = 0.02 * 0.01 * size = 0.02% of position (near zero)
        At price=0.50: fee = 0.02 * 0.50 * size = 1.00% of position
        At price=0.01: fee = 0.02 * 0.99 * size = 1.98% of position (expensive)
        """
        from config import TAKER_FEE_RATE, MAKER_FEE_RATE
        rate = TAKER_FEE_RATE if is_taker else MAKER_FEE_RATE
        return rate * size_usd * (1 - self.price)


class Market(BaseModel):
    condition_id: str
    question: str
    end_date_iso: str
    tokens: list[Token]
    volume_24h: float = 0.0
    liquidity: float = 0.0
    category: str = ""
    active: bool = True
    tags: list[str] = Field(default_factory=list)
    # Oracle dispute risk — populated by oracle_risk module
    dispute_risk: float = 0.0     # P(UMA dispute), 0–1
    wording_ambiguity: float = 0.0  # 0=clear, 1=very ambiguous

    @computed_field
    @property
    def days_to_resolution(self) -> float:
        from datetime import datetime, timezone, date
        try:
            s = self.end_date_iso.replace("Z", "+00:00")
            # Handle both "2026-04-12" (date only) and "2026-04-12T00:00:00+00:00"
            if "T" in s:
                end = datetime.fromisoformat(s)
            else:
                d = date.fromisoformat(s[:10])
                end = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
            now = datetime.now(timezone.utc)
            return max(0.0, (end - now).total_seconds() / 86400)
        except Exception:
            return 30.0

    @computed_field
    @property
    def yes_token(self) -> Optional[Token]:
        for t in self.tokens:
            if t.outcome.lower() in ("yes", "true", "1"):
                return t
        return self.tokens[0] if self.tokens else None

    @computed_field
    @property
    def no_token(self) -> Optional[Token]:
        for t in self.tokens:
            if t.outcome.lower() in ("no", "false", "0"):
                return t
        return self.tokens[1] if len(self.tokens) > 1 else None

    @computed_field
    @property
    def yes_no_sum(self) -> float:
        """YES + NO prices. < 1.0 = potential internal arb opportunity."""
        yes = self.yes_token
        no = self.no_token
        if yes and no:
            return yes.price + no.price
        return 1.0


class OrderBook(BaseModel):
    token_id: str
    timestamp: float = Field(default_factory=time.time)
    bids: list[tuple[float, float]] = Field(default_factory=list)  # [(price, size)]
    asks: list[tuple[float, float]] = Field(default_factory=list)

    @computed_field
    @property
    def best_bid(self) -> float:
        return self.bids[0][0] if self.bids else 0.0

    @computed_field
    @property
    def best_ask(self) -> float:
        return self.asks[0][0] if self.asks else 1.0

    @computed_field
    @property
    def spread(self) -> float:
        return self.best_ask - self.best_bid

    @computed_field
    @property
    def mid(self) -> float:
        return (self.best_bid + self.best_ask) / 2

    def bid_depth(self, levels: int = 3) -> float:
        return sum(s for _, s in self.bids[:levels])

    def ask_depth(self, levels: int = 3) -> float:
        return sum(s for _, s in self.asks[:levels])

    @computed_field
    @property
    def imbalance(self) -> float:
        """Positive = more bid pressure, negative = more ask pressure."""
        b = self.bid_depth()
        a = self.ask_depth()
        total = b + a
        if total == 0:
            return 0.0
        return (b - a) / total

    def is_stale(self, max_age_sec: float = 10.0) -> bool:
        return (time.time() - self.timestamp) > max_age_sec


# ── Signals ──────────────────────────────────────────────────────────────────

SignalStrategy = Literal[
    "internal_arb",
    "correlated_arb",
    "cross_platform",
    "closing_convergence",
    "oracle_convergence",
    "order_flow",
    "copy_trade",
    "fee_arbitrage",
    "mm_rebalance",
    "news_alpha",
]

class Signal(BaseModel):
    signal_id: str
    strategy: SignalStrategy
    condition_id: str
    token_id: str
    direction: Literal["BUY", "SELL"]
    model_prob: float       # your estimated true probability
    market_prob: float      # current market price
    edge: float             # model_prob - market_prob (signed, before fees)
    net_edge: float         # edge after fees
    confidence: float       # 0–1, drives Kelly effective probability
    urgency: Literal["LOW", "MEDIUM", "HIGH", "IMMEDIATE"]
    created_at: float = Field(default_factory=time.time)
    expires_at: float = 0.0
    # Staleness invalidation: cancel if price moves more than stale_threshold
    stale_price: float = 0.0     # price at signal generation
    stale_threshold: float = 0.0 # cancel if |current_price - stale_price| > this

    def is_stale(self, current_price: float) -> bool:
        if self.stale_threshold <= 0:
            return False
        return abs(current_price - self.stale_price) > self.stale_threshold

    def is_expired(self) -> bool:
        if self.expires_at <= 0:
            return False
        return time.time() > self.expires_at


class AggregatedSignal(BaseModel):
    """Output of signal_aggregator — one per market/direction after dedup + scoring."""
    condition_id: str
    token_id: str
    direction: Literal["BUY", "SELL"]
    composite_confidence: float
    best_net_edge: float
    urgency: Literal["LOW", "MEDIUM", "HIGH", "IMMEDIATE"]
    contributing_signals: list[Signal]
    created_at: float = Field(default_factory=time.time)


# ── Orders ───────────────────────────────────────────────────────────────────

class Order(BaseModel):
    order_id: Optional[str] = None
    condition_id: str
    token_id: str
    side: Literal["BUY", "SELL"]
    price: float
    size_usd: float
    order_type: Literal["GTC", "FOK", "IOC"] = "GTC"
    status: Literal["pending", "open", "filled", "cancelled", "failed"] = "pending"
    fill_price: Optional[float] = None
    created_at: float = Field(default_factory=time.time)
    strategy: str = ""


class Fill(BaseModel):
    order_id: str
    condition_id: str
    token_id: str
    side: Literal["BUY", "SELL"]
    fill_price: float
    fill_size: float
    fee_paid: float
    timestamp: float = Field(default_factory=time.time)


# ── Portfolio ─────────────────────────────────────────────────────────────────

class Position(BaseModel):
    condition_id: str
    token_id: str
    side: Literal["BUY", "SELL"]
    size_shares: float
    avg_entry_price: float
    current_price: float
    entry_time: float = Field(default_factory=time.time)
    strategy: str = ""
    # Oracle dispute risk at time of entry
    dispute_risk_at_entry: float = 0.0

    @computed_field
    @property
    def unrealized_pnl(self) -> float:
        if self.side == "BUY":
            return (self.current_price - self.avg_entry_price) * self.size_shares
        return (self.avg_entry_price - self.current_price) * self.size_shares

    @computed_field
    @property
    def notional_usd(self) -> float:
        return self.avg_entry_price * self.size_shares


class PortfolioState(BaseModel):
    bankroll: float
    positions: dict[str, Position] = Field(default_factory=dict)
    realized_pnl: float = 0.0
    peak_value: float = 0.0
    trade_count: int = 0      # for Kelly calibration phase

    @computed_field
    @property
    def unrealized_pnl(self) -> float:
        return sum(p.unrealized_pnl for p in self.positions.values())

    @computed_field
    @property
    def total_value(self) -> float:
        return self.bankroll + self.unrealized_pnl

    @computed_field
    @property
    def drawdown(self) -> float:
        if self.peak_value <= 0:
            return 0.0
        return (self.peak_value - self.total_value) / self.peak_value

    @computed_field
    @property
    def total_notional(self) -> float:
        return sum(p.notional_usd for p in self.positions.values())


# ── On-chain copy trading ────────────────────────────────────────────────────

class WalletStats(BaseModel):
    address: str
    total_pnl_usd: float
    sharpe_ratio: float
    win_rate: float
    avg_edge: float
    trade_count: int
    last_updated: float = Field(default_factory=time.time)
    is_active: bool = True


class OnChainTrade(BaseModel):
    """A trade decoded from Polygon blockchain logs."""
    tx_hash: str
    wallet_address: str
    condition_id: str
    token_id: str
    side: Literal["BUY", "SELL"]
    price: float
    size_usd: float
    block_number: int
    timestamp: float = Field(default_factory=time.time)


# ── Risk alerts ───────────────────────────────────────────────────────────────

class RiskAlert(BaseModel):
    level: Literal["INFO", "WARN", "CRITICAL"]
    message: str
    drawdown: Optional[float] = None
    timestamp: float = Field(default_factory=time.time)


# ── News ──────────────────────────────────────────────────────────────────────

class NewsItem(BaseModel):
    source: str
    headline: str
    url: str
    published_at: float
    sentiment: float      # -1 to +1
    matched_condition_ids: list[str] = Field(default_factory=list)
    confidence: float = 0.5
