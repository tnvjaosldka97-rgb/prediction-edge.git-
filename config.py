"""
Central configuration. All constants live here.
Load secrets from .env — never hardcode keys.
"""
import os
from dotenv import load_dotenv

load_dotenv()

# ── Polymarket API endpoints ─────────────────────────────────────────────────
CLOB_HOST      = "https://clob.polymarket.com"
GAMMA_HOST     = "https://gamma-api.polymarket.com"
DATA_API_HOST  = "https://data-api.polymarket.com"
WS_MARKET      = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
WS_USER        = "wss://ws-subscriptions-clob.polymarket.com/ws/user"

# ── External data ────────────────────────────────────────────────────────────
KALSHI_HOST    = "https://api.elections.kalshi.com/trade-api/v2"
METACULUS_HOST = "https://www.metaculus.com/api2"

# ── Polygon / on-chain ───────────────────────────────────────────────────────
POLYGON_RPC          = os.getenv("POLYGON_RPC", "https://polygon-rpc.com")
POLYGON_CHAIN_ID     = 137
# Polymarket CTF Exchange contract on Polygon
CTF_EXCHANGE_ADDRESS = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
POLYMARKET_NEG_RISK  = "0xC5d563A36AE78145C45a50134d48A1215220f80a"

# ── Wallet ───────────────────────────────────────────────────────────────────
PRIVATE_KEY   = os.getenv("PRIVATE_KEY", "")
WALLET_ADDRESS = os.getenv("WALLET_ADDRESS", "")
API_KEY       = os.getenv("POLY_API_KEY", "")
API_SECRET    = os.getenv("POLY_API_SECRET", "")
API_PASSPHRASE = os.getenv("POLY_API_PASSPHRASE", "")

# ── Trading parameters ───────────────────────────────────────────────────────
# $100 시드 최적화:
# - 최소 주문 $2 (Polymarket 최소값)
# - 엣지 기준 5%로 상향 (소액일수록 수수료 비율 체감이 큼)
# - 드로다운 기준 엄격하게 (잃으면 복구가 힘듦)
KELLY_FRACTION         = float(os.getenv("KELLY_FRACTION", "0.05"))
MIN_EDGE_AFTER_FEES    = 0.05        # $100 시드: 5%로 상향 (3%는 소액엔 너무 낮음)
MIN_ORDER_SIZE_USD     = 2.0         # Polymarket 최소 주문 크기
MAX_SINGLE_MARKET_PCT  = 0.08        # $100 시드: 8% ($8) — 너무 잘게 쪼개면 수수료만 냄
MAX_CATEGORY_PCT       = 0.25        # 25% max bankroll per category
MAX_DRAWDOWN_HALT      = 0.20        # $100 시드: 20%까지 허용 ($20 손실 시 halt)
MAX_DRAWDOWN_REDUCE    = 0.12        # 12% 손실 시 사이즈 절반
WHALE_THRESHOLD_USD    = 5_000       # 고래 감지 기준 (변경 없음)
VOLUME_SPIKE_RATIO     = 5.0         # 거래량 급등 기준 (변경 없음)

# ── Market making ────────────────────────────────────────────────────────────
MM_MIN_VOLUME_24H      = 50_000      # skip thin markets
MM_MIN_HOURS_TO_EXPIRY = 72          # don't quote within 72h of resolution
MM_MAX_SPREAD_TO_QUOTE = 0.15        # skip if spread already > 15 cents
MM_REQUOTE_THRESHOLD   = 0.005       # requote if desired vs actual > 0.5 cents
MM_MAX_INVENTORY_PCT   = 0.03        # max 3% bankroll inventory per market
# News risk multiplier thresholds
MM_NEWS_WIDEN_FACTOR   = 3.0         # widen spread 3x when news detected
MM_NEWS_PAUSE_FACTOR   = 10.0        # pause quoting entirely at 10x news risk

# ── Oracle / UMA risk ────────────────────────────────────────────────────────
# p(dispute) thresholds that affect position sizing
ORACLE_DISPUTE_THRESHOLD_SKIP = 0.08   # skip market if P(dispute) > 8%
ORACLE_DISPUTE_THRESHOLD_WARN = 0.03   # warn if P(dispute) > 3%
# After resolution announcement, trade the convergence within this window
ORACLE_CONVERGENCE_WINDOW_SEC = 300    # 5 minutes

# ── Fee model ────────────────────────────────────────────────────────────────
# Polymarket fee: fee_rate * price * (1 - price)
# At price 0.01: fee ≈ 0.0001 * rate (near zero)
# At price 0.50: fee = 0.25 * rate (maximum)
TAKER_FEE_RATE = 0.02   # 2% of notional, scaled by price*(1-price)
MAKER_FEE_RATE = 0.0    # makers pay no fee on Polymarket

# Near-certainty threshold — when token price EXCEEDS (1 - threshold),
# fees approach 0: fee = rate * size * (1 - price)
# At price > 0.95: fee < 0.1% of invested USD
# This is where we find systematically cheap opportunities
EXTREME_PRICE_THRESHOLD = 0.05  # p > 0.95 qualifies as near-certain

# ── On-chain copy trading ────────────────────────────────────────────────────
# Minimum 6-month Sharpe to be considered a top wallet
COPY_TRADE_MIN_SHARPE       = 1.5
COPY_TRADE_MAX_WALLETS       = 20     # follow top 20 wallets
COPY_TRADE_DELAY_BLOCKS      = 1      # copy within 1 block (~2 seconds)
COPY_TRADE_SIZE_FRACTION     = 0.5    # copy at 50% of whale position size
COPY_TRADE_MAX_PRICE_SLIP    = 0.03   # don't copy if price moved > 3% since whale

# ── Calibration ─────────────────────────────────────────────────────────────
# $100 시드는 50 트레이드부터 상향 시작 (100 기다리면 너무 오래 걸림)
KELLY_CALIBRATION_PHASES = {
    0:   0.05,   # 0~49 trades:   5% Kelly  ($5/트레이드)
    50:  0.08,   # 50~149 trades: 8% Kelly  ($8/트레이드)
    150: 0.12,   # 150~299:      12% Kelly  ($12/트레이드, 이 시점엔 자본도 복리로 늘어있을것)
    300: 0.18,   # 300+:         18% Kelly
}

# ── Rate limits ──────────────────────────────────────────────────────────────
CLOB_ORDERS_PER_MIN  = 55    # safely below 60/min hard limit
GAMMA_REQ_PER_MIN    = 90
DATA_API_REQ_PER_MIN = 60

# ── Database ─────────────────────────────────────────────────────────────────
DB_PATH = os.getenv("DB_PATH", "prediction_edge.db")

# ── Dry run mode ─────────────────────────────────────────────────────────────
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"
