"""
Prediction Edge — Main Orchestrator

Startup order:
  1. DB + config validation
  2. Market store (warm from REST)
  3. WebSocket real-time data
  4. All signal generators
  5. Signal aggregator (dedup + ensemble)
  6. Calibration tracker (feedback loop)
  7. Execution gateway
  8. Dashboard

Architecture:
  raw_signal_bus  → signal_aggregator → exec_signal_bus → process_signals → gateway
  orderbook_bus   → market_store (updated live via WebSocket)
  fill_bus        → portfolio state update
  calibration     → DB (outcome recording, Kelly improvement)
"""
import asyncio
import os
import time
from core.db import get_conn
from core.logger import log
from core.models import PortfolioState
from core.calibration import CalibrationTracker
from data.market_store import MarketStore
from data.polymarket_rest import fetch_active_markets
from data.polymarket_ws import start_websocket_manager
from dashboard.web_server import start as start_dashboard
from data.onchain_watcher import OnChainWatcher, build_wallet_database
from signals.oracle_monitor import OracleMonitor, score_oracle_dispute_risk
from signals.fee_arbitrage import FeeArbitrageScanner
from signals.closing_convergence import ClosingConvergenceScanner
from signals.order_flow import OrderFlowMonitor
from signals.correlated_arb import CorrelatedArbScanner
from signals.signal_aggregator import SignalAggregator
from signals.relation_builder import RelationGraphManager
from execution.gateway import ExecutionGateway
from execution.reconciler import PositionReconciler
from sizing.kelly import compute_kelly
from core.models import Order, AggregatedSignal, Signal, Fill, Position, OrderBook
from mm.market_maker import MarketMakerLoop
from data.clob_orderbook_poller import ClobOrderbookPoller
from signals.cross_platform_arb import CrossPlatformArbScanner
from signals.limitless_arb import LimitlessArbScanner
from signals.exit_signal import ExitSignalGenerator
from signals.claude_oracle import ClaudeOracleScanner
from signals.base_rate_oracle import BaseRateOracleScanner
from backtest.auto_tuner import AutoTuner
from core import db
import config


async def fill_consumer(fill_bus: asyncio.Queue, portfolio: PortfolioState, store: MarketStore):
    """Update portfolio state from every fill. This is what keeps positions and bankroll accurate."""
    while True:
        try:
            fill = await asyncio.wait_for(fill_bus.get(), timeout=1.0)
        except asyncio.TimeoutError:
            continue

        if not isinstance(fill, Fill):
            continue

        # DRY_RUN fills: bankroll + positions already handled in gateway._apply_dry_run_bookkeeping()
        # Skip everything except peak_value update and snapshot.
        # Both "dry_" (naive fallback) and "shadow_" (virtual_execute) are DRY_RUN fills.
        is_dry = fill.order_id.startswith("dry_") or fill.order_id.startswith("shadow_")

        if not is_dry:
            # LIVE fills: update positions and bankroll here
            key = fill.token_id
            if fill.side == "BUY":
                if key in portfolio.positions:
                    existing = portfolio.positions[key]
                    total_shares = existing.size_shares + fill.fill_size
                    avg_price = (
                        (existing.avg_entry_price * existing.size_shares + fill.fill_price * fill.fill_size)
                        / total_shares
                    )
                    portfolio.positions[key] = existing.model_copy(update={
                        "size_shares": total_shares,
                        "avg_entry_price": avg_price,
                        "current_price": fill.fill_price,
                    })
                else:
                    market = store.get_market(fill.condition_id)
                    from core.category import effective_category as _eff_cat
                    portfolio.positions[key] = Position(
                        condition_id=fill.condition_id,
                        token_id=fill.token_id,
                        side="BUY",
                        size_shares=fill.fill_size,
                        avg_entry_price=fill.fill_price,
                        current_price=fill.fill_price,
                        strategy=getattr(fill, "strategy", "") or "",
                        category=_eff_cat(market) if market else "",
                        dispute_risk_at_entry=market.dispute_risk if market else 0.0,
                    )
                portfolio.bankroll -= fill.fill_price * fill.fill_size + fill.fee_paid
            elif fill.side == "SELL":
                if key in portfolio.positions:
                    existing = portfolio.positions[key]
                    new_size = existing.size_shares - fill.fill_size
                    realized = (fill.fill_price - existing.avg_entry_price) * fill.fill_size - fill.fee_paid
                    portfolio.realized_pnl += realized
                    db.update_pnl_for_token(key, realized)
                    # Feed the hard killswitch — consecutive losses / daily
                    # loss caps trip automatically after this record.
                    from risk import killswitch as _ks
                    _ks.record_trade_result(realized)
                    if new_size <= 0.001:
                        del portfolio.positions[key]
                    else:
                        portfolio.positions[key] = existing.model_copy(update={
                            "size_shares": new_size,
                            "current_price": fill.fill_price,
                        })
                portfolio.bankroll += fill.fill_price * fill.fill_size - fill.fee_paid

        # Update peak value for drawdown tracking
        if portfolio.total_value > portfolio.peak_value:
            portfolio.peak_value = portfolio.total_value
        # Killswitch drawdown check — runs every fill, cheap.
        try:
            from risk import killswitch as _ks
            _ks.check_drawdown(portfolio)
        except Exception:
            pass

        # 체결 즉시 스냅샷 — 자산곡선 실시간 반영
        try:
            db.insert_snapshot(
                total_value=portfolio.total_value,
                bankroll=portfolio.bankroll,
                unrealized=portfolio.unrealized_pnl,
                realized=portfolio.realized_pnl,
                positions=len(portfolio.positions),
            )
        except Exception:
            pass


async def position_price_updater(portfolio: PortfolioState, store: MarketStore):
    """Keep position current_prices fresh so unrealized PnL is accurate."""
    while True:
        await asyncio.sleep(30)
        for token_id, pos in list(portfolio.positions.items()):
            # Try orderbook mid first (most accurate)
            book = store.get_orderbook(token_id)
            if book and not book.is_stale() and book.bids and book.asks:
                mid = book.mid
                portfolio.positions[token_id] = pos.model_copy(update={"current_price": mid})
                continue

            # Fallback: use Gamma API price from market store
            market = store.get_market(pos.condition_id)
            if market:
                for t in market.tokens:
                    if t.token_id == token_id and t.price > 0:
                        portfolio.positions[token_id] = pos.model_copy(update={"current_price": t.price})
                        break


async def market_maker_manager(store: MarketStore, gateway: ExecutionGateway, portfolio: PortfolioState):
    """Start/stop MM loops per market based on eligibility. Re-evaluates every 10 minutes."""
    active_loops: dict[str, MarketMakerLoop] = {}
    active_tasks: dict[str, asyncio.Task] = {}

    while True:
        await asyncio.sleep(600)
        try:
            eligible: dict[str, str] = {}  # token_id → condition_id
            for m in store.get_active_markets():
                if m.volume_24h < config.MM_MIN_VOLUME_24H:
                    continue
                if m.days_to_resolution < (config.MM_MIN_HOURS_TO_EXPIRY / 24):
                    continue
                if m.dispute_risk > config.ORACLE_DISPUTE_THRESHOLD_SKIP:
                    continue
                yes = m.yes_token
                if yes:
                    eligible[yes.token_id] = m.condition_id

            # Start new loops
            for token_id, condition_id in eligible.items():
                if token_id not in active_loops:
                    market = store.get_market(condition_id)
                    if not market:
                        continue
                    loop = MarketMakerLoop(
                        market=market,
                        token_id=token_id,
                        portfolio=portfolio,
                        gateway=gateway,
                        market_store=store,
                    )
                    task = asyncio.create_task(loop.run(), name=f"mm_{token_id[:8]}")
                    active_loops[token_id] = loop
                    active_tasks[token_id] = task
                    log.info(f"[MM] Started: {market.question[:50]}")

            # Stop ineligible loops
            for token_id in list(active_loops):
                if token_id not in eligible:
                    active_loops[token_id].stop()
                    active_tasks[token_id].cancel()
                    del active_loops[token_id]
                    del active_tasks[token_id]
                    log.info(f"[MM] Stopped: {token_id[:8]}")

        except Exception as e:
            log.error(f"MM manager error: {e}")


async def snapshot_loop(portfolio: PortfolioState, initial_bankroll: float = 75.0):
    """Portfolio 스냅샷을 5분마다 DB에 기록 + sanity check."""
    while True:
        await asyncio.sleep(300)
        try:
            tv = portfolio.total_value
            br = portfolio.bankroll
            pos_val = sum(p.current_price * p.size_shares for p in portfolio.positions.values())
            n_pos = len(portfolio.positions)

            # ── Sanity checks ────────────────────────────────────────────
            # 1. total_value should equal bankroll + position_value
            expected_tv = br + pos_val
            if abs(tv - expected_tv) > 0.10:
                log.error(
                    f"[SANITY] total_value mismatch! "
                    f"reported={tv:.2f} expected={expected_tv:.2f} "
                    f"bankroll={br:.2f} pos_val={pos_val:.2f}"
                )

            # 2. bankroll should never be negative
            if br < -0.01:
                log.error(f"[SANITY] NEGATIVE BANKROLL: ${br:.2f}")

            # 3. total_value shouldn't exceed 2x initial (likely bug)
            if tv > initial_bankroll * 2:
                log.warning(f"[SANITY] total_value suspiciously high: ${tv:.2f}")

            # 4. positions should have reasonable current_price (not 0.5 default)
            for tid, pos in portfolio.positions.items():
                if abs(pos.current_price - 0.5) < 0.001 and abs(pos.avg_entry_price - 0.5) > 0.1:
                    log.warning(
                        f"[SANITY] Position {tid[:12]} has default price 0.50 "
                        f"(entry={pos.avg_entry_price:.4f})"
                    )

            db.insert_snapshot(
                total_value=tv,
                bankroll=br,
                unrealized=portfolio.unrealized_pnl,
                realized=portfolio.realized_pnl,
                positions=n_pos,
            )
            log.info(
                f"[SNAP] ${tv:.2f} | cash=${br:.2f} pos=${pos_val:.2f} "
                f"| dd={portfolio.drawdown:.1%} | {n_pos} positions"
            )
        except Exception as e:
            log.debug(f"Snapshot error: {e}")


async def auto_calibrate_loop(orchestrator_holder: dict, interval_sec: int = 3600):
    """
    매시간 friction.calibrate 호출. trace 50건 이상 누적되면 모델 갱신.

    orchestrator_holder는 dict 래퍼 — calibrate가 모델을 swap하면 다음 라이브
    경로(향후 gateway가 사용할 때)도 동일 인스턴스 참조.
    """
    from friction.calibrate import calibrate, load_latest

    # 부팅 시 가장 최근 캘리브레이션 복원
    try:
        if load_latest(orchestrator_holder["orchestrator"]):
            log.info("[calibrate] loaded previous calibration snapshot on boot")
    except Exception as e:
        log.debug(f"[calibrate] no prior snapshot: {e}")

    while True:
        try:
            await asyncio.sleep(interval_sec)
            report = calibrate(orchestrator_holder["orchestrator"], min_traces=50)
            if report.saved:
                log.info(
                    f"[calibrate] updated: traces={report.n_traces_used} "
                    f"latency_mu={report.latency_mu_ms:.0f}ms "
                    f"cancel_rate={report.cancel_rate:.1%} "
                    f"rejections={report.n_rejected}"
                )
            else:
                log.debug(f"[calibrate] skipped: {report.skip_reason}")
        except Exception as e:
            log.warning(f"[calibrate] loop error: {e}")
            await asyncio.sleep(60)


async def market_refresh_loop(store: MarketStore):
    """REST market refresh every 60s. Also synthesizes orderbooks so signals work without WebSocket."""
    while True:
        try:
            markets = await fetch_active_markets(limit=500)
            for m in markets:
                m.dispute_risk = score_oracle_dispute_risk(m)
            await store.update_markets(markets)

            # Populate orderbooks from REST prices (WebSocket fallback)
            # Spread is approximate but lets all signal generators run
            synth_count = 0
            for m in markets:
                for t in m.tokens:
                    if t.price > 0:
                        half_spread = max(0.005, round(t.price * 0.01, 4))
                        book = OrderBook(
                            token_id=t.token_id,
                            bids=[(round(max(0.01, t.price - half_spread), 4), 500.0)],
                            asks=[(round(min(0.99, t.price + half_spread), 4), 500.0)],
                        )
                        await store.update_orderbook(book)
                        synth_count += 1

            log.info(f"[REST] Market store refreshed: {len(markets)} markets, {synth_count} orderbooks")
        except Exception as e:
            log.error(f"Market refresh error: {e}")
        await asyncio.sleep(60)


async def process_aggregated_signals(
    exec_bus: asyncio.Queue,
    store: MarketStore,
    gateway: ExecutionGateway,
    portfolio: PortfolioState,
):
    """
    Consume AggregatedSignals from the aggregator and execute them.
    This is the final execution loop — only aggregated, deduped signals arrive here.
    """
    while True:
        try:
            item = await asyncio.wait_for(exec_bus.get(), timeout=1.0)
        except asyncio.TimeoutError:
            continue

        # Handle both AggregatedSignal and raw Signal (from direct paths)
        if isinstance(item, AggregatedSignal):
            condition_id = item.condition_id
            token_id = item.token_id
            direction = item.direction
            confidence = item.composite_confidence
            net_edge = item.best_net_edge
            urgency = item.urgency
            strategy = item.contributing_signals[0].strategy if item.contributing_signals else "unknown"
            model_prob = item.contributing_signals[0].model_prob if item.contributing_signals else 0.5
            signal = item.contributing_signals[0] if item.contributing_signals else None
        elif isinstance(item, Signal):
            condition_id = item.condition_id
            token_id = item.token_id
            direction = item.direction
            confidence = item.confidence
            net_edge = item.net_edge
            urgency = item.urgency
            strategy = item.strategy
            model_prob = item.model_prob
            signal = item
        else:
            continue

        # Find market and current price
        market = store.get_market(condition_id)
        token = None
        current_price = 0.0
        if market:
            for t in market.tokens:
                if t.token_id == token_id:
                    token = t
                    current_price = t.price
                    break

        if not current_price:
            log.debug(f"No price for {token_id[:8]}, skipping")
            continue

        # Staleness check
        if signal and signal.is_stale(current_price):
            log.debug(f"Stale signal dropped: {strategy} {condition_id[:8]}")
            continue

        if signal and signal.is_expired():
            log.debug(f"Expired signal dropped: {strategy}")
            continue

        days = market.days_to_resolution if market else 30.0
        is_maker_order = urgency not in ("IMMEDIATE", "HIGH")
        fee_per_dollar = 0.0 if is_maker_order else config.TAKER_FEE_RATE * current_price * (1 - current_price)

        # SELL은 포지션 청산 — Kelly가 아니라 보유 수량에서 사이징.
        # Kelly는 신규 BUY edge 전제라, exit 시그널(model_prob≈market_price)이면
        # 항상 0 반환 → 2026-04-14 런에서 exit 134번 전부 스킵된 원인.
        if direction == "SELL":
            pos = portfolio.positions.get(token_id)
            if not pos or pos.size_shares <= 0:
                log.debug(f"SELL signal but no position: {token_id[:8]}")
                continue
            size_usd = pos.size_shares * current_price
            if size_usd < config.MIN_ORDER_SIZE_USD:
                log.debug(f"SELL position too small: ${size_usd:.2f} on {token_id[:8]}")
                continue
        else:
            # Correlation-aware Kelly: pass portfolio + market identity so
            # the sizer can shrink correlated exposures (same category, same
            # event cluster). Prevents ruin from stacked "independent" bets.
            from core.category import effective_category as _eff_cat
            size_usd = compute_kelly(
                model_prob=model_prob,
                market_price=current_price,
                bankroll=portfolio.bankroll,
                days_to_resolution=days,
                strategy=strategy,
                fee_cost_per_dollar=fee_per_dollar,
                is_maker=is_maker_order,
                portfolio=portfolio,
                condition_id=condition_id,
                category=_eff_cat(market) if market else "",
            )

            # 저edge reserve — 2026-04-14 런 교훈: fee_arb 3% edge가 첫 10분에
            # 자본 전부 소진 → 뒤따라온 claude_oracle 48% edge 시그널이 사이즈 0
            # 으로 starve. edge < 8% 시그널은 bankroll의 50%까지만 사용.
            if net_edge < 0.08:
                size_usd = min(size_usd, portfolio.bankroll * 0.5)

            if size_usd < config.MIN_ORDER_SIZE_USD:
                log.debug(f"Kelly size too small: ${size_usd:.2f} for {strategy}")
                continue

        order = Order(
            condition_id=condition_id,
            token_id=token_id,
            side=direction,
            price=current_price,
            size_usd=size_usd,
            order_type="GTC",
            strategy=strategy,
        )

        fill = await gateway.submit(order, signal=signal, market=market)
        if fill:
            portfolio.trade_count += 1
            log.info(
                f"[EXECUTED] [{strategy}] {direction} ${size_usd:.2f} @ {fill.fill_price:.4f} "
                f"| edge={net_edge:.2%} urgency={urgency} "
                f"| fee=${fill.fee_paid:.3f}"
            )

            # Internal arb: immediately submit the NO leg after YES fills
            if strategy == "internal_arb" and market and market.no_token:
                no_token = market.no_token
                no_book = store.get_orderbook(no_token.token_id)
                if no_book and not no_book.is_stale() and no_book.best_ask > 0:
                    # C2 fix: YES와 동일 share 수 맞추기 위해 NO leg size_usd 재계산
                    yes_shares = fill.fill_size  # YES leg에서 받은 실제 share 수
                    no_size_usd = yes_shares * no_book.best_ask
                    no_order = Order(
                        condition_id=condition_id,
                        token_id=no_token.token_id,
                        side="BUY",
                        price=no_book.best_ask,
                        size_usd=no_size_usd,
                        order_type="FOK",
                        strategy="internal_arb",
                    )
                    no_fill = await gateway.submit(no_order, signal=None, market=market)
                    if no_fill:
                        portfolio.trade_count += 1
                        log.info(
                            f"[INTERNAL ARB] Both legs filled: "
                            f"YES@{fill.fill_price:.4f} NO@{no_fill.fill_price:.4f} "
                            f"net_gap={1.0 - fill.fill_price - no_fill.fill_price:.4f}"
                        )
                    else:
                        log.warning(
                            f"[INTERNAL ARB] YES filled but NO leg missed — "
                            f"naked YES on {token_id[:8]}"
                        )


async def main():
    log.info("=" * 60)
    log.info("  Prediction Edge v2.0 - Starting")
    log.info(f"  Mode: {'DRY RUN (paper trading)' if config.DRY_RUN else 'LIVE TRADING'}")
    log.info("=" * 60)

    # Init DB schema
    get_conn()

    bankroll = float(os.getenv("BANKROLL", "1000"))
    # H3 fix: peak_value를 DB 스냅샷에서 복원 — 재시작 시 drawdown 보호 유지
    saved_peak = bankroll
    try:
        all_snaps = db.get_snapshots(limit=0)  # 전체
        if all_snaps:
            saved_peak = max(bankroll, max(row[1] for row in all_snaps))
    except Exception:
        pass
    portfolio = PortfolioState(bankroll=bankroll, peak_value=saved_peak)

    # 시작 즉시 초기 스냅샷 — 자산곡선 첫 점 확보
    try:
        db.insert_snapshot(
            total_value=portfolio.total_value,
            bankroll=portfolio.bankroll,
            unrealized=0.0,
            realized=0.0,
            positions=0,
        )
    except Exception:
        pass

    # Market store
    store = MarketStore()

    # Event buses
    raw_signal_bus: asyncio.Queue = asyncio.Queue(maxsize=2000)   # all raw signals
    exec_signal_bus: asyncio.Queue = asyncio.Queue(maxsize=500)   # aggregated signals
    orderbook_bus: asyncio.Queue = asyncio.Queue(maxsize=5000)    # WebSocket orderbooks
    fill_bus: asyncio.Queue = asyncio.Queue(maxsize=500)

    # Init execution gateway + reconciler
    gateway = ExecutionGateway(portfolio, fill_bus, store=store)
    reconciler = PositionReconciler(portfolio, gateway.get_clob)
    gateway.set_reconciler(reconciler)

    # 스타트업 인증 검증 — 실매매 모드에서 잘못된 키로 첫 주문까지 기다리지 않음
    creds_ok = await gateway.validate_credentials()
    if not config.DRY_RUN and not creds_ok:
        log.error("LIVE mode credential validation failed. Fix keys and restart.")
        return

    # Initial market fetch
    log.info("Fetching initial market data...")
    markets = await fetch_active_markets(limit=500)
    for m in markets:
        m.dispute_risk = score_oracle_dispute_risk(m)
    await store.update_markets(markets)
    log.info(f"Loaded {len(markets)} markets, {sum(len(m.tokens) for m in markets)} tokens")

    # Build wallet profitability database — 백그라운드로 실행 (대시보드 블로킹 방지)
    async def _build_wallet_db_bg():
        from core.db import get_conn as _gc
        conn = _gc()
        wallet_count = conn.execute("SELECT COUNT(*) FROM wallet_stats").fetchone()[0]
        if wallet_count == 0:
            log.info("No wallet data found. Building wallet database in background (~5 min)...")
            try:
                await build_wallet_database()
                log.info("Wallet database build complete.")
            except Exception as e:
                log.warning(f"Wallet database build failed: {e}. Copy trading will be limited.")
        else:
            log.info(f"Wallet database: {wallet_count} wallets loaded")

    # Init correlated arb scanner (relation graph auto-built and injected)
    corr_arb_scanner = CorrelatedArbScanner(store, raw_signal_bus)

    # 지갑 DB 백그라운드 빌드 태스크 등록
    asyncio.create_task(_build_wallet_db_bg(), name="wallet_db_build")

    # ── ACTIVE STRATEGIES ─────────────────────────────────────────────────────
    # Tier 1 (검증됨, 구조적 엣지): oracle_convergence, fee_arb, correlated_arb
    # Tier 2 (실험적, 캘리브레이션 전): claude_oracle, base_rate
    # Disabled:
    #   - ClosingConvergence: no model edge without external probability source
    #   - OrderFlowMonitor:   REST polling 60s delay, always arrives too late
    #   - MarketMaking:       requires real orderbook depth, not synthetic
    #   - CrossPlatformArb:   re-enable when Kalshi keys are configured
    # ─────────────────────────────────────────────────────────────────────────
    tasks = [
        # Data layer — always on
        asyncio.create_task(market_refresh_loop(store),              name="market_refresh"),
        asyncio.create_task(ClobOrderbookPoller(store, portfolio).start(), name="ob_poller"),

        # Strategy 1: Oracle Convergence
        # After UMA resolves a market, price drifts to 1.0 over 2-10 min.
        # Edge: 3-15% with near-zero dispute risk. Proven, repeatable.
        asyncio.create_task(OracleMonitor(store, raw_signal_bus).start(), name="oracle_monitor"),

        # Strategy 2: Fee Arbitrage (near-certain tokens p > 0.95)
        # Buy a token at 0.97 that will settle at 1.00. Fee ≈ 0.06%. Net ≈ 2.9%.
        # Also catches YES+NO internal arb when real orderbook confirms gap.
        asyncio.create_task(FeeArbitrageScanner(store, raw_signal_bus).start(), name="fee_arb"),

        # Strategy 3: Exit signals on existing positions
        # Lock in profits before they decay. Prevents giving back gains.
        asyncio.create_task(
            ExitSignalGenerator(portfolio, store, raw_signal_bus).start(),
            name="exit_signals"
        ),

        # Strategy 4: Correlated Market Arbitrage (re-enabled, Tier 1)
        # 관련 마켓 간 논리적 제약 위반 탐지: P(specific) > P(general) 같은 구조적 차익
        # 진짜 구조적 알파 — 시장 가격 불일치에서 오는 확실한 엣지
        asyncio.create_task(
            corr_arb_scanner.start(),
            name="correlated_arb"
        ),

        # Strategy 5: Claude Oracle (EXPERIMENTAL — Tier 2)
        # 뉴스 컨텍스트 + 2-샘플 LLM 추정. 캘리브레이션 검증 전.
        # aggregator weight 0.35, kelly shrinkage 높음 → 자동으로 작은 사이즈
        asyncio.create_task(
            ClaudeOracleScanner(store, raw_signal_bus).start(),
            name="claude_oracle"
        ),

        # Strategy 6: Statistical Base Rate (EXPERIMENTAL — Tier 2)
        # 무조건 기저율 → aggregator weight 0.30 → 최소 사이즈
        asyncio.create_task(
            BaseRateOracleScanner(store, raw_signal_bus).start(),
            name="base_rate"
        ),

        # Cross-platform arb: Kalshi (active only when keys are set)
        asyncio.create_task(
            CrossPlatformArbScanner(store, raw_signal_bus).start(),
            name="cross_platform_arb"
        ),

        # Strategy 7: Limitless Exchange cross-arb (no API key needed for scanning)
        # Polymarket fork on Base chain — same events, different prices
        # Public market data = always active. Hedge leg needs LIMITLESS_API_KEY.
        # portfolio passed for SELL signal (exit when Poly overpriced vs Limitless)
        asyncio.create_task(
            LimitlessArbScanner(store, raw_signal_bus, portfolio=portfolio).start(),
            name="limitless_arb"
        ),

        # Pipeline
        asyncio.create_task(
            SignalAggregator(raw_signal_bus, exec_signal_bus).start(),
            name="aggregator"
        ),
        asyncio.create_task(CalibrationTracker(store).start(), name="calibration"),
        asyncio.create_task(
            process_aggregated_signals(exec_signal_bus, store, gateway, portfolio),
            name="execution"
        ),
        asyncio.create_task(fill_consumer(fill_bus, portfolio, store), name="fill_consumer"),
        asyncio.create_task(position_price_updater(portfolio, store),  name="price_updater"),
        asyncio.create_task(reconciler.start(),                        name="reconciler"),
        asyncio.create_task(AutoTuner().start(),                       name="auto_tuner"),
        asyncio.create_task(snapshot_loop(portfolio, bankroll),          name="snapshot"),
    ]

    # Day 7-A: 자동 캘리브레이션 루프 — 매시간 friction_traces 검사 후 모델 갱신
    from friction.orchestrator import FrictionOrchestrator
    _friction_holder = {"orchestrator": FrictionOrchestrator()}
    tasks.append(asyncio.create_task(
        auto_calibrate_loop(_friction_holder, interval_sec=3600),
        name="auto_calibrate"
    ))

    # Day 8: 전략 자동 비활성화 — 매시간 t-검정으로 음수 유의 전략 OFF
    from risk.strategy_disabler import auto_disable_loop
    tasks.append(asyncio.create_task(
        auto_disable_loop(interval_sec=3600),
        name="auto_disable"
    ))

    # Day 14: 매일 새 알파 가설 자동 탐색 + 검증
    from research.agent import research_loop
    tasks.append(asyncio.create_task(
        research_loop(interval_hours=24),
        name="research_agent"
    ))

    # Shadow-Live mark-to-market loop: every 15 min, check resolved markets
    # and update virtual_trades with realized PnL. Runs only in DRY_RUN
    # since shadow data is only recorded in DRY_RUN path.
    if config.DRY_RUN:
        from shadow.mark_to_market import periodic_mark_loop
        _shadow_stop = asyncio.Event()
        tasks.append(asyncio.create_task(
            periodic_mark_loop(_shadow_stop, store), name="shadow_mtm"
        ))
        log.info("[Shadow] mark-to-market loop started")

    # WebSocket real-time orderbook (requires aiohttp connection)
    try:
        ws_tasks = await start_websocket_manager(store, orderbook_bus, signal_bus=raw_signal_bus)
        tasks.extend(ws_tasks)
        log.info(f"WebSocket: {len(ws_tasks)} connection(s) started")
    except Exception as e:
        log.warning(f"WebSocket startup failed: {e}. Falling back to REST polling.")

    # On-chain copy trading — 모든 Polygon RPC 사용 가능 (public RPC 포함)
    # Alchemy: 블록당 2s 폴링으로 더 빠름 / public RPC: 5-10s 딜레이 허용
    if config.POLYGON_RPC:
        tasks.append(asyncio.create_task(
            OnChainWatcher(store, raw_signal_bus, portfolio=portfolio).start(),
            name="onchain"
        ))
        rpc_type = "Alchemy (fast)" if "alchemy" in config.POLYGON_RPC.lower() else "public RPC"
        log.info(f"On-chain copy trading: ACTIVE ({rpc_type})")
    else:
        log.warning("On-chain copy trading: DISABLED (set POLYGON_RPC)")

    # Web dashboard — Railway PORT env var 우선 사용
    dashboard_port = int(os.environ.get("PORT", 8080))
    tasks.append(asyncio.create_task(
        start_dashboard(store, portfolio, lambda: gateway.stats, port=dashboard_port),
        name="dashboard"
    ))
    log.info(f"Dashboard: http://0.0.0.0:{dashboard_port}")

    log.info(f"System running with {len(tasks)} tasks. Ctrl+C to stop.")
    log.info(f"Active strategies: oracle_convergence, fee_arb (near-certain + internal_arb), "
             f"exit_signals, cross_platform_arb (if Kalshi keys set), "
             f"claude_oracle (if ANTHROPIC_API_KEY set)")

    try:
        await asyncio.gather(*tasks)
    except (KeyboardInterrupt, asyncio.CancelledError):
        log.info("Shutting down gracefully...")
        for t in tasks:
            t.cancel()
        # Print calibration report on exit
        from core.calibration import get_strategy_calibration_report
        report = get_strategy_calibration_report()
        if report:
            log.info("=== Final Calibration Report ===")
            for strategy, stats in report.items():
                log.info(f"  {strategy}: {stats['trades']} trades, "
                         f"{stats['accuracy']:.1%} accuracy, "
                         f"Kelly={stats['kelly_fraction']:.1%}")


if __name__ == "__main__":
    asyncio.run(main())
