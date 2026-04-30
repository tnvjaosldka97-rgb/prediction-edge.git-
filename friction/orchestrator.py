"""
Friction orchestrator — 7개 마찰 레이어를 순서대로 적용.

이 모듈이 backtest(realistic_engine), live DRY_RUN(gateway._simulate_fill),
shadow virtual_executor 모두의 단일 진입점.

순서:
  1. clob_quirks    — tick 라운딩, min size 결정론적 검증·정규화 (early reject)
  2. rejection      — rate limit, 확률적 거부
  3. latency        — submit→fill 지연 샘플링
  4. network_blip   — 지연 구간에 RPC 정전 발생했나?
  5. slippage       — 미래 호가창에서 walking
  6. partial_fill   — 주문타입별 부분 체결 비율
  7. fund_lock      — 정착 시간 (P&L 무관, 처리량 한도용)
"""
from __future__ import annotations
import random
import time
from dataclasses import dataclass, field
from typing import Callable, Optional

from friction.clob_quirks import ClobQuirks, QuirkResult
from friction.rejection import RejectionModel, RejectionResult
from friction.latency import LatencyModel, LatencySample
from friction.network_blip import NetworkBlipModel
from friction.slippage import SlippageModel, WalkResult
from friction.partial_fill import PartialFillModel, FillRatioResult
from friction.fund_lock import FundLockModel, FundLockResult


@dataclass
class SimulatedFill:
    """체결 시뮬 결과 — gateway·backtest 둘 다 이 형태로 받음."""
    accepted: bool
    rejection_reason: Optional[str]
    requested_price: float
    requested_size_usd: float
    normalized_price: float
    filled_size_usd: float
    filled_size_shares: float
    avg_fill_price: float
    fee_paid: float
    submit_to_fill_ms: float
    settle_delay_sec: float
    slippage_bps: float
    is_partial: bool
    network_blip_during: bool
    latency_timed_out: bool
    levels_consumed: int

    @property
    def is_full_fill(self) -> bool:
        return self.accepted and not self.is_partial and self.filled_size_usd > 0


# 외부 시그니처: 백테스트는 미래 호가 lookup, 라이브는 None (현재 호가 사용)
BookLookup = Callable[[float], object]  # ts -> OrderBook | None


class FrictionOrchestrator:
    def __init__(
        self,
        latency: Optional[LatencyModel] = None,
        slippage: Optional[SlippageModel] = None,
        partial_fill: Optional[PartialFillModel] = None,
        rejection: Optional[RejectionModel] = None,
        network_blip: Optional[NetworkBlipModel] = None,
        clob_quirks: Optional[ClobQuirks] = None,
        fund_lock: Optional[FundLockModel] = None,
        taker_fee_rate: float = 0.02,
        maker_fee_rate: float = 0.0,
    ):
        self.latency = latency or LatencyModel()
        self.slippage = slippage or SlippageModel()
        self.partial_fill = partial_fill or PartialFillModel()
        self.rejection = rejection or RejectionModel()
        self.network_blip = network_blip or NetworkBlipModel()
        self.clob_quirks = clob_quirks or ClobQuirks()
        self.fund_lock = fund_lock or FundLockModel()
        self.taker_fee_rate = taker_fee_rate
        self.maker_fee_rate = maker_fee_rate

    def simulate(
        self,
        side: str,                          # "BUY" / "SELL"
        size_usd: float,
        price: float,
        order_type: str,                    # "FOK" / "IOC" / "GTC"
        is_maker: bool,
        book_at_submit,                     # OrderBook (BUY는 asks, SELL은 bids 사용)
        submit_ts: float,
        future_book_lookup: Optional[BookLookup] = None,
        market_volatility_5m: float = 0.0,
        rng: Optional[random.Random] = None,
    ) -> SimulatedFill:
        r = rng or random

        empty = SimulatedFill(
            accepted=False,
            rejection_reason=None,
            requested_price=price,
            requested_size_usd=size_usd,
            normalized_price=price,
            filled_size_usd=0.0,
            filled_size_shares=0.0,
            avg_fill_price=0.0,
            fee_paid=0.0,
            submit_to_fill_ms=0.0,
            settle_delay_sec=0.0,
            slippage_bps=0.0,
            is_partial=False,
            network_blip_during=False,
            latency_timed_out=False,
            levels_consumed=0,
        )

        # 1. CLOB quirks — early reject
        q: QuirkResult = self.clob_quirks.normalize_and_check(price, size_usd)
        if not q.accepted:
            empty.rejection_reason = q.rejection_reason
            return empty
        norm_price = q.normalized_price

        # 2. Latency 먼저 샘플 (rejection이 latency 길이를 알아야 sig_expired 판정 가능)
        lat: LatencySample = self.latency.sample(rng=r)
        fill_ts = submit_ts + lat.delay_ms / 1000.0

        # 3. Rejection check
        rej: RejectionResult = self.rejection.check(
            size_usd=size_usd,
            price=norm_price,
            submit_ts=submit_ts,
            latency_ms=lat.delay_ms,
            rng=r,
        )
        if rej.rejected:
            empty.rejection_reason = rej.reason
            empty.normalized_price = norm_price
            empty.submit_to_fill_ms = lat.delay_ms
            return empty

        # 4. Network blip — submit_ts ~ fill_ts 구간에 정전 있었나?
        blip_during = self.network_blip.covers_interval(submit_ts, fill_ts)
        if blip_during:
            empty.rejection_reason = "network_blip"
            empty.normalized_price = norm_price
            empty.submit_to_fill_ms = lat.delay_ms
            empty.network_blip_during = True
            return empty

        # 5. 체결 시점 호가창 — 백테스트면 미래 lookup, 라이브면 drift 적용
        if future_book_lookup is not None:
            future_book = future_book_lookup(fill_ts) or book_at_submit
        else:
            # 라이브 시뮬: latency 동안 책이 우리한테 불리하게 움직였다고 가정
            try:
                from friction.book_drift import estimate_drift, apply_drift_to_book
                drift = estimate_drift(book_at_submit.token_id, side, lat.delay_ms)
                future_book = apply_drift_to_book(book_at_submit, drift.drift_pct)
            except Exception:
                future_book = book_at_submit

        # 6. Slippage — 호가 walking
        walk: WalkResult = self.slippage.walk(side, size_usd, future_book)

        if walk.filled_shares == 0:
            empty.rejection_reason = "no_liquidity"
            empty.normalized_price = norm_price
            empty.submit_to_fill_ms = lat.delay_ms
            return empty

        # 7. Partial fill — 주문 타입별 비율
        levels = future_book.asks if side == "BUY" else future_book.bids
        # depth at price: 첫 레벨 가격이 norm_price와 비슷한 만큼
        depth_at_price = walk.filled_usd  # walking 결과가 곧 가용 깊이의 USD 환산
        pf: FillRatioResult = self.partial_fill.compute(
            order_type=order_type,
            size_usd=size_usd,
            depth_at_price_usd=depth_at_price,
            market_volatility_5m=market_volatility_5m,
            rng=r,
        )

        actual_filled_usd = walk.filled_usd * pf.ratio
        actual_filled_shares = walk.filled_shares * pf.ratio
        is_partial = pf.ratio < 0.99 or actual_filled_usd < size_usd * 0.99

        if actual_filled_usd <= 0:
            empty.rejection_reason = "fok_no_full_fill"
            empty.normalized_price = norm_price
            empty.submit_to_fill_ms = lat.delay_ms
            return empty

        # 8. Fee — Polymarket 공식: rate * size * (1 - p)
        fee_rate = self.maker_fee_rate if is_maker else self.taker_fee_rate
        fee_paid = fee_rate * actual_filled_usd * (1 - walk.avg_fill_price)

        # 9. Fund lock — 정착 시간 (정보용)
        lock: FundLockResult = self.fund_lock.settle(rng=r)

        return SimulatedFill(
            accepted=True,
            rejection_reason=None,
            requested_price=price,
            requested_size_usd=size_usd,
            normalized_price=norm_price,
            filled_size_usd=actual_filled_usd,
            filled_size_shares=actual_filled_shares,
            avg_fill_price=walk.avg_fill_price,
            fee_paid=fee_paid,
            submit_to_fill_ms=lat.delay_ms,
            settle_delay_sec=lock.settle_delay_sec,
            slippage_bps=walk.slippage_bps,
            is_partial=is_partial,
            network_blip_during=False,
            latency_timed_out=lat.timed_out,
            levels_consumed=walk.levels_consumed,
        )

    def to_dict(self) -> dict:
        return {
            "latency": self.latency.to_dict(),
            "partial_fill": self.partial_fill.to_dict(),
            "rejection": self.rejection.to_dict(),
            "network_blip": self.network_blip.to_dict(),
            "clob_quirks": self.clob_quirks.to_dict(),
            "fund_lock": self.fund_lock.to_dict(),
            "taker_fee_rate": self.taker_fee_rate,
            "maker_fee_rate": self.maker_fee_rate,
        }
